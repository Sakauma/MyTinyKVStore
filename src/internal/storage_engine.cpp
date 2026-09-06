#include "storage_engine.h"

#include "io.h"
#include "key_codec.h"
#include "metrics_helpers.h"
#include "value_cache.h"
#include "writer_policy.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <cerrno>
#include <fcntl.h>
#include <limits.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#if defined(__linux__)
#include <sys/xattr.h>
#endif
#include <unistd.h>

namespace kvstore::internal {

namespace {

using Clock = std::chrono::steady_clock;

class CompactionFatalError : public KVStoreError {
public:
    explicit CompactionFatalError(const std::string& message) : KVStoreError(message) {}
};

template <typename Atomic>
void atomic_max(Atomic& target, uint64_t value) {
    uint64_t current = target.load(std::memory_order_relaxed);
    while (current < value &&
           !target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
    }
}

bool is_power_of_two(size_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

uint64_t elapsed_us(const Clock::time_point& start) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count());
}

void lock_file_exclusively(int fd, const std::string& path) {
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            throw KVStoreError("Database is already open by another process: " + path);
        }
        throw io_error("flock", path);
    }
}

class LockedFileGeneration {
public:
    LockedFileGeneration(int fd, std::string path, uint64_t id)
        : fd_(fd), path_(std::move(path)), id_(id) {}

    ~LockedFileGeneration() {
        close_if_open(fd_);
    }

    LockedFileGeneration(const LockedFileGeneration&) = delete;
    LockedFileGeneration& operator=(const LockedFileGeneration&) = delete;

    int fd() const noexcept {
        return fd_;
    }

    const std::string& path() const noexcept {
        return path_;
    }

    uint64_t id() const noexcept {
        return id_;
    }

    void SetPath(std::string path) {
        path_ = std::move(path);
    }

private:
    int fd_ = -1;
    std::string path_;
    uint64_t id_ = 0;
};

class ScopedFd {
public:
    ScopedFd() = default;
    explicit ScopedFd(int fd) : fd_(fd) {}
    ~ScopedFd() {
        close_if_open(fd_);
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    int get() const noexcept {
        return fd_;
    }

    int release() noexcept {
        const int result = fd_;
        fd_ = -1;
        return result;
    }

private:
    int fd_ = -1;
};

int open_database_file(const std::string& path, bool& created) {
    created = false;
    while (true) {
        int fd;
        do {
            fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
        } while (fd < 0 && errno == EINTR);
        if (fd >= 0) {
            return fd;
        }
        if (errno != ENOENT) {
            throw io_error("open", path);
        }

        do {
            fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        } while (fd < 0 && errno == EINTR);
        if (fd >= 0) {
            created = true;
            return fd;
        }
        if (errno != EEXIST) {
            throw io_error("create", path);
        }
    }
}

void fdatasync_or_throw(int fd, const std::string& path) {
    while (::fdatasync(fd) != 0) {
        if (errno == EINTR) {
            continue;
        }
        throw io_error("fdatasync", path);
    }
}

void copy_fd_region(int source_fd,
                    uint64_t source_offset,
                    int destination_fd,
                    uint64_t destination_offset,
                    uint64_t bytes,
                    const std::string& source_path,
                    const std::string& destination_path) {
    std::array<uint8_t, 1024 * 1024> buffer {};
    uint64_t copied = 0;
    while (copied < bytes) {
        const size_t request = static_cast<size_t>(std::min<uint64_t>(buffer.size(), bytes - copied));
        ssize_t nread;
        do {
            nread = ::pread(source_fd,
                            buffer.data(),
                            request,
                            static_cast<off_t>(source_offset + copied));
        } while (nread < 0 && errno == EINTR);
        if (nread < 0) {
            throw CompactionFatalError(io_error("pread", source_path).what());
        }
        if (nread == 0) {
            throw CompactionFatalError(
                "Unexpected EOF while copying compacted journal from " + source_path);
        }
        size_t written = 0;
        while (written < static_cast<size_t>(nread)) {
            ssize_t result;
            do {
                result = ::pwrite(destination_fd,
                                  buffer.data() + written,
                                  static_cast<size_t>(nread) - written,
                                  static_cast<off_t>(destination_offset + copied + written));
            } while (result < 0 && errno == EINTR);
            if (result < 0) {
                throw io_error("pwrite", destination_path);
            }
            if (result == 0) {
                throw KVStoreError("pwrite made no progress while compacting " + destination_path);
            }
            written += static_cast<size_t>(result);
        }
        copied += static_cast<uint64_t>(nread);
    }
}

void pwrite_buffer(int fd,
                   uint64_t offset,
                   const void* data,
                   size_t size,
                   const std::string& path) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    size_t written = 0;
    while (written < size) {
        ssize_t result;
        do {
            result = ::pwrite(fd,
                              bytes + written,
                              size - written,
                              static_cast<off_t>(offset + written));
        } while (result < 0 && errno == EINTR);
        if (result < 0) {
            throw io_error("pwrite", path);
        }
        if (result == 0) {
            throw KVStoreError("pwrite made no progress for " + path);
        }
        written += static_cast<size_t>(result);
    }
}

void pread_exact_at(int fd,
                    uint64_t offset,
                    void* data,
                    size_t size,
                    const std::string& path) {
    auto* bytes = static_cast<uint8_t*>(data);
    size_t read_bytes = 0;
    while (read_bytes < size) {
        ssize_t result;
        do {
            result = ::pread(fd,
                             bytes + read_bytes,
                             size - read_bytes,
                             static_cast<off_t>(offset + read_bytes));
        } while (result < 0 && errno == EINTR);
        if (result < 0) {
            throw io_error("pread", path);
        }
        if (result == 0) {
            throw KVStoreError("Unexpected EOF while reading " + path);
        }
        read_bytes += static_cast<size_t>(result);
    }
}

class BufferedSequentialWriter {
public:
    BufferedSequentialWriter(int fd,
                             std::string path,
                             uint64_t start_offset,
                             uint32_t checksum_seed = 0,
                             bool checksum_enabled = true)
        : fd_(fd),
          path_(std::move(path)),
          flushed_offset_(start_offset),
          logical_offset_(start_offset),
          checksum_(checksum_seed),
          checksum_enabled_(checksum_enabled) {}

    ~BufferedSequentialWriter() = default;

    void Write(const void* data, size_t size) {
        if (size == 0) {
            return;
        }
        if (size > std::numeric_limits<uint64_t>::max() - logical_offset_) {
            throw KVStoreError("Buffered file offset overflow for " + path_);
        }
        if (checksum_enabled_) {
            checksum_ = crc32c_extend(checksum_, data, size);
        }
        const auto* cursor = static_cast<const uint8_t*>(data);
        size_t remaining = size;
        while (remaining != 0) {
            const size_t available = buffer_.size() - buffered_bytes_;
            const size_t chunk = std::min(available, remaining);
            std::memcpy(buffer_.data() + buffered_bytes_, cursor, chunk);
            buffered_bytes_ += chunk;
            logical_offset_ += chunk;
            cursor += chunk;
            remaining -= chunk;
            if (buffered_bytes_ == buffer_.size()) {
                Flush();
            }
        }
    }

    void Flush() {
        if (buffered_bytes_ == 0) {
            return;
        }
        pwrite_buffer(fd_, flushed_offset_, buffer_.data(), buffered_bytes_, path_);
        flushed_offset_ += buffered_bytes_;
        buffered_bytes_ = 0;
    }

    uint64_t Offset() const noexcept {
        return logical_offset_;
    }

    uint32_t Checksum() const noexcept {
        return checksum_;
    }

private:
    int fd_ = -1;
    std::string path_;
    std::array<uint8_t, 1024 * 1024> buffer_ {};
    size_t buffered_bytes_ = 0;
    uint64_t flushed_offset_ = 0;
    uint64_t logical_offset_ = 0;
    uint32_t checksum_ = 0;
    bool checksum_enabled_ = true;
};

ScopedFd open_unlinked_spool(const std::string& database_path,
                              const std::string& label,
                              uint64_t sequence) {
    const std::string path = database_path + "." + label + "." +
                             std::to_string(::getpid()) + "." + std::to_string(sequence);
    const int fd = open_or_throw(path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (::unlink(path.c_str()) != 0) {
        const int saved_errno = errno;
        close_if_open(fd);
        errno = saved_errno;
        throw io_error("unlink compaction spool", path);
    }
    return ScopedFd(fd);
}

ScopedFd open_unique_compaction_file(const std::string& database_path,
                                     uint64_t sequence,
                                     std::string& created_path) {
    constexpr size_t kMaximumAttempts = 64;
    std::random_device random_source;
    for (size_t attempt = 0; attempt < kMaximumAttempts; ++attempt) {
        const uint64_t nonce = (static_cast<uint64_t>(random_source()) << 32U) ^
                               static_cast<uint64_t>(random_source());
        const std::string candidate = database_path + ".compact." +
                                      std::to_string(::getpid()) + "." +
                                      std::to_string(sequence) + "." +
                                      std::to_string(nonce);
        int fd;
        do {
            fd = ::open(candidate.c_str(),
                        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC,
                        0600);
        } while (fd < 0 && errno == EINTR);
        if (fd >= 0) {
            created_path = candidate;
            return ScopedFd(fd);
        }
        if (errno != EEXIST) {
            throw io_error("create compaction container", candidate);
        }
    }
    throw KVStoreError("Could not allocate a unique compaction container beside " +
                       database_path);
}

void copy_spool(int spool_fd,
                uint64_t bytes,
                BufferedSequentialWriter& destination,
                const std::string& label) {
    std::array<uint8_t, 1024 * 1024> buffer {};
    uint64_t copied = 0;
    while (copied < bytes) {
        const size_t chunk = static_cast<size_t>(
            std::min<uint64_t>(buffer.size(), bytes - copied));
        pread_exact_at(spool_fd, copied, buffer.data(), chunk, label);
        destination.Write(buffer.data(), chunk);
        copied += chunk;
    }
}

#if defined(__linux__)
constexpr size_t kXattrMetadataRetryLimit = 16;

bool xattrs_not_supported(int error) {
    return error == ENOTSUP || error == EOPNOTSUPP;
}

std::vector<char> read_xattr_names(int fd,
                                   const std::string& path,
                                   bool& supported) {
    for (size_t attempt = 0; attempt < kXattrMetadataRetryLimit; ++attempt) {
        ssize_t required;
        do {
            required = ::flistxattr(fd, nullptr, 0);
        } while (required < 0 && errno == EINTR);
        if (required < 0 && xattrs_not_supported(errno)) {
            supported = false;
            return {};
        }
        if (required < 0) {
            throw io_error("list xattrs", path);
        }

        std::vector<char> names(static_cast<size_t>(required));
        if (required == 0) {
            return names;
        }

        ssize_t actual;
        do {
            actual = ::flistxattr(fd, names.data(), names.size());
        } while (actual < 0 && errno == EINTR);
        if (actual >= 0) {
            names.resize(static_cast<size_t>(actual));
            return names;
        }
        if (errno == ERANGE) {
            continue;
        }
        if (xattrs_not_supported(errno)) {
            supported = false;
            return {};
        }
        throw io_error("read xattr names", path);
    }
    throw KVStoreError("xattr name list changed too frequently while compacting " + path);
}

std::vector<uint8_t> read_xattr_value(int fd,
                                      const char* name,
                                      const std::string& path,
                                      bool& supported) {
    for (size_t attempt = 0; attempt < kXattrMetadataRetryLimit; ++attempt) {
        ssize_t required;
        do {
            required = ::fgetxattr(fd, name, nullptr, 0);
        } while (required < 0 && errno == EINTR);
        if (required < 0 && xattrs_not_supported(errno)) {
            supported = false;
            return {};
        }
        if (required < 0) {
            throw io_error(std::string("read xattr size ") + name, path);
        }

        std::vector<uint8_t> value(static_cast<size_t>(required));
        if (required == 0) {
            return value;
        }

        ssize_t actual;
        do {
            actual = ::fgetxattr(fd, name, value.data(), value.size());
        } while (actual < 0 && errno == EINTR);
        if (actual >= 0) {
            value.resize(static_cast<size_t>(actual));
            return value;
        }
        if (errno == ERANGE) {
            continue;
        }
        if (xattrs_not_supported(errno)) {
            supported = false;
            return {};
        }
        throw io_error(std::string("read xattr ") + name, path);
    }
    throw KVStoreError(
        std::string("xattr value changed too frequently while compacting ") + name +
        " in " + path);
}
#endif

void copy_supported_file_metadata(int source_fd,
                                  int destination_fd,
                                  const std::string& source_path,
                                  const std::string& destination_path) {
    struct stat metadata {};
    if (::fstat(source_fd, &metadata) != 0) {
        throw io_error("fstat metadata source", source_path);
    }
    int result;
    do {
        result = ::fchown(destination_fd, metadata.st_uid, metadata.st_gid);
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
        throw io_error("fchown compacted container", destination_path);
    }
    do {
        result = ::fchmod(destination_fd, metadata.st_mode & 07777);
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
        throw io_error("fchmod compacted container", destination_path);
    }

#if defined(__linux__)
    bool xattrs_supported = true;
    std::vector<char> names = read_xattr_names(
        source_fd, source_path, xattrs_supported);
    if (!xattrs_supported) {
        return;
    }
    size_t cursor = 0;
    while (cursor < names.size()) {
        const char* name = names.data() + cursor;
        const size_t remaining_names = names.size() - cursor;
        const void* terminator = std::memchr(name, '\0', remaining_names);
        if (terminator == nullptr) {
            throw KVStoreError("Malformed xattr name list for " + source_path);
        }
        const size_t name_length = static_cast<const char*>(terminator) - name;
        if (name_length == 0) {
            throw KVStoreError("Empty xattr name in list for " + source_path);
        }
        std::vector<uint8_t> value = read_xattr_value(
            source_fd, name, source_path, xattrs_supported);
        if (!xattrs_supported) {
            return;
        }
        int set_result;
        do {
            set_result = ::fsetxattr(
                destination_fd, name, value.data(), value.size(), 0);
        } while (set_result != 0 && errno == EINTR);
        if (set_result != 0) {
            if (xattrs_not_supported(errno)) {
                return;
            }
            throw io_error(std::string("copy xattr ") + name, destination_path);
        }
        cursor += name_length + 1;
    }
#else
    (void)source_path;
    (void)destination_path;
#endif
}

}  // namespace

KVStoreOptions sanitize_options(KVStoreOptions options) {
    switch (options.durability) {
        case DurabilityMode::kSync:
        case DurabilityMode::kPeriodic:
        case DurabilityMode::kNoSync:
            break;
        default:
            throw KVStoreError("KVStoreOptions.durability is invalid");
    }
    if (!is_power_of_two(options.shard_count)) {
        throw KVStoreError("KVStoreOptions.shard_count must be a non-zero power of two");
    }
    if (options.worker_threads == 0) {
        const unsigned int detected = std::thread::hardware_concurrency();
        options.worker_threads = std::min<size_t>(32, std::max<size_t>(1, detected == 0 ? 1 : detected));
    }
    options.worker_threads = std::min<size_t>(32, std::max<size_t>(1, options.worker_threads));
    if (options.request_queue_capacity == 0) {
        options.request_queue_capacity = 1;
    }
    if (options.max_batch_size == 0) {
        options.max_batch_size = 1;
    }
    if (options.adaptive_recent_window_batches == 0) {
        options.adaptive_recent_window_batches = 1;
    }
    options.adaptive_recent_window_batches =
        std::min<size_t>(4096, options.adaptive_recent_window_batches);
    if (options.adaptive_recent_write_sample_limit == 0) {
        options.adaptive_recent_write_sample_limit = 1;
    }
    if (options.adaptive_queue_depth_threshold == 0) {
        options.adaptive_queue_depth_threshold = 1;
    }
    if (options.adaptive_batch_size_multiplier == 0) {
        options.adaptive_batch_size_multiplier = 1;
    }
    if (options.adaptive_batch_wal_bytes_multiplier == 0) {
        options.adaptive_batch_wal_bytes_multiplier = 1;
    }
    if (options.adaptive_flush_queue_depth_threshold == 0) {
        options.adaptive_flush_queue_depth_threshold = 1;
    }
    if (options.adaptive_flush_delay_divisor == 0) {
        options.adaptive_flush_delay_divisor = 1;
    }
    if (options.adaptive_read_heavy_delay_divisor == 0) {
        options.adaptive_read_heavy_delay_divisor = 1;
    }
    if (options.adaptive_read_heavy_batch_size_divisor == 0) {
        options.adaptive_read_heavy_batch_size_divisor = 1;
    }
    if (options.adaptive_fsync_pressure_delay_multiplier == 0) {
        options.adaptive_fsync_pressure_delay_multiplier = 1;
    }
    if (options.adaptive_compaction_pressure_delay_multiplier == 0) {
        options.adaptive_compaction_pressure_delay_multiplier = 1;
    }
    if (options.adaptive_wal_growth_delay_multiplier == 0) {
        options.adaptive_wal_growth_delay_multiplier = 1;
    }
    if (options.adaptive_objective_short_delay_divisor == 0) {
        options.adaptive_objective_short_delay_divisor = 1;
    }
    if (options.adaptive_objective_long_delay_multiplier == 0) {
        options.adaptive_objective_long_delay_multiplier = 1;
    }
    if (options.periodic_sync_interval_ms == 0) {
        options.periodic_sync_interval_ms = 1;
    }
    if (options.auto_compact_invalid_wal_ratio_percent > 100) {
        options.auto_compact_invalid_wal_ratio_percent = 100;
    }
    return options;
}

class StorageEngine::Impl {
public:
    explicit Impl(std::string db_path, KVStoreOptions options)
        : db_path_(std::move(db_path)),
          options_(sanitize_options(options)),
          value_cache_(options_.value_cache_bytes) {
        const auto recovery_start = Clock::now();
        open_and_recover();
        recovery_time_us_.store(elapsed_us(recovery_start), std::memory_order_relaxed);
        start_threads();
    }

    ~Impl() {
        stop_periodic_thread();
        stop_auto_compaction_thread();
        try {
            auto request = std::make_shared<Request>();
            request->kind = RequestKind::kShutdown;
            submit_and_wait(request, true);
        } catch (...) {
        }

        {
            std::lock_guard<std::mutex> lock(raw_mutex_);
            raw_stop_ = true;
        }
        {
            std::lock_guard<std::mutex> lock(outstanding_mutex_);
            outstanding_stop_ = true;
        }
        raw_not_empty_.notify_all();
        raw_not_full_.notify_all();
        outstanding_cv_.notify_all();
        prepared_cv_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        coordinator_stop_.store(true, std::memory_order_release);
        prepared_cv_.notify_all();
        if (coordinator_.joinable()) {
            coordinator_.join();
        }
        current_file_.reset();
    }

    void Put(std::string key, Value value) {
        validate_api_key_value(key, value);
        Mutation mutation {MutationType::kPut, std::move(key), std::move(value)};
        submit_write({std::move(mutation)}, {});
    }

    void Delete(std::string key) {
        validate_api_key(key);
        Mutation mutation {MutationType::kDelete, std::move(key), Value {}};
        submit_write({std::move(mutation)}, {});
    }

    void WriteBatch(std::vector<Mutation> operations) {
        if (operations.empty()) {
            return;
        }
        for (const auto& operation : operations) {
            validate_api_key_value(operation.key, operation.value);
        }
        submit_write(std::move(operations), {});
    }

    std::optional<Value> Get(const std::string& key) {
        return GetVersioned(key).value;
    }

    VersionedRead GetVersioned(const std::string& key) {
        throw_if_fatal();
        validate_api_key(key);
        read_requests_.fetch_add(1, std::memory_order_relaxed);
        const size_t shard_id = ShardForKey(key);
        Shard& shard = *shards_[shard_id];
        std::shared_lock<std::shared_mutex> lock(shard.mutex);
        VersionedRead result;
        result.shard_id = shard_id;
        result.shard_version = shard.version.load(std::memory_order_relaxed);
        try {
            const auto found = shard.values.find(key);
            if (found != shard.values.end()) {
                result.value = read_entry(key, found->second);
            } else {
                value_cache_misses_.fetch_add(1, std::memory_order_relaxed);
            }
        } catch (const std::exception& error) {
            set_fatal(error.what());
            throw;
        }
        return result;
    }

    std::vector<std::pair<std::string, Value>> Scan(const std::string& start_key,
                                                    const std::string& end_key) {
        return Scan(start_key, end_key, std::numeric_limits<size_t>::max());
    }

    std::vector<std::pair<std::string, Value>> Scan(const std::string& start_key,
                                                    const std::string& end_key,
                                                    size_t limit) {
        throw_if_fatal();
        if (start_key > end_key) {
            throw KVStoreError("Scan start key must not be greater than end key");
        }
        read_requests_.fetch_add(1, std::memory_order_relaxed);
        const std::string encoded_start = encode_string_key(start_key);
        const std::string encoded_end = encode_string_key(end_key);
        validate_api_key(encoded_start);
        validate_api_key(encoded_end);
        if (limit == 0) {
            return {};
        }

        std::vector<std::shared_lock<std::shared_mutex>> locks;
        locks.reserve(shards_.size());
        for (auto& shard : shards_) {
            locks.emplace_back(shard->mutex);
        }

        struct Cursor {
            size_t shard_id;
            std::set<std::string>::const_iterator iterator;
            std::set<std::string>::const_iterator end;
        };
        struct CursorGreater {
            bool operator()(const Cursor& lhs, const Cursor& rhs) const {
                return *lhs.iterator > *rhs.iterator;
            }
        };
        std::priority_queue<Cursor, std::vector<Cursor>, CursorGreater> heap;
        for (size_t shard_id = 0; shard_id < shards_.size(); ++shard_id) {
            const auto& keys = shards_[shard_id]->ordered_string_keys;
            auto iterator = keys.lower_bound(encoded_start);
            if (iterator != keys.end() && *iterator <= encoded_end) {
                heap.push(Cursor {shard_id, iterator, keys.end()});
            }
        }

        std::vector<std::pair<std::string, Entry>> snapshot;
        snapshot.reserve(std::min(limit, static_cast<size_t>(1024)));
        try {
            while (!heap.empty() && snapshot.size() < limit) {
                Cursor cursor = heap.top();
                heap.pop();
                const std::string key = *cursor.iterator;
                const auto value = shards_[cursor.shard_id]->values.find(key);
                if (value == shards_[cursor.shard_id]->values.end()) {
                    throw KVStoreError("String scan index is inconsistent with the point index");
                }
                snapshot.emplace_back(key, value->second);
                ++cursor.iterator;
                if (cursor.iterator != cursor.end && *cursor.iterator <= encoded_end) {
                    heap.push(cursor);
                }
            }
        } catch (const std::exception& error) {
            set_fatal(error.what());
            throw;
        }
        locks.clear();

        std::vector<std::pair<std::string, Value>> result;
        result.reserve(snapshot.size());
        try {
            for (const auto& [key, entry] : snapshot) {
                result.emplace_back(decode_string_key(key), read_entry_without_cache(entry));
            }
        } catch (const std::exception& error) {
            set_fatal(error.what());
            throw;
        }
        return result;
    }

    void CommitTransaction(std::vector<Mutation> operations,
                           std::map<size_t, uint64_t> expected_versions) {
        for (const auto& operation : operations) {
            validate_api_key_value(operation.key, operation.value);
        }
        auto request = std::make_shared<Request>();
        request->kind = RequestKind::kWrite;
        request->operations = std::move(operations);
        request->expected_versions = std::move(expected_versions);
        request->explicit_transaction = true;
        submit_and_wait(request, false);
    }

    void NoteTransactionRollback() noexcept {
        transaction_rollbacks_.fetch_add(1, std::memory_order_relaxed);
    }

    void Flush() {
        auto request = std::make_shared<Request>();
        request->kind = RequestKind::kFlush;
        submit_and_wait(request, false);
    }

    void Compact() {
        compact_impl(false);
    }

    KVStoreMetrics GetMetrics() const {
        KVStoreMetrics result;
        result.read_requests = read_requests_.load(std::memory_order_relaxed);
        result.enqueued_write_requests = enqueued_write_requests_.load(std::memory_order_relaxed);
        result.committed_write_requests = committed_write_requests_.load(std::memory_order_relaxed);
        result.committed_write_batches = committed_write_batches_.load(std::memory_order_relaxed);
        result.compact_requests = compact_requests_.load(std::memory_order_relaxed);
        result.wal_fsync_calls = wal_fsync_calls_.load(std::memory_order_relaxed);
        result.wal_bytes_written = wal_bytes_written_.load(std::memory_order_relaxed);
        result.wal_bytes_since_compaction = wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        result.live_wal_bytes_since_compaction = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t wal_bytes = result.wal_bytes_since_compaction;
        const uint64_t live_bytes = result.live_wal_bytes_since_compaction;
        result.obsolete_wal_bytes_since_compaction = wal_bytes > live_bytes ? wal_bytes - live_bytes : 0;
        result.last_committed_batch_size = last_committed_batch_size_.load(std::memory_order_relaxed);
        result.max_committed_batch_size = max_committed_batch_size_.load(std::memory_order_relaxed);
        result.last_committed_batch_wal_bytes = last_committed_batch_wal_bytes_.load(std::memory_order_relaxed);
        result.max_committed_batch_wal_bytes = max_committed_batch_wal_bytes_.load(std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(raw_mutex_);
            result.pending_queue_depth = raw_queue_.size();
        }
        result.max_pending_queue_depth = max_pending_queue_depth_.load(std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(prepared_mutex_);
            result.prepared_queue_depth = prepared_.size();
        }
        {
            std::lock_guard<std::mutex> lock(outstanding_mutex_);
            result.inflight_request_count = outstanding_requests_;
        }
        result.max_inflight_request_count =
            max_inflight_request_count_.load(std::memory_order_relaxed);
        result.manual_compactions_completed = manual_compactions_completed_.load(std::memory_order_relaxed);
        result.auto_compactions_completed = auto_compactions_completed_.load(std::memory_order_relaxed);
        result.auto_compaction_failures = auto_compaction_failures_.load(std::memory_order_relaxed);
        result.adaptive_batches_completed = adaptive_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_flush_batches_completed = adaptive_flush_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_latency_target_batches_completed =
            adaptive_latency_target_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_fsync_pressure_batches_completed =
            adaptive_fsync_pressure_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_read_heavy_batches_completed =
            adaptive_read_heavy_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_compaction_pressure_batches_completed =
            adaptive_compaction_pressure_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_wal_growth_batches_completed =
            adaptive_wal_growth_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_objective_short_delay_batches_completed =
            adaptive_objective_short_delay_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_objective_long_delay_batches_completed =
            adaptive_objective_long_delay_batches_completed_.load(std::memory_order_relaxed);
        result.adaptive_objective_throughput_batches_completed =
            adaptive_objective_throughput_batches_completed_.load(std::memory_order_relaxed);
        result.writer_wait_events = writer_wait_events_.load(std::memory_order_relaxed);
        result.writer_wait_time_us = writer_wait_time_us_.load(std::memory_order_relaxed);
        result.last_effective_batch_delay_us = last_effective_batch_delay_us_.load(std::memory_order_relaxed);
        result.min_effective_batch_delay_us = min_effective_batch_delay_us_.load(std::memory_order_relaxed);
        result.max_effective_batch_delay_us = max_effective_batch_delay_us_.load(std::memory_order_relaxed);
        result.observed_fsync_pressure_per_1000_writes =
            observed_fsync_pressure_per_1000_writes_.load(std::memory_order_relaxed);
        result.last_objective_pressure_score = last_objective_pressure_score_.load(std::memory_order_relaxed);
        result.last_objective_cost_score = last_objective_cost_score_.load(std::memory_order_relaxed);
        result.last_objective_throughput_score = last_objective_throughput_score_.load(std::memory_order_relaxed);
        result.last_objective_balance_score = last_objective_balance_score_.load(std::memory_order_relaxed);
        result.last_objective_mode = last_objective_mode_.load(std::memory_order_relaxed);
        result.total_snapshot_bytes_written = total_snapshot_bytes_written_.load(std::memory_order_relaxed);
        result.total_wal_bytes_reclaimed_by_compaction =
            total_wal_bytes_reclaimed_by_compaction_.load(std::memory_order_relaxed);
        for (size_t index = 0; index < kWriteLatencyBucketCount; ++index) {
            result.write_latency_histogram[index] = write_latency_histogram_[index].load(std::memory_order_relaxed);
        }
        result.approx_write_latency_p50_us =
            approximate_latency_percentile_us(result.write_latency_histogram, 50, 100);
        result.approx_write_latency_p95_us =
            approximate_latency_percentile_us(result.write_latency_histogram, 95, 100);
        result.approx_write_latency_p99_us =
            approximate_latency_percentile_us(result.write_latency_histogram, 99, 100);
        const RecentWindowSnapshot recent = recent_window_snapshot(result.read_requests);
        result.recent_observed_write_latency_p95_us = recent.write_latency_p95_us;
        result.recent_read_requests = recent.read_requests;
        result.recent_write_requests = recent.write_requests;
        result.recent_read_ratio_per_1000_ops = recent.read_ratio_per_1000_ops;
        result.recent_fsync_pressure_per_1000_writes =
            recent.fsync_pressure_per_1000_writes;
        result.recent_peak_queue_depth = recent.peak_queue_depth;
        result.recent_avg_batch_size = recent.avg_batch_size;
        const uint64_t batch_target = options_.adaptive_objective_target_batch_size != 0
                                          ? options_.adaptive_objective_target_batch_size
                                          : options_.max_batch_size;
        result.recent_batch_fill_per_1000 =
            batch_target == 0 ? 0 : std::min<uint64_t>(1000, (result.recent_avg_batch_size * 1000) / batch_target);
        result.recent_avg_batch_wal_bytes = recent.avg_batch_wal_bytes;
        result.recent_window_batch_count = recent.batch_count;
        result.observed_obsolete_wal_ratio_percent =
            wal_bytes == 0 ? 0 : (result.obsolete_wal_bytes_since_compaction * 100) / wal_bytes;
        result.prepared_write_requests = prepared_write_requests_.load(std::memory_order_relaxed);
        result.worker_tasks_completed = worker_tasks_completed_.load(std::memory_order_relaxed);
        result.worker_busy_time_us = worker_busy_time_us_.load(std::memory_order_relaxed);
        result.active_workers = active_workers_.load(std::memory_order_relaxed);
        result.max_active_workers = max_active_workers_.load(std::memory_order_relaxed);
        const uint64_t worker_elapsed_us = std::max<uint64_t>(1, elapsed_us(worker_pool_start_));
        const long double worker_capacity_us =
            static_cast<long double>(worker_elapsed_us) * options_.worker_threads;
        result.worker_utilization_per_1000 = static_cast<uint64_t>(std::min<long double>(
            1000.0L,
            (static_cast<long double>(result.worker_busy_time_us) * 1000.0L) / worker_capacity_us));
        result.group_commit_calls = group_commit_calls_.load(std::memory_order_relaxed);
        result.group_commit_requests = group_commit_requests_.load(std::memory_order_relaxed);
        result.max_group_commit_requests = max_group_commit_requests_.load(std::memory_order_relaxed);
        result.fdatasync_time_us = fdatasync_time_us_.load(std::memory_order_relaxed);
        result.max_fdatasync_time_us = max_fdatasync_time_us_.load(std::memory_order_relaxed);
        result.transaction_commits = transaction_commits_.load(std::memory_order_relaxed);
        result.transaction_conflicts = transaction_conflicts_.load(std::memory_order_relaxed);
        result.transaction_rollbacks = transaction_rollbacks_.load(std::memory_order_relaxed);
        result.compaction_pause_time_us = compaction_pause_time_us_.load(std::memory_order_relaxed);
        result.max_compaction_pause_time_us = max_compaction_pause_time_us_.load(std::memory_order_relaxed);
        result.recovery_time_us = recovery_time_us_.load(std::memory_order_relaxed);
        result.value_cache_hits = value_cache_hits_.load(std::memory_order_relaxed);
        result.value_cache_misses = value_cache_misses_.load(std::memory_order_relaxed);
        result.configured_worker_threads = options_.worker_threads;
        result.configured_shard_count = options_.shard_count;
        return result;
    }

    size_t ShardForKey(const std::string& key) const {
        return static_cast<size_t>(stable_key_hash(key) & (shards_.size() - 1));
    }

private:
    struct Entry {
        std::shared_ptr<LockedFileGeneration> file_generation;
        uint64_t value_offset = 0;
        uint32_t value_size = 0;
        uint32_t value_checksum = 0;
        uint64_t lsn = 0;
        std::array<uint64_t, 2> wal_epoch_ids {};
        std::array<uint64_t, 2> wal_charges {};
        uint64_t relocation_epoch = 0;
        uint64_t relocation_offset = 0;
    };

    struct WalAccountingEpoch {
        uint64_t id = 0;
        uint64_t wal_bytes = 0;
        uint64_t live_bytes = 0;
        std::unordered_map<std::string, uint64_t> tombstones;
    };

    struct RecentBatchSample {
        uint64_t id = 0;
        uint64_t read_requests = 0;
        uint64_t write_requests = 0;
        uint64_t fsync_calls_at_start = 0;
        uint64_t batch_size = 0;
        uint64_t wal_bytes = 0;
        uint64_t peak_queue_depth = 0;
    };

    struct RecentLatencySample {
        uint64_t batch_id = 0;
        uint64_t latency_us = 0;
    };

    struct RecentWindowSnapshot {
        uint64_t read_requests = 0;
        uint64_t write_requests = 0;
        uint64_t fsync_calls = 0;
        uint64_t fsync_pressure_per_1000_writes = 0;
        uint64_t read_ratio_per_1000_ops = 0;
        uint64_t write_latency_p95_us = 0;
        uint64_t peak_queue_depth = 0;
        uint64_t avg_batch_size = 0;
        uint64_t avg_batch_wal_bytes = 0;
        uint64_t batch_count = 0;
    };

    struct CheckpointReference {
        size_t shard_id = 0;
        std::string key;
        std::shared_ptr<LockedFileGeneration> file_generation;
        uint64_t source_value_offset = 0;
        uint64_t checkpoint_value_offset = 0;
        uint64_t lsn = 0;
        uint32_t value_size = 0;
        uint32_t value_checksum = 0;
    };

    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, Entry> values;
        std::set<std::string> ordered_string_keys;
        std::atomic<uint64_t> version {0};
    };

    enum class RequestKind {
        kWrite,
        kFlush,
        kShutdown,
    };

    struct Request {
        RequestKind kind = RequestKind::kWrite;
        uint64_t sequence = 0;
        std::vector<Mutation> operations;
        std::map<size_t, uint64_t> expected_versions;
        bool explicit_transaction = false;
        std::vector<uint8_t> payload;
        uint32_t payload_checksum = 0;
        uint64_t assigned_lsn = 0;
        uint64_t frame_offset = 0;
        std::shared_ptr<LockedFileGeneration> file_generation;
        FrameHeader frame_header {};
        FrameFooter frame_footer {};
        Clock::time_point enqueue_time = Clock::now();
        std::mutex completion_mutex;
        std::condition_variable completion_cv;
        bool completed = false;
        bool conflict = false;
        bool owns_outstanding_token = false;
        uint64_t completion_latency_us = 0;
        std::string error;
    };

    using RequestPtr = std::shared_ptr<Request>;

    std::string db_path_;
    KVStoreOptions options_;
    ValueCache value_cache_;
    std::shared_ptr<LockedFileGeneration> current_file_;
    uint64_t next_file_generation_id_ = 1;
    Superblock superblock_ {};
    uint64_t append_offset_ = 0;
    uint64_t last_lsn_ = 0;
    std::vector<std::unique_ptr<Shard>> shards_;

    mutable std::mutex raw_mutex_;
    std::condition_variable raw_not_empty_;
    std::condition_variable raw_not_full_;
    std::deque<RequestPtr> raw_queue_;
    bool raw_stop_ = false;

    mutable std::mutex prepared_mutex_;
    std::condition_variable prepared_cv_;
    std::map<uint64_t, RequestPtr> prepared_;
    uint64_t coordinator_sequence_ = 1;
    std::atomic<bool> coordinator_stop_ {false};

    std::vector<std::thread> workers_;
    std::thread coordinator_;
    uint64_t next_sequence_ = 1;  // Guarded by raw_mutex_.
    std::atomic<bool> stopping_ {false};
    std::atomic<bool> dirty_ {false};

    mutable std::mutex outstanding_mutex_;
    std::condition_variable outstanding_cv_;
    size_t outstanding_requests_ = 0;
    bool outstanding_stop_ = false;
    std::atomic<uint64_t> max_inflight_request_count_ {0};

    std::mutex commit_mutex_;
    std::mutex compaction_mutex_;
    std::atomic<uint64_t> compact_temp_sequence_ {0};
    std::atomic<uint64_t> relocation_epoch_sequence_ {1};

    std::mutex periodic_mutex_;
    std::condition_variable periodic_cv_;
    bool periodic_stop_ = false;
    std::thread periodic_thread_;

    std::mutex auto_compaction_mutex_;
    std::condition_variable auto_compaction_cv_;
    bool auto_compaction_stop_ = false;
    bool auto_compaction_requested_ = false;
    std::thread auto_compaction_thread_;
    Clock::time_point worker_pool_start_ = Clock::now();
    Clock::time_point auto_compaction_retry_after_ = Clock::time_point::min();

    std::atomic<bool> fatal_ {false};
    mutable std::mutex fatal_mutex_;
    std::string fatal_message_;

    std::atomic<uint64_t> read_requests_ {0};
    std::atomic<uint64_t> enqueued_write_requests_ {0};
    std::atomic<uint64_t> committed_write_requests_ {0};
    std::atomic<uint64_t> committed_write_batches_ {0};
    std::atomic<uint64_t> compact_requests_ {0};
    std::atomic<uint64_t> wal_fsync_calls_ {0};
    std::atomic<uint64_t> wal_bytes_written_ {0};
    std::atomic<uint64_t> wal_bytes_since_compaction_ {0};
    std::atomic<uint64_t> live_wal_bytes_since_compaction_ {0};
    std::array<WalAccountingEpoch, 2> wal_accounting_ {};
    size_t active_wal_accounting_slot_ = 0;
    int pending_wal_accounting_slot_ = -1;
    uint64_t next_wal_accounting_epoch_id_ = 1;
    std::atomic<uint64_t> last_committed_batch_size_ {0};
    std::atomic<uint64_t> max_committed_batch_size_ {0};
    std::atomic<uint64_t> last_committed_batch_wal_bytes_ {0};
    std::atomic<uint64_t> max_committed_batch_wal_bytes_ {0};
    std::atomic<uint64_t> max_pending_queue_depth_ {0};
    std::atomic<uint64_t> manual_compactions_completed_ {0};
    std::atomic<uint64_t> auto_compactions_completed_ {0};
    std::atomic<uint64_t> auto_compaction_failures_ {0};
    std::atomic<uint64_t> writer_wait_events_ {0};
    std::atomic<uint64_t> writer_wait_time_us_ {0};
    std::atomic<uint64_t> adaptive_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_flush_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_latency_target_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_fsync_pressure_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_read_heavy_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_compaction_pressure_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_wal_growth_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_objective_short_delay_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_objective_long_delay_batches_completed_ {0};
    std::atomic<uint64_t> adaptive_objective_throughput_batches_completed_ {0};
    std::atomic<uint64_t> last_effective_batch_delay_us_ {0};
    std::atomic<uint64_t> min_effective_batch_delay_us_ {0};
    std::atomic<uint64_t> max_effective_batch_delay_us_ {0};
    std::atomic<uint64_t> observed_fsync_pressure_per_1000_writes_ {0};
    std::atomic<uint64_t> last_objective_pressure_score_ {0};
    std::atomic<uint64_t> last_objective_cost_score_ {0};
    std::atomic<uint64_t> last_objective_throughput_score_ {0};
    std::atomic<int64_t> last_objective_balance_score_ {0};
    std::atomic<int64_t> last_objective_mode_ {0};
    mutable std::mutex recent_mutex_;
    std::deque<RecentBatchSample> recent_batches_;
    std::deque<RecentLatencySample> recent_write_latencies_;
    uint64_t recent_next_batch_id_ = 1;
    uint64_t recent_last_recorded_read_requests_ = 0;
    std::atomic<uint64_t> total_snapshot_bytes_written_ {0};
    std::atomic<uint64_t> total_wal_bytes_reclaimed_by_compaction_ {0};
    std::array<std::atomic<uint64_t>, kWriteLatencyBucketCount> write_latency_histogram_ {};
    std::atomic<uint64_t> prepared_write_requests_ {0};
    std::atomic<uint64_t> worker_tasks_completed_ {0};
    std::atomic<uint64_t> worker_busy_time_us_ {0};
    std::atomic<uint64_t> active_workers_ {0};
    std::atomic<uint64_t> max_active_workers_ {0};
    std::atomic<uint64_t> group_commit_calls_ {0};
    std::atomic<uint64_t> group_commit_requests_ {0};
    std::atomic<uint64_t> max_group_commit_requests_ {0};
    std::atomic<uint64_t> fdatasync_time_us_ {0};
    std::atomic<uint64_t> max_fdatasync_time_us_ {0};
    std::atomic<uint64_t> transaction_commits_ {0};
    std::atomic<uint64_t> transaction_conflicts_ {0};
    std::atomic<uint64_t> transaction_rollbacks_ {0};
    std::atomic<uint64_t> compaction_pause_time_us_ {0};
    std::atomic<uint64_t> max_compaction_pause_time_us_ {0};
    std::atomic<uint64_t> recovery_time_us_ {0};
    std::atomic<uint64_t> value_cache_hits_ {0};
    std::atomic<uint64_t> value_cache_misses_ {0};

    void validate_api_key(const std::string& key) const {
        if (key.empty() || key.size() > kMaxKeyBytes) {
            throw KVStoreError("Encoded key is empty or exceeds the storage format limit");
        }
    }

    void validate_api_key_value(const std::string& key, const Value& value) const {
        validate_api_key(key);
        if (value.bytes.size() > kMaxValueBytes) {
            throw KVStoreError("Value exceeds the storage format limit");
        }
    }

    Value read_backing_value(const Entry& entry) const {
        const std::shared_ptr<LockedFileGeneration> file = entry.file_generation;
        if (!file) {
            throw KVStoreError("Value entry has no backing file generation");
        }
        Value value(std::vector<uint8_t>(entry.value_size));
        size_t total = 0;
        while (total < value.bytes.size()) {
            ssize_t nread;
            do {
                nread = ::pread(file->fd(),
                                value.bytes.data() + total,
                                value.bytes.size() - total,
                                static_cast<off_t>(entry.value_offset + total));
            } while (nread < 0 && errno == EINTR);
            if (nread < 0) {
                throw io_error("pread value", file->path());
            }
            if (nread == 0) {
                throw KVStoreError("Value backing is truncated in " + file->path());
            }
            total += static_cast<size_t>(nread);
        }
        if (crc32c(value.bytes.data(), value.bytes.size()) != entry.value_checksum) {
            throw KVStoreError("Value checksum mismatch while reading " + file->path());
        }
        return value;
    }

    Value read_entry_without_cache(const Entry& entry) {
        return read_backing_value(entry);
    }

    Value read_entry(const std::string& key, const Entry& entry) {
        const auto cached = value_cache_.Lookup(key, entry.lsn);
        if (cached.has_value()) {
            value_cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return *cached;
        }
        value_cache_misses_.fetch_add(1, std::memory_order_relaxed);
        Value value = read_backing_value(entry);
        value_cache_.Insert(key, entry.lsn, value);
        return value;
    }

    std::array<size_t, 2> writable_wal_accounting_slots(size_t& count) const {
        std::array<size_t, 2> slots {active_wal_accounting_slot_, 0};
        count = 1;
        if (pending_wal_accounting_slot_ >= 0 &&
            static_cast<size_t>(pending_wal_accounting_slot_) != active_wal_accounting_slot_) {
            slots[count++] = static_cast<size_t>(pending_wal_accounting_slot_);
        }
        return slots;
    }

    void initialize_wal_accounting() {
        active_wal_accounting_slot_ = 0;
        pending_wal_accounting_slot_ = -1;
        wal_accounting_[0] = WalAccountingEpoch {};
        wal_accounting_[1] = WalAccountingEpoch {};
        wal_accounting_[0].id = next_wal_accounting_epoch_id_++;
    }

    void begin_pending_wal_accounting() {
        const size_t slot = 1U - active_wal_accounting_slot_;
        wal_accounting_[slot] = WalAccountingEpoch {};
        if (next_wal_accounting_epoch_id_ == 0) {
            throw KVStoreError("WAL accounting epoch space is exhausted");
        }
        wal_accounting_[slot].id = next_wal_accounting_epoch_id_++;
        pending_wal_accounting_slot_ = static_cast<int>(slot);
    }

    void abandon_pending_wal_accounting() noexcept {
        if (pending_wal_accounting_slot_ < 0) {
            return;
        }
        wal_accounting_[static_cast<size_t>(pending_wal_accounting_slot_)] = WalAccountingEpoch {};
        pending_wal_accounting_slot_ = -1;
    }

    void publish_active_wal_accounting() {
        const WalAccountingEpoch& epoch = wal_accounting_[active_wal_accounting_slot_];
        if (epoch.live_bytes > epoch.wal_bytes) {
            throw KVStoreError("Internal WAL accounting invariant violated");
        }
        wal_bytes_since_compaction_.store(epoch.wal_bytes, std::memory_order_relaxed);
        live_wal_bytes_since_compaction_.store(epoch.live_bytes, std::memory_order_relaxed);
    }

    void remove_live_wal_charge(Shard& shard,
                                const std::string& key,
                                size_t slot) {
        WalAccountingEpoch& epoch = wal_accounting_[slot];
        const auto value = shard.values.find(key);
        if (value != shard.values.end() &&
            value->second.wal_epoch_ids[slot] == epoch.id) {
            const uint64_t charge = value->second.wal_charges[slot];
            if (charge > epoch.live_bytes) {
                throw KVStoreError("Internal live WAL charge underflow");
            }
            epoch.live_bytes -= charge;
        }
        const auto tombstone = epoch.tombstones.find(key);
        if (tombstone != epoch.tombstones.end()) {
            if (tombstone->second > epoch.live_bytes) {
                throw KVStoreError("Internal tombstone WAL charge underflow");
            }
            epoch.live_bytes -= tombstone->second;
            epoch.tombstones.erase(tombstone);
        }
    }

    void account_mutation_before_apply(Shard& shard, const Mutation& operation) {
        size_t slot_count = 0;
        const auto slots = writable_wal_accounting_slots(slot_count);
        for (size_t index = 0; index < slot_count; ++index) {
            WalAccountingEpoch& epoch = wal_accounting_[slots[index]];
            if (operation.wal_charge > std::numeric_limits<uint64_t>::max() - epoch.wal_bytes) {
                throw KVStoreError("WAL accounting byte total overflow");
            }
            epoch.wal_bytes += operation.wal_charge;
            remove_live_wal_charge(shard, operation.key, slots[index]);
        }
    }

    void account_put_after_apply(Entry& entry, const Mutation& operation) {
        size_t slot_count = 0;
        const auto slots = writable_wal_accounting_slots(slot_count);
        for (size_t index = 0; index < slot_count; ++index) {
            const size_t slot = slots[index];
            WalAccountingEpoch& epoch = wal_accounting_[slot];
            if (operation.wal_charge > std::numeric_limits<uint64_t>::max() - epoch.live_bytes) {
                throw KVStoreError("Live WAL accounting byte total overflow");
            }
            entry.wal_epoch_ids[slot] = epoch.id;
            entry.wal_charges[slot] = operation.wal_charge;
            epoch.live_bytes += operation.wal_charge;
        }
    }

    void account_delete_after_apply(const Mutation& operation) {
        size_t slot_count = 0;
        const auto slots = writable_wal_accounting_slots(slot_count);
        for (size_t index = 0; index < slot_count; ++index) {
            WalAccountingEpoch& epoch = wal_accounting_[slots[index]];
            if (operation.wal_charge > std::numeric_limits<uint64_t>::max() - epoch.live_bytes) {
                throw KVStoreError("Live WAL accounting byte total overflow");
            }
            epoch.tombstones[operation.key] = operation.wal_charge;
            epoch.live_bytes += operation.wal_charge;
        }
    }

    void apply_operation_locked(Shard& shard,
                                const Mutation& operation,
                                uint64_t lsn,
                                const std::shared_ptr<LockedFileGeneration>& file_generation,
                                uint64_t value_offset,
                                bool populate_cache) {
        account_mutation_before_apply(shard, operation);
        if (operation.type == MutationType::kPut) {
            Entry& entry = shard.values[operation.key];
            entry = Entry {};
            entry.file_generation = file_generation;
            entry.value_offset = value_offset;
            entry.value_size = static_cast<uint32_t>(operation.value.bytes.size());
            entry.value_checksum = operation.value_checksum;
            entry.lsn = lsn;
            account_put_after_apply(entry, operation);
            if (populate_cache) {
                value_cache_.Insert(operation.key, lsn, operation.value);
            }
            if (is_string_key(operation.key)) {
                shard.ordered_string_keys.insert(operation.key);
            }
        } else {
            const auto found = shard.values.find(operation.key);
            if (found != shard.values.end()) {
                shard.values.erase(found);
            }
            account_delete_after_apply(operation);
            if (is_string_key(operation.key)) {
                shard.ordered_string_keys.erase(operation.key);
            }
        }
    }

    void open_and_recover() {
        bool created = false;
        int opened_fd = open_database_file(db_path_, created);
        try {
            lock_file_exclusively(opened_fd, db_path_);
            current_file_ = std::make_shared<LockedFileGeneration>(
                opened_fd, db_path_, next_file_generation_id_++);
            opened_fd = -1;
            struct stat st {};
            if (::fstat(current_file_->fd(), &st) != 0) {
                throw io_error("fstat", db_path_);
            }
            if (created) {
                initialize_file(current_file_->fd(), db_path_);
                maybe_trigger_failpoint("before_create_directory_sync");
                fsync_directory(db_path_);
                maybe_trigger_failpoint("after_create_directory_sync");
            } else if (st.st_size == 0) {
                throw KVStoreError(
                    "Existing database file is empty and was not modified: " + db_path_);
            }

            shards_.reserve(options_.shard_count);
            for (size_t index = 0; index < options_.shard_count; ++index) {
                shards_.push_back(std::make_unique<Shard>());
            }
            initialize_wal_accounting();
            const RecoveryResult recovery = recover_file(
                current_file_->fd(),
                db_path_,
                [this](const Mutation& operation, uint64_t lsn) { apply_recovered(operation, lsn); },
                true);
            superblock_ = recovery.superblock;
            append_offset_ = recovery.append_offset;
            last_lsn_ = recovery.last_lsn;
            if (wal_accounting_[active_wal_accounting_slot_].wal_bytes !=
                append_offset_ - superblock_.journal_offset) {
                throw KVStoreError("Recovered WAL accounting does not match physical journal bytes");
            }
            publish_active_wal_accounting();
            if (recovery.truncated_tail) {
                fdatasync_or_throw(current_file_->fd(), db_path_);
            }
        } catch (...) {
            close_if_open(opened_fd);
            current_file_.reset();
            throw;
        }
    }

    void apply_recovered(const Mutation& operation, uint64_t lsn) {
        Shard& shard = *shards_[ShardForKey(operation.key)];
        apply_operation_locked(
            shard, operation, lsn, current_file_, operation.value_offset, false);
    }

    void start_threads() {
        worker_pool_start_ = Clock::now();
        try {
            workers_.reserve(options_.worker_threads);
            for (size_t index = 0; index < options_.worker_threads; ++index) {
                workers_.emplace_back([this] { worker_loop(); });
                if (index == 0) {
                    maybe_trigger_failpoint("after_first_worker_start");
                }
            }
            coordinator_ = std::thread([this] { coordinator_loop(); });
            if (options_.auto_compact_wal_bytes_threshold != 0 ||
                options_.auto_compact_invalid_wal_ratio_percent != 0) {
                auto_compaction_thread_ = std::thread([this] { auto_compaction_loop(); });
            }
            if (options_.durability == DurabilityMode::kPeriodic) {
                periodic_thread_ = std::thread([this] { periodic_loop(); });
            }
        } catch (...) {
            stop_threads_after_start_failure();
            throw;
        }
    }

    void stop_threads_after_start_failure() noexcept {
        stop_periodic_thread();
        stop_auto_compaction_thread();
        {
            std::lock_guard<std::mutex> lock(raw_mutex_);
            raw_stop_ = true;
        }
        {
            std::lock_guard<std::mutex> lock(outstanding_mutex_);
            outstanding_stop_ = true;
        }
        coordinator_stop_.store(true, std::memory_order_release);
        raw_not_empty_.notify_all();
        raw_not_full_.notify_all();
        prepared_cv_.notify_all();
        outstanding_cv_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        if (coordinator_.joinable()) {
            coordinator_.join();
        }
    }

    void stop_periodic_thread() {
        {
            std::lock_guard<std::mutex> lock(periodic_mutex_);
            periodic_stop_ = true;
        }
        periodic_cv_.notify_all();
        if (periodic_thread_.joinable()) {
            periodic_thread_.join();
        }
    }

    void stop_auto_compaction_thread() {
        {
            std::lock_guard<std::mutex> lock(auto_compaction_mutex_);
            auto_compaction_stop_ = true;
        }
        auto_compaction_cv_.notify_all();
        if (auto_compaction_thread_.joinable()) {
            auto_compaction_thread_.join();
        }
    }

    void submit_write(std::vector<Mutation> operations,
                      std::map<size_t, uint64_t> expected_versions) {
        auto request = std::make_shared<Request>();
        request->kind = RequestKind::kWrite;
        request->operations = std::move(operations);
        request->expected_versions = std::move(expected_versions);
        submit_and_wait(request, false);
    }

    void acquire_outstanding_token(const RequestPtr& request) {
        std::unique_lock<std::mutex> lock(outstanding_mutex_);
        outstanding_cv_.wait(lock, [this] {
            return outstanding_stop_ || outstanding_requests_ < options_.request_queue_capacity;
        });
        if (outstanding_stop_) {
            throw KVStoreError("KVStore worker queue is stopped");
        }
        ++outstanding_requests_;
        request->owns_outstanding_token = true;
        atomic_max(max_inflight_request_count_, outstanding_requests_);
    }

    void release_outstanding_token(const RequestPtr& request) noexcept {
        bool released = false;
        {
            std::lock_guard<std::mutex> lock(outstanding_mutex_);
            if (request->owns_outstanding_token) {
                request->owns_outstanding_token = false;
                if (outstanding_requests_ != 0) {
                    --outstanding_requests_;
                }
                released = true;
            }
        }
        if (released) {
            outstanding_cv_.notify_one();
        }
    }

    void submit_and_wait(const RequestPtr& request, bool allow_stopping) {
        if (!allow_stopping) {
            throw_if_fatal();
            if (stopping_.load(std::memory_order_acquire)) {
                throw KVStoreError("KVStore is shutting down");
            }
        }
        acquire_outstanding_token(request);
        request->enqueue_time = Clock::now();
        try {
            {
                std::unique_lock<std::mutex> lock(raw_mutex_);
                raw_not_full_.wait(lock, [this] {
                    return raw_stop_ || raw_queue_.size() < options_.request_queue_capacity;
                });
                if (raw_stop_) {
                    throw KVStoreError("KVStore worker queue is stopped");
                }
                raw_queue_.push_back(request);
                if (next_sequence_ == std::numeric_limits<uint64_t>::max()) {
                    raw_queue_.pop_back();
                    throw KVStoreError("KVStore request sequence space is exhausted");
                }
                request->sequence = next_sequence_++;
                atomic_max(max_pending_queue_depth_, raw_queue_.size());
            }
        } catch (...) {
            release_outstanding_token(request);
            throw;
        }
        if (request->kind == RequestKind::kWrite) {
            enqueued_write_requests_.fetch_add(1, std::memory_order_relaxed);
        }
        raw_not_empty_.notify_one();

        std::unique_lock<std::mutex> completion_lock(request->completion_mutex);
        request->completion_cv.wait(completion_lock, [&request] { return request->completed; });
        if (!request->error.empty()) {
            if (request->conflict) {
                throw KVStoreConflictError(request->error);
            }
            throw KVStoreError(request->error);
        }
    }

    void complete(const RequestPtr& request, std::string error = {}, bool conflict = false) {
        uint64_t latency_us = 0;
        if (request->kind == RequestKind::kWrite) {
            latency_us = request->completion_latency_us == 0
                             ? std::max<uint64_t>(1, elapsed_us(request->enqueue_time))
                             : request->completion_latency_us;
        }
        bool completed_now = false;
        {
            std::lock_guard<std::mutex> lock(request->completion_mutex);
            if (request->completed) {
                return;
            }
            request->error = std::move(error);
            request->conflict = conflict;
            request->completion_latency_us = latency_us;
            request->completed = true;
            completed_now = true;
        }
        if (request->kind == RequestKind::kWrite) {
            write_latency_histogram_[latency_bucket(latency_us)].fetch_add(
                1,
                std::memory_order_relaxed);
        }
        if (completed_now) {
            release_outstanding_token(request);
            request->completion_cv.notify_all();
        }
    }

    void worker_loop() {
        while (true) {
            RequestPtr request;
            const auto wait_start = Clock::now();
            {
                std::unique_lock<std::mutex> lock(raw_mutex_);
                raw_not_empty_.wait(lock, [this] { return raw_stop_ || !raw_queue_.empty(); });
                writer_wait_events_.fetch_add(1, std::memory_order_relaxed);
                writer_wait_time_us_.fetch_add(elapsed_us(wait_start), std::memory_order_relaxed);
                if (raw_stop_ && raw_queue_.empty()) {
                    return;
                }
                request = raw_queue_.front();
                raw_queue_.pop_front();
            }
            raw_not_full_.notify_one();
            const auto busy_start = Clock::now();
            const uint64_t active = active_workers_.fetch_add(1, std::memory_order_relaxed) + 1;
            atomic_max(max_active_workers_, active);
            try {
                if (request->kind == RequestKind::kWrite && !request->operations.empty()) {
                    maybe_trigger_failpoint("before_request_prepare");
                    if (request->operations.size() > std::numeric_limits<uint32_t>::max()) {
                        throw KVStoreError("Transaction operation count exceeds the storage format limit");
                    }
                    request->payload = serialize_payload(request->operations);
                    request->payload_checksum = crc32c(
                        request->payload.data(), request->payload.size());
                    const uint32_t operation_count =
                        static_cast<uint32_t>(request->operations.size());
                    for (uint32_t operation_index = 0;
                         operation_index < operation_count;
                         ++operation_index) {
                        Mutation& operation = request->operations[operation_index];
                        operation.value_checksum = crc32c(
                            operation.value.bytes.data(), operation.value.bytes.size());
                        operation.wal_charge = mutation_physical_charge(
                            sizeof(MutationHeader) + operation.key.size() +
                                operation.value.bytes.size(),
                            operation_index,
                            operation_count);
                    }
                    prepared_write_requests_.fetch_add(1, std::memory_order_relaxed);
                }
            } catch (const std::exception& error) {
                request->error = error.what();
            }
            {
                std::lock_guard<std::mutex> lock(prepared_mutex_);
                prepared_.emplace(request->sequence, request);
            }
            prepared_cv_.notify_one();
            worker_busy_time_us_.fetch_add(elapsed_us(busy_start), std::memory_order_relaxed);
            worker_tasks_completed_.fetch_add(1, std::memory_order_relaxed);
            active_workers_.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    RequestPtr wait_for_sequence(uint64_t sequence) {
        std::unique_lock<std::mutex> lock(prepared_mutex_);
        prepared_cv_.wait(lock, [this, sequence] {
            return coordinator_stop_.load(std::memory_order_acquire) ||
                   prepared_.find(sequence) != prepared_.end();
        });
        const auto found = prepared_.find(sequence);
        if (found == prepared_.end()) {
            return {};
        }
        RequestPtr request = found->second;
        prepared_.erase(found);
        return request;
    }

    RequestPtr try_take_sequence_until(uint64_t sequence, const Clock::time_point& deadline) {
        std::unique_lock<std::mutex> lock(prepared_mutex_);
        const auto ready = [this, sequence] {
            return coordinator_stop_.load(std::memory_order_acquire) ||
                   prepared_.find(sequence) != prepared_.end();
        };
#if defined(KVSTORE_TSAN_INSTRUMENTED)
        // GCC 10's libtsan does not intercept pthread_cond_clockwait, which
        // libstdc++ uses for steady-clock waits. Use its intercepted realtime
        // path only in instrumented builds, then enforce the steady deadline.
        while (!ready()) {
            const auto now = Clock::now();
            if (now >= deadline) {
                break;
            }
            const auto remaining = deadline - now;
            auto wall_delay = std::chrono::duration_cast<std::chrono::system_clock::duration>(remaining);
            if (wall_delay < remaining) {
                ++wall_delay;
            }
            prepared_cv_.wait_until(
                lock,
                std::chrono::system_clock::now() + wall_delay);
        }
#else
        prepared_cv_.wait_until(lock, deadline, ready);
#endif
        if (Clock::now() >= deadline) {
            return {};
        }
        const auto found = prepared_.find(sequence);
        if (found == prepared_.end()) {
            return {};
        }
        RequestPtr request = found->second;
        if (request->kind != RequestKind::kWrite) {
            return {};
        }
        prepared_.erase(found);
        return request;
    }

    RecentWindowSnapshot recent_window_snapshot(uint64_t current_read_requests) const {
        RecentWindowSnapshot snapshot;
        std::vector<uint64_t> latencies;
        {
            std::lock_guard<std::mutex> lock(recent_mutex_);
            snapshot.batch_count = recent_batches_.size();
            uint64_t total_batch_size = 0;
            uint64_t total_wal_bytes = 0;
            for (const auto& batch : recent_batches_) {
                snapshot.read_requests += batch.read_requests;
                snapshot.write_requests += batch.write_requests;
                total_batch_size += batch.batch_size;
                total_wal_bytes += batch.wal_bytes;
                snapshot.peak_queue_depth =
                    std::max(snapshot.peak_queue_depth, batch.peak_queue_depth);
            }
            if (current_read_requests >= recent_last_recorded_read_requests_) {
                snapshot.read_requests +=
                    current_read_requests - recent_last_recorded_read_requests_;
            }
            latencies.reserve(recent_write_latencies_.size());
            for (const auto& sample : recent_write_latencies_) {
                latencies.push_back(sample.latency_us);
            }
            if (snapshot.batch_count != 0) {
                snapshot.avg_batch_size = total_batch_size / snapshot.batch_count;
                snapshot.avg_batch_wal_bytes = total_wal_bytes / snapshot.batch_count;
                const uint64_t current_fsync_calls =
                    wal_fsync_calls_.load(std::memory_order_relaxed);
                const uint64_t window_start_fsync_calls =
                    recent_batches_.front().fsync_calls_at_start;
                snapshot.fsync_calls =
                    current_fsync_calls >= window_start_fsync_calls
                        ? current_fsync_calls - window_start_fsync_calls
                        : current_fsync_calls;
            }
        }
        const uint64_t operations = snapshot.read_requests + snapshot.write_requests;
        snapshot.read_ratio_per_1000_ops =
            operations == 0 ? 0 : (snapshot.read_requests * 1000) / operations;
        snapshot.fsync_pressure_per_1000_writes =
            snapshot.write_requests == 0
                ? 0
                : (snapshot.fsync_calls * 1000 + snapshot.write_requests - 1) /
                      snapshot.write_requests;
        if (!latencies.empty()) {
            std::sort(latencies.begin(), latencies.end());
            const size_t rank = (latencies.size() * 95 + 99) / 100;
            snapshot.write_latency_p95_us = latencies[rank - 1];
        }
        return snapshot;
    }

    void record_recent_batch(const std::vector<RequestPtr>& accepted,
                             uint64_t wal_bytes,
                             uint64_t peak_queue_depth,
                             uint64_t fsync_calls_at_start) {
        const uint64_t current_reads = read_requests_.load(std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(recent_mutex_);
        const uint64_t batch_id = recent_next_batch_id_++;
        const uint64_t read_delta = current_reads >= recent_last_recorded_read_requests_
                                        ? current_reads - recent_last_recorded_read_requests_
                                        : current_reads;
        recent_last_recorded_read_requests_ = current_reads;
        recent_batches_.push_back(RecentBatchSample {
            batch_id,
            read_delta,
            accepted.size(),
            fsync_calls_at_start,
            accepted.size(),
            wal_bytes,
            peak_queue_depth,
        });
        for (const auto& request : accepted) {
            recent_write_latencies_.push_back(
                RecentLatencySample {batch_id, request->completion_latency_us});
            while (recent_write_latencies_.size() >
                   options_.adaptive_recent_write_sample_limit) {
                recent_write_latencies_.pop_front();
            }
        }
        while (recent_batches_.size() > options_.adaptive_recent_window_batches) {
            const uint64_t evicted_id = recent_batches_.front().id;
            recent_batches_.pop_front();
            while (!recent_write_latencies_.empty() &&
                   recent_write_latencies_.front().batch_id <= evicted_id) {
                recent_write_latencies_.pop_front();
            }
        }
    }

    BatchPolicy current_batch_policy(uint64_t& observed_queue_depth) {
        uint64_t queue_depth = 1;
        {
            std::lock_guard<std::mutex> lock(raw_mutex_);
            queue_depth += raw_queue_.size();
        }
        {
            std::lock_guard<std::mutex> lock(prepared_mutex_);
            queue_depth += prepared_.size();
        }
        observed_queue_depth = queue_depth;
        const RecentWindowSnapshot recent = recent_window_snapshot(
            read_requests_.load(std::memory_order_relaxed));

        const uint64_t wal_bytes = wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t live_bytes = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t obsolete_ratio =
            wal_bytes == 0 || live_bytes >= wal_bytes ? 0 : ((wal_bytes - live_bytes) * 100) / wal_bytes;

        return compute_batch_policy(options_, WriterPolicySignals {
            queue_depth,
            std::max<uint64_t>(queue_depth, recent.peak_queue_depth),
            recent.read_ratio_per_1000_ops,
            obsolete_ratio,
            recent.avg_batch_size,
            recent.avg_batch_wal_bytes,
            recent.write_latency_p95_us,
            recent.fsync_pressure_per_1000_writes,
        });
    }

    void coordinator_loop() {
        while (!coordinator_stop_.load(std::memory_order_acquire)) {
            RequestPtr first = wait_for_sequence(coordinator_sequence_);
            if (!first) {
                continue;
            }
            ++coordinator_sequence_;

            if (!first->error.empty()) {
                complete(first, first->error);
                continue;
            }
            if (fatal_.load(std::memory_order_acquire) && first->kind != RequestKind::kShutdown) {
                complete(first, fatal_message());
                continue;
            }
            if (first->kind == RequestKind::kFlush) {
                try {
                    sync_barrier();
                    complete(first);
                } catch (const std::exception& error) {
                    set_fatal(error.what());
                    complete(first, error.what());
                }
                continue;
            }
            if (first->kind == RequestKind::kShutdown) {
                stopping_.store(true, std::memory_order_release);
                try {
                    sync_barrier();
                    complete(first);
                } catch (const std::exception& error) {
                    set_fatal(error.what());
                    complete(first, error.what());
                }
                coordinator_stop_.store(true, std::memory_order_release);
                prepared_cv_.notify_all();
                continue;
            }

            std::vector<RequestPtr> batch;
            batch.push_back(first);
            uint64_t observed_queue_depth = 0;
            const BatchPolicy policy = current_batch_policy(observed_queue_depth);
            uint64_t estimated_bytes = first->payload.size() + sizeof(FrameHeader) + sizeof(FrameFooter);
            const Clock::time_point deadline =
                Clock::now() + std::chrono::microseconds(policy.batch_delay_us);
            while (batch.size() < policy.max_batch_size) {
                RequestPtr next = try_take_sequence_until(coordinator_sequence_, deadline);
                if (!next) {
                    break;
                }
                const uint64_t next_bytes = next->payload.size() + sizeof(FrameHeader) + sizeof(FrameFooter);
                if (policy.max_batch_wal_bytes != 0 &&
                    (estimated_bytes > policy.max_batch_wal_bytes ||
                     next_bytes > policy.max_batch_wal_bytes - estimated_bytes)) {
                    std::lock_guard<std::mutex> lock(prepared_mutex_);
                    prepared_.emplace(next->sequence, next);
                    break;
                }
                if (next_bytes > std::numeric_limits<uint64_t>::max() - estimated_bytes) {
                    std::lock_guard<std::mutex> lock(prepared_mutex_);
                    prepared_.emplace(next->sequence, next);
                    break;
                }
                ++coordinator_sequence_;
                estimated_bytes += next_bytes;
                batch.push_back(std::move(next));
                if (policy.batch_delay_us == 0) {
                    break;
                }
            }

            try {
                process_group(batch, policy, observed_queue_depth);
            } catch (const std::exception& error) {
                set_fatal(error.what());
                for (const auto& request : batch) {
                    complete(request, error.what());
                }
            }
        }
    }

    bool validate_request(const RequestPtr& request,
                          std::map<size_t, uint64_t>& virtual_versions) {
        for (const auto& [shard_id, expected] : request->expected_versions) {
            auto found = virtual_versions.find(shard_id);
            if (found == virtual_versions.end()) {
                found = virtual_versions.emplace(
                    shard_id,
                    shards_[shard_id]->version.load(std::memory_order_acquire)).first;
            }
            if (found->second != expected) {
                return false;
            }
        }
        for (const auto& operation : request->operations) {
            const size_t shard_id = ShardForKey(operation.key);
            auto found = virtual_versions.find(shard_id);
            if (found == virtual_versions.end()) {
                found = virtual_versions.emplace(
                    shard_id,
                    shards_[shard_id]->version.load(std::memory_order_acquire)).first;
            }
        }
        return true;
    }

    std::vector<size_t> touched_shards(const RequestPtr& request) const {
        std::vector<size_t> result;
        result.reserve(request->operations.size());
        for (const auto& operation : request->operations) {
            result.push_back(ShardForKey(operation.key));
        }
        std::sort(result.begin(), result.end());
        result.erase(std::unique(result.begin(), result.end()), result.end());
        return result;
    }

    void process_group(const std::vector<RequestPtr>& batch,
                       const BatchPolicy& policy,
                       uint64_t observed_queue_depth) {
        const uint64_t fsync_calls_at_start =
            wal_fsync_calls_.load(std::memory_order_relaxed);
        std::vector<RequestPtr> accepted;
        std::vector<RequestPtr> conflicts;
        std::vector<RequestPtr> read_only_successes;
        std::map<size_t, uint64_t> virtual_versions;
        uint64_t bytes = 0;
        for (const auto& request : batch) {
            if (!request->error.empty()) {
                complete(request, request->error);
                continue;
            }
            if (!validate_request(request, virtual_versions)) {
                conflicts.push_back(request);
                continue;
            }
            if (request->operations.empty()) {
                read_only_successes.push_back(request);
                continue;
            }
            accepted.push_back(request);
            for (size_t shard_id : touched_shards(request)) {
                ++virtual_versions[shard_id];
            }
        }

        if (!accepted.empty()) {
            std::lock_guard<std::mutex> commit_lock(commit_mutex_);
            const uint64_t wal_bytes_before =
                wal_accounting_[active_wal_accounting_slot_].wal_bytes;
            const std::shared_ptr<LockedFileGeneration> commit_generation = current_file_;
            for (const auto& request : accepted) {
                if (last_lsn_ == std::numeric_limits<uint64_t>::max()) {
                    throw KVStoreError("Transaction LSN space is exhausted");
                }
                request->assigned_lsn = ++last_lsn_;
                request->file_generation = commit_generation;
                request->frame_header = make_frame_header(
                    request->payload.size(),
                    static_cast<uint32_t>(request->operations.size()),
                    request->assigned_lsn,
                    request->payload_checksum);
                request->frame_footer = make_frame_footer(request->frame_header);
                if (request->frame_header.frame_bytes > std::numeric_limits<uint64_t>::max() - bytes) {
                    throw KVStoreError("Group commit WAL byte count overflow");
                }
                bytes += request->frame_header.frame_bytes;
            }
            write_frames(accepted);
            if (options_.durability == DurabilityMode::kSync) {
                maybe_trigger_failpoint("before_journal_sync");
                sync_file_locked();
                maybe_trigger_failpoint("after_wal_fsync_before_apply");
            }
            for (const auto& request : accepted) {
                apply_committed(request);
                request->file_generation.reset();
            }
            if (wal_bytes_before > std::numeric_limits<uint64_t>::max() - bytes ||
                wal_accounting_[active_wal_accounting_slot_].wal_bytes !=
                    wal_bytes_before + bytes) {
                throw KVStoreError("Committed WAL accounting does not match physical frame bytes");
            }
            publish_active_wal_accounting();
            dirty_.store(options_.durability != DurabilityMode::kSync, std::memory_order_release);
        }

        for (const auto& request : accepted) {
            request->completion_latency_us =
                std::max<uint64_t>(1, elapsed_us(request->enqueue_time));
        }
        if (!accepted.empty()) {
            record_recent_batch(
                accepted, bytes, observed_queue_depth, fsync_calls_at_start);
        }
        for (const auto& request : accepted) {
            committed_write_requests_.fetch_add(1, std::memory_order_relaxed);
            if (request->explicit_transaction) {
                transaction_commits_.fetch_add(1, std::memory_order_relaxed);
            }
            complete(request);
        }
        for (const auto& request : read_only_successes) {
            if (request->explicit_transaction) {
                transaction_commits_.fetch_add(1, std::memory_order_relaxed);
            }
            complete(request);
        }
        for (const auto& request : conflicts) {
            transaction_conflicts_.fetch_add(1, std::memory_order_relaxed);
            complete(request, "Transaction conflicted with a committed shard version", true);
        }
        if (!accepted.empty()) {
            committed_write_batches_.fetch_add(1, std::memory_order_relaxed);
            group_commit_calls_.fetch_add(1, std::memory_order_relaxed);
            group_commit_requests_.fetch_add(accepted.size(), std::memory_order_relaxed);
            atomic_max(max_group_commit_requests_, accepted.size());
            wal_bytes_written_.fetch_add(bytes, std::memory_order_relaxed);
            last_committed_batch_size_.store(accepted.size(), std::memory_order_relaxed);
            atomic_max(max_committed_batch_size_, accepted.size());
            last_committed_batch_wal_bytes_.store(bytes, std::memory_order_relaxed);
            atomic_max(max_committed_batch_wal_bytes_, bytes);
            last_effective_batch_delay_us_.store(policy.batch_delay_us, std::memory_order_relaxed);
            const uint64_t minimum = min_effective_batch_delay_us_.load(std::memory_order_relaxed);
            if (policy.batch_delay_us > 0 && (minimum == 0 || policy.batch_delay_us < minimum)) {
                min_effective_batch_delay_us_.store(policy.batch_delay_us, std::memory_order_relaxed);
            }
            atomic_max(max_effective_batch_delay_us_, policy.batch_delay_us);
            observed_fsync_pressure_per_1000_writes_.store(
                options_.durability == DurabilityMode::kSync
                    ? (1000 + accepted.size() - 1) / accepted.size()
                    : 0,
                std::memory_order_relaxed);
            last_objective_pressure_score_.store(policy.objective_pressure_score, std::memory_order_relaxed);
            last_objective_cost_score_.store(policy.objective_cost_score, std::memory_order_relaxed);
            last_objective_throughput_score_.store(policy.objective_throughput_score, std::memory_order_relaxed);
            last_objective_balance_score_.store(policy.objective_balance_score, std::memory_order_relaxed);
            last_objective_mode_.store(policy.objective_mode, std::memory_order_relaxed);
            if (policy.adaptive_batching) {
                adaptive_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.adaptive_flush) {
                adaptive_flush_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.latency_target_adjusted) {
                adaptive_latency_target_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.fsync_pressure_adjusted) {
                adaptive_fsync_pressure_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.read_heavy_adjusted) {
                adaptive_read_heavy_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.compaction_pressure_adjusted) {
                adaptive_compaction_pressure_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.wal_growth_adjusted) {
                adaptive_wal_growth_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.objective_short_delay_adjusted) {
                adaptive_objective_short_delay_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.objective_long_delay_adjusted) {
                adaptive_objective_long_delay_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            if (policy.objective_throughput_score > 0) {
                adaptive_objective_throughput_batches_completed_.fetch_add(1, std::memory_order_relaxed);
            }
            maybe_schedule_auto_compaction();
        }
    }

    void write_frames(const std::vector<RequestPtr>& requests) {
        maybe_trigger_failpoint("before_journal_write");
        if (!requests.empty() &&
            (failpoint_is_configured("after_frame_header_write") ||
             failpoint_is_configured("after_frame_payload_write"))) {
            const auto& request = requests.front();
            pwrite_buffer(
                request->file_generation->fd(),
                append_offset_,
                &request->frame_header,
                sizeof(FrameHeader),
                request->file_generation->path());
            maybe_trigger_failpoint("after_frame_header_write");
            pwrite_buffer(request->file_generation->fd(),
                          append_offset_ + sizeof(FrameHeader),
                          request->payload.data(),
                          request->payload.size(),
                          request->file_generation->path());
            maybe_trigger_failpoint("after_frame_payload_write");
        }

        std::vector<iovec> vectors;
        if (requests.size() > std::numeric_limits<size_t>::max() / 3) {
            throw KVStoreError("Group commit iovec count overflow");
        }
        vectors.reserve(requests.size() * 3);
        uint64_t total = 0;
        uint64_t next_frame_offset = append_offset_;
        const uint64_t max_file_offset = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
        for (const auto& request : requests) {
            const uint64_t frame_bytes = request->frame_header.frame_bytes;
            if (next_frame_offset > max_file_offset ||
                frame_bytes > max_file_offset - next_frame_offset ||
                frame_bytes > std::numeric_limits<uint64_t>::max() - total) {
                throw KVStoreError("Journal exceeds the platform file size limit");
            }
            request->frame_offset = next_frame_offset;
            iovec header_vector {};
            header_vector.iov_base = &request->frame_header;
            header_vector.iov_len = sizeof(request->frame_header);
            vectors.push_back(header_vector);
            iovec payload_vector {};
            payload_vector.iov_base = request->payload.data();
            payload_vector.iov_len = request->payload.size();
            vectors.push_back(payload_vector);
            iovec footer_vector {};
            footer_vector.iov_base = &request->frame_footer;
            footer_vector.iov_len = sizeof(request->frame_footer);
            vectors.push_back(footer_vector);
            total += frame_bytes;
            next_frame_offset += frame_bytes;
        }

        size_t first = 0;
        uint64_t offset = append_offset_;
        while (first < vectors.size()) {
            const int count = static_cast<int>(std::min<size_t>(vectors.size() - first, IOV_MAX));
            ssize_t written;
            do {
                written = ::pwritev(requests.front()->file_generation->fd(),
                                    vectors.data() + first,
                                    count,
                                    static_cast<off_t>(offset));
            } while (written < 0 && errno == EINTR);
            if (written < 0) {
                throw io_error("pwritev", requests.front()->file_generation->path());
            }
            if (written == 0) {
                throw KVStoreError("pwritev made no progress for " + db_path_);
            }
            offset += static_cast<uint64_t>(written);
            ssize_t remaining = written;
            while (remaining > 0 && first < vectors.size()) {
                if (remaining >= static_cast<ssize_t>(vectors[first].iov_len)) {
                    remaining -= static_cast<ssize_t>(vectors[first].iov_len);
                    ++first;
                } else {
                    vectors[first].iov_base = static_cast<uint8_t*>(vectors[first].iov_base) + remaining;
                    vectors[first].iov_len -= static_cast<size_t>(remaining);
                    remaining = 0;
                }
            }
        }
        append_offset_ += total;
        maybe_trigger_failpoint("after_frame_footer_write");
        maybe_trigger_failpoint("after_journal_write_before_sync");
    }

    void apply_committed(const RequestPtr& request) {
        const std::vector<size_t> shard_ids = touched_shards(request);
        std::vector<std::unique_lock<std::shared_mutex>> locks;
        locks.reserve(shard_ids.size());
        for (size_t shard_id : shard_ids) {
            locks.emplace_back(shards_[shard_id]->mutex);
        }
        uint64_t payload_cursor = 0;
        for (const auto& operation : request->operations) {
            Shard& shard = *shards_[ShardForKey(operation.key)];
            const uint64_t value_offset = request->frame_offset + sizeof(FrameHeader) +
                                          payload_cursor + sizeof(MutationHeader) +
                                          operation.key.size();
            apply_operation_locked(
                shard,
                operation,
                request->assigned_lsn,
                request->file_generation,
                value_offset,
                true);
            payload_cursor += sizeof(MutationHeader) + operation.key.size() + operation.value.bytes.size();
        }
        for (size_t shard_id : shard_ids) {
            shards_[shard_id]->version.fetch_add(1, std::memory_order_release);
        }
    }

    void sync_file_locked() {
        const auto start = Clock::now();
        fdatasync_or_throw(current_file_->fd(), current_file_->path());
        const uint64_t duration = elapsed_us(start);
        wal_fsync_calls_.fetch_add(1, std::memory_order_relaxed);
        fdatasync_time_us_.fetch_add(duration, std::memory_order_relaxed);
        atomic_max(max_fdatasync_time_us_, duration);
        dirty_.store(false, std::memory_order_release);
    }

    void sync_barrier() {
        throw_if_fatal();
        std::lock_guard<std::mutex> lock(commit_mutex_);
        sync_file_locked();
    }

    void periodic_loop() {
        std::unique_lock<std::mutex> lock(periodic_mutex_);
        while (!periodic_stop_) {
#if defined(KVSTORE_TSAN_INSTRUMENTED)
            periodic_cv_.wait_until(
                lock,
                std::chrono::system_clock::now() +
                    std::chrono::milliseconds(options_.periodic_sync_interval_ms),
                [this] { return periodic_stop_; });
#else
            periodic_cv_.wait_for(
                lock,
                std::chrono::milliseconds(options_.periodic_sync_interval_ms),
                [this] { return periodic_stop_; });
#endif
            if (periodic_stop_) {
                return;
            }
            if (!dirty_.load(std::memory_order_acquire)) {
                continue;
            }
            lock.unlock();
            try {
                Flush();
            } catch (...) {
                return;
            }
            lock.lock();
        }
    }

    void maybe_schedule_auto_compaction() {
        const uint64_t bytes = wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t live = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t obsolete_ratio = bytes == 0 || live >= bytes ? 0 : ((bytes - live) * 100) / bytes;
        const bool byte_threshold = options_.auto_compact_wal_bytes_threshold != 0 &&
                                    bytes >= options_.auto_compact_wal_bytes_threshold;
        const bool ratio_threshold = options_.auto_compact_invalid_wal_ratio_percent != 0 &&
                                     (options_.auto_compact_min_wal_bytes_for_ratio == 0 ||
                                      bytes >= options_.auto_compact_min_wal_bytes_for_ratio) &&
                                     obsolete_ratio >= options_.auto_compact_invalid_wal_ratio_percent;
        if (!byte_threshold && !ratio_threshold) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(auto_compaction_mutex_);
            if (Clock::now() < auto_compaction_retry_after_) {
                return;
            }
            auto_compaction_requested_ = true;
        }
        auto_compaction_cv_.notify_one();
    }

    void auto_compaction_loop() {
        while (true) {
            {
                std::unique_lock<std::mutex> lock(auto_compaction_mutex_);
                auto_compaction_cv_.wait(lock, [this] {
                    return auto_compaction_stop_ || auto_compaction_requested_;
                });
                if (auto_compaction_stop_) {
                    return;
                }
                auto_compaction_requested_ = false;
            }
            try {
                compact_impl(true);
            } catch (...) {
                auto_compaction_failures_.fetch_add(1, std::memory_order_relaxed);
                std::lock_guard<std::mutex> lock(auto_compaction_mutex_);
                auto_compaction_requested_ = false;
                auto_compaction_retry_after_ = Clock::now() + std::chrono::seconds(1);
            }
        }
    }

    void mark_checkpoint_relocations(std::vector<CheckpointReference>& references,
                                     uint64_t relocation_epoch) {
        std::vector<std::vector<size_t>> by_shard(shards_.size());
        for (size_t index = 0; index < references.size(); ++index) {
            by_shard[references[index].shard_id].push_back(index);
        }
        for (size_t shard_id = 0; shard_id < shards_.size(); ++shard_id) {
            std::unique_lock<std::shared_mutex> lock(shards_[shard_id]->mutex);
            for (size_t reference_index : by_shard[shard_id]) {
                const CheckpointReference& reference = references[reference_index];
                const auto found = shards_[shard_id]->values.find(reference.key);
                if (found == shards_[shard_id]->values.end()) {
                    continue;
                }
                Entry& entry = found->second;
                if (entry.lsn == reference.lsn &&
                    entry.file_generation == reference.file_generation &&
                    entry.value_offset == reference.source_value_offset &&
                    entry.value_size == reference.value_size &&
                    entry.value_checksum == reference.value_checksum) {
                    entry.relocation_epoch = relocation_epoch;
                    entry.relocation_offset = reference.checkpoint_value_offset;
                }
            }
        }
    }

    void write_checkpoint_objects(const std::vector<CheckpointReference>& references,
                                  int object_spool_fd) {
        constexpr uint64_t kReadWindowBytes = 1024ULL * 1024ULL;
        constexpr uint64_t kMaximumMergeGap = 64ULL * 1024ULL;
        BufferedSequentialWriter writer(
            object_spool_fd, "unlinked compaction object spool", 0, 0, false);
        std::array<uint8_t, 1024 * 1024> buffer {};

        size_t index = 0;
        while (index < references.size()) {
            const CheckpointReference& first = references[index];
            if (!first.file_generation) {
                throw CompactionFatalError(
                    "Compaction reference has no backing file generation");
            }
            if (writer.Offset() != first.checkpoint_value_offset) {
                throw CompactionFatalError(
                    "Compaction object spool offset changed unexpectedly");
            }

            if (first.value_size > kReadWindowBytes) {
                uint64_t consumed = 0;
                uint32_t checksum = 0;
                while (consumed < first.value_size) {
                    const size_t chunk = static_cast<size_t>(
                        std::min<uint64_t>(buffer.size(), first.value_size - consumed));
                    try {
                        pread_exact_at(first.file_generation->fd(),
                                       first.source_value_offset + consumed,
                                       buffer.data(),
                                       chunk,
                                       first.file_generation->path());
                    } catch (const std::exception& error) {
                        throw CompactionFatalError(error.what());
                    }
                    checksum = crc32c_extend(checksum, buffer.data(), chunk);
                    writer.Write(buffer.data(), chunk);
                    consumed += chunk;
                }
                if (checksum != first.value_checksum) {
                    throw CompactionFatalError(
                        "Value checksum mismatch while streaming compaction from " +
                        first.file_generation->path());
                }
                ++index;
                continue;
            }

            const uint64_t window_start = first.source_value_offset;
            uint64_t window_end = window_start + first.value_size;
            size_t end_index = index + 1;
            while (end_index < references.size()) {
                const CheckpointReference& next = references[end_index];
                if (next.file_generation != first.file_generation ||
                    next.value_size > kReadWindowBytes) {
                    break;
                }
                if (next.source_value_offset < window_end && next.value_size != 0) {
                    break;
                }
                const uint64_t gap = next.source_value_offset > window_end
                                         ? next.source_value_offset - window_end
                                         : 0;
                if (gap > kMaximumMergeGap ||
                    next.source_value_offset > std::numeric_limits<uint64_t>::max() -
                                                   next.value_size) {
                    break;
                }
                const uint64_t candidate_end = next.source_value_offset + next.value_size;
                const uint64_t merged_end = std::max(window_end, candidate_end);
                if (merged_end - window_start > kReadWindowBytes) {
                    break;
                }
                window_end = merged_end;
                ++end_index;
            }

            const size_t window_size = static_cast<size_t>(window_end - window_start);
            if (window_size != 0) {
                try {
                    pread_exact_at(first.file_generation->fd(),
                                   window_start,
                                   buffer.data(),
                                   window_size,
                                   first.file_generation->path());
                } catch (const std::exception& error) {
                    throw CompactionFatalError(error.what());
                }
            }
            for (size_t current = index; current < end_index; ++current) {
                const CheckpointReference& reference = references[current];
                if (writer.Offset() != reference.checkpoint_value_offset) {
                    throw CompactionFatalError(
                        "Compaction object ordering changed unexpectedly");
                }
                const uint64_t relative = reference.source_value_offset - window_start;
                const uint8_t* value = buffer.data() + relative;
                if (crc32c(value, reference.value_size) != reference.value_checksum) {
                    throw CompactionFatalError(
                        "Value checksum mismatch while coalescing compaction from " +
                        reference.file_generation->path());
                }
                writer.Write(value, reference.value_size);
            }
            index = end_index;
        }
        writer.Flush();
    }

    void migrate_entries_to_generation(
        const std::shared_ptr<LockedFileGeneration>& old_generation,
        const std::shared_ptr<LockedFileGeneration>& new_generation,
        uint64_t start_lsn,
        uint64_t start_offset,
        uint64_t end_offset,
        uint64_t relocation_epoch,
        const Superblock& compacted_superblock) {
        for (auto& shard : shards_) {
            std::unique_lock<std::shared_mutex> lock(shard->mutex);
            for (auto& [key, entry] : shard->values) {
                (void)key;
                if (entry.file_generation != old_generation) {
                    continue;
                }
                if (entry.lsn > start_lsn) {
                    if (entry.value_offset < start_offset ||
                        entry.value_offset > end_offset ||
                        entry.value_size > end_offset - entry.value_offset) {
                        throw KVStoreError(
                            "Compaction found a post-cut value outside the journal delta");
                    }
                    entry.value_offset = compacted_superblock.journal_offset +
                                         (entry.value_offset - start_offset);
                    entry.file_generation = new_generation;
                } else if (entry.relocation_epoch == relocation_epoch) {
                    entry.value_offset = compacted_superblock.object_offset +
                                         entry.relocation_offset;
                    entry.file_generation = new_generation;
                } else {
                    throw KVStoreError(
                        "Compaction could not relocate an unchanged checkpoint entry");
                }
                entry.relocation_epoch = 0;
                entry.relocation_offset = 0;
            }
        }
    }

    void compact_impl(bool automatic) {
        throw_if_fatal();
        if (!automatic) {
            compact_requests_.fetch_add(1, std::memory_order_relaxed);
        }
        std::unique_lock<std::mutex> compaction_lock(compaction_mutex_);

        uint64_t start_offset = 0;
        uint64_t start_lsn = 0;
        uint64_t generation = 0;
        std::shared_ptr<LockedFileGeneration> source_generation;
        bool accounting_started = false;
        bool accounting_switched = false;
        bool renamed = false;
        uint64_t pre_compaction_wal_bytes = 0;
        uint64_t post_compaction_wal_bytes = 0;
        uint64_t end_offset = 0;

        const uint64_t temp_id =
            compact_temp_sequence_.fetch_add(1, std::memory_order_relaxed);
        std::string temp_path;
        std::shared_ptr<LockedFileGeneration> temp_generation;
        bool temp_created = false;

        try {
            {
                std::lock_guard<std::mutex> lock(commit_mutex_);
                if (superblock_.generation == std::numeric_limits<uint64_t>::max()) {
                    throw KVStoreError("Storage generation space is exhausted");
                }
                start_offset = append_offset_;
                start_lsn = last_lsn_;
                generation = superblock_.generation + 1;
                source_generation = current_file_;
                if (!source_generation) {
                    throw CompactionFatalError(
                        "Compaction source has no backing file generation");
                }
                begin_pending_wal_accounting();
                accounting_started = true;
            }

            std::vector<CheckpointReference> references;
            uint64_t index_entries_bytes = 0;
            for (size_t shard_id = 0; shard_id < shards_.size(); ++shard_id) {
                std::shared_lock<std::shared_mutex> lock(shards_[shard_id]->mutex);
                for (const auto& [key, entry] : shards_[shard_id]->values) {
                    if (!entry.file_generation ||
                        entry.value_offset > std::numeric_limits<uint64_t>::max() -
                                                 entry.value_size) {
                        throw CompactionFatalError(
                            "Checkpoint entry has invalid backing metadata");
                    }
                    const uint64_t index_addition = sizeof(IndexEntryHeader) + key.size();
                    if (index_entries_bytes >
                        std::numeric_limits<uint64_t>::max() - index_addition) {
                        throw KVStoreError("Checkpoint index size overflow during compaction");
                    }
                    CheckpointReference reference;
                    reference.shard_id = shard_id;
                    reference.key = key;
                    reference.file_generation = entry.file_generation;
                    reference.source_value_offset = entry.value_offset;
                    reference.lsn = entry.lsn;
                    reference.value_size = entry.value_size;
                    reference.value_checksum = entry.value_checksum;
                    references.push_back(std::move(reference));
                    index_entries_bytes += index_addition;
                }
            }

            std::sort(references.begin(), references.end(),
                      [](const CheckpointReference& lhs,
                         const CheckpointReference& rhs) {
                          if (lhs.file_generation->id() != rhs.file_generation->id()) {
                              return lhs.file_generation->id() < rhs.file_generation->id();
                          }
                          if (lhs.source_value_offset != rhs.source_value_offset) {
                              return lhs.source_value_offset < rhs.source_value_offset;
                          }
                          return lhs.key < rhs.key;
                      });

            uint64_t object_length = 0;
            for (auto& reference : references) {
                reference.checkpoint_value_offset = object_length;
                if (object_length > std::numeric_limits<uint64_t>::max() -
                                        reference.value_size) {
                    throw KVStoreError("Checkpoint object size overflow during compaction");
                }
                object_length += reference.value_size;
            }

            if (index_entries_bytes >
                std::numeric_limits<uint64_t>::max() - sizeof(IndexHeader)) {
                throw KVStoreError("Checkpoint index exceeds implementation limits");
            }
            const uint64_t index_offset = kDataOffset;
            const uint64_t index_length = sizeof(IndexHeader) + index_entries_bytes;
            if (index_length > kMaxTransactionBytes * 16ULL) {
                throw KVStoreError("Checkpoint index exceeds implementation limits");
            }
            const uint64_t object_offset = index_offset + index_length;
            if (object_offset < index_offset ||
                object_offset > std::numeric_limits<uint64_t>::max() - object_length) {
                throw KVStoreError("Checkpoint object offset overflow");
            }
            const uint64_t journal_offset = object_offset + object_length;
            if (journal_offset >
                static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
                throw KVStoreError("Checkpoint exceeds the platform file size limit");
            }

            const uint64_t relocation_epoch =
                relocation_epoch_sequence_.fetch_add(1, std::memory_order_relaxed);
            if (relocation_epoch == 0 ||
                relocation_epoch == std::numeric_limits<uint64_t>::max()) {
                throw KVStoreError("Compaction relocation epoch space is exhausted");
            }
            mark_checkpoint_relocations(references, relocation_epoch);

            ScopedFd index_spool =
                open_unlinked_spool(db_path_, "index-spool", temp_id);
            ScopedFd object_spool =
                open_unlinked_spool(db_path_, "object-spool", temp_id);

            BufferedSequentialWriter index_writer(
                index_spool.get(), "unlinked compaction index spool", 0);
            for (const auto& reference : references) {
                const IndexEntryHeader header = make_index_entry_header(
                    reference.key,
                    reference.checkpoint_value_offset,
                    reference.value_size,
                    reference.value_checksum);
                index_writer.Write(&header, sizeof(header));
                index_writer.Write(reference.key.data(), reference.key.size());
            }
            index_writer.Flush();
            if (index_writer.Offset() != index_entries_bytes) {
                throw CompactionFatalError(
                    "Checkpoint index spool size changed during compaction");
            }
            write_checkpoint_objects(references, object_spool.get());

            ScopedFd raw_temp = open_unique_compaction_file(db_path_, temp_id, temp_path);
            temp_created = true;
            lock_file_exclusively(raw_temp.get(), temp_path);
            copy_supported_file_metadata(source_generation->fd(),
                                         raw_temp.get(),
                                         source_generation->path(),
                                         temp_path);
            temp_generation = std::make_shared<LockedFileGeneration>(
                raw_temp.get(), temp_path, next_file_generation_id_++);
            (void)raw_temp.release();

            if (::ftruncate(temp_generation->fd(), static_cast<off_t>(journal_offset)) != 0) {
                throw io_error("ftruncate", temp_path);
            }
            const IndexHeader index_header = make_index_header(
                references.size(), index_entries_bytes, index_writer.Checksum());
            BufferedSequentialWriter checkpoint_writer(
                temp_generation->fd(), temp_path, index_offset);
            checkpoint_writer.Write(&index_header, sizeof(index_header));
            copy_spool(index_spool.get(),
                       index_entries_bytes,
                       checkpoint_writer,
                       "unlinked compaction index spool");
            if (checkpoint_writer.Offset() != object_offset) {
                throw CompactionFatalError(
                    "Checkpoint object region did not follow its index");
            }
            copy_spool(object_spool.get(),
                       object_length,
                       checkpoint_writer,
                       "unlinked compaction object spool");
            checkpoint_writer.Flush();
            if (checkpoint_writer.Offset() != journal_offset) {
                throw CompactionFatalError(
                    "Checkpoint journal boundary changed during assembly");
            }
            const Superblock compacted_superblock = make_superblock(
                generation,
                start_lsn,
                index_offset,
                index_length,
                object_offset,
                object_length,
                journal_offset,
                checkpoint_writer.Checksum());
            write_superblocks(temp_generation->fd(), compacted_superblock, temp_path);
            maybe_trigger_failpoint("after_checkpoint_write_before_sync");

            const auto pause_start = Clock::now();
            std::unique_lock<std::mutex> commit_lock(commit_mutex_);
            if (current_file_ != source_generation) {
                throw CompactionFatalError(
                    "Database file generation changed during compaction");
            }
            end_offset = append_offset_;
            if (end_offset < start_offset || pending_wal_accounting_slot_ < 0) {
                throw CompactionFatalError(
                    "Journal offset or WAL epoch moved backwards during compaction");
            }
            const uint64_t delta_bytes = end_offset - start_offset;
            const size_t pending_slot =
                static_cast<size_t>(pending_wal_accounting_slot_);
            if (wal_accounting_[pending_slot].wal_bytes != delta_bytes) {
                throw CompactionFatalError(
                    "Compaction delta WAL accounting does not match copied bytes");
            }
            if (delta_bytes >
                static_cast<uint64_t>(std::numeric_limits<off_t>::max()) -
                    compacted_superblock.journal_offset) {
                throw KVStoreError("Compacted journal exceeds the platform file size limit");
            }
            copy_fd_region(source_generation->fd(),
                           start_offset,
                           temp_generation->fd(),
                           compacted_superblock.journal_offset,
                           delta_bytes,
                           source_generation->path(),
                           temp_path);
            if (::ftruncate(
                    temp_generation->fd(),
                    static_cast<off_t>(compacted_superblock.journal_offset + delta_bytes)) != 0) {
                throw io_error("ftruncate", temp_path);
            }
            maybe_trigger_failpoint("before_compaction_temp_sync");
            fdatasync_or_throw(temp_generation->fd(), temp_path);
            maybe_trigger_failpoint("after_compaction_temp_sync_before_rename");
            maybe_trigger_failpoint("before_snapshot_rename");
            maybe_trigger_failpoint("after_snapshot_fsync_before_rename");
            temp_generation->SetPath(db_path_);
            if (::rename(temp_path.c_str(), db_path_.c_str()) != 0) {
                temp_generation->SetPath(temp_path);
                throw io_error("rename compacted container", db_path_);
            }
            renamed = true;

            current_file_ = temp_generation;
            superblock_ = compacted_superblock;
            append_offset_ = compacted_superblock.journal_offset + delta_bytes;
            pre_compaction_wal_bytes =
                wal_accounting_[active_wal_accounting_slot_].wal_bytes;
            active_wal_accounting_slot_ = pending_slot;
            pending_wal_accounting_slot_ = -1;
            post_compaction_wal_bytes =
                wal_accounting_[active_wal_accounting_slot_].wal_bytes;
            publish_active_wal_accounting();
            accounting_switched = true;

            maybe_trigger_failpoint("after_snapshot_rename");
            maybe_trigger_failpoint("after_snapshot_rename_before_wal_reset");
            maybe_trigger_failpoint("after_wal_rotation_before_reopen");
            maybe_trigger_failpoint("after_compaction_rename_before_directory_sync");
            maybe_trigger_failpoint("before_compaction_directory_sync");
            fsync_directory(db_path_);
            maybe_trigger_failpoint("after_compaction_directory_sync");
            dirty_.store(false, std::memory_order_release);
            const uint64_t pause = elapsed_us(pause_start);
            compaction_pause_time_us_.fetch_add(pause, std::memory_order_relaxed);
            atomic_max(max_compaction_pause_time_us_, pause);
            commit_lock.unlock();

            maybe_trigger_failpoint("before_compaction_entry_migration");
            migrate_entries_to_generation(source_generation,
                                          temp_generation,
                                          start_lsn,
                                          start_offset,
                                          end_offset,
                                          relocation_epoch,
                                          compacted_superblock);
            maybe_trigger_failpoint("after_compaction_entry_migration");
            references.clear();
            source_generation.reset();

            total_snapshot_bytes_written_.fetch_add(
                index_length + object_length,
                std::memory_order_relaxed);
            total_wal_bytes_reclaimed_by_compaction_.fetch_add(
                pre_compaction_wal_bytes > post_compaction_wal_bytes
                    ? pre_compaction_wal_bytes - post_compaction_wal_bytes
                    : 0,
                std::memory_order_relaxed);
            if (automatic) {
                auto_compactions_completed_.fetch_add(1, std::memory_order_relaxed);
            } else {
                manual_compactions_completed_.fetch_add(1, std::memory_order_relaxed);
            }
        } catch (const CompactionFatalError& error) {
            if (accounting_started && !accounting_switched) {
                std::lock_guard<std::mutex> lock(commit_mutex_);
                abandon_pending_wal_accounting();
            }
            temp_generation.reset();
            if (temp_created && !renamed) {
                (void)::unlink(temp_path.c_str());
            }
            set_fatal(error.what());
            throw;
        } catch (const std::exception& error) {
            if (accounting_started && !accounting_switched) {
                std::lock_guard<std::mutex> lock(commit_mutex_);
                abandon_pending_wal_accounting();
            }
            temp_generation.reset();
            if (temp_created && !renamed) {
                (void)::unlink(temp_path.c_str());
            }
            if (renamed) {
                set_fatal(error.what());
            }
            throw;
        } catch (...) {
            if (accounting_started && !accounting_switched) {
                std::lock_guard<std::mutex> lock(commit_mutex_);
                abandon_pending_wal_accounting();
            }
            temp_generation.reset();
            if (temp_created && !renamed) {
                (void)::unlink(temp_path.c_str());
            }
            if (renamed) {
                set_fatal("Compaction failed after publishing the new file generation");
            }
            throw;
        }
    }

    void set_fatal(const std::string& message) {
        if (fatal_.load(std::memory_order_acquire)) {
            return;
        }
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        if (!fatal_.load(std::memory_order_relaxed)) {
            fatal_message_ = message;
            fatal_.store(true, std::memory_order_release);
        }
    }

    std::string fatal_message() const {
        std::lock_guard<std::mutex> lock(fatal_mutex_);
        return fatal_message_.empty() ? "KVStore entered a fatal state" : fatal_message_;
    }

    void throw_if_fatal() const {
        if (fatal_.load(std::memory_order_acquire)) {
            throw KVStoreError(fatal_message());
        }
    }
};

StorageEngine::StorageEngine(std::string db_path, KVStoreOptions options)
    : pimpl_(std::make_unique<Impl>(std::move(db_path), options)) {}

StorageEngine::~StorageEngine() = default;

void StorageEngine::Put(std::string key, Value value) {
    pimpl_->Put(std::move(key), std::move(value));
}

void StorageEngine::Delete(std::string key) {
    pimpl_->Delete(std::move(key));
}

void StorageEngine::WriteBatch(std::vector<Mutation> operations) {
    pimpl_->WriteBatch(std::move(operations));
}

std::optional<Value> StorageEngine::Get(const std::string& key) {
    return pimpl_->Get(key);
}

StorageEngine::VersionedRead StorageEngine::GetVersioned(const std::string& key) {
    return pimpl_->GetVersioned(key);
}

std::vector<std::pair<std::string, Value>> StorageEngine::Scan(const std::string& start_key,
                                                               const std::string& end_key) {
    return pimpl_->Scan(start_key, end_key);
}

std::vector<std::pair<std::string, Value>> StorageEngine::Scan(const std::string& start_key,
                                                               const std::string& end_key,
                                                               size_t limit) {
    return pimpl_->Scan(start_key, end_key, limit);
}

void StorageEngine::CommitTransaction(std::vector<Mutation> operations,
                                 std::map<size_t, uint64_t> expected_versions) {
    pimpl_->CommitTransaction(std::move(operations), std::move(expected_versions));
}

void StorageEngine::NoteTransactionRollback() noexcept {
    pimpl_->NoteTransactionRollback();
}

void StorageEngine::Flush() {
    pimpl_->Flush();
}

void StorageEngine::Compact() {
    pimpl_->Compact();
}

KVStoreMetrics StorageEngine::GetMetrics() const {
    return pimpl_->GetMetrics();
}

size_t StorageEngine::ShardForKey(const std::string& key) const {
    return pimpl_->ShardForKey(key);
}

}  // namespace kvstore::internal
