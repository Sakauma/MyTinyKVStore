#include "storage_engine.h"

#include "io.h"
#include "key_codec.h"
#include "writer_policy.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <limits>
#include <list>
#include <map>
#include <mutex>
#include <queue>
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
#include <unistd.h>

namespace kvstore::internal {

namespace {

using Clock = std::chrono::steady_clock;

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

size_t latency_bucket(uint64_t microseconds) {
    constexpr std::array<uint64_t, kWriteLatencyBucketCount - 1> limits {
        50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000,
    };
    for (size_t index = 0; index < limits.size(); ++index) {
        if (microseconds <= limits[index]) {
            return index;
        }
    }
    return kWriteLatencyBucketCount - 1;
}

uint64_t percentile_from_histogram(const std::array<uint64_t, kWriteLatencyBucketCount>& buckets,
                                   uint64_t percentile) {
    constexpr std::array<uint64_t, kWriteLatencyBucketCount> upper_bounds {
        50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 100001,
    };
    uint64_t total = 0;
    for (uint64_t count : buckets) {
        total += count;
    }
    if (total == 0) {
        return 0;
    }
    const uint64_t target = (total * percentile + 99) / 100;
    uint64_t cumulative = 0;
    for (size_t index = 0; index < buckets.size(); ++index) {
        cumulative += buckets[index];
        if (cumulative >= target) {
            return upper_bounds[index];
        }
    }
    return upper_bounds.back();
}

void lock_file_exclusively(int fd, const std::string& path) {
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            throw KVStoreError("Database is already open by another process: " + path);
        }
        throw io_error("flock", path);
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

uint32_t pread_copy(int source_fd,
                    uint64_t source_offset,
                    int destination_fd,
                    uint64_t destination_offset,
                    uint64_t bytes,
                    const std::string& source_path,
                    const std::string& destination_path) {
    std::array<uint8_t, 1024 * 1024> buffer {};
    uint64_t copied = 0;
    uint32_t checksum = 0;
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
            throw io_error("pread", source_path);
        }
        if (nread == 0) {
            throw KVStoreError("Unexpected EOF while copying compacted journal from " + source_path);
        }
        checksum = crc32c_extend(checksum, buffer.data(), static_cast<size_t>(nread));

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
    return checksum;
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

uint32_t crc_fd_region(int fd,
                       uint64_t offset,
                       uint64_t length,
                       uint32_t seed,
                       const std::string& path) {
    std::array<uint8_t, 1024 * 1024> buffer {};
    uint64_t consumed = 0;
    uint32_t checksum = seed;
    while (consumed < length) {
        const size_t request = static_cast<size_t>(std::min<uint64_t>(buffer.size(), length - consumed));
        ssize_t nread;
        do {
            nread = ::pread(fd,
                            buffer.data(),
                            request,
                            static_cast<off_t>(offset + consumed));
        } while (nread < 0 && errno == EINTR);
        if (nread < 0) {
            throw io_error("pread", path);
        }
        if (nread == 0) {
            throw KVStoreError("Unexpected EOF while checksumming " + path);
        }
        checksum = crc32c_extend(checksum, buffer.data(), static_cast<size_t>(nread));
        consumed += static_cast<uint64_t>(nread);
    }
    return checksum;
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
          options_(sanitize_options(options)) {
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
        raw_not_empty_.notify_all();
        raw_not_full_.notify_all();
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
        close_if_open(fd_);
        fd_ = -1;
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
        std::unique_lock<std::shared_mutex> lock(shard.mutex);
        VersionedRead result;
        result.shard_id = shard_id;
        result.shard_version = shard.version.load(std::memory_order_relaxed);
        try {
            const auto found = shard.values.find(key);
            if (found != shard.values.end()) {
                result.value = read_entry_locked(shard, found->second, true);
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
        throw_if_fatal();
        if (start_key > end_key) {
            throw KVStoreError("Scan start key must not be greater than end key");
        }
        read_requests_.fetch_add(1, std::memory_order_relaxed);
        const std::string encoded_start = encode_string_key(start_key);
        const std::string encoded_end = encode_string_key(end_key);
        validate_api_key(encoded_start);
        validate_api_key(encoded_end);

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

        std::vector<std::pair<std::string, Value>> result;
        try {
            while (!heap.empty()) {
                Cursor cursor = heap.top();
                heap.pop();
                const std::string key = *cursor.iterator;
                const auto value = shards_[cursor.shard_id]->values.find(key);
                if (value == shards_[cursor.shard_id]->values.end()) {
                    throw KVStoreError("String scan index is inconsistent with the point index");
                }
                result.emplace_back(decode_string_key(key), read_entry_without_cache(value->second));
                ++cursor.iterator;
                if (cursor.iterator != cursor.end && *cursor.iterator <= encoded_end) {
                    heap.push(cursor);
                }
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
        try {
            compact_impl(false);
        } catch (const std::exception& error) {
            set_fatal(error.what());
            throw;
        }
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
        result.manual_compactions_completed = manual_compactions_completed_.load(std::memory_order_relaxed);
        result.auto_compactions_completed = auto_compactions_completed_.load(std::memory_order_relaxed);
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
        result.approx_write_latency_p50_us = percentile_from_histogram(result.write_latency_histogram, 50);
        result.approx_write_latency_p95_us = percentile_from_histogram(result.write_latency_histogram, 95);
        result.approx_write_latency_p99_us = percentile_from_histogram(result.write_latency_histogram, 99);
        result.recent_observed_write_latency_p95_us = result.approx_write_latency_p95_us;
        result.recent_read_requests = result.read_requests;
        result.recent_write_requests = result.committed_write_requests;
        result.recent_read_ratio_per_1000_ops = recent_read_ratio_per_1000_ops_.load(std::memory_order_relaxed);
        result.recent_peak_queue_depth = recent_peak_queue_depth_.load(std::memory_order_relaxed);
        result.recent_avg_batch_size = result.last_committed_batch_size;
        const uint64_t batch_target = options_.adaptive_objective_target_batch_size != 0
                                          ? options_.adaptive_objective_target_batch_size
                                          : options_.max_batch_size;
        result.recent_batch_fill_per_1000 =
            batch_target == 0 ? 0 : std::min<uint64_t>(1000, (result.recent_avg_batch_size * 1000) / batch_target);
        result.recent_avg_batch_wal_bytes = result.last_committed_batch_wal_bytes;
        result.recent_window_batch_count = std::min<uint64_t>(64, result.committed_write_batches);
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
        std::shared_ptr<const Value> cached_value;
        uint64_t value_offset = 0;
        uint32_t value_size = 0;
        uint32_t value_checksum = 0;
        uint64_t lsn = 0;
        bool in_cache = false;
        uint64_t cache_charge = 0;
        std::list<Entry*>::iterator cache_position;
    };

    struct CheckpointReference {
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
        std::list<Entry*> cache_lru;
        uint64_t cache_bytes = 0;
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
        uint64_t assigned_lsn = 0;
        uint64_t frame_offset = 0;
        std::vector<uint8_t> frame;
        Clock::time_point enqueue_time = Clock::now();
        std::mutex completion_mutex;
        std::condition_variable completion_cv;
        bool completed = false;
        bool conflict = false;
        std::string error;
    };

    using RequestPtr = std::shared_ptr<Request>;

    std::string db_path_;
    KVStoreOptions options_;
    int fd_ = -1;
    Superblock superblock_ {};
    uint64_t append_offset_ = 0;
    uint64_t last_lsn_ = 0;
    std::vector<std::unique_ptr<Shard>> shards_;

    mutable std::mutex raw_mutex_;
    std::condition_variable raw_not_empty_;
    std::condition_variable raw_not_full_;
    std::deque<RequestPtr> raw_queue_;
    bool raw_stop_ = false;

    std::mutex prepared_mutex_;
    std::condition_variable prepared_cv_;
    std::map<uint64_t, RequestPtr> prepared_;
    uint64_t coordinator_sequence_ = 1;
    std::atomic<bool> coordinator_stop_ {false};

    std::vector<std::thread> workers_;
    std::thread coordinator_;
    uint64_t next_sequence_ = 1;  // Guarded by raw_mutex_.
    std::atomic<bool> stopping_ {false};
    std::atomic<bool> dirty_ {false};

    std::mutex commit_mutex_;
    std::mutex compaction_mutex_;
    std::atomic<uint64_t> compact_temp_sequence_ {0};

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
    std::unordered_map<std::string, uint64_t> latest_wal_record_bytes_;
    std::atomic<uint64_t> last_committed_batch_size_ {0};
    std::atomic<uint64_t> max_committed_batch_size_ {0};
    std::atomic<uint64_t> last_committed_batch_wal_bytes_ {0};
    std::atomic<uint64_t> max_committed_batch_wal_bytes_ {0};
    std::atomic<uint64_t> max_pending_queue_depth_ {0};
    std::atomic<uint64_t> manual_compactions_completed_ {0};
    std::atomic<uint64_t> auto_compactions_completed_ {0};
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
    std::atomic<uint64_t> recent_read_ratio_per_1000_ops_ {0};
    std::atomic<uint64_t> recent_peak_queue_depth_ {0};
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
        Value value(std::vector<uint8_t>(entry.value_size));
        size_t total = 0;
        while (total < value.bytes.size()) {
            ssize_t nread;
            do {
                nread = ::pread(fd_,
                                value.bytes.data() + total,
                                value.bytes.size() - total,
                                static_cast<off_t>(entry.value_offset + total));
            } while (nread < 0 && errno == EINTR);
            if (nread < 0) {
                throw io_error("pread value", db_path_);
            }
            if (nread == 0) {
                throw KVStoreError("Value backing is truncated in " + db_path_);
            }
            total += static_cast<size_t>(nread);
        }
        if (crc32c(value.bytes.data(), value.bytes.size()) != entry.value_checksum) {
            throw KVStoreError("Value checksum mismatch while reading " + db_path_);
        }
        return value;
    }

    Value read_entry_without_cache(const Entry& entry) {
        if (entry.cached_value) {
            value_cache_hits_.fetch_add(1, std::memory_order_relaxed);
            return *entry.cached_value;
        }
        value_cache_misses_.fetch_add(1, std::memory_order_relaxed);
        return read_backing_value(entry);
    }

    void remove_cached_value_locked(Shard& shard, Entry& entry) {
        if (!entry.in_cache) {
            entry.cached_value.reset();
            return;
        }
        shard.cache_bytes = shard.cache_bytes > entry.cache_charge
                                ? shard.cache_bytes - entry.cache_charge
                                : 0;
        shard.cache_lru.erase(entry.cache_position);
        entry.in_cache = false;
        entry.cache_charge = 0;
        entry.cached_value.reset();
    }

    void cache_value_locked(Shard& shard,
                            Entry& entry,
                            Value value) {
        remove_cached_value_locked(shard, entry);
        const uint64_t shard_budget = options_.value_cache_bytes / shards_.size();
        constexpr uint64_t kApproximateCacheMetadataBytes = 96;
        const uint64_t cache_charge = value.bytes.size() + kApproximateCacheMetadataBytes;
        if (shard_budget == 0 || value.bytes.empty() || cache_charge > shard_budget) {
            return;
        }
        entry.cached_value = std::make_shared<const Value>(std::move(value));
        shard.cache_lru.push_front(&entry);
        entry.cache_position = shard.cache_lru.begin();
        entry.in_cache = true;
        entry.cache_charge = cache_charge;
        shard.cache_bytes += cache_charge;

        while (shard.cache_bytes > shard_budget && !shard.cache_lru.empty()) {
            Entry* victim = shard.cache_lru.back();
            remove_cached_value_locked(shard, *victim);
        }
    }

    Value read_entry_locked(Shard& shard,
                            Entry& entry,
                            bool touch_lru) {
        if (entry.cached_value) {
            value_cache_hits_.fetch_add(1, std::memory_order_relaxed);
            if (touch_lru && entry.in_cache) {
                shard.cache_lru.splice(shard.cache_lru.begin(), shard.cache_lru, entry.cache_position);
                entry.cache_position = shard.cache_lru.begin();
            }
            return *entry.cached_value;
        }
        value_cache_misses_.fetch_add(1, std::memory_order_relaxed);
        Value value = read_backing_value(entry);
        Value result = value;
        cache_value_locked(shard, entry, std::move(value));
        return result;
    }

    void open_and_recover() {
        const bool existed = std::filesystem::exists(db_path_);
        fd_ = open_or_throw(db_path_, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        try {
            lock_file_exclusively(fd_, db_path_);
            struct stat st {};
            if (::fstat(fd_, &st) != 0) {
                throw io_error("fstat", db_path_);
            }
            if (st.st_size == 0) {
                initialize_file(fd_, db_path_);
                maybe_trigger_failpoint("before_create_directory_sync");
                fsync_directory(db_path_);
                maybe_trigger_failpoint("after_create_directory_sync");
            }

            shards_.reserve(options_.shard_count);
            for (size_t index = 0; index < options_.shard_count; ++index) {
                shards_.push_back(std::make_unique<Shard>());
            }
            const RecoveryResult recovery = recover_file(
                fd_,
                db_path_,
                [this](const Mutation& operation, uint64_t lsn) { apply_recovered(operation, lsn); },
                true);
            superblock_ = recovery.superblock;
            append_offset_ = recovery.append_offset;
            last_lsn_ = recovery.last_lsn;
            wal_bytes_since_compaction_.store(
                append_offset_ - superblock_.journal_offset,
                std::memory_order_relaxed);
            live_wal_bytes_since_compaction_.store(
                append_offset_ - superblock_.journal_offset,
                std::memory_order_relaxed);
            if (recovery.truncated_tail) {
                fdatasync_or_throw(fd_, db_path_);
            }
            (void)existed;
        } catch (...) {
            close_if_open(fd_);
            fd_ = -1;
            throw;
        }
    }

    void apply_recovered(const Mutation& operation, uint64_t lsn) {
        Shard& shard = *shards_[ShardForKey(operation.key)];
        if (operation.type == MutationType::kPut) {
            Entry& entry = shard.values[operation.key];
            remove_cached_value_locked(shard, entry);
            entry = Entry {};
            entry.value_offset = operation.value_offset;
            entry.value_size = static_cast<uint32_t>(operation.value.bytes.size());
            entry.value_checksum = operation.value_checksum;
            entry.lsn = lsn;
            cache_value_locked(shard, entry, operation.value);
            if (is_string_key(operation.key)) {
                shard.ordered_string_keys.insert(operation.key);
            }
        } else {
            const auto found = shard.values.find(operation.key);
            if (found != shard.values.end()) {
                remove_cached_value_locked(shard, found->second);
                shard.values.erase(found);
            }
            if (is_string_key(operation.key)) {
                shard.ordered_string_keys.erase(operation.key);
            }
        }
    }

    void start_threads() {
        worker_pool_start_ = Clock::now();
        workers_.reserve(options_.worker_threads);
        for (size_t index = 0; index < options_.worker_threads; ++index) {
            workers_.emplace_back([this] { worker_loop(); });
        }
        coordinator_ = std::thread([this] { coordinator_loop(); });
        auto_compaction_thread_ = std::thread([this] { auto_compaction_loop(); });
        if (options_.durability == DurabilityMode::kPeriodic) {
            periodic_thread_ = std::thread([this] { periodic_loop(); });
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

    void submit_and_wait(const RequestPtr& request, bool allow_stopping) {
        if (!allow_stopping) {
            throw_if_fatal();
            if (stopping_.load(std::memory_order_acquire)) {
                throw KVStoreError("KVStore is shutting down");
            }
        }
        request->enqueue_time = Clock::now();
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
        {
            std::lock_guard<std::mutex> lock(request->completion_mutex);
            if (request->completed) {
                return;
            }
            request->error = std::move(error);
            request->conflict = conflict;
            request->completed = true;
        }
        if (request->kind == RequestKind::kWrite) {
            write_latency_histogram_[latency_bucket(elapsed_us(request->enqueue_time))].fetch_add(
                1,
                std::memory_order_relaxed);
        }
        request->completion_cv.notify_all();
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
                    request->payload = serialize_payload(request->operations);
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

    BatchPolicy current_batch_policy() {
        uint64_t queue_depth = 1;
        {
            std::lock_guard<std::mutex> lock(raw_mutex_);
            queue_depth += raw_queue_.size();
        }
        {
            std::lock_guard<std::mutex> lock(prepared_mutex_);
            queue_depth += prepared_.size();
        }
        atomic_max(recent_peak_queue_depth_, queue_depth);

        const uint64_t reads = read_requests_.load(std::memory_order_relaxed);
        const uint64_t writes = committed_write_requests_.load(std::memory_order_relaxed);
        const uint64_t operations = reads + writes;
        const uint64_t read_ratio = operations == 0 ? 0 : (reads * 1000) / operations;
        recent_read_ratio_per_1000_ops_.store(read_ratio, std::memory_order_relaxed);

        const uint64_t wal_bytes = wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t live_bytes = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
        const uint64_t obsolete_ratio =
            wal_bytes == 0 || live_bytes >= wal_bytes ? 0 : ((wal_bytes - live_bytes) * 100) / wal_bytes;

        std::array<uint64_t, kWriteLatencyBucketCount> histogram {};
        for (size_t index = 0; index < histogram.size(); ++index) {
            histogram[index] = write_latency_histogram_[index].load(std::memory_order_relaxed);
        }
        return compute_batch_policy(options_, WriterPolicySignals {
            queue_depth,
            std::max<uint64_t>(queue_depth, max_pending_queue_depth_.load(std::memory_order_relaxed)),
            read_ratio,
            obsolete_ratio,
            last_committed_batch_size_.load(std::memory_order_relaxed),
            last_committed_batch_wal_bytes_.load(std::memory_order_relaxed),
            percentile_from_histogram(histogram, 95),
            observed_fsync_pressure_per_1000_writes_.load(std::memory_order_relaxed),
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
            const BatchPolicy policy = current_batch_policy();
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
                process_group(batch, policy);
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

    void process_group(const std::vector<RequestPtr>& batch, const BatchPolicy& policy) {
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
            for (const auto& request : accepted) {
                if (last_lsn_ == std::numeric_limits<uint64_t>::max()) {
                    throw KVStoreError("Transaction LSN space is exhausted");
                }
                request->assigned_lsn = ++last_lsn_;
                request->frame = serialize_frame(
                    request->payload,
                    static_cast<uint32_t>(request->operations.size()),
                    request->assigned_lsn);
                bytes += request->frame.size();
            }
            write_frames(accepted);
            if (options_.durability == DurabilityMode::kSync) {
                maybe_trigger_failpoint("before_journal_sync");
                sync_file_locked();
                maybe_trigger_failpoint("after_wal_fsync_before_apply");
            }
            for (const auto& request : accepted) {
                apply_committed(request);
                account_live_records(request);
            }
            wal_bytes_since_compaction_.fetch_add(bytes, std::memory_order_relaxed);
            dirty_.store(options_.durability != DurabilityMode::kSync, std::memory_order_release);
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
            const auto& frame = requests.front()->frame;
            const size_t prefix_bytes = failpoint_is_configured("after_frame_header_write")
                                            ? sizeof(FrameHeader)
                                            : frame.size() - sizeof(FrameFooter);
            pwrite_buffer(fd_, append_offset_, frame.data(), prefix_bytes, db_path_);
            maybe_trigger_failpoint("after_frame_header_write");
            maybe_trigger_failpoint("after_frame_payload_write");
        }

        std::vector<iovec> vectors;
        vectors.reserve(requests.size());
        uint64_t total = 0;
        uint64_t next_frame_offset = append_offset_;
        const uint64_t max_file_offset = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
        for (const auto& request : requests) {
            const uint64_t frame_bytes = request->frame.size();
            if (next_frame_offset > max_file_offset ||
                frame_bytes > max_file_offset - next_frame_offset ||
                frame_bytes > std::numeric_limits<uint64_t>::max() - total) {
                throw KVStoreError("Journal exceeds the platform file size limit");
            }
            request->frame_offset = next_frame_offset;
            iovec vector {};
            vector.iov_base = request->frame.data();
            vector.iov_len = request->frame.size();
            vectors.push_back(vector);
            total += frame_bytes;
            next_frame_offset += frame_bytes;
        }

        size_t first = 0;
        uint64_t offset = append_offset_;
        while (first < vectors.size()) {
            const int count = static_cast<int>(std::min<size_t>(vectors.size() - first, IOV_MAX));
            ssize_t written;
            do {
                written = ::pwritev(fd_, vectors.data() + first, count, static_cast<off_t>(offset));
            } while (written < 0 && errno == EINTR);
            if (written < 0) {
                throw io_error("pwritev", db_path_);
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
            if (operation.type == MutationType::kPut) {
                Entry& entry = shard.values[operation.key];
                remove_cached_value_locked(shard, entry);
                entry = Entry {};
                entry.value_offset = request->frame_offset + sizeof(FrameHeader) +
                                     payload_cursor + sizeof(MutationHeader) + operation.key.size();
                entry.value_size = static_cast<uint32_t>(operation.value.bytes.size());
                entry.value_checksum = crc32c(operation.value.bytes.data(), operation.value.bytes.size());
                entry.lsn = request->assigned_lsn;
                cache_value_locked(shard, entry, operation.value);
                if (is_string_key(operation.key)) {
                    shard.ordered_string_keys.insert(operation.key);
                }
            } else {
                const auto found = shard.values.find(operation.key);
                if (found != shard.values.end()) {
                    remove_cached_value_locked(shard, found->second);
                    shard.values.erase(found);
                }
                if (is_string_key(operation.key)) {
                    shard.ordered_string_keys.erase(operation.key);
                }
            }
            payload_cursor += sizeof(MutationHeader) + operation.key.size() + operation.value.bytes.size();
        }
        for (size_t shard_id : shard_ids) {
            shards_[shard_id]->version.fetch_add(1, std::memory_order_release);
        }
    }

    void account_live_records(const RequestPtr& request) {
        for (const auto& operation : request->operations) {
            const uint64_t current_bytes = sizeof(MutationHeader) +
                                           operation.key.size() +
                                           operation.value.bytes.size();
            const auto found = latest_wal_record_bytes_.find(operation.key);
            if (found != latest_wal_record_bytes_.end()) {
                const uint64_t live = live_wal_bytes_since_compaction_.load(std::memory_order_relaxed);
                live_wal_bytes_since_compaction_.store(
                    live > found->second ? live - found->second : 0,
                    std::memory_order_relaxed);
            }
            latest_wal_record_bytes_[operation.key] = current_bytes;
            live_wal_bytes_since_compaction_.fetch_add(current_bytes, std::memory_order_relaxed);
        }
    }

    void sync_file_locked() {
        const auto start = Clock::now();
        fdatasync_or_throw(fd_, db_path_);
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
                                     obsolete_ratio >= options_.auto_compact_invalid_wal_ratio_percent;
        if (!byte_threshold && !ratio_threshold) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(auto_compaction_mutex_);
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
            } catch (const std::exception& error) {
                set_fatal(error.what());
            }
        }
    }

    void compact_impl(bool automatic) {
        throw_if_fatal();
        if (!automatic) {
            compact_requests_.fetch_add(1, std::memory_order_relaxed);
        }
        std::unique_lock<std::mutex> compaction_lock(compaction_mutex_);

        uint64_t start_offset;
        uint64_t start_lsn;
        uint64_t generation;
        {
            std::lock_guard<std::mutex> lock(commit_mutex_);
            start_offset = append_offset_;
            start_lsn = last_lsn_;
            generation = superblock_.generation + 1;
        }

        std::vector<std::unordered_map<std::string, CheckpointReference>> checkpoint(shards_.size());
        uint64_t checkpoint_entries = 0;
        uint64_t index_entries_bytes = 0;
        uint64_t object_length = 0;
        for (size_t shard_id = 0; shard_id < shards_.size(); ++shard_id) {
            const auto& shard_pointer = shards_[shard_id];
            std::shared_lock<std::shared_mutex> lock(shard_pointer->mutex);
            auto& references = checkpoint[shard_id];
            references.reserve(shard_pointer->values.size());
            for (const auto& [key, entry] : shard_pointer->values) {
                const uint64_t index_addition = sizeof(IndexEntryHeader) + key.size();
                if (checkpoint_entries == std::numeric_limits<uint64_t>::max() ||
                    index_entries_bytes > std::numeric_limits<uint64_t>::max() - index_addition ||
                    object_length > std::numeric_limits<uint64_t>::max() - entry.value_size) {
                    throw KVStoreError("Checkpoint size overflow during compaction");
                }
                CheckpointReference reference;
                reference.source_value_offset = entry.value_offset;
                reference.checkpoint_value_offset = object_length;
                reference.lsn = entry.lsn;
                reference.value_size = entry.value_size;
                reference.value_checksum = entry.value_checksum;
                references.emplace(key, reference);
                ++checkpoint_entries;
                index_entries_bytes += index_addition;
                object_length += entry.value_size;
            }
        }

        if (index_entries_bytes > std::numeric_limits<uint64_t>::max() - sizeof(IndexHeader)) {
            throw KVStoreError("Checkpoint index exceeds implementation limits");
        }
        const uint64_t index_offset = kDataOffset;
        const uint64_t index_length = sizeof(IndexHeader) + index_entries_bytes;
        if (index_length > kMaxTransactionBytes * 16ULL) {
            throw KVStoreError("Checkpoint index exceeds implementation limits");
        }
        if (index_offset > std::numeric_limits<uint64_t>::max() - index_length) {
            throw KVStoreError("Checkpoint index offset overflow");
        }
        const uint64_t object_offset = index_offset + index_length;
        if (object_offset > std::numeric_limits<uint64_t>::max() - object_length) {
            throw KVStoreError("Checkpoint object offset overflow");
        }
        const uint64_t journal_offset = object_offset + object_length;
        if (journal_offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
            throw KVStoreError("Checkpoint exceeds the platform file size limit");
        }

        const uint64_t temp_id = compact_temp_sequence_.fetch_add(1, std::memory_order_relaxed);
        const std::string temp_path = db_path_ + ".compact." + std::to_string(::getpid()) + "." + std::to_string(temp_id);
        int temp_fd = -1;
        try {
            temp_fd = open_or_throw(temp_path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
            lock_file_exclusively(temp_fd, temp_path);
            if (::ftruncate(temp_fd, static_cast<off_t>(journal_offset)) != 0) {
                throw io_error("ftruncate", temp_path);
            }

            uint64_t index_cursor = index_offset + sizeof(IndexHeader);
            uint32_t index_entries_checksum = 0;
            for (const auto& shard_checkpoint : checkpoint) {
                for (const auto& [key, reference] : shard_checkpoint) {
                    const IndexEntryHeader header = make_index_entry_header(
                        key,
                        reference.checkpoint_value_offset,
                        reference.value_size,
                        reference.value_checksum);
                    pwrite_buffer(temp_fd, index_cursor, &header, sizeof(header), temp_path);
                    index_entries_checksum = crc32c_extend(
                        index_entries_checksum, &header, sizeof(header));
                    index_cursor += sizeof(header);
                    pwrite_buffer(temp_fd, index_cursor, key.data(), key.size(), temp_path);
                    index_entries_checksum = crc32c_extend(
                        index_entries_checksum, key.data(), key.size());
                    index_cursor += key.size();

                    const uint32_t copied_checksum = pread_copy(
                        fd_,
                        reference.source_value_offset,
                        temp_fd,
                        object_offset + reference.checkpoint_value_offset,
                        reference.value_size,
                        db_path_,
                        temp_path);
                    if (copied_checksum != reference.value_checksum) {
                        throw KVStoreError("Value checksum mismatch while streaming compaction from " + db_path_);
                    }
                }
            }
            if (index_cursor != index_offset + index_length) {
                throw KVStoreError("Checkpoint index size changed during compaction");
            }
            const IndexHeader index_header = make_index_header(
                checkpoint_entries, index_entries_bytes, index_entries_checksum);
            pwrite_buffer(temp_fd, index_offset, &index_header, sizeof(index_header), temp_path);

            uint32_t checkpoint_checksum = crc_fd_region(
                temp_fd, index_offset, index_length, 0, temp_path);
            checkpoint_checksum = crc_fd_region(
                temp_fd, object_offset, object_length, checkpoint_checksum, temp_path);
            const Superblock compacted_superblock = make_superblock(
                generation,
                start_lsn,
                index_offset,
                index_length,
                object_offset,
                object_length,
                journal_offset,
                checkpoint_checksum);
            write_superblocks(temp_fd, compacted_superblock, temp_path);
            maybe_trigger_failpoint("after_checkpoint_write_before_sync");

            const auto pause_start = Clock::now();
            std::unique_lock<std::mutex> commit_lock(commit_mutex_);
            const uint64_t end_offset = append_offset_;
            if (end_offset < start_offset) {
                throw KVStoreError("Journal offset moved backwards during compaction");
            }
            const uint64_t delta_bytes = end_offset - start_offset;
            if (delta_bytes > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) -
                                  compacted_superblock.journal_offset) {
                throw KVStoreError("Compacted journal exceeds the platform file size limit");
            }
            (void)pread_copy(fd_,
                             start_offset,
                             temp_fd,
                             compacted_superblock.journal_offset,
                             delta_bytes,
                             db_path_,
                             temp_path);
            if (::ftruncate(temp_fd,
                            static_cast<off_t>(compacted_superblock.journal_offset + delta_bytes)) != 0) {
                throw io_error("ftruncate", temp_path);
            }
            maybe_trigger_failpoint("before_compaction_temp_sync");
            fdatasync_or_throw(temp_fd, temp_path);
            maybe_trigger_failpoint("after_compaction_temp_sync_before_rename");
            maybe_trigger_failpoint("before_snapshot_rename");
            maybe_trigger_failpoint("after_snapshot_fsync_before_rename");
            std::vector<std::unique_lock<std::shared_mutex>> state_locks;
            state_locks.reserve(shards_.size());
            for (auto& shard : shards_) {
                state_locks.emplace_back(shard->mutex);
            }
            for (size_t shard_id = 0; shard_id < shards_.size(); ++shard_id) {
                for (const auto& [key, entry] : shards_[shard_id]->values) {
                    if (entry.lsn > start_lsn) {
                        if (entry.value_offset < start_offset ||
                            entry.value_offset > end_offset ||
                            entry.value_size > end_offset - entry.value_offset) {
                            throw KVStoreError("Compaction found a post-cut value outside the journal delta");
                        }
                        continue;
                    }
                    const auto found = checkpoint[shard_id].find(key);
                    if (found == checkpoint[shard_id].end() ||
                        found->second.lsn != entry.lsn ||
                        found->second.value_size != entry.value_size ||
                        found->second.value_checksum != entry.value_checksum) {
                        throw KVStoreError("Compaction checkpoint omitted or changed a live value");
                    }
                }
            }
            if (::rename(temp_path.c_str(), db_path_.c_str()) != 0) {
                throw io_error("rename compacted container", db_path_);
            }
            const int old_fd = fd_;
            fd_ = temp_fd;
            temp_fd = -1;
            superblock_ = compacted_superblock;
            append_offset_ = compacted_superblock.journal_offset + delta_bytes;
            for (size_t shard_id = 0; shard_id < shards_.size(); ++shard_id) {
                for (auto& [key, entry] : shards_[shard_id]->values) {
                    if (entry.lsn > start_lsn) {
                        entry.value_offset = compacted_superblock.journal_offset +
                                             (entry.value_offset - start_offset);
                    } else {
                        const auto checkpoint_entry = checkpoint[shard_id].find(key);
                        entry.value_offset = compacted_superblock.object_offset +
                                             checkpoint_entry->second.checkpoint_value_offset;
                    }
                }
            }
            close_if_open(old_fd);
            maybe_trigger_failpoint("after_snapshot_rename");
            maybe_trigger_failpoint("after_snapshot_rename_before_wal_reset");
            maybe_trigger_failpoint("after_wal_rotation_before_reopen");
            maybe_trigger_failpoint("after_compaction_rename_before_directory_sync");
            maybe_trigger_failpoint("before_compaction_directory_sync");
            fsync_directory(db_path_);
            maybe_trigger_failpoint("after_compaction_directory_sync");
            state_locks.clear();
            dirty_.store(false, std::memory_order_release);
            const uint64_t pause = elapsed_us(pause_start);
            compaction_pause_time_us_.fetch_add(pause, std::memory_order_relaxed);
            atomic_max(max_compaction_pause_time_us_, pause);
            const uint64_t reclaimed = wal_bytes_since_compaction_.exchange(
                delta_bytes,
                std::memory_order_relaxed);
            live_wal_bytes_since_compaction_.store(delta_bytes, std::memory_order_relaxed);
            latest_wal_record_bytes_.clear();
            commit_lock.unlock();

            total_snapshot_bytes_written_.fetch_add(
                index_length + object_length,
                std::memory_order_relaxed);
            total_wal_bytes_reclaimed_by_compaction_.fetch_add(
                reclaimed > delta_bytes ? reclaimed - delta_bytes : 0,
                std::memory_order_relaxed);
            if (automatic) {
                auto_compactions_completed_.fetch_add(1, std::memory_order_relaxed);
            } else {
                manual_compactions_completed_.fetch_add(1, std::memory_order_relaxed);
            }
        } catch (...) {
            close_if_open(temp_fd);
            ::unlink(temp_path.c_str());
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
