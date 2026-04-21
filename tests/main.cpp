#include "kvstore.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>
#include <sys/wait.h>

namespace {

#pragma pack(push, 1)
struct SnapshotHeader {
    char magic[8];
    uint32_t version;
    uint32_t reserved;
};

struct SnapshotEntryHeaderV1 {
    uint32_t magic;
    int32_t key;
    uint32_t value_size;
};

struct SnapshotEntryHeader {
    uint32_t magic;
    uint32_t key_size;
    uint32_t value_size;
};

struct WalRecordHeader {
    uint32_t magic;
    uint16_t version;
    uint8_t type;
    uint8_t reserved;
    uint32_t key_size;
    uint32_t value_size;
    uint32_t checksum;
};

struct WalRecordHeaderV1 {
    uint32_t magic;
    uint16_t version;
    uint8_t type;
    uint8_t reserved;
    int32_t key;
    uint32_t value_size;
    uint32_t checksum;
};
#pragma pack(pop)

constexpr char kSnapshotMagic[8] = {'K', 'V', 'S', 'N', 'A', 'P', '0', '1'};
constexpr uint32_t kWalMagic = 0x4B565741;
constexpr uint32_t kSnapshotEntryMagic = 0x4B565345;
constexpr char kIntKeyTag = '\x01';
constexpr char kStringKeyTag = '\x02';
constexpr char kBinaryKeyTag = '\x03';

struct FormatKeyStats {
    uint64_t total = 0;
    uint64_t int_keys = 0;
    uint64_t string_keys = 0;
    uint64_t binary_keys = 0;
    uint64_t unknown_keys = 0;
};

int run_inspect_format(const std::string& db_path);
int run_verify_format(const std::string& db_path);
double extract_json_number(const std::string& json, const std::string& key);
int run_benchmark_trend(const std::string& directory_path);

Value text(const std::string& input) {
    return Value(std::vector<uint8_t>(input.begin(), input.end()));
}

std::string as_string(const Value& value) {
    return std::string(value.bytes.begin(), value.bytes.end());
}

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

class TestDir {
public:
    explicit TestDir(const std::string& name) {
        static std::atomic<uint64_t> counter {0};
        const uint64_t suffix = counter.fetch_add(1, std::memory_order_relaxed);
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        path_ = std::filesystem::temp_directory_path() /
                ("kvstore_" + name + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(now_ns) + "_" + std::to_string(suffix));
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }

    ~TestDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    std::string file(const std::string& name) const {
        return (path_ / name).string();
    }

private:
    std::filesystem::path path_;
};

void append_bytes(const std::string& path, std::initializer_list<uint8_t> data) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    for (uint8_t byte : data) {
        out.put(static_cast<char>(byte));
    }
}

void flip_byte(const std::string& path, std::streamoff offset) {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    file.seekg(offset);
    const int original = file.get();
    require(original != EOF, "Expected byte to corrupt");
    file.seekp(offset);
    file.put(static_cast<char>(original ^ 0xFF));
}

void write_u32_at(const std::string& path, std::streamoff offset, uint32_t value) {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    require(file.is_open(), "Expected file to exist for overwrite");
    file.seekp(offset);
    file.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

void write_text_file(const std::string& path, const std::string& contents) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    require(out.is_open(), "Expected text file to be writable");
    out << contents;
}

std::string read_text_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    require(in.is_open(), "Expected text file to be readable");
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

uintmax_t file_size_or_zero(const std::string& path) {
    std::error_code ec;
    const auto size = std::filesystem::file_size(path, ec);
    return ec ? 0 : size;
}

void classify_encoded_key(std::string_view encoded_key, FormatKeyStats& stats) {
    stats.total += 1;
    if (encoded_key.empty()) {
        stats.unknown_keys += 1;
        return;
    }

    switch (encoded_key.front()) {
        case kIntKeyTag:
            stats.int_keys += 1;
            break;
        case kStringKeyTag:
            stats.string_keys += 1;
            break;
        case kBinaryKeyTag:
            stats.binary_keys += 1;
            break;
        default:
            stats.unknown_keys += 1;
            break;
    }
}

std::map<std::string, std::string> parse_kv_line(const std::string& line) {
    std::map<std::string, std::string> values;
    std::istringstream in(line);
    std::string token;
    while (in >> token) {
        const size_t equals = token.find('=');
        require(equals != std::string::npos, "Expected key=value token in inspect output");
        values[token.substr(0, equals)] = token.substr(equals + 1);
    }
    return values;
}

std::map<std::string, std::string> capture_inspect_format(const std::string& db_path, int expected_status = 0) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_inspect_format(db_path);
    std::cout.rdbuf(original);
    require(status == expected_status, "inspect-format should return the expected status");
    return parse_kv_line(out.str());
}

std::pair<int, std::string> capture_verify_format(const std::string& db_path) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_verify_format(db_path);
    std::cout.rdbuf(original);
    return {status, out.str()};
}

std::pair<int, std::map<std::string, std::string>> capture_benchmark_trend(const std::string& directory_path) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_benchmark_trend(directory_path);
    std::cout.rdbuf(original);
    return {status, parse_kv_line(out.str())};
}

struct BenchmarkConfig {
    std::string label;
    int writer_count = 8;
    int reader_count = 4;
    int duration_ms = 3000;
    int key_space = 50000;
};

struct BenchmarkResult {
    BenchmarkConfig config;
    KVStoreOptions options;
    KVStoreMetrics metrics;
    double duration_s = 0.0;
    uint64_t writes = 0;
    uint64_t reads = 0;
    double write_ops_per_s = 0.0;
    double read_ops_per_s = 0.0;
    double avg_write_latency_us = 0.0;
};

struct BaselineSummary {
    std::string file_name;
    double write_ops_per_s = 0.0;
    double read_ops_per_s = 0.0;
    double avg_write_latency_us = 0.0;
};

BenchmarkConfig make_benchmark_config(
    std::string label,
    int writer_count,
    int reader_count,
    int duration_ms,
    int key_space) {
    BenchmarkConfig config;
    config.label = std::move(label);
    config.writer_count = writer_count;
    config.reader_count = reader_count;
    config.duration_ms = duration_ms;
    config.key_space = key_space;
    return config;
}

uint64_t histogram_total(const std::array<uint64_t, kWriteLatencyBucketCount>& histogram) {
    uint64_t total = 0;
    for (uint64_t value : histogram) {
        total += value;
    }
    return total;
}

uint32_t fnv1a_append(uint32_t seed, const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t hash = seed;
    for (size_t i = 0; i < size; ++i) {
        hash ^= bytes[i];
        hash *= 16777619u;
    }
    return hash;
}

uint32_t legacy_checksum(uint8_t type, int32_t key, const Value& value) {
    uint32_t hash = 2166136261u;
    const uint32_t value_size = static_cast<uint32_t>(value.bytes.size());
    hash = fnv1a_append(hash, &type, sizeof(type));
    hash = fnv1a_append(hash, &key, sizeof(key));
    hash = fnv1a_append(hash, &value_size, sizeof(value_size));
    if (!value.bytes.empty()) {
        hash = fnv1a_append(hash, value.bytes.data(), value.bytes.size());
    }
    return hash;
}

void write_legacy_snapshot_v1(const std::string& path, const std::vector<std::pair<int32_t, Value>>& entries) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    SnapshotHeader header {};
    std::memcpy(header.magic, kSnapshotMagic, sizeof(kSnapshotMagic));
    header.version = 1;
    header.reserved = 0;
    out.write(reinterpret_cast<const char*>(&header), sizeof(header));
    for (const auto& [key, value] : entries) {
        const SnapshotEntryHeaderV1 entry {
            kSnapshotEntryMagic,
            key,
            static_cast<uint32_t>(value.bytes.size()),
        };
        out.write(reinterpret_cast<const char*>(&entry), sizeof(entry));
        if (!value.bytes.empty()) {
            out.write(reinterpret_cast<const char*>(value.bytes.data()), static_cast<std::streamsize>(value.bytes.size()));
        }
    }
}

void append_legacy_wal_record_v1(
    const std::string& path,
    uint8_t type,
    int32_t key,
    const Value& value) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    const WalRecordHeaderV1 header {
        kWalMagic,
        1,
        type,
        0,
        key,
        static_cast<uint32_t>(value.bytes.size()),
        legacy_checksum(type, key, value),
    };
    out.write(reinterpret_cast<const char*>(&header), sizeof(header));
    if (!value.bytes.empty()) {
        out.write(reinterpret_cast<const char*>(value.bytes.data()), static_cast<std::streamsize>(value.bytes.size()));
    }
}

void wait_for_start(std::atomic<int>& ready, std::atomic<bool>& start_signal, int target_count) {
    ready.fetch_add(1, std::memory_order_relaxed);
    while (ready.load(std::memory_order_acquire) < target_count) {
        std::this_thread::yield();
    }
    while (!start_signal.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

template <typename Predicate>
void wait_until(Predicate predicate, const std::string& failure_message, int timeout_ms = 2000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!predicate()) {
        throw std::runtime_error(failure_message);
    }
}

std::string g_program_path;

void run_failpoint_child(const std::string& scenario, const std::string& db_path) {
    pid_t child = ::fork();
    require(child >= 0, "fork should succeed");
    if (child == 0) {
        ::execl(
            g_program_path.c_str(),
            g_program_path.c_str(),
            "fault-inject",
            scenario.c_str(),
            db_path.c_str(),
            static_cast<char*>(nullptr));
        ::_exit(127);
    }

    int status = 0;
    require(::waitpid(child, &status, 0) == child, "waitpid should return the child pid");
    require(WIFEXITED(status), "fault-injection child should exit normally");
    require(WEXITSTATUS(status) == 86, "fault-injection child should terminate at the configured failpoint");
}

int run_fault_injection_scenario(const std::string& scenario, const std::string& db_path) {
    ::setenv("KVSTORE_FAIL_ACTION", "crash", 1);

    if (scenario == "wal_after_fsync") {
        KVStore store(db_path);
        store.Put(1, text("stable"));
        ::setenv("KVSTORE_FAILPOINT", "after_wal_fsync_before_apply", 1);
        store.Put(2, text("latest"));
        return 2;
    }

    if (scenario == "wal_after_fsync_batch") {
        KVStore store(db_path);
        store.Put(1, text("stable"));
        ::setenv("KVSTORE_FAILPOINT", "after_wal_fsync_before_apply", 1);
        store.WriteBatch({
            BatchWriteOperation::PutInt(2, text("latest")),
            BatchWriteOperation::Put("alpha", text("batch-value")),
            BatchWriteOperation::DeleteInt(1),
        });
        return 6;
    }

    if (scenario == "snapshot_after_rename") {
        KVStore store(db_path);
        store.Put(1, text("one"));
        store.Put(2, text("two"));
        ::setenv("KVSTORE_FAILPOINT", "after_snapshot_rename_before_wal_reset", 1);
        store.Compact();
        return 3;
    }

    if (scenario == "wal_rotation_before_reopen") {
        KVStore store(db_path);
        store.Put(10, text("ten"));
        store.Put(20, text("twenty"));
        ::setenv("KVSTORE_FAILPOINT", "after_wal_rotation_before_reopen", 1);
        store.Compact();
        return 4;
    }

    if (scenario == "snapshot_before_rename") {
        KVStore store(db_path);
        store.Put(7, text("seven"));
        store.Put(8, text("eight"));
        ::setenv("KVSTORE_FAILPOINT", "after_snapshot_fsync_before_rename", 1);
        store.Compact();
        return 5;
    }

    std::cerr << "Unknown fault-injection scenario: " << scenario << '\n';
    return 1;
}

int run_inspect_format(const std::string& db_path) {
    const std::string wal_path = db_path + ".wal";
    std::ifstream snapshot(db_path, std::ios::binary);
    if (!snapshot.is_open()) {
        std::cerr << "snapshot_exists=0" << std::endl;
        return 1;
    }

    SnapshotHeader snapshot_header {};
    snapshot.read(reinterpret_cast<char*>(&snapshot_header), sizeof(snapshot_header));
    if (snapshot.gcount() != static_cast<std::streamsize>(sizeof(snapshot_header))) {
        std::cerr << "snapshot_exists=1 snapshot_truncated=1" << std::endl;
        return 2;
    }

    FormatKeyStats snapshot_stats;
    bool snapshot_truncated = false;
    bool snapshot_entry_magic_ok = true;
    if (snapshot_header.version == 1) {
        while (true) {
            SnapshotEntryHeaderV1 entry {};
            snapshot.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            if (snapshot.gcount() == 0) {
                break;
            }
            if (snapshot.gcount() != static_cast<std::streamsize>(sizeof(entry))) {
                snapshot_truncated = true;
                break;
            }
            if (entry.magic != kSnapshotEntryMagic) {
                snapshot_entry_magic_ok = false;
                break;
            }
            snapshot_stats.total += 1;
            snapshot_stats.int_keys += 1;
            snapshot.seekg(entry.value_size, std::ios::cur);
            if (!snapshot.good()) {
                snapshot_truncated = true;
                break;
            }
        }
    } else if (snapshot_header.version == 2) {
        while (true) {
            SnapshotEntryHeader entry {};
            snapshot.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            if (snapshot.gcount() == 0) {
                break;
            }
            if (snapshot.gcount() != static_cast<std::streamsize>(sizeof(entry))) {
                snapshot_truncated = true;
                break;
            }
            if (entry.magic != kSnapshotEntryMagic) {
                snapshot_entry_magic_ok = false;
                break;
            }
            std::string key(entry.key_size, '\0');
            if (!key.empty()) {
                snapshot.read(key.data(), static_cast<std::streamsize>(key.size()));
                if (snapshot.gcount() != static_cast<std::streamsize>(key.size())) {
                    snapshot_truncated = true;
                    break;
                }
            }
            classify_encoded_key(key, snapshot_stats);
            snapshot.seekg(entry.value_size, std::ios::cur);
            if (!snapshot.good()) {
                snapshot_truncated = true;
                break;
            }
        }
    }

    bool rewrite_recommended = snapshot_header.version != 2;
    std::cout << "snapshot_exists=1"
              << " snapshot_magic_ok=" << (std::memcmp(snapshot_header.magic, kSnapshotMagic, sizeof(kSnapshotMagic)) == 0 ? 1 : 0)
              << " snapshot_version=" << snapshot_header.version
              << " snapshot_entries=" << snapshot_stats.total
              << " snapshot_int_keys=" << snapshot_stats.int_keys
              << " snapshot_string_keys=" << snapshot_stats.string_keys
              << " snapshot_binary_keys=" << snapshot_stats.binary_keys
              << " snapshot_unknown_key_types=" << snapshot_stats.unknown_keys
              << " snapshot_entry_magic_ok=" << (snapshot_entry_magic_ok ? 1 : 0)
              << " snapshot_truncated=" << (snapshot_truncated ? 1 : 0)
              << " snapshot_size=" << file_size_or_zero(db_path);

    std::ifstream wal(wal_path, std::ios::binary);
    if (!wal.is_open()) {
        std::cout << " wal_exists=0"
                  << " wal_records=0"
                  << " wal_put_records=0"
                  << " wal_delete_records=0"
                  << " wal_int_keys=0"
                  << " wal_string_keys=0"
                  << " wal_binary_keys=0"
                  << " wal_unknown_key_types=0"
                  << " wal_unknown_record_types=0"
                  << " wal_truncated=0"
                  << " rewrite_recommended=" << (rewrite_recommended ? 1 : 0)
                  << std::endl;
        return 0;
    }

    WalRecordHeader wal_header {};
    wal.read(reinterpret_cast<char*>(&wal_header), sizeof(wal_header));
    if (wal.gcount() == 0) {
        std::cout << " wal_exists=1 wal_empty=1 wal_size=" << file_size_or_zero(wal_path)
                  << " wal_records=0"
                  << " wal_put_records=0"
                  << " wal_delete_records=0"
                  << " wal_int_keys=0"
                  << " wal_string_keys=0"
                  << " wal_binary_keys=0"
                  << " wal_unknown_key_types=0"
                  << " wal_unknown_record_types=0"
                  << " wal_truncated=0"
                  << " rewrite_recommended=" << (rewrite_recommended ? 1 : 0)
                  << std::endl;
        return 0;
    }
    if (wal.gcount() != static_cast<std::streamsize>(sizeof(wal_header))) {
        std::cout << " wal_exists=1 wal_truncated=1 wal_size=" << file_size_or_zero(wal_path)
                  << " wal_records=0"
                  << " wal_put_records=0"
                  << " wal_delete_records=0"
                  << " wal_int_keys=0"
                  << " wal_string_keys=0"
                  << " wal_binary_keys=0"
                  << " wal_unknown_key_types=0"
                  << " wal_unknown_record_types=0"
                  << " rewrite_recommended=1"
                  << std::endl;
        return 0;
    }

    FormatKeyStats wal_stats;
    uint64_t wal_put_records = 0;
    uint64_t wal_delete_records = 0;
    uint64_t wal_unknown_record_types = 0;
    bool wal_truncated = false;
    const uint16_t wal_version = wal_header.version;
    rewrite_recommended = rewrite_recommended || wal_version != 2;
    wal.clear();
    wal.seekg(0, std::ios::beg);
    if (wal_version == 1) {
        while (true) {
            WalRecordHeaderV1 entry {};
            wal.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            if (wal.gcount() == 0) {
                break;
            }
            if (wal.gcount() != static_cast<std::streamsize>(sizeof(entry))) {
                wal_truncated = true;
                break;
            }
            if (entry.type == 1) {
                wal_put_records += 1;
            } else if (entry.type == 2) {
                wal_delete_records += 1;
            } else {
                wal_unknown_record_types += 1;
            }
            wal_stats.total += 1;
            wal_stats.int_keys += 1;
            wal.seekg(entry.value_size, std::ios::cur);
            if (!wal.good()) {
                wal_truncated = true;
                break;
            }
        }
    } else {
        while (true) {
            WalRecordHeader entry {};
            wal.read(reinterpret_cast<char*>(&entry), sizeof(entry));
            if (wal.gcount() == 0) {
                break;
            }
            if (wal.gcount() != static_cast<std::streamsize>(sizeof(entry))) {
                wal_truncated = true;
                break;
            }
            if (entry.type == 1) {
                wal_put_records += 1;
            } else if (entry.type == 2) {
                wal_delete_records += 1;
            } else {
                wal_unknown_record_types += 1;
            }
            std::string key(entry.key_size, '\0');
            if (!key.empty()) {
                wal.read(key.data(), static_cast<std::streamsize>(key.size()));
                if (wal.gcount() != static_cast<std::streamsize>(key.size())) {
                    wal_truncated = true;
                    break;
                }
            }
            classify_encoded_key(key, wal_stats);
            wal.seekg(entry.value_size, std::ios::cur);
            if (!wal.good()) {
                wal_truncated = true;
                break;
            }
        }
    }

    std::cout << " wal_exists=1"
              << " wal_empty=0"
              << " wal_magic=0x" << std::hex << wal_header.magic << std::dec
              << " wal_version=" << wal_header.version
              << " wal_first_type=" << static_cast<int>(wal_header.type)
              << " wal_size=" << file_size_or_zero(wal_path)
              << " wal_records=" << wal_stats.total
              << " wal_put_records=" << wal_put_records
              << " wal_delete_records=" << wal_delete_records
              << " wal_int_keys=" << wal_stats.int_keys
              << " wal_string_keys=" << wal_stats.string_keys
              << " wal_binary_keys=" << wal_stats.binary_keys
              << " wal_unknown_key_types=" << wal_stats.unknown_keys
              << " wal_unknown_record_types=" << wal_unknown_record_types
              << " wal_truncated=" << (wal_truncated ? 1 : 0)
              << " rewrite_recommended=" << (rewrite_recommended ? 1 : 0)
              << std::endl;
    return 0;
}

int run_rewrite_format(const std::string& db_path) {
    KVStore store(db_path);
    store.Compact();
    return 0;
}

int run_verify_format(const std::string& db_path) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int inspect_status = run_inspect_format(db_path);
    std::cout.rdbuf(original);

    if (inspect_status != 0) {
        std::cout << out.str();
        return inspect_status;
    }

    const auto values = parse_kv_line(out.str());
    const bool rewrite_recommended = values.count("rewrite_recommended") != 0 && values.at("rewrite_recommended") == "1";
    const bool snapshot_magic_ok = values.count("snapshot_magic_ok") == 0 || values.at("snapshot_magic_ok") == "1";
    const bool snapshot_entry_magic_ok =
        values.count("snapshot_entry_magic_ok") == 0 || values.at("snapshot_entry_magic_ok") == "1";
    const bool snapshot_truncated = values.count("snapshot_truncated") != 0 && values.at("snapshot_truncated") == "1";
    const bool wal_truncated = values.count("wal_truncated") != 0 && values.at("wal_truncated") == "1";

    std::cout << out.str();
    if (!snapshot_magic_ok || !snapshot_entry_magic_ok || snapshot_truncated || wal_truncated || rewrite_recommended) {
        return 2;
    }
    return 0;
}

int run_compatibility_matrix() {
    TestDir dir("compatibility_matrix");
    const auto value_or = [](const std::map<std::string, std::string>& values,
                             const std::string& key,
                             std::string fallback) {
        const auto it = values.find(key);
        return it == values.end() ? fallback : it->second;
    };

    const std::string current_db_path = dir.file("current_v2.dat");
    {
        KVStore store(current_db_path);
        store.Put(1, text("one"));
        store.Put(std::string("alpha"), text("two"));
        store.Put(std::vector<uint8_t> {0x01, 0x02}, text("three"));
        store.Compact();
    }

    const auto current_inspect = capture_inspect_format(current_db_path);
    const auto [current_verify_status, current_verify_output] = capture_verify_format(current_db_path);
    require(current_verify_status == 0, "current compatibility matrix case should verify cleanly");
    require(current_inspect.at("snapshot_version") == "2", "current case should report snapshot v2");
    require(current_inspect.at("rewrite_recommended") == "0", "current case should not recommend rewrite");

    const std::string legacy_db_path = dir.file("legacy_v1.dat");
    const std::string legacy_wal_path = legacy_db_path + ".wal";
    write_legacy_snapshot_v1(legacy_db_path, {
        {7, text("seven")},
        {8, text("eight")},
    });
    append_legacy_wal_record_v1(legacy_wal_path, 1, 9, text("nine"));

    const auto legacy_inspect = capture_inspect_format(legacy_db_path);
    const auto [legacy_verify_status, legacy_verify_output] = capture_verify_format(legacy_db_path);
    require(legacy_verify_status == 2, "legacy compatibility matrix case should require rewrite");
    require(legacy_inspect.at("snapshot_version") == "1", "legacy case should report snapshot v1");
    require(legacy_inspect.at("rewrite_recommended") == "1", "legacy case should recommend rewrite");

    require(run_rewrite_format(legacy_db_path) == 0, "legacy compatibility matrix case should rewrite successfully");
    const auto rewritten_inspect = capture_inspect_format(legacy_db_path);
    const auto [rewritten_verify_status, rewritten_verify_output] = capture_verify_format(legacy_db_path);
    require(rewritten_verify_status == 0, "rewritten legacy case should verify cleanly");
    require(rewritten_inspect.at("snapshot_version") == "2", "rewritten legacy case should upgrade to snapshot v2");
    require(rewritten_inspect.at("rewrite_recommended") == "0", "rewritten legacy case should stop recommending rewrite");

    std::cout << "case=current_v2"
              << " snapshot_version=" << current_inspect.at("snapshot_version")
              << " wal_version=" << value_or(current_inspect, "wal_version", "empty")
              << " verify_status=" << current_verify_status
              << " rewrite_recommended=" << current_inspect.at("rewrite_recommended")
              << " current_verify_output_bytes=" << current_verify_output.size()
              << '\n';
    std::cout << "case=legacy_v1"
              << " snapshot_version=" << legacy_inspect.at("snapshot_version")
              << " wal_version=" << value_or(legacy_inspect, "wal_version", "missing")
              << " verify_status=" << legacy_verify_status
              << " rewrite_recommended=" << legacy_inspect.at("rewrite_recommended")
              << " legacy_verify_output_bytes=" << legacy_verify_output.size()
              << '\n';
    std::cout << "case=legacy_v1_after_rewrite"
              << " snapshot_version=" << rewritten_inspect.at("snapshot_version")
              << " wal_exists=" << rewritten_inspect.at("wal_exists")
              << " verify_status=" << rewritten_verify_status
              << " rewrite_recommended=" << rewritten_inspect.at("rewrite_recommended")
              << " rewritten_verify_output_bytes=" << rewritten_verify_output.size()
              << '\n';
    return 0;
}

int run_compare_benchmark_baseline(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_write_ratio_pct = 85.0,
    double min_read_ratio_pct = 85.0,
    double max_latency_ratio_pct = 125.0,
    double max_p95_latency_ratio_pct = 150.0,
    double max_p99_latency_ratio_pct = 175.0,
    double max_fsync_pressure_ratio_pct = 150.0,
    double min_batch_fill_ratio_pct = 75.0) {
    const std::string baseline_json = read_text_file(baseline_path);
    const std::string candidate_json = read_text_file(candidate_path);

    const double baseline_write_ops_per_s = extract_json_number(baseline_json, "write_ops_per_s");
    const double candidate_write_ops_per_s = extract_json_number(candidate_json, "write_ops_per_s");
    const double baseline_read_ops_per_s = extract_json_number(baseline_json, "read_ops_per_s");
    const double candidate_read_ops_per_s = extract_json_number(candidate_json, "read_ops_per_s");
    const double baseline_avg_write_latency_us = extract_json_number(baseline_json, "avg_write_latency_us");
    const double candidate_avg_write_latency_us = extract_json_number(candidate_json, "avg_write_latency_us");
    const double baseline_p95_latency_us = extract_json_number(baseline_json, "approx_write_latency_p95_us");
    const double candidate_p95_latency_us = extract_json_number(candidate_json, "approx_write_latency_p95_us");
    const double baseline_p99_latency_us = extract_json_number(baseline_json, "approx_write_latency_p99_us");
    const double candidate_p99_latency_us = extract_json_number(candidate_json, "approx_write_latency_p99_us");
    const double baseline_fsync_pressure = extract_json_number(baseline_json, "observed_fsync_pressure_per_1000_writes");
    const double candidate_fsync_pressure = extract_json_number(candidate_json, "observed_fsync_pressure_per_1000_writes");
    const double baseline_batch_fill = extract_json_number(baseline_json, "recent_batch_fill_per_1000");
    const double candidate_batch_fill = extract_json_number(candidate_json, "recent_batch_fill_per_1000");

    require(baseline_write_ops_per_s > 0.0, "baseline write throughput must be positive");
    require(baseline_read_ops_per_s > 0.0, "baseline read throughput must be positive");
    require(baseline_avg_write_latency_us > 0.0, "baseline write latency must be positive");
    require(baseline_p95_latency_us > 0.0, "baseline p95 write latency must be positive");
    require(baseline_p99_latency_us > 0.0, "baseline p99 write latency must be positive");
    require(baseline_fsync_pressure > 0.0, "baseline fsync pressure must be positive");
    require(baseline_batch_fill > 0.0, "baseline batch fill must be positive");

    const double write_ratio_pct = (candidate_write_ops_per_s / baseline_write_ops_per_s) * 100.0;
    const double read_ratio_pct = (candidate_read_ops_per_s / baseline_read_ops_per_s) * 100.0;
    const double latency_ratio_pct = (candidate_avg_write_latency_us / baseline_avg_write_latency_us) * 100.0;
    const double p95_latency_ratio_pct = (candidate_p95_latency_us / baseline_p95_latency_us) * 100.0;
    const double p99_latency_ratio_pct = (candidate_p99_latency_us / baseline_p99_latency_us) * 100.0;
    const double fsync_pressure_ratio_pct = (candidate_fsync_pressure / baseline_fsync_pressure) * 100.0;
    const double batch_fill_ratio_pct = (candidate_batch_fill / baseline_batch_fill) * 100.0;

    const bool write_ok = write_ratio_pct >= min_write_ratio_pct;
    const bool read_ok = read_ratio_pct >= min_read_ratio_pct;
    const bool latency_ok = latency_ratio_pct <= max_latency_ratio_pct;
    const bool p95_latency_ok = p95_latency_ratio_pct <= max_p95_latency_ratio_pct;
    const bool p99_latency_ok = p99_latency_ratio_pct <= max_p99_latency_ratio_pct;
    const bool fsync_pressure_ok = fsync_pressure_ratio_pct <= max_fsync_pressure_ratio_pct;
    const bool batch_fill_ok = batch_fill_ratio_pct >= min_batch_fill_ratio_pct;
    const bool pass = write_ok && read_ok && latency_ok && p95_latency_ok && p99_latency_ok &&
                      fsync_pressure_ok && batch_fill_ok;

    std::cout << "baseline=" << baseline_path
              << " candidate=" << candidate_path
              << " write_ratio_pct=" << write_ratio_pct
              << " read_ratio_pct=" << read_ratio_pct
              << " latency_ratio_pct=" << latency_ratio_pct
              << " p95_latency_ratio_pct=" << p95_latency_ratio_pct
              << " p99_latency_ratio_pct=" << p99_latency_ratio_pct
              << " fsync_pressure_ratio_pct=" << fsync_pressure_ratio_pct
              << " batch_fill_ratio_pct=" << batch_fill_ratio_pct
              << " min_write_ratio_pct=" << min_write_ratio_pct
              << " min_read_ratio_pct=" << min_read_ratio_pct
              << " max_latency_ratio_pct=" << max_latency_ratio_pct
              << " max_p95_latency_ratio_pct=" << max_p95_latency_ratio_pct
              << " max_p99_latency_ratio_pct=" << max_p99_latency_ratio_pct
              << " max_fsync_pressure_ratio_pct=" << max_fsync_pressure_ratio_pct
              << " min_batch_fill_ratio_pct=" << min_batch_fill_ratio_pct
              << " status=" << (pass ? "pass" : "fail")
              << std::endl;
    return pass ? 0 : 2;
}

BaselineSummary load_baseline_summary(const std::filesystem::path& path) {
    const std::string json = read_text_file(path.string());
    BaselineSummary summary;
    summary.file_name = path.filename().string();
    summary.write_ops_per_s = extract_json_number(json, "write_ops_per_s");
    summary.read_ops_per_s = extract_json_number(json, "read_ops_per_s");
    summary.avg_write_latency_us = extract_json_number(json, "avg_write_latency_us");
    return summary;
}

int run_benchmark_trend(const std::string& directory_path) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(directory_path)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() != ".json") {
            continue;
        }
        files.push_back(entry.path());
    }

    std::sort(files.begin(), files.end());
    require(!files.empty(), "benchmark trend requires at least one baseline json file");

    std::vector<BaselineSummary> summaries;
    summaries.reserve(files.size());
    for (const auto& path : files) {
        summaries.push_back(load_baseline_summary(path));
    }

    const BaselineSummary& first = summaries.front();
    const BaselineSummary& latest = summaries.back();
    double min_write_ops_per_s = first.write_ops_per_s;
    double max_write_ops_per_s = first.write_ops_per_s;
    double min_read_ops_per_s = first.read_ops_per_s;
    double max_read_ops_per_s = first.read_ops_per_s;
    double min_avg_write_latency_us = first.avg_write_latency_us;
    double max_avg_write_latency_us = first.avg_write_latency_us;
    double sum_write_ops_per_s = 0.0;
    double sum_read_ops_per_s = 0.0;
    double sum_avg_write_latency_us = 0.0;

    for (const auto& summary : summaries) {
        min_write_ops_per_s = std::min(min_write_ops_per_s, summary.write_ops_per_s);
        max_write_ops_per_s = std::max(max_write_ops_per_s, summary.write_ops_per_s);
        min_read_ops_per_s = std::min(min_read_ops_per_s, summary.read_ops_per_s);
        max_read_ops_per_s = std::max(max_read_ops_per_s, summary.read_ops_per_s);
        min_avg_write_latency_us = std::min(min_avg_write_latency_us, summary.avg_write_latency_us);
        max_avg_write_latency_us = std::max(max_avg_write_latency_us, summary.avg_write_latency_us);
        sum_write_ops_per_s += summary.write_ops_per_s;
        sum_read_ops_per_s += summary.read_ops_per_s;
        sum_avg_write_latency_us += summary.avg_write_latency_us;
    }

    const double avg_write_ops_per_s = sum_write_ops_per_s / summaries.size();
    const double avg_read_ops_per_s = sum_read_ops_per_s / summaries.size();
    const double avg_avg_write_latency_us = sum_avg_write_latency_us / summaries.size();
    const double latest_vs_first_write_ratio_pct =
        first.write_ops_per_s == 0.0 ? 0.0 : (latest.write_ops_per_s / first.write_ops_per_s) * 100.0;
    const double latest_vs_first_read_ratio_pct =
        first.read_ops_per_s == 0.0 ? 0.0 : (latest.read_ops_per_s / first.read_ops_per_s) * 100.0;
    const double latest_vs_first_latency_ratio_pct =
        first.avg_write_latency_us == 0.0 ? 0.0 : (latest.avg_write_latency_us / first.avg_write_latency_us) * 100.0;

    std::cout << "count=" << summaries.size()
              << " oldest_file=" << first.file_name
              << " latest_file=" << latest.file_name
              << " avg_write_ops_per_s=" << avg_write_ops_per_s
              << " min_write_ops_per_s=" << min_write_ops_per_s
              << " max_write_ops_per_s=" << max_write_ops_per_s
              << " avg_read_ops_per_s=" << avg_read_ops_per_s
              << " min_read_ops_per_s=" << min_read_ops_per_s
              << " max_read_ops_per_s=" << max_read_ops_per_s
              << " avg_write_latency_us=" << avg_avg_write_latency_us
              << " min_write_latency_us=" << min_avg_write_latency_us
              << " max_write_latency_us=" << max_avg_write_latency_us
              << " latest_vs_oldest_write_ratio_pct=" << latest_vs_first_write_ratio_pct
              << " latest_vs_oldest_read_ratio_pct=" << latest_vs_first_read_ratio_pct
              << " latest_vs_oldest_latency_ratio_pct=" << latest_vs_first_latency_ratio_pct
              << std::endl;
    return 0;
}

KVStoreOptions benchmark_options() {
    KVStoreOptions options;
    options.max_batch_size = 32;
    options.max_batch_wal_bytes = 1 << 20;
    options.max_batch_delay_us = 2000;
    options.adaptive_recent_window_batches = 64;
    options.adaptive_recent_write_sample_limit = 512;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 1;
    options.adaptive_objective_latency_weight = 3;
    options.adaptive_objective_read_weight = 2;
    options.adaptive_objective_throughput_weight = 2;
    options.adaptive_objective_target_batch_size = 16;
    options.adaptive_objective_fsync_weight = 2;
    options.adaptive_objective_compaction_weight = 1;
    options.adaptive_objective_wal_growth_weight = 1;
    options.adaptive_objective_short_delay_score_threshold = 500;
    options.adaptive_objective_long_delay_score_threshold = 500;
    options.adaptive_objective_short_delay_divisor = 2;
    options.adaptive_objective_long_delay_multiplier = 2;
    options.adaptive_objective_max_batch_delay_us = 8000;
    options.adaptive_read_heavy_read_per_1000_ops_threshold = 700;
    options.adaptive_read_heavy_delay_divisor = 4;
    options.adaptive_read_heavy_batch_size_divisor = 2;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 8;
    options.adaptive_flush_delay_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 100;
    options.adaptive_latency_target_p95_us = 12000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 350;
    options.adaptive_fsync_pressure_delay_multiplier = 2;
    options.adaptive_fsync_pressure_max_batch_delay_us = 8000;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_compaction_pressure_delay_multiplier = 2;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 200;
    options.adaptive_wal_growth_delay_multiplier = 2;
    options.adaptive_wal_growth_max_batch_delay_us = 6000;
    options.adaptive_batching_enabled = true;
    options.adaptive_queue_depth_threshold = 8;
    options.adaptive_batch_size_multiplier = 4;
    options.adaptive_batch_wal_bytes_multiplier = 4;
    return options;
}

BenchmarkResult run_benchmark_capture(const BenchmarkConfig& config) {
    TestDir dir("bench_" + config.label);
    const std::string db_path = dir.file("store.dat");
    const KVStoreOptions options = benchmark_options();
    KVStore store(db_path, options);

    std::atomic<bool> stop {false};
    std::atomic<uint64_t> write_ops {0};
    std::atomic<uint64_t> read_ops {0};
    std::atomic<uint64_t> write_latency_ns {0};

    std::vector<std::thread> threads;
    for (int writer_id = 0; writer_id < config.writer_count; ++writer_id) {
        threads.emplace_back([&store, &stop, &write_ops, &write_latency_ns, &config, writer_id]() {
            std::mt19937 gen(1337 + writer_id);
            std::uniform_int_distribution<int> key_dist(writer_id * 100000, writer_id * 100000 + config.key_space - 1);
            while (!stop.load(std::memory_order_acquire)) {
                const int key = key_dist(gen);
                const auto begin = std::chrono::steady_clock::now();
                store.Put(key, text("payload_" + std::to_string(key)));
                const auto end = std::chrono::steady_clock::now();
                write_ops.fetch_add(1, std::memory_order_relaxed);
                write_latency_ns.fetch_add(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count(),
                    std::memory_order_relaxed);
            }
        });
    }

    for (int reader_id = 0; reader_id < config.reader_count; ++reader_id) {
        threads.emplace_back([&store, &stop, &read_ops, &config, reader_id]() {
            std::mt19937 gen(4242 + reader_id);
            std::uniform_int_distribution<int> key_dist(0, config.writer_count * 100000 + config.key_space - 1);
            while (!stop.load(std::memory_order_acquire)) {
                (void)store.Get(key_dist(gen));
                read_ops.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    const auto start = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(config.duration_ms));
    stop.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    const auto end = std::chrono::steady_clock::now();
    BenchmarkResult result;
    result.config = config;
    result.options = options;
    result.duration_s = std::chrono::duration<double>(end - start).count();
    result.writes = write_ops.load(std::memory_order_relaxed);
    result.reads = read_ops.load(std::memory_order_relaxed);
    result.write_ops_per_s = result.duration_s == 0.0 ? 0.0 : result.writes / result.duration_s;
    result.read_ops_per_s = result.duration_s == 0.0 ? 0.0 : result.reads / result.duration_s;
    result.avg_write_latency_us =
        result.writes == 0 ? 0.0 : static_cast<double>(write_latency_ns.load(std::memory_order_relaxed)) / result.writes / 1000.0;
    result.metrics = store.GetMetrics();
    return result;
}

std::string BenchmarkResultToJson(const BenchmarkResult& result) {
    std::ostringstream out;
    out << "{"
        << "\"label\":\"" << result.config.label << "\","
        << "\"workload\":{"
        << "\"writers\":" << result.config.writer_count << ','
        << "\"readers\":" << result.config.reader_count << ','
        << "\"duration_ms\":" << result.config.duration_ms << ','
        << "\"key_space\":" << result.config.key_space
        << "},"
        << "\"summary\":{"
        << "\"duration_s\":" << result.duration_s << ','
        << "\"writes\":" << result.writes << ','
        << "\"reads\":" << result.reads << ','
        << "\"write_ops_per_s\":" << result.write_ops_per_s << ','
        << "\"read_ops_per_s\":" << result.read_ops_per_s << ','
        << "\"avg_write_latency_us\":" << result.avg_write_latency_us
        << "},"
        << "\"options\":" << OptionsToJson(result.options) << ','
        << "\"metrics\":" << MetricsToJson(result.metrics)
        << "}";
    return out.str();
}

double extract_json_number(const std::string& json, const std::string& key) {
    const std::string needle = "\"" + key + "\":";
    const size_t pos = json.find(needle);
    require(pos != std::string::npos, "Expected numeric key in json: " + key);
    size_t cursor = pos + needle.size();
    while (cursor < json.size() && std::isspace(static_cast<unsigned char>(json[cursor]))) {
        ++cursor;
    }
    size_t end = cursor;
    while (end < json.size()) {
        const char ch = json[end];
        if ((ch >= '0' && ch <= '9') || ch == '-' || ch == '+' || ch == '.' || ch == 'e' || ch == 'E') {
            ++end;
            continue;
        }
        break;
    }
    require(end > cursor, "Expected numeric value in json for key: " + key);
    return std::stod(json.substr(cursor, end - cursor));
}

std::optional<KVStoreProfile> parse_profile_name(const std::string& name) {
    if (name == "balanced") {
        return KVStoreProfile::kBalanced;
    }
    if (name == "write-heavy") {
        return KVStoreProfile::kWriteHeavy;
    }
    if (name == "read-heavy") {
        return KVStoreProfile::kReadHeavy;
    }
    if (name == "low-latency") {
        return KVStoreProfile::kLowLatency;
    }
    return std::nullopt;
}

enum class SoakProfile {
    kBalanced,
    kWriteHeavy,
    kReadHeavy,
};

enum class ConcurrencyStressProfile {
    kBalanced,
    kWriteHeavy,
    kCompactionHeavy,
};

std::optional<SoakProfile> parse_soak_profile_name(const std::string& name) {
    if (name == "balanced") {
        return SoakProfile::kBalanced;
    }
    if (name == "write-heavy") {
        return SoakProfile::kWriteHeavy;
    }
    if (name == "read-heavy") {
        return SoakProfile::kReadHeavy;
    }
    return std::nullopt;
}

std::optional<ConcurrencyStressProfile> parse_concurrency_stress_profile_name(const std::string& name) {
    if (name == "balanced") {
        return ConcurrencyStressProfile::kBalanced;
    }
    if (name == "write-heavy") {
        return ConcurrencyStressProfile::kWriteHeavy;
    }
    if (name == "compaction-heavy") {
        return ConcurrencyStressProfile::kCompactionHeavy;
    }
    return std::nullopt;
}

struct SoakProfileConfig {
    KVStoreOptions options;
    int writer_count = 4;
    int reader_count = 2;
    int key_space = 256;
    int compaction_interval_ms = 50;
};

struct ConcurrencyStressProfileConfig {
    KVStoreOptions options;
    int writer_count = 8;
    int reader_count = 4;
    int metrics_reader_count = 1;
    int compactor_count = 1;
    int keys_per_writer = 96;
    int compaction_interval_ms = 5;
    int batch_width = 4;
};

SoakProfileConfig make_soak_profile_config(SoakProfile profile) {
    SoakProfileConfig config;
    config.options.max_batch_size = 32;
    config.options.max_batch_wal_bytes = 1 << 20;
    config.options.max_batch_delay_us = 2000;
    config.options.auto_compact_wal_bytes_threshold = 4096;
    config.options.auto_compact_invalid_wal_ratio_percent = 60;
    config.options.adaptive_flush_enabled = true;
    config.options.adaptive_flush_queue_depth_threshold = 8;
    config.options.adaptive_flush_delay_divisor = 4;
    config.options.adaptive_flush_min_batch_delay_us = 100;

    switch (profile) {
        case SoakProfile::kBalanced:
            return config;
        case SoakProfile::kWriteHeavy:
            config.writer_count = 6;
            config.reader_count = 1;
            config.key_space = 512;
            config.compaction_interval_ms = 80;
            config.options.max_batch_size = 64;
            config.options.max_batch_delay_us = 4000;
            config.options.auto_compact_wal_bytes_threshold = 8192;
            config.options.auto_compact_invalid_wal_ratio_percent = 70;
            return config;
        case SoakProfile::kReadHeavy:
            config.writer_count = 2;
            config.reader_count = 6;
            config.key_space = 256;
            config.compaction_interval_ms = 30;
            config.options.max_batch_size = 16;
            config.options.max_batch_delay_us = 1000;
            config.options.adaptive_flush_queue_depth_threshold = 4;
            config.options.adaptive_flush_min_batch_delay_us = 50;
            return config;
    }
    return config;
}

ConcurrencyStressProfileConfig make_concurrency_stress_profile_config(ConcurrencyStressProfile profile) {
    ConcurrencyStressProfileConfig config;
    config.options.max_batch_size = 32;
    config.options.max_batch_wal_bytes = 1 << 16;
    config.options.max_batch_delay_us = 1500;
    config.options.adaptive_batching_enabled = true;
    config.options.adaptive_queue_depth_threshold = 8;
    config.options.adaptive_batch_size_multiplier = 4;
    config.options.adaptive_batch_wal_bytes_multiplier = 4;
    config.options.adaptive_flush_enabled = true;
    config.options.adaptive_flush_queue_depth_threshold = 4;
    config.options.adaptive_flush_delay_divisor = 4;
    config.options.adaptive_flush_min_batch_delay_us = 50;
    config.options.auto_compact_wal_bytes_threshold = 4096;
    config.options.auto_compact_invalid_wal_ratio_percent = 60;
    config.options.adaptive_recent_window_batches = 32;
    config.options.adaptive_recent_write_sample_limit = 256;

    switch (profile) {
        case ConcurrencyStressProfile::kBalanced:
            return config;
        case ConcurrencyStressProfile::kWriteHeavy:
            config.writer_count = 12;
            config.reader_count = 2;
            config.keys_per_writer = 128;
            config.compaction_interval_ms = 8;
            config.options.max_batch_size = 64;
            config.options.max_batch_delay_us = 2500;
            config.options.auto_compact_wal_bytes_threshold = 8192;
            return config;
        case ConcurrencyStressProfile::kCompactionHeavy:
            config.writer_count = 6;
            config.reader_count = 3;
            config.compactor_count = 2;
            config.keys_per_writer = 80;
            config.compaction_interval_ms = 1;
            config.options.max_batch_size = 24;
            config.options.max_batch_delay_us = 800;
            config.options.auto_compact_wal_bytes_threshold = 2048;
            config.options.auto_compact_invalid_wal_ratio_percent = 45;
            return config;
    }
    return config;
}

int run_profile_json(const std::string& name) {
    const auto profile = parse_profile_name(name);
    if (!profile.has_value()) {
        std::cerr << "Unknown profile: " << name << '\n';
        return 1;
    }
    std::cout << OptionsToJson(RecommendedOptions(*profile)) << std::endl;
    return 0;
}

void test_basic_persistence() {
    TestDir dir("basic");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("alpha"));
        store.Put(2, text("beta"));
    }

    KVStore reopened(db_path);
    const auto value1 = reopened.Get(1);
    const auto value2 = reopened.Get(2);
    require(value1.has_value(), "key 1 should exist after reopen");
    require(value2.has_value(), "key 2 should exist after reopen");
    require(as_string(*value1) == "alpha", "key 1 should keep its value");
    require(as_string(*value2) == "beta", "key 2 should keep its value");
}

void test_string_keys_do_not_collide_with_int_keys() {
    TestDir dir("string_keys");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(42, text("int-key"));
        store.Put(std::string("42"), text("string-key"));
        store.Put(std::string("alpha"), text("word"));
    }

    KVStore reopened(db_path);
    const auto int_value = reopened.Get(42);
    const auto string_value = reopened.Get(std::string("42"));
    const auto alpha_value = reopened.Get(std::string("alpha"));
    require(int_value.has_value(), "int key should persist");
    require(string_value.has_value(), "string key should persist");
    require(alpha_value.has_value(), "string word key should persist");
    require(as_string(*int_value) == "int-key", "int key should keep its own namespace");
    require(as_string(*string_value) == "string-key", "string key should not collide with int namespace");
    require(as_string(*alpha_value) == "word", "string API should round-trip normal string keys");
}

void test_string_scan_returns_sorted_range() {
    TestDir dir("string_scan");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    store.Put(std::string("apple"), text("a"));
    store.Put(std::string("banana"), text("b"));
    store.Put(std::string("carrot"), text("c"));
    store.Put(std::string("date"), text("d"));
    store.Put(7, text("int-should-not-appear"));

    const auto results = store.Scan("banana", "date");
    require(results.size() == 3, "scan should return all string keys in the requested inclusive range");
    require(results[0].first == "banana" && as_string(results[0].second) == "b", "scan should start at banana");
    require(results[1].first == "carrot" && as_string(results[1].second) == "c", "scan should keep lexical order");
    require(results[2].first == "date" && as_string(results[2].second) == "d", "scan should include the upper bound");
}

void test_binary_keys_do_not_collide_with_other_namespaces() {
    TestDir dir("binary_keys");
    const std::string db_path = dir.file("store.dat");
    const std::vector<uint8_t> binary_42 = {'4', '2'};
    const std::vector<uint8_t> binary_raw = {0x00, 0xFF, 0x42};

    {
        KVStore store(db_path);
        store.Put(42, text("int-key"));
        store.Put(std::string("42"), text("string-key"));
        store.Put(binary_42, text("binary-key"));
        store.Put(binary_raw, text("raw-binary"));
    }

    KVStore reopened(db_path);
    const auto int_value = reopened.Get(42);
    const auto string_value = reopened.Get(std::string("42"));
    const auto binary_value = reopened.Get(binary_42);
    const auto raw_binary_value = reopened.Get(binary_raw);
    require(int_value.has_value() && as_string(*int_value) == "int-key", "int namespace should be preserved");
    require(string_value.has_value() && as_string(*string_value) == "string-key", "string namespace should be preserved");
    require(binary_value.has_value() && as_string(*binary_value) == "binary-key", "binary namespace should be preserved");
    require(raw_binary_value.has_value() && as_string(*raw_binary_value) == "raw-binary",
            "binary keys should support embedded zero bytes");
}

void test_write_batch_persists_mixed_key_types() {
    TestDir dir("batch_mixed_keys");
    const std::string db_path = dir.file("store.dat");
    const std::vector<uint8_t> binary_key = {0x00, 0x01, 0x02};

    {
        KVStore store(db_path);
        store.WriteBatch({
            BatchWriteOperation::PutInt(42, text("int-key")),
            BatchWriteOperation::Put("42", text("string-key")),
            BatchWriteOperation::Put("alpha", text("word")),
            BatchWriteOperation::PutBinary(binary_key, text("binary-word")),
        });
    }

    KVStore reopened(db_path);
    const auto int_value = reopened.Get(42);
    const auto string_value = reopened.Get(std::string("42"));
    const auto alpha_value = reopened.Get(std::string("alpha"));
    const auto binary_value = reopened.Get(binary_key);
    require(int_value.has_value(), "batch int key should persist");
    require(string_value.has_value(), "batch string key should persist");
    require(alpha_value.has_value(), "batch string key should persist");
    require(binary_value.has_value(), "batch binary key should persist");
    require(as_string(*int_value) == "int-key", "batch int key should keep its namespace");
    require(as_string(*string_value) == "string-key", "batch string key should not collide with int namespace");
    require(as_string(*alpha_value) == "word", "batch string key should round-trip");
    require(as_string(*binary_value) == "binary-word", "batch binary key should round-trip");
}

void test_write_batch_mixes_put_and_delete() {
    TestDir dir("batch_put_delete");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    store.Put(1, text("old"));
    store.Put(9, text("remove-me"));
    store.Put(std::string("alpha"), text("old-alpha"));

    store.WriteBatch({
        BatchWriteOperation::PutInt(1, text("new")),
        BatchWriteOperation::Delete("alpha"),
        BatchWriteOperation::Put("beta", text("beta-value")),
        BatchWriteOperation::DeleteInt(9),
    });

    const auto one = store.Get(1);
    const auto nine = store.Get(9);
    const auto alpha = store.Get(std::string("alpha"));
    const auto beta = store.Get(std::string("beta"));
    require(one.has_value() && as_string(*one) == "new", "batch put should replace existing int values");
    require(!nine.has_value(), "batch delete should remove int keys");
    require(!alpha.has_value(), "batch delete should remove string keys");
    require(beta.has_value() && as_string(*beta) == "beta-value", "batch put should insert new string keys");
}

void test_write_batch_preserves_operation_order() {
    TestDir dir("batch_order");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.WriteBatch({
            BatchWriteOperation::PutInt(7, text("first")),
            BatchWriteOperation::DeleteInt(7),
            BatchWriteOperation::PutInt(7, text("second")),
            BatchWriteOperation::Put("gamma", text("a")),
            BatchWriteOperation::Put("gamma", text("b")),
        });
    }

    KVStore reopened(db_path);
    const auto seven = reopened.Get(7);
    const auto gamma = reopened.Get(std::string("gamma"));
    require(seven.has_value() && as_string(*seven) == "second", "batch operations should apply in-order for int keys");
    require(gamma.has_value() && as_string(*gamma) == "b", "batch operations should apply in-order for string keys");
}

void test_put_copies_input_value() {
    TestDir dir("copy");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    Value original = text("alpha");
    store.Put(7, original);
    original.bytes[0] = static_cast<uint8_t>('x');

    const auto stored = store.Get(7);
    require(stored.has_value(), "copied value should exist");
    require(as_string(*stored) == "alpha", "stored value should not alias caller buffer");
}

void test_recovery_from_wal_without_compaction() {
    TestDir dir("wal_recovery");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(10, text("first"));
        store.Put(11, text("second"));
        store.Delete(10);
    }

    require(file_size_or_zero(db_path) == 16, "snapshot should still only contain the header before compaction");
    require(file_size_or_zero(wal_path) > sizeof(WalRecordHeader), "WAL should contain persisted operations");

    KVStore reopened(db_path);
    require(!reopened.Get(10).has_value(), "deleted key should stay deleted after WAL replay");
    const auto value = reopened.Get(11);
    require(value.has_value(), "remaining key should survive WAL replay");
    require(as_string(*value) == "second", "WAL replay should restore the latest value");
}

void test_legacy_v1_snapshot_and_wal_are_readable() {
    TestDir dir("legacy_v1_read");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    write_legacy_snapshot_v1(db_path, {
        {1, text("legacy-one")},
        {2, text("legacy-two")},
    });
    append_legacy_wal_record_v1(wal_path, 1, 3, text("legacy-three"));
    append_legacy_wal_record_v1(wal_path, 2, 2, Value {});

    KVStore store(db_path);
    const auto one = store.Get(1);
    const auto two = store.Get(2);
    const auto three = store.Get(3);
    require(one.has_value() && as_string(*one) == "legacy-one", "v1 snapshot key 1 should load");
    require(!two.has_value(), "v1 WAL delete should apply on top of snapshot state");
    require(three.has_value() && as_string(*three) == "legacy-three", "v1 WAL put should replay");
}

void test_legacy_v1_rewrite_upgrades_snapshot_to_v2() {
    TestDir dir("legacy_v1_upgrade");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    write_legacy_snapshot_v1(db_path, {
        {7, text("seven")},
    });
    append_legacy_wal_record_v1(wal_path, 1, 9, text("nine"));

    require(run_rewrite_format(db_path) == 0, "rewrite-format should succeed on legacy data");

    std::ifstream snapshot(db_path, std::ios::binary);
    SnapshotHeader header {};
    snapshot.read(reinterpret_cast<char*>(&header), sizeof(header));
    require(snapshot.gcount() == static_cast<std::streamsize>(sizeof(header)), "rewritten snapshot should contain a full header");
    require(header.version == 2, "rewrite-format should upgrade legacy snapshot files to version 2");
    require(file_size_or_zero(wal_path) == 0, "rewrite-format should rotate WAL back to empty");

    KVStore reopened(db_path);
    const auto seven = reopened.Get(7);
    const auto nine = reopened.Get(9);
    require(seven.has_value() && as_string(*seven) == "seven", "rewritten data should preserve snapshot value");
    require(nine.has_value() && as_string(*nine) == "nine", "rewritten data should preserve WAL value");
}

void test_inspect_format_reports_key_type_counts() {
    TestDir dir("inspect_counts");
    const std::string db_path = dir.file("store.dat");
    const std::vector<uint8_t> binary_snapshot_key = {0x10, 0x20, 0x30};
    const std::vector<uint8_t> binary_wal_key = {0x00, 0x01};

    {
        KVStore store(db_path);
        store.Put(1, text("int-snapshot"));
        store.Put(std::string("alpha"), text("string-snapshot"));
        store.Put(binary_snapshot_key, text("binary-snapshot"));
        store.Compact();
        store.Put(std::string("beta"), text("string-wal"));
        store.Put(binary_wal_key, text("binary-wal"));
        store.Delete(1);
    }

    const auto values = capture_inspect_format(db_path);
    require(values.at("snapshot_version") == "2", "inspect-format should report v2 snapshot");
    require(values.at("snapshot_entries") == "3", "inspect-format should count compacted snapshot entries");
    require(values.at("snapshot_int_keys") == "1", "inspect-format should count int snapshot keys");
    require(values.at("snapshot_string_keys") == "1", "inspect-format should count string snapshot keys");
    require(values.at("snapshot_binary_keys") == "1", "inspect-format should count binary snapshot keys");
    require(values.at("wal_version") == "2", "inspect-format should report v2 WAL");
    require(values.at("wal_records") == "3", "inspect-format should count WAL records after compaction");
    require(values.at("wal_put_records") == "2", "inspect-format should count WAL puts");
    require(values.at("wal_delete_records") == "1", "inspect-format should count WAL deletes");
    require(values.at("wal_int_keys") == "1", "inspect-format should count int WAL keys");
    require(values.at("wal_string_keys") == "1", "inspect-format should count string WAL keys");
    require(values.at("wal_binary_keys") == "1", "inspect-format should count binary WAL keys");
    require(values.at("rewrite_recommended") == "0", "current-format data should not require rewrite");
}

void test_inspect_format_recommends_rewrite_for_legacy_v1() {
    TestDir dir("inspect_legacy_v1");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    write_legacy_snapshot_v1(db_path, {
        {1, text("legacy-one")},
        {2, text("legacy-two")},
    });
    append_legacy_wal_record_v1(wal_path, 1, 3, text("legacy-three"));
    append_legacy_wal_record_v1(wal_path, 2, 2, Value {});

    const auto values = capture_inspect_format(db_path);
    require(values.at("snapshot_version") == "1", "inspect-format should report legacy snapshot version");
    require(values.at("snapshot_entries") == "2", "inspect-format should count legacy snapshot entries");
    require(values.at("snapshot_int_keys") == "2", "legacy snapshots should be recognized as int keys");
    require(values.at("wal_version") == "1", "inspect-format should report legacy WAL version");
    require(values.at("wal_records") == "2", "inspect-format should count legacy WAL records");
    require(values.at("wal_int_keys") == "2", "legacy WAL records should be recognized as int keys");
    require(values.at("rewrite_recommended") == "1", "legacy data should recommend rewrite");
}

void test_verify_format_accepts_current_layout() {
    TestDir dir("verify_current");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("one"));
        store.Put(std::string("alpha"), text("two"));
        store.Put(std::vector<uint8_t> {0xAA, 0xBB}, text("three"));
        store.Compact();
    }

    const auto [status, output] = capture_verify_format(db_path);
    require(status == 0, "verify-format should accept current layout");
    require(output.find("rewrite_recommended=0") != std::string::npos,
            "verify-format should print inspect output for current layout");
}

void test_verify_format_rejects_legacy_layout() {
    TestDir dir("verify_legacy");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    write_legacy_snapshot_v1(db_path, {
        {7, text("seven")},
    });
    append_legacy_wal_record_v1(wal_path, 1, 9, text("nine"));

    const auto [status, output] = capture_verify_format(db_path);
    require(status == 2, "verify-format should reject legacy layout and request rewrite");
    require(output.find("rewrite_recommended=1") != std::string::npos,
            "verify-format should expose the rewrite recommendation");
}

void test_compatibility_matrix_command_succeeds() {
    require(run_compatibility_matrix() == 0, "compatibility matrix command should succeed");
}

void test_benchmark_result_json_reports_summary_and_metrics() {
    const BenchmarkResult result = run_benchmark_capture(
        make_benchmark_config("test-baseline", 2, 1, 50, 128));
    const std::string json = BenchmarkResultToJson(result);
    require(json.find("\"label\":\"test-baseline\"") != std::string::npos,
            "benchmark baseline json should include the benchmark label");
    require(json.find("\"workload\":{") != std::string::npos,
            "benchmark baseline json should include workload metadata");
    require(json.find("\"summary\":{") != std::string::npos,
            "benchmark baseline json should include summary metrics");
    require(json.find("\"options\":{") != std::string::npos,
            "benchmark baseline json should include option metadata");
    require(json.find("\"metrics\":{") != std::string::npos,
            "benchmark baseline json should include kv metrics");
}

void test_compare_benchmark_baseline_passes_within_thresholds() {
    TestDir dir("compare_baseline_pass");
    const std::string baseline_path = dir.file("baseline.json");
    const std::string candidate_path = dir.file("candidate.json");

    BenchmarkResult baseline;
    baseline.config = make_benchmark_config("baseline", 1, 1, 100, 64);
    baseline.duration_s = 1.0;
    baseline.writes = 1000;
    baseline.reads = 2000;
    baseline.write_ops_per_s = 1000.0;
    baseline.read_ops_per_s = 2000.0;
    baseline.avg_write_latency_us = 100.0;
    baseline.metrics.approx_write_latency_p95_us = 1000;
    baseline.metrics.approx_write_latency_p99_us = 1200;
    baseline.metrics.observed_fsync_pressure_per_1000_writes = 200;
    baseline.metrics.recent_batch_fill_per_1000 = 500;

    BenchmarkResult candidate = baseline;
    candidate.config.label = "candidate";
    candidate.write_ops_per_s = 900.0;
    candidate.read_ops_per_s = 1800.0;
    candidate.avg_write_latency_us = 115.0;
    candidate.metrics.approx_write_latency_p95_us = 1300;
    candidate.metrics.approx_write_latency_p99_us = 1800;
    candidate.metrics.observed_fsync_pressure_per_1000_writes = 250;
    candidate.metrics.recent_batch_fill_per_1000 = 450;

    write_text_file(baseline_path, BenchmarkResultToJson(baseline));
    write_text_file(candidate_path, BenchmarkResultToJson(candidate));

    const int status = run_compare_benchmark_baseline(baseline_path, candidate_path);
    require(status == 0, "compare-baseline should pass when throughput and latency stay within thresholds");
}

void test_compare_benchmark_baseline_rejects_regression() {
    TestDir dir("compare_baseline_fail");
    const std::string baseline_path = dir.file("baseline.json");
    const std::string candidate_path = dir.file("candidate.json");

    BenchmarkResult baseline;
    baseline.config = make_benchmark_config("baseline", 1, 1, 100, 64);
    baseline.duration_s = 1.0;
    baseline.writes = 1000;
    baseline.reads = 2000;
    baseline.write_ops_per_s = 1000.0;
    baseline.read_ops_per_s = 2000.0;
    baseline.avg_write_latency_us = 100.0;
    baseline.metrics.approx_write_latency_p95_us = 1000;
    baseline.metrics.approx_write_latency_p99_us = 1200;
    baseline.metrics.observed_fsync_pressure_per_1000_writes = 200;
    baseline.metrics.recent_batch_fill_per_1000 = 500;

    BenchmarkResult candidate = baseline;
    candidate.config.label = "candidate";
    candidate.write_ops_per_s = 700.0;
    candidate.read_ops_per_s = 1500.0;
    candidate.avg_write_latency_us = 140.0;
    candidate.metrics.approx_write_latency_p95_us = 1700;
    candidate.metrics.approx_write_latency_p99_us = 2300;
    candidate.metrics.observed_fsync_pressure_per_1000_writes = 400;
    candidate.metrics.recent_batch_fill_per_1000 = 300;

    write_text_file(baseline_path, BenchmarkResultToJson(baseline));
    write_text_file(candidate_path, BenchmarkResultToJson(candidate));

    const int status = run_compare_benchmark_baseline(baseline_path, candidate_path);
    require(status == 2, "compare-baseline should reject throughput/latency regressions beyond thresholds");
}

void test_benchmark_trend_summarizes_history() {
    TestDir dir("benchmark_trend");
    const std::string baseline_dir = dir.file("baselines");
    std::filesystem::create_directories(baseline_dir);

    BenchmarkResult early;
    early.config = make_benchmark_config("early", 1, 1, 100, 64);
    early.write_ops_per_s = 1000.0;
    early.read_ops_per_s = 2000.0;
    early.avg_write_latency_us = 100.0;

    BenchmarkResult middle = early;
    middle.config.label = "middle";
    middle.write_ops_per_s = 1100.0;
    middle.read_ops_per_s = 1900.0;
    middle.avg_write_latency_us = 90.0;

    BenchmarkResult latest = early;
    latest.config.label = "latest";
    latest.write_ops_per_s = 1200.0;
    latest.read_ops_per_s = 2200.0;
    latest.avg_write_latency_us = 80.0;

    write_text_file((std::filesystem::path(baseline_dir) / "20260101T000000.json").string(), BenchmarkResultToJson(early));
    write_text_file((std::filesystem::path(baseline_dir) / "20260102T000000.json").string(), BenchmarkResultToJson(middle));
    write_text_file((std::filesystem::path(baseline_dir) / "20260103T000000.json").string(), BenchmarkResultToJson(latest));

    const auto [status, values] = capture_benchmark_trend(baseline_dir);
    require(status == 0, "benchmark trend should succeed for non-empty baseline directory");
    require(values.at("count") == "3", "benchmark trend should count all baseline files");
    require(values.at("oldest_file") == "20260101T000000.json", "benchmark trend should report the oldest file");
    require(values.at("latest_file") == "20260103T000000.json", "benchmark trend should report the latest file");
    require(std::stod(values.at("avg_write_ops_per_s")) > 1099.0 &&
                std::stod(values.at("avg_write_ops_per_s")) < 1101.0,
            "benchmark trend should report the average write throughput");
    require(std::stod(values.at("latest_vs_oldest_write_ratio_pct")) > 119.9 &&
                std::stod(values.at("latest_vs_oldest_write_ratio_pct")) < 120.1,
            "benchmark trend should report latest-vs-oldest write throughput ratio");
    require(std::stod(values.at("latest_vs_oldest_latency_ratio_pct")) > 79.9 &&
                std::stod(values.at("latest_vs_oldest_latency_ratio_pct")) < 80.1,
            "benchmark trend should report latest-vs-oldest latency ratio");
}

void test_soak_profiles_are_distinct() {
    const SoakProfileConfig balanced = make_soak_profile_config(SoakProfile::kBalanced);
    const SoakProfileConfig write_heavy = make_soak_profile_config(SoakProfile::kWriteHeavy);
    const SoakProfileConfig read_heavy = make_soak_profile_config(SoakProfile::kReadHeavy);

    require(write_heavy.writer_count > balanced.writer_count,
            "write-heavy soak profile should use more writers than balanced");
    require(read_heavy.reader_count > balanced.reader_count,
            "read-heavy soak profile should use more readers than balanced");
    require(write_heavy.options.max_batch_size > balanced.options.max_batch_size,
            "write-heavy soak profile should allow larger batches");
    require(read_heavy.options.max_batch_delay_us < balanced.options.max_batch_delay_us,
            "read-heavy soak profile should use shorter batch delays");
}

void test_ordering_and_updates() {
    TestDir dir("ordering");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(3, text("v1"));
        store.Delete(3);
        store.Put(3, text("v2"));
        store.Put(4, text("keep"));
    }

    KVStore reopened(db_path);
    const auto latest = reopened.Get(3);
    const auto other = reopened.Get(4);
    require(latest.has_value(), "key 3 should exist after final put");
    require(other.has_value(), "key 4 should exist after replay");
    require(as_string(*latest) == "v2", "operations must replay in commit order");
    require(as_string(*other) == "keep", "other keys should be unaffected");
}

void test_compaction_persists_snapshot_and_resets_wal() {
    TestDir dir("compaction");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(1, text("one"));
        store.Put(2, text("two"));
        store.Delete(1);
        store.Compact();
    }

    require(file_size_or_zero(db_path) > 16, "compaction should materialize live data into the snapshot");
    require(file_size_or_zero(wal_path) == 0, "compaction should rotate to an empty WAL");

    KVStore reopened(db_path);
    require(!reopened.Get(1).has_value(), "deleted key must not reappear after compaction");
    const auto survivor = reopened.Get(2);
    require(survivor.has_value(), "live key must survive compaction");
    require(as_string(*survivor) == "two", "compaction should keep the latest snapshot value");
}

void test_truncated_wal_tail_is_ignored() {
    TestDir dir("truncated_tail");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(9, text("stable"));
    }

    append_bytes(wal_path, {0xAA, 0xBB, 0xCC});

    KVStore reopened(db_path);
    const auto value = reopened.Get(9);
    require(value.has_value(), "valid WAL records should survive a truncated tail");
    require(as_string(*value) == "stable", "truncated WAL tail must be ignored");
}

void test_corrupted_wal_record_throws() {
    TestDir dir("corrupt_wal");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    {
        KVStore store(db_path);
        store.Put(5, text("first"));
        store.Put(6, text("second"));
    }

    flip_byte(wal_path, static_cast<std::streamoff>(sizeof(WalRecordHeader) + 1));

    bool threw = false;
    try {
        KVStore reopened(db_path);
        (void)reopened;
    } catch (const KVStoreError&) {
        threw = true;
    }
    require(threw, "corrupted WAL payload should raise KVStoreError");
}

void test_corrupted_snapshot_throws() {
    TestDir dir("corrupt_snapshot");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("snap"));
        store.Compact();
    }

    flip_byte(db_path, 0);

    bool threw = false;
    try {
        KVStore reopened(db_path);
        (void)reopened;
    } catch (const KVStoreError&) {
        threw = true;
    }
    require(threw, "corrupted snapshot magic should raise KVStoreError");
}

void test_unsupported_snapshot_version_throws() {
    TestDir dir("snapshot_version");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("versioned"));
        store.Compact();
    }

    write_u32_at(db_path, 8, 99);

    bool threw = false;
    try {
        KVStore reopened(db_path);
        (void)reopened;
    } catch (const KVStoreError&) {
        threw = true;
    }
    require(threw, "unsupported snapshot version should raise KVStoreError");
}

void test_crash_after_wal_fsync_recovers_latest_write() {
    TestDir dir("crash_wal_fsync");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child("wal_after_fsync", db_path);

    KVStore reopened(db_path);
    const auto stable = reopened.Get(1);
    const auto latest = reopened.Get(2);
    require(stable.has_value(), "stable key should persist after WAL-fsync crash");
    require(latest.has_value(), "latest WAL-synced key should be recovered after crash");
    require(as_string(*stable) == "stable", "stable key should preserve its value");
    require(as_string(*latest) == "latest", "replayed WAL should restore the latest synced value");
}

void test_crash_after_wal_fsync_recovers_latest_batch() {
    TestDir dir("crash_wal_batch");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child("wal_after_fsync_batch", db_path);

    KVStore reopened(db_path);
    const auto one = reopened.Get(1);
    const auto two = reopened.Get(2);
    const auto alpha = reopened.Get(std::string("alpha"));
    require(!one.has_value(), "batch delete should recover from WAL after crash");
    require(two.has_value() && as_string(*two) == "latest", "batch int put should recover from WAL after crash");
    require(alpha.has_value() && as_string(*alpha) == "batch-value",
            "batch string put should recover from WAL after crash");
}

void test_crash_after_snapshot_rename_recovers_consistent_state() {
    TestDir dir("crash_snapshot_rename");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child("snapshot_after_rename", db_path);

    KVStore reopened(db_path);
    const auto one = reopened.Get(1);
    const auto two = reopened.Get(2);
    require(one.has_value() && two.has_value(),
            "snapshot-rename crash should preserve all compacted keys");
    require(as_string(*one) == "one", "recovered state should keep key 1");
    require(as_string(*two) == "two", "recovered state should keep key 2");
}

void test_crash_before_snapshot_rename_replays_old_wal() {
    TestDir dir("crash_before_snapshot_rename");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child("snapshot_before_rename", db_path);

    KVStore reopened(db_path);
    const auto seven = reopened.Get(7);
    const auto eight = reopened.Get(8);
    require(seven.has_value() && eight.has_value(),
            "crash before snapshot rename should fall back to old snapshot plus WAL");
    require(as_string(*seven) == "seven", "recovered state should keep key 7");
    require(as_string(*eight) == "eight", "recovered state should keep key 8");
}

void test_crash_after_wal_rotation_recovers_snapshot() {
    TestDir dir("crash_wal_rotation");
    const std::string db_path = dir.file("store.dat");

    run_failpoint_child("wal_rotation_before_reopen", db_path);

    KVStore reopened(db_path);
    const auto ten = reopened.Get(10);
    const auto twenty = reopened.Get(20);
    require(ten.has_value() && twenty.has_value(),
            "crash after WAL rotation should preserve compacted snapshot state");
    require(as_string(*ten) == "ten", "recovered state should keep key 10");
    require(as_string(*twenty) == "twenty", "recovered state should keep key 20");
}

void test_concurrent_reads_and_writes() {
    TestDir dir("concurrency");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    std::thread writer([&store]() {
        for (int i = 0; i < 200; ++i) {
            store.Put(i, text("value_" + std::to_string(i)));
        }
    });

    std::thread reader([&store]() {
        for (int i = 0; i < 400; ++i) {
            const int key = i % 200;
            const auto value = store.Get(key);
            if (value.has_value()) {
                require(as_string(*value).rfind("value_", 0) == 0, "reader should only observe complete values");
            }
        }
    });

    writer.join();
    reader.join();

    KVStore reopened(db_path);
    for (int i = 0; i < 200; ++i) {
        const auto value = reopened.Get(i);
        require(value.has_value(), "all committed keys should persist after reopen");
    }
}

void test_many_concurrent_writers() {
    TestDir dir("many_writers");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    constexpr int kWriterCount = 8;
    constexpr int kWritesPerThread = 150;
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, writer_id]() {
            for (int index = 0; index < kWritesPerThread; ++index) {
                const int key = writer_id * 10000 + index;
                store.Put(key, text("writer_" + std::to_string(writer_id) + "_" + std::to_string(index)));
            }
        });
    }

    for (auto& writer : writers) {
        writer.join();
    }

    KVStore reopened(db_path);
    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        for (int index = 0; index < kWritesPerThread; ++index) {
            const int key = writer_id * 10000 + index;
            const auto value = reopened.Get(key);
            require(value.has_value(), "all concurrent writer keys should persist");
            require(as_string(*value) == "writer_" + std::to_string(writer_id) + "_" + std::to_string(index),
                    "concurrent writers must preserve each committed value");
        }
    }
}

void test_concurrent_compaction_with_writes() {
    TestDir dir("compact_with_writes");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    constexpr int kWriterCount = 4;
    constexpr int kWritesPerThread = 120;

    std::vector<std::thread> writers;
    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, writer_id]() {
            for (int index = 0; index < kWritesPerThread; ++index) {
                const int key = writer_id * 10000 + index;
                store.Put(key, text("value_" + std::to_string(writer_id) + "_" + std::to_string(index)));
                if (index % 20 == 0) {
                    store.Delete(key);
                    store.Put(key, text("value_" + std::to_string(writer_id) + "_" + std::to_string(index) + "_final"));
                }
            }
        });
    }

    std::thread compactor([&store]() {
        for (int round = 0; round < 6; ++round) {
            store.Compact();
        }
    });

    for (auto& writer : writers) {
        writer.join();
    }
    compactor.join();
    store.Compact();

    KVStore reopened(db_path);
    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        for (int index = 0; index < kWritesPerThread; ++index) {
            const int key = writer_id * 10000 + index;
            const auto value = reopened.Get(key);
            require(value.has_value(), "keys written during compaction should persist");
            std::string expected = "value_" + std::to_string(writer_id) + "_" + std::to_string(index);
            if (index % 20 == 0) {
                expected += "_final";
            }
            require(as_string(*value) == expected, "compaction must preserve the latest committed value");
        }
    }
}

void test_batching_metrics_are_reported() {
    TestDir dir("batch_metrics");
    const std::string db_path = dir.file("store.dat");
    KVStoreOptions options;
    options.max_batch_size = 8;
    options.max_batch_delay_us = 20000;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 8;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("value_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.enqueued_write_requests == kWriterCount, "metrics should count all enqueued writes");
    require(metrics.committed_write_requests == kWriterCount, "metrics should count all committed writes");
    require(metrics.committed_write_batches >= 1, "writer should report at least one committed batch");
    require(metrics.max_committed_batch_size > 1, "batched writer should group concurrent writes");
    require(metrics.wal_fsync_calls < metrics.committed_write_requests, "batching should reduce fsync count below write count");
    require(metrics.pending_queue_depth == 0, "queue should drain after all writers finish");
    require(histogram_total(metrics.write_latency_histogram) == metrics.committed_write_requests,
            "latency histogram should account for every committed write");
}

void test_recommended_profiles_are_distinct() {
    const KVStoreOptions balanced = RecommendedOptions(KVStoreProfile::kBalanced);
    const KVStoreOptions write_heavy = RecommendedOptions(KVStoreProfile::kWriteHeavy);
    const KVStoreOptions read_heavy = RecommendedOptions(KVStoreProfile::kReadHeavy);
    const KVStoreOptions low_latency = RecommendedOptions(KVStoreProfile::kLowLatency);

    require(write_heavy.max_batch_size > balanced.max_batch_size,
            "write-heavy profile should favor larger batches than balanced");
    require(read_heavy.max_batch_size < balanced.max_batch_size,
            "read-heavy profile should favor smaller batches than balanced");
    require(low_latency.max_batch_delay_us < balanced.max_batch_delay_us,
            "low-latency profile should shorten batch delay");
    require(write_heavy.auto_compact_wal_bytes_threshold > balanced.auto_compact_wal_bytes_threshold,
            "write-heavy profile should tolerate a larger WAL before compaction");
}

void test_concurrency_stress_profiles_are_distinct() {
    const ConcurrencyStressProfileConfig balanced =
        make_concurrency_stress_profile_config(ConcurrencyStressProfile::kBalanced);
    const ConcurrencyStressProfileConfig write_heavy =
        make_concurrency_stress_profile_config(ConcurrencyStressProfile::kWriteHeavy);
    const ConcurrencyStressProfileConfig compaction_heavy =
        make_concurrency_stress_profile_config(ConcurrencyStressProfile::kCompactionHeavy);

    require(write_heavy.writer_count > balanced.writer_count,
            "write-heavy stress profile should use more writers than balanced");
    require(write_heavy.options.max_batch_size > balanced.options.max_batch_size,
            "write-heavy stress profile should favor larger batches than balanced");
    require(compaction_heavy.compactor_count > balanced.compactor_count,
            "compaction-heavy stress profile should run more compactor threads");
    require(compaction_heavy.compaction_interval_ms < balanced.compaction_interval_ms,
            "compaction-heavy stress profile should compact more frequently than balanced");
    require(compaction_heavy.options.auto_compact_wal_bytes_threshold <
                balanced.options.auto_compact_wal_bytes_threshold,
            "compaction-heavy stress profile should compact at a smaller WAL threshold");
}

void test_options_to_json_reports_profile_fields() {
    const std::string json = OptionsToJson(RecommendedOptions(KVStoreProfile::kBalanced));
    require(!json.empty() && json.front() == '{' && json.back() == '}', "options json should be a JSON object");
    require(json.find("\"max_batch_size\":") != std::string::npos,
            "options json should include batching fields");
    require(json.find("\"adaptive_objective_enabled\":true") != std::string::npos,
            "options json should include boolean profile settings");
    require(json.find("\"auto_compact_wal_bytes_threshold\":") != std::string::npos,
            "options json should include compaction thresholds");
}

void test_metrics_to_json_reports_core_fields() {
    TestDir dir("metrics_json");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    store.Put(1, text("json"));
    store.Delete(1);

    const std::string json = MetricsToJson(store.GetMetrics());
    require(!json.empty() && json.front() == '{' && json.back() == '}', "metrics json should be a JSON object");
    require(json.find("\"committed_write_requests\":2") != std::string::npos,
            "metrics json should include committed write counts");
    require(json.find("\"write_latency_histogram\":[") != std::string::npos,
            "metrics json should include the latency histogram array");
    require(json.find("\"wal_fsync_calls\":") != std::string::npos,
            "metrics json should include fsync metrics");
}

void test_auto_compaction_triggers_and_preserves_state() {
    TestDir dir("auto_compaction");
    const std::string db_path = dir.file("store.dat");
    const std::string wal_path = db_path + ".wal";

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    options.auto_compact_wal_bytes_threshold = 128;

    {
        KVStore store(db_path, options);
        for (int i = 0; i < 12; ++i) {
            store.Put(i, text("auto_value_" + std::to_string(i)));
        }

        wait_until(
            [&store, &options]() {
                const KVStoreMetrics metrics = store.GetMetrics();
                return metrics.auto_compactions_completed >= 1 &&
                       metrics.wal_bytes_since_compaction < options.auto_compact_wal_bytes_threshold;
            },
            "auto compaction did not settle within the expected time");

        const KVStoreMetrics metrics = store.GetMetrics();
        require(metrics.auto_compactions_completed >= 1, "auto compaction should trigger after WAL crosses the threshold");
        require(metrics.wal_bytes_since_compaction < options.auto_compact_wal_bytes_threshold,
                "auto compaction should reset accumulated WAL bytes");
        require(file_size_or_zero(wal_path) < options.auto_compact_wal_bytes_threshold,
                "WAL should be rotated down after auto compaction");
    }

    KVStore reopened(db_path);
    for (int i = 0; i < 12; ++i) {
        const auto value = reopened.Get(i);
        require(value.has_value(), "auto compaction must preserve committed values");
        require(as_string(*value) == "auto_value_" + std::to_string(i),
                "reopened state should match values written before auto compaction");
    }
}

void test_manual_and_auto_compaction_metrics_coexist() {
    TestDir dir("manual_auto_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 2;
    options.max_batch_delay_us = 0;
    options.auto_compact_wal_bytes_threshold = 96;

    KVStore store(db_path, options);
    for (int i = 0; i < 6; ++i) {
        store.Put(i, text("payload_" + std::to_string(i)));
    }
    store.Compact();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.auto_compactions_completed >= 1, "auto compaction count should be visible in metrics");
    require(metrics.manual_compactions_completed == 1, "manual compaction count should be tracked separately");
    require(metrics.compact_requests == 1, "only explicit compaction requests should increment compact_requests");
}

void test_invalid_wal_ratio_triggers_auto_compaction() {
    TestDir dir("invalid_ratio_compaction");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 0;
    options.auto_compact_invalid_wal_ratio_percent = 50;

    {
        KVStore store(db_path, options);
        for (int i = 0; i < 12; ++i) {
            store.Put(42, text("overwrite_" + std::to_string(i)));
        }

        wait_until(
            [&store]() {
                const KVStoreMetrics metrics = store.GetMetrics();
                return metrics.auto_compactions_completed >= 1 &&
                       metrics.obsolete_wal_bytes_since_compaction <= metrics.live_wal_bytes_since_compaction;
            },
            "invalid WAL ratio auto compaction did not settle within the expected time");

        const KVStoreMetrics metrics = store.GetMetrics();
        require(metrics.auto_compactions_completed >= 1, "invalid WAL ratio should trigger auto compaction");
    }

    KVStore reopened(db_path);
    const auto value = reopened.Get(42);
    require(value.has_value(), "latest overwritten value should survive invalid-ratio compaction");
    require(as_string(*value) == "overwrite_11", "reopened state should contain the latest overwritten value");
}

void test_wal_obsolete_byte_metrics_are_consistent() {
    TestDir dir("wal_byte_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStore store(db_path);
    store.Put(1, text("first"));
    store.Put(1, text("second"));
    store.Delete(1);

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.wal_bytes_since_compaction == metrics.live_wal_bytes_since_compaction + metrics.obsolete_wal_bytes_since_compaction,
            "live and obsolete WAL bytes should partition the accumulated WAL size");
    require(metrics.obsolete_wal_bytes_since_compaction > 0,
            "overwrites and deletes should create obsolete WAL bytes");
}

void test_batch_wal_byte_limit_is_respected() {
    TestDir dir("batch_byte_limit");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 64;
    options.max_batch_wal_bytes = 60;
    options.max_batch_delay_us = 20000;

    KVStore store(db_path, options);

    constexpr int kWriterCount = 4;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("payload_1234567890"));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == kWriterCount, "all writes should commit under byte-limited batching");
    require(metrics.max_committed_batch_size <= 1, "small WAL byte limit should prevent multi-request batches");
}

void test_latency_histogram_tracks_write_requests() {
    TestDir dir("latency_histogram");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    for (int i = 0; i < 10; ++i) {
        store.Put(i, text("latency_" + std::to_string(i)));
    }
    store.Delete(5);

    const KVStoreMetrics metrics = store.GetMetrics();
    require(histogram_total(metrics.write_latency_histogram) == metrics.committed_write_requests,
            "histogram total should equal the number of committed writes");
    require(histogram_total(metrics.write_latency_histogram) == 11,
            "histogram should include puts and deletes but not compaction");
}

void test_adaptive_batching_expands_batch_under_queue_pressure() {
    TestDir dir("adaptive_batching");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 2;
    options.max_batch_delay_us = 20000;
    options.adaptive_batching_enabled = true;
    options.adaptive_queue_depth_threshold = 4;
    options.adaptive_batch_size_multiplier = 4;

    KVStore store(db_path, options);

    constexpr int kWriterCount = 8;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("adaptive_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == kWriterCount, "adaptive batching should still commit all queued writes");
    require(metrics.max_committed_batch_size > options.max_batch_size,
            "adaptive batching should grow batch size beyond the base limit under queue pressure");
    require(metrics.adaptive_batches_completed >= 1, "adaptive batching should report at least one adaptive batch");
}

void test_percentile_and_writer_metrics_are_reported() {
    TestDir dir("writer_metrics");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 6;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("writer_metric_payload_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    wait_until(
        [&store]() {
            const KVStoreMetrics metrics = store.GetMetrics();
            return metrics.writer_wait_events >= 1 && metrics.writer_wait_time_us > 0;
        },
        "writer thread did not report idle waiting after draining the queue");

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.max_pending_queue_depth >= 1, "queue high-water mark should be tracked");
    require(metrics.last_committed_batch_wal_bytes > 0, "last committed batch WAL bytes should be reported");
    require(metrics.max_committed_batch_wal_bytes >= metrics.last_committed_batch_wal_bytes,
            "max batch WAL bytes should be at least the last batch size");
    require(metrics.approx_write_latency_p50_us > 0, "p50 write latency should be reported");
    require(metrics.approx_write_latency_p95_us >= metrics.approx_write_latency_p50_us,
            "p95 write latency should not be below p50");
    require(metrics.approx_write_latency_p99_us >= metrics.approx_write_latency_p95_us,
            "p99 write latency should not be below p95");
}

void test_adaptive_flush_shortens_batch_delay() {
    TestDir dir("adaptive_flush");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 50000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 2;
    options.adaptive_flush_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 1000;

    KVStore store(db_path, options);

    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};

    std::thread writer1([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(1, text("flush_1"));
    });
    std::thread writer2([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(2, text("flush_2"));
    });

    while (ready.load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    wait_until(
        [&store]() {
            const KVStoreMetrics metrics = store.GetMetrics();
            return metrics.committed_write_requests >= 2 &&
                   metrics.committed_write_batches >= 1 &&
                   metrics.adaptive_flush_batches_completed >= 1;
        },
        "adaptive flush should commit the first shortened-delay batch before the delayed write arrives");

    std::thread writer3([&store]() {
        store.Put(3, text("flush_3"));
    });

    writer1.join();
    writer2.join();
    writer3.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.committed_write_requests == 3, "adaptive flush test should commit all writes");
    require(metrics.committed_write_batches >= 2, "adaptive flush should flush the early batch before the delayed write arrives");
    require(metrics.adaptive_flush_batches_completed >= 1, "adaptive flush should record at least one shortened-delay batch");
    require(metrics.min_effective_batch_delay_us > 0 &&
                metrics.min_effective_batch_delay_us < options.max_batch_delay_us,
            "adaptive flush should reduce the effective batch delay below the configured base delay");
}

void test_compaction_long_term_metrics_accumulate() {
    TestDir dir("compaction_totals");
    const std::string db_path = dir.file("store.dat");

    KVStore store(db_path);
    store.Put(1, text("first"));
    store.Compact();
    store.Put(2, text("second"));
    store.Delete(1);
    store.Compact();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.manual_compactions_completed == 2, "manual compaction count should reflect both runs");
    require(metrics.total_snapshot_bytes_written > sizeof(uint32_t),
            "compaction should accumulate snapshot bytes written over time");
    require(metrics.total_wal_bytes_reclaimed_by_compaction > 0,
            "compaction should accumulate reclaimed WAL bytes over time");
}

void test_latency_target_adaptive_flush_kicks_in() {
    TestDir dir("latency_target_flush");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 50000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 1000;
    options.adaptive_flush_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 1000;
    options.adaptive_latency_target_p95_us = 50;

    KVStore store(db_path, options);
    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("warmup_" + std::to_string(i)));
    }

    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::thread writer1([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(10, text("latency_target_a"));
    });
    std::thread writer2([&store, &ready, &start_signal]() {
        wait_for_start(ready, start_signal, 2);
        store.Put(11, text("latency_target_b"));
    });

    while (ready.load(std::memory_order_acquire) < 2) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::thread writer3([&store]() {
        store.Put(12, text("latency_target_c"));
    });

    writer1.join();
    writer2.join();
    writer3.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_latency_target_batches_completed >= 1,
            "latency target should trigger at least one earlier-flush batch");
    require(metrics.min_effective_batch_delay_us > 0 &&
                metrics.min_effective_batch_delay_us < options.max_batch_delay_us,
            "latency target should reduce effective batch delay below the base delay");
}

void test_fsync_pressure_can_relax_batch_delay() {
    TestDir dir("fsync_pressure");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 900;
    options.adaptive_fsync_pressure_delay_multiplier = 10;
    options.adaptive_fsync_pressure_max_batch_delay_us = 10000;

    KVStore store(db_path, options);
    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("pressure_" + std::to_string(i)));
    }

    store.Put(100, text("pressure_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_fsync_pressure_batches_completed >= 1,
            "high fsync pressure should trigger a relaxed-delay batch");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "fsync pressure should expand the effective batch delay above the base delay");
    require(metrics.observed_fsync_pressure_per_1000_writes >=
                options.adaptive_fsync_pressure_per_1000_writes_threshold,
            "observed fsync pressure metric should reflect the singleton-write workload");
    require(metrics.max_effective_batch_delay_us >= metrics.last_effective_batch_delay_us,
            "max effective batch delay should track the expanded delay");
}

void test_recent_window_metrics_capture_bursts() {
    TestDir dir("recent_window");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 4;
    options.max_batch_delay_us = 5000;
    options.adaptive_recent_window_batches = 4;
    options.adaptive_recent_write_sample_limit = 16;
    KVStore store(db_path, options);

    constexpr int kWriterCount = 4;
    std::atomic<int> ready {0};
    std::atomic<bool> start_signal {false};
    std::vector<std::thread> writers;

    for (int writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&store, &ready, &start_signal, writer_id]() {
            wait_for_start(ready, start_signal, kWriterCount);
            store.Put(writer_id, text("recent_" + std::to_string(writer_id)));
        });
    }

    while (ready.load(std::memory_order_acquire) < kWriterCount) {
        std::this_thread::yield();
    }
    start_signal.store(true, std::memory_order_release);

    for (auto& writer : writers) {
        writer.join();
    }

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_window_batch_count >= 1, "recent batch window should track completed batches");
    require(metrics.recent_peak_queue_depth >= 2,
            "recent queue peak should reflect that the burst created queue pressure");
    require(metrics.recent_avg_batch_size >= 1, "recent batch window should track average batch size");
    require(metrics.recent_observed_write_latency_p95_us > 0,
            "recent latency window should expose a non-zero recent p95");
}

void test_read_heavy_signal_prefers_shorter_batches() {
    TestDir dir("read_heavy");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 30000;
    options.adaptive_read_heavy_read_per_1000_ops_threshold = 800;
    options.adaptive_read_heavy_delay_divisor = 10;
    options.adaptive_read_heavy_batch_size_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 500;
    KVStore store(db_path, options);

    for (int i = 0; i < 128; ++i) {
        (void)store.Get(i);
    }

    std::thread writer1([&store]() {
        store.Put(1, text("read_heavy_a"));
    });
    std::thread writer2([&store]() {
        store.Put(2, text("read_heavy_b"));
    });
    writer1.join();
    writer2.join();

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.adaptive_read_heavy_batches_completed >= 1,
            "read-heavy workload should trigger reader-friendly batching");
    require(metrics.recent_read_ratio_per_1000_ops >= 800,
            "recent read ratio should reflect the read-heavy workload");
    require(metrics.last_effective_batch_delay_us < options.max_batch_delay_us,
            "read-heavy workload should shorten the effective batch delay");
}

void test_compaction_pressure_signal_relaxes_batch_delay() {
    TestDir dir("compaction_pressure");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_compaction_pressure_delay_multiplier = 5;
    KVStore store(db_path, options);

    for (int i = 0; i < 6; ++i) {
        store.Put(7, text("obsolete_" + std::to_string(i)));
    }
    store.Put(8, text("pressure_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.observed_obsolete_wal_ratio_percent >= 50,
            "obsolete WAL ratio should reflect repeated overwrites");
    require(metrics.adaptive_compaction_pressure_batches_completed >= 1,
            "high obsolete WAL ratio should relax batching before compaction");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "compaction pressure should expand the effective batch delay");
}

void test_wal_growth_signal_relaxes_batch_delay() {
    TestDir dir("wal_growth");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 1000;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 40;
    options.adaptive_wal_growth_delay_multiplier = 4;
    options.adaptive_wal_growth_max_batch_delay_us = 8000;
    options.adaptive_recent_window_batches = 8;
    KVStore store(db_path, options);

    for (int i = 0; i < 4; ++i) {
        store.Put(i, text("payload_abcdefghijklmnopqrstuvwxyz_" + std::to_string(i)));
    }
    store.Put(99, text("growth_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_avg_batch_wal_bytes >= options.adaptive_wal_growth_bytes_per_batch_threshold,
            "recent WAL growth metric should reflect large write batches");
    require(metrics.adaptive_wal_growth_batches_completed >= 1,
            "large recent WAL growth should relax the next batch delay");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "WAL growth signal should expand the effective batch delay");
}

void test_objective_policy_prefers_short_delay_under_latency_pressure() {
    TestDir dir("objective_short_delay");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 20000;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 4;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_fsync_weight = 0;
    options.adaptive_objective_compaction_weight = 0;
    options.adaptive_objective_wal_growth_weight = 0;
    options.adaptive_objective_short_delay_score_threshold = 500;
    options.adaptive_objective_short_delay_divisor = 10;
    options.adaptive_flush_min_batch_delay_us = 500;
    options.adaptive_latency_target_p95_us = 1000;
    KVStore store(db_path, options);

    store.Put(1, text("objective_latency_a"));
    store.Put(2, text("objective_latency_b"));
    store.Put(3, text("objective_latency_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_observed_write_latency_p95_us >= options.adaptive_latency_target_p95_us,
            "recent p95 should exceed the configured latency target");
    require(metrics.adaptive_objective_short_delay_batches_completed >= 1,
            "objective policy should shorten the batch delay under latency pressure");
    require(metrics.last_effective_batch_delay_us < options.max_batch_delay_us,
            "objective pressure score should shorten the effective batch delay");
    require(metrics.last_objective_pressure_score > metrics.last_objective_cost_score,
            "objective pressure score should dominate the cost score in the latency-heavy scenario");
    require(metrics.last_objective_balance_score > 0,
            "objective balance should be positive when pressure favors shorter delays");
}

void test_objective_policy_prefers_long_delay_under_cost_pressure() {
    TestDir dir("objective_long_delay");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 500;
    options.adaptive_recent_window_batches = 8;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 0;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_fsync_weight = 2;
    options.adaptive_objective_compaction_weight = 2;
    options.adaptive_objective_wal_growth_weight = 1;
    options.adaptive_objective_long_delay_score_threshold = 500;
    options.adaptive_objective_long_delay_multiplier = 6;
    options.adaptive_objective_max_batch_delay_us = 5000;
    options.adaptive_fsync_pressure_per_1000_writes_threshold = 800;
    options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold = 50;
    options.adaptive_wal_growth_bytes_per_batch_threshold = 40;
    KVStore store(db_path, options);

    for (int i = 0; i < 6; ++i) {
        store.Put(42, text("objective_cost_payload_abcdefghijklmnopqrstuvwxyz_" + std::to_string(i)));
    }
    store.Put(99, text("objective_cost_probe_payload"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.observed_fsync_pressure_per_1000_writes >= options.adaptive_fsync_pressure_per_1000_writes_threshold,
            "recent fsync pressure should exceed the configured objective cost scale");
    require(metrics.observed_obsolete_wal_ratio_percent >= options.adaptive_compaction_pressure_obsolete_ratio_percent_threshold,
            "obsolete WAL ratio should exceed the configured objective cost scale");
    require(metrics.adaptive_objective_long_delay_batches_completed >= 1,
            "objective policy should relax the batch delay when cost signals dominate");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "objective cost score should expand the effective batch delay");
    require(metrics.last_objective_cost_score > metrics.last_objective_pressure_score,
            "objective cost score should dominate the pressure score in the cost-heavy scenario");
    require(metrics.last_objective_balance_score < 0,
            "objective balance should be negative when cost favors longer delays");
}

void test_objective_throughput_score_can_dominate_latency_pressure() {
    TestDir dir("objective_throughput");
    const std::string db_path = dir.file("store.dat");

    KVStoreOptions options;
    options.max_batch_size = 1;
    options.max_batch_delay_us = 500;
    options.adaptive_recent_window_batches = 8;
    options.adaptive_objective_enabled = true;
    options.adaptive_objective_queue_weight = 0;
    options.adaptive_objective_latency_weight = 1;
    options.adaptive_objective_read_weight = 0;
    options.adaptive_objective_throughput_weight = 6;
    options.adaptive_objective_target_batch_size = 8;
    options.adaptive_objective_fsync_weight = 0;
    options.adaptive_objective_compaction_weight = 0;
    options.adaptive_objective_wal_growth_weight = 0;
    options.adaptive_objective_long_delay_score_threshold = 250;
    options.adaptive_objective_long_delay_multiplier = 6;
    options.adaptive_objective_max_batch_delay_us = 5000;
    options.adaptive_flush_min_batch_delay_us = 100;
    options.adaptive_latency_target_p95_us = 50;
    KVStore store(db_path, options);

    store.Put(1, text("objective_tp_a"));
    store.Put(2, text("objective_tp_b"));
    store.Put(3, text("objective_tp_probe"));

    const KVStoreMetrics metrics = store.GetMetrics();
    require(metrics.recent_observed_write_latency_p95_us >= options.adaptive_latency_target_p95_us,
            "recent p95 should still reflect latency pressure");
    require(metrics.recent_avg_batch_size < options.adaptive_objective_target_batch_size,
            "recent average batch size should remain below the throughput target");
    require(metrics.adaptive_objective_throughput_batches_completed >= 1,
            "objective controller should record batches influenced by throughput deficit");
    require(metrics.last_objective_throughput_score > 0,
            "objective controller should expose a non-zero throughput score");
    require(metrics.last_objective_cost_score > metrics.last_objective_pressure_score,
            "throughput deficit should outweigh latency pressure in this scenario");
    require(metrics.last_objective_mode < 0,
            "objective mode should report a long-delay decision when throughput dominates");
    require(metrics.last_effective_batch_delay_us > options.max_batch_delay_us,
            "throughput-dominated objective control should expand the effective batch delay");
}

void run_concurrency_stress_test(int duration_seconds, ConcurrencyStressProfile profile) {
    TestDir dir("concurrency_stress");
    const std::string db_path = dir.file("store.dat");

    const ConcurrencyStressProfileConfig config = make_concurrency_stress_profile_config(profile);
    const int total_keys = config.writer_count * config.keys_per_writer;
    std::vector<std::vector<std::optional<std::string>>> expected_by_writer(
        static_cast<size_t>(config.writer_count),
        std::vector<std::optional<std::string>>(static_cast<size_t>(config.keys_per_writer)));

    KVStoreMetrics final_metrics;
    {
        KVStore store(db_path, config.options);
        std::atomic<bool> stop {false};
        std::vector<std::thread> threads;

        for (int writer_id = 0; writer_id < config.writer_count; ++writer_id) {
            threads.emplace_back([&store, &stop, &config, &expected_by_writer, writer_id]() {
                std::mt19937 gen(15000 + writer_id);
                std::uniform_int_distribution<int> key_dist(0, config.keys_per_writer - 1);
                std::uniform_int_distribution<int> op_dist(0, 9);
                uint64_t version = 0;
                auto& expected = expected_by_writer[static_cast<size_t>(writer_id)];

                while (!stop.load(std::memory_order_acquire)) {
                    const int selector = op_dist(gen);
                    if (selector < 3) {
                        const int local_key = key_dist(gen);
                        const int global_key = writer_id * config.keys_per_writer + local_key;
                        store.Delete(global_key);
                        expected[static_cast<size_t>(local_key)] = std::nullopt;
                        continue;
                    }

                    if (selector < 7) {
                        const int local_key = key_dist(gen);
                        const int global_key = writer_id * config.keys_per_writer + local_key;
                        const std::string payload =
                            "stress_" + std::to_string(writer_id) + "_" +
                            std::to_string(local_key) + "_" + std::to_string(version++);
                        store.Put(global_key, text(payload));
                        expected[static_cast<size_t>(local_key)] = payload;
                        continue;
                    }

                    std::vector<BatchWriteOperation> operations;
                    std::vector<std::pair<int, std::optional<std::string>>> applied;
                    operations.reserve(static_cast<size_t>(config.batch_width));
                    applied.reserve(static_cast<size_t>(config.batch_width));
                    for (int batch_index = 0; batch_index < config.batch_width; ++batch_index) {
                        const int local_key = key_dist(gen);
                        const int global_key = writer_id * config.keys_per_writer + local_key;
                        const bool do_delete = (op_dist(gen) % 4) == 0;
                        if (do_delete) {
                            operations.push_back(BatchWriteOperation::DeleteInt(global_key));
                            applied.push_back({local_key, std::nullopt});
                            continue;
                        }
                        const std::string payload =
                            "stress_batch_" + std::to_string(writer_id) + "_" +
                            std::to_string(local_key) + "_" + std::to_string(version++);
                        operations.push_back(BatchWriteOperation::PutInt(global_key, text(payload)));
                        applied.push_back({local_key, payload});
                    }
                    store.WriteBatch(operations);
                    for (const auto& [local_key, value] : applied) {
                        expected[static_cast<size_t>(local_key)] = value;
                    }
                }
            });
        }

        for (int reader_id = 0; reader_id < config.reader_count; ++reader_id) {
            threads.emplace_back([&store, &stop, total_keys, reader_id]() {
                std::mt19937 gen(25000 + reader_id);
                std::uniform_int_distribution<int> key_dist(0, total_keys - 1);
                while (!stop.load(std::memory_order_acquire)) {
                    const auto value = store.Get(key_dist(gen));
                    if (value.has_value()) {
                        const std::string text_value = as_string(*value);
                        require(text_value.rfind("stress_", 0) == 0,
                                "concurrency stress readers should only observe complete values");
                    }
                }
            });
        }

        for (int observer_id = 0; observer_id < config.metrics_reader_count; ++observer_id) {
            threads.emplace_back([&store, &stop]() {
                uint64_t last_enqueued = 0;
                uint64_t last_committed = 0;
                while (!stop.load(std::memory_order_acquire)) {
                    const KVStoreMetrics metrics = store.GetMetrics();
                    require(metrics.enqueued_write_requests >= last_enqueued,
                            "enqueued write requests should be monotonic during stress");
                    require(metrics.committed_write_requests >= last_committed,
                            "committed write requests should be monotonic during stress");
                    require(metrics.pending_queue_depth <= metrics.max_pending_queue_depth,
                            "current queue depth should stay below the historical high watermark");
                    require(metrics.last_committed_batch_size <= metrics.max_committed_batch_size,
                            "last batch size should not exceed the historical batch maximum");
                    last_enqueued = metrics.enqueued_write_requests;
                    last_committed = metrics.committed_write_requests;
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                }
            });
        }

        for (int compactor_id = 0; compactor_id < config.compactor_count; ++compactor_id) {
            threads.emplace_back([&store, &stop, &config]() {
                while (!stop.load(std::memory_order_acquire)) {
                    store.Compact();
                    std::this_thread::sleep_for(std::chrono::milliseconds(config.compaction_interval_ms));
                }
            });
        }

        std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
        stop.store(true, std::memory_order_release);

        for (auto& thread : threads) {
            thread.join();
        }

        store.Compact();
        final_metrics = store.GetMetrics();
    }

    require(final_metrics.committed_write_requests > 0,
            "concurrency stress should commit at least one write");
    require(final_metrics.max_pending_queue_depth > 0,
            "concurrency stress should drive the pending queue above zero");
    require(final_metrics.manual_compactions_completed >= static_cast<uint64_t>(config.compactor_count),
            "concurrency stress should complete manual compactions");

    KVStore reopened(db_path);
    for (int writer_id = 0; writer_id < config.writer_count; ++writer_id) {
        for (int local_key = 0; local_key < config.keys_per_writer; ++local_key) {
            const int global_key = writer_id * config.keys_per_writer + local_key;
            const auto actual = reopened.Get(global_key);
            const auto& expected =
                expected_by_writer[static_cast<size_t>(writer_id)][static_cast<size_t>(local_key)];
            if (!expected.has_value()) {
                require(!actual.has_value(),
                        "deleted writer-owned keys should remain deleted after stress restart");
                continue;
            }
            require(actual.has_value(), "expected live stress key missing after restart");
            require(as_string(*actual) == *expected,
                    "reopened state should match the stress oracle for writer-owned keys");
        }
    }
}

void run_soak_test(int duration_seconds, SoakProfile profile) {
    TestDir dir("soak");
    const std::string db_path = dir.file("store.dat");

    const SoakProfileConfig config = make_soak_profile_config(profile);
    KVStore store(db_path, config.options);

    std::atomic<bool> stop {false};
    std::atomic<uint64_t> operation_sequence {0};
    std::map<int, std::pair<uint64_t, std::optional<std::string>>> oracle;
    std::mutex oracle_mutex;
    std::vector<std::thread> threads;

    for (int writer_id = 0; writer_id < config.writer_count; ++writer_id) {
        threads.emplace_back([&store, &stop, &oracle, &oracle_mutex, &config, &operation_sequence, writer_id]() {
            std::mt19937 gen(9000 + writer_id);
            std::uniform_int_distribution<int> key_dist(0, config.key_space - 1);
            std::uniform_int_distribution<int> op_dist(0, 9);
            uint64_t version = 0;
            while (!stop.load(std::memory_order_acquire)) {
                const int key = key_dist(gen);
                const bool do_delete = op_dist(gen) < 3;
                const uint64_t seq = operation_sequence.fetch_add(1, std::memory_order_relaxed);
                if (do_delete) {
                    store.Delete(key);
                    std::lock_guard<std::mutex> lock(oracle_mutex);
                    auto& slot = oracle[key];
                    if (seq >= slot.first) {
                        slot = {seq, std::nullopt};
                    }
                } else {
                    const std::string payload =
                        "value_" + std::to_string(writer_id) + "_" + std::to_string(key) + "_" + std::to_string(version++);
                    store.Put(key, text(payload));
                    std::lock_guard<std::mutex> lock(oracle_mutex);
                    auto& slot = oracle[key];
                    if (seq >= slot.first) {
                        slot = {seq, payload};
                    }
                }
            }
        });
    }

    for (int reader_id = 0; reader_id < config.reader_count; ++reader_id) {
        threads.emplace_back([&store, &stop, &config, reader_id]() {
            std::mt19937 gen(12000 + reader_id);
            std::uniform_int_distribution<int> key_dist(0, config.key_space - 1);
            while (!stop.load(std::memory_order_acquire)) {
                const auto value = store.Get(key_dist(gen));
                if (value.has_value()) {
                    require(as_string(*value).rfind("value_", 0) == 0, "soak readers should only observe complete values");
                }
            }
        });
    }

    threads.emplace_back([&store, &stop, &config]() {
        while (!stop.load(std::memory_order_acquire)) {
            store.Compact();
            std::this_thread::sleep_for(std::chrono::milliseconds(config.compaction_interval_ms));
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
    stop.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    store.Compact();

    std::map<int, std::optional<std::string>> expected;
    {
        std::lock_guard<std::mutex> lock(oracle_mutex);
        for (const auto& [key, value] : oracle) {
            expected[key] = value.second;
        }
    }

    KVStore reopened(db_path);
    for (const auto& [key, expected_value] : expected) {
        const auto actual = reopened.Get(key);
        if (!expected_value.has_value()) {
            require(!actual.has_value(), "deleted keys should remain deleted after soak restart");
            continue;
        }
        require(actual.has_value(), "expected live key missing after soak restart");
        require(as_string(*actual) == *expected_value, "reopened state should match soak oracle");
    }
}

void run_benchmark() {
    const BenchmarkResult result = run_benchmark_capture(
        make_benchmark_config("default", 8, 4, 3000, 50000));
    const KVStoreMetrics& metrics = result.metrics;

    std::cout << "[BENCH] duration_s=" << result.duration_s
              << " writes=" << result.writes
              << " reads=" << result.reads
              << " write_ops_per_s=" << result.write_ops_per_s
              << " read_ops_per_s=" << result.read_ops_per_s
              << " avg_write_latency_us=" << result.avg_write_latency_us
              << " p50_write_latency_us=" << metrics.approx_write_latency_p50_us
              << " p95_write_latency_us=" << metrics.approx_write_latency_p95_us
              << " p99_write_latency_us=" << metrics.approx_write_latency_p99_us
              << " recent_read_ratio_per_1000_ops=" << metrics.recent_read_ratio_per_1000_ops
              << " recent_p95_write_latency_us=" << metrics.recent_observed_write_latency_p95_us
              << " recent_peak_queue_depth=" << metrics.recent_peak_queue_depth
              << " recent_avg_batch_size=" << metrics.recent_avg_batch_size
              << " recent_avg_batch_wal_bytes=" << metrics.recent_avg_batch_wal_bytes
              << " recent_window_batch_count=" << metrics.recent_window_batch_count
              << " observed_fsync_pressure_per_1000_writes=" << metrics.observed_fsync_pressure_per_1000_writes
              << " observed_obsolete_wal_ratio_percent=" << metrics.observed_obsolete_wal_ratio_percent
              << " objective_pressure_score=" << metrics.last_objective_pressure_score
              << " objective_cost_score=" << metrics.last_objective_cost_score
              << " objective_throughput_score=" << metrics.last_objective_throughput_score
              << " objective_balance_score=" << metrics.last_objective_balance_score
              << " objective_mode=" << metrics.last_objective_mode
              << " last_batch_delay_us=" << metrics.last_effective_batch_delay_us
              << " min_batch_delay_us=" << metrics.min_effective_batch_delay_us
              << " max_batch_delay_us=" << metrics.max_effective_batch_delay_us
              << " committed_batches=" << metrics.committed_write_batches
              << " max_batch_size=" << metrics.max_committed_batch_size
              << " max_batch_wal_bytes=" << metrics.max_committed_batch_wal_bytes
              << " adaptive_batches=" << metrics.adaptive_batches_completed
              << " adaptive_flush_batches=" << metrics.adaptive_flush_batches_completed
              << " latency_target_batches=" << metrics.adaptive_latency_target_batches_completed
              << " fsync_pressure_batches=" << metrics.adaptive_fsync_pressure_batches_completed
              << " read_heavy_batches=" << metrics.adaptive_read_heavy_batches_completed
              << " objective_throughput_batches=" << metrics.adaptive_objective_throughput_batches_completed
              << " compaction_pressure_batches=" << metrics.adaptive_compaction_pressure_batches_completed
              << " wal_growth_batches=" << metrics.adaptive_wal_growth_batches_completed
              << " objective_short_delay_batches=" << metrics.adaptive_objective_short_delay_batches_completed
              << " objective_long_delay_batches=" << metrics.adaptive_objective_long_delay_batches_completed
              << " wal_fsync_calls=" << metrics.wal_fsync_calls
              << " wal_bytes_written=" << metrics.wal_bytes_written
              << " snapshot_bytes_written_total=" << metrics.total_snapshot_bytes_written
              << " wal_bytes_reclaimed_total=" << metrics.total_wal_bytes_reclaimed_by_compaction
              << " writer_wait_events=" << metrics.writer_wait_events
              << " writer_wait_time_us=" << metrics.writer_wait_time_us
              << " queue_high_watermark=" << metrics.max_pending_queue_depth
              << " recent_batch_fill_per_1000=" << metrics.recent_batch_fill_per_1000
              << " latency_histogram=";
    for (size_t i = 0; i < metrics.write_latency_histogram.size(); ++i) {
        if (i != 0) {
            std::cout << ',';
        }
        std::cout << metrics.write_latency_histogram[i];
    }
    std::cout
              << std::endl;
}

void run_benchmark_json() {
    TestDir dir("bench_json");
    const std::string db_path = dir.file("store.dat");
    KVStoreOptions options;
    options.max_batch_size = 16;
    options.max_batch_delay_us = 1000;
    options.adaptive_flush_enabled = true;
    options.adaptive_flush_queue_depth_threshold = 4;
    options.adaptive_flush_delay_divisor = 4;
    options.adaptive_flush_min_batch_delay_us = 100;

    KVStore store(db_path, options);
    for (int i = 0; i < 128; ++i) {
        store.Put(i, text("bench_json_" + std::to_string(i)));
    }
    for (int i = 0; i < 256; ++i) {
        (void)store.Get(i % 128);
    }

    std::cout << MetricsToJson(store.GetMetrics()) << std::endl;
}

void run_benchmark_baseline_json() {
    const BenchmarkResult result = run_benchmark_capture(
        make_benchmark_config("default-baseline", 8, 4, 3000, 50000));
    std::cout << BenchmarkResultToJson(result) << std::endl;
}

}  // namespace

int main(int argc, char* argv[]) {
    g_program_path = std::filesystem::absolute(argv[0]).string();
    if (argc > 1) {
        const std::string command = argv[1];
        if (command == "bench") {
            run_benchmark();
            return 0;
        }
        if (command == "bench-json") {
            run_benchmark_json();
            return 0;
        }
        if (command == "bench-baseline-json") {
            run_benchmark_baseline_json();
            return 0;
        }
        if (command == "compare-baseline") {
            if (argc < 4 || argc > 7) {
                std::cerr << "Usage: kv_test compare-baseline <baseline_json> <candidate_json> [min_write_ratio_pct min_read_ratio_pct max_latency_ratio_pct]" << std::endl;
                return 1;
            }
            const double min_write_ratio_pct = argc > 4 ? std::stod(argv[4]) : 85.0;
            const double min_read_ratio_pct = argc > 5 ? std::stod(argv[5]) : 85.0;
            const double max_latency_ratio_pct = argc > 6 ? std::stod(argv[6]) : 125.0;
            return run_compare_benchmark_baseline(argv[2], argv[3], min_write_ratio_pct, min_read_ratio_pct, max_latency_ratio_pct);
        }
        if (command == "trend-baselines") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test trend-baselines <baseline_dir>" << std::endl;
                return 1;
            }
            return run_benchmark_trend(argv[2]);
        }
        if (command == "profile-json") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test profile-json <balanced|write-heavy|read-heavy|low-latency>" << std::endl;
                return 1;
            }
            return run_profile_json(argv[2]);
        }
        if (command == "soak") {
            const int duration_seconds = argc > 2 ? std::stoi(argv[2]) : 10;
            const std::string profile_name = argc > 3 ? argv[3] : "balanced";
            const auto profile = parse_soak_profile_name(profile_name);
            if (!profile.has_value()) {
                std::cerr << "Unknown soak profile: " << profile_name << std::endl;
                return 1;
            }
            run_soak_test(duration_seconds, *profile);
            return 0;
        }
        if (command == "concurrency-stress") {
            const int duration_seconds = argc > 2 ? std::stoi(argv[2]) : 10;
            const std::string profile_name = argc > 3 ? argv[3] : "balanced";
            const auto profile = parse_concurrency_stress_profile_name(profile_name);
            if (!profile.has_value()) {
                std::cerr << "Unknown concurrency stress profile: " << profile_name << std::endl;
                return 1;
            }
            run_concurrency_stress_test(duration_seconds, *profile);
            return 0;
        }
        if (command == "inspect-format") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test inspect-format <db_path>" << std::endl;
                return 1;
            }
            return run_inspect_format(argv[2]);
        }
        if (command == "rewrite-format") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test rewrite-format <db_path>" << std::endl;
                return 1;
            }
            return run_rewrite_format(argv[2]);
        }
        if (command == "verify-format") {
            if (argc != 3) {
                std::cerr << "Usage: kv_test verify-format <db_path>" << std::endl;
                return 1;
            }
            return run_verify_format(argv[2]);
        }
        if (command == "compat-matrix") {
            return run_compatibility_matrix();
        }
        if (command == "fault-inject") {
            if (argc != 4) {
                std::cerr << "Usage: kv_test fault-inject <scenario> <db_path>" << std::endl;
                return 1;
            }
            return run_fault_injection_scenario(argv[2], argv[3]);
        }
        std::cerr << "Unknown command: " << command << '\n';
        std::cerr << "Usage: kv_test [bench|bench-json|bench-baseline-json|compare-baseline|trend-baselines|profile-json|soak|concurrency-stress|inspect-format|rewrite-format|verify-format|compat-matrix|fault-inject]" << std::endl;
        return 1;
    }

    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"basic persistence", test_basic_persistence},
        {"string keys do not collide with int keys", test_string_keys_do_not_collide_with_int_keys},
        {"string scan returns sorted range", test_string_scan_returns_sorted_range},
        {"binary keys do not collide with other namespaces", test_binary_keys_do_not_collide_with_other_namespaces},
        {"batch write persists mixed key types", test_write_batch_persists_mixed_key_types},
        {"batch write mixes put and delete", test_write_batch_mixes_put_and_delete},
        {"batch write preserves operation order", test_write_batch_preserves_operation_order},
        {"put copies input value", test_put_copies_input_value},
        {"recovery from WAL without compaction", test_recovery_from_wal_without_compaction},
        {"legacy v1 snapshot and wal are readable", test_legacy_v1_snapshot_and_wal_are_readable},
        {"legacy v1 rewrite upgrades snapshot to v2", test_legacy_v1_rewrite_upgrades_snapshot_to_v2},
        {"inspect format reports key type counts", test_inspect_format_reports_key_type_counts},
        {"inspect format recommends rewrite for legacy v1", test_inspect_format_recommends_rewrite_for_legacy_v1},
        {"verify format accepts current layout", test_verify_format_accepts_current_layout},
        {"verify format rejects legacy layout", test_verify_format_rejects_legacy_layout},
        {"compatibility matrix command succeeds", test_compatibility_matrix_command_succeeds},
        {"benchmark result json reports summary and metrics", test_benchmark_result_json_reports_summary_and_metrics},
        {"compare benchmark baseline passes within thresholds", test_compare_benchmark_baseline_passes_within_thresholds},
        {"compare benchmark baseline rejects regression", test_compare_benchmark_baseline_rejects_regression},
        {"benchmark trend summarizes history", test_benchmark_trend_summarizes_history},
        {"soak profiles are distinct", test_soak_profiles_are_distinct},
        {"ordering and updates", test_ordering_and_updates},
        {"compaction persists snapshot and resets WAL", test_compaction_persists_snapshot_and_resets_wal},
        {"truncated WAL tail is ignored", test_truncated_wal_tail_is_ignored},
        {"corrupted WAL record throws", test_corrupted_wal_record_throws},
        {"corrupted snapshot throws", test_corrupted_snapshot_throws},
        {"unsupported snapshot version throws", test_unsupported_snapshot_version_throws},
        {"crash after wal fsync recovers latest write", test_crash_after_wal_fsync_recovers_latest_write},
        {"crash after wal fsync recovers latest batch", test_crash_after_wal_fsync_recovers_latest_batch},
        {"crash after snapshot rename recovers consistent state",
         test_crash_after_snapshot_rename_recovers_consistent_state},
        {"crash before snapshot rename replays old wal", test_crash_before_snapshot_rename_replays_old_wal},
        {"crash after wal rotation recovers snapshot", test_crash_after_wal_rotation_recovers_snapshot},
        {"concurrent reads and writes", test_concurrent_reads_and_writes},
        {"many concurrent writers", test_many_concurrent_writers},
        {"concurrent compaction with writes", test_concurrent_compaction_with_writes},
        {"batching metrics are reported", test_batching_metrics_are_reported},
        {"recommended profiles are distinct", test_recommended_profiles_are_distinct},
        {"concurrency stress profiles are distinct", test_concurrency_stress_profiles_are_distinct},
        {"options to json reports profile fields", test_options_to_json_reports_profile_fields},
        {"metrics to json reports core fields", test_metrics_to_json_reports_core_fields},
        {"auto compaction triggers and preserves state", test_auto_compaction_triggers_and_preserves_state},
        {"manual and auto compaction metrics coexist", test_manual_and_auto_compaction_metrics_coexist},
        {"invalid wal ratio triggers auto compaction", test_invalid_wal_ratio_triggers_auto_compaction},
        {"wal obsolete byte metrics are consistent", test_wal_obsolete_byte_metrics_are_consistent},
        {"batch wal byte limit is respected", test_batch_wal_byte_limit_is_respected},
        {"latency histogram tracks write requests", test_latency_histogram_tracks_write_requests},
        {"adaptive batching expands batch under queue pressure", test_adaptive_batching_expands_batch_under_queue_pressure},
        {"percentile and writer metrics are reported", test_percentile_and_writer_metrics_are_reported},
        {"adaptive flush shortens batch delay", test_adaptive_flush_shortens_batch_delay},
        {"compaction long term metrics accumulate", test_compaction_long_term_metrics_accumulate},
        {"latency target adaptive flush kicks in", test_latency_target_adaptive_flush_kicks_in},
        {"fsync pressure can relax batch delay", test_fsync_pressure_can_relax_batch_delay},
        {"recent window metrics capture bursts", test_recent_window_metrics_capture_bursts},
        {"read heavy signal prefers shorter batches", test_read_heavy_signal_prefers_shorter_batches},
        {"compaction pressure signal relaxes batch delay", test_compaction_pressure_signal_relaxes_batch_delay},
        {"wal growth signal relaxes batch delay", test_wal_growth_signal_relaxes_batch_delay},
        {"objective policy prefers short delay under latency pressure",
         test_objective_policy_prefers_short_delay_under_latency_pressure},
        {"objective policy prefers long delay under cost pressure",
         test_objective_policy_prefers_long_delay_under_cost_pressure},
        {"objective throughput score can dominate latency pressure",
         test_objective_throughput_score_can_dominate_latency_pressure},
    };

    size_t passed = 0;
    for (const auto& [name, test] : tests) {
        try {
            test();
            ++passed;
            std::cout << "[PASS] " << name << '\n';
        } catch (const std::exception& ex) {
            std::cerr << "[FAIL] " << name << ": " << ex.what() << '\n';
            return 1;
        }
    }

    std::cout << "All " << passed << " tests passed." << std::endl;
    return 0;
}
