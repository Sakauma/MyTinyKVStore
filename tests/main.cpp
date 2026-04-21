#include "kvstore.h"
#include "internal/format.h"
#include "internal/metrics_helpers.h"
#include "tests/common/cli_entrypoints.h"
#include "tests/common/test_support.h"
#include "tests/integration/test_registry.h"

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

int run_inspect_format_impl(const std::string& db_path);
int run_verify_format_impl(const std::string& db_path);
double extract_json_number(const std::string& json, const std::string& key);
int run_benchmark_trend(const std::string& directory_path, size_t recent_window);
int run_benchmark_trend_json(const std::string& directory_path, size_t recent_window);
using test_support::append_bytes;
using test_support::as_string;
using test_support::file_size_or_zero;
using test_support::require;
using test_support::TestDir;
using test_support::text;

std::string read_text_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    require(in.is_open(), "Expected text file to be readable");
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
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
    const int status = run_inspect_format_impl(db_path);
    std::cout.rdbuf(original);
    require(status == expected_status, "inspect-format should return the expected status");
    return parse_kv_line(out.str());
}

std::pair<int, std::string> capture_verify_format(const std::string& db_path) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_verify_format_impl(db_path);
    std::cout.rdbuf(original);
    return {status, out.str()};
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

struct MicrobenchCaseResult {
    std::string name;
    double duration_s = 0.0;
    double ops_per_s = 0.0;
    uint64_t operations = 0;
    uint64_t bytes = 0;
};

struct MicrobenchComparisonResult {
    std::string name;
    double ops_ratio_pct = 0.0;
    double min_ratio_pct = 0.0;
};

std::vector<MicrobenchCaseResult> parse_microbench_results_json(const std::string& json);

struct MicrobenchTrendCaseSummary {
    std::string name;
    double avg_ops_per_s = 0.0;
    double min_ops_per_s = 0.0;
    double max_ops_per_s = 0.0;
    double recent_avg_ops_per_s = 0.0;
    double latest_vs_oldest_ratio_pct = 0.0;
    double latest_vs_recent_avg_ratio_pct = 0.0;
    std::string trend;
    std::string recent_trend;
};

std::string MicrobenchTrendSummaryToJson(const std::vector<MicrobenchTrendCaseSummary>& summaries);

struct BaselineSummary {
    std::string file_name;
    double write_ops_per_s = 0.0;
    double read_ops_per_s = 0.0;
    double avg_write_latency_us = 0.0;
};

struct TrendSummary {
    size_t count = 0;
    std::string oldest_file;
    std::string latest_file;
    size_t recent_window_count = 0;
    double avg_write_ops_per_s = 0.0;
    double min_write_ops_per_s = 0.0;
    double max_write_ops_per_s = 0.0;
    double recent_avg_write_ops_per_s = 0.0;
    double avg_read_ops_per_s = 0.0;
    double min_read_ops_per_s = 0.0;
    double max_read_ops_per_s = 0.0;
    double recent_avg_read_ops_per_s = 0.0;
    double avg_write_latency_us = 0.0;
    double min_write_latency_us = 0.0;
    double max_write_latency_us = 0.0;
    double recent_avg_write_latency_us = 0.0;
    double latest_vs_oldest_write_ratio_pct = 0.0;
    std::string write_trend;
    double latest_vs_recent_avg_write_ratio_pct = 0.0;
    std::string recent_write_trend;
    double latest_vs_oldest_read_ratio_pct = 0.0;
    std::string read_trend;
    double latest_vs_recent_avg_read_ratio_pct = 0.0;
    std::string recent_read_trend;
    double latest_vs_oldest_latency_ratio_pct = 0.0;
    std::string latency_trend;
    double latest_vs_recent_avg_latency_ratio_pct = 0.0;
    std::string recent_latency_trend;
};

std::string TrendSummaryToJson(const TrendSummary& summary);

struct StressSummary {
    std::string profile;
    int duration_seconds = 0;
    int writer_count = 0;
    int reader_count = 0;
    int compactor_count = 0;
    int recovery_reopen_cycles = 0;
    uint64_t committed_write_requests = 0;
    uint64_t max_pending_queue_depth = 0;
    uint64_t manual_compactions_completed = 0;
    uint64_t auto_compactions_completed = 0;
    uint64_t observed_fsync_pressure_per_1000_writes = 0;
    uint64_t last_effective_batch_delay_us = 0;
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

using test_support::wait_for_start;
using test_support::wait_until;

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

int run_inspect_format_impl(const std::string& db_path) {
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

    rewrite_recommended = rewrite_recommended || wal_truncated;

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

int run_rewrite_format_impl(const std::string& db_path) {
    KVStore store(db_path);
    store.Compact();
    return 0;
}

int run_verify_format_impl(const std::string& db_path) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int inspect_status = run_inspect_format_impl(db_path);
    std::cout.rdbuf(original);

    std::string inspect_output = out.str();
    if (!inspect_output.empty() && inspect_output.back() == '\n') {
        inspect_output.pop_back();
    }

    if (inspect_status != 0) {
        std::cout << inspect_output << " verify_reason=inspect_error" << std::endl;
        return inspect_status;
    }

    const auto values = parse_kv_line(inspect_output);
    const bool rewrite_recommended = values.count("rewrite_recommended") != 0 && values.at("rewrite_recommended") == "1";
    const bool snapshot_magic_ok = values.count("snapshot_magic_ok") == 0 || values.at("snapshot_magic_ok") == "1";
    const bool snapshot_entry_magic_ok =
        values.count("snapshot_entry_magic_ok") == 0 || values.at("snapshot_entry_magic_ok") == "1";
    const bool snapshot_truncated = values.count("snapshot_truncated") != 0 && values.at("snapshot_truncated") == "1";
    const bool wal_truncated = values.count("wal_truncated") != 0 && values.at("wal_truncated") == "1";

    std::string verify_reason = "current_layout";
    int status = 0;
    if (!snapshot_magic_ok) {
        verify_reason = "snapshot_magic";
        status = 2;
    } else if (!snapshot_entry_magic_ok) {
        verify_reason = "snapshot_entry_magic";
        status = 2;
    } else if (snapshot_truncated) {
        verify_reason = "snapshot_truncated";
        status = 2;
    } else if (wal_truncated) {
        verify_reason = "wal_truncated";
        status = 2;
    } else if (rewrite_recommended) {
        verify_reason = "migration_required";
        status = 2;
    }

    std::cout << inspect_output << " verify_reason=" << verify_reason << std::endl;
    return status;
}

int run_compatibility_matrix_impl() {
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
    const auto current_verify_values = parse_kv_line(current_verify_output);
    require(current_verify_status == 0, "current compatibility matrix case should verify cleanly");
    require(current_inspect.at("snapshot_version") == "2", "current case should report snapshot v2");
    require(current_inspect.at("rewrite_recommended") == "0", "current case should not recommend rewrite");
    require(current_verify_values.at("verify_reason") == "current_layout",
            "current compatibility matrix case should report a clean-layout reason");

    const std::string live_wal_db_path = dir.file("current_v2_live_wal.dat");
    {
        KVStore store(live_wal_db_path);
        store.Put(1, text("one"));
        store.Compact();
        store.Put(2, text("two"));
        store.Delete(1);
    }

    const auto live_wal_inspect = capture_inspect_format(live_wal_db_path);
    const auto [live_wal_verify_status, live_wal_verify_output] = capture_verify_format(live_wal_db_path);
    const auto live_wal_verify_values = parse_kv_line(live_wal_verify_output);
    require(live_wal_verify_status == 0, "current v2 layout with live WAL should verify cleanly");
    require(live_wal_inspect.at("snapshot_version") == "2", "live WAL case should keep snapshot v2");
    require(live_wal_inspect.at("wal_version") == "2", "live WAL case should report WAL v2");
    require(live_wal_inspect.at("wal_records") == "2", "live WAL case should retain post-snapshot WAL records");
    require(live_wal_inspect.at("rewrite_recommended") == "0",
            "live WAL case should not recommend rewrite");
    require(live_wal_verify_values.at("verify_reason") == "current_layout",
            "live WAL case should remain a clean current layout");

    const std::string truncated_db_path = dir.file("current_v2_truncated_wal.dat");
    const std::string truncated_wal_path = truncated_db_path + ".wal";
    {
        KVStore store(truncated_db_path);
        store.Put(1, text("stable"));
        store.Compact();
        store.Put(2, text("latest"));
    }
    append_bytes(truncated_wal_path, {0xAA, 0xBB, 0xCC});

    const auto truncated_inspect = capture_inspect_format(truncated_db_path);
    const auto [truncated_verify_status, truncated_verify_output] = capture_verify_format(truncated_db_path);
    const auto truncated_verify_values = parse_kv_line(truncated_verify_output);
    require(truncated_verify_status == 2, "truncated WAL case should require rewrite");
    require(truncated_inspect.at("snapshot_version") == "2", "truncated WAL case should keep snapshot v2");
    require(truncated_inspect.at("wal_truncated") == "1", "truncated WAL case should report WAL truncation");
    require(truncated_inspect.at("rewrite_recommended") == "1",
            "truncated WAL case should explicitly recommend rewrite");
    require(truncated_verify_values.at("verify_reason") == "wal_truncated",
            "truncated WAL case should expose the wal_truncated migration reason");

    require(run_rewrite_format_impl(truncated_db_path) == 0,
            "truncated WAL compatibility matrix case should rewrite successfully");
    const auto rewritten_truncated_inspect = capture_inspect_format(truncated_db_path);
    const auto [rewritten_truncated_verify_status, rewritten_truncated_verify_output] =
        capture_verify_format(truncated_db_path);
    const auto rewritten_truncated_verify_values = parse_kv_line(rewritten_truncated_verify_output);
    require(rewritten_truncated_verify_status == 0,
            "rewritten truncated WAL case should verify cleanly");
    require(rewritten_truncated_inspect.at("wal_truncated") == "0",
            "rewritten truncated WAL case should clear the truncation signal");
    require(rewritten_truncated_inspect.at("rewrite_recommended") == "0",
            "rewritten truncated WAL case should stop recommending rewrite");
    require(rewritten_truncated_verify_values.at("verify_reason") == "current_layout",
            "rewritten truncated WAL case should return to a clean-layout reason");

    const std::string legacy_db_path = dir.file("legacy_v1.dat");
    const std::string legacy_wal_path = legacy_db_path + ".wal";
    write_legacy_snapshot_v1(legacy_db_path, {
        {7, text("seven")},
        {8, text("eight")},
    });
    append_legacy_wal_record_v1(legacy_wal_path, 1, 9, text("nine"));

    const auto legacy_inspect = capture_inspect_format(legacy_db_path);
    const auto [legacy_verify_status, legacy_verify_output] = capture_verify_format(legacy_db_path);
    const auto legacy_verify_values = parse_kv_line(legacy_verify_output);
    require(legacy_verify_status == 2, "legacy compatibility matrix case should require rewrite");
    require(legacy_inspect.at("snapshot_version") == "1", "legacy case should report snapshot v1");
    require(legacy_inspect.at("rewrite_recommended") == "1", "legacy case should recommend rewrite");
    require(legacy_verify_values.at("verify_reason") == "migration_required",
            "legacy case should expose a migration-required reason");

    require(run_rewrite_format_impl(legacy_db_path) == 0, "legacy compatibility matrix case should rewrite successfully");
    const auto rewritten_inspect = capture_inspect_format(legacy_db_path);
    const auto [rewritten_verify_status, rewritten_verify_output] = capture_verify_format(legacy_db_path);
    const auto rewritten_verify_values = parse_kv_line(rewritten_verify_output);
    require(rewritten_verify_status == 0, "rewritten legacy case should verify cleanly");
    require(rewritten_inspect.at("snapshot_version") == "2", "rewritten legacy case should upgrade to snapshot v2");
    require(rewritten_inspect.at("rewrite_recommended") == "0", "rewritten legacy case should stop recommending rewrite");
    require(rewritten_verify_values.at("verify_reason") == "current_layout",
            "rewritten legacy case should return to a clean-layout reason");

    std::cout << "case=current_v2"
              << " snapshot_version=" << current_inspect.at("snapshot_version")
              << " wal_version=" << value_or(current_inspect, "wal_version", "empty")
              << " verify_status=" << current_verify_status
              << " verify_reason=" << current_verify_values.at("verify_reason")
              << " rewrite_recommended=" << current_inspect.at("rewrite_recommended")
              << " current_verify_output_bytes=" << current_verify_output.size()
              << '\n';
    std::cout << "case=legacy_v1"
              << " snapshot_version=" << legacy_inspect.at("snapshot_version")
              << " wal_version=" << value_or(legacy_inspect, "wal_version", "missing")
              << " verify_status=" << legacy_verify_status
              << " verify_reason=" << legacy_verify_values.at("verify_reason")
              << " rewrite_recommended=" << legacy_inspect.at("rewrite_recommended")
              << " legacy_verify_output_bytes=" << legacy_verify_output.size()
              << '\n';
    std::cout << "case=legacy_v1_after_rewrite"
              << " snapshot_version=" << rewritten_inspect.at("snapshot_version")
              << " wal_exists=" << rewritten_inspect.at("wal_exists")
              << " verify_status=" << rewritten_verify_status
              << " verify_reason=" << rewritten_verify_values.at("verify_reason")
              << " rewrite_recommended=" << rewritten_inspect.at("rewrite_recommended")
              << " rewritten_verify_output_bytes=" << rewritten_verify_output.size()
              << '\n';
    std::cout << "case=current_v2_live_wal"
              << " snapshot_version=" << live_wal_inspect.at("snapshot_version")
              << " wal_version=" << value_or(live_wal_inspect, "wal_version", "missing")
              << " wal_records=" << live_wal_inspect.at("wal_records")
              << " verify_status=" << live_wal_verify_status
              << " verify_reason=" << live_wal_verify_values.at("verify_reason")
              << " rewrite_recommended=" << live_wal_inspect.at("rewrite_recommended")
              << " live_wal_verify_output_bytes=" << live_wal_verify_output.size()
              << '\n';
    std::cout << "case=current_v2_truncated_wal"
              << " snapshot_version=" << truncated_inspect.at("snapshot_version")
              << " wal_truncated=" << truncated_inspect.at("wal_truncated")
              << " verify_status=" << truncated_verify_status
              << " verify_reason=" << truncated_verify_values.at("verify_reason")
              << " rewrite_recommended=" << truncated_inspect.at("rewrite_recommended")
              << " truncated_verify_output_bytes=" << truncated_verify_output.size()
              << '\n';
    std::cout << "case=current_v2_truncated_wal_after_rewrite"
              << " snapshot_version=" << rewritten_truncated_inspect.at("snapshot_version")
              << " wal_truncated=" << rewritten_truncated_inspect.at("wal_truncated")
              << " verify_status=" << rewritten_truncated_verify_status
              << " verify_reason=" << rewritten_truncated_verify_values.at("verify_reason")
              << " rewrite_recommended=" << rewritten_truncated_inspect.at("rewrite_recommended")
              << " rewritten_truncated_verify_output_bytes=" << rewritten_truncated_verify_output.size()
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

int run_compare_microbench(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_ops_ratio_pct = 80.0,
    double min_compaction_ratio_pct = 75.0,
    double min_rewrite_ratio_pct = 75.0,
    double min_recovery_ratio_pct = 80.0) {
    const std::vector<MicrobenchCaseResult> baseline_cases =
        parse_microbench_results_json(read_text_file(baseline_path));
    const std::vector<MicrobenchCaseResult> candidate_cases =
        parse_microbench_results_json(read_text_file(candidate_path));
    require(!baseline_cases.empty(), "microbench baseline must contain at least one case");
    require(!candidate_cases.empty(), "microbench candidate must contain at least one case");

    std::map<std::string, MicrobenchCaseResult> baseline_by_name;
    std::map<std::string, MicrobenchCaseResult> candidate_by_name;
    for (const auto& result : baseline_cases) {
        baseline_by_name[result.name] = result;
    }
    for (const auto& result : candidate_cases) {
        candidate_by_name[result.name] = result;
    }

    bool pass = true;
    std::vector<MicrobenchComparisonResult> comparisons;
    for (const auto& [name, baseline] : baseline_by_name) {
        const auto candidate_it = candidate_by_name.find(name);
        require(candidate_it != candidate_by_name.end(), "microbench candidate is missing a baseline case");
        require(baseline.ops_per_s > 0.0, "microbench baseline ops_per_s must be positive");

        double min_ratio_for_case = min_ops_ratio_pct;
        if (name == "compaction") {
            min_ratio_for_case = min_compaction_ratio_pct;
        } else if (name == "rewrite") {
            min_ratio_for_case = min_rewrite_ratio_pct;
        } else if (name == "recovery") {
            min_ratio_for_case = min_recovery_ratio_pct;
        }

        const double ops_ratio_pct = (candidate_it->second.ops_per_s / baseline.ops_per_s) * 100.0;
        comparisons.push_back({name, ops_ratio_pct, min_ratio_for_case});
        if (ops_ratio_pct < min_ratio_for_case) {
            pass = false;
        }
    }

    std::sort(comparisons.begin(), comparisons.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.name < rhs.name;
    });
    std::cout << "baseline=" << baseline_path
              << " candidate=" << candidate_path;
    for (const auto& comparison : comparisons) {
        std::cout << ' '
                  << comparison.name << "_ops_ratio_pct=" << comparison.ops_ratio_pct
                  << ' ' << comparison.name << "_min_ratio_pct=" << comparison.min_ratio_pct;
    }
    std::cout << " status=" << (pass ? "pass" : "fail") << std::endl;
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

std::string classify_throughput_trend(double ratio_pct) {
    if (ratio_pct >= 105.0) {
        return "improving";
    }
    if (ratio_pct <= 95.0) {
        return "regressing";
    }
    return "stable";
}

std::string classify_latency_trend(double ratio_pct) {
    if (ratio_pct <= 95.0) {
        return "improving";
    }
    if (ratio_pct >= 105.0) {
        return "regressing";
    }
    return "stable";
}

TrendSummary collect_benchmark_trend_summary(const std::string& directory_path, size_t recent_window) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(directory_path)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() != ".json") {
            continue;
        }
        if (entry.path().filename().string().rfind("microbench-", 0) == 0) {
            continue;
        }
        files.push_back(entry.path());
    }

    std::sort(files.begin(), files.end());
    require(!files.empty(), "benchmark trend requires at least one baseline json file");
    require(recent_window > 0, "benchmark trend recent window must be positive");

    std::vector<BaselineSummary> summaries;
    summaries.reserve(files.size());
    for (const auto& path : files) {
        summaries.push_back(load_baseline_summary(path));
    }

    const BaselineSummary& first = summaries.front();
    const BaselineSummary& latest = summaries.back();
    const size_t recent_count = std::min(recent_window, summaries.size());
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

    double recent_avg_write_ops_per_s = 0.0;
    double recent_avg_read_ops_per_s = 0.0;
    double recent_avg_write_latency_us = 0.0;
    for (size_t i = summaries.size() - recent_count; i < summaries.size(); ++i) {
        recent_avg_write_ops_per_s += summaries[i].write_ops_per_s;
        recent_avg_read_ops_per_s += summaries[i].read_ops_per_s;
        recent_avg_write_latency_us += summaries[i].avg_write_latency_us;
    }
    recent_avg_write_ops_per_s /= recent_count;
    recent_avg_read_ops_per_s /= recent_count;
    recent_avg_write_latency_us /= recent_count;

    const double avg_write_ops_per_s = sum_write_ops_per_s / summaries.size();
    const double avg_read_ops_per_s = sum_read_ops_per_s / summaries.size();
    const double avg_avg_write_latency_us = sum_avg_write_latency_us / summaries.size();
    const double latest_vs_first_write_ratio_pct =
        first.write_ops_per_s == 0.0 ? 0.0 : (latest.write_ops_per_s / first.write_ops_per_s) * 100.0;
    const double latest_vs_first_read_ratio_pct =
        first.read_ops_per_s == 0.0 ? 0.0 : (latest.read_ops_per_s / first.read_ops_per_s) * 100.0;
    const double latest_vs_first_latency_ratio_pct =
        first.avg_write_latency_us == 0.0 ? 0.0 : (latest.avg_write_latency_us / first.avg_write_latency_us) * 100.0;
    const double latest_vs_recent_avg_write_ratio_pct =
        recent_avg_write_ops_per_s == 0.0 ? 0.0 : (latest.write_ops_per_s / recent_avg_write_ops_per_s) * 100.0;
    const double latest_vs_recent_avg_read_ratio_pct =
        recent_avg_read_ops_per_s == 0.0 ? 0.0 : (latest.read_ops_per_s / recent_avg_read_ops_per_s) * 100.0;
    const double latest_vs_recent_avg_latency_ratio_pct =
        recent_avg_write_latency_us == 0.0 ? 0.0 : (latest.avg_write_latency_us / recent_avg_write_latency_us) * 100.0;
    const std::string write_trend = classify_throughput_trend(latest_vs_first_write_ratio_pct);
    const std::string read_trend = classify_throughput_trend(latest_vs_first_read_ratio_pct);
    const std::string latency_trend = classify_latency_trend(latest_vs_first_latency_ratio_pct);
    const std::string recent_write_trend = classify_throughput_trend(latest_vs_recent_avg_write_ratio_pct);
    const std::string recent_read_trend = classify_throughput_trend(latest_vs_recent_avg_read_ratio_pct);
    const std::string recent_latency_trend = classify_latency_trend(latest_vs_recent_avg_latency_ratio_pct);

    TrendSummary trend;
    trend.count = summaries.size();
    trend.oldest_file = first.file_name;
    trend.latest_file = latest.file_name;
    trend.recent_window_count = recent_count;
    trend.avg_write_ops_per_s = avg_write_ops_per_s;
    trend.min_write_ops_per_s = min_write_ops_per_s;
    trend.max_write_ops_per_s = max_write_ops_per_s;
    trend.recent_avg_write_ops_per_s = recent_avg_write_ops_per_s;
    trend.avg_read_ops_per_s = avg_read_ops_per_s;
    trend.min_read_ops_per_s = min_read_ops_per_s;
    trend.max_read_ops_per_s = max_read_ops_per_s;
    trend.recent_avg_read_ops_per_s = recent_avg_read_ops_per_s;
    trend.avg_write_latency_us = avg_avg_write_latency_us;
    trend.min_write_latency_us = min_avg_write_latency_us;
    trend.max_write_latency_us = max_avg_write_latency_us;
    trend.recent_avg_write_latency_us = recent_avg_write_latency_us;
    trend.latest_vs_oldest_write_ratio_pct = latest_vs_first_write_ratio_pct;
    trend.write_trend = write_trend;
    trend.latest_vs_recent_avg_write_ratio_pct = latest_vs_recent_avg_write_ratio_pct;
    trend.recent_write_trend = recent_write_trend;
    trend.latest_vs_oldest_read_ratio_pct = latest_vs_first_read_ratio_pct;
    trend.read_trend = read_trend;
    trend.latest_vs_recent_avg_read_ratio_pct = latest_vs_recent_avg_read_ratio_pct;
    trend.recent_read_trend = recent_read_trend;
    trend.latest_vs_oldest_latency_ratio_pct = latest_vs_first_latency_ratio_pct;
    trend.latency_trend = latency_trend;
    trend.latest_vs_recent_avg_latency_ratio_pct = latest_vs_recent_avg_latency_ratio_pct;
    trend.recent_latency_trend = recent_latency_trend;
    return trend;
}

int run_benchmark_trend(const std::string& directory_path, size_t recent_window) {
    const TrendSummary trend = collect_benchmark_trend_summary(directory_path, recent_window);
    std::cout << "count=" << trend.count
              << " oldest_file=" << trend.oldest_file
              << " latest_file=" << trend.latest_file
              << " recent_window_count=" << trend.recent_window_count
              << " avg_write_ops_per_s=" << trend.avg_write_ops_per_s
              << " min_write_ops_per_s=" << trend.min_write_ops_per_s
              << " max_write_ops_per_s=" << trend.max_write_ops_per_s
              << " recent_avg_write_ops_per_s=" << trend.recent_avg_write_ops_per_s
              << " avg_read_ops_per_s=" << trend.avg_read_ops_per_s
              << " min_read_ops_per_s=" << trend.min_read_ops_per_s
              << " max_read_ops_per_s=" << trend.max_read_ops_per_s
              << " recent_avg_read_ops_per_s=" << trend.recent_avg_read_ops_per_s
              << " avg_write_latency_us=" << trend.avg_write_latency_us
              << " min_write_latency_us=" << trend.min_write_latency_us
              << " max_write_latency_us=" << trend.max_write_latency_us
              << " recent_avg_write_latency_us=" << trend.recent_avg_write_latency_us
              << " latest_vs_oldest_write_ratio_pct=" << trend.latest_vs_oldest_write_ratio_pct
              << " write_trend=" << trend.write_trend
              << " latest_vs_recent_avg_write_ratio_pct=" << trend.latest_vs_recent_avg_write_ratio_pct
              << " recent_write_trend=" << trend.recent_write_trend
              << " latest_vs_oldest_read_ratio_pct=" << trend.latest_vs_oldest_read_ratio_pct
              << " read_trend=" << trend.read_trend
              << " latest_vs_recent_avg_read_ratio_pct=" << trend.latest_vs_recent_avg_read_ratio_pct
              << " recent_read_trend=" << trend.recent_read_trend
              << " latest_vs_oldest_latency_ratio_pct=" << trend.latest_vs_oldest_latency_ratio_pct
              << " latency_trend=" << trend.latency_trend
              << " latest_vs_recent_avg_latency_ratio_pct=" << trend.latest_vs_recent_avg_latency_ratio_pct
              << " recent_latency_trend=" << trend.recent_latency_trend
              << std::endl;
    return 0;
}

int run_benchmark_trend_json(const std::string& directory_path, size_t recent_window) {
    std::cout << TrendSummaryToJson(collect_benchmark_trend_summary(directory_path, recent_window)) << std::endl;
    return 0;
}

std::vector<MicrobenchTrendCaseSummary> collect_microbench_trend_summary(
    const std::string& directory_path,
    size_t recent_window) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(directory_path)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() != ".json") {
            continue;
        }
        if (entry.path().filename().string().rfind("microbench-", 0) != 0) {
            continue;
        }
        files.push_back(entry.path());
    }

    std::sort(files.begin(), files.end());
    require(!files.empty(), "microbench trend requires at least one microbench json file");
    require(recent_window > 0, "microbench trend recent window must be positive");

    std::map<std::string, std::vector<double>> history_by_case;
    for (const auto& path : files) {
        for (const auto& result : parse_microbench_results_json(read_text_file(path.string()))) {
            history_by_case[result.name].push_back(result.ops_per_s);
        }
    }

    std::vector<MicrobenchTrendCaseSummary> summaries;
    for (const auto& [name, history] : history_by_case) {
        require(!history.empty(), "microbench trend history must not be empty");
        const size_t recent_count = std::min(recent_window, history.size());
        double min_ops_per_s = history.front();
        double max_ops_per_s = history.front();
        double sum_ops_per_s = 0.0;
        for (double value : history) {
            min_ops_per_s = std::min(min_ops_per_s, value);
            max_ops_per_s = std::max(max_ops_per_s, value);
            sum_ops_per_s += value;
        }
        double recent_avg_ops_per_s = 0.0;
        for (size_t i = history.size() - recent_count; i < history.size(); ++i) {
            recent_avg_ops_per_s += history[i];
        }
        recent_avg_ops_per_s /= recent_count;

        const double avg_ops_per_s = sum_ops_per_s / history.size();
        const double latest_vs_oldest_ratio_pct =
            history.front() == 0.0 ? 0.0 : (history.back() / history.front()) * 100.0;
        const double latest_vs_recent_avg_ratio_pct =
            recent_avg_ops_per_s == 0.0 ? 0.0 : (history.back() / recent_avg_ops_per_s) * 100.0;

        summaries.push_back({
            name,
            avg_ops_per_s,
            min_ops_per_s,
            max_ops_per_s,
            recent_avg_ops_per_s,
            latest_vs_oldest_ratio_pct,
            latest_vs_recent_avg_ratio_pct,
            classify_throughput_trend(latest_vs_oldest_ratio_pct),
            classify_throughput_trend(latest_vs_recent_avg_ratio_pct),
        });
    }

    std::sort(summaries.begin(), summaries.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.name < rhs.name;
    });
    return summaries;
}

int run_microbench_trend(const std::string& directory_path, size_t recent_window) {
    const auto summaries = collect_microbench_trend_summary(directory_path, recent_window);
    for (const auto& summary : summaries) {
        std::cout << "name=" << summary.name
                  << " avg_ops_per_s=" << summary.avg_ops_per_s
                  << " min_ops_per_s=" << summary.min_ops_per_s
                  << " max_ops_per_s=" << summary.max_ops_per_s
                  << " recent_avg_ops_per_s=" << summary.recent_avg_ops_per_s
                  << " latest_vs_oldest_ratio_pct=" << summary.latest_vs_oldest_ratio_pct
                  << " latest_vs_recent_avg_ratio_pct=" << summary.latest_vs_recent_avg_ratio_pct
                  << " trend=" << summary.trend
                  << " recent_trend=" << summary.recent_trend
                  << std::endl;
    }
    return 0;
}

int run_microbench_trend_json(const std::string& directory_path, size_t recent_window) {
    std::cout << MicrobenchTrendSummaryToJson(collect_microbench_trend_summary(directory_path, recent_window))
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

std::string MicrobenchResultsToJson(const std::vector<MicrobenchCaseResult>& results) {
    std::ostringstream out;
    out << "{\"cases\":[";
    for (size_t i = 0; i < results.size(); ++i) {
        if (i != 0) {
            out << ',';
        }
        out << '{'
            << "\"name\":\"" << results[i].name << "\""
            << ",\"duration_s\":" << results[i].duration_s
            << ",\"ops_per_s\":" << results[i].ops_per_s
            << ",\"operations\":" << results[i].operations
            << ",\"bytes\":" << results[i].bytes
            << '}';
    }
    out << "]}";
    return out.str();
}

std::string TrendSummaryToJson(const TrendSummary& summary) {
    std::ostringstream out;
    out << '{'
        << "\"count\":" << summary.count
        << ",\"oldest_file\":\"" << summary.oldest_file << "\""
        << ",\"latest_file\":\"" << summary.latest_file << "\""
        << ",\"recent_window_count\":" << summary.recent_window_count
        << ",\"avg_write_ops_per_s\":" << summary.avg_write_ops_per_s
        << ",\"min_write_ops_per_s\":" << summary.min_write_ops_per_s
        << ",\"max_write_ops_per_s\":" << summary.max_write_ops_per_s
        << ",\"recent_avg_write_ops_per_s\":" << summary.recent_avg_write_ops_per_s
        << ",\"avg_read_ops_per_s\":" << summary.avg_read_ops_per_s
        << ",\"min_read_ops_per_s\":" << summary.min_read_ops_per_s
        << ",\"max_read_ops_per_s\":" << summary.max_read_ops_per_s
        << ",\"recent_avg_read_ops_per_s\":" << summary.recent_avg_read_ops_per_s
        << ",\"avg_write_latency_us\":" << summary.avg_write_latency_us
        << ",\"min_write_latency_us\":" << summary.min_write_latency_us
        << ",\"max_write_latency_us\":" << summary.max_write_latency_us
        << ",\"recent_avg_write_latency_us\":" << summary.recent_avg_write_latency_us
        << ",\"latest_vs_oldest_write_ratio_pct\":" << summary.latest_vs_oldest_write_ratio_pct
        << ",\"write_trend\":\"" << summary.write_trend << "\""
        << ",\"latest_vs_recent_avg_write_ratio_pct\":" << summary.latest_vs_recent_avg_write_ratio_pct
        << ",\"recent_write_trend\":\"" << summary.recent_write_trend << "\""
        << ",\"latest_vs_oldest_read_ratio_pct\":" << summary.latest_vs_oldest_read_ratio_pct
        << ",\"read_trend\":\"" << summary.read_trend << "\""
        << ",\"latest_vs_recent_avg_read_ratio_pct\":" << summary.latest_vs_recent_avg_read_ratio_pct
        << ",\"recent_read_trend\":\"" << summary.recent_read_trend << "\""
        << ",\"latest_vs_oldest_latency_ratio_pct\":" << summary.latest_vs_oldest_latency_ratio_pct
        << ",\"latency_trend\":\"" << summary.latency_trend << "\""
        << ",\"latest_vs_recent_avg_latency_ratio_pct\":" << summary.latest_vs_recent_avg_latency_ratio_pct
        << ",\"recent_latency_trend\":\"" << summary.recent_latency_trend << "\""
        << '}';
    return out.str();
}

std::vector<MicrobenchCaseResult> parse_microbench_results_json(const std::string& json) {
    std::vector<MicrobenchCaseResult> results;
    size_t search_from = 0;
    while (true) {
        const size_t name_pos = json.find("\"name\":\"", search_from);
        if (name_pos == std::string::npos) {
            break;
        }
        const size_t value_begin = name_pos + 8;
        const size_t value_end = json.find('"', value_begin);
        require(value_end != std::string::npos, "microbench json should contain a closing name quote");

        const size_t object_begin = json.rfind('{', name_pos);
        const size_t object_end = json.find('}', value_end);
        require(object_begin != std::string::npos && object_end != std::string::npos,
                "microbench json should contain complete case objects");

        const std::string object_json = json.substr(object_begin, object_end - object_begin + 1);
        MicrobenchCaseResult result;
        result.name = json.substr(value_begin, value_end - value_begin);
        result.duration_s = extract_json_number(object_json, "duration_s");
        result.ops_per_s = extract_json_number(object_json, "ops_per_s");
        result.operations = static_cast<uint64_t>(extract_json_number(object_json, "operations"));
        result.bytes = static_cast<uint64_t>(extract_json_number(object_json, "bytes"));
        results.push_back(result);
        search_from = object_end + 1;
    }
    return results;
}

std::string StressSummaryToJson(const StressSummary& summary) {
    std::ostringstream out;
    out << '{'
        << "\"profile\":\"" << summary.profile << "\""
        << ",\"duration_seconds\":" << summary.duration_seconds
        << ",\"writer_count\":" << summary.writer_count
        << ",\"reader_count\":" << summary.reader_count
        << ",\"compactor_count\":" << summary.compactor_count
        << ",\"recovery_reopen_cycles\":" << summary.recovery_reopen_cycles
        << ",\"committed_write_requests\":" << summary.committed_write_requests
        << ",\"max_pending_queue_depth\":" << summary.max_pending_queue_depth
        << ",\"manual_compactions_completed\":" << summary.manual_compactions_completed
        << ",\"auto_compactions_completed\":" << summary.auto_compactions_completed
        << ",\"observed_fsync_pressure_per_1000_writes\":" << summary.observed_fsync_pressure_per_1000_writes
        << ",\"last_effective_batch_delay_us\":" << summary.last_effective_batch_delay_us
        << '}';
    return out.str();
}

std::string MicrobenchTrendSummaryToJson(const std::vector<MicrobenchTrendCaseSummary>& summaries) {
    std::ostringstream out;
    out << "{\"cases\":[";
    for (size_t i = 0; i < summaries.size(); ++i) {
        if (i != 0) {
            out << ',';
        }
        out << '{'
            << "\"name\":\"" << summaries[i].name << "\""
            << ",\"avg_ops_per_s\":" << summaries[i].avg_ops_per_s
            << ",\"min_ops_per_s\":" << summaries[i].min_ops_per_s
            << ",\"max_ops_per_s\":" << summaries[i].max_ops_per_s
            << ",\"recent_avg_ops_per_s\":" << summaries[i].recent_avg_ops_per_s
            << ",\"latest_vs_oldest_ratio_pct\":" << summaries[i].latest_vs_oldest_ratio_pct
            << ",\"latest_vs_recent_avg_ratio_pct\":" << summaries[i].latest_vs_recent_avg_ratio_pct
            << ",\"trend\":\"" << summaries[i].trend << "\""
            << ",\"recent_trend\":\"" << summaries[i].recent_trend << "\""
            << '}';
    }
    out << "]}";
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
    kRecoveryHeavy,
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
    if (name == "recovery-heavy") {
        return ConcurrencyStressProfile::kRecoveryHeavy;
    }
    return std::nullopt;
}

std::string concurrency_stress_profile_name(ConcurrencyStressProfile profile) {
    switch (profile) {
        case ConcurrencyStressProfile::kBalanced:
            return "balanced";
        case ConcurrencyStressProfile::kWriteHeavy:
            return "write-heavy";
        case ConcurrencyStressProfile::kCompactionHeavy:
            return "compaction-heavy";
        case ConcurrencyStressProfile::kRecoveryHeavy:
            return "recovery-heavy";
    }
    return "unknown";
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
    int recovery_reopen_cycles = 1;
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
        case ConcurrencyStressProfile::kRecoveryHeavy:
            config.writer_count = 4;
            config.reader_count = 1;
            config.compactor_count = 1;
            config.keys_per_writer = 64;
            config.compaction_interval_ms = 2;
            config.batch_width = 3;
            config.recovery_reopen_cycles = 6;
            config.options.max_batch_size = 16;
            config.options.max_batch_delay_us = 700;
            config.options.auto_compact_wal_bytes_threshold = 2048;
            config.options.auto_compact_invalid_wal_ratio_percent = 40;
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

void test_microbench_json_reports_cases() {
    std::vector<MicrobenchCaseResult> results;
    results.push_back({"wal_append", 0.1, 1000.0, 100, 4096});
    results.push_back({"scan", 0.2, 500.0, 100, 0});
    results.push_back({"compaction", 0.3, 10.0, 3, 8192});
    results.push_back({"rewrite", 0.4, 5.0, 2, 4096});

    const std::string json = MicrobenchResultsToJson(results);
    require(json.find("\"cases\":[") != std::string::npos,
            "microbench json should include a cases array");
    require(json.find("\"name\":\"wal_append\"") != std::string::npos,
            "microbench json should include the wal_append case");
    require(json.find("\"name\":\"scan\"") != std::string::npos,
            "microbench json should include the scan case");
    require(json.find("\"name\":\"compaction\"") != std::string::npos,
            "microbench json should include the compaction case");
    require(json.find("\"name\":\"rewrite\"") != std::string::npos,
            "microbench json should include the rewrite case");
}

void test_stress_summary_json_reports_profile() {
    StressSummary summary;
    summary.profile = "balanced";
    summary.duration_seconds = 1;
    summary.writer_count = 8;
    summary.reader_count = 4;
    summary.compactor_count = 1;
    summary.recovery_reopen_cycles = 6;
    summary.committed_write_requests = 123;
    summary.max_pending_queue_depth = 7;

    const std::string json = StressSummaryToJson(summary);
    require(json.find("\"profile\":\"balanced\"") != std::string::npos,
            "stress summary json should include the profile name");
    require(json.find("\"committed_write_requests\":123") != std::string::npos,
            "stress summary json should include committed write counts");
    require(json.find("\"max_pending_queue_depth\":7") != std::string::npos,
            "stress summary json should include queue metrics");
    require(json.find("\"recovery_reopen_cycles\":6") != std::string::npos,
            "stress summary json should include recovery reopen counts");
}

void test_internal_format_helpers_round_trip_keys() {
    const std::string int_key = kvstore::internal::encode_int_key(42);
    const std::string string_key = kvstore::internal::encode_string_key("alpha");
    const std::string binary_key =
        kvstore::internal::encode_binary_key(std::vector<uint8_t> {0x00, 0x7F, 0xFF});

    require(!int_key.empty() && int_key.front() == kvstore::internal::kIntKeyTag,
            "encoded int keys should carry the int namespace tag");
    require(kvstore::internal::is_string_key(string_key),
            "encoded string keys should be recognized as string keys");
    require(kvstore::internal::decode_string_key(string_key) == "alpha",
            "string key helpers should round-trip the original string key");
    require(!binary_key.empty() && binary_key.front() == kvstore::internal::kBinaryKeyTag,
            "encoded binary keys should carry the binary namespace tag");
}

void test_internal_format_helpers_checksum_distinguishes_payloads() {
    const std::string key = kvstore::internal::encode_string_key("checksum");
    const Value first = text("value_a");
    const Value second = text("value_b");

    const uint32_t first_checksum =
        kvstore::internal::checksum_record(kvstore::internal::WalRecordType::kPut, key, first);
    const uint32_t second_checksum =
        kvstore::internal::checksum_record(kvstore::internal::WalRecordType::kPut, key, second);
    const uint32_t delete_checksum =
        kvstore::internal::checksum_record(kvstore::internal::WalRecordType::kDelete, key, Value {});

    require(first_checksum != second_checksum,
            "checksum helper should change when the payload changes");
    require(first_checksum != delete_checksum,
            "checksum helper should change when the record type changes");
}

void test_internal_metrics_helpers_compute_percentiles_and_ratios() {
    std::array<uint64_t, kWriteLatencyBucketCount> histogram {};
    histogram[0] = 1;
    histogram[4] = 2;
    histogram[7] = 1;

    require(kvstore::internal::approximate_latency_percentile_us(histogram, 1, 2) == 1000,
            "p50 helper should map into the first bucket that crosses the 50th percentile");
    require(kvstore::internal::approximate_latency_percentile_us(histogram, 95, 100) == 10000,
            "p95 helper should map into the tail bucket that crosses the percentile");
    require(kvstore::internal::capped_ratio_milli(16, 4) == 4000,
            "ratio helper should cap values at 4x");
    require(kvstore::internal::weighted_signal_score(200, 100, 3) == 6000,
            "weighted signal score should scale the capped ratio by the given weight");
    require(kvstore::internal::weighted_deficit_score(4, 8, 2) == 1000,
            "weighted deficit score should reflect the observed gap to target");
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
    const ConcurrencyStressProfileConfig recovery_heavy =
        make_concurrency_stress_profile_config(ConcurrencyStressProfile::kRecoveryHeavy);

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
    require(recovery_heavy.recovery_reopen_cycles > balanced.recovery_reopen_cycles,
            "recovery-heavy stress profile should repeat reopen validation more often than balanced");
    require(recovery_heavy.reader_count < balanced.reader_count,
            "recovery-heavy stress profile should trade reader threads for reopen checks");
    require(recovery_heavy.options.max_batch_delay_us < balanced.options.max_batch_delay_us,
            "recovery-heavy stress profile should prefer shorter delays around recovery checks");
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

StressSummary run_concurrency_stress_capture(int duration_seconds, ConcurrencyStressProfile profile) {
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

    for (int reopen_index = 0; reopen_index < config.recovery_reopen_cycles; ++reopen_index) {
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

    StressSummary summary;
    summary.profile = concurrency_stress_profile_name(profile);
    summary.duration_seconds = duration_seconds;
    summary.writer_count = config.writer_count;
    summary.reader_count = config.reader_count;
    summary.compactor_count = config.compactor_count;
    summary.recovery_reopen_cycles = config.recovery_reopen_cycles;
    summary.committed_write_requests = final_metrics.committed_write_requests;
    summary.max_pending_queue_depth = final_metrics.max_pending_queue_depth;
    summary.manual_compactions_completed = final_metrics.manual_compactions_completed;
    summary.auto_compactions_completed = final_metrics.auto_compactions_completed;
    summary.observed_fsync_pressure_per_1000_writes = final_metrics.observed_fsync_pressure_per_1000_writes;
    summary.last_effective_batch_delay_us = final_metrics.last_effective_batch_delay_us;
    return summary;
}

void run_concurrency_stress_test(int duration_seconds, ConcurrencyStressProfile profile) {
    const StressSummary summary = run_concurrency_stress_capture(duration_seconds, profile);
    std::cout << "[STRESS]"
              << " profile=" << summary.profile
              << " duration_seconds=" << summary.duration_seconds
              << " recovery_reopen_cycles=" << summary.recovery_reopen_cycles
              << " committed_write_requests=" << summary.committed_write_requests
              << " max_pending_queue_depth=" << summary.max_pending_queue_depth
              << " manual_compactions_completed=" << summary.manual_compactions_completed
              << " auto_compactions_completed=" << summary.auto_compactions_completed
              << " observed_fsync_pressure_per_1000_writes=" << summary.observed_fsync_pressure_per_1000_writes
              << " last_effective_batch_delay_us=" << summary.last_effective_batch_delay_us
              << std::endl;
}

void run_concurrency_stress_json(int duration_seconds, ConcurrencyStressProfile profile) {
    std::cout << StressSummaryToJson(run_concurrency_stress_capture(duration_seconds, profile)) << std::endl;
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

std::vector<MicrobenchCaseResult> run_microbench_capture() {
    std::vector<MicrobenchCaseResult> results;

    {
        TestDir dir("microbench_wal_append");
        const std::string db_path = dir.file("store.dat");
        KVStoreOptions options;
        options.max_batch_size = 1;
        options.max_batch_delay_us = 0;
        options.auto_compact_wal_bytes_threshold = 0;
        KVStore store(db_path, options);

        constexpr int kOperations = 500;
        const auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < kOperations; ++i) {
            store.Put(i, text("microbench_payload_" + std::to_string(i)));
        }
        const auto end = std::chrono::steady_clock::now();
        const double duration_s = std::chrono::duration<double>(end - start).count();
        results.push_back({
            "wal_append",
            duration_s,
            duration_s > 0.0 ? static_cast<double>(kOperations) / duration_s : 0.0,
            kOperations,
            store.GetMetrics().wal_bytes_written,
        });
    }

    {
        TestDir dir("microbench_scan");
        const std::string db_path = dir.file("store.dat");
        KVStore store(db_path);
        constexpr int kKeys = 2000;
        for (int i = 0; i < kKeys; ++i) {
            store.Put("scan:" + std::to_string(i), text("value_" + std::to_string(i)));
        }
        store.Compact();

        constexpr int kOperations = 250;
        const auto start = std::chrono::steady_clock::now();
        size_t scanned = 0;
        for (int i = 0; i < kOperations; ++i) {
            const auto rows = store.Scan("scan:100", "scan:399");
            scanned += rows.size();
        }
        const auto end = std::chrono::steady_clock::now();
        const double duration_s = std::chrono::duration<double>(end - start).count();
        results.push_back({
            "scan",
            duration_s,
            duration_s > 0.0 ? static_cast<double>(kOperations) / duration_s : 0.0,
            kOperations,
            scanned,
        });
    }

    {
        TestDir dir("microbench_recovery");
        const std::string db_path = dir.file("store.dat");
        {
            KVStore store(db_path);
            constexpr int kKeys = 1500;
            for (int i = 0; i < kKeys; ++i) {
                store.Put(i, text("recovery_" + std::to_string(i)));
            }
        }

        constexpr int kReopens = 25;
        const auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < kReopens; ++i) {
            KVStore reopened(db_path);
            const auto sample = reopened.Get(42);
            require(sample.has_value(), "microbench recovery should reopen valid data");
        }
        const auto end = std::chrono::steady_clock::now();
        const double duration_s = std::chrono::duration<double>(end - start).count();
        results.push_back({
            "recovery",
            duration_s,
            duration_s > 0.0 ? static_cast<double>(kReopens) / duration_s : 0.0,
            kReopens,
            0,
        });
    }

    {
        TestDir dir("microbench_compaction");
        const std::string db_path = dir.file("store.dat");
        KVStore store(db_path);
        constexpr int kKeys = 1500;
        for (int i = 0; i < kKeys; ++i) {
            store.Put(i, text("compact_" + std::to_string(i)));
        }
        for (int i = 0; i < kKeys / 3; ++i) {
            store.Delete(i);
        }

        constexpr int kOperations = 5;
        const auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < kOperations; ++i) {
            store.Compact();
            store.Put(5000 + i, text("compact_probe_" + std::to_string(i)));
        }
        const auto end = std::chrono::steady_clock::now();
        const double duration_s = std::chrono::duration<double>(end - start).count();
        results.push_back({
            "compaction",
            duration_s,
            duration_s > 0.0 ? static_cast<double>(kOperations) / duration_s : 0.0,
            kOperations,
            store.GetMetrics().total_snapshot_bytes_written,
        });
    }

    {
        TestDir dir("microbench_rewrite");
        const std::string db_path = dir.file("store.dat");
        {
            KVStore store(db_path);
            constexpr int kKeys = 1200;
            for (int i = 0; i < kKeys; ++i) {
                store.Put(i, text("rewrite_" + std::to_string(i)));
            }
        }

        constexpr int kOperations = 5;
        const auto start = std::chrono::steady_clock::now();
        uint64_t rewritten_bytes = 0;
        for (int i = 0; i < kOperations; ++i) {
            require(run_rewrite_format(db_path) == 0, "microbench rewrite should succeed");
            KVStore reopened(db_path);
            const auto sample = reopened.Get(42);
            require(sample.has_value(), "microbench rewrite should preserve valid data");
            rewritten_bytes = reopened.GetMetrics().total_snapshot_bytes_written;
        }
        const auto end = std::chrono::steady_clock::now();
        const double duration_s = std::chrono::duration<double>(end - start).count();
        results.push_back({
            "rewrite",
            duration_s,
            duration_s > 0.0 ? static_cast<double>(kOperations) / duration_s : 0.0,
            kOperations,
            rewritten_bytes,
        });
    }

    return results;
}

void run_microbench() {
    const auto results = run_microbench_capture();
    for (const auto& result : results) {
        std::cout << "[MICROBENCH]"
                  << " name=" << result.name
                  << " duration_s=" << result.duration_s
                  << " ops_per_s=" << result.ops_per_s
                  << " operations=" << result.operations
                  << " bytes=" << result.bytes
                  << std::endl;
    }
}

void run_microbench_json() {
    std::cout << MicrobenchResultsToJson(run_microbench_capture()) << std::endl;
}

void run_benchmark_baseline_json() {
    const BenchmarkResult result = run_benchmark_capture(
        make_benchmark_config("default-baseline", 8, 4, 3000, 50000));
    std::cout << BenchmarkResultToJson(result) << std::endl;
}

}  // namespace

int run_inspect_format(const std::string& db_path) {
    return run_inspect_format_impl(db_path);
}

int run_rewrite_format(const std::string& db_path) {
    return run_rewrite_format_impl(db_path);
}

int run_verify_format(const std::string& db_path) {
    return run_verify_format_impl(db_path);
}

int run_compatibility_matrix() {
    return run_compatibility_matrix_impl();
}

int run_compare_benchmark_baseline_entrypoint(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_write_ratio_pct,
    double min_read_ratio_pct,
    double max_latency_ratio_pct,
    double max_p95_latency_ratio_pct,
    double max_p99_latency_ratio_pct,
    double max_fsync_pressure_ratio_pct,
    double min_batch_fill_ratio_pct) {
    return run_compare_benchmark_baseline(
        baseline_path,
        candidate_path,
        min_write_ratio_pct,
        min_read_ratio_pct,
        max_latency_ratio_pct,
        max_p95_latency_ratio_pct,
        max_p99_latency_ratio_pct,
        max_fsync_pressure_ratio_pct,
        min_batch_fill_ratio_pct);
}

int run_compare_microbench_entrypoint(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_ops_ratio_pct,
    double min_compaction_ratio_pct,
    double min_rewrite_ratio_pct,
    double min_recovery_ratio_pct) {
    return run_compare_microbench(
        baseline_path,
        candidate_path,
        min_ops_ratio_pct,
        min_compaction_ratio_pct,
        min_rewrite_ratio_pct,
        min_recovery_ratio_pct);
}

int run_benchmark_trend_entrypoint(const std::string& directory_path, size_t recent_window) {
    return run_benchmark_trend(directory_path, recent_window);
}

int run_benchmark_trend_json_entrypoint(const std::string& directory_path, size_t recent_window) {
    return run_benchmark_trend_json(directory_path, recent_window);
}

int run_microbench_trend_json_entrypoint(const std::string& directory_path, size_t recent_window) {
    return run_microbench_trend_json(directory_path, recent_window);
}

int main(int argc, char* argv[]) {
    g_program_path = std::filesystem::absolute(argv[0]).string();
    if (argc > 1) {
        const std::string command = argv[1];
        if (command == "bench") {
            run_benchmark();
            return 0;
        }
        if (command == "microbench") {
            run_microbench();
            return 0;
        }
        if (command == "microbench-json") {
            run_microbench_json();
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
        if (command == "compare-microbench") {
            if (argc < 4 || argc > 8) {
                std::cerr << "Usage: kv_test compare-microbench <baseline_json> <candidate_json> [min_ops_ratio_pct min_compaction_ratio_pct min_rewrite_ratio_pct min_recovery_ratio_pct]" << std::endl;
                return 1;
            }
            const double min_ops_ratio_pct = argc > 4 ? std::stod(argv[4]) : 80.0;
            const double min_compaction_ratio_pct = argc > 5 ? std::stod(argv[5]) : 75.0;
            const double min_rewrite_ratio_pct = argc > 6 ? std::stod(argv[6]) : 75.0;
            const double min_recovery_ratio_pct = argc > 7 ? std::stod(argv[7]) : 80.0;
            return run_compare_microbench(
                argv[2],
                argv[3],
                min_ops_ratio_pct,
                min_compaction_ratio_pct,
                min_rewrite_ratio_pct,
                min_recovery_ratio_pct);
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
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-baselines <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_benchmark_trend(argv[2], recent_window);
        }
        if (command == "trend-baselines-json") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-baselines-json <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_benchmark_trend_json(argv[2], recent_window);
        }
        if (command == "trend-microbench") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-microbench <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_microbench_trend(argv[2], recent_window);
        }
        if (command == "trend-microbench-json") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-microbench-json <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_microbench_trend_json(argv[2], recent_window);
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
        if (command == "concurrency-stress-json") {
            const int duration_seconds = argc > 2 ? std::stoi(argv[2]) : 10;
            const std::string profile_name = argc > 3 ? argv[3] : "balanced";
            const auto profile = parse_concurrency_stress_profile_name(profile_name);
            if (!profile.has_value()) {
                std::cerr << "Unknown concurrency stress profile: " << profile_name << std::endl;
                return 1;
            }
            run_concurrency_stress_json(duration_seconds, *profile);
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
        std::cerr << "Usage: kv_test [bench|microbench|microbench-json|bench-json|bench-baseline-json|compare-microbench|compare-baseline|trend-baselines|trend-baselines-json|trend-microbench|trend-microbench-json|profile-json|soak|concurrency-stress|concurrency-stress-json|inspect-format|rewrite-format|verify-format|compat-matrix|fault-inject]" << std::endl;
        return 1;
    }

    kvstore::tests::integration::TestCases tests;
    kvstore::tests::integration::register_basic_kv_tests(tests);
    kvstore::tests::integration::register_benchmark_trend_tests(tests);
    kvstore::tests::integration::register_metrics_controller_tests(tests);
    kvstore::tests::integration::register_recovery_format_tests(tests);
    const std::vector<test_support::NamedTest> remaining_tests = {
        {"benchmark result json reports summary and metrics", test_benchmark_result_json_reports_summary_and_metrics},
        {"microbench json reports cases", test_microbench_json_reports_cases},
        {"stress summary json reports profile", test_stress_summary_json_reports_profile},
        {"internal format helpers round trip keys", test_internal_format_helpers_round_trip_keys},
        {"internal format helpers checksum distinguishes payloads", test_internal_format_helpers_checksum_distinguishes_payloads},
        {"internal metrics helpers compute percentiles and ratios", test_internal_metrics_helpers_compute_percentiles_and_ratios},
        {"soak profiles are distinct", test_soak_profiles_are_distinct},
        {"ordering and updates", test_ordering_and_updates},
        {"compaction persists snapshot and resets WAL", test_compaction_persists_snapshot_and_resets_wal},
        {"crash after wal fsync recovers latest write", test_crash_after_wal_fsync_recovers_latest_write},
        {"crash after wal fsync recovers latest batch", test_crash_after_wal_fsync_recovers_latest_batch},
        {"crash after snapshot rename recovers consistent state",
         test_crash_after_snapshot_rename_recovers_consistent_state},
        {"crash before snapshot rename replays old wal", test_crash_before_snapshot_rename_replays_old_wal},
        {"crash after wal rotation recovers snapshot", test_crash_after_wal_rotation_recovers_snapshot},
        {"concurrent reads and writes", test_concurrent_reads_and_writes},
        {"many concurrent writers", test_many_concurrent_writers},
        {"concurrent compaction with writes", test_concurrent_compaction_with_writes},
        {"recommended profiles are distinct", test_recommended_profiles_are_distinct},
        {"concurrency stress profiles are distinct", test_concurrency_stress_profiles_are_distinct},
        {"options to json reports profile fields", test_options_to_json_reports_profile_fields},
        {"metrics to json reports core fields", test_metrics_to_json_reports_core_fields},
    };
    tests.insert(tests.end(), remaining_tests.begin(), remaining_tests.end());

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
