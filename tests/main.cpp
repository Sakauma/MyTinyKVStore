#include "kvstore.h"
#include "tests/common/benchmark_entrypoints.h"
#include "tests/common/cli_entrypoints.h"
#include "tests/common/runtime_entrypoints.h"
#include "tests/common/test_support.h"
#include "tests/integration/test_registry.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
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

using test_support::append_bytes;
using test_support::as_string;
using test_support::file_size_or_zero;
using test_support::require;
using test_support::TestDir;
using test_support::text;

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

}

std::string stress_summary_json_fixture_entrypoint() {
    StressSummary summary;
    summary.profile = "balanced";
    summary.duration_seconds = 1;
    summary.writer_count = 8;
    summary.reader_count = 4;
    summary.compactor_count = 1;
    summary.recovery_reopen_cycles = 6;
    summary.committed_write_requests = 123;
    summary.max_pending_queue_depth = 7;
    return StressSummaryToJson(summary);
}

int run_profile_json_entrypoint(const std::string& name) {
    return run_profile_json(name);
}

int run_failpoint_child_entrypoint(const std::string& scenario, const std::string& db_path) {
    run_failpoint_child(scenario, db_path);
    return 0;
}

SoakProfileSummary soak_profile_summary_entrypoint(const std::string& profile_name) {
    const auto profile = parse_soak_profile_name(profile_name);
    require(profile.has_value(), "unknown soak profile");
    const SoakProfileConfig config = make_soak_profile_config(*profile);
    SoakProfileSummary summary;
    summary.writer_count = config.writer_count;
    summary.reader_count = config.reader_count;
    summary.key_space = config.key_space;
    summary.compaction_interval_ms = config.compaction_interval_ms;
    summary.max_batch_size = config.options.max_batch_size;
    summary.max_batch_delay_us = config.options.max_batch_delay_us;
    return summary;
}

ConcurrencyStressProfileSummary concurrency_stress_profile_summary_entrypoint(const std::string& profile_name) {
    const auto profile = parse_concurrency_stress_profile_name(profile_name);
    require(profile.has_value(), "unknown concurrency stress profile");
    const ConcurrencyStressProfileConfig config = make_concurrency_stress_profile_config(*profile);
    ConcurrencyStressProfileSummary summary;
    summary.writer_count = config.writer_count;
    summary.reader_count = config.reader_count;
    summary.compactor_count = config.compactor_count;
    summary.keys_per_writer = config.keys_per_writer;
    summary.compaction_interval_ms = config.compaction_interval_ms;
    summary.batch_width = config.batch_width;
    summary.recovery_reopen_cycles = config.recovery_reopen_cycles;
    summary.max_batch_size = config.options.max_batch_size;
    summary.max_batch_delay_us = config.options.max_batch_delay_us;
    summary.auto_compact_wal_bytes_threshold = config.options.auto_compact_wal_bytes_threshold;
    return summary;
}

int main(int argc, char* argv[]) {
    g_program_path = std::filesystem::absolute(argv[0]).string();
    if (argc > 1) {
        const std::string command = argv[1];
        if (command == "bench") {
            run_benchmark_command();
            return 0;
        }
        if (command == "microbench") {
            run_microbench_command();
            return 0;
        }
        if (command == "microbench-json") {
            run_microbench_json_command();
            return 0;
        }
        if (command == "bench-json") {
            return run_benchmark_json_entrypoint();
        }
        if (command == "bench-baseline-json") {
            run_benchmark_baseline_json_command();
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
            return run_compare_microbench_entrypoint(
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
            return run_compare_benchmark_baseline_entrypoint(
                argv[2],
                argv[3],
                min_write_ratio_pct,
                min_read_ratio_pct,
                max_latency_ratio_pct);
        }
        if (command == "trend-baselines") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-baselines <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_benchmark_trend_entrypoint(argv[2], recent_window);
        }
        if (command == "trend-baselines-json") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-baselines-json <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_benchmark_trend_json_entrypoint(argv[2], recent_window);
        }
        if (command == "trend-microbench") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-microbench <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_microbench_trend_entrypoint(argv[2], recent_window);
        }
        if (command == "trend-microbench-json") {
            if (argc != 3 && argc != 4) {
                std::cerr << "Usage: kv_test trend-microbench-json <baseline_dir> [recent_window_count]" << std::endl;
                return 1;
            }
            const size_t recent_window = argc == 4 ? static_cast<size_t>(std::stoul(argv[3])) : 5;
            return run_microbench_trend_json_entrypoint(argv[2], recent_window);
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
    kvstore::tests::integration::register_durability_smoke_tests(tests);
    kvstore::tests::integration::register_json_cli_tests(tests);
    kvstore::tests::integration::register_metrics_controller_tests(tests);
    kvstore::tests::integration::register_recovery_format_tests(tests);
    kvstore::tests::integration::register_runtime_concurrency_tests(tests);

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
