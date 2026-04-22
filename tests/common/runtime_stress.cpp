#include "tests/common/runtime_entrypoints.h"

#include "tests/common/benchmark_entrypoints.h"
#include "tests/common/runtime_shared.h"
#include "tests/common/test_support.h"

#include "kvstore.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>
#include <sys/wait.h>

namespace {

using test_support::as_string;
using test_support::require;
using test_support::TestDir;
using test_support::text;

std::string g_program_path;

void run_failpoint_child(const std::string& scenario, const std::string& db_path) {
    require(!g_program_path.empty(), "runtime program path must be initialized before running failpoint child");
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

StressSummary run_concurrency_stress_capture(int duration_seconds, ConcurrencyStressProfile profile) {
    TestDir dir("concurrency_stress");
    const std::string db_path = dir.file("store.dat");

    const ConcurrencyStressProfileConfig config = make_concurrency_stress_profile_config(profile);
    const int total_keys = config.writer_count * config.keys_per_writer;
    std::vector<std::vector<std::optional<std::string>>> expected_by_writer(
        static_cast<size_t>(config.writer_count),
        std::vector<std::optional<std::string>>(static_cast<size_t>(config.keys_per_writer)));
    std::atomic<uint64_t> put_operations {0};
    std::atomic<uint64_t> delete_operations {0};
    std::atomic<uint64_t> batch_requests {0};
    std::atomic<uint64_t> batch_put_operations {0};
    std::atomic<uint64_t> batch_delete_operations {0};

    KVStoreMetrics final_metrics;
    {
        KVStore store(db_path, config.options);
        std::atomic<bool> stop {false};
        std::vector<std::thread> threads;

        for (int writer_id = 0; writer_id < config.writer_count; ++writer_id) {
            threads.emplace_back([&store,
                                  &stop,
                                  &config,
                                  &expected_by_writer,
                                  &put_operations,
                                  &delete_operations,
                                  &batch_requests,
                                  &batch_put_operations,
                                  &batch_delete_operations,
                                  writer_id]() {
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
                        delete_operations.fetch_add(1, std::memory_order_relaxed);
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
                        put_operations.fetch_add(1, std::memory_order_relaxed);
                        expected[static_cast<size_t>(local_key)] = payload;
                        continue;
                    }

                    std::vector<BatchWriteOperation> operations;
                    std::vector<std::pair<int, std::optional<std::string>>> applied;
                    operations.reserve(static_cast<size_t>(config.batch_width));
                    applied.reserve(static_cast<size_t>(config.batch_width));
                    uint64_t local_batch_puts = 0;
                    uint64_t local_batch_deletes = 0;
                    for (int batch_index = 0; batch_index < config.batch_width; ++batch_index) {
                        const int local_key = key_dist(gen);
                        const int global_key = writer_id * config.keys_per_writer + local_key;
                        const bool do_delete = (op_dist(gen) % 4) == 0;
                        if (do_delete) {
                            operations.push_back(BatchWriteOperation::DeleteInt(global_key));
                            applied.push_back({local_key, std::nullopt});
                            ++local_batch_deletes;
                            continue;
                        }
                        const std::string payload =
                            "stress_batch_" + std::to_string(writer_id) + "_" +
                            std::to_string(local_key) + "_" + std::to_string(version++);
                        operations.push_back(BatchWriteOperation::PutInt(global_key, text(payload)));
                        applied.push_back({local_key, payload});
                        ++local_batch_puts;
                    }
                    store.WriteBatch(operations);
                    batch_requests.fetch_add(1, std::memory_order_relaxed);
                    batch_put_operations.fetch_add(local_batch_puts, std::memory_order_relaxed);
                    batch_delete_operations.fetch_add(local_batch_deletes, std::memory_order_relaxed);
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
    uint64_t final_live_objects = 0;
    for (const auto& by_writer : expected_by_writer) {
        for (const auto& value : by_writer) {
            if (value.has_value()) {
                ++final_live_objects;
            }
        }
    }
    summary.profile = concurrency_stress_profile_name(profile);
    summary.duration_seconds = duration_seconds;
    summary.writer_count = config.writer_count;
    summary.reader_count = config.reader_count;
    summary.compactor_count = config.compactor_count;
    summary.recovery_reopen_cycles = config.recovery_reopen_cycles;
    summary.committed_write_requests = final_metrics.committed_write_requests;
    summary.put_operations = put_operations.load(std::memory_order_relaxed);
    summary.delete_operations = delete_operations.load(std::memory_order_relaxed);
    summary.batch_requests = batch_requests.load(std::memory_order_relaxed);
    summary.batch_put_operations = batch_put_operations.load(std::memory_order_relaxed);
    summary.batch_delete_operations = batch_delete_operations.load(std::memory_order_relaxed);
    summary.final_live_objects = final_live_objects;
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
              << " put_operations=" << summary.put_operations
              << " delete_operations=" << summary.delete_operations
              << " batch_requests=" << summary.batch_requests
              << " batch_put_operations=" << summary.batch_put_operations
              << " batch_delete_operations=" << summary.batch_delete_operations
              << " final_live_objects=" << summary.final_live_objects
              << " max_pending_queue_depth=" << summary.max_pending_queue_depth
              << " manual_compactions_completed=" << summary.manual_compactions_completed
              << " auto_compactions_completed=" << summary.auto_compactions_completed
              << " observed_fsync_pressure_per_1000_writes=" << summary.observed_fsync_pressure_per_1000_writes
              << " last_effective_batch_delay_us=" << summary.last_effective_batch_delay_us
              << std::endl;
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
                        "value_" + std::to_string(writer_id) + "_" + std::to_string(key) + "_" +
                        std::to_string(version++);
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
                    require(as_string(*value).rfind("value_", 0) == 0,
                            "soak readers should only observe complete values");
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

}  // namespace

void set_runtime_program_path_entrypoint(const std::string& program_path) {
    g_program_path = std::filesystem::absolute(program_path).string();
}

int run_fault_injection_scenario_entrypoint(const std::string& scenario, const std::string& db_path) {
    return run_fault_injection_scenario(scenario, db_path);
}

int run_failpoint_child_entrypoint(const std::string& scenario, const std::string& db_path) {
    run_failpoint_child(scenario, db_path);
    return 0;
}

int run_soak_command_entrypoint(int duration_seconds, const std::string& profile_name) {
    const auto profile = parse_soak_profile_name(profile_name);
    if (!profile.has_value()) {
        std::cerr << "Unknown soak profile: " << profile_name << std::endl;
        return 1;
    }
    run_soak_test(duration_seconds, *profile);
    return 0;
}

int run_concurrency_stress_command_entrypoint(int duration_seconds, const std::string& profile_name) {
    const auto profile = parse_concurrency_stress_profile_name(profile_name);
    if (!profile.has_value()) {
        std::cerr << "Unknown concurrency stress profile: " << profile_name << std::endl;
        return 1;
    }
    run_concurrency_stress_test(duration_seconds, *profile);
    return 0;
}

int run_concurrency_stress_json_entrypoint(int duration_seconds, const std::string& profile_name) {
    const auto profile = parse_concurrency_stress_profile_name(profile_name);
    if (!profile.has_value()) {
        std::cerr << "Unknown concurrency stress profile: " << profile_name << std::endl;
        return 1;
    }
    std::cout << stress_summary_to_json(run_concurrency_stress_capture(duration_seconds, *profile)) << std::endl;
    return 0;
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
    summary.put_operations = 99;
    summary.delete_operations = 24;
    summary.batch_requests = 17;
    summary.batch_put_operations = 60;
    summary.batch_delete_operations = 11;
    summary.final_live_objects = 33;
    summary.max_pending_queue_depth = 7;
    return stress_summary_to_json(summary);
}
