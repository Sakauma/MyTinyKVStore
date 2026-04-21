#include "tests/common/benchmark_entrypoints.h"

#include "kvstore.h"
#include "tests/common/cli_entrypoints.h"
#include "tests/common/test_support.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

using test_support::as_string;
using test_support::require;
using test_support::TestDir;
using test_support::text;

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
        result.writes == 0 ? 0.0
                           : static_cast<double>(write_latency_ns.load(std::memory_order_relaxed)) / result.writes / 1000.0;
    result.metrics = store.GetMetrics();
    return result;
}

std::string benchmark_result_to_json(const BenchmarkResult& result) {
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

std::string microbench_results_to_json(const std::vector<MicrobenchCaseResult>& results) {
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

}  // namespace

void run_benchmark_command() {
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
    std::cout << std::endl;
}

int run_benchmark_json_entrypoint() {
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
    return 0;
}

void run_microbench_command() {
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

void run_microbench_json_command() {
    std::cout << microbench_results_to_json(run_microbench_capture()) << std::endl;
}

void run_benchmark_baseline_json_command() {
    const BenchmarkResult result = run_benchmark_capture(
        make_benchmark_config("default-baseline", 8, 4, 3000, 50000));
    std::cout << benchmark_result_to_json(result) << std::endl;
}

std::string benchmark_result_json_fixture_entrypoint() {
    BenchmarkResult result;
    result.config = make_benchmark_config("default-baseline", 8, 4, 3000, 50000);
    result.options = benchmark_options();
    result.duration_s = 1.0;
    result.writes = 1000;
    result.reads = 2000;
    result.write_ops_per_s = 1000.0;
    result.read_ops_per_s = 2000.0;
    result.avg_write_latency_us = 100.0;
    result.metrics.committed_write_requests = 1000;
    result.metrics.wal_fsync_calls = 50;
    return benchmark_result_to_json(result);
}

std::string microbench_results_json_fixture_entrypoint() {
    return microbench_results_to_json({
        {"wal_append", 0.1, 1000.0, 100, 4096},
        {"scan", 0.2, 500.0, 100, 0},
        {"compaction", 0.3, 10.0, 3, 8192},
        {"rewrite", 0.4, 5.0, 2, 4096},
    });
}
