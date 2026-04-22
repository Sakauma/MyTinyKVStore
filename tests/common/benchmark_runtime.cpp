#include "tests/common/benchmark_entrypoints.h"

#include "tests/common/benchmark_shared.h"
#include "kvstore.h"
#include "tests/common/test_support.h"

#include <iostream>

namespace {

using test_support::TestDir;
using test_support::text;

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
