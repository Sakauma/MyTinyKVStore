#ifndef KVSTORE_TESTS_COMMON_BENCHMARK_ENTRYPOINTS_H
#define KVSTORE_TESTS_COMMON_BENCHMARK_ENTRYPOINTS_H

#include <cstddef>
#include <string>

std::string benchmark_result_json_fixture_entrypoint();
std::string microbench_results_json_fixture_entrypoint();
std::string stress_summary_json_fixture_entrypoint();

int run_compare_benchmark_baseline_entrypoint(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_write_ratio_pct = 85.0,
    double min_read_ratio_pct = 85.0,
    double max_latency_ratio_pct = 125.0,
    double max_p95_latency_ratio_pct = 150.0,
    double max_p99_latency_ratio_pct = 175.0,
    double max_fsync_pressure_ratio_pct = 150.0,
    double min_batch_fill_ratio_pct = 75.0);

int run_compare_microbench_entrypoint(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_ops_ratio_pct = 80.0,
    double min_compaction_ratio_pct = 75.0,
    double min_rewrite_ratio_pct = 75.0,
    double min_recovery_ratio_pct = 80.0);

int run_benchmark_trend_entrypoint(const std::string& directory_path, size_t recent_window);
int run_benchmark_trend_json_entrypoint(const std::string& directory_path, size_t recent_window);
int run_microbench_trend_json_entrypoint(const std::string& directory_path, size_t recent_window);
int run_benchmark_json_entrypoint();
int run_profile_json_entrypoint(const std::string& name);

#endif  // KVSTORE_TESTS_COMMON_BENCHMARK_ENTRYPOINTS_H
