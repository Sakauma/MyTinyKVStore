#include "tests/common/benchmark_entrypoints.h"

#include "tests/common/benchmark_analysis.h"

#include <iostream>
#include <string>
#include <vector>

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
    const BenchmarkBaselineComparison comparison = compare_benchmark_baseline(
        baseline_path,
        candidate_path,
        min_write_ratio_pct,
        min_read_ratio_pct,
        max_latency_ratio_pct,
        max_p95_latency_ratio_pct,
        max_p99_latency_ratio_pct,
        max_fsync_pressure_ratio_pct,
        min_batch_fill_ratio_pct);

    std::cout << "baseline=" << baseline_path
              << " candidate=" << candidate_path
              << " write_ratio_pct=" << comparison.write_ratio_pct
              << " read_ratio_pct=" << comparison.read_ratio_pct
              << " latency_ratio_pct=" << comparison.latency_ratio_pct
              << " p95_latency_ratio_pct=" << comparison.p95_latency_ratio_pct
              << " p99_latency_ratio_pct=" << comparison.p99_latency_ratio_pct
              << " fsync_pressure_ratio_pct=" << comparison.fsync_pressure_ratio_pct
              << " batch_fill_ratio_pct=" << comparison.batch_fill_ratio_pct
              << " min_write_ratio_pct=" << min_write_ratio_pct
              << " min_read_ratio_pct=" << min_read_ratio_pct
              << " max_latency_ratio_pct=" << max_latency_ratio_pct
              << " max_p95_latency_ratio_pct=" << max_p95_latency_ratio_pct
              << " max_p99_latency_ratio_pct=" << max_p99_latency_ratio_pct
              << " max_fsync_pressure_ratio_pct=" << max_fsync_pressure_ratio_pct
              << " min_batch_fill_ratio_pct=" << min_batch_fill_ratio_pct
              << " status=" << (comparison.pass ? "pass" : "fail")
              << std::endl;
    return comparison.pass ? 0 : 2;
}

int run_compare_microbench_entrypoint(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_ops_ratio_pct,
    double min_compaction_ratio_pct,
    double min_rewrite_ratio_pct,
    double min_recovery_ratio_pct) {
    bool pass = false;
    const std::vector<MicrobenchComparisonResult> comparisons = compare_microbench(
        baseline_path,
        candidate_path,
        min_ops_ratio_pct,
        min_compaction_ratio_pct,
        min_rewrite_ratio_pct,
        min_recovery_ratio_pct,
        &pass);

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

int run_benchmark_trend_entrypoint(const std::string& directory_path, size_t recent_window) {
    const BenchmarkTrendSummary trend = collect_benchmark_trend_summary(directory_path, recent_window);
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

int run_benchmark_trend_json_entrypoint(const std::string& directory_path, size_t recent_window) {
    std::cout << benchmark_trend_summary_to_json(collect_benchmark_trend_summary(directory_path, recent_window))
              << std::endl;
    return 0;
}

int run_microbench_trend_entrypoint(const std::string& directory_path, size_t recent_window) {
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

int run_microbench_trend_json_entrypoint(const std::string& directory_path, size_t recent_window) {
    std::cout << microbench_trend_summary_to_json(collect_microbench_trend_summary(directory_path, recent_window))
              << std::endl;
    return 0;
}
