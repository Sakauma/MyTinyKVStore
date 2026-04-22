#ifndef KVSTORE_TESTS_COMMON_BENCHMARK_ANALYSIS_H
#define KVSTORE_TESTS_COMMON_BENCHMARK_ANALYSIS_H

#include "tests/common/benchmark_shared.h"

#include <cstddef>
#include <string>
#include <vector>

struct BenchmarkBaselineComparison {
    double write_ratio_pct = 0.0;
    double read_ratio_pct = 0.0;
    double latency_ratio_pct = 0.0;
    double p95_latency_ratio_pct = 0.0;
    double p99_latency_ratio_pct = 0.0;
    double fsync_pressure_ratio_pct = 0.0;
    double batch_fill_ratio_pct = 0.0;
    bool pass = false;
};

struct MicrobenchComparisonResult {
    std::string name;
    double ops_ratio_pct = 0.0;
    double min_ratio_pct = 0.0;
};

struct BenchmarkTrendSummary {
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

BenchmarkBaselineComparison compare_benchmark_baseline(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_write_ratio_pct,
    double min_read_ratio_pct,
    double max_latency_ratio_pct,
    double max_p95_latency_ratio_pct,
    double max_p99_latency_ratio_pct,
    double max_fsync_pressure_ratio_pct,
    double min_batch_fill_ratio_pct);

std::vector<MicrobenchComparisonResult> compare_microbench(
    const std::string& baseline_path,
    const std::string& candidate_path,
    double min_ops_ratio_pct,
    double min_compaction_ratio_pct,
    double min_rewrite_ratio_pct,
    double min_recovery_ratio_pct,
    bool* pass);

BenchmarkTrendSummary collect_benchmark_trend_summary(const std::string& directory_path, size_t recent_window);
std::string benchmark_trend_summary_to_json(const BenchmarkTrendSummary& summary);

std::vector<MicrobenchTrendCaseSummary> collect_microbench_trend_summary(
    const std::string& directory_path,
    size_t recent_window);
std::string microbench_trend_summary_to_json(const std::vector<MicrobenchTrendCaseSummary>& summaries);

#endif  // KVSTORE_TESTS_COMMON_BENCHMARK_ANALYSIS_H
