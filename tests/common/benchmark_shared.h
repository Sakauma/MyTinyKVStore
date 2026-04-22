#ifndef KVSTORE_TESTS_COMMON_BENCHMARK_SHARED_H
#define KVSTORE_TESTS_COMMON_BENCHMARK_SHARED_H

#include "kvstore.h"

#include <cstdint>
#include <string>
#include <vector>

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
    int key_space);

KVStoreOptions benchmark_options();
BenchmarkResult run_benchmark_capture(const BenchmarkConfig& config);
std::vector<MicrobenchCaseResult> run_microbench_capture();
std::string benchmark_result_to_json(const BenchmarkResult& result);
std::string microbench_results_to_json(const std::vector<MicrobenchCaseResult>& results);

#endif  // KVSTORE_TESTS_COMMON_BENCHMARK_SHARED_H
