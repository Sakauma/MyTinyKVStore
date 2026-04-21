#include "tests/integration/test_registry.h"

#include "tests/common/benchmark_entrypoints.h"
#include "tests/common/test_support.h"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

namespace kvstore::tests::integration {
namespace {

using test_support::require;
using test_support::TestDir;

void write_text_file(const std::string& path, const std::string& contents) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    require(out.is_open(), "Expected text file to be writable");
    out << contents;
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

std::map<std::string, std::string> parse_kv_line(const std::string& line) {
    std::map<std::string, std::string> values;
    std::istringstream in(line);
    std::string token;
    while (in >> token) {
        const size_t equals = token.find('=');
        require(equals != std::string::npos, "Expected key=value token");
        values[token.substr(0, equals)] = token.substr(equals + 1);
    }
    return values;
}

std::string benchmark_summary_json(
    double write_ops_per_s,
    double read_ops_per_s,
    double avg_write_latency_us,
    double p95_latency_us = 1000.0,
    double p99_latency_us = 1200.0,
    double fsync_pressure = 200.0,
    double batch_fill = 500.0) {
    std::ostringstream out;
    out << '{'
        << "\"write_ops_per_s\":" << write_ops_per_s
        << ",\"read_ops_per_s\":" << read_ops_per_s
        << ",\"avg_write_latency_us\":" << avg_write_latency_us
        << ",\"approx_write_latency_p95_us\":" << p95_latency_us
        << ",\"approx_write_latency_p99_us\":" << p99_latency_us
        << ",\"observed_fsync_pressure_per_1000_writes\":" << fsync_pressure
        << ",\"recent_batch_fill_per_1000\":" << batch_fill
        << '}';
    return out.str();
}

std::string microbench_results_json(
    std::initializer_list<std::tuple<const char*, double, uint64_t, uint64_t>> cases) {
    std::ostringstream out;
    out << "{\"cases\":[";
    size_t index = 0;
    for (const auto& [name, ops_per_s, operations, bytes] : cases) {
        if (index++ != 0) {
            out << ',';
        }
        out << '{'
            << "\"name\":\"" << name << "\""
            << ",\"duration_s\":1.0"
            << ",\"ops_per_s\":" << ops_per_s
            << ",\"operations\":" << operations
            << ",\"bytes\":" << bytes
            << '}';
    }
    out << "]}";
    return out.str();
}

std::pair<int, std::string> capture_benchmark_trend_json(
    const std::string& directory_path,
    size_t recent_window = 5) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_benchmark_trend_json_entrypoint(directory_path, recent_window);
    std::cout.rdbuf(original);
    return {status, out.str()};
}

std::pair<int, std::map<std::string, std::string>> capture_benchmark_trend(
    const std::string& directory_path,
    size_t recent_window = 5) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_benchmark_trend_entrypoint(directory_path, recent_window);
    std::cout.rdbuf(original);
    return {status, parse_kv_line(out.str())};
}

std::pair<int, std::string> capture_microbench_trend_json(
    const std::string& directory_path,
    size_t recent_window = 5) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = run_microbench_trend_json_entrypoint(directory_path, recent_window);
    std::cout.rdbuf(original);
    return {status, out.str()};
}

void test_benchmark_trend_json_reports_recent_window() {
    TestDir dir("benchmark_trend_json");
    const std::string baseline_dir = dir.file("baselines");
    std::filesystem::create_directories(baseline_dir);

    write_text_file(
        (std::filesystem::path(baseline_dir) / "20260101T000000.json").string(),
        benchmark_summary_json(1000.0, 2000.0, 100.0));
    write_text_file(
        (std::filesystem::path(baseline_dir) / "20260102T000000.json").string(),
        benchmark_summary_json(1200.0, 2100.0, 90.0));

    const auto [status, json] = capture_benchmark_trend_json(baseline_dir, 2);
    require(status == 0, "trend-baselines-json should succeed for non-empty baseline directories");
    require(json.find("\"recent_window_count\":2") != std::string::npos,
            "trend json should include the recent window count");
    require(extract_json_number(json, "latest_vs_recent_avg_write_ratio_pct") > 109.0 &&
                extract_json_number(json, "latest_vs_recent_avg_write_ratio_pct") < 109.1,
            "trend json should include recent-window write ratios");
    require(json.find("\"recent_write_trend\":\"improving\"") != std::string::npos,
            "trend json should include recent trend classification");
}

void test_compare_benchmark_baseline_passes_within_thresholds() {
    TestDir dir("compare_baseline_pass");
    const std::string baseline_path = dir.file("baseline.json");
    const std::string candidate_path = dir.file("candidate.json");

    write_text_file(baseline_path, benchmark_summary_json(1000.0, 2000.0, 100.0, 1000.0, 1200.0, 200.0, 500.0));
    write_text_file(candidate_path, benchmark_summary_json(900.0, 1800.0, 115.0, 1300.0, 1800.0, 250.0, 450.0));

    const int status = run_compare_benchmark_baseline_entrypoint(baseline_path, candidate_path);
    require(status == 0, "compare-baseline should pass when throughput and latency stay within thresholds");
}

void test_compare_benchmark_baseline_rejects_regression() {
    TestDir dir("compare_baseline_fail");
    const std::string baseline_path = dir.file("baseline.json");
    const std::string candidate_path = dir.file("candidate.json");

    write_text_file(baseline_path, benchmark_summary_json(1000.0, 2000.0, 100.0, 1000.0, 1200.0, 200.0, 500.0));
    write_text_file(candidate_path, benchmark_summary_json(700.0, 1500.0, 140.0, 1700.0, 2300.0, 400.0, 300.0));

    const int status = run_compare_benchmark_baseline_entrypoint(baseline_path, candidate_path);
    require(status == 2, "compare-baseline should reject throughput/latency regressions beyond thresholds");
}

void test_compare_microbench_passes_within_thresholds() {
    TestDir dir("compare_microbench_pass");
    const std::string baseline_path = dir.file("baseline.json");
    const std::string candidate_path = dir.file("candidate.json");

    write_text_file(
        baseline_path,
        microbench_results_json({
            {"wal_append", 200.0, 500, 1000},
            {"scan", 1000.0, 250, 2000},
            {"recovery", 120.0, 25, 0},
            {"compaction", 30.0, 5, 4000},
            {"rewrite", 25.0, 5, 3000},
        }));
    write_text_file(
        candidate_path,
        microbench_results_json({
            {"wal_append", 170.0, 500, 1000},
            {"scan", 900.0, 250, 2000},
            {"recovery", 100.0, 25, 0},
            {"compaction", 24.0, 5, 4000},
            {"rewrite", 20.0, 5, 3000},
        }));

    const int status = run_compare_microbench_entrypoint(baseline_path, candidate_path);
    require(status == 0, "compare-microbench should pass when case ratios remain within thresholds");
}

void test_compare_microbench_rejects_regression() {
    TestDir dir("compare_microbench_fail");
    const std::string baseline_path = dir.file("baseline.json");
    const std::string candidate_path = dir.file("candidate.json");

    write_text_file(
        baseline_path,
        microbench_results_json({
            {"wal_append", 200.0, 500, 1000},
            {"scan", 1000.0, 250, 2000},
            {"recovery", 120.0, 25, 0},
            {"compaction", 30.0, 5, 4000},
            {"rewrite", 25.0, 5, 3000},
        }));
    write_text_file(
        candidate_path,
        microbench_results_json({
            {"wal_append", 120.0, 500, 1000},
            {"scan", 700.0, 250, 2000},
            {"recovery", 80.0, 25, 0},
            {"compaction", 18.0, 5, 4000},
            {"rewrite", 15.0, 5, 3000},
        }));

    const int status = run_compare_microbench_entrypoint(baseline_path, candidate_path);
    require(status == 2, "compare-microbench should reject regressions beyond per-case thresholds");
}

void test_benchmark_trend_summarizes_history() {
    TestDir dir("benchmark_trend");
    const std::string baseline_dir = dir.file("baselines");
    std::filesystem::create_directories(baseline_dir);

    write_text_file(
        (std::filesystem::path(baseline_dir) / "20260101T000000.json").string(),
        benchmark_summary_json(1000.0, 2000.0, 100.0));
    write_text_file(
        (std::filesystem::path(baseline_dir) / "20260102T000000.json").string(),
        benchmark_summary_json(1100.0, 1900.0, 90.0));
    write_text_file(
        (std::filesystem::path(baseline_dir) / "20260103T000000.json").string(),
        benchmark_summary_json(1200.0, 2200.0, 80.0));

    const auto [status, values] = capture_benchmark_trend(baseline_dir, 2);
    require(status == 0, "benchmark trend should succeed for non-empty baseline directory");
    require(values.at("count") == "3", "benchmark trend should count all baseline files");
    require(values.at("oldest_file") == "20260101T000000.json", "benchmark trend should report the oldest file");
    require(values.at("latest_file") == "20260103T000000.json", "benchmark trend should report the latest file");
    require(values.at("recent_window_count") == "2", "benchmark trend should report the applied recent window size");
    require(std::stod(values.at("avg_write_ops_per_s")) > 1099.0 &&
                std::stod(values.at("avg_write_ops_per_s")) < 1101.0,
            "benchmark trend should report the average write throughput");
    require(std::stod(values.at("recent_avg_write_ops_per_s")) > 1149.0 &&
                std::stod(values.at("recent_avg_write_ops_per_s")) < 1151.0,
            "benchmark trend should report the recent-window average write throughput");
    require(std::stod(values.at("latest_vs_oldest_write_ratio_pct")) > 119.9 &&
                std::stod(values.at("latest_vs_oldest_write_ratio_pct")) < 120.1,
            "benchmark trend should report latest-vs-oldest write throughput ratio");
    require(std::stod(values.at("latest_vs_recent_avg_write_ratio_pct")) > 104.2 &&
                std::stod(values.at("latest_vs_recent_avg_write_ratio_pct")) < 104.4,
            "benchmark trend should report latest-vs-recent-average write throughput ratio");
    require(values.at("write_trend") == "improving",
            "benchmark trend should classify higher write throughput as improving");
    require(values.at("recent_write_trend") == "stable",
            "benchmark trend should classify small recent throughput differences as stable");
    require(values.at("read_trend") == "improving",
            "benchmark trend should classify higher read throughput as improving");
    require(values.at("recent_read_trend") == "improving",
            "benchmark trend should classify recent read throughput against the recent window");
    require(std::stod(values.at("latest_vs_oldest_latency_ratio_pct")) > 79.9 &&
                std::stod(values.at("latest_vs_oldest_latency_ratio_pct")) < 80.1,
            "benchmark trend should report latest-vs-oldest latency ratio");
    require(std::stod(values.at("latest_vs_recent_avg_latency_ratio_pct")) > 94.0 &&
                std::stod(values.at("latest_vs_recent_avg_latency_ratio_pct")) < 94.2,
            "benchmark trend should report latest-vs-recent-average latency ratio");
    require(values.at("latency_trend") == "improving",
            "benchmark trend should classify lower latency as improving");
    require(values.at("recent_latency_trend") == "improving",
            "benchmark trend should classify lower recent latency as improving");
}

void test_microbench_trend_summarizes_history() {
    TestDir dir("microbench_trend");
    const std::string baseline_dir = dir.file("baselines");
    std::filesystem::create_directories(baseline_dir);

    write_text_file(
        (std::filesystem::path(baseline_dir) / "microbench-20260101T000000.json").string(),
        microbench_results_json({
            {"wal_append", 100.0, 500, 1},
            {"compaction", 20.0, 5, 1},
        }));
    write_text_file(
        (std::filesystem::path(baseline_dir) / "microbench-20260102T000000.json").string(),
        microbench_results_json({
            {"wal_append", 110.0, 500, 1},
            {"compaction", 18.0, 5, 1},
        }));
    write_text_file(
        (std::filesystem::path(baseline_dir) / "microbench-20260103T000000.json").string(),
        microbench_results_json({
            {"wal_append", 120.0, 500, 1},
            {"compaction", 25.0, 5, 1},
        }));

    const auto [status, json] = capture_microbench_trend_json(baseline_dir, 2);
    require(status == 0, "microbench trend json should succeed for non-empty directories");
    require(json.find("\"name\":\"wal_append\"") != std::string::npos,
            "microbench trend json should include the wal_append case");
    require(json.find("\"name\":\"compaction\"") != std::string::npos,
            "microbench trend json should include the compaction case");
    require(json.find("\"trend\":\"improving\"") != std::string::npos,
            "microbench trend json should classify improving throughput");
}

}  // namespace

void register_benchmark_trend_tests(TestCases& tests) {
    tests.push_back({"benchmark trend json reports recent window", test_benchmark_trend_json_reports_recent_window});
    tests.push_back({"compare microbench passes within thresholds", test_compare_microbench_passes_within_thresholds});
    tests.push_back({"compare microbench rejects regression", test_compare_microbench_rejects_regression});
    tests.push_back({"compare benchmark baseline passes within thresholds",
                     test_compare_benchmark_baseline_passes_within_thresholds});
    tests.push_back({"compare benchmark baseline rejects regression",
                     test_compare_benchmark_baseline_rejects_regression});
    tests.push_back({"benchmark trend summarizes history", test_benchmark_trend_summarizes_history});
    tests.push_back({"microbench trend summarizes history", test_microbench_trend_summarizes_history});
}

}  // namespace kvstore::tests::integration
