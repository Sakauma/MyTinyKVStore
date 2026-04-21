#include "tests/integration/test_registry.h"

#include "tests/common/benchmark_entrypoints.h"
#include "tests/common/test_support.h"

#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

namespace kvstore::tests::integration {
namespace {

using test_support::require;

std::pair<int, std::string> capture_stdout(const std::function<int()>& fn) {
    std::ostringstream out;
    auto* original = std::cout.rdbuf(out.rdbuf());
    const int status = fn();
    std::cout.rdbuf(original);
    return {status, out.str()};
}

void test_benchmark_result_json_reports_summary_and_metrics() {
    const std::string json = benchmark_result_json_fixture_entrypoint();
    require(json.find("\"label\":\"default-baseline\"") != std::string::npos,
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
    const std::string json = microbench_results_json_fixture_entrypoint();
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
    const std::string json = stress_summary_json_fixture_entrypoint();
    require(json.find("\"profile\":\"balanced\"") != std::string::npos,
            "stress summary json should include the profile name");
    require(json.find("\"committed_write_requests\":") != std::string::npos,
            "stress summary json should include committed write counts");
    require(json.find("\"max_pending_queue_depth\":") != std::string::npos,
            "stress summary json should include queue metrics");
    require(json.find("\"recovery_reopen_cycles\":") != std::string::npos,
            "stress summary json should include recovery reopen counts");
}

void test_options_to_json_reports_profile_fields() {
    const auto [status, json] = capture_stdout([]() {
        return run_profile_json_entrypoint("balanced");
    });
    require(status == 0, "profile-json entrypoint should succeed");
    require(!json.empty() && json.front() == '{',
            "options json should be a JSON object");
    require(json.find("\"max_batch_size\":") != std::string::npos,
            "options json should include batching fields");
    require(json.find("\"adaptive_objective_enabled\":true") != std::string::npos,
            "options json should include boolean profile settings");
    require(json.find("\"auto_compact_wal_bytes_threshold\":") != std::string::npos,
            "options json should include compaction thresholds");
}

void test_metrics_to_json_reports_core_fields() {
    const auto [status, json] = capture_stdout([]() {
        return run_benchmark_json_entrypoint();
    });
    require(status == 0, "bench-json entrypoint should succeed");
    require(!json.empty() && json.front() == '{',
            "metrics json should be a JSON object");
    require(json.find("\"committed_write_requests\":") != std::string::npos,
            "metrics json should include committed write counts");
    require(json.find("\"write_latency_histogram\":[") != std::string::npos,
            "metrics json should include the latency histogram array");
    require(json.find("\"wal_fsync_calls\":") != std::string::npos,
            "metrics json should include fsync metrics");
}

}  // namespace

void register_json_cli_tests(TestCases& tests) {
    tests.push_back({"benchmark result json reports summary and metrics",
                     test_benchmark_result_json_reports_summary_and_metrics});
    tests.push_back({"microbench json reports cases", test_microbench_json_reports_cases});
    tests.push_back({"stress summary json reports profile", test_stress_summary_json_reports_profile});
    tests.push_back({"options to json reports profile fields", test_options_to_json_reports_profile_fields});
    tests.push_back({"metrics to json reports core fields", test_metrics_to_json_reports_core_fields});
}

}  // namespace kvstore::tests::integration
