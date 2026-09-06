#include "tests/integration/test_registry.h"

namespace kvstore::tests::integration {

void register_all_integration_tests(TestCases& tests) {
    register_basic_kv_tests(tests);
    register_benchmark_trend_tests(tests);
    register_durability_smoke_tests(tests);
    register_json_cli_tests(tests);
    register_metrics_controller_tests(tests);
    register_recovery_format_tests(tests);
    register_runtime_concurrency_tests(tests);
    register_transaction_tests(tests);
}

std::vector<std::string> integration_test_groups() {
    return {
        "basic",
        "benchmark-trend",
        "durability",
        "json-cli",
        "metrics-controller",
        "recovery-format",
        "runtime-concurrency",
        "transaction",
    };
}

bool register_integration_test_group(const std::string& group, TestCases& tests) {
    if (group == "basic") {
        register_basic_kv_tests(tests);
    } else if (group == "benchmark-trend") {
        register_benchmark_trend_tests(tests);
    } else if (group == "durability") {
        register_durability_smoke_tests(tests);
    } else if (group == "json-cli") {
        register_json_cli_tests(tests);
    } else if (group == "metrics-controller") {
        register_metrics_controller_tests(tests);
    } else if (group == "recovery-format") {
        register_recovery_format_tests(tests);
    } else if (group == "runtime-concurrency") {
        register_runtime_concurrency_tests(tests);
    } else if (group == "transaction") {
        register_transaction_tests(tests);
    } else {
        return false;
    }
    return true;
}

}  // namespace kvstore::tests::integration
