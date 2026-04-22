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
}

}  // namespace kvstore::tests::integration
