#ifndef KVSTORE_INTEGRATION_TEST_REGISTRY_H
#define KVSTORE_INTEGRATION_TEST_REGISTRY_H

#include "tests/common/test_support.h"

#include <vector>

namespace kvstore::tests::integration {

using TestCases = std::vector<test_support::NamedTest>;

void register_basic_kv_tests(TestCases& tests);
void register_benchmark_trend_tests(TestCases& tests);
void register_json_cli_tests(TestCases& tests);
void register_metrics_controller_tests(TestCases& tests);
void register_recovery_format_tests(TestCases& tests);

}  // namespace kvstore::tests::integration

#endif  // KVSTORE_INTEGRATION_TEST_REGISTRY_H
