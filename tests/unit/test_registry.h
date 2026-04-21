#ifndef KVSTORE_UNIT_TEST_REGISTRY_H
#define KVSTORE_UNIT_TEST_REGISTRY_H

#include "tests/common/test_support.h"

#include <vector>

namespace kvstore::tests::unit {

using TestCases = std::vector<test_support::NamedTest>;

void register_request_runtime_tests(TestCases& tests);
void register_internal_helpers_tests(TestCases& tests);
void register_writer_wait_tests(TestCases& tests);
void register_writer_execution_tests(TestCases& tests);
void register_wal_accounting_tests(TestCases& tests);

}  // namespace kvstore::tests::unit

#endif  // KVSTORE_UNIT_TEST_REGISTRY_H
