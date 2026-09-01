#ifndef KVSTORE_UNIT_TEST_REGISTRY_H
#define KVSTORE_UNIT_TEST_REGISTRY_H

#include "tests/common/test_support.h"

#include <vector>

namespace kvstore::tests::unit {

using TestCases = std::vector<test_support::NamedTest>;

void register_internal_helpers_tests(TestCases& tests);
void register_storage_format_tests(TestCases& tests);
void register_value_cache_tests(TestCases& tests);

}  // namespace kvstore::tests::unit

#endif  // KVSTORE_UNIT_TEST_REGISTRY_H
