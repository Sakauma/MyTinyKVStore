#include "tests/unit/test_registry.h"

int main() {
    kvstore::tests::unit::TestCases tests;
    kvstore::tests::unit::register_internal_helpers_tests(tests);
    kvstore::tests::unit::register_storage_format_tests(tests);
    kvstore::tests::unit::register_value_cache_tests(tests);
    return test_support::run_named_tests(tests);
}
