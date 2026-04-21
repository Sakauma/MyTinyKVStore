#include "tests/unit/test_registry.h"

int main() {
    kvstore::tests::unit::TestCases tests;
    kvstore::tests::unit::register_request_runtime_tests(tests);
    kvstore::tests::unit::register_writer_wait_tests(tests);
    kvstore::tests::unit::register_writer_execution_tests(tests);
    kvstore::tests::unit::register_wal_accounting_tests(tests);
    return test_support::run_named_tests(tests);
}
