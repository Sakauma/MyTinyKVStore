#include "tests/unit/test_registry.h"

#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    kvstore::tests::unit::TestCases tests;
    kvstore::tests::unit::register_internal_helpers_tests(tests);
    kvstore::tests::unit::register_storage_format_tests(tests);
    kvstore::tests::unit::register_value_cache_tests(tests);

    if (argc == 1) {
        return test_support::run_named_tests(tests, {}, "unit");
    }
    const std::string command = argv[1];
    if (command == "--list" && argc == 2) {
        return test_support::list_named_tests(tests);
    }
    if (command == "--filter" && argc == 3 && argv[2][0] != '\0') {
        return test_support::run_named_tests(tests, argv[2], "unit");
    }

    std::cerr << "Usage: kv_unit_test [--list|--filter <substring>]" << std::endl;
    return 2;
}
