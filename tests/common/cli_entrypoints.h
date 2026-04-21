#ifndef KVSTORE_TESTS_COMMON_CLI_ENTRYPOINTS_H
#define KVSTORE_TESTS_COMMON_CLI_ENTRYPOINTS_H

#include <string>

int run_inspect_format(const std::string& db_path);
int run_rewrite_format(const std::string& db_path);
int run_verify_format(const std::string& db_path);
int run_compatibility_matrix();

#endif  // KVSTORE_TESTS_COMMON_CLI_ENTRYPOINTS_H
