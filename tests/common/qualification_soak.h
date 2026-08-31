#ifndef KVSTORE_TESTS_COMMON_QUALIFICATION_SOAK_H
#define KVSTORE_TESTS_COMMON_QUALIFICATION_SOAK_H

#include <cstddef>
#include <cstdint>
#include <string>

int run_qualification_soak_json_entrypoint(const std::string& db_path,
                                           uint64_t minimum_duration_seconds,
                                           uint64_t required_unique_keys,
                                           size_t writer_count,
                                           size_t value_bytes);

#endif  // KVSTORE_TESTS_COMMON_QUALIFICATION_SOAK_H
