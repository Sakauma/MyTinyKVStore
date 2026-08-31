#ifndef KVSTORE_TESTS_COMMON_QUALIFICATION_BENCHMARK_H
#define KVSTORE_TESTS_COMMON_QUALIFICATION_BENCHMARK_H

#include <cstddef>
#include <cstdint>
#include <string>

int run_qualification_benchmark_json_entrypoint(uint64_t prefill_keys,
                                                uint64_t operations,
                                                size_t writer_count,
                                                size_t value_bytes,
                                                size_t rounds,
                                                const std::string& distribution,
                                                bool compaction_enabled);

#endif  // KVSTORE_TESTS_COMMON_QUALIFICATION_BENCHMARK_H
