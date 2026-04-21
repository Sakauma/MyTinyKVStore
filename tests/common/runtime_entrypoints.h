#ifndef KVSTORE_TESTS_COMMON_RUNTIME_ENTRYPOINTS_H
#define KVSTORE_TESTS_COMMON_RUNTIME_ENTRYPOINTS_H

#include <cstddef>
#include <cstdint>
#include <string>

struct SoakProfileSummary {
    int writer_count = 0;
    int reader_count = 0;
    int key_space = 0;
    int compaction_interval_ms = 0;
    size_t max_batch_size = 0;
    uint32_t max_batch_delay_us = 0;
};

struct ConcurrencyStressProfileSummary {
    int writer_count = 0;
    int reader_count = 0;
    int compactor_count = 0;
    int keys_per_writer = 0;
    int compaction_interval_ms = 0;
    int batch_width = 0;
    int recovery_reopen_cycles = 0;
    size_t max_batch_size = 0;
    uint32_t max_batch_delay_us = 0;
    uint64_t auto_compact_wal_bytes_threshold = 0;
};

void set_runtime_program_path_entrypoint(const std::string& program_path);
int run_fault_injection_scenario_entrypoint(const std::string& scenario, const std::string& db_path);
int run_failpoint_child_entrypoint(const std::string& scenario, const std::string& db_path);
int run_soak_command_entrypoint(int duration_seconds, const std::string& profile_name);
int run_concurrency_stress_command_entrypoint(int duration_seconds, const std::string& profile_name);
int run_concurrency_stress_json_entrypoint(int duration_seconds, const std::string& profile_name);
SoakProfileSummary soak_profile_summary_entrypoint(const std::string& profile_name);
ConcurrencyStressProfileSummary concurrency_stress_profile_summary_entrypoint(const std::string& profile_name);

#endif  // KVSTORE_TESTS_COMMON_RUNTIME_ENTRYPOINTS_H
