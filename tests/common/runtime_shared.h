#ifndef KVSTORE_TESTS_COMMON_RUNTIME_SHARED_H
#define KVSTORE_TESTS_COMMON_RUNTIME_SHARED_H

#include "kvstore.h"

#include <cstdint>
#include <optional>
#include <string>

struct StressSummary {
    std::string profile;
    int duration_seconds = 0;
    int writer_count = 0;
    int reader_count = 0;
    int compactor_count = 0;
    int recovery_reopen_cycles = 0;
    uint64_t committed_write_requests = 0;
    uint64_t put_operations = 0;
    uint64_t delete_operations = 0;
    uint64_t batch_requests = 0;
    uint64_t batch_put_operations = 0;
    uint64_t batch_delete_operations = 0;
    uint64_t final_live_objects = 0;
    uint64_t max_pending_queue_depth = 0;
    uint64_t manual_compactions_completed = 0;
    uint64_t auto_compactions_completed = 0;
    uint64_t observed_fsync_pressure_per_1000_writes = 0;
    uint64_t last_effective_batch_delay_us = 0;
};

enum class SoakProfile {
    kBalanced,
    kWriteHeavy,
    kReadHeavy,
};

enum class ConcurrencyStressProfile {
    kBalanced,
    kWriteHeavy,
    kCompactionHeavy,
    kRecoveryHeavy,
};

struct SoakProfileConfig {
    KVStoreOptions options;
    int writer_count = 4;
    int reader_count = 2;
    int key_space = 256;
    int compaction_interval_ms = 50;
};

struct ConcurrencyStressProfileConfig {
    KVStoreOptions options;
    int writer_count = 8;
    int reader_count = 4;
    int metrics_reader_count = 1;
    int compactor_count = 1;
    int keys_per_writer = 96;
    int compaction_interval_ms = 5;
    int batch_width = 4;
    int recovery_reopen_cycles = 1;
};

std::string stress_summary_to_json(const StressSummary& summary);

std::optional<KVStoreProfile> parse_profile_name(const std::string& name);
std::optional<SoakProfile> parse_soak_profile_name(const std::string& name);
std::optional<ConcurrencyStressProfile> parse_concurrency_stress_profile_name(const std::string& name);
std::string concurrency_stress_profile_name(ConcurrencyStressProfile profile);

SoakProfileConfig make_soak_profile_config(SoakProfile profile);
ConcurrencyStressProfileConfig make_concurrency_stress_profile_config(ConcurrencyStressProfile profile);

#endif  // KVSTORE_TESTS_COMMON_RUNTIME_SHARED_H
