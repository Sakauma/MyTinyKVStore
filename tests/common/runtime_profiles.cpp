#include "tests/common/runtime_entrypoints.h"

#include "tests/common/benchmark_entrypoints.h"
#include "tests/common/runtime_shared.h"
#include "tests/common/test_support.h"

#include <iostream>
#include <optional>
#include <sstream>
#include <string>

namespace {

using test_support::require;

}  // namespace

std::string stress_summary_to_json(const StressSummary& summary) {
    std::ostringstream out;
    out << '{'
        << "\"profile\":\"" << summary.profile << "\""
        << ",\"duration_seconds\":" << summary.duration_seconds
        << ",\"writer_count\":" << summary.writer_count
        << ",\"reader_count\":" << summary.reader_count
        << ",\"compactor_count\":" << summary.compactor_count
        << ",\"recovery_reopen_cycles\":" << summary.recovery_reopen_cycles
        << ",\"committed_write_requests\":" << summary.committed_write_requests
        << ",\"max_pending_queue_depth\":" << summary.max_pending_queue_depth
        << ",\"manual_compactions_completed\":" << summary.manual_compactions_completed
        << ",\"auto_compactions_completed\":" << summary.auto_compactions_completed
        << ",\"observed_fsync_pressure_per_1000_writes\":" << summary.observed_fsync_pressure_per_1000_writes
        << ",\"last_effective_batch_delay_us\":" << summary.last_effective_batch_delay_us
        << '}';
    return out.str();
}

std::optional<KVStoreProfile> parse_profile_name(const std::string& name) {
    if (name == "balanced") {
        return KVStoreProfile::kBalanced;
    }
    if (name == "write-heavy") {
        return KVStoreProfile::kWriteHeavy;
    }
    if (name == "read-heavy") {
        return KVStoreProfile::kReadHeavy;
    }
    if (name == "low-latency") {
        return KVStoreProfile::kLowLatency;
    }
    return std::nullopt;
}

std::optional<SoakProfile> parse_soak_profile_name(const std::string& name) {
    if (name == "balanced") {
        return SoakProfile::kBalanced;
    }
    if (name == "write-heavy") {
        return SoakProfile::kWriteHeavy;
    }
    if (name == "read-heavy") {
        return SoakProfile::kReadHeavy;
    }
    return std::nullopt;
}

std::optional<ConcurrencyStressProfile> parse_concurrency_stress_profile_name(const std::string& name) {
    if (name == "balanced") {
        return ConcurrencyStressProfile::kBalanced;
    }
    if (name == "write-heavy") {
        return ConcurrencyStressProfile::kWriteHeavy;
    }
    if (name == "compaction-heavy") {
        return ConcurrencyStressProfile::kCompactionHeavy;
    }
    if (name == "recovery-heavy") {
        return ConcurrencyStressProfile::kRecoveryHeavy;
    }
    return std::nullopt;
}

std::string concurrency_stress_profile_name(ConcurrencyStressProfile profile) {
    switch (profile) {
        case ConcurrencyStressProfile::kBalanced:
            return "balanced";
        case ConcurrencyStressProfile::kWriteHeavy:
            return "write-heavy";
        case ConcurrencyStressProfile::kCompactionHeavy:
            return "compaction-heavy";
        case ConcurrencyStressProfile::kRecoveryHeavy:
            return "recovery-heavy";
    }
    return "unknown";
}

SoakProfileConfig make_soak_profile_config(SoakProfile profile) {
    SoakProfileConfig config;
    config.options.max_batch_size = 32;
    config.options.max_batch_wal_bytes = 1 << 20;
    config.options.max_batch_delay_us = 2000;
    config.options.auto_compact_wal_bytes_threshold = 4096;
    config.options.auto_compact_invalid_wal_ratio_percent = 60;
    config.options.adaptive_flush_enabled = true;
    config.options.adaptive_flush_queue_depth_threshold = 8;
    config.options.adaptive_flush_delay_divisor = 4;
    config.options.adaptive_flush_min_batch_delay_us = 100;

    switch (profile) {
        case SoakProfile::kBalanced:
            return config;
        case SoakProfile::kWriteHeavy:
            config.writer_count = 6;
            config.reader_count = 1;
            config.key_space = 512;
            config.compaction_interval_ms = 80;
            config.options.max_batch_size = 64;
            config.options.max_batch_delay_us = 4000;
            config.options.auto_compact_wal_bytes_threshold = 8192;
            config.options.auto_compact_invalid_wal_ratio_percent = 70;
            return config;
        case SoakProfile::kReadHeavy:
            config.writer_count = 2;
            config.reader_count = 6;
            config.key_space = 256;
            config.compaction_interval_ms = 30;
            config.options.max_batch_size = 16;
            config.options.max_batch_delay_us = 1000;
            config.options.adaptive_flush_queue_depth_threshold = 4;
            config.options.adaptive_flush_min_batch_delay_us = 50;
            return config;
    }
    return config;
}

ConcurrencyStressProfileConfig make_concurrency_stress_profile_config(ConcurrencyStressProfile profile) {
    ConcurrencyStressProfileConfig config;
    config.options.max_batch_size = 32;
    config.options.max_batch_wal_bytes = 1 << 16;
    config.options.max_batch_delay_us = 1500;
    config.options.adaptive_batching_enabled = true;
    config.options.adaptive_queue_depth_threshold = 8;
    config.options.adaptive_batch_size_multiplier = 4;
    config.options.adaptive_batch_wal_bytes_multiplier = 4;
    config.options.adaptive_flush_enabled = true;
    config.options.adaptive_flush_queue_depth_threshold = 4;
    config.options.adaptive_flush_delay_divisor = 4;
    config.options.adaptive_flush_min_batch_delay_us = 50;
    config.options.auto_compact_wal_bytes_threshold = 4096;
    config.options.auto_compact_invalid_wal_ratio_percent = 60;
    config.options.adaptive_recent_window_batches = 32;
    config.options.adaptive_recent_write_sample_limit = 256;

    switch (profile) {
        case ConcurrencyStressProfile::kBalanced:
            return config;
        case ConcurrencyStressProfile::kWriteHeavy:
            config.writer_count = 12;
            config.reader_count = 2;
            config.keys_per_writer = 128;
            config.compaction_interval_ms = 8;
            config.options.max_batch_size = 64;
            config.options.max_batch_delay_us = 2500;
            config.options.auto_compact_wal_bytes_threshold = 8192;
            return config;
        case ConcurrencyStressProfile::kCompactionHeavy:
            config.writer_count = 6;
            config.reader_count = 3;
            config.compactor_count = 2;
            config.keys_per_writer = 80;
            config.compaction_interval_ms = 1;
            config.options.max_batch_size = 24;
            config.options.max_batch_delay_us = 800;
            config.options.auto_compact_wal_bytes_threshold = 2048;
            config.options.auto_compact_invalid_wal_ratio_percent = 45;
            return config;
        case ConcurrencyStressProfile::kRecoveryHeavy:
            config.writer_count = 4;
            config.reader_count = 1;
            config.compactor_count = 1;
            config.keys_per_writer = 64;
            config.compaction_interval_ms = 2;
            config.batch_width = 3;
            config.recovery_reopen_cycles = 6;
            config.options.max_batch_size = 16;
            config.options.max_batch_delay_us = 700;
            config.options.auto_compact_wal_bytes_threshold = 2048;
            config.options.auto_compact_invalid_wal_ratio_percent = 40;
            return config;
    }
    return config;
}

int run_profile_json_entrypoint(const std::string& name) {
    const auto profile = parse_profile_name(name);
    if (!profile.has_value()) {
        std::cerr << "Unknown profile: " << name << '\n';
        return 1;
    }
    std::cout << OptionsToJson(RecommendedOptions(*profile)) << std::endl;
    return 0;
}

SoakProfileSummary soak_profile_summary_entrypoint(const std::string& profile_name) {
    const auto profile = parse_soak_profile_name(profile_name);
    require(profile.has_value(), "unknown soak profile");
    const SoakProfileConfig config = make_soak_profile_config(*profile);
    SoakProfileSummary summary;
    summary.writer_count = config.writer_count;
    summary.reader_count = config.reader_count;
    summary.key_space = config.key_space;
    summary.compaction_interval_ms = config.compaction_interval_ms;
    summary.max_batch_size = config.options.max_batch_size;
    summary.max_batch_delay_us = config.options.max_batch_delay_us;
    return summary;
}

ConcurrencyStressProfileSummary concurrency_stress_profile_summary_entrypoint(const std::string& profile_name) {
    const auto profile = parse_concurrency_stress_profile_name(profile_name);
    require(profile.has_value(), "unknown concurrency stress profile");
    const ConcurrencyStressProfileConfig config = make_concurrency_stress_profile_config(*profile);
    ConcurrencyStressProfileSummary summary;
    summary.writer_count = config.writer_count;
    summary.reader_count = config.reader_count;
    summary.compactor_count = config.compactor_count;
    summary.keys_per_writer = config.keys_per_writer;
    summary.compaction_interval_ms = config.compaction_interval_ms;
    summary.batch_width = config.batch_width;
    summary.recovery_reopen_cycles = config.recovery_reopen_cycles;
    summary.max_batch_size = config.options.max_batch_size;
    summary.max_batch_delay_us = config.options.max_batch_delay_us;
    summary.auto_compact_wal_bytes_threshold = config.options.auto_compact_wal_bytes_threshold;
    return summary;
}
