#ifndef KVSTORE_INTERNAL_WRITER_POLICY_H
#define KVSTORE_INTERNAL_WRITER_POLICY_H

#include "kvstore.h"

#include <cstdint>

namespace kvstore::internal {

struct BatchPolicy {
    size_t max_batch_size;
    uint64_t max_batch_wal_bytes;
    uint32_t batch_delay_us;
    uint64_t observed_queue_depth;
    uint64_t objective_pressure_score;
    uint64_t objective_cost_score;
    uint64_t objective_throughput_score;
    int64_t objective_balance_score;
    int64_t objective_mode;
    bool adaptive_batching;
    bool adaptive_flush;
    bool latency_target_adjusted;
    bool fsync_pressure_adjusted;
    bool read_heavy_adjusted;
    bool compaction_pressure_adjusted;
    bool wal_growth_adjusted;
    bool objective_short_delay_adjusted;
    bool objective_long_delay_adjusted;
};

struct WriterPolicySignals {
    uint64_t request_queue_depth;
    uint64_t recent_peak_queue_depth;
    uint64_t recent_read_ratio;
    uint64_t current_obsolete_ratio;
    uint64_t recent_avg_batch_size;
    uint64_t recent_avg_batch_wal_bytes;
    uint64_t observed_p95_us;
    uint64_t observed_fsync_pressure;
};

BatchPolicy compute_batch_policy(const KVStoreOptions& options, const WriterPolicySignals& signals);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_WRITER_POLICY_H
