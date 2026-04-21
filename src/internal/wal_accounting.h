#ifndef KVSTORE_INTERNAL_WAL_ACCOUNTING_H
#define KVSTORE_INTERNAL_WAL_ACCOUNTING_H

#include "kvstore.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <string>

namespace kvstore::internal {

void track_latest_wal_record(
    std::map<std::string, uint64_t>& latest_wal_record_bytes,
    std::atomic<uint64_t>& live_wal_bytes_since_compaction,
    const std::string& key,
    uint64_t record_size);

uint64_t obsolete_wal_ratio_percent(uint64_t total_wal_bytes, uint64_t live_wal_bytes);
bool should_auto_compact(const KVStoreOptions& options, uint64_t total_wal_bytes, uint64_t live_wal_bytes);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_WAL_ACCOUNTING_H
