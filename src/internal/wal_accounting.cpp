#include "wal_accounting.h"

namespace kvstore::internal {

void track_latest_wal_record(
    std::map<std::string, uint64_t>& latest_wal_record_bytes,
    std::atomic<uint64_t>& live_wal_bytes_since_compaction,
    const std::string& key,
    uint64_t record_size) {
    const auto it = latest_wal_record_bytes.find(key);
    if (it != latest_wal_record_bytes.end()) {
        live_wal_bytes_since_compaction.fetch_sub(it->second, std::memory_order_relaxed);
        it->second = record_size;
    } else {
        latest_wal_record_bytes[key] = record_size;
    }
    live_wal_bytes_since_compaction.fetch_add(record_size, std::memory_order_relaxed);
}

uint64_t obsolete_wal_ratio_percent(uint64_t total_wal_bytes, uint64_t live_wal_bytes) {
    if (total_wal_bytes == 0) {
        return 0;
    }
    const uint64_t obsolete_wal_bytes =
        total_wal_bytes >= live_wal_bytes ? total_wal_bytes - live_wal_bytes : 0;
    return (obsolete_wal_bytes * 100) / total_wal_bytes;
}

bool should_auto_compact(const KVStoreOptions& options, uint64_t total_wal_bytes, uint64_t live_wal_bytes) {
    const uint64_t obsolete_wal_bytes =
        total_wal_bytes >= live_wal_bytes ? total_wal_bytes - live_wal_bytes : 0;

    if (options.auto_compact_wal_bytes_threshold > 0 &&
        total_wal_bytes >= options.auto_compact_wal_bytes_threshold) {
        return true;
    }

    if (options.auto_compact_invalid_wal_ratio_percent == 0 || total_wal_bytes == 0) {
        return false;
    }

    return obsolete_wal_bytes * 100 >=
           static_cast<uint64_t>(options.auto_compact_invalid_wal_ratio_percent) * total_wal_bytes;
}

}  // namespace kvstore::internal
