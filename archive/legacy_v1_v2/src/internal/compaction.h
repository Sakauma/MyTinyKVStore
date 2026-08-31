#ifndef KVSTORE_INTERNAL_COMPACTION_H
#define KVSTORE_INTERNAL_COMPACTION_H

#include "kvstore.h"

#include <cstdint>
#include <map>
#include <string>

namespace kvstore::internal {

uint64_t rewrite_snapshot_and_reset_wal(
    const std::string& db_file_path,
    const std::string& wal_file_path,
    const std::map<std::string, Value>& snapshot_state);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_COMPACTION_H
