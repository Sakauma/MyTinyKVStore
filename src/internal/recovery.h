#ifndef KVSTORE_INTERNAL_RECOVERY_H
#define KVSTORE_INTERNAL_RECOVERY_H

#include "kvstore.h"

#include <functional>
#include <map>
#include <string>

namespace kvstore::internal {

using StateMap = std::map<std::string, Value>;
using WalReplayCallback = std::function<void(const std::string&, uint64_t)>;

void ensure_snapshot_file_exists(const std::string& db_file_path);
void load_snapshot_into_state(const std::string& db_file_path, StateMap& state);
void replay_wal_into_state(
    const std::string& wal_file_path,
    StateMap& state,
    const WalReplayCallback& note_latest_wal_record);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_RECOVERY_H
