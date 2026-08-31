#ifndef KVSTORE_INTERNAL_STORAGE_ENGINE_H
#define KVSTORE_INTERNAL_STORAGE_ENGINE_H

#include "kvstore.h"
#include "storage_format.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace kvstore::internal {

class StorageEngine {
public:
    struct VersionedRead {
        std::optional<Value> value;
        size_t shard_id = 0;
        uint64_t shard_version = 0;
    };

    StorageEngine(std::string db_path, KVStoreOptions options);
    ~StorageEngine();

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;

    void Put(std::string key, Value value);
    void Delete(std::string key);
    void WriteBatch(std::vector<Mutation> operations);
    std::optional<Value> Get(const std::string& key);
    VersionedRead GetVersioned(const std::string& key);
    std::vector<std::pair<std::string, Value>> Scan(const std::string& start_key,
                                                    const std::string& end_key);

    void CommitTransaction(std::vector<Mutation> operations,
                           std::map<size_t, uint64_t> expected_versions);
    void NoteTransactionRollback() noexcept;
    void Flush();
    void Compact();
    KVStoreMetrics GetMetrics() const;

    size_t ShardForKey(const std::string& key) const;

private:
    class Impl;
    std::unique_ptr<Impl> pimpl_;
};

KVStoreOptions sanitize_options(KVStoreOptions options);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_STORAGE_ENGINE_H
