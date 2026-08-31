#include "kvstore.h"

#include "internal/key_codec.h"
#include "internal/metrics_helpers.h"
#include "internal/observability.h"
#include "internal/storage_engine.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using kvstore::internal::StorageEngine;
using kvstore::internal::Mutation;
using kvstore::internal::MutationType;
using kvstore::internal::encode_binary_key;
using kvstore::internal::encode_int_key;
using kvstore::internal::encode_string_key;

std::string encode_batch_key(const BatchWriteOperation& operation) {
    switch (operation.key_kind) {
        case BatchWriteOperation::KeyKind::kString:
            return encode_string_key(operation.key);
        case BatchWriteOperation::KeyKind::kInt:
            try {
                size_t parsed = 0;
                const int key = std::stoi(operation.key, &parsed);
                if (parsed != operation.key.size()) {
                    throw KVStoreError("BatchWriteOperation integer key contains trailing characters");
                }
                return encode_int_key(key);
            } catch (const std::exception&) {
                throw KVStoreError("BatchWriteOperation integer key is invalid: " + operation.key);
            }
        case BatchWriteOperation::KeyKind::kBinary:
            return encode_binary_key(operation.binary_key);
    }
    throw KVStoreError("BatchWriteOperation contains an unknown key kind");
}

}  // namespace

class KVStore::Impl {
public:
    Impl(std::string db_path, KVStoreOptions options)
        : engine(std::make_shared<StorageEngine>(std::move(db_path), options)) {}

    std::shared_ptr<StorageEngine> engine;
};

class KVTransaction::Impl {
public:
    explicit Impl(std::shared_ptr<StorageEngine> engine_value)
        : engine(std::move(engine_value)) {}

    ~Impl() {
        rollback();
    }

    std::shared_ptr<StorageEngine> engine;
    std::vector<Mutation> operations;
    std::map<size_t, uint64_t> expected_versions;
    bool active = true;

    void require_active() const {
        if (!active || !engine) {
            throw KVStoreError("Transaction is no longer active");
        }
    }

    void observe_shard(const std::string& key) {
        const size_t shard_id = engine->ShardForKey(key);
        if (expected_versions.find(shard_id) != expected_versions.end()) {
            return;
        }
        const auto read = engine->GetVersioned(key);
        expected_versions.emplace(read.shard_id, read.shard_version);
    }

    std::optional<Value> get(const std::string& key) {
        require_active();
        for (auto iterator = operations.rbegin(); iterator != operations.rend(); ++iterator) {
            if (iterator->key != key) {
                continue;
            }
            if (iterator->type == MutationType::kDelete) {
                return std::nullopt;
            }
            return iterator->value;
        }
        const auto read = engine->GetVersioned(key);
        const auto [found, inserted] = expected_versions.emplace(read.shard_id, read.shard_version);
        if (!inserted && found->second != read.shard_version) {
            throw KVStoreConflictError("Transaction observed a shard version change while reading");
        }
        return read.value;
    }

    void put(std::string key, Value value) {
        require_active();
        observe_shard(key);
        operations.push_back(Mutation {MutationType::kPut, std::move(key), std::move(value)});
    }

    void erase(std::string key) {
        require_active();
        observe_shard(key);
        operations.push_back(Mutation {MutationType::kDelete, std::move(key), Value {}});
    }

    void commit() {
        require_active();
        active = false;
        engine->CommitTransaction(std::move(operations), std::move(expected_versions));
    }

    void rollback() noexcept {
        if (!active || !engine) {
            return;
        }
        active = false;
        operations.clear();
        expected_versions.clear();
        engine->NoteTransactionRollback();
    }
};

KVStore::KVStore(const std::string& db_path)
    : pimpl_(std::make_unique<Impl>(db_path, KVStoreOptions {})) {}

KVStore::KVStore(const std::string& db_path, KVStoreOptions options)
    : pimpl_(std::make_unique<Impl>(db_path, options)) {}

KVStore::~KVStore() = default;

BatchWriteOperation BatchWriteOperation::Put(std::string key, Value value) {
    BatchWriteOperation operation;
    operation.type = Type::kPut;
    operation.key_kind = KeyKind::kString;
    operation.key = std::move(key);
    operation.value = std::move(value);
    return operation;
}

BatchWriteOperation BatchWriteOperation::Delete(std::string key) {
    BatchWriteOperation operation;
    operation.type = Type::kDelete;
    operation.key_kind = KeyKind::kString;
    operation.key = std::move(key);
    return operation;
}

BatchWriteOperation BatchWriteOperation::PutInt(int key, Value value) {
    BatchWriteOperation operation;
    operation.type = Type::kPut;
    operation.key_kind = KeyKind::kInt;
    operation.key = std::to_string(key);
    operation.value = std::move(value);
    return operation;
}

BatchWriteOperation BatchWriteOperation::DeleteInt(int key) {
    BatchWriteOperation operation;
    operation.type = Type::kDelete;
    operation.key_kind = KeyKind::kInt;
    operation.key = std::to_string(key);
    return operation;
}

BatchWriteOperation BatchWriteOperation::PutBinary(std::vector<uint8_t> key, Value value) {
    BatchWriteOperation operation;
    operation.type = Type::kPut;
    operation.key_kind = KeyKind::kBinary;
    operation.binary_key = std::move(key);
    operation.value = std::move(value);
    return operation;
}

BatchWriteOperation BatchWriteOperation::DeleteBinary(std::vector<uint8_t> key) {
    BatchWriteOperation operation;
    operation.type = Type::kDelete;
    operation.key_kind = KeyKind::kBinary;
    operation.binary_key = std::move(key);
    return operation;
}

void KVStore::Put(int key, Value value) {
    pimpl_->engine->Put(encode_int_key(key), std::move(value));
}

void KVStore::Put(const std::string& key, Value value) {
    pimpl_->engine->Put(encode_string_key(key), std::move(value));
}

void KVStore::Put(const std::vector<uint8_t>& key, Value value) {
    pimpl_->engine->Put(encode_binary_key(key), std::move(value));
}

void KVStore::WriteBatch(const std::vector<BatchWriteOperation>& operations) {
    if (operations.empty()) {
        return;
    }
    std::vector<Mutation> encoded;
    encoded.reserve(operations.size());
    for (const auto& operation : operations) {
        MutationType mutation_type;
        switch (operation.type) {
            case BatchWriteOperation::Type::kPut:
                mutation_type = MutationType::kPut;
                break;
            case BatchWriteOperation::Type::kDelete:
                if (!operation.value.bytes.empty()) {
                    throw KVStoreError("BatchWriteOperation delete must not contain a value");
                }
                mutation_type = MutationType::kDelete;
                break;
            default:
                throw KVStoreError("BatchWriteOperation contains an unknown operation type");
        }
        encoded.push_back(Mutation {
            mutation_type,
            encode_batch_key(operation),
            mutation_type == MutationType::kPut ? operation.value : Value {},
        });
    }
    pimpl_->engine->WriteBatch(std::move(encoded));
}

std::optional<Value> KVStore::Get(int key) {
    return pimpl_->engine->Get(encode_int_key(key));
}

std::optional<Value> KVStore::Get(const std::string& key) {
    return pimpl_->engine->Get(encode_string_key(key));
}

std::optional<Value> KVStore::Get(const std::vector<uint8_t>& key) {
    return pimpl_->engine->Get(encode_binary_key(key));
}

void KVStore::Delete(int key) {
    pimpl_->engine->Delete(encode_int_key(key));
}

void KVStore::Delete(const std::string& key) {
    pimpl_->engine->Delete(encode_string_key(key));
}

void KVStore::Delete(const std::vector<uint8_t>& key) {
    pimpl_->engine->Delete(encode_binary_key(key));
}

std::vector<std::pair<std::string, Value>> KVStore::Scan(const std::string& start_key,
                                                        const std::string& end_key) {
    return pimpl_->engine->Scan(start_key, end_key);
}

KVTransaction KVStore::BeginTransaction() {
    return KVTransaction(std::make_unique<KVTransaction::Impl>(pimpl_->engine));
}

void KVStore::Flush() {
    pimpl_->engine->Flush();
}

void KVStore::Compact() {
    pimpl_->engine->Compact();
}

KVStoreMetrics KVStore::GetMetrics() {
    return pimpl_->engine->GetMetrics();
}

KVTransaction::KVTransaction(std::unique_ptr<Impl> impl)
    : pimpl_(std::move(impl)) {}

KVTransaction::KVTransaction(KVTransaction&&) noexcept = default;

KVTransaction& KVTransaction::operator=(KVTransaction&&) noexcept = default;

KVTransaction::~KVTransaction() {
    Rollback();
}

KVTransaction::Impl& KVTransaction::require_impl() {
    if (!pimpl_) {
        throw KVStoreError("Transaction is no longer active");
    }
    return *pimpl_;
}

std::optional<Value> KVTransaction::Get(int key) {
    return require_impl().get(encode_int_key(key));
}

std::optional<Value> KVTransaction::Get(const std::string& key) {
    return require_impl().get(encode_string_key(key));
}

std::optional<Value> KVTransaction::Get(const std::vector<uint8_t>& key) {
    return require_impl().get(encode_binary_key(key));
}

void KVTransaction::Put(int key, Value value) {
    require_impl().put(encode_int_key(key), std::move(value));
}

void KVTransaction::Put(const std::string& key, Value value) {
    require_impl().put(encode_string_key(key), std::move(value));
}

void KVTransaction::Put(const std::vector<uint8_t>& key, Value value) {
    require_impl().put(encode_binary_key(key), std::move(value));
}

void KVTransaction::Delete(int key) {
    require_impl().erase(encode_int_key(key));
}

void KVTransaction::Delete(const std::string& key) {
    require_impl().erase(encode_string_key(key));
}

void KVTransaction::Delete(const std::vector<uint8_t>& key) {
    require_impl().erase(encode_binary_key(key));
}

void KVTransaction::Commit() {
    require_impl().commit();
}

void KVTransaction::Rollback() noexcept {
    if (pimpl_) {
        pimpl_->rollback();
    }
}

std::string MetricsToJson(const KVStoreMetrics& metrics) {
    return kvstore::internal::metrics_to_json(metrics);
}

KVStoreOptions RecommendedOptions(KVStoreProfile profile) {
    return kvstore::internal::recommended_options(profile);
}

std::string OptionsToJson(const KVStoreOptions& options) {
    return kvstore::internal::options_to_json(options);
}
