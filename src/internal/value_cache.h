#ifndef KVSTORE_INTERNAL_VALUE_CACHE_H
#define KVSTORE_INTERNAL_VALUE_CACHE_H

#include "kvstore.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace kvstore::internal {

class ValueCache {
public:
    explicit ValueCache(uint64_t capacity_bytes);
    ~ValueCache();

    ValueCache(const ValueCache&) = delete;
    ValueCache& operator=(const ValueCache&) = delete;

    std::optional<Value> Lookup(const std::string& key, uint64_t lsn) const;
    void Insert(const std::string& key, uint64_t lsn, const Value& value);

    uint64_t ChargeBytes() const noexcept;
    uint64_t CapacityBytes() const noexcept;
    size_t SegmentCount() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> pimpl_;
};

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_VALUE_CACHE_H
