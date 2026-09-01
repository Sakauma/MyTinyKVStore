#include "value_cache.h"

#include "storage_format.h"

#include <algorithm>
#include <atomic>
#include <iterator>
#include <list>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace kvstore::internal {
namespace {

uint64_t mix64(uint64_t value) {
    value ^= value >> 30U;
    value *= 0xBF58476D1CE4E5B9ULL;
    value ^= value >> 27U;
    value *= 0x94D049BB133111EBULL;
    value ^= value >> 31U;
    return value;
}

uint64_t cache_hash(const std::string& key, uint64_t lsn) {
    return mix64(stable_key_hash(key) ^ mix64(lsn + 0x9E3779B97F4A7C15ULL));
}

size_t segment_count_for_capacity(uint64_t capacity_bytes) {
    if (capacity_bytes == 0) {
        return 0;
    }
    constexpr uint64_t kBytesPerSegment = 64ULL * 1024ULL;
    const uint64_t desired = std::max<uint64_t>(
        1, std::min<uint64_t>(64, capacity_bytes / kBytesPerSegment));
    size_t count = 1;
    while (count <= desired / 2) {
        count *= 2;
    }
    return count;
}

struct CacheKey {
    std::string key;
    uint64_t lsn = 0;

    bool operator==(const CacheKey& other) const {
        return lsn == other.lsn && key == other.key;
    }
};

struct CacheKeyHash {
    size_t operator()(const CacheKey& key) const {
        return static_cast<size_t>(cache_hash(key.key, key.lsn));
    }
};

struct CacheNode {
    CacheKey key;
    std::shared_ptr<const Value> value;
    uint64_t charge = 0;
    mutable std::atomic<bool> referenced {true};
};

uint64_t node_charge(const std::string& key, const Value& value) {
    constexpr uint64_t kContainerOverhead =
        sizeof(CacheNode) + sizeof(CacheKey) + sizeof(Value) +
        sizeof(std::shared_ptr<CacheNode>) + 7 * sizeof(void*);
    const uint64_t key_bytes = static_cast<uint64_t>(key.size()) * 2;
    const uint64_t value_bytes = static_cast<uint64_t>(value.bytes.size());
    if (key_bytes > std::numeric_limits<uint64_t>::max() - value_bytes ||
        key_bytes + value_bytes > std::numeric_limits<uint64_t>::max() - kContainerOverhead) {
        return std::numeric_limits<uint64_t>::max();
    }
    return key_bytes + value_bytes + kContainerOverhead;
}

}  // namespace

class ValueCache::Impl {
public:
    explicit Impl(uint64_t capacity_bytes)
        : capacity_bytes_(capacity_bytes),
          segment_count_(segment_count_for_capacity(capacity_bytes)) {
        segments_.reserve(segment_count_);
        for (size_t index = 0; index < segment_count_; ++index) {
            const uint64_t base = capacity_bytes_ / segment_count_;
            const uint64_t remainder = capacity_bytes_ % segment_count_;
            auto segment = std::make_unique<Segment>();
            segment->budget = base + (index < remainder ? 1U : 0U);
            segments_.push_back(std::move(segment));
        }
    }

    std::optional<Value> Lookup(const std::string& key, uint64_t lsn) const {
        if (segment_count_ == 0) {
            return std::nullopt;
        }
        const CacheKey lookup {key, lsn};
        Segment& segment = segment_for(key, lsn);
        std::shared_ptr<CacheNode> node;
        {
            std::shared_lock<std::shared_mutex> lock(segment.mutex);
            const auto found = segment.entries.find(lookup);
            if (found == segment.entries.end()) {
                return std::nullopt;
            }
            node = *found->second;
            node->referenced.store(true, std::memory_order_relaxed);
        }
        return *node->value;
    }

    void Insert(const std::string& key, uint64_t lsn, const Value& value) {
        if (segment_count_ == 0) {
            return;
        }
        Segment& segment = segment_for(key, lsn);
        const uint64_t charge = node_charge(key, value);

        const CacheKey lookup {key, lsn};
        std::unique_lock<std::shared_mutex> lock(segment.mutex);
        const auto existing = segment.entries.find(lookup);
        if (existing != segment.entries.end()) {
            erase_entry(segment, existing);
        }
        if (charge > segment.budget) {
            return;
        }

        while (segment.charge > segment.budget - charge) {
            evict_one(segment);
        }

        auto node = std::make_shared<CacheNode>();
        node->key = lookup;
        node->value = std::make_shared<const Value>(value);
        node->charge = charge;
        segment.clock.push_back(node);
        const auto position = std::prev(segment.clock.end());
        try {
            segment.entries.emplace(lookup, position);
        } catch (...) {
            segment.clock.pop_back();
            throw;
        }
        if (segment.hand == segment.clock.end()) {
            segment.hand = segment.clock.begin();
        }
        segment.charge += charge;
        total_charge_.fetch_add(charge, std::memory_order_relaxed);
    }

    uint64_t ChargeBytes() const noexcept {
        return total_charge_.load(std::memory_order_relaxed);
    }

    uint64_t CapacityBytes() const noexcept {
        return capacity_bytes_;
    }

    size_t SegmentCount() const noexcept {
        return segment_count_;
    }

private:
    struct Segment {
        using Clock = std::list<std::shared_ptr<CacheNode>>;
        using ClockIterator = Clock::iterator;

        Segment() : hand(clock.end()) {}

        mutable std::shared_mutex mutex;
        std::unordered_map<CacheKey, ClockIterator, CacheKeyHash> entries;
        Clock clock;
        ClockIterator hand;
        uint64_t charge = 0;
        uint64_t budget = 0;
    };

    uint64_t capacity_bytes_ = 0;
    size_t segment_count_ = 0;
    std::vector<std::unique_ptr<Segment>> segments_;
    mutable std::atomic<uint64_t> total_charge_ {0};

    Segment& segment_for(const std::string& key, uint64_t lsn) const {
        const size_t index = static_cast<size_t>(cache_hash(key, lsn)) &
                             (segment_count_ - 1);
        return *segments_[index];
    }

    void evict_one(Segment& segment) {
        while (!segment.clock.empty()) {
            if (segment.hand == segment.clock.end()) {
                segment.hand = segment.clock.begin();
            }
            const auto candidate_position = segment.hand;
            const std::shared_ptr<CacheNode> candidate = *candidate_position;
            ++segment.hand;
            if (candidate->referenced.exchange(false, std::memory_order_relaxed)) {
                continue;
            }

            const auto entry = segment.entries.find(candidate->key);
            if (entry == segment.entries.end() || entry->second != candidate_position) {
                throw KVStoreError("CLOCK cache index is inconsistent with its eviction ring");
            }
            erase_entry(segment, entry);
            return;
        }
    }

    using EntryIterator = std::unordered_map<
        CacheKey,
        Segment::ClockIterator,
        CacheKeyHash>::iterator;

    void erase_entry(Segment& segment, EntryIterator entry) {
        const auto position = entry->second;
        const uint64_t charge = (*position)->charge;
        if (segment.hand == position) {
            ++segment.hand;
        }
        segment.clock.erase(position);
        segment.entries.erase(entry);
        if (segment.clock.empty()) {
            segment.hand = segment.clock.end();
        }
        segment.charge -= charge;
        total_charge_.fetch_sub(charge, std::memory_order_relaxed);
    }
};

ValueCache::ValueCache(uint64_t capacity_bytes)
    : pimpl_(std::make_unique<Impl>(capacity_bytes)) {}

ValueCache::~ValueCache() = default;

std::optional<Value> ValueCache::Lookup(const std::string& key, uint64_t lsn) const {
    return pimpl_->Lookup(key, lsn);
}

void ValueCache::Insert(const std::string& key, uint64_t lsn, const Value& value) {
    pimpl_->Insert(key, lsn, value);
}

uint64_t ValueCache::ChargeBytes() const noexcept {
    return pimpl_->ChargeBytes();
}

uint64_t ValueCache::CapacityBytes() const noexcept {
    return pimpl_->CapacityBytes();
}

size_t ValueCache::SegmentCount() const noexcept {
    return pimpl_->SegmentCount();
}

}  // namespace kvstore::internal
