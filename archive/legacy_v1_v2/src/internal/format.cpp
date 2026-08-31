#include "format.h"

#include <stdexcept>

namespace kvstore::internal {

static_assert(sizeof(WalRecordHeader) == 20, "Unexpected WAL header size");
static_assert(sizeof(WalRecordHeaderV1) == 20, "Unexpected WAL v1 header size");
static_assert(sizeof(SnapshotHeader) == 16, "Unexpected snapshot header size");
static_assert(sizeof(SnapshotEntryHeader) == 12, "Unexpected snapshot entry size");
static_assert(sizeof(SnapshotEntryHeaderV1) == 12, "Unexpected snapshot v1 entry size");

namespace {

uint32_t fnv1a_append(uint32_t seed, const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t hash = seed;
    for (size_t i = 0; i < size; ++i) {
        hash ^= bytes[i];
        hash *= 16777619u;
    }
    return hash;
}

}  // namespace

SnapshotHeader make_snapshot_header() {
    SnapshotHeader header {};
    std::memcpy(header.magic, kSnapshotMagic, sizeof(kSnapshotMagic));
    header.version = kSnapshotVersion;
    header.reserved = 0;
    return header;
}

std::string encode_int_key(int32_t key) {
    std::string encoded(1, kIntKeyTag);
    encoded.push_back(static_cast<char>((static_cast<uint32_t>(key) >> 24) & 0xFF));
    encoded.push_back(static_cast<char>((static_cast<uint32_t>(key) >> 16) & 0xFF));
    encoded.push_back(static_cast<char>((static_cast<uint32_t>(key) >> 8) & 0xFF));
    encoded.push_back(static_cast<char>(static_cast<uint32_t>(key) & 0xFF));
    return encoded;
}

std::string encode_string_key(const std::string& key) {
    std::string encoded(1, kStringKeyTag);
    encoded += key;
    return encoded;
}

std::string encode_binary_key(const std::vector<uint8_t>& key) {
    std::string encoded(1, kBinaryKeyTag);
    if (!key.empty()) {
        encoded.append(reinterpret_cast<const char*>(key.data()), key.size());
    }
    return encoded;
}

bool is_string_key(const std::string& key) {
    return !key.empty() && key.front() == kStringKeyTag;
}

std::string decode_string_key(const std::string& key) {
    if (!is_string_key(key)) {
        throw KVStoreError("Attempted to decode non-string key");
    }
    return key.substr(1);
}

uint32_t checksum_record(WalRecordType type, const std::string& key, const Value& value) {
    uint32_t hash = 2166136261u;
    const uint8_t type_byte = static_cast<uint8_t>(type);
    const uint32_t key_size = static_cast<uint32_t>(key.size());
    const uint32_t size = static_cast<uint32_t>(value.bytes.size());
    hash = fnv1a_append(hash, &type_byte, sizeof(type_byte));
    hash = fnv1a_append(hash, &key_size, sizeof(key_size));
    hash = fnv1a_append(hash, &size, sizeof(size));
    if (!key.empty()) {
        hash = fnv1a_append(hash, key.data(), key.size());
    }
    if (!value.bytes.empty()) {
        hash = fnv1a_append(hash, value.bytes.data(), value.bytes.size());
    }
    return hash;
}

uint32_t checksum_record_v1(WalRecordType type, int32_t key, const Value& value) {
    uint32_t hash = 2166136261u;
    const uint8_t type_byte = static_cast<uint8_t>(type);
    const uint32_t size = static_cast<uint32_t>(value.bytes.size());
    hash = fnv1a_append(hash, &type_byte, sizeof(type_byte));
    hash = fnv1a_append(hash, &key, sizeof(key));
    hash = fnv1a_append(hash, &size, sizeof(size));
    if (!value.bytes.empty()) {
        hash = fnv1a_append(hash, value.bytes.data(), value.bytes.size());
    }
    return hash;
}

}  // namespace kvstore::internal
