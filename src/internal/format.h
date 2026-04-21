#ifndef KVSTORE_INTERNAL_FORMAT_H
#define KVSTORE_INTERNAL_FORMAT_H

#include "kvstore.h"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace kvstore::internal {

inline constexpr uint32_t kWalMagic = 0x4B565741;
inline constexpr uint16_t kWalVersion = 2;
inline constexpr uint32_t kSnapshotEntryMagic = 0x4B565345;
inline constexpr uint32_t kSnapshotVersion = 2;
inline constexpr char kSnapshotMagic[8] = {'K', 'V', 'S', 'N', 'A', 'P', '0', '1'};
inline constexpr char kIntKeyTag = '\x01';
inline constexpr char kStringKeyTag = '\x02';
inline constexpr char kBinaryKeyTag = '\x03';

enum class WalRecordType : uint8_t {
    kPut = 1,
    kDelete = 2,
};

#pragma pack(push, 1)
struct WalRecordHeader {
    uint32_t magic;
    uint16_t version;
    uint8_t type;
    uint8_t reserved;
    uint32_t key_size;
    uint32_t value_size;
    uint32_t checksum;
};

struct WalRecordHeaderV1 {
    uint32_t magic;
    uint16_t version;
    uint8_t type;
    uint8_t reserved;
    int32_t key;
    uint32_t value_size;
    uint32_t checksum;
};

struct SnapshotHeader {
    char magic[8];
    uint32_t version;
    uint32_t reserved;
};

struct SnapshotEntryHeader {
    uint32_t magic;
    uint32_t key_size;
    uint32_t value_size;
};

struct SnapshotEntryHeaderV1 {
    uint32_t magic;
    int32_t key;
    uint32_t value_size;
};
#pragma pack(pop)

std::string encode_int_key(int32_t key);
std::string encode_string_key(const std::string& key);
std::string encode_binary_key(const std::vector<uint8_t>& key);
bool is_string_key(const std::string& key);
std::string decode_string_key(const std::string& key);
uint32_t checksum_record(WalRecordType type, const std::string& key, const Value& value);
uint32_t checksum_record_v1(WalRecordType type, int32_t key, const Value& value);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_FORMAT_H
