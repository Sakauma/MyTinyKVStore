#ifndef KVSTORE_INTERNAL_STORAGE_FORMAT_H
#define KVSTORE_INTERNAL_STORAGE_FORMAT_H

#include "kvstore.h"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace kvstore::internal {

inline constexpr char kContainerMagic[8] = {'M', 'T', 'K', 'V', '0', '0', '0', '3'};
inline constexpr uint32_t kFormatVersion = 3;
inline constexpr uint64_t kSuperblockBytes = 4096;
inline constexpr uint64_t kDataOffset = kSuperblockBytes * 2;
inline constexpr uint64_t kMaxKeyBytes = 1024ULL * 1024ULL;
inline constexpr uint64_t kMaxValueBytes = 64ULL * 1024ULL * 1024ULL;
inline constexpr uint64_t kMaxTransactionBytes = 256ULL * 1024ULL * 1024ULL;

inline constexpr uint32_t kIndexMagic = 0x33445849;       // IXD3
inline constexpr uint32_t kIndexEntryMagic = 0x33454B49;  // IKE3
inline constexpr uint32_t kFrameMagic = 0x334D5246;       // FRM3
inline constexpr uint32_t kMutationMagic = 0x3354554D;    // MUT3
inline constexpr uint32_t kFooterMagic = 0x33444E45;      // END3

enum class MutationType : uint8_t {
    kPut = 1,
    kDelete = 2,
};

#pragma pack(push, 1)
struct Superblock {
    char magic[8];
    uint32_t version;
    uint32_t header_bytes;
    uint64_t generation;
    uint64_t checkpoint_lsn;
    uint64_t index_offset;
    uint64_t index_length;
    uint64_t object_offset;
    uint64_t object_length;
    uint64_t journal_offset;
    uint64_t checkpoint_checksum;
    uint32_t flags;
    uint32_t checksum;
    uint8_t reserved[32];
};

struct IndexHeader {
    uint32_t magic;
    uint32_t version;
    uint64_t entry_count;
    uint64_t entries_bytes;
    uint32_t entries_checksum;
    uint32_t checksum;
};

struct IndexEntryHeader {
    uint32_t magic;
    uint32_t key_size;
    uint64_t value_offset;
    uint32_t value_size;
    uint32_t value_checksum;
    uint32_t checksum;
    uint32_t reserved;
};

struct FrameHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
    uint32_t header_bytes;
    uint64_t frame_bytes;
    uint64_t lsn;
    uint32_t operation_count;
    uint32_t payload_checksum;
    uint32_t header_checksum;
    uint32_t reserved;
};

struct MutationHeader {
    uint32_t magic;
    uint8_t type;
    uint8_t reserved[3];
    uint32_t key_size;
    uint32_t value_size;
    uint32_t checksum;
};

struct FrameFooter {
    uint32_t magic;
    uint32_t version;
    uint64_t lsn;
    uint64_t frame_bytes;
    uint32_t payload_checksum;
    uint32_t checksum;
};
#pragma pack(pop)

struct Mutation {
    MutationType type = MutationType::kPut;
    std::string key;
    Value value;
    uint64_t value_offset = 0;
    uint32_t value_checksum = 0;
    uint64_t wal_charge = 0;
    bool has_backing = false;
};

struct CheckpointImage {
    Superblock superblock {};
    std::vector<uint8_t> index;
    std::vector<uint8_t> objects;
};

struct RecoveryResult {
    Superblock superblock {};
    uint64_t append_offset = 0;
    uint64_t last_lsn = 0;
    uint64_t checkpoint_entries = 0;
    uint64_t journal_frames = 0;
    uint64_t journal_operations = 0;
    uint64_t put_operations = 0;
    uint64_t delete_operations = 0;
    uint64_t int_keys = 0;
    uint64_t string_keys = 0;
    uint64_t binary_keys = 0;
    uint32_t valid_superblocks = 0;
    bool degraded_superblocks = false;
    bool truncated_tail = false;
};

using ApplyCallback = std::function<void(const Mutation&, uint64_t)>;

uint32_t crc32c(const void* data, size_t size);
uint32_t crc32c_extend(uint32_t seed, const void* data, size_t size);
uint32_t crc32c_software_extend(uint32_t seed, const void* data, size_t size);
uint64_t stable_key_hash(const std::string& key);
uint64_t mutation_physical_charge(uint64_t encoded_mutation_bytes,
                                  uint32_t operation_index,
                                  uint32_t operation_count);

Superblock make_superblock(uint64_t generation,
                           uint64_t checkpoint_lsn,
                           uint64_t index_offset,
                           uint64_t index_length,
                           uint64_t object_offset,
                           uint64_t object_length,
                           uint64_t journal_offset,
                           uint64_t checkpoint_checksum);
IndexHeader make_index_header(uint64_t entry_count,
                              uint64_t entries_bytes,
                              uint32_t entries_checksum);
IndexEntryHeader make_index_entry_header(const std::string& key,
                                         uint64_t value_offset,
                                         uint32_t value_size,
                                         uint32_t value_checksum);
bool valid_superblock(const Superblock& superblock);
void write_superblocks(int fd, const Superblock& superblock, const std::string& path);
void initialize_file(int fd, const std::string& path);

std::vector<uint8_t> serialize_payload(const std::vector<Mutation>& operations);
FrameHeader make_frame_header(uint64_t payload_bytes,
                              uint32_t operation_count,
                              uint64_t lsn,
                              uint32_t payload_checksum);
FrameFooter make_frame_footer(const FrameHeader& header);
std::vector<uint8_t> serialize_frame(const std::vector<uint8_t>& payload,
                                     uint32_t operation_count,
                                     uint64_t lsn);
CheckpointImage build_checkpoint(const std::vector<Mutation>& entries,
                                 uint64_t generation,
                                 uint64_t checkpoint_lsn);
void write_checkpoint_image(int fd,
                            const CheckpointImage& image,
                            const std::string& path);

RecoveryResult recover_file(int fd,
                            const std::string& path,
                            const ApplyCallback& apply,
                            bool repair_truncated_tail);
RecoveryResult inspect_file(const std::string& path);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_STORAGE_FORMAT_H
