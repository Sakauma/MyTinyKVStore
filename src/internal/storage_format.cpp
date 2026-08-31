#include "storage_format.h"

#include "io.h"
#include "key_codec.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <string_view>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace kvstore::internal {

static_assert(sizeof(Superblock) <= kSuperblockBytes, "superblock must fit in one block");
static_assert(sizeof(FrameHeader) == 44, "unexpected frame header size");
static_assert(sizeof(MutationHeader) == 20, "unexpected mutation header size");
static_assert(sizeof(FrameFooter) == 32, "unexpected frame footer size");

namespace {

template <typename T>
void append_object(std::vector<uint8_t>& output, const T& value) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
    output.insert(output.end(), bytes, bytes + sizeof(T));
}

void append_bytes(std::vector<uint8_t>& output, const void* data, size_t size) {
    if (size == 0) {
        return;
    }
    const auto* bytes = static_cast<const uint8_t*>(data);
    output.insert(output.end(), bytes, bytes + size);
}

void pread_exact(int fd, void* buffer, size_t size, uint64_t offset, const std::string& path) {
    auto* cursor = static_cast<uint8_t*>(buffer);
    size_t total = 0;
    while (total < size) {
        const ssize_t nread = ::pread(fd,
                                      cursor + total,
                                      size - total,
                                      static_cast<off_t>(offset + total));
        if (nread < 0 && errno == EINTR) {
            continue;
        }
        if (nread < 0) {
            throw io_error("pread", path);
        }
        if (nread == 0) {
            throw KVStoreError("Unexpected EOF in storage container: " + path);
        }
        total += static_cast<size_t>(nread);
    }
}

void pwrite_all(int fd, const void* buffer, size_t size, uint64_t offset, const std::string& path) {
    const auto* cursor = static_cast<const uint8_t*>(buffer);
    size_t total = 0;
    while (total < size) {
        const ssize_t written = ::pwrite(fd,
                                         cursor + total,
                                         size - total,
                                         static_cast<off_t>(offset + total));
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written < 0) {
            throw io_error("pwrite", path);
        }
        if (written == 0) {
            throw KVStoreError("pwrite made no progress for " + path);
        }
        total += static_cast<size_t>(written);
    }
}

uint64_t file_size(int fd, const std::string& path) {
    struct stat st {};
    if (::fstat(fd, &st) != 0) {
        throw io_error("fstat", path);
    }
    if (st.st_size < 0) {
        throw KVStoreError("Negative file size for " + path);
    }
    return static_cast<uint64_t>(st.st_size);
}

uint32_t superblock_checksum(const Superblock& value) {
    Superblock normalized = value;
    normalized.checksum = 0;
    return crc32c(&normalized, sizeof(normalized));
}

uint32_t index_header_checksum(const IndexHeader& value) {
    IndexHeader normalized = value;
    normalized.checksum = 0;
    return crc32c(&normalized, sizeof(normalized));
}

uint32_t index_entry_checksum(const IndexEntryHeader& value, std::string_view key) {
    IndexEntryHeader normalized = value;
    normalized.checksum = 0;
    uint32_t checksum = crc32c(&normalized, sizeof(normalized));
    return crc32c_extend(checksum, key.data(), key.size());
}

uint32_t frame_header_checksum(const FrameHeader& value) {
    FrameHeader normalized = value;
    normalized.header_checksum = 0;
    return crc32c(&normalized, sizeof(normalized));
}

uint32_t mutation_checksum(const MutationHeader& value,
                           std::string_view key,
                           const Value& payload) {
    MutationHeader normalized = value;
    normalized.checksum = 0;
    uint32_t checksum = crc32c(&normalized, sizeof(normalized));
    checksum = crc32c_extend(checksum, key.data(), key.size());
    return crc32c_extend(checksum, payload.bytes.data(), payload.bytes.size());
}

uint32_t footer_checksum(const FrameFooter& value) {
    FrameFooter normalized = value;
    normalized.checksum = 0;
    return crc32c(&normalized, sizeof(normalized));
}

bool add_overflows(uint64_t lhs, uint64_t rhs) {
    return rhs > std::numeric_limits<uint64_t>::max() - lhs;
}

void validate_encoded_key(std::string_view key) {
    if (key.empty() || key.size() > kMaxKeyBytes) {
        throw KVStoreError("Encoded key is empty or exceeds the storage format limit");
    }
    switch (key.front()) {
        case kIntKeyTag:
            if (key.size() != 5) {
                throw KVStoreError("Integer key has an invalid encoded length");
            }
            return;
        case kStringKeyTag:
        case kBinaryKeyTag:
            return;
        default:
            throw KVStoreError("Key uses an unknown namespace tag");
    }
}

void validate_key_and_value(const Mutation& operation) {
    validate_encoded_key(operation.key);
    if (operation.type != MutationType::kPut && operation.type != MutationType::kDelete) {
        throw KVStoreError("Mutation contains an unknown operation type");
    }
    if (operation.value.bytes.size() > kMaxValueBytes) {
        throw KVStoreError("Value exceeds the storage format limit");
    }
    if (operation.type == MutationType::kDelete && !operation.value.bytes.empty()) {
        throw KVStoreError("Delete mutation contains a value");
    }
}

void classify_key(const std::string& key, RecoveryResult& result) {
    if (key.empty()) {
        return;
    }
    switch (key.front()) {
        case kIntKeyTag:
            ++result.int_keys;
            break;
        case kStringKeyTag:
            ++result.string_keys;
            break;
        case kBinaryKeyTag:
            ++result.binary_keys;
            break;
        default:
            break;
    }
}

uint32_t crc_file_region(int fd,
                         uint64_t offset,
                         uint64_t length,
                         uint32_t seed,
                         const std::string& path) {
    std::array<uint8_t, 1024 * 1024> buffer {};
    uint32_t checksum = seed;
    uint64_t consumed = 0;
    while (consumed < length) {
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(buffer.size(), length - consumed));
        pread_exact(fd, buffer.data(), chunk, offset + consumed, path);
        checksum = crc32c_extend(checksum, buffer.data(), chunk);
        consumed += chunk;
    }
    return checksum;
}

Superblock read_best_superblock(int fd,
                                const std::string& path,
                                uint64_t size,
                                uint32_t& valid_count) {
    if (size < kDataOffset) {
        throw KVStoreError("Storage container header is truncated: " + path);
    }

    Superblock copies[2] {};
    pread_exact(fd, &copies[0], sizeof(Superblock), 0, path);
    pread_exact(fd, &copies[1], sizeof(Superblock), kSuperblockBytes, path);

    const bool valid[2] = {valid_superblock(copies[0]), valid_superblock(copies[1])};
    valid_count = static_cast<uint32_t>(valid[0]) + static_cast<uint32_t>(valid[1]);
    if (valid_count == 0) {
        throw KVStoreError("Both superblocks are invalid: " + path);
    }
    const Superblock& selected = !valid[0] ? copies[1]
                                       : !valid[1] ? copies[0]
                                                   : (copies[1].generation > copies[0].generation ? copies[1] : copies[0]);

    if (selected.index_offset < kDataOffset ||
        selected.checkpoint_checksum > std::numeric_limits<uint32_t>::max() ||
        add_overflows(selected.index_offset, selected.index_length) ||
        add_overflows(selected.object_offset, selected.object_length) ||
        selected.index_offset + selected.index_length > size ||
        selected.object_offset + selected.object_length > size ||
        selected.journal_offset > size ||
        selected.object_offset < selected.index_offset + selected.index_length ||
        selected.journal_offset < selected.object_offset + selected.object_length) {
        throw KVStoreError("Superblock contains invalid region boundaries: " + path);
    }
    return selected;
}

void recover_checkpoint(int fd,
                        const std::string& path,
                        const Superblock& superblock,
                        const ApplyCallback& apply,
                        RecoveryResult& result) {
    if (superblock.index_length == 0 && superblock.object_length == 0) {
        return;
    }
    if (superblock.index_length < sizeof(IndexHeader)) {
        throw KVStoreError("storage checkpoint index is truncated: " + path);
    }
    if (superblock.index_length > kMaxTransactionBytes * 16ULL ||
        superblock.object_length > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        throw KVStoreError("storage checkpoint region exceeds implementation limits: " + path);
    }

    uint32_t checkpoint_crc = crc_file_region(
        fd, superblock.index_offset, superblock.index_length, 0, path);
    checkpoint_crc = crc_file_region(
        fd, superblock.object_offset, superblock.object_length, checkpoint_crc, path);
    if (checkpoint_crc != static_cast<uint32_t>(superblock.checkpoint_checksum)) {
        throw KVStoreError("storage checkpoint checksum mismatch: " + path);
    }

    IndexHeader header {};
    pread_exact(fd, &header, sizeof(header), superblock.index_offset, path);
    if (header.magic != kIndexMagic || header.version != kFormatVersion ||
        header.checksum != index_header_checksum(header) ||
        header.entries_bytes != superblock.index_length - sizeof(header) ||
        header.entry_count > header.entries_bytes / sizeof(IndexEntryHeader)) {
        throw KVStoreError("storage checkpoint index header is invalid: " + path);
    }
    if (crc_file_region(fd,
                        superblock.index_offset + sizeof(header),
                        header.entries_bytes,
                        0,
                        path) !=
        header.entries_checksum) {
        throw KVStoreError("storage checkpoint index checksum mismatch: " + path);
    }

    uint64_t cursor = superblock.index_offset + sizeof(header);
    const uint64_t index_end = superblock.index_offset + superblock.index_length;
    for (uint64_t entry_index = 0; entry_index < header.entry_count; ++entry_index) {
        if (index_end - cursor < sizeof(IndexEntryHeader)) {
            throw KVStoreError("storage checkpoint entry header is truncated: " + path);
        }
        IndexEntryHeader entry {};
        pread_exact(fd, &entry, sizeof(entry), cursor, path);
        cursor += sizeof(entry);
        if (entry.magic != kIndexEntryMagic || entry.key_size == 0 ||
            entry.key_size > kMaxKeyBytes || entry.value_size > kMaxValueBytes ||
            index_end - cursor < entry.key_size ||
            add_overflows(entry.value_offset, entry.value_size) ||
            entry.value_offset + entry.value_size > superblock.object_length) {
            throw KVStoreError("storage checkpoint entry contains invalid lengths: " + path);
        }
        std::string key(entry.key_size, '\0');
        pread_exact(fd, key.data(), key.size(), cursor, path);
        cursor += entry.key_size;
        validate_encoded_key(key);
        if (entry.checksum != index_entry_checksum(entry, key)) {
            throw KVStoreError("storage checkpoint entry checksum mismatch: " + path);
        }
        Value value(std::vector<uint8_t>(entry.value_size));
        if (entry.value_size != 0) {
            pread_exact(fd,
                        value.bytes.data(),
                        value.bytes.size(),
                        superblock.object_offset + entry.value_offset,
                        path);
        }
        if (crc32c(value.bytes.data(), value.bytes.size()) != entry.value_checksum) {
            throw KVStoreError("storage checkpoint object checksum mismatch: " + path);
        }
        classify_key(key, result);
        ++result.checkpoint_entries;
        if (apply) {
            Mutation operation {MutationType::kPut, std::move(key), std::move(value)};
            operation.value_offset = superblock.object_offset + entry.value_offset;
            operation.value_checksum = entry.value_checksum;
            operation.has_backing = true;
            apply(operation, superblock.checkpoint_lsn);
        }
    }
    if (cursor != index_end) {
        throw KVStoreError("storage checkpoint index has trailing bytes: " + path);
    }
}

std::vector<Mutation> parse_payload(const uint8_t* data,
                                      size_t size,
                                      uint32_t expected_operations,
                                      const std::string& path) {
    std::vector<Mutation> operations;
    operations.reserve(expected_operations);
    size_t cursor = 0;
    for (uint32_t index = 0; index < expected_operations; ++index) {
        if (size - cursor < sizeof(MutationHeader)) {
            throw KVStoreError("storage transaction mutation header is truncated: " + path);
        }
        MutationHeader header {};
        std::memcpy(&header, data + cursor, sizeof(header));
        cursor += sizeof(header);
        const bool valid_type = header.type == static_cast<uint8_t>(MutationType::kPut) ||
                                header.type == static_cast<uint8_t>(MutationType::kDelete);
        if (header.magic != kMutationMagic || !valid_type || header.key_size == 0 ||
            header.key_size > kMaxKeyBytes || header.value_size > kMaxValueBytes ||
            (header.type == static_cast<uint8_t>(MutationType::kDelete) && header.value_size != 0) ||
            add_overflows(header.key_size, header.value_size) ||
            size - cursor < static_cast<uint64_t>(header.key_size) + header.value_size) {
            throw KVStoreError("storage transaction mutation is invalid: " + path);
        }
        std::string key(reinterpret_cast<const char*>(data + cursor), header.key_size);
        cursor += header.key_size;
        validate_encoded_key(key);
        const uint64_t value_offset = cursor;
        Value value(std::vector<uint8_t>(header.value_size));
        if (header.value_size != 0) {
            std::memcpy(value.bytes.data(), data + cursor, header.value_size);
            cursor += header.value_size;
        }
        if (header.checksum != mutation_checksum(header, key, value)) {
            throw KVStoreError("storage transaction mutation checksum mismatch: " + path);
        }
        Mutation operation {
            static_cast<MutationType>(header.type),
            std::move(key),
            std::move(value),
        };
        operation.value_offset = value_offset;
        operation.value_checksum = crc32c(operation.value.bytes.data(), operation.value.bytes.size());
        operation.has_backing = operation.type == MutationType::kPut;
        operations.push_back(std::move(operation));
    }
    if (cursor != size) {
        throw KVStoreError("storage transaction payload has trailing bytes: " + path);
    }
    return operations;
}

}  // namespace

uint32_t crc32c_extend(uint32_t seed, const void* data, size_t size) {
    uint32_t crc = ~seed;
    const auto* bytes = static_cast<const uint8_t*>(data);
    for (size_t index = 0; index < size; ++index) {
        crc ^= bytes[index];
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask = 0U - (crc & 1U);
            crc = (crc >> 1U) ^ (0x82F63B78U & mask);
        }
    }
    return ~crc;
}

uint32_t crc32c(const void* data, size_t size) {
    return crc32c_extend(0, data, size);
}

uint64_t stable_key_hash(const std::string& key) {
    uint64_t hash = 1469598103934665603ULL;
    for (const unsigned char byte : key) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

Superblock make_superblock(uint64_t generation,
                           uint64_t checkpoint_lsn,
                           uint64_t index_offset,
                           uint64_t index_length,
                           uint64_t object_offset,
                           uint64_t object_length,
                           uint64_t journal_offset,
                           uint64_t checkpoint_checksum) {
    Superblock result {};
    std::memcpy(result.magic, kContainerMagic, sizeof(kContainerMagic));
    result.version = kFormatVersion;
    result.header_bytes = sizeof(Superblock);
    result.generation = generation;
    result.checkpoint_lsn = checkpoint_lsn;
    result.index_offset = index_offset;
    result.index_length = index_length;
    result.object_offset = object_offset;
    result.object_length = object_length;
    result.journal_offset = journal_offset;
    result.checkpoint_checksum = checkpoint_checksum;
    result.flags = 0;
    result.checksum = superblock_checksum(result);
    return result;
}

IndexHeader make_index_header(uint64_t entry_count,
                              uint64_t entries_bytes,
                              uint32_t entries_checksum) {
    IndexHeader result {
        kIndexMagic,
        kFormatVersion,
        entry_count,
        entries_bytes,
        entries_checksum,
        0,
    };
    result.checksum = index_header_checksum(result);
    return result;
}

IndexEntryHeader make_index_entry_header(const std::string& key,
                                         uint64_t value_offset,
                                         uint32_t value_size,
                                         uint32_t value_checksum) {
    if (key.empty() || key.size() > kMaxKeyBytes || value_size > kMaxValueBytes) {
        throw KVStoreError("Cannot encode an invalid storage checkpoint entry");
    }
    IndexEntryHeader result {
        kIndexEntryMagic,
        static_cast<uint32_t>(key.size()),
        value_offset,
        value_size,
        value_checksum,
        0,
        0,
    };
    result.checksum = index_entry_checksum(result, key);
    return result;
}

bool valid_superblock(const Superblock& superblock) {
    return std::memcmp(superblock.magic, kContainerMagic, sizeof(kContainerMagic)) == 0 &&
           superblock.version == kFormatVersion &&
           superblock.header_bytes == sizeof(Superblock) &&
           superblock.checksum == superblock_checksum(superblock);
}

void write_superblocks(int fd, const Superblock& superblock, const std::string& path) {
    std::array<uint8_t, kSuperblockBytes> block {};
    std::memcpy(block.data(), &superblock, sizeof(superblock));
    pwrite_all(fd, block.data(), block.size(), 0, path);
    pwrite_all(fd, block.data(), block.size(), kSuperblockBytes, path);
}

void initialize_file(int fd, const std::string& path) {
    if (::ftruncate(fd, static_cast<off_t>(kDataOffset)) != 0) {
        throw io_error("ftruncate", path);
    }
    const Superblock superblock = make_superblock(
        1, 0, kDataOffset, 0, kDataOffset, 0, kDataOffset, 0);
    write_superblocks(fd, superblock, path);
    fsync_file(fd, path);
}

std::vector<uint8_t> serialize_payload(const std::vector<Mutation>& operations) {
    std::vector<uint8_t> payload;
    for (const auto& operation : operations) {
        validate_key_and_value(operation);
        const uint64_t addition = sizeof(MutationHeader) + operation.key.size() + operation.value.bytes.size();
        if (addition > kMaxTransactionBytes || payload.size() > kMaxTransactionBytes - addition) {
            throw KVStoreError("Transaction exceeds the storage format limit");
        }
        MutationHeader header {
            kMutationMagic,
            static_cast<uint8_t>(operation.type),
            {0, 0, 0},
            static_cast<uint32_t>(operation.key.size()),
            static_cast<uint32_t>(operation.value.bytes.size()),
            0,
        };
        header.checksum = mutation_checksum(header, operation.key, operation.value);
        append_object(payload, header);
        append_bytes(payload, operation.key.data(), operation.key.size());
        append_bytes(payload, operation.value.bytes.data(), operation.value.bytes.size());
    }
    return payload;
}

std::vector<uint8_t> serialize_frame(const std::vector<uint8_t>& payload,
                                     uint32_t operation_count,
                                     uint64_t lsn) {
    if (operation_count == 0 || lsn == 0 || payload.size() > kMaxTransactionBytes) {
        throw KVStoreError("Cannot serialize an empty or oversized storage transaction frame");
    }
    const uint64_t frame_bytes = sizeof(FrameHeader) + payload.size() + sizeof(FrameFooter);
    FrameHeader header {
        kFrameMagic,
        static_cast<uint16_t>(kFormatVersion),
        0,
        sizeof(FrameHeader),
        frame_bytes,
        lsn,
        operation_count,
        crc32c(payload.data(), payload.size()),
        0,
        0,
    };
    header.header_checksum = frame_header_checksum(header);
    FrameFooter footer {
        kFooterMagic,
        kFormatVersion,
        lsn,
        frame_bytes,
        header.payload_checksum,
        0,
    };
    footer.checksum = footer_checksum(footer);

    std::vector<uint8_t> frame;
    frame.reserve(static_cast<size_t>(frame_bytes));
    append_object(frame, header);
    append_bytes(frame, payload.data(), payload.size());
    append_object(frame, footer);
    return frame;
}

CheckpointImage build_checkpoint(const std::vector<Mutation>& entries,
                                 uint64_t generation,
                                 uint64_t checkpoint_lsn) {
    CheckpointImage image;
    image.index.resize(sizeof(IndexHeader));
    for (const auto& entry : entries) {
        validate_key_and_value(entry);
        if (entry.type != MutationType::kPut) {
            throw KVStoreError("Checkpoint image may contain only put entries");
        }
        IndexEntryHeader header = make_index_entry_header(
            entry.key,
            image.objects.size(),
            static_cast<uint32_t>(entry.value.bytes.size()),
            crc32c(entry.value.bytes.data(), entry.value.bytes.size()));
        append_object(image.index, header);
        append_bytes(image.index, entry.key.data(), entry.key.size());
        append_bytes(image.objects, entry.value.bytes.data(), entry.value.bytes.size());
    }

    IndexHeader index_header = make_index_header(
        entries.size(),
        image.index.size() - sizeof(IndexHeader),
        crc32c(image.index.data() + sizeof(IndexHeader), image.index.size() - sizeof(IndexHeader)));
    std::memcpy(image.index.data(), &index_header, sizeof(index_header));

    const uint64_t index_offset = kDataOffset;
    const uint64_t object_offset = index_offset + image.index.size();
    const uint64_t journal_offset = object_offset + image.objects.size();
    uint32_t checkpoint_checksum = crc32c(image.index.data(), image.index.size());
    checkpoint_checksum = crc32c_extend(checkpoint_checksum, image.objects.data(), image.objects.size());
    image.superblock = make_superblock(generation,
                                      checkpoint_lsn,
                                      index_offset,
                                      image.index.size(),
                                      object_offset,
                                      image.objects.size(),
                                      journal_offset,
                                      checkpoint_checksum);
    return image;
}

void write_checkpoint_image(int fd,
                            const CheckpointImage& image,
                            const std::string& path) {
    if (::ftruncate(fd, static_cast<off_t>(image.superblock.journal_offset)) != 0) {
        throw io_error("ftruncate", path);
    }
    if (!image.index.empty()) {
        pwrite_all(fd, image.index.data(), image.index.size(), image.superblock.index_offset, path);
    }
    if (!image.objects.empty()) {
        pwrite_all(fd, image.objects.data(), image.objects.size(), image.superblock.object_offset, path);
    }
    write_superblocks(fd, image.superblock, path);
}

RecoveryResult recover_file(int fd,
                            const std::string& path,
                            const ApplyCallback& apply,
                            bool repair_truncated_tail) {
    RecoveryResult result;
    uint64_t size = file_size(fd, path);
    result.superblock = read_best_superblock(fd, path, size, result.valid_superblocks);
    result.last_lsn = result.superblock.checkpoint_lsn;
    recover_checkpoint(fd, path, result.superblock, apply, result);

    uint64_t offset = result.superblock.journal_offset;
    while (offset < size) {
        const uint64_t remaining = size - offset;
        if (remaining < sizeof(FrameHeader)) {
            result.truncated_tail = true;
            break;
        }
        FrameHeader header {};
        pread_exact(fd, &header, sizeof(header), offset, path);
        if (header.magic != kFrameMagic || header.version != kFormatVersion ||
            header.header_bytes != sizeof(FrameHeader) ||
            header.header_checksum != frame_header_checksum(header) ||
            header.operation_count == 0 ||
            header.frame_bytes < sizeof(FrameHeader) + sizeof(MutationHeader) + sizeof(FrameFooter) ||
            header.frame_bytes > kMaxTransactionBytes + sizeof(FrameHeader) + sizeof(FrameFooter)) {
            throw KVStoreError("Invalid storage transaction frame header at offset " + std::to_string(offset));
        }
        if (header.frame_bytes > remaining) {
            result.truncated_tail = true;
            break;
        }
        if (header.lsn <= result.last_lsn) {
            throw KVStoreError("storage transaction LSN is not strictly increasing at offset " + std::to_string(offset));
        }

        const size_t payload_size = static_cast<size_t>(
            header.frame_bytes - sizeof(FrameHeader) - sizeof(FrameFooter));
        const uint64_t minimum_mutation_bytes = sizeof(MutationHeader) + 1;
        if (header.operation_count > payload_size / minimum_mutation_bytes) {
            throw KVStoreError("storage transaction operation count exceeds its payload at offset " +
                               std::to_string(offset));
        }
        std::vector<uint8_t> payload(payload_size);
        pread_exact(fd, payload.data(), payload.size(), offset + sizeof(FrameHeader), path);
        if (crc32c(payload.data(), payload.size()) != header.payload_checksum) {
            throw KVStoreError("storage transaction payload checksum mismatch at offset " + std::to_string(offset));
        }
        FrameFooter footer {};
        pread_exact(fd,
                    &footer,
                    sizeof(footer),
                    offset + sizeof(FrameHeader) + payload.size(),
                    path);
        if (footer.magic != kFooterMagic || footer.version != kFormatVersion ||
            footer.lsn != header.lsn || footer.frame_bytes != header.frame_bytes ||
            footer.payload_checksum != header.payload_checksum ||
            footer.checksum != footer_checksum(footer)) {
            throw KVStoreError("Invalid storage transaction commit footer at offset " + std::to_string(offset));
        }

        auto operations = parse_payload(payload.data(), payload.size(), header.operation_count, path);
        for (auto& operation : operations) {
            if (operation.has_backing) {
                operation.value_offset += offset + sizeof(FrameHeader);
            }
            classify_key(operation.key, result);
            if (operation.type == MutationType::kPut) {
                ++result.put_operations;
            } else {
                ++result.delete_operations;
            }
            if (apply) {
                apply(operation, header.lsn);
            }
        }
        ++result.journal_frames;
        result.journal_operations += operations.size();
        result.last_lsn = header.lsn;
        offset += header.frame_bytes;
    }

    result.append_offset = offset;
    if (result.truncated_tail && repair_truncated_tail) {
        if (::ftruncate(fd, static_cast<off_t>(offset)) != 0) {
            throw io_error("truncate incomplete storage journal tail", path);
        }
        size = offset;
    }
    (void)size;
    return result;
}

RecoveryResult inspect_file(const std::string& path) {
    const int fd = open_or_throw(path, O_RDONLY | O_CLOEXEC);
    try {
        RecoveryResult result = recover_file(fd, path, {}, false);
        ::close(fd);
        return result;
    } catch (...) {
        ::close(fd);
        throw;
    }
}

}  // namespace kvstore::internal
