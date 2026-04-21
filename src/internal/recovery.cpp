#include "recovery.h"

#include "format.h"
#include "io.h"

#include <cstring>
#include <filesystem>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace kvstore::internal {

void ensure_snapshot_file_exists(const std::string& db_file_path) {
    if (std::filesystem::exists(db_file_path)) {
        return;
    }

    const int fd = open_or_throw(db_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    const SnapshotHeader header = make_snapshot_header();
    write_all(fd, &header, sizeof(header));
    fsync_file(fd, db_file_path);
    ::close(fd);
    fsync_directory(db_file_path);
}

void load_snapshot_into_state(const std::string& db_file_path, StateMap& state) {
    state.clear();
    const int fd = open_or_throw(db_file_path, O_RDONLY);
    SnapshotHeader header {};
    const size_t header_bytes = read_up_to(fd, &header, sizeof(header));
    if (header_bytes != sizeof(header)) {
        ::close(fd);
        throw KVStoreError("Snapshot header is truncated: " + db_file_path);
    }
    if (std::memcmp(header.magic, kSnapshotMagic, sizeof(kSnapshotMagic)) != 0) {
        ::close(fd);
        throw KVStoreError("Snapshot magic mismatch: " + db_file_path);
    }
    if (header.version != 1 && header.version != kSnapshotVersion) {
        ::close(fd);
        throw KVStoreError("Unsupported snapshot version: " + std::to_string(header.version));
    }

    while (true) {
        SnapshotEntryHeader entry {};
        const size_t entry_bytes = read_up_to(fd, &entry, sizeof(entry));
        if (entry_bytes == 0) {
            break;
        }
        if (entry_bytes != sizeof(entry)) {
            ::close(fd);
            throw KVStoreError("Snapshot entry header is truncated: " + db_file_path);
        }
        if (entry.magic != kSnapshotEntryMagic) {
            ::close(fd);
            throw KVStoreError("Snapshot entry magic mismatch: " + db_file_path);
        }

        std::string key;
        const uint32_t value_size = entry.value_size;
        if (header.version == 1) {
            key = encode_int_key(static_cast<int32_t>(entry.key_size));
        } else {
            key.resize(entry.key_size);
            if (!key.empty()) {
                const size_t key_bytes = read_up_to(fd, key.data(), key.size());
                if (key_bytes != key.size()) {
                    ::close(fd);
                    throw KVStoreError("Snapshot key is truncated: " + db_file_path);
                }
            }
        }

        std::vector<uint8_t> data(value_size);
        if (!data.empty()) {
            const size_t value_bytes = read_up_to(fd, data.data(), data.size());
            if (value_bytes != data.size()) {
                ::close(fd);
                throw KVStoreError("Snapshot value is truncated: " + db_file_path);
            }
        }
        state[key] = Value(std::move(data));
    }

    ::close(fd);
}

void replay_wal_into_state(
    const std::string& wal_file_path,
    StateMap& state,
    const WalReplayCallback& note_latest_wal_record) {
    const int fd = open_or_throw(wal_file_path, O_RDONLY | O_CREAT, 0644);
    struct stat st {};
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        throw io_error("fstat", wal_file_path);
    }

    const off_t file_size = st.st_size;
    off_t offset = 0;
    while (offset < file_size) {
        if (file_size - offset < static_cast<off_t>(sizeof(WalRecordHeader))) {
            break;
        }

        WalRecordHeader header {};
        const size_t header_bytes = read_up_to(fd, &header, sizeof(header));
        if (header_bytes != sizeof(header)) {
            ::close(fd);
            throw KVStoreError("WAL header truncated before EOF: " + wal_file_path);
        }
        offset += sizeof(header);

        if (header.magic != kWalMagic) {
            ::close(fd);
            throw KVStoreError("WAL magic mismatch at offset " + std::to_string(offset - sizeof(header)));
        }
        if (header.version != 1 && header.version != kWalVersion) {
            ::close(fd);
            throw KVStoreError("Unsupported WAL version: " + std::to_string(header.version));
        }
        if (header.type != static_cast<uint8_t>(WalRecordType::kPut) &&
            header.type != static_cast<uint8_t>(WalRecordType::kDelete)) {
            ::close(fd);
            throw KVStoreError("Unknown WAL record type at offset " + std::to_string(offset - sizeof(header)));
        }
        if (header.type == static_cast<uint8_t>(WalRecordType::kDelete) && header.value_size != 0) {
            ::close(fd);
            throw KVStoreError("Delete WAL record has payload at offset " + std::to_string(offset - sizeof(header)));
        }

        std::string key;
        int32_t legacy_key = 0;
        if (header.version == 1) {
            legacy_key = static_cast<int32_t>(header.key_size);
            key = encode_int_key(legacy_key);
            if (offset + header.value_size > file_size) {
                break;
            }
        } else {
            if (offset + header.key_size + header.value_size > file_size) {
                break;
            }
            key.resize(header.key_size);
            if (!key.empty()) {
                const size_t key_bytes = read_up_to(fd, key.data(), key.size());
                if (key_bytes != key.size()) {
                    ::close(fd);
                    throw KVStoreError("WAL key truncated before EOF: " + wal_file_path);
                }
            }
            offset += header.key_size;
        }

        Value value(std::vector<uint8_t>(header.value_size));
        if (!value.bytes.empty()) {
            const size_t payload_bytes = read_up_to(fd, value.bytes.data(), value.bytes.size());
            if (payload_bytes != value.bytes.size()) {
                ::close(fd);
                throw KVStoreError("WAL payload truncated before EOF: " + wal_file_path);
            }
        }
        offset += header.value_size;

        const auto type = static_cast<WalRecordType>(header.type);
        const uint32_t expected_checksum =
            header.version == 1 ? checksum_record_v1(type, legacy_key, value)
                                : checksum_record(type, key, value);
        if (expected_checksum != header.checksum) {
            ::close(fd);
            throw KVStoreError("WAL checksum mismatch at offset " + std::to_string(offset - sizeof(header) - header.value_size));
        }

        if (type == WalRecordType::kPut) {
            state[key] = std::move(value);
        } else {
            state.erase(key);
        }

        if (note_latest_wal_record) {
            note_latest_wal_record(key, sizeof(WalRecordHeader) + key.size() + header.value_size);
        }
    }

    ::close(fd);
}

}  // namespace kvstore::internal
