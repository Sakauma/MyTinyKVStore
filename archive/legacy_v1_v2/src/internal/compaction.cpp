#include "compaction.h"

#include "format.h"
#include "io.h"

#include <fcntl.h>
#include <unistd.h>

namespace kvstore::internal {

uint64_t rewrite_snapshot_and_reset_wal(
    const std::string& db_file_path,
    const std::string& wal_file_path,
    const std::map<std::string, Value>& snapshot_state) {
    const std::string temp_snapshot_path = db_file_path + ".compact.tmp";
    const std::string temp_wal_path = wal_file_path + ".tmp";

    uint64_t snapshot_bytes_written = sizeof(SnapshotHeader);
    for (const auto& [key, value] : snapshot_state) {
        snapshot_bytes_written += sizeof(SnapshotEntryHeader) + key.size() + value.bytes.size();
    }

    int snapshot_fd = -1;
    int empty_wal_fd = -1;
    try {
        snapshot_fd = open_or_throw(temp_snapshot_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        const SnapshotHeader snapshot_header = make_snapshot_header();
        write_all(snapshot_fd, &snapshot_header, sizeof(snapshot_header));
        for (const auto& [key, value] : snapshot_state) {
            const SnapshotEntryHeader entry = {
                kSnapshotEntryMagic,
                static_cast<uint32_t>(key.size()),
                static_cast<uint32_t>(value.bytes.size()),
            };
            write_all(snapshot_fd, &entry, sizeof(entry));
            if (!key.empty()) {
                write_all(snapshot_fd, key.data(), key.size());
            }
            if (!value.bytes.empty()) {
                write_all(snapshot_fd, value.bytes.data(), value.bytes.size());
            }
        }
        fsync_file(snapshot_fd, temp_snapshot_path);
        maybe_trigger_failpoint("after_snapshot_fsync_before_rename");
        ::close(snapshot_fd);
        snapshot_fd = -1;

        if (::rename(temp_snapshot_path.c_str(), db_file_path.c_str()) != 0) {
            throw io_error("rename", temp_snapshot_path);
        }
        fsync_directory(db_file_path);
        maybe_trigger_failpoint("after_snapshot_rename_before_wal_reset");

        empty_wal_fd = open_or_throw(temp_wal_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        fsync_file(empty_wal_fd, temp_wal_path);
        ::close(empty_wal_fd);
        empty_wal_fd = -1;

        if (::rename(temp_wal_path.c_str(), wal_file_path.c_str()) != 0) {
            throw io_error("rename", temp_wal_path);
        }
        fsync_directory(wal_file_path);
        maybe_trigger_failpoint("after_wal_rotation_before_reopen");
    } catch (...) {
        close_if_open(snapshot_fd);
        close_if_open(empty_wal_fd);
        throw;
    }

    return snapshot_bytes_written;
}

}  // namespace kvstore::internal
