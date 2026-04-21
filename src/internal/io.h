#ifndef KVSTORE_INTERNAL_IO_H
#define KVSTORE_INTERNAL_IO_H

#include "kvstore.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <sys/types.h>

namespace kvstore::internal {

KVStoreError io_error(const std::string& action, const std::string& path);
ssize_t read_once(int fd, void* buffer, size_t size);
size_t read_up_to(int fd, void* buffer, size_t size);
void write_all(int fd, const void* buffer, size_t size);
void fsync_file(int fd, const std::string& path);
void fsync_directory(const std::string& path);
int open_or_throw(const std::string& path, int flags, mode_t mode = 0644);
void close_if_open(int fd);
void maybe_trigger_failpoint(const char* name);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_IO_H
