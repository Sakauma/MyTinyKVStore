#ifndef KVSTORE_INTERNAL_IO_H
#define KVSTORE_INTERNAL_IO_H

#include "kvstore.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <sys/types.h>

namespace kvstore::internal {

KVStoreError io_error(const std::string& action, const std::string& path);
void fsync_file(int fd, const std::string& path);
void fsync_directory(const std::string& path);
int open_or_throw(const std::string& path, int flags, mode_t mode = 0644);
void close_if_open(int fd);
bool failpoint_is_configured(const char* name);
// Internal synchronization hook for deterministic tests; not part of the public API.
using FailpointTestCallback = std::function<void(const char*)>;
void install_failpoint_test_callback(FailpointTestCallback callback);
void clear_failpoint_test_callback();
void maybe_trigger_failpoint(const char* name);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_IO_H
