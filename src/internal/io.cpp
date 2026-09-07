#include "io.h"

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore::internal {
namespace {

std::atomic<bool> failpoint_test_callback_enabled {false};
std::shared_ptr<const FailpointTestCallback> failpoint_test_callback;
std::mutex failpoint_test_callback_control_mutex;

}  // namespace

KVStoreError io_error(const std::string& action, const std::string& path) {
    return KVStoreError(action + " failed for " + path + ": " + std::strerror(errno));
}

void fsync_file(int fd, const std::string& path) {
    int result;
    do {
        result = ::fsync(fd);
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
        throw io_error("fsync", path);
    }
}

void fsync_directory(const std::string& path) {
    const std::filesystem::path file_path(path);
    const std::filesystem::path parent =
        file_path.has_parent_path() ? file_path.parent_path() : std::filesystem::current_path();
    int dir_fd;
    do {
        dir_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    } while (dir_fd < 0 && errno == EINTR);
    if (dir_fd < 0) {
        throw io_error("open directory", parent.string());
    }
    int sync_result;
    do {
        sync_result = ::fsync(dir_fd);
    } while (sync_result != 0 && errno == EINTR);
    if (sync_result != 0) {
        const int saved_errno = errno;
        ::close(dir_fd);
        errno = saved_errno;
        throw io_error("fsync directory", parent.string());
    }
    ::close(dir_fd);
}

int open_or_throw(const std::string& path, int flags, mode_t mode) {
    int fd;
    do {
        fd = ::open(path.c_str(), flags, mode);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        throw io_error("open", path);
    }
    return fd;
}

void close_if_open(int fd) {
    if (fd >= 0) {
        ::close(fd);
    }
}

bool failpoint_is_configured(const char* name) {
    const char* configured = std::getenv("KVSTORE_FAILPOINT");
    return configured != nullptr && std::strcmp(configured, name) == 0;
}

void install_failpoint_test_callback(FailpointTestCallback callback) {
    auto installed = std::make_shared<const FailpointTestCallback>(std::move(callback));
    std::lock_guard<std::mutex> lock(failpoint_test_callback_control_mutex);
    std::atomic_store_explicit(
        &failpoint_test_callback, std::move(installed), std::memory_order_release);
    failpoint_test_callback_enabled.store(true, std::memory_order_release);
}

void clear_failpoint_test_callback() {
    std::lock_guard<std::mutex> lock(failpoint_test_callback_control_mutex);
    failpoint_test_callback_enabled.store(false, std::memory_order_release);
    std::atomic_store_explicit(
        &failpoint_test_callback,
        std::shared_ptr<const FailpointTestCallback> {},
        std::memory_order_release);
}

void maybe_trigger_failpoint(const char* name) {
    if (failpoint_test_callback_enabled.load(std::memory_order_acquire)) {
        const std::shared_ptr<const FailpointTestCallback> callback =
            std::atomic_load_explicit(
                &failpoint_test_callback, std::memory_order_acquire);
        if (callback) {
            (*callback)(name);
        }
    }
    if (!failpoint_is_configured(name)) {
        return;
    }

    const char* action = std::getenv("KVSTORE_FAIL_ACTION");
    if (action != nullptr && std::strcmp(action, "throw") == 0) {
        throw KVStoreError(std::string("Injected failpoint: ") + name);
    }
    if (action != nullptr && std::strcmp(action, "enospc") == 0) {
        errno = ENOSPC;
        throw io_error(std::string("Injected I/O failure at ") + name, "database");
    }

    ::_exit(86);
}

}  // namespace kvstore::internal
