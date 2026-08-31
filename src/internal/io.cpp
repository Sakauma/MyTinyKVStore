#include "io.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>

#include <fcntl.h>
#include <unistd.h>

namespace kvstore::internal {

KVStoreError io_error(const std::string& action, const std::string& path) {
    return KVStoreError(action + " failed for " + path + ": " + std::strerror(errno));
}

ssize_t read_once(int fd, void* buffer, size_t size) {
    while (true) {
        const ssize_t nread = ::read(fd, buffer, size);
        if (nread < 0 && errno == EINTR) {
            continue;
        }
        return nread;
    }
}

size_t read_up_to(int fd, void* buffer, size_t size) {
    auto* cursor = static_cast<uint8_t*>(buffer);
    size_t total = 0;
    while (total < size) {
        const ssize_t nread = read_once(fd, cursor + total, size - total);
        if (nread < 0) {
            throw KVStoreError("Read failed: " + std::string(std::strerror(errno)));
        }
        if (nread == 0) {
            break;
        }
        total += static_cast<size_t>(nread);
    }
    return total;
}

void write_all(int fd, const void* buffer, size_t size) {
    const auto* cursor = static_cast<const uint8_t*>(buffer);
    size_t total = 0;
    while (total < size) {
        ssize_t nwritten = ::write(fd, cursor + total, size - total);
        if (nwritten < 0 && errno == EINTR) {
            continue;
        }
        if (nwritten < 0) {
            throw KVStoreError("Write failed: " + std::string(std::strerror(errno)));
        }
        if (nwritten == 0) {
            throw KVStoreError("Write made no progress");
        }
        total += static_cast<size_t>(nwritten);
    }
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

void maybe_trigger_failpoint(const char* name) {
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
