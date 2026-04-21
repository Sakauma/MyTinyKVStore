#include "tests/common/test_support.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>
#include <unistd.h>

namespace test_support {

Value text(const std::string& input) {
    return Value(std::vector<uint8_t>(input.begin(), input.end()));
}

std::string as_string(const Value& value) {
    return std::string(value.bytes.begin(), value.bytes.end());
}

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

TestDir::TestDir(const std::string& name) {
    static std::atomic<uint64_t> counter {0};
    const uint64_t suffix = counter.fetch_add(1, std::memory_order_relaxed);
    const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    path_ = std::filesystem::temp_directory_path() /
            ("kvstore_" + name + "_" + std::to_string(::getpid()) + "_" +
             std::to_string(now_ns) + "_" + std::to_string(suffix));
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
}

TestDir::~TestDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
}

std::string TestDir::file(const std::string& name) const {
    return (path_ / name).string();
}

void append_bytes(const std::string& path, std::initializer_list<uint8_t> data) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    for (uint8_t byte : data) {
        out.put(static_cast<char>(byte));
    }
}

uintmax_t file_size_or_zero(const std::string& path) {
    std::error_code ec;
    const auto size = std::filesystem::file_size(path, ec);
    return ec ? 0 : size;
}

void wait_for_start(std::atomic<int>& ready, std::atomic<bool>& start_signal, int target_count) {
    ready.fetch_add(1, std::memory_order_relaxed);
    while (ready.load(std::memory_order_acquire) < target_count) {
        std::this_thread::yield();
    }
    while (!start_signal.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

int run_named_tests(const std::vector<NamedTest>& tests) {
    try {
        for (const auto& test : tests) {
            test.fn();
            std::cout << "[unit] PASS " << test.name << '\n';
        }
    } catch (const std::exception& error) {
        std::cerr << "[unit] FAIL " << error.what() << '\n';
        return 1;
    }
    return 0;
}

}  // namespace test_support
