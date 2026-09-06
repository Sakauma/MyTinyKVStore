#include "tests/common/test_support.h"

#include <cstdlib>
#include <cstring>
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

TestDir::~TestDir() noexcept {
    try {
        const char* keep_artifacts = std::getenv("KVSTORE_KEEP_TEST_ARTIFACTS");
        if (keep_artifacts != nullptr && std::strcmp(keep_artifacts, "1") == 0) {
            try {
                std::cerr << "[test] kept artifacts: " << path_.native() << '\n';
            } catch (...) {
            }
            return;
        }
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    } catch (...) {
    }
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

int list_named_tests(const std::vector<NamedTest>& tests, const std::string& filter) {
    size_t selected = 0;
    for (const auto& test : tests) {
        if (!filter.empty() && test.name.find(filter) == std::string::npos) {
            continue;
        }
        std::cout << test.name << '\n';
        ++selected;
    }
    if (selected == 0) {
        std::cerr << "No tests matched filter: " << filter << '\n';
        return 2;
    }
    return 0;
}

int run_named_tests(const std::vector<NamedTest>& tests,
                    const std::string& filter,
                    const std::string& suite_name) {
    size_t selected = 0;
    size_t passed = 0;
    size_t failed = 0;
    for (const auto& test : tests) {
        if (!filter.empty() && test.name.find(filter) == std::string::npos) {
            continue;
        }
        ++selected;
        try {
            test.fn();
            ++passed;
            std::cout << "[PASS] " << test.name << '\n';
        } catch (const std::exception& error) {
            ++failed;
            std::cerr << "[FAIL] " << test.name << ": " << error.what() << '\n';
        } catch (...) {
            ++failed;
            std::cerr << "[FAIL] " << test.name << ": unknown exception\n";
        }
    }

    if (selected == 0) {
        std::cerr << "No tests matched filter: " << filter << '\n';
        return 2;
    }

    if (!suite_name.empty()) {
        std::cout << suite_name << ": ";
    }
    std::cout << passed << " passed, " << failed << " failed, "
              << selected << " selected." << std::endl;
    return failed == 0 ? 0 : 1;
}

}  // namespace test_support
