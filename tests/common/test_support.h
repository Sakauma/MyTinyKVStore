#ifndef KVSTORE_TEST_SUPPORT_H
#define KVSTORE_TEST_SUPPORT_H

#include "kvstore.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace test_support {

struct NamedTest {
    std::string name;
    std::function<void()> fn;
};

Value text(const std::string& input);
std::string as_string(const Value& value);
void require(bool condition, const std::string& message);

class TestDir {
public:
    explicit TestDir(const std::string& name);
    ~TestDir();

    std::string file(const std::string& name) const;

private:
    std::filesystem::path path_;
};

void append_bytes(const std::string& path, std::initializer_list<uint8_t> data);
uintmax_t file_size_or_zero(const std::string& path);
void wait_for_start(std::atomic<int>& ready, std::atomic<bool>& start_signal, int target_count);
int run_named_tests(const std::vector<NamedTest>& tests);

template <typename Predicate>
void wait_until(Predicate predicate, const std::string& failure_message, int timeout_ms = 2000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!predicate()) {
        throw std::runtime_error(failure_message);
    }
}

}  // namespace test_support

#endif  // KVSTORE_TEST_SUPPORT_H
