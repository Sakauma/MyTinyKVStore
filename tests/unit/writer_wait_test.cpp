#include "tests/unit/test_registry.h"

#include "internal/writer_wait.h"

#include <atomic>
#include <thread>

namespace kvstore::tests::unit {
namespace {

using namespace kvstore::internal;
using test_support::require;

void test_wait_for_queue_activity_records_metrics() {
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic<uint64_t> wait_events {0};
    std::atomic<uint64_t> wait_time_us {0};
    bool ready = false;

    std::thread notifier([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        {
            std::lock_guard<std::mutex> lock(mutex);
            ready = true;
        }
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lock(mutex);
    wait_for_queue_activity(cv, wait_events, wait_time_us, lock, [&]() { return ready; });
    lock.unlock();
    notifier.join();

    require(wait_events.load(std::memory_order_relaxed) == 1, "writer wait should count wait events");
    require(wait_time_us.load(std::memory_order_relaxed) > 0, "writer wait should accumulate wait time");
}

void test_wait_for_queue_activity_until_reports_timeout() {
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic<uint64_t> wait_events {0};
    std::atomic<uint64_t> wait_time_us {0};
    bool ready = false;

    std::unique_lock<std::mutex> lock(mutex);
    const bool awakened = wait_for_queue_activity_until(
        cv,
        wait_events,
        wait_time_us,
        lock,
        std::chrono::steady_clock::now() + std::chrono::milliseconds(10),
        [&]() { return ready; });
    lock.unlock();

    require(!awakened, "writer wait-until should report timeout when no queue activity occurs");
    require(wait_events.load(std::memory_order_relaxed) == 1, "writer wait-until should count wait events");
    require(wait_time_us.load(std::memory_order_relaxed) > 0, "writer wait-until should accumulate wait time");
}

}  // namespace

void register_writer_wait_tests(TestCases& tests) {
    tests.push_back({"writer wait records metrics", test_wait_for_queue_activity_records_metrics});
    tests.push_back({"writer wait-until reports timeout", test_wait_for_queue_activity_until_reports_timeout});
}

}  // namespace kvstore::tests::unit
