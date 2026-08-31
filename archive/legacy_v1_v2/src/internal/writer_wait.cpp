#include "writer_wait.h"

namespace kvstore::internal {

void record_writer_wait(
    std::atomic<uint64_t>& writer_wait_time_us,
    std::chrono::steady_clock::duration wait_duration) {
    const auto waited_us = std::chrono::duration_cast<std::chrono::microseconds>(wait_duration).count();
    if (waited_us > 0) {
        writer_wait_time_us.fetch_add(static_cast<uint64_t>(waited_us), std::memory_order_relaxed);
    }
}

void wait_for_queue_activity(
    std::condition_variable& queue_cv,
    std::atomic<uint64_t>& writer_wait_events,
    std::atomic<uint64_t>& writer_wait_time_us,
    std::unique_lock<std::mutex>& lock,
    const std::function<bool()>& predicate) {
    const auto wait_begin = std::chrono::steady_clock::now();
    writer_wait_events.fetch_add(1, std::memory_order_relaxed);
    queue_cv.wait(lock, predicate);
    record_writer_wait(writer_wait_time_us, std::chrono::steady_clock::now() - wait_begin);
}

bool wait_for_queue_activity_until(
    std::condition_variable& queue_cv,
    std::atomic<uint64_t>& writer_wait_events,
    std::atomic<uint64_t>& writer_wait_time_us,
    std::unique_lock<std::mutex>& lock,
    const std::chrono::steady_clock::time_point& deadline,
    const std::function<bool()>& predicate) {
    const auto wait_begin = std::chrono::steady_clock::now();
    writer_wait_events.fetch_add(1, std::memory_order_relaxed);
    const bool awakened = queue_cv.wait_until(lock, deadline, predicate);
    record_writer_wait(writer_wait_time_us, std::chrono::steady_clock::now() - wait_begin);
    return awakened;
}

}  // namespace kvstore::internal
