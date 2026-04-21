#ifndef KVSTORE_INTERNAL_WRITER_WAIT_H
#define KVSTORE_INTERNAL_WRITER_WAIT_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>

namespace kvstore::internal {

void record_writer_wait(
    std::atomic<uint64_t>& writer_wait_time_us,
    std::chrono::steady_clock::duration wait_duration);

void wait_for_queue_activity(
    std::condition_variable& queue_cv,
    std::atomic<uint64_t>& writer_wait_events,
    std::atomic<uint64_t>& writer_wait_time_us,
    std::unique_lock<std::mutex>& lock,
    const std::function<bool()>& predicate);

bool wait_for_queue_activity_until(
    std::condition_variable& queue_cv,
    std::atomic<uint64_t>& writer_wait_events,
    std::atomic<uint64_t>& writer_wait_time_us,
    std::unique_lock<std::mutex>& lock,
    const std::chrono::steady_clock::time_point& deadline,
    const std::function<bool()>& predicate);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_WRITER_WAIT_H
