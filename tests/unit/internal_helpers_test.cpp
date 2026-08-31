#include "tests/unit/test_registry.h"

#include "internal/io.h"
#include "internal/key_codec.h"
#include "internal/metrics_helpers.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <pthread.h>
#include <poll.h>
#include <unistd.h>

namespace kvstore::tests::unit {
namespace {

using test_support::require;
volatile std::sig_atomic_t g_interrupt_ack_fd = -1;

void record_interrupt(int) {
    const int saved_errno = errno;
    const char marker = 's';
    if (g_interrupt_ack_fd >= 0) {
        ssize_t result;
        do {
            result = ::write(g_interrupt_ack_fd, &marker, 1);
        } while (result < 0 && errno == EINTR);
    }
    errno = saved_errno;
}

void test_internal_format_helpers_round_trip_keys() {
    const std::string int_key = kvstore::internal::encode_int_key(42);
    const std::string string_key = kvstore::internal::encode_string_key("alpha");
    const std::string binary_key =
        kvstore::internal::encode_binary_key(std::vector<uint8_t> {0x00, 0x7F, 0xFF});

    require(!int_key.empty() && int_key.front() == kvstore::internal::kIntKeyTag,
            "encoded int keys should carry the int namespace tag");
    require(kvstore::internal::is_string_key(string_key),
            "encoded string keys should be recognized as string keys");
    require(kvstore::internal::decode_string_key(string_key) == "alpha",
            "string key helpers should round-trip the original string key");
    require(!binary_key.empty() && binary_key.front() == kvstore::internal::kBinaryKeyTag,
            "encoded binary keys should carry the binary namespace tag");
}

void test_internal_metrics_helpers_compute_percentiles_and_ratios() {
    std::array<uint64_t, kWriteLatencyBucketCount> histogram {};
    histogram[0] = 1;
    histogram[4] = 2;
    histogram[7] = 1;

    require(kvstore::internal::approximate_latency_percentile_us(histogram, 1, 2) == 1000,
            "p50 helper should map into the first bucket that crosses the 50th percentile");
    require(kvstore::internal::approximate_latency_percentile_us(histogram, 95, 100) == 10000,
            "p95 helper should map into the tail bucket that crosses the percentile");
    require(kvstore::internal::capped_ratio_milli(16, 4) == 4000,
            "ratio helper should cap values at 4x");
    require(kvstore::internal::weighted_signal_score(200, 100, 3) == 6000,
            "weighted signal score should scale the capped ratio by the given weight");
    require(kvstore::internal::weighted_deficit_score(4, 8, 2) == 1000,
            "weighted deficit score should reflect the observed gap to target");
}

void test_io_read_once_retries_after_eintr() {
    int pipe_fds[2] {-1, -1};
    int interrupt_ack_fds[2] {-1, -1};
    require(::pipe(pipe_fds) == 0, "pipe should be available for the EINTR test");
    require(::pipe(interrupt_ack_fds) == 0,
            "interrupt acknowledgement pipe should be available for the EINTR test");

    struct sigaction action {};
    struct sigaction previous {};
    action.sa_handler = record_interrupt;
    ::sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    require(::sigaction(SIGUSR1, &action, &previous) == 0,
            "SIGUSR1 handler should be installed without SA_RESTART");

    g_interrupt_ack_fd = interrupt_ack_fds[1];
    std::atomic<bool> entered_read {false};
    ssize_t read_result = -2;
    char received = 0;
    std::thread reader([&] {
        entered_read.store(true, std::memory_order_release);
        read_result = kvstore::internal::read_once(pipe_fds[0], &received, 1);
    });

    while (!entered_read.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const int signal_result = ::pthread_kill(reader.native_handle(), SIGUSR1);
    struct pollfd acknowledgement_poll {interrupt_ack_fds[0], POLLIN, 0};
    int poll_result;
    do {
        poll_result = ::poll(&acknowledgement_poll, 1, 1000);
    } while (poll_result < 0 && errno == EINTR);
    char acknowledgement = 0;
    const ssize_t acknowledgement_result =
        poll_result > 0 ? ::read(interrupt_ack_fds[0], &acknowledgement, 1) : -1;

    const char expected = 'x';
    const ssize_t write_result = ::write(pipe_fds[1], &expected, 1);
    reader.join();
    (void)::sigaction(SIGUSR1, &previous, nullptr);
    g_interrupt_ack_fd = -1;
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
    ::close(interrupt_ack_fds[0]);
    ::close(interrupt_ack_fds[1]);

    require(signal_result == 0 && acknowledgement_result == 1 && acknowledgement == 's',
            "the blocking read should receive the interrupt signal");
    require(write_result == 1, "the EINTR test should write its completion byte");
    require(read_result == 1 && received == expected,
            "read_once should retry EINTR and return the subsequent byte");
}

}  // namespace

void register_internal_helpers_tests(TestCases& tests) {
    tests.push_back({"internal format helpers round trip keys", test_internal_format_helpers_round_trip_keys});
    tests.push_back({"internal metrics helpers compute percentiles and ratios",
                     test_internal_metrics_helpers_compute_percentiles_and_ratios});
    tests.push_back({"internal I/O retries EINTR", test_io_read_once_retries_after_eintr});
}

}  // namespace kvstore::tests::unit
