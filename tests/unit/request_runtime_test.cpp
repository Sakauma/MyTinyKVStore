#include "tests/unit/test_registry.h"

#include "internal/request_runtime.h"

#include <atomic>
#include <thread>

namespace kvstore::tests::unit {
namespace {

using namespace kvstore::internal;
using test_support::require;
using test_support::wait_until;

void test_enqueue_and_wait_completes() {
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    RequestQueue queue;
    std::atomic<bool> has_fatal_error {false};
    std::string fatal_error_message;
    std::atomic<uint64_t> max_pending_queue_depth {0};
    std::atomic<uint64_t> compact_requests {0};
    std::atomic<uint64_t> enqueued_write_requests {0};
    RequestRuntimeState state {
        queue_mutex,
        queue_cv,
        queue,
        has_fatal_error,
        fatal_error_message,
        max_pending_queue_depth,
        compact_requests,
        enqueued_write_requests,
    };

    auto request = std::make_shared<Request>();
    request->type = RequestType::kPut;
    request->key = "alpha";
    request->value = test_support::text("one");

    std::atomic<bool> completed {false};
    std::thread waiter([&]() {
        enqueue_and_wait(state, request);
        completed.store(true, std::memory_order_release);
    });

    wait_until([&]() {
        std::lock_guard<std::mutex> lock(queue_mutex);
        return queue.size() == 1;
    }, "enqueue_and_wait should place the request in the queue");
    complete_request(request, "");
    waiter.join();

    require(completed.load(std::memory_order_acquire), "enqueue_and_wait should return after completion");
    require(enqueued_write_requests.load(std::memory_order_relaxed) == 1,
            "enqueue_and_wait should increment the enqueued-write metric");
    require(max_pending_queue_depth.load(std::memory_order_relaxed) == 1,
            "enqueue_and_wait should track max queue depth");
}

void test_enter_fatal_state_propagates_to_pending_requests() {
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    RequestQueue queue;
    std::atomic<bool> has_fatal_error {false};
    std::string fatal_error_message;
    std::atomic<uint64_t> max_pending_queue_depth {0};
    std::atomic<uint64_t> compact_requests {0};
    std::atomic<uint64_t> enqueued_write_requests {0};
    RequestRuntimeState state {
        queue_mutex,
        queue_cv,
        queue,
        has_fatal_error,
        fatal_error_message,
        max_pending_queue_depth,
        compact_requests,
        enqueued_write_requests,
    };

    auto pending = std::make_shared<Request>();
    pending->type = RequestType::kDelete;
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        queue.push_back(pending);
    }

    enter_fatal_state(state, "fatal");

    require(has_fatal_error.load(std::memory_order_acquire), "fatal state should be published");
    require(fatal_error_message == "fatal", "fatal error text should be retained");
    require(pending->done, "pending requests should be completed during fatal-state transition");
    require(pending->error == "fatal", "pending requests should carry the fatal error");

    bool threw = false;
    try {
        throw_if_fatal(state);
    } catch (const KVStoreError& error) {
        threw = true;
        require(std::string(error.what()) == "fatal", "throw_if_fatal should surface the stored fatal message");
    }
    require(threw, "throw_if_fatal should reject new work after fatal state");
}

}  // namespace

void register_request_runtime_tests(TestCases& tests) {
    tests.push_back({"request runtime enqueue_and_wait completes", test_enqueue_and_wait_completes});
    tests.push_back({"request runtime fatal state propagates", test_enter_fatal_state_propagates_to_pending_requests});
}

}  // namespace kvstore::tests::unit
