#ifndef KVSTORE_INTERNAL_REQUEST_RUNTIME_H
#define KVSTORE_INTERNAL_REQUEST_RUNTIME_H

#include "kvstore.h"
#include "format.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace kvstore::internal {

struct BatchMutation {
    WalRecordType type;
    std::string key;
    Value value;
};

enum class RequestType {
    kPut,
    kDelete,
    kBatch,
    kCompact,
};

struct Request {
    RequestType type {};
    std::string key;
    Value value;
    std::vector<BatchMutation> batch_operations;
    std::chrono::steady_clock::time_point enqueue_time = std::chrono::steady_clock::now();
    std::mutex done_mutex;
    std::condition_variable done_cv;
    bool done = false;
    std::string error;
};

using RequestPtr = std::shared_ptr<Request>;
using RequestQueue = std::deque<RequestPtr>;

struct RequestCompletionCallbacks {
    std::function<void(const Request&)> on_successful_write;
};

struct RequestRuntimeState {
    std::mutex& queue_mutex;
    std::condition_variable& queue_cv;
    RequestQueue& request_queue;
    std::atomic<bool>& has_fatal_error;
    std::string& fatal_error_message;
    std::atomic<uint64_t>& max_pending_queue_depth;
    std::atomic<uint64_t>& compact_requests;
    std::atomic<uint64_t>& enqueued_write_requests;
};

void throw_if_fatal(const RequestRuntimeState& state);
void throw_if_fatal_locked(const RequestRuntimeState& state);
void enqueue_and_wait(RequestRuntimeState state, const RequestPtr& request);
void complete_request(
    const RequestPtr& request,
    const std::string& error,
    const RequestCompletionCallbacks& callbacks = {});
void fail_requests(
    const std::vector<RequestPtr>& requests,
    const std::string& error,
    const RequestCompletionCallbacks& callbacks = {});
void enter_fatal_state(
    RequestRuntimeState state,
    const std::string& error,
    const RequestCompletionCallbacks& callbacks = {});

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_REQUEST_RUNTIME_H
