#include "request_runtime.h"

#include "metrics_helpers.h"

namespace kvstore::internal {

void throw_if_fatal(const RequestRuntimeState& state) {
    if (!state.has_fatal_error.load(std::memory_order_acquire)) {
        return;
    }
    std::lock_guard<std::mutex> lock(state.queue_mutex);
    throw KVStoreError(state.fatal_error_message);
}

void throw_if_fatal_locked(const RequestRuntimeState& state) {
    if (state.has_fatal_error.load(std::memory_order_relaxed)) {
        throw KVStoreError(state.fatal_error_message);
    }
}

void enqueue_and_wait(RequestRuntimeState state, const RequestPtr& request) {
    throw_if_fatal(state);
    {
        std::lock_guard<std::mutex> lock(state.queue_mutex);
        throw_if_fatal_locked(state);
        state.request_queue.push_back(request);
        update_atomic_max(state.max_pending_queue_depth, state.request_queue.size());
        if (request->type == RequestType::kCompact) {
            state.compact_requests.fetch_add(1, std::memory_order_relaxed);
        } else {
            state.enqueued_write_requests.fetch_add(1, std::memory_order_relaxed);
        }
    }
    state.queue_cv.notify_one();

    std::unique_lock<std::mutex> done_lock(request->done_mutex);
    request->done_cv.wait(done_lock, [&request] { return request->done; });
    if (!request->error.empty()) {
        throw KVStoreError(request->error);
    }
}

void complete_request(
    const RequestPtr& request,
    const std::string& error,
    const RequestCompletionCallbacks& callbacks) {
    if (request->type != RequestType::kCompact && error.empty() && callbacks.on_successful_write) {
        callbacks.on_successful_write(*request);
    }
    {
        std::lock_guard<std::mutex> lock(request->done_mutex);
        request->done = true;
        request->error = error;
    }
    request->done_cv.notify_one();
}

void fail_requests(
    const std::vector<RequestPtr>& requests,
    const std::string& error,
    const RequestCompletionCallbacks& callbacks) {
    for (const auto& request : requests) {
        complete_request(request, error, callbacks);
    }
}

void enter_fatal_state(
    RequestRuntimeState state,
    const std::string& error,
    const RequestCompletionCallbacks& callbacks) {
    RequestQueue pending_requests;
    {
        std::lock_guard<std::mutex> lock(state.queue_mutex);
        if (state.has_fatal_error.load(std::memory_order_relaxed)) {
            return;
        }
        state.fatal_error_message = error;
        state.has_fatal_error.store(true, std::memory_order_release);
        pending_requests.swap(state.request_queue);
    }

    for (const auto& request : pending_requests) {
        complete_request(request, error, callbacks);
    }
}

}  // namespace kvstore::internal
