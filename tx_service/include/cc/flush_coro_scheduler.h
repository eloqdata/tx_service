/**
 * C++20 coroutine scheduler for FlushDataWorker. Uses only std::coroutine
 * and std::thread; no bthread. See
 * .cursor/plans/2026-02-03-flush-data-coroutine-coro-demo.md
 */
#pragma once

#include <butil/logging.h>

#include <atomic>
#include <coroutine>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <vector>

namespace txservice
{

// --- 1. Task scheduler (ready queue, PostReadyHandle, RunLoopOnce) ---
struct TaskScheduler
{
    std::queue<std::coroutine_handle<>> ready_queue;
    std::mutex mtx;
    // Optional callback invoked when a handle is posted (e.g. from async
    // completion). Used to wake FlushDataWorker so it runs the scheduler
    // immediately instead of waiting for wait_for timeout.
    std::function<void()> on_post_ready_;

    void SetOnPostReady(std::function<void()> f)
    {
        on_post_ready_ = std::move(f);
    }

    void PostReadyHandle(std::coroutine_handle<> h)
    {
        {
            std::lock_guard<std::mutex> lk(mtx);
            ready_queue.push(h);
            // LOG(INFO) << "TaskScheduler: post ready handle, size = "
            //          << ready_queue.size() << ", this = " << this;
        }
        if (on_post_ready_)
        {
            on_post_ready_();
        }
    }

    size_t QueueSize()
    {
        std::lock_guard<std::mutex> lk(mtx);
        return ready_queue.size();
    }

    void RunLoopOnce()
    {
        std::coroutine_handle<> h;
        {
            std::lock_guard<std::mutex> lk(mtx);
            if (ready_queue.empty())
                return;
            h = ready_queue.front();
            ready_queue.pop();
        }

        if (h && h.done())
        {
            // LOG(INFO) << "TaskScheduler: handle already done";
        }

        // Only resume if not already done (avoids double-resume segfault if
        // the same handle was posted twice; do not destroy when done - the
        // frame may already have been destroyed in final_suspend).
        if (h && !h.done())
            h.resume();
    }

    bool IsEmpty()
    {
        std::lock_guard<std::mutex> lk(mtx);
        return ready_queue.empty();
    }
};

// --- 2. Task<T> coroutine type (unified: void and value-returning) ---
template <typename T>
struct Task;

template <>
struct Task<void>
{
    struct promise_type
    {
        std::coroutine_handle<> continuation;
        std::function<void()>
            on_complete;  // optional; called when task ends (e.g. JoinAll)

        Task get_return_object()
        {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend()
        {
            return {};
        }
        void return_void()
        {
        }

        struct FinalAwaitable
        {
            bool await_ready() noexcept
            {
                return false;
            }
            void await_suspend(std::coroutine_handle<promise_type> h) noexcept
            {
                if (h.promise().on_complete)
                    h.promise().on_complete();
                if (h.promise().continuation)
                    h.promise().continuation.resume();
                else
                    h.destroy();
            }
            void await_resume() noexcept
            {
            }
        };
        FinalAwaitable final_suspend() noexcept
        {
            return {};
        }
        void unhandled_exception()
        {
            std::terminate();
        }
    };

    std::coroutine_handle<promise_type> handle;

    // co_await starts child via symmetric
    // transfer; await_ready() true if already done.
    bool await_ready()
    {
        return handle.done();
    }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h)
    {
        handle.promise().continuation = h;
        return handle;  // symmetric transfer: start child, don't go to queue
    }
    void await_resume()
    {
    }
};

template <typename T>
struct Task
{
    struct promise_type
    {
        T result{};
        std::coroutine_handle<> continuation;
        std::function<void()>
            on_complete;  // optional; called when task ends (e.g. JoinAll)

        Task get_return_object()
        {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend()
        {
            return {};
        }
        void return_value(T val)
        {
            result = std::move(val);
        }

        struct FinalAwaitable
        {
            bool await_ready() noexcept
            {
                return false;
            }
            void await_suspend(std::coroutine_handle<promise_type> h) noexcept
            {
                if (h.promise().on_complete)
                    h.promise().on_complete();
                if (h.promise().continuation)
                    h.promise().continuation.resume();
                else
                    h.destroy();
            }
            void await_resume() noexcept
            {
            }
        };
        FinalAwaitable final_suspend() noexcept
        {
            return {};
        }
        void unhandled_exception()
        {
            std::terminate();
        }
    };

    std::coroutine_handle<promise_type> handle;

    // co_await starts child via symmetric
    // transfer; await_ready() true if already done.
    bool await_ready()
    {
        return handle.done();
    }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h)
    {
        handle.promise().continuation = h;
        return handle;  // symmetric transfer: start child, don't go to queue
    }

    T await_resume()
    {
        return std::move(handle.promise().result);
    }
};

// --- 2b. JoinAll (Rust style): Lazy sub-tasks + batch PostReadyHandle ---
// No gate sub-coroutine: each child's promise has on_complete set to decrement
// a shared counter; when it reaches 0, post parent once. Children complete
// and decrement by themselves.
struct JoinAllState
{
    std::atomic<size_t> count{0};
    TaskScheduler *sched{nullptr};
    std::coroutine_handle<> parent_h;
};

template <typename T>
struct JoinAllAwaitable
{
    std::vector<Task<T>> &tasks;
    TaskScheduler *sched;

    bool await_ready()
    {
        if (tasks.empty())
            return true;
        for (auto &t : tasks)
        {
            if (!t.handle.done())
                return false;
        }
        return true;
    }

    void await_suspend(std::coroutine_handle<> h)
    {
        if (tasks.empty())
        {
            sched->PostReadyHandle(h);
            return;
        }
        auto state = std::make_shared<JoinAllState>();
        state->count = tasks.size();
        state->sched = sched;
        state->parent_h = h;
        for (auto &t : tasks)
        {
            t.handle.promise().on_complete = [state]()
            {
                // Use fetch_sub return value so only the thread that
                // decrements from 1 to 0 posts the parent. Otherwise
                // two threads could both see count == 0 after decrementing
                // and both call PostReadyHandle(parent_h) (double resume).
                if (state->count.fetch_sub(1) == 1)
                {
                    state->sched->PostReadyHandle(state->parent_h);
                }
            };
            t.handle.promise().continuation = nullptr;
        }
        for (auto &t : tasks)
        {
            sched->PostReadyHandle(t.handle);
        }
    }

    std::vector<T> await_resume()
    {
        std::vector<T> results;
        results.reserve(tasks.size());
        for (auto &t : tasks)
        {
            results.push_back(std::move(t.handle.promise().result));
        }
        return results;
    }
};

template <typename T>
inline JoinAllAwaitable<T> JoinAll(std::vector<Task<T>> &tasks,
                                   TaskScheduler *sched)
{
    return JoinAllAwaitable<T>{tasks, sched};
}

// --- 3. Yield: minimal awaitable (no TaskAwaitable, no closure) ---
// Memory-safe: only holds TaskScheduler*; callback created in await_suspend
// with only [handle, sched].
struct YieldAwaitable
{
    TaskScheduler *sched;

    bool await_ready() const
    {
        return false;
    }
    void await_suspend(std::coroutine_handle<> h)
    {
        sched->PostReadyHandle(h);
    }
    void await_resume()
    {
    }
};

inline YieldAwaitable Yield(TaskScheduler *sched)
{
    return YieldAwaitable{sched};
}

}  // namespace txservice
