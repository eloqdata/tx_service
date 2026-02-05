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

    void PostReadyHandle(std::coroutine_handle<> h)
    {
        std::lock_guard<std::mutex> lk(mtx);
        ready_queue.push(h);
        LOG(INFO) << "TaskScheduler: post ready handle, size = "
                  << ready_queue.size();
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
            LOG(INFO) << "TaskScheduler: handle already done";
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
                state->count--;
                if (state->count.load() == 0)
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

// --- 3. TaskAwaitable<R> for async ops and Yield ---
// Optional ready_fn: when set, await_ready() returns ready_fn(); else false.
template <typename R>
struct TaskAwaitable
{
    TaskScheduler *sched;
    std::function<void(std::function<void(R)>)> start_op;
    std::function<bool()> ready_fn_{};

    TaskAwaitable() = default;
    TaskAwaitable(TaskScheduler *s,
                  std::function<void(std::function<void(R)>)> op,
                  std::function<bool()> ready = {})
        : sched(s), start_op(std::move(op)), ready_fn_(std::move(ready))
    {
    }

    bool await_ready() const
    {
        if (ready_fn_)
        {
            LOG(INFO) << "TaskAwaitable: await_ready, ready_fn_() = "
                      << ready_fn_();
            return ready_fn_();
        }
        return false;
    }
    void await_suspend(std::coroutine_handle<> h)
    {
        LOG(INFO) << "TaskAwaitable: await_suspend";
        res_ = std::make_shared<std::optional<R>>();
        auto resume_lambda = [this, h, s = this->sched](R v)
        {
            LOG(INFO) << "TaskAwaitable: post ready handle";
            res_->emplace(std::move(v));
            s->PostReadyHandle(h);
        };

        start_op(resume_lambda);
    }

    R await_resume()
    {
        return std::move(res_->value_or(R()));
    }

private:
    std::shared_ptr<std::optional<R>> res_;
};

// void specialization: no return value
template <>
struct TaskAwaitable<void>
{
    TaskScheduler *sched;
    std::function<void(std::function<void()>)> start_op;
    std::function<bool()> ready_fn_{};

    TaskAwaitable() = default;
    TaskAwaitable(TaskScheduler *s,
                  std::function<void(std::function<void()>)> op,
                  std::function<bool()> ready = {})
        : sched(s), start_op(std::move(op)), ready_fn_(std::move(ready))
    {
    }

    bool await_ready() const
    {
        if (ready_fn_)
        {
            LOG(INFO) << "TaskAwaitable: await_ready, ready_fn_() = "
                      << ready_fn_();
            return ready_fn_();
        }
        return false;
    }
    void await_suspend(std::coroutine_handle<> h)
    {
        start_op([h, s = this->sched]() { s->PostReadyHandle(h); });
    }
    void await_resume()
    {
    }
};

// --- 4. Yield: yield to scheduler ---
inline TaskAwaitable<void> Yield(TaskScheduler *sched)
{
    return TaskAwaitable<void>{sched, [](auto cb) { cb(); }};
}

}  // namespace txservice
