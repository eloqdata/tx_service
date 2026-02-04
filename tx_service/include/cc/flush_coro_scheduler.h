/**
 * C++20 coroutine scheduler for FlushDataWorker. Uses only std::coroutine
 * and std::thread; no bthread. See
 * .cursor/plans/2026-02-03-flush-data-coroutine-coro-demo.md
 */
#pragma once

#include <butil/logging.h>

#include <coroutine>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>

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

    bool await_ready()
    {
        return false;
    }
    void await_suspend(std::coroutine_handle<> h)
    {
        handle.promise().continuation = h;
        handle.resume();
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

    bool await_ready()
    {
        return false;
    }
    void await_suspend(std::coroutine_handle<> h)
    {
        handle.promise().continuation = h;
        handle.resume();
    }
    T await_resume()
    {
        return std::move(handle.promise().result);
    }
};

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
        start_op(
            [this, h, s = this->sched](R v)
            {
                LOG(INFO) << "TaskAwaitable: post ready handle";
                res_->emplace(std::move(v));
                s->PostReadyHandle(h);
            });
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
