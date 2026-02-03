/**
 * C++20 coroutine scheduler for FlushDataWorker. Uses only std::coroutine
 * and std::thread; no bthread. See
 * .cursor/plans/2026-02-03-flush-data-coroutine-coro-demo.md
 */
#pragma once

#include <coroutine>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>

namespace txservice
{

// --- 1. Task scheduler (ready queue, PostReadyHandle, RunLoopOnce) ---
struct FlushCoroTaskScheduler
{
    std::queue<std::coroutine_handle<>> ready_queue;
    std::mutex mtx;

    void PostReadyHandle(std::coroutine_handle<> h)
    {
        std::lock_guard<std::mutex> lk(mtx);
        ready_queue.push(h);
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
        if (h)
            h.resume();
    }

    bool IsEmpty()
    {
        std::lock_guard<std::mutex> lk(mtx);
        return ready_queue.empty();
    }
};

// --- 2. Task<void> coroutine type (no return value) ---
struct FlushCoroTask
{
    struct promise_type
    {
        std::coroutine_handle<> continuation;

        FlushCoroTask get_return_object()
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

// --- 3. TaskAwaitable<R> for async ops and Yield ---
// Optional ready_fn: when set, await_ready() returns ready_fn(); else false.
template <typename R>
struct FlushCoroTaskAwaitable
{
    FlushCoroTaskScheduler *sched;
    std::function<void(std::function<void(R)>)> start_op;
    std::function<bool()> ready_fn_{};

    bool await_ready() const
    {
        if (ready_fn_)
            return ready_fn_();
        return false;
    }
    void await_suspend(std::coroutine_handle<> h)
    {
        res_ = std::make_shared<std::optional<R>>();
        start_op(
            [this, h, s = this->sched](R v)
            {
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
struct FlushCoroTaskAwaitable<void>
{
    FlushCoroTaskScheduler *sched;
    std::function<void(std::function<void()>)> start_op;
    std::function<bool()> ready_fn_{};

    bool await_ready() const
    {
        if (ready_fn_)
            return ready_fn_();
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
inline FlushCoroTaskAwaitable<void> FlushCoroYield(
    FlushCoroTaskScheduler *sched)
{
    return FlushCoroTaskAwaitable<void>{sched, [](auto cb) { cb(); }};
}

}  // namespace txservice
