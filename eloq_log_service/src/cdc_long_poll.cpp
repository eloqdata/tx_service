/**
 * @file
 * The CdcScan long poll; see cdc_long_poll.h.
 */
#include "cdc_long_poll.h"

#include <bthread/bthread.h>
#include <bthread/unstable.h>
#include <butil/logging.h>
#include <butil/time.h>

#include <utility>
#include <vector>

namespace txlog
{
/**
 * @brief One request's state. `completion` and `completed` are set once; the
 * list position and the timer fields are guarded by the poll's mutex.
 */
struct CdcLongPoll::Request
{
    /**
     * @brief The poll it was prepared by, which outlives it until its
     * completion has returned.
     */
    CdcLongPoll *poll{nullptr};
    Completion completion;               ///< Runs once, when the request ends.
    std::atomic<bool> completed{false};  ///< Taken by the source that wins.
    bool parked{false};                  ///< In the poll's list.
    std::list<std::shared_ptr<Request>>::iterator position;  ///< Its entry.
    bool has_timer{false};         ///< A deadline timer is set.
    bthread_timer_t timer{};       ///< The deadline timer.
    TimerArg *timer_arg{nullptr};  ///< The timer's argument.
};

CdcLongPoll::~CdcLongPoll()
{
    {
        std::lock_guard<std::mutex> lk(mutex_);
        stopped_ = true;
    }
    CompleteAll(Wake::kStop);
    // Completions and timer callbacks use this object; a prepared request is
    // parked, and so completed, by its caller right after its preparation.
    while (pending_.load(std::memory_order_acquire) != 0)
    {
        bthread_usleep(1000);
    }
}

std::shared_ptr<CdcLongPoll::Request> CdcLongPoll::Prepare(
    Completion completion)
{
    auto request = std::make_shared<Request>();
    request->poll = this;
    request->completion = std::move(completion);
    pending_.fetch_add(1, std::memory_order_acq_rel);
    return request;
}

void CdcLongPoll::Park(const std::shared_ptr<Request> &request,
                       uint32_t wait_ms)
{
    {
        std::lock_guard<std::mutex> lk(mutex_);
        if (request->completed.load(std::memory_order_acquire))
        {
            return;
        }
        if (!stopped_)
        {
            request->position = parked_.insert(parked_.end(), request);
            request->parked = true;
            auto *arg = new TimerArg{this, request};
            // The callback may run at once, on the timer thread; it waits for
            // this lock before it touches the request.
            pending_.fetch_add(1, std::memory_order_acq_rel);
            if (bthread_timer_add(&request->timer,
                                  butil::milliseconds_from_now(wait_ms),
                                  &CdcLongPoll::OnDeadline,
                                  arg) == 0)
            {
                request->has_timer = true;
                request->timer_arg = arg;
            }
            else
            {
                // Without a timer the request still ends at the next advance
                // of arrival_complete_ts or at the step-down.
                LOG(WARNING) << "CdcScan long poll: no deadline timer for a "
                                "parked scan";
                pending_.fetch_sub(1, std::memory_order_acq_rel);
                delete arg;
            }
            return;
        }
    }
    Complete(request, Wake::kStop);
}

void CdcLongPoll::Complete(const std::shared_ptr<Request> &request, Wake why)
{
    if (request->completed.exchange(true, std::memory_order_acq_rel))
    {
        return;
    }
    Finish(request, why);
}

void CdcLongPoll::CompleteAll(Wake why)
{
    std::vector<std::shared_ptr<Request>> woken;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        woken.assign(parked_.begin(), parked_.end());
    }
    for (const std::shared_ptr<Request> &request : woken)
    {
        Complete(request, why);
    }
}

void CdcLongPoll::Cancel(std::shared_ptr<Request> request)
{
    // Once the call has ended the request is completed and its poll may be
    // gone, so only the winner of the flag touches the poll; the poll waits
    // for every uncompleted request before it is destroyed.
    if (request->completed.exchange(true, std::memory_order_acq_rel))
    {
        return;
    }
    request->poll->Finish(request, Wake::kCancel);
}

size_t CdcLongPoll::Size() const
{
    std::lock_guard<std::mutex> lk(mutex_);
    return parked_.size();
}

void CdcLongPoll::OnDeadline(void *arg)
{
    auto *timer_arg = static_cast<TimerArg *>(arg);
    CdcLongPoll *poll = timer_arg->poll;
    poll->Complete(timer_arg->request, Wake::kDeadline);
    delete timer_arg;
    poll->pending_.fetch_sub(1, std::memory_order_acq_rel);
}

void *CdcLongPoll::RunCompletion(void *arg)
{
    auto *task = static_cast<Task *>(arg);
    CdcLongPoll *poll = task->poll;
    task->request->completion(task->why);
    task->request->completion = nullptr;
    delete task;
    poll->pending_.fetch_sub(1, std::memory_order_acq_rel);
    return nullptr;
}

void CdcLongPoll::Finish(const std::shared_ptr<Request> &request, Wake why)
{
    TimerArg *timer_arg = nullptr;
    bthread_timer_t timer{};
    {
        std::lock_guard<std::mutex> lk(mutex_);
        if (request->parked)
        {
            parked_.erase(request->position);
            request->parked = false;
        }
        if (request->has_timer)
        {
            request->has_timer = false;
            timer = request->timer;
            timer_arg = request->timer_arg;
        }
    }
    // A timer deleted before it ran never runs, and its argument is freed
    // here; a running or finished one frees its own.
    if (timer_arg != nullptr && bthread_timer_del(timer) == 0)
    {
        delete timer_arg;
        pending_.fetch_sub(1, std::memory_order_acq_rel);
    }

    bthread_t tid;
    auto *task = new Task{this, request, why};
    if (bthread_start_background(
            &tid, nullptr, &CdcLongPoll::RunCompletion, task) != 0)
    {
        RunCompletion(task);
    }
}
}  // namespace txlog
