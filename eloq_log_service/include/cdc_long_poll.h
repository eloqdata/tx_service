/**
 * @file
 * The CdcScan long poll (docs/cdc-change-stream.md in the EloqDoc repo, §4.2
 * "Long poll", §5.8 "Logger side"): the scans a log group holds until
 * something may have become emittable.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>

namespace txlog
{
/**
 * @brief The parked long-poll CdcScan requests of one log group.
 *
 * A scan that finds nothing emittable and may wait (`wait_ms > 0`) is parked
 * here instead of being answered. Four sources complete it, on different
 * threads, and the first one wins: an advance of the group's
 * arrival_complete_ts (the only change that can make a record emittable), the
 * request's deadline, the client's cancellation, and the node's step-down or
 * shutdown. Every source takes the request's `completed` flag with an
 * exchange, and only the winner runs its completion, so the brpc `done`
 * closure runs exactly once: twice is undefined behavior, never leaks the
 * call until the client's timeout.
 *
 * Completions run on bthreads of their own, never on the thread of the source:
 * the raft apply thread and the timer thread must not do a scan's work. The
 * list is guarded by a plain mutex, held only to insert and remove entries;
 * nothing waits on a condition variable (see the threading gotcha in the
 * data_substrate CLAUDE.md).
 *
 * Use: Prepare() a request, register its cancellation (Cancel) with the call,
 * then Park() it. The cancellation is registered first because it may fire at
 * once; a parked request could complete and end the call before the
 * registration, which would then use a finished call.
 */
class CdcLongPoll
{
public:
    /** @brief Why a parked request completes. */
    enum class Wake
    {
        kAdvance,   ///< arrival_complete_ts advanced: scan again.
        kDeadline,  ///< The request's wait elapsed: scan again.
        kCancel,    ///< The client cancelled the call: any answer will do.
        kStop,      ///< The node stopped leading or shuts down: NotLeader.
    };

    /**
     * @brief Answers a parked request, running the call's done closure.
     * Called exactly once, on a bthread.
     */
    using Completion = std::function<void(Wake)>;

    /** @brief One request, prepared or parked. */
    struct Request;

    CdcLongPoll() = default;
    CdcLongPoll(const CdcLongPoll &) = delete;
    CdcLongPoll &operator=(const CdcLongPoll &) = delete;

    /**
     * @brief Completes every request with kStop, from now on also those parked
     * later, and waits until no completion or deadline timer is running.
     */
    ~CdcLongPoll();

    /**
     * @brief Makes a request that can be completed but is not parked yet.
     *
     * @param completion How to answer it.
     * @return The request, to pass to Cancel() and Park().
     */
    std::shared_ptr<Request> Prepare(Completion completion);

    /**
     * @brief Parks a prepared request and starts its deadline. A request
     * already completed (cancelled) is left alone; one parked after the poll
     * was stopped is completed with kStop.
     *
     * @param request The request.
     * @param wait_ms How long it may wait; its deadline completes it with
     *        kDeadline.
     */
    void Park(const std::shared_ptr<Request> &request, uint32_t wait_ms);

    /**
     * @brief Completes one request, unless another source already has.
     *
     * @param request The request, prepared or parked.
     * @param why The reason passed to its completion.
     */
    void Complete(const std::shared_ptr<Request> &request, Wake why);

    /**
     * @brief Completes every parked request.
     *
     * @param why The reason passed to their completions.
     */
    void CompleteAll(Wake why);

    /**
     * @brief Completes a request with kCancel, unless another source already
     * has. The target of the call's NotifyOnCancel closure, which brpc runs
     * once the call is cancelled or has ended, whichever comes first; after
     * the end it finds the request completed and touches nothing else.
     *
     * @param request The request; the closure keeps it alive.
     */
    static void Cancel(std::shared_ptr<Request> request);

    /** @return The number of parked requests. */
    size_t Size() const;

private:
    /** @brief A deadline timer's argument; keeps its request alive. */
    struct TimerArg
    {
        CdcLongPoll *poll;
        std::shared_ptr<Request> request;
    };

    /** @brief A completion's bthread argument. */
    struct Task
    {
        CdcLongPoll *poll;
        std::shared_ptr<Request> request;
        Wake why;
    };

    /**
     * @brief The bthread_timer_add callback of a request's deadline.
     *
     * @param arg The TimerArg, which it deletes.
     */
    static void OnDeadline(void *arg);

    /**
     * @brief The bthread that runs a completion.
     *
     * @param arg The Task, which it deletes.
     * @return nullptr.
     */
    static void *RunCompletion(void *arg);

    /**
     * @brief Completes a request whose `completed` flag the caller took:
     * unparks it, deletes its timer and starts its completion.
     *
     * @param request The request.
     * @param why The reason passed to its completion.
     */
    void Finish(const std::shared_ptr<Request> &request, Wake why);

    mutable std::mutex mutex_;  ///< Guards parked_, stopped_ and each
                                ///< request's list and timer fields.
    std::list<std::shared_ptr<Request>> parked_;  ///< The parked requests.
    bool stopped_{false};  ///< Set by the destructor: parks complete at once.
    /**
     * @brief Prepared requests whose completion has not returned, plus
     * deadline timers that may still run; the destructor waits for zero.
     */
    std::atomic<int64_t> pending_{0};
};
}  // namespace txlog
