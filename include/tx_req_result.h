/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <cassert>
#include <functional>
#include <mutex>
#include <utility>
#include <variant>

#include "error_messages.h"

namespace txservice
{
enum struct TxResultStatus
{
    Unknown,
    Finished,
    Error
};

enum class UpsertResult
{
    Succeeded = 0,  // The request has finished and passed
    Failed,         // The request failed in running
    Unverified  // The request returned without finish and will continue to run
                // from log recover
};

struct StatusCombo
{
    TxResultStatus status_;
    bool waiting_;
};

struct YieldResume
{
    YieldResume() = delete;
    YieldResume(const std::function<void()> *yield,
                const std::function<void()> *resume)
        : yield_(yield), resume_(resume)
    {
    }

    const std::function<void()> *yield_{nullptr};
    const std::function<void()> *resume_{nullptr};
};

struct MutexCond
{
    MutexCond() = default;

    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;
};

/**
 * @brief The result of a transaction request, i.e., begin, read, write,
 * scan_begin, scan_next, scan_end, commit and abort. The sender of the request
 * is blocked, when the result has not returned. If the sender needs to yield by
 * context switching, the sender should block on a condition variable and be
 * woke up later when Finish() is invoked by the tx service.
 *
 * @tparam T The type of the returned result of the tx request.
 */
template <typename T>
class TxResult
{
public:
    TxResult() = delete;

    TxResult(const std::function<void()> *yield_fp,
             const std::function<void()> *resume_fp)
        : value_(),
          status_combo_({TxResultStatus::Unknown, false}),
          error_code_(TxErrorCode::NO_ERROR),
          allow_resume_call_(resume_fp != nullptr),
          control_(
              yield_fp != nullptr
                  ? std::variant<YieldResume, MutexCond>(
                        std::in_place_type<YieldResume>, yield_fp, resume_fp)
                  : std::variant<YieldResume, MutexCond>(
                        std::in_place_type<MutexCond>))
    {
    }

    TxResultStatus Status()
    {
        return status_combo_.load(std::memory_order_relaxed).status_;
    }

    bool IsError() const
    {
        return status_combo_.load(std::memory_order_relaxed).status_ ==
               TxResultStatus::Error;
    }

    T &Value()
    {
        return value_;
    }

    const T &Value() const
    {
        return value_;
    }

    TxErrorCode ErrorCode() const
    {
        return error_code_;
    }

    template <typename U>
    void Finish(U &&val)
    {
        value_ = std::forward<U>(val);

        if (HasYieldResume())
        {
            StatusCombo state = status_combo_.load(std::memory_order_relaxed);

            while (!status_combo_.compare_exchange_weak(
                state,
                {TxResultStatus::Finished, state.waiting_},
                std::memory_order_acq_rel))
                ;

            if (state.waiting_ && allow_resume_call_)
            {
                const std::function<void()> *resume = GetResume();
                // The resume functor schedules the coroutine waiting for the
                // result to re-run/resume from the point it yields, i.e.,
                // inside Wait().
                allow_resume_call_ = false;
                (*resume)();
            }
        }
        else
        {
            bthread::Mutex &mux = GetMutex();
            bthread::ConditionVariable &cv = GetCond();
            // cv notification needs to be in the lock scope. This is because
            // the tx request is owned by the sender and the sending thread may
            // wake up spuriously before notify_one() is called. If so, the
            // sending thread moves forward and de-allocate the tx request,
            // before notify_one() is called, causing invalid memory access.
            std::unique_lock<bthread::Mutex> lk(mux);
            StatusCombo state = status_combo_.load(std::memory_order_relaxed);
            status_combo_.store({TxResultStatus::Finished, state.waiting_},
                                std::memory_order_relaxed);
            if (state.waiting_)
            {
                cv.notify_one();
            }
        }
    }

    void FinishError(TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR)
    {
        error_code_ = err_code;

        if (HasYieldResume())
        {
            StatusCombo state = status_combo_.load(std::memory_order_relaxed);

            while (!status_combo_.compare_exchange_weak(
                state,
                {TxResultStatus::Error, state.waiting_},
                std::memory_order_acq_rel))
                ;

            if (state.waiting_ && allow_resume_call_)
            {
                const std::function<void()> *resume = GetResume();
                allow_resume_call_ = false;
                (*resume)();
            }
        }
        else
        {
            bthread::Mutex &mux = GetMutex();
            bthread::ConditionVariable &cv = GetCond();
            // cv notification needs to be in the lock scope. This is because
            // the tx request is owned by the sender and the sending thread may
            // wake up spuriously before notify_one() is called. If so, the
            // sending thread moves forward and de-allocate the tx request,
            // before notify_one() is called, causing invalid memory access.
            std::unique_lock<bthread::Mutex> lk(mux);
            StatusCombo state = status_combo_.load(std::memory_order_relaxed);
            status_combo_.store({TxResultStatus::Error, state.waiting_},
                                std::memory_order_relaxed);
            if (state.waiting_)
            {
                cv.notify_one();
            }
        }
    }

    /**
     * @brief Set the Error Code for tx request.
     * For example, set the reason for transaction abort.
     *
     * @param err_code
     */
    void SetErrorCode(TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR)
    {
        error_code_ = err_code;
    }

    void Reset(const std::function<void()> *yield_fptr = nullptr,
               const std::function<void()> *resume_fptr = nullptr)
    {
        status_combo_.store({TxResultStatus::Unknown, false},
                            std::memory_order_relaxed);
        error_code_ = TxErrorCode::NO_ERROR;

        if (yield_fptr != nullptr)
        {
            assert(resume_fptr != nullptr);
            control_.template emplace<YieldResume>(yield_fptr, resume_fptr);
        }
        else if (HasYieldResume())
        {
            control_.template emplace<MutexCond>();
        }

        allow_resume_call_ = resume_fptr != nullptr;
    }

    /**
     * The resume func can only be called once, either when cc_result->SetFinish
     * or tx_result->SetFinish.
     * When the txm sends the ccrequest, the resume functor is released to
     * CcHandlerResult. If the txm fails to send the ccrequest, i.e. term
     * failure when InitTxnOp, the resume functor will be called when tx_result
     * is SetFinished or SetError.
     *
     * @return
     */
    const std::function<void()> *ReleaseResumeFunc()
    {
        if (allow_resume_call_)
        {
            const std::function<void()> *resume = GetResume();
            allow_resume_call_ = false;
            return resume;
        }
        else
        {
            return nullptr;
        }
    }

    int Wait()
    {
        if (HasYieldResume())
        {
            StatusCombo state = status_combo_.load(std::memory_order_acquire);
            while (state.status_ == TxResultStatus::Unknown)
            {
                bool success = status_combo_.compare_exchange_weak(
                    state,
                    {TxResultStatus::Unknown, true},
                    std::memory_order_acq_rel);

                if (success)
                {
                    const std::function<void()> *yield = GetYield();
                    // The yield functor invokes the coroutine's resume() and
                    // returns the control to the caller of the coroutine that
                    // sends the tx request, i.e., the runtime thread executing
                    // the query. The runtime thread skips the blocking
                    // coroutine and moves on to process the next command.
                    (*yield)();
                    break;
                }
            }
        }
        else
        {
            bthread::Mutex &mux = GetMutex();
            bthread::ConditionVariable &cv = GetCond();

            std::unique_lock<bthread::Mutex> lk(mux);
            StatusCombo state = status_combo_.load(std::memory_order_relaxed);
            while (state.status_ == TxResultStatus::Unknown)
            {
                status_combo_.store({TxResultStatus::Unknown, true},
                                    std::memory_order_relaxed);
                cv.wait(lk);
                state = status_combo_.load(std::memory_order_relaxed);
            }
        }

        return 0;
    }

private:
    bool HasYieldResume() const
    {
        return std::holds_alternative<YieldResume>(control_);
    }

    const std::function<void()> *GetYield() const
    {
        return std::get<YieldResume>(control_).yield_;
    }

    const std::function<void()> *GetResume() const
    {
        return std::get<YieldResume>(control_).resume_;
    }

    bthread::Mutex &GetMutex()
    {
        return std::get<MutexCond>(control_).mux_;
    }

    bthread::ConditionVariable &GetCond()
    {
        return std::get<MutexCond>(control_).cv_;
    }

    T value_;
    std::atomic<StatusCombo> status_combo_;
    TxErrorCode error_code_;
    bool allow_resume_call_{true};

    std::variant<YieldResume, MutexCond> control_;

    inline static int initial_wait_time_us_ = 100;
    inline static int max_wait_time_us_ = 20000;

    template <typename Subtype, typename ResultType>
    friend struct TemplateTxRequest;
};
}  // namespace txservice
