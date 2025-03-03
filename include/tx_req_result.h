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

#include <mutex>

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
    TxResult(const std::function<void()> *yield_fp,
             const std::function<void()> *resume_fp)
        : value_(),
          status_(TxResultStatus::Unknown),
          error_code_(TxErrorCode::NO_ERROR),
          mutex_(),
          cv_(),
          waiting_(false),
          yield_func_(yield_fp),
          resume_func_(resume_fp)
#if defined ON_KEY_OBJECT && defined EXT_TX_PROC_ENABLED
          ,
          allow_resume_call_(resume_fp != nullptr)
#endif
    {
    }

    TxResultStatus Status()
    {
        std::lock_guard<bthread::Mutex> lk(mutex_);
        return status_;
    }

    bool IsError() const
    {
        return status_ == TxResultStatus::Error;
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

    void Finish(const T &val)
    {
#if defined ON_KEY_OBJECT && defined EXT_TX_PROC_ENABLED
        if (resume_func_ != nullptr)
        {
            // No need for lock since the txrequest sender bthread and the
            // txm forward thread are the same thread or coordinated
            // already.
            value_ = val;
            status_ = TxResultStatus::Finished;

            // The yield func and resume func can only be called once each.
            if (allow_resume_call_)
            {
                allow_resume_call_ = false;
                (*resume_func_)();
            }
            // resume_func_ = nullptr;
            return;
        }
#endif

        std::unique_lock<bthread::Mutex> lk(mutex_);

        value_ = val;
        status_ = TxResultStatus::Finished;

        if (waiting_)
        {
            if (resume_func_ != nullptr)
            {
                lk.unlock();
                // The resume functor schedules the coroutine waiting for the
                // result to re-run/resume from the point it yields, i.e.,
                // inside Wait().
                (*resume_func_)();
            }
            else if (yield_func_ == nullptr)
            {
                // cv notification needs to be in the lock scope. This is
                // because the tx request is owned by the sender and the sending
                // thread may wake up spuriously before notify_one() is called.
                // If so, the sending thread moves forward and de-allocate the
                // tx request, before notify_one() is called, causing invalid
                // memory access.
                cv_.notify_one();
            }
        }
    }

    void Finish(T &&val)
    {
#if defined ON_KEY_OBJECT && defined EXT_TX_PROC_ENABLED
        if (resume_func_ != nullptr)
        {
            // No need for lock since the txrequest sender bthread and the
            // txm forward thread are the same thread or coordinated
            // already.
            value_ = val;
            status_ = TxResultStatus::Finished;

            // The yield func and resume func can only be called once each.
            if (allow_resume_call_)
            {
                allow_resume_call_ = false;
                (*resume_func_)();
            }
            return;
        }
#endif

        std::unique_lock<bthread::Mutex> lk(mutex_);

        value_ = std::move(val);
        status_ = TxResultStatus::Finished;

        if (waiting_)
        {
            if (resume_func_ != nullptr)
            {
                lk.unlock();
                // The resume functor schedules the waiting coroutine to
                // re-run/resume from the point it is blocking for the result,
                // i.e., inside Wait().
                (*resume_func_)();
            }
            else if (yield_func_ == nullptr)
            {
                cv_.notify_one();
            }
        }
    }

    void FinishError(TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR)
    {
#if defined ON_KEY_OBJECT && defined EXT_TX_PROC_ENABLED
        if (resume_func_ != nullptr)
        {
            // No need for lock since the txrequest sender bthread and the
            // txm forward thread are the same thread or coordinated
            // already.
            status_ = TxResultStatus::Error;
            error_code_ = err_code;

            // The yield func and resume func can only be called once each.
            if (allow_resume_call_)
            {
                allow_resume_call_ = false;
                (*resume_func_)();
            }
            return;
        }
#endif

        std::unique_lock<bthread::Mutex> lk(mutex_);

        status_ = TxResultStatus::Error;
        error_code_ = err_code;

        if (waiting_)
        {
            if (resume_func_ != nullptr)
            {
                lk.unlock();
                (*resume_func_)();
            }
            else if (yield_func_ == nullptr)
            {
                cv_.notify_one();
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
        std::lock_guard<bthread::Mutex> lk(mutex_);

        status_ = TxResultStatus::Unknown;
        error_code_ = TxErrorCode::NO_ERROR;
        waiting_ = false;
        yield_func_ = yield_fptr;
        resume_func_ = resume_fptr;

#ifdef ON_KEY_OBJECT
        allow_resume_call_ = resume_fptr != nullptr;
#endif
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
#if defined ON_KEY_OBJECT && defined EXT_TX_PROC_ENABLED
        if (allow_resume_call_)
        {
            allow_resume_call_ = false;
            return resume_func_;
        }
#endif
        return nullptr;
    }

    int Wait()
    {
        if (yield_func_ != nullptr)
        {
            std::unique_lock<bthread::Mutex> lk(mutex_);

            if (status_ == TxResultStatus::Unknown)
            {
                waiting_ = true;
                lk.unlock();

                // The yield functor invokes the coroutine's resume() and
                // returns the control to the caller of the coroutine that sends
                // the tx request, i.e., the runtime thread executing the query.
                // The runtime thread skips the blocking coroutine and moves on
                // to process the next command.
                (*yield_func_)();
            }
        }
        else
        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            waiting_ = true;
            while (status_ == TxResultStatus::Unknown)
            {
                cv_.wait(lk);
            }
        }

        return 0;
    }

private:
    T value_;
    TxResultStatus status_;
    TxErrorCode error_code_;
    bthread::Mutex mutex_;
    bthread::ConditionVariable cv_;

    bool waiting_{false};
    const std::function<void()> *yield_func_;
    // the resume func might be passed to cc handler result
    const std::function<void()> *resume_func_;

#ifdef ON_KEY_OBJECT
    bool allow_resume_call_{};

    inline static int initial_wait_time_us_ = 100;
    inline static int max_wait_time_us_ = 20000;
#endif

    template <typename Subtype, typename ResultType>
    friend struct TemplateTxRequest;
};
}  // namespace txservice
