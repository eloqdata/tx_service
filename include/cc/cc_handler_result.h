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

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>

#include "error_messages.h"  // CcErrorCode

namespace txservice
{
class TransactionExecution;

enum class HandlerResultErrorType
{
    // Ccnode is not the raft leader
    NotLeader = -1,
    // Other errors
    Error = 1,
    // Unknown
    Unknown = 3
};

/**
 * @brief CcHandlerResultBase is the base class of CcHandlerResult of different
 * operators which provides SetError and SetFinished API.
 *
 */
class CcHandlerResultBase
{
public:
    CcHandlerResultBase(TransactionExecution *txm) : txm_(txm)
    {
    }

    CcHandlerResultBase(const CcHandlerResultBase &) = delete;
    CcHandlerResultBase &operator=(const CcHandlerResultBase &) = delete;

    CcHandlerResultBase(CcHandlerResultBase &&rhs) = delete;

    virtual ~CcHandlerResultBase() = default;
    virtual bool SetError(CcErrorCode err_code) = 0;
    virtual bool SetFinished() = 0;

    virtual bool ForceError() = 0;

    virtual bool SetResultByStreamThread() = 0;
    virtual bool SetResultByTimeoutThread() = 0;

    bool IsFinished() const
    {
        return is_finished_.load(std::memory_order_acquire);
    }

    bool IsError() const
    {
        return error_code_.load(std::memory_order_relaxed) !=
               CcErrorCode::NO_ERROR;
    }

    void SetRemoteFinished()
    {
        remote_ref_cnt_.fetch_sub(1, std::memory_order_relaxed);
        SetFinished();
    }

    void SetRemoteError(CcErrorCode err_code)
    {
        remote_ref_cnt_.fetch_sub(1, std::memory_order_relaxed);
        SetError(err_code);
    }

    void SetLocalOrRemoteError(CcErrorCode err_code)
    {
        if (RemoteRefCnt() > 0)
        {
            SetRemoteError(err_code);
        }
        else
        {
            SetError(err_code);
        }
    }

    CcErrorCode ErrorCode() const
    {
        return error_code_.load(std::memory_order_relaxed);
    }

    const std::string ErrorMsg() const
    {
        auto it = cc_error_messages.find(ErrorCode());
        if (it != cc_error_messages.end())
        {
            return it->second;
        }
        return "CcErrorCode:" + std::to_string(static_cast<int>(ErrorCode()));
    }

    void SetRefCnt(uint32_t cnt)
    {
        if (cnt <= 0)
        {
            ClearRefCnt();
            return;
        }
        ref_cnted_ = true;
        ref_cnt_.store(cnt, std::memory_order_relaxed);
        remote_ref_cnt_.store(0, std::memory_order_relaxed);
    }

    void ClearRefCnt()
    {
        ref_cnted_ = false;
        ref_cnt_.store(0, std::memory_order_relaxed);
        remote_ref_cnt_.store(0, std::memory_order_relaxed);
    }

    uint32_t RefCnt() const
    {
        return ref_cnt_.load(std::memory_order_relaxed);
    }

    void IncrementRemoteRef()
    {
        remote_ref_cnt_.fetch_add(1, std::memory_order_acquire);
    }

    uint32_t RemoteRefCnt()
    {
        return remote_ref_cnt_.load(std::memory_order_relaxed);
    }

    uint32_t LocalRefCnt()
    {
        uint32_t total = ref_cnt_.load(std::memory_order_relaxed);
        uint32_t remote = remote_ref_cnt_.load(std::memory_order_relaxed);
        return total - remote > 0 ? total - remote : 0;
    }

    void Reset()
    {
        error_code_.store(CcErrorCode::NO_ERROR, std::memory_order_relaxed);
        ClearRefCnt();
#ifdef EXT_TX_PROC_ENABLED
        is_blocking_ = false;
#endif
        is_finished_.store(false, std::memory_order_release);

        // Reset `result_status_` to `0` if `result_status_` == `-1 (Timeout)
        // Otherwise, Nothing to do.
        int32_t expect = -1;
        result_status_.compare_exchange_strong(
            expect, expect + 1, std::memory_order_release);
    }

    void ResetTxm(TransactionExecution *txm)
    {
        assert(txm != nullptr);
        txm_ = txm;
    }

    TransactionExecution *Txm()
    {
        return txm_;
    }

#ifdef EXT_TX_PROC_ENABLED
    void SetToBlock()
    {
        is_blocking_ = true;
    }

protected:
    /**
     * @brief True, if the tx is expecting this result and is blocked on it.
     * Upon finishing, the cc handler result enlists blocked tx to resume
     * execution.
     *
     */
    bool is_blocking_{false};
#endif

protected:
    std::atomic<bool> is_finished_{false};

    // Use this variable to guarantee only one thread can set the
    // CcHandlerResult once remote response is received. Positive
    // numbers(1,2,3...) denotes how many responses are being handled by stream
    // thread, and txm can not timeout when result_status_>0. Negative
    // number(-1) denotes timeout thread is going to set CcHandlerResult.
    std::atomic<int32_t> result_status_{0};

    std::atomic<CcErrorCode> error_code_{CcErrorCode::NO_ERROR};
    bool ref_cnted_{false};
    std::atomic<uint32_t> ref_cnt_{0};
    std::atomic<uint32_t> remote_ref_cnt_{0};
    // The parent tx state machine who sends a cc request and waits on this
    // handler result. The handler result is bound to a fixed tx machine. The tx
    // machine, however, may be re-used repeatedly for different user-level
    // tx's.
    TransactionExecution *txm_{nullptr};
};

template <typename T>
class CcHandlerResult : public CcHandlerResultBase
{
public:
    CcHandlerResult(TransactionExecution *txm)
        : CcHandlerResultBase(txm), result_()
    {
    }

    CcHandlerResult(const CcHandlerResult &rhs) = delete;

    CcHandlerResult(CcHandlerResult &&rhs) noexcept
        : CcHandlerResultBase(rhs.txm_),
          result_(std::move(rhs.result_)),
          post_lambda_(std::move(rhs.post_lambda_))
    {
        is_finished_ = rhs.is_finished_.load(std::memory_order_relaxed);
        result_status_ = rhs.result_status_.load(std::memory_order_relaxed);
        error_code_ = rhs.error_code_.load(std::memory_order_relaxed);
        ref_cnted_ = rhs.ref_cnted_;
        ref_cnt_ = rhs.ref_cnt_.load(std::memory_order_relaxed);
        remote_ref_cnt_ = rhs.remote_ref_cnt_.load(std::memory_order_relaxed);
#ifdef EXT_TX_PROC_ENABLED
        is_blocking_ = rhs.is_blocking_;
#endif
    }

    CcHandlerResult &operator=(const CcHandlerResult &rhs) = delete;

    void Reset()
    {
        CcHandlerResultBase::Reset();
        runtime_resume_func_ = nullptr;
    }

    void SetValue(const T &val) = delete;

    void SetValue(T &&val)
    {
        result_ = std::move(val);
    }

    const T &Value() const
    {
        return result_;
    }

    T &Value()
    {
        return result_;
    }

    bool SetFinished() override;

    bool SetError(CcErrorCode err_code) override;

    bool SetResultByStreamThread() override
    {
        int32_t expect = 0;
        while (
            !result_status_.compare_exchange_strong(expect,
                                                    expect + 1,
                                                    std::memory_order_acquire,
                                                    std::memory_order_relaxed))
        {
            if (expect == -1)
            {
                // Txm being timeout, reject this response message.
                return false;
            }
        }
        return true;
    }

    bool SetResultByTimeoutThread() override
    {
        if (result_status_.load(std::memory_order_acquire) == -1)
        {
            return true;
        }

        int32_t expect = 0;
        bool succeed = result_status_.compare_exchange_strong(
            expect, -1, std::memory_order_acquire, std::memory_order_relaxed);
        return succeed;
    }

    void DecreaseCurrentHandlingResponse()
    {
        uint32_t res = result_status_.fetch_sub(1, std::memory_order_release);
        assert(res > 0);
        // This silences the -Wunused-but-set-variable warning without any
        // runtime overhead.
        (void) res;
    }

    void UnsetByTimeoutThread()
    {
        result_status_.store(0, std::memory_order_release);
    }

    /**
     * @brief Forces the handler result to an error state.
     *
     * The method is used exclusively to force a tx to stop waiting for a remote
     * cc request's response and to set the request's result to an error state.
     * In case the remote response returns at the same time (which is unlikely),
     * ForceError() either precedes or follows the response's two consecutive
     * invocations of (a) SetValue()/SetError() and (b) SetFinished(). If
     * ForceError() follows, the prior SetFinish() will prevent it from setting
     * the error code. The request finishes normally. If ForceError() prcedes,
     * it will prevent the invocation of SetFinish(), but cannot prevent
     * SetError() or SetValue(). This means that when the tx moves on to cancel
     * the current request, the request's result is guaranteed to have an error
     * code, but the result value may be set by the remote response, or the
     * error code is the code returned by the remote reseponse. At any rate,
     * this is still correct in that the request finishes with an error and the
     * tx moves on as expected.
     *
     * @return true, if the result is forced to be errored; false, if the result
     * has already been set by the remote request's resposne.
     */
    bool ForceError() override;

private:
    T result_;

public:
    std::function<void(CcHandlerResult<T> *)> post_lambda_;
    const std::function<void()> *runtime_resume_func_{};
};
}  // namespace txservice
