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
#include "cc/cc_handler_result.h"

#include <atomic>
#include <string>
#include <vector>

#include "tx_execution.h"
#include "tx_trace.h"

namespace txservice
{
template <typename T>
bool CcHandlerResult<T>::SetFinished()
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        static_cast<T *>(&result_),
        [this]() -> std::string
        {
            if (this->txm_ == nullptr)
            {
                return std::string("\"txm_\":\"nullptr\"");
            }
            else
            {
                return std::string("\"tx_number\":")
                    .append(std::to_string(this->txm_->TxNumber()))
                    .append(",\"tx_term\":")
                    .append(std::to_string(this->txm_->TxTerm()))
                    .append(",\"ref_cnt_\":")
                    .append(std::to_string(
                        this->ref_cnt_.load(std::memory_order_acquire)));
            }
        });
    TX_TRACE_DUMP(static_cast<T *>(&result_));

#ifdef EXT_TX_PROC_ENABLED
    // Warning! As soon as is_finished_ is set, the txm can be forwarded and
    // Reset, which means CcHandlerResult could be reset.
    // Copy the runtime_resume_func_ since it could be set to empty once
    // is_finished_ is set.
    const std::function<void()> *resume_func = runtime_resume_func_;
    TransactionExecution *txm = txm_;
    bool is_blocking = is_blocking_;
#endif

    if (ref_cnted_)
    {
        auto r = ref_cnt_.fetch_sub(1, std::memory_order_relaxed);
        if (r == 1)
        {
            bool expect = false;
            if (is_finished_.compare_exchange_strong(
                    expect, true, std::memory_order_acq_rel))
            {
                if (post_lambda_)
                {
                    post_lambda_(this);
                }
#ifdef EXT_TX_PROC_ENABLED
                if (resume_func)
                {
                    (*resume_func)();
                }
                else if (txm != nullptr && is_blocking)
                {
                    txm->Enlist();
                }
#endif
                return true;
            }
        }
    }
    else
    {
        bool expect = false;
        if (is_finished_.compare_exchange_strong(
                expect, true, std::memory_order_acq_rel))
        {
            if (post_lambda_)
            {
                post_lambda_(this);
            }

#ifdef EXT_TX_PROC_ENABLED
            if (resume_func)
            {
                (*resume_func)();
            }
            else if (txm != nullptr && is_blocking)
            {
                txm->Enlist();
            }
#endif
            return true;
        }
    }

    return false;
};

template <typename T>
bool CcHandlerResult<T>::SetError(CcErrorCode err_code)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        (int8_t) err_code,
        [this]() -> std::string
        {
            if (this->txm_ == nullptr)
            {
                return std::string("\"txm_\":\"nullptr\"");
            }
            else
            {
                return std::string("\"tx_number\":")
                    .append(std::to_string(this->txm_->TxNumber()))
                    .append("\"tx_term\":")
                    .append(std::to_string(this->txm_->TxTerm()));
            }
        });
    CcErrorCode no_error = CcErrorCode::NO_ERROR;
    error_code_.compare_exchange_strong(
        no_error, err_code, std::memory_order_relaxed);
    return SetFinished();
};

template <typename T>
bool CcHandlerResult<T>::ForceError()
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        (int8_t) 0,
        [this]() -> std::string
        {
            if (this->txm_ == nullptr)
            {
                return std::string("\"txm_\":\"nullptr\"");
            }
            else
            {
                return std::string("\"tx_number\":")
                    .append(std::to_string(this->txm_->TxNumber()))
                    .append("\"tx_term\":")
                    .append(std::to_string(this->txm_->TxTerm()));
            }
        });

#ifdef EXT_TX_PROC_ENABLED
    // Warning! As soon as is_finished_ is set, the txm can be forwarded and
    // Reset, which means CcHandlerResult could be reset.
    // Copy the runtime_resume_func_ since it could be set to empty once
    // is_finished_ is set.
    const std::function<void()> *resume_func = runtime_resume_func_;
    TransactionExecution *txm = txm_;
    bool is_blocking = is_blocking_;
#endif

    bool expect = false;
    bool success = is_finished_.compare_exchange_strong(
        expect, true, std::memory_order_acq_rel);

    if (success)
    {
        CcErrorCode no_error = CcErrorCode::NO_ERROR;
        error_code_.compare_exchange_strong(
            no_error, CcErrorCode::FORCE_FAIL, std::memory_order_acq_rel);

        if (post_lambda_)
        {
            post_lambda_(this);
        }

#ifdef EXT_TX_PROC_ENABLED
        if (resume_func)
        {
            (*resume_func)();
        }
        else if (txm != nullptr && is_blocking)
        {
            txm->Enlist();
        }
#endif
    }

    return success;
};

template class CcHandlerResult<InitTxResult>;
template class CcHandlerResult<ReadKeyResult>;
template class CcHandlerResult<ScanNextResult>;
template class CcHandlerResult<ScanOpenResult>;
template class CcHandlerResult<RangeScanSliceResult>;
template class CcHandlerResult<AcquireAllResult>;
template class CcHandlerResult<std::vector<AcquireKeyResult>>;
template class CcHandlerResult<Void>;
template class CcHandlerResult<TxId>;
template class CcHandlerResult<PostProcessResult>;
template class CcHandlerResult<bool>;
template class CcHandlerResult<uint64_t>;
template class CcHandlerResult<int8_t>;
template class CcHandlerResult<std::string>;
template class CcHandlerResult<ObjectCommandResult>;
template class CcHandlerResult<std::vector<int64_t>>;
template class CcHandlerResult<UploadBatchResult>;
template class CcHandlerResult<PackSkError>;
}  // namespace txservice
