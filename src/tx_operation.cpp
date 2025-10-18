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
#include "tx_operation.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc/cc_handler_result.h"
#include "cc_handler.h"
#include "cc_map.h"
#include "error_messages.h"  //CcErrorCode
#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "log_type.h"
#include "range_record.h"
#include "range_slice.h"
#include "sequences/sequences.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_service_common.h"
#include "tx_trace.h"
#include "tx_util.h"
#include "tx_worker_pool.h"
#include "type.h"

DECLARE_bool(cmd_read_catalog);

namespace txservice
{
class AbortReason
{
public:
    enum : uint32_t
    {
        kUploadVersion = 0,
        kSetCommitTs = 1,
        kNegativeCommitTs = 2,
        kUpdateMaxCommitTs = 3,
        kRereadBlank = 4,
        kNewVersionCreated = 5,
        kUpdateCommitLowerBound = 6,
        kConflictTsIsSmaller = 7,
        kMax
    };
};

/**
 * @brief Re-process the failed operation if the error code is -1, which
 * indicated a term change or send message failure. Retry at most RETRY_NUM
 * number of times. The first retry waits 0 secs, the second one waits 2
 * seconds, the third one waits 4 seconds and so on.
 *
 */
void TransactionOperation::ReRunOp(TransactionExecution *txm)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        txm,
        (
            [txm, this]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()))
                    .append(",\"retry_num_\":")
                    .append(std::to_string(this->retry_num_));
            }));
    if (retry_num_ <= 0)
    {
        return;
    }

    // sleep for a while and then execute the new operation.
    // sleep time is based on retry_num_.
    txm->sleep_op_.sleep_secs_ =
        RETRY_NUM >= retry_num_ ? (RETRY_NUM - retry_num_) * 2 : 2;
    is_running_ = false;
    retry_num_--;

    // put sleep operation on top of the stack.
    txm->PushOperation(&txm->sleep_op_);
    txm->StartTiming();
}

ReadOperation::ReadOperation(
    TransactionExecution *txm,
    CcHandlerResult<ReadKeyResult> *lock_range_bucket_result)
    : hd_result_(txm), lock_range_bucket_result_(lock_range_bucket_result)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ReadOperation::Reset()
{
    hd_result_.Value().Reset();
    hd_result_.Reset();
    local_cache_miss_ = false;
    lock_range_bucket_result_->Value().Reset();
    lock_range_bucket_result_->Reset();

    op_start_ = metrics::TimePoint::max();

    is_running_ = false;
}

void ReadOperation::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        if (!read_tx_req_->read_local_)
        {
            // Just returned from lock_bucket_op_, check lock_bucket_result_.
            assert(lock_range_bucket_result_->IsFinished());
            if (lock_range_bucket_result_->IsError())
            {
                // There is an error when getting the input key's bucket or
                // range. The read operation is set to be errored.
                hd_result_.SetError(lock_range_bucket_result_->ErrorCode());
                hd_result_.ForceError();

                txm->PostProcess(*this);
                return;
            }
        }

        // Need to make sure current node is still leader since we will visit
        // bucket meta data which is only valid if current node is still ng
        // leader.
        if (!txm->CheckLeaderTerm() && !txm->CheckStandbyTerm())
        {
            hd_result_.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            hd_result_.ForceError();
            txm->PostProcess(*this);
            return;
        }

        LOG(INFO) << "read op process";
        txm->Process(*this);
        return;
    }

    const CcEntryAddr &cce_addr = hd_result_.Value().cce_addr_;

    if (hd_result_.IsFinished())
    {
        LOG(INFO) << "read op done";
        if (hd_result_.ErrorCode() == CcErrorCode::PIN_RANGE_SLICE_FAILED ||
            hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
            hd_result_.ErrorCode() == CcErrorCode::DATA_STORE_ERR)
        {
            // The read request was directed to a non-leader node. Updates
            // the leader cache.
            if (hd_result_.ErrorCode() ==
                CcErrorCode::REQUESTED_NODE_NOT_LEADER)
            {
                Sharder::Instance().UpdateLeader(cce_addr.NodeGroupId());
            }
            if (retry_num_ > 0)
            {
                hd_result_.Value().Reset();
                hd_result_.Reset();
                ReRunOp(txm);
                return;
            }
        }

        if (read_tx_req_->tab_name_->IsHashPartitioned() &&
            hd_result_.ErrorCode() == CcErrorCode::OUT_OF_MEMORY)
        {
            // For hash partition, keep retrying since checkpoint will
            // clean up memory for new insert.
            // For range partition, keep retrying with range read lock might
            // block checkpoint, which will cause dirty data fail to flush, and
            // we will keep getting OOM error.
            retry_num_++;
            hd_result_.Value().Reset();
            hd_result_.Reset();
            ReRunOp(txm);
            return;
        }
        txm->PostProcess(*this);
    }
    else
    {
        CODE_FAULT_INJECTOR("read_operation_timeout", {
            LOG(INFO) << "FaultInject  read_operation_timeout";
            FaultInject::Instance().InjectFault("read_operation_timeout",
                                                "remove");

            bool force_error = hd_result_.ForceError();
            if (force_error)
            {
                txm->PostProcess(*this);
            }
            return;
        });

        if (txm->IsTimeOut())
        {
            if (!hd_result_.Value().is_local_)
            {
                if (!txm->CheckLeaderTerm())
                {
                    hd_result_.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
                    txm->PostProcess(*this);
                    return;
                }

                if (cce_addr.Term() > 0)
                {
                    // ack is received, but timeout happens.

                    // Trigger deadlock check.
                    DeadLockCheck::RequestCheck();

                    txm->cc_handler_->BlockCcReqCheck(
                        txm->TxNumber(),
                        txm->TxTerm(),
                        txm->CommandId(),
                        cce_addr,
                        &hd_result_,
                        ResultTemplateType::ReadKeyResult);
                }
                else
                {
                    // ack is not received, but timeout happens.

                    // For non-blocking concurrency control protocols, the read
                    // request is expected to return instantly. For lock-based
                    // protocols, if the read request is blocked, the cc node
                    // will send an acknowledgement to update the key's term. In
                    // either case, if the read key's term is not set, the tx
                    // has not received any response or acknowledgement from the
                    // key's cc node group. The read request is forced to be
                    // errored upon timeout.
                    // FIXME(lzx): Is it more appropriate to retry?
                    // If the tx node fails, also force the tx to abort
                    // instantly.
                    bool force_error = hd_result_.ForceError();
                    if (force_error)
                    {
                        txm->PostProcess(*this);
                    }
                    // If forcing error fails, it means that the remote response
                    // returns normally and the tx has been moved from the
                    // waiting queue to the execution queue. Does not continue
                    // execution. The tx will be re-executed when the tx
                    // processor visits it in the execution queue.
                }
            }
            else
            {
                // local request timeout
                DeadLockCheck::RequestCheck();
            }
        }
    }
    // TODO: for locking-based protocols, even though the tx may be blocked
    // arbitrarily long after the read request is acknowledged, we still
    // need to periodically check liveness of the remote node and force the
    // tx to cancel if the remote node is unresponsive.
}

void ReadLocalOperation::Reset()
{
    key_ = nullptr;
    table_name_ =
        TableName{empty_sv, TableType::RangePartition, TableEngine::None};
    rec_ = nullptr;
    hd_result_ = nullptr;
    execute_immediately_ = true;
}

void ReadLocalOperation::Forward(txservice::TransactionExecution *txm)
{
    if (hd_result_->IsFinished())
    {
        if (!txm->CheckLeaderTerm() && !txm->CheckStandbyTerm())
        {
            hd_result_->SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            hd_result_->ForceError();
            txm->PostProcess(*this);
        }
        else if (hd_result_->IsError())
        {
            assert(hd_result_->ErrorCode() ==
                       CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT ||
                   hd_result_->ErrorCode() == CcErrorCode::DATA_STORE_ERR);
            // If acquire range read lock blocked by DDL, check if tx has
            // already acquired other range read lock. If so we need to abort
            // tx since it might cause dead lock with range split. If this tx
            // has not acquired any range read lock, we can safely retry here.
            const auto &rset = txm->rw_set_.MetaDataReadSet();
            for (const auto &[cce_addr, read_entry_pair] : rset)
            {
                if (read_entry_pair.second != catalog_ccm_name_sv &&
                    read_entry_pair.second != cluster_config_ccm_name_sv)
                {
                    // Abort tx.
                    txm->PostProcess(*this);
                    return;
                }
            }
            hd_result_->Value().Reset();
            hd_result_->Reset();
            execute_immediately_ = false;
            txm->Process(*this);
            return;
        }
        else
        {
            // Read local succeeded
            txm->PostProcess(*this);
        }
    }
    else
    {
        // TODO(zkl): wait some time and timeout, for async FetchTableRanges
        //  from KV.
        // The get-range request has not finished. The caller operation
        // of this LockRangeOperation cannot proceed without knowing the input
        // key's range. Since lock read range is always a local request, we
        // don't need to check for timeout.
        return;
    }
}

PostReadOperation::PostReadOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
}

void PostReadOperation::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void PostReadOperation::Reset(const CcEntryAddr *cce_addr,
                              const ReadSetEntry *read_set_entry)
{
    cce_addr_ = cce_addr;
    read_set_entry_ = read_set_entry;
    hd_result_.Reset();
}

void PostReadOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
}

AcquireWriteOperation::AcquireWriteOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void AcquireWriteOperation::Reset(size_t acquire_write_cnt, size_t wentry_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(acquire_write_cnt);

    std::vector<AcquireKeyResult> &acquire_key_vec = hd_result_.Value();
    size_t old_size = acquire_key_vec.size();
    acquire_key_vec.resize(acquire_write_cnt);
    for (size_t idx = old_size; idx < acquire_write_cnt; ++idx)
    {
        acquire_key_vec[idx].remote_ack_cnt_ = &remote_ack_cnt_;
    }

    remote_ack_cnt_.store(0, std::memory_order_relaxed);
    acquire_write_entries_.resize(wentry_cnt);

    rset_has_expired_ = false;
    op_start_ = metrics::TimePoint::max();
}

void AcquireWriteOperation::AggregateAcquiredKeys(TransactionExecution *txm)
{
    std::vector<AcquireKeyResult> &acquire_key_vec = hd_result_.Value();
    size_t res_idx = 0;
    for (WriteSetEntry *write_entry : acquire_write_entries_)
    {
        AcquireKeyResult &acquire_key_res = acquire_key_vec[res_idx++];
        CcEntryAddr &addr = acquire_key_res.cce_addr_;

        int64_t term = addr.Term();
        if (term < 0)
        {
            write_entry->cce_addr_.SetCceLock(0, -1, 0);
        }
        else if (acquire_key_res.commit_ts_ == 0)
        {
            DLOG(INFO) << "txm fails to acquire write lock due to network "
                          "partition or deadlock, txn: "
                       << txm->TxNumber();
            // This branch is for acquiring write lock fails. We need to set
            // term to -1 in wset because we do not want to do
            // CcHandler::PostWrite() on this entry, also set term in
            // AcquireKeyResult because we want to update acquire_write_cnt
            // during TransactionExecution::Abort().
            write_entry->cce_addr_.SetCceLock(0, -1, 0);
            addr.SetTerm(-1);
        }
        else
        {
            // Assigns to the write entry the cc entry address obtained
            // in the acquire phase.
            write_entry->cce_addr_ = addr;
            uint64_t read_version = txm->rw_set_.DedupRead(addr);
            if (read_version > 0 && read_version != acquire_key_res.commit_ts_)
            {
                // Each write-set key acquires a write lock and gets the
                // key's last validation ts and commit ts. If the write
                // key has been read before and the key's commit ts
                // mismatches the prior version, this is not a
                // repeatable read.
                rset_has_expired_ = true;
                DLOG(INFO) << "set rset_has_expired_, txn: " << txm->TxNumber()
                           << "; read_version: " << read_version
                           << "; acquire_key_res.commit_ts_: "
                           << acquire_key_res.commit_ts_;
            }
        }

        for (auto &[forward_shard_code, cce_addr] : write_entry->forward_addr_)
        {
            AcquireKeyResult &acquire_key_res = acquire_key_vec[res_idx++];
            CcEntryAddr &addr = acquire_key_res.cce_addr_;
            term = addr.Term();
            if (term < 0)
            {
                cce_addr.SetCceLock(0, -1, 0);
            }
            else if (acquire_key_res.commit_ts_ == 0)
            {
                // acqurie write failed on forward addr.
                cce_addr.SetCceLock(0, -1, 0);
                // Set term to -1 so that post write will not be sent to this
                // addr.
                addr.SetTerm(-1);
            }
            else
            {
                // Assigns to the write entry the cc entry address obtained
                // in the acquire phase.
                cce_addr = addr;
            }

            // No need to dedup forwarded req since they are not visible to read
            // op.
        }
    }
}

void AcquireWriteOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            Sharder::Instance().UpdateLeaders();
            if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        AggregateAcquiredKeys(txm);
        txm->PostProcess(*this);
    }
    else
    {
        CODE_FAULT_INJECTOR("acquire_operation_timeout", {
            LOG(INFO)
                << "FaultInject  acquire_operation_timeout remote_ack_cnt_:"
                << remote_ack_cnt_;
            FaultInject::Instance().InjectFault("acquire_operation_timeout",
                                                "remove");

            bool success = hd_result_.ForceError();
            if (success)
            {
                AggregateAcquiredKeys(txm);
                txm->PostProcess(*this);
            }
            return;
        });

        // TODO(zkl): AcquireWriteOp should wait for local requests to finish
        if (txm->IsTimeOut())
        {
            if (hd_result_.LocalRefCnt() != 0)
            {
                // Local requests haven't finished. Trigger deadlock check.
                DeadLockCheck::RequestCheck();
            }
            else if (!txm->CheckLeaderTerm())
            {
                bool success = hd_result_.ForceError();
                if (success)
                {
                    AggregateAcquiredKeys(txm);
                    txm->PostProcess(*this);
                }
            }
            else if (remote_ack_cnt_.load(std::memory_order_acquire) == 0)
            {
                // All remote acks are received, but timeout still happens.

                // Trigger deadlock check.
                DeadLockCheck::RequestCheck();

                // Check if the blocked keys are still blocked in the lock
                // queue.
                std::vector<AcquireKeyResult> &vct_akr = hd_result_.Value();
                for (size_t i = 0; i < vct_akr.size(); i++)
                {
                    AcquireKeyResult &akr = vct_akr[i];

                    if (!akr.IsRemoteHdResultSet(std::memory_order_acquire) &&
                        akr.cce_addr_.Term() > 0 /*&& akr.commit_ts_ == 0*/)
                    {
                        txm->cc_handler_->BlockCcReqCheck(
                            txm->TxNumber(),
                            txm->TxTerm(),
                            txm->CommandId(),
                            akr.cce_addr_,
                            &hd_result_,
                            ResultTemplateType::AcquireKeyResult,
                            i);
                    }
                }
            }
            else
            {
                // Some of the acks are not received, but timeout happens.

                // FIXME(lzx): Is it more appropriate to retry if
                // remote_ack_cnt_>0 ? If the tx node fails, force the tx to
                // abort instantly.
                bool success = hd_result_.ForceError();
                if (success)
                {
                    AggregateAcquiredKeys(txm);
                    txm->PostProcess(*this);
                }
                // Else, all acquire-write requests finish normally. The tx must
                // have been moved from the waiting queue to the execution
                // queue. Does not forword the tx now, as it will be re-executed
                // when the tx processor visits the execution queue.
            }
            // else, hd_result_ is being set by stream thread, do nothing in
            // this turn.
        }
    }
}

void LockWriteRangeBucketsOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
    }
    else if (lock_result_->IsFinished())
    {
        // Make sure current node is still ng leader since we will visit
        // bucket info meta data in post process which is only valid when
        // current node is still ng leader.
        if (!txm->CheckLeaderTerm())
        {
            lock_result_->SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            lock_result_->ForceError();
            txm->PostProcess(*this);
        }
        else if (lock_result_->IsError())
        {
            assert(lock_result_->ErrorCode() ==
                   CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT);
            // If acquire range/bucket read lock blocked by DDL, check if tx has
            // already acquired other range/bucket read lock. If so we need to
            // abort tx since it might cause dead lock with bucket migration. If
            // this tx has not acquired any range/buckets read lock, we can
            // safely retry here.
            const auto &rset = txm->rw_set_.MetaDataReadSet();
            for (const auto &[cce_addr, read_entry_pair] : rset)
            {
                if (read_entry_pair.second != catalog_ccm_name_sv &&
                    read_entry_pair.second != range_bucket_ccm_name_sv &&
                    read_entry_pair.second != cluster_config_ccm_name_sv)
                {
                    // Abort tx.
                    txm->PostProcess(*this);
                    return;
                }
            }
            lock_result_->Value().Reset();
            lock_result_->Reset();
            is_running_ = false;
            execute_immediately_ = false;
            txm->Process(*this);
            return;
        }
        else
        {
            txm->PostProcess(*this);
        }
    }
}

// Check if the write key needs to be forward written.
void LockWriteRangeBucketsOp::Advance(TransactionExecution *txm)
{
    if (table_it_->first.IsHashPartitioned())
    {
        if (!txm->CheckLeaderTerm())
        {
            lock_result_->SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            lock_result_->ForceError();
            txm->PostProcess(*this);
            return;
        }
        const BucketInfo *bucket_info = nullptr;
        while (write_key_it_ != write_key_end_)
        {
            // Check whether the bucket of this write key has been locked
            // before locking bucket. If yes, just use it.
            const TxKey &write_key = write_key_it_->first;
            uint16_t bucket_id =
                Sharder::MapKeyHashToBucketId(write_key.Hash());
            bucket_info = txm->FastToGetBucket(bucket_id);
            if (bucket_info != nullptr)
            {
                NodeGroupId bucket_ng = bucket_info->BucketOwner();
                NodeGroupId new_bucket_ng = bucket_info->DirtyBucketOwner();

                // Updates the sharding codes of this key according to bucket
                // info.
                const TxKey &write_tx_key = write_key_it_->first;
                size_t hash = write_tx_key.Hash();
                WriteSetEntry &write_entry = write_key_it_->second;
                write_entry.key_shard_code_ =
                    (bucket_ng << 10) | (hash & 0x3FF);
                // If current bucket is migrating, forward to new range owner.
                if (new_bucket_ng != UINT32_MAX)
                {
                    write_entry.forward_addr_.try_emplace(
                        (new_bucket_ng << 10) | (hash & 0x3FF));
                    txm->rw_set_.IncreaseFowardWriteCnt(1);
                }
                write_key_it_++;
            }
            else
            {
                break;
            }
        }
    }
    else
    {
        // Advances the write key iterator such that it points to the first key
        // belonging to the next range.
        TxKey range_end_key = txm->range_rec_.GetRangeInfo()->EndTxKey();
        auto next_range_start = write_key_it_;
        if (range_end_key.Type() == KeyType::PositiveInf)
        {
            next_range_start = write_key_end_;
        }
        else
        {
            TableWriteSet &table_write_set = table_it_->second.second;
            next_range_start = table_write_set.lower_bound(range_end_key);
        }

        NodeGroupId range_ng = txm->range_rec_.GetRangeOwnerNg()->BucketOwner();
        NodeGroupId new_bucket_ng =
            txm->range_rec_.GetRangeOwnerNg()->DirtyBucketOwner();

        const std::vector<const BucketInfo *> *splitting_range_ngs =
            txm->range_rec_.GetNewRangeOwnerNgs();

        // Updates the sharding codes of the write-set keys belonging to this
        // range. The higher 22 bits represent the range ID.
        NodeGroupId new_range_ng = UINT32_MAX;
        NodeGroupId new_range_new_bucket_ng = UINT32_MAX;
        size_t new_range_idx = 0;

        auto *range_info = txm->range_rec_.GetRangeInfo();
        while (write_key_it_ != next_range_start)
        {
            const TxKey &write_tx_key = write_key_it_->first;
            WriteSetEntry &write_entry = write_key_it_->second;
            size_t hash = write_tx_key.Hash();
            write_entry.key_shard_code_ = (range_ng << 10) | (hash & 0x3FF);
            // If current range is migrating, forward to new range owner.
            if (new_bucket_ng != UINT32_MAX)
            {
                write_entry.forward_addr_.try_emplace((new_bucket_ng << 10) |
                                                      (hash & 0x3FF));
            }

            // If range is splitting and the key will fall on a new range after
            // split is finished, register forward_addr_ to indicate
            // entry needs to be double written.
            while (range_info->IsDirty() &&
                   new_range_idx < range_info->NewKey()->size() &&
                   !(write_tx_key < range_info->NewKey()->at(new_range_idx)))
            {
                new_range_ng =
                    splitting_range_ngs->at(new_range_idx)->BucketOwner();
                new_range_new_bucket_ng =
                    splitting_range_ngs->at(new_range_idx++)
                        ->DirtyBucketOwner();
            }
            if (new_range_ng != UINT32_MAX)
            {
                if (new_range_ng != range_ng)
                {
                    write_entry.forward_addr_.try_emplace((new_range_ng << 10) |
                                                          (hash & 0x3FF));
                }
                // If the new range is migrating, forward to the new owner of
                // new range.
                if (new_range_new_bucket_ng != UINT32_MAX &&
                    new_range_new_bucket_ng != range_ng)
                {
                    write_entry.forward_addr_.try_emplace(
                        (new_range_new_bucket_ng << 10) | (hash & 0x3FF));
                }
            }

            txm->rw_set_.IncreaseFowardWriteCnt(
                write_entry.forward_addr_.size());
            ++write_key_it_;
        }
    }

    if (write_key_it_ == write_key_end_)
    {
        // Has acquired bucket locks for all write keys in the current table.
        // Moves to the next table, if there are any.
        ++table_it_;
        if (table_it_ != table_end_)
        {
            write_key_it_ = table_it_->second.second.begin();
            write_key_end_ = table_it_->second.second.end();
        }
    }
}

SetCommitTsOperation::SetCommitTsOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void SetCommitTsOperation::Reset()
{
    hd_result_.Reset();
}

void SetCommitTsOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    // SetCommitTsOperation is a local call, should always succeeds.
    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
}

ValidateOperation::ValidateOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
}

void ValidateOperation::Reset(size_t read_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(read_cnt);
    hd_result_.Value().Clear();
    if (read_cnt == 0)
    {
        hd_result_.SetFinished();
    }

    op_start_ = metrics::TimePoint::max();
}

bool ValidateOperation::IsError()
{
    // If validating read keys returns one or more conflicting tx's who are
    // holding write locks on the read keys during validation, validation is
    // considered failed and the tx is aborted. In theory, it's possible to
    // negotiate conflicting tx's such that if conflicting tx's agree to
    // commit at timestamps later than this (read) tx's commit timestamp,
    // validation still succeeds and this tx is allowed to commit. For
    // simplicity, we skip the negotiation step for now. Note that for 2PL,
    // the validation phase releases read locks acquired earlier. Since read
    // locks block writes, validation always succeeds.

    return hd_result_.IsFinished() &&
           (hd_result_.IsError() || hd_result_.Value().Size() > 0);
}

void ValidateOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        // All validation requests have returned, either successfully or
        // with error codes. Post-processing skips read-set keys.
        bool read_lock_term_valid = txm->rw_set_.ClearDataReadSet();
        if (!read_lock_term_valid)
        {
            // Two read locks on the same node group have different term, the
            // Validate Op must have failed.
            assert(hd_result_.IsError());
        }

        // validation cannot re-run since the remote locks of the readset
        // are lost during auto-failover, we should abort the transaction if
        // remote node, which contains read entries, is dead.
        txm->PostProcess(*this);
    }
    else if (hd_result_.LocalRefCnt() == 0 && txm->IsTimeOut())
    {
        bool success = hd_result_.ForceError();
        if (success)
        {
            txm->PostProcess(*this);
        }

        // Else, all post-read requests finish normally, meaning the tx has
        // been moved from the waiting queue to the execution queue. Does
        // not forword the tx now, as it will be re-executed when the tx
        // processor visits the execution queue.
    }
}

WriteToLogOp::WriteToLogOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

// Update leader cache using leader info from log service response.
static void UpdateLeadersFromLogInfo(
    const google::protobuf::Map<uint32_t, txlog::LeaderInfo> &ng_leader_info)
{
    if (ng_leader_info.empty())
    {
        return;
    }

    // Fetch a snapshot of ng configs (copy) for safe concurrent access.
    auto ng_configs = Sharder::Instance().GetNodeGroupConfigs();

    for (const auto &entry : ng_leader_info)
    {
        uint32_t ng_id = entry.first;
        const txlog::LeaderInfo &leader_info = entry.second;
        const std::string &leader_ip = leader_info.ip();
        const uint32_t leader_port = leader_info.port();
        int64_t leader_term = leader_info.term();

        auto ng_it = ng_configs.find(ng_id);
        if (ng_it == ng_configs.end())
        {
            DLOG(WARNING) << "Log leader info for unknown ng_id=" << ng_id;
            continue;
        }

        // Find matching node_id by hostname/IP and port match
        const std::vector<NodeConfig> &node_configs = ng_it->second;
        uint32_t leader_node_id = UINT32_MAX;
        for (const auto &node_config : node_configs)
        {
            // The log replay service listens on GET_LOG_REPLAY_RPC_PORT(port_)
            // Compare both host and the corresponding replay port.
            if (node_config.host_name_ == leader_ip &&
                static_cast<uint32_t>(
                    GET_LOG_REPLAY_RPC_PORT(node_config.port_)) == leader_port)
            {
                leader_node_id = node_config.node_id_;
                break;
            }
        }

        if (leader_node_id == UINT32_MAX)
        {
            // No direct match, skip but log for diagnostics
            DLOG(WARNING) << "Cannot map leader '" << leader_ip << ":"
                          << leader_port << "' to node_id for ng_id=" << ng_id;
            continue;
        }

        DLOG(INFO) << "Update leader from log service: ng_id=" << ng_id
                   << ", node_id=" << leader_node_id
                   << ", term=" << leader_term;
        Sharder::Instance().UpdateLeader(ng_id, leader_node_id, leader_term);
    }
}

void WriteToLogOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (Sharder::Instance().LeaderTerm(txm->TxCcNodeId()) > 0)
        {
            if (log_type_ == TxLogType::DATA &&
                hd_result_.ErrorCode() ==
                    CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // For DML transactions, the coordinator must keep retrying the
                // WriteLog request until getting a clear response, either
                // success or failure, or the coordinator itself is no longer
                // leader. In the last case, the committing process interrupts
                // with an unknown result, and an error message "Log service is
                // unreachable, transaction status is unknown" is returned. For
                // these result unknown txns, The coordinator must skip the
                // PostProcess and the locks on participants remain. The
                // participants ccnodes will do the PostProcess individually via
                // orphan lock recovery mechanism.
                CODE_FAULT_INJECTOR("write_log_result_unknown", {
                    LOG(INFO) << "skipping to updatetxn";
                    txm->PostProcess(*this);
                    return;
                });

                if (retry_num_ > 0)
                {
                    LOG(WARNING)
                        << "Write Log Request result unknown, "
                           "remain retrying time: "
                        << retry_num_ << ", tx_number: " << txm->TxNumber();
                    // log request return unknown status, we need to set retry
                    // flag to inform log service that this is a retried request
                    ::txlog::LogRequest &log_req = log_closure_.LogRequest();
                    ::txlog::WriteLogRequest *log_rec =
                        log_req.mutable_write_log_request();
                    log_rec->set_retry(true);
                    ReRunOp(txm);
                }
                else
                {
                    LOG(WARNING) << "Write Log Request result unknown, stop "
                                    "retrying, tx_number: "
                                 << txm->TxNumber();
                    txm->PostProcess(*this);
                }
                return;
            }
            else if (hd_result_.ErrorCode() == CcErrorCode::LOG_NODE_NOT_LEADER)
            {
                // Log group leader might be outdated. Wait for the leader
                // refresh and retry.
                if (retry_num_ > 0)
                {
                    txm->txlog_->RequestRefreshLeader(log_group_id_);
                    ReRunOp(txm);
                }
                else
                {
                    txm->PostProcess(*this);
                }
                return;
            }
        }

        // If the WritLog succeeds and log response carries node group leader
        // info, try to update leader cache.
        txlog::LogResponse &log_resp = log_closure_.LogResponse();
        brpc::Controller *cntl = log_closure_.Controller();
        if (!cntl->Failed() &&
            log_resp.response_status() ==
                ::txlog::LogResponse_ResponseStatus_Success &&
            log_resp.has_write_log_response())
        {
            const txlog::WriteLogResponse &write_log_resp =
                log_resp.write_log_response();
            const auto &ng_leader_info =
                write_log_resp.node_group_leader_info();
            UpdateLeadersFromLogInfo(ng_leader_info);
        }
        txm->PostProcess(*this);
    }
}

void WriteToLogOp::Reset()
{
    log_group_id_ = 0;
    hd_result_.Reset();
    log_closure_.Reset();
    op_start_ = metrics::TimePoint::max();
}

void WriteToLogOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

UpdateTxnStatus::UpdateTxnStatus(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void UpdateTxnStatus::Reset()
{
    hd_result_.Reset();
}

void UpdateTxnStatus::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.IsError())
        {
            // Updating the tx's status should never fail.
            txm->PostProcess(*this);
        }
        else
        {
            txm->PostProcess(*this);
        }
    }
}

InitTxnOperation::InitTxnOperation(TransactionExecution *txm)
    : log_group_id_(UINT32_MAX), tx_ng_id_(UINT32_MAX), hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void InitTxnOperation::Reset()
{
    hd_result_.Reset();
}

void InitTxnOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        LOG(INFO) << "Process InitTxnOperation " << this ;
        txm->Process(*this);
    }


    if (hd_result_.IsFinished())
    {
        LOG(INFO) << "Forward InitTxnOperation " << this << " is finished";
        txm->PostProcess(*this);
    }
}

PostProcessOp::PostProcessOp(TransactionExecution *txm)
    : stage_(&hd_result_),
      hd_result_(txm),
      meta_data_hd_result_(txm),
      catalog_post_all_hd_result_(txm)
{
}

void PostProcessOp::Reset(size_t write_cnt,
                          size_t data_read_cnt,
                          size_t meta_data_read_cnt,
                          size_t catalog_write_all_cnt,
                          bool forward_to_update_txn_op)
{
    stage_ = &hd_result_;

    forward_to_update_txn_op_ = forward_to_update_txn_op;

    hd_result_.Reset();
    hd_result_.Value().Clear();
    if (write_cnt + data_read_cnt == 0)
    {
        hd_result_.SetFinished();
    }
    else
    {
        hd_result_.SetRefCnt(write_cnt + data_read_cnt);
    }

    meta_data_hd_result_.Reset();
    meta_data_hd_result_.Value().Clear();
    if (meta_data_read_cnt == 0)
    {
        meta_data_hd_result_.SetFinished();
    }
    else
    {
        meta_data_hd_result_.SetRefCnt(meta_data_read_cnt);
    }

    catalog_write_all_cnt_ = catalog_write_all_cnt;
    catalog_post_all_hd_result_.Reset();
    catalog_post_all_hd_result_.Value().Clear();
    if (catalog_write_all_cnt == 0)
    {
        catalog_post_all_hd_result_.SetFinished();
    }
    else
    {
        catalog_post_all_hd_result_.SetRefCnt(catalog_write_all_cnt);
    }

    op_start_ = metrics::TimePoint::max();
}

void PostProcessOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (stage_ == &hd_result_)
    {
        if (hd_result_.IsFinished())
        {
            stage_ = &catalog_post_all_hd_result_;
            if (catalog_post_all_hd_result_.IsFinished())
            {
                Forward(txm);  // Immediately Forward.
            }
            else
            {
                txm->ReleaseCatalogWriteAll(catalog_post_all_hd_result_);
            }
        }
        else if (hd_result_.LocalRefCnt() == 0 && txm->IsTimeOut())
        {
            bool force_error = hd_result_.ForceError();
            if (force_error)
            {
                stage_ = &catalog_post_all_hd_result_;
                if (catalog_post_all_hd_result_.IsFinished())
                {
                    Forward(txm);  // Immediately Forward.
                }
                else
                {
                    txm->ReleaseCatalogWriteAll(catalog_post_all_hd_result_);
                }
            }
        }
    }
    else if (stage_ == &catalog_post_all_hd_result_)
    {
        if (catalog_post_all_hd_result_.IsFinished())
        {
            stage_ = &meta_data_hd_result_;
            if (meta_data_hd_result_.IsFinished())
            {
                Forward(txm);  // Immediately Forward.
            }
            else
            {
                txm->ReleaseMetaDataReadLock(meta_data_hd_result_);
            }
        }
        else if (catalog_post_all_hd_result_.LocalRefCnt() == 0 &&
                 txm->IsTimeOut())
        {
            if (txm->CheckLeaderTerm())
            {
                // The orphan lock recovery mechanism doesn't handle catalog
                // orphan wlock. It depends on the coordinator to guarantee
                // release catalog wlock.
                catalog_post_all_hd_result_.Reset();
                catalog_post_all_hd_result_.SetRefCnt(catalog_write_all_cnt_);
                txm->ReleaseCatalogWriteAll(catalog_post_all_hd_result_);
            }
            else
            {
                bool force_error = catalog_post_all_hd_result_.ForceError();
                if (force_error)
                {
                    txm->PostProcess(*this);  // Immediately PostProcess.
                }
            }
        }
    }
    else
    {
        assert(stage_ == &meta_data_hd_result_);
        if (meta_data_hd_result_.IsFinished())
        {
            txm->PostProcess(*this);
        }
    }
}

ReloadCacheOperation::ReloadCacheOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ReloadCacheOperation::Reset(uint32_t hres_ref_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(hres_ref_cnt);
}

void ReloadCacheOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
    else if (hd_result_.LocalRefCnt() == 0 && txm->IsTimeOut())
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeOut",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });
        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->PostProcess(*this);
        }
    }
}

FaultInjectOp::FaultInjectOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void FaultInjectOp::Reset()
{
    hd_result_.Reset();
}

void FaultInjectOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.Value() == true)
        {
            succeed_ = true;
            txm->PostProcess(*this);
        }
        else
        {
            succeed_ = false;
            txm->PostProcess(*this);
        }
    }
    else if (txm->IsTimeOut())
    {
        bool force_success = hd_result_.ForceError();
        if (force_success)
        {
            txm->PostProcess(*this);
        }
    }
}

ScanOpenOperation::ScanOpenOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ScanOpenOperation::Reset()
{
    hd_result_.Reset();
}

void ScanOpenOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        // Error code -1 indicates send message failed or term changed.
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
            retry_num_ > 0)
        {
            Sharder::Instance().UpdateLeaders();
            ReRunOp(txm);
            return;
        }
        else
        {
            txm->PostProcess(*this);
        }
    }
    else if (txm->IsTimeOut())
    {
        DeadLockCheck::RequestCheck();

        if (hd_result_.LocalRefCnt() > 0)
        {
            // Never force error for local request.
            return;
        }

        if (retry_num_ > 0 &&
            (txm->CheckLeaderTerm() || txm->CheckStandbyTerm()))
        {
            ReRunOp(txm);
            return;
        }

        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->PostProcess(*this);
        }
    }
}

ScanNextOperation::ScanNextOperation(TransactionExecution *txm)
    : hd_result_(txm),
      slice_hd_result_(txm),
      lock_range_result_(txm),
      unlock_range_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ScanNextOperation::Reset()
{
    alias_ = 0;
    scan_state_ = nullptr;
    range_table_name_ =
        TableName(empty_sv, TableType::RangePartition, TableEngine::None);
    op_start_ = metrics::TimePoint::max();
    ResetResult();
}

void ScanNextOperation::ResetResult()
{
    slice_hd_result_.Value().Reset();
    slice_hd_result_.Reset();
    unlock_range_result_.Reset();
    lock_range_result_.Reset();
    hd_result_.Reset();
}

void ScanNextOperation::Forward(TransactionExecution *txm)
{
    CcScanner &scanner = *scan_state_->scanner_;

    // start the state machine if not running.
    if (!is_running_)
    {
        if (scanner.Type() == CcmScannerType::HashPartition)
        {
            txm->Process(*this);
        }
        else if (scanner.Type() == CcmScannerType::RangePartition)
        {
            if (!lock_range_result_.IsFinished())
            {
                // The locking-next-range request has not finished. The scan
                // next operation cannot proceed without locking the range.
                return;
            }

            assert(lock_range_result_.IsFinished());

            if (lock_range_result_.IsError())
            {
                // There is an error when getting the next range's lock and
                // ID. The scan next operation is set to be errored.
                DLOG(ERROR) << "Failed to get next range id, "
                            << lock_range_result_.ErrorMsg() << ", tx "
                            << txm->TxNumber();
                slice_hd_result_.SetError(lock_range_result_.ErrorCode());
                unlock_range_result_.SetFinished();
            }
            else
            {
                // Need to make sure current node is still leader before
                // visiting range and bucket meta data.
                if (!txm->CheckLeaderTerm())
                {
                    slice_hd_result_.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
                    unlock_range_result_.SetFinished();
                    txm->PostProcess(*this);
                }
                else
                {
                    const ReadKeyResult &read_res = lock_range_result_.Value();
                    // For scans, range locks are released when the last/first
                    // slice of the range is scanned. Hence, the range lock is
                    // always put into the read set. If the tx terminates the
                    // scan early before the last/first slice is encountered,
                    // the range lock is released in post-processing.
                    // TODO: release the range lock in the scan close phase.
                    txm->rw_set_.AddRead(
                        read_res.cce_addr_, read_res.ts_, &range_table_name_);

                    scan_state_->range_cce_addr_ = read_res.cce_addr_;
                    scan_state_->range_id_ =
                        range_rec_.GetRangeInfo()->PartitionId();
                    scan_state_->range_ng_ =
                        range_rec_.GetRangeOwnerNg()->BucketOwner();
                    txm->Process(*this);
                }
                return;
            }
        }
    }

    if (scanner.Type() == CcmScannerType::HashPartition &&
        hd_result_.IsFinished())
    {
        // Error code REQUESTED_NODE_NOT_LEADER indicates send message failed or
        // term changed.
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            Sharder::Instance().UpdateLeader(hd_result_.Value().node_group_id_);
            if (retry_num_ > 0)
            {
                hd_result_.Reset();
                ReRunOp(txm);
                return;
            }
        }

        scanner.SetStatus(ScannerStatus::Open);
        txm->PostProcess(*this);
    }
    else if (scanner.Type() == CcmScannerType::RangePartition &&
             slice_hd_result_.IsFinished())
    {
        if (slice_hd_result_.ErrorCode() ==
                CcErrorCode::PIN_RANGE_SLICE_FAILED ||
            slice_hd_result_.ErrorCode() ==
                CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
            slice_hd_result_.ErrorCode() == CcErrorCode::DATA_STORE_ERR)
        {
            if (slice_hd_result_.ErrorCode() ==
                CcErrorCode::REQUESTED_NODE_NOT_LEADER)
            {
                Sharder::Instance().UpdateLeader(
                    slice_hd_result_.Value().cc_ng_id_);
            }
            if (retry_num_ > 0)
            {
                slice_hd_result_.Value().Reset();
                slice_hd_result_.Reset();
                ReRunOp(txm);
                return;
            }
        }

        if (!slice_hd_result_.IsError() &&
            scanner.Status() == ScannerStatus::Blocked)
        {
            RangeScanSliceResult &scan_slice_result = slice_hd_result_.Value();

            // If the scanned slice is the last (or first) of the range and
            // the tx's isolation level is less than Repeatable Read,
            // unlocks the range now.
            if (scan_slice_result.slice_position_ != SlicePosition::Middle &&
                scan_state_->range_cce_addr_.CceLockPtr() != 0)
            {
                if (txm->iso_level_ < IsolationLevel::RepeatableRead)
                {
                    if (lock_range_result_.IsFinished())
                    {
                        txm->rw_set_.DedupRead(range_table_name_,
                                               scan_state_->range_cce_addr_);

                        CcReqStatus ret = txm->cc_handler_->PostRead(
                            txm->TxNumber(),
                            txm->TxTerm(),
                            txm->CommandId(),
                            0,
                            0,
                            0,
                            scan_state_->range_cce_addr_,
                            unlock_range_result_,
                            true);
                        if (ret == CcReqStatus::Processed)
                        {
                            txm->AdvanceCommand();
                        }

                        // After the unlock range request is sent,
                        // lock_range_result_ is reset. When the tx machine is
                        // re-executed, its status is unfinished, indicating
                        // that the scan next operation is waiting for the
                        // response of unlocking the current range.
                        lock_range_result_.Reset();
                        return;
                    }
                    else if (!unlock_range_result_.IsFinished())
                    {
                        return;
                    }
                }
                else
                {
                    // Reset lock_range_result_ for acquire lock on the next
                    // range if necessary
                    lock_range_result_.Reset();
                }
            }

            scan_state_->SetSliceLastKey(scan_slice_result.MoveLastKey());
            scan_state_->inclusive_ =
                Direction() == ScanDirection::Forward ? false : true;
            scan_state_->slice_position_ = scan_slice_result.slice_position_;
        }

        scanner.SetStatus(ScannerStatus::Open);
        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeout",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });

        DeadLockCheck::RequestCheck();

        if (scanner.Type() == CcmScannerType::RangePartition)
        {
            if (slice_hd_result_.Value().is_local_)
            {
                // For local request, just rely on the result of the deadlock
                // checking.
                return;
            }

            if (retry_num_ > 0 &&
                (txm->CheckLeaderTerm() || txm->CheckStandbyTerm()))
            {
                ReRunOp(txm);
                return;
            }

            bool force_error = slice_hd_result_.ForceError();
            if (force_error)
            {
                txm->PostProcess(*this);
            }
        }
        else
        {
            if (hd_result_.Value().is_local_)
            {
                // Never force error for local request.
                return;
            }

            if (retry_num_ > 0 &&
                (txm->CheckLeaderTerm() || txm->CheckStandbyTerm()))
            {
                ReRunOp(txm);
                return;
            }

            bool force_error = hd_result_.ForceError();
            if (force_error)
            {
                txm->PostProcess(*this);
            }
        }
    }
}

AcquireAllOp::AcquireAllOp(TransactionExecution *txm)
{
    hd_results_.reserve(8);
    retry_num_ = 1;

    for (size_t idx = 0; idx < 8; ++idx)
    {
        auto &res = hd_results_.emplace_back(txm);
        TX_TRACE_ASSOCIATE(this, &res);

        res.post_lambda_ = [this](CcHandlerResult<AcquireAllResult> *hres)
        {
            if (hres->IsError())
            {
                fail_cnt_.fetch_add(1, std::memory_order_relaxed);

                const AcquireAllResult &acq_result = hres->Value();
                if (acq_result.node_term_ < 0 &&
                    acq_result.remote_ack_cnt_ != nullptr)
                {
                    remote_ack_cnt_.fetch_sub(1, std::memory_order_relaxed);
                }
            }

            finish_cnt_.fetch_add(1, std::memory_order_relaxed);
        };
    }
}

void AcquireAllOp::Resize(size_t new_size)
{
    size_t old_size = hd_results_.size();

    if (new_size > old_size)
    {
        for (size_t idx = old_size; idx < new_size; ++idx)
        {
            // All cc handler results in an operation points to the same tx
            // machine.
            auto &res = hd_results_.emplace_back(hd_results_.at(0).Txm());
            TX_TRACE_ASSOCIATE(this, &res);

            res.post_lambda_ = [this](CcHandlerResult<AcquireAllResult> *hres)
            {
                if (hres->IsError())
                {
                    fail_cnt_.fetch_add(1, std::memory_order_relaxed);

                    const AcquireAllResult &acq_result = hres->Value();
                    if (acq_result.node_term_ < 0 &&
                        acq_result.remote_ack_cnt_ != nullptr)
                    {
                        remote_ack_cnt_.fetch_sub(1, std::memory_order_relaxed);
                    }
                }

                finish_cnt_.fetch_add(1, std::memory_order_relaxed);
            };
        }
    }
}

void AcquireAllOp::Reset(size_t node_cnt)
{
    finish_cnt_.store(0, std::memory_order_relaxed);
    fail_cnt_.store(0, std::memory_order_relaxed);
    remote_ack_cnt_.store(0, std::memory_order_relaxed);
    upload_cnt_ = node_cnt * keys_.size();

    // Reset results since we rely on node term to decide if
    // the received ack is the first ack message. See OnReceiveCcMsg
    // in cc_stream_receiver.cpp
    for (size_t idx = 0; idx < hd_results_.size(); ++idx)
    {
        AcquireAllResult &res = hd_results_.at(idx).Value();
        res.node_term_ = -1;
        res.last_vali_ts_ = 1;
        res.commit_ts_ = 1;
    }
    Resize(upload_cnt_);
}

void AcquireAllOp::ResetHandlerTxm(TransactionExecution *txm)
{
    for (size_t idx = 0; idx < hd_results_.size(); ++idx)
    {
        hd_results_.at(idx).ResetTxm(txm);
    }
}

void AcquireAllOp::Forward(TransactionExecution *txm)
{
    if (!txm->CheckLeaderTerm())
    {
        for (size_t hd_idx = 0; hd_idx < upload_cnt_; ++hd_idx)
        {
            auto &hd_res = hd_results_[hd_idx];
            if (hd_res.IsFinished())
            {
                continue;
            }
            else
            {
                hd_res.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
                hd_res.ForceError();
            }
        }
        txm->PostProcess(*this);
        return;
    }
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (remote_ack_cnt_.load(std::memory_order_relaxed) > 0)
    {
        if (txm->IsTimeOut())
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "Forward.IsTimeOut",
                txm,
                [txm]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(txm->TxNumber()))
                        .append(",\"term\":")
                        .append(std::to_string(txm->TxTerm()));
                });
            // At least one remote acquire request has not received
            // acknowledgement and the upload phase has timed out. Forces
            // un-acknowledged requests to finish with an error.
            size_t force_error_cnt = 0;
            for (size_t idx = 0; idx < upload_cnt_; ++idx)
            {
                CcHandlerResult<AcquireAllResult> &hd_result = hd_results_[idx];

                const AcquireAllResult &acquire_res = hd_result.Value();

                uint64_t ts = std::max(acquire_res.commit_ts_ + 1,
                                       acquire_res.last_vali_ts_ + 1);
                txm->commit_ts_bound_ = std::max(txm->commit_ts_bound_, ts);

                // Dedup read set for successful acquire
                if (!hd_result.IsError())
                {
                    const CcEntryAddr &cce_addr = acquire_res.local_cce_addr_;
                    uint64_t read_version = txm->rw_set_.DedupRead(cce_addr);
                    if (read_version > 0 &&
                        read_version != acquire_res.commit_ts_)
                    {
                        // Each write-set key acquires a write lock and gets
                        // the key's last validation ts and commit ts. If
                        // the write key has been read before and the key's
                        // commit ts mismatches the prior version, this is
                        // not a repeatable read.
                        fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                    }
                }

                if (acquire_res.node_term_ < 0)
                {
                    bool success = hd_result.ForceError();
                    if (success)
                    {
                        ++force_error_cnt;
                    }
                    else if (hd_result.ErrorCode() ==
                             CcErrorCode::REQUESTED_NODE_NOT_LEADER)
                    {
                        uint32_t ng_id =
                            acquire_res.local_cce_addr_.NodeGroupId();
                        Sharder::Instance().UpdateLeader(ng_id);
                        if (retry_num_ > 0)
                        {
                            ReRunOp(txm);
                            return;
                        }
                    }
                }
            }

            if (force_error_cnt > 0)
            {
                txm->PostProcess(*this);
            }
            // Else, all remote requests finish normally, meaning the tx has
            // been moved from the waiting queue to the execution queue.
            // Does not forword the tx now, as it will be re-executed when
            // the tx processor visits the execution queue.
        }
    }
    else if (finish_cnt_.load(std::memory_order_relaxed) == upload_cnt_)
    {
        // TODO: for locking-based protocols, though the tx may be blocked
        // arbitrarily long, after all acquire requests are acknowledged, we
        // still need to periodically check liveness of the remote node.

        if (fail_cnt_.load(std::memory_order_relaxed) == 0)
        {
            for (size_t idx = 0; idx < upload_cnt_; ++idx)
            {
                const AcquireAllResult &acquire_res = hd_results_[idx].Value();
                uint64_t ts = std::max(acquire_res.commit_ts_ + 1,
                                       acquire_res.last_vali_ts_ + 1);
                txm->commit_ts_bound_ = std::max(txm->commit_ts_bound_, ts);

                assert(acquire_res.node_term_ >= 0);

                // Dedup read set
                const CcEntryAddr &cce_addr = acquire_res.local_cce_addr_;
                uint64_t read_version = txm->rw_set_.DedupRead(cce_addr);
                if (read_version > 0 && read_version != acquire_res.commit_ts_)
                {
                    // Each write-set key acquires a write lock and gets the
                    // key's last validation ts and commit ts. If the write
                    // key has been read before and the key's commit ts
                    // mismatches the prior version, this is not a
                    // repeatable read.
                    fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
        else
        {
            for (size_t idx = 0; idx < upload_cnt_; ++idx)
            {
                CcHandlerResult<AcquireAllResult> &hd_result = hd_results_[idx];
                if (hd_result.IsError())
                {
                    if (hd_result.ErrorCode() ==
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER)
                    {
                        uint32_t ng_id =
                            hd_result.Value().local_cce_addr_.NodeGroupId();
                        Sharder::Instance().UpdateLeader(ng_id);
                        if (retry_num_ > 0)
                        {
                            ReRunOp(txm);
                            return;
                        }
                    }
                    fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    // Dedup read set for successful acquire
                    const AcquireAllResult &acquire_all_result =
                        hd_result.Value();
                    const CcEntryAddr &cce_addr =
                        acquire_all_result.local_cce_addr_;
                    uint64_t read_version = txm->rw_set_.DedupRead(cce_addr);
                    if (read_version > 0 &&
                        read_version != acquire_all_result.commit_ts_)
                    {
                        // Each write-set key acquires a write lock and gets
                        // the key's last validation ts and commit ts. If
                        // the write key has been read before and the key's
                        // commit ts mismatches the prior version, this is
                        // not a repeatable read.
                        fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }

        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
    {
        // For non-blocking concurrency control protocols, the AcquireAllOp is
        // expected to return instantly. For 2PL, if the request is blocked, the
        // cc node will send an acknowledgement to update the node term.
        for (size_t hd_idx = 0; hd_idx < upload_cnt_; ++hd_idx)
        {
            auto &hd_res = hd_results_[hd_idx];
            if (hd_res.IsFinished())
            {
                continue;
            }

            AcquireAllResult &ac_res = hd_results_[hd_idx].Value();

            bool has_blocked_remote_cce =
                ac_res.blocked_remote_cce_addr_.size() > 0;

            if (ac_res.node_term_ > 0 && has_blocked_remote_cce)
            {
                uint32_t node_group_id = ac_res.local_cce_addr_.NodeGroupId();

                // Check the liveness of remote node
                txm->cc_handler_->BlockAcquireAllCcReqCheck(
                    node_group_id,
                    txm->TxNumber(),
                    txm->TxTerm(),
                    txm->CommandId(),
                    ac_res.blocked_remote_cce_addr_,
                    &hd_results_[hd_idx]);
            }
        }
    }
}

uint64_t AcquireAllOp::MaxTs() const
{
    uint64_t max_ts = 0;
    for (size_t idx = 0; idx < upload_cnt_; ++idx)
    {
        const AcquireAllResult &acq_all_res = hd_results_[idx].Value();
        uint64_t ts =
            std::max(acq_all_res.commit_ts_ + 1, acq_all_res.last_vali_ts_ + 1);
        max_ts = std::max(max_ts, ts);
    }
    return max_ts;
}

bool AcquireAllOp::IsDeadlock() const
{
    for (size_t idx = 0; idx < upload_cnt_; ++idx)
    {
        if (hd_results_[idx].IsError() &&
            hd_results_[idx].ErrorCode() == CcErrorCode::DEAD_LOCK_ABORT)
        {
            return true;
        }
    }

    return false;
}

PostWriteAllOp::PostWriteAllOp(TransactionExecution *txm) : hd_result_(txm)
{
}

void PostWriteAllOp::Reset(uint32_t ng_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(ng_cnt * keys_.size());
}

void PostWriteAllOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void PostWriteAllOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.IsError() &&
            hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            Sharder::Instance().UpdateLeaders();
        }
        txm->PostProcess(*this);
    }
    else if (hd_result_.LocalRefCnt() == 0)
    {
        if (txm->IsTimeOut(4))
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "Forward.IsTimeOut",
                txm,
                [txm]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(txm->TxNumber()))
                        .append(",\"term\":")
                        .append(std::to_string(txm->TxTerm()));
                });

            bool force_error = hd_result_.ForceError();
            if (force_error)
            {
                txm->PostProcess(*this);
            }
        }
    }
}

bool PostWriteAllOp::IsFailed()
{
    assert(hd_result_.IsFinished());
    return hd_result_.IsError();
}

CatalogAcquireAllOp::CatalogAcquireAllOp(TransactionExecution *txm)
    : succeed_(false),
      op_(nullptr),
      lock_cluster_config_op_(),
      acquire_all_intent_op_(txm),
      acquire_all_lock_op_(txm),
      read_cluster_result_(txm),
      cluster_conf_rec_()
{
    lock_cluster_config_op_.table_name_ = TableName(cluster_config_ccm_name_sv,
                                                    TableType::ClusterConfig,
                                                    TableEngine::None);
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;
}

void CatalogAcquireAllOp::Reset()
{
    succeed_ = false;
    op_ = nullptr;

    read_cluster_result_.Reset();

    acquire_all_intent_op_.Reset(0);
    acquire_all_intent_op_.keys_.clear();

    acquire_all_lock_op_.Reset(0);
    acquire_all_lock_op_.keys_.clear();
}

void CatalogAcquireAllOp::SetCatalogWriteSet(
    const std::map<TxKey, ReplicaWriteSetEntry> &wset)
{
    acquire_all_intent_op_.keys_.reserve(wset.size());
    acquire_all_lock_op_.keys_.reserve(wset.size());
    for (const auto &[write_key, write_entry] : wset)
    {
        acquire_all_intent_op_.keys_.push_back(write_key.GetShallowCopy());
        acquire_all_lock_op_.keys_.push_back(write_key.GetShallowCopy());
    }
}

uint64_t CatalogAcquireAllOp::MaxTs() const
{
    return acquire_all_lock_op_.MaxTs();
}

void CatalogAcquireAllOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        op_ = &lock_cluster_config_op_;
        txm->PushOperation(&lock_cluster_config_op_);
        txm->Process(lock_cluster_config_op_);
    }
    else if (op_ == &lock_cluster_config_op_)
    {
        if (lock_cluster_config_op_.hd_result_->IsError())
        {
            LOG(ERROR) << "Upsert table read cluster config failed, tx_number:"
                       << txm->TxNumber();
            txm->PostProcess(*this);
        }
        else
        {
            op_ = &acquire_all_intent_op_;
            txm->PushOperation(&acquire_all_intent_op_);
            txm->Process(acquire_all_intent_op_);
        }
    }
    else if (op_ == &acquire_all_intent_op_)
    {
        if (acquire_all_intent_op_.fail_cnt_.load(std::memory_order_relaxed) >
            0)
        {
            LOG(ERROR) << "Upsert table acquire write intent failed, tx_number:"
                       << txm->TxNumber();
            txm->PostProcess(*this);
        }
        else
        {
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            txm->Process(acquire_all_lock_op_);
        }
    }
    else
    {
        assert(op_ == &acquire_all_lock_op_);
        if (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Upsert table schema transaction failed to "
                          "acquire write lock, tx_number:"
                       << txm->TxNumber();
            txm->PostProcess(*this);
        }
        else
        {
            succeed_ = true;
            txm->PostProcess(*this);
        }
    }
}

DsUpsertTableOp::DsUpsertTableOp(const TableName *table_name,
                                 OperationType op_type,
                                 TransactionExecution *txm)
    : table_name_(table_name), op_type_(op_type), hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void DsUpsertTableOp::Reset()
{
    hd_result_.Reset();
}

void DsUpsertTableOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void DsUpsertTableOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.IsError())
        {
            assert(hd_result_.ErrorCode() == CcErrorCode::DATA_STORE_ERR);
            if (retry_num_ == 0)
            {
                DLOG(ERROR) << "flush schema error: can not create table "
                               "in kv store";

                txm->upsert_resp_->SetErrorCode(TxErrorCode::DATA_STORE_ERROR);
                txm->PostProcess(*this);
            }
            else if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }
        else
        {
            txm->PostProcess(*this);
        }
    }
}

SchemaOp::SchemaOp(const std::string_view table_name_sv,
                   TableEngine table_engine,
                   const std::string &current_image,
                   const std::string &dirty_image,
                   uint64_t schema_ts,
                   OperationType op_type)
    : table_key_(TableName(table_name_sv.data(),
                           table_name_sv.size(),
                           TableType::Primary,
                           table_engine))
{
    catalog_rec_.SetSchemaImage(current_image);
    catalog_rec_.SetDirtySchemaImage(dirty_image);
    image_str_ = current_image;
    dirty_image_str_ = dirty_image;
    curr_schema_ts_ = schema_ts;
    op_type_ = op_type;
}

void SchemaOp::FillPrepareLogRequestCommon(TransactionExecution *txm,
                                           WriteToLogOp &prepare_log_op)
{
    prepare_log_op.log_type_ = TxLogType::PREPARE;

    prepare_log_op.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op.log_closure_.LogRequest().mutable_write_log_request();

    prepare_log_rec->set_tx_term(txm->TxTerm());
    prepare_log_rec->set_txn_number(txm->TxNumber());
    prepare_log_rec->set_commit_timestamp(txm->CommitTs());

    ::txlog::SchemaOpMessage *prepare_schema_msg =
        prepare_log_rec->mutable_log_content()->mutable_schema_log();
    prepare_schema_msg->set_table_name_str(table_key_.Name().String());
    prepare_schema_msg->set_table_type(
        ::txlog::ToRemoteType::ConvertTableType(table_key_.Name().Type()));
    prepare_schema_msg->set_table_engine(
        ::txlog::ToRemoteType::ConvertTableEngine(table_key_.Name().Engine()));
    prepare_schema_msg->set_old_catalog_blob(catalog_rec_.SchemaImage());
    prepare_schema_msg->set_catalog_ts(curr_schema_ts_);
    prepare_schema_msg->set_new_catalog_blob(catalog_rec_.DirtySchemaImage());
    prepare_schema_msg->mutable_table_op()->set_op_type(
        static_cast<::google::protobuf::uint32>(op_type_));
    prepare_schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_PrepareSchema);
}

void SchemaOp::FillCommitLogRequestCommon(TransactionExecution *txm,
                                          WriteToLogOp &commit_log_op)
{
    commit_log_op.log_type_ = TxLogType::COMMIT;

    commit_log_op.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op.log_closure_.LogRequest().mutable_write_log_request();

    commit_log_rec->set_tx_term(txm->TxTerm());
    commit_log_rec->set_txn_number(txm->TxNumber());

    ::txlog::SchemaOpMessage *commit_schema_msg =
        commit_log_rec->mutable_log_content()->mutable_schema_log();

    commit_schema_msg->set_table_name_str(table_key_.Name().String());
    commit_schema_msg->set_table_type(
        ::txlog::ToRemoteType::ConvertTableType(table_key_.Name().Type()));
    commit_schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_CommitSchema);

    // The prepare log keeps all cc nodes' terms and match them in the log
    // service to detect invalidated write intents. The commit log, however,
    // does not match terms in the log service. This is because all
    // operations after the prepare log are retried or replayed upon
    // failures to guarantee that the schema operation always roll forward.
    // If a cc node fails over, the new node must restore write intents
    // gained prior to the prepare log and then replay operations between
    // the prepare log and the commit log, which in this case upgrade write
    // intents to write locks. So, there is no need to check the liveness of
    // write locks when flushing the commit log.
    commit_log_rec->mutable_node_terms()->clear();
}

void SchemaOp::FillCleanLogRequestCommon(TransactionExecution *txm,
                                         WriteToLogOp &clean_log_op)
{
    clean_log_op.log_type_ = TxLogType::CLEAN;

    clean_log_op.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *clean_log_rec =
        clean_log_op.log_closure_.LogRequest().mutable_write_log_request();

    clean_log_rec->set_tx_term(txm->TxTerm());
    clean_log_rec->set_txn_number(txm->TxNumber());

    ::txlog::SchemaOpMessage *clean_schema_msg =
        clean_log_rec->mutable_log_content()->mutable_schema_log();

    clean_schema_msg->set_table_name_str(table_key_.Name().String());
    clean_schema_msg->set_table_type(
        ::txlog::ToRemoteType::ConvertTableType(table_key_.Name().Type()));
    clean_schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_CleanSchema);
    clean_log_rec->mutable_node_terms()->clear();
}

UpsertTableOp::UpsertTableOp(const std::string_view table_name_str,
                             TableEngine table_engine,
                             const std::string &current_image,
                             uint64_t curr_schema_ts,
                             const std::string &dirty_image,
                             OperationType op_type,
                             TransactionExecution *txm)
    : SchemaOp(table_name_str,
               table_engine,
               current_image,
               dirty_image,
               curr_schema_ts,
               op_type),
      lock_cluster_config_op_(),
      acquire_all_intent_op_(txm),
      prepare_log_op_(txm),
      post_all_intent_op_(txm),
      unlock_cluster_config_op_(txm),
      upsert_kv_table_op_(&table_key_.Name(), op_type, txm),
      update_sequence_table_op_(txm),
      acquire_all_lock_op_(txm),
      commit_log_op_(txm),
      clean_ccm_op_(txm),
      post_all_lock_op_(txm),
      clean_log_op_(txm),
      read_cluster_result_(txm)
{
    assert(op_type_ == OperationType::CreateTable ||
           op_type_ == OperationType::DropTable ||
           op_type_ == OperationType::TruncateTable ||
           op_type_ == OperationType::Update);

    lock_cluster_config_op_.table_name_ = TableName(cluster_config_ccm_name_sv,
                                                    TableType::ClusterConfig,
                                                    TableEngine::None);
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.keys_.emplace_back(&table_key_);
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    post_all_intent_op_.table_name_ = &catalog_ccm_name;
    post_all_intent_op_.keys_.emplace_back(&table_key_);
    post_all_intent_op_.recs_.push_back(&catalog_rec_);
    post_all_intent_op_.op_type_ = op_type_;
    post_all_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.keys_.emplace_back(&table_key_);
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.keys_.emplace_back(&table_key_);
    post_all_lock_op_.recs_.push_back(&catalog_rec_);
    post_all_lock_op_.op_type_ = op_type_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    clean_ccm_op_.Clear();

    TX_TRACE_ASSOCIATE(this, &acquire_all_intent_op_, "acquire_all_intent_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_intent_op_, "post_all_intent_op_");
    TX_TRACE_ASSOCIATE(this, &upsert_kv_table_op_, "upsert_kv_table_op_");
    TX_TRACE_ASSOCIATE(
        this, &update_sequence_table_op_, "update_sequence_table_op_");
    TX_TRACE_ASSOCIATE(this, &acquire_all_lock_op_, "acquire_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");

    is_force_finished = false;
}

void UpsertTableOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        op_ = &lock_cluster_config_op_;
        txm->PushOperation(&lock_cluster_config_op_);
        txm->Process(lock_cluster_config_op_);
    }
    else if (op_ == &lock_cluster_config_op_)
    {
        if (lock_cluster_config_op_.hd_result_->IsError())
        {
            LOG(ERROR) << "Upsert table read cluster config failed, tx_number:"
                       << txm->TxNumber();
            if (!prepare_log_op_.hd_result_.IsFinished())
            {
                txm->commit_ts_ = tx_op_failed_ts_;
            }
            ForceToFinish(txm);
            return;
        }

        if (prepare_log_op_.hd_result_.IsFinished())
        {
            assert(op_type_ == OperationType::CreateTable ||
                   op_type_ == OperationType::Update);
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            op_ = &acquire_all_intent_op_;
            txm->PushOperation(&acquire_all_intent_op_);
            DLOG(INFO) << "txn: " << txm->TxNumber()
                       << " process acquire_all_intent_op_";
            txm->Process(acquire_all_intent_op_);
        }
    }
    else if (op_ == &acquire_all_intent_op_)
    {
        if (acquire_all_intent_op_.fail_cnt_.load(std::memory_order_relaxed) >
            0)
        {
            LOG(ERROR) << "Upsert table acquire write intent failed, tx_number:"
                       << txm->TxNumber();
            txm->upsert_resp_->SetErrorCode(
                TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL);
            // Fails to acquire the write intent on the schema. Since write
            // intents only conflict with other writes, there must be
            // another tx trying to modify the same table's schema. Stops
            // the schema operation. Set the commit ts to 0 to signal that
            // the following post write operation releases all write
            // intents.
            txm->commit_ts_ = tx_op_failed_ts_;
            // Moves to the last operation that removes all write
            // intents/locks.
            op_ = &post_all_lock_op_;
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
            return;
        }

        // Assigns a commit timestamp to the tx state machine as the version
        // of the new schema. Unlike the conventional commit ts, the
        // schema's commit ts is not chosen via the SetTs stage. But it
        // follows a similar set of rules: the new schema's version should
        // be greater than 1) the current version, 2) the maximal commit ts
        // of all tx's that have read the schema, and 3) the local time when
        // the tx starts.
        txm->commit_ts_ = txm->commit_ts_bound_ + 1;

        for (size_t idx = 0; idx < acquire_all_intent_op_.upload_cnt_; ++idx)
        {
            const AcquireAllResult &acq_all_res =
                acquire_all_intent_op_.hd_results_[idx].Value();
            uint64_t ts = std::max(acq_all_res.commit_ts_ + 1,
                                   acq_all_res.last_vali_ts_ + 1);
            txm->commit_ts_ = std::max(txm->commit_ts_, ts);
        }

        op_ = &prepare_log_op_;
        FillPrepareLogRequest(txm);
        txm->PushOperation(&prepare_log_op_);
        CODE_FAULT_INJECTOR("upsert_table_prepare_log_fail", {
            LOG(INFO) << "FaultInject  upsert_table_prepare_log_fail";
            prepare_log_op_.is_running_ = true;
            prepare_log_op_.hd_result_.SetError(CcErrorCode::WRITE_LOG_FAILED);
            return;
        });
        DLOG(INFO) << "txn: " << txm->TxNumber() << " process prepare_log_op_";
        txm->Process(prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        if (prepare_log_op_.hd_result_.IsError())
        {
            if (prepare_log_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the
                // coordinator itself is no longer leader
                if (txm->CheckLeaderTerm())
                {
                    LOG(WARNING)
                        << "Upsert table write prepare log result unknown, "
                           "tx_number:"
                        << txm->TxNumber() << ", keep retrying";
                    // set retry flag and retry prepare log
                    ::txlog::WriteLogRequest *log_req =
                        prepare_log_op_.log_closure_.LogRequest()
                            .mutable_write_log_request();
                    log_req->set_retry(true);
                    txm->PushOperation(&prepare_log_op_);
                    txm->Process(prepare_log_op_);
                }
                else
                {
                    LOG(ERROR) << "Upsert table write prepare log result "
                                  "unknown, tx_number:"
                               << txm->TxNumber()
                               << ", not leader any more, stop retrying";
                    // Not leader anymore, just quit. New leader will know
                    // whether prepare log succeeds and continue the rest if
                    // it does. Should not release the write intents. If
                    // prepare log is not written, the write intents will be
                    // released individually via orphan lock recovery
                    // mechanism.
                    txm->upsert_resp_->SetErrorCode(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);

                    txm->upsert_resp_->Finish(UpsertResult::Failed);
                    txm->state_stack_.pop_back();
                    assert(txm->state_stack_.empty());
                    LocalCcShards *local_shards =
                        Sharder::Instance().GetLocalCcShards();
                    std::unique_lock<std::mutex> lk(
                        local_shards->table_schema_op_pool_mux_);
                    local_shards->table_schema_op_pool_.emplace_back(
                        std::move(txm->schema_op_));
                }
            }
            else
            {
                LOG(ERROR)
                    << "Upsert table write prepare log failed, tx_number:"
                    << txm->TxNumber();
                // Fails to flush the prepare log. The schema operation is
                // considered failed if the prepare log is not flushed. The
                // commit ts is set to 0 to signal that the following post
                // write operation releases all write intents.
                txm->commit_ts_ = tx_op_failed_ts_;
                // Moves to the last operation that removes all write
                // intents/locks.
                op_ = &post_all_lock_op_;

                txm->upsert_resp_->SetErrorCode(
                    TxErrorCode::UPSERT_TABLE_PREPARE_FAIL);

                txm->PushOperation(&post_all_lock_op_);
                txm->Process(post_all_lock_op_);
            }
        }
        else
        {
            ACTION_FAULT_INJECTOR("upsert_table_crash_after_prepare_log");
            op_ = &post_all_intent_op_;

            txm->PushOperation(&post_all_intent_op_);
            DLOG(INFO) << "txn: " << txm->TxNumber()
                       << " process post_all_intent_op_";
            txm->Process(post_all_intent_op_);
        }
    }
    else if (op_ == &post_all_intent_op_)
    {
        if (post_all_intent_op_.IsFailed())
        {
            // After the prepare log is flushed, the schema op is guaranteed
            // to proceed. Retry this step to install the dirty schema in the tx
            // service, if the tx node is still the leader. The tx is also
            // allowed to proceed if the tx is in the recovery mode and the tx
            // node is a leader candidate.

            if (txm->CheckLeaderTerm())
            {
                // set catalog_rec_'s binary_value_ to image_str since it
                // could be set to TableSchemaView pointer in localshard.
                catalog_rec_.SetSchemaImage(image_str_);

                txm->PushOperation(&post_all_intent_op_);
                DLOG(INFO) << "txn: " << txm->TxNumber()
                           << " process post_all_intent_op_";
                txm->Process(post_all_intent_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else if (op_type_ == OperationType::DropTable ||
                 op_type_ == OperationType::TruncateTable)
        {
            // For DROP TABLE and TRUNCATE TABLE operations, the data store
            // operation of deleting the k-v table happens after the commit log
            // is flushed.
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            DLOG(INFO) << "txn: " << txm->TxNumber()
                       << " process acquire_all_lock_op_";
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            // Release cluster config op before doing data store op.
            op_ = &unlock_cluster_config_op_;
            // Get cce addr of cluster config read lock from rset.
            const CcEntryAddr *cluster_config_addr = nullptr;
            const ReadSetEntry *cluster_config_entry = nullptr;
            const auto &meta_rset = txm->rw_set_.MetaDataReadSet();
            for (const auto &[cce_addr, read_entry_pair] : meta_rset)
            {
                if (read_entry_pair.second == cluster_config_ccm_name_sv)
                {
                    cluster_config_addr = &cce_addr;
                    cluster_config_entry = &read_entry_pair.first;
                    break;
                }
            }
            assert(cluster_config_addr && cluster_config_entry);
            unlock_cluster_config_op_.Reset(cluster_config_addr,
                                            cluster_config_entry);
            txm->PushOperation(&unlock_cluster_config_op_);
            txm->Process(unlock_cluster_config_op_);
        }
    }
    else if (op_ == &unlock_cluster_config_op_)
    {
        assert(op_type_ == OperationType::CreateTable ||
               op_type_ == OperationType::Update ||
               op_type_ == OperationType::TruncateTable);
        if (unlock_cluster_config_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // Releasing a local read lock should never fail
                assert(false);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        op_ = &upsert_kv_table_op_;
        // The post write request right after flushing the prepare log
        // installs the dirty schema in the tx service and returns a
        // local view (pointer) of the committed and dirty schema.
        upsert_kv_table_op_.table_schema_old_ = catalog_rec_.Schema();
        upsert_kv_table_op_.table_schema_ = catalog_rec_.DirtySchema();
        upsert_kv_table_op_.alter_table_info_ = nullptr;
        upsert_kv_table_op_.write_time_ = txm->commit_ts_;
        txm->PushOperation(&upsert_kv_table_op_);
        txm->Process(upsert_kv_table_op_);
    }
    else if (op_ == &upsert_kv_table_op_)
    {
        if (upsert_kv_table_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // Keep retrying if it is DropTable, TruncateTable.
                if (op_type_ == OperationType::DropTable ||
                    op_type_ == OperationType::TruncateTable)
                {
                    txm->PushOperation(&upsert_kv_table_op_);
                    DLOG(INFO) << "txn: " << txm->TxNumber()
                               << " process upsert_kv_table_op_";
                    txm->Process(upsert_kv_table_op_);
                }
                else
                {
                    // Set txm->commit_ts_ to 0 to indicate there is a flush
                    // error during upsert_kv_table_op_.
                    txm->commit_ts_ = tx_op_failed_ts_;

                    /*
                    After upsert kv fails, we need to flush a commit log to
                    indicate this error.

                    If we skip this commit log and jump to post_all_lock_op_
                    directly, once the participant crashes at the point between
                    it releases write intent and the coordinator flushes
                    clean_log, then during recovery, the participant sees a
                    prepare_log(whose commit_ts is not 0) and recovers write
                    lock and dirty_catalog.

                    Since the coordinator has finished its job, the write
                    lock recovered by participant becomes orphan lock, and the
                    dirty catalog can not be rejected either.

                    Also, in the current design, post_all_intent_op_ does not
                    release the write intent, which means the write intent is
                    still being held after upsert_kv_table_op_(during
                    CreateTable or AddIndex). If create table or add index in kv
                    fails, the only thing we should do after writing commit_log
                    is to reject dirty schema. So there is no need to upgrade
                    write intent to write lock, and it is safe to skip
                    acquire_all_lock_op_ and jump directly to commit_log_op_.

                    */

                    op_ = &commit_log_op_;
                    FillCommitLogRequest(txm);
                    txm->PushOperation(&commit_log_op_);
                    txm->Process(commit_log_op_);
                }
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else if (op_type_ == OperationType::CreateTable ||
                 op_type_ == OperationType::DropTable ||
                 op_type_ == OperationType::TruncateTable)
        {
            bool need_update_sequence_table =
                !table_key_.Name().IsHashPartitioned() ||
                (op_type_ == OperationType::CreateTable
                     ? catalog_rec_.DirtySchema()->HasAutoIncrement()
                     : catalog_rec_.Schema()->HasAutoIncrement());
            if (need_update_sequence_table)
            {
                update_sequence_table_op_.op_func_ =
                    [this](AsyncOp<Void> &async_op) mutable
                {
                    CcHandlerResult<Void> &hd_res = async_op.hd_result_;
#ifdef EXT_TX_PROC_ENABLED
                    hd_res.SetToBlock();
                    // The memory fence ensures that the block flag is set
                    // before the background thread is executed.
                    std::atomic_thread_fence(std::memory_order_release);
#endif
                    assert(txservice::Sequences::Initialized());
                    // Launch a new thread instead of sending it to workerpool
                    // to avoid being blocked during write lock is held.
                    async_op.worker_thread_ = std::thread(
                        [this, &hd_res]() mutable
                        {
                            bool succ = true;
                            const TableName &table_name = table_key_.Name();
                            // Update sequence table about the range id info.
                            std::vector<TableName> index_names =
                                op_type_ == OperationType::CreateTable
                                    ? catalog_rec_.DirtySchema()->IndexNames()
                                    : catalog_rec_.Schema()->IndexNames();
                            if (op_type_ == OperationType::CreateTable ||
                                op_type_ == OperationType::TruncateTable)
                            {
                                if (!table_name.IsHashPartitioned())
                                {
                                    int32_t init_range_id =
                                        Sequences::InitialRangePartitionIdOf(
                                            table_name);
                                    succ =
                                        Sequences::InitIdOfTableRangePartition(
                                            table_name, init_range_id);
                                }

                                // secondary index are always range partitioned.
                                if (succ && index_names.size() > 0)
                                {
                                    succ = std::all_of(
                                        index_names.begin(),
                                        index_names.end(),
                                        [](const TableName &index_name)
                                        {
                                            int32_t init_range_id = Sequences::
                                                InitialRangePartitionIdOf(
                                                    index_name);
                                            return Sequences::
                                                InitIdOfTableRangePartition(
                                                    index_name, init_range_id);
                                        });
                                }
                            }
                            else if (op_type_ == OperationType::DropTable)
                            {
                                if (!table_name.IsHashPartitioned())
                                {
                                    succ = Sequences::DeleteSequence(
                                        table_name,
                                        SequenceType::RangePartitionId);
                                }

                                if (succ && index_names.size() > 0)
                                {
                                    succ = std::all_of(
                                        index_names.begin(),
                                        index_names.end(),
                                        [](const TableName &index_name)
                                        {
                                            return Sequences::DeleteSequence(
                                                index_name,
                                                SequenceType::RangePartitionId);
                                        });
                                }
                            }

                            bool has_auto_increment =
                                op_type_ == OperationType::CreateTable
                                    ? catalog_rec_.DirtySchema()
                                          ->HasAutoIncrement()
                                    : catalog_rec_.Schema()->HasAutoIncrement();
                            if (succ && has_auto_increment)
                            {
                                // Update sequence table about the auto
                                // increment info.
                                if (op_type_ == OperationType::CreateTable ||
                                    op_type_ == OperationType::TruncateTable)
                                {
                                    succ =
                                        Sequences::InitIdOfAutoIncrementColumn(
                                            table_name);
                                }
                                else if (op_type_ == OperationType::DropTable)
                                {
                                    succ = Sequences::DeleteSequence(
                                        table_name,
                                        SequenceType::AutoIncrementColumn);
                                }
                            }

                            if (succ)
                            {
                                hd_res.SetFinished();
                            }
                            else
                            {
                                LOG(ERROR) << "UpsertTableOp: failed to update"
                                              " sequence info for table: "
                                           << table_name.StringView();
                                hd_res.SetError(
                                    CcErrorCode::UPDATE_SEQUENCE_TABLE_FAIL);
                            }
                        });
                };

                DLOG(INFO) << "txn: " << txm->TxNumber()
                           << " process update_sequence_table_op_";
                op_ = &update_sequence_table_op_;
                txm->PushOperation(&update_sequence_table_op_);
                txm->Process(update_sequence_table_op_);
            }
            else if (op_type_ == OperationType::DropTable ||
                     op_type_ == OperationType::TruncateTable)
            {
                const TableSchema *table_old_schema = catalog_rec_.Schema();
                assert(table_old_schema->GetBaseTableName() ==
                       table_key_.Name());
                assert(clean_ccm_op_.table_names_.empty());

                auto clean_ccm_names = table_old_schema->IndexNames();

                clean_ccm_names.emplace_back(
                    table_old_schema->GetBaseTableName().StringView().data(),
                    table_old_schema->GetBaseTableName().StringView().size(),
                    table_old_schema->GetBaseTableName().Type(),
                    table_old_schema->GetBaseTableName().Engine());
                clean_ccm_op_.table_names_ = std::move(clean_ccm_names);
                clean_ccm_op_.clean_type_ = CleanType::CleanCcm;
                clean_ccm_op_.commit_ts_ = txm->CommitTs();

                LOG(INFO) << "UpsertTableOp: Clean all ccmap on all node "
                             "groups, txn: "
                          << txm->TxNumber();

                op_ = &clean_ccm_op_;
                txm->PushOperation(&clean_ccm_op_);
                txm->Process(clean_ccm_op_);
            }
            else
            {
                op_ = &lock_cluster_config_op_;
                txm->PushOperation(&lock_cluster_config_op_);
                txm->Process(lock_cluster_config_op_);
            }
        }
        else
        {
            assert(op_type_ == OperationType::Update);
            op_ = &lock_cluster_config_op_;
            txm->PushOperation(&lock_cluster_config_op_);
            txm->Process(lock_cluster_config_op_);
        }
    }
    else if (op_ == &update_sequence_table_op_)
    {
        if (update_sequence_table_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // Retry
                LOG(WARNING)
                    << "Upsert table schema transaction retry to update"
                       " sequence table, tx_number:"
                    << txm->TxNumber();
                txm->PushOperation(&update_sequence_table_op_);
                txm->Process(update_sequence_table_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }

        if (op_type_ == OperationType::CreateTable)
        {
            op_ = &lock_cluster_config_op_;
            txm->PushOperation(&lock_cluster_config_op_);
            txm->Process(lock_cluster_config_op_);
        }
        else
        {
            assert(op_type_ == OperationType::DropTable ||
                   op_type_ == OperationType::TruncateTable);

            const TableSchema *table_old_schema = catalog_rec_.Schema();
            assert(table_old_schema->GetBaseTableName() == table_key_.Name());
            assert(clean_ccm_op_.table_names_.empty());

            auto clean_ccm_names = table_old_schema->IndexNames();

            clean_ccm_names.emplace_back(
                table_old_schema->GetBaseTableName().StringView().data(),
                table_old_schema->GetBaseTableName().StringView().size(),
                table_old_schema->GetBaseTableName().Type(),
                table_old_schema->GetBaseTableName().Engine());
            clean_ccm_op_.table_names_ = std::move(clean_ccm_names);

            clean_ccm_op_.clean_type_ = CleanType::CleanCcm;
            clean_ccm_op_.commit_ts_ = txm->CommitTs();

            LOG(INFO)
                << "UpsertTableOp: Clean all ccmap on all node groups, txn: "
                << txm->TxNumber();

            op_ = &clean_ccm_op_;
            txm->PushOperation(&clean_ccm_op_);
            DLOG(INFO) << "txn: " << txm->TxNumber()
                       << " process clean_ccm_op_";
            txm->Process(clean_ccm_op_);
        }
    }
    else if (op_ == &acquire_all_lock_op_)
    {
        if (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_relaxed) > 0)
        {
            // Fails to acquire the write lock. The schema operation can
            // only roll forward after flushing the prepare log. Retries the
            // request if the tx node is still the leader or the tx is in
            // the recovery mode and the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                LOG(ERROR) << "Upsert table schema transaction failed to "
                              "acquire write lock, tx_number:"
                           << txm->TxNumber();
                // We downgrade catalog write lock to avoid blocking other
                // transaction which acquiring catalog read lock. And then retry
                // to acquire catalog write lock.
                if (acquire_all_lock_op_.IsDeadlock())
                {
                    LOG(ERROR) << "Upsert table schema transaction deadlocks "
                                  "with other transaction, downgrade write "
                                  "lock, tx_number: "
                               << txm->TxNumber();
                    post_all_lock_op_.write_type_ =
                        PostWriteType::DowngradeLock;
                    op_ = &post_all_lock_op_;
                    txm->PushOperation(&post_all_lock_op_);
                    txm->Process(post_all_lock_op_);
                }
                else
                {
                    txm->PushOperation(&acquire_all_lock_op_);
                    txm->Process(acquire_all_lock_op_);
                }
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else
        {
            op_ = &commit_log_op_;
            FillCommitLogRequest(txm);
            txm->PushOperation(&commit_log_op_);
            DLOG(INFO) << "txn: " << txm->TxNumber()
                       << " process commit_log_op_";
            txm->Process(commit_log_op_);
        }
    }
    else if (op_ == &commit_log_op_)
    {
        if (commit_log_op_.hd_result_.IsError())
        {
            // Fails to flush the commit log. Retries the operation if the
            // tx node is still the leader or the tx is in the  recovery
            // mode and the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                // set retry flag and retry commit log
                ::txlog::WriteLogRequest *log_req =
                    commit_log_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
                txm->PushOperation(&commit_log_op_);
                DLOG(INFO) << "txn: " << txm->TxNumber()
                           << " process commit_log_op_";
                txm->Process(commit_log_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else
        {
            if (op_type_ == OperationType::DropTable ||
                op_type_ == OperationType::TruncateTable)
            {
                // `clean_ccm_op` will notify each node group leader performs
                // `UpsertTable` and `KickoutData` independently for rocksdb.
                if (op_type_ == OperationType::TruncateTable &&
                    !txservice_skip_kv &&
                    !Sharder::Instance()
                         .GetDataStoreHandler()
                         ->IsSharedStorage())
                {
                    assert(clean_ccm_op_.table_names_.empty());
                    clean_ccm_op_.table_names_.emplace_back(
                        table_key_.Name().StringView().data(),
                        table_key_.Name().StringView().size(),
                        table_key_.Name().Type(),
                        table_key_.Name().Engine());
                    clean_ccm_op_.clean_type_ = CleanType::CleanCcm;
                    clean_ccm_op_.commit_ts_ = txm->CommitTs();

                    LOG(INFO) << "UpsertTableOp: Clean all ccmap on all node "
                                 "groups, txn: "
                              << txm->TxNumber();

                    op_ = &clean_ccm_op_;
                    txm->PushOperation(&clean_ccm_op_);
                    DLOG(INFO) << "txn: " << txm->TxNumber()
                               << " process clean_ccm_op_";
                    txm->Process(clean_ccm_op_);
                }
                else
                {
                    op_ = &upsert_kv_table_op_;
                    upsert_kv_table_op_.table_schema_old_ =
                        catalog_rec_.Schema();
                    upsert_kv_table_op_.table_schema_ =
                        catalog_rec_.DirtySchema();
                    upsert_kv_table_op_.alter_table_info_ = nullptr;
                    upsert_kv_table_op_.write_time_ = txm->commit_ts_;
                    txm->PushOperation(&upsert_kv_table_op_);
                    DLOG(INFO) << "txn: " << txm->TxNumber()
                               << " process upsert_kv_table_op_";
                    txm->Process(upsert_kv_table_op_);
                }
            }
            else
            {
                ACTION_FAULT_INJECTOR("upsert_table_post_all_lock");
                assert(post_all_lock_op_.write_type_ ==
                       PostWriteType::PostCommit);
                catalog_rec_.SetSchemaImage(catalog_rec_.DirtySchemaImage());
                catalog_rec_.ClearDirtySchema();
                op_ = &post_all_lock_op_;
                txm->PushOperation(&post_all_lock_op_);
                txm->Process(post_all_lock_op_);
            }
        }
    }
    else if (op_ == &clean_ccm_op_)
    {
        assert(op_type_ == OperationType::TruncateTable ||
               op_type_ == OperationType::DropTable);
        if (clean_ccm_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                LOG(ERROR)
                    << "UpsertTableOp: failed to truncate table, err code: "
                    << (int) clean_ccm_op_.hd_result_.ErrorCode()
                    << ", err msg: " << clean_ccm_op_.hd_result_.ErrorMsg()
                    << ", txn: " << txm->TxNumber() << ", keep retrying";
                op_ = &clean_ccm_op_;
                txm->PushOperation(&clean_ccm_op_);
                txm->Process(clean_ccm_op_);
            }
            else
            {
                ForceToFinish(txm);
            }

            return;
        }

        clean_ccm_op_.Clear();

        // Clear write set before commit dirty schema.
        txm->rw_set_.ClearWriteSet(table_key_.Name());
        txm->rw_set_.ClearReadSet(table_key_.Name());

        // Update catalog record to the latest schema.
        catalog_rec_.SetSchemaImage(catalog_rec_.DirtySchemaImage());
        catalog_rec_.ClearDirtySchema();

        // For DROP TABLE and TRUNCATE TABLE, the data store operation
        // happens after all write locks are acquired and commit log is
        // flushed.
        op_ = &post_all_lock_op_;
        txm->PushOperation(&post_all_lock_op_);
        DLOG(INFO) << "txn: " << txm->TxNumber()
                   << " process post_all_lock_op_";
        txm->Process(post_all_lock_op_);
    }
    else if (op_ == &post_all_lock_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            // The tx node is no longer the leader or leader candidate(during
            // recovery), ForceToFinish.
            ForceToFinish(txm);
            return;
        }

        // The tx's modification of the schema has finished. If the tx has
        // previously read the same schema and keeps a pointer in the read set
        // to the cc entry of the schema, removes it from the read set. As a
        // result, the tx will not try to release the read lock of the schema
        // when committing.
        for (size_t idx = 0; idx < acquire_all_intent_op_.upload_cnt_; ++idx)
        {
            const CcEntryAddr &schema_entry_addr =
                acquire_all_intent_op_.hd_results_[idx].Value().local_cce_addr_;
            if (schema_entry_addr.NodeGroupId() == txm->TxCcNodeId())
            {
                txm->rw_set_.DedupRead(schema_entry_addr);
            }
        }

        if (acquire_all_intent_op_.fail_cnt_.load(std::memory_order_relaxed) >
            0)
        {
            // The schema operation failed at acquire_all_intent_op_, without
            // flushing the prepare log. Do not retry post-processing (release
            // write intents) even if it fails. Remaining write intents on the
            // schema, if there are any, will be recovered by individual cc
            // nodes separately.

            txm->upsert_resp_->Finish(UpsertResult::Failed);

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());

            LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(shards->table_schema_op_pool_mux_);
            shards->table_schema_op_pool_.emplace_back(
                std::move(txm->schema_op_));
        }
        else if (post_all_lock_op_.hd_result_.IsError())
        {
            // post_all_lock_op_ returns an error:
            // 1. if flush kv succeeds, the schema op is guaranteed to succeed
            // and can only roll forward. Retry this step to install the
            // committed schema and remove write locks;
            // 2. if flush kx fails, the schema op has to roll backward. Retry
            // this step to reject dirty schema and remove write locks.
            txm->PushOperation(&post_all_lock_op_);
            DLOG(INFO) << "txn: " << txm->TxNumber()
                       << " process post_all_lock_op_, "
                       << post_all_lock_op_.hd_result_.ErrorMsg();
            txm->Process(post_all_lock_op_);
            return;
        }
        else
        {
            // post_all_lock_op_ has finished without an error.
            assert(!post_all_lock_op_.IsFailed());

            if (post_all_lock_op_.write_type_ == PostWriteType::DowngradeLock)
            {
                // Reset write type to PostCommit
                post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
                op_ = &acquire_all_lock_op_;
                txm->PushOperation(&acquire_all_lock_op_);
                txm->Process(acquire_all_lock_op_);
                return;
            }
            else if (txm->commit_ts_ != tx_op_failed_ts_)
            {
                assert(post_all_lock_op_.write_type_ !=
                       PostWriteType::DowngradeLock);
            }
            else
            {
                assert(post_all_lock_op_.write_type_ !=
                       PostWriteType::DowngradeLock);
                // Flush kv failed, or it is recovering from a flush kv failure.
                // This schema op has already been rolled back by now, only need
                // to flush clean log here.
                // Also, flush kv failure does not require lock upgrade(write
                // intent to write lock). So the CcEntryAddr needs to be kept in
                // rset in order to release the read lock when committing.
            }

            op_ = &clean_log_op_;
            FillCleanLogRequestCommon(txm, clean_log_op_);
            txm->PushOperation(&clean_log_op_);
            DLOG(INFO) << "txn: " << txm->TxNumber()
                       << " process clean_log_op_";
            txm->Process(clean_log_op_);
        }
    }
    else if (op_ == &clean_log_op_)
    {
        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        if (clean_log_op_.hd_result_.IsError() && txm->CheckLeaderTerm())
        {
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            txm->PushOperation(&clean_log_op_);
            DLOG(INFO) << "txn: " << txm->TxNumber()
                       << " process clean_log_op_";
            txm->Process(clean_log_op_);
        }
        else
        {
            CODE_FAULT_INJECTOR("alter_schema_term_changed", {
                LOG(INFO) << "FaultInject  alter_schema_term_changed";
                is_force_finished = true;
            });

            if (txm->commit_ts_ == tx_op_failed_ts_)
            {
                // Flush kv error or fail to flush prepare_log.
                txm->upsert_resp_->Finish(UpsertResult::Failed);
            }
            else
            {
                assert(txm->commit_ts_ > 0);
                if (is_force_finished)
                {
                    txm->upsert_resp_->Finish(UpsertResult::Unverified);
                }
                else
                {
                    txm->upsert_resp_->Finish(UpsertResult::Succeeded);
                }
            }

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());

            txm->schema_op_->catalog_rec_.Reset();
            std::unique_lock<std::mutex> lk(shards->table_schema_op_pool_mux_);
            shards->table_schema_op_pool_.emplace_back(
                std::move(txm->schema_op_));
        }
    }
}

void UpsertTableOp::Reset(const std::string_view table_name_str,
                          TableEngine table_engine,
                          const std::string &current_image,
                          uint64_t curr_schema_ts,
                          const std::string &dirty_image,
                          OperationType op_type,
                          TransactionExecution *txm)
{
    assert(op_type_ == OperationType::CreateTable ||
           op_type_ == OperationType::Update ||
           op_type_ == OperationType::DropTable ||
           op_type_ == OperationType::TruncateTable);

    // reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;

    // reset SchemaOp
    table_key_.Name() = TableName(table_name_str.data(),
                                  table_name_str.size(),
                                  TableType::Primary,
                                  table_engine);
    catalog_rec_.SetSchemaImage(current_image);
    catalog_rec_.SetDirtySchemaImage(dirty_image);
    image_str_ = current_image;
    dirty_image_str_ = dirty_image;
    curr_schema_ts_ = curr_schema_ts;

    // reset UpsertTableOp
    op_type_ = op_type;
    op_ = nullptr;

    // reset op
    read_cluster_result_.Reset();
    cluster_conf_rec_.Reset();
    lock_cluster_config_op_.Reset();
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.table_name_ = TableName(cluster_config_ccm_name_sv,
                                                    TableType::ClusterConfig,
                                                    TableEngine::None);
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;
    prepare_log_op_.Reset();
    unlock_cluster_config_op_.Reset();
    upsert_kv_table_op_.Reset();
    update_sequence_table_op_.Reset();
    commit_log_op_.Reset();
    clean_log_op_.Reset();

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.keys_.clear();
    acquire_all_intent_op_.keys_.emplace_back(&table_key_);
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    post_all_intent_op_.table_name_ = &catalog_ccm_name;
    post_all_intent_op_.keys_.clear();
    post_all_intent_op_.keys_.emplace_back(&table_key_);
    post_all_intent_op_.recs_.clear();
    post_all_intent_op_.recs_.push_back(&catalog_rec_);
    post_all_intent_op_.op_type_ = op_type_;
    post_all_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    upsert_kv_table_op_.alter_table_info_ = nullptr;
    upsert_kv_table_op_.op_type_ = op_type_;
    upsert_kv_table_op_.write_time_ = 0;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.keys_.clear();
    acquire_all_lock_op_.keys_.emplace_back(&table_key_);
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.keys_.clear();
    post_all_lock_op_.keys_.emplace_back(&table_key_);
    post_all_lock_op_.recs_.clear();
    post_all_lock_op_.recs_.push_back(&catalog_rec_);
    post_all_lock_op_.op_type_ = op_type_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    clean_ccm_op_.Clear();
    clean_ccm_op_.hd_result_.Reset();

    // reset cc_handler_res txm
    read_cluster_result_.ResetTxm(txm);
    acquire_all_intent_op_.ResetHandlerTxm(txm);
    prepare_log_op_.ResetHandlerTxm(txm);
    post_all_intent_op_.ResetHandlerTxm(txm);
    unlock_cluster_config_op_.ResetHandlerTxm(txm);
    upsert_kv_table_op_.ResetHandlerTxm(txm);
    update_sequence_table_op_.ResetHandlerTxm(txm);
    acquire_all_lock_op_.ResetHandlerTxm(txm);
    commit_log_op_.ResetHandlerTxm(txm);
    clean_ccm_op_.ResetHandlerTxm(txm);
    post_all_lock_op_.ResetHandlerTxm(txm);
    clean_log_op_.ResetHandlerTxm(txm);

    is_force_finished = false;
}

void UpsertTableOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    FillPrepareLogRequestCommon(txm, prepare_log_op_);
    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    auto &node_terms = *prepare_log_rec->mutable_node_terms();
    node_terms.clear();
    for (size_t idx = 0; idx < acquire_all_intent_op_.upload_cnt_; ++idx)
    {
        const AcquireAllResult &hres_val =
            acquire_all_intent_op_.hd_results_[idx].Value();
        uint32_t ng_id = hres_val.local_cce_addr_.NodeGroupId();
        node_terms[ng_id] = hres_val.node_term_;
    }
}

void UpsertTableOp::FillCommitLogRequest(TransactionExecution *txm)
{
    FillCommitLogRequestCommon(txm, commit_log_op_);

    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    assert(txm->commit_ts_ != tx_op_failed_ts_ ||
           upsert_kv_table_op_.hd_result_.IsError());
    commit_log_rec->set_commit_timestamp(txm->commit_ts_);
}

void UpsertTableOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    is_force_finished = true;
    Forward(txm);
}

FlushUpdateTableOp::FlushUpdateTableOp(TransactionExecution *txm)
    : op_(nullptr),
      post_all_intent_op_(txm),
      update_kv_table_op_(txm),
      need_write_log_(false),
      commit_log_op_(txm),
      clean_log_op_(txm)
{
    post_all_intent_op_.table_name_ = &catalog_ccm_name;
    post_all_intent_op_.op_type_ = OperationType::Update;
    post_all_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    update_kv_table_op_.op_func_ = [txm](AsyncOp<Void> &async_op)
    {
        CcHandlerResult<Void> &hd_res = async_op.hd_result_;
        hd_res.SetRefCnt(txm->rw_set_.CatalogWriteSetSize());
        for (const auto &[write_key, write_entry] :
             txm->rw_set_.CatalogWriteSet())
        {
            const CatalogRecord *catalog_rec =
                static_cast<const CatalogRecord *>(write_entry.rec_.get());
            assert(catalog_rec->DirtySchema());

            txm->cc_handler_->DataStoreUpsertTable(catalog_rec->Schema(),
                                                   catalog_rec->DirtySchema(),
                                                   OperationType::Update,
                                                   txm->CommitTs(),
                                                   txm->TxCcNodeId(),
                                                   txm->TxTerm(),
                                                   hd_res,
                                                   nullptr);
        }
    };
}

void FlushUpdateTableOp::Reset()
{
    op_ = nullptr;

    post_all_intent_op_.Reset(0);
    post_all_intent_op_.keys_.clear();
    post_all_intent_op_.recs_.clear();

    update_kv_table_op_.Reset();

    need_write_log_ = false;
    commit_log_op_.Reset();
    clean_log_op_.Reset();
}

void FlushUpdateTableOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        op_ = &post_all_intent_op_;
        post_all_intent_op_.keys_.reserve(txm->rw_set_.CatalogWriteSetSize());
        post_all_intent_op_.recs_.reserve(txm->rw_set_.CatalogWriteSetSize());
        for (const auto &[write_key, write_entry] :
             txm->rw_set_.CatalogWriteSet())
        {
            post_all_intent_op_.keys_.push_back(write_key.GetShallowCopy());
            post_all_intent_op_.recs_.push_back(write_entry.rec_.get());
        }
        txm->PushOperation(&post_all_intent_op_);
        txm->Process(post_all_intent_op_);
    }
    else if (op_ == &post_all_intent_op_)
    {
        if (post_all_intent_op_.IsFailed())
        {
            if (txm->CheckLeaderTerm())
            {
                txm->PushOperation(&post_all_intent_op_);
                txm->Process(post_all_intent_op_);
            }
            else
            {
                LOG(ERROR) << "txm: " << txm->TxNumber()
                           << " failed to post all intent. error: "
                           << post_all_intent_op_.hd_result_.ErrorMsg();
                Reset();
                txm->state_stack_.clear();
                txm->Abort();
            }
        }
        else
        {
            op_ = &update_kv_table_op_;
            txm->PushOperation(&update_kv_table_op_);
            txm->Process(update_kv_table_op_);
        }
    }
    else if (op_ == &update_kv_table_op_)
    {
        if (update_kv_table_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                txm->PushOperation(&update_kv_table_op_);
                txm->Process(update_kv_table_op_);
            }
            else
            {
                LOG(ERROR) << "txm: " << txm->TxNumber()
                           << " failed to update kv table. error: "
                           << update_kv_table_op_.hd_result_.ErrorMsg();
                Reset();
                txm->state_stack_.clear();
                txm->Abort();
            }
        }
        else
        {
            if (need_write_log_)
            {
                txm->FillCommitCatalogsLogRequest(commit_log_op_);
                op_ = &commit_log_op_;
                txm->PushOperation(&commit_log_op_);
                txm->Process(commit_log_op_);
            }
            else
            {
                Reset();
                txm->state_stack_.pop_back();
                assert(txm->state_stack_.back() == &txm->post_process_);
            }
        }
    }
    else if (op_ == &commit_log_op_)
    {
        if (commit_log_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // set retry flag and retry commit log
                ::txlog::WriteLogRequest *log_req =
                    commit_log_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
                txm->PushOperation(&commit_log_op_);
                DLOG(INFO) << "txn: " << txm->TxNumber()
                           << " process commit_log_op_";
                txm->Process(commit_log_op_);
            }
        }
        else
        {
            txm->FillCleanCatalogsLogRequest(clean_log_op_);
            op_ = &clean_log_op_;
            txm->PushOperation(&clean_log_op_);
            txm->Process(clean_log_op_);
        }
    }
    else
    {
        assert(op_ == &clean_log_op_);
        if (clean_log_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // set retry flag and retry clean log
                ::txlog::WriteLogRequest *log_req =
                    clean_log_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
                txm->PushOperation(&clean_log_op_);
                DLOG(INFO) << "txn: " << txm->TxNumber()
                           << " process clean_log_op_";
                txm->Process(clean_log_op_);
            }
            else
            {
                Reset();
                txm->state_stack_.clear();
                txm->Abort();
            }
        }
        else
        {
            Reset();
            txm->state_stack_.pop_back();
            assert(txm->state_stack_.back() == &txm->post_process_);
        }
    }
}

SleepOperation::SleepOperation(TransactionExecution *txm)
{
}

void SleepOperation::Forward(TransactionExecution *txm)
{
    // forward of sleep op will check whether the sleep time reached. Note
    // that we cannot use sleep_for API since the TxProcessor thread cannot
    // be blocked. Instead we use the TimeOut interface to simulate sleep.
    // It may not be accurate, but retry logic is not sensitive to it.
    if (txm->IsTimeOut(sleep_secs_))
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeOut",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });
        // pop the sleep op and re-execute the last failed op.
        txm->state_stack_.pop_back();
    }
}

CleanCcEntryForTestOp::CleanCcEntryForTestOp(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void CleanCcEntryForTestOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.Value() == true)
        {
            succeed_ = true;
            txm->PostProcess(*this);
        }
        else
        {
            succeed_ = false;
            txm->PostProcess(*this);
        }
    }
}

NoOp::NoOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void NoOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeOut",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });
        hd_result_.ForceError();
        txm->PostProcess(*this);
    }
}

template <typename ResultType>
AsyncOp<ResultType>::AsyncOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

template <typename ResultType>
AsyncOp<ResultType>::~AsyncOp()
{
    if (worker_thread_.joinable())
    {
        worker_thread_.join();
    }
}

template <typename ResultType>
void AsyncOp<ResultType>::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

template <typename ResultType>
void AsyncOp<ResultType>::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (worker_thread_.joinable())
        {
            // The worker thread must terminate after the hd_result.SetFinished.
            // It is the caller's responsibility to ensure that this
            // worker_thread_ does not have any more work to do after the worker
            // thread function returns.
            worker_thread_.join();
        }
        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeOut",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });
        // Not necessary to have timeout for ds operation, you don't
        // estimate the proper op time out secs, and the dsop will finished
        // anyway

        // bool succ = hd_result_.ForceError();
        // if (succ)
        // {
        //     txm->PostProcess(*this);
        // }
        DLOG(INFO) << "timeout for ayncop.";
    }
}

template <typename ResultType>
void AsyncOp<ResultType>::Reset()
{
    hd_result_.Reset();
    if (worker_thread_.joinable())
    {
        worker_thread_.join();
    }
}

template struct AsyncOp<PostProcessResult>;
template struct AsyncOp<Void>;
template struct AsyncOp<GenerateSkParallelResult>;

CompositeTransactionOperation::CompositeTransactionOperation() : op_(nullptr)
{
}

template <typename Op>
void CompositeTransactionOperation::ForwardToSubOperation(
    TransactionExecution *txm, Op *next_op)
{
    op_ = next_op;
    txm->PushOperation(next_op);
    txm->Process(*next_op);
}

template <typename Op>
void CompositeTransactionOperation::RetrySubOperation(TransactionExecution *txm,
                                                      Op *last_sub_op,
                                                      bool retry_immediately)
{
    if (!retry_immediately)
    {
        op_ = last_sub_op;
        txm->PushOperation(last_sub_op, 1);
        last_sub_op->ReRunOp(txm);
    }
    else
    {
        ForwardToSubOperation(txm, last_sub_op);
    }
}

KickoutDataOp::KickoutDataOp(TransactionExecution *txm) : hd_result_(txm)
{
}

void KickoutDataOp::Reset()
{
    table_name_ = nullptr;
    start_key_ = TxKey();
    end_key_ = TxKey();
    node_group_ = 0;
    hd_result_.Reset();
}

void KickoutDataOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void KickoutDataOp::Forward(TransactionExecution *txm)
{
    // Start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        // If leader-transferred, do not need to kickout data any more.
        if (hd_result_.IsError() &&
            hd_result_.ErrorCode() != CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
            hd_result_.ErrorCode() != CcErrorCode::TX_NODE_NOT_LEADER)
        {
            if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        txm->PostProcess(*this);
    }
    else if (hd_result_.LocalRefCnt() == 0)
    {
        if (txm->IsTimeOut(10))
        {
            LOG(WARNING) << "Kickout data operation timeout 10s";
            bool force_error = hd_result_.ForceError();
            if (force_error)
            {
                txm->PostProcess(*this);
                return;
            }
        }
    }
}

KickoutDataAllOp::KickoutDataAllOp(TransactionExecution *txm) : hd_result_(txm)
{
}

void KickoutDataAllOp::Reset(uint32_t ng_cnt, size_t table_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(ng_cnt * table_cnt);
}

void KickoutDataAllOp::Clear()
{
    table_names_.clear();
    table_names_.shrink_to_fit();
}

void KickoutDataAllOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void KickoutDataAllOp::Forward(TransactionExecution *txm)
{
    // Start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        // If leader-transferred, do not need to kickout data any more.
        if (hd_result_.IsError() &&
            hd_result_.ErrorCode() != CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
            hd_result_.ErrorCode() != CcErrorCode::TX_NODE_NOT_LEADER)
        {
            if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        txm->PostProcess(*this);
        return;
    }

    if (hd_result_.LocalRefCnt() == 0 && txm->IsTimeOut(10))
    {
        LOG(WARNING) << "Kickout data all operation timeout 10s";
        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->PostProcess(*this);
            return;
        }
    }
}

/**
 * @brief Construct a new Split Flush Range Op:: Split Flush Range Op object
 */
SplitFlushRangeOp::SplitFlushRangeOp(
    const TableName &table_name,
    std::shared_ptr<const TableSchema> &&table_schema,
    TableRangeEntry *range_entry,
    std::vector<std::pair<TxKey, int32_t>> &&new_range_info,
    TransactionExecution *txm,
    bool is_dirty)
    : CompositeTransactionOperation(),
      table_schema_(std::move(table_schema)),
      table_name_(table_name.String(), table_name.Type(), table_name.Engine()),
      range_table_name_(table_name_.StringView(),
                        TableType::RangePartition,
                        table_name_.Engine()),
      read_cluster_result_(txm),
      range_entry_(range_entry),
      new_range_info_(std::move(new_range_info)),
      is_dirty_(is_dirty),
      lock_cluster_config_op_(),
      prepare_acquire_all_write_op_(txm),
      prepare_log_op_(txm),
      install_new_range_op_(txm),
      unlock_cluster_config_op_(txm),
      data_sync_op_(txm),
      commit_acquire_all_write_op_(txm),
      update_key_cache_op_(txm),
      commit_log_op_(txm),
      ds_upsert_range_op_(txm),
      kickout_old_range_data_op_(txm),
      post_all_lock_op_(txm),
      ds_clean_old_range_op_(txm),
      clean_log_op_(txm)
{
    // The clone of the input range info makes a new copy of the range's start
    // key, while the range's end key references that of the old range info in
    // LocalCcShards. The new copy is necessary, because we may have separate
    // threads to persist range updates to stable storage, while the tx's node
    // group fails over and the old range is invalidated.
    range_info_ = range_entry->GetRangeInfo()->Clone();
    old_start_key_ = range_info_->StartTxKey();
    old_end_key_ = range_info_->EndTxKey();

    range_record_ = std::make_unique<RangeRecord>(range_info_.get(), nullptr);

    lock_cluster_config_op_.table_name_ = TableName(cluster_config_ccm_name_sv,
                                                    TableType::ClusterConfig,
                                                    TableEngine::None);
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;

    prepare_acquire_all_write_op_.table_name_ = &range_table_name_;
    prepare_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    prepare_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    prepare_acquire_all_write_op_.keys_.emplace_back(
        old_start_key_.GetShallowCopy());

    install_new_range_op_.table_name_ = &range_table_name_;
    install_new_range_op_.write_type_ = PostWriteType::PrepareCommit;
    install_new_range_op_.op_type_ = OperationType::Update;
    install_new_range_op_.keys_.emplace_back(old_start_key_.GetShallowCopy());
    install_new_range_op_.recs_.push_back(range_record_.get());

    commit_acquire_all_write_op_.table_name_ = &range_table_name_;
    commit_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    commit_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    commit_acquire_all_write_op_.keys_.emplace_back(
        old_start_key_.GetShallowCopy());

    kickout_old_range_data_op_.table_name_ = &table_name_;
    kickout_old_range_data_op_.node_group_ = txm->TxCcNodeId();

    post_all_lock_op_.table_name_ = &range_table_name_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
    post_all_lock_op_.op_type_ = OperationType::Update;
    post_all_lock_op_.keys_.emplace_back(old_start_key_.GetShallowCopy());
    post_all_lock_op_.recs_.push_back(range_record_.get());

    TX_TRACE_ASSOCIATE(
        this, &prepare_acquire_all_write_op_, "prepare_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this, &install_new_range_op_, "install_new_range_op_");
    TX_TRACE_ASSOCIATE(this, &data_sync_op_, "data_sync_op_");
    TX_TRACE_ASSOCIATE(
        this, &commit_acquire_all_write_op_, "commit_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &ds_upsert_range_op_, "ds_upsert_range_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &ds_clean_old_range_op_, "ds_clean_old_range_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
}

void SplitFlushRangeOp::Reset(
    const TableName &table_name,
    std::shared_ptr<const TableSchema> &&table_schema,
    TableRangeEntry *range_entry,
    std::vector<std::pair<TxKey, int32_t>> &&new_range_info,
    TransactionExecution *txm,
    bool is_dirty)
{
    // Reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;
    op_start_ = metrics::TimePoint::max();

    // Reset CompositeTransactionOperation
    op_ = nullptr;

    // Reset SplitFlushRangeOp
    table_name_ =
        TableName(table_name.String(), table_name.Type(), table_name.Engine());
    table_schema_ = std::move(table_schema);
    range_table_name_ = TableName(table_name_.StringView(),
                                  TableType::RangePartition,
                                  table_name_.Engine());

    range_entry_ = range_entry;
    range_info_ = range_entry_->GetRangeInfo()->Clone();
    old_start_key_ = range_info_->StartTxKey();
    old_end_key_ = range_info_->EndTxKey();

    range_record_ = std::make_unique<RangeRecord>(range_info_.get(), nullptr);

    is_dirty_ = is_dirty;

    new_range_info_ = std::move(new_range_info);

    // Reset all sub-operations
    lock_cluster_config_op_.Reset();

    prepare_acquire_all_write_op_.ResetHandlerTxm(txm);

    prepare_log_op_.Reset();
    prepare_log_op_.ResetHandlerTxm(txm);

    install_new_range_op_.ResetHandlerTxm(txm);

    unlock_cluster_config_op_.Reset();
    unlock_cluster_config_op_.ResetHandlerTxm(txm);

    data_sync_op_.Reset();
    data_sync_op_.ResetHandlerTxm(txm);

    commit_acquire_all_write_op_.ResetHandlerTxm(txm);

    update_key_cache_op_.Reset();
    update_key_cache_op_.ResetHandlerTxm(txm);

    commit_log_op_.Reset();
    commit_log_op_.ResetHandlerTxm(txm);

    ds_upsert_range_op_.Reset();
    ds_upsert_range_op_.ResetHandlerTxm(txm);

    kickout_old_range_data_op_.Reset();
    kickout_old_range_data_op_.ResetHandlerTxm(txm);

    post_all_lock_op_.ResetHandlerTxm(txm);

    ds_clean_old_range_op_.Reset();
    ds_clean_old_range_op_.ResetHandlerTxm(txm);

    clean_log_op_.Reset();
    clean_log_op_.ResetHandlerTxm(txm);

    read_cluster_result_.Reset();
    read_cluster_result_.ResetTxm(txm);
    cluster_conf_rec_.Reset();
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.table_name_ = TableName(cluster_config_ccm_name_sv,
                                                    TableType::ClusterConfig,
                                                    TableEngine::None);
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;

    prepare_acquire_all_write_op_.table_name_ = &range_table_name_;
    prepare_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    prepare_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    prepare_acquire_all_write_op_.keys_.clear();
    prepare_acquire_all_write_op_.keys_.emplace_back(
        old_start_key_.GetShallowCopy());

    install_new_range_op_.table_name_ = &range_table_name_;
    install_new_range_op_.write_type_ = PostWriteType::PrepareCommit;
    install_new_range_op_.op_type_ = OperationType::Update;
    install_new_range_op_.keys_.clear();
    install_new_range_op_.keys_.emplace_back(old_start_key_.GetShallowCopy());
    install_new_range_op_.recs_.clear();
    install_new_range_op_.recs_.push_back(range_record_.get());

    commit_acquire_all_write_op_.table_name_ = &range_table_name_;
    commit_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    commit_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    commit_acquire_all_write_op_.keys_.clear();
    commit_acquire_all_write_op_.keys_.emplace_back(
        old_start_key_.GetShallowCopy());

    kickout_old_range_data_op_.table_name_ = &table_name_;
    kickout_old_range_data_op_.node_group_ = txm->TxCcNodeId();

    post_all_lock_op_.table_name_ = &range_table_name_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
    post_all_lock_op_.op_type_ = OperationType::Update;
    post_all_lock_op_.keys_.clear();
    post_all_lock_op_.keys_.emplace_back(old_start_key_.GetShallowCopy());
    post_all_lock_op_.recs_.clear();
    post_all_lock_op_.recs_.push_back(range_record_.get());

    kickout_data_it_ = {};

    TX_TRACE_ASSOCIATE(
        this, &prepare_acquire_all_write_op_, "prepare_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this, &install_new_range_op_, "install_new_range_op_");
    TX_TRACE_ASSOCIATE(this, &data_sync_op_, "data_syn_op_");
    TX_TRACE_ASSOCIATE(
        this, &commit_acquire_all_write_op_, "commit_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &ds_upsert_range_op_, "ds_upsert_range_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &ds_clean_old_range_op_, "ds_clean_old_range_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
}

void SplitFlushRangeOp::ClearInfos()
{
    // release TxKey ownership to reduce memory usage
    range_info_ = nullptr;
    new_range_info_.clear();

    new_range_info_.shrink_to_fit();

    range_record_ = nullptr;
}

void SplitFlushRangeOp::Forward(TransactionExecution *txm)
{
    if (txm->TxStatus() == TxnStatus::Recovering &&
        Sharder::Instance().LeaderTerm(txm->TxCcNodeId()) < 0)
    {
        // This is a recovered tx and replay is not done yet. We should wait for
        // replay finish before forwarding tx machine.
        if (Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId()) !=
            txm->TxTerm())
        {
            // Recovered term is invalid. Do not call ForceToFinish as it will
            // cause infinite recursive call. Clean up tx state directly.
            txm->bool_resp_->Finish(false);

            ClearInfos();

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());

            assert(this == txm->split_flush_op_.get());
            LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(
                shards->split_flush_range_op_pool_mux_);
            shards->split_flush_range_op_pool_.emplace_back(
                std::move(txm->split_flush_op_));
            assert(txm->split_flush_op_ == nullptr);
        }
        return;
    }
    if (op_ == nullptr)
    {
        // Initialize commit ts as the start time of tx. This value will
        // be updated after prepaire_acquire_all_write_op_.
        txm->commit_ts_ = txm->start_ts_ + 1;
        op_ = &lock_cluster_config_op_;
        txm->PushOperation(&lock_cluster_config_op_);
        txm->Process(lock_cluster_config_op_);
    }
    else if (op_ == &lock_cluster_config_op_)
    {
        if (lock_cluster_config_op_.hd_result_->IsError())
        {
            LOG(ERROR) << "Split Flush read cluster config failed, tx_number:"
                       << txm->TxNumber();
            ForceToFinish(txm);
            return;
        }

        if (!prepare_log_op_.hd_result_.IsFinished())
        {
            // Acquire Write lock on range entry on all node groups and
            // calculate commit ts for tx.
            LOG(INFO)
                << "Split Flush transaction prepare acquire all, range id "
                << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
            ForwardToSubOperation(txm, &prepare_acquire_all_write_op_);
        }
        else
        {
            assert(!commit_log_op_.hd_result_.IsFinished());
            LOG(INFO) << "Split Flush transaction commit acqurie all, range id "
                      << range_info_->PartitionId()
                      << ", txn: " << txm->TxNumber();
            // Upgrade to write lock again for commit phase.
            ForwardToSubOperation(txm, &commit_acquire_all_write_op_);
        }
    }
    else if (op_ == &prepare_acquire_all_write_op_)
    {
        if (prepare_acquire_all_write_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Split Flush transaction failed to obtain write "
                          "lock, tx_number:"
                       << txm->TxNumber();

            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
            return;
        }

        // Update the commit ts. The commit ts is the max value between
        // 1) max commit ts of last tx that updated records in this range
        // partition on all node groups, 2) local timestamp when tx started.
        // The commit ts is used to decide whether a record needs to be
        // flushed during the next phase. We want to make sure every records
        // modified before commit ts is flushed to new partition in KV
        // store.
        for (size_t idx = 0; idx < prepare_acquire_all_write_op_.upload_cnt_;
             ++idx)
        {
            const AcquireAllResult &acq_all_res =
                prepare_acquire_all_write_op_.hd_results_[idx].Value();
            uint64_t ts = std::max(acq_all_res.commit_ts_ + 1,
                                   acq_all_res.last_vali_ts_ + 1);
            txm->commit_ts_ = std::max(txm->commit_ts_, ts);
        }

        // Write prepare log in next subop.
        FillPrepareLogRequest(txm);
        LOG(INFO) << "Split Flush transaction write prepare log, range id "
                  << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        assert(txm->rw_set_.WriteSetSize() == 0);
        if (prepare_log_op_.hd_result_.IsError())
        {
            if (prepare_log_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the
                // coordinator itself is no longer leader
                if (txm->CheckLeaderTerm())
                {
                    LOG(WARNING)
                        << "Split range write prepare log result unknown, "
                           "tx_number:"
                        << txm->TxNumber() << ", keep retrying";
                    // set retry flag and retry prepare log
                    ::txlog::WriteLogRequest *log_req =
                        prepare_log_op_.log_closure_.LogRequest()
                            .mutable_write_log_request();
                    log_req->set_retry(true);
                    RetrySubOperation(txm, &prepare_log_op_);
                }
                else
                {
                    LOG(ERROR) << "Split range write prepare log result "
                                  "unknown, tx_number:"
                               << txm->TxNumber()
                               << ", not leader any more, stop retrying";
                    // Not leader anymore, just quit. New leader will know
                    // whether prepare log succeeds and continue the rest if
                    // it does. Should not release the write intents. If
                    // prepare log is not written, the write intents will be
                    // released individually via orphan lock recovery
                    // mechanism.
                    ForceToFinish(txm);
                }
            }
            else
            {
                // Set commit ts to 0 to indicate transaction failure.
                // post_all_lock_op_ will release locks acquired.
                txm->commit_ts_ = tx_op_failed_ts_;
                ForwardToSubOperation(txm, &post_all_lock_op_);
            }
            return;
        }

        // Fill in new range info to old range record.
        range_info_->dirty_ts_ = txm->commit_ts_;
        range_info_->SetNewRanges(new_range_info_);

        // Install dirty range info on all node groups and downgrade to
        // write intent lock in next subop.
        range_record_->SetRangeInfo(range_info_.get());

        LOG(INFO) << "Split Flush transaction install dirty range, range id "
                  << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &install_new_range_op_);
    }
    else if (op_ == &install_new_range_op_)
    {
        if (install_new_range_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                LOG(ERROR) << "Split Flush transaction failed to install dirty "
                              "range, tx number "
                           << txm->TxNumber();
                range_record_->SetRangeInfo(range_info_.get());
                RetrySubOperation(txm, &install_new_range_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }

        CODE_FAULT_INJECTOR("split_flush_range_install_dirty_continue",
                            { return; });

        // Get cce addr of cluster config read lock from rset.
        const CcEntryAddr *cluster_config_addr = nullptr;
        const ReadSetEntry *cluster_config_entry = nullptr;
        const auto &meta_rset = txm->rw_set_.MetaDataReadSet();
        for (const auto &[cce_addr, read_entry_pair] : meta_rset)
        {
            if (read_entry_pair.second == cluster_config_ccm_name_sv)
            {
                cluster_config_addr = &cce_addr;
                cluster_config_entry = &read_entry_pair.first;
                break;
            }
        }
        assert(cluster_config_addr && cluster_config_entry);
        unlock_cluster_config_op_.Reset(cluster_config_addr,
                                        cluster_config_entry);

        LOG(INFO) << "Split Flush transaction unlock cluster config, range id "
                  << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &unlock_cluster_config_op_);
    }
    else if (op_ == &unlock_cluster_config_op_)
    {
        if (unlock_cluster_config_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // Releasing a local read lock should never fail as long as
                // leader is not tranferred away.
                assert(false);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }

        if (commit_log_op_.hd_result_.IsFinished())
        {
            // Delete stale data from old partition
            ds_clean_old_range_op_.op_func_ =
                [partition_id = range_info_->partition_id_,
                 &start_key = new_range_info_.front().first,
                 &table_name = table_name_,
                 table_schema = table_schema_.get()](AsyncOp<Void> &async_op)
            {
                CcHandlerResult<Void> &hd_res = async_op.hd_result_;

                TxWorkerPool *tx_worker_pool =
                    Sharder::Instance().GetTxWorkerPool();
                store::DataStoreHandler *const store_hd =
                    Sharder::Instance().GetLocalCcShards()->store_hd_;
#ifdef EXT_TX_PROC_ENABLED
                hd_res.SetToBlock();
#endif
                tx_worker_pool->SubmitWork(
                    [partition_id,
                     &start_key,
                     table_name,
                     table_schema,
                     &hd_res,
                     store_hd](size_t)
                    {
                        bool succ = store_hd->DeleteOutOfRangeData(
                            table_name, partition_id, &start_key, table_schema);
                        if (succ)
                        {
                            hd_res.SetFinished();
                        }
                        else
                        {
                            hd_res.SetError(CcErrorCode::DATA_STORE_ERR);
                        }
                    });
            };
            LOG(INFO) << "Split Flush transaction clean old range data in kv "
                         "store, range id "
                      << range_info_->PartitionId()
                      << ", txn: " << txm->TxNumber();
            ForwardToSubOperation(txm, &ds_clean_old_range_op_);
        }
        else
        {
            assert(prepare_log_op_.hd_result_.IsFinished());

            auto local_cc_shards = Sharder::Instance().GetLocalCcShards();
            data_sync_op_.op_func_ = [this,
                                      table_schema = table_schema_,
                                      txn = txm->TxNumber(),
                                      tx_term = txm->tx_term_,
                                      node_group = txm->TxCcNodeId(),
                                      ckpt_ts = txm->commit_ts_,
                                      &local_cc_shards = *local_cc_shards](
                                         AsyncOp<Void> &async_op) mutable
            {
                CcHandlerResult<Void> &hd_res = async_op.hd_result_;
#ifdef EXT_TX_PROC_ENABLED
                hd_res.SetToBlock();
#endif
                // Enqueue DataSyncTask for subranges.
                local_cc_shards.EnqueueDataSyncTaskForSplittingRange(
                    table_name_,
                    node_group,
                    tx_term,
                    table_schema,
                    range_entry_,
                    ckpt_ts,
                    is_dirty_,
                    txn,
                    &hd_res);
            };

            LOG(INFO) << "Split Flush transaction data sync, range id "
                      << range_info_->PartitionId()
                      << ", txn: " << txm->TxNumber();
            ForwardToSubOperation(txm, &data_sync_op_);
        }
    }
    else if (op_ == &data_sync_op_)
    {
        if (data_sync_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                LOG(ERROR)
                    << "Split Flush transaction failed to data sync, tx number "
                    << txm->TxNumber();

                bool retry_imme = !(data_sync_op_.hd_result_.ErrorCode() ==
                                    CcErrorCode::DATA_STORE_ERR);
                RetrySubOperation(txm, &data_sync_op_, retry_imme);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }

        CODE_FAULT_INJECTOR("term_SplitFlushOp_DataSyncOp_Continue", {
            LOG(INFO) << "FaultInject term_SplitFlushOp_DataSyncOp_Continue";
            return;
        });

        LOG(INFO) << "Split Flush transaction lock cluster config, range id"
                  << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
        // Upgrade to write lock again for commit phase.
        ForwardToSubOperation(txm, &lock_cluster_config_op_);
    }
    else if (op_ == &commit_acquire_all_write_op_)
    {
        if (commit_acquire_all_write_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            if (txm->CheckLeaderTerm())
            {
                LOG(ERROR) << "Split Flush transaction failed to obtain write "
                              "lock, tx_number:"
                           << txm->TxNumber();
                if (commit_acquire_all_write_op_.IsDeadlock())
                {
                    LOG(ERROR)
                        << "Split Flush transaction deadlocks with other "
                           "transactions, downgrade write lock, tx_number:"
                        << txm->TxNumber();
                    // We downgrade range write lock to write intent lock.
                    post_all_lock_op_.write_type_ =
                        PostWriteType::DowngradeLock;
                    ForwardToSubOperation(txm, &post_all_lock_op_);
                }
                else
                {
                    RetrySubOperation(txm, &commit_acquire_all_write_op_);
                }
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }

        ACTION_FAULT_INJECTOR("range_split_commit_acquire_all");

        if (table_name_.IsBase() && txservice_enable_key_cache)
        {
            // Update keycache
            update_key_cache_op_.op_func_ =
                [this,
                 txm,
                 &old_end_key = old_end_key_,
                 &new_ranges = new_range_info_](AsyncOp<Void> &async_op)
            {
                NodeGroupId node_group = txm->TxCcNodeId();
                int64_t tx_term = txm->TxTerm();
                LocalCcShards *local_shards =
                    Sharder::Instance().GetLocalCcShards();
                // The new ranges that still lands to the same ng after split.
                std::vector<std::pair<const TxKey *, const TxKey *>> ranges;
                ranges.reserve(new_ranges.size());
                for (auto iter = new_ranges.begin(); iter != new_ranges.end();
                     ++iter)
                {
                    if (local_shards->GetRangeOwner(iter->second, node_group)
                            ->BucketOwner() == node_group)
                    {
                        const TxKey *start_key = &(iter->first);
                        const TxKey *end_key =
                            std::next(iter) == new_ranges.end()
                                ? &old_end_key
                                : &(std::next(iter)->first);
                        ranges.push_back({start_key, end_key});
                    }
                    else
                    {
                        // Will be kicked out.
                    }
                }

                CcHandlerResult<Void> &hd_result = async_op.hd_result_;
                if (ranges.size() == 0)
                {
                    hd_result.SetFinished();
                    return;
                }

                hd_result.SetRefCnt(ranges.size());

                // For keys that are splitted to another ng, they will be
                // removed from key cache when they are eviceted from ccm. But
                // for keys that still lands on the same ng after split, we need
                // to delete them from key cache to avoid an ever increasing key
                // cache load factor.
                assert(table_name_.Type() == TableType::Primary);
                for (auto &key_pair : ranges)
                {
                    txm->cc_handler_->UpdateKeyCache(
                        table_name_,
                        node_group,
                        tx_term,
                        *key_pair.first,
                        *key_pair.second,
                        range_entry_->RangeSlices(),
                        hd_result);
                }
            };

            LOG(INFO) << "Split Flush transaction update keycache, range id "
                      << range_info_->PartitionId()
                      << ", txn: " << txm->TxNumber();
            ForwardToSubOperation(txm, &update_key_cache_op_);
        }
        else
        {
            FillCommitLogRequest(txm);
            LOG(INFO) << "Split Flush transaction write commit log, range id "
                      << range_info_->PartitionId()
                      << ", txn: " << txm->TxNumber();
            ForwardToSubOperation(txm, &commit_log_op_);
        }
    }
    else if (op_ == &update_key_cache_op_)
    {
        if (update_key_cache_op_.hd_result_.IsError())
        {
            // ng term changed.
            assert(update_key_cache_op_.hd_result_.ErrorCode() ==
                   CcErrorCode::NG_TERM_CHANGED);
            ForceToFinish(txm);
            return;
        }

        FillCommitLogRequest(txm);
        LOG(INFO) << "Split Flush transaction write commit log, range id "
                  << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &commit_log_op_);
    }
    else if (op_ == &commit_log_op_)
    {
        if (commit_log_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // error & retry
                ::txlog::WriteLogRequest *log_req =
                    commit_log_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
                RetrySubOperation(txm, &commit_log_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }
        // Split the range slices based on the range split keys.
        std::vector<SplitRangeInfo> splitted_range_info =
            GenSplittedRangeInfos();

        // Insert new ranges into data store range table. Update
        // range slice size of the old range.
        ds_upsert_range_op_.op_func_ =
            [&table_name = table_name_,
             range_info = std::move(splitted_range_info),
             tx_ts = txm->commit_ts_](AsyncOp<Void> &async_op) mutable
        {
            CcHandlerResult<Void> &hd_res = async_op.hd_result_;
#ifdef EXT_TX_PROC_ENABLED
            hd_res.SetToBlock();
            // The memory fence ensures that the block flag is set before the
            // background data store thread is executed.
            std::atomic_thread_fence(std::memory_order_release);
#endif
            // Launch a new thread instead of sending it to workerpool to
            // avoid being blocked during write lock is held.
            async_op.worker_thread_ = std::thread(
                [table_name,
                 range_info = std::move(range_info),
                 tx_ts,
                 &hd_res]() mutable
                {
                    store::DataStoreHandler *const store_hd =
                        Sharder::Instance().GetLocalCcShards()->store_hd_;
                    bool succ = store_hd->UpsertRanges(
                        table_name, std::move(range_info), tx_ts);
                    if (succ)
                    {
                        hd_res.SetFinished();
                    }
                    else
                    {
                        hd_res.SetError(CcErrorCode::DATA_STORE_ERR);
                    }
                });
        };

        LOG(INFO) << "Split Flush transaction upsert new range spec, range id "
                  << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &ds_upsert_range_op_);
    }
    else if (op_ == &ds_upsert_range_op_)
    {
        if (ds_upsert_range_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // error & retry
                LOG(ERROR)
                    << "Split Flush transaction failed to update range info "
                       "in data store, tx_number:"
                    << txm->TxNumber();
                RetrySubOperation(txm, &ds_upsert_range_op_, false);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }
        cleaning_old_range_dirty_owner_ = false;
        kickout_data_it_ = new_range_info_.cbegin();
        // Kickout old range data. For those data that now falls on a
        // new node, we need to kickout them out from the old node's ccmap.
        // We don't care about the commit ts of the target cc entry, all entries
        // fall into the migrated new ranges should be kicked out no matter
        // what.
        if (ForwardKickoutIterator(txm))
        {
            LOG(INFO) << "Split Flush transaction post all lock, range id "
                      << range_info_->PartitionId()
                      << ", txn: " << txm->TxNumber();

            // All of the new ranges falls on the same node, proceed to post
            // write all. Now broadcast slice info to all nodes through
            // PostWriteAll. New ranges might land on other nodes.
            range_record_->SetRangeInfo(range_info_.get());

            assert(post_all_lock_op_.write_type_ == PostWriteType::PostCommit);
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }
        else
        {
            ForwardToSubOperation(txm, &kickout_old_range_data_op_);
        }
    }
    else if (op_ == &kickout_old_range_data_op_)
    {
        if (kickout_old_range_data_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // error & retry
                LOG(ERROR) << "Split Flush transaction failed to kickout old "
                              "range data"
                              ", tx_number:"
                           << txm->TxNumber();
                RetrySubOperation(txm, &kickout_old_range_data_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }
        if (ForwardKickoutIterator(txm))
        {
            LOG(INFO) << "Split Flush transaction post all lock, range id "
                      << range_info_->PartitionId()
                      << ", txn: " << txm->TxNumber();

            // Now broadcast slice info to all nodes through PostWriteAll. New
            // ranges might land on other nodes.
            range_record_->SetRangeInfo(range_info_.get());

            assert(post_all_lock_op_.write_type_ == PostWriteType::PostCommit);
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }
        else
        {
            ForwardToSubOperation(txm, &kickout_old_range_data_op_);
        }
    }
    else if (op_ == &post_all_lock_op_)
    {
        if (post_all_lock_op_.hd_result_.IsError())
        {
            // error & retry
            if (!txm->CheckLeaderTerm())
            {
                ForceToFinish(txm);
            }
            else
            {
                LOG(ERROR) << "Split Flush transaction failed at post all "
                              "lock, tx_number:"
                           << txm->TxNumber() << ", err code "
                           << static_cast<int>(
                                  post_all_lock_op_.hd_result_.ErrorCode())
                           << ", msg "
                           << post_all_lock_op_.hd_result_.ErrorMsg();

                range_record_->SetRangeInfo(range_info_.get());
                RetrySubOperation(txm, &post_all_lock_op_);
            }
            return;
        }

        if (txm->commit_ts_ == tx_op_failed_ts_)
        {
            // If tx failed before writing prepare log, exit
            // after releasing orphaned lock.
            ForceToFinish(txm);
            return;
        }

        if (post_all_lock_op_.write_type_ == PostWriteType::DowngradeLock)
        {
            post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
            // Retry to acquire range write lock on all node group.
            ForwardToSubOperation(txm, &commit_acquire_all_write_op_);
            return;
        }

        const CcEntryAddr *cluster_config_addr = nullptr;
        const ReadSetEntry *cluster_config_entry = nullptr;
        const auto &meta_rset = txm->rw_set_.MetaDataReadSet();
        for (const auto &[cce_addr, read_entry_pair] : meta_rset)
        {
            if (read_entry_pair.second == cluster_config_ccm_name_sv)
            {
                cluster_config_addr = &cce_addr;
                cluster_config_entry = &read_entry_pair.first;
                break;
            }
        }
        assert(cluster_config_addr && cluster_config_entry);
        unlock_cluster_config_op_.Reset(cluster_config_addr,
                                        cluster_config_entry);

        LOG(INFO) << "Split Flush transaction unlock cluster config, range id "
                  << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &unlock_cluster_config_op_);
    }
    else if (op_ == &ds_clean_old_range_op_)
    {
        if (ds_clean_old_range_op_.hd_result_.IsError())
        {
            // error & retry
            if (txm->CheckLeaderTerm())
            {
                LOG(ERROR) << "Split Flush transaction failed to delete data "
                              "in old range "
                              "in data store, tx_number:"
                           << txm->TxNumber();
                RetrySubOperation(txm, &ds_clean_old_range_op_, false);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }

        FillCleanLogRequest(txm);
        LOG(INFO) << "Split Flush transaction write clean log, range id "
                  << range_info_->PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &clean_log_op_);
    }
    else if (op_ == &clean_log_op_)
    {
        if (clean_log_op_.hd_result_.IsError() && txm->CheckLeaderTerm())
        {
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &clean_log_op_);
            return;
        }
        else
        {
            if (txm->commit_ts_ == tx_op_failed_ts_)
            {
                txm->bool_resp_->Finish(false);
            }
            else
            {
                txm->bool_resp_->Finish(true);
            }

            ClearInfos();

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());

            assert(this == txm->split_flush_op_.get());
            LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(
                shards->split_flush_range_op_pool_mux_);
            shards->split_flush_range_op_pool_.emplace_back(
                std::move(txm->split_flush_op_));
            assert(txm->split_flush_op_ == nullptr);
        }
    }
}

/**
 * @brief Fill prepare log for Split-Flush Tx. We need to have old range
 * info and new range info, commit ts (for deciding the records that needs
 * to be flushed) in the log record.
 *
 * @param txm
 */
void SplitFlushRangeOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    prepare_log_op_.log_type_ = TxLogType::PREPARE;
    prepare_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    // Set general transaction information
    prepare_log_rec->set_tx_term(txm->tx_term_);
    prepare_log_rec->set_txn_number(txm->TxNumber());
    prepare_log_rec->set_commit_timestamp(txm->commit_ts_);
    prepare_log_rec->clear_node_terms();
    auto &node_terms = *prepare_log_rec->mutable_node_terms();
    for (size_t idx = 0; idx < prepare_acquire_all_write_op_.upload_cnt_; ++idx)
    {
        const AcquireAllResult &hres_val =
            prepare_acquire_all_write_op_.hd_results_[idx].Value();
        uint32_t ng_id = hres_val.local_cce_addr_.NodeGroupId();
        node_terms[ng_id] = hres_val.node_term_;
    }

    // Set split-flush tx information
    ::txlog::SplitRangeOpMessage *prepare_split_msg =
        prepare_log_rec->mutable_log_content()->mutable_split_range_log();
    prepare_split_msg->set_table_name(range_table_name_.String());
    prepare_split_msg->set_table_engine(
        ::txlog::ToRemoteType::ConvertTableEngine(range_table_name_.Engine()));
    prepare_split_msg->set_stage(
        ::txlog::SplitRangeOpMessage_Stage_PrepareSplit);
    // Set range info for splitting range
    prepare_split_msg->set_partition_id(range_info_->PartitionId());
    prepare_split_msg->set_range_key_neg_inf(false);
    switch (old_start_key_.Type())
    {
    case KeyType::NegativeInf:
        prepare_split_msg->set_range_key_neg_inf(true);
        break;
    default:
        old_start_key_.Serialize(*prepare_split_msg->mutable_range_key_value());
        break;
    }
    for (auto &new_range : new_range_info_)
    {
        prepare_split_msg->add_new_partition_id(new_range.second);
        std::string new_range_key;
        new_range.first.Serialize(new_range_key);
        prepare_split_msg->add_new_range_key(new_range_key);
    }
}

void SplitFlushRangeOp::FillCommitLogRequest(TransactionExecution *txm)
{
    commit_log_op_.log_type_ = TxLogType::COMMIT;
    commit_log_op_.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    commit_log_rec->set_tx_term(txm->tx_term_);
    commit_log_rec->set_txn_number(txm->TxNumber());
    commit_log_rec->set_commit_timestamp(txm->commit_ts_);
    auto commit_split_msg =
        commit_log_rec->mutable_log_content()->mutable_split_range_log();
    commit_split_msg->set_stage(::txlog::SplitRangeOpMessage_Stage_CommitSplit);

    // Fill the slice info
    std::vector<const StoreSlice *> slices =
        range_entry_->RangeSlices()->Slices();
    auto slice_it = slices.begin();
    commit_split_msg->add_slice_sizes((*slice_it)->Size());
    slice_it++;
    for (; slice_it != slices.end(); slice_it++)
    {
        std::string slice_key;
        (*slice_it)->StartTxKey().Serialize(slice_key);
        commit_split_msg->add_slice_keys(slice_key);
        commit_split_msg->add_slice_sizes((*slice_it)->Size());
    }

    // The prepare log keeps all cc nodes' terms and match them in the log
    // service to detect invalidated write intents. The commit log, however,
    // does not match terms in the log service.
    // If a cc node fails over, the new node must restore write intents
    // gained prior to the prepare log and then replay operations between
    // the prepare log and the commit log, which in this case upgrade write
    // intents to write locks. So, there is no need to check the liveness of
    // write locks when flushing the commit log.
    commit_log_rec->mutable_node_terms()->clear();
}

void SplitFlushRangeOp::FillCleanLogRequest(TransactionExecution *txm)
{
    clean_log_op_.log_type_ = TxLogType::CLEAN;

    clean_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *clean_log_rec =
        clean_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    clean_log_rec->set_tx_term(txm->tx_term_);
    clean_log_rec->set_txn_number(txm->TxNumber());

    ::txlog::SplitRangeOpMessage *clean_split_msg =
        clean_log_rec->mutable_log_content()->mutable_split_range_log();

    clean_split_msg->set_stage(::txlog::SplitRangeOpMessage_Stage_CleanSplit);
    clean_log_rec->mutable_node_terms()->clear();
}

void SplitFlushRangeOp::ForceToFinish(TransactionExecution *txm)
{
    txm->commit_ts_ = tx_op_failed_ts_;
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    Forward(txm);
}

std::vector<SplitRangeInfo> SplitFlushRangeOp::GenSplittedRangeInfos()
{
    // Split the range slices based on the range split keys.
    std::vector<SplitRangeInfo> splitted_range_info;
    std::vector<const StoreSlice *> slices =
        range_entry_->RangeSlices()->Slices();
    auto slice_it = slices.begin();
    TxKey start_key = old_start_key_.GetShallowCopy();
    int32_t range_id = range_entry_->RangeSlices()->PartitionId();
    std::vector<const StoreSlice *> subrange_slices;
    // First slice is always left in the old range. Put it into vector
    // first to avoid dealing with null start key.
    subrange_slices.push_back(*slice_it);
    slice_it++;
    for (auto &info : new_range_info_)
    {
        while (slice_it != slices.end() &&
               (*slice_it)->StartTxKey() < info.first)
        {
            subrange_slices.push_back(*slice_it);
            slice_it++;
        }
        splitted_range_info.emplace_back(
            std::move(start_key), range_id, std::move(subrange_slices));
        subrange_slices.clear();
        start_key = info.first.GetShallowCopy();
        range_id = info.second;
    }
    // The rest of the slices belong the last new range.
    for (; slice_it != slices.end(); slice_it++)
    {
        subrange_slices.push_back(*slice_it);
    }
    splitted_range_info.emplace_back(
        std::move(start_key), range_id, std::move(subrange_slices));

    return splitted_range_info;
}

bool SplitFlushRangeOp::ForwardKickoutIterator(TransactionExecution *txm)
{
    auto local_shards = Sharder::Instance().GetLocalCcShards();

    if (!cleaning_old_range_dirty_owner_)
    {
        for (; kickout_data_it_ != new_range_info_.cend(); kickout_data_it_++)
        {
            auto new_range_bucket_info = local_shards->GetRangeOwner(
                kickout_data_it_->second, txm->TxCcNodeId());
            NodeGroupId new_owner = new_range_bucket_info->BucketOwner();
            NodeGroupId dirty_new_owner =
                new_range_bucket_info->DirtyBucketOwner();
            if (new_owner != txm->TxCcNodeId() &&
                dirty_new_owner != txm->TxCcNodeId())
            {
                // Note that even if the new node group falls on the same node,
                // we still need to clean the cc entry from native ccmap since
                // failover and native ccmaps are separated.
                kickout_old_range_data_op_.start_key_ =
                    kickout_data_it_->first.GetShallowCopy();
                if (std::next(kickout_data_it_) == new_range_info_.cend())
                {
                    kickout_old_range_data_op_.end_key_ =
                        old_end_key_.GetShallowCopy();
                }
                else
                {
                    kickout_old_range_data_op_.end_key_ =
                        std::next(kickout_data_it_)->first.GetShallowCopy();
                }
                kickout_old_range_data_op_.clean_type_ =
                    CleanType::CleanRangeData;
                kickout_old_range_data_op_.node_group_ = txm->TxCcNodeId();
                LOG(INFO)
                    << "Split Flush transaction kickout old data in range "
                    << kickout_data_it_->second << ", original range id "
                    << range_info_->PartitionId()
                    << ", txn: " << txm->TxNumber();
                kickout_data_it_++;
                return false;
            }
        }
        assert(kickout_data_it_ == new_range_info_.cend());
        kickout_data_it_ = new_range_info_.cbegin();
        cleaning_old_range_dirty_owner_ = true;
    }

    // If the range split happens during data migration, all changes in the
    // range will be forwarded to the dirty bucket owner. So when we kick out
    // data that is splitted to another ng, we need to kick out the data on the
    // dirty bucket owner as well.
    if (cleaning_old_range_dirty_owner_)
    {
        NodeGroupId old_range_dirty_owner =
            local_shards
                ->GetRangeOwner(range_info_->PartitionId(), txm->TxCcNodeId())
                ->DirtyBucketOwner();
        if (old_range_dirty_owner == UINT32_MAX)
        {
            return true;
        }
        assert(old_range_dirty_owner != txm->TxCcNodeId());
        for (; kickout_data_it_ != new_range_info_.cend(); kickout_data_it_++)
        {
            auto new_range_bucket_info = local_shards->GetRangeOwner(
                kickout_data_it_->second, txm->TxCcNodeId());
            NodeGroupId new_owner = new_range_bucket_info->BucketOwner();
            NodeGroupId dirty_new_owner =
                new_range_bucket_info->DirtyBucketOwner();
            if (new_owner != old_range_dirty_owner &&
                dirty_new_owner != old_range_dirty_owner)
            {
                // Note that even if the new node group falls on the same node,
                // we still need to clean the cc entry from native ccmap since
                // failover and native ccmaps are separated.
                kickout_old_range_data_op_.start_key_ =
                    kickout_data_it_->first.GetShallowCopy();
                if (std::next(kickout_data_it_) == new_range_info_.cend())
                {
                    kickout_old_range_data_op_.end_key_ =
                        old_end_key_.GetShallowCopy();
                }
                else
                {
                    kickout_old_range_data_op_.end_key_ =
                        std::next(kickout_data_it_)->first.GetShallowCopy();
                }
                kickout_old_range_data_op_.clean_type_ =
                    CleanType::CleanRangeData;
                kickout_old_range_data_op_.node_group_ = old_range_dirty_owner;

                LOG(INFO)
                    << "Split Flush transaction kickout old data in range "
                    << kickout_data_it_->second << ", original range id "
                    << range_info_->PartitionId()
                    << ", txn: " << txm->TxNumber();
                kickout_data_it_++;
                return false;
            }
        }
    }

    assert(kickout_data_it_ == new_range_info_.cend());
    return true;
}

ReleaseScanExtraLockOp::ReleaseScanExtraLockOp(TransactionExecution *txm)
    : hd_result_(txm)
{
}

void ReleaseScanExtraLockOp::Reset()
{
    hd_result_.Reset();
    hd_result_.Value().Clear();
}

void ReleaseScanExtraLockOp::Forward(TransactionExecution *txm)
{
    // do not wait on remote
    if (hd_result_.IsFinished() || hd_result_.LocalRefCnt() == 0)
    {
        txm->PostProcess(*this);
    }
}

AnalyzeTableAllOp::AnalyzeTableAllOp(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void AnalyzeTableAllOp::Reset(uint32_t hres_ref_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(hres_ref_cnt);
}

void AnalyzeTableAllOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
        return;
    }

    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
    else if (hd_result_.LocalRefCnt() == 0 && txm->IsTimeOut(600))
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeOut",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });

        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->PostProcess(*this);
        }
    }
}

BroadcastStatisticsOp::BroadcastStatisticsOp(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void BroadcastStatisticsOp::Reset(uint32_t hres_ref_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(hres_ref_cnt);
}

void BroadcastStatisticsOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
        return;
    }

    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
    else if (hd_result_.LocalRefCnt() == 0 && txm->IsTimeOut(600))
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeOut",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });

        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->PostProcess(*this);
        }
    }
}

ObjectCommandOp::ObjectCommandOp(
    TransactionExecution *txm,
    CcHandlerResult<ReadKeyResult> *lock_range_bucket_result)
    : hd_result_(txm), lock_bucket_result_(lock_range_bucket_result)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ObjectCommandOp::Reset()
{
    table_name_ = nullptr;
    key_ = nullptr;
    command_ = nullptr;
    auto_commit_ = false;
    always_redirect_ = false;
    forward_key_shard_ = UINT32_MAX;
    catalog_read_success_ = false;
    is_running_ = false;
    // Do not reset the hd_result, which might be used when txn commits.
}

void ObjectCommandOp::Reset(const TableName *table_name,
                            const TxKey *key,
                            TxCommand *command,
                            bool auto_commit,
                            bool always_redirect)
{
    table_name_ = table_name;
    key_ = key;
    command_ = command;
    hd_result_.Reset();
    hd_result_.Value().Reset();
    auto_commit_ = auto_commit;
    always_redirect_ = always_redirect;
    lock_bucket_result_->Value().Reset();
    lock_bucket_result_->Reset();
    forward_key_shard_ = UINT32_MAX;
    catalog_read_success_ = false;
    is_running_ = false;
}

void ObjectCommandOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        if (FLAGS_cmd_read_catalog && !catalog_read_success_)
        {
            // Just returned from read_catalog_op_, check read_catalog_result_.
            const CcHandlerResult<ReadKeyResult> &read_catalog_result =
                txm->read_catalog_result_;
            assert(read_catalog_result.IsFinished());
            if (read_catalog_result.IsError() ||
                read_catalog_result.Value().rec_status_ != RecordStatus::Normal)
            {
                if (read_catalog_result.Value().rec_status_ !=
                    RecordStatus::Normal)
                {
                    LOG(ERROR) << "ReadCatalogOp record status not normal: "
                               << int(read_catalog_result.Value().rec_status_);
                }
                // There is an error when read the catalog. The read operation
                // is set to be errored.
                hd_result_.SetError(read_catalog_result.IsError()
                                        ? read_catalog_result.ErrorCode()
                                        : CcErrorCode::READ_CATALOG_FAIL);

                txm->PostProcess(*this);
                return;
            }

            txm->Process(*this);
            return;
        }
        // Just returned from lock_bucket_op_, check lock_bucket_result_.
        assert(lock_bucket_result_->IsFinished());
        if (lock_bucket_result_->IsError())
        {
            // There is an error when getting the input key's bucket. The
            // read operation is set to be errored.
            hd_result_.SetError(lock_bucket_result_->ErrorCode());

            txm->PostProcess(*this);
            return;
        }
        // Need to make sure current node is still leader since we will visit
        // bucket meta data which is only valid if current node is still ng
        // leader.
        if (!txm->CheckLeaderTerm() && !txm->CheckStandbyTerm())
        {
            hd_result_.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            hd_result_.ForceError();
            txm->PostProcess(*this);
            return;
        }

        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        CODE_FAULT_INJECTOR("ObjectCommandOp_NodeNotLeader", {
            LOG(INFO) << "FaultInject  ObjectCommandOp_NodeNotLeader";
            hd_result_.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            FaultInject::Instance().InjectFault("ObjectCommandOp_NodeNotLeader",
                                                "remove");
        });
        // If ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER, it can not
        // retry, because we cannot know if the command has executed or not,
        // some commands will lead to unpredictable result, for example lpop
        // rpush.
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            // Only update leader but not retry.
            Sharder::Instance().UpdateLeader(
                hd_result_.Value().cce_addr_.NodeGroupId());
        }
        txm->PostProcess(*this);
        return;
    }

    if (txm->IsTimeOut())
    {
        if (!hd_result_.Value().is_local_)
        {
            const CcEntryAddr &cce_addr = hd_result_.Value().cce_addr_;

            if (cce_addr.Term() < 0 ||
                (!txm->CheckLeaderTerm() && !txm->CheckStandbyTerm()))
            {
                // For non-blocking concurrency control protocols, the object
                // command is expected to return instantly. For 2PL, if the
                // request is blocked, the cc node will send an
                // acknowledgement to update the key's term. In either case, if
                // the object's term is not set, the tx has not received any
                // response or acknowledgement from the key's cc node group.
                // The request is forced to be errored upon timeout.

                bool force_error = hd_result_.ForceError();
                if (force_error)
                {
                    txm->PostProcess(*this);
                }
            }
            else
            {
                DeadLockCheck::RequestCheck();

                // Check if the blocked keys are still blocked in the lock
                // queue.
                txm->cc_handler_->BlockCcReqCheck(
                    txm->TxNumber(),
                    txm->TxTerm(),
                    txm->CommandId(),
                    cce_addr,
                    &hd_result_,
                    ResultTemplateType::ReadKeyResult);
            }
        }
        else
        {
            // local request timeout
            DeadLockCheck::RequestCheck();
        }
    }
}

MultiObjectCommandOp::MultiObjectCommandOp(
    TransactionExecution *txm,
    CcHandlerResult<ReadKeyResult> *lock_range_bucket_result)
    : txm_(txm), lock_bucket_result_(lock_range_bucket_result)
{
}

bool MultiObjectCommandOp::IsBlockCommand()
{
    return is_block_command_;
}

void MultiObjectCommandOp::Reset()
{
    tx_req_ = nullptr;

    // Reset counters and flags
    atm_cnt_.store(0, std::memory_order_relaxed);
    atm_block_cnt_.store(0, std::memory_order_relaxed);
    atm_local_cnt_.store(0, std::memory_order_relaxed);
    atm_err_code_.store(CcErrorCode::NO_ERROR, std::memory_order_relaxed);
    is_block_command_ = false;

    // Reset progress and state
    bucket_lock_cur_ = 0;
    catalog_read_success_ = false;
    is_running_ = false;
    // Do not reset the hd_results, which might be used when txn commits.
}

void MultiObjectCommandOp::Reset(MultiObjectCommandTxRequest *req)
{
    tx_req_ = req;
    size_t new_size = req->VctKey()->size();
    size_t min_len = std::min(new_size, vct_hd_result_.size());

    for (size_t i = 0; i < min_len; i++)
    {
        auto &hr = vct_hd_result_[i];
        hr.Reset();
        hr.Value().Reset();
    }

    if (new_size < vct_hd_result_.size())
    {
        while (min_len < vct_hd_result_.size())
        {
            vct_hd_result_.pop_back();
        }
    }
    else if (min_len < new_size)
    {
        for (size_t i = min_len; i < new_size; i++)
        {
            CcHandlerResult<ObjectCommandResult> hr(txm_);
            hr.Value().Reset();
            hr.Reset();
            hr.post_lambda_ = [this](CcHandlerResult<ObjectCommandResult> *res)
            {
                if (res->Value().is_local_)
                {
                    atm_local_cnt_.fetch_sub(1, std::memory_order_relaxed);
                }

                CcErrorCode err = res->ErrorCode();
                if (err != CcErrorCode::NO_ERROR &&
                    err != CcErrorCode::TASK_EXPIRED)
                {
                    atm_err_code_.store(err, std::memory_order_relaxed);
                }

                atm_block_cnt_.fetch_sub(1, std::memory_order_relaxed);

                atm_cnt_.fetch_sub(1, std::memory_order_release);
            };

            vct_hd_result_.emplace_back(std::move(hr));
        }
    }

    atm_local_cnt_.store(0, std::memory_order_relaxed);
    atm_err_code_.store(CcErrorCode::NO_ERROR, std::memory_order_relaxed);
    uint32_t num = tx_req_->Command()->NumOfFinishBlockCommands();
    if (num > 0)
    {
        vct_abort_hd_result_.clear();
        for (size_t i = 0; i < new_size; i++)
        {
            CcHandlerResult<ObjectCommandResult> hr(txm_);
            hr.Value().Reset();
            hr.Reset();
            hr.post_lambda_ = [this](CcHandlerResult<ObjectCommandResult> *res)
            {
                CcErrorCode err = res->ErrorCode();
                if (err != CcErrorCode::NO_ERROR &&
                    err != CcErrorCode::TASK_EXPIRED)
                {
                    atm_err_code_.store(err, std::memory_order_relaxed);
                }
                if (res->Value().is_local_)
                {
                    // We only increased atm_cnt_ for local discard requests.
                    atm_cnt_.fetch_sub(1, std::memory_order_release);
                    atm_local_cnt_.fetch_sub(1, std::memory_order_relaxed);
                }
            };

            vct_abort_hd_result_.emplace_back(std::move(hr));
        }

        is_block_command_ = true;
        atm_block_cnt_.store(num, std::memory_order_relaxed);
    }
    else
    {
        is_block_command_ = false;
        atm_block_cnt_.store(new_size, std::memory_order_relaxed);
    }

    atm_cnt_.store(new_size, std::memory_order_relaxed);

    vct_key_shard_code_.clear();
    vct_key_shard_code_.resize(new_size);
    for (auto &shard_code : vct_key_shard_code_)
    {
        shard_code.second = UINT32_MAX;
    }
    bucket_lock_cur_ = 0;
    lock_bucket_result_->Reset();
    lock_bucket_result_->Value().Reset();
    catalog_read_success_ = false;
    is_running_ = false;
}

void MultiObjectCommandOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        if (FLAGS_cmd_read_catalog && !catalog_read_success_)
        {
            // Just returned from read_catalog_op_, check read_catalog_result_.
            const CcHandlerResult<ReadKeyResult> &read_catalog_result =
                txm->read_catalog_result_;
            assert(read_catalog_result.IsFinished());
            if (read_catalog_result.IsError())
            {
                // There is an error when reading the catalog. This operation is
                // set to be errored.
                atm_err_code_.store(CcErrorCode::READ_CATALOG_FAIL,
                                    std::memory_order_relaxed);

                txm->PostProcess(*this);
                return;
            }

            txm->Process(*this);
            return;
        }
        // Just returned from lock_bucket_op_, check lock_bucket_result_.
        assert(lock_bucket_result_->IsFinished());
        if (lock_bucket_result_->IsError())
        {
            // There is an error when getting the input key's bucket. The
            // operation is set to be errored.
            txm->PostProcess(*this);
            return;
        }
        // Need to make sure current node is still leader since we will visit
        // bucket meta data which is only valid if current node is still ng
        // leader.
        if (!txm->CheckLeaderTerm() && !txm->CheckStandbyTerm())
        {
            atm_err_code_.store(CcErrorCode::TX_NODE_NOT_LEADER,
                                std::memory_order_relaxed);
            txm->PostProcess(*this);
            return;
        }
        txm->Process(*this);
        return;
    }

    // NumOfFinishBlockCommands()>0 means the current step is to run block
    // commands.
    if (tx_req_->Command()->NumOfFinishBlockCommands() > 0)
    {
        MultiObjectTxCommand *mcmd = tx_req_->Command();
        if (!mcmd->IsExpired() &&
            atm_block_cnt_.load(std::memory_order_relaxed) > 0)
        {
            return;
        }

        if (!mcmd->ForwardResult())
        {
            if (atm_cnt_.load(std::memory_order_acquire) == 0)
            {
                txm->PostProcess(*this);
            }

            return;
        }

        const std::vector<TxKey> *vct_key = tx_req_->VctKey();
        const std::vector<TxCommand *> *vct_cmd = tx_req_->VctCommand();
        uint64_t current_ts =
            dynamic_cast<LocalCcHandler *>(txm->cc_handler_)->GetTsBaseValue();

        int local_cnt = 0;

        for (size_t i = 0; i < vct_key->size(); i++)
        {
            TxCommand *cmd = vct_cmd->at(i);

            if (vct_hd_result_[i].IsFinished() ||
                cmd->GetBlockOperationType() != BlockOperation::Discard)
            {
                continue;
            }

            auto &hd_res = vct_abort_hd_result_[i];
            const TxKey &key = vct_key->at(i);
            uint32_t key_shard_code = 0;

            key_shard_code = vct_key_shard_code_[i].first;
            bool commit = false;

            int db_idx = GetDbIndex(tx_req_->table_name_);
            assert(txm->locked_db_[db_idx].first != nullptr);

            txm->cc_handler_->ObjectCommand(
                *tx_req_->table_name_,
                txm->locked_db_[db_idx].second,
                key,
                key_shard_code,
                *cmd,
                txm->TxNumber(),
                txm->tx_term_,
                txm->command_id_.load(std::memory_order_relaxed),
                current_ts,
                hd_res,
                txm->iso_level_,
                txm->protocol_,
                commit,
                tx_req_->always_redirect_);

            if (hd_res.Value().is_local_)
            {
                local_cnt++;
            }
        }

        if (local_cnt > 0)
        {
            // For abort commands, only local commands need to call SetFinished
            // to avoid to visit freed memory.
            atm_local_cnt_.fetch_add(local_cnt, std::memory_order_relaxed);
            atm_cnt_.fetch_add(local_cnt, std::memory_order_relaxed);
        }
        // Reset timeout timer after discard commands are sent.
        txm->StartTiming();
    }
    else if (atm_cnt_.load(std::memory_order_acquire) == 0)
    {
        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
    {
        if (atm_local_cnt_.load(std::memory_order_relaxed) == 0)
        {
            // Fast path check - if any error exists, we need to verify all
            // hd_results are either finished or can be force-errored
            if (atm_err_code_.load(std::memory_order_acquire) ==
                    CcErrorCode::NO_ERROR &&
                (txm->CheckLeaderTerm() || txm->CheckStandbyTerm()))
            {
                DeadLockCheck::RequestCheck();

                for (auto &hd_result : vct_hd_result_)
                {
                    if (hd_result.IsFinished())
                    {
                        // Already finished, continue to next
                        continue;
                    }

                    const CcEntryAddr &cce_addr = hd_result.Value().cce_addr_;
                    if (cce_addr.Term() > 0)
                    {
                        // Received an ack message that the ApplyCc was received
                        // but was blockedd by lock. We need to check if the
                        // remote node is still alive.

                        txm->cc_handler_->BlockCcReqCheck(
                            txm->TxNumber(),
                            txm->TxTerm(),
                            txm->CommandId(),
                            cce_addr,
                            &hd_result,
                            ResultTemplateType::ReadKeyResult);
                    }
                }
            }
            else
            {
                // found error, force error the unfinished requests

                for (auto &hd_result : vct_hd_result_)
                {
                    if (hd_result.IsFinished())
                    {
                        // Already finished, continue to next
                        continue;
                    }

                    // For non-blocking CC, commands return instantly. For
                    // 2PL, blocked requests send an acknowledgement to
                    // update the key's term. If the object's term is not
                    // set, it means no response or acknowledgement has been
                    // received from the key's CC node group, leading to a
                    // timeout error.
                    hd_result.ForceError();
                }

                if (atm_cnt_.load(std::memory_order_acquire) == 0)
                {
                    txm->PostProcess(*this);
                }
            }
        }
        else
        {
            DLOG(WARNING) << "MultiObjectCommandOp::Forward txm->IsTimeOut() "
                             "request deadlock check, tx: "
                          << txm->TxNumber() << " atm_local_cnt_: "
                          << atm_local_cnt_.load(std::memory_order_relaxed);

            // local request timeout
            DeadLockCheck::RequestCheck();
        }
    }
}

CmdForwardAcquireWriteOp::CmdForwardAcquireWriteOp(TransactionExecution *txm)
    : hd_result_(txm), txm_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void CmdForwardAcquireWriteOp::Reset(size_t acquire_write_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(acquire_write_cnt);

    std::vector<AcquireKeyResult> &acquire_key_vec = hd_result_.Value();
    size_t old_size = acquire_key_vec.size();
    acquire_key_vec.resize(acquire_write_cnt);
    for (size_t idx = old_size; idx < acquire_write_cnt; ++idx)
    {
        acquire_key_vec[idx].remote_ack_cnt_ = &remote_ack_cnt_;
    }

    remote_ack_cnt_.store(0, std::memory_order_relaxed);
    acquire_write_entries_.resize(acquire_write_cnt);

    op_start_ = metrics::TimePoint::max();
}

void CmdForwardAcquireWriteOp::AggregateAcquiredKeys(TransactionExecution *txm)
{
    std::vector<AcquireKeyResult> &acquire_key_vec = hd_result_.Value();
    size_t res_idx = 0;
    for (CmdForwardEntry *forward_entry : acquire_write_entries_)
    {
        AcquireKeyResult &acquire_key_res = acquire_key_vec[res_idx++];
        CcEntryAddr &addr = acquire_key_res.cce_addr_;

        int64_t term = addr.Term();
        if (term < 0)
        {
            forward_entry->cce_addr_.SetCceLock(0, -1, 0);
        }
        else
        {
            assert(forward_entry->cce_addr_.Empty());
            forward_entry->cce_addr_ = addr;
            txm_->cmd_set_.IncreaseObjectCntWithWriteLock();
        }
    }
}

void CmdForwardAcquireWriteOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            Sharder::Instance().UpdateLeaders();
            if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        AggregateAcquiredKeys(txm);
        txm->PostProcess(*this);
    }
    else
    {
        if (txm->IsTimeOut())
        {
            if (remote_ack_cnt_.load(std::memory_order_acquire) == 0 &&
                txm->CheckLeaderTerm())
            {
                // All remote acks are received, but timeout still happens.
                DeadLockCheck::RequestCheck();

                std::vector<AcquireKeyResult> &vct_akr = hd_result_.Value();
                for (size_t i = 0; i < vct_akr.size(); i++)
                {
                    AcquireKeyResult &akr = vct_akr[i];

                    if (!akr.IsRemoteHdResultSet(std::memory_order_acquire) &&
                        akr.cce_addr_.Term() > 0 /*&& akr.commit_ts_ == 0*/)
                    {
                        txm->cc_handler_->BlockCcReqCheck(
                            txm->TxNumber(),
                            txm->TxTerm(),
                            txm->CommandId(),
                            akr.cce_addr_,
                            &hd_result_,
                            ResultTemplateType::AcquireKeyResult,
                            i);
                    }
                }
            }
            else
            {
                // Some of the acks are not received, but timeout happens.
                bool success = hd_result_.ForceError();
                if (success)
                {
                    AggregateAcquiredKeys(txm);
                    txm->PostProcess(*this);
                }
            }
        }
    }
}

ClusterScaleOp::ClusterScaleOp(
    const std::string &id,
    ClusterScaleOpType event_type,
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &&new_ng_config,
    TransactionExecution *txm)
    : CompositeTransactionOperation(),
      prepare_log_op_(txm),
      acquire_cluster_config_write_op_(txm),
      update_cluster_config_log_op_(txm),
      install_cluster_config_op_(txm),
      notify_migration_op_(txm),
      check_migration_is_finished_op_(txm),
      clean_log_op_(txm),
      pub_buckets_migrate_begin_op_(txm),
      pub_buckets_migrate_end_op_(txm),
      id_(id),
      event_type_(event_type),
      new_ng_config_(std::move(new_ng_config)),
      mux_(),
      txn_(txm->TxNumber())
{
    acquire_cluster_config_write_op_.table_name_ = &cluster_config_ccm_name;
    acquire_cluster_config_write_op_.keys_.emplace_back(
        TxKey(VoidKey::NegativeInfinity()));
    acquire_cluster_config_write_op_.cc_op_ = CcOperation::Write;
    acquire_cluster_config_write_op_.protocol_ = CcProtocol::Locking;

    install_cluster_config_op_.table_name_ = &cluster_config_ccm_name;
    install_cluster_config_op_.keys_.emplace_back(
        TxKey(VoidKey::NegativeInfinity()));
    install_cluster_config_op_.recs_.push_back(&cluster_config_rec_);
    install_cluster_config_op_.op_type_ = OperationType::Update;
    // cluster update is a 1pc. There is no dirty state.
    install_cluster_config_op_.write_type_ = PostWriteType::Commit;
}

void ClusterScaleOp::Reset(
    const std::string &id,
    ClusterScaleOpType event_type,
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &&new_ng_config,
    TransactionExecution *txm)
{
    op_ = nullptr;
    id_ = id;
    event_type_ = event_type;
    new_ng_config_ = std::move(new_ng_config);

    txn_ = txm->TxNumber();

    prepare_log_op_.Reset();
    prepare_log_op_.ResetHandlerTxm(txm);

    acquire_cluster_config_write_op_.ResetHandlerTxm(txm);

    update_cluster_config_log_op_.Reset();
    update_cluster_config_log_op_.ResetHandlerTxm(txm);

    install_cluster_config_op_.ResetHandlerTxm(txm);

    auto ng_count = Sharder::Instance().NodeGroupCount();

    notify_migration_op_.Clear();
    notify_migration_op_.Reset(ng_count);

    check_migration_is_finished_op_.Reset();

    clean_log_op_.Reset();
    clean_log_op_.ResetHandlerTxm(txm);

    pub_buckets_migrate_begin_op_.Reset();
    pub_buckets_migrate_begin_op_.ResetHandlerTxm(txm);
    pub_buckets_migrate_end_op_.Reset();
    pub_buckets_migrate_end_op_.ResetHandlerTxm(txm);

    assert(bucket_migrate_infos_.empty());

    acquire_cluster_config_write_op_.table_name_ = &cluster_config_ccm_name;
    acquire_cluster_config_write_op_.keys_.clear();
    acquire_cluster_config_write_op_.keys_.emplace_back(
        TxKey(VoidKey::NegativeInfinity()));
    acquire_cluster_config_write_op_.cc_op_ = CcOperation::Write;
    acquire_cluster_config_write_op_.protocol_ = CcProtocol::Locking;

    install_cluster_config_op_.table_name_ = &cluster_config_ccm_name;
    install_cluster_config_op_.keys_.clear();
    install_cluster_config_op_.keys_.emplace_back(
        TxKey(VoidKey::NegativeInfinity()));
    install_cluster_config_op_.recs_.clear();
    install_cluster_config_op_.recs_.push_back(&cluster_config_rec_);
    install_cluster_config_op_.op_type_ = OperationType::Update;
    install_cluster_config_op_.write_type_ = PostWriteType::Commit;
}

void ClusterScaleOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        LOG(INFO) << "Cluster scale transaction prepare acquire write all on "
                     "cluster scale table, txn: "
                  << txm->TxNumber();

        txm->commit_ts_ = txm->commit_ts_bound_ + 1;

        if (event_type_ == ClusterScaleOpType::AddNodeGroup ||
            event_type_ == ClusterScaleOpType::RemoveNode)
        {
            std::set<NodeGroupId> new_ng_set;
            for (const auto &it : new_ng_config_)
            {
                new_ng_set.emplace(it.first);
            }
            bucket_migrate_infos_ =
                Sharder::Instance()
                    .GetLocalCcShards()
                    ->GenerateBucketMigrationPlan(new_ng_set);
        }

        FillPrepareLogRequest(txm);
        LOG(INFO) << "Cluster scale transaction write prepare log, txn: "
                  << txm->TxNumber();
        ForwardToSubOperation(txm, &prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            // Failed before write log succeed due to leader transfer.
            // Notify caller.

            txm->string_resp_->FinishError(
                TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
            ForceToFinish(txm);
            return;
        }
        // prepare cluster config string for tx result
        std::string config_str;
        // total ngs
        config_str.append(std::to_string(new_ng_config_.size()));
        config_str.append("\n");

        for (const auto &pair : new_ng_config_)
        {
            // ng id
            config_str.append(std::to_string(pair.first));
            config_str.append(" ");

            // member nodes
            for (const auto &node : pair.second)
            {
                // node id
                config_str.append(std::to_string(node.node_id_));
                config_str.append(" ");

                // host name
                config_str.append(node.host_name_);
                config_str.append(" ");

                // port
                config_str.append(std::to_string(node.port_));
                config_str.append(" ");

                // is candidate
                config_str.append(std::to_string(node.is_candidate_));
                config_str.append(" ");
            }
            config_str.append("\n");
        }

        config_str.append(std::to_string(txm->commit_ts_));
        config_str.append("\n");

        if (prepare_log_op_.hd_result_.IsError())
        {
            if (prepare_log_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the
                // coordinator itself is no longer leader
                if (txm->CheckLeaderTerm())
                {
                    DLOG(WARNING) << "Cluster scale write prepare log "
                                     "result unknown, "
                                     "tx_number:"
                                  << txm->TxNumber() << ", keep retrying";
                    // set retry flag and retry prepare log
                    ::txlog::WriteLogRequest *log_req =
                        prepare_log_op_.log_closure_.LogRequest()
                            .mutable_write_log_request();
                    log_req->set_retry(true);
                    RetrySubOperation(txm, &prepare_log_op_);
                }
                else
                {
                    DLOG(ERROR) << "Cluster scale write prepare log result "
                                   "unknown, tx_number:"
                                << txm->TxNumber()
                                << ", not leader any more, stop retrying";
                    // Not leader anymore, just quit. New leader will know
                    // whether prepare log succeeds and continue the rest if
                    // it does. Caller need to query new leader of node
                    // group to know if write log has succeeded.
                    txm->string_resp_->FinishError(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);
                    ForceToFinish(txm);
                }
            }
            else if (prepare_log_op_.hd_result_.ErrorCode() ==
                     CcErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERR)
            {
                txm->string_resp_->Value() = config_str;
                txm->string_resp_->FinishError(
                    TxErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERROR);
                ForceToFinish(txm);
            }
            else
            {
                LOG(ERROR) << "Failed to write cluster scale prepare log, txn: "
                           << txm->TxNumber();
                // Notify called that the operation has failed
                txm->string_resp_->FinishError(TxErrorCode::WRITE_LOG_FAIL);
                ForceToFinish(txm);
            }
            return;
        }

        // Notify caller that log has been written.
        if (txm->TxStatus() != TxnStatus::Recovering)
        {
            txm->string_resp_->Finish(config_str);
        }

        if (bucket_migrate_infos_.empty())
        {
            // Skip bucket migration subops and directly forward to update
            // cluster config.
            ForwardToSubOperation(txm, &acquire_cluster_config_write_op_);
            return;
        }

        pub_buckets_migrate_begin_op_.op_func_ = [](AsyncOp<Void> &async_op)
        {
            async_op.worker_thread_ = std::thread(
                [&hd_res = async_op.hd_result_]
                {
                    bool result = true;
                    bool rpc_fail = false;
                    ClusterScaleOp::SendBucketsMigratingRpc(
                        true, result, rpc_fail);

                    if (!rpc_fail && result)
                    {
                        hd_res.SetFinished();
                    }
                    else
                    {
                        // only for retry.
                        hd_res.SetError(CcErrorCode::UNDEFINED_ERR);
                    }
                });
        };

        LOG(INFO) << "Cluster scale transaction publish "
                     "bucket_migrating_begin to all node groups ,txn: "
                  << txm->TxNumber();
        ForwardToSubOperation(txm, &pub_buckets_migrate_begin_op_);
    }
    else if (op_ == &pub_buckets_migrate_begin_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (pub_buckets_migrate_begin_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Cluster scale transaction failed to execute "
                          "pub_buckets_migrate_begin_op_ , tx_number:"
                       << txm->TxNumber();

            // Retry until succeed.
            RetrySubOperation(txm, &pub_buckets_migrate_begin_op_);
            return;
        }

        if (event_type_ == ClusterScaleOpType::AddNodeGroup)
        {
            // If we're adding new nodes, add new nodes into cluster
            // before migrating data
            LOG(INFO) << "Cluster scale transaction acquire write lock on all "
                         "nodes, txn "
                      << txm->TxNumber();
            ForwardToSubOperation(txm, &acquire_cluster_config_write_op_);
        }
        else
        {
            // For remove nodes, just start migration right away. We will
            // update cluster config and remove nodes when migration is
            // done.

            notify_migration_op_.migrate_plans_ = bucket_migrate_infos_;

            LOG(INFO)
                << "Cluster scale transaction notify data migration, txn: "
                << txm->TxNumber();
            ForwardToSubOperation(txm, &notify_migration_op_);
        }
    }
    else if (op_ == &acquire_cluster_config_write_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (acquire_cluster_config_write_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Cluster scale transaction failed to obtain write "
                          "lock on all node groups "
                          ", tx_number:"
                       << txm->TxNumber();

            if (acquire_cluster_config_write_op_.IsDeadlock())
            {
                LOG(ERROR) << "Cluster scale transaction deadlocks with other "
                              "transactions, downgrade write lock, tx_number:"
                           << txm->TxNumber();
                // We downgrade write lock to resolve deadlock issue.
                // And then retry to acquire write lock.
                install_cluster_config_op_.write_type_ =
                    PostWriteType::DowngradeLock;
                ForwardToSubOperation(txm, &install_cluster_config_op_);
            }
            else
            {
                // We need to roll forward after prepare log is written. Retry
                // until succeed.
                RetrySubOperation(txm, &acquire_cluster_config_write_op_);
            }
            return;
        }

        txm->commit_ts_ =
            std::max(txm->commit_ts_, acquire_cluster_config_write_op_.MaxTs());
        FillUpdateClusterConfigLogRequest(txm);
        LOG(INFO) << "Cluster scale transaction write update cluster config "
                     "log, txn: "
                  << txm->TxNumber();
        ForwardToSubOperation(txm, &update_cluster_config_log_op_);
    }
    else if (op_ == &update_cluster_config_log_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }
        if (update_cluster_config_log_op_.hd_result_.IsError())
        {
            // error & retry
            ::txlog::WriteLogRequest *log_req =
                update_cluster_config_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &update_cluster_config_log_op_);
            return;
        }

        ACTION_FAULT_INJECTOR("cluster_config_after_cluster_config_log");

        //  Broadcast the new cluster config to all nodes through post write
        //  all.
        cluster_config_rec_.SetVersion(txm->commit_ts_);
        cluster_config_rec_.SetNodeGroupConfigs(&new_ng_config_);

        LOG(INFO) << "Cluster scale transaction update cluster config, txn "
                  << txm->TxNumber();
        ForwardToSubOperation(txm, &install_cluster_config_op_);
    }
    else if (op_ == &install_cluster_config_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (install_cluster_config_op_.hd_result_.IsError())
        {
            RetrySubOperation(txm, &install_cluster_config_op_);
            return;
        }

        if (txm->CommitTs() == TransactionOperation::tx_op_failed_ts_)
        {
            // If tx failed, recycle the tx machine after releasing locks.
            ForceToFinish(txm);
            return;
        }

        if (install_cluster_config_op_.write_type_ ==
            PostWriteType::DowngradeLock)
        {
            // Retry to acquire write lock
            install_cluster_config_op_.write_type_ = PostWriteType::Commit;
            ForwardToSubOperation(txm, &acquire_cluster_config_write_op_);
            return;
        }

        if (event_type_ == ClusterScaleOpType::AddNodeGroup)
        {
            // We should not start the data migration process.
            LOG(INFO)
                << "Cluster scale transaction notify data migration, txn: "
                << txm->TxNumber() << ", tx_term: " << txm->TxTerm()
                << ", tx_ng_id: " << txm->TxCcNodeId();

            notify_migration_op_.migrate_plans_ = bucket_migrate_infos_;
            ForwardToSubOperation(txm, &notify_migration_op_);
        }
        else
        {
            if (bucket_migrate_infos_.empty())
            {
                LOG(INFO) << "Cluster scale transaction write clean log, txn "
                          << txm->TxNumber();
                FillCleanLogRequest(txm);
                ForwardToSubOperation(txm, &clean_log_op_);
                return;
            }
            // Before writing clean log, we notify all nodes set
            // "LocalCcShards::buckets_migrating_" to false.
            pub_buckets_migrate_end_op_.op_func_ = [](AsyncOp<Void> &async_op)
            {
                async_op.worker_thread_ = std::thread(
                    [&hd_res = async_op.hd_result_]
                    {
                        bool result = true;
                        bool rpc_fail = false;
                        ClusterScaleOp::SendBucketsMigratingRpc(
                            false, result, rpc_fail);

                        if (!rpc_fail && result)
                        {
                            hd_res.SetFinished();
                        }
                        else
                        {
                            // only for retry.
                            hd_res.SetError(CcErrorCode::UNDEFINED_ERR);
                        }
                    });
            };

            LOG(INFO) << "Cluster scale transaction publish "
                         "bucket_migrating_end to all node groups ,txn: "
                      << txm->TxNumber();
            ForwardToSubOperation(txm, &pub_buckets_migrate_end_op_);
        }
    }
    else if (op_ == &notify_migration_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        CODE_FAULT_INJECTOR("retry_notify_migration_op", {
            LOG(INFO) << "Retry notify all node group to doing migration";
            for (auto &migrate_plan : notify_migration_op_.migrate_plans_)
            {
                migrate_plan.second.has_migration_tx_ = false;
            }

            FaultInject::Instance().InjectFault("retry_notify_migration_op",
                                                "remove");

            RetrySubOperation(txm, &notify_migration_op_);
            return;
        });

        // SetStatus(remote::ClusterScaleStatus::DATA_MIGRATION);
        assert(notify_migration_op_.unfinished_req_cnt_.load(
                   std::memory_order_relaxed) == 0);

        LOG(INFO) << "Cluster scale transaction wait for migration done, txn: "
                  << txm->TxNumber() << ", tx_term: " << txm->TxTerm()
                  << ", tx_ng_id: " << txm->TxCcNodeId();
        ForwardToSubOperation(txm, &check_migration_is_finished_op_);
    }
    else if (op_ == &check_migration_is_finished_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (!check_migration_is_finished_op_.migration_is_finished_)
        {
            LOG(INFO) << "Cluster scale transaction fail to check migration tx "
                         "status, txn "
                      << txm->TxNumber() << ", keey retrying";
            RetrySubOperation(txm, &check_migration_is_finished_op_);
            return;
        }

        if (event_type_ == ClusterScaleOpType::AddNodeGroup)
        {
            pub_buckets_migrate_end_op_.op_func_ = [](AsyncOp<Void> &async_op)
            {
                async_op.worker_thread_ = std::thread(
                    [&hd_res = async_op.hd_result_]
                    {
                        bool result = true;
                        bool rpc_fail = false;
                        ClusterScaleOp::SendBucketsMigratingRpc(
                            false, result, rpc_fail);

                        if (!rpc_fail && result)
                        {
                            hd_res.SetFinished();
                        }
                        else
                        {
                            // only for retry.
                            hd_res.SetError(CcErrorCode::UNDEFINED_ERR);
                        }
                    });
            };

            LOG(INFO) << "Cluster scale transaction publish "
                         "bucket_migrating_end to all node groups ,txn: "
                      << txm->TxNumber();
            ForwardToSubOperation(txm, &pub_buckets_migrate_end_op_);
        }
        else
        {
            assert(event_type_ == ClusterScaleOpType::RemoveNode);
            LOG(INFO)
                << "Cluster scale transaction acquire cluster config write "
                   "lock, txn: "
                << txm->TxNumber();
            ForwardToSubOperation(txm, &acquire_cluster_config_write_op_);
        }
    }
    else if (op_ == &pub_buckets_migrate_end_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (pub_buckets_migrate_end_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Cluster scale transaction failed to execute "
                          "pub_buckets_migrate_end_op_ , tx_number:"
                       << txm->TxNumber();

            // Retry until succeed.
            RetrySubOperation(txm, &pub_buckets_migrate_end_op_);
            return;
        }

        FillCleanLogRequest(txm);
        ForwardToSubOperation(txm, &clean_log_op_);
    }
    else if (op_ == &clean_log_op_)
    {
        if (clean_log_op_.hd_result_.IsError() && txm->CheckLeaderTerm())
        {
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &clean_log_op_);
            return;
        }

        ClearContainer();
        txm->state_stack_.pop_back();

        {
            auto shards = Sharder::Instance().GetLocalCcShards();
            std::lock_guard<std::mutex> lk(shards->cluster_scale_op_mux_);
            shards->cluster_scale_op_pool_.push_back(
                std::move(txm->cluster_scale_op_));
        }

        if (txm->CommitTs() != tx_op_failed_ts_)
        {
            LOG(INFO) << "Cluster scale transaction succeeded, txn: "
                      << txm->TxNumber() << ", tx_term: " << txm->TxTerm()
                      << ", tx_ng_id: " << txm->TxCcNodeId();
            // There is no one waiting on the tx request anymore, commit
            // and recycle the tx machine on our own.
            txm->Commit();
        }
        else
        {
            txm->Abort();
        }
    }
}

void ClusterScaleOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    Forward(txm);
}

void ClusterScaleOp::ClearContainer()
{
    new_ng_config_.clear();

    bucket_migrate_infos_.clear();

    notify_migration_op_.Clear();
}

void ClusterScaleOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    prepare_log_op_.log_type_ = TxLogType::PREPARE;

    prepare_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    prepare_log_rec->set_tx_term(txm->tx_term_);
    prepare_log_rec->set_txn_number(txm->TxNumber());

    prepare_log_rec->set_commit_timestamp(txm->commit_ts_);
    ::txlog::ClusterScaleOpMessage *cluster_scale_msg =
        prepare_log_rec->mutable_log_content()->mutable_cluster_scale_log();
    switch (event_type_)
    {
    case ClusterScaleOpType::AddNodeGroup:
        cluster_scale_msg->set_event_type(
            ::txlog::ClusterScaleOpMessage_ScaleOpType_AddNodeGroup);
        break;
    case ClusterScaleOpType::RemoveNode:
        cluster_scale_msg->set_event_type(
            ::txlog::ClusterScaleOpMessage_ScaleOpType_RemoveNode);
        break;
    case ClusterScaleOpType::AddNodeGroupPeers:
        cluster_scale_msg->set_event_type(
            ::txlog::ClusterScaleOpMessage_ScaleOpType_AddNodeGroupPeers);
        break;
    default:
        assert(false);
    }

    cluster_scale_msg->set_id(id_);

    cluster_scale_msg->set_stage(::txlog::ClusterScaleStage::PrepareScale);
    for (auto conf_pair : new_ng_config_)
    {
        ::txlog::NodegroupConfig *ng_conf =
            cluster_scale_msg->add_new_ng_configs();
        ng_conf->set_ng_id(conf_pair.first);
        for (auto &node : conf_pair.second)
        {
            ng_conf->add_member_nodes(node.node_id_);
            ng_conf->add_is_candidate(node.is_candidate_);
        }
        ::txlog::NodeConfig *node_conf = cluster_scale_msg->add_node_configs();
        node_conf->set_node_id(conf_pair.second[0].node_id_);
        node_conf->set_host_name(conf_pair.second[0].host_name_);
        node_conf->set_port(conf_pair.second[0].port_);
    }

    auto &ng_bucket_migrate_process =
        *cluster_scale_msg->mutable_node_group_bucket_migrate_process();

    for (const auto &migrate_plan : bucket_migrate_infos_)
    {
        NodeGroupId ng_id = migrate_plan.first;
        auto [iter, inserted] = ng_bucket_migrate_process.insert(
            {ng_id, txlog::NodeGroupMigrateMessage()});
        assert(inserted);

        iter->second.clear_migration_txns();
        iter->second.set_stage(::txlog::NodeGroupMigrateMessage_Stage::
                                   NodeGroupMigrateMessage_Stage_NotStarted);
        iter->second.set_old_owner(ng_id);

        auto &bucket_ids = migrate_plan.second.bucket_ids_;
        auto &new_owner_ngs = migrate_plan.second.new_owner_ngs_;
        assert(bucket_ids.size() == new_owner_ngs.size());

        auto &bucket_migrate_process =
            *iter->second.mutable_bucket_migrate_process();

        for (size_t idx = 0; idx < bucket_ids.size(); ++idx)
        {
            auto [bucket_migrate_process_iter, inserted] =
                bucket_migrate_process.insert(
                    {bucket_ids[idx], ::txlog::BucketMigrateMessage()});
            assert(inserted);
            bucket_migrate_process_iter->second.set_bucket_id(bucket_ids[idx]);
            bucket_migrate_process_iter->second.set_migration_txn(0);
            bucket_migrate_process_iter->second.set_migrate_ts(0);
            bucket_migrate_process_iter->second.set_old_owner(ng_id);
            bucket_migrate_process_iter->second.set_new_owner(
                new_owner_ngs[idx]);
            bucket_migrate_process_iter->second.set_stage(
                ::txlog::BucketMigrateStage::NotStarted);
        }
    }
}

void ClusterScaleOp::FillUpdateClusterConfigLogRequest(
    TransactionExecution *txm)
{
    update_cluster_config_log_op_.log_type_ = TxLogType::COMMIT;
    update_cluster_config_log_op_.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *log_rec =
        update_cluster_config_log_op_.log_closure_.LogRequest()
            .mutable_write_log_request();

    log_rec->set_tx_term(txm->tx_term_);
    log_rec->set_txn_number(txm->TxNumber());
    log_rec->set_commit_timestamp(txm->commit_ts_);
    ::txlog::ClusterScaleOpMessage *cluster_scale_msg =
        log_rec->mutable_log_content()->mutable_cluster_scale_log();
    cluster_scale_msg->set_stage(::txlog::ClusterScaleStage::ConfigUpdate);
    log_rec->mutable_node_terms()->clear();
}

void ClusterScaleOp::FillCleanLogRequest(TransactionExecution *txm)
{
    clean_log_op_.log_type_ = TxLogType::CLEAN;
    clean_log_op_.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *log_rec =
        clean_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    log_rec->set_tx_term(txm->tx_term_);
    log_rec->set_txn_number(txm->TxNumber());
    log_rec->set_commit_timestamp(txm->commit_ts_);
    ::txlog::ClusterScaleOpMessage *cluster_scale_msg =
        log_rec->mutable_log_content()->mutable_cluster_scale_log();
    cluster_scale_msg->set_stage(::txlog::ClusterScaleStage::CleanScale);
    log_rec->mutable_node_terms()->clear();
}

void ClusterScaleOp::SendBucketsMigratingRpc(bool is_migrating,
                                             bool &result,
                                             bool &rpc_error)
{
    auto all_node_groups = Sharder::Instance().AllNodeGroups();
    auto ng_cnt = all_node_groups->size();
    std::vector<remote::PubBucketsMigratingRequest> req_vec;
    std::vector<remote::PubBucketsMigratingResponse> resp_vec;
    std::vector<std::unique_ptr<brpc::Controller>> cntl_vec;
    req_vec.resize(ng_cnt);
    resp_vec.resize(ng_cnt);
    cntl_vec.resize(ng_cnt);
    //  rpc to all nodes
    size_t idx = 0;
    for (uint32_t ng_id : *all_node_groups)
    {
        uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);

        assert(channel != nullptr);
        remote::CcRpcService_Stub stub(channel.get());

        auto &req = req_vec.at(idx);
        req.set_node_group_id(ng_id);
        req.set_is_migrating(is_migrating);

        auto &resp = resp_vec.at(idx);
        cntl_vec[idx] = std::make_unique<brpc::Controller>();
        cntl_vec[idx]->set_timeout_ms(5000);
        cntl_vec[idx]->set_max_retry(3);
        stub.PublishBucketsMigrating(
            cntl_vec[idx].get(), &req, &resp, brpc::DoNothing());
        idx++;
    }

    for (auto &cntl : cntl_vec)
    {
        brpc::Join(cntl->call_id());
    }

    result = true;
    rpc_error = false;
    idx = 0;
    for (uint32_t ng_id : *all_node_groups)
    {
        if (cntl_vec.at(idx)->Failed())
        {
            rpc_error = true;
        }
        else if (!resp_vec.at(idx).success())
        {
            LOG(INFO) << "SendBucketsMigratingRpc failed, ng#" << ng_id;
            result = false;
        }
        idx++;
    }
    DLOG(INFO) << "SendBucketsMigratingRpc ,ng_cnt:" << ng_cnt
               << ",res:" << static_cast<int>(result)
               << ",rpcfailed:" << static_cast<int>(rpc_error);
}

CheckMigrationIsFinishedOp::CheckMigrationIsFinishedOp(
    TransactionExecution *txm)
    : closure_(&migration_is_finished_, &rpc_is_finished_)
{
}

void CheckMigrationIsFinishedOp::Reset()
{
    migration_is_finished_ = false;
    rpc_is_finished_ = false;
    closure_.Reset();
}

void CheckMigrationIsFinishedOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
    }
    else if (rpc_is_finished_.load(std::memory_order_acquire))
    {
        if (txm->CheckLeaderTerm())
        {
            if (!migration_is_finished_)
            {
                retry_num_ = 4;
                ReRunOp(txm);
            }
            else
            {
                txm->PostProcess(*this);
            }
        }
        else
        {
            // Not leader anymore
            txm->PostProcess(*this);
        }
    }
}

NotifyStartMigrateOp::NotifyStartMigrateOp(TransactionExecution *txm)
{
    auto ng_cnt = Sharder::Instance().NodeGroupCount();
    for (uint32_t idx = 0; idx < ng_cnt; ++idx)
    {
        auto closure = std::make_unique<NotifyMigrationClosure>(
            &unfinished_req_cnt_, migrate_plans_);
        closures_.push_back(std::move(closure));
    }
}

void NotifyStartMigrateOp::Clear()
{
    closures_.clear();
    closures_.shrink_to_fit();
    migrate_plans_.clear();
}

void NotifyStartMigrateOp::Reset(size_t node_group_count)
{
    // Reset response and controller
    for (size_t idx = 0; idx < closures_.size(); ++idx)
    {
        closures_[idx]->Reset();
    }

    // Resize the vector of the node group terms.
    size_t old_size = closures_.size();
    if (node_group_count > old_size)
    {
        for (size_t idx = old_size; idx < node_group_count; ++idx)
        {
            auto closure = std::make_unique<NotifyMigrationClosure>(
                &unfinished_req_cnt_, migrate_plans_);
            closures_.emplace_back(std::move(closure));
        }
    }
}

void NotifyStartMigrateOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
    }
    else if (unfinished_req_cnt_.load(std::memory_order_acquire) == 0)
    {
        if (txm->CheckLeaderTerm())
        {
            for (const auto &migrate_plan : migrate_plans_)
            {
                if (!migrate_plan.second.has_migration_tx_)
                {
                    retry_num_ = 5;
                    ReRunOp(txm);
                    return;
                }
            }

            txm->PostProcess(*this);
        }
        else
        {
            // Not leader anymore
            txm->PostProcess(*this);
        }
    }
}

void NotifyStartMigrateOp::InitDataMigration(TxNumber tx_number,
                                             NodeGroupId old_owner_id)
{
    auto shards = Sharder::Instance().GetLocalCcShards();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(old_owner_id);

    auto iter = migrate_plans_.find(old_owner_id);
    assert(iter != migrate_plans_.end());

    if (dest_node_id == shards->NodeId())
    {
        Sharder::Instance().GetTxWorkerPool()->SubmitWork(
            [cluster_scale_txn = tx_number,
             old_owner_id = old_owner_id,
             bucket_ids = iter->second.bucket_ids_,        // Copy
             new_owner_ngs = iter->second.new_owner_ngs_,  // Copy
             unfinished_cnt = &unfinished_req_cnt_,
             &migrate_plans = migrate_plans_](size_t) mutable
            {
                TxLog *tx_log = Sharder::Instance().GetLogAgent();
                auto cluster_scale_tx_log_ng_id =
                    tx_log->GetLogGroupId(cluster_scale_txn);

                TxService *tx_service =
                    Sharder::Instance().GetLocalCcShards()->GetTxservice();
                std::vector<TransactionExecution *> txms;
                std::vector<TxNumber> txns;
                // Each worker processes 10 buckets at a time.
                // int worker_tx_cnt =
                //     bucket_ids.size() > 100 ? 10 : (bucket_ids.size() / 10) +
                //     1;
                // // Each worker processes 10 buckets at a time.
                // std::vector<std::vector<uint16_t>> bucket_ids_per_task(1);
                // std::vector<std::vector<NodeGroupId>>
                // new_owner_ngs_per_task(1); std::vector<uint16_t>
                // *cur_task_bucket_ids =
                //     &bucket_ids_per_task.back();
                // std::vector<NodeGroupId> *cur_task_new_owner_ngs =
                //     &new_owner_ngs_per_task.back();
                // for (size_t i = 0; i < bucket_ids.size(); i++)
                // {
                //     cur_task_bucket_ids->push_back(bucket_ids[i]);
                //     cur_task_new_owner_ngs->push_back(new_owner_ngs[i]);
                //     if (cur_task_bucket_ids->size() == 10 &&
                //         i != bucket_ids.size() - 1)
                //     {
                //         bucket_ids_per_task.push_back(std::vector<uint16_t>());
                //         new_owner_ngs_per_task.push_back(
                //             std::vector<NodeGroupId>());
                //         cur_task_bucket_ids = &bucket_ids_per_task.back();
                //         cur_task_new_owner_ngs =
                //         &new_owner_ngs_per_task.back();
                //     }
                // }

                // Each worker processes all buckets that are on a specific
                // core.
                int worker_tx_cnt =
                    Sharder::Instance().GetLocalCcShards()->Count();
                // Put all buckets on the same core to the same task.
                std::vector<std::vector<uint16_t>> bucket_ids_per_task(
                    worker_tx_cnt);
                std::vector<std::vector<NodeGroupId>> new_owner_ngs_per_task(
                    worker_tx_cnt);
                for (size_t i = 0; i < bucket_ids.size(); i++)
                {
                    uint16_t core_id =
                        Sharder::Instance().ShardBucketIdToCoreIdx(
                            bucket_ids[i]);
                    bucket_ids_per_task[core_id].push_back(bucket_ids[i]);
                    new_owner_ngs_per_task[core_id].push_back(new_owner_ngs[i]);
                }

                // Remove empty tasks.
                for (auto task_it = bucket_ids_per_task.begin();
                     task_it != bucket_ids_per_task.end();)
                {
                    if (task_it->empty())
                    {
                        worker_tx_cnt--;
                        task_it = bucket_ids_per_task.erase(task_it);
                    }
                    else
                    {
                        task_it++;
                    }
                }
                for (int i = 0; i < worker_tx_cnt; i++)
                {
                    TransactionExecution *txm = tx_service->NewTx();

                    InitTxRequest init_req;
                    // Set isolation level to RepeatableRead to ensure the
                    // readlock will be set during the execution of the
                    // following ReadTxRequest.
                    init_req.iso_level_ = IsolationLevel::RepeatableRead;
                    init_req.protocol_ = CcProtocol::Locking;
                    // Set tx node group id
                    init_req.tx_ng_id_ = old_owner_id;
                    // Set log node group id to ensure the log will be write
                    // to special location.
                    init_req.log_group_id_ = cluster_scale_tx_log_ng_id;

                    init_req.Reset();
                    txm->Execute(&init_req);
                    init_req.Wait();

                    if (init_req.IsError())
                    {
                        for (auto cur_txm : txms)
                        {
                            AbortTxRequest abort_req;
                            cur_txm->Execute(&abort_req);
                            abort_req.Wait();
                        }
                        unfinished_cnt->fetch_sub(1, std::memory_order_release);
                        return;
                    }

                    txms.push_back(txm);
                    txns.push_back(txm->TxNumber());
                }

                std::shared_ptr<DataMigrationStatus> status =
                    std::make_shared<DataMigrationStatus>(
                        cluster_scale_txn,
                        std::move(bucket_ids_per_task),
                        std::move(new_owner_ngs_per_task),
                        std::move(txns));
                // All worker txs has been started, now write the first
                // prepare log, the first tx will write a prepare log that
                // marks the node group migration process has been started.
                // The log contains all of the worker txns, so once this log
                // is written, the migration of this node gorup is always
                // going to succeed.
                DataMigrationTxRequest migrate_req(status);
                txms[0]->Execute(&migrate_req);
                migrate_req.Wait();

                if (!migrate_req.IsError())
                {
                    migrate_plans[old_owner_id].has_migration_tx_ = true;
                    LOG(INFO) << "Data migration started on node group "
                              << old_owner_id;
                    for (size_t i = 1; i < txms.size(); i++)
                    {
                        // If the log is successfully written, start the
                        // rest of the workers.
                        DataMigrationTxRequest migrate_req(status);
                        txms[i]->Execute(&migrate_req);
                        migrate_req.Wait();
                    }
                }
                else
                {
                    assert(migrate_req.IsError());
                    if (migrate_req.ErrorCode() ==
                        TxErrorCode::DUPLICATE_MIGRATION_TX_ERROR)
                    {
                        // Migration on this node group is already in progress.
                        // We can mark the migration init as success.
                        migrate_plans[old_owner_id].has_migration_tx_ = true;
                    }
                    else
                    {
                        migrate_plans[old_owner_id].has_migration_tx_ = false;
                    }
                    // Abort rest of the workers.
                    for (size_t i = 1; i < txms.size(); i++)
                    {
                        AbortTxRequest abort_req;
                        txms[i]->Execute(&abort_req);
                        abort_req.Wait();
                    }
                }
                unfinished_cnt->fetch_sub(1, std::memory_order_release);
            });
    }
    else
    {
        // For remote node, use RPC service
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);
        if (channel == nullptr)
        {
            // Fail to establish the channel to the target node.
            LOG(ERROR) << "NotifyMigration RPC: Fail to init the channel to the"
                          " leader of ng#"
                       << old_owner_id;
            unfinished_req_cnt_.fetch_sub(1, std::memory_order_release);
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());
        auto &closure = closures_[old_owner_id];
        closure->SetChannel(dest_node_id, channel);
        closure->Controller()->set_timeout_ms(-1);
        auto &request = closure->Request();
        request.Clear();
        request.set_orig_owner(old_owner_id);
        request.set_tx_number(tx_number);

        auto &bucket_ids = iter->second.bucket_ids_;
        auto &new_owner_ngs = iter->second.new_owner_ngs_;

        for (size_t idx = 0; idx < bucket_ids.size(); ++idx)
        {
            remote::InitMigrationRequest_MigrateInfo *migrate_info =
                request.add_migrate_infos();
            migrate_info->set_bucket_id(bucket_ids[idx]);
            migrate_info->set_new_owner(new_owner_ngs[idx]);
        }

        stub.InitDataMigration(closure->Controller(),
                               &request,
                               &closure->Response(),
                               closure.get());
        LOG(INFO) << "Remote RPC NotifyMigration of ng#" << old_owner_id << ".";
    }
}

DataMigrationOp::DataMigrationOp(TransactionExecution *txm,
                                 std::shared_ptr<DataMigrationStatus> status)
    : CompositeTransactionOperation(),
      write_first_prepare_log_op_(txm),
      write_before_locking_log_op_(txm),
      prepare_bucket_lock_op_(txm),
      prepare_log_op_(txm),
      install_dirty_bucket_op_(txm),
      data_sync_op_(txm),
      acquire_bucket_lock_op_(txm),
      commit_log_op_(txm),
      kickout_data_op_(txm),
      post_all_bucket_lock_op_(txm),
      clean_log_op_(txm),
      write_last_clean_log_op_(txm),
      status_(status)
{
    prepare_bucket_lock_op_.table_name_ = &range_bucket_ccm_name;
    prepare_bucket_lock_op_.cc_op_ = CcOperation::Write;
    prepare_bucket_lock_op_.protocol_ = CcProtocol::Locking;

    acquire_bucket_lock_op_.table_name_ = &range_bucket_ccm_name;
    acquire_bucket_lock_op_.cc_op_ = CcOperation::Write;
    acquire_bucket_lock_op_.protocol_ = CcProtocol::Locking;

    install_dirty_bucket_op_.table_name_ = &range_bucket_ccm_name;
    install_dirty_bucket_op_.write_type_ = PostWriteType::PrepareCommit;
    install_dirty_bucket_op_.op_type_ = OperationType::Update;

    post_all_bucket_lock_op_.table_name_ = &range_bucket_ccm_name;
    post_all_bucket_lock_op_.write_type_ = PostWriteType::PostCommit;
    post_all_bucket_lock_op_.op_type_ = OperationType::Update;
}

void DataMigrationOp::Reset(TransactionExecution *txm,
                            std::shared_ptr<DataMigrationStatus> status)
{
    // Reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;
    op_start_ = metrics::TimePoint::max();

    // Reset CompositeTransactionOperation
    op_ = nullptr;

    migrate_bucket_idx_ = 0;

    status_ = status;

    write_first_prepare_log_op_.Reset();
    write_first_prepare_log_op_.ResetHandlerTxm(txm);

    write_before_locking_log_op_.Reset();
    write_before_locking_log_op_.ResetHandlerTxm(txm);

    prepare_bucket_lock_op_.ResetHandlerTxm(txm);

    prepare_log_op_.Reset();
    prepare_log_op_.ResetHandlerTxm(txm);

    install_dirty_bucket_op_.ResetHandlerTxm(txm);

    data_sync_op_.Reset();
    data_sync_op_.ResetHandlerTxm(txm);

    acquire_bucket_lock_op_.ResetHandlerTxm(txm);

    commit_log_op_.Reset();
    commit_log_op_.ResetHandlerTxm(txm);

    kickout_data_op_.Reset();
    kickout_data_op_.ResetHandlerTxm(txm);

    post_all_bucket_lock_op_.ResetHandlerTxm(txm);

    clean_log_op_.Reset();
    clean_log_op_.ResetHandlerTxm(txm);

    write_last_clean_log_op_.Reset();
    write_last_clean_log_op_.ResetHandlerTxm(txm);

    assert(migrate_bucket_idx_ == 0);

    for (size_t i = 0; i < bucket_keys_.size(); i++)
    {
        bucket_info_[i].Reset();
        bucket_records_[i].SetBucketInfo(&bucket_info_[i]);
    }

    prepare_bucket_lock_op_.table_name_ = &range_bucket_ccm_name;
    prepare_bucket_lock_op_.cc_op_ = CcOperation::Write;
    prepare_bucket_lock_op_.protocol_ = CcProtocol::Locking;

    acquire_bucket_lock_op_.table_name_ = &range_bucket_ccm_name;
    install_dirty_bucket_op_.write_type_ = PostWriteType::PrepareCommit;
    install_dirty_bucket_op_.op_type_ = OperationType::Update;

    post_all_bucket_lock_op_.table_name_ = &range_bucket_ccm_name;
    post_all_bucket_lock_op_.write_type_ = PostWriteType::PostCommit;
    post_all_bucket_lock_op_.op_type_ = OperationType::Update;
}

void DataMigrationOp::PrepareNextRoundBuckets()
{
    // Prepare next round of bucket keys
    bucket_keys_.resize(status_->bucket_ids_[migrate_bucket_idx_].size());
    bucket_records_.resize(status_->bucket_ids_[migrate_bucket_idx_].size());
    bucket_info_.resize(status_->bucket_ids_[migrate_bucket_idx_].size());

    // Reset the keys and records in subops.
    prepare_bucket_lock_op_.keys_.clear();
    acquire_bucket_lock_op_.keys_.clear();
    install_dirty_bucket_op_.keys_.clear();
    install_dirty_bucket_op_.recs_.clear();
    post_all_bucket_lock_op_.keys_.clear();
    post_all_bucket_lock_op_.recs_.clear();
    for (size_t i = 0; i < bucket_keys_.size(); i++)
    {
        // initialize bucket keys for buckets that are going to be processed
        // in this round.
        bucket_keys_[i].bucket_id_ =
            status_->bucket_ids_[migrate_bucket_idx_][i];
        bucket_info_[i].Reset();
        bucket_records_[i].SetBucketInfo(&bucket_info_[i]);

        // initialize bucket keys and records in the ops for this round.
        prepare_bucket_lock_op_.keys_.emplace_back(&bucket_keys_[i]);

        acquire_bucket_lock_op_.keys_.emplace_back(&bucket_keys_[i]);

        install_dirty_bucket_op_.keys_.emplace_back(&bucket_keys_[i]);
        install_dirty_bucket_op_.recs_.emplace_back(&bucket_records_[i]);

        post_all_bucket_lock_op_.keys_.emplace_back(&bucket_keys_[i]);
        post_all_bucket_lock_op_.recs_.emplace_back(&bucket_records_[i]);
    }
}

void DataMigrationOp::Forward(TransactionExecution *txm)
{
    if (txm->TxStatus() == TxnStatus::Recovering &&
        Sharder::Instance().LeaderTerm(txm->TxCcNodeId()) < 0)
    {
        // This is a recovered tx and replay is not done yet. We should wait
        // for replay finish before forwarding tx machine.
        if (Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId()) !=
            txm->TxTerm())
        {
            // Recovered term is invalid. Do not call ForceToFinish as it
            // will cause infinite recursive call. Clean up tx state
            // directly.
            Clear();
            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());

            {
                auto shards = Sharder::Instance().GetLocalCcShards();
                std::lock_guard<std::mutex> lk(
                    shards->data_migration_op_pool_mux_);
                shards->migration_op_pool_.push_back(
                    std::move(txm->migration_op_));
            }
            // Abort and recyle txm
            txm->Abort();
        }
        return;
    }
    if (op_ == nullptr)
    {
        migrate_bucket_idx_ = status_->next_bucket_idx_.fetch_add(1);
        if (migrate_bucket_idx_ >= status_->bucket_ids_.size())
        {
            if (status_->unfinished_worker_.fetch_sub(
                    1, std::memory_order_release) == 1)
            {
                // Last worker quit should write last clean log that marks
                // the node group migration has been finished.
                LOG(INFO) << "Data migration: write last clean log"
                          << ", tx number: " << txm->TxNumber();
                FillLastLogRequest(txm);
                ForwardToSubOperation(txm, &write_last_clean_log_op_);
            }
            else
            {
                LOG(INFO) << "Data migration: worker finished migrating "
                             "buckets, tx_number "
                          << txm->TxNumber();
                ForceToFinish(txm);
            }

            return;
        }

        PrepareNextRoundBuckets();

        if (migrate_bucket_idx_ == 0 &&
            txm->TxStatus() != TxnStatus::Recovering)
        {
            // Write the first log to check if the log of cluster scale tx
            // is exsit.
            FillFirstLogRequest(txm, status_->migration_txns_);
            ForwardToSubOperation(txm, &write_first_prepare_log_op_);
        }
        else
        {
            assert(status_->bucket_ids_[migrate_bucket_idx_].size());
            std::string log_msg("Data migration: start migrate bucket: ");
            for (auto id : status_->bucket_ids_[migrate_bucket_idx_])
            {
                log_msg += std::to_string(id) + ", ";
            }
            log_msg += " txn: ";
            log_msg += std::to_string(txm->TxNumber());
            LOG(INFO) << log_msg;
            FillLogRequest(txm,
                           &write_before_locking_log_op_,
                           TxLogType::COMMIT,
                           txlog::BucketMigrateStage::BeforeLocking);
            ForwardToSubOperation(txm, &write_before_locking_log_op_);
        }
    }
    else if (op_ == &write_first_prepare_log_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            txm->void_resp_->FinishError(
                TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
            ForceToFinish(txm);
            return;
        }

        if (write_first_prepare_log_op_.hd_result_.IsError())
        {
            CcErrorCode prepare_log_op_err_code =
                write_first_prepare_log_op_.hd_result_.ErrorCode();
            if (prepare_log_op_err_code ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the
                // coordinator itself is no longer leader
                int64_t tx_node_term =
                    Sharder::Instance().LeaderTerm(txm->TxCcNodeId());
                if (tx_node_term > 0)
                {
                    // set retry flag and retry prepare log
                    ::txlog::WriteLogRequest *log_req =
                        write_first_prepare_log_op_.log_closure_.LogRequest()
                            .mutable_write_log_request();
                    log_req->set_retry(true);
                    RetrySubOperation(txm, &write_first_prepare_log_op_);
                }
                else
                {
                    // Not leader anymore, just quit. New leader will know
                    // whether prepare log succeeds and continue the rest if
                    // it does. Caller need to query new leader of node
                    // group to know if write log has succeeded.
                    txm->void_resp_->FinishError(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);
                    ForceToFinish(txm);
                }
            }
            else if (prepare_log_op_err_code ==
                     CcErrorCode::DUPLICATE_MIGRATION_TX_ERR)
            {
                // This migration transaction is duplicated. There is
                // another transaction doing the data migration. We need to
                // abort this migration tx. But we can mark the migration tx
                // as started.
                LOG(WARNING)
                    << "Data migration: duplicate migration tx detected, "
                       "cluster_scale_txn: "
                    << status_->cluster_scale_txn_
                    << ", txn: " << txm->TxNumber();

                txm->void_resp_->FinishError(
                    TxErrorCode::DUPLICATE_MIGRATION_TX_ERROR);
                ForceToFinish(txm);
            }
            else
            {
                txm->void_resp_->FinishError(TxErrorCode::WRITE_LOG_FAIL);
                ForceToFinish(txm);
            }
            return;
        }

        // Notify requester that log has been written.
        txm->void_resp_->Finish(void_);
        txm->commit_ts_ = txm->commit_ts_bound_ + 1;
        std::string log_msg("Data migration: start migrate bucket: ");
        for (auto id : status_->bucket_ids_[migrate_bucket_idx_])
        {
            log_msg += std::to_string(id) + ", ";
        }
        log_msg += " txn: ";
        log_msg += std::to_string(txm->TxNumber());
        LOG(INFO) << log_msg;

        FillLogRequest(txm,
                       &write_before_locking_log_op_,
                       TxLogType::COMMIT,
                       txlog::BucketMigrateStage::BeforeLocking);
        ForwardToSubOperation(txm, &write_before_locking_log_op_);
    }
    else if (op_ == &write_before_locking_log_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (write_before_locking_log_op_.hd_result_.IsError())
        {
            LOG(WARNING) << "Data migration: fail to write migrate txn log, "
                         << ", tx_number:" << txm->TxNumber()
                         << ", keep retrying";
            // set retry flag and retry commit log
            ::txlog::WriteLogRequest *log_req =
                write_before_locking_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &write_before_locking_log_op_);
            return;
        }
        if (migrate_bucket_idx_ == 20)
        {
            ACTION_FAULT_INJECTOR("data_migrate_before_prepare_log");
        }

        LOG(INFO) << "Data migration: prepare bucket lock"
                  << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &prepare_bucket_lock_op_);
    }
    else if (op_ == &prepare_bucket_lock_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (prepare_bucket_lock_op_.fail_cnt_.load(std::memory_order_relaxed) >
            0)
        {
            LOG(ERROR) << "Data Migration: failed to prepare acquire all"
                       << ", tx_number: " << txm->TxNumber()
                       << ", Keep retrying";

            if (prepare_bucket_lock_op_.IsDeadlock())
            {
                LOG(ERROR) << "Data migration: deadlocks with other "
                              "transactions, downgrade write lock, tx_number:"
                           << txm->TxNumber();
                post_all_bucket_lock_op_.write_type_ =
                    PostWriteType::DowngradeLock;
                ForwardToSubOperation(txm, &post_all_bucket_lock_op_);
            }
            else
            {
                RetrySubOperation(txm, &prepare_bucket_lock_op_);
            }
            return;
        }

        txm->commit_ts_ =
            std::max(txm->commit_ts_, prepare_bucket_lock_op_.MaxTs());

        LOG(INFO) << "Data migration: write prepare log"
                  << ", txn: " << txm->TxNumber();

        FillLogRequest(txm,
                       &prepare_log_op_,
                       TxLogType::PREPARE,
                       txlog::BucketMigrateStage::PrepareMigrate);
        ForwardToSubOperation(txm, &prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (prepare_log_op_.hd_result_.IsError())
        {
            LOG(WARNING) << "Data migration: fail to write prepare log, "
                         << ", tx_number:" << txm->TxNumber()
                         << ", keep retrying";
            // set retry flag and retry commit log
            ::txlog::WriteLogRequest *log_req =
                prepare_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &prepare_log_op_);
            return;
        }

        auto local_shards = Sharder::Instance().GetLocalCcShards();
        ranges_in_bucket_snapshot_.clear();
        for (auto id : status_->bucket_ids_[migrate_bucket_idx_])
        {
            auto range_ids =
                local_shards->GetRangesInBucket(id, txm->TxCcNodeId());
            for (auto &[tbl, ranges] : range_ids)
            {
                auto tbl_it = ranges_in_bucket_snapshot_.try_emplace(tbl);
                tbl_it.first->second.insert(ranges.begin(), ranges.end());
            }
        }

        hash_partitioned_tables_snapshot_.clear();
        auto all_catalog_snapshot = local_shards->GetCatalogTableNameSnapshot(
            txm->TxCcNodeId(), txm->CommitTs());
        for (auto &[tbl, is_dirty] : all_catalog_snapshot)
        {
            if (tbl.IsHashPartitioned())
            {
                assert(!is_dirty);
                hash_partitioned_tables_snapshot_.insert(tbl);
            }
        }

        if (ranges_in_bucket_snapshot_.empty() &&
            hash_partitioned_tables_snapshot_.empty())
        {
            // There's no range that needs to be flushed to data store.
            // Commit the new bucket info directly.
            LOG(INFO) << "Data migration: post write all"
                      << ", txn: " << txm->TxNumber();
            for (size_t i = 0;
                 i < status_->bucket_ids_[migrate_bucket_idx_].size();
                 i++)
            {
                bucket_info_[i].Reset();
                bucket_info_[i].Set(
                    status_->new_owner_ngs_[migrate_bucket_idx_][i],
                    txm->CommitTs());
                bucket_records_[i].SetBucketInfo(&bucket_info_[i]);
            }
            post_all_bucket_lock_op_.write_type_ = PostWriteType::Commit;
            ForwardToSubOperation(txm, &post_all_bucket_lock_op_);
        }
        else
        {
            LOG(INFO) << "Data migration: install dirty bucket, "
                      << ", txn: " << txm->TxNumber();

            for (size_t i = 0;
                 i < status_->bucket_ids_[migrate_bucket_idx_].size();
                 i++)
            {
                const BucketInfo *bucket_info =
                    Sharder::Instance().GetLocalCcShards()->GetBucketInfo(
                        status_->bucket_ids_[migrate_bucket_idx_][i],
                        txm->TxCcNodeId());
                bucket_info_[i] = *bucket_info;
                assert(txm->CommitTs() > bucket_info_[i].Version());
                bucket_info_[i].SetDirty(
                    status_->new_owner_ngs_[migrate_bucket_idx_][i],
                    txm->CommitTs());
                bucket_records_[i].SetBucketInfo(&bucket_info_[i]);
            }
            ForwardToSubOperation(txm, &install_dirty_bucket_op_);
        }
    }
    else if (op_ == &install_dirty_bucket_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (install_dirty_bucket_op_.hd_result_.IsError())
        {
            LOG(ERROR)
                << "Data migration: failed to install dirty bucekct record"
                << ",tx number : " << txm->TxNumber() << ", keep retrying"
                << ", err msg : "
                << install_dirty_bucket_op_.hd_result_.ErrorMsg();
            for (size_t i = 0;
                 i < status_->bucket_ids_[migrate_bucket_idx_].size();
                 i++)
            {
                bucket_records_[i].SetBucketInfo(&bucket_info_[i]);
            }
            RetrySubOperation(txm, &install_dirty_bucket_op_);
            return;
        }

        CODE_FAULT_INJECTOR("data_migration_install_dirty_continue",
                            { return; });

        LOG(INFO) << "Data migration: flush data in bucket"
                  << ", txn: " << txm->TxNumber();

        data_sync_op_.op_func_ = [this, txm](AsyncOp<Void> &async_op)
        {
            LocalCcShards *shard = Sharder::Instance().GetLocalCcShards();
            shard->EnqueueDataSyncTaskForBucket(
                ranges_in_bucket_snapshot_,
                hash_partitioned_tables_snapshot_,
                status_->bucket_ids_[migrate_bucket_idx_],
                true,
                txm->TxCcNodeId(),
                txm->TxTerm(),
                txm->CommitTs(),
                &async_op.hd_result_);
        };

        ACTION_FAULT_INJECTOR("data_migrate_after_install_dirty");
        ForwardToSubOperation(txm, &data_sync_op_);
    }
    else if (op_ == &data_sync_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (data_sync_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Data migration: failed to sync data in bucket "
                       << ",tx number : " << txm->TxNumber()
                       << ", keep retrying"
                       << ", err msg : " << data_sync_op_.hd_result_.ErrorMsg();
            RetrySubOperation(txm, &data_sync_op_);
            return;
        }

        LOG(INFO) << "Data migration: commit acquire all"
                  << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &acquire_bucket_lock_op_);
    }
    else if (op_ == &acquire_bucket_lock_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (acquire_bucket_lock_op_.fail_cnt_.load(std::memory_order_relaxed) >
            0)
        {
            LOG(ERROR) << "Data migration: failed to commit acquire all"
                       << ", tx_number:" << txm->TxNumber()
                       << ", Keep retrying";

            assert(post_all_bucket_lock_op_.write_type_ ==
                   PostWriteType::PostCommit);

            if (acquire_bucket_lock_op_.IsDeadlock())
            {
                LOG(ERROR) << "Data migration: deadlocks with other "
                              "transactions, downgrade write lock, tx_number:"
                           << txm->TxNumber();
                // Downgrade write lock to write intent
                post_all_bucket_lock_op_.write_type_ =
                    PostWriteType::DowngradeLock;
                ForwardToSubOperation(txm, &post_all_bucket_lock_op_);
            }
            else
            {
                RetrySubOperation(txm, &acquire_bucket_lock_op_);
            }
            return;
        }

        LOG(INFO) << "Data migration: write commit log"
                  << ", txn: " << txm->TxNumber();

        FillLogRequest(txm,
                       &commit_log_op_,
                       TxLogType::COMMIT,
                       txlog::BucketMigrateStage::CommitMigrate);
        ForwardToSubOperation(txm, &commit_log_op_);
    }
    else if (op_ == &commit_log_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (commit_log_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Data migration: fail to write commit log"
                       << ", tx_number:" << txm->TxNumber()
                       << ", keep retrying";
            // set retry flag and retry commit log
            ::txlog::WriteLogRequest *log_req =
                commit_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &commit_log_op_);
            return;
        }

        // Remove all data in this bucket. We need to retake another
        // snapshot of ranges in bucket here since new data could be
        // inserted into this bucket since when we took the first snapshot
        // in the first phase of commit.
        for (size_t i = 0; i < status_->bucket_ids_[migrate_bucket_idx_].size();
             i++)
        {
            bucket_info_[i].Reset();
            bucket_info_[i].SetDirty(
                status_->new_owner_ngs_[migrate_bucket_idx_][i],
                txm->CommitTs());
            bucket_records_[i].SetBucketInfo(&bucket_info_[i]);
        }

        auto local_shards = Sharder::Instance().GetLocalCcShards();
        ranges_in_bucket_snapshot_.clear();
        for (auto id : status_->bucket_ids_[migrate_bucket_idx_])
        {
            auto range_ids =
                local_shards->GetRangesInBucket(id, txm->TxCcNodeId());
            for (auto &[tbl, ranges] : range_ids)
            {
                auto tbl_it = ranges_in_bucket_snapshot_.try_emplace(tbl);
                tbl_it.first->second.insert(ranges.begin(), ranges.end());
            }
        }
        kickout_range_tbl_it_ = ranges_in_bucket_snapshot_.cbegin();
        //  For hash partition, send kickout cc to each cc map to kickout
        //  data in this bucket.
        hash_partitioned_tables_snapshot_.clear();
        auto all_catalog_snapshot = local_shards->GetCatalogTableNameSnapshot(
            txm->TxCcNodeId(), txm->CommitTs());
        for (auto &[tbl, is_dirty] : all_catalog_snapshot)
        {
            if (tbl.IsHashPartitioned())
            {
                assert(!is_dirty);
                hash_partitioned_tables_snapshot_.insert(tbl);
            }
        }
        kickout_hash_partitioned_tbl_it_ =
            hash_partitioned_tables_snapshot_.cbegin();

        // Test drop table t1 concurrently. See eloq_test repo. table
        // name need to keep consistent
        CODE_FAULT_INJECTOR("add_dropped_table_for_test", {
            std::string t1_table_name = "./test/t1";
            TableName t1_tbl(
                t1_table_name, TableType::Primary, TableEngine::EloqSql);
            for (auto id : status_->bucket_ids_[migrate_bucket_idx_])
            {
                if (id == 1109)
                {
                    auto tbl_it =
                        ranges_in_bucket_snapshot_.try_emplace(t1_tbl);
                    tbl_it.first->second.insert(24);
                    LOG(INFO)
                        << "Add new dropped table for test, range id = " << 24
                        << ", bucket id = " << id;
                }
            }
        });

        if (hash_partitioned_tables_snapshot_.empty() &&
            ranges_in_bucket_snapshot_.empty())
        {
            LOG(INFO) << "Data migration: post write all"
                      << ", txn: " << txm->TxNumber();
            post_all_bucket_lock_op_.write_type_ = PostWriteType::PostCommit;
            ForwardToSubOperation(txm, &post_all_bucket_lock_op_);
            return;
        }

        if (kickout_range_tbl_it_ != ranges_in_bucket_snapshot_.cend())
        {
            kickout_data_op_.node_group_ = txm->TxCcNodeId();
            kickout_range_it_ = kickout_range_tbl_it_->second.cbegin();

            // Table name is ranges_in_bucket_snapshot_ is of type range
            // partition, need to convert it first.
            TableType type;
            if (TableName::IsBase(kickout_range_tbl_it_->first.StringView()))
            {
                type = TableType::Primary;
            }
            else if (TableName::IsUniqueSecondary(
                         kickout_range_tbl_it_->first.StringView()))
            {
                type = TableType::UniqueSecondary;
            }
            else
            {
                type = TableType::Secondary;
            }
            kickout_range_table_ =
                TableName{kickout_range_tbl_it_->first.StringView(),
                          type,
                          kickout_range_tbl_it_->first.Engine()};
            kickout_data_op_.table_name_ = &kickout_range_table_;
            // All data in this range is clean target.
            kickout_data_op_.clean_type_ =
                CleanType::CleanRangeDataForMigration;

            bool table_exist = false;

            while (!table_exist)
            {
                auto range_keys =
                    Sharder::Instance().GetLocalCcShards()->GetTableRangeKeys(
                        *kickout_data_op_.table_name_,
                        kickout_data_op_.node_group_,
                        *kickout_range_it_);

                table_exist = range_keys.has_value();

                // Table has been dropped.
                if (!table_exist)
                {
                    // Move to next table
                    if (++kickout_range_tbl_it_ ==
                        ranges_in_bucket_snapshot_.cend())
                    {
                        break;
                    }

                    TableType type;
                    if (TableName::IsBase(
                            kickout_range_tbl_it_->first.StringView()))
                    {
                        type = TableType::Primary;
                    }
                    else if (TableName::IsUniqueSecondary(
                                 kickout_range_tbl_it_->first.StringView()))
                    {
                        type = TableType::UniqueSecondary;
                    }
                    else
                    {
                        type = TableType::Secondary;
                    }
                    kickout_range_table_ =
                        TableName{kickout_range_tbl_it_->first.StringView(),
                                  type,
                                  kickout_range_tbl_it_->first.Engine()};
                    kickout_data_op_.table_name_ = &kickout_range_table_;
                    kickout_range_it_ = kickout_range_tbl_it_->second.cbegin();
                }
                else
                {
                    assert(range_keys.has_value());
                    kickout_data_op_.range_id_ = *kickout_range_it_;
                    kickout_data_op_.range_version_ =
                        std::get<0>(range_keys.value());
                    kickout_data_op_.start_key_ =
                        std::move(std::get<1>(range_keys.value()));
                    kickout_data_op_.end_key_ =
                        std::move(std::get<2>(range_keys.value()));
                }
            }
        }
        if (kickout_range_tbl_it_ == ranges_in_bucket_snapshot_.cend())
        {
            if (kickout_hash_partitioned_tbl_it_ ==
                hash_partitioned_tables_snapshot_.cend())
            {
                LOG(INFO) << "Data migration: post write all"
                          << ", txn: " << txm->TxNumber();
                post_all_bucket_lock_op_.write_type_ =
                    PostWriteType::PostCommit;
                ForwardToSubOperation(txm, &post_all_bucket_lock_op_);
                return;
            }
            kickout_data_op_.node_group_ = txm->TxCcNodeId();
            kickout_data_op_.table_name_ = &(*kickout_hash_partitioned_tbl_it_);
            kickout_data_op_.start_key_ = TxKey();
            kickout_data_op_.end_key_ = TxKey();
            kickout_data_op_.bucket_ids_ =
                &status_->bucket_ids_[migrate_bucket_idx_];
            // Check if the key is hashed to this bucket
            kickout_data_op_.clean_type_ = CleanType::CleanBucketData;
        }
        LOG(INFO) << "Data migration: kickout bucket data"
                  << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &kickout_data_op_);
    }
    else if (op_ == &kickout_data_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (kickout_data_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Data migration: fail to kickout range data"
                       << ", table name "
                       << kickout_range_tbl_it_->first.StringView()
                       << ", tx_number:" << txm->TxNumber()
                       << ", keep retrying";
            RetrySubOperation(txm, &kickout_data_op_);
            return;
        }

        if (kickout_range_tbl_it_ != ranges_in_bucket_snapshot_.cend())
        {
            if (++kickout_range_it_ == kickout_range_tbl_it_->second.cend())
            {
                if (++kickout_range_tbl_it_ ==
                    ranges_in_bucket_snapshot_.cend())
                {
                    LOG(INFO) << "Data migration: post write all"
                              << ", txn: " << txm->TxNumber();
                    post_all_bucket_lock_op_.write_type_ =
                        PostWriteType::PostCommit;
                    ForwardToSubOperation(txm, &post_all_bucket_lock_op_);
                    return;
                }
                TableType type;
                if (TableName::IsBase(
                        kickout_range_tbl_it_->first.StringView()))
                {
                    type = TableType::Primary;
                }
                else if (TableName::IsUniqueSecondary(
                             kickout_range_tbl_it_->first.StringView()))
                {
                    type = TableType::UniqueSecondary;
                }
                else
                {
                    type = TableType::Secondary;
                }
                kickout_range_table_ =
                    TableName{kickout_range_tbl_it_->first.StringView(),
                              type,
                              kickout_range_tbl_it_->first.Engine()};
                kickout_data_op_.table_name_ = &kickout_range_table_;
                kickout_range_it_ = kickout_range_tbl_it_->second.cbegin();
            }

            bool table_exist = false;

            while (!table_exist)
            {
                auto range_keys =
                    Sharder::Instance().GetLocalCcShards()->GetTableRangeKeys(
                        *kickout_data_op_.table_name_,
                        kickout_data_op_.node_group_,
                        *kickout_range_it_);

                table_exist = range_keys.has_value();

                // Table has been dropped. So we don't need to kickout data on
                // this table
                if (!table_exist)
                {
                    // Move to next table
                    if (++kickout_range_tbl_it_ ==
                        ranges_in_bucket_snapshot_.cend())
                    {
                        // Move to hash partitioned tables
                        break;
                    }
                    TableType type;
                    if (TableName::IsBase(
                            kickout_range_tbl_it_->first.StringView()))
                    {
                        type = TableType::Primary;
                    }
                    else if (TableName::IsUniqueSecondary(
                                 kickout_range_tbl_it_->first.StringView()))
                    {
                        type = TableType::UniqueSecondary;
                    }
                    else
                    {
                        type = TableType::Secondary;
                    }
                    kickout_range_table_ =
                        TableName{kickout_range_tbl_it_->first.StringView(),
                                  type,
                                  kickout_range_tbl_it_->first.Engine()};
                    kickout_data_op_.table_name_ = &kickout_range_table_;
                    kickout_range_it_ = kickout_range_tbl_it_->second.cbegin();
                }
                else
                {
                    assert(range_keys.has_value());
                    kickout_data_op_.range_id_ = *kickout_range_it_;
                    kickout_data_op_.range_version_ =
                        std::get<0>(range_keys.value());
                    kickout_data_op_.start_key_ =
                        std::move(std::get<1>(range_keys.value()));
                    kickout_data_op_.end_key_ =
                        std::move(std::get<2>(range_keys.value()));
                }
            }
        }

        if (kickout_range_tbl_it_ == ranges_in_bucket_snapshot_.cend())
        {
            // Kickout hash partitioned tables after range partitioned tables.
            if (++kickout_hash_partitioned_tbl_it_ ==
                hash_partitioned_tables_snapshot_.cend())
            {
                LOG(INFO) << "Data migration: post write all"
                          << ", txn: " << txm->TxNumber();
                post_all_bucket_lock_op_.write_type_ =
                    PostWriteType::PostCommit;
                ForwardToSubOperation(txm, &post_all_bucket_lock_op_);
                return;
            }
            kickout_data_op_.table_name_ = &(*kickout_hash_partitioned_tbl_it_);
        }

        ForwardToSubOperation(txm, &kickout_data_op_);
    }
    else if (op_ == &post_all_bucket_lock_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (post_all_bucket_lock_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Data migration: fail to post all bucket lock"
                       << ", tx number: " << txm->TxNumber();
            for (size_t i = 0;
                 i < status_->bucket_ids_[migrate_bucket_idx_].size();
                 i++)
            {
                bucket_records_[i].SetBucketInfo(&bucket_info_[i]);
            }
            RetrySubOperation(txm, &post_all_bucket_lock_op_);
            return;
        }

        if (post_all_bucket_lock_op_.write_type_ ==
            PostWriteType::DowngradeLock)
        {
            post_all_bucket_lock_op_.write_type_ = PostWriteType::PostCommit;

            if (prepare_log_op_.hd_result_.IsFinished())
            {
                ForwardToSubOperation(txm, &acquire_bucket_lock_op_);
            }
            else
            {
                ForwardToSubOperation(txm, &prepare_bucket_lock_op_);
            }

            return;
        }

        FillLogRequest(txm,
                       &clean_log_op_,
                       TxLogType::CLEAN,
                       ::txlog::BucketMigrateStage::CleanMigrate);

        LOG(INFO) << "Data migration: write clean log"
                  << ", tx number: " << txm->TxNumber();
        ForwardToSubOperation(txm, &clean_log_op_);
    }
    else if (op_ == &clean_log_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
            return;
        }

        if (clean_log_op_.hd_result_.IsError())
        {
            LOG(WARNING) << "Data migration: fail to write clean log"
                         << ", tx_number:" << txm->TxNumber()
                         << ", keep retrying";
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &clean_log_op_);
            return;
        }

        // Process next set of buckets
        op_ = nullptr;
    }
    else if (op_ == &write_last_clean_log_op_)
    {
        if (write_last_clean_log_op_.hd_result_.IsError() &&
            txm->CheckLeaderTerm())
        {
            // Retry
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                write_last_clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &write_last_clean_log_op_);
            return;
        }

        Clear();
        txm->state_stack_.pop_back();
        assert(txm->state_stack_.empty());

        {
            auto shards = Sharder::Instance().GetLocalCcShards();
            std::lock_guard<std::mutex> lk(shards->data_migration_op_pool_mux_);
            shards->migration_op_pool_.push_back(std::move(txm->migration_op_));
        }
        // Commit the tx and recyle txm
        txm->Commit();
    }
}

void DataMigrationOp::FillFirstLogRequest(TransactionExecution *txm,
                                          std::vector<uint64_t> &migration_txns)
{
    write_first_prepare_log_op_.log_type_ = TxLogType::PREPARE;
    write_first_prepare_log_op_.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *log_rec =
        write_first_prepare_log_op_.log_closure_.LogRequest()
            .mutable_write_log_request();

    log_rec->set_tx_term(txm->tx_term_);
    log_rec->set_txn_number(txm->TxNumber());
    log_rec->set_commit_timestamp(txm->CommitTs());

    ::txlog::DataMigrateTxLogMessage *migration_tx_log_message =
        log_rec->mutable_log_content()->mutable_migration_log();

    migration_tx_log_message->set_cluster_scale_tx_number(
        status_->cluster_scale_txn_);
    migration_tx_log_message->set_stage(
        ::txlog::DataMigrateTxLogMessage_Stage::
            DataMigrateTxLogMessage_Stage_Prepare);
    assert(txm->TxNumber() == migration_txns[0]);
    for (auto txn : migration_txns)
    {
        migration_tx_log_message->add_migration_txns(txn);
    }

    log_rec->clear_node_terms();
}

void DataMigrationOp::FillLastLogRequest(TransactionExecution *txm)
{
    write_last_clean_log_op_.log_type_ = TxLogType::CLEAN;
    write_last_clean_log_op_.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *log_rec =
        write_last_clean_log_op_.log_closure_.LogRequest()
            .mutable_write_log_request();

    log_rec->set_tx_term(txm->tx_term_);
    log_rec->set_txn_number(txm->TxNumber());
    log_rec->set_commit_timestamp(txm->CommitTs());

    ::txlog::DataMigrateTxLogMessage *migration_tx_log_message =
        log_rec->mutable_log_content()->mutable_migration_log();

    migration_tx_log_message->set_cluster_scale_tx_number(
        status_->cluster_scale_txn_);
    migration_tx_log_message->set_stage(
        ::txlog::DataMigrateTxLogMessage_Stage::
            DataMigrateTxLogMessage_Stage_Clean);
}

void DataMigrationOp::FillLogRequest(TransactionExecution *txm,
                                     WriteToLogOp *log_op,
                                     TxLogType log_type,
                                     txlog::BucketMigrateStage migrate_stage)
{
    log_op->log_type_ = log_type;
    log_op->log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *log_rec =
        log_op->log_closure_.LogRequest().mutable_write_log_request();
    log_rec->set_tx_term(txm->tx_term_);
    log_rec->set_txn_number(txm->TxNumber());
    log_rec->set_commit_timestamp(txm->CommitTs());

    ::txlog::DataMigrateTxLogMessage *migration_tx_log_message =
        log_rec->mutable_log_content()->mutable_migration_log();
    migration_tx_log_message->set_cluster_scale_tx_number(
        status_->cluster_scale_txn_);
    migration_tx_log_message->set_stage(
        ::txlog::DataMigrateTxLogMessage_Stage::
            DataMigrateTxLogMessage_Stage_Commit);

    for (size_t i = 0; i < status_->bucket_ids_[migrate_bucket_idx_].size();
         i++)
    {
        migration_tx_log_message->add_bucket_ids(
            status_->bucket_ids_[migrate_bucket_idx_][i]);
    }
    migration_tx_log_message->set_migrate_ts(txm->CommitTs());
    migration_tx_log_message->set_migrate_txn(txm->TxNumber());
    migration_tx_log_message->set_bucket_stage(migrate_stage);
}

void DataMigrationOp::ForceToFinish(TransactionExecution *txm)
{
    write_last_clean_log_op_.hd_result_.SetFinished();
    op_ = &write_last_clean_log_op_;
    Forward(txm);
}

void DataMigrationOp::Clear()
{
    migrate_bucket_idx_ = 0;

    status_ = nullptr;

    ranges_in_bucket_snapshot_.clear();
    hash_partitioned_tables_snapshot_.clear();
}

BatchReadOperation::BatchReadOperation(
    TransactionExecution *txm,
    CcHandlerResult<ReadKeyResult> *lock_range_bucket_result)
    : lock_range_bucket_result_(lock_range_bucket_result)
{
    CcHandlerResult<ReadKeyResult> &hd_res = hd_result_vec_.emplace_back(txm);
    hd_res.post_lambda_ = [this](CcHandlerResult<ReadKeyResult> *res)
    {
#ifdef EXT_TX_PROC_ENABLED
        uint32_t cnt = unfinished_cnt_.fetch_sub(1, std::memory_order_relaxed);
        // This is the last response. Enlists the tx for execution.
        if (cnt == 1)
        {
            res->Txm()->Enlist();
        }
#else
        unfinished_cnt_.fetch_sub(1, std::memory_order_relaxed);
#endif
    };
}

void BatchReadOperation::Reset()
{
    std::vector<ScanBatchTuple> &read_batch = batch_read_tx_req_->read_batch_;
    size_t cnt = read_batch.size();
    lock_range_bucket_result_->Value().Reset();
    lock_range_bucket_result_->Reset();
    lock_index_ = 0;

    if (cnt > range_ids_.size())
    {
        range_ids_.resize(cnt);
    }

    if (cnt > hd_result_vec_.size())
    {
        hd_result_vec_.reserve(cnt);
    }
    else
    {
        while (hd_result_vec_.size() > cnt)
        {
            hd_result_vec_.pop_back();
        }
    }

    unfinished_cnt_.store(cnt, std::memory_order_relaxed);
    local_cache_checked_ = false;

    for (size_t idx = 0; idx < cnt; ++idx)
    {
        if (idx < hd_result_vec_.size())
        {
            hd_result_vec_[idx].Value().Reset();
            hd_result_vec_[idx].Reset();
        }
        else
        {
            CcHandlerResult<ReadKeyResult> &head_hd_result = hd_result_vec_[0];
            CcHandlerResult<ReadKeyResult> &new_hd_result =
                hd_result_vec_.emplace_back(head_hd_result.Txm());
            new_hd_result.post_lambda_ = head_hd_result.post_lambda_;
        }
    }
    op_start_ = metrics::TimePoint::max();
}

void BatchReadOperation::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        assert(lock_range_bucket_result_->IsFinished());
        std::vector<ScanBatchTuple> &read_batch =
            batch_read_tx_req_->read_batch_;

        if (lock_range_bucket_result_->IsError())
        {
            txm->PostProcess(*this);
        }
        else if (!txm->CheckLeaderTerm())
        {
            // If the current node is not the leader of the node group, the
            // range and bucket info returned by the lock-range request
            // should not be accessed. Hence, the lock range result is reset
            // to be errored.
            lock_range_bucket_result_->Reset();
            lock_range_bucket_result_->SetError(
                CcErrorCode::TX_NODE_NOT_LEADER);
            txm->PostProcess(*this);
        }
        else if (lock_index_ < read_batch.size())
        {
            if (!batch_read_tx_req_->tab_name_->IsHashPartitioned())
            {
                // A range has been locked. Assigns the range's node group to
                // all keys belonging to this range.
                const RangeRecord *range_rec = static_cast<RangeRecord *>(
                    lock_range_bucket_result_->Value().rec_);
                TxKey range_end_key = range_rec->GetRangeInfo()->EndTxKey();
                NodeGroupId range_ng =
                    range_rec->GetRangeOwnerNg()->BucketOwner();

                auto cmp = [](const ScanBatchTuple &tuple, const TxKey &end_key)
                { return tuple.key_ < end_key; };

                for (; lock_index_ < read_batch.size() &&
                       cmp(read_batch[lock_index_], range_end_key);
                     ++lock_index_)
                {
                    if (read_batch[lock_index_].status_ !=
                        RecordStatus::Unknown)
                    {
                        continue;
                    }
                    read_batch[lock_index_].cce_addr_.SetNodeGroupId(range_ng);
                    range_ids_[lock_index_] =
                        range_rec->GetRangeInfo()->PartitionId();
                }
            }
            else
            {
                const RangeBucketRecord *bucket_rec =
                    static_cast<RangeBucketRecord *>(
                        lock_range_bucket_result_->Value().rec_);
                NodeGroupId bucket_ng =
                    bucket_rec->GetBucketInfo()->BucketOwner();

                read_batch[lock_index_].cce_addr_.SetNodeGroupId(bucket_ng);
                ++lock_index_;
            }
            txm->Process(*this);
        }
        else
        {
            // Range locks have been acquired. But one or more reads failed.
            // Retry reads.
            txm->Process(*this);
        }
        return;
    }

    if (IsFinished())
    {
        uint32_t err_cnt = 0;
        std::unordered_set<uint32_t> update_leader_set;
        bool out_of_memory_error = false;
        for (auto &hd_result : hd_result_vec_)
        {
            if (hd_result.ErrorCode() == CcErrorCode::PIN_RANGE_SLICE_FAILED ||
                hd_result.ErrorCode() ==
                    CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                hd_result.ErrorCode() == CcErrorCode::DATA_STORE_ERR)
            {
                ++err_cnt;
                if (hd_result.ErrorCode() ==
                    CcErrorCode::REQUESTED_NODE_NOT_LEADER)
                {
                    const CcEntryAddr &cce_addr = hd_result.Value().cce_addr_;
                    auto insert_it =
                        update_leader_set.emplace(cce_addr.NodeGroupId());
                    if (insert_it.second)
                    {
                        Sharder::Instance().UpdateLeader(
                            cce_addr.NodeGroupId());
                    }
                }
                // Failed read requests will be retried. Resets their
                // handler results.
                hd_result.Value().Reset();
                hd_result.Reset();
            }
            else if (hd_result.ErrorCode() == CcErrorCode::OUT_OF_MEMORY)
            {
                ++err_cnt;
                out_of_memory_error = true;
                hd_result.Value().Reset();
                hd_result.Reset();
            }
        }

        if (out_of_memory_error)
        {
            retry_num_++;
            unfinished_cnt_.store(err_cnt, std::memory_order_relaxed);
            ReRunOp(txm);
            return;
        }
        if (err_cnt > 0 && retry_num_ > 0)
        {
            unfinished_cnt_.store(err_cnt, std::memory_order_relaxed);
            ReRunOp(txm);
            return;
        }
        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
    {
        for (CcHandlerResult<ReadKeyResult> &hd_result : hd_result_vec_)
        {
            if (hd_result.IsFinished())
            {
                continue;
            }

            if (!hd_result.Value().is_local_)
            {
                const CcEntryAddr &cce_addr = hd_result.Value().cce_addr_;
                if (cce_addr.Term() > 0 && txm->CheckLeaderTerm())
                {
                    DeadLockCheck::RequestCheck();

                    txm->cc_handler_->BlockCcReqCheck(
                        txm->TxNumber(),
                        txm->TxTerm(),
                        txm->CommandId(),
                        cce_addr,
                        &hd_result,
                        ResultTemplateType::ReadKeyResult);
                }
                else
                {
                    hd_result.ForceError();
                    assert(hd_result.IsFinished());
                }
            }
            else
            {
                // local request timeout
                DeadLockCheck::RequestCheck();
            }
        }

        if (IsFinished())
        {
            // All read requests have finished, after forcing unresponsive
            // remote requests to finish with errors. Advances the command
            // so that the tx is forwarded again on the batch read
            // operation, which determines whether to retry or to finish the
            // operation.
            txm->AdvanceCommand();
        }
        else
        {
            txm->StartTiming();
        }
    }
}

InvalidateTableCacheOp::InvalidateTableCacheOp(TransactionExecution *txm)
    : hd_result_(txm)
{
}

void InvalidateTableCacheOp::Reset(uint32_t hres_ref_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(hres_ref_cnt);
}

void InvalidateTableCacheOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void InvalidateTableCacheOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.IsError() &&
            hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            Sharder::Instance().UpdateLeaders();
        }
        txm->PostProcess(*this);
    }
    else if (hd_result_.LocalRefCnt() == 0)
    {
        if (txm->IsTimeOut())
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "Forward.IsTimeOut",
                txm,
                [txm]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(txm->TxNumber()))
                        .append(",\"term\":")
                        .append(std::to_string(txm->TxTerm()));
                });

            bool force_error = hd_result_.ForceError();
            if (force_error)
            {
                txm->PostProcess(*this);
            }
        }
    }
}

InvalidateTableCacheCompositeOp::InvalidateTableCacheCompositeOp(
    const TableName *table_name, TransactionExecution *txm)
    : CompositeTransactionOperation(),
      table_name_(table_name),
      catalog_key_(*table_name),
      acquire_all_lock_op_(txm),
      invalidate_table_cache_op_(txm),
      post_all_lock_op_(txm)
{
    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.keys_.emplace_back(&catalog_key_);
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;
    invalidate_table_cache_op_.table_name_ = table_name;
    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.keys_.emplace_back(&catalog_key_);
    post_all_lock_op_.recs_.push_back(&catalog_rec_);
    post_all_lock_op_.op_type_ = OperationType::Update;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
}

void InvalidateTableCacheCompositeOp::Reset(const TableName *table_name,
                                            TransactionExecution *txm)
{
    // Reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;
    op_start_ = metrics::TimePoint::max();

    // Reset CompositeTransactionOperation
    op_ = nullptr;

    table_name_ = table_name;
    catalog_key_ = CatalogKey(*table_name);
    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.keys_.clear();
    acquire_all_lock_op_.keys_.emplace_back(&catalog_key_);
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;
    invalidate_table_cache_op_.table_name_ = table_name;
    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.keys_.clear();
    post_all_lock_op_.keys_.emplace_back(&catalog_key_);
    post_all_lock_op_.recs_.clear();
    post_all_lock_op_.recs_.push_back(&catalog_rec_);
    post_all_lock_op_.op_type_ = OperationType::Update;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    acquire_all_lock_op_.ResetHandlerTxm(txm);
    invalidate_table_cache_op_.ResetHandlerTxm(txm);
    post_all_lock_op_.ResetHandlerTxm(txm);
}

void InvalidateTableCacheCompositeOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        ForwardToSubOperation(txm, &acquire_all_lock_op_);
    }
    else if (op_ == &acquire_all_lock_op_)
    {
        if (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Invalidate table cache transaction failed to obtain "
                          "write lock, tx_number:"
                       << txm->TxNumber();
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
            return;
        }

        ForwardToSubOperation(txm, &invalidate_table_cache_op_);
    }
    else if (op_ == &invalidate_table_cache_op_)
    {
        // Release write lock. No value needs to apply.
        txm->commit_ts_ = tx_op_failed_ts_;
        ForwardToSubOperation(txm, &post_all_lock_op_);
    }
    else if (op_ == &post_all_lock_op_)
    {
        if (post_all_lock_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                RetrySubOperation(txm, &post_all_lock_op_);
                return;
            }
            else
            {
                txm->void_resp_->FinishError(
                    TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
            }
        }
        else
        {
            assert(txm->commit_ts_ == tx_op_failed_ts_);
            if (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_relaxed) >
                0)
            {
                for (size_t idx = 0; idx < acquire_all_lock_op_.upload_cnt_;
                     ++idx)
                {
                    CcHandlerResult<AcquireAllResult> &hd_result =
                        acquire_all_lock_op_.hd_results_[idx];
                    if (hd_result.IsError())
                    {
                        txm->void_resp_->FinishError(
                            txm->ConvertCcError(hd_result.ErrorCode()));
                        break;
                    }
                }
                assert(txm->void_resp_->IsError());
            }
            else if (invalidate_table_cache_op_.hd_result_.IsError())
            {
                txm->void_resp_->FinishError(txm->ConvertCcError(
                    invalidate_table_cache_op_.hd_result_.ErrorCode()));
            }
            else
            {
                txm->void_resp_->Finish(void_);
            }
        }

        txm->state_stack_.pop_back();
        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        assert(txm->state_stack_.empty());
        std::unique_lock<std::mutex> lk(shards->invalidate_table_cache_op_mux_);
        shards->invalidate_table_cache_op_pool_.emplace_back(
            std::move(txm->invalidate_table_cache_composite_op_));
    }
}

}  // namespace txservice
