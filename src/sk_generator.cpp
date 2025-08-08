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
#include "sk_generator.h"

#include <mutex>
#include <optional>

#include "error_messages.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_util.h"

namespace txservice
{
void SkGenerator::Reset(const TxKey *start_key,
                        const TxKey *end_key,
                        uint64_t scan_ts,
                        const TableName &base_table_name,
                        NodeGroupId node_group_id,
                        int32_t partition_id,
                        uint64_t tx_number,
                        int64_t tx_term,
                        const std::vector<TableName> &new_indexes_name)
{
    base_table_name_ = &base_table_name;
    task_status_ = nullptr;
    start_key_ = start_key;
    end_key_ = end_key;
    is_key_str_ = false;
    scan_ts_ = scan_ts;
    new_indexes_name_ = &new_indexes_name;
    partition_id_ = partition_id;
    if (node_group_id_ != node_group_id || tx_number_ != tx_number ||
        tx_term_ != tx_term)
    {
        node_group_id_ = node_group_id;
        tx_number_ = tx_number;
        tx_term_ = tx_term;
        sk_encoder_vec_.clear();
        sk_encoder_vec_.reserve(new_indexes_name_->size());
    }
    else
    {
        for (auto &encoder : sk_encoder_vec_)
        {
            encoder->Reset();
        }
    }
    upload_index_ctx_.Reset(node_group_id_);
    scan_batch_size_ = LocalCcShards::DATA_SYNC_SCAN_BATCH_SIZE;
    task_result_ = CcErrorCode::NO_ERROR;
    pack_sk_err_.Reset();
    scanned_items_count_ = 0;
    table_schema_ = nullptr;
}

void SkGenerator::Reset(const std::string &start_key_str,
                        const std::string &end_key_str,
                        uint64_t scan_ts,
                        const TableName &base_table_name,
                        NodeGroupId node_group_id,
                        int32_t partition_id,
                        uint64_t tx_number,
                        int64_t tx_term,
                        const std::vector<TableName> &new_indexes_name)
{
    base_table_name_ = &base_table_name;
    task_status_ = nullptr;
    start_key_str_ = &start_key_str;
    end_key_str_ = &end_key_str;
    is_key_str_ = true;
    scan_ts_ = scan_ts;
    new_indexes_name_ = &new_indexes_name;
    partition_id_ = partition_id;
    if (node_group_id_ != node_group_id || tx_number_ != tx_number ||
        tx_term_ != tx_term)
    {
        node_group_id_ = node_group_id;
        tx_number_ = tx_number;
        tx_term_ = tx_term;
        sk_encoder_vec_.clear();
        sk_encoder_vec_.reserve(new_indexes_name_->size());
    }
    else
    {
        for (auto &encoder : sk_encoder_vec_)
        {
            encoder->Reset();
        }
    }
    upload_index_ctx_.Reset(node_group_id_);
    scan_batch_size_ = LocalCcShards::DATA_SYNC_SCAN_BATCH_SIZE;
    task_result_ = CcErrorCode::NO_ERROR;
    pack_sk_err_.Reset();
    scanned_items_count_ = 0;
    table_schema_ = nullptr;
}

void SkGenerator::ProcessTask()
{
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    // Check the task status
    task_status_ = cc_shards->GetGenerateSkStatus(
        node_group_id_, tx_number_, partition_id_, tx_term_);
    if (!task_status_->StartGenerateSk(tx_term_))
    {
        // Terminate itself
        LOG(WARNING) << "Terminate this generate index task of ng#"
                     << node_group_id_ << " for partition id: " << partition_id_
                     << " with end key: ,"
                     << " caused by the tx term is expired.";
        task_result_ = CcErrorCode::TX_NODE_NOT_LEADER;
        return;
    }

    int64_t ng_term = Sharder::Instance().TryPinNodeGroupData(node_group_id_);
    if (ng_term < 0)
    {
        LOG(WARNING) << "ProcessTask: Generate index on non-leader "
                     << "node for partition: " << partition_id_ << " of ng#"
                     << node_group_id_ << ", terminate directly.";
        task_result_ = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        return;
    }
    // Unpin the node group data immediately.
    Sharder::Instance().UnpinNodeGroupData(node_group_id_);

    // guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [this](void *)
        {
            upload_index_ctx_.TerminateWorkers();

            auto result = task_status_->TaskStatus();
            task_status_->FinishGenerateSk();
            if (result == GenerateSkStatus::Status::Terminating)
            {
                LOG(ERROR) << "Terminate this generate sk task of ng#"
                           << node_group_id_
                           << " for partition id: " << partition_id_
                           << "  caused by TX_NODE_NOT_LEADER";
                task_result_ = CcErrorCode::TX_NODE_NOT_LEADER;
                return;
            }
        });

    LOG(INFO) << "ProcessTask: Generate index on range#" << partition_id_
              << " for base table: " << base_table_name_->Trace() << " of ng#"
              << node_group_id_;

    CODE_FAULT_INJECTOR("term_AlterTableIndex_RangeBoundaryMismatch", {
        // The range have changed, return error.
        DLOG(ERROR) << "The boundary of range#" << partition_id_
                    << " has changed for table: " << base_table_name_->Trace()
                    << " of ng#" << node_group_id_ << ". Terminated this task.";
        task_result_ = CcErrorCode::GET_RANGE_ID_ERR;
        FaultInject::Instance().InjectFault(
            "term_AlterTableIndex_RangeBoundaryMismatch", "remove");
        return;
    });

    const TableName &range_table_name =
        TableName(base_table_name_->StringView(),
                  TableType::RangePartition,
                  base_table_name_->Engine());
    TransactionExecution *acq_range_lock_txm =
        txservice::NewTxInit(cc_shards->GetTxService(),
                             IsolationLevel::RepeatableRead,
                             CcProtocol::Locking,
                             node_group_id_);

    if (acq_range_lock_txm == nullptr)
    {
        LOG(ERROR) << "ProcessTask: Node not leader of ng#" << node_group_id_;
        task_result_ = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        return;
    }

    // Acquire the range read lock
    ReadTxRequest read_range_req;
    RangeRecord range_rec;
    TxKey range_start_key;
    if (is_key_str_)
    {
        if (start_key_str_->size() == 0)
        {
            range_start_key = cc_shards->GetCatalogFactory()->NegativeInfKey();
            read_range_req.Set(&range_table_name,
                               0,
                               &range_start_key,
                               &range_rec,
                               false,
                               false,
                               true);
        }
        else
        {
            read_range_req.Set(&range_table_name,
                               0,
                               start_key_str_,
                               &range_rec,
                               false,
                               false,
                               true);
        }
    }
    else
    {
        read_range_req.Set(
            &range_table_name, 0, start_key_, &range_rec, false, false, true);
    }
    read_range_req.Reset();
    acq_range_lock_txm->Execute(&read_range_req);
    read_range_req.Wait();
    if (read_range_req.IsError())
    {
        // This read operation might fail if it's blocked by a write
        // lock acquired by range split.
        LOG(ERROR) << "ProcessTask: Acquire range#" << partition_id_
                   << " read lock failed for table: "
                   << base_table_name_->Trace() << " of ng#" << node_group_id_
                   << ", with error: " << read_range_req.ErrorMsg();
        task_result_ = CcErrorCode::GET_RANGE_ID_ERR;
        return;
    }

    // Check the range boundary.
    range_start_key = range_rec.GetRangeInfo()->StartTxKey();
    TxKey range_end_key = range_rec.GetRangeInfo()->EndTxKey();

    bool range_doundary_mismatch = false;
    if (!is_key_str_)
    {
        range_doundary_mismatch = !(range_end_key == *end_key_);
    }
    else
    {
        std::string serialized_end_key;
        if (range_end_key.Type() == KeyType::Normal)
        {
            range_end_key.Serialize(serialized_end_key);
        }
        range_doundary_mismatch =
            (serialized_end_key.length() != end_key_str_->length() ||
             serialized_end_key.compare(*end_key_str_));
    }
    if (range_doundary_mismatch)
    {
        // The range have changed, return error.
        LOG(ERROR) << "ProcessTask: The boundary of range#" << partition_id_
                   << " has changed for table: " << base_table_name_->Trace()
                   << " of ng#" << node_group_id_
                   << ". Terminated this range task.";
        task_result_ = CcErrorCode::GET_RANGE_ID_ERR;
        // Release the range read locks.
        txservice::CommitTx(acq_range_lock_txm);
        return;
    }

    ScanAndEncodeIndex(&range_start_key,
                       &range_end_key,
                       ng_term,
                       acq_range_lock_txm->TxNumber());

    // Release the range read locks.
    txservice::CommitTx(acq_range_lock_txm);

    upload_index_ctx_.WaitUntilUploadFinished();

    defer_unpin.reset();

    LOG(INFO) << "ProcessTask: Finished generate index from range#"
              << partition_id_
              << " for base table: " << base_table_name_->Trace() << " of ng#"
              << node_group_id_
              << " with result: " << CcErrorMessage(task_result_);
}

void SkGenerator::ScanAndEncodeIndex(const TxKey *start_key,
                                     const TxKey *end_key,
                                     int64_t ng_term,
                                     uint64_t tx_number)
{
    assert(new_indexes_name_->size() > 0);
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    table_schema_ = cc_shards->GetSharedDirtyTableSchema(
        new_indexes_name_->front(), node_group_id_);
    if (table_schema_ == nullptr)
    {
        // The dirty schema is not available, thare are two situations:
        // 1) The node is no longer the leader of the node group. 2) The catalog
        // already been committed.
        LOG(ERROR)
            << "ScanAndEncodeIndex: Get table dirty schema failed for table: "
            << base_table_name_->StringView() << " of ng#" << node_group_id_;
        task_result_ = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        return;
    }
    size_t core_cnt = cc_shards->Count();

    RangePartitionDataSyncScanCc scan_req(*base_table_name_,
                            scan_ts_,
                            node_group_id_,
                            ng_term,
                            core_cnt,
                            scan_batch_size_,
                            tx_number,
                            start_key,
                            end_key,
                            false,
                            true,
                            true
    );

    CcErrorCode scan_res = CcErrorCode::NO_ERROR;
    bool scan_data_drained = false;
    bool scan_pk_finished = false;
    std::vector<TxKey> last_finished_pos;
    last_finished_pos.reserve(core_cnt);
    for (size_t i = 0; i < core_cnt; ++i)
    {
        last_finished_pos.emplace_back(start_key->Clone());
    }

    TxKey target_key;
    const TxRecord *target_rec = nullptr;
    uint64_t version_ts = 0;
    UploadIndexContext::TableIndexSet new_index_set;
    bool Upload_worker_started = false;
    size_t reserve_size = core_cnt * scan_batch_size_;
    size_t batch_tuples = 0;

    do
    {
        batch_tuples = 0;
        for (size_t idx = 0; idx < core_cnt; ++idx)
        {
            cc_shards->EnqueueToCcShard(idx, &scan_req);
        }
        scan_req.Wait();

        if (scan_req.IsError())
        {
            scan_res = scan_req.ErrorCode();
            LOG(ERROR) << "ScanAndEncodeIndex: Scan pk items failed on range#"
                       << partition_id_
                       << " for base table: " << base_table_name_->StringView()
                       << " of ng#" << node_group_id_
                       << " with error: " << CcErrorMessage(scan_res);
            if (scan_res == CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                scan_res == CcErrorCode::NG_TERM_CHANGED)
            {
                task_result_ = scan_res;
                break;
            }
            else if (scan_res == CcErrorCode::OUT_OF_MEMORY ||
                     scan_res == CcErrorCode::DATA_STORE_ERR)
            {
                std::this_thread::sleep_for(std::chrono::seconds(30));
                // Reset the paused key.
                for (size_t i = 0; i < core_cnt; ++i)
                {
                    const TxKey &paused_key = scan_req.PausePos(i).first;
                    if (!scan_req.IsDrained(i))
                    {
                        // Should use one copy of the key, instead of move the
                        // ownership of the key, because this round of scan may
                        // failed again.
                        assert(paused_key.IsOwner());
                        paused_key.Copy(last_finished_pos[i]);
                    }
                }
                scan_req.Reset();
                scan_pk_finished = false;
                scan_res = CcErrorCode::NO_ERROR;
                continue;
            }
            else
            {
                assert(false && "Unknown scan error.");
                task_result_ = scan_res;
                return;
            }
        }

        scan_data_drained = true;

        for (auto tbl_name_it = new_indexes_name_->cbegin();
             tbl_name_it != new_indexes_name_->cend();
             ++tbl_name_it)
        {
            std::vector<WriteEntry> &index_set =
                new_index_set
                    .emplace(std::piecewise_construct,
                             std::forward_as_tuple(tbl_name_it->StringView(),
                                                   tbl_name_it->Type(),
                                                   tbl_name_it->Engine()),
                             std::forward_as_tuple())
                    .first->second;
            index_set.reserve(reserve_size);

            size_t vec_idx = tbl_name_it - new_indexes_name_->cbegin();
            SkEncoder *sk_encoder = nullptr;
            if (vec_idx >= sk_encoder_vec_.size())
            {
                sk_encoder_vec_.emplace_back(
                    table_schema_->CreateSkEncoder(*tbl_name_it));
            }
            sk_encoder = sk_encoder_vec_[vec_idx].get();

            for (size_t core_idx = 0; core_idx < core_cnt; ++core_idx)
            {
                for (size_t key_idx = 0;
                     key_idx < scan_req.accumulated_scan_cnt_.at(core_idx);
                     ++key_idx)
                {
                    auto &tuple = scan_req.DataSyncVec(core_idx).at(key_idx);
                    target_key = tuple.Key();
                    target_rec = tuple.Payload();
                    version_ts = tuple.commit_ts_;
                    if (tuple.payload_status_ == RecordStatus::Deleted)
                    {
                        // Skip the deleted record.
                        continue;
                    }
                    assert(target_key.KeyPtr() != nullptr &&
                           target_rec != nullptr);

                    int32_t appended_sk_size = sk_encoder->AppendPackedSk(
                        &target_key, target_rec, version_ts, index_set);
                    if (appended_sk_size < 0)
                    {
                        LOG(ERROR)
                            << "ScanAndEncodeIndex: Failed to encode "
                            << "key for index: " << tbl_name_it->StringView()
                            << "of ng#" << node_group_id_;
                        // Finish the pack sk operation
                        task_result_ = CcErrorCode::PACK_SK_ERR;
                        pack_sk_err_ = std::move(sk_encoder->GetError());
                        scan_req.UnpinSlices();
                        return;
                    }
                } /* End of each key */

                if (tbl_name_it == new_indexes_name_->cbegin())
                {
                    batch_tuples += scan_req.accumulated_scan_cnt_.at(core_idx);
                    if (batch_tuples % 10240 == 0 &&
                        !task_status_->CheckTxTermStatus())
                    {
                        LOG(WARNING)
                            << "ScanAndEncodeIndex: Terminate this task cause "
                            << "the tx leader transferred of ng#"
                            << node_group_id_;
                        task_status_->TerminateGenerateSk();
                        task_result_ = CcErrorCode::TX_NODE_NOT_LEADER;
                        scan_req.UnpinSlices();
                        return;
                    }
                    // Update the last finished key.
                    auto &paused_key = scan_req.PausePos(core_idx).first;
                    if (!scan_req.IsDrained(core_idx))
                    {
                        if (last_finished_pos[core_idx].IsOwner())
                        {
                            last_finished_pos[core_idx].Copy(paused_key);
                        }
                        else
                        {
                            last_finished_pos[core_idx] = paused_key.Clone();
                        }
                    }
                    // If the data is drained
                    scan_data_drained =
                        scan_req.IsDrained(core_idx) && scan_data_drained;
                }
            } /* End of each core */
        } /* End of foreach new_indexes_name */

        scan_pk_finished = scan_data_drained;
        scan_req.Reset();
        scanned_items_count_ += batch_tuples;
        if (batch_tuples > 0)
        {
            if (!Upload_worker_started)
            {
                upload_index_ctx_.InitUploadWorkers();
                Upload_worker_started = true;
            }
            upload_index_ctx_.EnqueueNewIndexes(std::move(new_index_set));
        }

        scan_pk_finished =
            scan_pk_finished || !upload_index_ctx_.UploadSucceed();
        if (scan_pk_finished && !scan_data_drained)
        {
            auto upload_res = upload_index_ctx_.UploadResult();
            assert(upload_res == CcErrorCode::TX_NODE_NOT_LEADER ||
                   upload_res == CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            LOG(ERROR) << "ScanAndEncodeIndex: Terminate scan on range#"
                       << partition_id_
                       << " for table: " << base_table_name_->Trace()
                       << "of ng#" << node_group_id_
                       << " caused by upload task failed."
                       << static_cast<uint32_t>(upload_res);
            scan_req.UnpinSlices();
            task_result_ = upload_res;
        }
    } while (!scan_pk_finished);

    DLOG(INFO) << "ScanAndEncodeIndex: Finish scan and encode index on range#"
               << partition_id_ << " for table:" << base_table_name_->Trace()
               << " of ng#" << node_group_id_
               << " with pk items: " << scanned_items_count_;
}

void UploadIndexContext::Reset(NodeGroupId ng_id)
{
    node_group_id_ = ng_id;
    worker_thds_.clear();
    free_head_ = 0;
    pending_head_ = 0;
    pending_task_cnt_ = 0;
    ongoing_task_cnt_ = 0;
    status_ = WorkerStatus::Active;
    upload_batch_size_ = 128;
    upload_result_ = CcErrorCode::NO_ERROR;
    leader_terms_.clear();
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    leader_terms_.resize(ng_cnt, INIT_TERM);
}

void UploadIndexContext::InitUploadWorkers()
{
    if (worker_thds_.size() == 0)
    {
        for (size_t i = 0; i < UploadIndexWorkerSize; ++i)
        {
            worker_thds_.push_back(
                std::thread([this]() { UploadIndexWorker(); }));
        }
    }
}

void UploadIndexContext::TerminateWorkers()
{
    {
        std::unique_lock<std::mutex> lk(mux_);
        assert(status_ == WorkerStatus::Active);
        status_ = WorkerStatus::Terminated;
        consumer_cv_.notify_all();
    }

    // loop over worker threads and join them
    for (size_t i = 0; i < worker_thds_.size(); i++)
    {
        worker_thds_[i].join();
    }
}

void UploadIndexContext::WaitUntilUploadFinished()
{
    std::unique_lock<std::mutex> lk(mux_);
    // Wait until no ongoing task.
    producer_cv_.wait(
        lk,
        [this]()
        {
            return (ongoing_task_cnt_ == 0 && pending_task_cnt_ == 0) ||
                   upload_result_ != CcErrorCode::NO_ERROR;
        });
}

void UploadIndexContext::EnqueueNewIndexes(TableIndexSet &&new_indexes)
{
    std::unique_lock<std::mutex> lk(mux_);
    if ((pending_task_cnt_ + ongoing_task_cnt_) == UploadIndexWorkerSize)
    {
        // Wait until get free task slot.
        producer_cv_.wait(lk,
                          [this]() {
                              return (pending_task_cnt_ + ongoing_task_cnt_) <
                                     UploadIndexWorkerSize;
                          });
    }

    auto task_status = task_pool_.at(free_head_).task_status_;
    while (task_status != UploadTaskStatus::Free)
    {
        free_head_ = (free_head_ + 1) % UploadIndexWorkerSize;
        task_status = task_pool_.at(free_head_).task_status_;
    }

    auto &free_task = task_pool_.at(free_head_);
    free_task.table_index_set_ = std::move(new_indexes);
    free_task.task_status_ = UploadTaskStatus::Pending;
    ++pending_task_cnt_;
    free_head_ = (free_head_ + 1) % UploadIndexWorkerSize;
    consumer_cv_.notify_one();
}

void UploadIndexContext::RecycleUploadTask(UploadIndexTask &task,
                                           CcErrorCode task_res)
{
    std::unique_lock<std::mutex> lk(mux_);
    task.task_status_ = UploadTaskStatus::Free;
    --ongoing_task_cnt_;
    upload_result_ =
        (upload_result_ == CcErrorCode::NO_ERROR ? task_res : upload_result_);
    producer_cv_.notify_one();
}

CcErrorCode UploadIndexContext::UploadEncodedIndex(UploadIndexTask &upload_task)
{
    // The relationship between one WriteEntry and another, this indicate that
    // for the specific node group, which TxKeys belong to it.
    std::unordered_map<TableName, NGIndexSet> ng_index_set;
    CcErrorCode res = CcErrorCode::NO_ERROR;
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    TransactionExecution *acq_range_lock_txm =
        txservice::NewTxInit(cc_shards->GetTxService(),
                             IsolationLevel::RepeatableRead,
                             CcProtocol::Locking,
                             node_group_id_);

    if (acq_range_lock_txm == nullptr)
    {
        LOG(ERROR) << "UploadEncodedIndex: Transaction node not leader of ng#"
                   << node_group_id_;
        return CcErrorCode::REQUESTED_NODE_NOT_LEADER;
    }

    res = AcquireRangeReadLocks(acq_range_lock_txm, upload_task, ng_index_set);
    if (res != CcErrorCode::NO_ERROR)
    {
        return res;
    }
    DLOG(INFO) << "UploadEncodedIndex: Upload encoded indexes of ng#"
               << node_group_id_
               << " with txn: " << acq_range_lock_txm->TxNumber();

    res = UploadIndexInternal(ng_index_set);

    ReleaseRangeReadLocks(acq_range_lock_txm, true);

    DLOG(INFO) << "UploadEncodedIndex: Finished of ng#" << node_group_id_
               << " with result: " << CcErrorMessage(res);
    return res;
}

CcErrorCode UploadIndexContext::UploadIndexInternal(
    std::unordered_map<TableName, NGIndexSet> &ng_index_set)
{
    size_t entry_vec_size = 0;
    size_t batch_req_cnt = 0;
    bthread::Mutex req_mux;
    bthread::ConditionVariable req_cv;
    size_t finished_upload_count = 0;
    CcErrorCode upload_res_code = CcErrorCode::NO_ERROR;
    size_t upload_req_count = 0;
    for (auto &[table_name, ng_entries] : ng_index_set)
    {
        for (auto &[ng_id, entry_vec] : ng_entries)
        {
            entry_vec_size = entry_vec.size();
            batch_req_cnt = (entry_vec_size / upload_batch_size_ +
                             (entry_vec_size % upload_batch_size_ ? 1 : 0));

            int64_t &expected_term = leader_terms_.at(ng_id);

            size_t start_idx = 0;
            size_t end_idx =
                (batch_req_cnt > 1 ? upload_batch_size_ : entry_vec_size);
            for (size_t idx = 0; idx < batch_req_cnt; ++idx)
            {
                SendIndexes(table_name,
                            ng_id,
                            expected_term,
                            entry_vec,
                            (end_idx - start_idx),
                            start_idx,
                            req_mux,
                            req_cv,
                            finished_upload_count,
                            upload_res_code);
                ++upload_req_count;
                // Next batch
                start_idx = end_idx;
                end_idx = ((start_idx + upload_batch_size_) > entry_vec_size
                               ? entry_vec_size
                               : (start_idx + upload_batch_size_));
            }
        }
    }

    {
        std::unique_lock<bthread::Mutex> req_lk(req_mux);
        while (upload_req_count != finished_upload_count)
        {
            req_cv.wait(req_lk);
        }
    }

    return upload_res_code;
}

void UploadIndexContext::SendIndexes(
    const TableName &table_name,
    NodeGroupId dest_ng_id,
    int64_t &ng_term,
    const std::vector<WriteEntry *> &write_entry_vec,
    size_t batch_size,
    size_t start_key_idx,
    bthread::Mutex &req_mux,
    bthread::ConditionVariable &req_cv,
    size_t &finished_req_cnt,
    CcErrorCode &res_code)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(dest_ng_id);
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();
    if (dest_node_id == cc_shards->NodeId())
    {
        UploadBatchCc *req_ptr = NextRequest();
        req_ptr->Reset(table_name,
                       dest_ng_id,
                       ng_term,
                       core_cnt,
                       batch_size,
                       start_key_idx,
                       write_entry_vec,
                       req_mux,
                       req_cv,
                       finished_req_cnt,
                       res_code,
                       UploadBatchType::SkIndexData);

        for (size_t core = 0; core < core_cnt; ++core)
        {
            cc_shards->EnqueueToCcShard(core, req_ptr);
        }
    }
    else
    {
        // remote node
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);
        if (channel == nullptr)
        {
            // Fail to establish the channel to the tx node. Do not update the
            // leader term of input node group.
            LOG(ERROR) << "SendIndexes: Failed to init the channel of ng#"
                       << dest_ng_id;
            std::unique_lock<bthread::Mutex> req_lk(req_mux);
            res_code = CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED;
            ++finished_req_cnt;
            req_cv.notify_one();
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());

        UploadBatchClosure *upload_batch_closure = new UploadBatchClosure(
            [dest_ng_id,
             &res_code,
             &finished_req_cnt,
             &req_mux,
             &req_cv,
             &ng_term](CcErrorCode res, int32_t dest_term)
            {
                std::unique_lock<bthread::Mutex> req_lk(req_mux);
                res_code = res;
                if (res == CcErrorCode::NO_ERROR)
                {
                    if (ng_term == INIT_TERM)
                    {
                        ng_term = dest_term;
                    }
                    else if (ng_term != dest_term)
                    {
                        LOG(ERROR)
                            << "Response for upload batch failed of ng#"
                            << dest_ng_id
                            << " of term mismatch, with expected term: "
                            << ng_term << " and actual term: " << dest_term;
                        res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                    }
                    else
                    {
                        assert(ng_term == dest_term);
                    }
                }
                else
                {
                    LOG(ERROR)
                        << "Response for upload batch failed of ng#"
                        << dest_ng_id << ", with error: " << (uint32_t) res;
                }
                ++finished_req_cnt;
                req_cv.notify_one();
            },
            UploadTimeout,
            true);
        upload_batch_closure->SetChannel(dest_node_id, channel);

        remote::UploadBatchRequest *req_ptr =
            upload_batch_closure->UploadBatchRequest();
        req_ptr->set_node_group_id(dest_ng_id);
        req_ptr->set_node_group_term(ng_term);
        req_ptr->set_table_name_str(table_name.String());
        req_ptr->set_table_type(
            remote::ToRemoteType::ConvertTableType(table_name.Type()));
        req_ptr->set_table_engine(
            remote::ToRemoteType::ConvertTableEngine(table_name.Engine()));
        size_t end_key_idx = start_key_idx + batch_size;
        req_ptr->set_kind(remote::UploadBatchKind::SK_DATA);
        req_ptr->set_batch_size(batch_size);
        // keys
        req_ptr->clear_keys();
        std::string *keys_str = req_ptr->mutable_keys();
        // records
        req_ptr->clear_records();
        std::string *recs_str = req_ptr->mutable_records();
        // commit_ts
        req_ptr->clear_commit_ts();
        std::string *commit_ts_str = req_ptr->mutable_commit_ts();
        size_t len_sizeof = sizeof(uint64_t);
        const char *val_ptr = nullptr;
        // rec_status
        req_ptr->clear_rec_status();
        std::string *rec_status_str = req_ptr->mutable_rec_status();
        // All generated sk should be normal status.
        const RecordStatus rec_status = RecordStatus::Normal;
        for (size_t idx = start_key_idx; idx < end_key_idx; ++idx)
        {
            write_entry_vec.at(idx)->key_.Serialize(*keys_str);
            write_entry_vec.at(idx)->rec_->Serialize(*recs_str);
            val_ptr = reinterpret_cast<const char *>(
                &(write_entry_vec.at(idx)->commit_ts_));
            commit_ts_str->append(val_ptr, len_sizeof);
            rec_status_str->append(reinterpret_cast<const char *>(&rec_status),
                                   sizeof(rec_status));
        }

        brpc::Controller *cntl_ptr = upload_batch_closure->Controller();
        cntl_ptr->set_timeout_ms(UploadTimeout);
        remote::UploadBatchResponse *resp_ptr =
            upload_batch_closure->UploadBatchResponse();
        // Asynchronous mode
        stub.UploadBatch(cntl_ptr, req_ptr, resp_ptr, upload_batch_closure);
        DLOG(INFO) << "UploadBatch service of ng#" << dest_ng_id;
    }
}

CcErrorCode UploadIndexContext::AcquireRangeReadLocks(
    TransactionExecution *acq_lock_txm,
    UploadIndexTask &upload_task,
    std::unordered_map<TableName, NGIndexSet> &ng_index_set)
{
    for (auto table_it = upload_task.table_index_set_.begin();
         table_it != upload_task.table_index_set_.end();
         ++table_it)
    {
        const TableName &range_table_name =
            TableName(table_it->first.StringView(),
                      TableType::RangePartition,
                      table_it->first.Engine());

        auto &table_write_entrys = table_it->second;
        auto [it, inserted] = ng_index_set.try_emplace(table_it->first);
        auto &ng_table_write_entrys = it->second;
        if (!inserted)
        {
            ng_table_write_entrys.clear();
        }

        const TxKey *write_key = nullptr;
        for (auto write_entry_it = table_write_entrys.begin();
             write_entry_it != table_write_entrys.end();)
        {
            write_key = &write_entry_it->key_;

            RangeRecord range_rec;
            ReadTxRequest read_range_req(&range_table_name,
                                         0,
                                         write_key,
                                         &range_rec,
                                         false,
                                         false,
                                         true);
            acq_lock_txm->Execute(&read_range_req);
            read_range_req.Wait();
            TxErrorCode tx_res = read_range_req.ErrorCode();
            if (tx_res != TxErrorCode::NO_ERROR)
            {
                ReleaseRangeReadLocks(acq_lock_txm, false);

                LOG(ERROR) << "!!!ERROR!!! Read range info failed finally with "
                           << "error message: " << read_range_req.ErrorMsg()
                           << ", for table: " << range_table_name.Trace();
                if (tx_res == TxErrorCode::CC_REQ_FOLLOWER)
                {
                    return CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                }
                else
                {
                    return CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT;
                }
            }

            AdvanceWriteEntryForRangeInfo(range_rec,
                                          write_entry_it,
                                          table_write_entrys.end(),
                                          ng_table_write_entrys);

        } /* End of table write entrys */
    } /* End of tables */
    return CcErrorCode::NO_ERROR;
}

/**
 * @brief
 * @param is_success false is acquire range read lock failed.
 */
void UploadIndexContext::ReleaseRangeReadLocks(
    TransactionExecution *acq_lock_txm, bool is_success)
{
    CommitTxRequest commit_req;
    commit_req.to_commit_ = is_success;
    acq_lock_txm->CommitTx(commit_req);
}

void UploadIndexContext::AdvanceWriteEntryForRangeInfo(
    const RangeRecord &range_record,
    std::vector<WriteEntry>::iterator &cur_write_entry_it,
    const std::vector<WriteEntry>::iterator &write_entry_end,
    NGIndexSet &ng_write_entrys)
{
    // Advances the write entry iterator such that it points to the first key
    // belonging to the next range.
    TxKey range_end_key = range_record.GetRangeInfo()->EndTxKey();
    auto next_range_start = cur_write_entry_it;
    if (range_end_key.Type() == KeyType::PositiveInf)
    {
        next_range_start = write_entry_end;
    }
    else
    {
        next_range_start = std::lower_bound(
            cur_write_entry_it,
            write_entry_end,
            range_end_key,
            [](const WriteEntry &a, const TxKey &val) { return a.key_ < val; });
    }

    NodeGroupId range_ng = range_record.GetRangeOwnerNg()->BucketOwner();
    NodeGroupId new_bucket_ng =
        range_record.GetRangeOwnerNg()->DirtyBucketOwner();

    const std::vector<const BucketInfo *> *splitting_range_ngs =
        range_record.GetNewRangeOwnerNgs();

    // Updates the sharding codes of the write-entry keys belonging to this
    // range. The higher 22 bits represent the range ID.
    NodeGroupId new_range_ng = UINT32_MAX;
    NodeGroupId new_range_new_bucket_ng = UINT32_MAX;
    size_t new_range_idx = 0;

    auto *range_info = range_record.GetRangeInfo();
    while (cur_write_entry_it != next_range_start)
    {
        WriteEntry &write_entry = *cur_write_entry_it;
        auto ng_it = ng_write_entrys.try_emplace(range_ng);
        ng_it.first->second.push_back(&write_entry);

        // If current range is migrating, forward to new range owner.
        if (new_bucket_ng != UINT32_MAX)
        {
            ng_write_entrys.try_emplace(new_bucket_ng)
                .first->second.push_back(&write_entry);
        }

        // If range is splitting and the key will fall on a new range after
        // split is finished, register forward_addr_ to indicate
        // entry needs to be double written.
        while (range_info->IsDirty() &&
               new_range_idx < range_info->NewKey()->size() &&
               !(write_entry.key_ < range_info->NewKey()->at(new_range_idx)))
        {
            new_range_ng =
                splitting_range_ngs->at(new_range_idx)->BucketOwner();
            new_range_new_bucket_ng =
                splitting_range_ngs->at(new_range_idx++)->DirtyBucketOwner();
        }
        if (new_range_ng != UINT32_MAX)
        {
            if (new_range_ng != range_ng)
            {
                ng_write_entrys.try_emplace(new_range_ng)
                    .first->second.push_back(&write_entry);
            }
            // If the new range is migrating, forward to the new owner of new
            // range.
            if (new_range_new_bucket_ng != UINT32_MAX &&
                new_range_new_bucket_ng != range_ng)
            {
                ng_write_entrys.try_emplace(new_range_new_bucket_ng)
                    .first->second.push_back(&write_entry);
            }
        }

        ++cur_write_entry_it;
    }
}

UploadBatchCc *UploadIndexContext::NextRequest()
{
    std::unique_lock<std::mutex> lk(mux_);
    return upload_req_pool_.NextRequest();
}

void UploadIndexContext::UploadIndexWorker()
{
    std::unique_lock<std::mutex> lk(mux_);
    while (status_ == WorkerStatus::Active)
    {
        consumer_cv_.wait(lk,
                          [this]() {
                              return pending_task_cnt_ > 0 ||
                                     status_ != WorkerStatus::Active;
                          });

        if (!pending_task_cnt_)
        {
            continue;
        }
        else if (upload_result_ != CcErrorCode::NO_ERROR)
        {
            producer_cv_.notify_one();
            break;
        }

        auto task_status = task_pool_.at(pending_head_).task_status_;
        while (task_status != UploadTaskStatus::Pending)
        {
            pending_head_ = (pending_head_ + 1) % UploadIndexWorkerSize;
            task_status = task_pool_.at(pending_head_).task_status_;
        }
        auto &upload_task = task_pool_.at(pending_head_);
        assert(upload_task.task_status_ == UploadTaskStatus::Pending);
        upload_task.task_status_ = UploadTaskStatus::Ongoing;
        --pending_task_cnt_;
        ++ongoing_task_cnt_;
        pending_head_ = (pending_head_ + 1) % UploadIndexWorkerSize;
        lk.unlock();

        for (auto table_it = upload_task.table_index_set_.begin();
             table_it != upload_task.table_index_set_.end();
             ++table_it)
        {
            // Sort for each index table.
            std::sort(table_it->second.begin(),
                      table_it->second.end(),
                      [](const WriteEntry &e1, const WriteEntry &e2)
                      { return e1.key_ < e2.key_; });
        }

        CcErrorCode res_code = CcErrorCode::NO_ERROR;
        do
        {
            res_code = UploadEncodedIndex(upload_task);
            if (res_code == CcErrorCode::TX_NODE_NOT_LEADER ||
                res_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                res_code == CcErrorCode::NG_TERM_CHANGED)
            {
                LOG(ERROR)
                    << "Upload this batch sk records failed with error code: "
                    << CcErrorMessage(res_code)
                    << ". Terminate this range task.";
                break;
            }
            else if (res_code == CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED ||
                     res_code == CcErrorCode::REQUEST_LOST)
            {
                LOG(ERROR)
                    << "Upload this batch sk records failed with error code: "
                    << CcErrorMessage(res_code) << ". Retry after 1s.";
                std::this_thread::sleep_for(1s);
                continue;
            }
            else if (res_code == CcErrorCode::OUT_OF_MEMORY ||
                     res_code == CcErrorCode::DATA_STORE_ERR ||
                     res_code == CcErrorCode::PIN_RANGE_SLICE_FAILED ||
                     res_code ==
                         CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT)
            {
                LOG(ERROR)
                    << "Upload this batch sk records failed with error code: "
                    << CcErrorMessage(res_code) << ". Retry after 30s."
                    << " with upload batch size: " << upload_batch_size_;
                std::this_thread::sleep_for(std::chrono::seconds(30));
                continue;
            }
            else
            {
                DLOG(INFO)
                    << "Upload this batch sk records finished with result: "
                    << CcErrorMessage(res_code);
                assert(res_code == CcErrorCode::NO_ERROR);
            }
        } while (res_code != CcErrorCode::NO_ERROR);

        RecycleUploadTask(upload_task, res_code);

        if (res_code != CcErrorCode::NO_ERROR)
        {
            assert(res_code == CcErrorCode::TX_NODE_NOT_LEADER ||
                   res_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                   res_code == CcErrorCode::NG_TERM_CHANGED);
            break;
        }
        if (!lk.owns_lock())
        {
            lk.lock();
        }
    }
    DLOG(INFO) << "Finish upload worker.";
}

}  // namespace txservice
