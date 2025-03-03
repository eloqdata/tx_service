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
#include "tx_index_operation.h"

#include <brpc/channel.h>

#include <algorithm>
#include <memory>

#include "error_messages.h"
#include "local_cc_shards.h"
#include "remote/remote_type.h"
#include "sk_generator.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_trace.h"

namespace txservice
{

UpsertTableIndexOp::UpsertTableIndexOp(
    const std::string_view table_name_sv,
    const std::string &current_image,
    uint64_t curr_schema_ts,
    const std::string &dirty_image,
    const std::string &alter_table_info_image,
    OperationType op_type,
    PackSkError *store_pack_sk_err,
    TransactionExecution *txm)
    : SchemaOp(
          table_name_sv, current_image, dirty_image, curr_schema_ts, op_type),
      lock_cluster_config_op_(),
      acquire_all_intent_op_(txm),
      upgrade_all_intent_to_lock_op_(txm),
      prepare_log_op_(txm),
      downgrade_all_lock_to_intent_op_(txm),
      unlock_cluster_config_op_(txm),
      upsert_kv_table_op_(&table_key_.Name(), op_type, txm),
      generate_sk_parallel_op_(txm),
      flush_all_old_tuples_sk_op_(txm),
      prepare_data_log_op_(txm),
      acquire_all_lock_op_(txm),
      commit_log_op_(txm),
      clean_ccm_op_(txm),
      post_all_lock_op_(txm),
      clean_log_op_(txm),
      read_cluster_result_(txm),
      alter_table_info_image_str_(alter_table_info_image),
      store_pack_sk_err_(store_pack_sk_err)
{
    assert(op_type_ == OperationType::AddIndex ||
           op_type_ == OperationType::DropIndex);

    lock_cluster_config_op_.table_name_ =
        TableName(cluster_config_ccm_name_sv, TableType::ClusterConfig);
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.keys_.emplace_back(&table_key_);
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    upgrade_all_intent_to_lock_op_.table_name_ = &catalog_ccm_name;
    upgrade_all_intent_to_lock_op_.keys_.emplace_back(&table_key_);
    upgrade_all_intent_to_lock_op_.cc_op_ = CcOperation::Write;
    upgrade_all_intent_to_lock_op_.protocol_ = CcProtocol::Locking;

    downgrade_all_lock_to_intent_op_.table_name_ = &catalog_ccm_name;
    downgrade_all_lock_to_intent_op_.keys_.emplace_back(&table_key_);
    downgrade_all_lock_to_intent_op_.recs_.push_back(&catalog_rec_);
    downgrade_all_lock_to_intent_op_.op_type_ = op_type_;
    downgrade_all_lock_to_intent_op_.write_type_ = PostWriteType::PrepareCommit;

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

    alter_table_info_.DeserializeAlteredTableInfo(alter_table_info_image_str_);

    is_force_finished_ = false;

    new_indexes_name_.reserve(alter_table_info_.index_add_count_);
    for (auto index_it = alter_table_info_.index_add_names_.cbegin();
         index_it != alter_table_info_.index_add_names_.cend();
         ++index_it)
    {
        new_indexes_name_.emplace_back(index_it->first.StringView(),
                                       index_it->first.Type());
    }

    TxKey neg_key = Sharder::Instance()
                        .GetLocalCcShards()
                        ->GetCatalogFactory()
                        ->NegativeInfKey();
    last_scanned_end_key_.Release();
    last_scanned_end_key_ = neg_key.GetShallowCopy();
    is_last_scanned_key_str_ = false;
    scanned_pk_range_count_ = 0;
    last_finished_end_key_.Release();
    last_finished_end_key_ = neg_key.GetShallowCopy();
    is_last_finished_key_str_ = false;
    finished_pk_range_count_ = 0;
    total_scanned_pk_items_count_ = 0;
#ifdef NDEBUG
    scan_batch_range_size_ = 10;
#else
    scan_batch_range_size_ = 3;
#endif
    ResetLeaderTerms();

    TX_TRACE_ASSOCIATE(this, &acquire_all_intent_op_, "acquire_all_intent_op_");
    TX_TRACE_ASSOCIATE(this,
                       &upgrade_all_intent_to_lock_op_,
                       "upgrade_all_intent_to_lock_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this,
                       &downgrade_all_lock_to_intent_op_,
                       "downgrade_all_lock_to_intent_op_");
    TX_TRACE_ASSOCIATE(this, &upsert_kv_table_op_, "upsert_kv_table_op_");
    TX_TRACE_ASSOCIATE(
        this, &generate_sk_parallel_op_, "generate_sk_parallel_op_");
    TX_TRACE_ASSOCIATE(
        this, &flush_all_old_tuples_sk_op_, "flush_all_old_tuples_sk_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_data_log_op_, "prepare_data_log_op_");
    TX_TRACE_ASSOCIATE(this, &acquire_all_lock_op_, "acquire_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &clean_ccm_op_, "clean_ccm_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
}

void UpsertTableIndexOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        LOG(INFO) << "Alter Table Index transaction lock cluster config"
                  << " , txn: " << txm->TxNumber();
        op_ = &lock_cluster_config_op_;
        txm->PushOperation(&lock_cluster_config_op_);
        txm->Process(lock_cluster_config_op_);
    }
    else if (op_ == &lock_cluster_config_op_)
    {
        if (lock_cluster_config_op_.hd_result_->IsError())
        {
            LOG(ERROR) << "Alter Table Index read cluster config failed, txn:"
                       << txm->TxNumber();
            if (!prepare_log_op_.hd_result_.IsFinished() &&
                !prepare_data_log_op_.hd_result_.IsFinished())
            {
                txm->commit_ts_ = tx_op_failed_ts_;
            }
            ForceToFinish(txm);
            return;
        }
        if (prepare_log_op_.hd_result_.IsFinished() ||
            prepare_data_log_op_.hd_result_.IsFinished())
        {
            assert(op_type_ == OperationType::AddIndex);
            LOG(INFO) << "Alter Table Index transaction post acquire all"
                      << " write lock, txn: " << txm->TxNumber();
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            // Acquire write intent, and then upgrade to write lock(Locking),
            // rather than acquire write lock(OCC) directly, aim to avoid two
            // situations: (1) concurrent DDL deadlock. (2) concurrent DML cause
            // to always abort this tx.
            LOG(INFO)
                << "Alter Table Index transaction prepare acquire all write"
                << " intent, txn: " << txm->TxNumber();
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
            LOG(ERROR) << "Alter Table Index for table: "
                       << table_key_.Name().Trace()
                       << ", acquire write intent failed, txn: "
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

        // To avoid deadlock. If hold write lock directly, rather than get
        // write intent, and upgrade to write lock, concurrent DDL on the
        // same table may cause to deadlock.
        LOG(INFO) << "Alter Table Index transaction prepare acquire all write"
                  << " lock, txn: " << txm->TxNumber();
        op_ = &upgrade_all_intent_to_lock_op_;
        txm->PushOperation(&upgrade_all_intent_to_lock_op_);
        txm->Process(upgrade_all_intent_to_lock_op_);
    }
    else if (op_ == &upgrade_all_intent_to_lock_op_)
    {
        if (upgrade_all_intent_to_lock_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Alter Table Index for table: "
                       << table_key_.Name().Trace()
                       << ", upgrade write lock failed, txn: "
                       << txm->TxNumber();
            // Set the commit ts to 0 to signal that the following post write
            // operation releases all write intents.
            txm->commit_ts_ = tx_op_failed_ts_;
            // Moves to the last operation that removes all write
            // intents/locks.
            op_ = &post_all_lock_op_;
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
            return;
        }

        // Assigns a commit timestamp to the txm as the version of the new
        // schema. Rules to calculate the commit ts: the new schema's version
        // should be greater than (1) the current version, (2) the maximal
        // commit ts of all tx that have read the schema, (3) the local time
        // when the tx starts.
        txm->commit_ts_ = txm->commit_ts_bound_ + 1;

        for (size_t idx = 0; idx < upgrade_all_intent_to_lock_op_.upload_cnt_;
             ++idx)
        {
            const AcquireAllResult &upgrade_all_res =
                upgrade_all_intent_to_lock_op_.hd_results_[idx].Value();
            uint64_t ts = std::max(upgrade_all_res.commit_ts_ + 1,
                                   upgrade_all_res.last_vali_ts_ + 1);
            txm->commit_ts_ = std::max(txm->commit_ts_, ts);
        }

        LOG(INFO) << "Alter Table Index transaction write prepare log, txn: "
                  << txm->TxNumber()
                  << ". The schema version: " << txm->commit_ts_;
        op_ = &prepare_log_op_;
        FillPrepareLogRequest(txm);
        txm->PushOperation(&prepare_log_op_);
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
                // clear response, either success or failure, or the coordinator
                // itself is no longer leader
                if (txm->CheckLeaderTerm())
                {
                    LOG(WARNING) << "Alter Table Index for table: "
                                 << table_key_.Name().Trace()
                                 << ", write prepare log result unknown, txn: "
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
                    txm->commit_ts_ = tx_op_failed_ts_;
                    ForceToFinish(txm);
                }
            }
            else
            {
                LOG(ERROR) << "Alter Table Index for table: "
                           << table_key_.Name().Trace()
                           << ", write prepare log failed with error message: "
                           << prepare_log_op_.hd_result_.ErrorMsg()
                           << ", txn: " << txm->TxNumber();
                // Fails to flush the prepare log. The schema operation is
                // considered failed if the prepare log is not flushed. The
                // commit ts is set to 0 to signal that the following post write
                // operation releases all write intents.
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
            ACTION_FAULT_INJECTOR("term_AlterTableIndex_PrepareCommitAllWLOp");
            LOG(INFO) << "Alter Table Index transaction install dirty table"
                      << " schema, txn: " << txm->TxNumber();
            op_ = &downgrade_all_lock_to_intent_op_;

            txm->PushOperation(&downgrade_all_lock_to_intent_op_);
            txm->Process(downgrade_all_lock_to_intent_op_);
        }
    }
    else if (op_ == &downgrade_all_lock_to_intent_op_)
    {
        if (downgrade_all_lock_to_intent_op_.hd_result_.IsError())
        {
            // After the prepare log is flushed, the schema op is guaranteed
            // to succeed and can only roll forward. Retry this step to
            // install the dirty schema in the tx service, if the tx node is
            // still the leader. The tx is also allowed to proceed if the tx
            // is in the recovery mode and the tx node is a leader
            // candidate.
            if (txm->CheckLeaderTerm())
            {
                // set catalog_rec_'s binary_value_ to image_str since it
                // could be set to TableSchemaView pointer in localshard.
                catalog_rec_.SetSchemaImage(image_str_);
                catalog_rec_.SetDirtySchemaImage(dirty_image_str_);

                txm->PushOperation(&downgrade_all_lock_to_intent_op_);
                txm->Process(downgrade_all_lock_to_intent_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        // TODO(ysw): For DropIndex, since we already got all WriteLock, so it
        // OK as the order below:
        // WriteLock->Preparelog->InstallDirtySchema(but donot downgrade to
        // WriteIntent)->Commitlog.
        else if (op_type_ == OperationType::DropIndex)
        {
            // For DROP INDEX opertaion, the data store operation of deleting
            // the k-v table happens after the commit log is flushed.
            LOG(INFO) << "Alter Table Index transaction post acquire all"
                      << " write lock, txn: " << txm->TxNumber();
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            // Release cluster config op before doing data store op.
            op_ = &unlock_cluster_config_op_;
            // Get cce addr of cluster config read lock from rset.
            auto &rset = txm->rw_set_.ReadSet();
            auto &cluster_config_rset = rset.at(cluster_config_ccm_name);
            assert(cluster_config_rset.size() == 1);
            for (const auto &[cce_addr, rset_entry] : cluster_config_rset)
            {
                unlock_cluster_config_op_.Reset(&cce_addr, &rset_entry);
            }
            LOG(INFO) << "Alter Table Index transaction release cluster config "
                         "lock, txn: "
                      << txm->TxNumber();
            txm->PushOperation(&unlock_cluster_config_op_);
            txm->Process(unlock_cluster_config_op_);
        }
    }
    else if (op_ == &unlock_cluster_config_op_)
    {
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
                return;
            }
        }
        assert(op_type_ == OperationType::AddIndex);
        LOG(INFO) << "Alter Table Index transaction upsert data store"
                  << " info for base table: " << table_key_.Name().Trace()
                  << ", txn: " << txm->TxNumber();
        uint16_t retry_times = 100;
        CODE_FAULT_INJECTOR("trigger_flush_kv_error", { retry_times = 3; });
        op_ = &upsert_kv_table_op_;
        // The post write request right after flushing the prepare log
        // installs the dirty schema in the tx service and returns a
        // local view (pointer) of the committed and dirty schema.
        upsert_kv_table_op_.table_schema_old_ = catalog_rec_.Schema();
        upsert_kv_table_op_.table_schema_ = catalog_rec_.DirtySchema();
        upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
        upsert_kv_table_op_.write_time_ = txm->commit_ts_;
        txm->PushOperation(&upsert_kv_table_op_, retry_times);
        txm->Process(upsert_kv_table_op_);
    }
    else if (op_ == &upsert_kv_table_op_)
    {
        if (upsert_kv_table_op_.hd_result_.IsError())
        {
            // The data store operation failed. Retries the operation if the
            // tx node is the leader or the tx is in the recovery mode and
            // the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                // NOTE: The logic of this part is consistent with the logic in
                // UpsertTableOp::Forward.
                // Keep retrying if it is DropIndex or rollback AddIndex.
                if (op_type_ == OperationType::DropIndex ||
                    generate_sk_parallel_op_.hd_result_.IsError())
                {
                    txm->PushOperation(&upsert_kv_table_op_);
                    txm->Process(upsert_kv_table_op_);
                }
                else
                {
                    LOG(ERROR) << "Alter Table Index for table: "
                               << table_key_.Name().Trace()
                               << ", Failed to create tables in kv store"
                               << ". Txn: " << txm->TxNumber();

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
        else if (op_type_ == OperationType::DropIndex ||
                 generate_sk_parallel_op_.hd_result_.IsError())
        {
            assert(clean_ccm_op_.table_names_.empty());
            for (const auto &index_drop_name :
                 alter_table_info_.index_drop_names_)
            {
                clean_ccm_op_.table_names_.emplace_back(
                    index_drop_name.first.StringView().data(),
                    index_drop_name.first.StringView().size(),
                    index_drop_name.first.Type());
            }

            if (op_type_ == OperationType::DropIndex)
            {
                clean_ccm_op_.commit_ts_ = txm->CommitTs();
            }
            else
            {
                assert(op_type_ == OperationType::AddIndex &&
                       txm->commit_ts_ == tx_op_failed_ts_);
                clean_ccm_op_.commit_ts_ = upsert_kv_table_op_.write_time_;
            }
            clean_ccm_op_.clean_type_ = CleanType::CleanCcm;

            LOG(INFO) << "Alter Table Index transaction clean cc map, txn: "
                      << txm->TxNumber();
            op_ = &clean_ccm_op_;
            txm->PushOperation(&clean_ccm_op_);
            txm->Process(clean_ccm_op_);
        }
        else
        {
            if (txm->TxStatus() == TxnStatus::Recovering &&
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId()) > 0)
            {
                // If this txm is in the recovering state, should wait until the
                // data log replay finished to avoid data lost.
                return;
            }

            LOG(INFO) << "Alter Table Index transaction start generate sk "
                      << "with the parallelism: "
                      << static_cast<uint32_t>(scan_batch_range_size_)
                      << " of each node group for table: "
                      << table_key_.Name().Trace() << " with the start key: "
                      << ", txn: " << txm->TxNumber();

            if (is_last_finished_key_str_)
            {
                last_scanned_end_key_str_ = last_finished_end_key_str_;
            }
            else
            {
                last_scanned_end_key_ = last_finished_end_key_.GetShallowCopy();
            }
            is_last_scanned_key_str_ = is_last_finished_key_str_;
            ResetLeaderTerms();
            generate_sk_parallel_op_.Reset();
            generate_sk_parallel_op_.hd_result_.Value().Reset();
            generate_sk_parallel_op_.op_func_ = [this, txm]()
            {
                generate_sk_parallel_op_.worker_thread_ = std::thread(
                    [this, txm]()
                    {
                        auto &hd_res = generate_sk_parallel_op_.hd_result_;
                        hd_res.Reset();
#ifdef EXT_TX_PROC_ENABLED
                        hd_res.SetToBlock();
#endif
                        this->DispatchRangeTask(txm, hd_res);
                    });
            };
            op_ = &generate_sk_parallel_op_;
            txm->PushOperation(&generate_sk_parallel_op_);
            txm->Process(generate_sk_parallel_op_);
        }
    }
    else if (op_ == &clean_ccm_op_)
    {
        assert(op_type_ == OperationType::DropIndex ||
               generate_sk_parallel_op_.hd_result_.IsError());

        if (clean_ccm_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                LOG(ERROR) << "Alter Table Index transaction failed to clean "
                              "ccmap, err code: "
                           << (int) clean_ccm_op_.hd_result_.ErrorCode()
                           << ", err msg: "
                           << clean_ccm_op_.hd_result_.ErrorMsg()
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

        // For DROP INDEX, the data store operation happens after all
        // write locks are acquired and commit log is flushed.
        LOG(INFO) << "Alter Table Index transaction commit dirty table"
                  << " schema, txn: " << txm->TxNumber();
        op_ = &post_all_lock_op_;
        txm->PushOperation(&post_all_lock_op_);
        txm->Process(post_all_lock_op_);
    }
    else if (op_ == &generate_sk_parallel_op_)
    {
        if (generate_sk_parallel_op_.hd_result_.IsError())
        {
            LOG(ERROR)
                << "Alter Table Index generate sk parallel failed for table: "
                << table_key_.Name().Trace() << " with error: "
                << generate_sk_parallel_op_.hd_result_.ErrorMsg()
                << ", txn:" << txm->TxNumber();
            if (txm->CheckLeaderTerm())
            {
                CcErrorCode err_code =
                    generate_sk_parallel_op_.hd_result_.ErrorCode();
                assert(err_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                       err_code == CcErrorCode::PIN_RANGE_SLICE_FAILED ||
                       err_code == CcErrorCode::ACQUIRE_LOCK_BLOCKED ||
                       err_code == CcErrorCode::DATA_STORE_ERR ||
                       err_code == CcErrorCode::OUT_OF_MEMORY ||
                       err_code == CcErrorCode::REQUEST_LOST ||
                       err_code == CcErrorCode::PACK_SK_ERR ||
                       err_code == CcErrorCode::UNIQUE_CONSTRAINT);

                if (err_code == CcErrorCode::PACK_SK_ERR ||
                    err_code == CcErrorCode::UNIQUE_CONSTRAINT)
                {
                    // If creating index violates constraints, then the failure
                    // is unrecoverable, and the tx should do rollback.
                    //
                    // Besides generate_sk_parallel_op_ set commit_ts to
                    // tx_op_failed_ts_ when constrains violation,
                    // upsert_kv_table_op_ also set commit_ts to
                    // tx_op_failed_ts_ when flush kv failed. As a result,
                    // `op_type == Operation::AddIndex && txm->commit_ts_ ==
                    // tx_op_failed_ts_` doesn't means abort by constrains
                    // violation.
                    txm->commit_ts_ = tx_op_failed_ts_;

                    txm->upsert_resp_->SetErrorCode(
                        TransactionExecution::ConvertCcError(err_code));
                    if (err_code == CcErrorCode::PACK_SK_ERR)
                    {
                        if (store_pack_sk_err_)
                        {
                            PackSkError &pack_sk_err =
                                generate_sk_parallel_op_.hd_result_.Value();
                            *store_pack_sk_err_ = std::move(pack_sk_err);
                        }
                    }

                    op_ = &acquire_all_lock_op_;
                    txm->PushOperation(&acquire_all_lock_op_);
                    txm->Process(acquire_all_lock_op_);
                    return;
                }

                // Retry from the last finished end key. Reset last end key.
                if (is_last_finished_key_str_)
                {
                    last_scanned_end_key_str_ = last_finished_end_key_str_;
                }
                else
                {
                    last_scanned_end_key_ =
                        last_finished_end_key_.GetShallowCopy();
                }
                is_last_scanned_key_str_ = is_last_finished_key_str_;
                // Reset scanned pk range count.
                scanned_pk_range_count_ = finished_pk_range_count_;
                ResetLeaderTerms();
                generate_sk_parallel_op_.Reset();
                txm->PushOperation(&generate_sk_parallel_op_, 1);
                // To sleep serval seconds.
                generate_sk_parallel_op_.ReRunOp(txm);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }

        if (total_scanned_pk_items_count_ == 0)
        {
            // There is no pk items in batch range task, and no sk items
            // generated, therefore there is no need to flush sk.
            op_ = &prepare_data_log_op_;
            prepare_data_log_op_.hd_result_.SetFinished();
            // Update the last finished end key.
            last_finished_end_key_ = last_scanned_end_key_.GetShallowCopy();
            is_last_finished_key_str_ = false;
            finished_pk_range_count_ = scanned_pk_range_count_;
            DLOG(INFO) << "Alter Table Indedx no need to perform flush sk "
                       << "operation for this batch range task for base table: "
                       << table_key_.Name().Trace()
                       << ". Txn: " << txm->TxNumber();
            Forward(txm);
            return;
        }

        CODE_FAULT_INJECTOR(
            "term_AlterTableIndex_GeneratePackedSkOp_Continue", {
                static uint64_t count = 0;
                if (count++ % 1000000 == 0)
                {
                    DLOG(INFO) << "FaultInject term_AlterTableIndex_Generate"
                                  "PackedSkOp_Continue";
                }
                return;
            });

        ACTION_FAULT_INJECTOR("term_AlterTableIndex_FlushNewPackedSKOp");
        assert(op_type_ == OperationType::AddIndex);
        assert(alter_table_info_.index_add_count_ ==
               alter_table_info_.index_add_names_.size());

        LOG(INFO) << "Alter Table Index transaction flush old sk record "
                  << "for base table: " << table_key_.Name().Trace()
                  << ". Already scanned " << scanned_pk_range_count_
                  << " pk ranges. Txn: " << txm->TxNumber()
                  << ", and commit ts: " << txm->commit_ts_;

        flush_all_old_tuples_sk_op_.op_func_ =
            [this, txm, &hd_res = flush_all_old_tuples_sk_op_.hd_result_]
        {
            store::DataStoreHandler *const store_hd =
                Sharder::Instance().GetLocalCcShards()->store_hd_;
            if (store_hd->ByPassDataStore())
            {
                // The table was not created on kv store, so there is no need to
                // execute flush operation. This is to speed up test case only.
                hd_res.SetFinished();
                return;
            }
            // Send the flush data request to the node groups to which
            // the new packed sk data sharding, so obtain the node group
            // count from the @@expected_ng_terms.
            auto &new_index_names = this->alter_table_info_.index_add_names_;
            size_t table_cnt = new_index_names.size();
            auto add_index_it = new_index_names.cbegin();
            assert(add_index_it != new_index_names.cend());

            uint32_t ng_cnt = leader_terms_.size();
            hd_res.Reset();
            hd_res.SetRefCnt(ng_cnt * table_cnt);

            for (uint32_t nid = 0; nid < ng_cnt; ++nid)
            {
                int64_t expected_term = leader_terms_.at(nid);
                for (add_index_it = new_index_names.cbegin();
                     add_index_it != new_index_names.cend();
                     ++add_index_it)
                {
                    this->FlushDataIntoDataStore(add_index_it->first,
                                                 nid,
                                                 txm->commit_ts_,
                                                 true,
                                                 hd_res,
                                                 expected_term);
                }
            }
        };

        op_ = &flush_all_old_tuples_sk_op_;
        txm->PushOperation(&flush_all_old_tuples_sk_op_);
        txm->Process(flush_all_old_tuples_sk_op_);
    }
    else if (op_ == &flush_all_old_tuples_sk_op_)
    {
        if (flush_all_old_tuples_sk_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                if (flush_all_old_tuples_sk_op_.hd_result_.ErrorCode() ==
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                    flush_all_old_tuples_sk_op_.hd_result_.ErrorCode() ==
                        CcErrorCode::REQUEST_LOST)
                {
                    LOG(WARNING)
                        << "Alter table index flush all old sk tuples failed "
                        << "for table: " << table_key_.Name().Trace()
                        << " because of leader transferred. Retry generate "
                        << "packed sk data, txn: " << txm->TxNumber();
                    // For this stage, should re-execute from the previous stage
                    // if leader transferred.
                    // Reset last end key.
                    if (is_last_finished_key_str_)
                    {
                        last_scanned_end_key_str_ = last_finished_end_key_str_;
                    }
                    else
                    {
                        last_scanned_end_key_ =
                            last_finished_end_key_.GetShallowCopy();
                    }
                    is_last_scanned_key_str_ = is_last_finished_key_str_;
                    // Reset scanned pk count.
                    scanned_pk_range_count_ = finished_pk_range_count_;
                    ResetLeaderTerms();
                    generate_sk_parallel_op_.Reset();
                    op_ = &generate_sk_parallel_op_;
                    txm->PushOperation(&generate_sk_parallel_op_, 1);
                    // To sleep serval seconds.
                    generate_sk_parallel_op_.ReRunOp(txm);
                }
                else
                {
                    LOG(WARNING)
                        << "Alter table index flush all old sk tuples failed "
                        << "for table: " << table_key_.Name().Trace()
                        << " with error message: "
                        << flush_all_old_tuples_sk_op_.hd_result_.ErrorMsg()
                        << ". Retry flush old sk operation, txn: "
                        << txm->TxNumber();

                    op_ = &flush_all_old_tuples_sk_op_;
                    txm->PushOperation(&flush_all_old_tuples_sk_op_);
                    txm->Process(flush_all_old_tuples_sk_op_);
                }
            }
            else
            {
                LOG(INFO) << "Alter table index flush all old sk tuples on "
                          << "non-leader node, terminate directly for txn: "
                          << txm->TxNumber();
                ForceToFinish(txm);
            }
        }
        else
        {
            ACTION_FAULT_INJECTOR(
                "term_AlterTableIndex_FlushPrepareIndexTableLogOp");
            // Update the last finished end key.
            last_finished_end_key_ = last_scanned_end_key_.GetShallowCopy();
            is_last_finished_key_str_ = false;
            finished_pk_range_count_ = scanned_pk_range_count_;
            LOG(INFO) << "Alter Table Index transaction write prepare index"
                      << " log with last finished end key: "
                      << ". Base table: " << table_key_.Name().Trace()
                      << ". Txn: " << txm->TxNumber();
            op_ = &prepare_data_log_op_;
            FillPrepareDataLogRequest(txm);
            txm->PushOperation(&prepare_data_log_op_);
            txm->Process(prepare_data_log_op_);
        }
    }
    else if (op_ == &prepare_data_log_op_)
    {
        if (prepare_data_log_op_.hd_result_.IsError())
        {
            // Fails to flush the prepare flush log. Retries the operation if
            // the tx node is still the leader or the tx is in the recovery
            // mode and the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                // set retry flag and retry commit log
                ::txlog::WriteLogRequest *log_req =
                    prepare_data_log_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
                txm->PushOperation(&prepare_data_log_op_);
                txm->Process(prepare_data_log_op_);
            }
            else
            {
                ForceToFinish(txm);
            }

            return;
        }

        if (txm->TxStatus() == TxnStatus::Recovering &&
            Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId()) > 0)
        {
            // If this txm is in the recovering state, should wait until the
            // data log replay finished to avoid data lost.
            return;
        }

        if (!is_last_finished_key_str_ &&
            (last_finished_end_key_.Type() == KeyType::PositiveInf))
        {
            // Reach the end.
            LOG(INFO) << "Alter Table Index transaction lock cluster config"
                      << ", txn: " << txm->TxNumber();
            op_ = &lock_cluster_config_op_;
            txm->PushOperation(&lock_cluster_config_op_);
            txm->Process(lock_cluster_config_op_);
        }
        else
        {
            // Next batch range task
            LOG(INFO) << "Alter Table Index transaction continue to generate "
                      << "sk parallel for table: " << table_key_.Name().Trace()
                      << " with the start key: "
                      << ". Txn: " << txm->TxNumber();
            // Reset last end key.
            if (is_last_finished_key_str_)
            {
                last_scanned_end_key_str_ = last_finished_end_key_str_;
            }
            else
            {
                last_scanned_end_key_ = last_finished_end_key_.GetShallowCopy();
            }
            is_last_scanned_key_str_ = is_last_finished_key_str_;
            ResetLeaderTerms();
            generate_sk_parallel_op_.Reset();
            generate_sk_parallel_op_.op_func_ = [this, txm]()
            {
                generate_sk_parallel_op_.worker_thread_ = std::thread(
                    [this, txm]()
                    {
                        auto &hd_res = generate_sk_parallel_op_.hd_result_;
                        hd_res.Reset();
#ifdef EXT_TX_PROC_ENABLED
                        hd_res.SetToBlock();
#endif
                        this->DispatchRangeTask(txm, hd_res);
                    });
            };
            op_ = &generate_sk_parallel_op_;
            txm->PushOperation(&generate_sk_parallel_op_);
            txm->Process(generate_sk_parallel_op_);
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
                // We downgrade catalog write lock to avoid blocking other
                // transaction which acquiring catalog read lock. And then retry
                // to acquire catalog write lock.
                if (acquire_all_lock_op_.IsDeadlock())
                {
                    LOG(INFO) << "Alter Table Index transaction deadlocks with "
                              << "other transaction, downgrade write lock"
                              << ", txn: " << txm->TxNumber();
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
            LOG(INFO) << "Alter Table Index transaction write commit log"
                      << ", txn: " << txm->TxNumber();
            op_ = &commit_log_op_;
            FillCommitLogRequest(txm);
            txm->PushOperation(&commit_log_op_);
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
                txm->Process(commit_log_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else if (op_type_ == OperationType::DropIndex)
        {
            ACTION_FAULT_INJECTOR("term_AlterTableDropIndex_PostCommitAllWLOp");
            LOG(INFO) << "Drop Index transaction upsert data store"
                      << " info, txn: " << txm->TxNumber();
            op_ = &upsert_kv_table_op_;
            upsert_kv_table_op_.table_schema_ = catalog_rec_.DirtySchema();
            upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
            upsert_kv_table_op_.write_time_ = txm->commit_ts_;
            txm->PushOperation(&upsert_kv_table_op_);
            txm->Process(upsert_kv_table_op_);
        }
        else if (generate_sk_parallel_op_.hd_result_.IsError())
        {
            LOG(WARNING) << "Rollback Index transaction generate sk op, txm: "
                         << txm->TxNumber();

            // Undo the CREATE INDEX operation with a DROP INDEX op.
            op_ = &upsert_kv_table_op_;

            upsert_kv_table_op_.op_type_ = OperationType::DropIndex;
            std::swap(upsert_kv_table_op_.table_schema_old_,
                      upsert_kv_table_op_.table_schema_);
            std::swap(upsert_kv_table_op_.alter_table_info_->index_add_names_,
                      upsert_kv_table_op_.alter_table_info_->index_drop_names_);
            std::swap(upsert_kv_table_op_.alter_table_info_->index_add_count_,
                      upsert_kv_table_op_.alter_table_info_->index_drop_count_);
            upsert_kv_table_op_.write_time_ =
                std::max(upsert_kv_table_op_.write_time_,
                         txm->commit_ts_bound_) +
                1;
            txm->PushOperation(&upsert_kv_table_op_);
            txm->Process(upsert_kv_table_op_);
        }
        else
        {
            ACTION_FAULT_INJECTOR("term_AlterTableIndex_PostCommitAllWLOp");
            LOG(INFO) << "Alter Table Index transaction commit dirty table"
                      << " schema, txn: " << txm->TxNumber();

            assert(post_all_lock_op_.write_type_ == PostWriteType::PostCommit);
            op_ = &post_all_lock_op_;
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
        }
    }
    else if (op_ == &post_all_lock_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
        }
        else if (acquire_all_intent_op_.fail_cnt_.load(
                     std::memory_order_relaxed) > 0)
        {
            // The schema operation failed without flushing the prepare log.
            // Do not retry post-processing (release write intents) even if
            // it fails. Remaining write intents on the schema, if there are
            // any, will be recovered by individual cc nodes separately.
            txm->upsert_resp_->Finish(UpsertResult::Failed);

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            LocalCcShards *local_cc_shards =
                Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(
                local_cc_shards->table_index_op_pool_mux_);
            local_cc_shards->table_index_op_pool_.emplace_back(
                std::move(txm->index_op_));
        }
        else if (post_all_lock_op_.hd_result_.IsError())
        {
            // post_all_lock_op_ returns an error:
            // 1. if flush kv succeeds, the schema op is guaranteed to succeed
            // and can only roll forward. Retry this step to install the
            // committed schema and remove write locks;
            // 2. if flush kv fails, the schema op has to roll backward. Retry
            // this step to reject dirty schema and remove write locks.
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
        }
        else
        {
            // post_all_lock_op_ has finished without an error.
            assert(!post_all_lock_op_.IsFailed());

            if (post_all_lock_op_.write_type_ == PostWriteType::DowngradeLock)
            {
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
                // The tx's modification of the schema has succeeded. If the tx
                // has previously read the same schema and keeps a pointer in
                // the read set to the cc entry of the schema, removes it from
                // the read set. As a result, the tx will not try to release the
                // read lock of the schema when committing.

                for (size_t idx = 0; idx < acquire_all_lock_op_.upload_cnt_;
                     ++idx)
                {
                    const CcEntryAddr &schema_entry_addr =
                        acquire_all_lock_op_.hd_results_[idx]
                            .Value()
                            .local_cce_addr_;
                    if (schema_entry_addr.NodeGroupId() == txm->TxCcNodeId())
                    {
                        txm->rw_set_.DedupRead(schema_entry_addr);
                    }
                }
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

            LOG(INFO) << "Alter Table Index transaction write clean log"
                      << ", txn: " << txm->TxNumber();
            op_ = &clean_log_op_;
            FillCleanLogRequestCommon(txm, clean_log_op_);
            txm->PushOperation(&clean_log_op_);
            txm->Process(clean_log_op_);
        }
    }
    else if (op_ == &clean_log_op_)
    {
        // When a cc node leader begins recovery, the candidate term is set
        // to the Raft term. When recovery finishes, the candidate term is
        // set to -1 after the leader term. So, obtains the candidate term
        // before the leader term.

        if (clean_log_op_.hd_result_.IsError() && txm->CheckLeaderTerm())
        {
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            txm->PushOperation(&clean_log_op_);
            txm->Process(clean_log_op_);
        }
        else
        {
            CODE_FAULT_INJECTOR("alter_schema_term_changed", {
                LOG(INFO) << "FaultInject  alter_schema_term_changed";
                is_force_finished_ = true;
            });
            if (txm->commit_ts_ == tx_op_failed_ts_)
            {
                // - Flush kv error.
                // - Fail to flush prepare_log.
                // - Violate constraints.
                txm->upsert_resp_->Finish(UpsertResult::Failed);
            }
            else
            {
                assert(txm->commit_ts_ > 0);
                if (is_force_finished_)
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

            txm->index_op_->catalog_rec_.Reset();
            LocalCcShards *local_cc_shards =
                Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(
                local_cc_shards->table_index_op_pool_mux_);
            local_cc_shards->table_index_op_pool_.emplace_back(
                std::move(txm->index_op_));
        }
    }
    else
    {
        assert(false);
    }
}

void UpsertTableIndexOp::Reset(const std::string_view table_name_str,
                               const std::string &current_image,
                               uint64_t curr_schema_ts,
                               const std::string &dirty_image,
                               const std::string &alter_table_image,
                               OperationType op_type,
                               PackSkError *store_pack_sk_err,
                               TransactionExecution *txm)
{
    assert(op_type_ == OperationType::AddIndex ||
           op_type_ == OperationType::DropIndex);

    // 1. Reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;

    // 2. Reset SchemaOp
    table_key_.Name() = TableName(
        table_name_str.data(), table_name_str.size(), TableType::Primary);
    catalog_rec_.Reset();
    catalog_rec_.SetSchemaImage(current_image);
    catalog_rec_.SetDirtySchemaImage(dirty_image);

    image_str_ = current_image;
    dirty_image_str_ = dirty_image;
    curr_schema_ts_ = curr_schema_ts;
    op_type_ = op_type;

    // 3. Reset UpsertTableIndexOp
    op_ = nullptr;

    read_cluster_result_.Reset();
    read_cluster_result_.ResetTxm(txm);
    cluster_conf_rec_.Reset();
    lock_cluster_config_op_.Reset();
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.table_name_ =
        TableName(cluster_config_ccm_name_sv, TableType::ClusterConfig);
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;

    alter_table_info_image_str_ = alter_table_image;
    alter_table_info_.Reset();
    alter_table_info_.DeserializeAlteredTableInfo(alter_table_info_image_str_);

    prepare_log_op_.Reset();
    unlock_cluster_config_op_.Reset();
    upsert_kv_table_op_.Reset();
    generate_sk_parallel_op_.Reset();
    flush_all_old_tuples_sk_op_.Reset();
    prepare_data_log_op_.Reset();
    commit_log_op_.Reset();
    clean_log_op_.Reset();

    clean_ccm_op_.Clear();
    clean_ccm_op_.hd_result_.Reset();

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.keys_.clear();
    acquire_all_intent_op_.keys_.emplace_back(&table_key_);
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    upgrade_all_intent_to_lock_op_.table_name_ = &catalog_ccm_name;
    upgrade_all_intent_to_lock_op_.keys_.clear();
    upgrade_all_intent_to_lock_op_.keys_.emplace_back(&table_key_);
    upgrade_all_intent_to_lock_op_.cc_op_ = CcOperation::Write;
    upgrade_all_intent_to_lock_op_.protocol_ = CcProtocol::Locking;

    downgrade_all_lock_to_intent_op_.table_name_ = &catalog_ccm_name;
    downgrade_all_lock_to_intent_op_.keys_.clear();
    downgrade_all_lock_to_intent_op_.keys_.emplace_back(&table_key_);
    downgrade_all_lock_to_intent_op_.recs_.clear();
    downgrade_all_lock_to_intent_op_.recs_.push_back(&catalog_rec_);
    downgrade_all_lock_to_intent_op_.op_type_ = op_type_;
    downgrade_all_lock_to_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
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

    // Reset cc_handler_res txm
    acquire_all_intent_op_.ResetHandlerTxm(txm);
    upgrade_all_intent_to_lock_op_.ResetHandlerTxm(txm);
    prepare_log_op_.ResetHandlerTxm(txm);
    downgrade_all_lock_to_intent_op_.ResetHandlerTxm(txm);
    unlock_cluster_config_op_.ResetHandlerTxm(txm);
    upsert_kv_table_op_.ResetHandlerTxm(txm);
    generate_sk_parallel_op_.ResetHandlerTxm(txm);
    flush_all_old_tuples_sk_op_.ResetHandlerTxm(txm);
    prepare_data_log_op_.ResetHandlerTxm(txm);
    acquire_all_lock_op_.ResetHandlerTxm(txm);
    commit_log_op_.ResetHandlerTxm(txm);
    clean_ccm_op_.ResetHandlerTxm(txm);
    post_all_lock_op_.ResetHandlerTxm(txm);
    clean_log_op_.ResetHandlerTxm(txm);
    is_force_finished_ = false;

    new_indexes_name_.clear();
    new_indexes_name_.reserve(alter_table_info_.index_add_count_);
    for (auto index_it = alter_table_info_.index_add_names_.cbegin();
         index_it != alter_table_info_.index_add_names_.cend();
         ++index_it)
    {
        new_indexes_name_.emplace_back(index_it->first.StringView(),
                                       index_it->first.Type());
    }

    ResetLeaderTerms();
    TxKey neg_inf = Sharder::Instance()
                        .GetLocalCcShards()
                        ->GetCatalogFactory()
                        ->NegativeInfKey();
    last_scanned_end_key_ = neg_inf.GetShallowCopy();
    is_last_scanned_key_str_ = false;
    scanned_pk_range_count_ = 0;
    last_finished_end_key_ = neg_inf.GetShallowCopy();
    is_last_finished_key_str_ = false;
    finished_pk_range_count_ = 0;
    total_scanned_pk_items_count_ = 0;
#ifdef NDEBUG
    scan_batch_range_size_ = 10;
#else
    scan_batch_range_size_ = 3;
#endif

    store_pack_sk_err_ = store_pack_sk_err;
}

void UpsertTableIndexOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    FillPrepareLogRequestCommon(txm, prepare_log_op_);

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    ::txlog::SchemaOpMessage *prepare_schema_msg =
        prepare_log_rec->mutable_log_content()->mutable_schema_log();
    prepare_schema_msg->set_alter_table_info_blob(alter_table_info_image_str_);

    auto &node_terms = *prepare_log_rec->mutable_node_terms();
    node_terms.clear();
    for (size_t idx = 0; idx < upgrade_all_intent_to_lock_op_.upload_cnt_;
         ++idx)
    {
        const AcquireAllResult &hres_val =
            upgrade_all_intent_to_lock_op_.hd_results_[idx].Value();
        uint32_t ng_id = hres_val.local_cce_addr_.NodeGroupId();
        node_terms[ng_id] = hres_val.node_term_;
    }
}

void UpsertTableIndexOp::FillPrepareDataLogRequest(TransactionExecution *txm)
{
    prepare_data_log_op_.log_type_ = TxLogType::PREPARE;

    prepare_data_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_data_log_rec =
        prepare_data_log_op_.log_closure_.LogRequest()
            .mutable_write_log_request();

    prepare_data_log_rec->set_tx_term(txm->tx_term_);
    prepare_data_log_rec->set_txn_number(txm->tx_number_);
    prepare_data_log_rec->set_commit_timestamp(txm->commit_ts_);

    ::txlog::SchemaOpMessage *prepare_data_msg =
        prepare_data_log_rec->mutable_log_content()->mutable_schema_log();
    prepare_data_msg->set_stage(::txlog::SchemaOpMessage_Stage_PrepareData);

    if (last_finished_end_key_.Type() == KeyType::PositiveInf)
    {
        // reach to the last range
        prepare_data_msg->set_last_key_type(
            txlog::SchemaOpMessage::LastKeyType::
                SchemaOpMessage_LastKeyType_PosInfKey);
        prepare_data_msg->mutable_last_key_value()->append("00");
    }
    else
    {
        prepare_data_msg->set_last_key_type(
            txlog::SchemaOpMessage::LastKeyType::
                SchemaOpMessage_LastKeyType_NormalKey);
        last_finished_end_key_.Serialize(
            *prepare_data_msg->mutable_last_key_value());
    }

    prepare_data_log_rec->mutable_node_terms()->clear();
}

void UpsertTableIndexOp::FillCommitLogRequest(TransactionExecution *txm)
{
    FillCommitLogRequestCommon(txm, commit_log_op_);

    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    assert(txm->commit_ts_ != tx_op_failed_ts_ ||
           upsert_kv_table_op_.hd_result_.IsError() ||
           generate_sk_parallel_op_.hd_result_.IsError());
    commit_log_rec->set_commit_timestamp(txm->commit_ts_);
}

void UpsertTableIndexOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    is_force_finished_ = true;
    Forward(txm);
}

/**
 * @param is_dirty If true, should use the dirty table schema.
 */
void UpsertTableIndexOp::FlushDataIntoDataStore(const TableName &table_name,
                                                NodeGroupId ng_id,
                                                uint64_t data_sync_ts,
                                                bool is_dirty,
                                                CcHandlerResult<Void> &hres,
                                                int64_t ng_term)
{
    LocalCcShards *local_cc_shards = Sharder::Instance().GetLocalCcShards();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif

    if (dest_node_id == local_cc_shards->NodeId())
    {
        if (ng_term < 0 &&
            (ng_term = Sharder::Instance().LeaderTerm(ng_id)) < 0)
        {
            LOG(ERROR) << "FlushData operation: request node not the "
                          "leader of ng#"
                       << ng_id;
            hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return;
        }
        if (ng_term < 0)
        {
            Sharder::Instance().UpdateLeader(ng_id);
            hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return;
        }
        assert(ng_term > 0);
        uint64_t table_last_synced_ts = 0;
        auto status = std::make_shared<DataSyncStatus>(false);
        local_cc_shards->EnqueueDataSyncTaskForTable(table_name,
                                                     ng_id,
                                                     ng_term,
                                                     data_sync_ts,
                                                     table_last_synced_ts,
                                                     false,
                                                     is_dirty,
                                                     false,
                                                     status,
                                                     &hres);
    }
    else
    {
        // For remote node, use RPC service
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);
        if (channel == nullptr)
        {
            LOG(ERROR) << "FlushDataIntoDataStore: Failed to init the channel"
                          " to the leader of ng#"
                       << ng_id;
            hres.SetError(CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED);
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());

        FlushDataAllClosure *flush_data_closure =
            new FlushDataAllClosure(&hres);
        flush_data_closure->SetChannel(dest_node_id, channel);

        remote::FlushDataAllRequest *req_ptr =
            flush_data_closure->FlushDataAllRequest();
        req_ptr->set_table_name_str(table_name.String());
        req_ptr->set_table_type(
            remote::ToRemoteType::ConvertTableType(table_name.Type()));
        req_ptr->set_node_group_id(ng_id);
        req_ptr->set_node_group_term(ng_term);
        req_ptr->set_data_sync_ts(data_sync_ts);
        req_ptr->set_is_dirty(is_dirty);

        remote::FlushDataAllResponse *resp_ptr =
            flush_data_closure->FlushDataAllResponse();
        brpc::Controller *cntl = flush_data_closure->Controller();
        cntl->set_timeout_ms(-1);
        // Asynchronous mode
        stub.FlushDataAll(cntl, req_ptr, resp_ptr, flush_data_closure);
        DLOG(INFO) << "Acquire FlushDataAll service of ng#" << ng_id << ".";
    }
}

void UpsertTableIndexOp::ResetLeaderTerms()
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    leader_terms_.resize(ng_cnt);
    size_t cnt = leader_terms_.size();
    for (size_t idx = 0; idx < cnt; ++idx)
    {
        leader_terms_.at(idx) = INIT_TERM;
    }
}

void UpsertTableIndexOp::DispatchRangeTask(
    TransactionExecution *upsert_index_txm,
    CcHandlerResult<PackSkError> &hd_res)
{
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    const TableName &base_table_name = table_key_.Name();
    const TableName &range_table_name =
        TableName(base_table_name.StringView(), TableType::RangePartition);
    uint32_t local_ng_id = Sharder::Instance().NativeNodeGroup();
    uint64_t tx_number = upsert_index_txm->TxNumber();
    int64_t tx_term = upsert_index_txm->TxTerm();
    TxKey target_range_end_key =
        cc_shards->GetCatalogFactory()->PositiveInfKey();
    TxKey target_range_start_key = last_scanned_end_key_.GetShallowCopy();

    uint32_t node_group_cnt = 0;
    size_t batch_range_cnt = 0;
    // Protect the task result.
    std::mutex task_mux;
    std::condition_variable task_cv;
    bool all_task_started = false;
    uint32_t unfinished_task_cnt = 1;
    CcErrorCode task_res = CcErrorCode::NO_ERROR;
    PackSkError pack_sk_err;
    uint32_t pk_items_count = 0;
    uint32_t dispatched_task_count = 0;

    std::function<void(TxKey batch_range_start_key,
                       TxKey batch_range_end_key,
                       const std::string *batch_range_start_key_str,
                       const std::string *batch_range_end_key_str,
                       TxKey &last_scanned_end_key,
                       bool &is_last_scanned_key_str,
                       size_t batch_range_cnt,
                       uint32_t &actual_task_cnt)>
        dispatch_batch_tasks;

    dispatch_batch_tasks =
        [this,
         &base_table_name,
         &range_table_name,
         &local_ng_id,
         cc_shards,
         &tx_number,
         &tx_term,
         scan_ts = upsert_index_txm->commit_ts_,
         &task_mux,
         &task_cv,
         &all_task_started,
         &unfinished_task_cnt,
         &task_res,
         &pack_sk_err,
         &pk_items_count,
         &dispatched_task_count,
         &dispatch_batch_tasks](TxKey batch_range_start_key,
                                TxKey batch_range_end_key,
                                const std::string *batch_range_start_key_str,
                                const std::string *batch_range_end_key_str,
                                TxKey &last_scanned_end_key,
                                bool &is_last_scanned_key_str,
                                size_t batch_range_cnt,
                                uint32_t &actual_task_cnt)
    {
        TransactionExecution *acq_range_lock_txm = nullptr;
        InitTxRequest init_req;
        acq_range_lock_txm = cc_shards->GetTxService()->NewTx();
        init_req.iso_level_ = IsolationLevel::RepeatableRead;
        init_req.protocol_ = CcProtocol::Locking;
        init_req.tx_ng_id_ = local_ng_id;
        // Init the txm until succeed or the txm node is not leader.
        do
        {
            init_req.Reset();
            acq_range_lock_txm->Execute(&init_req);
            init_req.Wait();
            if (init_req.IsError())
            {
                if (!Sharder::Instance().CheckLeaderTerm(local_ng_id, tx_term))
                {
                    LOG(ERROR) << "DispatchRangeTask: Transaction node "
                                  "not leader.";
                    std::unique_lock<std::mutex> lk(task_mux);
                    task_res = CcErrorCode::TX_NODE_NOT_LEADER;
                    return;
                }
                LOG(WARNING)
                    << "Init acquire range txm failed for table: "
                    << range_table_name.Trace() << " of ng#" << local_ng_id
                    << ", with error: " << init_req.ErrorMsg()
                    << ". Retry after 3s.";
                std::this_thread::sleep_for(3s);
            }
        } while (init_req.IsError());

        ReadTxRequest read_range_req;
        RangeRecord range_rec;
        TxKey curr_range_start_key = batch_range_start_key.GetShallowCopy();
        TxKey curr_range_end_key = TxKey();
        int32_t partition_id = 0;
        NodeGroupId range_owner = 0;
        size_t idx = 0;
        bool acquire_next_range = false;
        std::string log_info;

        do
        {
            if (!is_last_scanned_key_str)
            {
                read_range_req.Set(&range_table_name,
                                   0,
                                   &curr_range_start_key,
                                   &range_rec,
                                   false,
                                   false,
                                   true);
            }
            else
            {
                read_range_req.Set(&range_table_name,
                                   0,
                                   batch_range_start_key_str,
                                   &range_rec,
                                   false,
                                   false,
                                   true);
            }
            // Acquire range read lock.
            read_range_req.Reset();
            acq_range_lock_txm->Execute(&read_range_req);
            read_range_req.Wait();
            if (read_range_req.IsError())
            {
                // This read operation might fail if it's blocked by a
                // write lock acquired by range split.
                LOG(ERROR) << "Acquire range read lock failed for table: "
                           << range_table_name.Trace() << " of ng#"
                           << local_ng_id
                           << ", with error: " << read_range_req.ErrorMsg();
                break;
            }

            curr_range_start_key = range_rec.GetRangeInfo()->StartTxKey();
            curr_range_end_key = range_rec.GetRangeInfo()->EndTxKey();
            partition_id = range_rec.GetRangeInfo()->PartitionId();
            range_owner = range_rec.GetRangeOwnerNg()->BucketOwner();
            if (curr_range_start_key.KeyPtr() == nullptr)
            {
                curr_range_start_key =
                    cc_shards->GetCatalogFactory()->NegativeInfKey();
            }
            if (curr_range_end_key.KeyPtr() == nullptr)
            {
                curr_range_end_key =
                    cc_shards->GetCatalogFactory()->PositiveInfKey();
            }

            HandleRangeTask(base_table_name,
                            partition_id,
                            curr_range_start_key.GetShallowCopy(),
                            curr_range_end_key.GetShallowCopy(),
                            range_owner,
                            scan_ts,
                            tx_number,
                            tx_term,
                            task_mux,
                            task_cv,
                            unfinished_task_cnt,
                            all_task_started,
                            pk_items_count,
                            dispatched_task_count,
                            task_res,
                            pack_sk_err,
                            dispatch_batch_tasks);
            ++actual_task_cnt;

            if (batch_range_cnt > 0)
            {
                acquire_next_range =
                    ++idx < batch_range_cnt &&
                    curr_range_end_key.Type() == KeyType::Normal;
            }
            else
            {
                if (batch_range_end_key.KeyPtr())
                {
                    acquire_next_range =
                        curr_range_end_key < batch_range_end_key;
                }
                else
                {
                    assert(batch_range_end_key_str);
                    std::string serialized_end_key;
                    if (curr_range_end_key.Type() == KeyType::Normal)
                    {
                        curr_range_end_key.Serialize(serialized_end_key);
                    }
                    acquire_next_range =
                        serialized_end_key.length() !=
                            batch_range_end_key_str->length() ||
                        serialized_end_key.compare(*batch_range_end_key_str);
                }
            }
            // Update the last range end key
            is_last_scanned_key_str = false;
            last_scanned_end_key = curr_range_end_key.GetShallowCopy();
            // Read the next range.
            curr_range_start_key = curr_range_end_key.GetShallowCopy();
            log_info.append(std::to_string(partition_id)).append(",");
        } while (acquire_next_range);

        LOG(INFO) << "Process this batch task for table: "
                  << base_table_name.Trace() << " of range ids: " << log_info
                  << ". Acquire range lock txn: "
                  << acq_range_lock_txm->TxNumber();
        // Relase the range locks.
        CommitTxRequest commit_req;
        acq_range_lock_txm->CommitTx(commit_req);
    };

    // Begin to dispatch pk range task until flush sk is needed.
    do
    {
        node_group_cnt = Sharder::Instance().NodeGroupCount();
        batch_range_cnt = node_group_cnt * scan_batch_range_size_;
        target_range_start_key = last_scanned_end_key_.GetShallowCopy();
        all_task_started = false;
        unfinished_task_cnt = 1;
        task_res = CcErrorCode::NO_ERROR;
        pk_items_count = 0;
        dispatched_task_count = 0;
        uint32_t actual_task_cnt = 0;

        dispatch_batch_tasks(
            target_range_start_key.GetShallowCopy(),
            target_range_end_key.GetShallowCopy(),
            (is_last_scanned_key_str_ ? last_scanned_end_key_str_ : nullptr),
            nullptr,
            last_scanned_end_key_,
            is_last_scanned_key_str_,
            batch_range_cnt,
            actual_task_cnt);

        // Wait the result
        std::unique_lock<std::mutex> lk(task_mux);
        dispatched_task_count += actual_task_cnt;
        all_task_started = true;
        --unfinished_task_cnt;
        task_cv.wait(lk,
                     [&all_task_started, &unfinished_task_cnt]()
                     { return all_task_started && unfinished_task_cnt == 0; });

        for (auto &workers : local_task_workers_)
        {
            workers.join();
        }
        local_task_workers_.clear();

        if (task_res != CcErrorCode::NO_ERROR)
        {
            LOG(ERROR) << "Generate sk task failed for table: "
                       << base_table_name.Trace()
                       << ", with error: " << CcErrorMessage(task_res);
            if (task_res == CcErrorCode::PACK_SK_ERR)
            {
                hd_res.SetValue(std::move(pack_sk_err));
            }
            hd_res.SetError(task_res);
            return;
        }
        else
        {
            // Update the scanned pk range count.
            scanned_pk_range_count_ += dispatched_task_count;
            total_scanned_pk_items_count_ += pk_items_count;
            DLOG(INFO) << "Generate sk task successfully for this "
                          "batch ranges."
                       << " Base table: " << base_table_name.Trace();
        }
    } while (!NeedTriggerFlushSkOp());
    DLOG(INFO) << "Generate sk batch task finished."
               << " Base table: " << base_table_name.Trace();
    hd_res.SetFinished();
}

void UpsertTableIndexOp::HandleRangeTask(
    const TableName &base_table_name,
    int32_t partition_id,
    TxKey range_start_key,
    TxKey range_end_key,
    NodeGroupId range_owner,
    uint64_t scan_ts,
    uint64_t tx_number,
    int64_t tx_term,
    std::mutex &task_mux,
    std::condition_variable &task_cv,
    uint32_t &unfinished_task_cnt,
    bool &all_task_started,
    uint32_t &total_pk_items_count,
    uint32_t &dispatched_task_count,
    CcErrorCode &task_res,
    PackSkError &pack_sk_err,
    std::function<void(TxKey batch_range_start_key,
                       TxKey batch_range_end_key,
                       const std::string *batch_range_start_key_str,
                       const std::string *batch_range_end_key_str,
                       TxKey &last_scanned_end_key,
                       bool &is_last_scanned_key_str,
                       size_t batch_range_cnt,
                       uint32_t &actual_task_cnt)> &dispatch_func)
{
    uint32_t local_node_id = Sharder::Instance().NodeId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(range_owner);
    if (dest_node_id == local_node_id)
    {
        local_task_workers_.push_back(std::thread(
            [this,
             &base_table_name,
             partition_id,
             range_start_key = std::move(range_start_key),
             range_end_key = std::move(range_end_key),
             range_owner,
             scan_ts,
             tx_number,
             tx_term,
             &ng_terms = leader_terms_,
             &sk_names = new_indexes_name_,
             &task_mux,
             &task_cv,
             &unfinished_task_cnt,
             &all_task_started,
             &total_pk_items_count,
             &dispatched_task_count,
             &task_res,
             &pack_sk_err,
             &dispatch_func]()
            {
                while (Sharder::Instance().LeaderTerm(range_owner) < 0 &&
                       Sharder::Instance().CandidateLeaderTerm(range_owner) > 0)
                {
                    // Waiting until log replay finished on this node
                    // group. Including data(.pk) log and and
                    // catalog(.table range info) log.
                    LOG(WARNING) << "GenerateSkFromPk of ng#" << range_owner
                                 << " for partition id: " << partition_id
                                 << " waiting log replay finished.";
                    std::this_thread::sleep_for(3s);
                }

                LocalCcShards *cc_shards =
                    Sharder::Instance().GetLocalCcShards();
                std::unique_ptr<SkGenerator> sk_generator = nullptr;
                {
                    std::lock_guard<std::mutex> lk(
                        cc_shards->table_index_op_pool_mux_);
                    if (cc_shards->sk_generator_pool_.empty())
                    {
                        sk_generator = std::make_unique<SkGenerator>();
                    }
                    else
                    {
                        assert(cc_shards->sk_generator_pool_.back() != nullptr);
                        sk_generator =
                            std::move(cc_shards->sk_generator_pool_.back());
                        cc_shards->sk_generator_pool_.pop_back();
                    }
                }

                sk_generator->Reset(&range_start_key,
                                    &range_end_key,
                                    scan_ts,
                                    base_table_name,
                                    range_owner,
                                    partition_id,
                                    tx_number,
                                    tx_term,
                                    sk_names);
                sk_generator->ProcessTask();

                CcErrorCode res_code = sk_generator->TaskResult();
                if (res_code == CcErrorCode::GET_RANGE_ID_ERR)
                {
                    LOG(WARNING)
                        << "Terminate this generate index task of ng#"
                        << range_owner << " for partition id: " << partition_id
                        << " for table: " << base_table_name.Trace()
                        << " caused by the boundary of partition "
                           "mismatch.";

                    // recycle skgenerator
                    {
                        std::lock_guard<std::mutex> lk(
                            cc_shards->table_index_op_pool_mux_);
                        cc_shards->sk_generator_pool_.emplace_back(
                            std::move(sk_generator));
                    }
                    // Update the task status
                    {
                        std::lock_guard<std::mutex> task_lk(task_mux);
                        all_task_started = false;
                        --unfinished_task_cnt;
                    }

                    // Re-dispatch this range task.
                    TxKey last_scanned_end_key =
                        range_start_key.GetShallowCopy();
                    bool is_last_scanned_key_str = false;
                    uint32_t actual_task_cnt = 0;
                    do
                    {
                        dispatch_func(range_start_key.GetShallowCopy(),
                                      range_end_key.GetShallowCopy(),
                                      nullptr,
                                      nullptr,
                                      last_scanned_end_key,
                                      is_last_scanned_key_str,
                                      0,
                                      actual_task_cnt);
                        {
                            std::lock_guard<std::mutex> task_lk(task_mux);
                            if (task_res == CcErrorCode::TX_NODE_NOT_LEADER)
                            {
                                all_task_started = true;
                                task_cv.notify_one();
                                return;
                            }
                        }
                    } while (last_scanned_end_key < range_end_key);

                    // Update the task status
                    {
                        std::lock_guard<std::mutex> task_lk(task_mux);
                        dispatched_task_count += (actual_task_cnt - 1);
                        all_task_started = true;
                        task_cv.notify_one();
                    }
                    return;
                }

                std::unique_lock<std::mutex> task_lk(task_mux);
                if (res_code != CcErrorCode::NO_ERROR)
                {
                    LOG(ERROR)
                        << "Finish this generate index task of ng#"
                        << range_owner << " for partition id: " << partition_id
                        << " caused by error: " << CcErrorMessage(res_code);
                }
                else
                {
                    // check the terms
                    auto &terms = sk_generator->NodeGroupTerms();
                    for (size_t idx = 0; idx < terms.size(); ++idx)
                    {
                        auto &term = terms.at(idx);
                        if (term < 0)
                        {
                            continue;
                        }

                        auto &ng_term = ng_terms.at(idx);
                        if (ng_term < 0)
                        {
                            ng_term = term;
                        }
                        else if (ng_term > 0 && ng_term != term)
                        {
                            LOG(ERROR)
                                << "Generate index failed of ng#" << range_owner
                                << " for partition id: " << partition_id
                                << " caused by leader transferred.";
                            res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                            break;
                        }
                        else
                        {
                            assert(ng_term == term);
                        }
                    }

                    total_pk_items_count += sk_generator->ScannedItemsCount();
                }

                --unfinished_task_cnt;
                if (task_res == CcErrorCode::NO_ERROR)
                {
                    task_res = res_code;
                    if (res_code == CcErrorCode::PACK_SK_ERR)
                    {
                        pack_sk_err = std::move(sk_generator->GetPackSkError());
                    }
                }
                task_cv.notify_one();
                task_lk.unlock();

                // recycle skgenerator
                {
                    std::lock_guard<std::mutex> lk(
                        cc_shards->table_index_op_pool_mux_);
                    cc_shards->sk_generator_pool_.emplace_back(
                        std::move(sk_generator));
                }
            }));
    }
    else
    {
        // remote node
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);
        if (channel == nullptr)
        {
            // Fail to establish the channel to the tx node.
            LOG(ERROR) << "Acquire GenerateSkFromPk failed to init the "
                          "channel of ng#"
                       << range_owner;
            std::unique_lock<std::mutex> lk(task_mux);
            task_res = task_res == CcErrorCode::NO_ERROR
                           ? CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED
                           : task_res;
            task_cv.notify_one();
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());

        GenerateSkFromPkClosure *closure =
            new GenerateSkFromPkClosure(task_mux,
                                        task_cv,
                                        leader_terms_,
                                        unfinished_task_cnt,
                                        all_task_started,
                                        total_pk_items_count,
                                        dispatched_task_count,
                                        task_res,
                                        pack_sk_err,
                                        dispatch_func);
        closure->SetChannel(dest_node_id, channel);

        brpc::Controller *cntl_ptr = closure->Controller();
        cntl_ptr->set_timeout_ms(-1);
        auto req_ptr = closure->GenerateSkFromPkRequest();
        req_ptr->set_table_name_str(base_table_name.String());
        req_ptr->set_node_group_id(range_owner);
        req_ptr->set_tx_number(tx_number);
        req_ptr->set_tx_term(tx_term);
        req_ptr->set_scan_ts(scan_ts);
        req_ptr->set_partition_id(partition_id);
        assert(range_start_key.KeyPtr() != nullptr &&
               range_end_key.KeyPtr() != nullptr);
        if (range_start_key.Type() == KeyType::Normal)
        {
            range_start_key.Serialize(*req_ptr->mutable_start_key());
        }
        if (range_end_key.Type() == KeyType::Normal)
        {
            range_end_key.Serialize(*req_ptr->mutable_end_key());
        }

        for (size_t idx = 0; idx < new_indexes_name_.size(); ++idx)
        {
            req_ptr->add_new_sk_name_str(new_indexes_name_.at(idx).String());
            req_ptr->add_new_sk_type(remote::ToRemoteType::ConvertTableType(
                new_indexes_name_.at(idx).Type()));
        }

        auto resp_ptr = closure->GenerateSkFromPkResponse();
        // Asynchronous mode
        stub.GenerateSkFromPk(cntl_ptr, req_ptr, resp_ptr, closure);
        DLOG(INFO) << "Acquire GenerateSkFromPk service for partition id: "
                   << partition_id << " with start key: "
                   << " and end key: "
                   << " of ng#" << range_owner;
    }

    {
        std::lock_guard<std::mutex> task_lk(task_mux);
        ++unfinished_task_cnt;
    }
}

}  // namespace txservice
