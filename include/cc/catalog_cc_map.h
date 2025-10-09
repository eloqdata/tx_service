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

#include <algorithm>
#include <chrono>
#include <memory>  // make_shared
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_protocol.h"
#include "cc_request.h"
#include "error_messages.h"  //CcErrorCode
#include "fault_inject.h"
#include "local_cc_shards.h"
#include "log.pb.h"
#include "log_type.h"
#include "non_blocking_lock.h"
#include "reader_writer_cntl.h"
#include "sharder.h"
#include "template_cc_map.h"
#include "tx_command.h"
#include "tx_operation.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class CatalogCcMap
    : public TemplateCcMap<CatalogKey, CatalogRecord, true, false>
{
public:
    CatalogCcMap(const CatalogCcMap &rhs) = delete;
    ~CatalogCcMap() = default;

    /**
     * @brief Constructs a new catalog cc map object. The catalog cc map has no
     * schema, so the schema's timestamp is set to 1 (the beginning of history).
     *
     * @param shard
     */
    CatalogCcMap(CcShard *shard,
                 NodeGroupId cc_ng_id,
                 const TableName &table_name)
        : TemplateCcMap<CatalogKey, CatalogRecord, true, false>(
              shard, cc_ng_id, table_name, 1, nullptr, false)
    {
    }

    void Clean() override
    {
        TemplateCcMap::Clean();
        table_locks_.clear();
    }

    using TemplateCcMap::Execute;

    bool Execute(AcquireAllCc &req) override
    {
        // For the first AcuireWriteAll request (which acquires write intents),
        // sets the reader-writer-control block to coordinate with runtime
        // queries that are accessing the schema w/o concurrency control and
        // blocks future runtime queries from accessing the schema via the
        // control block.
        uint32_t ng_id = req.NodeGroupId();
        if (shard_->IsNative(ng_id) && req.CcOp() == CcOperation::ReadForWrite)
        {
            int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
            CcHandlerResult<AcquireAllResult> *hd_res = req.Result();
            if (ng_term < 0)
            {
                return hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            }

            const CatalogKey *catalog_key = nullptr;
            if (req.Key() != nullptr)
            {
                catalog_key = static_cast<const CatalogKey *>(req.Key());
            }
            else
            {
                assert(*req.KeyStrType() == KeyType::Normal);

                const std::string *key_str = req.KeyStr();
                assert(key_str != nullptr);
                std::unique_ptr<CatalogKey> decoded_key =
                    std::make_unique<CatalogKey>();
                size_t offset = 0;
                decoded_key->Deserialize(key_str->data(), offset, KeySchema());
                catalog_key = decoded_key.get();
                req.SetDecodedKey(TxKey(std::move(decoded_key)));
            }

            const TableName &tbl_name = catalog_key->Name();
            std::shared_ptr<ReaderWriterObject<TableSchema>> schema_cntl =
                shard_->FindEmplaceSchemaCntl(tbl_name);
            AddWriterResult ret = schema_cntl->AddWriter(&req);
            switch (ret)
            {
            case AddWriterResult::WriteConflict:
                // There is a write-write conflict
                return hd_res->SetError(
                    CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT);
            case AddWriterResult::WritePending:
                // There are readers accessing the schema. Blocks the
                // request until all existing readers finish.
                return false;
            case AddWriterResult::Invalid:
                // The control block is invalid. Deletes it from the shard.
                shard_->DeleteSchemaCntl(tbl_name);
                schema_cntl = shard_->FindEmplaceSchemaCntl(tbl_name);
                ret = schema_cntl->AddWriter(&req);
                assert(ret == AddWriterResult::Success);
                break;
            default:
                break;
            }
        }

        return TemplateCcMap::Execute(req);
    }

    bool Execute(PostWriteAllCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

        CODE_FAULT_INJECTOR("during_post_write_all", {
            std::string &action = FaultInject::Instance()
                                      .Entry("during_post_write_all")
                                      ->vctAction_.front();
            if ((req.CommitType() == PostWriteType::PrepareCommit &&
                 action == "prepare_commit_panic") ||
                (req.CommitType() == PostWriteType::PostCommit &&
                 req.CommitTs() == TransactionOperation::tx_op_failed_ts_ &&
                 action == "post_commit_panic"))
            {
                int retval;
                sigset_t new_mask;
                sigfillset(&new_mask);

                retval = kill(getpid(), SIGKILL);
                assert(retval == 0);
                retval = sigsuspend(&new_mask);
                fprintf(stderr,
                        "sigsuspend returned %d errno %d \n",
                        retval,
                        errno);
                assert(false); /* With full signal mask, we should never
                                  return here. */
            }
            else if ((req.CommitType() == PostWriteType::PrepareCommit &&
                      action == "prepare_commit_sleep") ||
                     (req.CommitType() == PostWriteType::PostCommit &&
                      req.CommitTs() ==
                          TransactionOperation::tx_op_failed_ts_ &&
                      action == "post_commit_sleep"))
            {
                LOG(INFO) << "sleep 10 seconds: ";
                sleep(10);
                // remove fault injection after sleep
                FaultInject::Instance().InjectFault("during_post_write_all",
                                                    "remove");
            }
        });

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        CODE_FAULT_INJECTOR("term_CatalogCcMap_Execute_PostWriteAllCc", {
            LOG(INFO)
                << "FaultInject  term_CatalogCcMap_Execute_PostWriteAllCc";
            ng_term = -1;
            FaultInject::Instance().InjectFault(
                "term_CatalogCcMap_Execute_PostWriteAllCc", "remove");
        });
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const CatalogKey *table_key = nullptr;
        if (req.Key() != nullptr)
        {
            table_key = static_cast<const CatalogKey *>(req.Key());
        }
        else
        {
            switch (*req.KeyStrType())
            {
            case KeyType::NegativeInf:
            case KeyType::PositiveInf:
                // For catalog, key type can't be Inf
                assert(false);
                break;
            case KeyType::Normal:
                const std::string *key_str = req.KeyStr();
                assert(key_str != nullptr);
                std::unique_ptr<CatalogKey> decoded_key =
                    std::make_unique<CatalogKey>();
                size_t offset = 0;
                decoded_key->Deserialize(key_str->data(), offset, KeySchema());
                table_key = decoded_key.get();
                req.SetDecodedKey(TxKey(std::move(decoded_key)));
                break;
            }
        }

        Iterator it =
            TemplateCcMap<CatalogKey, CatalogRecord, true, false>::Find(
                *table_key);
        CcEntry<CatalogKey, CatalogRecord, true, false> *cce_ptr = it->second;

        // Check whether cce key lock holder is the given tx of the
        // PostWriteAllCc before applying change.
        if (cce_ptr == nullptr || cce_ptr->GetKeyLock() == nullptr ||
            !cce_ptr->GetKeyLock()->HasWriteLockOrWriteIntent(req.Txn()))
        {
            // When the catalog entry is null in the post-write-all
            // phase, it means that (1) the cc req is a resend request and
            // previous has successed, (2) the cc node group must have failed
            // over once, and (3) there is no catalog op in the log (so that the
            // recovered cc node group has no catalog entry). No catalog op in
            // the log means that this schema op fails before the prepare log
            // and this post-write-all request is to release the write
            // lock/intent. Given that the cc node group has failed once, the
            // previously acquired intent/lock has gone. There is no need to
            // proceed to release the intent/lock. The request is set to
            // finished.
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                req.Result()->SetFinished();
                req.SetDecodedPayload(nullptr);
                return true;
            }
            else
            {
                req.ResetCcm();
                MoveRequest(&req, shard_->core_id_ + 1);
                return false;
            }
        }

        CatalogRecord *schema_rec = nullptr;
        CatalogEntry *catalog_entry = nullptr;

        // First setup the schema_rec that will replace the current catalog rec.
        switch (req.CommitType())
        {
        case PostWriteType::PrepareCommit:
        {
            if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_)
            {
                // When the commit ts is 0, the request commits nothing and only
                // removes the write intents/locks acquired earlier.
                return TemplateCcMap::Execute(req);
            }

            if (shard_->core_id_ == 0)
            {
                // For prepare commit or commit, instantiates the dirty schema
                // instance in LocalCcShards when processing the request at the
                // first shard/core.

                if (req.Payload() != nullptr)
                {
                    // When the request comes from a tx in the same node, the
                    // request references a schema record in the tx's space.
                    schema_rec = static_cast<CatalogRecord *>(req.Payload());
                }
                else
                {
                    // When the request comes from a remote tx, the request
                    // contains a serialized representation of the catalog, but
                    // not a reference of a schema record. Allocates a schema
                    // record.
                    assert(req.PayloadStr() != nullptr);
                    std::unique_ptr<CatalogRecord> decoded_rec =
                        std::make_unique<CatalogRecord>();
                    if (req.OpType() != OperationType::DropTable)
                    {
                        size_t offset = 0;
                        decoded_rec->Deserialize(req.PayloadStr()->data(),
                                                 offset);
                    }
                    schema_rec = decoded_rec.get();
                    req.SetDecodedPayload(std::move(decoded_rec));
                }

                catalog_entry =
                    shard_->CreateDirtyCatalog(table_key->Name(),
                                               req.NodeGroupId(),
                                               schema_rec->DirtySchemaImage(),
                                               req.CommitTs());

                // For alter index, we need to initialize meta data for the
                // new indexes at prepare stage since the new indexes may
                // receive cc requests during the 2 phase commit DDL tx..
                if ((req.OpType() == OperationType::AddIndex ||
                     req.OpType() == OperationType::DropIndex))
                {
                    if (catalog_entry->schema_ == nullptr)
                    {
                        // For alter table, in some case, the current schema may
                        // not exists yet, so should create the current schema.
                        // For example, this node is the participant node of the
                        // alter table transaction, and does not execute any
                        // transaction about this table before this alter table
                        // tx since server start.
                        shard_->FetchCatalog(table_key->Name(),
                                             req.NodeGroupId(),
                                             ng_term,
                                             &req);
                        return false;
                    }

                    if (!shard_->LoadRangesAndStatisticsNx(
                            catalog_entry->schema_.get(),
                            req.NodeGroupId(),
                            ng_term,
                            &req))
                    {
                        return false;
                    }

                    // Bind statistics for the dirty schema.
                    catalog_entry->dirty_schema_->BindStatistics(
                        catalog_entry->schema_->StatisticsObject());
                    if (req.OpType() == OperationType::AddIndex)
                    {
                        // For ALTER TABLE, set the dirty index name, and this
                        // info should be clean when commit dirty schema. For
                        // CREATE TABLE, there is no matter that do not set the
                        // dirty index info, because this table is invisible
                        // until create table transaction commit.
                        auto new_index_names =
                            catalog_entry->dirty_schema_->IndexNames();
                        auto old_index_names =
                            catalog_entry->schema_->IndexNames();
                        std::vector<TableName> dirty_index_names;
                        for (const TableName &new_index_name : new_index_names)
                        {
                            if (std::find(old_index_names.begin(),
                                          old_index_names.end(),
                                          new_index_name) ==
                                old_index_names.end())
                            {
                                dirty_index_names.emplace_back(
                                    new_index_name.StringView(),
                                    new_index_name.Type(),
                                    new_index_name.Engine());
                            }
                        }

                        // Load ranges for the new added indexes. We cannot
                        // simply initialize it with empty range table since we
                        // might have pre-defined range table based on the data
                        // distribution offered by caller.
                        for (const TableName &index_name : dirty_index_names)
                        {
                            TableName index_range_name{
                                index_name.StringView(),
                                TableType::RangePartition,
                                index_name.Engine()};

                            auto ranges = shard_->GetTableRangesForATable(
                                index_range_name, req.NodeGroupId());
                            if (ranges == nullptr)
                            {
                                shard_->FetchTableRanges(index_range_name,
                                                         &req,
                                                         req.NodeGroupId(),
                                                         ng_term);
                                return false;
                            }

                            for (auto &range : *ranges)
                            {
                                range.second->SetVersion(req.CommitTs());
                                NodeGroupId range_owner =
                                    shard_
                                        ->GetRangeOwner(
                                            range.second->GetRangeInfo()
                                                ->PartitionId(),
                                            req.NodeGroupId())
                                        ->BucketOwner();
                                if (range_owner == req.NodeGroupId() &&
                                    range.second->RangeSlices() == nullptr)
                                {
                                    // The owner this this range, and this is a
                                    // empty range.

                                    std::unique_lock<std::mutex> heap_lk(
                                        shard_->local_shards_
                                            .table_ranges_heap_mux_);
                                    bool is_override_thd =
                                        mi_is_override_thread();
                                    mi_threadid_t prev_thd = mi_override_thread(
                                        shard_->local_shards_
                                            .GetTableRangesHeapThreadId());
                                    mi_heap_t *prev_heap = mi_heap_set_default(
                                        shard_->local_shards_
                                            .GetTableRangesHeap());

                                    std::vector<SliceInitInfo> slices;
                                    range.second->InitRangeSlices(
                                        std::move(slices),
                                        req.NodeGroupId(),
                                        false,
                                        true,
                                        UINT64_MAX,
                                        false);

                                    mi_heap_set_default(prev_heap);
                                    if (is_override_thd)
                                    {
                                        mi_override_thread(prev_thd);
                                    }
                                    else
                                    {
                                        mi_restore_default_thread_id();
                                    }
                                    heap_lk.unlock();
                                }
                            }
                        }

                        for (const TableName &index_name : dirty_index_names)
                        {
                            catalog_entry->schema_->StatisticsObject()
                                ->CreateIndex(index_name,
                                              catalog_entry->dirty_schema_
                                                  ->IndexKeySchema(index_name),
                                              cc_ng_id_);
                        }
                    }
                }

                schema_rec->Set(catalog_entry->schema_,
                                catalog_entry->dirty_schema_,
                                catalog_entry->schema_version_);
            }
            else
            {
                assert(req.Payload() != nullptr);
                schema_rec = static_cast<CatalogRecord *>(req.Payload());
                catalog_entry =
                    shard_->GetCatalog(table_key->Name(), req.NodeGroupId());

                if (catalog_entry == nullptr)
                {
                    // The table catalog haven't been initialized. Just return.
                    req.Result()->SetError(
                        CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                    return true;
                }
            }

            break;
        }
        case PostWriteType::UpdateDirty:
        {
            assert(req.OpType() == OperationType::AddIndex);
            assert(req.CommitTs() != TransactionOperation::tx_op_failed_ts_);
            if (shard_->core_id_ == 0)
            {
                // For update dirty, retrieves the current and dirty schema pair
                // from the current shard.
                if (req.Payload() != nullptr)
                {
                    // When the request comes from a tx in the same node, the
                    // request references a schema record in the tx's space.
                    schema_rec = static_cast<CatalogRecord *>(req.Payload());
                }
                else
                {
                    assert(req.PayloadStr() != nullptr);
                    // When the request comes from a remote tx, allocates a
                    // schema record, which acts as a container referencing the
                    // current and dirty schema pair.
                    std::unique_ptr<CatalogRecord> decoded_rec =
                        std::make_unique<CatalogRecord>();
                    size_t offset = 0;
                    decoded_rec->Deserialize(req.PayloadStr()->data(), offset);

                    schema_rec = decoded_rec.get();
                    req.SetDecodedPayload(std::move(decoded_rec));
                }

                catalog_entry =
                    shard_->GetCatalog(table_key->Name(), req.NodeGroupId());

                if (catalog_entry == nullptr)
                {
                    // The target table catalog haven't been initialized. Just
                    // return.
                    req.Result()->SetError(
                        CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                    return true;
                }

                if (catalog_entry->dirty_schema_->SchemaImage() !=
                    schema_rec->DirtySchemaImage())
                {
                    assert(catalog_entry->dirty_schema_version_ ==
                           req.CommitTs());
                    std::shared_ptr<TableSchema> old_dirty_schema =
                        catalog_entry->dirty_schema_;
                    shard_->UpdateDirtyCatalog(table_key->Name(),
                                               schema_rec->DirtySchemaImage(),
                                               catalog_entry);
                    catalog_entry->dirty_schema_->BindStatistics(
                        old_dirty_schema->StatisticsObject());
                }

                schema_rec->Set(catalog_entry->schema_,
                                catalog_entry->dirty_schema_,
                                catalog_entry->schema_version_);
            }
            else
            {
                assert(req.Payload() != nullptr);
                schema_rec = static_cast<CatalogRecord *>(req.Payload());
                catalog_entry =
                    shard_->GetCatalog(table_key->Name(), req.NodeGroupId());

                if (catalog_entry == nullptr)
                {
                    // The table catalog haven't been initialized. Just return.
                    req.Result()->SetError(
                        CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                    return true;
                }
            }
            break;
        }
        case PostWriteType::PostCommit:
        {
            if (shard_->IsNative(req.NodeGroupId()))
            {
                const TableName &tbl_name = table_key->Name();
                std::shared_ptr<ReaderWriterObject<TableSchema>> schema_cntl =
                    shard_->FindSchemaCntl(tbl_name);
                if (schema_cntl != nullptr)
                {
                    schema_cntl->FinishWriter(req.Txn());
                    shard_->DeleteSchemaCntl(tbl_name);
                }
            }

            catalog_entry =
                shard_->GetCatalog(table_key->Name(), req.NodeGroupId());

            if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_)
            {
                // The catalog_entry could be null if the txn aborts after
                // acquire write intent failure.

                // For add index op, we create new sk ccmap, table ranges and
                // table statistics for the new sk during prepare phase. If
                // flush kv failed, should clean these up. But, if the dirty
                // schema is nullptr, that is mean, this is the recovering
                // transaction, and there is no need to drop the new sk ccmap.
                if (req.OpType() == OperationType::AddIndex &&
                    catalog_entry != nullptr &&
                    catalog_entry->dirty_schema_ != nullptr)
                {
                    std::vector<TableName> new_index_names =
                        catalog_entry->dirty_schema_->IndexNames();
                    std::vector<TableName> old_index_names =
                        catalog_entry->schema_->IndexNames();
                    for (const TableName &new_index_name : new_index_names)
                    {
                        if (std::find(old_index_names.begin(),
                                      old_index_names.end(),
                                      new_index_name) == old_index_names.end())
                        {
                            shard_->DropCcm(new_index_name, req.NodeGroupId());
                            // Clean up table ranges for new sk.
                            const TableName index_range_name{
                                new_index_name.StringView(),
                                TableType::RangePartition,
                                new_index_name.Engine()};
                            shard_->DropCcm(index_range_name,
                                            req.NodeGroupId());
                            if (shard_->core_id_ == shard_->core_cnt_ - 1)
                            {
                                shard_->CleanTableRange(index_range_name,
                                                        req.NodeGroupId());
                                Statistics *statistics =
                                    catalog_entry->dirty_schema_
                                        ->StatisticsObject()
                                        .get();
                                statistics->DropIndex(new_index_name);
                            }
                        }
                    }
                }

                // Flush kv fails, need to clear dirty CatalogEntry
                if (shard_->core_id_ == shard_->core_cnt_ - 1 &&
                    catalog_entry != nullptr)
                {
                    catalog_entry->RejectDirtySchema();
                }

                if (cce_ptr->payload_.cur_payload_)
                {
                    cce_ptr->payload_.cur_payload_->ClearDirtySchema();
                    cce_ptr->payload_.cur_payload_->SetDirtySchemaImage("");
                }
                return TemplateCcMap::Execute(req);
            }

            assert(catalog_entry != nullptr);
            if (shard_->core_id_ == 0)
            {
                // For post commit, retrieves the current and dirty schema pair
                // from the current shard.
                if (req.Payload() != nullptr)
                {
                    // When the request comes from a tx in the same node, the
                    // request references a schema record in the tx's space.
                    schema_rec = static_cast<CatalogRecord *>(req.Payload());
                }
                else
                {
                    assert(req.PayloadStr() != nullptr);
                    // When the request comes from a remote tx, allocates a
                    // schema record, which acts as a container referencing the
                    // current and dirty schema pair.
                    std::unique_ptr<CatalogRecord> decoded_rec =
                        std::make_unique<CatalogRecord>();
                    if (req.OpType() != OperationType::DropTable)
                    {
                        size_t offset = 0;
                        decoded_rec->Deserialize(req.PayloadStr()->data(),
                                                 offset);
                    }

                    schema_rec = decoded_rec.get();
                    req.SetDecodedPayload(std::move(decoded_rec));
                }

                if (req.OpType() == OperationType::CreateTable)
                {
                    std::vector<InitRangeEntry> range_init_vec;
                    std::string_view table_name_view =
                        table_key->Name().StringView();
                    int init_partition_id = 0;
                    if (table_name_view != "./mysql/sequences")
                    {
                        size_t tbl_name_hash =
                            std::hash<std::string_view>()(table_name_view);
                        init_partition_id = tbl_name_hash & 0xFFF;
                    }

                    // Use nullptr to represent negative infinity key here.
                    range_init_vec.emplace_back(
                        TxKey(), init_partition_id, req.CommitTs());

                    TableName range_table_name(table_name_view,
                                               TableType::RangePartition,
                                               table_key->Name().Engine());
                    shard_->local_shards_.InitTableRanges(range_table_name,
                                                          range_init_vec,
                                                          req.NodeGroupId(),
                                                          true);

                    std::vector<TableName> index_names =
                        catalog_entry->dirty_schema_->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        // Create range table for each sk index
                        TableName index_range_table_name{
                            index_name.StringView(),
                            TableType::RangePartition,
                            index_name.Engine()};

                        size_t tbl_name_hash = std::hash<std::string_view>()(
                            index_name.StringView());
                        init_partition_id = tbl_name_hash & 0xFFF;
                        range_init_vec.clear();
                        range_init_vec.emplace_back(
                            TxKey(), init_partition_id, req.CommitTs());
                        shard_->local_shards_.InitTableRanges(
                            index_range_table_name,
                            range_init_vec,
                            req.NodeGroupId(),
                            true);
                    }
                    shard_->InitTableStatistics(
                        catalog_entry->dirty_schema_.get(), cc_ng_id_);
                }
                else if (req.OpType() == OperationType::Update)
                {
                    assert(catalog_entry->schema_->StatisticsObject());
                    catalog_entry->dirty_schema_->BindStatistics(
                        catalog_entry->schema_->StatisticsObject());
                }

                // If recoverring from commit log, there is no dirty schema,
                // skip updating schema_rec
                if (catalog_entry->dirty_schema_ != nullptr)
                {
                    schema_rec->Set(catalog_entry->dirty_schema_,
                                    nullptr,
                                    catalog_entry->dirty_schema_version_);
                }
            }
            else
            {
                assert(req.Payload() != nullptr);
                schema_rec = static_cast<CatalogRecord *>(req.Payload());
            }
            break;
        }
        case PostWriteType::Commit:
        {
            // Schema operations always employ multi-stage commits.
            assert(true);
            break;
        }
        case PostWriteType::DowngradeLock:
        {
            // the request commits nothing and only downgrade the write
            // locks to write intent.
            return TemplateCcMap::Execute(req);
        }
        default:
            break;
        }

        assert(catalog_entry != nullptr);

        // When the request commits the schema operation, modifies the cc
        // map(s) at this shard.
        const TableSchema *old_schema = catalog_entry->schema_.get();
        const TableSchema *new_schema = catalog_entry->dirty_schema_.get();
        if (req.CommitType() == PostWriteType::PostCommit &&
            catalog_entry->dirty_schema_version_ > 0)
        {
            if (req.OpType() == OperationType::TruncateTable)
            {
                assert(table_key->Name().Engine() == TableEngine::EloqKv);
                // A remote tx is allowed to acquire write intents/locks and
                // drop a table, even if the table's schema has not been
                // initialized at this node. The earlier acquiring-write-intent
                // request creates a schema cc entry in the catalog cc map and a
                // node-level schema view. The version timestamp of the schema
                // is 0, if the schema is uninitialized (null). Or, the current
                // schema must not be null.

                assert(catalog_entry->schema_version_ == 0 ||
                       old_schema != nullptr);
                assert(new_schema->Version() ==
                       catalog_entry->dirty_schema_version_);

                // Sync ddl op to standby nodes.
                if (shard_->core_id_ == 0 &&
                    !shard_->GetSubscribedStandbys().empty())
                {
                    StandbyForwardEntry *forward_entry =
                        shard_->GetNextStandbyForwardEntry();
                    auto *forward_req = &forward_entry->Request();
                    forward_req->set_primary_leader_term(ng_term);
                    forward_req->set_tx_number(req.Txn());
                    forward_req->set_table_name(table_name_.String());
                    forward_req->set_table_type(
                        remote::ToRemoteType::ConvertTableType(
                            table_name_.Type()));
                    forward_req->set_table_engine(
                        remote::ToRemoteType::ConvertTableEngine(
                            table_name_.Engine()));
                    forward_req->set_key_shard_code(cc_ng_id_ << 10);
                    std::string key_str;
                    table_key->Serialize(key_str);
                    forward_req->set_key(std::move(key_str));
                    std::string rec_str;

                    assert(catalog_entry->schema_ != nullptr);
                    assert(catalog_entry->dirty_schema_ != nullptr);
                    assert(catalog_entry->dirty_schema_version_ > 0);

                    CatalogRecord temp_rec;
                    temp_rec.SetSchemaImage(
                        catalog_entry->schema_->SchemaImage());
                    temp_rec.SetDirtySchemaImage(
                        catalog_entry->dirty_schema_->SchemaImage());
                    assert(!temp_rec.SchemaImage().empty());
                    assert(!temp_rec.DirtySchemaImage().empty());

                    temp_rec.Serialize(rec_str);
                    forward_req->add_cmd_list(std::move(rec_str));

                    forward_req->set_commit_ts(
                        catalog_entry->dirty_schema_version_);
                    forward_req->set_schema_version(
                        catalog_entry->schema_version_);
                    shard_->ForwardStandbyMessage(forward_entry);
                }
            }
            else if (req.OpType() == OperationType::DropTable)
            {
                // A remote tx is allowed to acquire write intents/locks and
                // drop a table, even if the table's schema has not been
                // initialized at this node. The earlier acquiring-write-intent
                // request creates a schema cc entry in the catalog cc map and a
                // node-level schema view. The version timestamp of the schema
                // is 0, if the schema is uninitialized (null). Or, the current
                // schema must not be null.
                assert(catalog_entry->schema_version_ == 0 ||
                       old_schema != nullptr);

                // This is a DROP TABLE statement. Drops the cc maps
                // associated with the table in the final commit step.
                shard_->DropCcm(table_key->Name(), req.NodeGroupId());

                if (!table_key->Name().IsHashPartitioned())
                {
                    // Drop range table if exist
                    TableName range_table_name{table_key->Name().StringView(),
                                               TableType::RangePartition,
                                               table_key->Name().Engine()};
                    shard_->DropCcm(range_table_name, req.NodeGroupId());
                }

                if (old_schema != nullptr)
                {
                    std::vector<TableName> index_names =
                        old_schema->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        shard_->DropCcm(index_name, req.NodeGroupId());
                        // Drop range table if exist
                        TableName index_range_table_name{
                            index_name.StringView(),
                            TableType::RangePartition,
                            index_name.Engine()};
                        shard_->DropCcm(index_range_table_name,
                                        req.NodeGroupId());
                    }
                }
            }
            else if (req.OpType() == OperationType::CreateTable)
            {
                assert(catalog_entry->dirty_schema_version_ > 0 &&
                       new_schema != nullptr);

                // This is a CREATE TABLE statement. Creates the cc maps
                // associated with the table in the final commit step.
                shard_->CreateOrUpdatePkCcMap(table_key->Name(),
                                              new_schema,
                                              req.NodeGroupId(),
                                              true,
                                              true);

                std::vector<TableName> index_names = new_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    shard_->CreateOrUpdateSkCcMap(
                        index_name, new_schema, req.NodeGroupId());
                }
            }
            // Alter Table
            else
            {
                assert(old_schema != nullptr && new_schema != nullptr);
                // Update pk cc map using new schema.
                shard_->CreateOrUpdatePkCcMap(
                    table_key->Name(), new_schema, req.NodeGroupId(), false);

                if (!table_key->Name().IsHashPartitioned())
                {
                    // Update pk range table if exist.
                    TableName base_range_table_name{
                        table_key->Name().StringView(),
                        TableType::RangePartition,
                        table_key->Name().Engine()};
                    auto ranges = shard_->GetTableRangesForATable(
                        base_range_table_name, req.NodeGroupId());
                    if (ranges != nullptr)
                    {
                        shard_->CreateOrUpdateRangeCcMap(
                            base_range_table_name,
                            new_schema,
                            req.NodeGroupId(),
                            catalog_entry->dirty_schema_version_,
                            false);
                    }
                }

                if (req.OpType() == OperationType::AddIndex ||
                    req.OpType() == OperationType::DropIndex ||
                    req.OpType() == OperationType::Update)
                {
                    std::vector<TableName> new_index_names =
                        new_schema->IndexNames();
                    std::vector<TableName> old_index_names =
                        old_schema->IndexNames();

                    for (const TableName &old_index_name : old_index_names)
                    {
                        if (std::find(new_index_names.begin(),
                                      new_index_names.end(),
                                      old_index_name) != new_index_names.end())
                        {
                            // Update current sk cc map using new schema.
                            shard_->CreateOrUpdateSkCcMap(old_index_name,
                                                          new_schema,
                                                          req.NodeGroupId(),
                                                          false);
                            // Update current sk range table if exist.
                            TableName index_range_table_name{
                                old_index_name.StringView(),
                                TableType::RangePartition,
                                old_index_name.Engine()};
                            auto ranges = shard_->GetTableRangesForATable(
                                index_range_table_name, req.NodeGroupId());
                            if (ranges != nullptr)
                            {
                                shard_->CreateOrUpdateRangeCcMap(
                                    index_range_table_name,
                                    new_schema,
                                    req.NodeGroupId(),
                                    catalog_entry->dirty_schema_version_,
                                    false);
                            }
                        }
                        else
                        {
                            assert(req.OpType() == OperationType::DropIndex);
                            // Remove sk cc map for dropped index.
                            shard_->DropCcm(old_index_name, req.NodeGroupId());

                            // range table operation.
                            // Drop range table if exist
                            TableName old_index_range_table_name{
                                old_index_name.StringView(),
                                TableType::RangePartition,
                                old_index_name.Engine()};
                            shard_->DropCcm(old_index_range_table_name,
                                            req.NodeGroupId());
                        }
                    }
                }  // End of alter table index
            }

            UpdateTableLocks(table_key->Name().StringView(),
                             catalog_entry->dirty_schema_version_,
                             cce_ptr->GetKeyLock());
        }
        else if ((req.CommitType() == PostWriteType::PrepareCommit ||
                  req.CommitType() == PostWriteType::UpdateDirty) &&
                 catalog_entry->dirty_schema_version_ > 0)
        {
            // Prepare commit. For certain schema operations, e.g., create
            // secondary index, the cc map is modified in the prepare commit
            // step.
            // ALTER TABLE statement (include CREATE/DROP INDEX)
            if (req.OpType() == OperationType::AddIndex)
            {
                std::vector<TableName> new_index_names =
                    new_schema->IndexNames();
                std::vector<TableName> old_index_names =
                    old_schema->IndexNames();
                bool found = false;
                for (const TableName &new_index_name : new_index_names)
                {
                    found = false;
                    for (const auto &old_index_name : old_index_names)
                    {
                        if (!new_index_name.String().compare(
                                old_index_name.String()))
                        {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        // In this step, just create cc map for new sk.
                        // We will update current sk ccmap in PostCommit.
                        bool is_create =
                            req.CommitType() == PostWriteType::PrepareCommit;
                        shard_->CreateOrUpdateSkCcMap(new_index_name,
                                                      new_schema,
                                                      req.NodeGroupId(),
                                                      is_create);

                        // New sk range cc map should use the dirty schema
                        const TableName new_index_range_name{
                            new_index_name.StringView(),
                            TableType::RangePartition,
                            new_index_name.Engine()};
                        shard_->CreateOrUpdateRangeCcMap(
                            new_index_range_name,
                            new_schema,
                            req.NodeGroupId(),
                            catalog_entry->dirty_schema_version_,
                            is_create);
                    }
                }
            }
        }

        if (req.CommitType() == PostWriteType::PostCommit &&
            shard_->core_id_ == shard_->core_cnt_ - 1 &&
            catalog_entry->dirty_schema_version_ > 0)
        {
            // If this is a drop table req, drop the range table also
            // Drop table range before drop catalog
            if (req.OpType() == OperationType::DropTable)
            {
                shard_->CleanTableStatistics(table_key->Name(), cc_ng_id_);
                if (!table_key->Name().IsHashPartitioned())
                {
                    TableName range_table_name{table_key->Name().StringView(),
                                               TableType::RangePartition,
                                               table_key->Name().Engine()};
                    shard_->CleanTableRange(range_table_name,
                                            req.NodeGroupId());
                    if (old_schema != nullptr)
                    {
                        std::vector<TableName> index_names =
                            old_schema->IndexNames();
                        for (const TableName &index_name : index_names)
                        {
                            // Drop range table if exist
                            TableName index_range_table_name{
                                index_name.StringView(),
                                TableType::RangePartition,
                                index_name.Engine()};
                            shard_->CleanTableRange(index_range_table_name,
                                                    req.NodeGroupId());
                        }
                    }
                }
            }
            else if (req.OpType() == OperationType::AddIndex ||
                     req.OpType() == OperationType::DropIndex)
            {
                std::vector<TableName> new_index_names =
                    new_schema->IndexNames();
                std::vector<TableName> old_index_names =
                    old_schema->IndexNames();
                for (const TableName &old_index_name : old_index_names)
                {
                    // Drop old index range table if not exist any more
                    if (std::find(new_index_names.begin(),
                                  new_index_names.end(),
                                  old_index_name) == new_index_names.end())
                    {
                        TableName old_index_range_table_name{
                            old_index_name.StringView(),
                            TableType::RangePartition,
                            old_index_name.Engine()};
                        shard_->CleanTableRange(old_index_range_table_name,
                                                req.NodeGroupId());
                        Statistics *statistics =
                            old_schema->StatisticsObject().get();
                        statistics->DropIndex(old_index_name);
                    }
                }
            }
            catalog_entry->CommitDirtySchema();
        }

        return TemplateCcMap::Execute(req);
    }

    bool Execute(ReadCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        assert(req.IsLocal());

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        ng_term = std::max(ng_term, Sharder::Instance().StandbyNodeTerm());

        if (req.IsInRecovering())
        {
            ng_term = ng_term > 0
                          ? ng_term
                          : Sharder::Instance().CandidateLeaderTerm(ng_id);
        }

        CODE_FAULT_INJECTOR("term_CatalogCcMap_Execute_ReadCc", {
            LOG(INFO) << "FaultInject  term_CatalogCcMap_Execute_ReadCc";
            ng_term = -1;
            FaultInject::Instance().InjectFault(
                "term_CatalogCcMap_Execute_ReadCc", "remove");
        });

        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const CatalogKey *table_key =
            static_cast<const CatalogKey *>(req.Key());
        bool emplace = false;
        Iterator it = FindEmplace(*table_key, emplace, true, false);
        CcEntry<CatalogKey, CatalogRecord, true, false> *cce = it->second;
        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            const CatalogEntry *catalog_entry =
                shard_->GetCatalog(table_key->Name(), req.NodeGroupId());

            // If the read toward the catalog cc entry acquires the read
            // lock but the cc entry does not contain the value, checks if
            // the catalog has been constructed at this node. If so, turns
            // this request into a read-outside request that installs the
            // value in the cc entry.
            if (catalog_entry != nullptr && catalog_entry->schema_version_ > 0)
            {
                if (catalog_entry->schema_ != nullptr)
                {
                    {
#ifdef STATISTICS
                        // Initialize table statistics before create ccmap.
                        if (!shard_->LoadRangesAndStatisticsNx(
                                catalog_entry->schema_.get(),
                                req.NodeGroupId(),
                                ng_term,
                                &req))
                        {
                            return false;
                        }
#endif
                    }

                    // upload catalog record
                    cce->payload_.PassInCurrentPayload(
                        std::make_unique<CatalogRecord>());
                    cce->payload_.cur_payload_->Set(
                        catalog_entry->schema_,
                        catalog_entry->dirty_schema_,
                        catalog_entry->schema_version_);
                    cce->SetCommitTsPayloadStatus(
                        catalog_entry->schema_version_, RecordStatus::Normal);
                }
                else
                {
                    cce->SetCommitTsPayloadStatus(
                        catalog_entry->schema_version_, RecordStatus::Deleted);
                }
            }
            else
            {
                shard_->FetchCatalog(
                    table_key->Name(), req.NodeGroupId(), ng_term, &req);
                return false;
            }
        }

        // Increase ref cnt so that when setFinish is called in
        // TemplateCcMap::Execute, the waiters cannot be woken up. We still need
        // to access sch_rec which is owned by the caller.
        CcHandlerResult<ReadKeyResult> *hd_res = req.Result();
        hd_res->SetRefCnt(2);
        bool success = TemplateCcMap::Execute(req);

        ReadKeyResult &result = req.Result()->Value();
        CatalogRecord *sch_rec = static_cast<CatalogRecord *>(req.Record());

        // Returns the reader-writer control block to runtime when there is no
        // writer and no pending dirty schema. The control block provides a fast
        // path for runtime to keep a cache of the schema and to coordinate
        // modifications of the schema.
        if (success && shard_->IsNative(ng_id) &&
            result.rec_status_ == RecordStatus::Normal &&
            sch_rec->DirtySchema() == nullptr)
        {
            std::shared_ptr<ReaderWriterObject<TableSchema>> sch_cntl =
                shard_->FindEmplaceSchemaCntl(table_key->Name());
            if (sch_cntl->HasNoWriter())
            {
                if (sch_cntl->GetObjectPtr() == nullptr)
                {
                    sch_cntl->SetObject(sch_rec->CopySchema());
                }
                sch_rec->SetSchemaCntl(std::move(sch_cntl));
            }
        }
        hd_res->SetFinished();

        return success;
    }

    bool Execute(ReplayLogCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);
        int64_t ng_term =
            Sharder::Instance().CandidateLeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            LOG(INFO) << "ReplayLogCc, node_group(#" << req.NodeGroupId()
                      << ") term < 0, tx:" << req.Txn();
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return false;
        }

        ::txlog::SchemaOpMessage schema_op_msg;
        const std::string_view &content = req.LogContentView();
        schema_op_msg.ParseFromArray(content.data(), content.length());

        const CatalogEntry *catalog_entry = nullptr;

        // Need to parse the string if not include table type in protobuf
        TableType table_type = ::txlog::ToLocalType::ConvertCcTableType(
            schema_op_msg.table_type());
        TableEngine table_engine = ::txlog::ToLocalType::ConvertTableEngine(
            schema_op_msg.table_engine());
        std::string_view table_name_sv{schema_op_msg.table_name_str()};
        TableName table_name{table_name_sv, table_type, table_engine};

        if (shard_->core_id_ == 0)
        {
            CatalogKey table_key(table_name);
            Iterator it = Find(table_key);
            CcEntry<CatalogKey, CatalogRecord, true, false> *cce = it->second;
            if (cce != nullptr)
            {
                req.SetFinish();
                return true;
            }
        }

        std::pair<bool, CcErrorCode> &upsert_kv_err_code =
            req.UpsertKvErrCode();
        bool need_to_upsert_kv_table = upsert_kv_err_code.first;

        // 1. Replay the table catalog and table schema.
        // The first shard is in charge of creating catalog_entry.
        if (shard_->core_id_ == 0)
        {
            if (schema_op_msg.stage() ==
                    ::txlog::SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_PrepareSchema ||
                schema_op_msg.stage() == ::txlog::SchemaOpMessage_Stage::
                                             SchemaOpMessage_Stage_PrepareData)
            {
                // If we are recovering from prepare log, we need to restore to
                // the state right before commit log is flushed, so both current
                // and dirty schema are needed.
                auto [success, new_catalog_entry] = shard_->CreateReplayCatalog(
                    table_name,
                    req.NodeGroupId(),
                    schema_op_msg.old_catalog_blob(),
                    schema_op_msg.new_catalog_blob(),
                    schema_op_msg.catalog_ts(),
                    req.CommitTs());
                if (!success)
                {
                    // create fail, the catalog to be created is out of date
                    LOG(INFO)
                        << "create catalog fails, table name: " << table_name_sv
                        << ", catalog entry of the same or higher "
                           "version exists, stop replaying this schema op";
                    req.SetFinish();
                    return false;
                }
                catalog_entry = new_catalog_entry;
            }
            else
            {
                /*
                If we are recovering as participant from commit stage,
                we don't need to do kv_upsert_table_op_ so we can directly
                recover to the state before commit log is cleaned, that is:

                1. if dirty_schema_commit_ts>0, dirty schema is commited as
                current schema and write lock has been released.

                2. if dirty_schema_commit_ts=0, old schema is restored as
                current schema and write lock has been released.

                P.S. If it is create table statement and
                dirty_schema_commit_ts=0, the restored CatalogEntry has schema_
                set to nullptr and schema_version_ set to 1.
                */

                uint64_t dirty_schema_commit_ts = req.CommitTs();
                uint64_t old_schema_commit_ts = schema_op_msg.catalog_ts();

                // upsert kv store first, then create catalog entry
                if (need_to_upsert_kv_table && dirty_schema_commit_ts > 0 &&
                    !Sharder::Instance()
                         .GetDataStoreHandler()
                         ->IsSharedStorage())
                {
                    // When not using shared storage, the coordinator and
                    // participants must upsert the KV table during recovery
                    // process. Otherwise, there may be inconsistency between
                    // in-memory schema and kv store schema.

                    TableSchema::uptr schema_ptr =
                        Sharder::Instance()
                            .GetLocalCcShards()
                            ->CreateTableSchemaFromImage(
                                table_name,
                                schema_op_msg.old_catalog_blob(),
                                old_schema_commit_ts);
                    TableSchema::uptr dirty_schema_ptr =
                        Sharder::Instance()
                            .GetLocalCcShards()
                            ->CreateTableSchemaFromImage(
                                table_name,
                                schema_op_msg.new_catalog_blob(),
                                dirty_schema_commit_ts);

                    OperationType op_type = static_cast<OperationType>(
                        schema_op_msg.table_op().op_type());

                    upsert_kv_err_code = {false, CcErrorCode::NO_ERROR};
                    shard_->local_shards_.store_hd_->UpsertTable(
                        schema_ptr.get(),
                        dirty_schema_ptr.get(),
                        op_type,
                        req.CommitTs(),
                        cc_ng_id_,
                        ng_term,
                        nullptr,
                        nullptr,
                        &req,
                        shard_,
                        &upsert_kv_err_code.second);

                    return false;
                }
                else if (!need_to_upsert_kv_table &&
                         upsert_kv_err_code.second != CcErrorCode::NO_ERROR)
                {
                    req.Result()->SetError(upsert_kv_err_code.second);
                    return false;
                }

                auto [success, new_catalog_entry] = shard_->CreateCatalog(
                    table_name,
                    req.NodeGroupId(),
                    dirty_schema_commit_ts > 0
                        ? schema_op_msg.new_catalog_blob()
                        : schema_op_msg.old_catalog_blob(),
                    dirty_schema_commit_ts > 0 ? dirty_schema_commit_ts
                                               : old_schema_commit_ts);

                assert(new_catalog_entry != nullptr);
                if (!success)
                {
                    // create fail, the catalog to be created is out of date
                    LOG(INFO)
                        << "create catalog entry fails, table name: "
                        << table_name_sv
                        << ", catalog entry of the same or higher version "
                           "exists, stop replaying this schema op";
                    req.SetFinish();
                    return false;
                }
                catalog_entry = new_catalog_entry;
            }

            if (catalog_entry->schema_)
            {
                if (!table_name.IsHashPartitioned() &&
                    !shard_->LoadRangesAndStatisticsNx(
                        catalog_entry->schema_.get(),
                        req.NodeGroupId(),
                        ng_term,
                        &req))
                {
                    return false;
                }

                // Load range and stats for the new added indexes.
                if (catalog_entry->dirty_schema_)
                {
                    std::vector<TableName> new_index_names =
                        catalog_entry->dirty_schema_->IndexNames();
                    std::vector<TableName> old_index_names =
                        catalog_entry->schema_->IndexNames();
                    for (const TableName &new_index_name : new_index_names)
                    {
                        if (std::find(old_index_names.begin(),
                                      old_index_names.end(),
                                      new_index_name) == old_index_names.end())
                        {
                            TableName index_range_name{
                                new_index_name.StringView(),
                                TableType::RangePartition,
                                new_index_name.Engine()};
                            auto ranges = shard_->GetTableRangesForATable(
                                index_range_name, req.NodeGroupId());
                            if (ranges == nullptr)
                            {
                                shard_->FetchTableRanges(index_range_name,
                                                         &req,
                                                         req.NodeGroupId(),
                                                         ng_term);
                                return false;
                            }

                            catalog_entry->dirty_schema_->StatisticsObject()
                                ->CreateIndex(
                                    new_index_name,
                                    catalog_entry->dirty_schema_
                                        ->IndexKeySchema(new_index_name),
                                    req.NodeGroupId());
                        }
                    }
                }
            }
        }
        else
        {
            // other cores
            catalog_entry = shard_->GetCatalog(table_name, req.NodeGroupId());
            assert(catalog_entry != nullptr);
        }

        // 2. Replay the table ccmap.
        if (schema_op_msg.stage() == ::txlog::SchemaOpMessage_Stage::
                                         SchemaOpMessage_Stage_PrepareSchema ||
            schema_op_msg.stage() == ::txlog::SchemaOpMessage_Stage::
                                         SchemaOpMessage_Stage_PrepareData)
        {
            const TableSchema *old_schema = catalog_entry->schema_.get();
            const TableSchema *new_schema = catalog_entry->dirty_schema_.get();
            if (old_schema != nullptr && new_schema != nullptr)
            {
                // Alter table index operation.
                // Pk table ccmap using old schema.
                shard_->CreateOrUpdatePkCcMap(
                    table_name, old_schema, req.NodeGroupId());

                if (!table_name.IsHashPartitioned())
                {
                    // Pk range table ccmap
                    const TableName base_range_name{table_name.StringView(),
                                                    TableType::RangePartition,
                                                    table_name.Engine()};
                    shard_->CreateOrUpdateRangeCcMap(
                        base_range_name,
                        old_schema,
                        req.NodeGroupId(),
                        catalog_entry->schema_version_);
                }

                // Old sk table ccmap using old schema.
                std::vector<TableName> old_index_names =
                    old_schema->IndexNames();
                for (const auto &old_index_name : old_index_names)
                {
                    shard_->CreateOrUpdateSkCcMap(
                        old_index_name, old_schema, req.NodeGroupId());
                    // old sk range table ccmap
                    const TableName old_index_range_name{
                        old_index_name.StringView(),
                        TableType::RangePartition,
                        old_index_name.Engine()};
                    shard_->CreateOrUpdateRangeCcMap(
                        old_index_range_name,
                        old_schema,
                        req.NodeGroupId(),
                        catalog_entry->schema_version_);
                }

                // New sk table ccmap using new schema
                std::vector<TableName> new_index_names =
                    new_schema->IndexNames();
                for (const auto &new_index_name : new_index_names)
                {
                    if (std::find(old_index_names.cbegin(),
                                  old_index_names.cend(),
                                  new_index_name) == old_index_names.cend())
                    {
                        shard_->CreateOrUpdateSkCcMap(
                            new_index_name, new_schema, req.NodeGroupId());

                        // New sk range cc maps should use the dirty schema
                        const TableName new_index_range_name{
                            new_index_name.StringView(),
                            TableType::RangePartition,
                            new_index_name.Engine()};
                        shard_->CreateOrUpdateRangeCcMap(
                            new_index_range_name,
                            new_schema,
                            req.NodeGroupId(),
                            catalog_entry->dirty_schema_version_);
                    }
                }
            }
        }

        CatalogKey table_key(table_name);
        Iterator it = FindEmplace(table_key);
        CcEntry<CatalogKey, CatalogRecord, true, false> *cce = it->second;
        CcPage<CatalogKey, CatalogRecord, true, false> *ccp = it.GetPage();

        if (cce == nullptr)
        {
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        // 3. Replay the table write intent/lock.
        LockType lock_type = LockType::NoLock;
        TableName base_table_name(table_name.GetBaseTableNameSV(),
                                  TableType::Primary,
                                  table_name.Engine());
        OperationType op_type =
            static_cast<OperationType>(schema_op_msg.table_op().op_type());
        switch (schema_op_msg.stage())
        {
        case ::txlog::SchemaOpMessage_Stage::
            SchemaOpMessage_Stage_PrepareSchema:
        {
            // If the prepare log has been flushed, the recovered cc ng leader
            // replays all steps between the prepare log and the commit log,
            // including the write lock on the schema. The recovered write lock
            // is special in that the holding tx is not associated with the tx's
            // term (always set to 0). This is because after the prepare log,
            // the write lock on the schema and the coordinating tx are
            // guaranteed to be recovered upon failures. The tx's term is not
            // necessary here to mark whether or not if the coordinating tx has
            // failed or not.
            if (req.RangeSplitting(base_table_name) ||
                op_type == OperationType::AddIndex)
            {
                // If range splitting is also happening on this table, which
                // must have acquired a read lock on the catalog entry, that
                // means we only need to recover a write intent.
                // For add index operation, between the prepare log and the
                // prepare index table log, the recovered cc ng leader should
                // recover the write intent on the schema.
                lock_type = LockType::WriteIntent;
            }
            else
            {
                lock_type = LockType::WriteLock;
            }
            break;
        }
        case ::txlog::SchemaOpMessage_Stage::SchemaOpMessage_Stage_PrepareData:
        {
            lock_type = LockType::WriteIntent;
            break;
        }
        case ::txlog::SchemaOpMessage_Stage::SchemaOpMessage_Stage_CommitSchema:
        {
            break;
        }
        default:
            break;
        }

        if (lock_type == LockType::WriteIntent)
        {
            auto lock_pair = AcquireCceKeyLock(cce,
                                               cce->CommitTs(),
                                               ccp,
                                               cce->PayloadStatus(),
                                               &req,
                                               req.NodeGroupId(),
                                               ng_term,
                                               0,
                                               CcOperation::ReadForWrite,
                                               IsolationLevel::RepeatableRead,
                                               CcProtocol::OCC,
                                               0,
                                               false);
            assert(lock_pair.first == LockType::WriteIntent &&
                   lock_pair.second == CcErrorCode::NO_ERROR);
            // This silences the -Wunused-but-set-variable warning without any
            // runtime overhead.
            (void) lock_pair;
        }
        else if (lock_type == LockType::WriteLock)
        {
            auto lock_pair = AcquireCceKeyLock(cce,
                                               cce->CommitTs(),
                                               ccp,
                                               cce->PayloadStatus(),
                                               &req,
                                               req.NodeGroupId(),
                                               ng_term,
                                               0,
                                               CcOperation::Write,
                                               IsolationLevel::RepeatableRead,
                                               CcProtocol::Locking,
                                               0,
                                               false);
            // When a cc node recovers, no one should be holding read locks.
            // So, the acquire operation should always succeed.
            assert(lock_pair.first == LockType::WriteLock &&
                   lock_pair.second == CcErrorCode::NO_ERROR);
            // This silences the -Wunused-but-set-variable warning without any
            // runtime overhead.
            (void) lock_pair;
        }

        if (cce->payload_.cur_payload_ == nullptr)
        {
            cce->payload_.PassInCurrentPayload(
                std::make_unique<CatalogRecord>());
        }
        cce->payload_.cur_payload_->Set(catalog_entry->schema_,
                                        catalog_entry->dirty_schema_,
                                        catalog_entry->schema_version_);

        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
        }
        else
        {
            uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;
            bool is_coordinator = tx_node_id == req.NodeGroupId();
            int64_t tx_candidate_term =
                Sharder::Instance().CandidateLeaderTerm(tx_node_id);

            if (is_coordinator && tx_candidate_term >= 0)
            {
                // If the coordinating tx is bound to the recoverying cc
                // node, resumes the tx. This will spawn
                // a worker thread that will restore the transaction. It will
                // call SetFinish() after acqruing all needed locks.
                shard_->local_shards_.CreateSchemaRecoveryTx(
                    req, schema_op_msg, tx_candidate_term);
            }
            else
            {
                req.SetFinish();
            }
        }

        return false;
    }

    bool Execute(BroadcastStatisticsCc &req) override
    {
        CcHandlerResult<Void> *hd_res = req.Result();

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const TableName &table_name = *req.SamplingTableName();

        DLOG(INFO) << "Receive table statistics " << table_name.StringView()
                   << ". From ng #" << req.SamplePool()->ng_id() << ", to ng #"
                   << req.NodeGroupId()
                   << ". ng_records: " << req.SamplePool()->records()
                   << ", ng_samples: " << req.SamplePool()->samples_size();

        TableName base_table_name(table_name.GetBaseTableNameSV(),
                                  TableType::Primary,
                                  table_name.Engine());
        CatalogKey table_key(base_table_name);
        Iterator it = FindEmplace(table_key);
        CcEntry<CatalogKey, CatalogRecord, true, false> *cce = it->second;
        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            const CatalogEntry *catalog_entry =
                shard_->GetCatalog(base_table_name, req.NodeGroupId());
            if (catalog_entry != nullptr)
            {
                if (catalog_entry->schema_ != nullptr)
                {
                    // Initialize table statistics before create ccmap.
                    if (!table_name.IsHashPartitioned() &&
                        !shard_->LoadRangesAndStatisticsNx(
                            catalog_entry->schema_.get(),
                            req.NodeGroupId(),
                            ng_term,
                            &req))
                    {
                        return false;  // Loading...
                    }

                    // upload catalog record
                    cce->payload_.PassInCurrentPayload(
                        std::make_unique<CatalogRecord>());
                    cce->payload_.cur_payload_->Set(
                        catalog_entry->schema_,
                        catalog_entry->dirty_schema_,
                        catalog_entry->schema_version_);
                    cce->SetCommitTsPayloadStatus(
                        catalog_entry->schema_version_, RecordStatus::Normal);
                }
                else
                {
                    cce->SetCommitTsPayloadStatus(
                        catalog_entry->schema_version_, RecordStatus::Deleted);
                }
            }
            else
            {
                if (Statistics::LeaderNodeGroup(base_table_name) !=
                    req.NodeGroupId())
                {
                    hd_res->SetFinished();
                    return true;
                }
                else
                {
                    shard_->FetchCatalog(
                        base_table_name, req.NodeGroupId(), ng_term, &req);
                    return false;
                }
            }
        }

        if (cce->PayloadStatus() == RecordStatus::Normal)
        {
            const StatisticsEntry *statistics_entry =
                shard_->GetTableStatistics(base_table_name, req.NodeGroupId());
            assert(statistics_entry && statistics_entry->statistics_);
            const auto table_schema = [&req, cce]() -> const TableSchema *
            {
                if (cce->payload_.cur_payload_->Schema() &&
                    cce->payload_.cur_payload_->Schema()->Version() ==
                        req.SchemaVersion())
                {
                    return cce->payload_.cur_payload_->Schema();
                }
                else if (cce->payload_.cur_payload_->DirtySchema() &&
                         cce->payload_.cur_payload_->DirtySchema()->Version() ==
                             req.SchemaVersion())
                {
                    return cce->payload_.cur_payload_->DirtySchema();
                }
                else
                {
                    return nullptr;
                }
            }();

            if (table_schema)
            {
                statistics_entry->statistics_->OnRemoteStatisticsMessage(
                    table_name, table_schema, *req.SamplePool());
            }
        }

        hd_res->SetFinished();
        return true;
    }

    bool Execute(KeyObjectStandbyForwardCc &req) override
    {
        uint64_t commit_ts = req.CommitTs();

        const CatalogKey *table_key = nullptr;
        const std::string *key_str = req.KeyImage();
        assert(key_str != nullptr);
        std::unique_ptr<CatalogKey> decoded_key =
            std::make_unique<CatalogKey>();
        size_t offset = 0;
        decoded_key->Deserialize(key_str->data(), offset, KeySchema());
        table_key = decoded_key.get();

        CcEntry<CatalogKey, CatalogRecord, true, false> *cce;
        CcPage<CatalogKey, CatalogRecord, true, false> *ccp;
        if (req.CcePtr())
        {
            cce =
                static_cast<CcEntry<CatalogKey, CatalogRecord, true, false> *>(
                    req.CcePtr());
            ccp = static_cast<CcPage<CatalogKey, CatalogRecord, true, false> *>(
                cce->GetCcPage());
        }
        else
        {
            Iterator it = FindEmplace(*table_key);
            cce = it->second;
            ccp = it.GetPage();
        }

        // If the current node has not initialized the catalog entry, we fetch
        // the newest catalog from the primary node.
        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            const CatalogEntry *catalog_entry =
                shard_->GetCatalog(table_key->Name(), req.NodeGroupId());
            if (catalog_entry != nullptr && catalog_entry->schema_version_ > 0)
            {
                if (catalog_entry->schema_ != nullptr)
                {
                    // upload catalog record
                    cce->payload_.PassInCurrentPayload(
                        std::make_unique<CatalogRecord>());
                    cce->payload_.cur_payload_->Set(
                        catalog_entry->schema_,
                        catalog_entry->dirty_schema_,
                        catalog_entry->schema_version_);
                    // update commit ts
                    cce->SetCommitTsPayloadStatus(
                        catalog_entry->schema_version_, RecordStatus::Normal);
                }
                else
                {
                    cce->SetCommitTsPayloadStatus(
                        catalog_entry->schema_version_, RecordStatus::Deleted);
                }
            }
            else
            {
                // fetch catalog from primary node
                shard_->FetchCatalog(table_key->Name(),
                                     req.NodeGroupId(),
                                     req.StandbyNodeTerm(),
                                     &req);
                return false;
            }
        }

        if (commit_ts < cce->CommitTs())
        {
            // discard outdate request
            if (shard_->core_id_ + 1 == shard_->core_cnt_)
            {
                return req.SetFinish(*shard_);
            }
            else
            {
                MoveRequest(&req, shard_->core_id_ + 1);
                return false;
            }
        }

        // The catalog entry might be directly fetched from the
        // primary node, so we still need to execute UpsertTable in the store
        // handler. Otherwise, the kv storage will not have the latest kv table.
        bool same_commit_ts = (commit_ts == cce->CommitTs());

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;

        CODE_FAULT_INJECTOR("standby_forward_ddl_trigger_start_following", {
            // only trigger once
            txservice::FaultInject::Instance().InjectFault(
                "standby_forward_ddl_trigger_start_following", "remove");
            DLOG(INFO)
                << "fault inject: standby_forward_ddl_trigger_start_following";
            NodeGroupId native_ng = Sharder::Instance().NativeNodeGroup();
            int64_t cur_prim_term = Sharder::Instance().PrimaryNodeTerm();
            assert(cur_prim_term > 0);
            Sharder::Instance().OnStartFollowing(
                native_ng,
                cur_prim_term,
                Sharder::Instance().LeaderNodeId(native_ng),
                true);

            return req.SetFinish(*shard_);
        });

        if (req.GetDDLPhase() ==
            KeyObjectStandbyForwardCc::DDLPhase::AcquirePhase)
        {
            // acquire write lock on catalog rec
            if (req.CcePtr())
            {
                // resumed req.
                std::tie(acquired_lock, err_code) = LockHandleForResumedRequest(
                    cce,
                    cce->CommitTs(),
                    cce->PayloadStatus(),
                    &req,
                    Sharder::Instance().NativeNodeGroup(),
                    req.StandbyNodeTerm(),
                    req.TxTerm(),
                    CcOperation::Write,
                    IsolationLevel::RepeatableRead,
                    CcProtocol::Locking,
                    0,
                    false);
                assert(acquired_lock == LockType::WriteLock &&
                       err_code == CcErrorCode::NO_ERROR);
                req.SetCcePtr(nullptr);
            }
            else
            {
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(cce,
                                      cce->CommitTs(),
                                      ccp,
                                      cce->PayloadStatus(),
                                      &req,
                                      req.NodeGroupId(),
                                      req.StandbyNodeTerm(),
                                      req.TxTerm(),
                                      CcOperation::Write,
                                      IsolationLevel::RepeatableRead,
                                      CcProtocol::Locking,
                                      0,
                                      false);
            }
            switch (err_code)
            {
            case CcErrorCode::NO_ERROR:
            {
                if (shard_->core_id_ != shard_->core_cnt_ - 1)
                {
                    MoveRequest(&req, shard_->core_id_ + 1);
                    return false;
                }
                break;
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                req.SetCcePtr(cce);
                return false;
            }
            default:
            {
                // lock confilct: back off and retry.
                assert(false);
            }
            }  //-- end: switch
        }

        if (shard_->core_id_ == shard_->core_cnt_ - 1)
        {
            if (req.GetDDLPhase() ==
                KeyObjectStandbyForwardCc::DDLPhase::AcquirePhase)
            {
                // after locks are acquired on all cores, update catalog schema
                // in local cc shards.
                CatalogRecord tmp_rec;
                const std::string_view payload_sv = req.CommandList()->front();
                size_t offset = 0;
                tmp_rec.Deserialize(payload_sv.data(), offset);
                auto res =
                    shard_->CreateDirtyCatalog(table_key->Name(),
                                               cc_ng_id_,
                                               tmp_rec.DirtySchemaImage(),
                                               commit_ts);
                const CatalogEntry *catalog_entry = res;
                // If dirty schema is created successfully, it means that the
                // schema needs to be updated in kv store.
                if (!txservice_skip_kv &&
                    !shard_->local_shards_.store_hd_->IsSharedStorage())
                {
                    if (same_commit_ts &&
                        catalog_entry->schema_version_ == commit_ts)
                    {
                        req.SetOldTableSchema(Sharder::Instance()
                                                  .GetLocalCcShards()
                                                  ->CreateTableSchemaFromImage(
                                                      table_key->Name(),
                                                      tmp_rec.SchemaImage(),
                                                      req.SchemaVersion()));
                        req.SetDDLPhase(
                            KeyObjectStandbyForwardCc::DDLPhase::KvOpPhase);
                        shard_->local_shards_.store_hd_->UpsertTable(
                            req.GetOldTableSchema(),
                            catalog_entry->schema_.get(),
                            OperationType::TruncateTable,
                            commit_ts,
                            cc_ng_id_,
                            req.StandbyNodeTerm(),
                            nullptr,
                            nullptr,
                            &req,
                            shard_,
                            &req.DDLKvOpErrorCode());
                        return false;
                    }
                    else if (catalog_entry->dirty_schema_)
                    {
                        // If using non-shared kv, we need to perform kv op on
                        // standby node as well.
                        req.SetDDLPhase(
                            KeyObjectStandbyForwardCc::DDLPhase::KvOpPhase);
                        shard_->local_shards_.store_hd_->UpsertTable(
                            catalog_entry->schema_.get(),
                            catalog_entry->dirty_schema_.get(),
                            OperationType::TruncateTable,
                            commit_ts,
                            cc_ng_id_,
                            req.StandbyNodeTerm(),
                            nullptr,
                            nullptr,
                            &req,
                            shard_,
                            &req.DDLKvOpErrorCode());
                        return false;
                    }
                }
                // If there's no need to update kv store, move to install &
                // release phase and move the req back to core 0.
                req.SetDDLPhase(
                    KeyObjectStandbyForwardCc::DDLPhase::ReleasePhase);
                MoveRequest(&req, 0);
                return false;
            }
            else if (req.GetDDLPhase() ==
                     KeyObjectStandbyForwardCc::DDLPhase::KvOpPhase)
            {
                assert(!txservice_skip_kv &&
                       !shard_->local_shards_.store_hd_->IsSharedStorage());
                if (req.DDLKvOpErrorCode() == CcErrorCode::DATA_STORE_ERR)
                {
                    auto catalog_entry =
                        shard_->GetCatalog(table_key->Name(), cc_ng_id_);
                    if (same_commit_ts &&
                        catalog_entry->schema_version_ == commit_ts)
                    {
                        // retry kv op
                        req.DDLKvOpErrorCode() = CcErrorCode::NO_ERROR;
                        shard_->local_shards_.store_hd_->UpsertTable(
                            req.GetOldTableSchema(),
                            catalog_entry->schema_.get(),
                            OperationType::TruncateTable,
                            commit_ts,
                            cc_ng_id_,
                            req.StandbyNodeTerm(),
                            nullptr,
                            nullptr,
                            &req,
                            shard_,
                            &req.DDLKvOpErrorCode());
                        return false;
                    }
                    else
                    {
                        // retry kv op
                        req.DDLKvOpErrorCode() = CcErrorCode::NO_ERROR;
                        shard_->local_shards_.store_hd_->UpsertTable(
                            catalog_entry->schema_.get(),
                            catalog_entry->dirty_schema_.get(),
                            OperationType::TruncateTable,
                            commit_ts,
                            cc_ng_id_,
                            req.StandbyNodeTerm(),
                            nullptr,
                            nullptr,
                            &req,
                            shard_,
                            &req.DDLKvOpErrorCode());
                        return false;
                    }
                }
                else if (req.DDLKvOpErrorCode() == CcErrorCode::NO_ERROR)
                {
                    req.SetDDLPhase(
                        KeyObjectStandbyForwardCc::DDLPhase::ReleasePhase);
                    MoveRequest(&req, 0);
                    return false;
                }
                else if (req.DDLKvOpErrorCode() == CcErrorCode::NG_TERM_CHANGED)
                {
                    // if ng term has changed, this cc should be rejected on
                    // ValidTermCheck() and should not reach here.
                    assert(false);
                }
                else
                {
                    assert(false);
                }
            }
        }

        if (req.GetDDLPhase() ==
            KeyObjectStandbyForwardCc::DDLPhase::ReleasePhase)
        {
            // Install new schema image in record and release the lock
            if (cce->PayloadStatus() == RecordStatus::Unknown)
            {
                cce->payload_.PassInCurrentPayload(
                    std::make_unique<CatalogRecord>());
            }
            CatalogEntry *catalog_entry =
                shard_->GetCatalog(table_key->Name(), req.NodeGroupId());
            catalog_entry->CommitDirtySchema();

            cce->payload_.cur_payload_->Set(
                catalog_entry->schema_, nullptr, commit_ts);
            cce->SetCommitTsPayloadStatus(commit_ts, RecordStatus::Normal);
            // clean up data in ccm
            CcMap *ccm = shard_->GetCcm(table_key->Name(), cc_ng_id_);
            if (ccm != nullptr && ccm->SchemaTs() < commit_ts)
            {
                shard_->DropCcm(table_key->Name(), cc_ng_id_);
                shard_->DequeueWaitListAfterSchemaUpdated();
            }

            ReleaseCceLock(cce->GetKeyLock(),
                           cce,
                           req.Txn(),
                           cc_ng_id_,
                           LockType::WriteLock,
                           false);

            UpdateTableLocks(table_key->Name().StringView(),
                             catalog_entry->schema_version_,
                             cce->GetKeyLock());

            if (shard_->core_id_ == (shard_->core_cnt_ - 1))
            {
                return req.SetFinish(*shard_);
            }
            else
            {
                MoveRequest(&req, shard_->core_id_ + 1);
                return false;
            }
        }

        // should not reach here
        assert(false);
        return true;
    }

    bool Execute(InvalidateTableCacheCc &req) override
    {
        CcHandlerResult<Void> *hd_res = req.Result();

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const TableName &base_table_name = *req.invalidate_table_name_;
        CatalogKey catalog_key(base_table_name);
        Iterator it =
            TemplateCcMap<CatalogKey, CatalogRecord, true, false>::Find(
                catalog_key);
        CcEntry<CatalogKey, CatalogRecord, true, false> *cce = it->second;
        assert(cce->GetKeyLock()->HasWriteLock(req.Txn()));

        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            const CatalogEntry *catalog_entry =
                shard_->GetCatalog(base_table_name, req.NodeGroupId());
            if (catalog_entry != nullptr)
            {
                assert(catalog_entry->schema_);
                // upload catalog record
                cce->payload_.PassInCurrentPayload(
                    std::make_unique<CatalogRecord>());
                cce->payload_.cur_payload_->Set(catalog_entry->schema_,
                                                catalog_entry->dirty_schema_,
                                                catalog_entry->schema_version_);
                cce->SetCommitTsPayloadStatus(catalog_entry->schema_version_,
                                              RecordStatus::Normal);
            }
            else
            {
                shard_->FetchCatalog(
                    base_table_name, req.NodeGroupId(), ng_term, &req);
                return false;
            }
        }

        CatalogRecord *catalog_rec = cce->payload_.cur_payload_.get();
        const TableSchema *table_schema = catalog_rec->Schema();

        TableName base_range_name(base_table_name.StringView(),
                                  TableType::RangePartition,
                                  base_table_name.Engine());
        shard_->DropCcm(base_table_name, cc_ng_id_);
        shard_->DropCcm(base_range_name, cc_ng_id_);

        for (TableName index_table_name : table_schema->IndexNames())
        {
            TableName index_range_name(index_table_name.StringView(),
                                       TableType::RangePartition,
                                       index_table_name.Engine());
            shard_->DropCcm(index_table_name, cc_ng_id_);
            shard_->DropCcm(index_range_name, cc_ng_id_);
        }

        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
            return false;
        }
        else
        {
            shard_->CleanTableRange(base_range_name, cc_ng_id_);
            shard_->CleanTableStatistics(base_table_name, cc_ng_id_);

            for (TableName index_table_name : table_schema->IndexNames())
            {
                TableName index_range_name(index_table_name.StringView(),
                                           TableType::RangePartition,
                                           index_table_name.Engine());
                shard_->CleanTableRange(index_range_name, cc_ng_id_);
                shard_->CleanTableStatistics(index_table_name, cc_ng_id_);
            }

            hd_res->SetFinished();
            return true;
        }
    }

    TableType Type() const override
    {
        return TableType::Catalog;
    }

    bool HasWriteLock(const TableName &table_name, uint32_t node_group_id)
    {
        CatalogKey table_key{TableName{
            table_name.StringView(), TableType::Primary, table_name.Engine()}};
        TxKey catalog_tx_key(&table_key);
        auto it = Find(table_key);
        if (it == End())
        {
            return false;
        }

        CcEntry<CatalogKey, CatalogRecord, true, false> *catalog_cce =
            it->second;
        const auto keylock = catalog_cce->GetKeyLock();
        if (keylock == nullptr)
        {
            return false;
        }
        return keylock->HasWriteLock();
    }

    std::tuple<CcErrorCode, NonBlockingLock *, uint64_t> ReadTable(
        const TableName &table_name,
        uint32_t node_group_id,
        int64_t ng_term,
        TxNumber tx_number)
    {
        bool read_success = false;
        uint64_t schema_version = 0;
        NonBlockingLock *lock_ptr = nullptr;

        auto lock_it = table_locks_.find(table_name.StringView());
        if (lock_it == table_locks_.end())
        {
            CatalogKey table_key{
                txservice::TableName{table_name.StringView(),
                                     txservice::TableType::Primary,
                                     table_name.Engine()}};
            TxKey catalog_tx_key(&table_key);
            auto it = FindEmplace(table_key);

            CcEntry<CatalogKey, CatalogRecord, true, false> *catalog_cce =
                it->second;
            CcPage<CatalogKey, CatalogRecord, true, false> *catalog_ccp =
                it.GetPage();

            const CatalogEntry *catalog_entry =
                shard_->GetCatalog(table_key.Name(), node_group_id);

            if (catalog_entry == nullptr || catalog_entry->schema_ == nullptr)
            {
                // The catalog entry hasn't been initialized, try to load it
                // from data store.
                shard_->FetchCatalog(
                    table_name, node_group_id, ng_term, nullptr);
                return {CcErrorCode::READ_CATALOG_FAIL, nullptr, 0};
            }

            assert(catalog_entry != nullptr &&
                   catalog_entry->schema_version_ > 0);
            assert(catalog_entry->schema_ != nullptr);

            schema_version = catalog_entry->schema_version_;

            lock_ptr =
                &catalog_cce->GetOrCreateKeyLock(shard_, this, catalog_ccp);
            auto [new_lock_it, _] = table_locks_.try_emplace(
                table_name.StringView(), lock_ptr, schema_version);
            read_success =
                new_lock_it->second.first->AcquireReadLockFast(tx_number);

            if (read_success &&
                catalog_cce->PayloadStatus() == RecordStatus::Unknown)
            {
                assert(schema_version == new_lock_it->second.second);
                // upload catalog record
                catalog_cce->payload_.PassInCurrentPayload(
                    std::make_unique<CatalogRecord>());
                catalog_cce->payload_.cur_payload_->Set(
                    catalog_entry->schema_,
                    catalog_entry->dirty_schema_,
                    schema_version);
                catalog_cce->SetCommitTsPayloadStatus(schema_version,
                                                      RecordStatus::Normal);
            }
        }
        else
        {
            read_success =
                lock_it->second.first->AcquireReadLockFast(tx_number);
            lock_ptr = lock_it->second.first;
            schema_version = lock_it->second.second;
        }

        if (read_success)
        {
            assert(lock_ptr != nullptr);
            assert(schema_version > 0);
#ifndef NDEBUG
            shard_->UpsertLockHoldingTx(tx_number,
                                        ng_term,
                                        nullptr,
                                        false,
                                        node_group_id,
                                        table_name_.Type());
#endif
            return {CcErrorCode::NO_ERROR, lock_ptr, schema_version};
        }
        else
        {
            return {CcErrorCode::READ_CATALOG_CONFLICT, nullptr, 0};
        }
    }

private:
    void UpdateTableLocks(std::string_view table_name_sv,
                          uint64_t schema_version,
                          NonBlockingLock *lock_ptr)
    {
        auto table_locks_iter = table_locks_.find(table_name_sv);
        if (table_locks_iter != table_locks_.end())
        {
            table_locks_iter->second.second = schema_version;
        }
        else
        {
            table_locks_.try_emplace(table_name_sv, lock_ptr, schema_version);
        }
    }

    // An index structure to directly get the lock via table name.
    // WARNING: This is based on the assumption that the locks for catalog cc
    // entryies are never recycled. If the assumption is violated, this
    // structured should not be used.
    // TODO: Better separate lock of catalog ccmap from CcShard locks
    //
    // The uint64_t represents the table schema version and it is only used in
    // eloqkv, the schema version in eloqsql will be passed via storage handler.
    absl::flat_hash_map<std::string, std::pair<NonBlockingLock *, uint64_t>>
        table_locks_;
};
}  // namespace txservice
