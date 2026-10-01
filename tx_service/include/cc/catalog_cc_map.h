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
#include "absl/strings/string_view.h"
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
                DLOG(WARNING)
                    << "AcquireAllCc from txn: " << req.Txn()
                    << ", on table: " << table_name_.StringView()
                    << ", key: " << catalog_key->ToString() << " is conflict.";
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

        // Kills the node (*_panic) or stalls it for 10 seconds (*_sleep) during
        // a schema op's PrepareCommit, or during the Rollback of a failed one.
        CODE_FAULT_INJECTOR("during_post_write_all", {
            std::string &action = FaultInject::Instance()
                                      .Entry("during_post_write_all")
                                      ->vctAction_.front();
            if ((req.CommitType() == PostWriteType::PrepareCommit &&
                 action == "prepare_commit_panic") ||
                (req.CommitType() == PostWriteType::Rollback &&
                 action == "rollback_panic"))
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
                     (req.CommitType() == PostWriteType::Rollback &&
                      action == "rollback_sleep"))
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
        // Find() returns End() when the key is absent, and End() is not
        // dereferenceable. The null-entry case below is expected here (it is
        // exactly the failed-over node group the comment describes), so the
        // iterator has to be range-checked before it->second.
        CcEntry<CatalogKey, CatalogRecord, true, false> *cce_ptr =
            it == End() ? nullptr : it->second;

        // Check whether cce key lock holder is the given tx of the
        // PostWriteAllCc before applying change.
        if (cce_ptr == nullptr || cce_ptr->GetKeyLock() == nullptr ||
            !cce_ptr->GetKeyLock()->HasWriteLockOrWriteIntent(req.Txn()))
        {
            // The tx holds no lock on the entry here, so the post write has
            // nothing left to do and finishes. That is the case when (1) the
            // request is a resend whose first copy succeeded; (2) the node
            // group failed over while the schema op had no log yet, so the
            // op failed before its prepare log and the request only releases
            // an intent or lock the failover already dropped; or (3) the node
            // group, a participant, recovered from the op's commit log, which
            // installs the committed schema and restores no lock there. Only
            // the coordinator's node group restores one, see
            // Execute(ReplayLogCc&).
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

        switch (req.CommitType())
        {
        case PostWriteType::PrepareCommit:
            return PrepareCommitSchema(req, *table_key, ng_term);
        case PostWriteType::UpdateDirty:
            return UpdateDirtySchema(req, *table_key);
        case PostWriteType::PostCommit:
            return PostCommitSchema(req, *table_key, *cce_ptr, ng_term);
        case PostWriteType::Rollback:
            return RollBackSchema(req, *table_key);
        case PostWriteType::DowngradeLock:
            // The request commits nothing and only downgrades the write locks
            // to write intents.
            return TemplateCcMap::Execute(req);
        default:
            // Schema operations always employ multi-stage commits.
            assert(false);
            return TemplateCcMap::Execute(req);
        }
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
        Iterator it = FindEmplace(*table_key, &emplace, true, false);
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
                        if (Sharder::Instance().GetLocalCcShards()->store_hd_ !=
                                nullptr &&
                            !table_key->Name().IsHashPartitioned() &&
                            !shard_->LoadRangesAndStatisticsNx(
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
            req.AbortCcRequest(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
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
            // Find() returns End() when the key is absent (template_cc_map.h),
            // and End() is not dereferenceable. During replay the catalog entry
            // is normally absent -- that is the whole point of replaying it --
            // so the iterator must be range-checked before it->second.
            CcEntry<CatalogKey, CatalogRecord, true, false> *cce =
                it == End() ? nullptr : it->second;
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
                    return true;
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
                // A rolled-back add index commits at a real timestamp: the
                // table keeps its current schema, at that timestamp.
                const bool rolled_back = schema_op_msg.rolled_back();
                const std::string &committed_blob =
                    rolled_back ? schema_op_msg.old_catalog_blob()
                                : schema_op_msg.new_catalog_blob();

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
                                committed_blob,
                                dirty_schema_commit_ts);

                    OperationType op_type = static_cast<OperationType>(
                        schema_op_msg.table_op().op_type());
                    if (op_type == OperationType::CreateTable || rolled_back)
                    {
                        // A create's kv tables exist since before its commit
                        // log (UpsertTableOp::CreateKvTables), with their
                        // ranges at the prepare-stage version; only the catalog
                        // row may be missing. Op type Update is the store's
                        // catalog-only write. A rolled-back add index likewise
                        // writes only its catalog row, the current schema; the
                        // recovered coordinator drops the index's kv tables.
                        op_type = OperationType::Update;
                    }

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
                    req.AbortCcRequest(upsert_kv_err_code.second);
                    return true;
                }

                auto [success, new_catalog_entry] = shard_->CreateCatalog(
                    table_name,
                    req.NodeGroupId(),
                    dirty_schema_commit_ts > 0
                        ? committed_blob
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
                    return true;
                }
                catalog_entry = new_catalog_entry;
            }

            if (catalog_entry->schema_)
            {
                if (Sharder::Instance().GetLocalCcShards()->store_hd_ !=
                        nullptr &&
                    !table_name.IsHashPartitioned() &&
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
            // Only the coordinator's node group restores a lock at the commit
            // stage: a write intent on the schema, under which the recovered
            // op redoes its steps after the commit log, data store writes
            // among them, and which the op's PostCommit or Rollback releases.
            // The intent keeps a newer DDL on the table out until then, so no
            // late write of the recovered op lands after one. It is not a
            // write lock: the entry already holds the committed schema (step
            // 1), so the op's effect is in place for everyone else, and a read
            // of the entry must not wait for the op. The op's PostWriteAll
            // needs the node group's leader term, which comes only once the
            // log replay has finished, and that replay reads this entry to
            // replay the table's data (CcShard::InitCcm, which fails on a
            // write lock). The condition is the one below that starts the
            // recovered op. A participant installs the committed schema with
            // no lock, and the op's later post writes, finding no lock of
            // theirs, finish at once (Execute(PostWriteAllCc&)).
            const uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;
            if (tx_node_id == req.NodeGroupId() &&
                Sharder::Instance().CandidateLeaderTerm(tx_node_id) >= 0)
            {
                lock_type = LockType::WriteIntent;
            }
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
        if (catalog_entry->schema_ == nullptr &&
            catalog_entry->dirty_schema_ != nullptr)
        {
            assert(op_type == OperationType::CreateTable);
            cce->SetCommitTsPayloadStatus(1, RecordStatus::Deleted);
        }
        else if ((op_type == OperationType::AddIndex ||
                  op_type == OperationType::DropIndex) &&
                 (schema_op_msg.stage() ==
                      ::txlog::SchemaOpMessage_Stage::
                          SchemaOpMessage_Stage_PrepareSchema ||
                  schema_op_msg.stage() ==
                      ::txlog::SchemaOpMessage_Stage::
                          SchemaOpMessage_Stage_PrepareData))
        {
            // Replaying a prepare-stage log restores the state after
            // PrepareCommit, at which an index op published its timestamp on
            // the entry under the prepare-stage write lock (TemplateCcMap,
            // Execute(PostWriteAllCc&)). The recovered lock may be only an
            // intent, but the entry must carry the same timestamp, so that a tx
            // reading the dirty schema here still commits above the index op.
            // The record set above is complete, so the entry is loaded now; an
            // entry left Unknown must keep commit ts 0 (see TemplateCcMap). The
            // commit ts only moves forward, so a repeated replay changes
            // nothing.
            cce->SetCommitTsPayloadStatus(catalog_entry->dirty_schema_version_,
                                          RecordStatus::Normal);
        }
        else if (schema_op_msg.stage() ==
                     ::txlog::SchemaOpMessage_Stage::
                         SchemaOpMessage_Stage_CommitSchema &&
                 catalog_entry->schema_version_ > 0)
        {
            // Replaying a commit log installs the committed schema, or the
            // current one for an op the log rolled back, at its version and
            // with no dirty schema. The record set above is complete, so the
            // entry is loaded now, as a catalog read would load it. Left
            // Unknown, it would take the commit ts of a Rollback the recovered
            // op sends above 0 while keeping its status, and the first read
            // could not load it at that same ts: the commit ts only moves
            // forward. A PostCommit or Rollback at the same ts changes nothing.
            cce->SetCommitTsPayloadStatus(catalog_entry->schema_version_,
                                          catalog_entry->schema_ != nullptr
                                              ? RecordStatus::Normal
                                              : RecordStatus::Deleted);
        }

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

        return true;
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
                    if (Sharder::Instance().GetLocalCcShards()->store_hd_ !=
                            nullptr &&
                        !table_name.IsHashPartitioned() &&
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
        CcEntry<CatalogKey, CatalogRecord, true, false> *cce =
            it == End() ? nullptr : it->second;

        if (cce == nullptr)
        {
            // The catalog write lock was taken on the old leader by
            // InvalidateTableCacheCompositeOp's acquire_all_lock_op_, and there
            // is no leader-term re-check before this request is routed. If the
            // node group failed over in between, this lands on a new leader
            // whose catalog cc map has neither the entry nor the lock -- cc
            // entries and locks do not survive failover. The ng_term check
            // above still passes, because the new leader's term is valid. There
            // is nothing cached to invalidate, so finish the request instead of
            // dereferencing.
            if (shard_->core_id_ < shard_->core_cnt_ - 1)
            {
                req.ResetCcm();
                MoveRequest(&req, shard_->core_id_ + 1);
                return false;
            }
            hd_res->SetFinished();
            return true;
        }

        // Only meaningful when the entry survived, i.e. no failover happened.
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

    /**
     * GetTableSchema stored in each shard's CatalogCcMap. Used to initialize
     * the Pk and Sk ccmaps.
     * @param table_name
     * @param cc_ng_id
     * @param cc_ng_term
     * @param requester
     * @return InitCcmResult describing the schema lookup outcome.
     */
    InitCcmResult GetTableSchema(const TableName &table_name,
                                 NodeGroupId cc_ng_id,
                                 int64_t cc_ng_term,
                                 CcRequestBase *requester)
    {
        const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                        TableType::Primary,
                                        table_name.Engine()};

        CatalogKey table_key{base_table_name};

        bool emplace = false;
        Iterator it = FindEmplace(table_key, &emplace, true, false);
        CcEntry<CatalogKey, CatalogRecord, true, false> *catalog_cce =
            it->second;

        if (catalog_cce->PayloadStatus() == RecordStatus::Unknown)
        {
            const CatalogEntry *catalog_entry =
                shard_->GetCatalog(table_key.Name(), cc_ng_id);

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
                        assert(requester != nullptr);
                        // Initialize table statistics before create ccmap.
                        if (Sharder::Instance().GetLocalCcShards()->store_hd_ !=
                                nullptr &&
                            !table_name.IsHashPartitioned() &&
                            !shard_->LoadRangesAndStatisticsNx(
                                catalog_entry->schema_.get(),
                                cc_ng_id,
                                cc_ng_term,
                                requester))
                        {
                            return InitCcmResult::Retry();
                        }
#endif
                    }

                    // upload catalog record
                    catalog_cce->payload_.PassInCurrentPayload(
                        std::make_unique<CatalogRecord>());
                    catalog_cce->payload_.cur_payload_->Set(
                        catalog_entry->schema_,
                        catalog_entry->dirty_schema_,
                        catalog_entry->schema_version_);
                    catalog_cce->SetCommitTsPayloadStatus(
                        catalog_entry->schema_version_, RecordStatus::Normal);
                }
                else
                {
                    catalog_cce->SetCommitTsPayloadStatus(
                        catalog_entry->schema_version_, RecordStatus::Deleted);
                }
            }
            else
            {
                // Global CatalogEntry does not exist.
                shard_->FetchCatalog(
                    table_key.Name(), cc_ng_id, cc_ng_term, requester);
                return InitCcmResult::Retry();
            }
        }

        const auto keylock = catalog_cce->GetKeyLock();
        if (keylock != nullptr && keylock->HasWriteLock())
        {
            DLOG(WARNING)
                << "InitCcm failure, target table: " << table_name.StringView()
                << " has write lock on catalog_cc_map, which means, it is "
                   "being modified on this shard.";
            return InitCcmResult::Failure(CcErrorCode::READ_CATALOG_CONFLICT);
        }

        RecordStatus payload_status = catalog_cce->PayloadStatus();
        if (payload_status == RecordStatus::Deleted)
        {
            return InitCcmResult::Failure(
                CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
        }

        assert(payload_status == RecordStatus::Normal);
        CatalogRecord *catalog_rec = catalog_cce->payload_.cur_payload_.get();
        assert(catalog_rec != nullptr);
        assert(catalog_rec->Schema() != nullptr);

        return InitCcmResult::Success(catalog_rec->Schema());
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

        std::string_view table_name_sv = table_name.StringView();
        absl::string_view absl_table_name_sv(table_name_sv.data(),
                                             table_name_sv.size());
        auto lock_it = table_locks_.find(absl_table_name_sv);
        if (lock_it == table_locks_.end())
        {
            CatalogKey table_key{
                txservice::TableName{table_name_sv,
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
                std::string(table_name_sv), lock_ptr, schema_version);
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
    /** @brief A cc entry of the catalog cc map. */
    using CatalogCce = CcEntry<CatalogKey, CatalogRecord, true, false>;

    /**
     * @brief PrepareCommit of a schema op: installs the op's dirty schema on
     * this node and the record the template puts in the cc entry of every
     * core. An index op also readies its table (PrepareIndexOp), and an add
     * index creates its new indexes' cc maps.
     *
     * @param req The request.
     * @param table_key The table's catalog key.
     * @param ng_term The node group's leader term.
     * @return As Execute(): whether the request is done with this core.
     */
    bool PrepareCommitSchema(PostWriteAllCc &req,
                             const CatalogKey &table_key,
                             int64_t ng_term)
    {
        // Every sender prepares after its prepare log, or, for
        // FlushUpdateTableOp, once its tx has committed: at a commit ts above
        // 0. A post write at 0 is a Rollback or a DowngradeLock
        // (PostWriteType::Rollback).
        assert(req.CommitTs() != TransactionOperation::tx_op_failed_ts_);

        const NodeGroupId ng_id = req.NodeGroupId();
        CatalogEntry *catalog_entry = nullptr;
        if (shard_->core_id_ == 0)
        {
            // The first core instantiates the dirty schema in LocalCcShards.
            // A DROP TABLE's payload carries no image.
            CatalogRecord *schema_rec =
                RequestRecord(req, req.OpType() != OperationType::DropTable);
            catalog_entry =
                shard_->CreateDirtyCatalog(table_key.Name(),
                                           ng_id,
                                           schema_rec->DirtySchemaImage(),
                                           req.CommitTs());
            if ((req.OpType() == OperationType::AddIndex ||
                 req.OpType() == OperationType::DropIndex) &&
                !PrepareIndexOp(req, table_key, *catalog_entry, ng_term))
            {
                return false;
            }

            schema_rec->Set(catalog_entry->schema_,
                            catalog_entry->dirty_schema_,
                            catalog_entry->schema_version_);
        }
        else
        {
            // Core 0 set up the record the template installs.
            assert(req.Payload() != nullptr);
            catalog_entry = shard_->GetCatalog(table_key.Name(), ng_id);
            if (catalog_entry == nullptr)
            {
                // The table catalog haven't been initialized. Just return.
                req.Result()->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                return true;
            }
        }

        if (req.OpType() == OperationType::AddIndex &&
            catalog_entry->dirty_schema_version_ > 0)
        {
            SetAddedIndexCcms(*catalog_entry, ng_id, true);
        }
        return TemplateCcMap::Execute(req);
    }

    /**
     * @brief Readies an index op's table on this node for its dirty schema.
     * Done once, on core 0, after the dirty schema is installed.
     *
     * 1. The current schema. The DDL read it on the coordinator's node group
     *    only, so another node group may not have loaded the table yet.
     *    Installing the dirty schema first is harmless: a current schema
     *    fetched later is older than it and keeps it
     * (CatalogEntry::InitSchema), and CreateDirtyCatalog changes nothing when
     * the request runs again.
     * 2. The table's statistics (building them needs the table's ranges),
     *    which the dirty schema takes over: it becomes the current schema.
     * 3. For an add index, the new indexes, which the table's cc maps and
     *    transactions may reach while the op runs: their ranges, at the op's
     *    version and with empty slices for the ranges this node group owns
     *    and has none for yet, and their entries in the statistics.
     *
     * A load is asynchronous: the request runs again once it completes.
     *
     * @param req The PrepareCommit request.
     * @param table_key The table's catalog key.
     * @param catalog_entry The table's catalog entry, holding the dirty schema.
     * @param ng_term The node group's leader term.
     * @return false while something is being fetched.
     */
    bool PrepareIndexOp(PostWriteAllCc &req,
                        const CatalogKey &table_key,
                        CatalogEntry &catalog_entry,
                        int64_t ng_term)
    {
        const NodeGroupId ng_id = req.NodeGroupId();
        if (catalog_entry.schema_ == nullptr)
        {
            shard_->FetchCatalog(table_key.Name(), ng_id, ng_term, &req);
            return false;
        }
        if (Sharder::Instance().GetLocalCcShards()->store_hd_ != nullptr &&
            !table_key.Name().IsHashPartitioned() &&
            shard_->LoadRangesAndStatisticsNx(
                catalog_entry.schema_.get(), ng_id, ng_term, &req) == nullptr)
        {
            return false;
        }
        catalog_entry.dirty_schema_->BindStatistics(
            catalog_entry.schema_->StatisticsObject());
        if (req.OpType() != OperationType::AddIndex)
        {
            return true;
        }

        const std::vector<TableName> added_index_names =
            IndexesNotIn(*catalog_entry.dirty_schema_, *catalog_entry.schema_);
        // A new index may come with pre-defined ranges, based on the data
        // distribution the caller offered, so its ranges are loaded rather
        // than initialized empty.
        for (const TableName &index_name : added_index_names)
        {
            const TableName index_range_name = RangeTableNameOf(index_name);
            auto ranges =
                shard_->GetTableRangesForATable(index_range_name, ng_id);
            if (ranges == nullptr)
            {
                shard_->FetchTableRanges(
                    index_range_name, &req, ng_id, ng_term);
                return false;
            }

            for (auto &range : *ranges)
            {
                range.second->SetVersion(req.CommitTs());
                NodeGroupId range_owner =
                    shard_
                        ->GetRangeOwner(
                            range.second->GetRangeInfo()->PartitionId(), ng_id)
                        ->BucketOwner();
                if (range_owner == ng_id &&
                    range.second->RangeSlices() == nullptr)
                {
                    // This node group owns the range, which is empty.
                    InitEmptyRangeSlices(*range.second, ng_id);
                }
            }
        }

        for (const TableName &index_name : added_index_names)
        {
            catalog_entry.schema_->StatisticsObject()->CreateIndex(
                index_name,
                catalog_entry.dirty_schema_->IndexKeySchema(index_name),
                cc_ng_id_);
        }
        return true;
    }

    /**
     * @brief UpdateDirty of an add index: replaces the dirty schema if the
     * request carries a new image of it, and moves the new indexes' cc maps to
     * it.
     *
     * @param req The request.
     * @param table_key The table's catalog key.
     * @return As Execute(): whether the request is done with this core.
     */
    bool UpdateDirtySchema(PostWriteAllCc &req, const CatalogKey &table_key)
    {
        assert(req.OpType() == OperationType::AddIndex);
        assert(req.CommitTs() != TransactionOperation::tx_op_failed_ts_);
        const NodeGroupId ng_id = req.NodeGroupId();
        CatalogEntry *catalog_entry =
            shard_->GetCatalog(table_key.Name(), ng_id);
        if (catalog_entry == nullptr)
        {
            // The table catalog haven't been initialized. Just return.
            req.Result()->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
            return true;
        }

        if (shard_->core_id_ == 0)
        {
            CatalogRecord *schema_rec = RequestRecord(req);
            if (catalog_entry->dirty_schema_->SchemaImage() !=
                schema_rec->DirtySchemaImage())
            {
                assert(catalog_entry->dirty_schema_version_ == req.CommitTs());
                std::shared_ptr<TableSchema> old_dirty_schema =
                    catalog_entry->dirty_schema_;
                shard_->UpdateDirtyCatalog(table_key.Name(),
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
            // Core 0 set up the record the template installs.
            assert(req.Payload() != nullptr);
        }

        if (catalog_entry->dirty_schema_version_ > 0)
        {
            SetAddedIndexCcms(*catalog_entry, ng_id, false);
        }
        return TemplateCcMap::Execute(req);
    }

    /**
     * @brief Points the cc maps of an add index's new indexes at the dirty
     * schema, on this core. The existing indexes' cc maps move at PostCommit.
     *
     * @param catalog_entry The table's catalog entry, holding the dirty schema.
     * @param ng_id The node group.
     * @param is_create Whether to create the cc maps: at PrepareCommit.
     */
    void SetAddedIndexCcms(const CatalogEntry &catalog_entry,
                           NodeGroupId ng_id,
                           bool is_create)
    {
        for (const TableName &index_name :
             IndexesNotIn(*catalog_entry.dirty_schema_, *catalog_entry.schema_))
        {
            SetTableCcms(index_name,
                         catalog_entry.dirty_schema_.get(),
                         ng_id,
                         catalog_entry.dirty_schema_version_,
                         is_create);
        }
    }

    /**
     * @brief PostCommit of a schema op: promotes the dirty schema at the
     * commit timestamp.
     *
     * Core 0 fills the request's record from the node's catalog entry: the
     * requester's record for a local request, one the request owns for a
     * remote one, which carries none (RequestRecord). The template installs it
     * in every core's cc entry, copy-on-write
     * (VersionedPayload::SetCurrentPayload). Each core moves the table's cc
     * maps; the last core cleans what the op removed and commits the catalog
     * entry; the template publishes the commit timestamp and releases the
     * lock.
     *
     * @param req The request.
     * @param table_key The table's catalog key.
     * @param cce The table's cc entry on this core.
     * @param ng_term The node group's leader term.
     * @return As Execute(): whether the request is done with this core.
     */
    bool PostCommitSchema(PostWriteAllCc &req,
                          const CatalogKey &table_key,
                          CatalogCce &cce,
                          int64_t ng_term)
    {
        FinishSchemaWriter(req, table_key);
        CatalogEntry *catalog_entry =
            shard_->GetCatalog(table_key.Name(), req.NodeGroupId());

        // Only a Rollback leaves the commit timestamp at 0.
        const uint64_t commit_ts = req.CommitTs();
        assert(commit_ts != TransactionOperation::tx_op_failed_ts_);
        assert(catalog_entry != nullptr);
        if (shard_->core_id_ == 0)
        {
            CommitNodeLevelSchema(req, table_key, *catalog_entry);
            // The record promotes the dirty schema, at the commit timestamp,
            // which is the schema record's new version; DROP TABLE's dirty
            // schema is null, so its record holds no schema. An entry without
            // a dirty schema version has nothing to promote and keeps its
            // current schema.
            CatalogRecord *rec = RequestRecord(req, false);
            if (catalog_entry->dirty_schema_version_ > 0)
            {
                rec->Set(catalog_entry->dirty_schema_, nullptr, commit_ts);
            }
            else
            {
                rec->Set(catalog_entry->schema_,
                         nullptr,
                         catalog_entry->schema_version_);
            }
        }
        else
        {
            // Core 0 filled the record the template installs.
            assert(req.Payload() != nullptr);
        }

        if (catalog_entry->dirty_schema_version_ > 0)
        {
            CommitTableCcms(req, table_key, *catalog_entry, cce, ng_term);
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                CleanRemovedTables(req, table_key, *catalog_entry);
                catalog_entry->CommitDirtySchema(commit_ts);
            }
        }
        return TemplateCcMap::Execute(req);
    }

    /**
     * @brief Rollback of a schema op, its second phase instead of PostCommit:
     * discards the dirty schema and undoes what the prepare stage did.
     *
     * An op that failed, before or after its prepare log, commits at timestamp
     * 0, which leaves the catalog entry's version alone; an add index whose
     * build broke a constraint commits at its commit-stage timestamp, which
     * the catalog entry takes. No table's version changes. An add index's new
     * cc maps, made at its prepare stage, go on every core, their ranges and
     * statistics on the last.
     *
     * An op that failed before its prepare log installed no dirty schema, so
     * there is nothing to discard: the request then carries no record and the
     * template only releases the lock. Having failed to acquire its write
     * intents, such an op may hold them on some cores only, core 0 or not.
     * An op that installed a dirty schema holds every core; core 0 fills the
     * record the template installs on each.
     *
     * @param req The request.
     * @param table_key The table's catalog key.
     * @return As Execute(): whether the request is done with this core.
     */
    bool RollBackSchema(PostWriteAllCc &req, const CatalogKey &table_key)
    {
        FinishSchemaWriter(req, table_key);
        CatalogEntry *catalog_entry =
            shard_->GetCatalog(table_key.Name(), req.NodeGroupId());
        const uint64_t commit_ts = req.CommitTs();

        // A dirty schema on this node can only be the op's own: another op
        // installs one only once it holds the write intent on every core of
        // every node group, which it cannot while this op holds any. The last
        // core drops it, after every other core has looked.
        //
        // An entry without one keeps its schema, and the template only
        // releases the lock. Either the op failed before installing a dirty
        // schema, at commit ts 0, or this is the coordinator's node group
        // recovered from a commit log that rolled the op back: replay
        // installed the current schema at the commit ts, loaded the entry at
        // it, and restored the write lock this request releases.
        if (catalog_entry == nullptr ||
            catalog_entry->dirty_schema_version_ == 0)
        {
            assert(commit_ts == TransactionOperation::tx_op_failed_ts_ ||
                   (catalog_entry != nullptr &&
                    catalog_entry->schema_version_ == commit_ts));
            req.SetDecodedPayload(nullptr);
            return TemplateCcMap::Execute(req);
        }

        const NodeGroupId ng_id = req.NodeGroupId();
        const bool last_core = shard_->core_id_ == shard_->core_cnt_ - 1;
        assert(commit_ts == TransactionOperation::tx_op_failed_ts_ ||
               catalog_entry->schema_ != nullptr);
        if (req.OpType() == OperationType::AddIndex)
        {
            assert(catalog_entry->schema_ != nullptr &&
                   catalog_entry->dirty_schema_ != nullptr);
            for (const TableName &index_name : IndexesNotIn(
                     *catalog_entry->dirty_schema_, *catalog_entry->schema_))
            {
                DropTableCcms(index_name, ng_id);
                if (last_core)
                {
                    CleanIndexRangesAndStatistics(
                        index_name,
                        *catalog_entry->dirty_schema_->StatisticsObject(),
                        ng_id);
                }
            }
        }

        // The record the template installs on every core: the table's current
        // schema and no dirty schema, at the commit timestamp if the op has
        // one, else at the current version.
        if (shard_->core_id_ == 0)
        {
            CatalogRecord *rec = RequestRecord(req, false);
            rec->Set(catalog_entry->schema_,
                     nullptr,
                     commit_ts != TransactionOperation::tx_op_failed_ts_
                         ? commit_ts
                         : catalog_entry->schema_version_);
        }
        else
        {
            // Core 0 filled the record the template installs.
            assert(req.Payload() != nullptr);
        }

        if (last_core)
        {
            catalog_entry->RollBackDirtySchema(commit_ts);
        }
        return TemplateCcMap::Execute(req);
    }

    /**
     * @brief Ends the op's hold on the schema's reader-writer control block,
     * which its AcquireAllCc took on the native node group, so that runtime
     * queries may cache the schema again. Part of both second phases,
     * PostCommit and Rollback.
     *
     * @param req The request.
     * @param table_key The table's catalog key.
     */
    void FinishSchemaWriter(const PostWriteAllCc &req,
                            const CatalogKey &table_key)
    {
        if (!shard_->IsNative(req.NodeGroupId()))
        {
            return;
        }
        const TableName &tbl_name = table_key.Name();
        std::shared_ptr<ReaderWriterObject<TableSchema>> schema_cntl =
            shard_->FindSchemaCntl(tbl_name);
        if (schema_cntl != nullptr)
        {
            schema_cntl->FinishWriter(req.Txn());
            shard_->DeleteSchemaCntl(tbl_name);
        }
    }

    /**
     * @brief The node-level work of committing a schema op, done once, on core
     * 0, before any core moves its cc maps.
     *
     * TRUNCATE TABLE's table is a new one, whose version is the commit-stage
     * timestamp (UpsertTableOp::AssignCommitStageTs), so its schema is rebuilt
     * at it before it is promoted. CREATE TABLE installs the table's and its
     * indexes' initial ranges and statistics, and ALTER TABLE binds the
     * table's statistics to the dirty schema. Neither has anything to do when
     * the op is recovering from its commit log: replay installed the committed
     * schema, leaving no dirty one, and the coordinator's node group reaches
     * here under the write lock replay restored.
     *
     * @param req The PostCommit request.
     * @param table_key The table's catalog key.
     * @param catalog_entry The table's catalog entry.
     */
    void CommitNodeLevelSchema(const PostWriteAllCc &req,
                               const CatalogKey &table_key,
                               CatalogEntry &catalog_entry)
    {
        const uint64_t commit_ts = req.CommitTs();
        // The version the dirty schema was installed with at the prepare
        // stage, which the kv tables of a CREATE TABLE and so the table's
        // initial ranges carry: a node that loads them from the store gets the
        // same.
        const uint64_t prepared_version = catalog_entry.dirty_schema_version_;
        if (req.OpType() == OperationType::TruncateTable &&
            catalog_entry.dirty_schema_ != nullptr &&
            commit_ts > prepared_version)
        {
            TableSchema::uptr at_commit =
                shard_->local_shards_.CreateTableSchemaFromImage(
                    table_key.Name(),
                    catalog_entry.dirty_schema_->SchemaImage(),
                    commit_ts);
            if (catalog_entry.dirty_schema_->StatisticsObject())
            {
                at_commit->BindStatistics(
                    catalog_entry.dirty_schema_->StatisticsObject());
            }
            catalog_entry.SetDirtySchema(std::move(at_commit), commit_ts);
        }

        if (req.OpType() == OperationType::CreateTable &&
            catalog_entry.dirty_schema_ != nullptr)
        {
            InitCreatedTableRange(
                table_key.Name(), prepared_version, req.NodeGroupId());
            for (const TableName &index_name :
                 catalog_entry.dirty_schema_->IndexNames())
            {
                InitCreatedTableRange(
                    index_name, prepared_version, req.NodeGroupId());
            }
            shard_->InitTableStatistics(catalog_entry.dirty_schema_.get(),
                                        cc_ng_id_);
        }
        else if (req.OpType() == OperationType::Update &&
                 catalog_entry.dirty_schema_ != nullptr)
        {
            assert(catalog_entry.schema_->StatisticsObject());
            catalog_entry.dirty_schema_->BindStatistics(
                catalog_entry.schema_->StatisticsObject());
        }
    }

    /**
     * @brief Moves a committed schema op's cc maps on this core: TRUNCATE
     * TABLE's to the new table, DROP TABLE's away, CREATE TABLE's into being,
     * and ALTER TABLE's to the new schema, dropping those of a dropped index.
     *
     * @param req The PostCommit request.
     * @param table_key The table's catalog key.
     * @param catalog_entry The table's catalog entry, still holding the current
     * and the dirty schema.
     * @param cce The table's cc entry on this core.
     * @param ng_term The node group's leader term.
     */
    void CommitTableCcms(const PostWriteAllCc &req,
                         const CatalogKey &table_key,
                         const CatalogEntry &catalog_entry,
                         CatalogCce &cce,
                         int64_t ng_term)
    {
        const NodeGroupId ng_id = req.NodeGroupId();
        const TableName &table_name = table_key.Name();
        const TableSchema *old_schema = catalog_entry.schema_.get();
        const TableSchema *new_schema = catalog_entry.dirty_schema_.get();
        const uint64_t dirty_version = catalog_entry.dirty_schema_version_;
        switch (req.OpType())
        {
        case OperationType::TruncateTable:
        {
            assert(table_name.Engine() == TableEngine::EloqKv);
            // Only the base table's cc map moves to the new schema: EloqKV
            // tables have no indexes. A truncate of a table with indexes would
            // have to move each index's map too, since CleanCcm empties them,
            // and its data step, which drops only the base table's kv table,
            // would have to reset ranges and statistics too
            // (UpsertTableOp::TruncateUntilCommitLog).
            // A remote tx is allowed to acquire write intents/locks and drop a
            // table, even if the table's schema has not been initialized at
            // this node. The earlier acquiring-write-intent request creates a
            // schema cc entry in the catalog cc map and a node-level schema
            // view. The version timestamp of the schema is 0, if the schema is
            // uninitialized (null). Or, the current schema must not be null.
            assert(catalog_entry.schema_version_ == 0 || old_schema != nullptr);
            assert(new_schema->Version() == dirty_version);
            shard_->UpdateCcmSchema(table_name,
                                    ng_id,
                                    catalog_entry.dirty_schema_.get(),
                                    dirty_version);
            if (shard_->core_id_ == 0 &&
                !shard_->GetSubscribedStandbys().empty())
            {
                ForwardTruncateToStandbys(
                    req, table_key, catalog_entry, ng_term);
            }
            break;
        }
        case OperationType::DropTable:
        {
            // As for truncate, the current schema may be uninitialized here.
            assert(catalog_entry.schema_version_ == 0 || old_schema != nullptr);
            DropTableCcms(table_name, ng_id);
            if (old_schema != nullptr)
            {
                for (const TableName &index_name : old_schema->IndexNames())
                {
                    DropTableCcms(index_name, ng_id);
                }
            }
            break;
        }
        case OperationType::CreateTable:
        {
            assert(new_schema != nullptr);
            shard_->CreateOrUpdatePkCcMap(
                table_name, new_schema, ng_id, true, true);
            for (const TableName &index_name : new_schema->IndexNames())
            {
                shard_->CreateOrUpdateSkCcMap(index_name, new_schema, ng_id);
            }
            break;
        }
        default:
        {
            // ALTER TABLE: the table's cc maps take the new schema; an index
            // the op dropped loses its cc maps. An add index's new indexes are
            // not in the old schema and are left alone: their cc maps point at
            // the dirty schema since PrepareCommit (SetAddedIndexCcms), at the
            // op's version, and CommitDirtySchema promotes that schema object
            // as it is.
            assert(old_schema != nullptr && new_schema != nullptr);
            SetTableCcms(table_name, new_schema, ng_id, dirty_version, false);
            if (req.OpType() == OperationType::AddIndex ||
                req.OpType() == OperationType::DropIndex ||
                req.OpType() == OperationType::Update)
            {
                const std::vector<TableName> new_index_names =
                    new_schema->IndexNames();
                for (const TableName &old_index_name : old_schema->IndexNames())
                {
                    if (std::find(new_index_names.begin(),
                                  new_index_names.end(),
                                  old_index_name) != new_index_names.end())
                    {
                        SetTableCcms(old_index_name,
                                     new_schema,
                                     ng_id,
                                     dirty_version,
                                     false);
                    }
                    else
                    {
                        assert(req.OpType() == OperationType::DropIndex);
                        DropTableCcms(old_index_name, ng_id);
                    }
                }
            }
            break;
        }
        }

        UpdateTableLocks(
            table_name.StringView(), dirty_version, cce.GetKeyLock());
    }

    /**
     * @brief Cleans the node-level structures of the tables and indexes a
     * committed schema op removed: their ranges and statistics. Done once, on
     * the last core, after every core dropped their cc maps.
     *
     * @param req The PostCommit request.
     * @param table_key The table's catalog key.
     * @param catalog_entry The table's catalog entry, still holding the current
     * and the dirty schema.
     */
    void CleanRemovedTables(const PostWriteAllCc &req,
                            const CatalogKey &table_key,
                            const CatalogEntry &catalog_entry)
    {
        const NodeGroupId ng_id = req.NodeGroupId();
        const TableName &table_name = table_key.Name();
        const TableSchema *old_schema = catalog_entry.schema_.get();
        if (req.OpType() == OperationType::DropTable)
        {
            shard_->CleanTableStatistics(table_name, cc_ng_id_);
            if (!table_name.IsHashPartitioned())
            {
                shard_->CleanTableRange(RangeTableNameOf(table_name), ng_id);
                if (old_schema != nullptr)
                {
                    for (const TableName &index_name : old_schema->IndexNames())
                    {
                        shard_->CleanTableRange(RangeTableNameOf(index_name),
                                                ng_id);
                    }
                }
            }
        }
        else if (req.OpType() == OperationType::AddIndex ||
                 req.OpType() == OperationType::DropIndex)
        {
            for (const TableName &index_name :
                 IndexesNotIn(*old_schema, *catalog_entry.dirty_schema_))
            {
                CleanIndexRangesAndStatistics(
                    index_name, *old_schema->StatisticsObject(), ng_id);
            }
        }
    }

    /**
     * @brief The name of the range-partition table that holds a table's
     * ranges.
     *
     * @param table A base or index table.
     * @return A view of @p table's name, valid while @p table is.
     */
    static TableName RangeTableNameOf(const TableName &table)
    {
        return TableName{
            table.StringView(), TableType::RangePartition, table.Engine()};
    }

    /**
     * @brief The indexes of one schema that another schema does not have.
     *
     * @param schema The schema whose indexes are listed.
     * @param other The schema they are compared with.
     * @return The names of @p schema's indexes absent from @p other, in
     * @p schema's order.
     */
    static std::vector<TableName> IndexesNotIn(const TableSchema &schema,
                                               const TableSchema &other)
    {
        std::vector<TableName> names = schema.IndexNames();
        const std::vector<TableName> other_names = other.IndexNames();
        names.erase(std::remove_if(names.begin(),
                                   names.end(),
                                   [&other_names](const TableName &name)
                                   {
                                       return std::find(other_names.begin(),
                                                        other_names.end(),
                                                        name) !=
                                              other_names.end();
                                   }),
                    names.end());
        return names;
    }

    /**
     * @brief The catalog record of a post-write-all request.
     *
     * A local request references the requester's record. A remote one
     * carries it serialized, or carries none (PostCommit, Rollback); a record
     * the request owns holds it, which the later cores read as the request's
     * payload.
     *
     * @param req The request.
     * @param decode Whether to decode a remote payload; a DROP TABLE's
     * PrepareCommit, a PostCommit and a Rollback carry nothing to decode. A
     * remote request at commit timestamp 0 has no payload string at all.
     * @return The record.
     */
    static CatalogRecord *RequestRecord(PostWriteAllCc &req, bool decode = true)
    {
        if (req.Payload() != nullptr)
        {
            return static_cast<CatalogRecord *>(req.Payload());
        }
        assert(!decode || req.PayloadStr() != nullptr);
        std::unique_ptr<CatalogRecord> decoded_rec =
            std::make_unique<CatalogRecord>();
        if (decode)
        {
            size_t offset = 0;
            decoded_rec->Deserialize(req.PayloadStr()->data(), offset);
        }
        CatalogRecord *rec = decoded_rec.get();
        req.SetDecodedPayload(std::move(decoded_rec));
        return rec;
    }

    /**
     * @brief Drops a table's cc map and, for a range-partitioned table, the cc
     * map of its ranges, on this core.
     *
     * @param table A base or index table.
     * @param ng_id The node group.
     */
    void DropTableCcms(const TableName &table, NodeGroupId ng_id)
    {
        shard_->DropCcm(table, ng_id);
        if (!table.IsHashPartitioned())
        {
            shard_->DropCcm(RangeTableNameOf(table), ng_id);
        }
    }

    /**
     * @brief Points a table's cc map, and the cc map of its ranges if this
     * node has them loaded, at a schema, on this core.
     *
     * @param table A base or index table.
     * @param schema The schema the cc maps take.
     * @param ng_id The node group.
     * @param range_version The version the range cc map takes.
     * @param is_create Whether to create a cc map that does not exist.
     */
    void SetTableCcms(const TableName &table,
                      const TableSchema *schema,
                      NodeGroupId ng_id,
                      uint64_t range_version,
                      bool is_create)
    {
        if (table.IsBase())
        {
            shard_->CreateOrUpdatePkCcMap(table, schema, ng_id, is_create);
        }
        else
        {
            shard_->CreateOrUpdateSkCcMap(table, schema, ng_id, is_create);
        }
        if (table.IsHashPartitioned())
        {
            return;
        }
        const TableName range_table = RangeTableNameOf(table);
        if (shard_->GetTableRangesForATable(range_table, ng_id) != nullptr)
        {
            shard_->CreateOrUpdateRangeCcMap(
                range_table, schema, ng_id, range_version, is_create);
        }
    }

    /**
     * @brief Cleans the node-level structures of an index that is gone: its
     * ranges, and its entry in the table's statistics. Done once, on the last
     * core, after every core dropped the index's cc maps.
     *
     * @param index The index.
     * @param statistics The table's statistics.
     * @param ng_id The node group.
     */
    void CleanIndexRangesAndStatistics(const TableName &index,
                                       Statistics &statistics,
                                       NodeGroupId ng_id)
    {
        shard_->CleanTableRange(RangeTableNameOf(index), ng_id);
        statistics.DropIndex(index);
    }

    /**
     * @brief Installs, on this node, the single initial range of a base or
     * index table that CREATE TABLE made.
     *
     * The range spans the whole key space (a null start key is negative
     * infinity), and its partition id is derived from the table's name; the
     * sequences table's is 0.
     *
     * @param table The base or index table.
     * @param version The range's version: the version the table's kv tables
     * were created at.
     * @param ng_id The node group.
     */
    void InitCreatedTableRange(const TableName &table,
                               uint64_t version,
                               NodeGroupId ng_id)
    {
        const std::string_view name = table.StringView();
        const int partition_id =
            name == "./mysql/sequences"
                ? 0
                : static_cast<int>(std::hash<std::string_view>()(name) & 0xFFF);
        std::vector<InitRangeEntry> range_init_vec;
        range_init_vec.emplace_back(TxKey(), partition_id, version);
        shard_->local_shards_.InitTableRanges(
            RangeTableNameOf(table), range_init_vec, ng_id, true);
    }

    /**
     * @brief Gives an empty range, which this node group owns, its (empty)
     * slices, allocated on the table-ranges heap.
     *
     * @param range The range.
     * @param ng_id The node group.
     */
    void InitEmptyRangeSlices(TableRangeEntry &range, NodeGroupId ng_id)
    {
        std::unique_lock<std::mutex> heap_lk(
            shard_->local_shards_.table_ranges_heap_mux_);
        bool is_override_thd = mi_is_override_thread();
        mi_threadid_t prev_thd = mi_override_thread(
            shard_->local_shards_.GetTableRangesHeapThreadId());
        mi_heap_t *prev_heap =
            mi_heap_set_default(shard_->local_shards_.GetTableRangesHeap());

#if defined(WITH_JEMALLOC)
        uint32_t prev_arena;
        JemallocArenaSwitcher::ReadCurrentArena(prev_arena);
        auto table_range_arena_id =
            shard_->local_shards_.GetTableRangesArenaId();
        JemallocArenaSwitcher::SwitchToArena(table_range_arena_id);
#endif

        std::vector<SliceInitInfo> slices;
        range.InitRangeSlices(
            std::move(slices), ng_id, false, true, UINT64_MAX, false);

        mi_heap_set_default(prev_heap);
        if (is_override_thd)
        {
            mi_override_thread(prev_thd);
        }
        else
        {
            mi_restore_default_thread_id();
        }

#if defined(WITH_JEMALLOC)
        JemallocArenaSwitcher::SwitchToArena(prev_arena);
#endif
        heap_lk.unlock();
    }

    /**
     * @brief Forwards a committed TRUNCATE TABLE to the standby nodes
     * subscribed to this node group.
     *
     * @param req The PostCommit request.
     * @param table_key The table's catalog key.
     * @param catalog_entry The table's catalog entry, still holding the old
     * and the new schema.
     * @param ng_term The node group's leader term.
     */
    void ForwardTruncateToStandbys(const PostWriteAllCc &req,
                                   const CatalogKey &table_key,
                                   const CatalogEntry &catalog_entry,
                                   int64_t ng_term)
    {
        auto forward_entry_ptr = std::make_unique<StandbyForwardEntry>();
        StandbyForwardEntry *forward_entry = forward_entry_ptr.get();
        auto *forward_req = &forward_entry->Request();
        forward_req->set_primary_leader_term(ng_term);
        forward_req->set_tx_number(req.Txn());
        forward_req->set_table_name(table_name_.String());
        forward_req->set_table_type(
            remote::ToRemoteType::ConvertTableType(table_name_.Type()));
        forward_req->set_table_engine(
            remote::ToRemoteType::ConvertTableEngine(table_name_.Engine()));
        forward_req->set_key_shard_code(cc_ng_id_ << 10);
        std::string key_str;
        table_key.Serialize(key_str);
        forward_req->set_key(std::move(key_str));
        std::string rec_str;

        assert(catalog_entry.schema_ != nullptr);
        assert(catalog_entry.dirty_schema_ != nullptr);
        assert(catalog_entry.dirty_schema_version_ > 0);

        CatalogRecord temp_rec;
        temp_rec.SetSchemaImage(catalog_entry.schema_->SchemaImage());
        temp_rec.SetDirtySchemaImage(
            catalog_entry.dirty_schema_->SchemaImage());
        assert(!temp_rec.SchemaImage().empty());
        assert(!temp_rec.DirtySchemaImage().empty());

        temp_rec.Serialize(rec_str);
        forward_req->add_cmd_list(std::move(rec_str));

        forward_req->set_commit_ts(catalog_entry.dirty_schema_version_);
        forward_req->set_schema_version(catalog_entry.schema_version_);
        shard_->ForwardStandbyMessage(forward_entry_ptr.release());
    }

    void UpdateTableLocks(std::string_view table_name_sv,
                          uint64_t schema_version,
                          NonBlockingLock *lock_ptr)
    {
        absl::string_view absl_table_name_sv(table_name_sv.data(),
                                             table_name_sv.size());
        auto table_locks_iter = table_locks_.find(absl_table_name_sv);
        if (table_locks_iter != table_locks_.end())
        {
            table_locks_iter->second.second = schema_version;
        }
        else
        {
            table_locks_.try_emplace(
                std::string(table_name_sv), lock_ptr, schema_version);
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
