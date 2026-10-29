#pragma once

#include <butil/logging.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cdc_scan.h"
#include "log.pb.h"

namespace txlog
{
enum struct LogItemType
{
    DataLog,
    SchemaLog,
    SplitRangeLog,
    ClusterScaleLog,
    ClusterGateLog  ///< The log group's cluster gate slot, while it has one.
};

struct Item
{
public:
    using Pointer = std::shared_ptr<Item>;

    Item() = default;

    Item(uint64_t tx_number,
         uint64_t timestamp,
         std::string log_message,
         LogItemType item_type,
         uint32_t cc_ng = UINT32_MAX)
        : tx_number_(tx_number),
          timestamp_(timestamp),
          log_message_(std::move(log_message)),
          item_type_(item_type),
          cc_ng_(cc_ng)
    {
    }

    uint64_t tx_number_;
    uint64_t timestamp_;
    std::string log_message_;
    LogItemType item_type_;
    uint32_t cc_ng_;
};

class ItemIterator
{
public:
    explicit ItemIterator(std::vector<Item::Pointer> &&item_list)
        : ddl_list_(std::move(item_list)), ddl_idx_(0) {};
    virtual ~ItemIterator() = default;
    ItemIterator(const ItemIterator &) = delete;
    void operator=(const ItemIterator &) = delete;

    virtual void SeekToFirst() = 0;
    virtual bool Valid() = 0;
    virtual void Next() = 0;
    virtual const Item &GetItem() = 0;

    virtual void SeekToDDLFirst() = 0;
    virtual bool ValidDDL() = 0;
    virtual void NextDDL() = 0;
    virtual const Item &GetDDLItem() = 0;

    virtual size_t IteratorNum() = 0;
    virtual void SeekToFirst(size_t idx) = 0;
    virtual bool Valid(size_t idx) = 0;
    virtual void Next(size_t idx) = 0;
    virtual const Item &GetItem(size_t idx) = 0;

protected:
    std::vector<Item::Pointer> ddl_list_;
    size_t ddl_idx_{0};
};

struct WriteLogStateMachineTask;

class LogState
{
public:
    using Pointer = std::unique_ptr<LogState>;

    LogState() = default;
    virtual ~LogState() = default;

    virtual void AddLogItem(uint32_t cc_ng_id,
                            uint64_t tx_number,
                            uint64_t timestamp,
                            const std::string &log_message) = 0;

    virtual void AddLogItemBatch(uint32_t cc_ng_id,
                                 uint64_t tx_number,
                                 uint64_t timestamp,
                                 const std::string &log_message)
    {
        AddLogItem(cc_ng_id, tx_number, timestamp, log_message);
    };

    virtual void FlushLogItemBatch() {};

    virtual std::pair<bool, std::unique_ptr<ItemIterator>> GetLogReplayList(
        uint32_t node_group_id, uint64_t start_timestamp) = 0;

    virtual std::pair<bool, Item::Pointer> SearchTxDataLog(
        uint64_t tx_number, uint32_t cc_ng_id, uint64_t lower_bound_ts = 0) = 0;

    /**
     * @brief Serves one page of the CDC log scan (cdc_scan.h).
     *
     * Called from the RPC thread, concurrently with on_apply; only the leader
     * calls it. Every implementation must read MinCkptTs() for
     * `arrival_complete_ts` before it opens its view of the stores. The
     * default is for states that keep no scannable history.
     *
     * @param request The scan request.
     * @param[out] response The page; its `status` is left to the caller.
     * @param[out] error Set to the reason when the result is not Ok.
     * @return How the scan went; see CdcScanStatus.
     */
    virtual CdcScanStatus CdcScan(const CdcScanRequest & /*request*/,
                                  CdcScanResponse * /*response*/,
                                  std::string *error)
    {
        *error = "this log state keeps no history a CDC scan could read";
        return CdcScanStatus::Unsupported;
    }

    /**
     * Stores cc node group's latest state.
     *
     * latest_txn_no_ is to keep track of the cc node group's latest committed
     * txn number, so we don't get repeated txn numbers.
     *
     * last_ckpt_ts_ stores this cc node's last checkpoint timestamp on this log
     * group, for ReplayLog usage.
     * Besides, for LogStateRocksDBImpl, since the oldest items are deleted,
     * there can be a large amount of "tombstones" in the beginning of each
     * ng_id. As a result, this query might be exceptionally slow: Seek(<ng_id,
     * 0, 0>). To mitigate this problem, remember the last truncate timestamp of
     * each ng_id, and iterate from <ng_id, last_ckpt_ts, 0>.
     */
    struct CcNgInfo
    {
        explicit CcNgInfo(int64_t term,
                          std::string leader_ip,
                          uint32_t leader_port,
                          uint32_t latest_txn_no = 0,
                          uint64_t latest_commit_ts = 0,
                          uint64_t ckpt_ts = 0)
            : term_(term),
              leader_ip_(leader_ip),
              leader_port_(leader_port),
              latest_txn_no_(latest_txn_no),
              latest_commit_ts_(latest_commit_ts),
              last_ckpt_ts_(ckpt_ts)
        {
        }

        int64_t term_;
        std::string leader_ip_;
        uint32_t leader_port_;
        uint32_t latest_txn_no_{};
        uint64_t latest_commit_ts_{};
        uint64_t last_ckpt_ts_{};
    };

    /**
     * Search schema log of transaction tx_number
     * @param tx_number
     * @return whether the schema log is committed and the stage
     */
    std::pair<bool, SchemaOpMessage_Stage> SearchTxSchemaLog(uint64_t tx_number)
    {
        // this func is called in RecoverTx rpc thread, might be concurrent with
        // braft on_apply when processing WriteLogRequest
        std::shared_lock s_lk(log_state_mutex_);

        auto catalog_it = tx_catalog_ops_.find(tx_number);
        if (catalog_it == tx_catalog_ops_.end())
        {
            return {false, SchemaOpMessage_Stage_Stage_MIN};
        }
        return {true, catalog_it->second.SchemasOpStage()};
    }

    virtual int Start()
    {
        return 0;
    }

    void UpsertSchemaOp(uint64_t tx_no,
                        uint64_t commit_ts,
                        const SchemaOpMessage &schema_op)
    {
        // this func is called when on_apply processing WriteLogRequest, might
        // be concurrent with SearchTxSchemaLog() in RecoverTx rpc thread
        std::unique_lock lk(log_state_mutex_);

        assert(!schema_op.table_name_str().empty() &&
               schema_op.table_type() == CcTableType::Primary);

        SchemaOpMessage::Stage new_stage = schema_op.stage();
        // only insert new entry at prepare stage
        if (new_stage ==
            SchemaOpMessage_Stage::SchemaOpMessage_Stage_PrepareSchema)
        {
            auto [it, success] =
                tx_catalog_ops_.try_emplace(tx_no, schema_op, commit_ts);
            if (!success)
            {
                LOG(INFO) << "duplicate prepare log detected, txn: " << tx_no
                          << ", ignore";
            }
            else
            {
                // An index op holds the catalog write lock across its prepare
                // log, so its CDC record goes in here, at the timestamp that
                // log carries: the op's catalog version.
                const uint32_t op_type = CdcDdlOpType(schema_op);
                if (op_type == kCdcAddIndex || op_type == kCdcDropIndex)
                {
                    AppendCdcDdl(commit_ts, commit_ts, tx_no, schema_op);
                }
            }
        }
        else
        {
            auto catalog_it = tx_catalog_ops_.find(tx_no);
            if (catalog_it != tx_catalog_ops_.end())
            {
                // The schema operation has been logged. Only updates the stage.
                CatalogOp &catalog_op = catalog_it->second;
                if (new_stage > catalog_op.SchemasOpStage())
                {
                    if (new_stage == SchemaOpMessage_Stage::
                                         SchemaOpMessage_Stage_PrepareData)
                    {
                        // For the schema operation that need to deal with the
                        // data, such as ADD INDEX.
                        SchemaOpMessage &schema_op_msg =
                            *catalog_op.GetSchemaOpMsg(
                                schema_op.table_name_str(),
                                schema_op.table_type());
                        schema_op_msg.set_last_key_type(
                            schema_op.last_key_type());
                        schema_op_msg.set_last_key_value(
                            schema_op.last_key_value());
                        schema_op_msg.set_new_catalog_blob(
                            schema_op.new_catalog_blob());
                        schema_op_msg.set_stage(new_stage);
                    }
                    else if (new_stage ==
                             SchemaOpMessage_Stage::
                                 SchemaOpMessage_Stage_CommitSchema)
                    {
                        // The op's catalog version, the timestamp its prepare
                        // log carried, before the commit log's replaces it.
                        const uint64_t catalog_ts = catalog_op.CommitTs();
                        // The commit log's timestamp replaces the prepare
                        // log's: 0 for an op that failed after its prepare
                        // log, else the commit timestamp, which CREATE, DROP
                        // and TRUNCATE TABLE re-assign at the commit stage. An
                        // op recovered from its commit log then resumes with
                        // the timestamp it committed at.
                        catalog_op.SetCommitTs(commit_ts);
                        SchemaOpMessage &schema_op_msg =
                            *catalog_op.GetSchemaOpMsg(
                                schema_op.table_name_str(),
                                schema_op.table_type());
                        // A rolled-back add index commits at a real timestamp
                        // too; recovery reads the mark instead.
                        if (schema_op.rolled_back())
                        {
                            schema_op_msg.set_rolled_back(true);
                        }
                        schema_op_msg.set_stage(new_stage);
                        // The commit log itself carries neither the engine
                        // nor the op type; the prepare log's message does.
                        RecordCdcDdlCommit(
                            tx_no, catalog_ts, commit_ts, schema_op_msg);
                    }
                    else if (new_stage == SchemaOpMessage_Stage::
                                              SchemaOpMessage_Stage_CleanSchema)
                    {
                        // Schema logs at CleanSchema stage will be kept in
                        // LogState for some time instead of be erased
                        // immediately. This is to filter those retried stale
                        // WriteLogRequest of previous stage (prepare log) yet
                        // come to log service after clean log finished. The
                        // schema log will be erased when all node group's
                        // ckpt_ts are one hour greater than its commit ts.
                        //
                        // For pure DDL transactions, CatalogOp contains only
                        // one catalog image. For logical alter table DDL inside
                        // DML transactions, CatalogOp might contain multiple
                        // catalog images. Given one catalog, recovery for the
                        // above two scene share same DDL recovery workflow.
                        // Clear the specified SchemaOpMsg.
                        catalog_op.Clear(schema_op.table_name_str(),
                                         schema_op.table_type());
                    }
                }
                else if (new_stage == catalog_op.SchemasOpStage())
                {
                    if (new_stage == SchemaOpMessage_Stage::
                                         SchemaOpMessage_Stage_PrepareData)
                    {
                        SchemaOpMessage &schema_op_msg =
                            *catalog_op.GetSchemaOpMsg(
                                schema_op.table_name_str(),
                                schema_op.table_type());
                        schema_op_msg.set_last_key_type(
                            schema_op.last_key_type());
                        schema_op_msg.set_last_key_value(
                            schema_op.last_key_value());
                        schema_op_msg.set_new_catalog_blob(
                            schema_op.new_catalog_blob());
                    }
                }
                else
                {
                    LOG(INFO)
                        << "duplicate prepare log detected, txn: " << tx_no
                        << ", ignore";
                }
            }
        }
    }

    void UpsertSchemaOpWithinDML(
        uint64_t tx_no,
        uint64_t commit_ts,
        const ::google::protobuf::RepeatedPtrField<SchemaOpMessage> &schemas_op)
    {
        // this func is called when on_apply processing WriteLogRequest, might
        // be concurrent with SearchTxSchemaLog() in RecoverTx rpc thread
        std::unique_lock lk(log_state_mutex_);

        assert(commit_ts > 0);

        SchemaOpMessage::Stage new_stage = schemas_op.at(0).stage();
        auto [it, success] =
            tx_catalog_ops_.try_emplace(tx_no, schemas_op, commit_ts);
        if (!success)
        {
            CatalogOp &catalog_op = it->second;
            SchemaOpMessage::Stage old_stage = catalog_op.SchemasOpStage();
            if (new_stage > old_stage)
            {
                if (new_stage == SchemaOpMessage_Stage::
                                     SchemaOpMessage_Stage_CommitSchema &&
                    old_stage == SchemaOpMessage_Stage::
                                     SchemaOpMessage_Stage_PrepareSchema)
                {
                    catalog_op.CommitAll();
                }
                else if (new_stage == SchemaOpMessage_Stage::
                                          SchemaOpMessage_Stage_CleanSchema &&
                         old_stage == SchemaOpMessage_Stage::
                                          SchemaOpMessage_Stage_CommitSchema)
                {
                    catalog_op.ClearAll();
                }
            }
            else
            {
                LOG(INFO) << "duplicate commit log detected, txn: " << tx_no
                          << ", ignore";
            }
        }
    }

    std::pair<bool, SplitRangeOpMessage_Stage> SearchTxSplitRangeOp(
        uint64_t tx_number)
    {
        // this func is called in RecoverTx rpc thread, might be concurrent with
        // braft on_apply when processing WriteLogRequest
        std::shared_lock s_lk(log_state_mutex_);
        auto iter = tx_split_range_ops_.find(tx_number);
        if (iter == tx_split_range_ops_.end())
        {
            return {false, SplitRangeOpMessage_Stage_Stage_MIN};
        }
        else
        {
            return {true, iter->second.split_range_op_message_.stage()};
        }
    }

    void UpdateSplitRangeOp(uint64_t tx_num,
                            uint64_t commit_ts,
                            const SplitRangeOpMessage &split_range_op_message)
    {
        // this func is called when on_apply processing WriteLogRequest, might
        // be concurrent with SearchTxSplitRangeOp() in RecoverTx rpc thread
        std::unique_lock x_lk(log_state_mutex_);

        SplitRangeOpMessage::Stage new_stage = split_range_op_message.stage();
        // only insert new entry at prepare stage
        if (new_stage ==
            SplitRangeOpMessage_Stage::SplitRangeOpMessage_Stage_PrepareSplit)
        {
            auto [it, success] = tx_split_range_ops_.try_emplace(
                tx_num, split_range_op_message, commit_ts);
            if (!success)
            {
                LOG(INFO) << "duplicate split range prepare log detected, txn: "
                          << tx_num << ", ignore";
            }
        }
        else
        {
            auto split_range_op_it = tx_split_range_ops_.find(tx_num);
            if (split_range_op_it != tx_split_range_ops_.end())
            {
                SplitRangeOpMessage &split_range_msg =
                    split_range_op_it->second.split_range_op_message_;
                if (new_stage > split_range_msg.stage())
                {
                    if (new_stage == SplitRangeOpMessage_Stage::
                                         SplitRangeOpMessage_Stage_CommitSplit)
                    {
                        // slice specs are just written in commit stage log.
                        split_range_msg.clear_slice_keys();
                        split_range_msg.clear_slice_sizes();

                        assert(split_range_op_message.slice_keys_size() + 1 ==
                               split_range_op_message.slice_sizes_size());
                        int idx = 0;
                        for (; idx < split_range_op_message.slice_keys_size();
                             idx++)
                        {
                            split_range_msg.add_slice_keys(
                                split_range_op_message.slice_keys(idx));
                            split_range_msg.add_slice_sizes(
                                split_range_op_message.slice_sizes(idx));
                        }
                        split_range_msg.add_slice_sizes(
                            split_range_op_message.slice_sizes(idx));
                    }
                    else
                    {
                        // SplitRange logs at CleanSplit stage will be kept in
                        // LogState for some time instead of be erased
                        // immediately. This is to filter those retried stale
                        // WriteLogRequest of previous stage (prepare log) yet
                        // come to log service after clean log finished. The
                        // SplitRange log will be erased when all node group's
                        // ckpt_ts are one hour greater than its commit ts.
                        assert(new_stage ==
                               SplitRangeOpMessage_Stage::
                                   SplitRangeOpMessage_Stage_CleanSplit);

                        // Free the memory used by split_range_msg by overriding
                        // it as split_range_msg.Clear() won't free the memory
                        // used by message.
                        split_range_op_it->second.split_range_op_message_ =
                            SplitRangeOpMessage();
                    }
                    split_range_msg.set_stage(new_stage);
                }
                else
                {
                    LOG(INFO)
                        << "duplicate split range log detected, txn: " << tx_num
                        << ", stage: " << int(new_stage) << ", ignore";
                }
            }
        }
    }

    void CleanSplitRangeOps(uint64_t txn)
    {
        std::unique_lock x_lk(log_state_mutex_);
        auto split_range_op_it = tx_split_range_ops_.find(txn);
        if (split_range_op_it != tx_split_range_ops_.end())
        {
            tx_split_range_ops_.erase(split_range_op_it);
        }
    }

    bool UpdateNodeGroupBucketMigrateLog(
        uint64_t tx_num,
        uint64_t commit_ts,
        const DataMigrateTxLogMessage &bucket_migrate_log)
    {
        std::unique_lock x_lk(log_state_mutex_);

        auto cluster_scale_txn = bucket_migrate_log.cluster_scale_tx_number();
        auto log_stage = bucket_migrate_log.stage();

        auto cluster_scale_op_iter =
            tx_cluster_scale_ops_.find(cluster_scale_txn);
        ClusterScaleStage match_cluster_scale_stage;

        if (cluster_scale_op_iter != tx_cluster_scale_ops_.end())
        {
            auto &cluster_scale_op_msg =
                cluster_scale_op_iter->second.cluster_scale_op_message_;
            if (cluster_scale_op_msg.event_type() ==
                ClusterScaleOpMessage_ScaleOpType::
                    ClusterScaleOpMessage_ScaleOpType_AddNodeGroup)
            {
                match_cluster_scale_stage = ClusterScaleStage::ConfigUpdate;
            }
            else
            {
                // Add node group peers op type does not need to do bucket
                // migrate.
                assert(cluster_scale_op_msg.event_type() ==
                       ClusterScaleOpMessage_ScaleOpType::
                           ClusterScaleOpMessage_ScaleOpType_RemoveNode);
                match_cluster_scale_stage = ClusterScaleStage::PrepareScale;
            }
        }

        if (log_stage == DataMigrateTxLogMessage_Stage::
                             DataMigrateTxLogMessage_Stage_Prepare)
        {
            // ClusterScale Transaction has already finished, we need to reject
            // this migration transaction to write preapre log.
            if (cluster_scale_op_iter == tx_cluster_scale_ops_.end())
            {
                LOG(INFO) << "cluster scale log not found, txn: "
                          << cluster_scale_txn
                          << "reject migration transaction write prepare log "
                             "request, txn: "
                          << tx_num;
                return false;
            }

            auto &cluster_scale_op_msg =
                cluster_scale_op_iter->second.cluster_scale_op_message_;
            if (cluster_scale_op_msg.stage() != match_cluster_scale_stage)
            {
                LOG(INFO) << "cluster scale log stage no match, txn: "
                          << cluster_scale_txn
                          << "reject migration transaction write prepare log "
                             "request, txn: "
                          << tx_num;
                return false;
            }
        }

        if (cluster_scale_op_iter != tx_cluster_scale_ops_.end())
        {
            auto &cluster_scale_op_msg =
                cluster_scale_op_iter->second.cluster_scale_op_message_;

            if (cluster_scale_op_msg.stage() == match_cluster_scale_stage)
            {
                uint32_t migration_tx_ng_id = (tx_num >> 32L) >> 10L;

                if (log_stage == DataMigrateTxLogMessage_Stage::
                                     DataMigrateTxLogMessage_Stage_Prepare)
                {
                    auto &ng_bucket_migrate_process =
                        *cluster_scale_op_msg
                             .mutable_node_group_bucket_migrate_process();

                    auto iter =
                        ng_bucket_migrate_process.find(migration_tx_ng_id);
                    if (iter != ng_bucket_migrate_process.end())
                    {
                        if (iter->second.stage() <
                            NodeGroupMigrateMessage_Stage::
                                NodeGroupMigrateMessage_Stage_Prepared)
                        {
                            iter->second.set_stage(
                                NodeGroupMigrateMessage_Stage::
                                    NodeGroupMigrateMessage_Stage_Prepared);
                            iter->second.set_old_owner(migration_tx_ng_id);
                            for (int idx = 0;
                                 idx < bucket_migrate_log.migration_txns_size();
                                 idx++)
                            {
                                // Empty log used by RecoverTx.
                                tx_data_migration_ops_.insert(
                                    bucket_migrate_log.migration_txns(idx));
                                iter->second.add_migration_txns(
                                    bucket_migrate_log.migration_txns(idx));
                            }
                        }
                        else
                        {
                            // The first migration tx is the one that writes the
                            // first prepare log.
                            if (iter->second.migration_txns().Get(0) != tx_num)
                            {
                                LOG(INFO)
                                    << "duplicate migration tx log, but "
                                       "migration txn no match, "
                                       "cluster_scale_txn: "
                                    << cluster_scale_txn
                                    << ", reject migration transaction write "
                                       "prepare log "
                                       "request, migration_txn: "
                                    << tx_num;
                                // We need to reject this migration txn
                                return false;
                            }

                            LOG(INFO) << "duplicate migration tx log "
                                         "detected, txn: "
                                      << tx_num << ", ignore";
                        }
                    }
                }
                else if (log_stage == DataMigrateTxLogMessage_Stage::
                                          DataMigrateTxLogMessage_Stage_Commit)
                {
                    // Write bucket migrate log
                    auto &ng_bucket_migrate_process =
                        *cluster_scale_op_msg
                             .mutable_node_group_bucket_migrate_process();

                    auto ng_bucket_migrate_iter =
                        ng_bucket_migrate_process.find(migration_tx_ng_id);

                    if (ng_bucket_migrate_iter !=
                        ng_bucket_migrate_process.end())
                    {
                        auto &bucket_migrate_process =
                            *ng_bucket_migrate_iter->second
                                 .mutable_bucket_migrate_process();

                        auto new_bucket_migrate_stage =
                            bucket_migrate_log.bucket_stage();
                        auto bucket_ids = bucket_migrate_log.bucket_ids();

                        for (int idx = 0; idx < bucket_ids.size(); idx++)
                        {
                            uint32_t bucket_id = bucket_ids.at(idx);
                            auto bucket_migrate_iter =
                                bucket_migrate_process.find(bucket_id);
                            if (bucket_migrate_iter !=
                                bucket_migrate_process.end())
                            {
                                if (new_bucket_migrate_stage >
                                    bucket_migrate_iter->second.stage())
                                {
                                    // Set bucket migrate stage
                                    bucket_migrate_iter->second.set_stage(
                                        new_bucket_migrate_stage);
                                    // The first log before acquiring bucket
                                    // write lock will update the migration txn
                                    // for this bucket.
                                    if (new_bucket_migrate_stage ==
                                        BucketMigrateStage::BeforeLocking)
                                    {
                                        bucket_migrate_iter->second
                                            .set_migration_txn(tx_num);
                                    }
                                    else if (new_bucket_migrate_stage ==
                                             BucketMigrateStage::PrepareMigrate)
                                    {
                                        bucket_migrate_iter->second
                                            .set_migrate_ts(bucket_migrate_log
                                                                .migrate_ts());
                                    }
                                }
                                else
                                {
                                    LOG(INFO) << "duplicate migration tx log "
                                                 "detected, txn: "
                                              << tx_num
                                              << ", bucket id: " << bucket_id
                                              << ", stage: "
                                              << new_bucket_migrate_stage
                                              << ", ignore";
                                }
                            }
                        }
                    }
                }
                else
                {
                    assert(log_stage ==
                           DataMigrateTxLogMessage_Stage::
                               DataMigrateTxLogMessage_Stage_Clean);
                    auto &ng_bucket_migrate_process =
                        *cluster_scale_op_msg
                             .mutable_node_group_bucket_migrate_process();

                    auto iter =
                        ng_bucket_migrate_process.find(migration_tx_ng_id);
                    if (iter != ng_bucket_migrate_process.end())
                    {
                        if (iter->second.stage() <
                            NodeGroupMigrateMessage_Stage::
                                NodeGroupMigrateMessage_Stage_Cleaned)
                        {
                            // Remove empty log of DataMigrationTx
                            for (auto idx = 0;
                                 idx < iter->second.migration_txns_size();
                                 idx++)
                            {
                                tx_data_migration_ops_.erase(
                                    iter->second.migration_txns(idx));
                            }
                            iter->second.set_stage(
                                NodeGroupMigrateMessage_Stage::
                                    NodeGroupMigrateMessage_Stage_Cleaned);
                        }
                        else
                        {
                            LOG(INFO)
                                << "duplicate migration tx log detected, txn: "
                                << tx_num << ", ignore";
                        }
                    }
                }
            }
        }

        return true;
    }

    std::pair<bool, ClusterScaleStage> SearchTxClusterScaleOp(uint64_t tx_num)
    {
        std::shared_lock s_lk(log_state_mutex_);

        auto iter = tx_cluster_scale_ops_.find(tx_num);
        if (iter == tx_cluster_scale_ops_.end())
        {
            return {false, ClusterScaleStage_MIN};
        }

        return {true, iter->second.cluster_scale_op_message_.stage()};
    }

    bool CheckAllMigrationIsFinished(uint64_t cluster_scale_txn)
    {
        std::shared_lock s_lk(log_state_mutex_);

        auto iter = tx_cluster_scale_ops_.find(cluster_scale_txn);
        if (iter == tx_cluster_scale_ops_.end())
        {
            return false;
        }

        auto &cluster_scale_op_message = iter->second.cluster_scale_op_message_;
        auto &ng_migrate_process =
            cluster_scale_op_message.node_group_bucket_migrate_process();
        for (const auto &info : ng_migrate_process)
        {
            auto &ng_migrate_message = info.second;
            if (ng_migrate_message.stage() !=
                NodeGroupMigrateMessage_Stage::
                    NodeGroupMigrateMessage_Stage_Cleaned)
            {
                return false;
            }
        }

        return true;
    }

    CheckClusterScaleStatusResponse::Status CheckClusterScaleStatus(
        const std::string &id)
    {
        std::shared_lock s_lk(log_state_mutex_);
        for (const auto &scale_ops : tx_cluster_scale_ops_)
        {
            if (scale_ops.second.cluster_scale_op_message_.id() == id)
            {
                if (scale_ops.second.cluster_scale_op_message_.stage() ==
                    ClusterScaleStage::CleanScale)
                {
                    return CheckClusterScaleStatusResponse::Status::
                        CheckClusterScaleStatusResponse_Status_FINISHED;
                }
                else
                {
                    return CheckClusterScaleStatusResponse::Status::
                        CheckClusterScaleStatusResponse_Status_STARTED;
                }
            }
        }

        return CheckClusterScaleStatusResponse::Status::
            CheckClusterScaleStatusResponse_Status_NO_STARTED;
    }

    bool SearchTxDataMigrationOp(uint64_t tx_num)
    {
        std::shared_lock s_lk(log_state_mutex_);
        return tx_data_migration_ops_.count(tx_num) > 0;
    }

    std::pair<bool, uint64_t> UpdateClusterScaleOp(
        uint64_t tx_num,
        uint64_t commit_ts,
        const ClusterScaleOpMessage &cluster_scale_op_message)
    {
        // this func is called when on_apply processing WriteLogRequest,
        // might be concurrent with SearchTxClusterScaleOp() in RecoverTx
        // rpc thread
        std::unique_lock x_lk(log_state_mutex_);

        ClusterScaleStage new_stage = cluster_scale_op_message.stage();
        // only insert new entry at prepare stage
        if (new_stage == ClusterScaleStage::PrepareScale)
        {
            if (newest_cluster_scale_txn_ != ((uint64_t) UINT32_MAX << 32L))
            {
                auto tx_cluster_scale_log_iter =
                    tx_cluster_scale_ops_.find(newest_cluster_scale_txn_);
                if (tx_cluster_scale_log_iter != tx_cluster_scale_ops_.end())
                {
                    if (tx_cluster_scale_log_iter->second
                            .cluster_scale_op_message_.stage() !=
                        ClusterScaleStage::CleanScale)
                    {
                        LOG(INFO) << "Another cluster scale tx "
                                  << tx_cluster_scale_log_iter->first
                                  << " is ongoing, current txn: " << tx_num
                                  << ", write cluster scale prepare log fail.";
                        return {false, tx_cluster_scale_log_iter->first};
                    }

                    if (tx_cluster_scale_log_iter->second
                                .cluster_scale_op_message_.id() ==
                            cluster_scale_op_message.id() &&
                        tx_cluster_scale_log_iter->first != tx_num)
                    {
                        LOG(INFO) << "The requested scale event has already "
                                     "finished, finished txn: "
                                  << tx_cluster_scale_log_iter->first
                                  << ", current txn: " << tx_num
                                  << ", id = " << cluster_scale_op_message.id();
                        return {false, tx_cluster_scale_log_iter->first};
                    }
                }
            }

            auto [it, success] = tx_cluster_scale_ops_.try_emplace(
                tx_num, cluster_scale_op_message, commit_ts);
            if (!success)
            {
                LOG(INFO)
                    << "duplicate cluster scale prepare log detected, txn: "
                    << tx_num << ", ignore";
            }
            else
            {
                newest_cluster_scale_txn_ = tx_num;
            }
        }
        else
        {
            auto cluster_scale_op_it = tx_cluster_scale_ops_.find(tx_num);
            if (cluster_scale_op_it != tx_cluster_scale_ops_.end())
            {
                auto &cluster_scale_msg =
                    cluster_scale_op_it->second.cluster_scale_op_message_;
                if (new_stage > cluster_scale_msg.stage())
                {
                    cluster_scale_msg.set_stage(new_stage);
                    cluster_scale_op_it->second.commit_ts_ = commit_ts;
                }
                else
                {
                    LOG(INFO) << "duplicate cluster scale log detected, txn: "
                              << tx_num << ", ignore";
                }
            }
        }
        return {true, tx_num};
    }

    bool UpdateNgTerm(uint32_t node_group_id,
                      int64_t term,
                      std::string leader_ip,
                      uint32_t leader_port)
    {
        // this func is called when on_apply processing ReplayLogRequest,
        // might be concurrent with GetNgLeaderTerm() in RecoverTx rpc
        // thread
        std::unique_lock lk(log_state_mutex_);

        LOG(INFO) << "Updating node group term, ng:" << node_group_id
                  << ",term:" << term;

        auto ng_it = cc_ng_info_.try_emplace(
            node_group_id, term, leader_ip, leader_port);
        if (!ng_it.second)
        {
            CcNgInfo &leader_info = ng_it.first->second;
            // The node group's term exist in the log state machine. Only
            // updates the term if the existing term is less than the
            // specified term. If term equals leader_info.term, this is a
            // resent ReplayLogRequest.
            if (leader_info.term_ <= term)
            {
                LOG(INFO) << "Update leader_info.term_:" << leader_info.term_
                          << " to: " << term;
                LOG(INFO) << "Update leader_info.leader_ip_"
                          << leader_info.leader_ip_ << " to: " << leader_ip;
                LOG(INFO) << "Update leader_info.leader_port_"
                          << leader_info.leader_port_ << " to: " << leader_port;

                leader_info.term_ = term;
                leader_info.leader_ip_ = leader_ip;
                leader_info.leader_port_ = leader_port;
                return true;
            }
            else
            {
                // The node group's term in the log state machine is greater
                // than the specified term. This is possible when a cc node
                // becomes the leader, sends an update-term request to log
                // groups, and then fails over to a new leader. The new
                // leader sends another update-term request, which arrives
                // at the log group earlier than the old leader's
                // update-term request
                LOG(INFO) << "Nothing to update";
                return false;
            }
        }
        else
        {
            LOG(INFO) << "Create a new leader info";
            return true;
        }
    }

    bool RemoveCcNodeGroup(uint32_t node_group_id, int64_t term)
    {
        // this func is called when on_apply processing
        // RemoveCcNodeGroupRequest, might be concurrent with GetNgLeaderTerm()
        // in RecoverTx rpc thread
        std::unique_lock lk(log_state_mutex_);

        LOG(INFO) << "Removing cc node group, ng:" << node_group_id
                  << ",term:" << term;

        auto ng_it = cc_ng_info_.find(node_group_id);
        if (ng_it != cc_ng_info_.end())
        {
            CcNgInfo &leader_info = ng_it->second;
            // The node group's term exist in the log state machine. Only
            // reset the term if the existing term is less than the
            // specified term.
            assert(leader_info.term_ == term);
            if (leader_info.term_ <= term)
            {
                LOG(INFO) << "Removed cc node group:" << node_group_id;
                cc_ng_info_.erase(ng_it);
                return true;
            }
            else
            {
                // The node group's term in the log state machine is greater
                // than the specified term. This is possible when a cc node
                // becomes the leader, sends an update-term request to log
                // groups, and then fails over to a new leader. The new
                // leader sends another update-term request, which arrives
                // at the log group earlier than the old leader's
                // update-term request
                LOG(INFO)
                    << "Failed to remove cc node group for term not match, term"
                    << term << ", leader term:" << leader_info.term_;
                return false;
            }
        }
        else
        {
            LOG(INFO) << "Not found cc node group to remove";
            return true;
        }
    }

    // now, there should be only one snapshot running.
    virtual void BeginSnapshot() = 0;

    /**
     * read and load snapshot files.
     * files are in form of relative paths to snapshot_path
     */
    virtual int ReadSnapshot(const std::string &snapshot_path,
                             const std::vector<std::string> &files) = 0;

    /**
     * write snapshot files to snapshot_path and return the filenames in
     * relative path. An empty result indicates failure; a successful snapshot
     * always contains the metadata file, even if there are no data logs.
     */
    virtual std::vector<std::string> WriteSnapshot(
        const std::string &snapshot_path) = 0;

    virtual void CleanSnapshotState()
    {
        snapshot_cc_ng_info_.clear();
        snapshot_tx_catalog_ops_.clear();
        snapshot_tx_split_range_ops_.clear();
        snapshot_tx_cluster_scale_ops_.clear();
        snapshot_cdc_ddl_history_.clear();
        snapshot_cluster_gate_.reset();
    }

    virtual void Clear()
    {
        // RecoverTx and CDC scans read this state from RPC threads, and a
        // scan begun while this node led can outlast its step-down.
        std::unique_lock lk(log_state_mutex_);
        cdc_ddl_history_.clear();
        snapshot_cdc_ddl_history_.clear();
        cluster_gate_.reset();
        snapshot_cluster_gate_.reset();
        cc_ng_info_.clear();
        snapshot_cc_ng_info_.clear();
        tx_catalog_ops_.clear();
        snapshot_tx_catalog_ops_.clear();
        tx_split_range_ops_.clear();
        snapshot_tx_split_range_ops_.clear();
        tx_cluster_scale_ops_.clear();
        snapshot_tx_cluster_scale_ops_.clear();
    }

    int64_t GetNgLeaderTerm(uint32_t cc_ng_id) const
    {
        // this func is also called in RecoverTx rpc thread, which might be
        // concurrent with state machine's on_apply when processing
        // ReplayLogRequest
        std::shared_lock s_lk(log_state_mutex_);

        auto iter = cc_ng_info_.find(cc_ng_id);
        if (iter == cc_ng_info_.end())
        {
            return -1;
        }
        else
        {
            return iter->second.term_;
        }
    }

    uint32_t LatestCommittedTxnNumber(uint32_t cc_ng) const
    {
        auto it = cc_ng_info_.find(cc_ng);
        if (it == cc_ng_info_.end())
        {
            return 0;
        }
        return it->second.latest_txn_no_;
    }

    void UpdateLatestCommittedTxnNumber(uint32_t tx_cc_ng, uint32_t tx_ident)
    {
        // access different fields of node group info with RecoverTx RPC
        // thread, no need to lock
        auto it = cc_ng_info_.find(tx_cc_ng);
        if (it == cc_ng_info_.end())
        {
            return;
        }
        CcNgInfo &info = it->second;

        // to handle the situation that committed txn number wraps around
        // uint32, assuming that active txn numbers won't span half of
        // UINT32_MAX
        if (tx_ident - info.latest_txn_no_ < (UINT32_MAX >> 1))
        {
            info.latest_txn_no_ = tx_ident;
        }
    }

    uint64_t LatestCommitTsOfAllNodeGroups() const
    {
        uint64_t latest_commit_ts = 0;
        for (const auto &entry : cc_ng_info_)
        {
            latest_commit_ts =
                std::max(latest_commit_ts, entry.second.latest_commit_ts_);
        }
        return latest_commit_ts;
    }

    void UpdateLatestCommitTs(uint32_t tx_cc_ng, uint64_t commit_ts)
    {
        // access different fields of node group info with RecoverTx RPC
        // thread, no need to lock
        auto it = cc_ng_info_.find(tx_cc_ng);
        if (it == cc_ng_info_.end())
        {
            return;
        }
        CcNgInfo &info = it->second;

        if (commit_ts != 0 && commit_ts > info.latest_commit_ts_)
        {
            info.latest_commit_ts_ = commit_ts;
        }
    }

    void UpdateCkptTs(uint32_t cc_ng, uint64_t timestamp)
    {
        // this func is called when on_apply processing UpdateCkptTsRequest,
        // might be concurrent with LastCkptTimestamp() in RecoverTx rpc
        // thread
        std::unique_lock lk(log_state_mutex_);

        auto it = cc_ng_info_.find(cc_ng);
        if (it != cc_ng_info_.end())
        {
            CcNgInfo &info = it->second;
            if (timestamp > info.last_ckpt_ts_)
            {
                info.last_ckpt_ts_ = timestamp;
                bool updated = UpdateMinCkptTsOfAllNodeGroups();
                if (updated)
                {
                    TryCleanMultiStageOps();
                }
            }
        }
    }

    uint64_t MinCkptTs() const
    {
        std::shared_lock s_lk(log_state_mutex_);
        return min_ckpt_ts_;
    }

    /**
     * @brief Sets how far behind the checkpoint the data log is kept for CDC
     * consumers (docs/cdc-change-stream.md in the EloqDoc repo, §4.5).
     *
     * @param seconds The retention window; 0 keeps no window, which is how
     *        the purges behaved before it existed.
     */
    void SetCdcRetentionSeconds(uint64_t seconds)
    {
        cdc_retention_us_.store(seconds * 1000 * 1000,
                                std::memory_order_relaxed);
    }

    /**
     * @brief The CDC retention floor for a checkpoint.
     *
     * A purge may remove a file only if every key in it lies below the
     * floor, so every record within the retention window behind the
     * checkpoint stays readable.
     *
     * @param ckpt_ts The checkpoint timestamp, in microseconds like every
     *        commit timestamp.
     * @return `ckpt_ts` less the retention window, or 0 if the window reaches
     *         past it.
     */
    uint64_t CdcRetentionFloor(uint64_t ckpt_ts) const
    {
        const uint64_t retention_us =
            cdc_retention_us_.load(std::memory_order_relaxed);
        return ckpt_ts > retention_us ? ckpt_ts - retention_us : 0;
    }

    /**
     * @brief Drops the CDC DDL history's records below a data log purge's
     * floor.
     *
     * Called by the purges of the data log (the RocksDB state's SST purge, the
     * cloud state's move to the archive) whenever one runs, with the floor it
     * purges by, so that DDL records leave with the data they are scanned
     * with. Trimming at every checkpoint instead would drop each record as
     * soon as a scan could return it unless a retention window is set. Runs
     * on the purge's own thread.
     *
     * @param floor The purge's floor: records whose ddl_ts is below it go.
     */
    void TrimCdcDdlHistory(uint64_t floor)
    {
        std::unique_lock lk(log_state_mutex_);
        cdc_ddl_history_.erase(
            cdc_ddl_history_.begin(),
            cdc_ddl_history_.lower_bound(CdcTimestampPosition(floor)));
    }

    /**
     * @brief Copies part of the CDC DDL history, the source of a scan's DDL
     * stream (CdcDdlHistoryCursor).
     *
     * Called from the RPC thread, concurrently with on_apply.
     *
     * @param after A DDL-stream position: empty, a timestamp
     *        (CdcTimestampPosition) or a record's position (CdcDdlPosition).
     * @param upper_ts The highest ddl_ts to copy.
     * @param[out] records Appended with every record positioned strictly
     *        after `after` whose ddl_ts is at most `upper_ts`, in position
     *        order.
     */
    void CopyCdcDdlHistory(const std::string &after,
                           uint64_t upper_ts,
                           std::vector<CdcDdlRecord> *records) const
    {
        std::shared_lock s_lk(log_state_mutex_);
        for (auto it = cdc_ddl_history_.upper_bound(after);
             it != cdc_ddl_history_.end() && it->second.ddl_ts() <= upper_ts;
             ++it)
        {
            records->push_back(it->second);
        }
    }

    uint64_t LastCkptTimestamp(uint32_t cc_ng_id)
    {
        // this func is also called in RecoverTx rpc thread, which might be
        // concurrent with state machine's on_apply when processing
        // ReplayLogRequest
        std::shared_lock s_lk(log_state_mutex_);

        auto iter = cc_ng_info_.find(cc_ng_id);
        if (iter == cc_ng_info_.end())
        {
            return 0;
        }
        else
        {
            return iter->second.last_ckpt_ts_;
        }
    }

    std::unordered_map<uint32_t, CcNgInfo> CopyCcNgInfo() const
    {
        std::shared_lock s_lk(log_state_mutex_);
        std::unordered_map<uint32_t, CcNgInfo> cc_ng_info_copy = cc_ng_info_;
        return cc_ng_info_copy;
    }

    const std::unordered_map<uint32_t, CcNgInfo> &GetCcNgInfo() const
    {
        // This is only used in on_apply when passing node group leader info to
        // multi-stage txn coordinator. No need for lock.
        return cc_ng_info_;
    }

    virtual uint64_t GetApproximateReplayLogSize()
    {
        return 0;
    };

protected:
    /**
     * @brief The snapshot file of the CDC DDL history, in the states that
     * serve CDC scans.
     */
    static constexpr const char *kCdcDdlHistoryFile = "cdc_ddl_history";

    /**
     * @brief The snapshot file of the cluster gate slot, in the states that
     * serve CDC scans.
     */
    static constexpr const char *kClusterGateFile = "cluster_gate";

    /**
     * @brief A scan's DDL stream over this state's CDC DDL history.
     *
     * @return A cursor that copies from the history when it is positioned.
     */
    CdcDdlHistoryCursor CdcDdlStream() const
    {
        return CdcDdlHistoryCursor(
            [this](const std::string &after,
                   uint64_t upper_ts,
                   std::vector<CdcDdlRecord> *records)
            { CopyCdcDdlHistory(after, upper_ts, records); });
    }

public:
    /**
     * @brief Applies a cluster gate log entry (EloqDoc
     * docs/cdc-change-stream.md §6.2): the slot takes the entry's value if
     * the entry is newer than the value it holds.
     *
     * The slot is one value overwritten in place, not a per-operation record,
     * and is never purged: only a resume clears a pause. Every gate
     * transaction is pinned to log group 0, so only that group's slot is ever
     * set, and its successive entries carry increasing commit timestamps.
     *
     * @param txn The gate transaction.
     * @param commit_ts The entry's commit timestamp, the value's version.
     * @param msg The gate's new value.
     */
    void UpdateClusterGate(uint64_t txn,
                           uint64_t commit_ts,
                           const ClusterGateLogMessage &msg)
    {
        // on_apply writes; RecoverTx reads from an RPC thread.
        std::unique_lock lk(log_state_mutex_);
        if (cluster_gate_.has_value() && cluster_gate_->version >= commit_ts)
        {
            return;
        }
        cluster_gate_ = ClusterGateSlot{msg, commit_ts, txn};
    }

    /**
     * @brief Whether a gate transaction's log entry was applied, for a write
     * log retried after its result went unknown and for orphan lock recovery
     * (RecoverTx), which must not clear a logged gate tx's lock.
     *
     * @param txn The gate transaction.
     * @return true if the slot holds that transaction's value.
     */
    bool SearchTxClusterGate(uint64_t txn) const
    {
        std::shared_lock s_lk(log_state_mutex_);
        return cluster_gate_.has_value() && cluster_gate_->txn == txn;
    }

    /**
     * @brief The cluster gate slot, for tests and for the gate's replay.
     *
     * @param[out] msg The gate's value, when the slot holds one.
     * @param[out] version Its commit timestamp.
     * @param[out] txn The transaction that set it.
     * @return false if the slot is empty.
     */
    bool GetClusterGate(ClusterGateLogMessage *msg,
                        uint64_t *version,
                        uint64_t *txn) const
    {
        std::shared_lock s_lk(log_state_mutex_);
        if (!cluster_gate_.has_value())
        {
            return false;
        }
        *msg = cluster_gate_->value;
        *version = cluster_gate_->version;
        *txn = cluster_gate_->txn;
        return true;
    }

protected:
    void MakeCopyOfNgInfoAndCatalogOps()
    {
        snapshot_cc_ng_info_ = cc_ng_info_;
        snapshot_tx_catalog_ops_ = tx_catalog_ops_;
        snapshot_tx_split_range_ops_ = tx_split_range_ops_;
        snapshot_tx_cluster_scale_ops_ = tx_cluster_scale_ops_;
        // A purge thread may trim the history meanwhile (TrimCdcDdlHistory).
        std::shared_lock s_lk(log_state_mutex_);
        // Under the lock for the reason GetClusterGateList() gives.
        snapshot_cluster_gate_ = cluster_gate_;
        snapshot_cdc_ddl_history_.clear();
        snapshot_cdc_ddl_history_.reserve(cdc_ddl_history_.size());
        for (const auto &[position, record] : cdc_ddl_history_)
        {
            snapshot_cdc_ddl_history_.push_back(record);
        }
    }

    /**
     * @brief Writes the snapshot's copy of the CDC DDL history.
     *
     * The history has a file of its own in the states that serve CDC scans,
     * so the file the memory state shares with them keeps its format. Layout:
     * a u32 record count, then per record a u32 length and the serialized
     * CdcDdlRecord, which holds everything its position is rebuilt from.
     *
     * @param os The snapshot file.
     */
    void WriteSnapshotCdcDdlHistoryTo(std::ofstream &os)
    {
        uint32_t cnt = snapshot_cdc_ddl_history_.size();
        os.write(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        LOG(INFO) << "write CDC DDL history size: " << cnt;
        std::string buf;
        for (const CdcDdlRecord &record : snapshot_cdc_ddl_history_)
        {
            buf.clear();
            record.SerializeToString(&buf);
            uint32_t length = buf.size();
            os.write(reinterpret_cast<char *>(&length), sizeof(uint32_t));
            os.write(buf.data(), length);
        }
    }

    /**
     * @brief Loads a CDC DDL history that WriteSnapshotCdcDdlHistoryTo()
     * wrote, replacing the current one.
     *
     * @param is The snapshot file.
     * @return False if the file is truncated, a record's length runs past the
     *         file's end, or a record does not parse; the history is then
     *         left as it was.
     */
    bool LoadCdcDdlHistoryFrom(std::ifstream &is)
    {
        // The file's end bounds every record length read from it, so a
        // corrupt length fails the load instead of sizing a huge buffer.
        const std::streampos begin = is.tellg();
        is.seekg(0, std::ios::end);
        const std::streampos end = is.tellg();
        is.seekg(begin);

        std::map<std::string, CdcDdlRecord> history;
        uint32_t cnt;
        is.read(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        if (is.fail())
        {
            LOG(ERROR) << "snapshot read failed: could not read the CDC DDL "
                          "history count";
            return false;
        }
        std::string buf;
        for (uint32_t i = 0; i < cnt; i++)
        {
            uint32_t length;
            is.read(reinterpret_cast<char *>(&length), sizeof(uint32_t));
            if (is.fail() || length > end - is.tellg())
            {
                LOG(ERROR) << "snapshot read failed: CDC DDL record " << i
                           << " of " << cnt << " is truncated or corrupt";
                return false;
            }
            buf.resize(length);
            is.read(buf.data(), length);
            CdcDdlRecord record;
            if (is.fail() || !record.ParseFromString(buf))
            {
                LOG(ERROR) << "snapshot read failed: CDC DDL record " << i
                           << " of " << cnt << " is truncated or corrupt";
                return false;
            }
            std::string position =
                CdcDdlPosition(record.ddl_ts(), record.txn_number());
            history.emplace(std::move(position), std::move(record));
        }
        LOG(INFO) << "read CDC DDL history size: " << cnt;
        std::unique_lock lk(log_state_mutex_);
        cdc_ddl_history_ = std::move(history);
        return true;
    }

    /**
     * @brief Writes the snapshot's copy of the cluster gate slot.
     *
     * The slot has a file of its own, as the CDC DDL history does, so the
     * file the memory state shares keeps its format. Layout: a u8 that is 1
     * when the slot holds a value, then the u64 version, the u64 txn, a u32
     * length and the serialized ClusterGateLogMessage.
     *
     * @param os The snapshot file.
     */
    void WriteSnapshotClusterGateTo(std::ofstream &os)
    {
        uint8_t present = snapshot_cluster_gate_.has_value() ? 1 : 0;
        os.write(reinterpret_cast<char *>(&present), sizeof(uint8_t));
        if (!present)
        {
            return;
        }
        std::string buf;
        snapshot_cluster_gate_->value.SerializeToString(&buf);
        uint32_t length = buf.size();
        os.write(reinterpret_cast<char *>(&snapshot_cluster_gate_->version),
                 sizeof(uint64_t));
        os.write(reinterpret_cast<char *>(&snapshot_cluster_gate_->txn),
                 sizeof(uint64_t));
        os.write(reinterpret_cast<char *>(&length), sizeof(uint32_t));
        os.write(buf.data(), length);
        LOG(INFO) << "write cluster gate, version: "
                  << snapshot_cluster_gate_->version
                  << ", paused: " << snapshot_cluster_gate_->value.paused();
    }

    /**
     * @brief Loads a cluster gate slot that WriteSnapshotClusterGateTo()
     * wrote, replacing the current one.
     *
     * @param is The snapshot file.
     * @return False if the file is truncated, the value's length runs past
     *         the file's end, or the value does not parse; the slot is then
     *         left as it was.
     */
    bool LoadClusterGateFrom(std::ifstream &is)
    {
        const std::streampos begin = is.tellg();
        is.seekg(0, std::ios::end);
        const std::streampos end = is.tellg();
        is.seekg(begin);

        uint8_t present;
        is.read(reinterpret_cast<char *>(&present), sizeof(uint8_t));
        if (is.fail() || present > 1)
        {
            LOG(ERROR) << "snapshot read failed: corrupt cluster gate file";
            return false;
        }
        std::optional<ClusterGateSlot> slot;
        if (present)
        {
            ClusterGateSlot read;
            uint32_t length;
            is.read(reinterpret_cast<char *>(&read.version), sizeof(uint64_t));
            is.read(reinterpret_cast<char *>(&read.txn), sizeof(uint64_t));
            is.read(reinterpret_cast<char *>(&length), sizeof(uint32_t));
            if (is.fail() || length > end - is.tellg())
            {
                LOG(ERROR) << "snapshot read failed: the cluster gate is "
                              "truncated or corrupt";
                return false;
            }
            std::string buf(length, '\0');
            is.read(buf.data(), length);
            if (is.fail() || !read.value.ParseFromString(buf))
            {
                LOG(ERROR) << "snapshot read failed: the cluster gate is "
                              "truncated or corrupt";
                return false;
            }
            slot = std::move(read);
            LOG(INFO) << "read cluster gate, version: " << slot->version
                      << ", paused: " << slot->value.paused();
        }
        std::unique_lock lk(log_state_mutex_);
        cluster_gate_ = std::move(slot);
        return true;
    }

    void WriteSnapshotNgInfoAndCatalogOpsTo(std::ofstream &os)
    {
        uint32_t cnt = snapshot_cc_ng_info_.size();
        os.write(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        LOG(INFO) << "write snapshot leader info size: " << cnt;
        for (const auto &[ng_id, leader_info] : snapshot_cc_ng_info_)
        {
            uint32_t ng = ng_id;
            int64_t term = leader_info.term_;
            std::string leader_ip = leader_info.leader_ip_;
            uint32_t leader_port = leader_info.leader_port_;
            uint32_t latest_txn_no = leader_info.latest_txn_no_;
            uint64_t latest_commit_ts = leader_info.latest_commit_ts_;
            uint64_t last_ckpt_ts = leader_info.last_ckpt_ts_;
            // cc ng id
            os.write(reinterpret_cast<char *>(&ng), sizeof(uint32_t));
            // cc ng term
            os.write(reinterpret_cast<char *>(&term), sizeof(int64_t));
            // cc ng leader ip, 1 byte(max 256) is enough for storing both
            // ipv4 and ipv6 address length
            uint8_t leader_ip_len = (uint8_t) leader_ip.size();
            os.write(reinterpret_cast<char *>(&leader_ip_len), sizeof(uint8_t));
            os.write(reinterpret_cast<char *>(leader_ip.data()), leader_ip_len);
            // cc ng leader port
            os.write(reinterpret_cast<char *>(&leader_port), sizeof(uint32_t));
            // latest txn
            os.write(reinterpret_cast<char *>(&latest_txn_no),
                     sizeof(uint32_t));
            os.write(reinterpret_cast<char *>(&latest_commit_ts),
                     sizeof(uint64_t));
            os.write(reinterpret_cast<char *>(&last_ckpt_ts), sizeof(uint64_t));
            LOG(INFO) << "write leader info, ng_id: " << ng_id
                      << ", term: " << leader_info.term_ << ", leader_ip_len: "
                      << static_cast<unsigned>(leader_ip_len)
                      << ", leader_ip: " << leader_ip
                      << ", leader_port: " << leader_port
                      << ", latest_txn_no: " << latest_txn_no
                      << ", last_ckpt_ts: " << last_ckpt_ts;
        }
        cnt = snapshot_tx_catalog_ops_.size();
        os.write(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        LOG(INFO) << "write catalog ops size: " << cnt;
        std::string buf;
        for (const auto &[txn, catalog_op] : snapshot_tx_catalog_ops_)
        {
            uint64_t txn_no = txn;
            os.write(reinterpret_cast<char *>(&txn_no), sizeof(uint64_t));

            uint64_t commit_ts = catalog_op.CommitTs();
            os.write(reinterpret_cast<char *>(&commit_ts), sizeof(uint64_t));

            uint16_t schemas_cnt = catalog_op.SchemaOpMsgCount();
            os.write(reinterpret_cast<char *>(&schemas_cnt), sizeof(uint16_t));

            for (uint16_t idx = 0; idx < schemas_cnt; ++idx)
            {
                buf.clear();

                const SchemaOpMessage &schema_op_msg =
                    catalog_op.SchemaOpMsgs()[idx];
                schema_op_msg.SerializeToString(&buf);
                uint32_t length = buf.size();
                os.write(reinterpret_cast<char *>(&length), sizeof(uint32_t));
                os.write(buf.data(), length);
                LOG(INFO) << "write catalog op, txn: " << txn_no
                          << ", table name: " << schema_op_msg.table_name_str()
                          << ", stage: " << int(schema_op_msg.stage())
                          << ", commit_ts: " << commit_ts;
            }
        }
        cnt = snapshot_tx_split_range_ops_.size();
        os.write(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        LOG(INFO) << "write split range ops size: " << cnt;
        for (const auto &[txn, split_range_op] : snapshot_tx_split_range_ops_)
        {
            buf.clear();
            uint64_t txn_no = txn;
            const SplitRangeOpMessage &split_range_op_msg =
                split_range_op.split_range_op_message_;
            uint64_t commit_ts = split_range_op.commit_ts_;
            os.write(reinterpret_cast<char *>(&txn_no), sizeof(uint64_t));
            split_range_op_msg.SerializeToString(&buf);
            uint32_t length = buf.size();
            os.write(reinterpret_cast<char *>(&length), sizeof(uint32_t));
            os.write(buf.data(), length);
            os.write(reinterpret_cast<char *>(&commit_ts), sizeof(uint64_t));
            LOG(INFO) << "write split range op, txn: " << txn_no
                      << ", range table name: "
                      << split_range_op_msg.table_name()
                      << ", stage: " << int(split_range_op_msg.stage())
                      << ", commit_ts: " << commit_ts;
        }

        cnt = snapshot_tx_cluster_scale_ops_.size();
        os.write(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        LOG(INFO) << "write cluster scale ops size: " << cnt;
        for (const auto &[txn, scale_op] : snapshot_tx_cluster_scale_ops_)
        {
            buf.clear();
            uint64_t txn_no = txn;
            const ClusterScaleOpMessage &scale_op_msg =
                scale_op.cluster_scale_op_message_;
            uint64_t commit_ts = scale_op.commit_ts_;
            os.write(reinterpret_cast<char *>(&txn_no), sizeof(uint64_t));
            scale_op_msg.SerializeToString(&buf);
            uint32_t length = buf.size();
            os.write(reinterpret_cast<char *>(&length), sizeof(uint32_t));
            os.write(buf.data(), length);
            os.write(reinterpret_cast<char *>(&commit_ts), sizeof(uint64_t));
            LOG(INFO) << "write cluster scale op, txn: " << txn_no
                      << ", stage: " << int(scale_op_msg.stage())
                      << ", commit_ts: " << commit_ts;
        }
    }

    void LoadNgInfoAndCatalogOpsFrom(std::ifstream &is)
    {
        uint32_t cnt;
        is.read(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        if (is.fail())
        {
            LOG(ERROR) << "snapshot read failed: could not read ng_info count";
            return;
        }
        LOG(INFO) << "read snapshot leader info size: " << cnt;
        for (uint32_t i = 0; i < cnt; i++)
        {
            uint32_t ng_id;
            int64_t term;
            uint8_t leader_ip_len;
            std::string leader_ip;
            uint32_t leader_port;
            uint32_t latest_txn_no;
            uint64_t latest_commit_ts;
            uint64_t last_ckpt_ts;
            // cc ng id
            is.read(reinterpret_cast<char *>(&ng_id), sizeof(uint32_t));
            // cc ng term
            is.read(reinterpret_cast<char *>(&term), sizeof(int64_t));
            // cc ng leader ip
            is.read(reinterpret_cast<char *>(&leader_ip_len), sizeof(uint8_t));
            leader_ip.resize(leader_ip_len);
            is.read(reinterpret_cast<char *>(leader_ip.data()), leader_ip_len);
            // cc ng leader port
            is.read(reinterpret_cast<char *>(&leader_port), sizeof(uint32_t));
            // cc ng latest txn
            is.read(reinterpret_cast<char *>(&latest_txn_no), sizeof(uint32_t));
            // latest commit ts
            is.read(reinterpret_cast<char *>(&latest_commit_ts),
                    sizeof(uint64_t));
            // cc ng lat ckpt ts
            is.read(reinterpret_cast<char *>(&last_ckpt_ts), sizeof(uint64_t));
            if (is.fail())
            {
                LOG(ERROR) << "snapshot read failed: truncated ng_info entry "
                           << i << " of " << cnt;
                return;
            }
            cc_ng_info_.try_emplace(ng_id,
                                    term,
                                    leader_ip,
                                    leader_port,
                                    latest_txn_no,
                                    latest_commit_ts,
                                    last_ckpt_ts);
            LOG(INFO) << "read snapshot leader info: ng_id: " << ng_id
                      << ", term: " << term << ", leader_ip_len: "
                      << static_cast<unsigned>(leader_ip_len)
                      << ", leader_ip: " << leader_ip
                      << ", leader_port: " << leader_port
                      << ", latest_txn_no: " << latest_txn_no
                      << ", latest_commit_ts: " << latest_commit_ts
                      << ", last_ckpt_ts: " << last_ckpt_ts;
        }
        is.read(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        if (is.fail())
        {
            LOG(ERROR)
                << "snapshot read failed: could not read catalog ops count";
            return;
        }
        LOG(INFO) << "read snapshot catalog ops size: " << cnt;
        std::string buf;
        for (uint32_t i = 0; i < cnt; i++)
        {
            uint64_t txn;
            is.read(reinterpret_cast<char *>(&txn), sizeof(uint64_t));

            uint64_t commit_ts;
            is.read(reinterpret_cast<char *>(&commit_ts), sizeof(uint64_t));

            uint16_t schemas_cnt;
            is.read(reinterpret_cast<char *>(&schemas_cnt), sizeof(uint16_t));
            if (is.fail())
            {
                LOG(ERROR)
                    << "snapshot read failed: truncated catalog op header " << i
                    << " of " << cnt;
                return;
            }

            std::vector<SchemaOpMessage> msgs;
            msgs.reserve(schemas_cnt);
            for (uint16_t idx = 0; idx < schemas_cnt; ++idx)
            {
                uint32_t length;
                SchemaOpMessage &msg = msgs.emplace_back();
                is.read(reinterpret_cast<char *>(&length), sizeof(uint32_t));
                buf.resize(length);
                is.read(buf.data(), length);
                if (is.fail())
                {
                    LOG(ERROR)
                        << "snapshot read failed: truncated schema op message "
                        << idx << " of " << schemas_cnt << " in catalog op "
                        << i;
                    return;
                }
                if (!msg.ParseFromString(buf))
                {
                    LOG(ERROR)
                        << "snapshot read failed: could not parse schema op "
                           "message "
                        << idx << " of " << schemas_cnt << " in catalog op "
                        << i;
                    is.setstate(std::ios::failbit);
                    return;
                }
                LOG(INFO) << "read snapshot schema op, txn: " << txn
                          << ", table name: " << msg.table_name_str()
                          << ", stage: " << int(msg.stage())
                          << ", commit_ts: " << commit_ts;
            }

            tx_catalog_ops_.try_emplace(txn, std::move(msgs), commit_ts);
        }
        is.read(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        if (is.fail())
        {
            LOG(ERROR)
                << "snapshot read failed: could not read split range ops count";
            return;
        }
        LOG(INFO) << "read snapshot split range ops size: " << cnt;
        for (uint32_t i = 0; i < cnt; i++)
        {
            uint64_t txn;
            uint32_t length;
            SplitRangeOpMessage split_range_op_msg;
            uint64_t commit_ts;

            is.read(reinterpret_cast<char *>(&txn), sizeof(uint64_t));
            is.read(reinterpret_cast<char *>(&length), sizeof(uint32_t));
            buf.resize(length);
            is.read(buf.data(), length);
            if (is.fail())
            {
                LOG(ERROR)
                    << "snapshot read failed: truncated split range op entry "
                    << i << " of " << cnt;
                return;
            }
            if (!split_range_op_msg.ParseFromString(buf))
            {
                LOG(ERROR)
                    << "snapshot read failed: could not parse split range op "
                       "message "
                    << i << " of " << cnt;
                is.setstate(std::ios::failbit);
                return;
            }
            is.read(reinterpret_cast<char *>(&commit_ts), sizeof(uint64_t));
            if (is.fail())
            {
                LOG(ERROR) << "snapshot read failed: truncated split range op "
                              "commit_ts entry "
                           << i << " of " << cnt;
                return;
            }
            tx_split_range_ops_.try_emplace(txn, split_range_op_msg, commit_ts);
            LOG(INFO) << "read snapshot split range op, txn: " << txn
                      << ", range table name: "
                      << split_range_op_msg.table_name()
                      << ", stage: " << int(split_range_op_msg.stage())
                      << ", commit_ts: " << commit_ts;
        }

        is.read(reinterpret_cast<char *>(&cnt), sizeof(uint32_t));
        if (is.fail())
        {
            LOG(ERROR) << "snapshot read failed: could not read cluster scale "
                          "ops count";
            return;
        }
        LOG(INFO) << "read snapshot cluster scale ops size: " << cnt;
        for (uint32_t i = 0; i < cnt; i++)
        {
            uint64_t txn;
            uint32_t length;
            ClusterScaleOpMessage scale_op_msg;
            uint64_t commit_ts;
            is.read(reinterpret_cast<char *>(&txn), sizeof(uint64_t));
            is.read(reinterpret_cast<char *>(&length), sizeof(uint32_t));
            buf.resize(length);
            is.read(buf.data(), length);
            if (is.fail())
            {
                LOG(ERROR)
                    << "snapshot read failed: truncated cluster scale op entry "
                    << i << " of " << cnt;
                return;
            }
            if (!scale_op_msg.ParseFromString(buf))
            {
                LOG(ERROR)
                    << "snapshot read failed: could not parse cluster scale op "
                       "message "
                    << i << " of " << cnt;
                is.setstate(std::ios::failbit);
                return;
            }
            is.read(reinterpret_cast<char *>(&commit_ts), sizeof(uint64_t));
            if (is.fail())
            {
                LOG(ERROR)
                    << "snapshot read failed: truncated cluster scale op "
                       "commit_ts entry "
                    << i << " of " << cnt;
                return;
            }
            tx_cluster_scale_ops_.try_emplace(txn, scale_op_msg, commit_ts);
            LOG(INFO) << "read snapshot cluster scale op, txn: " << txn
                      << ", stage: " << int(scale_op_msg.stage())
                      << ", commit_ts: " << commit_ts;
        }
    }

    void GetSchemaOpList(std::vector<Item::Pointer> &res)
    {
        for (const auto &[txn, catalog_op] : tx_catalog_ops_)
        {
            for (uint16_t idx = 0; idx < catalog_op.SchemaOpMsgCount(); ++idx)
            {
                const SchemaOpMessage &msg = catalog_op.SchemaOpMsgs()[idx];
                if (msg.stage() !=
                    SchemaOpMessage_Stage::SchemaOpMessage_Stage_CleanSchema)
                {
                    std::string schema_op_str;
                    msg.SerializeToString(&schema_op_str);
                    res.emplace_back(
                        std::make_shared<Item>(txn,
                                               catalog_op.CommitTs(),
                                               std::move(schema_op_str),
                                               LogItemType::SchemaLog));
                }
            }
        }
    }

    void GetSplitRangeOpList(std::vector<Item::Pointer> &res)
    {
        for (const auto &[txn, split_range_op] : tx_split_range_ops_)
        {
            if (split_range_op.split_range_op_message_.stage() ==
                SplitRangeOpMessage_Stage::SplitRangeOpMessage_Stage_CleanSplit)
            {
                continue;
            }
            std::string split_range_op_str;
            // Add table name firstly
            std::string table_name =
                split_range_op.split_range_op_message_.table_name();
            uint8_t tabname_len = table_name.length();
            const char *ptr = reinterpret_cast<const char *>(&tabname_len);
            split_range_op_str.append(ptr, sizeof(uint8_t));
            split_range_op_str.append(table_name.data(), tabname_len);
            auto table_engine =
                split_range_op.split_range_op_message_.table_engine();
            const char *table_engine_ptr =
                reinterpret_cast<const char *>(&table_engine);
            split_range_op_str.append(table_engine_ptr, sizeof(uint8_t));
            // then, add split range op
            split_range_op.split_range_op_message_.AppendToString(
                &split_range_op_str);

            res.emplace_back(
                std::make_shared<Item>(txn,
                                       split_range_op.commit_ts_,
                                       std::move(split_range_op_str),
                                       LogItemType::SplitRangeLog));
        }
    }

    void GetClusterScaleOpList(std::vector<Item::Pointer> &res)
    {
        for (const auto &[txn, scale_op] : tx_cluster_scale_ops_)
        {
            if (scale_op.cluster_scale_op_message_.stage() ==
                ClusterScaleStage::CleanScale)
            {
                continue;
            }
            std::string scale_op_str;
            scale_op.cluster_scale_op_message_.AppendToString(&scale_op_str);
            res.emplace_back(
                std::make_shared<Item>(txn,
                                       scale_op.commit_ts_,
                                       std::move(scale_op_str),
                                       LogItemType::ClusterScaleLog));
        }
        // There could only be at most one ongoing cluster scale operation.
        assert(res.size() <= 1);
    }

    /**
     * @brief Adds the cluster gate slot to a replay list, as one item, when
     * the slot holds a value: a recovering node group reinstalls the gate
     * from it, however old the value is.
     *
     * @param[out] res The replay list.
     */
    void GetClusterGateList(std::vector<Item::Pointer> &res)
    {
        // The list is built while the ReplayLog entry applies, on the thread
        // that writes the slot, so no write interleaves today. The lock keeps
        // the read safe if a list is ever built elsewhere: a torn copy of the
        // protobuf would crash, not ship a stale item.
        std::shared_lock s_lk(log_state_mutex_);
        if (!cluster_gate_.has_value())
        {
            return;
        }
        std::string gate_str;
        cluster_gate_->value.AppendToString(&gate_str);
        res.emplace_back(std::make_shared<Item>(cluster_gate_->txn,
                                                cluster_gate_->version,
                                                std::move(gate_str),
                                                LogItemType::ClusterGateLog));
    }

    bool UpdateMinCkptTsOfAllNodeGroups()
    {
        uint64_t min_ts = UINT64_MAX;
        for (const auto &[ng_id, ng_info] : cc_ng_info_)
        {
            min_ts = std::min(min_ts, ng_info.last_ckpt_ts_);
        }
        if (min_ts > min_ckpt_ts_)
        {
            min_ckpt_ts_ = min_ts;
            return true;
        }
        return false;
    }

    /**
     * The multi-stage logs at clean stage will be erased when all
     * node group's ckpt_ts are one hour greater than its commit ts.
     */
    void TryCleanMultiStageOps()
    {
        using namespace std::chrono_literals;
        uint64_t one_hour = std::chrono::microseconds(1h).count();
        for (auto it = tx_catalog_ops_.begin(); it != tx_catalog_ops_.end();)
        {
            const CatalogOp &op = it->second;
            auto stage = op.SchemasOpStage();
            if (stage ==
                    SchemaOpMessage_Stage::SchemaOpMessage_Stage_CleanSchema &&
                min_ckpt_ts_ > op.CommitTs() + one_hour)
            {
                LOG(INFO) << "erasing schema op at clean stage after one hour, "
                             "commit_ts: "
                          << op.CommitTs() << ", min ckpt_ts: " << min_ckpt_ts_;
                it = tx_catalog_ops_.erase(it);
            }
            else
            {
                it++;
            }
        }
        for (auto it = tx_split_range_ops_.begin();
             it != tx_split_range_ops_.end();)
        {
            const SplitRangeOp &op = it->second;
            auto stage = op.split_range_op_message_.stage();
            if (stage == SplitRangeOpMessage_Stage::
                             SplitRangeOpMessage_Stage_CleanSplit &&
                min_ckpt_ts_ > op.commit_ts_ + one_hour)
            {
                LOG(INFO) << "erasing range split op at clean stage after one "
                             "hour, commit_ts: "
                          << op.commit_ts_ << ", min ckpt_ts: " << min_ckpt_ts_;
                it = tx_split_range_ops_.erase(it);
            }
            else
            {
                it++;
            }
        }
        for (auto it = tx_cluster_scale_ops_.begin();
             it != tx_cluster_scale_ops_.end();)
        {
            const ClusterScaleOp &op = it->second;
            auto stage = op.cluster_scale_op_message_.stage();
            if (stage == ClusterScaleStage::CleanScale &&
                min_ckpt_ts_ > op.commit_ts_ + one_hour)
            {
                LOG(INFO)
                    << "erasing cluster scale op at clean stage after one "
                       "hour, commit_ts: "
                    << op.commit_ts_ << ", min ckpt_ts: " << min_ckpt_ts_;
                it = tx_cluster_scale_ops_.erase(it);
            }
            else
            {
                it++;
            }
        }
    }

    /**
     * @name CDC DDL history
     * txservice::OperationType values of the schema ops the history records;
     * the log service does not include the tx service's headers.
     * @{
     */
    static constexpr uint32_t kCdcCreateTable = 5;  ///< CreateTable.
    static constexpr uint32_t kCdcDropTable = 6;    ///< DropTable.
    static constexpr uint32_t kCdcAddIndex = 8;     ///< AddIndex.
    static constexpr uint32_t kCdcDropIndex = 9;    ///< DropIndex.
    /** @} */

    /**
     * @brief Which kind of CDC DDL record a schema op's message makes, if
     * any.
     *
     * Only EloqDoc's CREATE and DROP TABLE and its index ops are recorded.
     * Index ops arrive as table ops too, with their index names in
     * alter_table_info_blob. Not recorded: ALTER (OperationType::Update, how
     * collMod reaches the tx service), TRUNCATE, other engines, and the
     * logical alters logged inside DML.
     *
     * @param msg The schema op's message as the prepare log carried it.
     * @return The op type (kCdcCreateTable, kCdcDropTable, kCdcAddIndex or
     *         kCdcDropIndex), or 0 if the op is not recorded.
     */
    static uint32_t CdcDdlOpType(const SchemaOpMessage &msg)
    {
        if (msg.table_engine() != TableEngine::EloqDoc || !msg.has_table_op())
        {
            return 0;
        }
        const uint32_t op_type = msg.table_op().op_type();
        return op_type == kCdcCreateTable || op_type == kCdcDropTable ||
                       op_type == kCdcAddIndex || op_type == kCdcDropIndex
                   ? op_type
                   : 0;
    }

    /**
     * @brief Adds a record to the CDC DDL history. Caller holds the unique
     * lock.
     *
     * The record must be appended above the log group's checkpoint
     * (docs/cdc-change-stream.md in the EloqDoc repo, §4.4): a scan may
     * already have passed any lower timestamp. A replayed log entry appends
     * the same record at the same position again, which leaves the history
     * as it was.
     *
     * @param ddl_ts The record's stream timestamp.
     * @param catalog_ts The op's catalog version, the timestamp its prepare
     *        log carried.
     * @param txn The schema op's transaction number.
     * @param msg The schema op's message.
     */
    void AppendCdcDdl(uint64_t ddl_ts,
                      uint64_t catalog_ts,
                      uint64_t txn,
                      const SchemaOpMessage &msg)
    {
        if (ddl_ts <= min_ckpt_ts_)
        {
            LOG(WARNING) << "CDC DDL record of txn " << txn << " at " << ddl_ts
                         << " is not above the checkpoint " << min_ckpt_ts_
                         << "; a CDC scan may already have passed it";
        }
        CdcDdlRecord record;
        record.set_ddl_ts(ddl_ts);
        record.set_catalog_ts(catalog_ts);
        record.set_txn_number(txn);
        *record.mutable_schema_op() = msg;
        cdc_ddl_history_.try_emplace(CdcDdlPosition(ddl_ts, txn),
                                     std::move(record));
    }

    /**
     * @brief Updates the CDC DDL history for a schema op's commit log.
     * Caller holds the unique lock.
     *
     * CREATE and DROP TABLE take the catalog write lock only before their
     * commit log, so their record goes in here, at the commit log's
     * timestamp; rolled back (at timestamp 0) they make none. An index op's
     * record went in at its prepare log: a rollback at timestamp 0 takes it
     * out again, and one at a timestamp above 0, the add index whose build
     * broke a constraint, is a record of its own there, carrying the
     * rolled-back mark, from which the runtime makes the dropIndexes event.
     *
     * @param txn The schema op's transaction number.
     * @param catalog_ts The op's catalog version, the timestamp its prepare
     *        log carried.
     * @param commit_ts The commit log's timestamp.
     * @param msg The op's message as the log state holds it after the commit
     *        log, with its rolled-back mark.
     */
    void RecordCdcDdlCommit(uint64_t txn,
                            uint64_t catalog_ts,
                            uint64_t commit_ts,
                            const SchemaOpMessage &msg)
    {
        const uint32_t op_type = CdcDdlOpType(msg);
        if (op_type == kCdcCreateTable || op_type == kCdcDropTable)
        {
            if (commit_ts != 0 && !msg.rolled_back())
            {
                AppendCdcDdl(commit_ts, catalog_ts, txn, msg);
            }
        }
        else if (op_type == kCdcAddIndex || op_type == kCdcDropIndex)
        {
            if (commit_ts == 0)
            {
                cdc_ddl_history_.erase(CdcDdlPosition(catalog_ts, txn));
            }
            else if (msg.rolled_back())
            {
                AppendCdcDdl(commit_ts, catalog_ts, txn, msg);
            }
        }
    }

    /**
     * @brief The CDC DDL history (docs/cdc-change-stream.md in the EloqDoc
     * repo, §4.3): one record per recorded schema op, two for a rolled-back
     * add index, keyed by position (CdcDdlPosition), so that key order is
     * (ddl_ts, txn) order.
     *
     * on_apply and the purges write it under the unique lock; scans copy
     * from it under the shared lock. Kept in memory and saved in the raft
     * snapshot by the states that serve CDC scans, and trimmed by the purges
     * of the data log, by their floor (TrimCdcDdlHistory). The memory state
     * purges nothing, and keeps its history too.
     */
    std::map<std::string, CdcDdlRecord> cdc_ddl_history_;
    /**
     * @brief The history's records as of the snapshot being saved, in
     * position order. A plain copy: the snapshot only writes them out, and
     * loading rebuilds each record's position.
     */
    std::vector<CdcDdlRecord> snapshot_cdc_ddl_history_;

    /**
     * @brief The cluster gate slot's value, as UpdateClusterGate keeps it.
     */
    struct ClusterGateSlot
    {
        ClusterGateLogMessage value;  ///< The gate's latest value.
        uint64_t version{0};          ///< Commit timestamp of its entry.
        uint64_t txn{0};              ///< The gate transaction that set it.
    };

    /**
     * @brief The cluster gate slot (EloqDoc docs/cdc-change-stream.md
     * §6.2): set by cluster gate log entries, saved in the raft snapshot by
     * the states that serve CDC scans, replayed to recovering node groups,
     * and never purged. Empty until the first gate entry, and in every log
     * group but group 0.
     */
    std::optional<ClusterGateSlot> cluster_gate_;
    /** @brief The slot as of the snapshot being saved. */
    std::optional<ClusterGateSlot> snapshot_cluster_gate_;

    std::unordered_map<uint32_t, CcNgInfo> cc_ng_info_;
    std::unordered_map<uint32_t, CcNgInfo> snapshot_cc_ng_info_;

    // to erase finished schema ops and split range ops after one hour
    uint64_t min_ckpt_ts_{};

    /**
     * @brief The CDC retention window in microseconds. Set once at start and
     * read by the purge threads, hence atomic.
     */
    std::atomic<uint64_t> cdc_retention_us_{0};

    struct CatalogOp
    {
        CatalogOp(const SchemaOpMessage &schema_op, uint64_t commit_ts)
            : schemas_op_msg_({schema_op}), commit_ts_(commit_ts)
        {
        }

        CatalogOp(SchemaOpMessage &&schema_op, uint64_t commit_ts)
            : schemas_op_msg_({std::move(schema_op)}), commit_ts_(commit_ts)
        {
        }

        CatalogOp(const ::google::protobuf::RepeatedPtrField<SchemaOpMessage>
                      &schemas_op,
                  uint64_t commit_ts)
            : schemas_op_msg_(schemas_op.begin(), schemas_op.end()),
              commit_ts_(commit_ts)
        {
        }

        CatalogOp(std::vector<SchemaOpMessage> schemas_op, uint64_t commit_ts)
            : schemas_op_msg_(std::move(schemas_op)), commit_ts_(commit_ts)
        {
        }

        uint64_t CommitTs() const
        {
            return commit_ts_;
        }

        void SetCommitTs(uint64_t commit_ts)
        {
            commit_ts_ = commit_ts;
        }

        // If contains only one SchemaOpMessage, return its stage.
        // If contains multiple SchemaOpMessage, return clean only when all
        // clean.
        SchemaOpMessage_Stage SchemasOpStage() const
        {
            if (schemas_op_msg_.size() == 1)
            {
                return schemas_op_msg_.front().stage();
            }
            else
            {
                bool all_cleaned =
                    std::all_of(schemas_op_msg_.begin(),
                                schemas_op_msg_.end(),
                                [](const SchemaOpMessage &schema_op_msg) {
                                    return schema_op_msg.stage() ==
                                           SchemaOpMessage_Stage_CleanSchema;
                                });
                if (all_cleaned)
                {
                    return SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_CleanSchema;
                }

                bool all_committed =
                    std::all_of(schemas_op_msg_.begin(),
                                schemas_op_msg_.end(),
                                [](const SchemaOpMessage &schema_op_msg) {
                                    return schema_op_msg.stage() ==
                                           SchemaOpMessage_Stage_CommitSchema;
                                });
                if (all_committed)
                {
                    return SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_CommitSchema;
                }
                else
                {
                    return SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_PrepareSchema;
                }
            }
        }

        void CommitAll()
        {
            for (SchemaOpMessage &schema_op_msg : schemas_op_msg_)
            {
                schema_op_msg.set_stage(SchemaOpMessage_Stage_CommitSchema);
            }
        }

        void ClearAll()
        {
            for (SchemaOpMessage &schema_op_msg : schemas_op_msg_)
            {
                schema_op_msg.Clear();
                schema_op_msg.set_stage(SchemaOpMessage_Stage_CleanSchema);
            }
        }

        void Clear(const std::string &table_name, CcTableType table_type)
        {
            auto it = std::find_if(
                schemas_op_msg_.begin(),
                schemas_op_msg_.end(),
                [&table_name, table_type](const SchemaOpMessage &schema_op_msg)
                {
                    return schema_op_msg.table_name_str() == table_name &&
                           schema_op_msg.table_type() == table_type;
                });
            assert(it != schemas_op_msg_.end());
            SchemaOpMessage &schema_op_msg = *it;
            schema_op_msg.Clear();
            schema_op_msg.set_stage(SchemaOpMessage_Stage_CleanSchema);
        }

        SchemaOpMessage *GetSchemaOpMsg(const std::string &table_name,
                                        CcTableType table_type)
        {
            auto it = std::find_if(
                schemas_op_msg_.begin(),
                schemas_op_msg_.end(),
                [&table_name, table_type](const SchemaOpMessage &schema_op_msg)
                {
                    return schema_op_msg.table_name_str() == table_name &&
                           schema_op_msg.table_type() == table_type;
                });

            if (it != schemas_op_msg_.end())
            {
                return std::addressof(*it);
            }
            else
            {
                return nullptr;
            }
        }

        const SchemaOpMessage *SchemaOpMsgs() const
        {
            return schemas_op_msg_.data();
        }

        SchemaOpMessage *SchemaOpMsgs()
        {
            return schemas_op_msg_.data();
        }

        size_t SchemaOpMsgCount() const
        {
            return schemas_op_msg_.size();
        }

    private:
        // DML-trigger-DDL allows update to multiple catalogs.
        // pure-DDL allows update to only one catalog.
        std::vector<SchemaOpMessage> schemas_op_msg_;
        uint64_t commit_ts_;
    };

    /**
     * @brief A collection ongoing tx's and their ongoing schema operations.
     *
     */
    std::unordered_map<uint64_t, CatalogOp> tx_catalog_ops_;
    std::unordered_map<uint64_t, CatalogOp> snapshot_tx_catalog_ops_;

    /**
     * protects concurrent access to log state, more specifically, CcNode
     * group term and last_ckpt_ts and tx_catalog_ops_. RecoverTx reads them
     * and state machine on_apply modifies them. Since RecoverTx is
     * processed in separate RPC thread, they are concurrent.
     */
    mutable std::shared_mutex log_state_mutex_;

    struct SplitRangeOp
    {
        SplitRangeOp(const SplitRangeOpMessage &split_range_op_message,
                     uint64_t commit_ts)
            : split_range_op_message_(split_range_op_message),
              commit_ts_(commit_ts)
        {
        }

        SplitRangeOp(SplitRangeOpMessage &split_range_op_message,
                     uint64_t commit_ts)
            : split_range_op_message_(split_range_op_message),
              commit_ts_(commit_ts)
        {
        }

        SplitRangeOpMessage split_range_op_message_;
        uint64_t commit_ts_;
    };

    /**
     * @brief A collection ongoint tx's and their ongoing split range
     * operations.
     *
     */
    std::unordered_map<uint64_t, SplitRangeOp> tx_split_range_ops_;
    std::unordered_map<uint64_t, SplitRangeOp> snapshot_tx_split_range_ops_;

    struct ClusterScaleOp
    {
        ClusterScaleOp(const ClusterScaleOpMessage &cluster_scale_op_message,
                       uint64_t commit_ts)
            : cluster_scale_op_message_(cluster_scale_op_message),
              commit_ts_(commit_ts)
        {
        }

        ClusterScaleOp(ClusterScaleOpMessage &cluster_scale_op_message,
                       uint64_t commit_ts)
            : cluster_scale_op_message_(cluster_scale_op_message),
              commit_ts_(commit_ts)
        {
        }

        ClusterScaleOpMessage cluster_scale_op_message_;
        uint64_t commit_ts_;
    };

    uint64_t newest_cluster_scale_txn_{(uint64_t) UINT32_MAX << 32L};
    std::unordered_map<uint64_t, ClusterScaleOp> tx_cluster_scale_ops_;
    std::unordered_map<uint64_t, ClusterScaleOp> snapshot_tx_cluster_scale_ops_;
    // Empty log. Just used by RecoverTx.
    std::unordered_set<uint64_t> tx_data_migration_ops_;
};
}  // namespace txlog
