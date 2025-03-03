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

#include <glog/logging.h>

#include <vector>

#include "cc_req_pool.h"
#include "read_write_entry.h"
#include "rpc_closure.h"
#include "tx_request.h"
#include "type.h"

namespace txservice
{
class TransactionExecution;

class UploadIndexContext
{
public:
    using TableIndexSet =
        std::unordered_map<TableName, std::vector<WriteEntry>>;
    using NGIndexSet =
        std::unordered_map<NodeGroupId, std::vector<WriteEntry *>>;

private:
    enum struct UploadTaskStatus
    {
        Free = 0,
        Pending,
        Ongoing
    };

    struct UploadIndexTask
    {
        // The original WriteEntry set for each index table.
        TableIndexSet table_index_set_;
        UploadTaskStatus task_status_{UploadTaskStatus::Free};
    };

#ifdef NDEBUG
    static constexpr uint16_t UploadTimeout = 10000;  // ms
    static constexpr size_t UploadIndexWorkerSize = 5;
#else
    static constexpr uint16_t UploadTimeout = 1000;  // ms
    static constexpr size_t UploadIndexWorkerSize = 2;
#endif

public:
    UploadIndexContext() = default;

    UploadIndexContext(const UploadIndexContext &rhs) = delete;
    UploadIndexContext(UploadIndexContext &&rhs) = delete;

    void Reset(NodeGroupId ng_id);
    void InitUploadWorkers();
    void TerminateWorkers();
    void WaitUntilUploadFinished();
    void EnqueueNewIndexes(TableIndexSet &&new_indexes);
    void RecycleUploadTask(UploadIndexTask &task, CcErrorCode task_res);
    void UploadIndexWorker();

    bool UploadSucceed()
    {
        std::unique_lock<std::mutex> lk(mux_);
        return upload_result_ == CcErrorCode::NO_ERROR;
    }

    CcErrorCode UploadResult()
    {
        std::unique_lock<std::mutex> lk(mux_);
        return upload_result_;
    }

    const std::vector<int64_t> &NodeGroupTerms() const
    {
        return leader_terms_;
    }

private:
    CcErrorCode UploadEncodedIndex(UploadIndexTask &upload_task);
    CcErrorCode UploadIndexInternal(
        std::unordered_map<TableName, NGIndexSet> &ng_index_set);
    void SendIndexes(const TableName &table_name,
                     NodeGroupId dest_ng_id,
                     int64_t &ng_term,
                     const std::vector<WriteEntry *> &write_entry_vec,
                     size_t batch_size,
                     size_t start_key_idx,
                     bthread::Mutex &req_mux,
                     bthread::ConditionVariable &req_cv,
                     size_t &finished_req_cnt,
                     CcErrorCode &res_code);
    // Acquire and release range read lock.
    CcErrorCode AcquireRangeReadLocks(
        TransactionExecution *acq_lock_txm,
        UploadIndexTask &upload_task,
        std::unordered_map<TableName, NGIndexSet> &ng_index_set);
    void ReleaseRangeReadLocks(TransactionExecution *acq_lock_txm,
                               bool is_success);
    void AdvanceWriteEntryForRangeInfo(
        const RangeRecord &range_record,
        std::vector<WriteEntry>::iterator &cur_write_entry_it,
        const std::vector<WriteEntry>::iterator &write_entry_end,
        NGIndexSet &ng_write_entrys);
    UploadBatchCc *NextRequest();

    NodeGroupId node_group_id_{0};
    std::vector<std::thread> worker_thds_;
    std::array<UploadIndexTask, UploadIndexWorkerSize> task_pool_;
    uint8_t free_head_{0};
    uint8_t pending_head_{0};
    size_t pending_task_cnt_{0};
    size_t ongoing_task_cnt_{0};
    std::mutex mux_;
    std::condition_variable producer_cv_;
    std::condition_variable consumer_cv_;
    WorkerStatus status_{WorkerStatus::Active};
    uint32_t upload_batch_size_{128};
    CcErrorCode upload_result_{CcErrorCode::NO_ERROR};
    // Store the node group leader terms after acquired them.
    std::vector<int64_t> leader_terms_;
    // For each node group, and each index table
    CcRequestPool<UploadBatchCc> upload_req_pool_;
};

class SkGenerator
{
public:
    SkGenerator() = default;

    void Reset(const TxKey *start_key,
               const TxKey *end_key,
               uint64_t scan_ts,
               const TableName &base_table_name,
               NodeGroupId node_group_id,
               int32_t partition_id,
               uint64_t tx_number,
               int64_t tx_term,
               std::vector<TableName> &new_indexes_name);

    void Reset(const std::string &start_key_str,
               const std::string &end_key_str,
               uint64_t scan_ts,
               const TableName &base_table_name,
               NodeGroupId node_group_id,
               int32_t partition_id,
               uint64_t tx_number,
               int64_t tx_term,
               std::vector<TableName> &new_indexes_name);

    void ProcessTask();

    const std::vector<int64_t> &NodeGroupTerms() const
    {
        return upload_index_ctx_.NodeGroupTerms();
    }

    CcErrorCode TaskResult() const
    {
        return task_result_;
    }

    const PackSkError &GetPackSkError() const
    {
        return pack_sk_err_;
    }

    PackSkError &GetPackSkError()
    {
        return pack_sk_err_;
    }

    size_t ScannedItemsCount() const
    {
        return scanned_items_count_;
    }

private:
    /**
     * @brief Scan pk items, and generate sk items.
     *
     */
    void ScanAndEncodeIndex(const TxKey *start_key,
                            const TxKey *end_key,
                            int64_t ng_term,
                            uint64_t tx_number);

    const TableName *base_table_name_;
    NodeGroupId node_group_id_{0};
    uint64_t tx_number_{0};
    int64_t tx_term_{INIT_TERM};
    GenerateSkStatus *task_status_{nullptr};
    int32_t partition_id_{0};
    uint64_t scan_ts_{0};
    union
    {
        const TxKey *start_key_;
        const std::string *start_key_str_;
    };
    union
    {
        const TxKey *end_key_;
        const std::string *end_key_str_;
    };
    bool is_key_str_{false};
    const std::vector<TableName> *new_indexes_name_{nullptr};
    UploadIndexContext upload_index_ctx_;
    std::vector<SkEncoder::uptr> sk_encoder_vec_;
    size_t scan_batch_size_{LocalCcShards::DATA_SYNC_SCAN_BATCH_SIZE};

    CcErrorCode task_result_{CcErrorCode::NO_ERROR};

    // Detail error information for CcErrorCode::PACK_SK_ERR.
    PackSkError pack_sk_err_;

    size_t scanned_items_count_{0};
    std::shared_ptr<TableSchema> table_schema_{nullptr};
};

}  // namespace txservice
