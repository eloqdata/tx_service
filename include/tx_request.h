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
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "range_slice.h"
#include "scan.h"
#include "tx_command.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_req_result.h"
#include "type.h"

namespace txservice
{
struct DataMigrationStatus;
struct TxRequest
{
public:
    using Uptr = std::unique_ptr<TxRequest>;

    virtual ~TxRequest() = default;
    virtual void Process(TransactionExecution *txm) = 0;
    // virtual bool Finish() const = 0;

    virtual void SetError(
        TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR) = 0;
};

template <typename Subtype, typename T>
struct TemplateTxRequest : TxRequest
{
    TemplateTxRequest(const std::function<void()> *yield_fptr,
                      const std::function<void()> *resume_fptr,
                      TransactionExecution *txm = nullptr)
        : tx_result_(yield_fptr, resume_fptr), txm_(txm)
    {
    }

    virtual ~TemplateTxRequest() = default;

    void Process(TransactionExecution *txm) override
    {
        txm->ProcessTxRequest(static_cast<Subtype &>(*this));
    }

    bool IsFinished()
    {
        return tx_result_.Status() != TxResultStatus::Unknown;
    }

    bool IsError() const
    {
        return tx_result_.IsError();
    }

    TxErrorCode ErrorCode() const
    {
        return tx_result_.ErrorCode();
    }

    const std::string &ErrorMsg() const
    {
        return TxErrorMessage(ErrorCode());
    }

#if defined EXT_TX_PROC_ENABLED
    void ForceExternalForwardOnce(TransactionExecution *txm)
    {
        // Allow the txm to be forwarded both externally and by
        // TxProcessor leads to complexity. Just let the bthread to
        // forward the txm itself and don't enlist the txm even if it
        // fails.

        bool allow_enlist_txm = false;
        while (tx_result_.Status() == TxResultStatus::Unknown &&
               !txm->ExternalForward(allow_enlist_txm))
        {
            (*tx_result_.GetResume())();
            (*tx_result_.GetYield())();
        }
    }
#endif

    void Wait()
    {
#ifdef EXT_TX_PROC_ENABLED
        if (cc_notify_)
        {
            assert(txm_ != nullptr);
            ForceExternalForwardOnce(txm_);
            // After first forward, the ccrequest must have been sent and could
            // have already finished, no matter, always wait once so that we
            // don't need lock and condition variable (which is costly compared
            // to bthread block and resume).
            (*tx_result_.GetYield())();

            // No need for lock when accessing tx_result_.status_ since the txm
            // can only be externally forwarded and the reader and writer are
            // the same thread.
            if (tx_result_.Status() == TxResultStatus::Unknown)
            {
                // Forward the txm again, after the second forward, the tx
                // result must be finished.
                ForceExternalForwardOnce(txm_);
            }
            assert(tx_result_.Status() != TxResultStatus::Unknown);
            return;
        }
#endif
        TxResultStatus result_status = TxResultStatus::Unknown;
        do
        {
#ifdef EXT_TX_PROC_ENABLED
            if (txm_ != nullptr)
            {
                txm_->ExternalForward();
                tx_result_.Wait();
            }
#endif
            result_status = tx_result_.Status();
        } while (result_status == TxResultStatus::Unknown);
    }

    const T &Result() const
    {
        return tx_result_.Value();
    }

    void Reset()
    {
        tx_result_.Reset();
    }

    void SetError(TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR) override
    {
        tx_result_.FinishError(err_code);
    }

    bool TrySetTxm(TransactionExecution *txm)
    {
        if (!txm_)
        {
            txm_ = txm;
            return true;
        }
        return false;
    }

    TxResult<T> tx_result_;

protected:
    TransactionExecution *txm_{nullptr};
    // If true, cc request will wake up runtime coroutine directly. Otherwise,
    // cc request will only notify txm, and runtime coroutine will be resumed by
    // txm.
    bool cc_notify_{false};
    friend class TransactionExecution;
};

struct InitTxRequest : public TemplateTxRequest<InitTxRequest, size_t>
{
    InitTxRequest(IsolationLevel level = IsolationLevel::ReadCommitted,
                  CcProtocol proto = CcProtocol::OCC,
                  const std::function<void()> *yield_fptr = nullptr,
                  const std::function<void()> *resume_fptr = nullptr,
                  TransactionExecution *txm = nullptr,
                  uint32_t tx_ng_id = UINT32_MAX,
                  uint32_t log_group_id = UINT32_MAX)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          iso_level_(level),
          protocol_(proto),
          tx_ng_id_(tx_ng_id),
          log_group_id_(log_group_id)
    {
    }

    ~InitTxRequest() = default;

    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};
    uint32_t tx_ng_id_{UINT32_MAX};
    uint32_t log_group_id_{UINT32_MAX};
};

struct ReadTxRequest
    : public TemplateTxRequest<ReadTxRequest, std::pair<RecordStatus, uint64_t>>
{
public:
    ReadTxRequest(const TableName *tab_name = nullptr,
                  uint64_t schema_version = 0,
                  const TxKey *key = nullptr,
                  TxRecord *rec = nullptr,
                  bool is_for_write = false,
                  bool is_for_share = false,
                  bool read_local = false,
                  uint64_t ts = 0,
                  bool is_covering_keys = false,
                  bool is_recovering = false,
                  bool point_read_on_cache_miss = false,
                  const std::function<void()> *yield_fptr = nullptr,
                  const std::function<void()> *resume_fptr = nullptr,
                  TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          tab_name_(tab_name),
          key_(key),
          is_str_key_(false),
          rec_(rec),
          is_for_write_(is_for_write),
          is_for_share_(is_for_share),
          read_local_(read_local),
          ts_(ts),
          schema_version_(schema_version),
          is_covering_keys_(is_covering_keys),
          is_recovering_(is_recovering),
          point_read_on_cache_miss_(point_read_on_cache_miss)
    {
    }

    void Set(const TableName *tab_name,
             uint64_t schema_version,
             const TxKey *key,
             TxRecord *rec,
             bool is_for_write = false,
             bool is_for_share = false,
             bool read_local = false,
             uint64_t ts = 0,
             bool is_covering_keys = false,
             bool is_recovering = false,
             bool point_read_on_cache_miss = false)
    {
        tab_name_ = tab_name;
        key_ = key;
        is_str_key_ = false;
        rec_ = rec;
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        read_local_ = read_local;
        ts_ = ts;
        schema_version_ = schema_version;
        is_covering_keys_ = is_covering_keys;
        is_recovering_ = is_recovering;
        point_read_on_cache_miss_ = point_read_on_cache_miss;
    }

    void Set(const TableName *tab_name,
             uint64_t schema_version,
             const std::string *key_str,
             TxRecord *rec,
             bool is_for_write = false,
             bool is_for_share = false,
             bool read_local = false,
             uint64_t ts = 0,
             bool is_covering_keys = false,
             bool is_recovering = false,
             bool point_read_on_cache_miss = false)
    {
        tab_name_ = tab_name;
        key_str_ = key_str;
        is_str_key_ = true;
        rec_ = rec;
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        read_local_ = read_local;
        ts_ = ts;
        schema_version_ = schema_version;
        is_covering_keys_ = is_covering_keys;
        is_recovering_ = is_recovering;
        point_read_on_cache_miss_ = point_read_on_cache_miss;
    }

    const TableName *tab_name_;
    union
    {
        const TxKey *key_;
        const std::string *key_str_;
    };
    bool is_str_key_;
    TxRecord *rec_;
    bool is_for_write_;  // used for "select ... for update".
    bool is_for_share_;  // used for "select ... lock in share mode".
    bool read_local_;

    /*

    Here, the timestamp serves two roles:

    1. When mvcc is enabled, this ts represents the start timestamp of the
    transaction.

    2. When mvcc is disabled, this ts represents the secondary key commit
    timestamp when performing a PkRead preceded by a sk read/scan. This ts is
    required in PkRead() to check whether the pk row is valid to read.

    The pk row is invalid to read if it is being modified concurrently. For
    example, txn#1 is updating a row (1,a,1) into (1,b,1) in table t1(i INT, j
    CHAR, k INT, PRIMARY KEY(i), UNIQUE(j)), while txn#2 wants to read a row
    where j=b. If ,in txn#1, (b,1) has been inserted into sk table(t1*~~j) while
    (1,b,1) has not been inserted into base table(t1), txn#2 will see (b,1) and
    use i=1 to do a pk read, but then find (1,a,1) instead, which is incorrect.

    */
    uint64_t ts_;
    uint64_t schema_version_;

    // For unique_sk point query
    bool is_covering_keys_;

    // If this is a read request for recovering. If true we should
    // rely on candidate leader term instead of current term to decide
    // if node is valid leader.
    bool is_recovering_;

    bool point_read_on_cache_miss_;
};

struct ReadOutsideTxRequest
    : public TemplateTxRequest<ReadOutsideTxRequest, RecordStatus>
{
public:
    ReadOutsideTxRequest(TxRecord &rec,
                         bool is_deleted,
                         uint64_t commit_ts,
                         std::vector<VersionTxRecord> *archives,
                         const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          rec_(rec),
          is_deleted_(is_deleted),
          commit_ts_(commit_ts),
          archives_(archives)
    {
    }

    TxRecord &rec_;
    bool is_deleted_;
    uint64_t commit_ts_;
    std::vector<VersionTxRecord> *archives_;
};

struct UpsertTxRequest : public TemplateTxRequest<UpsertTxRequest, Void>
{
    UpsertTxRequest(const TableName *tab_name,
                    TxKey tx_key,
                    TxRecord::Uptr rec,
                    OperationType operation_type,
                    const std::function<void()> *yield_fptr = nullptr,
                    const std::function<void()> *resume_fptr = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr),
          tab_name_(tab_name),
          tx_key_(std::move(tx_key)),
          rec_(std::move(rec)),
          operation_type_(operation_type)
    {
    }

    const TableName *tab_name_;
    TxKey tx_key_;
    TxRecord::Uptr rec_;
    OperationType operation_type_;
};

struct BucketScanSavePoint
{
    BucketScanPlan PickPlan(size_t current_idx)
    {
        if (prev_pause_idx_ == UINT64_MAX || prev_pause_idx_ != current_idx)
        {
            // clear prev pause position
            // pause_position_.clear();
            BucketScanPlan plan(
                current_idx, &bucket_groups_[current_idx], nullptr);
            return plan;
        }
        else
        {
            BucketScanPlan plan(
                current_idx, &bucket_groups_[current_idx], &pause_position_);
            return plan;
        }
    }

    size_t PlanSize()
    {
        return bucket_groups_.size();
    }

    bool IsValidCursor(uint64_t version)
    {
        // the eloqkv client using the cursor id to resume the
        // scan. In this scenario, the cluster config may have changed.
        // This is a very rare case, we'll treat it here as an iterator
        // invalidation. eloqkv will return RD_ERR_INVALID_CURSOR to
        // client.
        if (cluster_config_version_ == UINT64_MAX)
        {
            cluster_config_version_ = version;
        }

        if (version != cluster_config_version_)
        {
            return false;
        }

        return true;
    }

    void Debug()
    {
        size_t cnt = 0;
        for (auto &group : bucket_groups_)
        {
            for (auto &[node_group_id, buckets] : group)
            {
                cnt += buckets.size();
            }
        }

        LOG(INFO) << "==yf: cluster config version = "
                  << cluster_config_version_
                  << ", prev pause index = " << prev_pause_idx_
                  << ", group size = " << bucket_groups_.size()
                  << ", position size = " << pause_position_.size()
                  << ", cnt = " << cnt;
    }

    uint64_t cluster_config_version_{UINT64_MAX};
    size_t prev_pause_idx_{UINT64_MAX};
    std::vector<absl::flat_hash_map<NodeGroupId, std::vector<uint16_t>>>
        bucket_groups_;
    absl::flat_hash_map<NodeGroupId,
                        absl::flat_hash_map<uint16_t, BucketScanProgress>>
        pause_position_;
};

struct ScanOpenTxRequest : public TemplateTxRequest<ScanOpenTxRequest, size_t>
{
    ScanOpenTxRequest() : TemplateTxRequest(nullptr, nullptr)
    {
    }

    ScanOpenTxRequest(const TableName *tabname,
                      uint64_t schema_version,
                      ScanIndexType index_type,
                      const TxKey *start_key,
                      bool start_inclusive = true,
                      const TxKey *end_key = nullptr,
                      bool end_inclusive = true,
                      ScanDirection direction = ScanDirection::Forward,
                      bool is_ckpt = false,
                      bool is_for_write = false,
                      bool is_for_share = false,
                      bool is_covering_keys = false,
                      bool is_require_keys = true,
                      bool is_require_recs = true,
                      bool is_require_sort = true,
                      bool is_read_local = false,
                      const std::function<void()> *yield_fptr = nullptr,
                      const std::function<void()> *resume_fptr = nullptr,
                      TransactionExecution *txm = nullptr,
                      int32_t obj_type = -1,
                      std::string_view scan_pattern = {},
                      BucketScanSavePoint *save_point = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          tab_name_(tabname),
          indx_type_(index_type),
          start_key_(start_key),
          start_inclusive_(start_inclusive),
          end_key_(end_key),
          end_inclusive_(end_inclusive),
          direct_(direction),
          is_ckpt_delta_(is_ckpt),
          is_for_write_(is_for_write),
          is_for_share_(is_for_share),
          is_covering_keys_(is_covering_keys),
          is_require_keys_(is_require_keys),
          is_require_recs_(is_require_recs),
          is_require_sort_(is_require_sort),
          read_local_(is_read_local),
          scan_alias_(UINT64_MAX),
          schema_version_(schema_version),
          obj_type_(obj_type),
          scan_pattern_(scan_pattern),
          bucket_scan_save_point_(save_point)
    {
    }

    void Reset(const TableName *tabname,
               uint64_t schema_version,
               ScanIndexType index_type,
               const TxKey *start_key,
               bool start_inclusive = true,
               const TxKey *end_key = nullptr,
               bool end_inclusive = true,
               ScanDirection direction = ScanDirection::Forward,
               bool is_ckpt = false,
               bool is_for_write = false,
               bool is_for_share = false,
               bool is_covering_keys = false,
               bool is_require_keys = true,
               bool is_require_recs = true,
               bool is_require_sort = true,
               bool is_read_local = false,
               const std::function<void()> *yield_fptr = nullptr,
               const std::function<void()> *resume_fptr = nullptr,
               TransactionExecution *txm = nullptr,
               int32_t obj_type = -1,
               std::string_view scan_pattern = {},
               BucketScanSavePoint *save_point = nullptr)
    {
        tx_result_.Reset(yield_fptr, resume_fptr);
        txm_ = txm;
        tab_name_ = tabname;
        indx_type_ = index_type;
        start_key_ = start_key;
        start_inclusive_ = start_inclusive;
        end_key_ = end_key;
        end_inclusive_ = end_inclusive;
        direct_ = direction;
        is_ckpt_delta_ = is_ckpt;
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        is_covering_keys_ = is_covering_keys;
        is_require_keys_ = is_require_keys;
        is_require_recs_ = is_require_recs;
        is_require_sort_ = is_require_sort;
        read_local_ = is_read_local;
        scan_alias_ = UINT64_MAX;
        schema_version_ = schema_version;
        obj_type_ = obj_type;
        scan_pattern_ = scan_pattern;
        bucket_scan_save_point_ = save_point;
    }

    const TxKey *StartKey() const
    {
        return start_key_;
    }

    const TxKey *EndKey() const
    {
        return end_key_;
    }

    const TableName *tab_name_{nullptr};
    ScanIndexType indx_type_{ScanIndexType::Primary};
    const TxKey *start_key_{nullptr};
    bool start_inclusive_{false};
    const TxKey *end_key_{nullptr};
    bool end_inclusive_{false};
    ScanDirection direct_{ScanDirection::Forward};
    bool is_ckpt_delta_{false};
    bool is_for_write_{false};
    bool is_for_share_{false};
    bool is_covering_keys_{true};
    bool is_require_keys_{true};
    bool is_require_recs_{true};
    bool is_require_sort_{true};
    bool read_local_{false};
    uint64_t scan_alias_{UINT64_MAX};
    uint64_t schema_version_{0};

    int32_t obj_type_{-1};
    std::string_view scan_pattern_;

    BucketScanSavePoint *bucket_scan_save_point_{nullptr};
};

struct ScanBatchTuple
{
    ScanBatchTuple() = default;

    ScanBatchTuple(TxKey key, TxRecord *rec)
        : key_(std::move(key)), record_(rec)
    {
    }
    ScanBatchTuple(TxKey key,
                   TxRecord *rec,
                   RecordStatus status,
                   uint64_t version)
        : key_(std::move(key)),
          record_(rec),
          status_(status),
          version_ts_(version)
    {
    }

    ScanBatchTuple(TxKey key,
                   TxRecord *rec,
                   RecordStatus status,
                   uint64_t version,
                   const CcEntryAddr &cce_addr)
        : key_(std::move(key)),
          record_(rec),
          status_(status),
          version_ts_(version),
          cce_addr_(cce_addr)
    {
    }

    ScanBatchTuple(const ScanBatchTuple &rhs) = delete;

    ScanBatchTuple(ScanBatchTuple &&rhs)
        : key_(std::move(rhs.key_)),
          record_(rhs.record_),
          status_(rhs.status_),
          version_ts_(rhs.version_ts_),
          cce_addr_(rhs.cce_addr_)
    {
    }

    TxKey key_;
    TxRecord *record_{nullptr};
    RecordStatus status_{RecordStatus::Unknown};
    uint64_t version_ts_{0};
    CcEntryAddr cce_addr_;
};

struct ScanBatchTxRequest : public TemplateTxRequest<ScanBatchTxRequest, bool>
{
    ScanBatchTxRequest() = delete;

    ScanBatchTxRequest(uint64_t alias,
                       const TableName &table_name,
                       std::vector<ScanBatchTuple> *batch_vec,
                       const std::function<void()> *yield_fptr = nullptr,
                       const std::function<void()> *resume_fptr = nullptr,
                       TransactionExecution *txm = nullptr,
                       int32_t obj_type = -1,
                       std::string_view scan_pattern = {},
                       BucketScanPlan *bucket_scan_plan = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          alias_(alias),
          table_name_(table_name),
          batch_(batch_vec),
          obj_type_(obj_type),
          scan_pattern_(scan_pattern),
          bucket_scan_plan_(bucket_scan_plan)
    {
        batch_->clear();
    }

    uint64_t alias_;
    const TableName &table_name_;
    std::vector<ScanBatchTuple> *batch_;

    // Used for range partition scanner.
    uint32_t prefetch_slice_cnt_{0};

    int32_t obj_type_{-1};
    std::string_view scan_pattern_;

    BucketScanPlan *bucket_scan_plan_{nullptr};
};

struct UnlockTuple
{
    UnlockTuple(const CcEntryAddr &cce_addr,
                uint64_t version_ts,
                RecordStatus status)
        : cce_addr_(cce_addr), version_ts_(version_ts), status_(status)
    {
    }

    CcEntryAddr cce_addr_;
    uint64_t version_ts_;
    RecordStatus status_;
};

struct ScanCloseTxRequest : public TemplateTxRequest<ScanCloseTxRequest, Void>
{
    ScanCloseTxRequest() = delete;

    ScanCloseTxRequest(uint64_t alias,
                       const TableName &table_name,
                       const std::function<void()> *yield_fptr = nullptr,
                       const std::function<void()> *resume_fptr = nullptr,
                       TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          alias_(alias),
          table_name_(table_name.StringView().data(),
                      table_name.StringView().size(),
                      table_name.Type(),
                      table_name.Engine()),
          in_use_(true)
    {
    }

    ScanCloseTxRequest(const std::vector<ScanBatchTuple> &scan_batch,
                       size_t scan_batch_idx,
                       uint64_t alias,
                       const TableName &table_name,
                       const std::function<void()> *yield_fptr = nullptr,
                       const std::function<void()> *resume_fptr = nullptr,
                       TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          alias_(alias),
          table_name_(table_name.StringView().data(),
                      table_name.StringView().size(),
                      table_name.Type(),
                      table_name.Engine()),
          in_use_(true)
    {
        for (size_t idx = scan_batch_idx; idx < scan_batch.size(); ++idx)
        {
            const ScanBatchTuple &tuple = scan_batch[idx];
            unlock_batch_.emplace_back(
                tuple.cce_addr_, tuple.version_ts_, tuple.status_);
        }
    }

    void Reset(uint64_t alias, const TableName &table_name)
    {
        assert(!in_use_.load(std::memory_order_relaxed));

        tx_result_.Reset();
        alias_ = alias;

        table_name_ = TableName(table_name.StringView().data(),
                                table_name.StringView().size(),
                                table_name.Type(),
                                table_name.Engine());
        in_use_.store(true, std::memory_order_relaxed);
    }

    std::vector<UnlockTuple> unlock_batch_;
    uint64_t alias_{UINT64_MAX};
    // TableName owner
    TableName table_name_;
    std::atomic<bool> in_use_{false};
};

struct AbortTxRequest : public TemplateTxRequest<AbortTxRequest, bool>
{
    AbortTxRequest(const std::function<void()> *yield_fptr = nullptr,
                   const std::function<void()> *resume_fptr = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr)
    {
    }
};

struct CommitTxRequest : public TemplateTxRequest<CommitTxRequest, bool>
{
    CommitTxRequest(bool to_commit = true,
                    const std::function<void()> *yield_fptr = nullptr,
                    const std::function<void()> *resume_fptr = nullptr,
                    TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm), to_commit_(to_commit)
    {
    }

    bool to_commit_{true};
};

struct UpsertTableTxRequest
    : public TemplateTxRequest<UpsertTableTxRequest, UpsertResult>
{
    UpsertTableTxRequest(const TableName *table_name,
                         const std::string *curr_image,
                         uint64_t schema_ts,
                         const std::string *dirty_image,
                         txservice::OperationType op_type,
                         const std::string *alter_table_info_image = nullptr,
                         const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          table_name_(table_name),
          curr_image_(curr_image),
          curr_schema_ts_(schema_ts),
          dirty_image_(dirty_image),
          op_type_(op_type),
          alter_table_info_image_(alter_table_info_image)
    {
        if (op_type == OperationType::AddIndex)
        {
            pack_sk_err_ = std::make_unique<PackSkError>();
        }
    }

    const TableName *table_name_;
    const std::string *curr_image_;
    uint64_t curr_schema_ts_;
    const std::string *dirty_image_;
    txservice::OperationType op_type_;
    const std::string *alter_table_info_image_;

    // Available when create index raise pack sk error. Alloctes a PackSkError
    // object to store error message raised by SkGenerator.
    std::unique_ptr<PackSkError> pack_sk_err_;
};

struct SplitFlushTxRequest : public TemplateTxRequest<SplitFlushTxRequest, bool>
{
    SplitFlushTxRequest(const TableName &table_name,
                        std::shared_ptr<const TableSchema> &&schema,
                        TableRangeEntry *range_entry,
                        std::vector<std::pair<TxKey, int32_t>> &&new_range_info,
                        bool is_dirty)
        : TemplateTxRequest(nullptr, nullptr, nullptr),
          table_name_(&table_name),
          schema_(std::move(schema)),
          range_entry_(range_entry),
          new_range_info_(std::move(new_range_info)),
          is_dirty_(is_dirty)
    {
    }
    const TableName *table_name_{nullptr};
    std::shared_ptr<const TableSchema> schema_{nullptr};
    TableRangeEntry *range_entry_{nullptr};
    std::vector<std::pair<TxKey, int32_t>> new_range_info_;
    bool is_dirty_{false};
};

struct DataMigrationTxRequest
    : public TemplateTxRequest<DataMigrationTxRequest, Void>
{
    DataMigrationTxRequest(std::shared_ptr<DataMigrationStatus> status)
        : TemplateTxRequest(nullptr, nullptr, nullptr), status_(status)
    {
    }

    std::shared_ptr<DataMigrationStatus> status_;
};

struct AnalyzeTableTxRequest
    : public TemplateTxRequest<AnalyzeTableTxRequest, Void>
{
    AnalyzeTableTxRequest(const TableName *table_name = nullptr,
                          const std::function<void()> *yield_fptr = nullptr,
                          const std::function<void()> *resume_fptr = nullptr,
                          TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          table_name_(table_name)
    {
    }

    const TableName *table_name_{nullptr};
};

struct BroadcastStatisticsTxRequest
    : public TemplateTxRequest<BroadcastStatisticsTxRequest, Void>
{
    BroadcastStatisticsTxRequest(
        const TableName *table_name = nullptr,
        uint64_t schema_ts = 0,
        const remote::NodeGroupSamplePool *sample_pool = nullptr,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr,
        TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          table_name_(table_name),
          schema_ts_(schema_ts),
          sample_pool_(sample_pool)
    {
    }

    const TableName *table_name_{nullptr};
    uint64_t schema_ts_{0};
    const remote::NodeGroupSamplePool *sample_pool_{nullptr};
};

struct ObjectCommandTxRequest
    : public TemplateTxRequest<ObjectCommandTxRequest, RecordStatus>
{
    ObjectCommandTxRequest() : TemplateTxRequest(nullptr, nullptr, nullptr)
    {
    }

    template <typename KeyT>
    ObjectCommandTxRequest(const TableName *table_name,
                           const KeyT *key,
                           TxCommand *command,
                           bool auto_commit = true,
                           bool always_redirect = false,
                           TransactionExecution *txm = nullptr,
                           const std::function<void()> *yield_fptr = nullptr,
                           const std::function<void()> *resume_fptr = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          table_name_(table_name),
          key_(key),
          command_(command),
          auto_commit_(auto_commit),
          always_redirect_(always_redirect),
          is_cmd_owner_(false)
    {
        this->cc_notify_ = yield_fptr != nullptr;
    }

    template <typename KeyT>
    ObjectCommandTxRequest(const TableName *table_name,
                           const KeyT *key,
                           std::unique_ptr<TxCommand> command,
                           bool auto_commit = true,
                           bool always_redirect = false,
                           TransactionExecution *txm = nullptr)
        : TemplateTxRequest(nullptr, nullptr, txm),
          table_name_(table_name),
          key_(key),
          command_uptr_(std::move(command)),
          auto_commit_(auto_commit),
          always_redirect_(always_redirect),
          is_cmd_owner_(true)
    {
    }

    template <typename KeyT>
    ObjectCommandTxRequest(const TableName *table_name,
                           std::unique_ptr<KeyT> key,
                           std::unique_ptr<TxCommand> command,
                           bool auto_commit = true,
                           bool always_redirect = false,
                           TransactionExecution *txm = nullptr)
        : TemplateTxRequest(nullptr, nullptr, txm),
          table_name_(table_name),
          key_(std::move(key)),
          command_uptr_(std::move(command)),
          auto_commit_(auto_commit),
          always_redirect_(always_redirect),
          is_cmd_owner_(true)
    {
    }

    ObjectCommandTxRequest(ObjectCommandTxRequest &&rhs)
        : TemplateTxRequest(nullptr, nullptr, rhs.txm_),
          table_name_(rhs.table_name_),
          key_(std::move(rhs.key_)),
          auto_commit_(rhs.auto_commit_),
          always_redirect_(rhs.always_redirect_),
          is_cmd_owner_(rhs.is_cmd_owner_)
    {
        if (rhs.is_cmd_owner_)
        {
            command_uptr_ = std::move(rhs.command_uptr_);
        }
        else
        {
            command_ = rhs.command_;
        }
    }

    ~ObjectCommandTxRequest()
    {
        if (is_cmd_owner_)
        {
            command_uptr_.reset();
        }
    }

    const TxKey *Key() const
    {
        return &key_;
    }

    TxCommand *Command() const
    {
        return is_cmd_owner_ ? command_uptr_.get() : command_;
    }

    const TableName *table_name_;
    TxKey key_;
    union
    {
        TxCommand *command_{};
        std::unique_ptr<TxCommand> command_uptr_;
    };

    bool auto_commit_{};
    // Whether to always redirect the command if data is not on local node
    // This is true for non-simple command to avoid cross slot error
    bool always_redirect_{};
    // whether this object is pointer owner
    bool is_cmd_owner_{};

    bool operator<(const ObjectCommandTxRequest &r) const
    {
        return *this->Key() < *r.Key();
    }

    bool operator<(const MultiObjectCommandTxRequest &r) const;
};

struct MultiObjectCommandTxRequest
    : public TemplateTxRequest<MultiObjectCommandTxRequest,
                               std::vector<RecordStatus>>
{
    MultiObjectCommandTxRequest() : TemplateTxRequest(nullptr, nullptr, nullptr)
    {
    }

    MultiObjectCommandTxRequest(const TableName *table_name,
                                MultiObjectTxCommand *cmd,
                                bool auto_commit = true,
                                bool always_redirect = false,
                                TransactionExecution *txm = nullptr,
                                bool is_watch_keys = false)
        : TemplateTxRequest(nullptr, nullptr, txm),
          table_name_(table_name),
          auto_commit_(auto_commit),
          always_redirect_(always_redirect || cmd->KeyPointers()->size() > 1 ||
                           cmd->CmdSteps() > 1),
          multi_obj_cmd_(cmd),
          is_cmd_owner_(false),
          is_watch_keys_(is_watch_keys)
    {
    }

    MultiObjectCommandTxRequest(const TableName *table_name,
                                std::unique_ptr<MultiObjectTxCommand> cmd_uptr,
                                bool auto_commit = true,
                                bool always_redirect = false,
                                TransactionExecution *txm = nullptr,
                                bool is_watch_keys = false)
        : TemplateTxRequest(nullptr, nullptr, txm),
          table_name_(table_name),
          auto_commit_(auto_commit),
          always_redirect_(always_redirect ||
                           cmd_uptr->KeyPointers()->size() > 1 ||
                           cmd_uptr->CmdSteps() > 1),
          multi_obj_cmd_uptr_(std::move(cmd_uptr)),
          is_cmd_owner_(true),
          is_watch_keys_(is_watch_keys)
    {
    }

    MultiObjectCommandTxRequest(const MultiObjectCommandTxRequest &rhs) =
        delete;

    MultiObjectCommandTxRequest(MultiObjectCommandTxRequest &&rhs)
        : TemplateTxRequest(nullptr, nullptr, rhs.txm_),
          table_name_(rhs.table_name_),
          auto_commit_(rhs.auto_commit_),
          always_redirect_(rhs.always_redirect_),
          is_watch_keys_(rhs.is_watch_keys_)
    {
        if (is_cmd_owner_)
        {
            multi_obj_cmd_uptr_ = nullptr;
        }
        if (rhs.is_cmd_owner_)
        {
            multi_obj_cmd_uptr_ = std::move(rhs.multi_obj_cmd_uptr_);
        }
        else
        {
            multi_obj_cmd_ = rhs.multi_obj_cmd_;
        }
        is_cmd_owner_ = rhs.is_cmd_owner_;
    }

    ~MultiObjectCommandTxRequest() override
    {
        if (is_cmd_owner_)
        {
            multi_obj_cmd_uptr_ = nullptr;
        }
    }

    MultiObjectCommandTxRequest &operator=(
        const MultiObjectCommandTxRequest &rhs) = delete;
    MultiObjectCommandTxRequest &operator=(MultiObjectCommandTxRequest &&rhs) =
        delete;

    bool operator<(const ObjectCommandTxRequest &r) const
    {
        auto max_key_it =
            std::max_element(this->VctKey()->begin(),
                             this->VctKey()->end(),
                             [](const TxKey &lhs, const TxKey &rhs) -> bool
                             { return lhs < rhs; });
        return *max_key_it < *r.Key();
    }

    bool operator<(const MultiObjectCommandTxRequest &r) const
    {
        const auto &key_ptrs = this->VctKey();
        const auto &r_key_ptrs = r.VctKey();
        auto max_key_it =
            std::max_element(key_ptrs->begin(),
                             key_ptrs->end(),
                             [](const TxKey &lhs, const TxKey &rhs) -> bool
                             { return lhs < rhs; });
        auto r_min_key_it =
            std::min_element(r_key_ptrs->begin(),
                             r_key_ptrs->end(),
                             [](const TxKey &lhs, const TxKey &rhs) -> bool
                             { return lhs < rhs; });
        return *max_key_it < *r_min_key_it;
    }

    const std::vector<TxKey> *VctKey() const
    {
        MultiObjectTxCommand *cmd =
            is_cmd_owner_ ? multi_obj_cmd_ : multi_obj_cmd_uptr_.get();
        assert(cmd != nullptr);
        return cmd->KeyPointers();
    }

    const std::vector<TxCommand *> *VctCommand() const
    {
        MultiObjectTxCommand *cmd =
            is_cmd_owner_ ? multi_obj_cmd_ : multi_obj_cmd_uptr_.get();
        assert(cmd != nullptr);
        return cmd->CommandPointers();
    }

    MultiObjectTxCommand *Command() const
    {
        return is_cmd_owner_ ? multi_obj_cmd_ : multi_obj_cmd_uptr_.get();
    }

    const TableName *table_name_;
    bool auto_commit_{};
    bool always_redirect_{};
    union
    {
        MultiObjectTxCommand *multi_obj_cmd_{};
        std::unique_ptr<MultiObjectTxCommand> multi_obj_cmd_uptr_;
    };
    bool is_cmd_owner_{};
    bool is_watch_keys_{};
};

inline bool ObjectCommandTxRequest::operator<(
    const MultiObjectCommandTxRequest &r) const
{
    const TxKey *key_ptr = Key();
    const auto &r_key_ptrs = r.VctKey();
    auto r_min_key_it = std::min_element(
        r_key_ptrs->begin(),
        r_key_ptrs->end(),
        [](const TxKey &lhs, const TxKey &rhs) -> bool { return lhs < rhs; });
    return *key_ptr < *r_min_key_it;
}

struct PublishTxRequest : public TemplateTxRequest<PublishTxRequest, Void>
{
    PublishTxRequest(std::string_view chan,
                     std::string_view message,
                     TransactionExecution *txm = nullptr)
        : TemplateTxRequest(nullptr, nullptr, txm),
          chan_(chan),
          message_(message)
    {
    }

    std::string_view chan_;
    std::string_view message_;
};

struct ClusterScaleTxRequest
    : public TemplateTxRequest<ClusterScaleTxRequest, std::string>
{
    ClusterScaleTxRequest(
        const std::string &id,
        ClusterScaleOpType scale_type,
        const std::vector<std::pair<std::string, uint16_t>> *delta_nodes,
        uint32_t ng_id = UINT32_MAX,
        const std::vector<bool> *is_candidate = nullptr)
        : TemplateTxRequest(nullptr, nullptr),
          id_(id),
          scale_type_(scale_type),
          delta_nodes_(delta_nodes),
          ng_id_(ng_id),
          is_candidate_(is_candidate)
    {
    }

    std::string id_;

    ClusterScaleOpType scale_type_;
    // Case adding node, to indicate added node info;
    // Case removing node, to indicate removed node info;
    const std::vector<std::pair<std::string, uint16_t>> *delta_nodes_;

    // For add node group peers, to indicate the node group id to add peers to;
    // For remove node group peers, to indicate the node group id to remove
    // peers from;
    uint32_t ng_id_{UINT32_MAX};

    // For add node group peers, to indicate the is_candidate for each added
    // node;
    const std::vector<bool> *is_candidate_{nullptr};
};

struct SchemaRecoveryTxRequest
    : public TemplateTxRequest<SchemaRecoveryTxRequest, UpsertResult>
{
    SchemaRecoveryTxRequest(const ::txlog::SchemaOpMessage &schema_op_msg)
        : TemplateTxRequest(nullptr, nullptr), schema_op_msg_(schema_op_msg)
    {
    }

    const ::txlog::SchemaOpMessage &schema_op_msg_;
};

struct RangeSplitRecoveryTxRequest
    : public TemplateTxRequest<RangeSplitRecoveryTxRequest, bool>
{
    RangeSplitRecoveryTxRequest(
        const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
        std::shared_ptr<const TableSchema> &&table_schema,
        int32_t partition_id,
        TableRangeEntry *range_entry,
        std::vector<TxKey> &&new_range_keys,
        std::vector<int32_t> &&new_partition_ids,
        uint32_t node_group_id,
        bool is_dirty)
        : TemplateTxRequest(nullptr, nullptr),
          ds_split_range_op_msg_(ds_split_range_op_msg),
          table_schema_(std::move(table_schema)),
          partition_id_(partition_id),
          range_entry_(range_entry),
          new_range_keys_(std::move(new_range_keys)),
          new_partition_ids_(std::move(new_partition_ids)),
          node_group_id_(node_group_id),
          is_dirty_(is_dirty)
    {
    }

    const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg_;
    std::shared_ptr<const TableSchema> table_schema_{nullptr};
    int32_t partition_id_;
    TableRangeEntry *range_entry_;
    std::vector<TxKey> new_range_keys_;
    std::vector<int32_t> new_partition_ids_;
    uint32_t node_group_id_;
    bool is_dirty_;
};

struct ReloadCacheTxRequest
    : public TemplateTxRequest<ReloadCacheTxRequest, Void>
{
    ReloadCacheTxRequest(const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm)
    {
    }
};

struct FaultInjectTxRequest
    : public TemplateTxRequest<FaultInjectTxRequest, bool>
{
    FaultInjectTxRequest(const std::string &fault_name,
                         const std::string &fault_paras,
                         std::vector<int> &vct_node_id,
                         const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          fault_name_(fault_name),
          fault_paras_(fault_paras)
    {
        vct_node_id_.swap(vct_node_id);
    }

    const std::string fault_name_;
    const std::string fault_paras_;
    std::vector<int> vct_node_id_;
};

// for test
struct CleanCcEntryForTestTxRequest
    : public TemplateTxRequest<CleanCcEntryForTestTxRequest, bool>
{
    CleanCcEntryForTestTxRequest(const TableName *tab_name = nullptr,
                                 const TxKey *key = nullptr,
                                 bool only_archives = false,
                                 bool flush = true)
        : TemplateTxRequest(nullptr, nullptr),
          tab_name_(tab_name),
          key_(key),
          only_archives_{only_archives},
          flush_{flush}
    {
    }

    const TableName *tab_name_;
    const TxKey *key_;
    bool only_archives_;
    bool flush_;
};

// Batch read records from pk
struct BatchReadTxRequest : public TemplateTxRequest<BatchReadTxRequest, Void>
{
public:
    BatchReadTxRequest(const TableName *tab_name,
                       uint64_t schema_version,
                       std::vector<ScanBatchTuple> &tuple_batch,
                       bool is_for_write = false,
                       bool is_for_share = false,
                       bool read_local = false,
                       const std::function<void()> *yield_fptr = nullptr,
                       const std::function<void()> *resume_fptr = nullptr,
                       TransactionExecution *txm = nullptr,
                       uint64_t corresponding_sk_commit_ts = 0,
                       bool local_cache_checked = false)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          tab_name_(tab_name),
          read_batch_(tuple_batch),
          is_for_write_(is_for_write),
          is_for_share_(is_for_share),
          read_local_(read_local),
          corresponding_sk_commit_ts_(corresponding_sk_commit_ts),
          schema_version_(schema_version),
          local_cache_checked_(local_cache_checked)
    {
    }

    void Set(const TableName *tab_name,
             uint64_t schema_version,
             std::vector<ScanBatchTuple> &batch_read_pri,
             bool is_for_write = false,
             bool is_for_share = false,
             bool read_local = false,
             uint64_t corresponding_sk_commit_ts = 0)
    {
        tab_name_ = tab_name;
        read_batch_ = std::move(batch_read_pri);
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        read_local_ = read_local;
        corresponding_sk_commit_ts_ = corresponding_sk_commit_ts;
        schema_version_ = schema_version;
        local_cache_checked_ = false;
    }

    const TableName *tab_name_;
    std::vector<ScanBatchTuple> &read_batch_;
    bool is_for_write_;  // used for "select ... for update".
    bool is_for_share_;  // used for "select ... lock in share mode".
    bool read_local_;
    uint64_t corresponding_sk_commit_ts_;
    uint64_t schema_version_;
    bool local_cache_checked_;
};

struct InvalidateTableCacheTxRequest
    : public TemplateTxRequest<InvalidateTableCacheTxRequest, Void>
{
public:
    InvalidateTableCacheTxRequest(
        const TableName *table_name,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr,
        TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          table_name_(table_name)
    {
    }

    const TableName *table_name_;
};

}  // namespace txservice
