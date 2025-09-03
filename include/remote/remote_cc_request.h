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

#include <bthread/moodycamelqueue.h>

#include <atomic>
#include <memory>  // unique_ptr

#include "cc/cc_request.h"
#include "proto/cc_request.pb.h"
#include "tx_record.h"  // RecordStatus
#include "type.h"

namespace txservice
{
template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
class TemplateCcMap;

template <typename SkT, typename PkT>
class SkCcMap;

class CcMap;

namespace remote
{
class CcStreamSender;

struct RemoteAcquire : public AcquireCc
{
public:
    RemoteAcquire();
    RemoteAcquire(const RemoteAcquire &rhs) = delete;
    RemoteAcquire(RemoteAcquire &&rhs) = delete;
    void Reset(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};

    CcHandlerResult<std::vector<AcquireKeyResult>> cc_res_{nullptr};
};

struct RemoteAcquireAll : public AcquireAllCc
{
public:
    RemoteAcquireAll();
    RemoteAcquireAll(const RemoteAcquireAll &rhs) = delete;
    RemoteAcquireAll(RemoteAcquireAll &&rhs) = delete;
    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

    void TryAcknowledge(int64_t term = -1,
                        uint32_t node_group_id = 0,
                        uint32_t core_id = 0,
                        uint64_t cce_lock_ptr = 0)
    {
        if (Protocol() == CcProtocol::Locking)
        {
            std::lock_guard<std::mutex> lk(mux_);
            core_cnt_ -= 1;

            if (term != -1)
            {
                auto &cce_addr = cce_addrs_.emplace_back();
                cce_addr.SetNodeGroupId(node_group_id);
                cce_addr.SetCceLock(cce_lock_ptr, term, core_id);
            }

            if (core_cnt_ == 0 && !cce_addrs_.empty())
            {
                Acknowledge();
            }
        }
    }

    void SetCoreCnt(size_t core_cnt)
    {
        core_cnt_ = core_cnt;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    KeyType key_type_{KeyType::Normal};

    size_t core_cnt_{0};
    std::vector<CcEntryAddr> cce_addrs_;
    CcHandlerResult<AcquireAllResult> cc_res_{nullptr};

    void Acknowledge();
};

struct RemotePostRead : public PostReadCc
{
public:
    RemotePostRead();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};

    CcEntryAddr cce_addr_;
    CcHandlerResult<PostProcessResult> cc_res_{nullptr};
    bool need_resp_{true};
};

struct RemoteRead : public ReadCc
{
public:
    RemoteRead();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    CcHandlerResult<ReadKeyResult> cc_res_{nullptr};
};

struct RemoteReadOutside : public CcRequestBase
{
public:
    RemoteReadOutside() = default;
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

    CcMap *Ccm()
    {
        LruEntry *lru_entry = cce_addr_.ExtractCce();
        return lru_entry->GetCcMap();
    }

    bool Execute(CcShard &ccs) override
    {
        if (cce_addr_.Term() !=
            Sharder::Instance().LeaderTerm(cce_addr_.NodeGroupId()))
        {
            LOG(INFO) << "RemoteReadOutside, node_group(#"
                      << cce_addr_.NodeGroupId() << ") term < 0, tx:" << Txn()
                      << " ,cce_lock: "
                      << reinterpret_cast<void *>(cce_addr_.CceLockPtr());
            Finish();
            return true;
        }

        return Ccm()->Execute(*this);
    }

    void Finish();

    const CcEntryAddr &CceAddr() const
    {
        return cce_addr_;
    }

    uint64_t CommitTs() const
    {
        return commit_ts_;
    }

    ::txservice::RecordStatus RecordStatus() const
    {
        return rec_status_;
    }

private:
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    const std::string *rec_str_{nullptr};
    ::txservice::RecordStatus rec_status_;

    uint64_t commit_ts_{0};
    CcEntryAddr cce_addr_;

    template <typename KeyT,
              typename ValueT,
              bool VersionedRecord,
              bool RangePartitioned>
    friend class ::txservice::TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;
};

struct RemotePostWrite : public PostWriteCc
{
public:
    RemotePostWrite();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};

    CcEntryAddr cce_addr_;
    CcHandlerResult<PostProcessResult> cc_res_{nullptr};
};

struct RemotePostWriteAll : public PostWriteAllCc
{
public:
    RemotePostWriteAll();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    KeyType key_type_{KeyType::Normal};

    CcHandlerResult<PostProcessResult> cc_res_{nullptr};
};

struct RemoteScanOpen : public TemplatedCcRequest<RemoteScanOpen, Void>
{
public:
    RemoteScanOpen();

    void Reset(std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt);
    void Free() override;
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

    uint16_t CommandId()
    {
        return input_msg_->command_id();
    }

    bool IsForWrite() const
    {
        return is_for_write_;
    }

    bool IsCoveringKeys() const
    {
        return is_covering_keys_;
    }

    uint64_t ReadTimestamp() const
    {
        return snapshot_ts_;
    }

    void SetCcePtr(LruEntry *ptr, int core_id)
    {
        cce_ptr_.at(core_id) = ptr;
    }

    LruEntry *CcePtr(int core_id) const
    {
        return cce_ptr_[core_id];
    }

    ScanType CcePtrScanType(int core_id)
    {
        return cce_ptr_scan_type_[core_id];
    }

    void SetCcePtrScanType(ScanType scan_type, int core_id)
    {
        cce_ptr_scan_type_[core_id] = scan_type;
    }

    void SetIsWaitForPostWrite(bool is_wait, int core_id)
    {
        is_wait_for_post_write_[core_id] = is_wait;
    }

    bool IsWaitForPostWrite(int core_id) const
    {
        return is_wait_for_post_write_[core_id];
    }

    uint64_t GetSchemaVersion() const
    {
        return schema_version_;
    }

    int32_t GetRedisObjectType() const
    {
        return obj_type_;
    }
    const std::string_view &GetRedisScanPattern() const
    {
        return scan_pattern_;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};

    KeyType key_type_{KeyType::Normal};
    const std::string *start_key_str_{nullptr};
    bool inclusive_{true};
    ScanDirection direct_{ScanDirection::Forward};
    std::vector<RemoteScanCache> scan_caches_;
    bool is_ckpt_delta_{false};
    CcHandlerResult<Void> cc_res_{nullptr};
    std::atomic<uint32_t> unfinish_cnt_{0};
    bool is_for_write_{false};
    bool is_covering_keys_{false};
    bool is_require_keys_{true};
    bool is_require_recs_{true};
    uint64_t snapshot_ts_{0};
    std::vector<bool> is_wait_for_post_write_;

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    std::vector<LruEntry *> cce_ptr_;
    // scan type for above cce_ptr_
    std::vector<ScanType> cce_ptr_scan_type_;
    uint64_t schema_version_{0};

    int32_t obj_type_{-1};
    std::string_view scan_pattern_;

    template <typename KeyT,
              typename ValueT,
              bool VersionedRecord,
              bool RangePartitioned>
    friend class ::txservice::TemplateCcMap;

    friend class ::txservice::CcMap;
};

struct RemoteScanNextBatch
    : public TemplatedCcRequest<RemoteScanNextBatch, Void>
{
public:
    RemoteScanNextBatch();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    bool ValidTermCheck() override;

    bool IsForWrite() const
    {
        return is_for_write_;
    }

    bool IsCoveringKeys() const
    {
        return is_covering_keys_;
    }

    uint64_t ReadTimestamp() const
    {
        return snapshot_ts_;
    }

    void SetCcePtr(LruEntry *ptr)
    {
        cce_ptr_ = ptr;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

    ScanType CcePtrScanType()
    {
        return cce_ptr_scan_type_;
    }

    void SetCcePtrScanType(ScanType scan_type)
    {
        cce_ptr_scan_type_ = scan_type;
    }

    void SetIsWaitForPostWrite(bool is_wait)
    {
        is_wait_for_post_write_ = is_wait;
    }

    bool IsWaitForPostWrite() const
    {
        return is_wait_for_post_write_;
    }

    CcEntryAddr PriorCceAddr()
    {
        return prior_cce_addr_;
    }

    int32_t GetRedisObjectType() const
    {
        return obj_type_;
    }
    const std::string_view &GetRedisScanPattern() const
    {
        return scan_pattern_;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    CcEntryAddr prior_cce_addr_;
    RemoteScanCache scan_cache_;
    // The address of the CC map of the blocked core.
    CcHandlerResult<Void> cc_res_{nullptr};
    uint64_t snapshot_ts_{0};

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};

    ScanDirection direct_{ScanDirection::Forward};
    bool is_ckpt_delta_{false};
    bool is_for_write_{false};
    bool is_covering_keys_{false};
    bool is_require_keys_{true};
    bool is_require_recs_{true};
    bool is_wait_for_post_write_{false};
    // scan type for above cce_ptr_
    ScanType cce_ptr_scan_type_{ScanType::ScanUnknow};
    TableType tbl_type_;

    int32_t obj_type_{-1};
    std::string_view scan_pattern_;

    template <typename KeyT,
              typename ValueT,
              bool VersionedRecord,
              bool RangePartitioned>
    friend class ::txservice::TemplateCcMap;

    friend class ::txservice::CcMap;
};

struct RemoteScanSlice : public ScanSliceCc
{
public:
    RemoteScanSlice();
    void Reset(std::unique_ptr<CcMessage> input_msg, uint16_t core_cnt);

private:
    ScanSliceResponse output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    TableName remote_tbl_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    CcHandlerResult<RangeScanSliceResult> cc_res_{nullptr};
    std::vector<RemoteScanSliceCache> scan_cache_vec_;
};

struct RemoteReloadCacheCc : public ReloadCacheCc
{
public:
    RemoteReloadCacheCc();

    RemoteReloadCacheCc(const RemoteReloadCacheCc &) = delete;
    RemoteReloadCacheCc(RemoteReloadCacheCc &&) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};

    CcHandlerResult<Void> cc_res_{nullptr};

    friend class RemoteCcHandler;
};

struct RemoteFaultInjectCC : public FaultInjectCC
{
public:
    RemoteFaultInjectCC();

    RemoteFaultInjectCC(const RemoteFaultInjectCC &rhs) = delete;
    RemoteFaultInjectCC(RemoteFaultInjectCC &&rhs) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};

    CcHandlerResult<bool> cc_res_{nullptr};

    friend class RemoteCcHandler;
};

struct RemoteBroadcastStatisticsCc : public BroadcastStatisticsCc
{
public:
    RemoteBroadcastStatisticsCc();
    RemoteBroadcastStatisticsCc(const RemoteBroadcastStatisticsCc &rhs) =
        delete;
    RemoteBroadcastStatisticsCc(RemoteBroadcastStatisticsCc &&rhs) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);

private:
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    CcHandlerResult<Void> cc_res_{nullptr};

    friend class RemoteCcHandler;
};

struct RemoteAnalyzeTableAllCc : public AnalyzeTableAllCc
{
public:
    RemoteAnalyzeTableAllCc();
    RemoteAnalyzeTableAllCc(const RemoteAnalyzeTableAllCc &rhs) = delete;
    RemoteAnalyzeTableAllCc(RemoteAnalyzeTableAllCc &&rhs) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    CcHandlerResult<Void> cc_res_{nullptr};

    friend class RemoteCcHandler;
};

struct RemoteCleanCcEntryForTestCc : public CleanCcEntryForTestCc
{
public:
    RemoteCleanCcEntryForTestCc();

    RemoteCleanCcEntryForTestCc(const RemoteCleanCcEntryForTestCc &rhs) =
        delete;
    RemoteCleanCcEntryForTestCc(RemoteCleanCcEntryForTestCc &&rhs) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};

    CcHandlerResult<bool> cc_res_{nullptr};

    friend class RemoteCcHandler;
};

struct RemoteCheckDeadLockCc : public CheckDeadLockCc
{
public:
    RemoteCheckDeadLockCc()
    {
    }

    virtual ~RemoteCheckDeadLockCc() = default;
    RemoteCheckDeadLockCc(const RemoteCheckDeadLockCc &rhs) = delete;
    RemoteCheckDeadLockCc(RemoteCheckDeadLockCc &&rhs) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);
    bool Execute(CcShard &ccs) override;

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};
};

struct RemoteAbortTransactionCc : public AbortTransactionCc
{
public:
    RemoteAbortTransactionCc()
    {
    }
    virtual ~RemoteAbortTransactionCc() = default;
    RemoteAbortTransactionCc(const RemoteAbortTransactionCc &rhs) = delete;
    RemoteAbortTransactionCc(RemoteAbortTransactionCc &&rhs) = delete;
    void Reset(std::unique_ptr<CcMessage> input_msg);
    bool Execute(CcShard &ccs) override;

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};
};

struct RemoteBlockReqCheckCc : public CcRequestBase
{
public:
    RemoteBlockReqCheckCc()
    {
    }
    virtual ~RemoteBlockReqCheckCc() = default;
    RemoteBlockReqCheckCc(const RemoteBlockReqCheckCc &rhs) = delete;
    RemoteBlockReqCheckCc(RemoteBlockReqCheckCc &&rhs) = delete;
    void Reset(std::unique_ptr<CcMessage> input_msg, size_t unfinished_cnt);
    bool Execute(CcShard &ccs) override;

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;

    std::mutex mux_;
    size_t unfinish_core_cnt_{0};
    bool term_changed_{false};
    bool all_finished_{true};
    CcStreamSender *hd_{nullptr};
};

struct RemoteKickoutCcEntry : public KickoutCcEntryCc
{
public:
    RemoteKickoutCcEntry();
    RemoteKickoutCcEntry(const RemoteKickoutCcEntry &rhs) = delete;
    RemoteKickoutCcEntry(RemoteKickoutCcEntry &&rhs) = delete;
    void Reset(std::unique_ptr<CcMessage> input_msg);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    TableName table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    CcStreamSender *hd_{nullptr};
    CcHandlerResult<Void> cc_res_{nullptr};
};

struct RemoteApplyCc : public ApplyCc
{
public:
    RemoteApplyCc();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

protected:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    CcHandlerResult<ObjectCommandResult> cc_res_{nullptr};
};

struct RemoteUploadTxCommandsCc : public UploadTxCommandsCc
{
public:
    RemoteUploadTxCommandsCc();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    // TableName remote_table_name_{empty_sv, TableType::Primary,
    // txservice::TableEngine::None};

    CcEntryAddr cce_addr_;
    std::vector<std::string> cmds_vec_;
    CcHandlerResult<PostProcessResult> cc_res_{nullptr};
};

struct RemoteDbSizeCc : public DbSizeCc
{
public:
    RemoteDbSizeCc();
    void Reset(std::unique_ptr<CcMessage> input_msg, size_t core_cnt);
    bool Execute(CcShard &ccs) override;

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    std::function<void()> post_lambda_;
    std::vector<TableName> redis_table_names_;
};

struct RemoteInvalidateTableCacheCc : public InvalidateTableCacheCc
{
public:
    RemoteInvalidateTableCacheCc();
    RemoteInvalidateTableCacheCc(const RemoteInvalidateTableCacheCc &rhs) =
        delete;
    RemoteInvalidateTableCacheCc(RemoteInvalidateTableCacheCc &&rhs) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{
        empty_sv, TableType::Primary, txservice::TableEngine::None};
    CcHandlerResult<Void> cc_res_{nullptr};

    friend class RemoteCcHandler;
};
}  // namespace remote
}  // namespace txservice
