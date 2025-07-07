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

#include <map>
#include <memory>
#include <utility>  // std::pair

#include "cc/cc_req_base.h"
#include "cc_protocol.h"
#include "error_messages.h"  // CcErrorCode
#include "tx_record.h"       // RecordStatus
#include "type.h"            // LockType, LockOpStatus

namespace txservice
{
namespace remote
{
struct RemoteScanOpen;
struct RemoteScanNextBatch;
struct RemoteReadOutside;
}  // namespace remote

struct LruEntry;
struct LruPage;
struct TxObject;

struct AcquireCc;
struct AcquireAllCc;
struct PostWriteCc;
struct PostWriteAllCc;
struct PostReadCc;
struct ReadCc;
struct ScanCloseCc;
struct ScanOpenBatchCc;
struct ScanNextBatchCc;
struct ScanSliceCc;
struct NegotiateCc;
struct DataSyncScanCc;
struct CkptUpdateCc;
struct CkptTs;
struct BroadcastStatisticsCc;
struct AnalyzeTableAllCc;
struct ReplayLogCc;
struct ReloadCacheCc;
struct FaultInjectCC;
struct CleanCcEntryForTestCc;
struct FillStoreSliceCc;
struct InitKeyCacheCc;
struct KickoutCcEntryCc;
struct ApplyCc;
struct UploadTxCommandsCc;
struct UploadBatchCc;
struct DefragShardHeapCc;
struct UploadRangeSlicesCc;
struct UploadBatchSlicesCc;
struct UpdateKeyCacheCc;
struct KeyObjectStandbyForwardCc;
struct EscalateStandbyCcmCc;
struct RestoreCcMapCc;
struct InvalidateTableCacheCc;
#ifdef RANGE_PARTITION_ENABLED
struct SampleSubRangeKeysCc;
struct ScanSliceDeltaSizeCc;
#endif

enum struct ScanType : uint8_t
{
    ScanKey = 0,
    ScanGap,
    ScanBoth,
    ScanUnknow
};

enum struct CleanType
{
    /**
     * This used to kickout the ccentries that donot belong to this node anymore
     * during split range operation. In this case, the `CleanPageAndReBalance()`
     * is invoked during execute `KickoutCcEntryCc` request, and the CcPage come
     * from the request's `start_key`. Then will clean cc entries that are in
     * the specific range.
     */
    CleanRangeData = 0,
    /**
     * This used to kickout the ccentries that donot belong to this node anymore
     * during data migration. In this case, the range read lock hasn't been
     * acquried. So checking the range version to ensure the key range is
     * correct.
     */
    CleanRangeDataForMigration,
    /**
     * This is used to kickout the ccentries that no longer belong to this node
     * group anymore during bucket migration. All data in the specified bucket
     * should be cleaned regardless of its persistance status and lock status.
     */
    CleanBucketData,
    /**
     * This used to kickout the ccentries during alter table operation. In this
     * case, the `CleanPageAndReBalance()` is invoked during execute
     * `KickoutCcEntryCc` request, and the CcPage come from the request's
     * `start_key`. Then will clean cc entries that match the below conditions:
     * 1) `commit_ts` less than the the timestamp specified by the request, 2)
     * large than 1, that is to say not the initial entry, 3) is free, that is
     * to say, the ccentry has been checkpointed and have no lock on it.
     */
    CleanForAlterTable,
    /**
     * This is used to clean all data during upsert table operation.
     */
    CleanCcm,
    /*
     * Clean deleted data in ccms. This is only used when we do not allow cache
     * replacement when memory is full.
     */
    CleanDeletedData,

    CleanDataForTest,
};

class CcShard;
struct TableSchema;
struct KeySchema;
struct RecordSchema;
class NonBlockingLock;

class CcMap
{
public:
    using uptr = std::unique_ptr<CcMap>;

    CcMap(CcShard *shard,
          NodeGroupId cc_ng_id,
          const TableName &table_name,
          const TableSchema *table_schema,
          uint64_t schema_ts,
          bool ccm_has_full_entries = false)
        : shard_(shard),
          cc_ng_id_(cc_ng_id),
          table_name_(table_name.StringView().data(),
                      table_name.StringView().size(),
                      table_name.Type(),
                      table_name.Engine()),
          ccm_has_full_entries_(ccm_has_full_entries),
          schema_ts_(schema_ts),
          table_schema_(table_schema)
    {
    }

    virtual ~CcMap() = default;

    virtual bool Execute(AcquireCc &req) = 0;
    virtual bool Execute(AcquireAllCc &req) = 0;
    virtual bool Execute(PostWriteCc &req) = 0;
    virtual bool Execute(PostWriteAllCc &req) = 0;
    virtual bool Execute(PostReadCc &req) = 0;
    virtual bool Execute(ReadCc &req) = 0;
    virtual bool Execute(ScanCloseCc &req) = 0;
    virtual bool Execute(ScanOpenBatchCc &req) = 0;
    virtual bool Execute(ScanNextBatchCc &req) = 0;
    virtual bool Execute(remote::RemoteScanOpen &req) = 0;
    virtual bool Execute(remote::RemoteScanNextBatch &req) = 0;
    virtual bool Execute(ScanSliceCc &req) = 0;
    virtual bool Execute(DataSyncScanCc &req) = 0;
    virtual bool Execute(remote::RemoteReadOutside &req) = 0;
    virtual bool Execute(BroadcastStatisticsCc &req) = 0;
    virtual bool Execute(AnalyzeTableAllCc &req) = 0;
    virtual bool Execute(ReplayLogCc &req) = 0;
    virtual bool Execute(ReloadCacheCc &req) = 0;
    virtual bool Execute(FaultInjectCC &req) = 0;
    virtual bool Execute(CleanCcEntryForTestCc &req) = 0;
    virtual bool Execute(FillStoreSliceCc &req) = 0;
    virtual bool Execute(InitKeyCacheCc &req) = 0;
    virtual bool Execute(KickoutCcEntryCc &req) = 0;
    virtual bool Execute(UploadBatchCc &req) = 0;
    virtual bool Execute(ApplyCc &req) = 0;
    virtual bool Execute(UploadTxCommandsCc &req) = 0;
    virtual bool Execute(DefragShardHeapCc &req) = 0;
    virtual bool Execute(UploadRangeSlicesCc &req) = 0;
    virtual bool Execute(UploadBatchSlicesCc &req) = 0;
    virtual bool Execute(UpdateKeyCacheCc &req) = 0;
    virtual bool Execute(KeyObjectStandbyForwardCc &req) = 0;
    virtual bool Execute(EscalateStandbyCcmCc &req) = 0;
    virtual bool Execute(RestoreCcMapCc &req) = 0;
    virtual bool Execute(InvalidateTableCacheCc &req) = 0;
#ifdef RANGE_PARTITION_ENABLED
    virtual bool Execute(SampleSubRangeKeysCc &req) = 0;
    virtual bool Execute(ScanSliceDeltaSizeCc &req) = 0;
#endif

    virtual size_t size() const = 0;
    virtual size_t NormalObjectSize()
    {
        assert(false);
        return 0;
    }

    virtual std::pair<size_t, LruPage *> CleanPageAndReBalance(
        LruPage *page,
        KickoutCcEntryCc *kickout_cc = nullptr,
        bool *is_success = nullptr) = 0;
    virtual void Clean() = 0;

    virtual void CleanEntry(LruEntry *entry, LruPage *page) = 0;

    virtual bool CleanBatchPages(size_t clean_page_cnt) = 0;

    // Called when FetchRecord returns. Backfill will clean up the read intent
    // added by fetch record and update cce status based on the fetch result.
    // If the fetch fails, cce will be erased if it is not used by other reqs.
    virtual bool BackFill(LruEntry *cce,
                          uint64_t commit_ts,
                          RecordStatus status,
                          const std::string &rec_str)
    {
        assert(false);
        return false;
    }

    /**
     * Used for debug to verify the map_link is complete.
     */
    virtual size_t VerifyOrdering() = 0;

    virtual TableType Type() const = 0;
    virtual const txservice::KeySchema *KeySchema() const = 0;
    virtual const txservice::RecordSchema *RecordSchema() const = 0;

    uint64_t SchemaTs() const
    {
        return schema_ts_;
    }

    const TableSchema *GetTableSchema() const
    {
        return table_schema_;
    }

    void SetTableSchema(const TableSchema *table_schema)
    {
        table_schema_ = table_schema;
    }

    void SetSchemaTs(uint64_t schema_ts)
    {
        schema_ts_ = schema_ts;
    }

    CcShard *const shard_;
    NodeGroupId cc_ng_id_;
    TableName table_name_;  // string owner
    // Kv store can be skipped if we know ccm contains all the entries. This is
    // crucial for performance. This flag is true when the table is created and
    // no LRU kickout happens on this ccm. In future, we should make it at range
    // level: the kv access unit is range.
    bool ccm_has_full_entries_{false};
    // The largest commit ts of dirty cc entries in this cc map. This value
    // might be larger than the actual max commit ts of cc entries. Currently
    // used to decide if this cc map has dirty data after a given ts.
    uint64_t last_dirty_commit_ts_{0};

protected:
    /**
     * @brief After the input request is executed at the current shard, moves
     * the request to another shard for execution.
     *
     * @param cc_req The cc request executed at the cc map.
     * @param target_core_id The destination shard/core ID to which the request
     * is moved.
     */
    void MoveRequest(CcRequestBase *cc_req, uint32_t target_core_id);

    /**
     * @brief Acquire key lock of ccentry. If succes, this method will
     * 'UpsertLockHoldingTx', else, it recover the transaction holding lock.
     *
     * @param cce
     * @param req
     * @param ng_id
     * @param ng_term
     * @param tx_term
     * @param cc_op
     * @param iso_level
     * @param protocol
     * @param is_resume If true, it means that the request is restored from
     * lock blocking queue, that is, the request just acquired the lock.
     * @return std::pair<LockType,CcErrorCode> : the first arg of the pair is
     * the lock that the request will acquire, the second is the error code.
     */
    std::pair<LockType, CcErrorCode> AcquireCceKeyLock(
        LruEntry *cce,
        uint64_t commit_ts,
        LruPage *page,
        RecordStatus cce_payload_status,
        CcRequestBase *req,
        uint32_t ng_id,
        int64_t ng_term,
        int64_t tx_term,
        CcOperation cc_op,
        IsolationLevel iso_level,
        CcProtocol protocol,
        uint64_t read_ts,
        bool is_covering_keys,
        CcMap *ccm = nullptr)
    {
        // deduce the lock type to acquire
        LockType lock_type = LockTypeUtil::DeduceLockType(
            cc_op, iso_level, protocol, is_covering_keys);

        return AcquireCceKeyLock(cce,
                                 commit_ts,
                                 page,
                                 cce_payload_status,
                                 req,
                                 ng_id,
                                 ng_term,
                                 tx_term,
                                 lock_type,
                                 cc_op,
                                 iso_level,
                                 protocol,
                                 read_ts,
                                 ccm);
    }

    std::pair<LockType, CcErrorCode> AcquireCceKeyLock(
        LruEntry *cce,
        uint64_t commit_ts,
        LruPage *page,
        RecordStatus cce_payload_status,
        CcRequestBase *req,
        uint32_t ng_id,
        int64_t ng_term,
        int64_t tx_term,
        LockType lock_type,
        CcOperation cc_op,
        IsolationLevel iso_level,
        CcProtocol protocol,
        uint64_t read_ts,
        CcMap *ccm = nullptr);

    /**
     * @brief do check after request is resumed from lock blocking queue.
     * @return  std::pair<LockType,CcErrorCode> : the first arg of the pair is
     * the lock that the request will acquire, the second is the error code.
     */
    std::pair<LockType, CcErrorCode> LockHandleForResumedRequest(
        LruEntry *cce,
        uint64_t commit_ts,
        RecordStatus cce_payload_status,
        CcRequestBase *req,
        uint32_t ng_id,
        int64_t ng_term,
        int64_t tx_term,
        CcOperation cc_op,
        IsolationLevel iso_level,
        CcProtocol protocol,
        uint64_t read_ts,
        bool is_covering_keys);

    void RecoverTxForLockConfilct(NonBlockingLock &lock,
                                  LockType lock_type,
                                  uint32_t ng_id,
                                  int64_t ng_term);

    void DowngradeCceKeyWriteLock(LruEntry *cce, TxNumber tx_number);

    /**
     * @brief ReleaseLock and DeleteLockHoldingTx
     *
     * @param cce
     * @param tx_number
     * @param lock_type
     * @param recycle_lock Whether recycle the lock if it's empty.
     */
    void ReleaseCceLock(NonBlockingLock *lock,
                        LruEntry *cce,
                        TxNumber tx_number,
                        uint32_t ng_id,
                        LockType lk_type = LockType::NoLock,
                        bool recycle_lock = true,
                        TxObject *object = nullptr) const;

    /**
     * @brief The version of this ccmap. It is the version of the corresponding
     * KeySchema. This value is different from version of the TableSchema.
     */
    uint64_t schema_ts_{1};
    const TableSchema *table_schema_;
    friend struct ReadCc;
    friend struct ApplyCc;
};
}  // namespace txservice
