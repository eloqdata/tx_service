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

#include <memory>
#include <vector>

#include "catalog_factory.h"
#include "cc_handler_result.h"
#include "cc_protocol.h"
#include "ccm_scanner.h"
#include "scan.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
struct UpsertTableOp;
struct SplitFlushRangeOp;
struct UpsertTableIndexOp;

enum struct CcReqStatus
{
    /**
     * @brief The cc request is sent to a shard at the local node.
     *
     */
    SentLocal = 0,
    /**
     * @brief The cc request is sent to a shard at a remote node.
     *
     */
    SentRemote,
    /**
     * @brief The cc request is sent to the shard to which the handler is bound
     * and has been processed.
     *
     */
    Processed
};

class CcHandler
{
public:
    using Pointer = std::unique_ptr<CcHandler>;

    virtual ~CcHandler() = default;

    /**
     * @brief Acquires a write lock for the input key in the concurrency control
     * (cc) map. When there is no conflict, the request puts a write lock on the
     * key's cc entry and returns the cc entry's version. When there is a
     * conflict, the request is blocked and put into a waiting queue, if the tx
     * is under 2PL. The request returns with an error upon conflicts, if the tx
     * is under OCC/MVCC, forcing the tx to abort immediately.
     *
     * @param table_name Table name of the input key
     * @param key The key to be locked
     * @param txid Tx ID
     * @param tx_term The term of the tx node
     * @param ts Start timestamp of the tx
     * @param is_insert Whether or not the write operation is an insert
     * @param hres Result handler of the request
     * @param proto Concurrency control protocol
     */
    virtual void AcquireWrite(
        const TableName &table_name,
        const uint64_t schema_version,
        const TxKey &key,
        uint32_t key_shard_code,
        TxNumber tx_number,
        int64_t tx_term,
        uint16_t command_id,
        uint64_t ts,
        bool is_insert,
        CcHandlerResult<std::vector<AcquireKeyResult>> &hres,
        uint32_t hd_res_idx,
        CcProtocol proto,
        IsolationLevel iso_level) = 0;

    /**
     * @brief Acquires write locks for the input key in all shards. This method
     * is used for replicated cc maps, where identical cc maps appear in all
     * nodes. Replicated cc maps are for data frequently accessed, rarely
     * modified and needs to be strongly consistent and performant, e.g., table
     * catalog and range partition function.
     *
     * @param table_name Table name of the input key
     * @param key The key to be locked
     * @param txid Tx ID
     * @param tx_term Term of the tx node
     * @param ts Start timestamp of the tx
     * @param is_insert Whether or not the write operation is an insert
     * @param hres Result handler of the request
     * @param proto Concurrency control protocol, 2PL or OCC/MVCC
     */
    virtual void AcquireWriteAll(const TableName &table_name,
                                 const TxKey &key,
                                 NodeGroupId ng_id,
                                 TxNumber txn,
                                 int64_t tx_term,
                                 uint16_t command_id,
                                 bool is_insert,
                                 CcHandlerResult<AcquireAllResult> &hres,
                                 CcProtocol proto,
                                 CcOperation cc_op) = 0;

    virtual void PostWriteAll(const TableName &table_name,
                              const TxKey &key,
                              TxRecord &rec,
                              NodeGroupId ng_id,
                              uint64_t tx_number,
                              int64_t tx_term,
                              uint16_t command_id,
                              uint64_t ts,
                              CcHandlerResult<PostProcessResult> &hres,
                              OperationType op_type,
                              PostWriteType post_write_type) = 0;

    /**
     * @brief Post-processes a write key. Post-processing clears the write lock,
     * and if the tx commits, installs the committed record.
     *
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param commit_ts Commit timestamp, if the tx commits. 0, if the tx aborts
     * and the sole purpose of the request is to release the write lock of the
     * key.
     * @param ccentry_addr Address of the cc entry, on which the write lock
     * is put.
     * @param record Pointer to the committed record. Null, if the tx aborts.
     * @param is_deleted Whether or not the write deletes a record
     * @param hres Result handler of the request
     */
    virtual CcReqStatus PostWrite(TxNumber tx_number,
                                  int64_t tx_term,
                                  uint16_t command_id,
                                  uint64_t commit_ts,
                                  const CcEntryAddr &ccentry_addr,
                                  const TxRecord *record,
                                  OperationType operation_type,
                                  uint32_t key_shard_code,
                                  CcHandlerResult<PostProcessResult> &hres) = 0;

    /**
     * @briefPost-processes a read/scan key. Post-processing clears the read
     * lock or intention on the key's cc entry, matches the input key/gap
     * timestamps against those of the cc entry and updates the cc entry's
     * last_vali_ts field. The last_vali_ts field forces future transactions
     * writing the key to commit at timestamps later than the reading tx. For
     * OCC/MVCC, mismatches of the key/gap timestamps indicate that version
     * stability is violated. For isolation levels greater than or equal to
     * repeatable read, the reading tx needs to abort.
     *
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param key_ts Version of the key. 0, if the tx does not read the key and
     * does not check version stability.
     * @param gap_ts Version of the gap. 0, if the tx does not read the gap and
     * does not check version stability.
     * @param commit_ts Commit timestamp of the tx. 0, if the tx aborts.
     * @param ccentry_addr Address of the cc entry
     * @param hres Result handler of the request
     * @param is_local
     * @param need_remote_resp
     * @return CcReqStatus
     */
    virtual CcReqStatus PostRead(uint64_t tx_number,
                                 int64_t tx_term,
                                 uint16_t command_id,
                                 uint64_t key_ts,
                                 uint64_t gap_ts,
                                 uint64_t commit_ts,
                                 const CcEntryAddr &ccentry_addr,
                                 CcHandlerResult<PostProcessResult> &hres,
                                 bool is_local = false,
                                 bool need_remote_resp = true) = 0;

    /**
     * @brief Reads the input key and returns the key's record. The request puts
     * a read lock (for 2PL) or intention (for OCC/MVCC) on the key's cc entry,
     * if the tx's isolation level is equal to or greater than repeatable read.
     * The request is blocked and put into a waiting queue, if (1) there is a
     * read-write conflict, (2) the tx is under 2PL, and (3) the isolation level
     * is greater than or equal to repeatable read.
     *
     * @param table_name Table name of the input key
     * @param key The key to be read
     * @param rec Key's record to be filled
     * @param read_type Read type
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param ts Start timestamp of the tx
     * @param hres Result handler of the read request
     * @param iso_level Isolation level
     * @param proto Concurrency control (cc) protocol
     */
    virtual void Read(const TableName &table_name,
                      const uint64_t schema_version,
                      const TxKey &key,
                      uint32_t key_shard_code,
                      TxRecord &rec,
                      ReadType read_type,
                      uint64_t tx_number,
                      int64_t tx_term,
                      uint16_t command_id,
                      const uint64_t ts,
                      CcHandlerResult<ReadKeyResult> &hres,
                      IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                      CcProtocol proto = CcProtocol::OCC,
                      bool is_for_write = false,
                      bool is_covering_keys = false,
                      bool point_read_on_miss = false) = 0;

    /**
     * @brief Brings the previously-read key's record into the cc map for
     * caching. The method is only called after Read(), which starts
     * concurrency control for a key not in the cc map and thus does not return
     * the key's record.
     *
     * @param tx_term Term of the tx node
     * @param rec Record to be cached
     * @param is_deleted Whether or not the record is deleted
     * @param commit_ts Commit Timestamp
     * @param cce_addr Address of the key's cc entry
     * @param hres Result handler of the request
     * @param archives Historical versions
     */
    virtual void ReadOutside(
        int64_t tx_term,
        uint16_t command_id,
        TxRecord &rec,
        bool is_deleted,
        uint64_t commit_ts,
        const CcEntryAddr &cce_addr,
        CcHandlerResult<ReadKeyResult> &hres,
        std::vector<VersionTxRecord> *archives = nullptr) = 0;

    /**
     * @brief ReadLocal is used to read replicated cc maps, which contain a cc
     * map replica in every shard. An optimization for ReadLocal is to execute
     * the request directly without putting the request into the execution
     * queue.
     *
     * @param table_name Table name of the input key
     * @param key The key to be read
     * @param rec Key's record to be filled
     * @param read_type Read type
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param ts Start timestamp of the tx
     * @param hres Result handler of the read request
     * @param iso_level Isolation level
     * @param proto Concurrency control (cc) protocol
     */
    virtual bool ReadLocal(
        const TableName &table_name,
        const TxKey &key,
        TxRecord &record,
        ReadType read_type,
        uint64_t tx_number,
        int64_t tx_term,
        uint16_t command_id,
        const uint64_t ts,
        CcHandlerResult<ReadKeyResult> &hres,
        IsolationLevel iso_level = IsolationLevel::RepeatableRead,
        CcProtocol proto = CcProtocol::Locking,
        bool is_for_write = false,
        bool is_recovring = false,
        bool execute_immediately = true) = 0;

    virtual bool ReadLocal(
        const TableName &table_name,
        const std::string &key_str,
        TxRecord &record,
        ReadType read_type,
        uint64_t tx_number,
        int64_t tx_term,
        uint16_t command_id,
        const uint64_t ts,
        CcHandlerResult<ReadKeyResult> &hres,
        IsolationLevel iso_level = IsolationLevel::RepeatableRead,
        CcProtocol proto = CcProtocol::Locking,
        bool is_for_write = false,
        bool is_recovring = false) = 0;

    virtual void ScanOpen(
        const TableName &table_name,
        const uint64_t schema_version,
        ScanIndexType index_type,
        const TxKey &start_key,
        bool inclusive,
        uint64_t tx_number,
        int64_t tx_term,
        uint16_t command_id,
        uint64_t start_ts,
        CcHandlerResult<ScanOpenResult> &hd_res,
        ScanDirection direction = ScanDirection::Forward,
        IsolationLevel iso_level = IsolationLevel::ReadCommitted,
        CcProtocol proto = CcProtocol::OCC,
        bool is_for_write = false,
        bool is_ckpt_delta = false,
        bool is_covering_keys = false,
        bool is_require_keys = true,
        bool is_require_recs = true,
        bool is_require_sort = true,
        int32_t obj_type = -1,
        const std::string_view &scan_pattern = {}) = 0;

    virtual void ScanOpenLocal(
        const TableName &table_name,
        ScanIndexType index_type,
        const TxKey &start_key,
        bool inclusive,
        uint64_t tx_number,
        int64_t tx_term,
        uint16_t command_id,
        uint64_t ts,
        CcHandlerResult<ScanOpenResult> &hd_res,
        ScanDirection direction = ScanDirection::Forward,
        IsolationLevel iso_level = IsolationLevel::ReadCommitted,
        CcProtocol proto = CcProtocol::OCC,
        bool is_for_write = false,
        bool is_ckpt_delta = false) = 0;

    virtual void ScanNextBatch(uint64_t tx_number,
                               int64_t tx_term,
                               uint16_t command_id,
                               uint64_t start_ts,
                               CcScanner &scanner,
                               CcHandlerResult<ScanNextResult> &hd_res,
                               int32_t obj_type = -1,
                               const std::string_view &scan_pattern = {}) = 0;

    virtual void ScanNextBatch(
        const TableName &tbl_name,
        const uint64_t schema_version,
        uint32_t range_id,
        NodeGroupId range_owner,
        int64_t cc_ng_term,
        const TxKey *start_key,
        bool start_inclusive,
        const TxKey *end_key,
        bool end_inclusive,
        uint32_t prefetch_size,
        uint64_t read_ts,
        uint64_t tx_number,
        int64_t tx_term,
        uint16_t command_id,
        CcHandlerResult<RangeScanSliceResult> &hd_res,
        IsolationLevel iso_level = IsolationLevel::ReadCommitted,
        CcProtocol proto = CcProtocol::OCC) = 0;

    virtual void ScanNextBatchLocal(
        uint64_t tx_number,
        int64_t tx_term,
        uint16_t command_id,
        uint64_t start_ts,
        CcScanner &scanner,
        CcHandlerResult<ScanNextResult> &hd_res) = 0;

    virtual void ScanClose(const TableName &table_name,
                           ScanDirection direction,
                           std::unique_ptr<CcScanner> scanner) = 0;

    /// <summary>
    /// Starts a new tx and returns the tx ID.
    /// </summary>
    /// <param name="entry"></param>
    /// <param name="local_time"></param>
    /// <param name="max_txn_execution_time_ms"></param>
    /// <param name=""></param>
    virtual void NewTxn(CcHandlerResult<InitTxResult> &hres,
                        IsolationLevel iso_level,
                        NodeGroupId tx_ng_id,
                        uint32_t log_group_id) = 0;

    /// <summary>
    /// Sets the commit timestamp of the input tx.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="commit_ts"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    virtual void SetCommitTimestamp(const TxId &txid,
                                    uint64_t commit_ts,
                                    CcHandlerResult<uint64_t> &) = 0;

    /// <summary>
    /// Negotiates with the input tx such that it commits no later than the
    /// input timestamp.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="commit_ts_lower_bound"></param>
    /// <param name=""></param>
    virtual void UpdateCommitLowerBound(const TxId &txn_id,
                                        uint64_t commit_ts_lower_bound,
                                        CcHandlerResult<uint64_t> &) = 0;

    /// <summary>
    /// Sets the tx status to committed or aborted, after the fate of the tx is
    /// determined.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="status"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    virtual void UpdateTxnStatus(const TxId &txn_id,
                                 IsolationLevel iso_level,
                                 TxnStatus status,
                                 CcHandlerResult<Void> &) = 0;

    virtual void ReloadCache(NodeGroupId ng_id,
                             TxNumber tx_number,
                             int64_t tx_term,
                             uint16_t command_id,
                             CcHandlerResult<Void> &hres) = 0;

    virtual void FaultInject(const std::string &fault_name,
                             const std::string &fault_paras,
                             int64_t tx_term,
                             uint16_t command_id,
                             const TxId &txid,
                             std::vector<int> &vct_node_id,
                             CcHandlerResult<bool> &hres) = 0;

    virtual void DataStoreUpsertTable(
        const TableSchema *old_schema,
        const TableSchema *schema,
        OperationType op_type,
        uint64_t write_time,
        NodeGroupId ng_id,
        int64_t tx_term,
        CcHandlerResult<Void> &hres,
        const txservice::AlterTableInfo *alter_table_info = nullptr) = 0;

    virtual void AnalyzeTableAll(const TableName &table_name,
                                 NodeGroupId ng_id,
                                 TxNumber tx_number,
                                 int64_t tx_term,
                                 uint16_t command_id,
                                 CcHandlerResult<Void> &hres) = 0;

    virtual void BroadcastStatistics(
        const TableName &table_name,
        uint64_t schema_ts,
        const remote::NodeGroupSamplePool &sample_pool,
        NodeGroupId ng_id,
        TxNumber tx_number,
        int64_t tx_term,
        uint16_t command_id,
        CcHandlerResult<Void> &hres) = 0;

    virtual uint32_t GetNodeId() const = 0;

    /**
     * Execute obj_cmd on the object specified by key to get the result.
     *
     * @param table_name
     * @param key
     * @param key_shard_code
     * @param obj_cmd
     * @param txn
     * @param tx_term
     * @param command_id the value of "TxExection::command_id_", it has nothing
     * to do with "TxCommand".
     * @param tx_ts
     * @param hres
     * @param proto
     * @param commit if true, not just execute the command to get the result,
     * but also commit it on the object
     */
    virtual void ObjectCommand(const TableName &table_name,
                               const uint64_t schema_version,
                               const TxKey &key,
                               uint32_t key_shard_code,
                               TxCommand &obj_cmd,
                               TxNumber txn,
                               int64_t tx_term,
                               uint16_t command_id,
                               uint64_t tx_ts,
                               CcHandlerResult<ObjectCommandResult> &hres,
                               IsolationLevel iso_level,
                               CcProtocol proto,
                               bool commit,
                               bool always_redirect) = 0;

    virtual void PublishMessage(uint64_t ng_id,
                                int64_t tx_term,
                                std::string_view chan,
                                std::string_view message) = 0;
    /**
     * Upload and commit object commands to the cc map on the bucket's new owner
     * when the bucket is in migration. The commands has applied on the old node
     * group cc map. (Double write)
     *
     */
    virtual CcReqStatus UploadTxCommands(
        uint64_t tx_number,
        int64_t tx_term,
        uint16_t command_id,
        const CcEntryAddr &cce_addr,
        uint64_t obj_version,
        uint64_t commit_ts,
        const std::vector<std::string> *cmd_list,
        bool has_overwrite,
        CcHandlerResult<PostProcessResult> &hres) = 0;

    virtual void CleanCcEntryForTest(const TableName &table_name,
                                     const TxKey &key,
                                     bool only_archives,
                                     bool flush,
                                     uint64_t tx_number,
                                     int64_t tx_term,
                                     uint16_t command_id,
                                     CcHandlerResult<bool> &hres) = 0;

    virtual void BlockCcReqCheck(uint64_t tx_number,
                                 int64_t tx_term,
                                 uint16_t command_id,
                                 const CcEntryAddr &cce_addr,
                                 CcHandlerResultBase *hres,
                                 ResultTemplateType type,
                                 size_t acq_key_result_vec_idx = 0) = 0;

    virtual void BlockAcquireAllCcReqCheck(uint32_t ng_id,
                                           uint64_t tx_number,
                                           int64_t tx_term,
                                           uint16_t command_id,
                                           std::vector<CcEntryAddr> &cce_addr,
                                           CcHandlerResultBase *hres) = 0;

    /**
     * @brief Kickout the ccentrise that are between start_key and
     * end_key from ccmap.
     *
     * @param ng_id The node group id that to execute the operation.
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param command_id
     * @param hres Result handler of the request
     * @param clean_type The clean type of the data
     * @param bucket_id only used if clean type is CleanBucketData
     * @param start_key The start key of the data
     * @param end_key The end key of the data
     * @param clean_ts only used if clean type is CleanForAlterTable
     */
    virtual void KickoutData(const TableName &table_name,
                             uint32_t ng_id,
                             TxNumber tx_number,
                             int64_t tx_term,
                             uint64_t command_id,
                             CcHandlerResult<Void> &hres,
                             CleanType clean_type,
                             std::vector<uint16_t> *bucket_ids = nullptr,
                             const TxKey *start_key = nullptr,
                             const TxKey *end_key = nullptr,
                             uint64_t clean_ts = 0,
                             int32_t range_id = INT32_MAX,
                             uint64_t range_version = UINT64_MAX) = 0;

    virtual void VerifyOrphanLock(TxNumber txn) = 0;

    virtual void InvalidateTableCache(const TableName &table_name,
                                      uint32_t ng_id,
                                      TxNumber tx_number,
                                      int64_t tx_term,
                                      uint64_t command_id,
                                      CcHandlerResult<Void> &hres) = 0;

#ifdef RANGE_PARTITION_ENABLED
    /**
     * @brief Delete key that are between start_key and end_key from key cache.
     *
     * @param table_name table name.
     * @param ng_id The node group id that to execute the operation.
     * @param tx_term Term of the tx node
     * @param start_key The start key of the data
     * @param end_key The end key of the data
     * @param store_range The store range that the keys belong to.
     * @param hres Result handler of the request
     */
    virtual void UpdateKeyCache(const TableName &table_name,
                                NodeGroupId ng_id,
                                int64_t tx_term,
                                const TxKey &start_key,
                                const TxKey &end_key,
                                StoreRange *store_range,
                                CcHandlerResult<Void> &hres) = 0;
#endif
};

}  // namespace txservice
