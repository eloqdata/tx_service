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

#include <memory>
#include <vector>

#include "cc/cc_req_pool.h"
#include "proto/cc_request.pb.h"
#include "remote/cc_stream_sender.h"
#include "remote_cc_request.h"
#include "tx_operation_result.h"
#include "tx_record.h"  // RecordStatus;VersionTxRecord

namespace txservice
{
class LocalCcShards;

namespace remote
{
class RemoteCcHandler
{
public:
    RemoteCcHandler(CcStreamSender &stream_sender);
    ~RemoteCcHandler() = default;

    void AcquireWrite(uint32_t src_node_id,
                      NodeGroupId dest_ng_id,
                      const TableName &table_name,
                      uint64_t schema_version,
                      const TxKey &key,
                      uint32_t key_shard_code,
                      TxNumber txn,
                      int64_t tx_term,
                      uint16_t command_id,
                      uint64_t ts,
                      bool is_insert,
                      CcHandlerResult<std::vector<AcquireKeyResult>> &hres,
                      uint32_t hd_res_idx,
                      CcProtocol proto = CcProtocol::OCC,
                      IsolationLevel iso_level = IsolationLevel::ReadCommitted);

    void AcquireWriteAll(uint32_t src_node_id,
                         const TableName &table_name,
                         const TxKey &key,
                         uint32_t node_group_id,
                         TxNumber tx_number,
                         int64_t tx_term,
                         uint16_t command_id,
                         bool is_insert,
                         CcHandlerResult<AcquireAllResult> &hres,
                         CcProtocol proto,
                         CcOperation cc_op);

    void PostWrite(uint32_t src_node_id,
                   uint64_t tx_number,
                   int64_t tx_term,
                   uint16_t command_id,
                   uint64_t commit_ts,
                   const CcEntryAddr &cce_addr,
                   const TxRecord *record,
                   OperationType operation_type,
                   uint32_t key_shard_code,
                   CcHandlerResult<PostProcessResult> &hres);

    void PostWriteAll(uint32_t src_node_id,
                      const TableName &table_name,
                      const TxKey &key,
                      TxRecord &rec,
                      NodeGroupId ng_id,
                      uint64_t tx_number,
                      int64_t tx_term,
                      uint16_t command_id,
                      uint64_t commit_ts,
                      CcHandlerResult<PostProcessResult> &hres,
                      OperationType op_type,
                      PostWriteType post_write_type);

    void PostRead(uint32_t src_node_id,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint16_t command_id,
                  uint64_t key_ts,
                  uint64_t gap_ts,
                  uint64_t commit_ts,
                  const CcEntryAddr &cce_addr,
                  CcHandlerResult<PostProcessResult> &hres,
                  bool need_remote_resp = true);

    void Read(uint32_t src_node_id,
              NodeGroupId dest_ng_id,
              const TableName &table_name,
              const uint64_t schema_version,
              const TxKey &key,
              uint32_t key_shard_code,
              const TxRecord &record,
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
              bool point_read_on_miss = false,
              int32_t partition_id = -1);

    void ReadOutside(int64_t tx_term,
                     uint16_t command_id,
                     const TxRecord &record,
                     bool is_deleted,
                     uint64_t commit_ts,
                     const CcEntryAddr &cce_addr,
                     std::vector<VersionTxRecord> *archives = nullptr);

    void ScanOpen(uint32_t src_node_id,
                  const TableName &table_name,
                  const uint64_t schema_version,
                  ScanIndexType index_type,
                  uint32_t node_group_id,
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
                  bool is_ckpt = false,
                  bool is_covering_keys = false,
                  bool is_require_keys = true,
                  bool is_require_recs = true,
                  bool is_require_sort = true,
                  int32_t obj_type = -1,
                  const std::string_view &scan_pattern = {});

    void ScanNext(uint32_t src_node_id,
                  uint32_t ng_id,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint16_t command_id,
                  uint64_t start_ts,
                  ScanCache *scan_cache,
                  CcHandlerResult<ScanNextResult> &hd_res,
                  IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                  CcProtocol proto = CcProtocol::OCC,
                  bool is_for_write = false,
                  bool is_ckpt = false,
                  bool is_covering_keys = false,
                  bool is_require_keys = true,
                  bool is_require_recs = true,
                  bool is_require_sort = true,
                  int32_t obj_type = -1,
                  const std::string_view &scan_pattern = {});

    void ScanNext(uint32_t src_node_id,
                  const TableName &tbl_name,
                  uint64_t schema_version,
                  uint32_t range_id,
                  NodeGroupId cc_ng_id,
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
                  IsolationLevel iso_level,
                  CcProtocol proto);

    void ScanClose(const TableName &table_name,
                   size_t alias,
                   const TxKey &end_key,
                   bool inclusive,
                   CcProtocol proto = CcProtocol::OCC,
                   LockType lock_type = LockType::ReadLock)
    {
    }

    void UpdateCommitLowerBound(const TxId &txid,
                                uint64_t commit_ts_lower_bound,
                                CcHandlerResult<uint64_t> &)
    {
    }

    void ReloadCache(uint32_t src_node_id,
                     NodeGroupId ng_id,
                     TxNumber tx_number,
                     int64_t tx_term,
                     uint16_t command_id,
                     CcHandlerResult<Void> &hres);

    void FaultInject(uint32_t src_node_id,
                     const std::string &fault_name,
                     const std::string &fault_paras,
                     int64_t tx_term,
                     uint16_t command_id,
                     const TxId &txid,
                     int node_id,
                     CcHandlerResult<bool> &hres);

    void AnalyzeTableAll(uint32_t src_node_id,
                         const TableName &table_name,
                         NodeGroupId ng_id,
                         TxNumber tx_number,
                         int64_t tx_term,
                         uint16_t command_id,
                         CcHandlerResult<Void> &hres);

    void BroadcastStatistics(uint32_t src_node_id,
                             const TableName &table_name,
                             uint64_t schema_ts,
                             const remote::NodeGroupSamplePool &sample_pool,
                             NodeGroupId ng_id,
                             TxNumber tx_number,
                             int64_t tx_term,
                             uint16_t command_id,
                             CcHandlerResult<Void> &hres);

    void CleanCcEntryForTest(uint32_t src_node_id,
                             const TableName &table_name,
                             const TxKey &key,
                             bool only_archives,
                             bool flush,
                             uint32_t key_shard_code,
                             uint64_t tx_number,
                             int64_t tx_term,
                             uint16_t command_id,
                             CcHandlerResult<bool> &hres);
    void BlockCcReqCheck(uint32_t src_node_id,
                         uint64_t tx_number,
                         int64_t tx_term,
                         uint16_t command_id,
                         const CcEntryAddr &cce_addr,
                         CcHandlerResultBase *hres,
                         ResultTemplateType type,
                         size_t acq_key_result_vec_idx = 0);

    void BlockAcquireAllCcReqCheck(uint32_t src_node_id,
                                   uint32_t node_group_id,
                                   uint64_t tx_number,
                                   int64_t tx_term,
                                   uint16_t command_id,
                                   std::vector<CcEntryAddr> &cce_addr,
                                   CcHandlerResultBase *hres);

    /**
     * @brief Kickout the ccentrise whose commit_ts less than @@ckpt_ts from
     * ccmap.
     *
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param ng_id Id of the node group that to execute the request.
     */
    void KickoutData(uint32_t src_node_id,
                     TxNumber tx_number,
                     int64_t tx_term,
                     uint64_t command_id,
                     const TableName &table_name,
                     uint32_t ng_id,
                     txservice::CleanType clean_type,
                     const TxKey *start_key,
                     const TxKey *end_key,
                     CcHandlerResult<Void> &hres,
                     uint64_t clean_ts = 0);

    void ObjectCommand(uint32_t src_node_id,
                       NodeGroupId dest_ng_id,
                       const TableName &table_name,
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
                       bool commit);

    void PublishMessage(uint64_t ng_id,
                        int64_t tx_term,
                        std::string_view chan,
                        std::string_view message);

    void UploadTxCommands(uint32_t src_node_id,
                          uint64_t tx_number,
                          int64_t tx_term,
                          uint16_t command_id,
                          const CcEntryAddr &cce_addr,
                          uint64_t obj_version,
                          uint64_t commit_ts,
                          const std::vector<std::string> *cmd_list,
                          bool has_overwrite,
                          CcHandlerResult<PostProcessResult> &hres);

    void InvalidateTableCache(uint32_t src_node_id,
                              const TableName &table_name,
                              NodeGroupId ng_id,
                              TxNumber tx_number,
                              int64_t tx_term,
                              uint16_t command_id,
                              CcHandlerResult<Void> &hres);

private:
    CcStreamSender &stream_sender_;
};
}  // namespace remote

}  // namespace txservice
