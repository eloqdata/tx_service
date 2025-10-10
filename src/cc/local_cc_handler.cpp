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
#include "local_cc_handler.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <tuple>

#include "catalog_cc_map.h"
#include "cc_map.h"
#include "cc_protocol.h"
#include "ccm_scanner.h"
#include "error_messages.h"  //CcErrorCode
#include "local_cc_shards.h"
#include "remote/remote_cc_handler.h"
#include "sharder.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_trace.h"
#include "type.h"

txservice::LocalCcHandler::LocalCcHandler(uint32_t thd_id,
                                          LocalCcShards &shards)
    : thd_id_(thd_id),
      cc_shards_(shards),
      remote_hd_(*Sharder::Instance().GetCcStreamSender())
{
}

void txservice::LocalCcHandler::AcquireWrite(
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
    IsolationLevel iso_level)
{
    uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    AcquireKeyResult &acquire_result = hres.Value()[hd_res_idx];
    acquire_result.cce_addr_.SetNodeGroupId(ng_id);
    acquire_result.cce_addr_.SetCceLock(0, -1, 0);
    acquire_result.commit_ts_ = 0;
    acquire_result.last_vali_ts_ = 0;

#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        acquire_result.SetRemoteHdResult(true, std::memory_order_relaxed);
        AcquireCc *req = acquire_pool.NextRequest();
        req->Reset(&table_name,
                   schema_version,
                   &key,
                   key_shard_code,
                   tx_number,
                   tx_term,
                   ts,
                   is_insert,
                   &hres,
                   hd_res_idx,
                   proto,
                   iso_level);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);

        cc_shards_.EnqueueCcRequest(thd_id_, key_shard_code, req);
    }
    else
    {
        hres.IncreaseRemoteRef();
        acquire_result.remote_ack_cnt_->fetch_add(1, std::memory_order_relaxed);
        acquire_result.SetRemoteHdResult(false, std::memory_order_relaxed);
        remote_hd_.AcquireWrite(cc_shards_.node_id_,
                                ng_id,
                                table_name,
                                schema_version,
                                key,
                                key_shard_code,
                                tx_number,
                                tx_term,
                                command_id,
                                ts,
                                is_insert,
                                hres,
                                hd_res_idx,
                                proto,
                                iso_level);
    }
}

void txservice::LocalCcHandler::AcquireWriteAll(
    const TableName &table_name,
    const TxKey &key,
    NodeGroupId ng_id,
    TxNumber txn,
    int64_t tx_term,
    uint16_t command_id,
    bool is_insert,
    CcHandlerResult<AcquireAllResult> &hres,
    CcProtocol proto,
    CcOperation cc_op)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    hres.Value().local_cce_addr_.SetNodeGroupId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        hres.Value().remote_ack_cnt_ = nullptr;
        AcquireAllCc *req = acquire_all_pool_.NextRequest();
        req->Reset(&table_name,
                   &key,
                   ng_id,
                   txn,
                   tx_term,
                   is_insert,
                   &hres,
                   cc_shards_.Count(),
                   proto,
                   cc_op);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        // The request is dispatched to the first core and then passed to
        // remaining cores consecutively.
        cc_shards_.EnqueueCcRequest(thd_id_, 0, req);
    }
    else
    {
        hres.Value().remote_ack_cnt_->fetch_add(1, std::memory_order_acquire);
        remote_hd_.AcquireWriteAll(cc_shards_.node_id_,
                                   table_name,
                                   key,
                                   ng_id,
                                   txn,
                                   tx_term,
                                   command_id,
                                   is_insert,
                                   hres,
                                   proto,
                                   cc_op);
    }
}

void txservice::LocalCcHandler::PostWriteAll(
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
    PostWriteType post_write_type)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    uint32_t shard_code = tx_number >> 32L;
    uint32_t cc_ng_id = shard_code >> 10;
    if (dest_node_id == cc_shards_.node_id_)
    {
        PostWriteAllCc *req = postwrite_all_pool_.NextRequest();

        // When PostWriteAll is directed to leaders of two cc node groups in
        // same physical node, the two cc requests should reference their own
        // records. The record of a PostWriteAllCc has two roles: (1) upload a
        // serialized schema image, (2) return a pointer to the schema object
        // cached in the tx service.
        if (ng_id == cc_ng_id)
        {
            req->Reset(&table_name,
                       &key,
                       ng_id,
                       tx_number,
                       commit_ts,
                       &rec,
                       op_type,
                       &hres,
                       post_write_type,
                       tx_term);
        }
        else
        {
            std::unique_ptr<TxRecord> dup_rec = rec.Clone();
            req->Reset(&table_name,
                       &key,
                       ng_id,
                       tx_number,
                       commit_ts,
                       std::move(dup_rec),
                       op_type,
                       &hres,
                       post_write_type,
                       tx_term);
        }

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        // The request is dispatched to the first core and then passed to
        // remaining cores consecutively.
        cc_shards_.EnqueueCcRequest(thd_id_, 0, req);
    }
    else
    {
        hres.IncreaseRemoteRef();
        remote_hd_.PostWriteAll(cc_shards_.node_id_,
                                table_name,
                                key,
                                rec,
                                ng_id,
                                tx_number,
                                tx_term,
                                command_id,
                                commit_ts,
                                hres,
                                op_type,
                                post_write_type);
    }
}

txservice::CcReqStatus txservice::LocalCcHandler::PostWrite(
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    const TxRecord *record,
    OperationType operation_type,
    uint32_t key_shard_code,
    CcHandlerResult<PostProcessResult> &hres)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    CcReqStatus req_status{CcReqStatus::SentLocal};

#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    if (dest_node_id == cc_shards_.node_id_)
    {
        PostWriteCc *req = postwrite_pool.NextRequest();
        req->Reset(&cce_addr,
                   tx_number,
                   tx_term,
                   commit_ts,
                   record,
                   operation_type,
                   key_shard_code,
                   &hres);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, cce_addr.CoreId(), req);
    }
    else
    {
        hres.Value().is_local_ = false;
        hres.IncreaseRemoteRef();
        req_status = CcReqStatus::SentRemote;
        remote_hd_.PostWrite(cc_shards_.node_id_,
                             tx_number,
                             tx_term,
                             command_id,
                             commit_ts,
                             cce_addr,
                             record,
                             operation_type,
                             key_shard_code,
                             hres);
    }
    return req_status;
}

txservice::CcReqStatus txservice::LocalCcHandler::PostRead(
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t key_ts,
    uint64_t gap_ts,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<PostProcessResult> &hres,
    bool is_local,
    bool need_remote_resp)
{
    if (IsStandbyTx(tx_term))
    {
        if (Sharder::Instance().StandbyNodeTerm() != cce_addr.Term())
        {
            hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return CcReqStatus::Processed;
        }
        PostReadCc *req = postread_pool_.NextRequest();
        req->Reset(
            &cce_addr, tx_number, tx_term, commit_ts, key_ts, gap_ts, &hres);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
#ifdef EXT_TX_PROC_ENABLED
        hres.SetToBlock();
#endif
        cc_shards_.EnqueueCcRequest(thd_id_, cce_addr.CoreId(), req);
        return CcReqStatus::SentLocal;
    }

    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    CcReqStatus req_status{CcReqStatus::SentLocal};

    if (dest_node_id == cc_shards_.node_id_ || is_local)
    {
        PostReadCc *req = postread_pool_.NextRequest();
        req->Reset(
            &cce_addr, tx_number, tx_term, commit_ts, key_ts, gap_ts, &hres);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        if (thd_id_ == cce_addr.CoreId())
        {
            CcShard *ccs = cc_shards_.cc_shards_[thd_id_].get();
            bool finish = req->Execute(*ccs);
            if (finish)
            {
                req->Free();
                req_status = CcReqStatus::Processed;
            }
#ifdef EXT_TX_PROC_ENABLED
            else
            {
                hres.SetToBlock();
            }
#endif
        }
        else
        {
#ifdef EXT_TX_PROC_ENABLED
            hres.SetToBlock();
#endif
            cc_shards_.EnqueueCcRequest(thd_id_, cce_addr.CoreId(), req);
        }
    }
    else
    {
#ifdef EXT_TX_PROC_ENABLED
        hres.SetToBlock();
#endif
        hres.Value().is_local_ = false;
        hres.IncreaseRemoteRef();
        req_status = CcReqStatus::SentRemote;
        remote_hd_.PostRead(cc_shards_.node_id_,
                            tx_number,
                            tx_term,
                            command_id,
                            key_ts,
                            gap_ts,
                            commit_ts,
                            cce_addr,
                            hres,
                            need_remote_resp);
    }
    return req_status;
}

void txservice::LocalCcHandler::Read(const TableName &table_name,
                                     const uint64_t schema_version,
                                     const TxKey &key,
                                     uint32_t key_shard_code,
                                     TxRecord &record,
                                     ReadType read_type,
                                     uint64_t tx_number,
                                     int64_t tx_term,
                                     uint16_t command_id,
                                     const uint64_t ts,
                                     CcHandlerResult<ReadKeyResult> &hres,
                                     IsolationLevel iso_level,
                                     CcProtocol proto,
                                     bool is_for_write,
                                     bool is_covering_keys,
                                     bool point_read_on_miss,
                                     int32_t partition_id)
{
    hres.Value().rec_ = &record;
    uint32_t cc_ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    ReadKeyResult &read_result = hres.Value();
    CcEntryAddr &cce_addr = read_result.cce_addr_;
    cce_addr.SetNodeGroupId(cc_ng_id);
    cce_addr.SetCceLock(0, -1, 0);
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(cc_ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        read_result.is_local_ = true;

        ReadCc *req = read_pool.NextRequest();
        req->Reset(&table_name,
                   schema_version,
                   &key,
                   key_shard_code,
                   &record,
                   read_type,
                   tx_number,
                   tx_term,
                   ts,
                   &hres,
                   iso_level,
                   proto,
                   is_for_write,
                   is_covering_keys,
                   nullptr,
                   false,
                   point_read_on_miss,
                   partition_id);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, key_shard_code, req);
    }
    else
    {
        read_result.is_local_ = false;

        remote_hd_.Read(cc_shards_.node_id_,
                        cc_ng_id,
                        table_name,
                        schema_version,
                        key,
                        key_shard_code,
                        record,
                        read_type,
                        tx_number,
                        tx_term,
                        command_id,
                        ts,
                        hres,
                        iso_level,
                        proto,
                        is_for_write,
                        is_covering_keys,
                        point_read_on_miss,
                        partition_id);
    }
}

/*
 * ReadOutside fills the tuple read from KV into cache.
 */
void txservice::LocalCcHandler::ReadOutside(
    int64_t tx_term,
    uint16_t command_id,
    TxRecord &rec,
    bool is_deleted,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<ReadKeyResult> &hres,
    std::vector<VersionTxRecord> *archives)
{
    assert(cce_addr.CceLockPtr() != 0);

    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        ReadType read_type =
            is_deleted ? ReadType::OutsideDeleted : ReadType::OutsideNormal;

        hres.Value().cce_addr_.SetCceLock(cce_addr.CceLockPtr(),
                                          cce_addr.Term(),
                                          cce_addr.NodeGroupId(),
                                          cce_addr.CoreId());

        ReadCc *req = read_pool.NextRequest();
        // A read-outside request brings a record into the cc map for caching.
        // Its tx number is meaningless: it does not represent the tx committing
        // the record. Thus, it is set to 0. The commit timestamp is set to 1,
        // the beginning of history. Once a record is flushed to the data store
        // and kicked out from the cc map, the record history only exists in the
        // log. We pretend the record exists since the beginning of history,
        // which is good enough for the concurrency control purpose. The
        // isolation level is set to read committed, so that the request leaves
        // no read intention or lock on the cc entry.
        req->Reset(nullptr,
                   0,
                   nullptr,
                   ng_id << 10,
                   &rec,
                   read_type,
                   0,
                   tx_term,
                   commit_ts,
                   &hres,
                   IsolationLevel::ReadCommitted,
                   CcProtocol::OCC,
                   false,
                   archives);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
#ifdef EXT_TX_PROC_ENABLED
        hres.SetToBlock();
#endif
        cc_shards_.EnqueueCcRequest(thd_id_, cce_addr.CoreId(), req);
    }
    else
    {
        remote_hd_.ReadOutside(
            tx_term, command_id, rec, is_deleted, commit_ts, cce_addr);
        // we don't care whether the remote request succeeds or not,
        // since it's just a fill of cache.
        hres.Value().rec_status_ = RecordStatus::RemoteUnknown;
        hres.SetFinished();
    }
}

bool txservice::LocalCcHandler::ReadLocal(const TableName &table_name,
                                          const TxKey &key,
                                          TxRecord &record,
                                          ReadType read_type,
                                          uint64_t tx_number,
                                          int64_t tx_term,
                                          uint16_t command_id,
                                          const uint64_t ts,
                                          CcHandlerResult<ReadKeyResult> &hres,
                                          IsolationLevel iso_level,
                                          CcProtocol proto,
                                          bool is_for_write,
                                          bool is_recovering,
                                          bool execute_immediately)
{
    ReadKeyResult &read_result = hres.Value();
    read_result.rec_ = &record;
    read_result.rec_status_ = RecordStatus::Unknown;
    read_result.ts_ = 0;
    read_result.is_local_ = true;
    CcEntryAddr &cce_addr = read_result.cce_addr_;

    CcShard *ccs;
    if (table_name == cluster_config_ccm_name)
    {
        // cluster config map is only initialized on core 0. If we're
        // visiting cluster config ccm, we need to send a regular read
        // req to another core.
        ccs = cc_shards_.cc_shards_[0].get();
    }
    else
    {
        ccs = cc_shards_.cc_shards_[thd_id_].get();
    }
    int64_t term;
    uint32_t shard_code = tx_number >> 32L;
    uint32_t cc_ng_id = shard_code >> 10;
    if (is_recovering)
    {
        term = Sharder::Instance().CandidateLeaderTerm(cc_ng_id);
    }
    else
    {
        int64_t ng_leader_term = Sharder::Instance().LeaderTerm(cc_ng_id);
        int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
        term = std::max(ng_leader_term, standby_node_term);
    }
    cce_addr.SetNodeGroupId(cc_ng_id);
    cce_addr.SetCceLock(0, term, 0);

    if (term < 0)
    {
        // When a tx starts, the tx can only be bound to a native cc node who is
        // the leader. Since a read local request is dispatched to the same
        // shard to which the tx is bound, if the native cc node is not the
        // leader now, returns an error.
        hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        return true;
    }

    // Standby transaction only execute local request.
    if (IsStandbyTx(tx_term) && is_for_write)
    {
        // redis smart client needs to resend this command to primary node
        hres.SetError(CcErrorCode::DATA_NOT_ON_LOCAL_NODE);
        return true;
    }

    ReadCc *read_req = read_pool.NextRequest();
    read_req->Reset(&table_name,
                    0,
                    &key,
                    shard_code,
                    &record,
                    read_type,
                    tx_number,
                    tx_term,
                    ts,
                    &hres,
                    iso_level,
                    proto,
                    is_for_write,
                    false,
                    nullptr,
                    is_recovering);
    TX_TRACE_ACTION(this, read_req);
    TX_TRACE_DUMP(read_req);

    CcMap *ccm = ccs->GetCcm(table_name, cc_ng_id);
    bool finished = false;

    if (ccm != nullptr && thd_id_ == ccs->core_id_ && execute_immediately)
    {
        //__catalog table will be preloaded when ccshard constructed
        finished = ccm->Execute(*read_req);
        if (finished)
        {
            read_req->Free();
        }
#ifdef EXT_TX_PROC_ENABLED
        else
        {
            hres.SetToBlock();
        }
#endif
    }
    else
    {
#ifdef EXT_TX_PROC_ENABLED
        hres.SetToBlock();
#endif
        // otherwise, let the TemplateCcRequest load in the data
        ccs->Enqueue(read_req);
    }

    return finished;
}

std::tuple<txservice::CcErrorCode, txservice::NonBlockingLock *, uint64_t>
txservice::LocalCcHandler::ReadCatalog(const TableName &table_name,
                                       uint32_t ng_id,
                                       int64_t ng_term,
                                       TxNumber tx_number) const
{
    CcShard *shard_ = cc_shards_.cc_shards_[thd_id_].get();
    const TableName &catalog_table_name = txservice::catalog_ccm_name;
    CcMap *ccm = shard_->GetCcm(catalog_table_name, ng_id);
    assert(ccm != nullptr);
    CatalogCcMap *catalog_ccm = reinterpret_cast<CatalogCcMap *>(ccm);

    return catalog_ccm->ReadTable(table_name, ng_id, ng_term, tx_number);
}

bool txservice::LocalCcHandler::ReleaseCatalogRead(NonBlockingLock *lock,
                                                   TxNumber tx_number) const
{
    CcShard *shard_ = cc_shards_.cc_shards_[thd_id_].get();
    return lock->ReleaseReadLockFast(shard_, tx_number);
}

bool txservice::LocalCcHandler::ReadLocal(const TableName &table_name,
                                          const std::string &key_str,
                                          TxRecord &record,
                                          ReadType read_type,
                                          uint64_t tx_number,
                                          int64_t tx_term,
                                          uint16_t command_id,
                                          const uint64_t ts,
                                          CcHandlerResult<ReadKeyResult> &hres,
                                          IsolationLevel iso_level,
                                          CcProtocol proto,
                                          bool is_for_write,
                                          bool is_recovering)
{
    ReadKeyResult &read_result = hres.Value();
    read_result.rec_ = &record;
    read_result.rec_status_ = RecordStatus::Unknown;
    read_result.ts_ = 0;
    read_result.is_local_ = true;
    CcEntryAddr &cce_addr = read_result.cce_addr_;

    CcShard *ccs;
    if (table_name == cluster_config_ccm_name)
    {
        // cluster config map is only initialized on core 0. If we're
        // visiting cluster config ccm, we need to send a regular read
        // req to another core.
        ccs = cc_shards_.cc_shards_[0].get();
    }
    else
    {
        ccs = cc_shards_.cc_shards_[thd_id_].get();
    }
    int64_t term = -1;
    uint32_t shard_code = tx_number >> 32L;
    uint32_t cc_ng_id = shard_code >> 10;
    if (is_recovering)
    {
        term = Sharder::Instance().CandidateLeaderTerm(cc_ng_id);
    }

    if (term < 0)
    {
        int64_t ng_leader_term = Sharder::Instance().LeaderTerm(cc_ng_id);
        int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
        term = std::max(ng_leader_term, standby_node_term);
    }
    cce_addr.SetNodeGroupId(cc_ng_id);
    cce_addr.SetCceLock(0, term, 0);

    if (term < 0)
    {
        // When a tx starts, the tx can only be bound to a native cc node who is
        // the leader. Since a read local request is dispatched to the same
        // shard to which the tx is bound, if the native cc node is not the
        // leader now, returns an error.
        hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        return true;
    }

    ReadCc *read_req = read_pool.NextRequest();
    read_req->Reset(&table_name,
                    0,
                    key_str,
                    shard_code,
                    &record,
                    read_type,
                    tx_number,
                    tx_term,
                    ts,
                    &hres,
                    iso_level,
                    proto,
                    is_for_write,
                    false,
                    nullptr);
    TX_TRACE_ACTION(this, read_req);
    TX_TRACE_DUMP(read_req);

    CcMap *ccm = ccs->GetCcm(table_name, cc_ng_id);
    bool finished = false;

    if (ccm != nullptr && thd_id_ == ccs->core_id_)
    {
        //__catalog table will be preloaded when ccshard constructed
        finished = ccm->Execute(*read_req);
        if (finished)
        {
            read_req->Free();
        }
#ifdef EXT_TX_PROC_ENABLED
        else
        {
            hres.SetToBlock();
        }
#endif
    }
    else
    {
#ifdef EXT_TX_PROC_ENABLED
        hres.SetToBlock();
#endif
        // otherwise, let the TemplateCcRequest load in the data
        ccs->Enqueue(read_req);
    }

    return finished;
}

void txservice::LocalCcHandler::ScanOpen(
    const TableName &table_name,
    const uint64_t schema_version,
    ScanIndexType index_type,
    const TxKey &start_key,
    bool inclusive,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t ts,
    CcHandlerResult<ScanOpenResult> &hd_res,
    ScanDirection direction,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_for_write,
    bool is_ckpt_delta,
    bool is_covering_keys,
    bool is_require_keys,
    bool is_require_recs,
    bool is_require_sort,
    int32_t obj_type,
    const std::string_view &scan_pattern)
{
    CcShard &local_shard = *cc_shards_.cc_shards_[thd_id_];
    uint32_t shard_code = tx_number >> 32L;
    uint32_t cc_ng_id = shard_code >> 10;
    bool is_standby_tx = IsStandbyTx(tx_term);

    if (is_standby_tx)
    {
        // Standby node does not communicate with other node groups so
        // scan on standby node only works if there's only 1 node group.
        // Standby node does not support scan for write.
        if (Sharder::Instance().NodeGroupCount() > 1 || is_for_write)
        {
            hd_res.SetError(CcErrorCode::DATA_NOT_ON_LOCAL_NODE);
            return;
        }
    }

    std::unique_ptr<CcScanner> ccm_scanner = nullptr;
    if (table_name.Type() == TableType::Secondary ||
        table_name.Type() == TableType::UniqueSecondary)
    {
        const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                        TableType::Primary,
                                        table_name.Engine()};
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(base_table_name, cc_ng_id);

        if (catalog_entry == nullptr || catalog_entry->schema_ == nullptr)
        {
            hd_res.SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
            return;
        }

        const SecondaryKeySchema *index_key_schema =
            catalog_entry->schema_->IndexKeySchema(table_name);
        if (index_key_schema == nullptr)
        {
            hd_res.SetError(CcErrorCode::REQUESTED_INDEX_TABLE_NOT_EXISTS);
            return;
        }

        if (direction == ScanDirection::Forward &&
            sk_forward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                    .Size() > 0)
        {
            ccm_scanner = std::move(
                sk_forward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                    .Peek());
            sk_forward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                .Dequeue();
            ccm_scanner->Reset(index_key_schema);
        }
        else if (direction == ScanDirection::Backward &&
                 sk_backward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                         .Size() > 0)
        {
            ccm_scanner = std::move(
                sk_backward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                    .Peek());
            sk_backward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                .Dequeue();
            ccm_scanner->Reset(index_key_schema);
        }
        else
        {
            ccm_scanner = local_shard.GetCatalogFactory(table_name.Engine())
                              ->CreateSkCcmScanner(direction, index_key_schema);
        }
    }
    else
    {
        const KeySchema *key_schema = nullptr;
        // We don't need key schema for eloqkv for serialize/deserialize.
        if (table_name.Engine() == TableEngine::EloqKv)
        {
            const CatalogEntry *catalog_entry =
                local_shard.GetCatalog(table_name, cc_ng_id);

            if (catalog_entry == nullptr || catalog_entry->schema_ == nullptr)
            {
                hd_res.SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                return;
            }

            key_schema = catalog_entry->schema_->KeySchema();
        }
        if (direction == ScanDirection::Forward &&
            pk_forward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                    .Size() > 0)
        {
            ccm_scanner = std::move(
                pk_forward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                    .Peek());
            pk_forward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                .Dequeue();
            ccm_scanner->Reset(key_schema);
        }
        else if (direction == ScanDirection::Backward &&
                 pk_backward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                         .Size() > 0)
        {
            ccm_scanner = std::move(
                pk_backward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                    .Peek());
            pk_backward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                .Dequeue();
            ccm_scanner->Reset(key_schema);
        }
        else
        {
            ccm_scanner = local_shard.GetCatalogFactory(table_name.Engine())
                              ->CreatePkCcmScanner(direction, key_schema);
        }
    }

    ScanOpenResult &open_result = hd_res.Value();

    open_result.scanner_ = std::move(ccm_scanner);
    CcScanner *scanner_ptr = open_result.scanner_.get();
    assert(open_result.scan_alias_ < UINT64_MAX);
    scanner_ptr->SetStatus(ScannerStatus::Open);
    // scanner_ptr->SetDrainCacheMode(false);
    scanner_ptr->is_ckpt_delta_ = is_ckpt_delta;
    scanner_ptr->is_for_write_ = is_for_write;
    scanner_ptr->is_covering_keys_ = is_covering_keys;
    scanner_ptr->is_require_keys_ = is_require_keys;
    scanner_ptr->is_require_recs_ = is_require_recs;
    scanner_ptr->is_require_sort_ = is_require_sort;
    scanner_ptr->iso_level_ = iso_level;
    scanner_ptr->protocol_ = proto;
    scanner_ptr->read_local_ = false;
    scanner_ptr->cc_op_ =
        CcScanner::DeduceCcOperation(scanner_ptr->IndexType(), is_for_write);
    scanner_ptr->lock_type_ = LockTypeUtil::DeduceLockType(
        scanner_ptr->cc_op_, iso_level, proto, is_covering_keys);

    // Actual scan is done in scan next.
    hd_res.SetFinished();
    return;
}

void txservice::LocalCcHandler::ScanOpenLocal(
    const TableName &table_name,
    ScanIndexType index_type,
    const TxKey &start_key,
    bool inclusive,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t ts,
    CcHandlerResult<ScanOpenResult> &hd_res,
    ScanDirection direction,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_for_write,
    bool is_ckpt_delta)
{
    // TODO: consolidate these kind term check in some common place
    if (tx_term < 0)
    {
        // When a tx starts, the tx can only be bound to a native cc node who is
        // the leader. Since a read local request is dispatched to the same
        // shard to which the tx is bound, if the native cc node is not the
        // leader now, returns an error.
        hd_res.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
        return;
    }

    CcShard &local_shard = *cc_shards_.cc_shards_.at(thd_id_);
    uint32_t shard_code = tx_number >> 32L;
    uint32_t cc_ng_id = shard_code >> 10;

    const KeySchema *schema = nullptr;
    std::unique_ptr<CcScanner> ccm_scanner = nullptr;
    if (table_name.Type() == TableType::RangePartition)
    {
        const TableName base_table_name{
            table_name.StringView(), TableType::Primary, table_name.Engine()};
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(base_table_name, cc_ng_id);
        if (catalog_entry != nullptr && catalog_entry->schema_ != nullptr)
        {
            schema = catalog_entry->schema_.get()->KeySchema();
        }

        ccm_scanner =
            local_shard.GetCatalogFactory(table_name.Engine())
                ->CreateRangeCcmScanner(direction, schema, table_name);
    }
    else if (table_name.Type() == TableType::Secondary ||
             table_name.Type() == TableType::UniqueSecondary)
    {
        const TableName base_table_name{
            table_name.StringView(), TableType::Primary, table_name.Engine()};
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(base_table_name, cc_ng_id);
        if (catalog_entry != nullptr && catalog_entry->schema_ != nullptr)
        {
            schema = catalog_entry->schema_.get()->IndexKeySchema(table_name);
        }
        ccm_scanner = local_shard.GetCatalogFactory(table_name.Engine())
                          ->CreateSkCcmScanner(direction, schema);
    }
    else
    {
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(table_name, cc_ng_id);

        if (catalog_entry != nullptr && catalog_entry->schema_ != nullptr)
        {
            schema = catalog_entry->schema_.get()->KeySchema();
        }

        ccm_scanner = local_shard.GetCatalogFactory(table_name.Engine())
                          ->CreatePkCcmScanner(direction, schema);
    }

    if (ccm_scanner == nullptr)
    {
        hd_res.SetError(CcErrorCode::CREATE_CCM_SCANNER_FAILED);
        return;
    }

    ScanOpenResult &open_result = hd_res.Value();
    std::set<NodeGroupId> node_groups;
    node_groups.emplace(Sharder::Instance().NativeNodeGroup());
    open_result.Reset(node_groups);

    open_result.scanner_ = std::move(ccm_scanner);
    CcScanner *scanner_ptr = open_result.scanner_.get();
    assert(open_result.scan_alias_ < UINT64_MAX);
    scanner_ptr->SetStatus(ScannerStatus::Open);
    // scanner_ptr->SetDrainCacheMode(false);
    scanner_ptr->is_ckpt_delta_ = is_ckpt_delta;
    scanner_ptr->is_for_write_ = is_for_write;
    scanner_ptr->iso_level_ = iso_level;
    scanner_ptr->protocol_ = proto;
    scanner_ptr->read_local_ = true;
    scanner_ptr->cc_op_ =
        CcScanner::DeduceCcOperation(scanner_ptr->IndexType(), is_for_write);
    scanner_ptr->lock_type_ = LockTypeUtil::DeduceLockType(
        scanner_ptr->cc_op_, iso_level, proto, false);

    hd_res.SetFinished();
    return;
}

void txservice::LocalCcHandler::ScanNextBatch(
    const TableName &table_name,
    NodeGroupId node_group_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t start_ts,
    const TxKey &start_key,
    bool start_inclusive,
    const TxKey &end_key,
    bool end_inclusive,
    CcScanner &scanner,
    const std::vector<DataStoreSearchCond> *pushdown_cond,
    CcHandlerResult<ScanNextResult> &hd_res,
    int32_t obj_type,
    const std::string_view &scan_pattern)
{
    bool is_standby_tx = IsStandbyTx(tx_term);
#ifdef EXT_TX_PROC_ENABLED
    hd_res.SetToBlock();
#endif

    BucketScanPlan *plan = hd_res.Value().current_scan_plan_;
    bool is_local_req =
        Sharder::Instance().LeaderNodeId(node_group_id) == cc_shards_.node_id_;
    if (scanner.read_local_ && !is_local_req)
    {
        hd_res.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        return;
    }

    if (is_standby_tx || is_local_req)
    {
        assert(plan->Buckets().count(node_group_id) > 0);

        absl::flat_hash_map<uint16_t, BucketScanProgress>
            *bucket_scan_progress = plan->GetBucketScanProgress(node_group_id);
        if (bucket_scan_progress->empty())
        {
            for (const auto &bucket_id : *plan->Buckets(node_group_id))
            {
                uint16_t target_core =
                    Sharder::Instance().ShardBucketIdToCoreIdx(bucket_id);

                auto iter = bucket_scan_progress->find(target_core);
                if (iter != bucket_scan_progress->end())
                {
                    iter->second.scan_buckets_.emplace(bucket_id, false);
                }
                else
                {
                    auto em_it = bucket_scan_progress->try_emplace(
                        target_core, start_key.Clone(), start_inclusive);
                    em_it.first->second.scan_buckets_.emplace(bucket_id, false);
                }
            }
        }

        ScanNextBatchCc *req = scan_next_pool.NextRequest();
        req->Reset(table_name,
                   node_group_id,
                   plan->GetNodeGroupTerm(node_group_id),
                   tx_number,
                   start_ts,
                   end_key,
                   end_inclusive,
                   plan,
                   pushdown_cond,
                   tx_term,
                   &hd_res,
                   scanner.iso_level_,
                   scanner.protocol_,
                   scanner.is_for_write_,
                   scanner.is_ckpt_delta_,
                   scanner.is_covering_keys_,
                   scanner.is_require_keys_,
                   scanner.is_require_recs_,
                   obj_type,
                   scan_pattern);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);

        for (const auto &[core_idx, scan_progress] :
             *req->GetBucketScanProgress())
        {
            cc_shards_.EnqueueCcRequest(thd_id_, core_idx, req);
        }
    }
    else
    {
        hd_res.IncreaseRemoteRef();
        remote_hd_.ScanNext(cc_shards_.node_id_,
                            node_group_id,
                            plan->GetNodeGroupTerm(node_group_id),
                            tx_number,
                            tx_term,
                            command_id,
                            start_ts,
                            table_name,
                            start_key,
                            start_inclusive,
                            end_key,
                            end_inclusive,
                            *plan->Buckets(node_group_id),
                            *plan->GetBucketScanProgress(node_group_id),
                            hd_res,
                            scanner.iso_level_,
                            scanner.protocol_,
                            scanner.is_for_write_,
                            scanner.is_ckpt_delta_,
                            scanner.is_covering_keys_,
                            scanner.is_require_keys_,
                            scanner.is_require_recs_,
                            obj_type,
                            scan_pattern);
    }
}

void txservice::LocalCcHandler::ScanNextBatch(
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
    IsolationLevel iso_level,
    CcProtocol proto)
{
#ifdef EXT_TX_PROC_ENABLED
    hd_res.SetToBlock();
#endif
    hd_res.Value().cc_ng_id_ = range_owner;
    CcScanner &scanner = *hd_res.Value().ccm_scanner_;

    uint32_t node_id = Sharder::Instance().LeaderNodeId(range_owner);
    if (node_id == cc_shards_.node_id_)
    {
        hd_res.Value().is_local_ = true;

        ScanSliceCc *req = scan_slice_pool.NextRequest();
        req->Set(tbl_name,
                 schema_version,
                 range_id,
                 range_owner,
                 cc_ng_term,
                 start_key,
                 start_inclusive,
                 end_key,
                 end_inclusive,
                 read_ts,
                 tx_number,
                 tx_term,
                 hd_res,
                 iso_level,
                 proto,
                 scanner.is_for_write_,
                 scanner.is_covering_keys_,
                 scanner.is_require_keys_,
                 scanner.is_require_recs_,
                 prefetch_size);

        uint32_t core_cnt = cc_shards_.Count();
        req->SetShardCount(core_cnt);

        // When the cc ng term is less than 0, this is the first scan of the
        // specified range.
        if (cc_ng_term < 0)
        {
            scanner.ResetShards(core_cnt);
        }

        for (uint32_t core_id = 0; core_id < core_cnt; ++core_id)
        {
            ScanCache *cache = scanner.Cache(core_id);
            const ScanTuple *last_tuple = cache->LastTuple();

            req->SetPriorCceLockAddr(
                last_tuple != nullptr ? last_tuple->cce_addr_.CceLockPtr() : 0,
                core_id);
        }

        scanner.ResetCaches();

        uint32_t core_rand = butil::fast_rand();

        // The scan slice request is dispatched to the first core. The first
        // core tries to pin the slice in memory and if succeeds, further
        // dispatches the request to remaining cores for parallel scans.
        cc_shards_.EnqueueCcRequest(thd_id_, core_rand % core_cnt, req);
    }
    else
    {
        hd_res.Value().is_local_ = false;
        remote_hd_.ScanNext(cc_shards_.node_id_,
                            tbl_name,
                            schema_version,
                            range_id,
                            range_owner,
                            cc_ng_term,
                            start_key,
                            start_inclusive,
                            end_key,
                            end_inclusive,
                            prefetch_size,
                            read_ts,
                            tx_number,
                            tx_term,
                            command_id,
                            hd_res,
                            iso_level,
                            proto);
    }
}

void txservice::LocalCcHandler::ScanClose(const TableName &table_name,
                                          ScanDirection direction,
                                          std::unique_ptr<CcScanner> scanner)
{
    assert(scanner->Direction() == direction);

    scanner->Close();

    if (table_name.Type() == TableType::Primary)
    {
        assert(scanner->IndexType() == ScanIndexType::Primary);

        if (direction == ScanDirection::Forward)
        {
            pk_forward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                .Enqueue(std::move(scanner));
        }
        else
        {
            pk_backward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                .Enqueue(std::move(scanner));
        }
    }
    else if (table_name.Type() == TableType::Secondary)
    {
        assert(scanner->IndexType() == ScanIndexType::Secondary);

        if (direction == ScanDirection::Forward)
        {
            sk_forward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                .Enqueue(std::move(scanner));
        }
        else
        {
            sk_backward_scanner_[static_cast<int>(table_name.Engine()) - 1]
                .Enqueue(std::move(scanner));
        }
    }
}

void txservice::LocalCcHandler::NewTxn(CcHandlerResult<InitTxResult> &hres,
                                       IsolationLevel iso_level,
                                       NodeGroupId tx_ng_id,
                                       uint32_t log_group_id)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);

    int64_t term = Sharder::Instance().LeaderTerm(tx_ng_id);
    if (term < 0)
    {
        term = Sharder::Instance().StandbyNodeTerm();
    }

    // Code injection for test InitTxRequest failure
    CODE_FAULT_INJECTOR("init_tx_error", {
        // This injection just run once
        term = -2;
    });

    if (term >= 0)
    {
        // NewTx reads each ccshard's next_tx_ident_, which is set concurrently
        // by log replay thread when native cc node finishes log replay. The two
        // events are synchronized by leader_term_.
        TEntry &tx = ccs.NewTx(tx_ng_id, log_group_id, term);
        InitTxResult &init_tx_res = hres.Value();
        init_tx_res.txid_ = tx.GetTxId(ccs.GlobalCoreId(tx_ng_id));
        TxNumber txn = init_tx_res.txid_.TxNumber();
        init_tx_res.start_ts_ = tx.lower_bound_;
        init_tx_res.term_ = tx.term_;
        hres.SetFinished();

        // Update active tx info
        if (iso_level == IsolationLevel::Snapshot)
        {
            ccs.AddActiveSiTx(txn, tx.lower_bound_);
        }
    }
    else
    {
        // When the native cc node is not the leader, we could try to
        // find another cc node group whose leader is in this node and
        // bind the new tx to it. However, since this node is not the
        // preferred leader of that cc node group, it is very likely
        // that that leader will be transferred soon. For simplicity, we stop
        // creating new tx's if the native node is not the leader for now.
        hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
    }
}

void txservice::LocalCcHandler::SetCommitTimestamp(
    const TxId &txid, uint64_t commit_ts, CcHandlerResult<uint64_t> &hres)
{
    CcShard &ccs = *cc_shards_.cc_shards_[thd_id_];
    TEntry &tx = ccs.tx_vec_.at(txid.vec_idx_);
    assert(tx.ident_ == txid.ident_);
    uint64_t local_ts = ccs.Now();
    tx.commit_ts_ = std::max(local_ts, std::max(tx.lower_bound_, commit_ts));

    // The thread-local timer is monotonically increasing. If the
    // comparison fails, the timer must have been advanced by the
    // machine clock and the newest time must be greater than
    // tx.commit_ts_ + 1.
    ccs.UpdateTsBase(std::max(local_ts, tx.commit_ts_ + 1));

    hres.SetValue(std::move(tx.commit_ts_));
    hres.SetFinished();
}

void txservice::LocalCcHandler::UpdateCommitLowerBound(
    const TxId &txid,
    uint64_t commit_ts_lower_bound,
    CcHandlerResult<uint64_t> &hres)
{
    NegotiateCc *req = negoti_pool.NextRequest();
    req->Reset(&txid, commit_ts_lower_bound, &hres);
    TX_TRACE_ACTION(this, req);
    TX_TRACE_DUMP(req);
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    // The lower 10 bits represent the local core Id. The remaining high
    // bits represent the node Id.
    uint16_t local_core_id = txid.global_core_id_ & 0x3FF;
    cc_shards_.EnqueueCcRequest(thd_id_, local_core_id, req);
}

void txservice::LocalCcHandler::UpdateTxnStatus(const TxId &txid,
                                                IsolationLevel iso_level,
                                                TxnStatus status,
                                                CcHandlerResult<Void> &hres)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);
    TEntry &te = ccs.tx_vec_.at(txid.vec_idx_);
    assert(te.ident_ == txid.ident_);
    te.status_ = status;
    hres.SetFinished();

    // Update active tx info
    if (iso_level == IsolationLevel::Snapshot)
    {
        ccs.RemoveActiveSiTx(txid.TxNumber());
    }
}

void txservice::LocalCcHandler::ReloadCache(NodeGroupId ng_id,
                                            TxNumber tx_number,
                                            int64_t tx_term,
                                            uint16_t command_id,
                                            CcHandlerResult<Void> &hres)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.NodeId())
    {
        hres.SetFinished();  // Skip local node.
    }
    else
    {
        hres.IncreaseRemoteRef();
        remote_hd_.ReloadCache(
            cc_shards_.node_id_, ng_id, tx_number, tx_term, command_id, hres);
    }
}

void txservice::LocalCcHandler::FaultInject(const std::string &fault_name,
                                            const std::string &fault_paras,
                                            int64_t tx_term,
                                            uint16_t command_id,
                                            const TxId &txid,
                                            std::vector<int> &vct_node_id,
                                            CcHandlerResult<bool> &hres)
{
    if (vct_node_id.size() == 0)
        vct_node_id.push_back(cc_shards_.node_id_);
    else if (vct_node_id[0] == -1)
    {
        vct_node_id.clear();
        auto all_nodes_sptr = Sharder::Instance().GetAllNodesConfigs();
        for (auto &[nid, _] : *all_nodes_sptr)
        {
            vct_node_id.push_back(nid);
        }
    }

    hres.SetRefCnt(vct_node_id.size());
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    for (int dest_node_id : vct_node_id)
    {
        if (dest_node_id == (int) cc_shards_.node_id_)
        {
            FaultInjectCC *req = fault_inject_pool.NextRequest();
            req->Reset(&fault_name, &fault_paras, &hres);
            TX_TRACE_ACTION(this, req);
            TX_TRACE_DUMP(req);
            cc_shards_.EnqueueCcRequest(0, req);
        }
        else
        {
            remote_hd_.FaultInject(cc_shards_.node_id_,
                                   fault_name,
                                   fault_paras,
                                   tx_term,
                                   command_id,
                                   txid,
                                   dest_node_id,
                                   hres);
        }
    }
}

void txservice::LocalCcHandler::DataStoreUpsertTable(
    const TableSchema *old_schema,
    const TableSchema *schema,
    OperationType op_type,
    uint64_t write_time,
    NodeGroupId ng_id,
    int64_t tx_term,
    CcHandlerResult<Void> &hres,
    const txservice::AlterTableInfo *alter_table_info)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif

    CODE_FAULT_INJECTOR("trigger_flush_kv_error", {
        hres.SetError(CcErrorCode::DATA_STORE_ERR);
        return;
    });

    ACTION_FAULT_INJECTOR("kv_upsert_table");

    cc_shards_.store_hd_->UpsertTable(old_schema,
                                      schema,
                                      op_type,
                                      write_time,
                                      ng_id,
                                      tx_term,
                                      &hres,
                                      alter_table_info);
}

void txservice::LocalCcHandler::AnalyzeTableAll(const TableName &table_name,
                                                NodeGroupId ng_id,
                                                TxNumber tx_number,
                                                int64_t tx_term,
                                                uint16_t command_id,
                                                CcHandlerResult<Void> &hres)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.NodeId())
    {
        AnalyzeTableAllCc *req = analyze_table_all_pool.NextRequest();
        req->Reset(&table_name, ng_id, tx_number, tx_term, &hres);
        cc_shards_.EnqueueCcRequest(thd_id_, 0, req);
    }
    else
    {
        hres.IncreaseRemoteRef();
        remote_hd_.AnalyzeTableAll(cc_shards_.node_id_,
                                   table_name,
                                   ng_id,
                                   tx_number,
                                   tx_term,
                                   command_id,
                                   hres);
    }
}

void txservice::LocalCcHandler::BroadcastStatistics(
    const TableName &table_name,
    uint64_t schema_ts,
    const remote::NodeGroupSamplePool &sample_pool,
    NodeGroupId ng_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    CcHandlerResult<Void> &hres)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif

    DLOG(INFO) << "Broadcast table statistics " << table_name.StringView()
               << ". From ng #" << sample_pool.ng_id() << ", to ng #" << ng_id
               << ". ng_records: " << sample_pool.records()
               << ", ng_samples: " << sample_pool.samples_size();

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.NodeId())
    {
        BroadcastStatisticsCc *req = broadcast_stat_pool.NextRequest();
        req->Reset(ng_id,
                   &table_name,
                   schema_ts,
                   sample_pool,
                   tx_number,
                   tx_term,
                   &hres);
        cc_shards_.EnqueueCcRequest(thd_id_, 0, req);
    }
    else
    {
        hres.IncreaseRemoteRef();
        remote_hd_.BroadcastStatistics(cc_shards_.node_id_,
                                       table_name,
                                       schema_ts,
                                       sample_pool,
                                       ng_id,
                                       tx_number,
                                       tx_term,
                                       command_id,
                                       hres);
    }
}

void txservice::LocalCcHandler::ObjectCommand(
    const txservice::TableName &table_name,
    const uint64_t schema_version,
    const txservice::TxKey &key,
    uint32_t key_shard_code,
    txservice::TxCommand &obj_cmd,
    txservice::TxNumber txn,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t tx_ts,
    txservice::CcHandlerResult<txservice::ObjectCommandResult> &hres,
    IsolationLevel iso_level,
    txservice::CcProtocol proto,
    bool commit,
    bool always_redirect)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    hres.Value().cce_addr_.SetCceLock(0, -1, ng_id, 0);

    bool is_standby_tx = IsStandbyTx(tx_term);
    // Standby transaction only execute local request.
    if (is_standby_tx)
    {
        if (Sharder::Instance().NativeNodeGroup() != ng_id)
        {
            // the standby node isn't allow to communicate with the master node
            // of any other node group
            hres.SetError(CcErrorCode::DATA_NOT_ON_LOCAL_NODE);
            return;
        }

        if (!obj_cmd.IsReadOnly())
        {
            // redis smart client needs to resend this command to primary node
            DLOG(WARNING) << "!!! DATA_NOT_ON_LOCAL_NODE !!";
            hres.SetError(CcErrorCode::DATA_NOT_ON_LOCAL_NODE);
            return;
        }

        ApplyCc *req = apply_pool.NextRequest();
        req->Reset(&table_name,
                   schema_version,
                   &key,
                   key_shard_code,
                   &obj_cmd,
                   nullptr,
                   txn,
                   tx_term,
                   tx_ts,
                   &hres,
                   proto,
                   iso_level,
                   commit);
        cc_shards_.EnqueueCcRequest(thd_id_, key_shard_code, req);
        return;
    }

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        ApplyCc *req = apply_pool.NextRequest();
        req->Reset(&table_name,
                   schema_version,
                   &key,
                   key_shard_code,
                   &obj_cmd,
                   nullptr,
                   txn,
                   tx_term,
                   tx_ts,
                   &hres,
                   proto,
                   iso_level,
                   commit);
        cc_shards_.EnqueueCcRequest(thd_id_, key_shard_code, req);
    }
    else
    {
        if (!txservice_auto_redirect_redis_cmd && !always_redirect)
        {
            DLOG(WARNING) << "!!! DATA_NOT_ON_LOCAL_NODE !!";
            hres.SetError(CcErrorCode::DATA_NOT_ON_LOCAL_NODE);
            return;
        }
        // set "cmd_result_" for deserializing the command result returned from
        // remote node.
        hres.Value().cmd_result_ = obj_cmd.GetResult();
        hres.Value().is_local_ = false;
        remote_hd_.ObjectCommand(cc_shards_.node_id_,
                                 ng_id,
                                 table_name,
                                 schema_version,
                                 key,
                                 key_shard_code,
                                 obj_cmd,
                                 txn,
                                 tx_term,
                                 command_id,
                                 tx_ts,
                                 hres,
                                 iso_level,
                                 proto,
                                 commit);
    }
}

txservice::CcReqStatus txservice::LocalCcHandler::UploadTxCommands(
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    const CcEntryAddr &cce_addr,
    uint64_t obj_version,
    uint64_t commit_ts,
    const std::vector<std::string> *cmd_list,
    bool has_overwrite,
    CcHandlerResult<PostProcessResult> &hres)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    CcReqStatus req_status{CcReqStatus::SentLocal};

#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    if (dest_node_id == cc_shards_.node_id_)
    {
        UploadTxCommandsCc *req = cmd_commit_pool.NextRequest();
        req->Reset(&cce_addr,
                   tx_number,
                   tx_term,
                   obj_version,
                   commit_ts,
                   cmd_list,
                   has_overwrite,
                   &hres);
        cc_shards_.EnqueueCcRequest(thd_id_, cce_addr.CoreId(), req);
    }
    else
    {
        hres.Value().is_local_ = false;
        hres.IncreaseRemoteRef();
        req_status = CcReqStatus::SentRemote;
        remote_hd_.UploadTxCommands(cc_shards_.node_id_,
                                    tx_number,
                                    tx_term,
                                    command_id,
                                    cce_addr,
                                    obj_version,
                                    commit_ts,
                                    cmd_list,
                                    has_overwrite,
                                    hres);
    }
    return req_status;
}

void txservice::LocalCcHandler::PublishMessage(uint64_t ng_id,
                                               int64_t tx_term,
                                               std::string_view chan,
                                               std::string_view message)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id != cc_shards_.node_id_)
    {
        DLOG(INFO) << "publish message to remote node: " << ng_id;
        remote_hd_.PublishMessage(ng_id, tx_term, chan, message);
    }
}

void txservice::LocalCcHandler::CleanCcEntryForTest(const TableName &table_name,
                                                    const TxKey &key,
                                                    bool only_archives,
                                                    bool flush,
                                                    uint64_t tx_number,
                                                    int64_t tx_term,
                                                    uint16_t command_id,
                                                    CcHandlerResult<bool> &hres)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    // uint32_t shard_code = Sharder::Instance().ShardCode(key.Hash());
    // TODO(lzx): Remove this function.
    assert(false);
    uint32_t shard_code = 0;
    uint32_t shard_id = shard_code >> 10;

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(shard_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        CleanCcEntryForTestCc *req = clean_cc_entry_pool.NextRequest();
        req->Reset(&table_name,
                   &key,
                   only_archives,
                   flush,
                   shard_code,
                   tx_number,
                   tx_term,
                   &hres);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        remote_hd_.CleanCcEntryForTest(cc_shards_.node_id_,
                                       table_name,
                                       key,
                                       only_archives,
                                       flush,
                                       shard_code,
                                       tx_number,
                                       tx_term,
                                       command_id,
                                       hres);
    }
}

void txservice::LocalCcHandler::KickoutData(const TableName &table_name,
                                            uint32_t ng_id,
                                            TxNumber tx_number,
                                            int64_t tx_term,
                                            uint64_t command_id,
                                            CcHandlerResult<Void> &hres,
                                            CleanType clean_type,
                                            std::vector<uint16_t> *bucket_id,
                                            const TxKey *start_key,
                                            const TxKey *end_key,
                                            uint64_t clean_ts,
                                            int32_t range_id,
                                            uint64_t range_version)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        KickoutCcEntryCc *req = kickout_ccentry_pool_.NextRequest();
        // For hash partition, all data in a single bucket should be hashed to
        // the same core.
        uint16_t core_cnt = clean_type == CleanType::CleanBucketData
                                ? 1
                                : Sharder::Instance().GetLocalCcShardsCount();
        req->Reset(table_name,
                   ng_id,
                   &hres,
                   core_cnt,
                   clean_type,
                   start_key,
                   end_key,
                   bucket_id,
                   clean_ts,
                   range_id,
                   range_version);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        if (clean_type == CleanType::CleanBucketData)
        {
            // For clean bucket data just send req to the core which the
            // buckets belongs to. All buckets passed in should be on the same
            // core.
            assert(bucket_id && !bucket_id->empty());
            cc_shards_.EnqueueToCcShard(
                Sharder::Instance().ShardBucketIdToCoreIdx((*bucket_id)[0]),
                req);
        }
        else
        {
            // Dispatch the request to all cores and run in parallel
            for (uint16_t idx = 0;
                 idx < Sharder::Instance().GetLocalCcShardsCount();
                 idx++)
            {
                cc_shards_.EnqueueToCcShard(idx, req);
            }
        }
    }
    else
    {
        // Increment remote reference counter
        hres.IncreaseRemoteRef();
        // Only alter table and truncate table will try to clean data on remote
        // node.
        remote_hd_.KickoutData(cc_shards_.node_id_,
                               tx_number,
                               tx_term,
                               command_id,
                               table_name,
                               ng_id,
                               clean_type,
                               start_key,
                               end_key,
                               hres,
                               clean_ts);
    }
}

void txservice::LocalCcHandler::VerifyOrphanLock(TxNumber txn)
{
    cc_shards_.GetCcShard(thd_id_)->VerifyOrphanLock(cc_shards_.NodeId(), txn);
}

void txservice::LocalCcHandler::InvalidateTableCache(
    const TableName &table_name,
    uint32_t ng_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint64_t command_id,
    CcHandlerResult<Void> &hres)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.NodeId())
    {
        InvalidateTableCacheCc *req = invalidate_table_cache_pool.NextRequest();
        req->Reset(&table_name, ng_id, tx_number, tx_term, &hres);
        cc_shards_.EnqueueCcRequest(thd_id_, 0, req);
    }
    else
    {
        hres.IncreaseRemoteRef();
        remote_hd_.InvalidateTableCache(cc_shards_.node_id_,
                                        table_name,
                                        ng_id,
                                        tx_number,
                                        tx_term,
                                        command_id,
                                        hres);
    }
}

void txservice::LocalCcHandler::UpdateKeyCache(const TableName &table_name,
                                               NodeGroupId ng_id,
                                               int64_t tx_term,
                                               const TxKey &start_key,
                                               const TxKey &end_key,
                                               StoreRange *store_range,
                                               CcHandlerResult<Void> &hres)
{
#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif

    size_t core_cnt = cc_shards_.Count();
    UpdateKeyCacheCc *req = update_key_cache_pool_.NextRequest();
    req->Reset(table_name,
               ng_id,
               tx_term,
               core_cnt,
               start_key,
               end_key,
               store_range,
               &hres);
    for (size_t idx = 0; idx < core_cnt; ++idx)
    {
        cc_shards_.EnqueueCcRequest(idx, req);
    }
}

/*
 * Get the node id which runs the current transaction.
 */
uint32_t txservice::LocalCcHandler::GetNodeId() const
{
    return cc_shards_.NodeId();
}

/**
 * @brief Get value of "ts_base".
 */
uint64_t txservice::LocalCcHandler::GetTsBaseValue() const
{
    return cc_shards_.TsBase();
}

txservice::LocalCcHandler::BlockCheckStatus
txservice::LocalCcHandler::PreBlockCcReqCheck(CcHandlerResultBase *hres)
{
    uint64_t last_block_check_ts = hres->BlockReqCheckTs();

    uint64_t now_ts = LocalCcShards::ClockTs();
    using namespace std::chrono_literals;
    // TODO remove this hard code 10 seconds
    uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::seconds(10))
                            .count();

    if (last_block_check_ts != 0)
    {
        // There is already an ongoing block check for this request.
        if (now_ts - last_block_check_ts > duration)
        {
            // The BlockCcReqCheck has timed out without receiving the
            // result. SetError.
            hres->ForceError();
            return BlockCheckStatus::TimeOut;
        }
        return BlockCheckStatus::Ongoing;
    }

    hres->SetBlockReqCheckTs(now_ts);
    return BlockCheckStatus::Normal;
}

void txservice::LocalCcHandler::BlockCcReqCheck(uint64_t tx_number,
                                                int64_t tx_term,
                                                uint16_t command_id,
                                                const CcEntryAddr &cce_addr,
                                                CcHandlerResultBase *hres,
                                                ResultTemplateType type,
                                                size_t acq_key_result_vec_idx)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    // If the ccrequest is in same node, it is not need to check. We think one
    // node is stable and and it will always ok or the entire process crash.
    if (dest_node_id != cc_shards_.node_id_)
    {
        BlockCheckStatus last_check_status = PreBlockCcReqCheck(hres);

        if (last_check_status == BlockCheckStatus::TimeOut)
        {
            LOG(WARNING) << "txn: " << tx_number
                         << " BlockCcReqCheck times out, ForcedError.";
            return;
        }
        if (last_check_status == BlockCheckStatus::Ongoing)
        {
            // Last check has neither received the result nor timed out, just
            // wait.
            return;
        }
        assert(last_check_status == BlockCheckStatus::Normal);

#ifdef EXT_TX_PROC_ENABLED
        hres->SetToBlock();
#endif
        remote_hd_.BlockCcReqCheck(cc_shards_.node_id_,
                                   tx_number,
                                   tx_term,
                                   command_id,
                                   cce_addr,
                                   hres,
                                   type,
                                   acq_key_result_vec_idx);
    }
    // Failover might happen and dest_node_id might not be the original node the
    // request is sent to. It's possible that the node group to which this cc
    // request was sent has failed over to current node. Cannot treat this as a
    // local request, otherwise the txm blocks forever.
    else if (!Sharder::Instance().CheckLeaderTerm(ng_id, cce_addr.Term()))
    {
        LOG(WARNING) << "Fail over happens while cc request is being blocked, "
                        "ForceError, txn: "
                     << tx_number;
        hres->ForceError();
    }
    else
    {
        // No need to check the liveness of the local node.
    }
}

void txservice::LocalCcHandler::BlockAcquireAllCcReqCheck(
    uint32_t ng_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    std::vector<CcEntryAddr> &cce_addrs,
    CcHandlerResultBase *hres)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    // If the ccrequest is in same node, it is not need to check. We think one
    // node is stable and and it will always ok or the entire process crash.
    if (dest_node_id != cc_shards_.node_id_)
    {
        BlockCheckStatus last_check_status = PreBlockCcReqCheck(hres);

        if (last_check_status == BlockCheckStatus::TimeOut)
        {
            LOG(WARNING) << "txn: " << tx_number
                         << " BlockCcReqCheck times out, ForcedError.";
            return;
        }
        if (last_check_status == BlockCheckStatus::Ongoing)
        {
            // Last check has neither received the result nor timed out, just
            // wait.
            return;
        }
        assert(last_check_status == BlockCheckStatus::Normal);

#ifdef EXT_TX_PROC_ENABLED
        hres->SetToBlock();
#endif
        remote_hd_.BlockAcquireAllCcReqCheck(cc_shards_.node_id_,
                                             ng_id,
                                             tx_number,
                                             tx_term,
                                             command_id,
                                             cce_addrs,
                                             hres);
    }
    // Failover might happen and dest_node_id might not be the original node the
    // request is sent to. It's possible that the node group to which this cc
    // request was sent has failed over to current node. Cannot treat this as a
    // local request, otherwise the txm blocks forever.
    else if (!Sharder::Instance().CheckLeaderTerm(ng_id, cce_addrs[0].Term()))
    {
        LOG(WARNING) << "txn: " << tx_number
                     << "Fail over happens while cc request is being blocked, "
                        "ForceError.";
        hres->ForceError();
    }
    else
    {
        // No need to check the liveness of the local node.
    }
}
