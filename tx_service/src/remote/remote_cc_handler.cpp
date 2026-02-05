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
#include "remote_cc_handler.h"

#include <iostream>

#include "cc/local_cc_shards.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_trace.h"

txservice::remote::RemoteCcHandler::RemoteCcHandler(CcStreamSender &sender)
    : stream_sender_(sender)
{
}

void txservice::remote::RemoteCcHandler::AcquireWrite(
    uint32_t src_id,
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
    CcProtocol proto,
    IsolationLevel iso_level,
    bool abort_if_oom)
{
    /*message AcquireRequest
    {
        uint32 src_node_id = 1;
        string tablename = 3;
        string key = 4;
        uint32 key_shard_code = 5;
        uint32 vec_idx = 6;
        uint64 ts = 7;
        bool insert = 8;
    }*/

    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireRequest);
    send_msg.set_tx_number(txn);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    AcquireRequest *acq = send_msg.mutable_acquire_req();
    acq->set_src_node_id(src_id);
    acq->set_table_name_str(table_name.String());
    acq->set_table_type(ToRemoteType::ConvertTableType(table_name.Type()));
    acq->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    acq->clear_key();
    key.Serialize(*acq->mutable_key());

    acq->set_vec_idx(hd_res_idx);
    acq->set_ts(ts);
    acq->set_schema_version(schema_version);
    acq->set_insert(is_insert);
    acq->set_key_shard_code(key_shard_code);
    acq->set_protocol(ToRemoteType::ConvertProtocol(proto));
    acq->set_iso_level(ToRemoteType::ConvertIsolation(iso_level));
    acq->set_abort_if_oom(abort_if_oom);

    stream_sender_.SendMessageToNg(dest_ng_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::AcquireWriteAll(
    uint32_t src_node_id,
    const TableName &table_name,
    const TxKey &key,
    uint32_t node_group_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    bool is_insert,
    CcHandlerResult<AcquireAllResult> &hres,
    CcProtocol proto,
    CcOperation cc_op,
    bool abort_if_oom)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireAllRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    AcquireAllRequest *acq_all = send_msg.mutable_acquire_all_req();
    acq_all->set_src_node_id(src_node_id);
    acq_all->set_table_name_str(table_name.String());
    acq_all->set_table_type(ToRemoteType::ConvertTableType(table_name.Type()));
    acq_all->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    switch (key.Type())
    {
    case KeyType::NegativeInf:
        acq_all->set_neg_inf(true);
        break;
    case KeyType::PositiveInf:
        acq_all->set_pos_inf(true);
        break;
    case KeyType::Normal:
        acq_all->clear_key();
        key.Serialize(*acq_all->mutable_key());
        break;
    default:
        assert(false);
    }

    acq_all->set_node_group_id(node_group_id);
    acq_all->set_insert(is_insert);
    acq_all->set_protocol(ToRemoteType::ConvertProtocol(proto));
    acq_all->set_cc_op(ToRemoteType::ConvertCcOperation(cc_op));
    acq_all->set_abort_if_oom(abort_if_oom);

    stream_sender_.SendMessageToNg(node_group_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::PostWrite(
    uint32_t src_node_id,
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
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostCommitRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    PostCommitRequest *post_commit = send_msg.mutable_postcommit_req();
    post_commit->set_src_node_id(src_node_id);
    post_commit->set_node_group_id(cce_addr.NodeGroupId());
    CceAddr_msg *cce_addr_msg = post_commit->mutable_cce_addr();
    assert(cce_addr.CceLockPtr() != 0);
    cce_addr_msg->set_cce_lock_ptr(cce_addr.CceLockPtr());
    cce_addr_msg->set_term(cce_addr.Term());
    cce_addr_msg->set_core_id(cce_addr.CoreId());

    post_commit->clear_record();
    if (commit_ts > 0 && operation_type != OperationType::Delete)
    {
        // The commit ts is 0, if the post-write request is used to clear the
        // write lock when the tx aborts.
        if (record != nullptr)
        {
            record->Serialize(*post_commit->mutable_record());
        }
    }

    post_commit->set_commit_ts(commit_ts);
    post_commit->set_operation_type(static_cast<uint32_t>(operation_type));
    post_commit->set_key_shard_code(key_shard_code);

    stream_sender_.SendMessageToNg(cce_addr.NodeGroupId(), send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::PostWriteAll(
    uint32_t src_node_id,
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
    PostWriteType post_write_type,
    bool abort_if_oom)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostWriteAllRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    PostWriteAllRequest *post_write_all = send_msg.mutable_post_write_all_req();
    post_write_all->set_src_node_id(src_node_id);
    post_write_all->set_table_name_str(table_name.String());
    post_write_all->set_table_type(
        ToRemoteType::ConvertTableType(table_name.Type()));
    post_write_all->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    post_write_all->set_node_group_id(ng_id);

    switch (key.Type())
    {
    case KeyType::NegativeInf:
        post_write_all->set_neg_inf(true);
        break;
    case KeyType::PositiveInf:
        post_write_all->set_pos_inf(true);
        break;
    case KeyType::Normal:
        post_write_all->clear_key();
        key.Serialize(*post_write_all->mutable_key());
        break;
    default:
        assert(false);
    }

    post_write_all->set_commit_ts(commit_ts);

    post_write_all->clear_record();
    if (commit_ts > 0 && op_type != OperationType::Delete &&
        op_type != OperationType::DropTable)
    {
        // The commit ts is 0, if the post-write request is used to clear the
        // write lock when the tx aborts.
        rec.Serialize(*post_write_all->mutable_record());
    }

    post_write_all->set_operation_type(static_cast<uint32_t>(op_type));

    CommitType commit_type =
        ToRemoteType::ConvertPostWriteType(post_write_type);
    post_write_all->set_commit_type(commit_type);
    post_write_all->set_abort_if_oom(abort_if_oom);

    stream_sender_.SendMessageToNg(ng_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::PostRead(
    uint32_t src_node_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t key_ts,
    uint64_t gap_ts,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<PostProcessResult> &hres,
    bool need_remote_resp,
    txservice::PostReadType post_read_type)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ValidateRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    ValidateRequest *vali = send_msg.mutable_validate_req();
    vali->set_src_node_id(src_node_id);
    vali->set_node_group_id(cce_addr.NodeGroupId());
    CceAddr_msg *cce_addr_msg = vali->mutable_cce_addr();
    cce_addr_msg->set_cce_lock_ptr(cce_addr.CceLockPtr());
    cce_addr_msg->set_term(cce_addr.Term());
    cce_addr_msg->set_core_id(cce_addr.CoreId());
    vali->set_commit_ts(commit_ts);
    vali->set_key_ts(key_ts);
    vali->set_gap_ts(gap_ts);
    vali->set_need_resp(need_remote_resp);
    vali->set_post_read_type(ToRemoteType::ConvertPostReadType(post_read_type));

    stream_sender_.SendMessageToNg(cce_addr.NodeGroupId(), send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::Read(
    uint32_t src_node_id,
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
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_for_write,
    bool is_covering_keys,
    bool point_read_on_miss,
    int32_t partition_id,
    bool abort_if_oom)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    ReadRequest *read = send_msg.mutable_read_req();
    read->set_src_node_id(src_node_id);
    read->set_table_name_str(table_name.String());
    read->set_table_type(ToRemoteType::ConvertTableType(table_name.Type()));
    read->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    read->clear_key();
    key.Serialize(*read->mutable_key());
    read->set_key_shard_code(key_shard_code);
    read->set_iso_level(ToRemoteType::ConvertIsolation(iso_level));
    read->set_protocol(ToRemoteType::ConvertProtocol(proto));
    read->set_is_for_write(is_for_write);
    read->set_is_covering_keys(is_covering_keys);
    read->set_point_read_on_miss(point_read_on_miss);
    read->set_partition_id(partition_id);

    read->clear_record();
    switch (read_type)
    {
    case ReadType::Inside:
        read->set_read_type(ReadRequest_ReadType::ReadRequest_ReadType_INSIDE);
        break;
    case ReadType::OutsideNormal:
    {
        read->set_read_type(
            ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_NORMAL);
        std::string *rec_str = read->mutable_record();
        record.Serialize(*rec_str);
        break;
    }
    case ReadType::OutsideDeleted:
        read->set_read_type(
            ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_DELETED);
        break;
    default:
        break;
    }

    read->set_ts(ts);
    read->set_schema_version(schema_version);
    read->set_abort_if_oom(abort_if_oom);

    stream_sender_.SendMessageToNg(dest_ng_id, send_msg, &hres);
}

/*
 * ReadOutside fills the tuple read from KV into cache.
 */
void txservice::remote::RemoteCcHandler::ReadOutside(
    int64_t tx_term,
    uint16_t command_id,
    const TxRecord &record,
    bool is_deleted,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    std::vector<VersionTxRecord> *archives)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadOutsideRequest);
    send_msg.set_tx_number(0);
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    ReadOutsideRequest *read_outside = send_msg.mutable_read_outside_req();
    assert(cce_addr.CceLockPtr() != 0);
    read_outside->set_node_group_id(cce_addr.NodeGroupId());

    CceAddr_msg *cce_msg = read_outside->mutable_cce_addr();
    cce_msg->set_cce_lock_ptr(cce_addr.CceLockPtr());
    cce_msg->set_term(cce_addr.Term());
    cce_msg->set_core_id(cce_addr.CoreId());

    read_outside->set_rec_status(is_deleted ? RecordStatusType::DELETED
                                            : RecordStatusType::NORMAL);
    read_outside->set_commit_ts(commit_ts);

    read_outside->clear_record();
    if (!is_deleted)
    {
        record.Serialize(*read_outside->mutable_record());
    }

    if (archives != nullptr)
    {
        for (const VersionTxRecord &vrecord : *archives)
        {
            VersionTxRecord_msg *vrec_msg = read_outside->add_archives();
            vrec_msg->set_version_ts(vrecord.commit_ts_);
            vrec_msg->set_rec_status(
                ToRemoteType::ConvertRecordStatus(vrecord.record_status_));
            vrecord.record_->Serialize(*vrec_msg->mutable_record());
        }
    }

    // ReadOutside doesn't care the execution of fill tuple succeeds or not.
    // Return value of SendRequest could be ignored.
    stream_sender_.SendMessageToNg(cce_addr.NodeGroupId(), send_msg);
}

void txservice::remote::RemoteCcHandler::ScanOpen(
    uint32_t src_node_id,
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
    ScanDirection direction,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_for_write,
    bool is_ckpt,
    bool is_covering_keys,
    bool is_require_keys,
    bool is_require_recs,
    int32_t obj_type,
    const std::string_view &scan_pattern)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanOpenRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hd_res.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    ScanOpenRequest *scan_open = send_msg.mutable_scan_open_req();
    scan_open->set_src_node_id(src_node_id);
    scan_open->set_table_name_str(table_name.String());
    scan_open->set_table_type(
        ToRemoteType::ConvertTableType(table_name.Type()));
    scan_open->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    scan_open->set_shard_id(node_group_id);

    switch (start_key.Type())
    {
    case KeyType::NegativeInf:
        scan_open->set_neg_inf(true);
        break;
    case KeyType::PositiveInf:
        scan_open->set_pos_inf(true);
        break;
    default:
        scan_open->clear_key();
        start_key.Serialize(*scan_open->mutable_key());
        break;
    }

    scan_open->set_inclusive(inclusive);
    scan_open->set_direction(direction == ScanDirection::Forward);
    scan_open->set_ts(ts);
    scan_open->set_iso_level(ToRemoteType::ConvertIsolation(iso_level));
    scan_open->set_protocol(ToRemoteType::ConvertProtocol(proto));
    scan_open->set_is_for_write(is_for_write);
    scan_open->set_ckpt(is_ckpt);
    scan_open->set_is_covering_keys(is_covering_keys);
    scan_open->set_is_require_keys(is_require_keys);
    scan_open->set_is_require_recs(is_require_recs);
    scan_open->set_obj_type(obj_type);
    scan_open->set_scan_pattern(std::string(scan_pattern));
    scan_open->set_schema_version(schema_version);

    stream_sender_.SendMessageToNg(node_group_id, send_msg, &hd_res);
}

void txservice::remote::RemoteCcHandler::ScanNext(
    uint32_t src_node_id,
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t start_ts,
    const TableName &table_name,
    const TxKey &start_key,
    bool start_key_inclusive,
    const TxKey &end_key,
    bool end_key_inclusive,
    const std::vector<uint16_t> &bucket_ids,
    const absl::flat_hash_map<uint16_t, BucketScanProgress>
        &bucket_scan_progress,
    CcHandlerResult<ScanNextResult> &hd_res,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_for_write,
    bool is_ckpt,
    bool is_covering_keys,
    bool is_require_keys,
    bool is_require_recs,
    int32_t obj_type,
    const std::string_view &scan_pattern)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanNextRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hd_res.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    ScanNextRequest *scan_next = send_msg.mutable_scan_next_req();

    scan_next->set_src_node_id(src_node_id);
    scan_next->set_node_group_id(ng_id);
    scan_next->set_node_group_term(ng_term);
    // set table name
    scan_next->set_table_name_str(table_name.String());
    scan_next->set_table_type(
        ToRemoteType::ConvertTableType(table_name.Type()));
    scan_next->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    // set direction
    // scan_next->set_direction(true);
    // set ts
    scan_next->set_ts(start_ts);
    scan_next->set_ckpt(is_ckpt);
    scan_next->set_iso_level(ToRemoteType::ConvertIsolation(iso_level));
    scan_next->set_protocol(ToRemoteType::ConvertProtocol(proto));
    scan_next->set_is_for_write(is_for_write);
    scan_next->set_is_covering_keys(is_covering_keys);
    scan_next->set_is_require_keys(is_require_keys);
    scan_next->set_is_require_recs(is_require_recs);
    scan_next->set_obj_type(obj_type);
    scan_next->set_scan_pattern(std::string(scan_pattern));

    // set end key
    switch (end_key.Type())
    {
    case KeyType::NegativeInf:
    {
        scan_next->clear_end_key();
        scan_next->mutable_end_key()->set_neg_inf(true);
        break;
    }
    case KeyType::PositiveInf:
    {
        scan_next->clear_end_key();
        scan_next->mutable_end_key()->set_pos_inf(true);
        break;
    }
    default:
    {
        scan_next->clear_end_key();
        end_key.Serialize(*scan_next->mutable_end_key()->mutable_key());
        break;
    }
    }

    scan_next->set_end_key_inclusive(end_key_inclusive);

    scan_next->clear_scan_spec();
    if (bucket_scan_progress.empty())
    {
        // new plan
        ::txservice::remote::BucketScanInfoMsg *scan_info =
            scan_next->mutable_global_info();
        scan_info->clear_start_key();
        switch (start_key.Type())
        {
        case KeyType::NegativeInf:
        {
            scan_info->mutable_start_key()->set_neg_inf(true);
            break;
        }
        case KeyType::PositiveInf:
        {
            scan_info->mutable_start_key()->set_pos_inf(true);
            break;
        }
        default:
        {
            start_key.Serialize(*scan_info->mutable_start_key()->mutable_key());
            break;
        }
        }

        scan_info->set_start_key_inclusive(start_key_inclusive);
        for (const auto &bucket : bucket_ids)
        {
            scan_info->add_scan_buckets(bucket);
        }
    }
    else
    {
        ::txservice::remote::BucketScanProgressMap *progress_map =
            scan_next->mutable_progress();
        for (const auto &[core_id, progress] : bucket_scan_progress)
        {
            auto iter = progress_map->mutable_progress()->emplace(core_id);
            iter.first->second.clear_scan_buckets();
            for (const auto &[bucket_id, drained] : progress.scan_buckets_)
            {
                iter.first->second.mutable_scan_buckets()->emplace(bucket_id,
                                                                   drained);
            }
            iter.first->second.set_memory_is_drained(
                progress.memory_scan_is_finished_);
            progress.pause_key_.Serialize(
                *iter.first->second.mutable_start_key()->mutable_key());
            iter.first->second.set_start_key_inclusive(
                progress.pause_key_inclusive_);
        }
    }

    stream_sender_.SendMessageToNg(ng_id, send_msg, &hd_res);
}

void txservice::remote::RemoteCcHandler::ScanNext(
    uint32_t src_node_id,
    const TableName &tbl_name,
    const uint64_t schema_version,
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
    CcProtocol proto)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanSliceRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hd_res.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    ScanSliceRequest *scan_slice = send_msg.mutable_scan_slice_req();
    scan_slice->set_src_node_id(src_node_id);
    scan_slice->set_node_group_id(cc_ng_id);
    scan_slice->set_cc_ng_term(cc_ng_term);
    scan_slice->set_table_name_str(tbl_name.StringView().data());
    scan_slice->set_table_type(ToRemoteType::ConvertTableType(tbl_name.Type()));
    scan_slice->set_table_engine(
        ToRemoteType::ConvertTableEngine(tbl_name.Engine()));
    scan_slice->set_schema_version(schema_version);
    scan_slice->set_range_id(range_id);
    scan_slice->clear_start_key();
    if (start_key->Type() == KeyType::Normal)
    {
        start_key->Serialize(*scan_slice->mutable_start_key());
    }
    scan_slice->set_start_inclusive(start_inclusive);

    scan_slice->clear_end_key();
    if (end_key != nullptr && end_key->Type() == KeyType::Normal)
    {
        end_key->Serialize(*scan_slice->mutable_end_key());
        scan_slice->set_end_inclusive(end_inclusive);
    }

    bool forward =
        hd_res.Value().ccm_scanner_->Direction() == ScanDirection::Forward;
    scan_slice->set_is_forward(forward);
    scan_slice->set_ts(read_ts);

    CcScanner &scanner = *hd_res.Value().ccm_scanner_;

    scan_slice->clear_prior_cce_lock_vec();
    // When the cc ng term is greater than 0, this scan resumes the last scan in
    // the range. Sets the cc entry addresses where last scan stops.
    if (cc_ng_term > 0)
    {
        ScanCache *cache = scanner.Cache();
        const ScanTuple *last_tuple = cache->LastTuple();
        scan_slice->add_prior_cce_lock_vec(
            last_tuple != nullptr ? last_tuple->cce_addr_.CceLockPtr() : 0);

        scanner.ResetCaches();
    }

    scan_slice->set_iso_level(ToRemoteType::ConvertIsolation(iso_level));
    scan_slice->set_protocol(ToRemoteType::ConvertProtocol(proto));
    scan_slice->set_is_for_write(scanner.is_for_write_);
    scan_slice->set_is_covering_keys(scanner.is_covering_keys_);
    scan_slice->set_is_require_keys(scanner.is_require_keys_);
    scan_slice->set_is_require_recs(scanner.is_require_recs_);
    scan_slice->set_prefetch_size(prefetch_size);
    stream_sender_.SendMessageToNg(cc_ng_id, send_msg, &hd_res);
}

void txservice::remote::RemoteCcHandler::ReloadCache(
    uint32_t src_node_id,
    NodeGroupId ng_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    CcHandlerResult<Void> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReloadCacheRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_tx_number(tx_number);

    ReloadCacheRequest *reload_req = send_msg.mutable_reload_cache_req();
    reload_req->set_src_node_id(src_node_id);

    stream_sender_.SendMessageToNg(ng_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::FaultInject(
    uint32_t src_node_id,
    const std::string &fault_name,
    const std::string &fault_paras,
    int64_t tx_term,
    uint16_t command_id,
    const TxId &txid,
    int node_id,
    CcHandlerResult<bool> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_FaultInjectRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_tx_number(txid.TxNumber());

    FaultInjectRequest *fi_req = send_msg.mutable_fault_inject_req();
    fi_req->set_src_node_id(src_node_id);
    fi_req->set_fault_name(fault_name);
    fi_req->set_fault_paras(fault_paras);

    stream_sender_.SendMessageToNode(node_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::AnalyzeTableAll(
    uint32_t src_node_id,
    const TableName &table_name,
    NodeGroupId ng_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    CcHandlerResult<Void> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AnalyzeTableAllRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_tx_number(tx_number);

    AnalyzeTableAllRequest *analyze_req =
        send_msg.mutable_analyze_table_all_req();
    analyze_req->set_src_node_id(src_node_id);
    analyze_req->set_node_group_id(ng_id);
    analyze_req->set_table_name_str(table_name.String());
    analyze_req->set_table_type(
        ToRemoteType::ConvertTableType(table_name.Type()));
    analyze_req->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));

    stream_sender_.SendMessageToNg(ng_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::BroadcastStatistics(
    uint32_t src_node_id,
    const TableName &table_name,
    uint64_t schema_ts,
    const remote::NodeGroupSamplePool &sample_pool,
    NodeGroupId ng_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    CcHandlerResult<Void> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(CcMessage::MessageType::
                          CcMessage_MessageType_BroadcastStatisticsRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_tx_number(tx_number);

    remote::BroadcastStatisticsRequest *broadcast_stat_req =
        send_msg.mutable_broadcast_statistics_req();
    broadcast_stat_req->set_src_node_id(src_node_id);
    broadcast_stat_req->set_node_group_id(ng_id);
    broadcast_stat_req->set_table_type(
        remote::ToRemoteType::ConvertTableType(table_name.Type()));
    broadcast_stat_req->set_table_engine(
        remote::ToRemoteType::ConvertTableEngine(table_name.Engine()));
    broadcast_stat_req->set_table_name_str(table_name.String());
    broadcast_stat_req->set_schema_version(schema_ts);
    broadcast_stat_req->mutable_node_group_sample_pool()->CopyFrom(sample_pool);

    auto send_res = stream_sender_.SendMessageToNg(ng_id, send_msg, &hres);
    if (send_res.sent || send_res.queued_for_retry)
    {
        hres.SetFinished();  // Don't wait for remote result.
    }
}

void txservice::remote::RemoteCcHandler::CleanCcEntryForTest(
    uint32_t src_node_id,
    const TableName &table_name,
    const TxKey &key,
    bool only_archives,
    bool flush,
    uint32_t key_shard_code,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    CcHandlerResult<bool> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(CcMessage::MessageType::
                          CcMessage_MessageType_CleanCcEntryForTestRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_tx_number(tx_number);

    CleanCcEntryForTestRequest *clean_req =
        send_msg.mutable_clean_cc_entry_req();
    clean_req->set_src_node_id(src_node_id);
    clean_req->set_table_name_str(table_name.String());
    clean_req->set_table_type(
        ToRemoteType::ConvertTableType(table_name.Type()));
    clean_req->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    clean_req->clear_key();
    key.Serialize(*clean_req->mutable_key());
    clean_req->set_key_shard_code(key_shard_code);
    clean_req->set_only_archives(only_archives);
    clean_req->set_flush(flush);

    uint32_t cc_ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    stream_sender_.SendMessageToNg(cc_ng_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::BlockCcReqCheck(
    uint32_t src_node_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    const CcEntryAddr &cce_addr,
    CcHandlerResultBase *hres,
    ResultTemplateType type,
    size_t acq_key_result_vec_idx)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_BlockedCcReqCheckRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres->Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_tx_number(tx_number);

    BlockedCcReqCheckRequest *req = send_msg.mutable_blocked_check_req();
    req->set_src_node_id(src_node_id);
    req->set_result_temp_type((uint32_t) type);

    if (type == ResultTemplateType::AcquireKeyResult)
    {
        req->set_acq_key_result_vec_idx(acq_key_result_vec_idx);
    }

    req->set_node_group_id(cce_addr.NodeGroupId());

    req->clear_cce_addr();
    CceAddr_msg *cce_addr_msg = req->add_cce_addr();
    assert(cce_addr.CceLockPtr() != 0);
    cce_addr_msg->set_cce_lock_ptr(cce_addr.CceLockPtr());
    cce_addr_msg->set_term(cce_addr.Term());
    cce_addr_msg->set_core_id(cce_addr.CoreId());

    stream_sender_.SendMessageToNg(cce_addr.NodeGroupId(), send_msg, hres);
}

void txservice::remote::RemoteCcHandler::BlockAcquireAllCcReqCheck(
    uint32_t src_node_id,
    uint32_t node_group_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    std::vector<CcEntryAddr> &cce_addrs,
    CcHandlerResultBase *hres)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_BlockedCcReqCheckRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres->Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_tx_number(tx_number);

    BlockedCcReqCheckRequest *req = send_msg.mutable_blocked_check_req();
    req->set_src_node_id(src_node_id);
    req->set_result_temp_type((uint32_t) ResultTemplateType::AcquireAllResult);

    req->set_node_group_id(node_group_id);

    req->clear_cce_addr();

    for (const auto &addr : cce_addrs)
    {
        CceAddr_msg *cce_addr_msg = req->add_cce_addr();
        assert(addr.CceLockPtr() != 0);
        cce_addr_msg->set_cce_lock_ptr(addr.CceLockPtr());
        cce_addr_msg->set_term(addr.Term());
        cce_addr_msg->set_core_id(addr.CoreId());
    }

    stream_sender_.SendMessageToNg(node_group_id, send_msg, hres);
}

void txservice::remote::RemoteCcHandler::KickoutData(
    uint32_t src_node_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint64_t command_id,
    const TableName &table_name,
    uint32_t ng_id,
    txservice::CleanType clean_type,
    const TxKey *start_key,
    const TxKey *end_key,
    CcHandlerResult<Void> &hres,
    uint64_t clean_ts)
{
    CcMessage send_msg;

    // Message head
    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_KickoutDataRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));

    // Construct request body
    KickoutDataRequest *kickout_data_req = send_msg.mutable_kickout_data_req();
    kickout_data_req->set_src_node_id(src_node_id);
    kickout_data_req->set_table_name_str(table_name.String());
    kickout_data_req->set_table_type(
        ToRemoteType::ConvertTableType(table_name.Type()));
    kickout_data_req->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    kickout_data_req->set_node_group_id(ng_id);
    kickout_data_req->set_clean_ts(clean_ts);
    kickout_data_req->clear_start_key();
    kickout_data_req->clear_end_key();
    if (start_key && start_key->Type() == KeyType::Normal)
    {
        start_key->Serialize(*kickout_data_req->mutable_start_key());
    }
    if (end_key && end_key->Type() == KeyType::Normal)
    {
        end_key->Serialize(*kickout_data_req->mutable_end_key());
    }
    kickout_data_req->set_clean_type((txservice::remote::CleanType) clean_type);

    // Send message
    stream_sender_.SendMessageToNg(ng_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::ObjectCommand(
    uint32_t src_node_id,
    NodeGroupId dest_ng_id,
    const TableName &table_name,
    uint64_t schema_version,
    const TxKey &key,
    uint32_t key_shard_code,
    TxCommand &obj_cmd,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t tx_ts,
    CcHandlerResult<ObjectCommandResult> &hres,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool commit,
    bool abort_if_oom)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ApplyRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    ApplyRequest *apply_req = send_msg.mutable_apply_cc_req();
    apply_req->set_src_node_id(src_node_id);
    apply_req->set_table_name_str(table_name.String());
    apply_req->set_table_type(
        ToRemoteType::ConvertTableType(table_name.Type()));
    apply_req->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    apply_req->clear_key();
    key.Serialize(*apply_req->mutable_key());
    apply_req->set_key_shard_code(key_shard_code);
    apply_req->set_iso_level(ToRemoteType::ConvertIsolation(iso_level));
    apply_req->set_protocol(ToRemoteType::ConvertProtocol(proto));
    apply_req->set_tx_ts(tx_ts);
    apply_req->set_apply_and_commit(commit);
    apply_req->set_schema_version(schema_version);
    apply_req->set_abort_if_oom(abort_if_oom);

    apply_req->clear_cmd();
    std::string *cmd_str = apply_req->mutable_cmd();
    obj_cmd.Serialize(*cmd_str);

    stream_sender_.SendMessageToNg(dest_ng_id, send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::PublishMessage(
    uint64_t ng_id,
    int64_t tx_term,
    std::string_view chan,
    std::string_view message)
{
    CcMessage send_msg;
    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PublishRequest);
    send_msg.set_tx_term(tx_term);

    PublishRequest *publish_req = send_msg.mutable_publish_req();
    publish_req->set_chan(std::string(chan));
    publish_req->set_message(std::string(message));

    stream_sender_.SendMessageToNg(ng_id, send_msg);
}

void txservice::remote::RemoteCcHandler::UploadTxCommands(
    uint32_t src_node_id,
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
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_UploadTxCommandsRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);

    UploadTxCommandsRequest *cmds_req = send_msg.mutable_upload_cmds_req();
    cmds_req->set_src_node_id(src_node_id);
    cmds_req->set_node_group_id(cce_addr.NodeGroupId());

    CceAddr_msg *cce_addr_msg = cmds_req->mutable_cce_addr();
    assert(cce_addr.CceLockPtr() != 0);
    cce_addr_msg->set_cce_lock_ptr(cce_addr.CceLockPtr());
    cce_addr_msg->set_term(cce_addr.Term());
    cce_addr_msg->set_core_id(cce_addr.CoreId());

    cmds_req->set_object_version(obj_version);
    cmds_req->set_commit_ts(commit_ts);
    cmds_req->set_has_overwrite(has_overwrite);

    assert(cmd_list != nullptr);
    cmds_req->clear_cmd_list();
    for (const std::string &cmd_str : *cmd_list)
    {
        cmds_req->add_cmd_list(cmd_str);
    }

    stream_sender_.SendMessageToNg(cce_addr.NodeGroupId(), send_msg, &hres);
}

void txservice::remote::RemoteCcHandler::InvalidateTableCache(
    uint32_t src_node_id,
    const TableName &table_name,
    NodeGroupId ng_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    CcHandlerResult<Void> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(CcMessage::MessageType::
                          CcMessage_MessageType_InvalidateTableCacheRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_txm_addr(reinterpret_cast<uint64_t>(hres.Txm()));
    send_msg.set_tx_term(tx_term);
    send_msg.set_command_id(command_id);
    send_msg.set_tx_number(tx_number);

    InvalidateTableCacheRequest *invalidate_req =
        send_msg.mutable_invalidate_table_cache_req();
    invalidate_req->set_src_node_id(src_node_id);
    invalidate_req->set_node_group_id(ng_id);
    invalidate_req->set_table_name_str(table_name.String());
    invalidate_req->set_table_type(
        ToRemoteType::ConvertTableType(table_name.Type()));
    invalidate_req->set_table_engine(
        ToRemoteType::ConvertTableEngine(table_name.Engine()));
    stream_sender_.SendMessageToNg(ng_id, send_msg, &hres);
}
