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

#include <cstdint>
#include <string>
#include <unordered_map>

namespace txservice
{
enum struct TxErrorCode : uint16_t
{
    NO_ERROR = 0,
    UNDEFINED_ERR,
    INTERNAL_ERR_TIMEOUT,
    TX_INIT_FAIL,
    // A cc request is directed to a follower of the target cc node group.
    CC_REQ_FOLLOWER,

    //-- ReadOperation
    OCC_BREAK_REPEATABLE_READ,
    SI_R4W_ERR_KEY_WAS_UPDATED,

    //-- Upsert
    WRITE_SET_BYTES_COUNT_EXCEED_ERR,

    //-- AcquireWriteOp, AcquireAllOp
    // Fails to acquire write locks due to write-Write conflicts.
    WRITE_WRITE_CONFLICT,
    READ_WRITE_CONFLICT,
    DUPLICATE_KEY,

    //-- generate_sk_parallel_op_
    UNIQUE_CONSTRAINT,
    CAL_ENGINE_DEFINED_CONSTRAINT,

    //-- WriteToLogOp
    LOG_SERVICE_UNREACHABLE,
    WRITE_LOG_FAIL,

    DATA_STORE_ERROR,
    UPSERT_TABLE_PREPARE_FAIL,
    TRANSACTION_NODE_NOT_LEADER,
    UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL,

    // Detect dead lock and abort the transaction
    DEAD_LOCK_ABORT,
    NG_TERM_CHANGED,
    REQUEST_LOST,

    // Out of Memory
    OUT_OF_MEMORY,

    // Ckpt
    CKPT_PIN_RANGE_SLICE_FAIL,

    // Acquire range read lock
    GET_RANGE_ID_ERROR,

    //-- NotifyStartMigrationOp
    DUPLICATE_MIGRATION_TX_ERROR,
    DUPLICATE_CLUSTER_SCALE_TX_ERROR,
    INVALID_CLUSTER_SCALE_REQUEST,

    DATA_NOT_ON_LOCAL_NODE,

    // Execute TxRequest on a committed/aborted txn
    TX_REQUEST_TO_COMMITTED_ABORTED_TX,

    // Read catalog fail, table not initialized.
    READ_CATALOG_FAIL,

    // Catalog read write conflict, the table(db) is being modified.
    READ_CATALOG_CONFLICT,

    // TemplatedCcRequest Common
    REQUESTD_TABLE_NOT_EXISTS,
    REQUESTD_INDEX_TABLE_NOT_EXISTS,
    REQUESTD_TABLE_SCHEMA_MISMATCH,

    // ReadCc,ScanOpenBatchCc,ScanNextBatchCc,AcquireAllCc,AcquireCc,
    ACQUIRE_LOCK_BLOCKED,  // (only used by CcMap).
    // ReadCc,ScanOpenBatchCc,ScanNextBatchCc
    MVCC_READ_MUST_WAIT_WRITE,     // (only used by CcMap).
    MVCC_READ_FOR_WRITE_CONFLICT,  // latest version not fits the read timestamp
    // ReadLocal,ScanLocal
    TX_NODE_NOT_LEADER,
    // NegotiateCc
    NEGOTIATED_TX_UNKNOWN,
    NEGOTIATE_TX_ERR,
    // Scan
    CREATE_CCM_SCANNER_FAILED,
    // log service
    LOG_CLOSURE_RESULT_UNKNOWN_ERR,
    WRITE_LOG_FAILED,
    LOG_NODE_NOT_LEADER,
    DUPLICATE_MIGRATION_TX_ERR,
    DUPLICATE_CLUSTER_SCALE_TX_ERR,
    ESTABLISH_NODE_CHANNEL_FAILED,
    // Error when call system handler, like ReloadCacheCc.
    SYSTEM_HANDLER_ERR,
    TASK_EXPIRED,
    LOG_NOT_TRUNCATABLE,
    // Refuse to receive batch data sent from remote for cache.
    UPLOAD_BATCH_REJECTED,
    // RPC failed that can not retry
    RPC_CALL_ERR,
    // Update sequence table fail
    UPDATE_SEQUENCE_TABLE_FAIL,

};

static const std::unordered_map<TxErrorCode, std::string> tx_error_messages{
    {TxErrorCode::UNDEFINED_ERR,
     "Transaction failed due to an internal undefined error."},
    {TxErrorCode::INTERNAL_ERR_TIMEOUT,
     "Transaction failed due to internal request timeout."},
    {TxErrorCode::TX_INIT_FAIL, "Failed to initialize the transaction."},
    {TxErrorCode::CC_REQ_FOLLOWER,
     "Transaction failed due to internal cc request is directed to a "
     "follower."},
    {TxErrorCode::OCC_BREAK_REPEATABLE_READ,
     "OCC break repeatable read isolation level."},
    {TxErrorCode::SI_R4W_ERR_KEY_WAS_UPDATED,
     "Transaction failed due to record was changed and that breaks Snapshot "
     "Isolation Level."},
    {TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR,
     "Transaction failed due to write set bytes count too large."},
    {TxErrorCode::WRITE_WRITE_CONFLICT,
     "Transaction failed due to write-write conflicts."},
    {TxErrorCode::READ_WRITE_CONFLICT,
     "Transaction failed due to read-write conflicts."},
    {TxErrorCode::DUPLICATE_KEY, "Transaction failed due to duplicate key."},
    {TxErrorCode::UNIQUE_CONSTRAINT,
     "Create index violates unique constraint."},
    {TxErrorCode::CAL_ENGINE_DEFINED_CONSTRAINT,
     "Create index violates calculation engine defined constraint."},
    {TxErrorCode::LOG_SERVICE_UNREACHABLE,
     "Log service is unreachable, transaction status is unknown."},
    {TxErrorCode::WRITE_LOG_FAIL, "Write Log fails."},
    {TxErrorCode::DATA_STORE_ERROR, "Data storage is not available."},
    {TxErrorCode::UPSERT_TABLE_PREPARE_FAIL, "Failed at prepare phase."},
    {TxErrorCode::TRANSACTION_NODE_NOT_LEADER,
     "Transaction failed due to the transaction node is no longer the raft "
     "leader."},
    {TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL,
     "Failed at acquire write intent."},
    {TxErrorCode::DEAD_LOCK_ABORT, "Abort the transaction due to dead lock."},
    {TxErrorCode::NG_TERM_CHANGED,
     "The node group term changed and the related ccrequest was discarded."},
    {TxErrorCode::REQUEST_LOST, "The returned message of ccrequest missed."},
    {TxErrorCode::OUT_OF_MEMORY, "Transaction failed due to out of memory."},
    {TxErrorCode::CKPT_PIN_RANGE_SLICE_FAIL,
     "The checkpoint error due to pin range slice failed."},
    {TxErrorCode::GET_RANGE_ID_ERROR, "Acquire range read lock failed."},
    {TxErrorCode::DUPLICATE_MIGRATION_TX_ERROR,
     "Duplicate migration tx error."},
    {TxErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERROR,
     "Duplicate cluster scale tx error."},
    {TxErrorCode::INVALID_CLUSTER_SCALE_REQUEST,
     "Invalid cluster scale request."},
    {TxErrorCode::DATA_NOT_ON_LOCAL_NODE, "Data not on local node."},
    {TxErrorCode::TX_REQUEST_TO_COMMITTED_ABORTED_TX,
     "Execute TxRequest failed, transaction has committed/aborted"},
    {TxErrorCode::READ_CATALOG_FAIL, "Current db has not been initialized"},
    {TxErrorCode::READ_CATALOG_CONFLICT,
     "Read catalog conflict, the table(db) is being modified."},
    {TxErrorCode::REQUESTD_TABLE_NOT_EXISTS, "Requested table not exists."},
    {TxErrorCode::REQUESTD_INDEX_TABLE_NOT_EXISTS,
     "Requested index table not exists."},
    {TxErrorCode::REQUESTD_TABLE_SCHEMA_MISMATCH,
     "Requested table schema mismatch."},
    {TxErrorCode::ACQUIRE_LOCK_BLOCKED, "Acquire lock blocked."},
    {TxErrorCode::MVCC_READ_MUST_WAIT_WRITE,
     "MVCC read must wait write to finish."},
    {TxErrorCode::MVCC_READ_FOR_WRITE_CONFLICT,
     "MVCC read for write conflict."},
    {TxErrorCode::TX_NODE_NOT_LEADER, "Transaction node is not leader."},
    {TxErrorCode::NEGOTIATED_TX_UNKNOWN,
     "Negotiated transaction commit timestamp failed since conflict "
     "transaction status is unknown."},
    {TxErrorCode::NEGOTIATE_TX_ERR,
     "Negotiate transaction commit timestamp failed due to error."},
    {TxErrorCode::CREATE_CCM_SCANNER_FAILED,
     "Create in memory data scanner failed."},
    {TxErrorCode::WRITE_LOG_FAILED, "Write log failed."},
    {TxErrorCode::LOG_NODE_NOT_LEADER, "Log node is not leader."},
    {TxErrorCode::ESTABLISH_NODE_CHANNEL_FAILED,
     "Establish node channel failed."},
    {TxErrorCode::SYSTEM_HANDLER_ERR, "System handler error."},
    {TxErrorCode::TASK_EXPIRED, "Task expired."},
    {TxErrorCode::LOG_NOT_TRUNCATABLE, "Log not truncatable."},
    {TxErrorCode::UPLOAD_BATCH_REJECTED, "Upload batch rejected."},
    {TxErrorCode::RPC_CALL_ERR, "RPC call error."},
    {TxErrorCode::UPDATE_SEQUENCE_TABLE_FAIL, "Update sequence table fail."},
};

static inline const std::string &TxErrorMessage(TxErrorCode err_code)
{
    if (err_code != TxErrorCode::NO_ERROR)
    {
        auto it = tx_error_messages.find(err_code);
        if (it != tx_error_messages.end())
        {
            return it->second;
        }
    }

    static std::string empty_err_msg;
    return empty_err_msg;
}

enum struct CcErrorCode : uint8_t
{
    NO_ERROR = 0,
    UNDEFINED_ERR,

    // TxOperation
    FORCE_FAIL,

    // CcRequest Common
    REQUESTED_NODE_NOT_LEADER,

    // TemplatedCcRequest Common
    REQUESTED_TABLE_NOT_EXISTS,
    REQUESTED_INDEX_TABLE_NOT_EXISTS,
    REQUESTED_TABLE_SCHEMA_MISMATCH,

    // TransactionExecution::FillDataLogRequest
    NG_TERM_CHANGED,

    // ReadCc,ScanOpenBatchCc,ScanNextBatchCc,AcquireAllCc,AcquireCc,
    ACQUIRE_LOCK_BLOCKED,  // (only used by CcMap).
    // ACQUIRE_KEY_LOCK_FAILED,
    ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT,
    ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT,
    ACQUIRE_GAP_LOCK_FAILED,

    // ReadCc,ScanOpenBatchCc,ScanNextBatchCc
    MVCC_READ_MUST_WAIT_WRITE,     // (only used by CcMap).
    MVCC_READ_FOR_WRITE_CONFLICT,  // latest version not fits the read timestamp

    // ReadLocal,ScanLocal
    TX_NODE_NOT_LEADER,

    // NegotiateCc
    NEGOTIATED_TX_UNKNOWN,
    NEGOTIATE_TX_ERR,

    // Scan
    CREATE_CCM_SCANNER_FAILED,

    // AcquireAllCc, AcquireCc
    DUPLICATE_INSERT_ERR,

    // PostReadCc
    VALIDATION_FAILED_FOR_VERSION_MISMATCH,
    VALIDATION_FAILED_FOR_CONFILICTED_TXS,

    // range
    GET_RANGE_ID_ERR,
    PIN_RANGE_SLICE_FAILED,

    // data store handler
    DATA_STORE_ERR,

    // log service
    LOG_CLOSURE_RESULT_UNKNOWN_ERR,
    WRITE_LOG_FAILED,
    LOG_NODE_NOT_LEADER,
    DUPLICATE_MIGRATION_TX_ERR,
    DUPLICATE_CLUSTER_SCALE_TX_ERR,

    // Detect dead lock and abort the transaction
    DEAD_LOCK_ABORT,

    // Lost the request result
    REQUEST_LOST,

    // Shard memory full
    OUT_OF_MEMORY,

    ESTABLISH_NODE_CHANNEL_FAILED,
    //
    PACK_SK_ERR,
    UNIQUE_CONSTRAINT,

    // Error when call system handler, like ReloadCacheCc.
    SYSTEM_HANDLER_ERR,

    // For Redis, if key not on local node, return this error.
    DATA_NOT_ON_LOCAL_NODE,

    TASK_EXPIRED,

    LOG_NOT_TRUNCATABLE,

    // Refuse to receive batch data sent from remote for cache.
    UPLOAD_BATCH_REJECTED,

    // Read catalog fail, table not initialized or being modified.
    READ_CATALOG_FAIL,

    // Read catalog conflict.
    READ_CATALOG_CONFLICT,

    // RPC failed that can not retry
    RPC_CALL_ERR,

    // Update sequence table fail
    UPDATE_SEQUENCE_TABLE_FAIL,

    // NOTICE: please keep this variable at tail.
    LAST_ERROR_CODE,

};

static const std::unordered_map<CcErrorCode, std::string> cc_error_messages{
    {CcErrorCode::NO_ERROR, "NO_ERROR"},
    {CcErrorCode::UNDEFINED_ERR, "UNDEFINED_CC_ERR"},

    {CcErrorCode::FORCE_FAIL, "FORCE_FAIL"},
    {CcErrorCode::NG_TERM_CHANGED, "NG_TERM_CHANGED"},
    {CcErrorCode::REQUESTED_NODE_NOT_LEADER, "REQUESTED_NODE_NOT_LEADER"},
    {CcErrorCode::TX_NODE_NOT_LEADER, "TX_NODE_NOT_LEADER"},

    {CcErrorCode::NEGOTIATED_TX_UNKNOWN, "NEGOTIATED_TX_UNKNOWN"},
    {CcErrorCode::NEGOTIATE_TX_ERR, "NEGOTIATE_TX_ERR"},

    {CcErrorCode::REQUESTED_TABLE_NOT_EXISTS, "REQUESTED_TABLE_NOT_EXISTS"},
    {CcErrorCode::REQUESTED_INDEX_TABLE_NOT_EXISTS,
     "REQUESTED_INDEX_TABLE_NOT_EXISTS"},
    {CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH,
     "REQUESTED_TABLE_SCHEMA_MISMATCH"},
    {CcErrorCode::CREATE_CCM_SCANNER_FAILED, "CRATE_CCM_SCANNER_FAILED"},

    {CcErrorCode::DUPLICATE_INSERT_ERR, "DUPLICATE_INSERT_ERR"},
    {CcErrorCode::ACQUIRE_LOCK_BLOCKED, "ACQUIRE_LOCK_BLOCKED"},
    {CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT,
     "ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT"},
    {CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT,
     "ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT"},
    {CcErrorCode::ACQUIRE_GAP_LOCK_FAILED, "ACQUIRE_GAP_LOCK_FAILED"},
    {CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH,
     "VALIDATION_FAILED_FOR_VERSION_MISMATCH"},
    {CcErrorCode::VALIDATION_FAILED_FOR_CONFILICTED_TXS,
     "VALIDATION_FAILED_FOR_CONFILICTED_TXS"},
    {CcErrorCode::MVCC_READ_MUST_WAIT_WRITE, "MVCC_READ_MUST_WAIT_WRITE"},
    {CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT, "MVCC_READ_FOR_WRITE_CONFLICT"},

    {CcErrorCode::OUT_OF_MEMORY, "OUT_OF_MEMORY"},

    // range
    {CcErrorCode::GET_RANGE_ID_ERR, "GET_RANGE_ID_ERR"},
    {CcErrorCode::PIN_RANGE_SLICE_FAILED, "PIN_RANGE_SLICE_FAILED"},

    // data store handler
    {CcErrorCode::DATA_STORE_ERR, "DATA_STORE_ERR"},

    // log service
    {CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR,
     "LOG_CLOSURE_RESULT_UNKNOWN_ERR"},

    // detect dead lock
    {CcErrorCode::DEAD_LOCK_ABORT, "DEAD_LOCK_ABORT"},
    {CcErrorCode::REQUEST_LOST, "REQUEST_LOST"},

    // acquire leader term
    {CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED,
     "ESTABLISH_NODE_CHANNEL_FAILED"},

    {CcErrorCode::PACK_SK_ERR, "PACK_SK_ERROR"},
    {CcErrorCode::UNIQUE_CONSTRAINT, "UNIQUE_CONSTRAINT"},

    // Error when call system handler, like ReloadCacheCc.
    {CcErrorCode::SYSTEM_HANDLER_ERR, "SYSTEM_HANDLER_ERR"},

    {CcErrorCode::DATA_NOT_ON_LOCAL_NODE, "DATA_NOT_ON_LOCAL_NODE"},

    {CcErrorCode::TASK_EXPIRED, "TASK_EXPIRED"},

    {CcErrorCode::LOG_NOT_TRUNCATABLE, "LOG_NOT_TRUNCATABLE"},
    {CcErrorCode::UPLOAD_BATCH_REJECTED, "UPLOAD_BATCH_REJECTED"},

    {CcErrorCode::READ_CATALOG_FAIL, "READ_CATALOG_FAIL"},

    {CcErrorCode::WRITE_LOG_FAILED, "WRITE_LOG_FAIL"},
    {CcErrorCode::LOG_NODE_NOT_LEADER, "LOG_NODE_NOT_LEADER"},

    {CcErrorCode::READ_CATALOG_CONFLICT, "READ_CATALOG_CONFLICT"},
    {CcErrorCode::RPC_CALL_ERR, "RPC_CALL_ERR"},
    {CcErrorCode::UPDATE_SEQUENCE_TABLE_FAIL, "UPDATE_SEQUENCE_TABLE_FAIL"},

    // NOTICE: please keep this variable at tail.
    {CcErrorCode::LAST_ERROR_CODE, "LAST_ERROR_CODE"},
};

static inline const std::string CcErrorMessage(CcErrorCode error_code)
{
    auto it = cc_error_messages.find(error_code);
    if (it != cc_error_messages.end())
    {
        return it->second;
    }
    return "CcErrorCode:" + std::to_string(static_cast<int>(error_code));
}
}  // namespace txservice
