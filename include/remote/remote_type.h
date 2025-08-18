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

#include <cassert>  //assert

#include "cc_protocol.h"
#include "proto/cc_request.pb.h"
#include "tx_record.h"  // RecordStatus;
#include "type.h"

// NOTICE: The Conversion of Enum Type must not use default case.
namespace txservice
{

namespace remote
{

class ToRemoteType
{
public:
    static txservice::remote::IsolationType ConvertIsolation(
        txservice::IsolationLevel iso_level)
    {
        switch (iso_level)
        {
        case IsolationLevel::ReadCommitted:
            return IsolationType::ReadCommitted;
        case IsolationLevel::Snapshot:
            return IsolationType::SnapshotIsolation;
        case IsolationLevel::RepeatableRead:
            return IsolationType::RepeatableRead;
        case IsolationLevel::Serializable:
            return IsolationType::Serializable;
        default:
            assert(false);
            return IsolationType::ReadCommitted;
        }
    }

    static txservice::remote::CcProtocolType ConvertProtocol(
        txservice::CcProtocol proto)
    {
        switch (proto)
        {
        case CcProtocol::OCC:
            return CcProtocolType::Occ;
        case CcProtocol::Locking:
            return CcProtocolType::Locking;
        case CcProtocol::OccRead:
            return CcProtocolType::OccRead;
        default:
            assert(false);
            return CcProtocolType::Occ;
        }
    }

    static txservice::remote::CcLockType ConvertLockType(
        txservice::LockType lock_type)
    {
        switch (lock_type)
        {
        case LockType::NoLock:
            return CcLockType::NoLock;
        case LockType::ReadIntent:
            return CcLockType::ReadIntent;
        case LockType::ReadLock:
            return CcLockType::ReadLock;
        case LockType::WriteIntent:
            return CcLockType::WriteIntent;
        case LockType::WriteLock:
            return CcLockType::WriteLock;
        default:
            assert(false);
            return CcLockType::NoLock;
        }
    }

    static txservice::remote::CcOperationType ConvertCcOperation(
        txservice::CcOperation cc_op)
    {
        switch (cc_op)
        {
        case CcOperation::Read:
            return CcOperationType::Read;
        case CcOperation::ReadForWrite:
            return CcOperationType::ReadForWrite;
        case CcOperation::ReadSkIndex:
            return CcOperationType::ReadSkIndex;
        case CcOperation::Write:
            return CcOperationType::Write;
        default:
            assert(false);
            return CcOperationType::Read;
        }
    }

    static txservice::remote::CommitType ConvertPostWriteType(
        txservice::PostWriteType write_type)
    {
        switch (write_type)
        {
        case PostWriteType::PrepareCommit:
            return CommitType::PrepareCommit;
        case PostWriteType::PostCommit:
            return CommitType::PostCommit;
        case PostWriteType::Commit:
            return CommitType::Commit;
        case PostWriteType::DowngradeLock:
            return CommitType::DowngradeLock;
        case PostWriteType::UpdateDirty:
            return CommitType::UpdateDirty;
        default:
            assert(false);
            return CommitType::PostCommit;
        }
    }

    static txservice::remote::RecordStatusType ConvertRecordStatus(
        txservice::RecordStatus rec_status)
    {
        switch (rec_status)
        {
        case RecordStatus::Normal:
            return RecordStatusType::NORMAL;
        case RecordStatus::Deleted:
            return RecordStatusType::DELETED;
        case RecordStatus::Unknown:
            return RecordStatusType::UNDEFINED;
        case RecordStatus::RemoteUnknown:
            return RecordStatusType::UNDEFINED;
        case RecordStatus::VersionUnknown:
            return RecordStatusType::VERSIONUNDEFIND;
        case RecordStatus::BaseVersionMiss:
            return RecordStatusType::BaseVersionMiss;
        case RecordStatus::ArchiveVersionMiss:
            return RecordStatusType::ArchiveVersionMiss;
        default:
            assert(false);
            return RecordStatusType::UNDEFINED;
        }
    }

    static txservice::remote::CcTableType ConvertTableType(
        txservice::TableType table_type)
    {
        switch (table_type)
        {
        case TableType::Primary:
            return CcTableType::Primary;
        case TableType::Secondary:
            return CcTableType::Secondary;
        case TableType::UniqueSecondary:
            return CcTableType::UniqueSecondary;
        case TableType::Catalog:
            return CcTableType::Catalog;
        case TableType::RangePartition:
            return CcTableType::RangePartition;
        case TableType::ClusterConfig:
            return CcTableType::ClusterConfig;
        case TableType::RangeBucket:
            return CcTableType::RangeBucket;
        default:
            assert(false);
            return CcTableType::Primary;
        }
    }

    static txservice::remote::TableEngine ConvertTableEngine(
        txservice::TableEngine table_engine)
    {
        switch (table_engine)
        {
        case txservice::TableEngine::None:
            return txservice::remote::TableEngine::None;
        case txservice::TableEngine::EloqSql:
            return txservice::remote::TableEngine::EloqSql;
        case txservice::TableEngine::EloqDoc:
            return txservice::remote::TableEngine::EloqDoc;
        case txservice::TableEngine::EloqKv:
            return txservice::remote::TableEngine::EloqKv;
        case txservice::TableEngine::Sequence:
            return txservice::remote::TableEngine::Sequence;
        default:
            assert(false);
            return TableEngine::EloqSql;
        }
    }

    static int ConvertCcErrorCode(txservice::CcErrorCode error_code)
    {
        return static_cast<int>(error_code);
    }

    static remote::SlicePosition ConvertSlicePosition(
        txservice::SlicePosition slice_pos)
    {
        switch (slice_pos)
        {
        case txservice::SlicePosition::Middle:
            return remote::SlicePosition::Middle;
        case txservice::SlicePosition::FirstSliceInRange:
            return remote::SlicePosition::FirstSliceInRange;
        case txservice::SlicePosition::LastSliceInRange:
            return remote::SlicePosition::LastSliceInRange;
        case txservice::SlicePosition::FirstSlice:
            return remote::SlicePosition::FirstSlice;
        case txservice::SlicePosition::LastSlice:
            return remote::SlicePosition::LastSlice;
        default:
            return remote::SlicePosition::Middle;
        }
    }

    static remote::UploadBatchKind ConvertUploadBatchType(
        txservice::UploadBatchType data_type)
    {
        switch (data_type)
        {
        case UploadBatchType::SkIndexData:
            return remote::UploadBatchKind::SK_DATA;
        case UploadBatchType::DirtyBucketData:
            return remote::UploadBatchKind::DIRTY_BUCKET_DATA;
        default:
            assert(false);
            return remote::UploadBatchKind::SK_DATA;
        }
    }
};

class ToLocalType
{
public:
    static txservice::IsolationLevel ConvertIsolation(
        txservice::remote::IsolationType iso_type)
    {
        switch (iso_type)
        {
        case IsolationType::ReadCommitted:
            return IsolationLevel::ReadCommitted;
        case IsolationType::SnapshotIsolation:
            return IsolationLevel::Snapshot;
        case IsolationType::RepeatableRead:
            return IsolationLevel::RepeatableRead;
        case IsolationType::Serializable:
            return IsolationLevel::Serializable;
        default:
            assert(false);
            return IsolationLevel::ReadCommitted;
        }
    }

    static txservice::CcProtocol ConvertProtocol(
        txservice::remote::CcProtocolType proto)
    {
        switch (proto)
        {
        case CcProtocolType::Occ:
            return CcProtocol::OCC;
        case CcProtocolType::OccRead:
            return CcProtocol::OccRead;
        case CcProtocolType::Locking:
            return CcProtocol::Locking;
        default:
            assert(false);
            return CcProtocol::OCC;
        }
    }

    static txservice::LockType ConvertLockType(
        txservice::remote::CcLockType lock_type)
    {
        switch (lock_type)
        {
        case CcLockType::NoLock:
            return LockType::NoLock;
        case CcLockType::ReadIntent:
            return LockType::ReadIntent;
        case CcLockType::ReadLock:
            return LockType::ReadLock;
        case CcLockType::WriteIntent:
            return LockType::WriteIntent;
        case CcLockType::WriteLock:
            return LockType::WriteLock;
        default:
            assert(false);
            return LockType::NoLock;
        }
    }

    static txservice::CcOperation ConvertCcOperation(
        txservice::remote::CcOperationType cc_op_type)
    {
        switch (cc_op_type)
        {
        case CcOperationType::Read:
            return CcOperation::Read;
        case CcOperationType::ReadForWrite:
            return CcOperation::ReadForWrite;
        case CcOperationType::ReadSkIndex:
            return CcOperation::ReadSkIndex;
        case CcOperationType::Write:
            return CcOperation::Write;
        default:
            assert(false);
            return CcOperation::Read;
        }
    }

    static txservice::PostWriteType ConvertCommitType(
        txservice::remote::CommitType commit_type)
    {
        switch (commit_type)
        {
        case CommitType::PrepareCommit:
            return PostWriteType::PrepareCommit;
        case CommitType::PostCommit:
            return PostWriteType::PostCommit;
        case CommitType::Commit:
            return PostWriteType::Commit;
        case CommitType::DowngradeLock:
            return PostWriteType::DowngradeLock;
        case CommitType::UpdateDirty:
            return PostWriteType::UpdateDirty;
        default:
            assert(false);
            return PostWriteType::PostCommit;
        }
    }

    static txservice::RecordStatus ConvertRecordStatusType(
        txservice::remote::RecordStatusType status_type)
    {
        switch (status_type)
        {
        case RecordStatusType::NORMAL:
            return RecordStatus::Normal;
        case RecordStatusType::DELETED:
            return RecordStatus::Deleted;
        case RecordStatusType::UNDEFINED:
            return RecordStatus::Unknown;
        case RecordStatusType::VERSIONUNDEFIND:
            return RecordStatus::VersionUnknown;
        case RecordStatusType::BaseVersionMiss:
            return RecordStatus::BaseVersionMiss;
        case RecordStatusType::ArchiveVersionMiss:
            return RecordStatus::ArchiveVersionMiss;
        default:
            assert(false);
            return RecordStatus::Unknown;
        }
    }

    static txservice::TableType ConvertCcTableType(
        txservice::remote::CcTableType table_type)
    {
        switch (table_type)
        {
        case CcTableType::Primary:
            return TableType::Primary;
        case CcTableType::Secondary:
            return TableType::Secondary;
        case CcTableType::UniqueSecondary:
            return TableType::UniqueSecondary;
        case CcTableType::Catalog:
            return TableType::Catalog;
        case CcTableType::RangePartition:
            return TableType::RangePartition;
        case CcTableType::RangeBucket:
            return TableType::RangeBucket;
        case CcTableType::ClusterConfig:
            return TableType::ClusterConfig;
        default:
            assert(false);
            return TableType::Primary;
        }
    }

    static txservice::TableEngine ConvertTableEngine(
        txservice::remote::TableEngine table_engine)
    {
        switch (table_engine)
        {
        case txservice::remote::TableEngine::None:
            return txservice::TableEngine::None;
        case txservice::remote::TableEngine::EloqDoc:
            return txservice::TableEngine::EloqDoc;
        case txservice::remote::TableEngine::EloqKv:
            return txservice::TableEngine::EloqKv;
        case txservice::remote::TableEngine::EloqSql:
            return txservice::TableEngine::EloqSql;
        case txservice::remote::TableEngine::Sequence:
            return txservice::TableEngine::Sequence;
        default:
            assert(false);
            return txservice::TableEngine::None;
        }
    }

    static txservice::CleanType ConvertCleanType(
        txservice::remote::CleanType clean_type)
    {
        switch (clean_type)
        {
        case txservice::remote::CleanType::CleanRangeData:
            return txservice::CleanType::CleanRangeData;
        case txservice::remote::CleanType::CleanRangeDataForMigration:
            return txservice::CleanType::CleanRangeDataForMigration;
        case txservice::remote::CleanType::CleanBucketData:
            return txservice::CleanType::CleanBucketData;
        case txservice::remote::CleanType::CleanForAlterTable:
            return txservice::CleanType::CleanForAlterTable;
        case txservice::remote::CleanType::CleanCcm:
            return txservice::CleanType::CleanCcm;
        default:
            assert(false);
            return txservice::CleanType::CleanRangeData;
        }
    }

    static txservice::CcErrorCode ConvertCcErrorCode(int error_code)
    {
        assert(error_code >= 0 &&
               error_code < static_cast<int>(CcErrorCode::LAST_ERROR_CODE));
        return txservice::CcErrorCode(error_code);
    }

    static txservice::SlicePosition ConvertSlicePosition(
        remote::SlicePosition slice_pos)
    {
        switch (slice_pos)
        {
        case SlicePosition::Middle:
            return txservice::SlicePosition::Middle;
        case SlicePosition::FirstSliceInRange:
            return txservice::SlicePosition::FirstSliceInRange;
        case SlicePosition::LastSliceInRange:
            return txservice::SlicePosition::LastSliceInRange;
        case SlicePosition::FirstSlice:
            return txservice::SlicePosition::FirstSlice;
        case SlicePosition::LastSlice:
            return txservice::SlicePosition::LastSlice;
        default:
            return txservice::SlicePosition::Middle;
        }
    }

    static txservice::UploadBatchType ConvertUploadBatchType(
        remote::UploadBatchKind kind)
    {
        switch (kind)
        {
        case remote::UploadBatchKind::SK_DATA:
            return UploadBatchType::SkIndexData;
        case remote::UploadBatchKind::DIRTY_BUCKET_DATA:
            return UploadBatchType::DirtyBucketData;
        default:
            assert(false);
            return UploadBatchType::SkIndexData;
        }
    }
};

}  // namespace remote
}  // namespace txservice
