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

#include "purger_event_listener.h"

#include <rocksdb/db.h>

#include <string>

namespace EloqDS
{

PurgerEventListener::PurgerEventListener(
    const std::string &epoch,
    const std::string &bucket_name,
    const std::string &s3_object_path,
    std::shared_ptr<rocksdb::CloudStorageProvider> storage_provider,
    std::chrono::milliseconds entry_duration,
    std::chrono::milliseconds s3_update_interval)
    : bucket_name_(bucket_name), s3_object_path_(s3_object_path)
{
    sliding_window_ = std::make_unique<SlidingWindow>(entry_duration,
                                                      s3_update_interval,
                                                      epoch,
                                                      bucket_name_,
                                                      s3_object_path_,
                                                      storage_provider);

    LOG(INFO) << "PurgerEventListener created for epoch " << epoch
              << ", bucket: " << bucket_name_
              << ", object_path: " << s3_object_path_
              << ", window_duration: " << entry_duration.count() << "ms"
              << ", s3_update_interval: " << s3_update_interval.count() << "ms";
}

PurgerEventListener::~PurgerEventListener()
{
    Stop();
}

void PurgerEventListener::SetEpoch(const std::string &epoch)
{
    if (sliding_window_)
    {
        LOG(INFO) << "PurgerEventListener epoch updated from "
                  << (sliding_window_->GetEpoch().empty()
                          ? "empty"
                          : sliding_window_->GetEpoch())
                  << " to " << epoch;
        sliding_window_->SetEpoch(epoch);
    }
}

void PurgerEventListener::BlockPurger()
{
    if (sliding_window_)
    {
        sliding_window_->BlockPurger();
    }
}

void PurgerEventListener::OnFlushBegin(
    rocksdb::DB *db, const rocksdb::FlushJobInfo &flush_job_info)
{
    // Log flush begin event (similar to existing RocksDBEventListener)
    if (flush_job_info.triggered_writes_slowdown ||
        flush_job_info.triggered_writes_stop)
    {
        LOG(INFO) << "[PurgerEventListener] Flush begin, file: "
                  << flush_job_info.file_path
                  << ", job_id: " << flush_job_info.job_id
                  << ", thread: " << flush_job_info.thread_id
                  << ", file_number: " << flush_job_info.file_number
                  << ", triggered_writes_slowdown: "
                  << flush_job_info.triggered_writes_slowdown
                  << ", triggered_writes_stop: "
                  << flush_job_info.triggered_writes_stop
                  << ", smallest_seqno: " << flush_job_info.smallest_seqno
                  << ", largest_seqno: " << flush_job_info.largest_seqno
                  << ", flush_reason: "
                  << GetFlushReason(flush_job_info.flush_reason);
    }

    // Update sliding window with current max file number
    UpdateSlidingWindow(db, flush_job_info.thread_id, flush_job_info.job_id);
}

void PurgerEventListener::OnFlushCompleted(
    rocksdb::DB *db, const rocksdb::FlushJobInfo &flush_job_info)
{
    // Log flush completion event
    if (flush_job_info.triggered_writes_slowdown ||
        flush_job_info.triggered_writes_stop)
    {
        LOG(INFO) << "[PurgerEventListener] Flush completed, file: "
                  << flush_job_info.file_path
                  << ", job_id: " << flush_job_info.job_id
                  << ", thread: " << flush_job_info.thread_id
                  << ", file_number: " << flush_job_info.file_number
                  << ", triggered_writes_slowdown: "
                  << flush_job_info.triggered_writes_slowdown
                  << ", triggered_writes_stop: "
                  << flush_job_info.triggered_writes_stop
                  << ", smallest_seqno: " << flush_job_info.smallest_seqno
                  << ", largest_seqno: " << flush_job_info.largest_seqno
                  << ", flush_reason: "
                  << GetFlushReason(flush_job_info.flush_reason);
    }

    // Remove the entry from sliding window
    if (sliding_window_)
    {
        sliding_window_->RemoveFileNumber(flush_job_info.thread_id,
                                          flush_job_info.job_id);
    }
}

void PurgerEventListener::OnCompactionBegin(
    rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci)
{
    DLOG(INFO) << "[PurgerEventListener] Compaction begin, job_id: "
               << ci.job_id << ", thread: " << ci.thread_id
               << ", output_level: " << ci.output_level
               << ", input_files_size: " << ci.input_files.size()
               << ", compaction_reason: "
               << static_cast<int>(ci.compaction_reason);

    // Update sliding window with current max file number
    UpdateSlidingWindow(db, ci.thread_id, ci.job_id);
}

void PurgerEventListener::OnCompactionCompleted(
    rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci)
{
    DLOG(INFO) << "[PurgerEventListener] Compaction completed, job_id: "
               << ci.job_id << ", thread: " << ci.thread_id
               << ", output_level: " << ci.output_level
               << ", input_files_size: " << ci.input_files.size()
               << ", output_files_size: " << ci.output_files.size()
               << ", compaction_reason: "
               << static_cast<int>(ci.compaction_reason);

    // Remove the entry from sliding window
    if (sliding_window_)
    {
        sliding_window_->RemoveFileNumber(ci.thread_id, ci.job_id);
    }
}

void PurgerEventListener::Stop()
{
    if (sliding_window_)
    {
        sliding_window_->Stop();
        sliding_window_.reset();
    }
}

std::string PurgerEventListener::GetFlushReason(
    rocksdb::FlushReason flush_reason)
{
    switch (flush_reason)
    {
    case rocksdb::FlushReason::kOthers:
        return "Others";
    case rocksdb::FlushReason::kGetLiveFiles:
        return "GetLiveFiles";
    case rocksdb::FlushReason::kShutDown:
        return "ShutDown";
    case rocksdb::FlushReason::kExternalFileIngestion:
        return "ExternalFileIngestion";
    case rocksdb::FlushReason::kManualCompaction:
        return "ManualCompaction";
    case rocksdb::FlushReason::kWriteBufferManager:
        return "WriteBufferManager";
    case rocksdb::FlushReason::kWriteBufferFull:
        return "WriteBufferFull";
    case rocksdb::FlushReason::kTest:
        return "Test";
    case rocksdb::FlushReason::kDeleteFiles:
        return "DeleteFiles";
    case rocksdb::FlushReason::kAutoCompaction:
        return "AutoCompaction";
    case rocksdb::FlushReason::kManualFlush:
        return "ManualFlush";
    case rocksdb::FlushReason::kErrorRecovery:
        return "ErrorRecovery";
    default:
        return "Unknown";
    }
}

void PurgerEventListener::UpdateSlidingWindow(rocksdb::DB *db,
                                              uint64_t thread_id,
                                              uint64_t job_id)
{
    if (!db)
    {
        LOG(ERROR) << "[PurgerEventListener] DB pointer is null";
        return;
    }

    // Get current max file number from RocksDB
    uint64_t max_file_number = db->GetNextFileNumber() - 1;

    if (sliding_window_)
    {
        sliding_window_->AddFileNumber(max_file_number, thread_id, job_id);
    }
}

}  // namespace EloqDS
