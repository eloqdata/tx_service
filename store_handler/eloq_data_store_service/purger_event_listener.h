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

#include <glog/logging.h>
#include <rocksdb/cloud/cloud_storage_provider.h>
#include <rocksdb/listener.h>

#include <memory>
#include <mutex>
#include <string>

#include "purger_sliding_window.h"

namespace EloqDS
{

/**
 * @brief Enhanced EventListener for tracking file numbers to improve purger
 * safety
 *
 * This listener subscribes to FlushBegin and CompactionBegin events to capture
 * the maximum file number at the time of these operations. The file numbers are
 * fed into a sliding window which periodically updates S3 with the smallest
 * file number threshold to prevent premature deletion by the purger.
 */
class PurgerEventListener : public rocksdb::EventListener
{
public:
    /**
     * @brief Constructor for PurgerEventListener
     * @param epoch The epoch string for this DB instance
     * @param bucket_name S3 bucket name
     * @param s3_object_path S3 object path
     * @param storage_provider Cloud storage provider for S3 operations
     * @param entry_duration Duration to keep entries in sliding window even it
     * is deleted (default: 15 seconds, should be less than purger interval,
     * indicating the minimum update frequency, it prevents too frequent
     * updates)
     * @param s3_update_interval Interval for updating S3 file (default: 30
     * seconds, indicating the maximum update frequency)
     */
    PurgerEventListener(
        const std::string &epoch,
        const std::string &bucket_name,
        const std::string &s3_object_path,
        std::shared_ptr<rocksdb::CloudStorageProvider> storage_provider,
        std::chrono::milliseconds entry_duration = std::chrono::seconds(15),
        std::chrono::milliseconds s3_update_interval =
            std::chrono::seconds(30));

    /**
     * @brief Destructor - stops the sliding window
     */
    ~PurgerEventListener();

    /**
     * @brief Update the epoch string
     * @param epoch The new epoch string
     */
    void SetEpoch(const std::string &epoch);

    void BlockPurger();

    /**
     * @brief Called when a flush operation begins
     * @param db Pointer to the database instance
     * @param flush_job_info Information about the flush operation
     */
    void OnFlushBegin(rocksdb::DB *db,
                      const rocksdb::FlushJobInfo &flush_job_info) override;

    /**
     * @brief Called when a flush operation completes
     * @param db Pointer to the database instance
     * @param flush_job_info Information about the flush operation
     */
    void OnFlushCompleted(rocksdb::DB *db,
                          const rocksdb::FlushJobInfo &flush_job_info) override;

    /**
     * @brief Called when a compaction operation begins
     * @param db Pointer to the database instance
     * @param ci Information about the compaction operation
     */
    void OnCompactionBegin(rocksdb::DB *db,
                           const rocksdb::CompactionJobInfo &ci) override;

    /**
     * @brief Called when a compaction operation completes
     * @param db Pointer to the database instance
     * @param ci Information about the compaction operation
     */
    void OnCompactionCompleted(rocksdb::DB *db,
                               const rocksdb::CompactionJobInfo &ci) override;

    /**
     * @brief Stop the event listener and cleanup resources
     */
    void Stop();

    /**
     * @brief Get flush reason string for logging
     * @param flush_reason The flush reason enum value
     * @return String representation of the flush reason
     */
    std::string GetFlushReason(rocksdb::FlushReason flush_reason);

private:
    std::string bucket_name_;
    std::string s3_object_path_;

    std::unique_ptr<SlidingWindow> sliding_window_;

    /**
     * @brief Update sliding window with current max file number from DB
     * @param db Pointer to the database instance
     * @param thread_id The thread ID of the operation (default: 0)
     * @param job_id The job ID of the operation (default: 0)
     */
    void UpdateSlidingWindow(rocksdb::DB *db,
                             uint64_t thread_id = 0,
                             uint64_t job_id = 0);
};

}  // namespace EloqDS
