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

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

#include <memory>
#include <string>

namespace EloqDS
{

class S3FileDownloader
{
public:
    /**
     * @brief Construct S3FileDownloader from S3 URL
     * @param s3_url S3 URL in format: s3://bucket-name/object-path-prefix
     *                Example: s3://my-bucket/my-prefix/
     * @param region AWS region
     * @param aws_access_key_id AWS access key ID (empty to use default
     * provider)
     * @param aws_secret_key AWS secret key (empty to use default provider)
     * @param s3_endpoint_url Custom S3 endpoint URL (empty for default AWS
     * endpoint)
     */
    S3FileDownloader(const std::string &s3_url,
                     const std::string &region,
                     const std::string &aws_access_key_id,
                     const std::string &aws_secret_key,
                     const std::string &s3_endpoint_url = "");

    ~S3FileDownloader();

    /**
     * @brief Download a file from S3 to local path
     * @param s3_file_name File name in S3 (e.g., "000123.sst")
     * @param local_file_path Full local path to save the file
     * @return true if download succeeded, false otherwise
     */
    bool DownloadFile(const std::string &s3_file_name,
                      const std::string &local_file_path);

private:
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    std::string bucket_name_;
    std::string object_path_prefix_;
};

}  // namespace EloqDS
