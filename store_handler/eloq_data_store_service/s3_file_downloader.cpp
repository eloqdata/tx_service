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
#include "s3_file_downloader.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <algorithm>
#include <fstream>
#include <glog/logging.h>

namespace EloqDS
{

S3FileDownloader::S3FileDownloader(const std::string &s3_url,
                                   const std::string &region,
                                   const std::string &aws_access_key_id,
                                   const std::string &aws_secret_key,
                                   const std::string &s3_endpoint_url)
{
    // Parse S3 URL: s3://bucket-name/object-path-prefix
    // Example: s3://my-bucket/my-prefix/ -> bucket_name_ = "my-bucket", object_path_prefix_ = "my-prefix/"
    if (s3_url.find("s3://") != 0)
    {
        LOG(ERROR) << "Invalid S3 URL format, must start with s3://: " << s3_url;
        return;
    }
    
    std::string url_without_scheme = s3_url.substr(5);  // Remove "s3://"
    size_t slash_pos = url_without_scheme.find('/');
    
    if (slash_pos == std::string::npos)
    {
        // No object path prefix, just bucket name
        bucket_name_ = url_without_scheme;
        object_path_prefix_ = "";
    }
    else
    {
        bucket_name_ = url_without_scheme.substr(0, slash_pos);
        object_path_prefix_ = url_without_scheme.substr(slash_pos + 1);
        // Ensure object_path_prefix_ ends with '/' if not empty
        if (!object_path_prefix_.empty() && object_path_prefix_.back() != '/')
        {
            object_path_prefix_ += "/";
        }
    }
    
    if (bucket_name_.empty())
    {
        LOG(ERROR) << "Invalid S3 URL: bucket name is empty";
        return;
    }
    
    Aws::Client::ClientConfiguration config;
    config.region = region;
    
    if (!s3_endpoint_url.empty())
    {
        config.endpointOverride = s3_endpoint_url;
        // Determine scheme from endpoint
        std::string lower_endpoint = s3_endpoint_url;
        std::transform(lower_endpoint.begin(), lower_endpoint.end(),
                      lower_endpoint.begin(), ::tolower);
        if (lower_endpoint.find("https://") == 0)
        {
            config.scheme = Aws::Http::Scheme::HTTPS;
            config.verifySSL = true;  // keep TLS verification for HTTPS
        }
        else
        {
            config.scheme = Aws::Http::Scheme::HTTP;
            config.verifySSL = false;
        }
    }
    
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    if (!aws_access_key_id.empty() && !aws_secret_key.empty())
    {
        credentials_provider = 
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
                Aws::String(aws_access_key_id.c_str()), 
                Aws::String(aws_secret_key.c_str()));
    }
    else
    {
        credentials_provider = 
            std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
    }
    
    s3_client_ = std::make_shared<Aws::S3::S3Client>(
        credentials_provider,
        config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        true /* useVirtualAddressing */);
}

S3FileDownloader::~S3FileDownloader() = default;

bool S3FileDownloader::DownloadFile(const std::string &s3_file_name,
                                    const std::string &local_file_path)
{
    // Construct S3 object key: {object_path_prefix}{file_name}
    std::string s3_key = object_path_prefix_ + s3_file_name;
    
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_name_);
    request.SetKey(s3_key);
    
    auto outcome = s3_client_->GetObject(request);
    
    if (!outcome.IsSuccess())
    {
        LOG(ERROR) << "Failed to download " << s3_file_name 
                   << " from S3: " << outcome.GetError().GetMessage();
        return false;
    }
    
    // Write to local file
    std::ofstream out_file(local_file_path, std::ios::binary);
    if (!out_file.is_open())
    {
        LOG(ERROR) << "Failed to open local file for writing: " << local_file_path;
        return false;
    }
    
    auto &body = outcome.GetResult().GetBody();
    out_file << body.rdbuf();
    out_file.close();
    
    if (!out_file.good())
    {
        LOG(ERROR) << "Failed to write file: " << local_file_path;
        return false;
    }
    
    DLOG(INFO) << "Downloaded " << s3_file_name << " to " << local_file_path;
    return true;
}

}  // namespace EloqDS

