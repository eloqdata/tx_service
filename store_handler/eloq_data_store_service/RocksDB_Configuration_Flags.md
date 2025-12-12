# RocksDB Configuration Flags Documentation

This document provides a comprehensive overview of the configuration flags available for the EloqData Store Service's RocksDB and RocksDBCloud implementations. The configuration options control various aspects of RocksDB behavior, performance, and resource usage.

## Configuration Organization

All RocksDB and RocksDBCloud configuration flags are:

1. Organized under the `[store]` section in the DSS config INI file
2. Can be set via:
   - Command line flags (highest priority)
   - Values in the INI config file (medium priority)
   - Default values (lowest priority)

Example INI file configuration:

```ini
[store]
rocksdb_info_log_level = INFO
rocksdb_enable_stats = false
rocksdb_storage_path = /path/to/storage
# ... other configuration options ...
```

## RocksDB Flags

### General Configuration

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_info_log_level` | No | `"INFO"` | String (options: "DEBUG", "INFO", "WARN", "ERROR", "FATAL") | RocksDB store info log level |
| `rocksdb_enable_stats` | No | `false` | Boolean | Whether to enable RocksDB statistics |
| `rocksdb_stats_dump_period_sec` | No | `600` | Integer | Statistics dump period in seconds |
| `rocksdb_storage_path` | No | `""` (defaults to eloq_data_path + "/rocksdb_data") | String | Path for RocksDB data storage. For better performance, set to a fast disk. For RocksDBCloud implementation, the disk only needs to be large enough to host the size of rocksdb_cloud_sst_file_cache_size, not all data |
| `rocksdb_query_worker_num` | No | `16` | Integer | Number of async query workers |

### Write Behavior Configuration

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_max_write_buffer_number` | No | `8` | Integer | Maximum number of write buffers |
| `rocksdb_write_buffer_size` | No | `"64MB"` | Size string (number + MB/GB/TB) | Size of a single write buffer |
| `rocksdb_batch_write_size` | No | `"1MB"` | Size string (number + MB/GB/TB) | Batch write size when doing checkpoint |
| `rocksdb_write_rate_limit` | No | `"0MB"` | Size string (number + MB/GB/TB) | Write rate limit in bytes per second (0 means no limit) |

### Compaction Configuration

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_max_background_jobs` | No | `4` | Integer | Maximum number of background jobs |
| `rocksdb_max_background_flush` | No | `0` (automatically adjusted by RocksDB) | Integer | Maximum number of background flush operations |
| `rocksdb_max_background_compaction` | No | `0` (automatically adjusted by RocksDB) | Integer | Maximum number of background compaction operations |
| `rocksdb_compaction_style` | No | `"level"` | String (options: "level", "universal", "fifo", "none") | Compaction style |
| `rocksdb_max_subcompactions` | No | `1` | Integer | Maximum number of subcompactions |
| `rocksdb_periodic_compaction_seconds` | No | `86400` (24 hours) | Integer | SST files older than this value will be picked up for compaction |

### Level-Based Compaction Configuration

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_target_file_size_base` | No | `"64MB"` | Size string (number + MB/GB/TB) | Base file size for levels |
| `rocksdb_target_file_size_multiplier` | No | `1` | Integer | Multiplier for target file size at each level |
| `rocksdb_level0_stop_writes_trigger` | No | `36` | Integer | Number of level-0 files that triggers write stop |
| `rocksdb_level0_slowdown_writes_trigger` | No | `20` | Integer | Number of level-0 files that triggers write slowdown |
| `rocksdb_level0_file_num_compaction_trigger` | No | `4` | Integer | Number of level-0 files that triggers compaction |
| `rocksdb_max_bytes_for_level_base` | No | `"256MB"` | Size string (number + MB/GB/TB) | Maximum bytes for base level |
| `rocksdb_max_bytes_for_level_multiplier` | No | `10` | Integer | Multiplier for max bytes at each level |

### I/O Configuration

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_use_direct_io_for_flush_and_compaction` | No | `false` | Boolean | Whether to use direct I/O for flush and compaction |
| `rocksdb_use_direct_io_for_read` | No | `false` | Boolean | Whether to use direct I/O for read operations |

### Compaction Management

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_soft_pending_compaction_bytes_limit` | No | `"64GB"` | Size string (number + MB/GB/TB) | Soft limit for pending compaction bytes |
| `rocksdb_hard_pending_compaction_bytes_limit` | No | `"256GB"` | Size string (number + MB/GB/TB) | Hard limit for pending compaction bytes |
| `rocksdb_dialy_offpeak_time_utc` | No | Local time zone's 00:00-05:00 converted to UTC | Time range in format "HH:MM-HH:MM" | Daily off-peak time in UTC for scheduling compactions |

## RocksDBCloud Flags

These flags are only applicable when RocksDB Cloud is enabled with either S3 or GCS backends.

### Cloud Storage Configuration

**New URL-based Configuration (Recommended):**

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_cloud_oss_url` | No | `""` | OSS URL | Complete Object Storage Service URL. Supports s3://, gs://, http://, and https://. Takes precedence over legacy config if both provided. |

**URL Format Examples:**
- AWS S3: `s3://my-bucket/my-object-path`
- Google Cloud Storage: `gs://my-bucket/my-object-path`
- MinIO (HTTP): `http://localhost:9000/my-bucket/my-object-path`
- S3-compatible (HTTPS): `https://s3.amazonaws.com/my-bucket/my-object-path`

**URL Format Specification:**
- S3: `s3://{bucket_name}/{object_path}`
- GCS: `gs://{bucket_name}/{object_path}`
- HTTP/HTTPS: `http(s)://{host}:{port}/{bucket_name}/{object_path}`

**Supported Protocols:** `s3://`, `gs://`, `http://`, `https://`

**Legacy Configuration (Deprecated, use URL-based config instead):**

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_cloud_bucket_name` | No | `"rocksdb-cloud-test"` | String | Cloud storage bucket name (deprecated) |
| `rocksdb_cloud_bucket_prefix` | No | `"eloqkv-"` | String | Prefix for objects in the bucket (deprecated, not supported in URL config) |
| `rocksdb_cloud_object_path` | No | `"rocksdb_cloud"` | String | Path within the bucket (deprecated) |
| `rocksdb_cloud_s3_endpoint_url` | No | `""` | String | S3-compatible object store endpoint URL (deprecated) |

**Note:** If both `rocksdb_cloud_oss_url` and legacy options are provided, the URL-based configuration takes precedence and overrides the legacy settings.

**Other Cloud Configuration:**

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_cloud_region` | No | `"ap-northeast-1"` | String | Cloud region |

### Cloud Cache Configuration

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_cloud_sst_file_cache_size` | No | `"20GB"` | Size string (number + MB/GB/TB) | Size of SST file cache |
| `rocksdb_cloud_sst_file_cache_num_shard_bits` | No | `5` | Integer | Number of shard bits for the SST file cache (2^5 = 32 shards) |

### Cloud Operation Configuration

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `rocksdb_cloud_db_ready_timeout_sec` | No | `10` | Integer | Cloud DB ready timeout in seconds |
| `rocksdb_cloud_db_file_deletion_delay_sec` | No | `3600` | Integer | Delay for file deletion in cloud storage (seconds) |
| `rocksdb_cloud_warm_up_thread_num` | No | `1` | Integer | Number of warm-up threads |
| `rocksdb_cloud_purger_periodicity_secs` | No | `600` (10 minutes) | Integer | Periodicity for cloud purger in seconds |

### AWS S3-specific Configuration

These flags are only applicable when using S3 as the cloud storage backend.

| Flag Name | Required | Default Value | Format | Description |
|-----------|----------|--------------|--------|-------------|
| `aws_access_key_id` | No | `""` | String | AWS access key ID |
| `aws_secret_key` | No | `""` | String | AWS secret key |

## Value Format Requirements

Different flags have different format requirements:

1. **String Flags**: Can be any valid string value.
2. **Boolean Flags**: Must be `true` or `false`.
3. **Integer Flags**: Must be valid integers.
4. **Size Flags**: Must be in the format of a number followed by `MB`, `GB`, or `TB` (case insensitive). For example: `64MB`, `20GB`, `1TB`.
5. **Time Range Flags**: Must be in the format `HH:MM-HH:MM` in UTC time.

## Notes

1. All flags have default values and are therefore not strictly required.
2. RocksDB will automatically adjust some parameters if given 0 (like `rocksdb_max_background_flush` and `rocksdb_max_background_compaction`).
3. The storage path defaults to `eloq_data_path + "/rocksdb_data"` if not specified.
4. Size values must include units (`MB`, `GB`, or `TB`).
5. A write rate limit of "0MB" means no limit is applied.
6. The off-peak time is converted from local time to UTC by default.

