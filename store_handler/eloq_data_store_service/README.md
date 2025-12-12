# EloqData DataStore Service (DSS)

The DataStore Service (DSS) is a component of the EloqData system that provides storage functionality through various backend implementations. It serves as a persistent storage layer for EloqData KV.

## Overview

DSS is designed to work with different storage backends, including:
- RocksDB
- RocksDB Cloud with S3
- RocksDB Cloud with GCS (TODO)
- EloqStore

The service provides a unified interface for data storage operations regardless of the underlying storage technology.

## Building

The DataStore Service is built as part of the EloqData KV project. The main executable is called `dss_server`.

### Building with Different Storage Backends

The DataStore Service can be built with different storage backends. Use the following CMake commands to build with your preferred backend:

#### 1. Building with RocksDB
```bash
cmake -DWITH_DATA_STORE=ELOQDSS_ROCKSDB ..
make -j$(nproc)
```

#### 2. Building with RocksDB Cloud S3
```bash
cmake -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 ..
make -j$(nproc)
```

#### 3. Building with RocksDB Cloud GCS
```bash
cmake -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_GCS ..
make -j$(nproc)
```

#### 4. Building with EloqStore
```bash
cmake -DWITH_DATA_STORE=ELOQDSS_ELOQSTORE ..
make -j$(nproc)
```

Each build option sets specific compiler definitions that determine which storage backend implementation will be used.

## Configuration

### Command Line Flags

The `dss_server` program supports the following command line flags:

| Flag | Default | Required | Description |
|------|---------|----------|-------------|
| `--config` | `""` | No | Path to configuration file (*.ini). If provided, settings in this file take precedence over command line arguments unless explicitly overridden. |
| `--eloq_dss_peer_node` | `""` | No | Data store peer node address. Used to get cluster topology if data_store_config_file is not provided. If empty and no configuration exists, a single-node configuration will be created. |
| `--ip` | `"127.0.0.1"` | No | Server IP address that the DSS will bind to. |
| `--port` | `9100` | No | Server port that the DSS will listen on. |
| `--data_path` | `"./data"` | No | Directory path to save data. The DSS will create this directory if it does not exist. |
| `--log_file_name_prefix` | `"eloq_dss.log"` | No | Sets the prefix for log files. |
| `--enable_cache_replacement` | `true` | No | Enable cache replacement for supported storage backends. |
| `--bootstrap` | `false` | No | Init data store config file and exit. Currently only supports bootstrapping one node. |
| `--alsologtostderr` | `false` | No | Log to standard error as well as to log files. |
| `--log_dir` | OS-dependent | No | Directory where log files will be written. |

### Configuration File

DSS can also be configured through an INI-format configuration file, specified with the `--config` flag. Settings in this file take precedence over command line arguments unless explicitly overridden.

## Running

To start the DataStore Service:

```bash
./dss_server [options]
```

### Usage Examples

#### Basic Deployment Scenarios

1. **Start with default settings (single-node mode)**:
   ```bash
   ./dss_server
   ```
   This creates a standalone server on localhost:9100 with data stored in ./data

2. **Start with custom IP and port**:
   ```bash
   ./dss_server --ip=192.168.1.100 --port=9200
   ```

3. **Start with a configuration file**:
   ```bash
   ./dss_server --config=/path/to/dss_config.ini
   ```

4. **Bootstrap a new node**:
   ```bash
   ./dss_server --bootstrap --ip=192.168.1.100 --port=9200 --data_path=/path/to/data
   ```
   This initializes the node configuration and exits without starting the server

5. **Join an existing cluster**:
   ```bash
   ./dss_server --eloq_dss_peer_node=192.168.1.101:9100 --ip=192.168.1.102 --port=9100
   ```
   This connects to an existing peer node to retrieve cluster topology

#### Advanced Configuration Examples

6. **Configure with specific RocksDB settings**:
   ```bash
   ./dss_server --config=/path/to/config.ini
   ```
   
   Where config.ini contains:
   ```ini
   [store]
   rocksdb_info_log_level = INFO
   rocksdb_enable_stats = true
   rocksdb_max_write_buffer_number = 16
   rocksdb_write_buffer_size = 128MB
   rocksdb_max_background_jobs = 8
   ```

7. **Configure with RocksDB Cloud S3 settings**:
   ```bash
   ./dss_server --config=/path/to/s3_config.ini
   ```
   
   Where s3_config.ini contains (legacy format):
   ```ini
   [store]
   rocksdb_cloud_bucket_name = my-eloqdata-bucket
   rocksdb_cloud_bucket_prefix = prod-
   rocksdb_cloud_region = us-west-2
   rocksdb_cloud_sst_file_cache_size = 40GB
   aws_access_key_id = YOUR_ACCESS_KEY
   aws_secret_key = YOUR_SECRET_KEY
   ```

   **New URL-based configuration (Recommended):**
   ```ini
   [store]
   rocksdb_cloud_oss_url = s3://my-eloqdata-bucket/rocksdb_cloud
   rocksdb_cloud_region = us-west-2
   rocksdb_cloud_sst_file_cache_size = 40GB
   aws_access_key_id = YOUR_ACCESS_KEY
   aws_secret_key = YOUR_SECRET_KEY
   ```

   **For Google Cloud Storage:**
   ```ini
   [store]
   rocksdb_cloud_oss_url = gs://my-eloqdata-bucket/rocksdb_cloud
   rocksdb_cloud_region = us-west-2
   rocksdb_cloud_sst_file_cache_size = 40GB
   ```

   **For MinIO or S3-compatible storage:**
   ```ini
   [store]
   rocksdb_cloud_oss_url = http://localhost:9000/my-bucket/rocksdb_data
   rocksdb_cloud_region = us-east-1
   rocksdb_cloud_sst_file_cache_size = 40GB
   aws_access_key_id = minioadmin
   aws_secret_key = minioadmin
   ```

8. **Run with logging to stderr**:
   ```bash
   ./dss_server --alsologtostderr --ip=192.168.1.100 --port=9200
   ```

9. **Run with custom log directory and prefix**:
   ```bash
   ./dss_server --log_dir=/var/log/eloqdata --log_file_name_prefix=dss_prod.log
   ```

10. **Disable cache replacement for performance testing**:
    ```bash
    ./dss_server --enable_cache_replacement=false
    ```

#### Production Deployment Example

For a production deployment with multiple nodes, you would typically:

1. Bootstrap the first node:
   ```bash
   ./dss_server --bootstrap --ip=192.168.1.100 --port=9100 --data_path=/data/eloqdata/node1 --config=/etc/eloqdata/dss_prod.ini
   ```

2. Start the first node:
   ```bash
   ./dss_server --ip=192.168.1.100 --port=9100 --data_path=/data/eloqdata/node1 --config=/etc/eloqdata/dss_prod.ini
   ```

3. Join additional nodes to the cluster:
   ```bash
   ./dss_server --eloq_dss_peer_node=192.168.1.100:9100 --ip=192.168.1.101 --port=9100 --data_path=/data/eloqdata/node2 --config=/etc/eloqdata/dss_prod.ini
   ```

## RocksDB Cloud Configuration Migration Guide

### Overview

Starting from this version, we've introduced a simplified URL-based configuration for RocksDB Cloud (S3/GCS) to reduce configuration complexity. The new `rocksdb_cloud_oss_url` option consolidates multiple configuration parameters into a single URL.

### Why Migrate?

The legacy configuration required multiple separate parameters:
- `rocksdb_cloud_bucket_name`
- `rocksdb_cloud_bucket_prefix`
- `rocksdb_cloud_object_path`
- `rocksdb_cloud_s3_endpoint_url`

The new URL-based configuration simplifies this to a single parameter that's easier to understand and manage.

### Migration Examples

#### Example 1: Standard AWS S3 Configuration

**Legacy Configuration:**
```ini
[store]
rocksdb_cloud_bucket_name = my-production-bucket
rocksdb_cloud_bucket_prefix = eloqkv-
rocksdb_cloud_object_path = rocksdb_data
```

**New URL-based Configuration:**
```ini
[store]
rocksdb_cloud_oss_url = s3://my-production-bucket/rocksdb_data
```

**Note:** The `bucket_prefix` is not supported in URL-based configuration. If you were using `eloqkv-my-production-bucket`, you should include the prefix in the bucket name: `s3://eloqkv-my-production-bucket/rocksdb_data`

#### Example 2: MinIO or S3-compatible Storage

**Legacy Configuration:**
```ini
[store]
rocksdb_cloud_bucket_name = test-bucket
rocksdb_cloud_object_path = eloqdata
rocksdb_cloud_s3_endpoint_url = http://localhost:9000
```

**New URL-based Configuration:**
```ini
[store]
rocksdb_cloud_oss_url = http://localhost:9000/test-bucket/eloqdata
```

#### Example 3: HTTPS S3-compatible Storage

**Legacy Configuration:**
```ini
[store]
rocksdb_cloud_bucket_name = my-bucket
rocksdb_cloud_object_path = data/rocksdb
rocksdb_cloud_s3_endpoint_url = https://s3.custom-provider.com
```

**New URL-based Configuration:**
```ini
[store]
rocksdb_cloud_oss_url = https://s3.custom-provider.com/my-bucket/data/rocksdb
```

### TxLog (Log Service) Configuration

The same migration applies to transaction log service configuration, with `txlog_` prefix:

**Legacy:**
```ini
[local]
txlog_rocksdb_cloud_bucket_name = txlog-bucket
txlog_rocksdb_cloud_bucket_prefix = txlog-
txlog_rocksdb_cloud_object_path = logs
txlog_rocksdb_cloud_endpoint_url = 
```

**New:**
```ini
[local]
txlog_rocksdb_cloud_oss_url = s3://txlog-bucket/logs
```

### Important Notes

1. **Precedence:** If both the new URL-based configuration and legacy configuration are provided, the URL-based configuration takes precedence and overrides the legacy settings.

2. **Bucket Prefix Deprecation:** The `bucket_prefix` parameter is not supported in URL-based configuration. If you need a prefix, include it in the bucket name within the URL.

3. **Backward Compatibility:** The legacy configuration options are still supported and will continue to work. However, we recommend migrating to the URL-based configuration for better maintainability.

4. **Protocol Support:** The following protocols are supported:
   - `s3://` - AWS S3 (default endpoint)
   - `gs://` - Google Cloud Storage
   - `http://` - Custom S3-compatible endpoint (HTTP)
   - `https://` - Custom S3-compatible endpoint (HTTPS)

5. **Validation:** Invalid URLs will cause the application to fail at startup with a descriptive error message.

## Storage Backend Configuration

The DataStore Service is compiled with support for specific backend storage technologies. The build defines determine which backend is used. Additional backend-specific configuration can be set in the configuration file.

### Supported Storage Backends

#### 1. RocksDB (`DATA_STORE_TYPE_ELOQDSS_ROCKSDB`)
Standard RocksDB implementation for local storage. This backend provides:
- Local persistent storage
- High-performance key-value operations
- Custom compaction strategies
- LSM-tree based storage model

#### 2. RocksDB Cloud with S3 (`DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3`)
RocksDB with Amazon S3 as the persistent storage layer. This backend:
- Stores data in S3 buckets
- Maintains a local cache for frequently accessed data
- Provides seamless integration with AWS services
- Requires AWS credentials configuration

#### 3. RocksDB Cloud with GCS (`DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS`)
RocksDB with Google Cloud Storage as the persistent storage layer. This backend:
- Stores data in GCS buckets
- Maintains a local cache for frequently accessed data
- Provides seamless integration with Google Cloud services

#### 4. EloqStore (`DATA_STORE_TYPE_ELOQDSS_ELOQSTORE`)
EloqData's custom storage solution. This backend:
- Provides specialized storage optimized for EloqData workloads
- Includes custom features specific to EloqData requirements

For detailed RocksDB and RocksDBCloud configuration options, refer to the [RocksDB Configuration Flags](RocksDB_Configuration_Flags.md) documentation.

## Operations

### Data Storage

The DSS handles data operations through its RPC interface, including:
- Get/Put operations
- Batch operations
- Migrations (TODO)
- Replication (TODO)

### Cluster Management (TODO)

For multi-node configurations, DSS manages:
- Cluster topology
- Shard assignment
- Node discovery

## Logging

Logs are written to the directory specified by `--log_dir` with the prefix specified by `--log_file_name_prefix`. To also log to standard error, use the `--alsologtostderr` flag.

