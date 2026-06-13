# Store Handler (Persistent Storage)

The store handler is the persistence layer below the in-memory cc maps: it stores checkpointed/cold row data plus all durable metadata (table catalogs, range/slice maps, statistics, databases, MVCC archives) behind the abstract interface `txservice::store::DataStoreHandler` (`tx_service/include/store/data_store_handler.h`). The default backend is **EloqDSS** — a brpc `DataStoreService` (`store_handler/eloq_data_store_service/`) that owns numbered *data shards*, each backed by a pluggable `DataStore` engine (EloqStore, RocksDB, or RocksDB-Cloud on S3/GCS) — accessed from the tx side by `EloqDS::DataStoreServiceClient` (`store_handler/data_store_service_client.h/.cpp`), which maps kv partitions to DSS shards, bypasses RPC entirely when the owning shard lives in the same process, and retries via re-resolvable closures when a shard moves. Legacy direct handlers (embedded RocksDB, DynamoDB, BigTable) implement the same interface without the DSS layer.

Related docs: [01-architecture-overview.md](01-architecture-overview.md) (startup wiring), [03-concurrency-control.md](03-concurrency-control.md) (CcRequest model the callbacks complete into), [07-durability-and-recovery.md](07-durability-and-recovery.md) (checkpointer/data-sync, who calls `PutAll`), [08-range-and-bucket-management.md](08-range-and-bucket-management.md) (ranges/slices/buckets persisted here), [10-log-service.md](10-log-service.md) (the WAL that makes unflushed data durable).

## 1. Where It Sits and the Async Contract

Callers in the tx service (all verified call sites):

- **Checkpoint / data sync** — `checkpointer.cpp:571` and `local_cc_shards.cpp:5937` call `PutAll(flush_task, ...)` to write dirty entries, then `PersistKV()` (`local_cc_shards.cpp:5964`) to force durability. The data-sync worker passes `yield_fptr`/`resume_fptr` coroutine hooks so the flushing bthread parks while RPCs are in flight.
- **Cache-miss reads** — `CcShard::FetchRecord` (`cc_shard.cpp:2009`) and the cc maps (`template_cc_map.h`, `object_cc_map.h`) call `FetchRecord(FetchRecordCc*)` when a key isn't cached.
- **Slice loads** — `range_slice.cpp:649/711` calls `LoadRangeSlice(FillStoreSliceCc*)` to page a range slice into memory.
- **DDL / metadata** — schema ops call `UpsertTable`; catalog/range/statistics fills call `FetchTableCatalog`, `FetchTableRanges`, `FetchRangeSlices`, `FetchTableStatistics`; range splits call `UpsertRanges` / `UpdateRangeSlices`.
- **Archives & snapshots** — MVCC archive flushes (`PutArchivesAll`, `CopyBaseToArchive`), and `SnapshotManager` (`tx_service/include/store/snapshot_manager.h`) drives standby snapshot sync and backups through `CreateSnapshotForStandby/Backup`, `OnSnapshotReceived`, etc. SnapshotManager is a singleton holding the handler pointer, a standby-sync worker thread, and a backup task queue.

**Async contract.** Most data-plane calls take a `CcRequest`-derived object (`FetchRecordCc`, `FetchCatalogCc`, `FillStoreSliceCc`, ...) and return immediately; the handler completes the request later from an RPC callback by setting fields and re-enqueueing it to its CcShard (e.g. `fetch_cc->SetFinish(0)` in `FetchRecordCallback`, `data_store_service_client_closure.cpp:133`). The return value is `DataStoreOpStatus { Success, Retry, Error }`: `Success` means *scheduled* (result arrives via the cc request), `Retry` is returned by the cc-shard layer when the handler is saturated (`cc_shard.cpp:1979`), `Error` fails the request synchronously. Metadata/DDL bulk calls (`FetchTable`, `UpsertDatabase`, `UpdateRangeSlices`, `PutAll`, ...) are synchronous bools but accept optional `yield_fptr`/`resume_fptr` so a coroutine caller never blocks a worker thread.

## 2. The DataStoreHandler Interface by Purpose

All in `tx_service/include/store/data_store_handler.h`:

| Group | Methods | Notes |
|---|---|---|
| Data plane | `PutAll`, `PersistKV`, `Read` (sync; unimplemented in DSS client), `FetchRecord`, `FetchBucketData`, `LoadRangeSlice`, `DeleteOutOfRangeData`, `CreateDataSerachCondition` | `PutAll` takes `{kv_table_name → FlushTaskEntry vec}`; `PersistKV` marks end-of-checkpoint durability; `CreateDataSerachCondition` builds pushdown filters (DSS pushes an object-type equality filter) |
| DDL | `UpsertTable`, `CreateKVCatalogInfo`, `CreateNewKVCatalogInfo`, `DeserializeKVCatalogInfo`, `DropKvTable(Async)`, `AppendPreBuiltTable` | `UpsertTable` completes a `CcHandlerResult<Void>` or re-enqueues a CcRequest; `write_time` keeps eventual-consistency stores idempotent |
| Metadata | `FetchTableCatalog`, `FetchTable`, `FetchTableRanges`, `FetchRangeSlices`, `FetchTableRangeSize`, `Fetch(Current)TableStatistics`, `UpsertTableStatistics`, `UpsertRanges`, `UpdateRangeSlices`, `DiscoverAllTableNames` | Range/slice persistence backs [08](08-range-and-bucket-management.md) |
| Databases | `UpsertDatabase`, `DropDatabase`, `FetchDatabase`, `FetchAllDatabase` | per-engine (`TableEngine`) namespace records |
| Archives (MVCC) | `PutArchivesAll`, `CopyBaseToArchive`, `FetchVisibleArchive`, `FetchArchives` | historical versions, see [07](07-durability-and-recovery.md) |
| Snapshots / standby / backup | `CreateSnapshotForStandby/Backup`, `SendSnapshotToRemote`, `OnSnapshotReceived`, `OnUpdateStandbyCkptTs`, `RequestSyncSnapshot`, `DeleteStandbySnapshot(sBefore)`, `CurrentStandbySnapshotTs`, `SnapshotSyncDestPath`, `RemoveBackupSnapshot` | driven by `SnapshotManager`; mostly assert-stubs except in backends that support them |
| Lifecycle | `Connect`, `ScheduleTimerTasks`, `OnLeaderStart/Stop`, `OnStartFollowing`, `OnShutdown`, `RestoreTxCache`, `ApproxStoreKeyCount`, `CompactStore` | `RestoreTxCache` reloads cc maps from kv on startup (implemented by RocksDBHandler, not the DSS client) |

### Capability flags

| Flag | Meaning | Verified consumers |
|---|---|---|
| `IsSharedStorage()` (default true) | kv store is reachable by all nodes (cloud/remote) vs per-node local disk | standby skips checkpointing when shared (`checkpointer.cpp:207`); `VersionedLruEntry::IsPersistent` trusts the leader's ckpt-ts on shared storage (`cc_entry.cpp:64`) — i.e. cache eviction correctness depends on it; failover/recovery paths in `fault/cc_node.cpp`, `snapshot_manager.cpp` |
| `NeedCopyRange()` (pure virtual) | range split must physically copy rows to the new range's partition | `local_cc_shards.cpp:2623` (range split flush). True for DSS client and RocksDB handler, false for BigTable |
| `ByPassDataStore()` (default false) | skip kv writes entirely (DDL-only test mode) | `tx_index_operation.cpp:894`; only BigTable returns non-constant (`ddl_skip_kv_ && !is_bootstrap_`) |
| `NeedPersistKV()` (default false) | `PersistKV` is meaningful (write path buffers data; needs an explicit flush) | true for DSS client and RocksDB handler — their `PutAll` batches are written with `skip_wal=true` and made durable only by `PersistKV`→`FlushData` |

## 3. EloqDSS Architecture

### Deployment and wiring (`core/src/storage_init.cpp`)

`WITH_DATA_STORE` (top-level `CMakeLists.txt:9`, default `ELOQDSS_ELOQSTORE`; options `DYNAMODB`, `BIGTABLE`, `ROCKSDB`, `ELOQDSS_ROCKSDB`, `ELOQDSS_ROCKSDB_CLOUD_S3/GCS`, `ELOQDSS_ELOQSTORE`) selects one backend at compile time via `DATA_STORE_TYPE_*` defines. RocksDB-based choices force a matching `WITH_LOG_STATE` (fatal configure error on mismatch). `build_eloq_store.cmake` builds the vendored EloqStore submodule (C++20, boost::context coroutines, liburing, AWS S3 SDK) when the EloqStore backend is chosen.

For all `ELOQDSS_*` variants, `DataSubstrate::InitializeStorageHandler` always starts an **in-process** `DataStoreService` (data path `<data_path>/eloq_dss`, DSS port = tx port + 7 via `TxPort2DssPort`) and then wraps it in a `DataStoreServiceClient`. Two topology sources:

- `eloq_dss_peer_node` set → fetch the DSS cluster config from a running peer (`DataStoreService::FetchConfigFromPeer`, plain brpc `FetchDSSClusterConfig` with channel-init retries). This node may own zero shards (pure client).
- otherwise (bootstrap / single node) → derive the DSS config from the tx node-group config (`TxConfigsToDssClusterConfig`): one DSS shard per node group, the NG leader is the shard owner, candidates are members.

The client is constructed with `bind_data_shard_with_ng = eloq_dss_peer_node.empty()` (DSS shard ownership follows tx NG leadership) and a pointer to the in-process `DataStoreService`, enabling the local bypass.

### Key sharding: partition id → bucket → DSS shard

Two partition-id spaces arrive from the tx layer (the id passed to the client is used as the **kv partition id** unchanged — `KvPartitionIdOf(key_partition, is_range)` is the identity):

- **Hash-partitioned tables** (e.g. EloqKV objects): `partition_id = key_hash % 1024` (`Sharder::MapKeyHashToHashPartitionId`, `total_hash_partitions = 1024`).
- **Range-partitioned tables**: `partition_id` is the range id.

`GetShardIdByPartitionId(partition_id, is_range_partition)` (`data_store_service_client.cpp:4241`) maps to a bucket — hash partitions by `partition_id % 1024`, range partitions by `MurmurHash3(range_id) % 1024` (`sharder.h:209,239`) — then looks the bucket up in `bucket_infos_`, built deterministically at startup by `InitBucketsInfo`: each DSS shard id seeds an `std::mt19937` and claims 64 virtual nodes on a 1024-bucket consistent-hash ring. **The same partition id means different buckets depending on `is_range_partition` — passing the wrong flag silently routes to the wrong shard.**

Fixed kv partitions for metadata: catalog and database records live at kv partition 0 of their system tables (`FetchTableCatalog`/`UpsertCatalog` hardcode partition 0); statistics use `hash(table_name) % 1024`; range-slice segments use a separate 32-partition space (`KvPartitionIdOfRangeSlices = hash(table, range_id) % 32`).

### DataStoreServiceClient: closures, local bypass, retries

Every KV-level op (`Read`, `BatchWriteRecords`, `DeleteRange`, `FlushData`, `DropTable`, `ScanNext/Close`, `CreateSnapshotForBackup`) has a pooled closure (`data_store_service_client_closure.h`, thread-local `ObjectPool`s from `eloq_data_store_service/object_pool.h` — `Poolable` objects with `Use/Free/Clear` and `PoolableGuard` RAII). The `*Internal(closure)` dispatcher checks `IsLocalShard(shard_id)` (= colocated `DataStoreService::IsOwnerOfShard`): if local, it calls the service **directly in-process** writing results into closure-local fields (no protobuf serialization, no socket); otherwise it sends a brpc call (60 s timeout) through a cached channel. Accessors like `closure->Value()`/`Ts()`/`Result()` transparently read the local or remote variant.

Shard-owner caching: `dss_shards_[shard_id]` (atomic index) points into `dss_nodes_[1024]` slots holding `{host, port, brpc::Channel, shard_version, expired_ts}`. On RPC channel failure (other than `EOVERCROWDED`/`EAGAIN`/timeout) the closure calls `UpdateOwnerNodeIndexOfShard` to CAS-expire the node slot and rebuild the channel; expired slots are recycled after 10 s (`FindFreeNodeIndex`).

Retry semantics (`ReadClosure::Run` et al.): on `REQUESTED_NODE_NOT_OWNER` the server attaches `new_key_sharding` (new primary node + `shard_version`) to `CommonResult`; the client's `HandleShardingError` spins on `UpgradeShardVersion` (CAS on the node slot, `bthread_usleep(10ms)` backoff) and re-issues the request, up to `retry_limit_ = 2` retries per closure. A `NodeGroupChanged` sharding error is currently `LOG(FATAL)` (topology change handling is a TODO at `data_store_service_client.cpp:4471`). `SetupConfig` (registered as the DSS `update_config_listener_`) applies pushed topology updates guarded by `dss_topology_version_`.

`PutAll` (`PutAllImpl`) groups `FlushRecord`s by kv partition, builds ≤64 MB `BatchWriteRecords` batches (`MAX_WRITE_BATCH_SIZE`), and flushes **partitions concurrently but each partition serially** — `PartitionBatchCallback` chains the next batch of a partition only after the previous one completes; `SyncPutAllData`/`SyncConcurrentRequest` (max 32 in-flight) coordinate completion and support coroutine yield/resume. All checkpoint batches are written with `skip_wal=true`; durability comes from the subsequent `PersistKV` → `FlushData` across **all** shards. Synchronous helpers (`FetchTable`, `UpsertDatabase`, ...) use `SyncCallbackData` (bthread mutex/condvar, or yield/resume when provided). `UpsertTable` runs on a dedicated 1-thread `upsert_table_worker_` after pinning the node group and checking the tx term.

Scans: `DataStoreServiceScanner` / `SinglePartitionScanner` (`store_handler/data_store_service_scanner.h`) implement `store::DataStoreScanner` (`tx_service/include/store/data_store_scanner.h`: `Current/MoveNext/End`) by fanning `ScanNext` RPCs over partitions and merge-sorting with the heap helpers in `store_handler/kv_store.h` (`ScanHeapTuple`, `CacheCompare`). Server-side scan sessions are identified by `session_id`.

### ds_request.proto RPC catalog

| RPC | Purpose |
|---|---|
| `Read`, `FetchRecords`, `BatchWriteRecords`, `DeleteRange`, `FlushData` | point read, batched read, batched put/delete (with per-item ts/ttl/op, `skip_wal`), range delete, force-flush |
| `ScanNext`, `ScanClose` | sessioned scans with pushdown `SearchCondition`s |
| `CreateTable`, `DropTable` | kv table DDL |
| `GetApproxStoreKeyCount`, `CompactStore` | stats / manual compaction |
| `FetchDSSClusterConfig`, `UpdateDSSClusterConfig`, `UpdateDSShardConfig` | topology bootstrap & push |
| `ShardMigrate`, `ShardMigrateStatus`, `OpenDSShard`, `SwitchDSShardMode` | shard migration/failover (below) |
| `CreateSnapshotForBackup`, `SyncFileCache` | backup snapshot; primary→standby SST file-cache warm-up |
| `FaultInjectForTest` | test-only fault injection |

Every data response embeds `CommonResult {error_code, error_msg, new_key_sharding}` with `DataStoreError` codes (`REQUESTED_NODE_NOT_OWNER`, `WRITE_TO_READ_ONLY_DB`, `KEY_NOT_FOUND`, ...).

### DataStoreService: shard ownership, status, migration

`DataStoreService` (`eloq_data_store_service/data_store_service.h/.cpp`) is the brpc service. State is an `std::array<DataShard, 1000>`; each `DataShard` holds the `DataStore` instance, `std::atomic<DSShardStatus>`, write-request counter, latest term/snapshot-ts, and a per-shard `TTLWrapperCache` of scan iterators. `DSShardStatus` (C++ enum in `data_store_service_util.h`: `ReadOnly`, `ReadWrite`, `Closed`, `Starting`; the proto version lacks `Starting`) gates every RPC: reads require RO/RW, writes require RW (RO → `WRITE_TO_READ_ONLY_DB`), non-owned shard → `PrepareShardingError` fills `new_key_sharding` so clients can re-route. `StartService` opens the shards this node owns per the cluster config, registers the brpc server, and recovers any in-flight migration (`CheckAndRecoverMigrateTask`).

Shard migration (`ShardMigrate` RPC → `DoMigrate`, a persistent state machine journaled to `DSMigrateLog`, recoverable across crashes): (1) source switches RW→RO and saves config; (2) target is told `OpenDSShard` read-only with bumped `shard_version`; (3) members get `UpdateDSShardConfig`; (4) target switches to RW, source closes (`SwitchReadOnlyToClosed` waits for `ongoing_write_requests_` to drain). Clients learn of the move via `REQUESTED_NODE_NOT_OWNER` + `new_key_sharding`, or via the update-config listener. This only works for shared storage backends (the data itself is in S3/GCS or EloqStore cloud storage; only ownership moves).

For `ELOQDSS_ROCKSDB_CLOUD_S3`, a `FileCacheSyncWorker` periodically sends the primary's cached SST file list (`SyncFileCache` RPC) so standbys pre-download hot files via `S3FileDownloader` (`s3_file_downloader.h`), bounded by an SST-cache size limit with older (lower-numbered) files prioritized (`DetermineFilesToKeep`).

## 4. The DataStore Backend Abstraction Inside DSS

`DataStore` (`eloq_data_store_service/data_store.h`) is the per-shard engine interface: `Initialize`, `StartDB(term[, shard_id])`, `Shutdown`, async `Read/BatchWriteRecords/FlushData/DeleteRange/CreateTable/DropTable/ScanNext/ScanClose/CreateSnapshotForBackup`, `SwitchToReadOnly/ReadWrite`, plus standby hooks (`ReloadData`, `UpdateStandbyMasterStorePaths/Addr`). Requests are the `Poolable` wrappers in `internal_request.h` (`ReadRequest`, `WriteRecordsRequest`, `ScanRequest`, ...), each with an RPC-backed and a local (in-process) variant exposing one uniform accessor API — this is what makes the client's local bypass zero-copy. `DataStoreFactory` (`data_store_factory.h`) creates per-shard stores and exposes config (storage path, S3 bucket/region/credentials, SST cache size, `IsCloudMode`).

| Backend (`DataStoreFactoryType`) | Files | Storage | Notes |
|---|---|---|---|
| `RocksDBDataStore` | `rocksdb_data_store.h`, `rocksdb_data_store_common.*` | local disk `<path>/ds_<shard_id>/db` | not shared storage; no backup snapshot support (`assert(false)`) |
| `RocksDBCloudDataStore` | `rocksdb_cloud_data_store.h/.cpp`, `rocksdb_cloud_dump.cpp` | rocksdb-cloud on S3 or GCS | cloud manifest cookie `CLOUDMANIFEST-{branch}-{dss_shard_id}-{term}` gives each leader term its own manifest; max-term discovery on open; backup snapshots; SST file cache sync |
| `EloqStoreDataStore` | `eloq_store_data_store.h/.cpp`, `eloq_store_config.h`, `eloq_store_data_store_factory.h` | EloqStore engine (vendored submodule, **internals out of scope**) | integration surface only: `EloqStoreConfig` wraps `::eloqstore::KvOptions` + `branch_name_`; `StartDB(term, shard_id)` → `EloqStore::Start(branch, term, shard)`; ops are forwarded as async `::eloqstore::ReadRequest/BatchWriteRequest/ScanRequest` with static `OnRead/OnBatchWrite/...` completion callbacks and pooled `EloqStoreOperationData` bridging back to the DSS request; supports archive-tagged standby snapshots (`CreateSnapshotForStandby`, `ListArchiveTags`) and pull-based standby sync (`UpdateStandbyMasterStorePaths`) |

**Key layout.** The RocksDB backends store everything in one keyspace (no per-table column families at the DSS level): key = `kv_table_name + "/" + partition_id + "/" + user_key` (`RocksDBDataStoreCommon::BuildKey`, `rocksdb_data_store_common.cpp:1243`), value carries the record with the version-ts MSB flagging TTL presence. EloqStore receives `(tbl_name, partition_id)` natively as a `TableIdent`. Kv table names come from `KVCatalogInfo`: `CreateKVCatalogInfo` generates `"{engine_prefix}t{uuid}"` for base tables and `"{engine_prefix}{i|u}{uuid}"` for (unique) secondary indexes; system tables are fixed names (`table_catalogs`, `db_catalogs`, `table_ranges`, `table_range_slices`, `table_last_range_partition_id`, `table_statistics(_version)`, `mvcc_archives` — `data_store_service_client.cpp:97-107`). Range metadata encoding: range row = `[table + range_start_key] → [range_id, range_version, version, segment_cnt, range_size]`; slices are segmented as `[table + range_id + segment_id] → [version, (slice_key, slice_size)...]` to keep items small (comment block at `data_store_service_client.cpp:1386`).

**Archive encoding.** MVCC archives live in `mvcc_archives` with key = `kv_table_name + key + big_endian(commit_ts)` (`EncodeArchiveKey`; big-endian so versions of a key sort by commit-ts under a forward scan) and value = `[is_deleted, unpack_info, blob]` (`EncodeArchiveValue`); the archive's kv partition is `HashArchiveKey(kv_table_name, key) % 1024`, so a key's archive versions are colocated. `FetchVisibleArchive` is a reverse-bounded scan for the newest version with `commit_ts <= upper_bound_ts`.

**Standalone DSS.** `eloq_data_store_service/main.cpp` builds DSS as its own server binary (its CMake project), so the same service can run separately from tx nodes; in that deployment every tx node is a pure remote client (`eloq_dss_peer_node` points at the DSS cluster). The in-process colocated mode used by `storage_init.cpp` is the common path today.

**Bootstrap.** `DataStoreServiceClient::Connect()` is a no-op unless the node bootstraps the cluster (`need_bootstrap_`), in which case it retries `InitPreBuiltTables()` up to 5 times — writing catalog records (and, for range tables, initial range metadata) for pre-built system tables such as the sequences table registered via `AppendPreBuiltTable` before `Connect`.

## 5. Direct (Non-DSS) Handlers

- **`RocksDBHandlerImpl` / `RocksDBCloudHandlerImpl`** (`store_handler/rocksdb_handler.h`, selected by `WITH_DATA_STORE=ROCKSDB` → `DATA_STORE_TYPE_ROCKSDB`): embedded RocksDB inside the tx process, used by EloqKV local-disk builds. One column family per kv table (`column_families_` map), `IsSharedStorage() = false`, `NeedPersistKV() = true`, supports `RestoreTxCache` (parallel full-table iteration to repopulate cc maps), standby snapshots via rsync-style `SendSnapshotToRemote`, and an EloqKV-specific `TTLCompactionFilter` that drops expired Redis TTL objects during compaction. Includes EloqKV headers (`redis_object.h`) — it is effectively EloqKV-only despite living in this repo. `rocksdb_config.h` maps ini options (write buffers, level triggers, rate limits, cloud bucket/region settings) onto it.
- **`DynamoHandler`** (`store_handler/dynamo_handler.h`, `DATA_STORE_TYPE_DYNAMODB`): AWS DynamoDB backend via the AWS SDK with a worker pool; `NeedCopyRange() = true`. Maintained as an alternative shared-storage backend.
- **`BigTableHandler`** (`store_handler/bigtable_handler.h`, `DATA_STORE_TYPE_BIGTABLE`): Google BigTable backend; `NeedCopyRange() = false` (server-side range semantics) and the only handler with a non-trivial `ByPassDataStore()` (`ddl_skip_kv_ && !is_bootstrap_`). Legacy/alternative; selected the same way via `storage_init.cpp`.

`store_handler/store_util.h/.cpp` (`EloqShare` namespace) is shared plumbing: vector/string (de)serializers, endian converters used in archive-key encoding (commit-ts stored big-endian so newest sorts deterministically), and schema-image pack/unpack helpers (`SerializeSchemaImage` in `kv_store.h` packs frm + kv_info + key_schemas_ts).

## 6. TTL and Purge Mechanisms

- **Record TTL (data plane).** `BatchWriteRecords` items carry a `ttl` (ms epoch). On read, `FetchRecordCallback` treats an expired EloqKV record as `RecordStatus::Deleted` even if the store still has it. Physical reclamation is compaction-driven: `TTLCompactionFilter` (both `rocksdb_handler.h` and DSS `rocksdb_data_store_common.h`) drops expired entries during RocksDB compaction; the DSS variant flags TTL presence in the version-ts MSB (`MSB`/`MSB_MASK`).
- **Scan-iterator TTL (`TTLWrapperCache`, `data_store_service.h/.cpp`).** Open scan sessions cache their RocksDB iterator (`RocksDBIteratorTTLWrapper`) keyed by session id. A per-shard `dss_ttl` worker wakes every 3 s and erases not-in-use wrappers idle longer than the interval; `Borrow`/`Return` mark in-use; shard close force-erases.
- **Cloud SST purger (`purger_event_listener.h`, `purger_sliding_window.h`).** rocksdb-cloud's background purger deletes obsolete S3 files. `PurgerEventListener` tracks live file numbers across flush/compaction; a time-based `SlidingWindow` publishes the smallest in-use file number to S3 (`S3FileNumberUpdater`) so the purger never deletes a file a lagging reader (standby syncing the file cache) may still need, and can temporarily block the purger.

## 7. Core Flows

### Checkpoint flush (data sync worker → DSS)

```
DataSync worker (bthread, holds yield/resume coroutine hooks)        [07]
  │ store_hd_->PutAll(flush_task)                 local_cc_shards.cpp:5937
  ▼
DataStoreServiceClient::PutAllImpl
  │ group FlushRecords by kv partition (hash: per-record partition_id;
  │ range: one partition per flush entry)
  │ build ≤64MB batches per partition (PreparePartition[Range]Batches)
  ▼
per partition, serially:  BatchWriteRecords(..., skip_wal=true)
  │            IsLocalShard? ──yes──► DataStoreService::BatchWriteRecords
  │                 │ no                  (direct call, closure fields)
  │                 └──► brpc stub.BatchWriteRecords (60s timeout)
  ▼
PartitionBatchCallback ─► next batch of that partition, or
SyncPutAllData::OnPartitionCompleted ─► resume_fn / cv when all done
  │
  ▼ (caller, after all tables flushed)
store_hd_->PersistKV(kv_table_names)              local_cc_shards.cpp:5964
  └─► FlushData RPC/local on EVERY data shard  → only now is data durable
```

### Cache-miss read (`FetchRecord`)

```
CcMap::Execute miss (template_cc_map.h / object_cc_map.h)
  │ CcShard::FetchRecord → may return Retry when saturated (cc_shard.cpp:1979)
  ▼
DataStoreServiceClient::FetchRecord(fetch_cc)
  │ kv_partition = KvPartitionIdOf(partition_id, !IsHashPartitioned())
  │ shard_id     = GetShardIdByPartitionId(...)
  ▼
Read closure (local bypass or RPC) ─► FetchRecordCallback
  │ KEY_NOT_FOUND          → rec_status = Deleted, rec_ts = 1
  │ TTL expired (EloqKV)   → rec_status = Deleted
  │ NO_ERROR               → rec_str/rec_ts filled
  ▼
fetch_cc->SetFinish(0)  → re-enqueued on the owning CcShard          [03]
```

### Slice load (`LoadRangeSlice`)

`range_slice.cpp:649` → `LoadRangeSlice(FillStoreSliceCc*)` pins the node group, encodes start/end keys (negative infinity uses the catalog factory's packed-neg-inf key, positive infinity is the empty string), and issues a sessioned `ScanNext` (batch 1000, forward, end key bounds the scan server-side) against the range's kv partition; `LoadRangeSliceCallback` feeds rows into the slice (`AddDataItem`) and re-issues `ScanNext` with the returned `session_id` as long as batches come back full, then `SetKvFinish` unpins the node group. The `Retry`/`Error` statuses propagate to the slice state machine (`range_slice.cpp:660,724`, see [08](08-range-and-bucket-management.md)).

## 8. Gotchas and Invariants

- **`is_range_partition` must match the table type.** Hash and range partition ids are mapped to buckets by different functions; the same integer routes to different DSS shards depending on the flag (`GetShardIdByPartitionId`). Several call sites derive it from `table_name.IsHashPartitioned()` — keep that pattern.
- **`PutAll` alone is not durable.** Checkpoint batches set `skip_wal=true` on the DSS side; data is durable only after `PersistKV`/`FlushData` succeeds on every shard. The checkpointer must not advance ckpt-ts before `PersistKV` returns (see [07](07-durability-and-recovery.md)).
- **Per-partition write ordering.** `PutAllImpl` allows at most one in-flight batch per kv partition; cross-partition writes are concurrent. Code that adds write paths must preserve per-partition ordering (last-writer-wins keyed by `records_ts`).
- **Retries are bounded and not transparent.** `retry_limit_ = 2` per closure; after that the error surfaces to the caller (`PutAll` returns false; `FetchRecord` finishes the cc request with an error). `NodeGroupChanged` sharding errors crash the process today.
- **`IsSharedStorage()` is correctness-critical**, not a hint: on shared storage a standby trusts the leader's checkpoint-ts when deciding whether an evicted entry is persistent (`cc_entry.cpp:64`); claiming shared storage on a local-disk backend would let standbys evict unpersisted data. Note the colocated DSS client returns true for EloqStore and `IsCloudMode()` for RocksDB variants — plain `ELOQDSS_ROCKSDB` colocated is *not* shared.
- **DSS shard count limits are hardcoded**: `data_shards_` (service) is `std::array<DataShard, 1000>`, client `dss_shards_` is 1000 / `dss_nodes_` 1024; exceeding them is a startup `LOG(FATAL)`.
- **`DataShard::latest_snapshot_ts_` ordering assumes colocated deployment** — `ReloadData` calls must be sequential per shard, which holds only when tx and DSS share a process (comment at `data_store_service.h:782`).
- **bthread caution**: closures complete on brpc worker threads and callbacks may run `Notify()` under `bthread::Mutex`; the coroutine-resume path deliberately unlocks before calling `resume_fn` to avoid lock-ordering deadlocks (`SyncPutAllData::OnPartitionCompleted`). Follow that pattern when adding sync helpers (cf. the repo-wide bthread mutex rules in `CLAUDE.md` / [02](02-threading-model.md)).
- **`DropTable` is not atomic** across shards (comment at `data_store_service_client.cpp:4827`); a crash mid-drop can leave partial data, which is tolerated because kv table names are UUID-based and never reused.

## Key Files

| Area | Files |
|---|---|
| Interface | `tx_service/include/store/data_store_handler.h`, `data_store_scanner.h`, `snapshot_manager.h` |
| DSS client | `store_handler/data_store_service_client.h/.cpp`, `data_store_service_client_closure.h/.cpp`, `data_store_service_scanner.h/.cpp` |
| DSS service | `store_handler/eloq_data_store_service/data_store_service.h/.cpp`, `data_store_service_config.*`, `data_store_service_util.h`, `ds_request.proto`, `internal_request.h`, `object_pool.h`, `thread_worker_pool.h` |
| DSS backends | `data_store.h`, `data_store_factory.h`, `rocksdb_data_store*.{h,cpp}`, `rocksdb_cloud_data_store*.{h,cpp}`, `purger_*`, `s3_file_downloader.*`, `eloq_store_data_store*.{h,cpp}`, `eloq_store_config.*` |
| Direct handlers | `store_handler/rocksdb_handler.*`, `rocksdb_config.*`, `dynamo_handler.*`, `bigtable_handler.*`, `kv_store.h`, `store_util.*` |
| Wiring | `core/src/storage_init.cpp`, top-level `CMakeLists.txt` (`WITH_DATA_STORE`), `store_handler/eloq_data_store_service/build_eloq_store.cmake` |
