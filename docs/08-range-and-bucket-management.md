# 08 — Range & Bucket Management

Data Substrate places data on node groups (NGs) through two cooperating schemes: every key hashes to one of **1024 buckets** whose owner NG is tracked in `BucketInfo`, and (for SQL-style tables) keys additionally belong to **ranges** — contiguous key intervals, each mapped to a bucket via a hash of its range/partition ID. A range materializes in memory as a `StoreRange` divided into ~16 KB `StoreSlice`s that are loaded from the KV store on demand, pinned during reads/scans, and evicted under a dedicated memory budget. Ranges that outgrow 256 MB are split by a multi-stage `SplitFlushRangeOp` transaction; cluster scaling moves buckets between NGs with per-core `DataMigrationOp` worker transactions; and secondary-index build (`UpsertTableIndexOp`) scans primary-key ranges with `SkGenerator` and uploads encoded index entries to their owner NGs with `UploadBatchCc`. All three flows write multi-stage logs (see [10-log-service.md](10-log-service.md)) so they can be resumed after failover.

Key files: `tx_service/include/cc/range_slice.h` + `src/cc/range_slice.cpp`, `tx_service/include/range_record.h`, `tx_service/include/range_bucket_key_record.h`, `tx_service/include/cc/range_cc_map.h`, `tx_service/include/cc/range_bucket_cc_map.h`, `tx_service/include/cc/range_slice_type.h`, `tx_service/include/cc/slice_data_item.h`, `tx_service/include/cc/local_cc_shards.h` + `src/cc/local_cc_shards.cpp`, `tx_service/include/tx_operation.h` + `src/tx_operation.cpp`, `tx_service/include/sharder.h`, `tx_service/include/sk_generator.h` + `src/sk_generator.cpp`, `tx_service/include/tx_index_operation.h`, `tx_service/tx-log-protos/log.proto`.

Related docs: [03-concurrency-control.md](03-concurrency-control.md) (CcMap/CcEntry/locks), [04-transaction-execution.md](04-transaction-execution.md) (TransactionOperation state machines), [05-data-model-and-catalog.md](05-data-model-and-catalog.md) (TxKey, catalog factory), [06-distribution-and-clustering.md](06-distribution-and-clustering.md) (Sharder, ClusterScaleOp), [07-durability-and-recovery.md](07-durability-and-recovery.md) (data sync / checkpointer, replay), [09-store-handler.md](09-store-handler.md) (the `DataStoreHandler` calls used here).

## 1. Two partitioning schemes

| | Hash partitioning | Range partitioning |
|---|---|---|
| Used by | object/hash tables (EloqKV Redis objects; `TableEngine::InternalHash`) | SQL-style tables (EloqSQL/EloqDoc; `TableEngine::InternalRange`) |
| Key → placement | `Sharder::MapKeyHashToBucketId(hash)` = `(hash & 0x3FFF) % 1024` (`sharder.h`) | binary search in the table's sorted ranges → range partition ID → `Sharder::MapRangeIdToBucketId(range_id)` = MurmurHash3(range_id) % 1024 |
| Bucket → NG | `BucketInfo::BucketOwner()` | same |
| KV-store partition | `MapKeyHashToHashPartitionId(hash)` = `hash % total_hash_partitions` (1024, `type.h:632`) | one KV partition per range (`partition_id`) |
| CcMap class | `ObjectCcMap : TemplateCcMap<KeyT, ValueT, false, false>` (`cc/object_cc_map.h:56`) | `TemplateCcMap<KeyT, ValueT, Versioned, /*RangePartitioned=*/true>` |
| Growth | fixed 1024 buckets; rebalance = bucket migration | ranges split when > 256 MB; ranges also live in buckets, so both mechanisms compose |

`total_range_buckets = 0x400` (1024) and `total_hash_partitions = 0x400` are defined in `tx_service/include/type.h:632`. Buckets also have core affinity: `Sharder::ShardBucketIdToCoreIdx(bucket_id)` = `(bucket_id & 0x3FF) % core_cnt` (`src/sharder.cpp:509`); range-partitioned data has range affinity — a whole range is processed on core `(range_id & 0x3FF) % core_cnt` (e.g. `StoreSlice::StartLoading`, `range_slice.cpp:73`).

**The `RangePartitioned` template flag** (4th parameter of `TemplateCcMap`/`CcEntry`) selects the entry-info layout in `cc/cc_entry.h`: `RangePartitionedEntryInfo` / `RangePartitionedVersionedEntryInfo` add a per-record `data_store_size_` field (`INT32_MAX` = unknown, set during log replay). This is what lets the data-sync scan compute per-slice size deltas for split decisions (§4). Non-range maps (`ObjectCcMap`, `RangeCcMap`, `RangeBucketCcMap` themselves) use `RangePartitioned=false` and have no per-record store size.

**Bucket metadata.** `BucketInfo` (`range_bucket_key_record.h:36`) holds `bucket_owner_`, `version_`, dirty owner/version (set during migration) and an atomic `accepts_upload_batch_` flag. All 1024 `BucketInfo`s per NG live in `LocalCcShards` and are mirrored into the meta cc map `__range_bucket` (`RangeBucketCcMap`, one per shard, `cc/range_bucket_cc_map.h`) whose entries (`RangeBucketKey{bucket_id}` → `RangeBucketRecord` pointing at the shared `BucketInfo`) exist only to be **lockable**: normal txs read `buckets_infos_` lock-free; during cluster scale, `ClusterScaleOp::pub_buckets_migrate_begin_op_` flips `LocalCcShards::SetBucketMigrating(true)` on all nodes so readers go through `ReadCc` on the bucket cc map and take read locks (`tx_operation.h:1361`).

## 2. Range metadata: TableRangeEntry, RangeCcMap, fetching

Per (table, NG), `LocalCcShards::table_ranges_` is a `std::map<TxKey, TableRangeEntry::uptr>` keyed by range start key, with a secondary index `range_id → entry` (`local_cc_shards.h:2105`). `TemplateTableRangeEntry<KeyT>` (`range_record.h:531`) contains:

- `range_info_` — `TemplateRangeInfo<KeyT>`: start key (owned; or neg-inf), end key (raw pointer to the *next* entry's start key), `version_ts_`, plus dirty-split state: `new_key_`/`new_partition_id_`/`dirty_ts_`/`is_dirty_` (`range_record.h:98`). The first range (start = neg-inf) has reserved partition ID 0; new IDs are allocated from the sequence table during split.
- `range_slices_` — the `TemplateStoreRange<KeyT>` (§3). **Initialized lazily and only on the NG that owns the range's bucket**; safe to access only while the StoreRange is pinned or `TableRangeEntry::mux_` is held (comment at `range_record.h:818`).
- `dirty_range_slices_` — slice specs for *new* ranges uploaded by the splitting NG via `UploadRangeSlicesCc` (tuple `(accept_data, has_dml_since_ddl, new_range_id → slices)`), consumed when the split installs new entries.

`RangeCcMap<KeyT>` (`cc/range_cc_map.h:63`) is the per-shard meta cc map (`TableType::RangePartition` name) built from `table_ranges_`; each `RangeRecord` references the shared `RangeInfo` and the bucket cc entry that owns the range (`range_owner_rec_`). `ReadCc` on it does a *floor* lookup, then acquires **the bucket record lock first, the range record lock second** ("always acquire bucket lock before range lock to avoid internal dead lock", `range_cc_map.h:296`), and returns range + owner-bucket info to the tx.

**Fetching from the store** (`store/data_store_handler.h`): `FetchTableRanges(FetchTableRangesCc)` scans the range table in KV and returns `InitRangeEntry{start_key, partition_id, version_ts}` per range → `LocalCcShards::InitTableRanges` builds the entries; triggered from `CcShard::FetchTableRanges` whenever a request touches a table with no cached ranges (e.g. `UploadRangeSlicesCc::Execute`, `PinRangeSlice`). Per-range slice metadata is fetched by `FetchRangeSlicesReq` (supports multi-segment metadata via `segment_cnt_`/`segment_id_`, `cc_req_misc.h:254`); concurrent requesters queue on the in-flight request (`TableRangeEntry::fetch_range_slices_req_`).

**Dedicated ranges heap.** All range/slice metadata is allocated on a separate mimalloc heap (or jemalloc arena) created by `InitializeTableRangesHeap()` (`local_cc_shards.h:459`); every allocation site switches heap/arena around the mutation (see `CreateTableRange`, `UpdateSliceSpec`). Budget = `node_memory_limit_mb × range_slice_memory_limit_percent / 100` (flag default 10%, `core/src/tx_service_init.cpp:15`; stored as `range_slice_memory_limit_`, `src/cc/local_cc_shards.cpp:104`). `TableRangesMemoryFull()` compares allocated bytes against the limit; when full, `KickoutRangeSlices()` (`src/cc/local_cc_shards.cpp:1363`) drops whole `StoreRange` objects (`DropStoreRange`, requires `Pins()==0` under the entry's unique lock): first any range idle > 10 min, then least-recently-accessed ranges idle > 4 s, until usage ≤ 90% of the limit. Note its comment: it must not run on a TxProcessor (can block for milliseconds).

## 3. StoreRange and StoreSlice

`TemplateStoreRange<KeyT>` (`cc/range_slice.h:859`) = sorted `slices_` + owned `boundary_keys_` (slice *i* ends where slice *i+1* starts; range start/end keys are raw pointers into the table-range entries). Constants: `range_max_size` = 256 MB (1 MB under `SMALL_RANGE` test builds), `StoreSlice::slice_upper_bound` = 16 KB; `NeedSplitOrMerge()` when slice size > 16 KB or < 4 KB. A new/empty range starts as a single slice covering the whole range.

**Slice states** (`SliceStatus`, `cc/range_slice_type.h`):

| State | Meaning |
|---|---|
| `PartiallyCached` | not all of the slice's records are in the cc map (initial state, and after any kickout) |
| `FullyCached` | every record in the slice is cached; only then can the slice be pinned |
| `BeingLoaded` | a `FillStoreSliceCc` is inserting fetched records into the cc map; blocks kickout and pinning |

**Pinning.** Read/scan paths call `LocalCcShards::PinRangeSlice(s)` (`local_cc_shards.h:1209`) → find the `TableRangeEntry` (missing → `FetchTableRanges`, status `BlockedOnLoad`), then the `StoreRange` (missing → if this NG owns the range's bucket, `FetchRangeSlices`, else `NotOwner`), then `TemplateStoreRange::PinSlices` (`range_slice.h:1145`): under the range's shared lock + slice mutex, a `FullyCached` slice gets `++pins_` (and the range-level atomic `pins_`); scans batch-pin consecutive slices up to `max_pin_cnt` and prefetch-load further ones asynchronously. The returned `RangeSliceId{range*, slice*}` is what the tx later uses to `Unpin()`. Pin results (`RangeSliceOpStatus`, `range_slice.h:65`):

| Status | Cause / handling |
|---|---|
| `Successful` | fully cached & pinned |
| `BlockedOnLoad` | async KV load started; the cc request is queued on `slice.cc_queue_` and re-enqueued when filling commits |
| `Retry` | concurrent modification (`to_alter_` set by checkpointer, key cache loading, …) — re-execute immediately |
| `Delay` | slice was loaded < 4 s ago yet is partial again → cache thrashing; don't reload (`IsRecentLoad`, `range_slice.cpp:154`). During a range split the request is aborted with OOM instead |
| `NotOwner` | NG no longer owns the slice (failover right after a range split commit, before log truncation) |
| `KeyNotExists` | slice partial but the per-range cuckoo-filter key cache proves the key is absent — skip the KV load (`txservice_enable_key_cache`) |
| `NotPinned` | only with `no_load_on_miss` |
| `Error` | e.g. KV error |

**Loading.** `StoreRange::LoadSlice` (`range_slice.cpp:587`) allocates a pooled `FillStoreSliceCc` (`cc_req_misc.h:399`), bumps range `pins_`, and calls `DataStoreHandler::LoadRangeSlice` (async; `Retry` re-enqueues waiters, errors abort them **on their own shard** via `AbortCcRequests` — the execute/abort-on-same-shard rule). The store handler streams `SliceDataItem`s into the request; `FillStoreSliceCc::Execute` runs on core `(range_id & 0x3FF) % core_cnt` and batch-fills the cc map (records enter clean). `StartLoading` → `BeingLoaded`; `CommitLoading` → `FullyCached`, re-enqueues the waiting cc requests, releases the range pin; `SetLoadingError` → back to `PartiallyCached` and aborts waiters. `FillStoreSliceCc::AbortIfOom()` returns true — a memory-full shard aborts the fill instead of deadlocking on eviction.

**Eviction.** Cache replacement may only drop keys of a slice if `StoreSlice::Kickout()` succeeds — it fails when `pins_ > 0` or status is `BeingLoaded`; success flips the slice to `PartiallyCached` (`range_slice.h:344`). Whole-StoreRange eviction is §2's `KickoutRangeSlices`.

**Size tracking.** Each slice has `size_` (last persisted size) and `post_ckpt_size_` (`UINT64_MAX` = unset). The data-sync flow computes per-slice deltas from `data_store_size_` of dirty entries (`ScanSliceDeltaSizeCcForRangePartition`, `cc/cc_request.h:8538`, executed on the range's core), then `UpdateSlicePostCkptSize` sets `post_ckpt_size = size + delta`. After the KV flush, `UpdateSliceSizeAfterFlush(start,end,flush_res)` commits `post_ckpt_size_` into `size_` (or resets it on failure). When a slice outgrows its bound, the checkpointer splits it in memory via `UpdateSliceSpec(slice, SliceChangeInfo[])` (`range_slice.cpp:492`): it sets `to_alter_` (new pins get `Retry`), sleeps on the range `wait_cv_` until `ChangeAllowed()` (pins 0, not loading), rewrites `slices_`/`boundary_keys_` on the ranges heap, then persists the new spec with `UpdateRangeSlicesInStore` → `DataStoreHandler::UpdateRangeSlices`.

## 4. Range split, end to end

**Trigger** (in `LocalCcShards::DataSyncForRangePartitionedTable`-family code, `src/cc/local_cc_shards.cpp:4020`-4156): each per-range data-sync task first runs `ScanSliceDeltaSizeCcForRangePartition`; after `UpdateSlicePostCkptSize`, `CalculateRangeUpdate` (`:5430`) checks `StoreRange::PostCkptSize() > range_max_size` and, if so, calls `CalculateRangeSplitKeys` (`range_slice.h:1484`): split into `ceil(post_size / (256 MB × new_range_load_factor 0.6))` sub-ranges, accumulating whole slices; a slice bigger than the average sub-range is itself split by **key sampling** — `SampleSubRangeKeysCc` (`cc/cc_request.h:8935`, reservoir `RandomPairing` pool ≥ 1024) picks evenly spaced keys, and the slice is pre-split in memory (`UpdateSliceSpec`) so concurrent sub-range flush tasks never share a slice. The resulting split keys become a `RangeSplitTask` queued to the **range split workers** (`range_split_worker_ctx_`; `--range_split_worker_num`, default `core_num/2` with `EXT_TX_PROC_ENABLED`, `src/cc/local_cc_shards.cpp:123`).

`LocalCcShards::SplitFlushRange` (`:5679`) allocates new unique partition IDs from the sequence table (`GetNextRangePartitionId`) and drives a `SplitFlushTxRequest` (`tx_request.h:821`) on the data-sync txm, which runs **`SplitFlushRangeOp`** (`tx_operation.h:898`). Verified stage order (`SplitFlushRangeOp::Forward`, `src/tx_operation.cpp:4254`):

| # | Stage | What it does |
|---|---|---|
| 1 | `lock_cluster_config_op_` | read-lock local cluster config (no scaling during split) |
| 2 | `prepare_acquire_all_write_op_` | write-lock the range record on all NGs |
| 3 | `prepare_log_op_` | `SplitRangeOpMessage` stage `PrepareSplit` (old id, new ids, new start keys) |
| 4 | `install_new_range_op_` | PostWriteAll installs **dirty** range info (`UploadNewRangeInfo`) everywhere; writes now go to old + new partitions; lock downgraded to write intent |
| 5 | `unlock_cluster_config_op_` | release config read lock |
| 6 | `data_sync_op_` | flush in-memory data < commit_ts into the **new** KV partitions; if a new range lands on another NG/core, `RangeCacheSender` (`src/cc/local_cc_shards.cpp:7287`) first uploads the new slice specs via `UploadRangeSlicesCc` RPC (cached in the target's `dirty_range_slices_`, all `PartiallyCached`), then streams the records via `UploadBatchSlicesCc` RPCs to warm the future owner's cache |
| 7 | `commit_acquire_all_write_op_` | upgrade back to write lock on old + new partitions |
| 8 | `update_key_cache_op_` | rebuild key cache if enabled |
| 9 | `commit_log_op_` | stage `CommitSplit` |
| 10 | `ds_upsert_range_op_` | `DataStoreHandler::UpsertRanges` writes the new range entries + slice sizes to the store's range table |
| 11 | `kickout_old_range_data_op_` | kick data that now belongs to another NG out of the old owner's cc maps |
| 12 | `post_all_lock_op_` | install the new `TableRangeEntry`s on every node (`LocalCcShards::SplitTableRange`, `local_cc_shards.h:710`), commit dirty range info, release locks |
| 13 | `ds_clean_old_range_op_` | `DeleteOutOfRangeData` removes moved keys from the old KV partition (no locks needed — no longer visible) |
| 14 | `clean_log_op_` | stage `CleanSplit` removes the split log |

`SplitTableRange` on the **old owner** carves the in-memory `StoreRange` with `TemplateStoreRange::SplitRange(new_end)` — which scans every slice and *fails if any slice is pinned, being loaded, filling, or initializing its key cache* (`range_slice.h:1078`; possible when a failed-over node still holds reads despite the write lock) — and builds the new entries from the removed slices. On **other NGs** it consumes `ReleaseDirtyRangeSlices()` uploaded in stage 6. Both paths check `TableRangesMemoryFull()` afterward and trigger `KickoutRangeSlices()`.

**Recovery.** On log replay of an in-flight `SplitRangeOpMessage`, `LocalCcShards::CreateSplitRangeRecoveryTx` (`src/cc/local_cc_shards.cpp` ~`:1010`) re-pins the store range, re-applies the in-memory slice pre-split for `PrepareSplit`-stage logs, and runs a `RangeSplitRecoveryTxRequest` (`tx_request.h:1227`) that resumes `SplitFlushRangeOp` from the logged stage.

## 5. Bucket migration (cluster scale)

`ClusterScaleOp` (`tx_operation.h:1263`) computes per-old-owner `BucketMigrateInfo` plans (consistent-hash assignment in `LocalCcShards::InitRangeBuckets`-family code). Order: for **add node**, config change first, then migration; for **remove node**, migration first. `pub_buckets_migrate_begin_op_`/`end_op_` fence the lock-free bucket reads (§1).

`NotifyStartMigrateOp::InitDataMigration` (`src/tx_operation.cpp:6987`) runs on each old owner: it groups that NG's to-move buckets **by core** (`ShardBucketIdToCoreIdx`) and starts one migration worker tx per non-empty core. The first worker's `write_first_prepare_log_op_` records all worker tx numbers and is **rejected by the log service if the cluster-scale log no longer exists** (idempotence against late notifications; `DUPLICATE_MIGRATION_TX_ERROR` means migration is already running). Workers share a `DataMigrationStatus` (`local_cc_shards.h:104`: bucket batches, new owners, `next_bucket_idx_`, `unfinished_worker_`) and loop `PrepareNextRoundBuckets()` until all batches are done.

Per batch, `DataMigrationOp` (`tx_operation.h:1395`) stages — each log write carries a `BucketMigrateStage` (`tx_service/tx-log-protos/log.proto:135`):

| Stage | Log (`BucketMigrateStage`) | Action |
|---|---|---|
| `write_before_locking_log_op_` | `BeforeLocking` | makes the upcoming bucket write locks recoverable |
| `prepare_bucket_lock_op_` | — | AcquireAll write lock on the bucket records (all NGs) |
| `prepare_log_op_` | `PrepareMigrate` | |
| `install_dirty_bucket_op_` | — | PostWriteAll installs dirty owner into every `BucketInfo`; on the **new owner**, `UploadNewBucketInfo` also sets `accepts_upload_batch_ = true` (`src/cc/local_cc_shards.cpp:2150`); lock downgraded to write intent |
| `data_sync_op_` | — | flush the buckets' data to the store (range tables: per-range data sync for ranges in the bucket; hash tables: bucket scan). With `forward_cache_`, the scan also streams records to the dirty owner via `UploadBatchCc` RPCs of kind `DIRTY_BUCKET_DATA` (`src/cc/local_cc_shards.cpp:5018`) — best-effort cache warm-up, failures ignored |
| `acquire_bucket_lock_op_` | — | upgrade to write lock |
| `commit_log_op_` | `CommitMigrate` | |
| `kickout_data_op_` | — | old owner kicks the buckets' entries out of its cc maps and drops their `StoreRange`s (`DropStoreRangesInBucket`; uses `IsStoreRangeFree(sync_info=true)`, which also waits out any in-flight `FetchRangeSlicesReq`, `range_record.h:611`) |
| `post_all_bucket_lock_op_` | — | `CommitDirty()` the bucket info everywhere, release locks; `accepts_upload_batch_` reset to false |
| `clean_log_op_` | `CleanMigrate` | per-batch clean; last worker also writes `write_last_clean_log_op_` |

`UploadBatchCc` (`cc/cc_request.h:7560`) is the receiving cc request on the new owner (also used for index build, §6); its `ValidTermCheck` pins the destination NG term with a **lock-free atomic CAS** because the waiter may be a bthread while `Execute()` runs on the shard's main stack (`cc_request.h:7633`). The `accepts_upload_batch_` flag is turned off early if the new owner has to kick the bucket's data for memory — stale uploads could otherwise resurrect older versions than what is in KV (comment, `range_bucket_key_record.h:156`).

**Failover.** The `ClusterScaleOpMessage` log carries `node_group_bucket_migrate_process` (per-NG `NodeGroupMigrateMessage` with per-bucket `BucketMigrateMessage` stage/owners/txn). Replay (`src/tx_execution.cpp:8244`) recreates the migration worker txs at their logged `BucketMigrateStage` and re-runs the remaining buckets.

## 6. Secondary index build

`UpsertTableIndexOp` (`tx_index_operation.h:32`) is the DDL state machine (write-intent → prepare log → KV table creation → **`generate_sk_parallel_op_`** → flush old-tuple SK data → `PrepareData` log → … → commit/clean; skim the member comments for the full list). The parallel-generation granularity is the **PK range**: `DispatchRangeTask` sends each PK range to its owner NG — locally via `SkGenerator::ProcessTask`, remotely via the `GenerateSkFromPk` RPC (`proto/cc_request.proto:523`); the RPC handler (`src/remote/cc_node_service.cpp:1452`-region) spawns a `std::thread` and first waits for log replay if the node is still a candidate leader.

`SkGenerator` (`sk_generator.h:150`):

1. Registers with the NG's `GenerateSkStatus` (`local_cc_shards.h:129`): `StartGenerateSk(tx_term)` rejects stale terms and terminates/waits out an older run; the scan loop polls `CheckTxTermStatus()` and calls `TerminateGenerateSk()` if a newer term took over.
2. Scans the PK cc map range in batches (`RangePartitionDataSyncScanCc`, batch = `DATA_SYNC_SCAN_BATCH_SIZE` 3072, `src/sk_generator.cpp:325`), and for each visible record runs every new index's `SkEncoder::AppendPackedSk` to emit packed SK `WriteEntry`s (multikey detection and `PackSkError` propagate back in the RPC response / tx result).
3. Hands batches to `UploadIndexContext` (5 background upload workers; 2 in debug). `UploadEncodedIndex` acquires **range read locks** on the SK table's ranges (bucket+range, `AcquireRangeReadLocks`) to get a stable range→NG mapping, buckets the entries into per-(NG, sk-range) sets, and sends them: locally as pooled `UploadBatchCc` requests (`UploadBatchType::SkIndexData`), remotely as `UploadBatch` RPCs of kind `SK_DATA`, batch size 128. Uploaded entries land in the SK cc maps (and are later flushed by `flush_all_old_tuples_sk_op_`).

`has_dml_since_ddl_` (`range_slice.h:852`): set on a `StoreRange` when the index-build scan observes keys whose version exceeds the dirty schema version (concurrent DML during DDL). It is preserved across range splits (`SplitTableRange`) and shipped in `UploadRangeSlicesCc`, and lets recovery decide whether old-tuple SK data can be trusted as complete.

## 7. Gotchas / invariants (verified in code)

- **Lock order: bucket before range.** Range reads lock the owner bucket record first, then the range record (`range_cc_map.h:296`). Releasing in `~RangeCcMap` manually cleans leftover bucket read locks after drop-table.
- **Pin vs kickout vs alter.** `Kickout()` fails on pinned/loading slices; `UpdateSliceSpec` sets `to_alter_` then *blocks* on `wait_cv_` until `ChangeAllowed()`; `PinSlices` returns `Retry` for `to_alter_` slices but lets already-pinned requests proceed so the checkpointer can't deadlock with them (`range_slice.h:1187`).
- **StoreRange `pins_`** counts slice pins + in-flight loads + `TableRangeEntry::PinStoreRange()` and is the sole gate for dropping a `StoreRange` (`range_slice.h:820`). `IsStoreRangeFree(sync_info=true)` additionally requires no in-flight slice-metadata fetch (bucket migration cleanup).
- **`SplitRange` can fail even under the range write lock**: a node that failed over while holding a range read lock may still have requests touching slices; the split returns false rather than corrupting state (`range_slice.h:1089`).
- **Thrash guard**: re-loading a slice loaded < 4 s ago yields `Delay`; during a split the demanding request is aborted with OOM instead. Force-load (checkpointer/data-sync) bypasses the guard.
- **Memory-full hooks**: `CreateTableRange` / `UpdateSliceSpec` / `SplitTableRange` all check `TableRangesMemoryFull()` after allocating on the ranges heap and trigger `KickoutRangeSlices()` (never on a TxProcessor).
- **Version checks everywhere**: data-sync aborts if `range_entry->Version()` changed since task creation (`src/cc/local_cc_shards.cpp:3974`); `UploadRangeSlicesCc`/`UploadBatchSlicesCc` carry the dirty version and are dropped if it no longer matches (`UploadDirtyRangeSlices`, `range_record.h:696`); `UploadBatchCc` pins the destination term via atomic CAS.
- **`NotOwner` after split + failover**: log records replayed on the old NG may reference data now owned elsewhere until the checkpointer truncates the log; readers get `RangeSliceOpStatus::NotOwner` (`range_slice.h:96`).
- **Threading**: cc-request completion waits here follow the repo rule (see [02-threading-model.md](02-threading-model.md)) — e.g. `ClearCcNodeGroup::Wait` polls an atomic; `UploadBatchCc` avoids any shared `bthread::Mutex` between `Execute()` and its waiter.
