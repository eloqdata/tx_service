# 03 — Concurrency Control (CC) Module

The CC module is the in-memory heart of the tx service: data is hash-partitioned across per-core
`CcShard`s, each holding `CcMap`s (B-tree-like page structures of `CcEntry`s) for every
(table, node group) pair it serves. No caller ever touches a shard's data structures directly;
instead it enqueues a `CcRequestBase` whose `Execute(CcShard&)` runs on that shard's single
processing context (a `TxProcessor`, see `docs/02-threading-model.md`), making almost all shard
state lock-free single-writer. Isolation is enforced by `NonBlockingLock`s attached lazily to
entries; "non-blocking" means a conflicting request never blocks the shard thread — it is parked
in the lock's queue and re-enqueued when the lock is released. This document covers the request
execution model, shard anatomy, the map/entry/lock data structures, async results, deadlock
detection, and the invariants you must not break.

Related docs: `01-architecture-overview.md`, `02-threading-model.md` (who runs `Execute()`),
`04-transaction-execution.md` (the state machines that issue CC requests),
`05-data-model-and-catalog.md`, `07-durability-and-recovery.md` (checkpointing of dirty entries),
`08-range-and-bucket-management.md`.

## 1. The message-passing execution model

Defined in `tx_service/include/cc/cc_req_base.h`:

```cpp
struct CcRequestBase {
    // @return true, if the request needs to be freed and recycled; false, if not.
    virtual bool Execute(CcShard &ccs) = 0;
    ...
    std::atomic<bool> in_use_{false};   // pool occupancy flag (Use()/Free()/InUse())
    TxNumber tx_number_;
    CcProtocol proto_;                  // OCC / OccRead / Locking
    IsolationLevel isolation_level_;
};
```

Rules of the model (verified in `tx_service/src/cc/cc_shard.cpp::ProcessRequests`):

1. **All shard access goes through `Enqueue()` + `Execute()`.** `CcShard::Enqueue()` pushes into a
   `moodycamel::ConcurrentQueue` (multi-producer) and wakes the shard's TxProcessor.
   `ProcessRequests()` dequeues in batches of up to 64 (`req_buf_`) and calls `Execute(*this)`;
   if `Execute` returns `true` it calls `req->Free()`, returning the request to its
   `CcRequestPool` (`cc_req_pool.h` — a circular vector of reusable request objects keyed on the
   `in_use_` flag).
2. **`Execute()` must never block.** There are no waits inside `Execute()`; anything that cannot
   complete immediately returns `false` (so the request is *not* recycled) and arranges its own
   re-execution later. The four re-enqueue/retry patterns:
   - **Blocked on a lock**: the request is stored in the `NonBlockingLock`'s `blocking_queue_`;
     when the holder releases, `TryPopBlockingQueue()` re-enqueues it via `ccs->Enqueue()`
     (`tx_service/src/cc/non_blocking_lock.cpp`). Resumed requests detect resumption via a saved
     cce pointer (`req.CcePtr() != nullptr`) and call `LockHandleForResumedRequest()`.
   - **Cache miss / missing metadata**: `CcShard::FetchRecord / FetchCatalog / FetchTableRanges /
     FetchTableStatistics` issue async data-store reads (`FetchCc` subclasses in `cc_req_misc.h`),
     park the requester in the fetch request's `requesters_` list, and re-enqueue it when the
     fetch completes (see `TemplatedCcRequest::Execute` in `cc_request.h`).
   - **Out of memory**: `CcShard::EnqueueWaitListIfMemoryFull()` parks the request in
     `cc_wait_list_for_memory_`; `DequeueWaitListAfterMemoryFree()` re-enqueues (up to 20 at a
     time) after the LRU cleaner frees memory; requests answering `AbortIfOom()` true are aborted
     with `OUT_OF_MEMORY` instead.
   - **Multi-shard fan-out**: `CcMap::MoveRequest()` forwards a request to the next core; requests
     like `KickoutCcEntryCc` or scans re-enqueue *themselves* to yield after a batch.
3. **Cancellation** goes through `AbortCcRequest(CcErrorCode)`, which must set the error on the
   result and `Free()` the request (e.g. used by `CcShard::AbortCcRequests` and the OOM path).
4. A second queue, `low_priority_cc_queue_` (`EnqueueLowPriorityCcRequest`), carries background
   work; `ProcessLowPriorityRequests()` drains it one request at a time under a 50 µs budget per
   invocation. A third, `lazy_free_queue_`, destroys evicted `TxObject`s off the hot path.

`TemplatedCcRequest<RequestT, ResultType>` (`cc_request.h`) is the base for table-targeted
requests: its `Execute()` performs the node-group leader **term check**
(`REQUESTED_NODE_NOT_LEADER` on mismatch), resolves the `CcMap` (triggering catalog/range fetch
retry as above), caches `ccm_` for resumed executions, and dispatches to
`ccm->Execute(static_cast<RequestT&>(*this))`.

## 2. CcShard anatomy (`tx_service/include/cc/cc_shard.h`)

One `CcShard` per core (`core_id_`, `core_cnt_`). Key state:

| Member | Purpose |
|---|---|
| `native_ccms_` / `failover_ccms_` | `TableName -> CcMap` for the native node group / for node groups this node serves after failover. `GetCcm(table, ng)` looks up both. |
| `cc_queue_`, `low_priority_cc_queue_`, `lazy_free_queue_`, `cc_wait_list_for_memory_` | the request queues described above (queue sizes tracked in relaxed atomics so `IsIdle()` is cheap). |
| `tx_vec_` (`std::vector<TEntry>`) | per-shard transaction table. `NewTx()` scans circularly for a finished slot, grows ×1.5 when full. `TEntry` (`tentry.h`) holds `commit_ts_`, `lower_bound_` (start ts), `status_`, `ident_`, `term_`. TxNumber = `((ng_id << 10 \| core_id) << 32) \| ident` (`GlobalCoreId()`), so a tx number encodes its owning node group and core. |
| `lock_vec_` (`std::vector<KeyGapLockAndExtraData::uptr>`) | pooled lock objects (initial size `LOCK_ARRAY_INIT_SIZE` = 8192, doubles when exhausted; `TryResizeLockArray()` shrinks it again when sparse). `NewLock()` hands one to an entry; empty locks are recycled back. |
| `lock_holding_txs_` | `ng -> {TxNumber -> TxLockInfo}`: every lock/intent a tx holds in this shard (`cce_list_`), when it took its first write lock (`wlock_ts_`), and the tx coordinator term. Feeds `ActiveTxMinTs()` (checkpoint watermark), orphan-lock recovery (`CheckRecoverTx`), and deadlock collection (`CollectLockWaitingInfo`). |
| `head_ccp_`/`tail_ccp_`, `protected_head_page_`, `clean_start_ccp_` | LRU double-linked list of `LruPage`s: `head — [small pages] — protected_head_ — [large pages] — tail` (large objects get exclusive pages; SLRU-style policies share the divider). `UpdateLruList`/`DetachLru` maintain it; `access_counter_` stamps relative page order. |
| `shard_heap_`, `shard_data_sync_scan_heap_` | per-shard **mimalloc heaps** (`CcShardHeap`). `memory_limit_` = node limit × (1 − range-slice %) / core_cnt; the main heap gets 90 % of that, the data-sync-scan heap 0.1 × 0.25. `Full()`/`NeedCleanShard()`/`NeedDefragment()` gate admission, eviction, and `DefragShardHeapCc`. |
| `last_read_ts_` | max read timestamp seen on this shard; write txs must commit above it (read-write coordination, see comment at the member). |
| standby members | forward-message history queue + sequence bookkeeping for standby replication (primary side) and `standby_sequence_grps_` (follower side) — see `docs/standby_replication_protocol.md`. |

**Cache replacement.** `CcShard::Clean()` walks the LRU list from `clean_start_ccp_`, calling each
page's `parent_map_->CleanPageAndReBalance(page)`; one invocation scans ≤ 10 pages
(`freeBatchSize`) or 30 µs (`maxDuration`) then yields. Only "free" entries are evictable
(`VersionedLruEntry::IsFree()`: no locks and already checkpointed). `OutOfMemory()` is true when
the clean cursor has reached `tail_ccp_` without freeing anything; `OnDirtyDataFlushed()` resets
the cursor after a checkpoint. The actual eviction loop is driven by the self-re-enqueueing
`ShardCleanCc` request (`WakeUpShardCleanCc()`), and page cleaning itself is implemented with
`CcPageCleanGuard` (`cc_page_clean_guard.h`) as a mark (`clean_set_`) + compact pass.

**DispatchTask.** `CcShard::DispatchTask(idx, task)` wraps a `std::function<bool(CcShard&)>` in a
pooled `RunOnTxProcessorCc` and enqueues it to another shard — the standard way to run CPU-bound
work (e.g. `StoreRange::LoadSlice()`) on a specific shard's context.

## 3. LocalCcShards — the node-level container (`local_cc_shards.h`)

`LocalCcShards` owns the `std::vector<std::unique_ptr<CcShard>> cc_shards_` (one per
`core_num`) plus everything shared across shards:

- **Clock**: static `local_clock` advanced by a dedicated `tx_timer` thread (`TimerRun()`), read
  via `ClockTs()`; per-node `ts_base_` (`TsBase()` / monotonic CAS `UpdateTsBase()`) is the
  timestamp source for `CcShard::Now()`.
- **Catalog & table lifecycle**: `CreateCatalog / CreateDirtyCatalog / CreateReplayCatalog /
  GetCatalog / DropCatalogs` (the `CcShard` methods of the same names just delegate);
  `CreateCcTable` exists for unit tests only. Schema objects are shared across shards; per-shard
  `catalog_rw_cntl_` (`ReaderWriterObject<TableSchema>`, `reader_writer_cntl.h`) lets runtime
  readers use schemas without taking CC locks while DDL writers invalidate them.
- **Table ranges**: `table_ranges_` (range metadata per table) live in a dedicated mimalloc heap
  created by its own thread (`table_ranges_heap_thd_`, `InitializeTableRangesHeap()`), because
  range entries outlive any single shard heap. A separate heap/thread exists for hash-partition
  checkpoint scans.
- **Background workers** (`StartBackgroudWorkers()` in `src/cc/local_cc_shards.cpp`):
  flush-data workers (drain `FlushDataTask` buffers to the store handler, coroutine-based),
  range-split workers (`RangeSplitWorker`, count defaults to core_num/2 with
  `EXT_TX_PROC_ENABLED`), data-sync workers (one per core, `DataSyncWorker` — checkpoint scans),
  a statistics sync worker (`SyncTableStatisticsWorker`, if `realtime_sampling`), a heartbeat
  worker, a purge-deleted worker (only when cache replacement is disabled), an optional
  test-only kickout worker, and a jemalloc epoch worker under `WITH_JEMALLOC`. All are plain
  `std::thread`s (pthread waiters — safe to block).
- **Statistics**: `InitTableStatistics` / `GetTableStatistics` per (table, ng); sample pools feed
  `TemplateCcMap`'s `sample_pool_` for realtime key sampling.

## 4. CcMap hierarchy

`CcMap` (`cc_map.h`) is an abstract class with **one pure-virtual `Execute()` overload per CC
request type** (~40 of them) plus eviction hooks (`CleanPageAndReBalance`, `CleanEntry`,
`CleanBatchPages`) and `BackFill`/`BackFillArchives` (called when an async `FetchRecord`
completes; backfill removes the read intent the fetch installed and fills the entry, or erases
it on failure). It also hosts the shared lock-acquisition helpers (`AcquireCceKeyLock`,
`LockHandleForResumedRequest`, `ReleaseCceLock`, `MoveRequest`).

```
CcMap
 └── TemplateCcMap<KeyT, ValueT, VersionedRecord, RangePartitioned>   (template_cc_map.h, ~12k lines)
      ├── ObjectCcMap<KeyT, ValueT> = TemplateCcMap<KeyT, ValueT, false, false>   (object_cc_map.h)
      │     overrides ApplyCc / PostWriteCc / UploadBatchCc / UploadTxCommandsCc /
      │     KeyObjectStandbyForwardCc / RestoreCcMapCc / ReplayLogCc for object (KV/Redis) semantics
      ├── CatalogCcMap   = TemplateCcMap<CatalogKey, CatalogRecord, true, false>  (catalog_cc_map.h)
      ├── RangeCcMap<KeyT> = TemplateCcMap<KeyT, RangeRecord, true, false>        (range_cc_map.h)
      ├── RangeBucketCcMap = TemplateCcMap<RangeBucketKey, RangeBucketRecord, true, false>
      └── ClusterConfigCcMap = TemplateCcMap<VoidKey, ClusterConfigRecord, true, false>
```

- `VersionedRecord` selects MVCC payload storage; `RangePartitioned` adds per-entry
  `data_store_size_` tracking and per-range size accounting (`range_sizes_`, which triggers range
  splits at `StoreRange::range_max_size`).
- **RangeCcMap** maps a table's sorted ranges to partition IDs (class comment documents the
  partition-ID bisection scheme on splits). **RangeBucketCcMap** holds the bucket→owner mapping,
  fully pre-populated from `GetAllBucketInfos` at construction. **ClusterConfigCcMap** stores a
  single record on its `neg_inf_` sentinel purely for locking the cluster config, and lives only
  on core 0. These meta maps mostly exist so that DDL/topology changes can take CC locks like any
  data write.

### Page/entry organization (`cc_entry.h`, `template_cc_map.h`)

`TemplateCcMap` stores pages in `absl::btree_map<KeyT, unique_ptr<CcPage>> ccmp_`, keyed by each
page's first key, with sentinel `neg_inf_page_`/`pos_inf_page_` and sentinel entries
`neg_inf_`/`pos_inf_`. A `CcPage`:

- holds two parallel sorted vectors `keys_` and `entries_` (binary search via `Find`);
- splits at `split_threshold_ = 64` keys, merges/rebalances below `merge_threshold_ = 32`;
- is doubly linked to neighbors (`prev_page_`/`next_page_`) for ordered scans, and separately
  LRU-linked via its `LruPage` base (`lru_prev_`/`lru_next_`, `last_access_ts_`,
  `last_dirty_commit_ts_`, `smallest_ttl_`, `large_obj_page_`). **LRU is page-granular, not
  entry-granular.**

`CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>` (extends `LruEntry` →
`VersionedLruEntry`):

- `entry_info_.commit_ts_and_status_`: one packed uint64 — high 7 bytes commit ts, low 4 bits
  `RecordStatus` (Unknown/Normal/Deleted/...), bit 4 "latest version flushed" (non-MVCC), bit 5
  "being checkpointed". Versioned variants add an explicit `ckpt_ts_`; range-partitioned variants
  add `data_store_size_` (INT32_MAX = unknown, e.g. after log replay).
- payload: `VersionedPayload` (a `shared_ptr` current payload + `archives_` list of
  `VersionRecord`s for MVCC; `MvccGet(ts, …)` walks it, `ArchiveBeforeUpdate()` pushes the old
  version, `KickOutArchiveRecords(oldest_active_tx_ts)` trims it) or `NonVersionedPayload`
  (a `unique_ptr` updated in place — cheaper for large objects mutated by commands).
- `cc_lock_and_extra_` (`KeyGapLockAndExtraData*`, `non_blocking_lock.h`): **lazily attached** from
  the shard's `lock_vec_` by `GetOrCreateKeyLock()` and recycled when empty
  (`RecycleKeyLock`; an idle lock object may be reclaimed after `recycle_interval_us_` = 30 s).
  Besides the `NonBlockingLock` it carries the per-tx extra state: `pending_cmd_`,
  `dirty_payload_`(+status) for in-flight object commands, `buffered_cmd_list_` (out-of-order
  standby/replay commands buffered until their preceding version arrives), the standby
  `forward_entry_`, and `pin_count_` (keeps the entry in memory outside a tx context).
- A fresh entry starts with ts = 1 ("beginning of history") and `RecordStatus::Unknown` until a
  read fetches the value from the store or a write installs one.

### Execute() flow sketches (TemplateCcMap)

- **AcquireCc** (write-lock a key in the commit pipeline): term + schema-version check → locate or
  emplace the cce (`FindEmplace`; OOM ⇒ memory wait list) → `AcquireCceKeyLock(..., Write, ...)`.
  Success returns the entry's current `commit_ts_` (so the tx commits above it) and
  `last_vali_ts_ = max(shard.last_read_ts_, lock_ts)`; `ACQUIRE_LOCK_BLOCKED` returns false
  (request parked, remote requests send an ack first); other conflicts set an error (OCC
  back-off-and-retry).
- **ReadCc**: locate the key; deduce lock via `DeduceLockType`; on a memory miss with persistent
  data, install a read intent and `FetchRecord` (request re-executes after backfill); MVCC
  snapshot reads serve from `MvccGet` and may fetch archives from the store.
- **PostWriteCc** (commit/abort install): verifies the caller still holds the write lock
  (`lk->HasWriteLock() && WriteLockTx() == txn`; if not — e.g. the node failed over — it
  finishes silently), installs the new payload/Deleted status with the commit ts (commit_ts > 0)
  or just releases the lock (abort), archives the old version under MVCC, updates sample pools
  and range-size deltas, then `ReleaseCceLock` → `TryPopBlockingQueue` wakes waiters.

## 5. NonBlockingLock (`non_blocking_lock.h`, `non_blocking_lock.cpp`)

Per-entry lock word with four modes plus a fast-path counter:

- `read_intentions_` (`TxNumber -> count`): **conflict with nothing**; they only pin the entry
  against LRU kickout (used by OCC/MVCC reads and by scanners between batches).
- `read_locks_` (set) and `read_cnt_` (catalog-read fast path: `AcquireReadLockFast` just bumps a
  counter): block writers.
- `write_lk_type_` + `write_txn_`: a single `WriteIntent` or `WriteLock` holder.

Conflict rules (from `AcquireWriteLock` / `AcquireWriteIntent` / `AcquireReadLock` and the
blocking-queue comment): **WL conflicts with WL/WI/RL; WI conflicts with WL/WI; RL conflicts with
WL**. A read lock is also refused when a write lock sits at the head of the blocking queue
(writer-starvation fairness); the only queue head it overtakes is a WriteIntent. `NoLock`
requests (read-committed pk reads waiting for a newer pk version after an sk read) go to the
*front* of the queue since they conflict with nothing.

Behavior on conflict by protocol (`AcquireLock`, `cc_protocol.h`):

| Requested | OCC | OccRead | Locking |
|---|---|---|---|
| ReadIntent | always succeeds | always succeeds | n/a |
| ReadLock | n/a (not used) | n/a | **Blocked** → queue |
| WriteIntent | **Failed** → tx aborts/retries | Blocked → queue | Blocked → queue |
| WriteLock | **Failed** → tx aborts/retries | Blocked → queue | Blocked → queue |

`LockTypeUtil::DeduceLockType(cc_op, iso_level, protocol, is_covering_keys)`
(`tx_service/include/cc_protocol.h`) chooses the lock: `Write` ⇒ WriteLock; `ReadForWrite` ⇒
WriteIntent; plain `Read` ⇒ NoLock under ReadCommitted/Snapshot, ReadLock under RR/Serializable
with `Locking`, ReadIntent under RR/Serializable with OCC/OccRead; `ReadSkIndex` ⇒ NoLock for
snapshot or covering read-committed scans, else ReadLock.

Release paths (`ReleaseWriteLock`, `ReleaseReadLock`, `ClearTx`, `DowngradeWriteLock`) all end in
`TryPopBlockingQueue(ccs)`: pop queue heads while they are now grantable, granting the lock
*before* re-enqueueing the request to the shard queue (so a resumed request already owns its
lock). A second queue, `queue_block_cmds_`, holds blocked object commands (e.g. Redis
BLMOVE/BLMPOP) that re-execute when the object changes; `CcShard::active_blocking_txs_` tracks
those whose abort raced. Every grant/release also updates `CcShard::lock_holding_txs_` via
`UpsertLockHoldingTx`/`DeleteLockHoldingTx`, which is what deadlock detection and recovery read.

## 6. CcHandlerResult (`cc_handler_result.h`)

The async completion object a `TransactionExecution` waits on. `CcHandlerResult<T>` holds the
typed result value plus, in the base class: `is_finished_`, `error_code_`, an optional
**reference count** (`SetRefCnt(n)` for requests fanned out to n shards/nodes — each
`SetFinished()` decrements and only the last one flips `is_finished_` and runs `post_lambda_`),
and a `remote_ref_cnt_` to distinguish remote sub-results. `SetError()` latches the first error
then routes through `SetFinished()`. `ForceError()` exists solely for timing out remote requests:
the method comment documents the race contract — whichever of ForceError/remote-response wins,
the tx observes a finished result and may safely move on. Under `EXT_TX_PROC_ENABLED` the result
also re-enlists the blocked `txm_` for execution on finish (`is_blocking_`,
`runtime_resume_func_`). `block_req_check_ts_` timestamps the last liveness probe sent for a
request blocked in a lock queue.

## 7. Catalog of important CC request types

All in `tx_service/include/cc/cc_request.h` unless noted. One line each — purpose only.

| Group | Request | Purpose |
|---|---|---|
| Data path | `ReadCc` | read a key with deduced lock/intent; triggers `FetchRecord` on cache miss; MVCC version reads. |
| | `AcquireCc` | take a write lock (or queue/fail) on a key during commit. |
| | `PostWriteCc` | install the committed value / release the write lock on commit or abort. |
| | `PostReadCc` | post-commit read validation / read-lock release (sets `last_read_ts_`). |
| | `ApplyCc` | execute an object command (Redis-style) on the entry, possibly producing a dirty payload (`object_cc_map.h`). |
| | `UploadTxCommandsCc` | install a committed object-command batch (command-log replication path). |
| Broadcast (all shards/nodes, replicated maps) | `AcquireAllCc` | write-lock a key on *every* core/node — catalog, range, cluster-config writes. |
| | `PostWriteAllCc` | install/release the above everywhere. |
| Scans | `ScanOpenBatchCc` / `ScanNextBatchCc` / `ScanCloseCc` | open/advance/close a batched in-memory scan (hash-partitioned tables); remote twins `RemoteScanOpen`/`RemoteScanNextBatch`. |
| | `ScanSliceCc` | range-partitioned scan of a range slice, merging memory + store data. |
| Checkpoint & data sync | `CkptTsCc` | collect the per-shard min active-tx ts (checkpoint watermark) + heap/key stats; atomic-polling `Wait()`. |
| | `HashPartitionDataSyncScanCc` / `RangePartitionDataSyncScanCc` | scan dirty entries into `FlushRecord` vectors for checkpointing. |
| | `UpdateCceCkptTsCc` | mark scanned entries as flushed (advance `ckpt_ts_`) after a successful flush. |
| | `ReleaseDataSyncScanHeapCc` | release the data-sync scan heap memory on the owning shard. |
| Memory & maintenance | `KickoutCcEntryCc` | evict cc entries (variants in `CleanType`: range split/migration, bucket migration, alter table, truncate, deleted data). |
| | `ShardCleanCc` (`cc_req_misc.h`) | self-re-enqueueing LRU eviction driver when the heap is full. |
| | `DefragShardHeapCc` | walk pages and reallocate fragmented payloads on the shard heap. |
| | `CollectMemStatsCc`, `DbSizeCc`, `GetTableLastCommitTsCc`, `ActiveTxMaxTsCc`, `ClearTxCc` | shard-level stats/bookkeeping fan-outs (atomic-polling `Wait()`s). |
| Statistics | `AnalyzeTableAllCc`, `BroadcastStatisticsCc`, `SampleSubRangeKeysCc`, `ScanSliceDeltaSizeCcForRangePartition`, `ScanDeltaSizeCcForHashPartition` | sampling, stats broadcast, and range-split-size estimation. |
| Recovery / load | `ReplayLogCc`, `ParseDataLogCc` | replay WAL records into cc maps on failover (see `07-durability-and-recovery.md`, `10-log-service.md`). |
| | `RestoreCcMapCc`, `FillStoreSliceCc`, `InitKeyCacheCc`, `FetchRecordCc`, `FetchCatalogCc`, `FetchTableRangesCc`, `FetchTableStatisticsCc`, `FetchSnapshotCc`, `FetchBucketDataCc`, `FetchTableRangeSizeCc` (`cc_req_misc.h`) | async loads from the store handler that backfill maps/slices/metadata and re-enqueue their requesters. |
| Standby | `KeyObjectStandbyForwardCc` | apply a forwarded primary write on a standby (sequenced per shard); `EscalateStandbyCcmCc`, `RetryFailedStandbyMsgCc` support promotion and resend. |
| Migration / range ops | `UploadBatchCc` | bulk-insert migrated key batches into a new owner's cc maps. |
| | `UploadRangeSlicesCc`, `UploadBatchSlicesCc`, `UpdateKeyCacheCc` | install new range-slice metadata / slice data / key-cache updates after splits and migration (see `08-range-and-bucket-management.md`). |
| Control | `NegotiateCc` | look up a conflicting tx's `TEntry` to negotiate/lower its commit ts bound. |
| | `CheckTxStatusCc`, `CheckDeadLockCc`, `AbortTransactionCc`, `RequestAborterCc` | tx-status probes, wait-graph collection, victim abort. |
| | `ClearCcNodeGroup` (`cc_req_misc.h`) | drop all cc maps of a node group on leadership loss; atomic-polling `Wait()`. |
| | `RunOnTxProcessorCc` / `WaitableCc` (`cc_req_misc.h`) | run an arbitrary closure on a shard; `WaitableCc` adds atomic completion counting + polling `Wait()`. |

## 8. Deadlock detection (`tx_service/include/dead_lock_check.h`, `src/dead_lock_check.cpp`)

A singleton `DeadLockCheck` with its own `std::thread` runs every `time_interval_` (default
**5 s**, configurable via `SetTimeInterval`; `RequestCheck()` can trigger an immediate round).
Each round (`GatherLockDependancy`):

1. Enqueues one shared `CheckDeadLockCc` to **every local shard**; each shard's
   `CollectLockWaitingInfo()` walks `lock_holding_txs_` and records, per cc entry, the holder
   txids and waiter txids into `CheckDeadLockResult` (one slot per core, lock-free).
2. Sends a `DeadLockRequest` message to the leader of every other node group; their results come
   back via `MergeRemoteWaitingLockInfo`. Only entries that actually have waiters are shipped.
3. After all replies (waits at most half the interval), builds the tx wait-for graph
   (`GenerateTxWaitGraph` — edges waiter→holder via `TxEdge`), finds cycles (`DetectDeadLock`),
   and aborts victims (`RemoveDeadTransaction`) by sending `AbortTransactionCc` to the shard
   owning the victim's lock; the victim's queued request is failed with `DEAD_LOCK_ABORT`
   (`NonBlockingLock::AbortQueueRequest`).

The initiating node for a round is tracked (`check_node_id_`/`UpdateCheckNodeId`) so the cluster
has a single detector at a time.

## 9. Scanners (`ccm_scanner.h`)

`CcScanner` is the tx-side cursor abstraction over per-shard `ScanCache`s (batches of
`ScanTuple`s, default batch 128). It carries the scan's lock posture (`DeduceCcOperation`,
`DeduceScanTupleLockType`, covering-key/iso-level flags) so each batch acquires the right
intents/locks as it materializes.

- `HashParitionCcScanner<KeyT, ValueT>` *(sic)*: one `ShardCache` per core (plus per-bucket KV
  caches pulled from the store); `Merge()` k-way merges per-shard sorted batches into a global
  order; used with `ScanOpenBatchCc`/`ScanNextBatchCc`.
- `RangePartitionedCcmScanner<KeyT, ValueT, IsForward>`: a single `scan_cache_` since a range
  slice lives on one shard; used with `ScanSliceCc`, which itself merges memory entries with
  store-loaded slice data.
- Between batches the scanner leaves a **read intent** on the last tuple's entry so it is not
  evicted before `ScanNextBatch` resumes (`NonBlockingLock` comment on `read_intentions_`).

## 10. Gotchas / invariants

- **`Execute()` runs on the shard's processing context — which may be a brpc worker's main
  stack.** Under `ELOQ_MODULE_ENABLED`, brpc worker N drives CcShard N directly. A
  `bthread::Mutex`/`bthread::ConditionVariable` shared between `Execute()` (or anything it calls)
  and bthread waiters can **permanently deadlock a brpc worker** when the same primitive is
  acquired concurrently by multiple shards/requests: the custom brpc fork's wake routing prefers
  bthreads bound to a worker, so the wake can be handed to a bthread queued on the very worker
  that is blocked, and no timeout rescues it. The canonical fix is **`std::atomic` state +
  `bthread_usleep` exponential-backoff polling in `Wait()`** — see `CkptTsCc::Wait()`
  (`src/cc/cc_request.cpp`), `WaitableCc`/`ClearCcNodeGroup` (`cc_req_misc.h`, with explicit
  comments). Single-flight requests with a single bthread waiter (e.g.
  `WaitNoNakedBucketRefCc`) are structurally safe, as are pthread waiters. Full mechanism:
  `docs/02-threading-model.md`.
- **Never block or sleep inside `Execute()`.** Park the request (lock queue, fetch requester
  list, memory wait list) and return `false`, or re-enqueue yourself for batched work.
- **Return value of `Execute()` is an ownership statement.** `true` ⇒ the processor calls
  `Free()` and the pool may immediately reuse the object; never touch the request after returning
  `true`, and never return `true` if you stored the pointer somewhere (queue, requester list).
- **A resumed request already holds its lock.** `TryPopBlockingQueue` grants before re-enqueueing;
  resumed paths must go through `LockHandleForResumedRequest`, not re-acquire.
- **Term checks gate everything.** Every table-targeted request validates the node-group leader
  term on (re-)execution; entries' lock addresses (`CcEntryAddr`) embed the term so stale
  post-processing after failover is silently dropped (see `PostWriteCc::Execute`).
- **Entries can move; locks pin location indirection.** Page split/merge/defrag moves
  `CcEntry` objects between vectors; stable references go through `KeyGapLockAndExtraData`
  (which tracks `ccm_/page_/entry_` and is updated via `UpdateCcEntry`/`UpdateCcPage`) — never
  cache a raw `CcEntry*` across executions without holding a lock/intent/pin on it.
- **Eviction requires "free" entries.** No locks/intents/pins, not dirty (`CkptTs() >=
  CommitTs()` or flush bit set), no buffered commands. Read intents are how readers and scanners
  pin entries; forgetting one means your entry can vanish between batches.
- **Memory admission is per-shard.** `FindEmplace` returning a null entry means the shard heap is
  full; requests must use the memory wait list rather than spinning. Meta tables (catalog/range)
  bypass the limit.
- **`lock_holding_txs_` must mirror lock state exactly** — it drives checkpoint watermarks
  (`ActiveTxMinTs`), orphan-lock recovery (`CheckRecoverTx` for tx coordinators that died), and
  deadlock detection. `VerifyOrphanLock` asserts a finished tx left nothing behind.
- **TxNumber encodes location.** `(txn >> 32) >> 10` is the owning node group, `(txn >> 32) &
  0x3FF` the core; code routes recovery/negotiation requests with this arithmetic — do not invent
  tx numbers.
