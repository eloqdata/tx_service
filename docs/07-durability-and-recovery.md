# 07 — Durability & Recovery

**Summary.** In Data Substrate the in-memory cc maps on a node-group leader are the source of truth for hot data; durability comes from two cooperating sinks. Every committing write transaction synchronously appends a redo record to a replicated WAL ("log service") via the `TxLog` client (`tx_service/include/txlog.h`), and a background `Checkpointer` (`tx_service/include/checkpointer.h`) periodically scans dirty cc entries and flushes them to the kv store through `store::DataStoreHandler` (`tx_service/include/store/data_store_handler.h`). After a successful flush the checkpointer reports the checkpoint timestamp to all log groups (`UpdateCheckpointTs`) so logs at or below that ts can be truncated. Recovery of a failed node group is therefore: start with cold cc maps backed by the kv store, and replay the committed-but-not-checkpointed log records streamed back from the log service (`tx_service/include/fault/log_replay_service.h`), including resurrecting in-flight multi-stage operations (DDL, range split, bucket migration, cluster scaling). MVCC historical versions are kept as in-memory archive chains on `CcEntry` and flushed to a kv archive table; snapshots/backups of the kv store are orchestrated by `store::SnapshotManager`. This doc covers the client-side log contract only — the open log service internals are in [10-log-service.md](10-log-service.md), and standby replication in [standby_replication_protocol.md](standby_replication_protocol.md).

Related docs: [01-architecture-overview.md](01-architecture-overview.md), [02-threading-model.md](02-threading-model.md), [03-concurrency-control.md](03-concurrency-control.md), [04-transaction-execution.md](04-transaction-execution.md), [05-data-model-and-catalog.md](05-data-model-and-catalog.md), [06-distribution-and-clustering.md](06-distribution-and-clustering.md), [08-range-and-bucket-management.md](08-range-and-bucket-management.md), [09-store-handler.md](09-store-handler.md).

## 1. Durability model and modes

| Layer | Holds | Durability role |
|---|---|---|
| cc maps (memory, per shard) | latest committed versions + locks + MVCC archives | source of truth while the leader is alive |
| Log service (WAL) | redo records with `commit_ts > ckpt_ts` | crash recovery (redo-only; only committed txs write log, so there is no undo) |
| KV store (store handler) | all versions with `commit_ts <= ckpt_ts` (+ archive table) | cold data / cache misses / floor for log truncation |

### 1.1 Modes

Two global switches, set from `TxService::Initialize` parameters (`tx_service/include/tx_service.h:1192`, wired in `core/src/tx_service_init.cpp:312-313` as `skip_wal = !enable_wal`, `skip_kv = !enable_data_store`):

- `txservice_skip_wal` — data (DML) log records are not written: `TransactionExecution::Process(WriteToLogOp&)` short-circuits `TxLogType::DATA` requests (`tx_service/src/tx_execution.cpp:4985`). **Multi-stage 2PC logs (schema, range split, cluster scale, migration) are still written** so metadata stays consistent across the cluster. The checkpointer also skips `UpdateCheckpointTs` (`Checkpointer::NotifyLogOfCkptTs`, `tx_service/src/checkpointer.cpp:555-564`).
- `txservice_skip_kv` — no store handler. The `Checkpointer` constructor only starts its thread when `store_hd_ != nullptr` (`checkpointer.cpp:86-90`), and `CcNode::OnLeaderStart` skips `store_hd_->OnLeaderStart` (`tx_service/src/fault/cc_node.cpp:303-311`). All data must then fit in memory (eviction requires a checkpointed entry, §3.4).

### 1.2 In-process log server startup

`DataSubstrate::InitializeLogService` (`core/src/log_init.cpp:118`):

1. If `txlog_service_list` (flag or `[cluster]` ini section) is non-empty → standalone log service; just record the ip:port list for the `LogAgent`.
2. Otherwise (built with `WITH_LOG_SERVICE`) every distinct tx node hosts a log server on `local_port + 2`; the local `::txlog::LogServer` is constructed with `txlog_group_replica_num` (default 3, `log_init.cpp:11`) and the build-selected log-state backend (plain RocksDB or RocksDB-cloud; under `OPEN_LOG_SERVICE` the single-group implementation in `log_service/` — see [10-log-service.md](10-log-service.md)). `log_server_->Start()` runs it in-process (`log_init.cpp:623`).
3. The log server can push back on the tx service: when un-truncated replay log exceeds `notify_checkpointer_threshold_size` (default 1GB, checked every `check_replay_log_size_interval_sec`, `log_init.cpp:109-116`) it calls the `NotifyCheckpointer` RPC, handled by `RecoveryService::NotifyCheckpointer` → `Sharder::NotifyCheckPointer()` (`tx_service/src/fault/log_replay_service.cpp:386-395`).

## 2. WAL write path

### 2.1 Who writes log

`WriteToLogOp` (`tx_service/include/tx_operation.h:319-330`: a `TransactionOperation` holding a `LogClosure` + `CcHandlerResult<Void>`) is the only gateway from a transaction to the log service.

- A single-stage DML commit writes one DATA log after validation: `PostProcess(SetCommitTsOperation&)` (`tx_service/src/tx_execution.cpp:4271-4291`) requires log iff `!txservice_skip_wal && (cmd_set_.ObjectModified() || rw_set_.WriteSetSize() > 0 || rw_set_.CatalogWriteSetSize() > 0)`.
- Multi-stage operations embed several `WriteToLogOp` instances for their prepare/commit/clean stages (members like `prepare_log_op_`, `commit_log_op_`, `clean_log_op_`, `write_before_locking_log_op_` throughout `tx_operation.h:755-1481`).

### 2.2 Log record taxonomy (`tx_service/tx-log-protos/log.proto`)

`WriteLogRequest` carries `txn_number`, `commit_timestamp`, `tx_term`, `retry`, `log_group_id`, a `node_terms` map (ng_id → cc-leader term observed when locks were acquired) and one `LogContentMessage`:

| `log_content` arm | Message | Written by | Stages |
|---|---|---|---|
| `data_log` | `DataLogMessage` (per-ng binary blobs + piggybacked `schema_logs`) | DML commit | — |
| `schema_log` | `SchemaOpMessage` (old+new catalog blobs, table op/column op/index op) | DDL tx | `PrepareSchema, PrepareData, CommitSchema, CleanSchema` |
| `split_range_log` | `SplitRangeOpMessage` (old partition, new partition ids/keys, slice layout) | range split tx | `PrepareSplit, CommitSplit, CleanSplit` |
| `cluster_scale_log` | `ClusterScaleOpMessage` (new ng/node configs + per-ng `BucketMigrateMessage` progress) | cluster scale tx | `PrepareScale, ConfigUpdate, CleanScale` (+ `BucketMigrateStage`) |
| `migration_log` | `DataMigrateTxLogMessage` (bucket ids, worker txns) | bucket migration worker | `Prepare, Commit, Clean` |

The log service keeps multi-stage records in its state machine until the Clean record arrives — that is what makes recovery of in-flight operations possible (§5.2). It also dedups: re-sent migration / cluster-scale prepare logs get `DuplicateMigrationTx` / `DuplicateClusterScaleTx` responses.

### 2.3 Building a data log record — `FillDataLogRequest`

`TransactionExecution::FillDataLogRequest` (`tx_service/src/tx_execution.cpp:4531-4902`):

1. Fill header (`tx_term`, `txn_number`, `commit_timestamp`, `retry=false`).
2. Group the write set and the object command set by node group. For every cce address insert `{ng_id → term}` into `node_terms`; if a second key in the same ng has a **different term**, the ng failed over mid-tx and the fill fails with `NG_TERM_CHANGED` — the tx aborts without writing log (`tx_execution.cpp:4581-4591`). Read-lock ng terms are merged the same way (`:4888-4899`).
3. Keys being double-written during bucket/range forwarding are added to both the old and the new owner ng's blob (`forward_addr_` / `forward_entry_`, `:4608-4634`, `:4670-4698`).
4. Serialize one blob per ng (`node_txn_logs[ng_id]`), a concatenation of per-table sections:
   - **Record (write-set) section** (`:4709-4765`): 1-byte table-name length, name bytes, 1-byte engine, 1-byte type, 4-byte section length, then per key: serialized key, 1-byte `OperationType`, serialized record (omitted for deletes).
   - **Object command section** (`:4767-4846`): same table header, then per object: key bytes, 8-byte `object_version`, 8-byte ttl, 4-byte commands length, 1-byte `ignore_previous_version` (overwrite flag), 2-byte command count, then length-prefixed serialized commands. For objects the WAL stores **commands, not resulting values**; replay re-applies them.
5. Piggyback `schema_logs`: a `SchemaOpMessage` (stage `PrepareSchema`, old+dirty catalog blob) per entry of the catalog write set — used when a plain DML logically updates a catalog (e.g. an insert flips an index to multi-key) (`:4867-4885`). `FillCommitCatalogsLogRequest` / `FillCleanCatalogsLogRequest` (`:4904-4968`) later write the matching `CleanSchema` records.

### 2.4 Log group selection and the RPC flow

1. `Process(WriteToLogOp&)` (`tx_execution.cpp:4970-5026`) sets `log_group_id_ = txlog_->GetLogGroupId(tx_number)`; `GetLogGroupId = log_group_ids_[tx_number % LogGroupCount()]` (`tx_service/include/eloq_log_wrapper.h:145-149`). All records of one tx go to one log group.
2. `EloqLogAgent : TxLog` (`eloq_log_wrapper.h`) delegates to `txlog::LogAgent` (`tx_service/tx-log-protos/log_agent.h/.cpp`), the brpc client: per-log-group **leader cache** (`lg_leader_cache_`), channels to every log node (10 s timeout, 3 brpc retries, `log_agent.cpp:96-100`), async `LogService_Stub::WriteLog` (`log_agent.cpp:356-371`). Non-open builds run a background `check_leader_thd_` refreshing leaders via the braft route table; under `OPEN_LOG_SERVICE`, `RefreshLeader` is a no-op and the log service instead pushes leader changes via the `UpdateLogGroupLeader` RPC into `RecoveryService` → `LogAgent::UpdateLeaderCache`.
3. `LogClosure::Run` (`tx_service/include/log_closure.h:61-103`) maps the response to `CcErrorCode`: rpc failure or `Unknown` → `LOG_CLOSURE_RESULT_UNKNOWN_ERR`; `NotLeader` → `LOG_NODE_NOT_LEADER`; `Fail` → `WRITE_LOG_FAILED`; `Success` → finished.
4. `WriteToLogOp::Forward` (`tx_service/src/tx_operation.cpp:1029-1118`) implements retries (budget `retry_num_ = RETRY_NUM = 3`, `tx_operation.h:64,106`):
   - `LOG_CLOSURE_RESULT_UNKNOWN_ERR` on a DATA log → resend with `retry=true` while still leader; after the budget, give up with result **unknown**.
   - `LOG_NODE_NOT_LEADER` → `RequestRefreshLeader(log_group_id)` and rerun.
   - On success, the `WriteLogResponse.node_group_leader_info` map refreshes the sharder's ng-leader cache (`UpdateLeadersFromLogInfo`, `tx_operation.cpp:972-1027`).
5. `PostProcess(WriteToLogOp&)` (`tx_execution.cpp:5028-5178`): success → `Committed` and normal post-processing; definite failure → `Aborted` (write locks released, API error `WRITE_LOG_FAIL`); unknown → status `Unknown`, API error `LOG_SERVICE_UNREACHABLE`, and post-processing releases **zero write locks** (`:5150-5156`) — the orphan locks are resolved by lock recovery (§5.4), never by timeout.

## 3. Checkpointing

### 3.1 The Checkpointer thread

`Checkpointer` (`tx_service/include/checkpointer.h`, `tx_service/src/checkpointer.cpp`) runs one `std::thread` named `checkpointer` (only if a store handler exists). Loop (`Run()`, `checkpointer.cpp:433-490`):

- Wake every `checkpointer_interval` seconds (default 10, `core/src/tx_service_init.cpp:10`) **or** on `Notify(request_ckpt=true)`; explicit requests are rate-limited to one per `checkpointer_min_ckpt_request_interval` seconds (default 5, dedup via CAS on `request_ckpt_`, `checkpointer.cpp:497-518`).
- On `Terminate()` the loop exits and runs one final `Ckpt(is_last_ckpt=true)` which **waits for all flush tasks** before the process may shut down (`checkpointer.cpp:368-375`, `Join()`).

Notification sources: tx processors that find nothing clean to evict (`CcShard::NotifyCkpt`), the dirty-memory trigger (§3.5), the log service's `NotifyCheckpointer` RPC (§1.2), data-sync workers running dry, and shutdown.

### 3.2 Choosing the checkpoint ts — `GetNewCheckpointTs`

`GetNewCheckpointTs(ng, is_last_ckpt)` (`checkpointer.cpp:98-139`) broadcasts a `CkptTsCc` to every shard (`tx_service/include/cc/cc_request.h:2983-3156`) and takes the minimum:

- **Leader**: each shard contributes `ActiveTxMinTs(ng)` (`tx_service/include/cc/cc_shard.h:622-676`) = `min(wlock_ts − 1)` over all lock-holding txs of that ng (meta tables excluded); if none, the freshly re-synced wall clock − 1. So **ckpt_ts is always strictly below the earliest commit-pending write** — every record with `commit_ts <= ckpt_ts` is final. The same walk piggybacks `CheckRecoverTx` for locks held > 5 s (§5.4).
- **Standby**: shards contribute `MinLastStandbyConsistentTs()` instead, and `CkptTsCc::UpdateStandbyConsistentTs` (`cc_request.h:3108-3143`) pushes the primary's consistent ts + per-shard message sequence ids to subscribed standbys.
- **MVCC delay** (`checkpointer.cpp:123-136`): with MVCC on and not the final ckpt, let `delayed = raw − ckpt_delay_seconds·10⁶` (default 5 s, `tx_service_init.cpp:18`) and `min_si = TxStartTsCollector::GlobalMinSiTxStartTs()`; the result is `delayed` if `min_si < delayed`, else `min_si` if `min_si < raw`, else `raw` — i.e. clamped into `[delayed, raw]` around the oldest snapshot reader, so versions that active snapshot txs may read stay in memory.

### 3.3 One checkpoint round — `Ckpt()`

For each local node group (`checkpointer.cpp:141-431`):

1. Special cases first: a **candidate standby** doesn't checkpoint — it (re-)requests a storage snapshot from the primary via `RequestStorageSnapshotSync` (`:147-200`, §7); a synced **standby on shared storage** skips entirely (`:203-213`); a standby on private storage checkpoints its own kv store.
2. Skip the ng unless leader/candidate-leader (term from `Sharder`); compute `ckpt_ts` (§3.2); skip if `ckpt_ts <= GetNodeGroupCkptTs(ng)`.
3. `GetCatalogTableNameSnapshot(ng, ckpt_ts)` → map table → `is_dirty`. For each non-meta table: if not dirty and `GetTableLastCommitTsCc < last_ckpt_ts`, skip; else `EnqueueDataSyncTaskForTable(..., can_be_skipped = !is_last_ckpt, status)` (`:274-329`). The smallest valid per-table `last synced ts` may already allow an early `UpdateNodeGroupCkptTs` + `NotifyLogOfCkptTs` + `BrocastPrimaryCkptTs` (`:338-358`).
4. Mark `status->all_task_started_`; if all scans finished but flushes are pending, force-flush the buffer (`FlushCurrentFlushBuffer`). On the last ckpt, block on `status->cv_` until `unfinished_tasks_ == 0` (`:360-375`).
5. When all tasks are done without error and `need_truncate_log_`, truncate using `status->truncate_log_ts_` (which may exceed this round's `ckpt_ts` — see §8): `UpdateNodeGroupCkptTs` → `NotifyLogOfCkptTs` (→ `TxLog::UpdateCheckpointTs`, skipped under `txservice_skip_wal`) → `BrocastPrimaryCkptTs` to standbys (`:377-425`).

`LogAgent::UpdateCheckpointTs` (`log_agent.cpp:437-469`) fires the `UpdateCheckpointTsRequest{ng, term, ckpt_ts}` at **every** log group (100 ms timeout, fire-and-forget — a missed update just delays truncation until the next round).

### 3.4 Data sync tasks, scans, flush pipeline

Machinery: `tx_service/include/data_sync_task.h` + `tx_service/include/cc/local_cc_shards.h` / `src/cc/local_cc_shards.cpp`.

| Object | Granularity | Purpose |
|---|---|---|
| `DataSyncTask` | per core (hash-partitioned) or per range (range-partitioned) | one scan+flush unit; carries `data_sync_ts_`, ng id/term, flags (`is_dirty_`, `forward_cache_`, `is_standby_node_ckpt_`, `sync_ts_adjustable_`) |
| `DataSyncStatus` | per ckpt round per ng | counts `unfinished_tasks_` / `unfinished_scan_tasks_`, accumulates `truncate_log_ts_`, `need_truncate_log_`, error code |
| `FlushTaskEntry` | per scan batch | `data_sync_vec_` (base rows), `archive_vec_` (MVCC versions), `mv_base_vec_` (base→archive moves), schema, owning task |
| `FlushDataTask` | per data-sync worker | buffer of `FlushTaskEntry`s keyed by kv table name; flushed when > `max_pending_flush_size_` (default 100 MB, `data_sync_task.h:301`) |

Flow:

1. **Fan-out & dedup** — `EnqueueDataSyncTaskForTable` (`local_cc_shards.cpp:2791-2903`) creates per-core / per-range tasks. A `TaskLimiter` keyed by (ng, term, table, core/range) dedups (`EnqueueDataSyncTaskToCore`, `:2670-2789`): a skippable (regular-ckpt) task that already has a pending peer only raises `latest_pending_task_ts_` and sets `SetNoTruncateLog()` for this round; the queued task's `data_sync_ts_` is adjusted upward when it finally runs (`sync_ts_adjustable_`). Non-skippable tasks (migration, create index, last ckpt) always queue.
2. **Scan** — per-core `DataSyncWorker` threads (`local_cc_shards.cpp:3100-3209`, gated by `scan_concurrency_`) run `DataSyncForHashPartition` / `DataSyncForRangePartition`, repeatedly enqueueing `HashPartitionDataSyncScanCc` / `RangePartitionDataSyncScanCc` (`cc_request.h:3587` / `:3874`) onto the owning shard as **low-priority cc requests** (`EnqueueLowPriorityCcRequestToShard`, batch size `DATA_SYNC_SCAN_BATCH_SIZE`). The scan collects `FlushRecord`s for dirty entries with `commit_ts <= data_sync_ts` (plus archive versions), marks entries being-ckpt, and reports `LOG_NOT_TRUNCATABLE` if entries had to be skipped (e.g. EloqKV entries with unreplayed buffered commands) → `SetEntriesSkippedAndNoTruncateLog()` (`local_cc_shards.cpp:4902-4924`). During bucket migration the scan can forward records to dirty-bucket owners via `UploadBatch` (`forward_cache_`, `:4929-5058`).
3. **Flush** — batches go through `AddFlushTaskEntry` → per-worker `pending_flush_work_` queues (with merge, `local_cc_shards.cpp:5820-5907`) consumed by `FlushDataWorker` threads. `FlushDataImpl` (`:5915-6100`) executes in order:
   1. `CopyBaseToArchive` (MVCC only) — copy kv base rows about to be overwritten into the archive table;
   2. `PutAll` — write base rows;
   3. `PutArchivesAll` (MVCC only) — write in-memory archive versions;
   4. `PersistKV` if `store_hd_->NeedPersistKV()` (e.g. EloqStore) — batched fsync-equivalent;
   5. `UpdateCceCkptTsCc` per shard — stamp `cce->SetCkptTs(commit_ts)` on every flushed entry (only when `need_update_ckpt_ts_`);
   6. `WaitableCc` → `CcShard::OnDirtyDataFlushed()` — re-arm kickout requests blocked on dirty data.
4. **Completion & truncation** — `DataSyncTask::SetFinish/SetError` (`tx_service/src/data_sync_task.cpp:113-198`) maintain `truncate_log_ts_ = min(data_sync_ts_)` over the round's tasks. The last task to finish (or `Ckpt()` itself) performs `UpdateNodeGroupCkptTs` + `UpdateCheckpointTs` + `BrocastPrimaryCkptTs` (`tx_service/src/standby.cpp:107`, the `UpdateStandbyCkptTs` RPC). **Truncation contract: never report a ckpt ts unless every entry with `commit_ts <= ts` of this ng is durable in the kv store.**

### 3.5 ckpt_ts on entries, eviction, dirty-memory trigger

- `CkptTs()` / monotonic `SetCkptTs()` live in `VersionedLruEntry`'s entry info (`tx_service/include/cc/cc_entry.h:580-662`). `IsDirty()` = `CommitTs > CkptTs` (versioned) or flush-bit unset (non-versioned); `IsFree()` (no locks ∧ not dirty) gates eviction — **only checkpointed entries can be kicked out** (`LocalCcShards::KickoutPage`, `local_cc_shards.h:1566`, additionally consults range `last_sync_ts`/dirty-range version for range tables). When eviction finds nothing free, the tx processor calls `ckpter_->Notify()` — memory pressure drives checkpointing.
- `CcShard::CheckAndTriggerCkptByDirtyMemory` (`tx_service/src/cc/cc_shard.cpp:484-521`), checked every `dirty_memory_check_interval` (1000) key-stat updates: `dirty_memory = allocated_heap × dirty_key_ratio`; if it exceeds `dirty_memory_size_threshold_mb` (default 0 → 10% of the per-shard memory limit, min 1 MB, `cc_shard.cpp:126-141`), call `NotifyCkpt(true)`.

## 4. Data sync beyond checkpointing (overview)

The same task/scan/flush pipeline serves (all with `can_be_skipped=false`, usually waited on via a `CcHandlerResult`):

- **Range split** — sync a subrange before ownership changes (`EnqueueDataSyncTaskForSplittingRange`, `local_cc_shards.cpp:2573`; the second `DataSyncTask` constructor with `start_key/end_key/export_base_table_items`). See [08-range-and-bucket-management.md](08-range-and-bucket-management.md).
- **Bucket migration / cluster scale** — `EnqueueDataSyncTaskForBucket` (`local_cc_shards.cpp:2905`) flushes bucket data and forwards cache to the new owner. See [06-distribution-and-clustering.md](06-distribution-and-clustering.md) and [08-range-and-bucket-management.md](08-range-and-bucket-management.md).
- **Standby bootstrap & backup** — `SnapshotManager::RunOneRoundCheckpoint` (§7), `FlushDataAll` / `NotifyShutdownCkpt` RPCs.

## 5. Recovery

### 5.1 Leader start

The (proprietary) host manager elects this node leader of ng and issues the `OnLeaderStart` RPC (`tx_service/src/remote/cc_node_service.cpp:208-259`) → `Sharder::OnLeaderStart` → `CcNode::OnLeaderStart` (`tx_service/src/fault/cc_node.cpp:257-476`):

1. Single-flight guard (`is_processing_`); reject if already candidate/leader at ≥ term. A **candidate standby** cannot escalate: it clears its cache and asks for leader transfer (`:288-301`).
2. `store_hd_->OnLeaderStart(ng, term)` unless `txservice_skip_kv`.
3. If the node was a synced **standby** of this ng: unsubscribe all shards, drain in-flight standby requests, wait for DDL pins, then keep the warm cache — `replay_start_ts = MinLastStandbyConsistentTs + 1`, so only the log tail is replayed (`:313-453`). Otherwise cc maps start empty and `replay_start_ts = 0`.
4. `SetCandidateTerm(ng, term)`, clear `recovered_log_groups_`. The node is now a **candidate leader**: `LeaderTerm` is still −1 and normal cc requests are rejected; init bucket info and prebuilt tables; with cache replacement disabled, `RestoreTxCache` bulk-loads the store (`:456-473`).
5. Replay is requested via `TxLog::ReplayLog(ng, term, replay_ip, replay_port, all groups, replay_start_ts)` — a synchronous RPC announcing the new term to every log group and asking each to stream committed records back (`log_agent.h:134-149`). In the no-host-manager single-node path `Sharder` does this directly (`tx_service/src/sharder.cpp:478-503`). `RecoveryService`'s notify thread additionally watches candidate ngs with no active replay stream and re-requests replay every 5 s (`kReplayCheckIntervalUs`, `log_replay_service.cpp:99-170`).

### 5.2 The replay stream

Each log group leader opens a brpc stream to the node's `RecoveryService` (a `txlog::LogReplayService` on the log-replay port; `Connect` registers a `ConnectionInfo` per (ng, log group)). `on_received_messages` (`tx_service/src/fault/log_replay_service.cpp:430-825`) consumes `ReplayMessage`s, coordinated across log groups by a per-ng `NodeGroupReplayBarrier` with three phases (`ReplayPhase`): `CollectSplitTable` → `ReplayDdlLog` → `ReplayDataLog`.

Per message, strictly ordered:

1. **First message**: collect the set of tables with in-flight range splits from all `split_range_op_msgs` and wait at barrier #1, so schema replay knows to restore a write *intent* instead of a write lock on those catalogs (`:464-510`).
2. **Cluster scale op** (if any): one `ReplayLogCc` into the `ClusterConfig` ccm, then one into the `RangeBucket` ccm, each waited to completion (`:516-575`).
3. **Schema ops**: one `ReplayLogCc` per `SchemaOpMessage` into the catalog ccm, sequentially (`:578-614`).
4. **Range split ops**: parse, record split-range commit ts (needed by data replay to route keys), one `ReplayLogCc` each (`:617-697`); then barrier #2 (`WaitDdlReplayed`) gates data replay until **all** log groups finished DDL.
5. **Data records**: `ParseDataLogCc` requests round-robined across shards parse `binary_log_records` and fan out per-table `ReplayLogCc`s (`:712-727`). Backpressure: > 10000 in-flight requests blocks the stream callback (`:799-824`).
6. **Finish message**: wait for all in-flight requests, then `Sharder::FinishLogReplay(ng, term, lg_id, latest_txn_no, latest_commit_ts, last_ckpt_ts)` and close the stream (`:728-796`).

`ReplayLogCc` (`cc_request.h:4620`) dispatches to `CcMap::Execute(ReplayLogCc&)` overloads:

- **Object/data maps** (`tx_service/include/cc/object_cc_map.h:2205+`): deserialize each key+commands; skip keys whose bucket is not owned by this ng, keys sharded to other cores (cooperative multi-core walk via `NextCore`), records with `commit_ts <= cce->CommitTs()` (idempotency) or `commit_ts < schema_ts_`; otherwise re-apply the logged commands (or install/buffer them), handling TTL-expired objects.
- **Catalog map** (`tx_service/include/cc/catalog_cc_map.h:1272+`): for `PrepareSchema/PrepareData` stages, `CreateReplayCatalog` restores current + dirty schema, then `LocalCcShards::CreateSchemaRecoveryTx` (`local_cc_shards.cpp:767-822`) spawns a thread that builds a recovery `TransactionExecution` — `SetRecoverTxState(txn, tx_term, commit_ts)` (`tx_service/include/tx_execution.h:223`) reuses the original tx number with status `Recovering` — and drives a `SchemaRecoveryTxRequest` (`tx_service/include/tx_request.h:1216`) to roll the DDL forward, or aborts it. For commit/clean stages the final schema is installed directly, re-upserting the kv table when storage is not shared (`catalog_cc_map.h:1358-1453`).
- **Range map** (`range_cc_map.h:823`): restores range entries; an in-flight split spawns a `RangeSplitRecoveryTxRequest` via `CreateSplitRangeRecoveryTx` (`local_cc_shards.cpp:824+`).
- **Cluster config / bucket maps** (`cluster_config_cc_map.h:287`, `range_bucket_cc_map.h:371`): restore topology / bucket ownership; unfinished operations resume on a recovery txm via `RecoverClusterScale(scale_msg, dm_started, dm_finished)` and `RecoverDataMigration(migrate_msg, idx, status)` (`tx_execution.h:296-302`).

Every replay cc checks the **candidate term** (`CandidateLeaderTerm(ng)`, e.g. `catalog_cc_map.h:1286-1294`) and aborts with `REQUESTED_NODE_NOT_LEADER` if leadership moved on. Any error sets the connection's `recovery_error_`; the replay task is re-queued with a delay (`delayed_replay_queue_`, ~10 s) and the whole stream restarted.

### 5.3 Finishing replay

`Sharder::FinishLogReplay → CcNode::FinishLogGroupReplay` (`cc_node.cpp:80-132`):

1. Ignore the finish if the candidate term changed.
2. Raise `last_ckpt_ts_` to the max reported by log groups (`UpdateCkptTs`).
3. For the native ng, `SetTxIdent(latest_txn_no)` seeds tx-number generation above anything already logged.
4. Add the log group to `recovered_log_groups_`; only when **all** `log_group_cnt_` groups finished: `SetLeaderTerm(ng, candidate_term)`, clear candidate term, `NodeGroupFinishRecovery` — the ng starts serving. (Test envs without a log agent skip straight to leader, `sharder.cpp:496-502`.)

### 5.4 Orphan lock recovery — `CheckTxStatus` / `RecoverTx`

Trigger: a conflicting tx observes a lock held > 5 s → `CcShard::CheckRecoverTx` (`tx_service/src/cc/cc_shard.cpp:1261-1358`, rate-limited per lock via `last_recover_ts_`) → `Sharder::RecoverTx` → `RecoveryService::RecoverTx` queue. `ProcessRecoverTxTask` (`log_replay_service.cpp:1017-1170`):

1. Ask the lock-holder's coordinator ng (`CheckTxStatus` RPC, `cc_node_service.cpp:295+`; term-checked → `NOT_FOUND` if the coordinator failed over): `ONGOING` → do nothing; `ABORTED` → `ClearTx` (broadcast `ClearTxCc` to all shards releases the tx's locks).
2. Otherwise consult the tx's log group: `TxLog::RecoverTx(txn, tx_term, write_lock_ts, cc_ng_id, cc_ng_term, replay_addr)`. `RecoverTxStatus` semantics (`txlog.h:33-49`):

| Status | Meaning | Action |
|---|---|---|
| `Alive` | coordinator term unchanged, tx considered alive | combined with the inconclusive status check → tx aborted proactively; `ClearTx` |
| `NotCommitted` | coordinator failed over, no log record — can never commit (term mismatch) | `ClearTx` |
| `Committed` | log record exists | log group ships that tx's records back over a replay stream flagged `is_lock_recovery=true`; replay only applies a record if the tx still holds a write lock/intent on the entry (`object_cc_map.h:2289-2299`), then releases it |
| `RecoverError` | log group unreachable | do nothing; next conflicting tx retries |

## 6. MVCC & archives

- **Version chains**: on update of a versioned entry, `CcEntry::ArchiveBeforeUpdate` (`cc_entry.h:1101-1125`) moves the current payload to the front of the per-entry `archives_` list (`std::list<VersionRecord>`, descending `commit_ts`). `AddArchiveRecord(s)` back-fill fetched historical versions; `KickOutArchiveRecords(oldest_active_tx_ts)` (`:1207-1240`) trims versions no active tx can read.
- **Flush**: the ckpt scan emits archive versions and base→archive moves; `FlushDataImpl` persists them via `CopyBaseToArchive` + `PutArchivesAll` (`data_store_handler.h:309-325`). The MVCC ckpt delay (§3.2) keeps recently superseded versions in memory while snapshot readers may still need them.
- **Visibility**: `CcEntry::MvccGet(ts, rec)` (`cc_entry.h:1265-1325`) walks current + archives. If nothing visible in memory, the resulting status encodes where to look: `BaseVersionMiss` (`ckpt_ts <= ts` — base kv row suffices), `ArchiveVersionMiss` (`ckpt_ts > ts` — must read the kv archive table), `VersionUnknown` (`ckpt_ts == 0` — try both). The read path then issues `CcShard::FetchRecord(..., is_archive)` and blocks the `ReadCc` until backfill (`template_cc_map.h:2134-2160`); the store handler serves `FetchVisibleArchive` (latest version with `commit_ts <= upper_bound_ts`) / `FetchArchives` (`data_store_handler.h:331-347`).
- **GC bound**: `TxStartTsCollector` (`tx_service/include/tx_start_ts_collector.h`; default 60 s period, configured via `collect_active_tx_ts_interval_seconds` = 2 s) aggregates the global min snapshot-isolation tx start ts across node groups (`GetMinTxStartTs` RPC); it bounds the ckpt delay (§3.2) and archive trimming.

## 7. Snapshots & backup (overview)

`store::SnapshotManager` (`tx_service/include/store/snapshot_manager.h`, singleton on the primary; standby-sync worker thread + 1-thread `tx_snapbk` pool). Currently used by EloqKV only (header comment, `snapshot_manager.h:49`).

**Standby bootstrap** (full protocol in [standby_replication_protocol.md](standby_replication_protocol.md)):

1. A candidate standby asks the primary on every ckpt attempt: `Checkpointer::Ckpt` → `RequestStorageSnapshotSync` RPC (`checkpointer.cpp:147-200`; for non-EloqStore backends includes a user + rsync dest path from `SnapshotSyncDestPath()`).
2. The primary queues it (`OnSnapshotSyncRequested`, deduped per node/standby-term; a subscription-time `active_tx_max_ts` barrier is registered via `RegisterSubscriptionBarrier`).
3. `SyncWithStandby` (`tx_service/src/store/snapshot_manager.cpp:863+`): discard stale-term tasks; only serve a task once the current ckpt ts passes its subscription barrier; then `RunOneRoundCheckpoint(ng, term)` → `CreateSnapshotForStandby` → `SendSnapshotToRemote(ng, term, files, dest)` → notify the standby with the `OnSnapshotSynced` RPC, which lands in `CcNode::OnSnapshotReceived` / `DataStoreHandler::OnSnapshotReceived`. EloqStore builds additionally track snapshot ts for cleanup.
4. The standby then replays standby messages on top of the snapshot (see the standby protocol doc).

**Backup**: `CreateBackup` / `FetchBackup` / `TerminateBackup` RPCs (`tx_service/include/proto/cc_request.proto:538-540`, plus `CreateClusterBackup`/`FetchClusterBackup`) drive `SnapshotManager::CreateBackup` → queued `HandleBackupTask`: one round of checkpoint, then `DataStoreHandler::CreateSnapshotForBackup(backup_name, files, ts)` and optional remote upload; status polled via `BackupTaskStatus`, snapshots removed with `RemoveBackupSnapshot`.

## 8. Invariants & gotchas

- **ckpt_ts never passes an uncommitted write.** `ActiveTxMinTs` returns `min(wlock_ts) − 1`; a tx publishes its write-lock ts before receiving a commit ts. Corollary: a stuck tx (orphan lock) blocks log truncation for the whole ng — which is exactly why `CkptTsCc` piggybacks `CheckRecoverTx`.
- **Truncate only what is durable.** `UpdateCheckpointTs` is sent with `truncate_log_ts_` = min `data_sync_ts_` over the round's tasks, and only when no task errored and nothing was skipped. Skipped entries (buffered commands → `LOG_NOT_TRUNCATABLE`) or task-limiter dedup (`SetNoTruncateLog`) silently turn the round into flush-without-truncate.
- **`truncate_log_ts_` may exceed the round's `ckpt_ts`** when a queued task's ts was adjusted upward; `Ckpt()` deliberately truncates with `truncate_log_ts_`, not `ckpt_ts` (`checkpointer.cpp:397-424`).
- **Replay is idempotent.** Data replay skips `commit_ts <= cce.CommitTs()` and `commit_ts < schema_ts_`; catalog replay refuses to downgrade an existing catalog version; bucket-ownership filters drop foreign keys. Re-streaming after an error is safe.
- **Term discipline.** Replay runs under the *candidate* term; `LeaderTerm` flips only after **all** log groups finish (`FinishLogGroupReplay`). A `WriteLogRequest` whose `node_terms` mismatch the log service's view is rejected, so a tx that straddles a participant failover cannot commit; `FillDataLogRequest` already aborts locally on intra-tx term divergence (`NG_TERM_CHANGED`).
- **Unknown log result ⇒ locks stay.** After exhausting retries the coordinator leaves write locks in place (status `Unknown`); correctness relies on `CheckTxStatus`/`RecoverTx`, never on lock timeouts.
- **Eviction needs checkpointing.** Only entries with `CommitTs <= CkptTs` are evictable. With `skip_kv` there is no checkpointer at all; with cache replacement disabled, `RestoreTxCache` reloads the entire store on leader start.
- **Standby checkpoints** happen only on non-shared storage (each replica owns its kv store) and never call `UpdateCheckpointTs` (`is_standby_node_ckpt_`, `data_sync_task.cpp:145-155`); on shared storage only the primary flushes and broadcasts its ckpt ts so standbys can advance entry `ckpt_ts` and evict.
- **bthread caveat**: `CkptTsCc::Wait` / `WaitableCc::Wait` poll atomics with `bthread_usleep` backoff (`cc_req_misc.cpp:1138-1149`) instead of bthread condition variables — see the wake-routing deadlock pattern in [02-threading-model.md](02-threading-model.md) and `CLAUDE.md` before adding any new waitable cc request to this module.

## Appendix A — Configuration knobs

| Flag (ini key in `[local]` unless noted) | Default | Effect | Defined in |
|---|---|---|---|
| `checkpointer_interval` | 10 s | periodic ckpt wakeup | `core/src/tx_service_init.cpp:10` |
| `checkpointer_delay_seconds` | 5 s | MVCC ckpt-ts delay window (§3.2) | `tx_service_init.cpp:18` |
| `checkpointer_min_ckpt_request_interval` | 5 s | rate limit for `Notify(true)` requests | `tx_service_init.cpp:11` |
| `collect_active_tx_ts_interval_seconds` | 2 s | `TxStartTsCollector` period | `tx_service_init.cpp:19` |
| `dirty_memory_check_interval` | 1000 calls | how often a shard re-evaluates dirty memory | `tx_service_init.cpp:58` |
| `dirty_memory_size_threshold_mb` | 0 (= 10% shard limit) | dirty-memory ckpt trigger | `tx_service_init.cpp:61`, `cc_shard.cpp:126-141` |
| `txlog_service_list` (`[cluster]`) | "" | external log service endpoints; empty = in-process log server | `core/src/log_init.cpp:10` |
| `txlog_group_replica_num` (`[cluster]`) | 3 | replicas per log group | `log_init.cpp:11` |
| `enable_txlog_request_checkpoint` | true | log server may RPC `NotifyCheckpointer` | `log_init.cpp:34` |
| `notify_checkpointer_threshold_size` | 1GB | replay-log size that triggers that RPC | `log_init.cpp:113` |
| `report_ckpt` | true (Debug) | log per-round ckpt + shard memory reports | `checkpointer.cpp:44-53` |

## Appendix B — Key files

| Area | Files |
|---|---|
| Log client contract | `tx_service/include/txlog.h`, `tx_service/include/eloq_log_wrapper.h`, `tx_service/include/log_closure.h`, `tx_service/include/log_type.h`, `tx_service/tx-log-protos/log_agent.{h,cpp}`, `tx_service/tx-log-protos/log.proto` |
| WAL write path | `tx_service/include/tx_operation.h` (`WriteToLogOp`), `tx_service/src/tx_operation.cpp:966-1131`, `tx_service/src/tx_execution.cpp:4531-5178` |
| Checkpointing | `tx_service/include/checkpointer.h`, `tx_service/src/checkpointer.cpp`, `tx_service/include/data_sync_task.h`, `tx_service/src/data_sync_task.cpp`, `tx_service/include/cc/cc_request.h` (`CkptTsCc`, `*DataSyncScanCc`), `tx_service/src/cc/local_cc_shards.cpp` (workers, `FlushDataImpl`) |
| Recovery | `tx_service/include/fault/log_replay_service.h`, `tx_service/src/fault/log_replay_service.cpp`, `tx_service/include/fault/cc_node.h`, `tx_service/src/fault/cc_node.cpp`, `Execute(ReplayLogCc&)` overloads in `tx_service/include/cc/*_cc_map.h` |
| MVCC / archives | `tx_service/include/cc/cc_entry.h`, `tx_service/include/tx_start_ts_collector.h`, archive APIs in `tx_service/include/store/data_store_handler.h` |
| Snapshots / backup | `tx_service/include/store/snapshot_manager.h`, `tx_service/src/store/snapshot_manager.cpp`, snapshot APIs in `data_store_handler.h:404-517` |
| Bootstrapping | `core/src/log_init.cpp`, `core/src/tx_service_init.cpp` |
