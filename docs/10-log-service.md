# 10 — Log Service (Open-Source WAL, `log_service/`)

The log service is the write-ahead log of the tx service: at commit time a transaction ships its redo records to a *log group*, and a recovering cc node group leader streams those records back to rebuild un-checkpointed state (see `07-durability-and-recovery.md`). This document covers the **open-source** implementation in `log_service/`, selected by CMake option `OPEN_LOG_SERVICE=ON` (the default; `CMakeLists.txt:13`). A closed-source alternative implementation exists in `eloq_log_service/` and is selected when `OPEN_LOG_SERVICE=OFF`; it is not documented here beyond its shared RPC interface. The single most important fact to internalize: **despite raft/braft vocabulary in comments and the proto, the open implementation is a single-replica, non-replicated log server** — each `LogServer` process serves exactly one log group, persists records into a local RocksDB with synchronous (`sync=true`) writes, and there is no leader election, no `braft::Node`, and no log replication. The braft-flavored RPCs (`TransferLeader`, `AddPeer`/`RemovePeer`, `GetLogGroupConfig`, …) are empty stubs or `assert(false)` in `open_log_service.h`.

Shared protocol files live in `log_service/tx-log-protos/` (`log.proto`, `log_agent.h/.cpp`); both log service implementations and the tx service compile against them.

## 1. Role and component map

- The tx service's commit path (`PostWriteLog` in `04-transaction-execution.md` / `07-durability-and-recovery.md`) calls `txservice::TxLog::WriteLog()` (`tx_service/include/txlog.h`), implemented by `EloqLogAgent` (`tx_service/include/eloq_log_wrapper.h`) over `txlog::LogAgent`.
- A transaction's log group is `tx_number % LogGroupCount()` (`eloq_log_wrapper.h:145-149`); `CcShard` even skews new tx numbers so that a tx lands on a wanted log group (`tx_service/src/cc/cc_shard.cpp:888`).
- Checkpointer pushes its checkpoint timestamp to all log groups (`LogAgent::UpdateCheckpointTs`), which allows the log service to truncate everything below it.

| File | What it is |
|---|---|
| `log_service/include/log_server.h`, `src/log_server.cpp` | `LogServer` — hosting object: brpc server + `OpenLogServiceImpl` + `LogStateRocksDBImpl` |
| `log_service/include/open_log_service.h`, `src/open_log_service.cpp` | `OpenLogServiceImpl` — the `LogService` RPC implementation |
| `log_service/include/open_log_task.h`, `src/open_log_task.cpp` | `OpenLogTaskWorker` — worker thread pool, request batching |
| `log_service/include/log_state.h` | `LogState` (abstract), `Item`, `ItemIterator`; in-memory schema/range-split op tracking |
| `log_service/include/log_state_rocksdb_impl.h`, `src/log_state_rocksdb_impl.cpp` | RocksDB-backed `LogState`, key encoding, SST purge thread |
| `log_service/include/log_shipping_agent.h` | `LogShippingAgent` — ships committed records to a tx node over a brpc stream |
| `log_service/include/closure.h` | `RaftLogClosure` — braft-era closure (unused by the open service path; kept for the proto-level contract) |
| `log_service/include/fault_inject.h`, `log_utils.h` | debug fault injection (`ACTION_FAULT_INJECTOR`, e.g. `"log_during_log_shipping"`); size parsing/gflag helpers |
| `log_service/tx-log-protos/log.proto` | `LogService` + `LogReplayService` definitions and all messages |
| `log_service/tx-log-protos/log_agent.h/.cpp` | `LogAgent` — the client used by the tx service |

## 2. Deployment modes

**In-process (default for eloqkv single node).** `DataSubstrate::InitializeLogService` (`core/src/log_init.cpp:118`) starts an embedded `LogServer` when WAL is enabled (`WITH_LOG_SERVICE`) and `txlog_service_list` is empty:

- Each distinct tx node in the ng configs gets a txlog node id (in enumeration order) and txlog port = **tx node port + 2** (`log_init.cpp:172,194-198`).
- Storage path: `local://` + (`log_service_data_path` or the core data path) + `/log_service` (`log_init.cpp:136-145`). The `LogServer` constructor strips the 8-char `local://` prefix and opens RocksDB at `<path>/log_service/rocksdb` (`log_server.cpp:75-78`).
- Open-service construction (`log_init.cpp:551-575`): `LogServer(node_id, port, log_path, /*scan_threads=*/1, sst_files_size_limit, max_write_buffer_number, max_background_jobs, target_file_size_base)`. The node id doubles as the **log group id** (`log_server.cpp:71` passes it to `OpenLogServiceImpl`).
- Relevant gflags/ini keys: `txlog_rocksdb_storage_path`, `txlog_rocksdb_sst_files_size_limit` (default `500MB`), `txlog_rocksdb_max_write_buffer_number`, `txlog_rocksdb_max_background_jobs`, `txlog_rocksdb_target_file_size_base`, `txlog_group_replica_num` (default 3 — but see §8: nominal for the open service).

**Standalone server.** `log_service/src/launch_sv.cpp` (built as the `launch_sv` executable by `log_service/CMakeLists.txt:192`) parses `--conf ip:port,...`, `--node_id`, `--storage_path` (it prepends `local://`) plus RocksDB tuning flags, then runs one `LogServer`. Tx nodes point at it via `txlog_service_list=ip:port,...`, in which case `log_init.cpp` starts no embedded server and only records the address list for `LogAgent`. Note: `launch_server.cpp` and `launch_cl.cpp` reference a `raft_server.h` that does not exist in this submodule — they are stale leftovers and are not built.

`LogServer::Start()` (`log_server.cpp:81`) registers `OpenLogServiceImpl` with the URL mapping `/healthz => CheckHealth`, starts `OpenLogServiceImpl::Start()` (opens RocksDB, spawns workers), and starts brpc with `num_threads ≈ NUM_VCPU+1`. It also sets `raft_enable_leader_lease` and `circuit_breaker_max_isolation_duration_ms=4500` gflags (`log_server.cpp:122-132`) — braft-era settings, harmless here. The core process additionally sets `use_io_uring` and `raft_use_bthread_fsync` gflags from `enable_io_uring`/`raft_log_async_fsync` config (`core/src/data_substrate.cpp:895-915`); these affect the brpc fork's I/O engine and braft fsync behavior (only meaningful for the closed-source raft-based implementation; the open service fsyncs through RocksDB).

## 3. RPC surface

`service LogService` (`log.proto:453-470`); statuses come from `LogResponse::ResponseStatus` (`Success/Fail/NotLeader/Unknown/...`).

| RPC | Caller | Open-service behavior |
|---|---|---|
| `WriteLog(LogRequest)` | tx commit path (`WriteLogOp` via `LogAgent::WriteLog`) | Validates `log_group_id` matches this server's group, enqueues to a worker, waits, returns `Success`/`Fail`/`Unknown` (`open_log_service.cpp:31-53`) |
| `ReplayLog(LogRequest)` | recovering cc ng leader (`LogAgent::ReplayLog`, sync, retried every 2s until `Success`) | Computes replay start ts, spawns a `LogShippingAgent` that streams records back; replaces/terminates any previous agent (`open_log_service.cpp:55-125`) |
| `UpdateCheckpointTs(LogRequest)` | checkpointer (`LogAgent::UpdateCheckpointTs`, fire-and-forget to every group, 100ms timeout) | Persists ckpt ts + max txn no, enables truncation (`open_log_task.cpp:283-289`) |
| `RecoverTx(RecoverTxRequest)` | a tx that hits an orphan lock (`LogAgent::RecoverTx`) | Answers `Committed`/`NotCommitted`; ships the found data-log record asynchronously for lock release (`open_log_service.cpp:137-209`) |
| `RemoveCcNodeGroup(LogRequest)` | cluster scale-in (`LogAgent::RemoveCcNodeGroup`) | **Empty stub** |
| `CheckMigrationIsFinished`, `CheckClusterScaleStatus` | cluster-scale / bucket-migration coordinators (see `06-distribution-and-clustering.md`) | **Empty stubs** (the open service does not persist `ClusterScaleOpMessage`/`DataMigrateTxLogMessage`) |
| `TransferLeader(TransferRequest)` | tests | **Empty stub** |
| `AddPeer` / `RemovePeer` | operator tooling | `assert(false)` — not implemented |
| `GetLogGroupConfig` | `LogAgent::RefreshLeader` (closed impl only) | `assert(false)` — not implemented |
| `AttachLog` / `DetachLog` | cloud serverless attach/detach (closed impl) | **Empty stubs** |
| `CheckHealth` (`GET /healthz`) | health probes | Hard-codes one raft_stat `{log_group:"0", state:"LEADER"}` as JSON (`open_log_service.h:95-125`) |

`service LogReplayService` (`log.proto:536-540`) is implemented **on the tx node** by `RecoveryService` (`tx_service/include/fault/log_replay_service.h:108`):

| RPC | Caller | Purpose |
|---|---|---|
| `Connect(LogReplayConnectRequest)` | `LogShippingAgent::ConnectStream` | Accepts the brpc stream over which `ReplayMessage`s flow |
| `UpdateLogGroupLeader` | log server (closed impl) | Updates the tx node's `LogAgent` leader cache via `Sharder::UpdateLogGroupLeader` (`sharder.h:402`); never called by the open service |
| `NotifyCheckpointer` | log server (closed impl, when replay-log size exceeds `notify_checkpointer_threshold_size`) | Kicks the checkpointer; never called by the open service (no caller of `GetApproximateReplayLogSize` in `log_service/`) |

## 4. What is stored per log record

A `WriteLogRequest` (`log.proto:247-264`) carries: `txn_number`, `commit_timestamp`, `log_content` (oneof: `data_log` / `schema_log` / `split_range_log` / `cluster_scale_log` / `migration_log`), per-ng terms (`node_terms`, unused by the open service), `tx_term`, `retry`, `log_group_id`. `DataLogMessage.node_txn_logs` is a map `cc_ng_id → opaque blob` — the blob is the tx's serialized writes for that ng (decoded only by the tx-side replay, not by the log service).

`LogStateRocksDBImpl` uses two column families (`log_state_rocksdb_impl.cpp:253-255`):

- **default CF** — data log records. Key = `bswap64(commit_ts) ‖ bswap64(tx_number)` (16 bytes, big-endian, so lexicographic order = (ts, txn) order; `log_state_rocksdb_impl.h:45-58`). Value = the per-ng blob. A comment at `log_state_rocksdb_impl.cpp:230-234` notes keys were deliberately changed from ng-prefixed to timestamp-prefixed so that no compaction is needed (`disable_auto_compactions=true`, `num_levels=1`, `atomic_flush=true`).
- **meta CF** (`meta_cf`) — multi-stage op state and durable counters. Key = `bswap64(txn) ‖ bswap64(commit_ts) ‖ code` (17 bytes) where `code ∈ LogState::MetaOp {SchemaOp, RangeOp, LastCkpt, MaxTxn}` (`log_state.h:107-113`); `LastCkpt`/`MaxTxn` use the fixed key `(UINT64_MAX, UINT64_MAX, code)`.

In memory, `LogState` (`log_state.h`) additionally keeps:

- `tx_catalog_ops_` (`txn → CatalogOp`) — ongoing **schema (DDL) ops** with their `SchemaOpMessage_Stage` (`PrepareSchema → PrepareData → CommitSchema → CleanSchema`). `UpsertSchemaOp` enforces stage monotonicity, dedups retried prepare logs, and persists every stage change via `PersistSchemaOp` (`log_state.h:218-387`).
- `tx_split_range_ops_` (`txn → SplitRangeOp`) — ongoing **range-split ops** (`PrepareSplit → CommitSplit → CleanSplit`), same pattern (`log_state.h:454-553`).
- `cc_ng_info_` — a *single* `CcNgInfo` holding `latest_txn_no_` (highest committed tx ident, wrap-around tolerant; used so a recovering leader does not reuse tx numbers) and `last_ckpt_ts_` (`log_state.h:156-192`).
- `log_state_mutex_` (shared_mutex) protects the op maps: `RecoverTx` RPC threads read while write workers mutate.

Both op maps are rebuilt from the meta CF on `Start()` (`log_state_rocksdb_impl.cpp:310-506`).

## 5. Write path

```
tx commit (WriteLogOp)
  → LogAgent::WriteLog(lg_id = txn % group_cnt)         async, brpc channel to cached node
  → OpenLogServiceImpl::WriteLog                         rejects mismatched log_group_id
  → SubmitAndWait: round-robin enqueue to one of 4 OpenLogTaskWorkers
       (caller bthread waits on per-task bthread::Mutex/cv — single waiter, structurally safe)
  → OpenLogTaskWorker::WorkerThreadMain (4 pthreads/worker)
       try_dequeue_bulk up to 256 tasks
       pass 1: collect all kDataLog tasks' node_txn_logs into one batch
       AddLogItemBatch → single rocksdb::WriteBatch → db_->Write(sync=true)   ← group commit/fsync
       pass 2: non-data tasks → HandleWriteLog (schema op / split-range op upserts, also sync writes)
       notify each task's cv
```

Key facts (all in `open_log_task.cpp` / `log_state_rocksdb_impl.cpp`):

- **Durability**: `write_option_.sync = true; disableWAL = false` (`log_state_rocksdb_impl.cpp:508-509`). Every acknowledged `WriteLog` is fsynced through the RocksDB WAL. Batching across concurrently arriving requests amortizes the fsync.
- `UpdateLatestCommittedTxnNumber(txn & 0xFFFFFFFF)` runs after every write.
- On error, `SetWriteLogErrorResponse` returns `Unknown` for `retry=true` requests (client should re-resolve and retry) and `Fail` otherwise (`open_log_service.cpp:211-230`).
- **Verified gap**: pass 1 batches *every* `kDataLog` task, and batched tasks never reach `HandleWriteLog`; therefore `DataLogMessage.schema_logs` (logical DDL embedded in a DML log, e.g. EloqDoc index-type changes) is **not** persisted via `UpsertSchemaOpWithinDML` on this path — that call is effectively dead code in `open_log_task.cpp:231-235`. Be careful if you touch the batching logic.

## 6. Checkpoint timestamp and truncation

`UpdateCheckpointTs` → `LogState::UpdateCkptTs` (`log_state.h:582-601`): if the new ts is higher, persist `(LastCkpt, MaxTxn)` to the meta CF (retry loop until success), advance `last_ckpt_ts_`, then `TryCleanMultiStageOps()` erases schema/range ops that reached their clean stage more than **one hour** (commit-ts time) before the ckpt ts — kept around that long to absorb stale retried prepare logs (`log_state.h:678-739`).

Physical truncation of data logs is done by the **SST purge thread** (`LogStateRocksDBImpl::PurgingSstFiles`, `log_state_rocksdb_impl.cpp:634-801`): RocksDB flush events feed a queue (`RocksDBEventListener::OnFlushCompleted`); when accumulated SST size exceeds `sst_files_size_limit` (default 500MB) and the ckpt ts has advanced, every level-0 SST whose entire key range lies below `last_ckpt_ts - 1` is dropped via `db_->DeleteFile()`. Because keys are ts-prefixed and compaction is disabled, whole files age out cleanly. If no checkpoint ever happened (`ckpt_ts == 0`), nothing is purged.

## 7. Replay path (failover recovery)

```
new cc ng leader (tx side, RecoveryService / Sharder failover)
  → LogAgent::ReplayLog(ng_id, term, my_ip, my_port, ...)   sync; loops until Success
  → OpenLogServiceImpl::ReplayLog
       start_ts = last_ckpt_ts==0 ? 0 : last_ckpt_ts+1   (min with req.start_ts if set)
       LogState::GetLogReplayList(start_ts) → ItemIteratorRocksDBImpl
       terminate + replace previous LogShippingAgent (bthread_usleep poll)
  → LogShippingAgent thread:
       brpc StreamCreate + LogReplayService::Connect(my_ip:my_port)
       ship all DDL items first (schema ops not yet CleanSchema, split-range ops not CleanSplit)
       then ship data logs: scan_threads partitions of the [first_ts,last_ts] range in parallel,
         packed into ReplayMessage.binary_log_records blobs of ≤32KB
         (blob entry = commit_ts(8) ‖ [tx_no(8) only in lock-recovery batches] ‖ len(4) ‖ blob)
       finally ReplayFinishMsg{log_group_id, latest_txn_no, last_ckpt_ts, latest_commit_ts}
  → tx-side RecoveryService::on_received_messages converts each record into ReplayLogCc
       requests enqueued to the cc shards (see 07-durability-and-recovery.md)
```

Details verified in `log_shipping_agent.h`:

- The agent is a dedicated `std::thread`; the tx-node address comes from the request (`source_ip`/`source_port`), and hostnames are resolved through braft naming-service URLs.
- During initial replay the agent does **not** reconnect a broken stream — the tx side times out and re-sends `ReplayLogRequest`, which replaces the agent (`SendMessage` comment, `log_shipping_agent.h:604-616`). After the finish message, when shipping recovered-tx records, it *does* reconnect, because those are rare and asynchronous.
- The open service hard-codes `cc_node_group_term = DEFAULT_CC_NG_TERM = 1` in all replay messages (`log_shipping_agent.h:43`, `open_log_service.cpp:114`); it does not track cc ng terms.
- One `log_replay_worker_` per `OpenLogServiceImpl` — at most one shipping agent at a time, replaced wholesale on each `ReplayLog`.

**RecoverTx (orphan lock resolution).** When a tx hits a lock owned by a dead coordinator, `LockRecoveryWorker` calls `TxLog::RecoverTx` → `LogAgent::RecoverTx` → the lock-owner's log group. The open service (`open_log_service.cpp:137-209`) scans the data CF from `write_lock_ts - 1` (or `last_ckpt_ts + 1`) for a record whose key suffix equals the tx number (`SearchTxDataLog`, `log_state_rocksdb_impl.cpp:118-182`), and checks the in-memory schema/range-split maps. Outcome: data log found → `Committed`, and the record is handed to the shipping agent so replaying it clears the lock; multi-stage op found → `Committed` (a logged DDL/split is guaranteed to finish and release its own locks); nothing found → `NotCommitted` (the tx can never commit because its terms are stale — the caller may safely kill the lock).

## 8. LogAgent (client, `tx-log-protos/log_agent.cpp`)

- `Init(ips, ports, start_log_group_id, replica_num)` derives the group layout with `LogUtil::GenerateLogRaftGroupConfig` (`log_util.h:69`): replica_num is clamped to the node count, nodes are chunked into groups of `replica_num` (group ids increment from `start_log_group_id`), one brpc channel per node (10s timeout, 3 retries), and a per-group leader cache `lg_leader_cache_` initialized to node 0.
- Every request resolves its channel via `GetChannel(lg_id)` → cached leader node id. Under `OPEN_LOG_SERVICE`, `RefreshLeader`/`RequestRefreshLeader` are **no-ops** (`log_agent.cpp:153-157,342-346`) and the background `check_leader_thd_` is not started — there are no leaders to discover. The non-open build refreshes leaders through `braft::rtb` every 5s and on demand, and re-pulls group membership via `GetLogGroupConfig`.
- `WriteLog` is asynchronous (caller's closure); `ReplayLog` is synchronous with an infinite 2s-backoff retry loop honoring an `interrupt` flag; `UpdateCheckpointTs`/`RemoveCcNodeGroup` are best-effort broadcasts to all groups; `RecoverTx` is a single synchronous call returning `RecoverError` on RPC failure (next conflicting tx retries).
- The tx service consumes it through `EloqLogAgent : TxLog` (`eloq_log_wrapper.h`); `Sharder::UpdateLogGroupLeader` exposes `UpdateLeaderCache` to the `LogReplayService::UpdateLogGroupLeader` RPC.
- Comment in `log_agent.h:54-56` claims the agent is cloned per thread due to the cache, but the cache is `std::atomic` per group and stubs are created on the fly per call.

## 9. Cloud log-state variants (`WITH_LOG_STATE=ROCKSDB_CLOUD_S3/GCS`)

The cloud variants are **only available with the closed-source implementation**: `build_log_service.cmake:123` defines just `LOG_STATE_TYPE_RKDB` for the open service, while `LOG_STATE_TYPE_RKDB_ALL/_S3/_GCS` come from `build_eloq_log_service.cmake:23-28`. What is visible in open code:

- `core/src/log_init.cpp:299-538` assembles a `txlog::RocksDBCloudConfig` (bucket/prefix/object path or `txlog_rocksdb_cloud_object_store_service_url`, region, SST cache size, file-deletion delay, `txlog_rocksdb_cloud_log_retention_days` + daily purger schedule, archive object path + move interval, AWS keys for S3) and passes it to a richer `LogServer` constructor that exists only in `eloq_log_service/`. `WITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_*` force-matches `WITH_LOG_STATE` at configure time (`CMakeLists.txt:62-96`).
- `DataSubstrate::Shutdown` (`core/src/data_substrate.cpp:261-299`): in bootstrap mode it removes the log storage path after shutdown (and, for the non-open RocksDB log states, also the separate rocksdb storage path via `GetRocksDBStoragePath()` — that accessor doesn't exist on the open `LogServer`, the path is guarded by `!defined(OPEN_LOG_SERVICE)`).

## 10. Gotchas and invariants

1. **No replication in the open service.** `txlog_group_replica_num` is parsed and `LogAgent` groups nodes by it, but each open `LogServer` is an independent single-copy store; losing its disk loses un-checkpointed commits. The `NotLeader`/redirect machinery in the proto is exercised only by the closed implementation.
2. **log_group_id == LogServer node_id** (`log_server.cpp:71`), and a `WriteLog` whose `log_group_id` doesn't match is rejected (`open_log_service.cpp:42-50`). The tx side guarantees routing by `txn % LogGroupCount()` and by skewing new tx numbers per shard.
3. **Ordering**: data records sort by `(commit_ts, tx_number)` via big-endian keys; replay streams DDL ops first, then data logs — *in parallel partitions*, so cross-partition arrival order is not ts-ordered; the tx-side replay applies records idempotently by commit ts.
4. **Fsync is per group-commit batch** (`sync=true` WriteBatch). There is no separate raft-log fsync in the open service; `raft_use_bthread_fsync`/`use_io_uring` matter only for the closed one.
5. **Failover mid-write**: with a single replica there is no log-leader failover; if the server restarts, the client's pending RPC fails/times out and the commit-path retry sends `retry=true`, for which any subsequent error reports `Unknown` rather than `Fail` (so the tx is not wrongly aborted — its outcome is resolved later via `RecoverTx`).
6. **Truncation is file-granular and lazy** — only whole L0 SSTs strictly below `ckpt_ts - 1` are deleted, and only when total SST size exceeds `txlog_rocksdb_sst_files_size_limit`; memtable contents are never proactively flushed for truncation.
7. **Stage logs linger one hour past checkpoint** (clean-stage schema/split ops) to absorb stale retried prepare logs; `RecoverTx` treats *any* surviving multi-stage log as `Committed`.
8. **Single shipping agent**: a new `ReplayLog` (e.g. quick double failover) terminates and replaces the running agent; termination is polled with `bthread_usleep` (`open_log_service.cpp:95-102`), consistent with the bthread-deadlock rules in `02-threading-model.md`.
9. **Dead/empty surface**: cluster-scale and migration logs are accepted by the proto but ignored (`default:` in `HandleWriteLog`, empty `CheckClusterScaleStatus`/`CheckMigrationIsFinished`), so cluster scaling flows in `06-distribution-and-clustering.md` require the closed implementation. Likewise `DataLogMessage.schema_logs` is dropped on the batched write path (§5).
10. The per-task wait in `SubmitAndWait` uses a `bthread::Mutex`+cv with exactly one waiter — the safe single-flight pattern; do not extend it to multi-waiter use.

## 11. Test utilities (`log_service/test/`)

`replay_service_for_test.h` (also installed under `include/`) is a minimal `LogReplayService` that accepts the stream and parses `ReplayMessage`s — the standalone counterpart of the tx node's `RecoveryService`; `launch_replay_service.cpp` runs it as a server. `write_log_test.cpp` / `async_write_log_test.cpp` drive `WriteLog` load, `recover_time_test.cpp` measures replay, `rocksdb_test.cpp` exercises the log state; `test_utils.h` has request builders. Only `launch_sv`, `launch_replay_service`, `async_write_log_test`, and `rocksdb_test` are wired into `log_service/CMakeLists.txt`.

## Cross-references

- `01-architecture-overview.md` — where the log service sits in process bring-up (`core/src/data_substrate.cpp` calls `InitializeLogService` before the store handler).
- `06-distribution-and-clustering.md` — ng leadership/failover that triggers `ReplayLog`, cluster-scale flows that need the closed log service.
- `07-durability-and-recovery.md` — the tx-side half: `WriteLogOp`, checkpointer → `UpdateCheckpointTs`, `RecoveryService`/`ReplayLogCc`, orphan-lock recovery via `RecoverTx`.
