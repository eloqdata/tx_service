# 10 — Log Service (`eloq_log_service/`)

The log service is the write-ahead log of the tx service: at commit time a transaction ships its redo records to a *log group*, and a recovering cc node group leader streams those records back to rebuild un-checkpointed state (see `07-durability-and-recovery.md`). This document covers the in-tree implementation in `eloq_log_service/`, which is the canonical log service. There is no longer an open-source alternative or an `OPEN_LOG_SERVICE` build flag; the build always uses `build_eloq_log_service.cmake`, which produces the `logservice` CMake target (compiled when `WITH_LOG_SERVICE` is set). The most important structural difference from a simple WAL: **each log group is a real braft Raft cluster** — typically `txlog_group_replica_num` replicas per group — providing genuine HA, leader election, and log replication. Only the raft leader accepts writes; followers receive replicated entries via braft's normal path.

Protocol files live at `tx_service/tx-log-protos/` (`log.proto`, `log_agent.h/.cpp`, `log_util.h`); both the log service and the tx service compile against them.

## 1. Role and component map

- The tx service's commit path (`PostWriteLog` in `04-transaction-execution.md` / `07-durability-and-recovery.md`) calls `txservice::TxLog::WriteLog()` (`tx_service/include/txlog.h`), implemented by `EloqLogAgent` (`tx_service/include/eloq_log_wrapper.h`) over `txlog::LogAgent`.
- A transaction's log group is `tx_number % LogGroupCount()` (`eloq_log_wrapper.h`); `CcShard` skews new tx numbers so that a tx lands on a wanted log group (`tx_service/src/cc/cc_shard.cpp`).
- The checkpointer pushes its checkpoint timestamp to all log groups (`LogAgent::UpdateCheckpointTs`), allowing the log service to truncate data below it.
- The log service acts as a **term/leader registry**: before a new cc node group leader can serve requests, it registers its Raft term with the log service via the `ReplayLog` RPC. Subsequent `WriteLog` requests carry per-ng terms, and the log service rejects logs whose term is stale (the transaction's locks are no longer valid).

| File | What it is |
|---|---|
| `eloq_log_service/include/log_server.h`, `src/log_server.cpp` | `LogServer` — hosting object: brpc server + `LogServiceImpl` + per-group `LogInstance`s |
| `eloq_log_service/include/log_service.h` | `LogServiceImpl` — the `LogService` brpc RPC implementation; routes each request to the matching `LogInstance` by log group id; also handles `AttachLog`/`DetachLog` for serverless attach/detach |
| `eloq_log_service/include/log_instance.h`, `src/log_instance.cpp` | `LogInstance : braft::StateMachine` — one per log group replica on this node; owns the braft `Node`, drives `on_apply`, batches write-log tasks, manages per-cc-ng shipping agents |
| `eloq_log_service/include/log_state.h` | `LogState` (abstract), `Item`, `ItemIterator`; in-memory per-ng state: `cc_ng_info_` map (term, leader, latest txn no, last ckpt ts), schema/split-range/cluster-scale op maps, snapshot I/O helpers |
| `eloq_log_service/include/log_state_rocksdb_impl.h`, `src/log_state_rocksdb_impl.cpp` | RocksDB-backed `LogState`: key encoding `bswap64(ts) ‖ bswap32(ng_id) ‖ bswap64(txn)` (20 bytes), parallel-partition iterator for replay, SST purge thread |
| `eloq_log_service/include/log_state_memory_impl.h`, `src/log_state_memory_impl.cpp` | In-memory `LogState` (compiled with `WITH_LOG_STATE=MEMORY`; used in tests) |
| `eloq_log_service/src/log_state_rocksdb_cloud_impl.cpp` | RocksDB-Cloud-backed `LogState` for S3/GCS (`WITH_LOG_STATE=ROCKSDB_CLOUD_S3/GCS`); adds async CloudDB open on leader start and in-memory queue with high-watermark snapshot trigger |
| `eloq_log_service/include/log_shipping_agent.h` | `LogShippingAgent` — dedicated `std::thread`; connects a brpc stream to the tx-side `LogReplayService` and ships DDL items then data log records |
| `eloq_log_service/include/fault_inject.h`, `src/fault_inject.cpp` | `FaultInject` singleton + `ACTION_FAULT_INJECTOR` macro (compiled in when `WITH_FAULT_INJECT`); named injection points, e.g. `"log_during_log_shipping"` |
| `eloq_log_service/include/log_utils.h` | Size-string parsing (`parse_size`), `LOG_STATE_TYPE_RKDB_ALL`/`_CLOUD` compile-time macros, string utilities |
| `eloq_log_service/include/rocksdb_cloud_config.h` | `RocksDBCloudConfig` struct (bucket, prefix, region, SST-cache size, retention, archive path, purger schedule) |
| `tx_service/tx-log-protos/log.proto` | `LogService` + `LogReplayService` proto definitions and all request/response messages |
| `tx_service/tx-log-protos/log_agent.h/.cpp` | `LogAgent` — the client used by the tx service; maintains per-group leader caches and a background leader-refresh thread |
| `tx_service/tx-log-protos/log_util.h` | `LogUtil::GenerateLogRaftGroupConfig` — chunks the node list into replica groups, assigns group ids |

## 2. Deployment modes

**In-process (default for eloqkv single node).** `DataSubstrate::InitializeLogService` (`core/src/log_init.cpp`) starts an embedded `LogServer` when WAL is enabled (`WITH_LOG_SERVICE`) and `txlog_service_list` is empty:

- Each distinct tx node in the ng configs is assigned a txlog node id (in enumeration order) and txlog port = **tx node port + 2** (`log_init.cpp`).
- Storage paths: raft/snapshot state → `local://` + (`log_service_data_path` or the core data path) + `/log_service`; RocksDB log-state → same base path + `/log_service/rocksdb` (or `txlog_rocksdb_storage_path`).
- The `LogServer` constructor calls `LogServiceImpl::AddLogReplicas`, which calls `LogUtil::GenerateLogRaftGroupConfig` to partition all nodes into groups of `log_group_replica_num`, registers each group in braft's routing table, and creates one `LogInstance` per group replica local to this node.
- Relevant gflags/ini keys: `txlog_rocksdb_storage_path`, `txlog_rocksdb_sst_files_size_limit` (default `500MB`), `txlog_rocksdb_max_write_buffer_number` (default 8), `txlog_rocksdb_max_background_jobs` (default 12), `txlog_rocksdb_target_file_size_base`, `txlog_group_replica_num` (default 3), `logserver_snapshot_interval` (default 600 s), `enable_txlog_request_checkpoint`, `check_replay_log_size_interval_sec`, `notify_checkpointer_threshold_size` (default 128 MB).

**Standalone server.** `eloq_log_service/src/launch_sv.cpp` can be built as a separate `launch_sv` executable. Tx nodes point at it via `txlog_service_list=ip:port,...`; `log_init.cpp` then skips creating an embedded server and only records the address list for `LogAgent`. (`launch_server.cpp` and `launch_cl.cpp` in `src/` reference a stale raft-server header and are not normally built.)

`LogServer::Start()` (`log_server.cpp`) registers `LogServiceImpl` with the brpc server, starts each `LogInstance` (which opens `LogState` then calls `braft::Node::init`), and starts brpc. The core process also sets braft and brpc gflags (`raft_enable_leader_lease`, `use_io_uring`, `raft_use_bthread_fsync`) before calling `InitializeLogService`.

## 3. LogInstance and the braft state machine

`LogInstance` implements `braft::StateMachine`. One instance exists per log-group replica hosted on this node. Key lifecycle:

- `on_leader_start(term)`: stores `term` in `term_if_is_lg_leader_`, starts the RocksDB Cloud DB asynchronously if cloud-backed, calls `LogGroupLeaderUpdate` to notify the tx-side `LogAgent` of the new leader, and optionally starts the replay-log-size checker thread.
- `on_leader_stop`: clears `term_if_is_lg_leader_` (set to -1), terminates active shipping agents.
- `on_apply(iter)`: dispatches each committed raft entry to the appropriate handler — `WriteLog` tasks are batch-applied (`BatchAddLogItems`), DDL/split-range/cluster-scale/replay/checkpoint entries are applied individually.
- `on_snapshot_save`/`on_snapshot_load`: snapshots the in-memory portion of `LogState` (ng-info map, schema/split-range/cluster-scale op maps) to binary files via `WriteSnapshot`/`ReadSnapshot`; the RocksDB data log is not included in the snapshot (it persists independently).

Writes go through braft: `WriteLog` → serialize request to IOBuf → bind to `braft::Task` → `node_->apply(task)` → on majority-commit → `on_apply`. This gives true replication durability for `replica_num >= 3`.

## 4. RPC surface

`service LogService` (`log.proto`); statuses come from `LogResponse::ResponseStatus` (`Success/Fail/NotLeader/Unknown/...`).

| RPC | Caller | Behavior |
|---|---|---|
| `WriteLog(LogRequest)` | tx commit path | `LogServiceImpl` routes by `log_group_id`; `LogInstance::WriteLog` checks leadership (returns `NotLeader` if not leader), proposes via braft, waits for apply; returns `Success`/`Fail`/`NotLeader`/`Unknown` |
| `ReplayLog(LogRequest)` | recovering cc ng leader (`LogAgent::ReplayLog`, sync, retried every 2s until `Success`) | Updates ng term + leader info in `LogState` via braft, then spawns/replaces a `LogShippingAgent` for this ng |
| `UpdateCheckpointTs(LogRequest)` | checkpointer (`LogAgent::UpdateCheckpointTs`, fire-and-forget to every group) | Proposed via braft; on apply, `LogState::UpdateCkptTs` updates `last_ckpt_ts_` for the ng, recomputes `min_ckpt_ts_`, triggers `TryCleanMultiStageOps` |
| `RecoverTx(RecoverTxRequest)` | tx hitting an orphan lock | Searches `LogState` for the tx's data/DDL/split-range/cluster-scale log and the ng's current term; answers `Committed`/`NotCommitted`; ships data log asynchronously if committed |
| `RemoveCcNodeGroup(LogRequest)` | cluster scale-in | Proposed via braft; on apply, erases ng info from `LogState` |
| `CheckClusterScaleStatus` | cluster-scale coordinator | Routes to `LogInstance::CheckClusterScaleStatus`; inspects in-memory `tx_cluster_scale_ops_` |
| `CheckMigrationIsFinished` | bucket-migration coordinator | Routes to `LogInstance::CheckMigrationIsFinished` |
| `TransferLeader(TransferRequest)` | operator tooling / tests | Routes to `LogInstance::TransferLeader`; calls braft leadership transfer |
| `AddPeer` / `RemovePeer` | operator tooling | Routes to `LogInstance::ChangePeersToAdd`/`ChangePeersToRemove`; drives braft configuration change |
| `GetLogGroupConfig` | `LogAgent::RefreshLeader` | Returns current peer list from `LogInstance::peer_vct_` |
| `AttachLog` / `DetachLog` | cloud serverless attach/detach | `LogServiceImpl` handles; `AttachLog` reads a config file, calls `AddLogReplicas`, starts braft nodes; `DetachLog` shuts them down |
| `CheckHealth` (`GET /healthz`) | health probes | Queries braft `NodeManager` for all nodes; returns per-group raft state (LEADER/FOLLOWER/CANDIDATE) and service attach status as protobuf/JSON |

`service LogReplayService` (`log.proto`) is implemented **on the tx node** by `RecoveryService` (`tx_service/include/fault/log_replay_service.h`):

| RPC | Caller | Purpose |
|---|---|---|
| `Connect(LogReplayConnectRequest)` | `LogShippingAgent::ConnectStream` | Accepts the brpc stream over which `ReplayMessage`s flow |
| `UpdateLogGroupLeader` | log server (on leader start) | Updates the tx node's `LogAgent` leader cache via `Sharder::UpdateLogGroupLeader` |
| `NotifyCheckpointer` | log server (when replay-log size exceeds `notify_checkpointer_threshold_size`) | Kicks the checkpointer |

## 5. What is stored per log record

A `WriteLogRequest` (`log.proto`) carries: `txn_number`, `commit_timestamp`, `log_content` (oneof: `data_log` / `schema_log` / `split_range_log` / `cluster_scale_log` / `migration_log`), per-ng terms (`node_terms`), `tx_term`, `retry`, `log_group_id`. `DataLogMessage.node_txn_logs` is a map `cc_ng_id → opaque blob` — the blob is the tx's serialized writes for that ng.

`LogStateRocksDBImpl` stores data logs in a single RocksDB column family:

- **Key** = `bswap64(commit_ts) ‖ bswap32(ng_id) ‖ bswap64(tx_number)` (20 bytes, big-endian, so lexicographic order = `(ts, ng, txn)` order). Value = the per-ng blob.
- Data logs from all cc node groups are stored together in this single CF, filtered by `ng_id` at replay time.
- Auto-compaction is disabled (`disable_auto_compactions=true`, `num_levels=1`, `atomic_flush=true`); files age out via the SST purge thread (see §7).

In memory, `LogState` additionally keeps:

- `cc_ng_info_` (`ng_id → CcNgInfo`) — per-ng: raft term, leader ip/port, `latest_txn_no_` (wrap-around-tolerant highest committed tx ident), `latest_commit_ts_`, `last_ckpt_ts_`. Protected by `log_state_mutex_` (shared_mutex) for concurrent `RecoverTx` reads vs `on_apply` writes.
- `tx_catalog_ops_` (`txn → CatalogOp`) — ongoing **schema (DDL) ops** with their `SchemaOpMessage_Stage` (`PrepareSchema → PrepareData → CommitSchema → CleanSchema`). Stage transitions are monotone; cleaned entries linger one hour past `min_ckpt_ts_` to absorb stale retried logs.
- `tx_split_range_ops_` (`txn → SplitRangeOp`) — ongoing **range-split ops** (`PrepareSplit → CommitSplit → CleanSplit`), same pattern.
- `tx_cluster_scale_ops_` (`txn → ClusterScaleOp`) — ongoing **cluster-scale ops** (`PrepareScale → ConfigUpdate/… → CleanScale`).
- `tx_data_migration_ops_` — set of in-flight data migration tx numbers (populated from cluster-scale op tracking).
- `min_ckpt_ts_` — minimum of `last_ckpt_ts_` across all known ngs; updated on each `UpdateCkptTs`; triggers `TryCleanMultiStageOps`.

The in-memory op maps are rebuilt from the braft snapshot on `on_snapshot_load`. The snapshot does **not** include RocksDB data — that persists across restarts independently.

## 6. Write path

```
tx commit (WriteLogOp)
  → LogAgent::WriteLog(lg_id = txn % group_cnt)         async, brpc channel to cached leader
  → LogServiceImpl::WriteLog                             routes by log_group_id
  → LogInstance::WriteLog                                rejects if not leader (NotLeader)
       serialize request → braft::Task → node_->apply()
  → braft replication to majority of replicas
  → on_apply (leader + followers):
       kDataLog: BatchAddLogItems → single rocksdb::WriteBatch → db_->Write(sync=true)
       kSchemaLog / kSplitRangeLog / kClusterScaleLog / kMigrationLog: individual handlers
       kReplayLog: UpdateNgTerm + spawn LogShippingAgent
       kUpdateCkptTs: UpdateCkptTs + TryCleanMultiStageOps
  → braft closure: notify original WriteLog caller (Success/Fail)
```

Key facts:

- **Durability**: `write_option_.sync = true; disableWAL = false`. Every acknowledged `WriteLog` is fsynced through the RocksDB WAL on the log-state side; braft additionally fsyncs its own raft log to a majority of replicas.
- `UpdateLatestCommittedTxnNumber` and `UpdateLatestCommitTs` run after every data-log write.
- On error, `SetWriteLogErrorResponse` returns `Unknown` for `retry=true` requests (client should re-resolve and retry) and `Fail` otherwise.
- `BatchAddLogItems` batches up to 256 concurrent data-log tasks into one `rocksdb::WriteBatch` to amortize the fsync cost.

## 7. Checkpoint timestamp and truncation

`UpdateCkptTs(ng, ts)` in `LogState`: if the new ts advances the ng's `last_ckpt_ts_`, recomputes `min_ckpt_ts_` across all ngs, then calls `TryCleanMultiStageOps`: erases schema/split-range/cluster-scale op entries that have reached their clean stage and whose commit ts is more than **one hour** below `min_ckpt_ts_` (to absorb stale retried prepare logs; `log_state.h`).

Physical truncation of data logs is done by the **SST purge thread** (`LogStateRocksDBImpl`): RocksDB flush events feed a queue; when accumulated SST size exceeds `sst_files_size_limit` (default 500 MB) and the checkpoint ts has advanced, every L0 SST whose entire key range lies below `min_ckpt_ts - 1` is dropped via `db_->DeleteFile()`. Because keys are ts-prefixed and compaction is disabled, whole files age out cleanly. If no checkpoint has ever advanced (`min_ckpt_ts == 0`), nothing is purged.

For the RocksDB Cloud variants (`LOG_STATE_TYPE_RKDB_CLOUD`), truncation is handled differently: the in-memory data-log queue triggers a braft snapshot when it reaches `in_mem_data_log_queue_size_high_watermark` (default 500 000 entries); the cloud backend handles object lifecycle via `log_retention_days` and a daily log purger (`log_purger_schedule`).

## 8. Replay path (failover recovery)

```
new cc ng leader (tx side, RecoveryService / Sharder failover)
  → LogAgent::ReplayLog(ng_id, term, my_ip, my_port, ...)   sync; loops until Success
  → LogServiceImpl::ReplayLog → LogInstance::ReplayLog
       proposes UpdateNgTerm via braft
       on apply: UpdateNgTerm → cc_ng_info_ updated
       then: get iterator (LogState::GetLogReplayList(ng_id, start_ts))
             terminate + replace previous agent for this ng
  → LogShippingAgent thread:
       ConnectStream → LogReplayService::Connect(my_ip:my_port)
       ship all DDL items first (schema ops not CleanSchema, split-range not CleanSplit,
         cluster-scale not CleanScale)
       then ship data logs: scan_threads partitions of the key range in parallel,
         each packed into ReplayMessage blobs (entry = ts ‖ ng ‖ txn ‖ len ‖ blob)
       finally ReplayFinishMsg{log_group_id, latest_txn_no, last_ckpt_ts, latest_commit_ts}
  → tx-side RecoveryService::on_received_messages converts each record into ReplayLogCc
       requests enqueued to cc shards (see 07-durability-and-recovery.md)
```

Details:

- `start_ts` = `last_ckpt_ts + 1` for the ng (0 if never checkpointed), honoring `req.start_ts` if set.
- The agent is a dedicated `std::thread`; termination on replacement is polled with `std::this_thread::sleep_for(1s)` (agent thread) and checked via an atomic `shipping_agent_status_`.
- Multiple simultaneous ngs can each have their own shipping agent; `LogInstance` keeps a `std::unordered_map<uint32_t, std::unique_ptr<LogShippingAgent>> log_replay_workers_` keyed by ng id.
- The cc ng term carried in replay messages is the actual braft term from `cc_ng_info_`, not a hard-coded constant.

**RecoverTx (orphan lock resolution).** When a tx hits a lock owned by a dead coordinator, `LogAgent::RecoverTx` queries the lock-owner's log group. `LogInstance::RecoverTx` checks (a) `LogState::SearchTxDataLog` (scans the RocksDB CF from near the write_lock_ts for a record matching the tx number and ng), (b) `SearchTxSchemaLog`, (c) `SearchTxSplitRangeOp`, (d) `SearchTxClusterScaleOp`, (e) `SearchTxDataMigrationOp`, and (f) `GetNgLeaderTerm` vs the request's term. Outcome: record found → `Committed` (data log handed to the shipping agent for replay, which releases the lock); multi-stage op found → `Committed`; term mismatch or nothing found → `NotCommitted` (locks are stale; tx can be safely killed).

## 9. LogAgent (client, `tx-log-protos/log_agent.cpp`)

- `Init(ips, ports, start_log_group_id, replica_num)` derives the group layout with `LogUtil::GenerateLogRaftGroupConfig`: nodes are chunked into groups of `replica_num` (group ids from `start_log_group_id`), one brpc channel per node (10s timeout, 3 retries), per-group leader cache `lg_leader_cache_` initialized to node 0.
- `RefreshLeader(lg_id)`: queries braft's routing table (`braft::rtb::refresh_leader`) to find the current raft leader; updates the cache. A background `check_leader_thd_` refreshes leaders every 5s and on demand.
- `GetLogGroupConfig(lg_id)`: fetches the current peer list from the leader and updates the in-memory config; used after `AddPeer`/`RemovePeer`.
- `WriteLog` is asynchronous (caller's closure); `ReplayLog` is synchronous with an infinite 2s-backoff retry loop; `UpdateCheckpointTs`/`RemoveCcNodeGroup` are best-effort broadcasts to all groups; `RecoverTx` is a single synchronous call returning `RecoverError` on RPC failure.
- The tx service wraps it through `EloqLogAgent : TxLog` (`eloq_log_wrapper.h`); `Sharder::UpdateLogGroupLeader` exposes `UpdateLeaderCache` to the `LogReplayService::UpdateLogGroupLeader` RPC.

## 10. Cloud log-state variants (`WITH_LOG_STATE=ROCKSDB_CLOUD_S3/GCS`)

`build_eloq_log_service.cmake` supports four `WITH_LOG_STATE` values: `MEMORY`, `ROCKSDB` (default), `ROCKSDB_CLOUD_S3`, `ROCKSDB_CLOUD_GCS`. The cloud variants:

- Use `log_state_rocksdb_cloud_impl.cpp` (`LogStateRocksDBCloudImpl`) backed by `rocksdb-cloud-aws` or `rocksdb-cloud-gcp`.
- Require `RocksDBCloudConfig` (bucket name/prefix, region, SST cache size, file-deletion delay, `log_retention_days`, daily purger schedule `log_purger_schedule`, archive path, archive move interval). The config is assembled in `core/src/log_init.cpp` from gflags/ini.
- The `LogServer` constructor and `LogServiceImpl::AddLogReplicas` accept a `RocksDBCloudConfig` parameter (conditionally compiled with `LOG_STATE_TYPE_RKDB_CLOUD`).
- `LogInstance::on_leader_start` calls `log_state_->AsyncStartCloudDB` to open RocksDB Cloud asynchronously; writes are buffered in memory until the DB is ready.
- `WITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_*` force-sets `WITH_LOG_STATE` to the matching cloud variant at CMake configure time.
- `DataSubstrate::Shutdown` removes both the raft storage path and the separate RocksDB storage path (via `LogServer::GetRocksDBStoragePath()`) when using cloud log states.

## 11. Build (`build_eloq_log_service.cmake`)

The `logservice` CMake target is defined in `build_eloq_log_service.cmake` (included from the top-level `CMakeLists.txt` when `WITH_LOG_SERVICE` is set). Sources:

```
eloq_log_service/src/log_instance.cpp
eloq_log_service/src/log_server.cpp
eloq_log_service/src/log_state_rocksdb_impl.cpp
eloq_log_service/src/log_state_rocksdb_cloud_impl.cpp
eloq_log_service/src/log_state_memory_impl.cpp
eloq_log_service/src/fault_inject.cpp
eloq_log_service/src/INIReader.cpp
eloq_log_service/src/ini.c
tx_service/tx-log-protos/log.pb.cc
```

Include directories exported: `eloq_log_service/include/` and `tx_service/tx-log-protos/`. Links: brpc, braft, protobuf, gflags, glog, leveldb, rocksdb (or rocksdb-cloud), pthread. Compile-time log-state macros: `LOG_STATE_TYPE_MEM` (MEMORY), `LOG_STATE_TYPE_RKDB` + `LOG_STATE_TYPE_RKDB_ALL` (ROCKSDB), `LOG_STATE_TYPE_RKDB_S3` + `LOG_STATE_TYPE_RKDB_ALL` + `LOG_STATE_TYPE_RKDB_CLOUD` + `USE_AWS` (ROCKSDB_CLOUD_S3), `LOG_STATE_TYPE_RKDB_GCS` + … + `USE_GCP` (ROCKSDB_CLOUD_GCS). `LOG_SHIPPING_THREADS_NUM` is set to 1 for RocksDB-backed variants (a single scanner is sufficient because the iterator partitions the key range internally).

## 12. Gotchas and invariants

1. **Writes go through braft.** Unlike the old open service, `WriteLog` is replicated by braft to a majority before the response is returned. `NotLeader` responses are real and require the client to refresh its leader cache via `LogAgent::RefreshLeader`/`RequestRefreshLeader`.
2. **Term-based validity.** The log service is the authoritative source for cc ng terms. `WriteLog` whose `node_terms` carry a stale ng term is rejected; this is how the log service enforces that transactions with stale locks cannot commit after a cc ng failover.
3. **RocksDB key layout**: `bswap64(ts) ‖ bswap32(ng_id) ‖ bswap64(txn)` (20 bytes). The `ng_id` is embedded in the key (unlike the old open service which had separate per-ng CFs). Replay filters by `ng_id` at scan time using `ItemIteratorRocksDBImpl`.
4. **Parallel replay scan.** `GetLogReplayList` partitions the `[first_ts, last_ts]` key range into `scan_threads` sub-ranges, each backed by its own RocksDB iterator. `LogShippingAgent` ships the partitions in parallel from separate threads.
5. **Multiple shipping agents per instance.** `LogInstance` can have one shipping agent per cc ng; a new `ReplayLog` for the same ng terminates and replaces the existing agent atomically.
6. **Snapshot covers only in-memory state.** The braft snapshot checkpoint covers `cc_ng_info_`, `tx_catalog_ops_`, `tx_split_range_ops_`, `tx_cluster_scale_ops_` (serialized to binary files). Data logs in RocksDB are not snapshotted and survive restarts independently.
7. **Stage logs linger one hour past `min_ckpt_ts_`** (clean-stage schema/split-range/cluster-scale ops) to absorb stale retried prepare logs. `RecoverTx` treats any surviving multi-stage log as `Committed`.
8. **Truncation is file-granular and lazy** (RocksDB variant): only whole L0 SSTs strictly below `min_ckpt_ts - 1` are deleted, only when total SST size exceeds `sst_files_size_limit`; memtable contents are never proactively flushed for truncation.
9. **Fsync per group-commit batch**: data-log `WriteBatch` uses `sync=true`. Additionally, braft fsyncs its raft log on the majority path. The `raft_use_bthread_fsync`/`use_io_uring` gflags affect braft's fsync behavior.
10. **`enable_txlog_request_checkpoint`**: when true, the leader starts a background thread (`replay_log_size_checker_thd_`) that periodically calls `CheckReplayLogSizeAndNotifyCkptIfNeeded`; if the active replay log size exceeds `notify_checkpointer_threshold_size` (default 128 MB), it calls the tx-side `LogReplayService::NotifyCheckpointer` to trigger a checkpoint.

## Cross-references

- `01-architecture-overview.md` — where the log service sits in process bring-up (`core/src/data_substrate.cpp` calls `InitializeLogService` before the store handler).
- `06-distribution-and-clustering.md` — ng leadership/failover that triggers `ReplayLog`; cluster-scale flows that use `CheckClusterScaleStatus`/`CheckMigrationIsFinished`; `AddPeer`/`RemovePeer` for membership changes.
- `07-durability-and-recovery.md` — the tx-side half: `WriteLogOp`, checkpointer → `UpdateCheckpointTs`, `RecoveryService`/`ReplayLogCc`, orphan-lock recovery via `RecoverTx`.
