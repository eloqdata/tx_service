# Architecture Overview

Data Substrate (repo name `tx_service`) is EloqData's API-agnostic database foundation: a distributed, in-memory, transactional cache layer (the **tx service**) plus a pluggable WAL service (**log service**) and a pluggable persistent storage layer (**store handler / EloqDSS**). API engines — EloqKV (Redis), EloqSQL (MySQL), EloqDoc (MongoDB) — link this library as a git submodule, register a `CatalogFactory` that supplies their concrete key/record/schema types, and drive everything through `TxRequest`s. The core never knows what a "Redis hash" or a "SQL row" is; it sees type-erased keys, records, and commands (see [05-data-model-and-catalog.md](05-data-model-and-catalog.md)).

## Component Map

| Directory | What it is | Doc |
|---|---|---|
| `core/` | `DataSubstrate` singleton: config loading, engine registration, ordered startup of log service → metrics → storage → tx service | this file |
| `tx_service/` | The engine: CcShards, concurrency control, transaction state machines, distribution, checkpointing | [02](02-threading-model.md), [03](03-concurrency-control.md), [04](04-transaction-execution.md), [06](06-distribution-and-clustering.md), [07](07-durability-and-recovery.md), [08](08-range-and-bucket-management.md) |
| `store_handler/` | `DataStoreHandler` backends: EloqDSS client, embedded RocksDB, DynamoDB, BigTable | [09-store-handler.md](09-store-handler.md) |
| `store_handler/eloq_data_store_service/` | EloqDSS: a standalone (or in-process) brpc data-store service over RocksDB / RocksDB-Cloud / EloqStore | [09-store-handler.md](09-store-handler.md) |
| `log_service/` | Open-source replicated WAL service (braft-based log groups), selected by `OPEN_LOG_SERVICE=ON` | [10-log-service.md](10-log-service.md) |
| `tx_service/tx-log-protos/` | Shared protobuf definitions + `LogAgent` (log service client used by tx nodes) | [10-log-service.md](10-log-service.md) |
| `eloq_metrics/` | Metrics library (`Meter`/`MetricsRegistry`, Prometheus collector) | — |
| `tx_service/abseil-cpp/`, `store_handler/eloq_data_store_service/eloqstore/` | Vendored submodules | — |
| `eloq_log_service/`, `tx_service/raft_host_manager/` | **Proprietary components — intentionally not covered by these docs** | — |

## The Big Picture

```
   API engine (EloqKV / EloqSQL / EloqDoc)
        │  TxRequest (read / scan / upsert / object command / DDL / commit)
        ▼
   TransactionExecution (async state machine, one per active tx)      [04]
        │  CcRequest via CcHandler (Local- or Remote-)                [03][06]
        ▼
   CcShard 0..N-1  (one per core; CcMaps hold CcEntries + locks)      [03]
     │ owned & driven by TxProcessor N / brpc worker N                [02]
     │
     ├─ commit: WriteLog ───────────────► Log Service (replicated WAL) [10]
     ├─ checkpoint: flush dirty entries ► DataStoreHandler / EloqDSS   [07][09]
     └─ cache miss / slice load ◄──────── DataStoreHandler / EloqDSS   [08][09]
```

Key design decisions:

- **Thread-per-core, message passing.** Data is sharded across `CcShard`s, one per core. A shard is touched only by its owning processor; everyone else (transactions, RPC handlers, background workers) enqueues `CcRequest`s. There are almost no data locks — the per-key `NonBlockingLock` is a *transactional* lock (2PL/OCC), not a mutex. See [02-threading-model.md](02-threading-model.md) and [03-concurrency-control.md](03-concurrency-control.md).
- **Memory is the primary replica.** Committed data lives in cc maps; the WAL makes it durable; the kv store holds checkpointed/cold data. A node-group failover recovers memory state by replaying the log on top of the kv store. See [07-durability-and-recovery.md](07-durability-and-recovery.md).
- **Everything transactional is a state machine.** `TransactionExecution::Forward()` advances a stack of `TransactionOperation`s without blocking; blocked operations simply yield the processor. See [04-transaction-execution.md](04-transaction-execution.md).
- **Cluster = node groups + buckets.** Nodes form raft-replicated node groups (NG). Keys hash into 1024 buckets; each bucket is owned by an NG. Raft membership/election is delegated to an external *host manager* process (proprietary; interacted with only via RPC). See [06-distribution-and-clustering.md](06-distribution-and-clustering.md).

## Startup Sequence (`core/`)

`core/src/data_substrate.cpp` implements a singleton with a strict three-phase lifecycle (`InitState`: NotInitialized → ConfigLoaded → Started):

1. **`DataSubstrate::Init(config_file)`** — parses the ini file, loads core + network config (`LoadCoreAndNetworkConfig`). gflags take priority over ini values (`CheckCommandLineFlagIsDefault` pattern, used everywhere). Auto-configures `core_number` (~90% of vCPUs, minus reserved EloqStore cloud threads), `node_memory_limit_mb` (~80% of RAM), brpc `event_dispatcher_num`, `bthread_concurrency`. Resolves cluster topology from `cluster_config_file` (written by the host manager on config updates) or from `tx_ip_port_list`/standby/voter lists (`ParseNgConfig`), and verifies the local node is in the cluster. Adds the `Sequences` system table to prebuilt tables.
2. **`DataSubstrate::RegisterEngine(engine_type, catalog_factory, system_handler, prebuilt_tables, engine_metrics, publish_func)`** — called by each API engine between Init and Start. Engine slots are indexed by `TableEngine` (EloqSql=1, EloqKv=2, EloqDoc=3). For converged binaries, `EnableEngine` + `WaitForEnabledEnginesRegistered` let the loader block until all expected engines registered.
3. **`DataSubstrate::Start()`** — ordered phases:
   1. `InitializeLogService` (`core/src/log_init.cpp`): when `WITH_LOG_SERVICE`, starts an in-process `txlog::LogServer` on `tx_port + 2` ([10-log-service.md](10-log-service.md)).
   2. `InitializeMetrics` (`core/src/metrics_init.cpp`).
   3. `InitializeStorageHandler` (`core/src/storage_init.cpp`) if `enable_data_store`: instantiates the backend selected by `WITH_DATA_STORE`, possibly starting an in-process `DataStoreService` ([09-store-handler.md](09-store-handler.md)).
   4. `InitializeTxService` (`core/src/tx_service_init.cpp`): builds `TxService` → which constructs `LocalCcShards` (all shards) + `Checkpointer`, then `TxService::Start()` boots `SnapshotManager`, `Sharder` (incl. host manager fork), `TxStartTsCollector` (MVCC), `DeadLockCheck`, one `tx_proc_N` thread per core, registers the `TxServiceModule` into brpc (`ELOQ_MODULE_ENABLED`), and connects cc stream sender/receiver.

`Shutdown()` reverses this (tx service → storage → log service), with special bootstrap-mode cleanup that wipes log storage paths.

## Configuration Surface

All knobs follow *gflag overrides ini section* (`[local]`, `[cluster]`, `[store]`). The most important ones (defined in `core/src/data_substrate.cpp` and `core/src/tx_service_init.cpp`):

| Flag | Default | Meaning |
|---|---|---|
| `eloq_data_path` | /tmp/eloq_data | Root data path |
| `core_number` | 8 / auto | Number of CcShards/TxProcessors |
| `node_memory_limit_mb` | 8192 / auto | Per-node cc map memory budget |
| `enable_wal` / `enable_data_store` | true | Durability toggles (`enable_wal` requires `enable_data_store`) |
| `enable_mvcc` | false | MVCC version chains + snapshot reads |
| `enable_cache_replacement` | true | LRU kickout of clean entries (requires data store) |
| `tx_ip` / `tx_port` | 127.0.0.1:16379 | Local tx service address |
| `tx_ip_port_list`, `tx_standby_ip_port_list`, `tx_voter_ip_port_list` | — | Cluster topology bootstrap |
| `node_group_replica_num` | 3 | Members per node group |
| `bootstrap` | false | Init system tables and exit |
| `checkpointer_interval` | 10s | Ckpt cadence (also delay/min-interval variants) |
| `fork_host_manager` / `hm_ip` / `hm_port` / `hm_bin_path` | true | Host manager process wiring |
| `auto_redirect` | false | Redirect object commands to remote owner NG internally |
| `enable_key_cache` | false | Key cache (non-MVCC only) |
| `enable_io_uring` / `raft_log_async_fsync` | false | IO engine options |
| `max_standby_lag` | 400000 | Max primary→standby message lag |

Build-time options are described in the repo `CLAUDE.md` (`WITH_DATA_STORE`, `WITH_LOG_STATE`, `WITH_LOG_SERVICE`, `OPEN_LOG_SERVICE`, `EXT_TX_PROC_ENABLED`, `ELOQ_MODULE_ENABLED`).

## Global Singletons

A few process-wide singletons are referenced from everywhere; knowing them shortcuts a lot of code reading:

- `Sharder::Instance()` — topology, leadership terms, bucket mapping ([06](06-distribution-and-clustering.md)).
- `DataSubstrate::Instance()` — lifecycle + config (`core/include/data_substrate.h`).
- `DeadLockCheck` (static instance), `TxStartTsCollector::Instance()`, `store::SnapshotManager::Instance()`.
- Global behavior flags set by `TxService` ctor: `txservice_skip_wal`, `txservice_skip_kv`, `txservice_enable_cache_replacement`, `txservice_auto_redirect_redis_cmd`, `txservice_enable_key_cache`.
