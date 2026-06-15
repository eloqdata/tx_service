# Data Substrate Technical Documentation

Module-by-module design documentation for this repo, written so that an engineer (or an AI model) can understand the core design before reading the code. Every doc cites the source files it was derived from — when docs and code disagree, the code wins; please fix the doc in the same change.

## Reading order

| Doc | Module | Start here if you're touching… |
|---|---|---|
| [01-architecture-overview.md](01-architecture-overview.md) | System overview, `core/` init, configuration | anything (read first) |
| [02-threading-model.md](02-threading-model.md) | TxProcessor, brpc/bthread integration, shard latch, per-shard heaps | any concurrency primitive, wakeups, brpc workers |
| [03-concurrency-control.md](03-concurrency-control.md) | CcShard, CcMap/CcPage/CcEntry, NonBlockingLock, CcRequest lifecycle, deadlock detection | `tx_service/include/cc/`, locks, cache eviction |
| [04-transaction-execution.md](04-transaction-execution.md) | TransactionExecution state machine, TxRequests, commit pipeline | `tx_execution.*`, `tx_operation.h`, `tx_request.h` |
| [05-data-model-and-catalog.md](05-data-model-and-catalog.md) | TxKey/TxRecord/TxObject/TxCommand, schemas, CatalogFactory plug-in | engine integration, keys/records, DDL metadata |
| [06-distribution-and-clustering.md](06-distribution-and-clustering.md) | Sharder, node groups, leadership/terms, remote CC streams & RPCs, cluster scale | `sharder.*`, `remote/`, topology, failover |
| [07-durability-and-recovery.md](07-durability-and-recovery.md) | WAL writes, checkpointing, data sync, log replay recovery, MVCC archives | `checkpointer.*`, `txlog.h`, `fault/`, data sync |
| [08-range-and-bucket-management.md](08-range-and-bucket-management.md) | Hash buckets vs range partitions, StoreRange/StoreSlice, range split, bucket migration, index build | `range_slice.*`, `range_*`, migration, `sk_generator.h` |
| [09-store-handler.md](09-store-handler.md) | DataStoreHandler contract, EloqDSS service & client, RocksDB/cloud backends | `store_handler/` |
| [10-log-service.md](10-log-service.md) | Log service (`eloq_log_service/`), braft Raft replication, LogAgent client | WAL service, log replay plumbing |
| [standby_replication_protocol.md](standby_replication_protocol.md) | Standby replication protocol (pre-existing design doc) | standby/follower reads |

## Scope and exclusions

`eloq_log_service/` is now documented in `10-log-service.md`. **`tx_service/raft_host_manager/` is proprietary and intentionally undocumented here**; it is referenced only at its interface boundary (the `HostMangerService` RPC contract). The `store_handler/eloq_data_store_service/eloqstore/` submodule (EloqStore engine) is likewise described only at its integration surface.

## Maintenance rules

- **Code changes that alter behavior described here must update the corresponding doc in the same PR.** The per-doc "Key files" lists tell you which doc owns which source files.
- Keep docs grounded: cite file paths, prefer describing invariants and flows over API listings, and delete statements you can no longer verify.
- New module → new numbered doc + a row in the table above + a pointer from the repo `CLAUDE.md`.
