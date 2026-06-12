# Transaction Execution

A transaction in the tx service is an asynchronous state machine: `TransactionExecution` (a "txm"). API engines hand it `TxRequest`s; the txm decomposes each request into a stack of `TransactionOperation`s; `Forward()` advances the top operation one step at a time, never blocking — when an operation waits on cc requests (local or remote), `Forward()` simply returns control to the processor so it can run other txs or drain the shard's cc queue. This file explains the txm lifecycle, the request/operation vocabulary, and the commit pipeline.

Key files: `tx_service/include/tx_execution.h` + `tx_service/src/tx_execution.cpp`, `tx_service/include/tx_request.h`, `tx_service/include/tx_operation.h`, `tx_service/include/tx_req_result.h`, `tx_service/include/read_write_set.h`, `tx_service/include/command_set.h`, `tx_service/include/cc/cc_handler.h`.

## Lifecycle

1. **Creation.** `TxService::NewTx()` picks a `TxProcessor` (prime-offset round robin for load balance) and reuses a txm from the free pool or allocates one. With `EXT_TX_PROC_ENABLED`, `NewTx(shard_id)` creates an *external* txm pinned to a shard and tracked on that processor's external-active list — this is what API engines running on brpc workers use.
2. **Request submission.** The engine calls `txm->Execute(tx_req)` which enqueues into `tx_req_queue_`, then typically blocks/yields on the request's `TxResult` (requests carry optional `yield_fptr`/`resume_fptr` so a bthread can yield instead of blocking; `TemplateTxRequest<Subtype, T>` dispatches back via `txm->ProcessTxRequest(subtype&)`).
3. **Forwarding.** `Forward()` (called by the processor under the shard latch, or inline via `TxProcessor::ForwardTx` / `ExternalForward`) dequeues a request, lets it push operations onto `state_stack_`, then repeatedly `Process(op)` / checks completion / `PostProcess(op)` until the stack empties and the request's `TxResult` is finished.
4. **Completion.** On the final request (commit/abort), status becomes `TxnStatus::Finished`, `Reset()` clears state, and the txm returns to the free pool for reuse.

`InitTxRequest` starts the tx proper: `InitTxnOperation` registers a `TEntry` on the owner shard, yielding `TxId`/`tx_number_`, the **tx term** (the owner NG's current leader term — used for fencing everywhere), and `start_ts_`.

## Concurrency around a txm

- A txm is forwarded by exactly one thread at a time (shard latch, see [02-threading-model.md](02-threading-model.md)).
- `forward_latch_` arbitrates between `Forward()` (exclusive, −1) and the `CcStreamReceiver` writing remote results into the txm's `CcHandlerResult` (shared, +k). This prevents a timeout/retry in `Forward()` from freeing a result a stream thread is filling. See [06-distribution-and-clustering.md](06-distribution-and-clustering.md).
- `command_id_` increments per handled request / remote send; stale remote responses (from a timed-out, re-sent cc request) are discarded by matching `tx_number_` + command id.
- `IsTimeOut()` drives re-execution of stuck remote operations; `CheckLeaderTerm()` / `CheckStandbyTerm()` abort txs fenced off by leadership changes.

## TxRequest vocabulary

All in `tx_service/include/tx_request.h`; each maps to `ProcessTxRequest(...)` overloads in `tx_execution.h`.

| Group | Requests | Notes |
|---|---|---|
| Lifecycle | `InitTxRequest`, `CommitTxRequest`, `AbortTxRequest` | commit carries `to_commit` flag semantics via bool result |
| Point data | `ReadTxRequest`, `ReadOutsideTxRequest`, `BatchReadTxRequest`, `UpsertTxRequest` | record model (SQL/doc); upsert buffers into the write set, real work at commit |
| Scans | `ScanOpenTxRequest`, `ScanBatchTxRequest`, `ScanCloseTxRequest` | scan state kept per alias in `scans_`; see [03](03-concurrency-control.md) scanners |
| Object commands | `ObjectCommandTxRequest`, `MultiObjectCommandTxRequest`, `PublishTxRequest` | command model (EloqKV): `TxCommand` ships to the owner shard and executes against the `TxObject` (`ApplyCc`); auto-commit unless inside an explicit tx |
| DDL / schema | `UpsertTableTxRequest`, `SchemaRecoveryTxRequest`, `InvalidateTableCacheTxRequest` | drives `UpsertTableOp` multi-stage schema change |
| Statistics | `AnalyzeTableTxRequest`, `BroadcastStatisticsTxRequest` | |
| Maintenance / cluster | `SplitFlushTxRequest`, `RangeSplitRecoveryTxRequest`, `ClusterScaleTxRequest`, `DataMigrationTxRequest`, `ReloadCacheTxRequest`, `CleanArchivesTxRequest` | composite multi-stage ops, see [08](08-range-and-bucket-management.md)/[06](06-distribution-and-clustering.md) |
| Test | `FaultInjectTxRequest`, `CleanCcEntryForTestTxRequest` | Debug builds |

The read/write footprint accumulates in `ReadWriteSet` (`rw_set_`: read entries with version ts + lock info, table write sets) and `CommandSet` (`cmd_set_`: object commands for forwarding/logging). EloqKV database locks are cached in `locked_db_[16]`.

## The commit pipeline

`TransactionExecution::Commit()` (`tx_execution.cpp`) chooses a path:

- **Object-command txs with forward writes** (commands executed on remote/standby-forwarded objects): `CmdForwardAcquireWriteOp` first.
- **Data write set non-empty:** the full pipeline below.
- **Catalog writes only:** `CatalogAcquireAllOp` path (schema changes acquire write-all on catalog entries across shards).
- **Read-only:** skip straight to `SetCommitTsOperation` (or just validation when recovering).

Full write pipeline (each stage is a `TransactionOperation` pushed on the stack; all fan out cc requests through `CcHandler` and complete asynchronously):

1. `LockWriteRangeBucketsOp` — read-lock the range/bucket meta entries covering every write key, pinning placement ([08](08-range-and-bucket-management.md)); this is also where the tx learns each key's owner NG.
2. `AcquireWriteOperation` — `AcquireCc` write locks/intents on all write-set keys at their owner shards (2PL blocks, OCC fails fast; see [03](03-concurrency-control.md)).
3. `SetCommitTsOperation` — negotiate the commit timestamp: max over (local clock, start_ts, per-entry last-read/commit ts bounds gathered during acquisition).
4. `ValidateOperation` — OCC validation of the read set (`PostReadCc`: re-check versions / convert read intents), abort on conflict.
5. `WriteToLogOp` — build the redo record (`FillDataLogRequest`: for object commands the *commands* are logged; for records the values) and call `TxLog::WriteLog` to the tx's log group. WAL is the commit point ([07](07-durability-and-recovery.md), [10](10-log-service.md)).
6. `UpdateTxnStatus` — flip the `TEntry` to Committed/Aborted.
7. `PostProcessOp` — install committed values / roll back, release locks (`PostWriteCc` / `PostWriteAllCc`), downgrade meta locks, notify standby forwarding.

Aborts run the same tail (UpdateTxnStatus → PostProcess with rollback). A txm whose coordinator died is finished by **recovery**: lock holders' `TEntry`s are resolved via `CheckTxStatus`/`RecoverTx` ([07](07-durability-and-recovery.md)), and `SetRecoverTxState` lets a replacement txm release orphaned locks with a predetermined commit ts.

## Operations beyond the data path

`tx_service/include/tx_operation.h` also defines composite, multi-stage operations driven by the same Forward() loop; each owns its child ops and its own log records, and each has a recovery entry point:

- `UpsertTableOp` (schema/DDL: acquire-all catalog locks → log → kv `DsUpsertTableOp` → install dirty schema → flush → commit catalog), plus `UpsertTableIndexOp` for index builds (`tx_index_operation.h`, [08](08-range-and-bucket-management.md)).
- `SplitFlushRangeOp` (range split), `ClusterScaleOp` + `DataMigrationOp` (bucket migration), `InvalidateTableCacheCompositeOp`.
- `KickoutDataOp`/`KickoutDataAllOp` (cache eviction), `AnalyzeTableAllOp`/`BroadcastStatisticsOp` (statistics), `AsyncOp<T>` (generic kv-store async step), `NoOp`, `SleepOperation`.

## Isolation & protocols

Set per tx at init (`iso_level_`, `protocol_`): `IsolationLevel` {ReadCommitted, Snapshot (requires `enable_mvcc`), RepeatableRead, Serializable} × `CcProtocol` {OCC, OccRead, Locking}. The lock type taken by each operation is decided by `LockTypeUtil::DeduceLockType` (`tx_service/include/cc_protocol.h`); `DeduceReadLockType` additionally special-cases meta tables and covering-key index reads. Snapshot reads use MVCC version chains and `TxStartTsCollector` ([07](07-durability-and-recovery.md)).

## Gotchas

- Operation objects are *members* of the txm (`read_`, `acquire_write_`, `write_log_`, ...) — they are reused across requests; `Reset()` correctness on every path matters.
- `ReadLocalOperation` reads node-local meta (cluster config, ranges, buckets) without remote hops; it relies on meta cc maps being replicated to every node ([03](03-concurrency-control.md)).
- Blocking object commands (e.g. Redis BLPOP-style, `BlockOperation` in `tx_command.h`) park in `tx_progress_block_` on the processor and are re-enlisted by `CheckWaitingTxs()` (10ms cadence; regular stuck txs at 2s).
- `RETRY_NUM` / `state_forward_cnt_` guard against infinite re-execution; `ForwardFailed` keeps a tx on the on-fly queue rather than spinning.
