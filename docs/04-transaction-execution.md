# Transaction Execution

A transaction in the tx service is an asynchronous state machine: `TransactionExecution` (a "txm"). API engines hand it `TxRequest`s; the txm decomposes each request into a stack of `TransactionOperation`s; `Forward()` advances the top operation one step at a time, never blocking — when an operation waits on cc requests (local or remote), `Forward()` simply returns control to the processor so it can run other txs or drain the shard's cc queue. This file explains the txm lifecycle, the request/operation vocabulary, and the commit pipeline.

Key files: `tx_service/include/tx_execution.h` + `tx_service/src/tx_execution.cpp`, `tx_service/include/tx_request.h`, `tx_service/include/tx_operation.h`, `tx_service/src/tx_upsert_table_operation.cpp` (`UpsertTableOp`), `tx_service/include/tx_req_result.h`, `tx_service/include/read_write_set.h`, `tx_service/include/command_set.h`, `tx_service/include/cc/cc_handler.h`.

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

### Scan read ownership

Closing or draining a scanner does not release data read locks or read intents
while the transaction can still execute requests.
Returned locked CCEs already recorded as semantic data reads remain in the data
read set; command/write-set tuples keep their existing ownership paths.
Scanner-only last/trailing/error cleanup CCEs are stored there with version 0,
which skips version validation but keeps the CCE non-evictable until final
cleanup. Commit releases them through `ValidateOperation`; abort releases them
through `PostProcessOp`.

This retention can increase the read-set footprint and writer blocking for long
locking scans. It also makes node groups touched only by scanner pins part of
the final post-read term check, so a leadership change on one of those groups
can abort the transaction.

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

- `UpsertTableOp` (schema/DDL: the cluster config read lock, failing with `CLUSTER_GATED` while the cluster gate is paused ([06](06-distribution-and-clustering.md)) → catalog write intents → prepare log → install the dirty schema → CREATE's kv tables (`create_kv_table_op_`) → write locks, where CREATE/DROP/TRUNCATE re-assign the commit timestamp above every drained DML (`AssignCommitStageTs`) → on shared storage TRUNCATE's data (cc maps, kv table, sequences; `TruncateUntilCommitLog`) → commit log → the kv step (CREATE's catalog row, TRUNCATE's catalog row or, without shared storage, each node group's truncate, DROP's kv tables and sequences) → commit the schema at the commit timestamp → release the cluster config lock → clean log; a data-store step after the commit log, or TRUNCATE's before it, retries indefinitely, `ddl_kv_retry_interval_seconds` apart (30 s by default) with a warning each time (`SchemaOp::ArmKvRetryWait`, `kv_retry_wait_op_`), and never fails the op; CREATE's kv tables and sequence table update, before the commit log, are not retried: the create gives up (`AbandonCreate`): it drops what it created (`drop_kv_table_op_`, the store's drop-table path) and fails with a commit log at timestamp 0; ALTER writes its catalog row before the commit log and rolls back on failure). `UpsertTableOp` runs as a stackful coroutine (boost::context, on a 128 KB stack of its own): the first `Forward()` starts it and each later one resumes it once the step it awaits has left the tx's stack. Its path is written out flat in three segments cut at the two log records (`RunUntilPrepareLog`, `RunUntilCommitLog`, `RunAfterCommitLog`), which is where recovery enters it (`UpsertTableOp::Entry`); a resume can land on another thread, so the body touches no thread_local state. Index builds run `UpsertTableIndexOp` (`tx_index_operation.h`, [08](08-range-and-bucket-management.md)).
- `SplitFlushRangeOp` (range split), `ClusterScaleOp` + `DataMigrationOp` (bucket migration), `InvalidateTableCacheCompositeOp`.
- `ClusterGateOp` (`tx_cluster_gate_operation.cpp`; `ClusterGateTxRequest`, pause or resume of the cluster gate, [06](06-distribution-and-clustering.md)): a read of the record for write (a write intent, which schema ops' read locks pass and another gate op coordinated by the same node group waits for) on the coordinator's node group and the precondition (a pause of a paused gate fails with `CLUSTER_GATE_HELD`, and the abort releases the intent) → write intents on every node group with OCC, which only another gate op or a cluster scale can fail, and which fail rather than wait, so gate ops and cluster scale cannot deadlock → write locks on every node group with Locking, upgrading the intents, which waits for every schema op holding the read lock (the drain); `AcquireAllOp` then drops the read from the read set (`DedupRead`), as for any key read before it is written, so the commit's release of meta reads does not validate the version the install replaces → one `cluster_gate_log` entry carrying the granting node groups' terms, so a group that failed over since is refused by the log service → the post write all of a record carrying the new gate, which stores it and releases the lock. Any failure before the log (a conflict, a node group without a leader, a refused log) releases everything at `tx_op_failed_ts_`, backs off 1–3 s and starts over from the read inside the op; after the log the op rolls forward, retrying the install a second apart. The request is the whole transaction: the op commits or aborts it, and its result is the commit timestamp. The caller initializes the transaction with `log_group_id_ = 0`. Recovery (`RecoverClusterGate`) runs the install alone.
- `KickoutDataOp`/`KickoutDataAllOp` (cache eviction), `AnalyzeTableAllOp`/`BroadcastStatisticsOp` (statistics), `AsyncOp<T>` (generic kv-store async step; `SetBlockingWork` runs blocking work on the op's worker thread), `NoOp`, `SleepOperation`.

## Isolation & protocols

Set per tx at init (`iso_level_`, `protocol_`): `IsolationLevel` {ReadCommitted, Snapshot (requires `enable_mvcc`), RepeatableRead, Serializable} × `CcProtocol` {OCC, OccRead, Locking}. The lock type taken by each operation is decided by `LockTypeUtil::DeduceLockType` (`tx_service/include/cc_protocol.h`); `DeduceReadLockType` additionally special-cases meta tables and covering-key index reads. Snapshot reads use MVCC version chains and `TxStartTsCollector` ([07](07-durability-and-recovery.md)).

**Read-local overrides the tx's settings** (`Process(ReadOperation&)`, `tx_service/src/tx_execution.cpp`). `ReadLocal` is used for catalog reads, which must take a real read lock whatever the tx asked for, so the isolation level is forced up to at least `RepeatableRead`. The protocol is then chosen by `is_for_write_`:

| `is_for_write_` | protocol | lock | on conflict |
|---|---|---|---|
| `false` (catalog read) | `Locking` | `ReadLock` | blocks in the entry's queue |
| `true` (catalog read for write) | `OCC` | `WriteIntent` | **fails fast** → `ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT` |

The lock type is identical in both rows — `DeduceLockType` returns `WriteIntent` for `ReadForWrite` under every protocol. Only the failure status differs. Waiting on a catalog write intent closes a lock cycle against a DDL that is already past its prepare log and therefore cannot be aborted; see the rationale in [03](03-concurrency-control.md#5-nonblockinglock-non_blocking_lockh-non_blocking_lockcpp). `UpsertTableIndexOp::acquire_all_intent_op_` and `CatalogAcquireAllOp::acquire_all_intent_op_` already picked `OCC` for the same reason.

`Process(ScanOpenOperation&)` still hardcodes `Locking` for read-local scans, so a read-local scan for write would take a blocking write intent. No in-tree caller constructs one today.

A failed `CatalogAcquireAllOp` reports `AcquireAllOp::RepresentativeError()` on `bool_resp_` before aborting. `fail_cnt_` alone cannot supply a code — it is also raised on the dedup read-version-mismatch path where every handler result is successful — and an abort reported as `NO_ERROR` reads as success at the API boundary. Within one op, an infrastructure error (`REQUESTED_NODE_NOT_LEADER`, `NG_TERM_CHANGED`, timeouts) outranks a conflict error (`IsConflictError`, `error_messages.h`), because API layers retry conflicts and some of those retry loops have no bound of their own.

## Gotchas

- Operation objects are *members* of the txm (`read_`, `acquire_write_`, `write_log_`, ...) — they are reused across requests; `Reset()` correctness on every path matters.
- `ReadLocalOperation` reads node-local meta (cluster config, ranges, buckets) without remote hops; it relies on meta cc maps being replicated to every node ([03](03-concurrency-control.md)).
- Blocking object commands (e.g. Redis BLPOP-style, `BlockOperation` in `tx_command.h`) park in `tx_progress_block_` on the processor and are re-enlisted by `CheckWaitingTxs()` (10ms cadence; regular stuck txs at 2s).
- `RETRY_NUM` / `state_forward_cnt_` guard against infinite re-execution; `ForwardFailed` keeps a tx on the on-fly queue rather than spinning.
