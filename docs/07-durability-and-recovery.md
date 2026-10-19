# 07 — Durability & Recovery

Data Substrate separates transaction durability from long-term data storage.
The log service makes committed transaction effects recoverable on the commit
path; the store handler receives checkpointed base data, metadata, and MVCC
archives later. Recovery rebuilds an NG's in-memory serving state by combining
the durable store boundary with every relevant log group.

This separation keeps storage I/O out of transaction commit while imposing a
strict rule: WAL can be discarded only after the corresponding state is known
to be durable in the store.

## Durability boundaries

With WAL enabled, a transaction reaches its durability decision through
`WriteToLogOp`. Its log record carries the transaction identity, commit
timestamp, serialized effects, and the terms of participating NGs. The log
service rejects a write whose participant term is stale, preventing a
transaction that crossed a leadership change from committing under obsolete
locks.

Ordinary data transactions write their effects to one selected log group.
Schema changes, range splits, cluster scaling, and bucket migration use staged
records because their effects span metadata and data lifecycles. Those stages
are recovery state, not a second execution history: replay uses them to decide
which incomplete operation must be resumed or completed. The cluster gate,
which pauses schema operations while a change data capture consumer copies the
data, writes one unstaged record per change, kept in log group 0's slot.

A transaction's record holds a blob per NG it writes. A key double-written
while its range splits or its bucket migrates goes into the blob of every NG it
is written to, once per NG: until that NG checkpoints the key, its blob is all
that NG's recovery has of it.

An uncertain log result is not treated as an abort. The coordinator preserves
write locks until the result can be established from the log service, because
clearing them early could expose a committed value as aborted.

The supported durability modes are composed from two independent boundaries:

- WAL controls whether committed in-memory changes can be replayed.
- The data store controls checkpointing, cold reads, snapshots, and bounded
  cache replacement.

Mode validation and backend selection belong to startup configuration; the
architecture invariant is that any enabled cache eviction requires a durable
store copy.

## Checkpointing

`Checkpointer` runs per node and advances each locally led NG independently,
every `checkpointer_interval` (at least 5 s) or on request. It first computes a
safe timestamp that does not pass active transactions or outstanding write
locks: below each transaction's latest write-lock grant on a data table or
catalog entry (range, bucket and cluster-config write locks, intents and read
locks do not count). A DML transaction picks its commit timestamp after its
last write lock, and a schema operation commits above its catalog write lock,
so no transaction commits at or below a checkpoint timestamp once it is taken.
That is what makes the timestamp an NG reports to the log service a watermark
for change data capture. Data-sync tasks then scan dirty entries on their owner
shards, materialize base rows and archives, and flush them through
`DataStoreHandler`.

For backends where batch writes are only staged, `PersistKV` is the durability
barrier. Entry checkpoint timestamps and the NG checkpoint watermark cannot be
published as durable until that barrier succeeds. A failed, skipped, or
non-truncatable task prevents the affected checkpoint round from advancing the
log-truncation boundary.

Entry checkpoint timestamps advance at the complete backend durability
boundary. Non-MVCC EloqStore flushes publish each durable partition; RocksDB
backends wait for `PersistKV`, and MVCC also waits for archive writes. A merged
flush retains only the newest represented term for each NG, so discarded
lower-term writes cannot publish checkpoint state. Split-child flushes update
entries on their source range's owning shard.

Clean entries can be evicted after their checkpoint state is published. The NG
checkpoint timestamp and log-truncation boundary advance only after every
included task succeeds, independently of earlier entry-level progress.

Flush quota represents resident scan buffers. A completed partition releases
quota only after freeing the key and payload allocations it owns; shared
metadata and deferred buffers remain charged until the round releases them.
This keeps admission accounting bounded by memory actually retained across
successful, failed, and deferred writes.

The same data-sync machinery also supports range splits, index construction,
bucket migration, snapshots, and shutdown flushing. Those callers can require
a flush without allowing log truncation; a task's origin and completion state
therefore matter to the shared durability boundary.

Range data-sync scan batches retain keys and payload references allocated on
the source shard. Reset only re-arms the scan request; after consuming a batch,
the caller returns its remaining references through
`LocalCcShards::ReleaseScanResultsAndWait`, which schedules release on that
source shard and waits for completion. Payloads already transferred to a flush
task keep independent ownership, and releasing scan references does not clear
entries' checkpoint state or substitute for successful flush completion.

## Leader recovery

A newly elected node starts as a candidate leader. It establishes the storage
backend's leader lifecycle, initializes required metadata, and requests replay
from all log groups. If it was a fully synchronized standby, it may preserve
the warm cache and start replay after its last consistent timestamp; otherwise
it reconstructs state from the durable store plus the complete applicable log
history.

The node asks each log group for replay once, at its leader start, with one
bounded attempt, and asks again only a group that has neither finished the
term's replay nor opened a replay stream a few seconds after the last request
(`RecoveryService`'s watchdog); a stream that stalls is closed by its idle
timeout, which asks that group again. A group asked twice ships twice, the
second shipping replacing the first.

Replay orders dependencies before ordinary data. Cluster and bucket metadata,
the cluster gate, catalog operations, and range-split state are restored before
data records are allowed to complete. Barriers across log-group streams prevent
a data record from being interpreted against an obsolete schema or placement
model.

Each replay request is fenced by the candidate term. Data replay is idempotent:
records already represented by an equal or newer committed version are skipped,
and records outside the recovered NG's current ownership are ignored. In-flight
schema, split, scale, and migration operations are resumed from their durable
stage through recovery transaction requests.

A recovered schema operation's coordinator NG restores a write intent on the
catalog entry, so data replay of the table proceeds, and the operation resumes
after the log record it was recovered from, at the timestamp that record
carries. A participant restores the lock the coordinator may rely on: after a
prepare-stage record the write lock, unless a range split it coordinates holds
the table's catalog read lock, and for an index build only after its last
prepare-data record; after a commit-stage record the committed schema and no
lock ([range-split-schema-op-recovery.md](range-split-schema-op-recovery.md)). A
node whose catalog entry is write-locked this way finishes start-up only once
the operation commits. The recovered cluster gate is set in place, and the
gate's coordinator re-sends its install with no lock, releasing the write lock
wherever the gate transaction still holds it.

The node publishes its serving leader term only after every log group reports
replay complete. A broken replay stream is retried; partial progress never
authorizes normal traffic.

## Orphaned transaction locks

Locks can remain on participant NGs after the coordinator fails. Recovery first
checks whether the coordinator can still establish the transaction's state. If
that is inconclusive, `RecoveryService` asks the log service whether the
transaction is committed under the recorded coordinator and participant terms.

An uncommitted transaction can be cleared. A committed transaction is replayed
onto entries that still retain its write lock and then released. A transaction
found as a multi-stage operation or as the cluster gate transaction is also
committed, but nothing is shipped: its metadata locks stay until its
coordinator's recovery releases them. An unreachable
log service leaves the lock in place for a later retry. This favors correctness
over availability when the commit decision is unknown.

## MVCC persistence

When MVCC is enabled, an update archives the prior committed version before
installing the new one. Checkpoint data sync persists both base values and
historical versions: per entry, the newest version at or below the round's
checkpoint timestamp goes to the base table and older ones to the archives
table. Snapshot reads first consult the in-memory version chain and fetch the
necessary base or archive version from the store when the chain no longer
covers the requested timestamp. A snapshot transaction starts at the later of
the shard's timestamp base and its wall clock, and advances the base.

The oldest active snapshot transaction's start alone bounds the reclamation of
in-memory versions (`CcEntry::KickOutArchiveRecords`). The checkpoint delay
(`checkpointer_delay_seconds`, 5 s by default) is a cache residency choice:
while a snapshot transaction that started earlier runs, it lowers a round's
checkpoint timestamp by at most the delay, so recently updated entries stay
dirty and therefore resident, and a snapshot read finds their versions in
memory. Active-transaction timestamp collection is therefore part of the
garbage-collection model rather than only an observability feature.

## Snapshots and standby bootstrap

`SnapshotManager` coordinates storage snapshots for standby bootstrap and
backup. A standby snapshot is taken only after checkpoint progress passes the
subscription barrier, then the standby applies the overlapping incremental
stream with version checks. The detailed ordering and out-of-sync contract is
defined in
[standby_replication_protocol.md](standby_replication_protocol.md).

Snapshot creation does not replace WAL replay for leader recovery. It provides
a durable base image; replay supplies all effects after that base and resolves
incomplete durable operations.

Checkpoint rounds may start from the periodic interval, shutdown, or an
accepted request caused by memory pressure, the dirty-memory threshold, or an
explicit caller. Trigger diagnostics record why scheduling occurred, not
checkpoint completion; rate-limited or coalesced requests do not create a new
accepted-request event.

## Cross-cutting invariants

- A checkpoint timestamp never passes an active write whose outcome is not
  represented durably, and no transaction commits at or below it afterwards.
- Log truncation advances only after every included store write and required
  persistence barrier succeeds.
- A candidate term fences replay; a serving leader term is published only after
  all log groups finish.
- Replay is version-aware and idempotent so reconnecting a stream cannot apply
  a committed effect twice.
- Unknown commit status retains write locks until coordinator or WAL evidence
  makes cleanup safe.
- Cache eviction is legal only for versions covered by the store checkpoint.
- Data-sync scan references are released on their source shard after all
  consumers finish and before a scan request is reset or retained across waits.

## Source map

| Claim | Repository source |
|---|---|
| Transaction commit writes term-fenced WAL records | `tx_service/include/tx_operation.h`; `tx_service/src/tx_operation.cpp`; `tx_service/src/tx_execution.cpp`; `tx_service/tx-log-protos/log.proto` |
| The tx-side log abstraction provides write, checkpoint, replay, and transaction-recovery operations | `tx_service/include/txlog.h`; `tx_service/tx-log-protos/log_agent.h`; `tx_service/tx-log-protos/log_agent.cpp` |
| Checkpoint timestamps are derived and advanced by the checkpointer | `tx_service/include/checkpointer.h`; `tx_service/src/checkpointer.cpp` |
| Checkpoint triggers distinguish periodic, shutdown, memory-pressure, dirty-memory, and explicit requests | `tx_service/include/checkpointer.h`; `tx_service/src/checkpointer.cpp`; `tx_service/include/cc/cc_shard.h`; `tx_service/src/cc/cc_shard.cpp`; `tx_service/src/cc/cc_req_misc.cpp` |
| Dirty entries flow through data-sync tasks and the store durability barrier | `tx_service/include/data_sync_task.h`; `tx_service/src/data_sync_task.cpp`; `tx_service/src/cc/local_cc_shards.cpp`; `tx_service/include/store/data_store_handler.h` |
| Range data-sync scan references are released on their source shard after consumption | `tx_service/include/cc/cc_request.h`; `tx_service/include/cc/local_cc_shards.h`; `tx_service/src/cc/local_cc_shards.cpp` |
| Leader recovery replays all log groups under a candidate term before service | `tx_service/include/fault/cc_node.h`; `tx_service/src/fault/cc_node.cpp`; `tx_service/include/fault/log_replay_service.h`; `tx_service/src/fault/log_replay_service.cpp` |
| Replay restores metadata dependencies and resumes incomplete durable operations | `tx_service/include/cc/catalog_cc_map.h`; `tx_service/include/cc/range_cc_map.h`; `tx_service/include/cc/cluster_config_cc_map.h`; `tx_service/include/cc/range_bucket_cc_map.h` |
| MVCC versions are archived, fetched, and reclaimed against active snapshots | `tx_service/include/cc/cc_entry.h`; `tx_service/include/tx_start_ts_collector.h`; `tx_service/include/store/data_store_handler.h` |
| Standby and backup snapshots are coordinated above the storage interface | `tx_service/include/store/snapshot_manager.h`; `tx_service/src/store/snapshot_manager.cpp`; `tx_service/include/store/data_store_handler.h` |
| The checkpoint counts each transaction's latest data or catalog write-lock grant | `tx_service/include/cc/cc_shard.h`; `tx_service/src/cc/cc_shard.cpp`; `tx_service/tests/CkptLockRegistry-Test.cpp` |
| Forwarded keys are logged to every NG they are written to | `tx_service/src/tx_execution.cpp` |
| Replay is requested once per log group and again only for a silent group | `tx_service/src/sharder.cpp`; `tx_service/src/fault/log_replay_service.cpp`; `tx_service/tx-log-protos/log_agent.cpp` |
| Recovered schema operations restore intents or write locks by role, and the gate is reinstalled | `tx_service/include/cc/catalog_cc_map.h`; `tx_service/include/cc/cluster_config_cc_map.h`; `tx_service/src/tx_execution.cpp`; `docs/range-split-schema-op-recovery.md` |
| Checkpoint publication, split ownership, state and MVCC behavior have focused unit coverage | `tx_service/tests/CheckpointFlush-Test.cpp`; `tx_service/tests/RangeSplitCheckpoint-Test.cpp`; `tx_service/tests/CheckpointMetricsState-Test.cpp`; `tx_service/tests/CcEntry-Test.cpp`; `tx_service/tests/StartTsCollector-Test.cpp` |
