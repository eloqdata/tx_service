# Recovering a range split together with a schema op (deferred)

**Status: open, deferred.** Found while reviewing the recovery of schema ops in the EloqDoc CDC
project (PR 7, 2026-09-29). Nothing described under "Target design" is implemented. The current
behavior works, but it weakens a recovered schema op's catalog lock to keep recovery from waiting
on a range split.

Key files: `tx_service/src/fault/log_replay_service.cpp` (replay order, barriers),
`tx_service/include/cc/catalog_cc_map.h` (`Execute(ReplayLogCc&)`, the schema record's lock),
`tx_service/include/cc/range_cc_map.h` (`Execute(ReplayLogCc&)`, the split record's range state and
lock), `tx_service/src/cc/local_cc_shards.cpp` (`CreateSplitRangeRecoveryTx`, the data-sync task
limiter), `tx_service/include/cc/cc_request.h` (`ReplayLogCc::RangeSplitting`).

## 1. Why the two can be in flight together

- A range split runs inside its range's data sync. The split's tx holds the table's catalog read
  lock for the split's whole duration (`local_cc_shards.cpp`, "We're still holding catalog read
  lock here"). Its log has three stages (`SplitRangeOpMessage.Stage`): `PrepareSplit`,
  `CommitSplit`, `CleanSplit`; the clean log removes the op from the log state, so replay sees only
  the first two.
- A schema op holds the catalog write lock in one section (`UpsertTableOp`: from its write locks,
  just before its commit log, to `PostCommit`) or two (`UpsertTableIndexOp`: before its prepare
  log until `PrepareCommit` downgrades to an intent, and again before its commit log until
  `PostCommit`).
- A write lock excludes a read lock, so in normal execution a split never overlaps those sections.
  It runs while the schema op holds only a write intent, which a read lock does not conflict with
  (for example between an `UpsertTableOp`'s prepare log and its write locks), or after the op's
  `PostCommit`, while its commit record still waits for the clean log.
- A crash can therefore leave, for the same table, a schema op's record (prepare or commit stage)
  and a split's record (prepare or commit stage) in the log state. Both are recovered.

## 2. Current recovery

Replay of one log stream (`log_replay_service.cpp`):

1. **Collect split tables** (barrier #1). From the first replay message, read only the table name
   of each split record and gather the base tables across all streams. Nothing is replayed.
2. **Replay schema records**, one at a time, to the catalog map. Each `ReplayLogCc` carries a
   pointer to the set from step 1 (`ReplayLogCc::RangeSplitting`).
3. **Replay split records** to the range map.
4. **Barrier #2**: data-log replay waits until every stream has replayed its schema and split
   records.

A schema record restores the catalog lock by stage (`catalog_cc_map.h`):

| Stage | Node group | Lock restored |
|---|---|---|
| `PrepareSchema` | every node group | write lock; **write intent if the table is in the split set**, or for add index |
| `PrepareData` | every node group | write intent |
| `CommitSchema` | coordinator | write lock; **write intent if the table is in the split set** |
| `CommitSchema` | participant | none |

A split record restores (`range_cc_map.h`):

| Stage | Node group | State and locks |
|---|---|---|
| `PrepareSplit` | coordinator | write lock on the old range's cc entry; the recovery tx installs the dirty range |
| `PrepareSplit` | participant | dirty range info; write lock on the old range's cc entry |
| `CommitSplit` | coordinator | current and dirty range info, slice specs from the log; write lock on the old range's cc entry |
| `CommitSplit` | participant | dirty range info promoted to current, new range records; no lock |

On the split's coordinator, `CreateSplitRangeRecoveryTx` then takes blocking read locks on the
cluster config, the table's catalog entry and the range's bucket, registers the range's data-sync
task limiter, and only then finishes the replay request (`SetFinish()`), after which it resumes the
split.

**The coupling.** Recovery counts as finished only once every recovered split holds its catalog
read lock. A schema op's write lock on the same table would hold that up until the recovered
schema op releases it at `PostCommit`, possibly after retrying a failing data store step for as
long as the store is down. To avoid the stall, the schema record restores only a write intent when
its table is in the split set.

## 3. The flaw

The schema op's recovered lock is weakened so that a split can finish its part of recovery, though
in normal execution a split never overlaps a schema op's write-lock section. The weakening follows
only from where "recovery finished" is drawn, and it costs:

- **A weaker commit-stage lock.** The coordinator's lock after a commit log exists so that no newer
  DDL runs before the recovered op's steps after the commit log, data store writes among them. An
  intent still keeps a newer DDL out, but it lets DML reach the table on the coordinator's node
  group while those steps run.
- **Recovered state that depends on an unrelated op.** Whether a schema op comes back with a lock
  or an intent depends on whether some split happened to be in flight on the table.
- **Global over-reach.** The split set is global: a split whose coordinator is another node group,
  whose catalog read lock this node never re-takes, still weakens the lock here.

## 4. Target design

- **Schema records restore the lock their stage implies.** `PrepareSchema`: a write lock (an intent
  for add index, for add index's own reason: between its prepare log and its prepare index log it
  holds only an intent, and its build takes range read locks). `CommitSchema`: the coordinator's
  write lock. No split-set check.
- **A recovered split finishes its part of recovery once it holds what recovery must guarantee**:
  its range write locks, which the range map restores, and its data-sync task limiter entry. The
  limiter is a per-range marker (node group, term, table, range id): while it exists, a new data
  sync of the range queues behind the running one. Checkpoints start only after recovery, so the
  entry must exist before `SetFinish()`, as it does today. `CreateSplitRangeRecoveryTx` calls
  `SetFinish()` then and takes the catalog read lock afterwards, waiting behind a recovered schema
  op's write lock until that op's `PostCommit`. The cluster config and bucket read locks do not
  conflict with a schema op and may stay before `SetFinish()`.
- **The order is the commit-timestamp order.** A schema op recovered from its prepare log
  re-derives its commit-stage timestamp above every logged commit: the recovering node's clock
  starts at the log's latest commit timestamp (`Sharder::FinishLogReplay`), and `AcquireAllCc`
  reports at least the grant's local time. So a split in flight is ordered before the schema op,
  though it resumes after it. Effects ordered by timestamp need nothing; effects that are not must
  still follow the timestamp order:
  - **After a DROP**, the split's table is gone. The drop deleted the table's range records with a
    prefix delete that carries no timestamp (`DataStoreServiceClient::DeleteTableRanges`), and its
    `PostCommit` dropped the range cc map holding the split's restored range lock. The resumed
    split must give up: release its range lock, write its clean log (from `PrepareSplit` or
    `CommitSplit`), write nothing to the store, and pop its pending data-sync task.
  - **After an ALTER or an index op**, the split resumes under the new schema. Its recovery tx
    already picks the dirty or the current schema when it reads the catalog.
  - **CREATE and TRUNCATE** cannot have a split in flight: a new table has none yet, and a truncate
    runs only on EloqKV tables, which are hash partitioned.
  - **A schema op recovered from its commit log** has a timestamp below the split's, which started
    after the op's `PostCommit`. The op's recovered steps only redo work finished before the split
    began.

## 5. Constraints to keep

- A recovered schema op holding the catalog write lock must never wait on a range lock: the split
  holds its range write lock while it waits for the catalog. This holds today. `UpsertTableOp`
  takes no range locks; add index takes range read locks only during its build, under an intent.
- The limiter entry is registered before `SetFinish()`.
- Failures after `SetFinish()` need their own path. Today a failed read of the recovery tx's locks
  (`lock_meta_failed`, for example a lost leadership) aborts the replay request; after
  `SetFinish()` there is no replay request to abort, so the recovery tx must abort itself and leave
  the split to the next leader.

## 6. To-do

1. `CreateSplitRangeRecoveryTx`: move `SetFinish()` ahead of the catalog read lock, after the
   limiter registration.
2. The split's "table dropped" path: on a `Deleted` catalog record, release the range lock, write
   the clean log, pop the pending data-sync task.
3. The recovery tx's failure path after `SetFinish()` (section 5).
4. `catalog_cc_map.h`, `Execute(ReplayLogCc&)`: drop `req.RangeSplitting(...)` from the
   `PrepareSchema` and `CommitSchema` cases.
5. `log_replay_service.cpp`: remove barrier #1's collection of split tables and the
   `range_splitting_` plumbing into `ReplayLogCc` (`RangeSplitting`, the pointer passed with each
   schema record); it has no other consumer.
6. Tests, for a schema op and a split on the same table recovered together:
   - a DROP recovered from its prepare log with a split in `PrepareSplit`, and in `CommitSplit`:
     the split gives up, no range records of the dropped table remain, and the name can be reused;
   - an ALTER and an add index with a split in flight: the split resumes under the new schema;
   - a schema op recovered from its commit log with a split started after its `PostCommit`.

   These need a fault point that crashes a node during a split, and a way to force a split under
   the schema op (a small range size).
7. Docs: `07-durability-and-recovery.md` (recovery), `08-range-and-bucket-management.md` (range
   split).
