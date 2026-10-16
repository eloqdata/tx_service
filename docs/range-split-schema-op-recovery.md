# The catalog lock a recovered schema op restores, and range splits

**Status: resolved 2026-10-09**; section 6.2 resolved 2026-10-15. Found while reviewing
schema-op recovery in the EloqDoc CDC project (PR 7, 2026-09-29). Section 6.1 records a data loss
in range splits that the tests found, fixed 2026-10-10. Section 7 records the target design this
replaces.

Key files: `tx_service/include/cc/catalog_cc_map.h` (`Execute(ReplayLogCc&)`, the schema record's
lock), `tx_service/src/fault/log_replay_service.cpp` (barrier #1, the split set),
`tx_service/include/cc/range_cc_map.h` (`Execute(ReplayLogCc&)`, a split's recovery),
`tx_service/src/cc/local_cc_shards.cpp` (the data sync's catalog read lock, `SplitFlushRange`),
`tx_service/src/tx_operation.cpp` (`AcquireAllOp::Forward`, the liveness check of a blocked
request).

## 1. Coordinator and participant

Every node group holds a copy of a table's catalog entry, and a schema op locks the copy on every
node group. When a node group recovers, its replay of the op's log record restores a lock on its
copy, and what it must restore differs by role:

- **The coordinator's node group** (the node group of the op's tx,
  `(txn >> 32) >> 10 == ng_id`, and this node is its candidate leader) also recovers the op itself
  (`CreateSchemaRecoveryTx`). The recovered op redoes every step after the log record it was
  recovered from, including the write locks it takes before its commit log, and that step drains
  the DML admitted in the meantime. So the coordinator only needs to keep a newer DDL off the table
  until then: a write intent.
- **A participant** recovers while the coordinator lives on elsewhere, and cannot tell how far the
  coordinator got. The coordinator may already hold this node group's write lock, the last step
  before its commit log, and it does not ask for it again. So a participant restores the lock the
  coordinator may rely on. After a prepare-stage record, that is the write lock, the state right
  before the commit log: without it, DML admitted on the recovered node group could commit above
  the op's commit timestamp under the old schema, breaking the barrier the commit log draws. After a
  commit-stage record, it restores the committed schema and no lock. The coordinator's later post
  writes find no lock of the tx there and finish at once.

| Record | Coordinator's node group | Participant |
|---|---|---|
| `PrepareSchema` | write intent; the op resumes after its prepare log | write lock, with the two exceptions in section 2 |
| `PrepareData` (add index) | write intent; the op resumes after its prepare data log | after the build's last record, the write lock, with the split exception of section 2; before it, a write intent (section 6.2) |
| `CommitSchema` | write intent; the op resumes after its commit log and releases the participants | the committed schema, no lock |

The restored lock's holder carries term 0: after the prepare log the lock and the op are recovered
whatever fails, so no term marks the holder as failed.

## 2. When a participant can restore only an intent

A participant restores a write intent instead of the write lock exactly when the coordinator cannot
hold this node group's write lock:

1. **An add index's `PrepareSchema` record.** Between its prepare log and its first prepare data log
   the op holds only an intent: its write lock spans the prepare log, and the step right after
   (`PrepareCommit`, `downgrade_all_lock_to_intent_op_`) turns it into an intent. Replay rebuilds
   the state after that step (the dirty schema at the op's timestamp), and the downgrade, when it
   comes, finds an intent and changes nothing.
2. **A split of the table that this node group coordinates is in flight.** The data sync that runs
   a split reads the table's catalog with a local read lock (`read_local`), on its own node group
   only. It takes the lock before the split's first log, runs the split inside the same tx
   (`SplitFlushRange`, `split_txm` is the data sync's tx) through its clean log, and commits only
   after it. So while the split's record is in the log state, the read lock is held, and on
   recovery the split's recovery tx (`CreateSplitRangeRecoveryTx`) takes it again before replay
   finishes. A read lock excludes every write lock, so the coordinator holds no write lock on this
   node group, neither a prepare-stage one nor its commit-phase one. Restoring the write lock would
   both overstate the state and stall recovery: replay finishes only once the recovered split holds
   its catalog read lock, which the write lock would hold up until the op's `PostCommit`.

A split coordinated by another node group locks nothing on this one and proves nothing about it.
Barrier #1 therefore collects, from every log group's first replay message, only the splits whose
tx belongs to the recovering node group, under the condition `range_cc_map.h` uses to recover a
split there.

This holds for every writer of a prepare-stage schema record:

| Writer | Write lock across its prepare record | Write lock again before its commit log |
|---|---|---|
| `UpsertTableOp` (create, drop, truncate, alter) | no, intents | yes, at the commit-stage timestamp |
| `UpsertTableIndexOp` (add, drop index) | yes, until `PrepareCommit` downgrades it | yes (drop index acquires; add index after its last prepare data log) |
| A DML that updates a catalog (`CatalogAcquireAllOp`, `FlushUpdateTableOp`; EloqDoc's insert that makes an index multikey) | yes, across the data log that carries the record, until `PrepareCommit` downgrades it | no, intents until its commit log |

Whatever the writer, a split's read lock on a node group means the op is outside every write-lock
section there. Create and truncate cannot have a split in flight: a new table has no ranges to
split, and a truncate runs only on EloqKV tables, which are hash partitioned.

## 3. The coordinator's lost request

When a participant recovers with a split of its own in flight, the coordinator's write lock
request to it was waiting behind the split's read lock, and it is lost with the old leader. The
coordinator finds this and sends the request again:

1. Under 2PL a participant that cannot grant a lock at once acknowledges the request, with the
   blocked entry's address and its term. Once the request stays unfinished past the tx's timeout
   (`TransactionExecution::IsTimeOut`, 10 s), `AcquireAllOp::Forward` sends
   `BlockAcquireAllCcReqCheck` to the node group. The new leader compares the request's term with
   its own and answers `ErrorTerm`, which fails the request with `NG_TERM_CHANGED`. A request no
   longer queued is answered `REQUEST_LOST`; a check unanswered for 10 s, or a request never
   acknowledged, fails the same way. A request still waiting legitimately is answered `BlockQueue`
   and keeps waiting.
2. The failed step is retried while the coordinator leads: `UpsertTableOp::AcquireWriteLocks`, and
   the index op's commit-stage `acquire_all_lock_op_`, both because a schema op past its prepare
   log can only roll forward.
3. The new request reaches the participant after its recovery has finished, behind the recovered
   split's read lock and the read locks of the DML admitted since. The grant waits for them, and
   its timestamps put the commit timestamp above them: the barrier holds.

The same mechanism serves a participant that restored the write lock: the request sent again finds
the tx's own lock.

## 4. What this replaced

Before, a `PrepareSchema` record restored a write lock on every node group, the coordinator's
included, and an intent on any node group when the table had a split in flight anywhere in the
cluster (barrier #1 collected every split record of every log group). Two consequences:

- **A hole on participants.** With a split in flight on node group M, the coordinator's write lock
  can be granted on another node group N while its request waits on M. If N then recovered, it
  restored only an intent, although the coordinator held, and relied on, N's write lock. DML
  admitted on N could commit above the op's commit timestamp under the old schema. For EloqDoc's
  change data capture that is a write event after its collection's drop or alter.
- **A stall on the coordinator's node group.** Its write lock held up the recovery of a split on the
  same table there until the recovered op's `PostCommit`, possibly for as long as a failing data
  store step kept retrying.

## 5. Tests

- `cmake/tests/ddl_recovery.py` (viii) in EloqDoc: an ALTER killed after its prepare log, on a single
  node (the coordinator's node group), is rolled forward, and the table's documents written before
  the crash are replayed.
- `cmake/tests/multinode/ddl_split_recovery.py` in EloqDoc, on a cluster of three node groups run as
  separate processes (`cluster.py`; a build with fault points and the EloqStore data store service,
  first run with RocksDB). A checkpoint splits the loaded collection's range on node group X and a fault point
  (`split_flush_range_install_dirty_continue`) holds the split after its prepare log; an ALTER from
  Y waits for X's write lock; Z is the third node group:
  - (i) X recovers: replay finishes, the coordinator's lost request fails and is sent again, and the
    ALTER commits once the recovered split finishes;
  - (ii) Z, which granted its write lock, recovers: it restores the write lock, so it serves nothing
    until the ALTER commits (its start-up reads every collection's catalog, EloqDoc's
    `KVCatalog::init`). Built without this change, Z restored an intent and served at once at the
    old catalog version: the test fails there;
  - (iii) Y recovers: the recovered ALTER takes its write locks again, Y's own included, and commits
    once the split is released;
  - (iv) X coordinates the ALTER itself and recovers: the recovered split and the ALTER both finish.

  A node whose catalog entry a recovered schema op write-locks finishes its start-up only once the
  op commits, as reads of the table through it would wait without the crash.

  Controls without a schema op test the split itself, every document checked after each: (c0) a
  split; (c1) a split held and released; (c2) a split held while the new range's owner-to-be
  restarts; (c3) a split, then a restart of the new range's owner at once; (c4) the same with a
  clean restart of the whole cluster; (c5) the same after 60 s of checkpoints; (c6), (c7), (c8) 1,000
  documents of the new range written during a held split, 250, 1 and 10 per transaction, then a
  restart of the new range's owner at once.

  The controls exposed a separate, older defect, section 6.1: (c2), (c3), (c4), (c6) and (c8) lost
  documents, and (ii)'s document check failed whenever Z owned the new range. With its fix, all of
  (c0)–(c8) and (i)–(iv) pass.

## 6. Related findings

### 6.1 Forwarded writes logged once per transaction (fixed 2026-10-10)

A split's new range lost records when its owner restarted before checkpointing them: during the
split (c2), right after it (c3), or in a clean restart of the whole cluster (c4). Schema ops play no
part. The records lost were all writes committed during the split, after it installed its dirty
range; the old data reaches the new owner safely.

- **Old data** (committed at or below the split's data sync timestamp) is flushed by the split's
  own data sync into the new range's partition, on the new owner's data store shard, before the
  split's commit log. The records the old owner then uploads to the new owner are installed as
  already persisted.
- **Writes during the split** are double written: once the dirty range is installed, a write to
  the new range goes to the old owner and to the new owner (`WriteSetEntry::forward_addr_`), and
  the tx's data log record carries the key in both node groups' blobs. The split's data sync does
  not cover them: until the new owner checkpoints them, its blob in the log is their only durable
  copy, which its recovery replays.

`TransactionExecution::FillDataLogRequest` put a forwarded key into the new owner's blob only if
that node group had no entry yet (`if (!inserted) continue;`, commit a64a31d, #454, 2026-03-16), so
it logged the tx's first such key per node group and dropped the rest. A key forwarded to several
cores of one node group needs logging there only once; the check applied that to the whole
transaction instead of to the key. In a 320 MB
load in batches of 500, the split started during the load, and every document of the new range
written after its dirty range install was lost except the first of each batch. Single-key
transactions lost nothing (c7). 1,000 documents in transactions of 250 kept 4 (c6), and in
transactions of 10 kept 100 (c8). Waiting 60 s of checkpoints before the restart lost nothing (c5).

The fix logs every forwarded key into the new owner's blob, once per node group, skipping only the
key's own node group and a node group it is already logged to. A bucket migration's double writes
take the same path (`LockWriteRangeBucketsOp::Advance` sets `forward_addr_` for a hash-partitioned
key whose bucket migrates and for a range whose bucket migrates), so their transactions were logged
the same way; no test here runs a migration. Object commands (`CmdSetEntry::forward_entry_`) were
always logged per entry. With the fix, every control and scenario of section 5 passes on the
EloqStore build, and so do `ddl_recovery.py` and `cluster_gate.py` on a single node.

### 6.2 Add index's prepare data records (resolved 2026-10-15)

Add index writes a `PrepareData` record after each batch of its build, under intents, and after
the last one takes its commit-stage write locks. The record carries the build's last finished key,
and the last record's is positive infinity (`last_key_type = PosInfKey`). The log state keeps the
newest record's key (a `PrepareData` record for an op already at that stage replaces the key), so a
recovering node group replays it.

A participant therefore restores:

- **After the last record** (`PosInfKey`): the write lock. The build is over, and the coordinator,
  about to take or already holding its commit-stage write lock, does not ask for it again. The
  split exception of section 2 still applies: with a split that this node group coordinates in
  flight, a write intent, since that split's catalog read lock excludes the coordinator's write
  lock here.
- **Before it**: a write intent, since the build still runs under intents. A write lock then would
  block DML on the node group until the index commits, and could block the build's own reads.

The coordinator's node group restores a write intent in both cases: the recovered op resumes from
the record and, at the last one, takes the commit-stage write locks itself. No new record is
needed; the last key already marks the end of the build.

## 7. The target design this replaces

The earlier version of this document kept the write lock on every node group and proposed letting a
recovered split finish its part of recovery before taking its catalog read lock
(`CreateSplitRangeRecoveryTx` calling `SetFinish()` first), so that the split resumes after the
schema op and gives up when the op was a drop. With the roles of section 1 that is not needed: the
coordinator restores an intent, and a participant whose own split is in flight restores one
because the coordinator cannot hold its write lock, so no recovered split waits on a recovered
schema op.
