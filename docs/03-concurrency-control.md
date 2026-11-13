# Concurrency Control

The concurrency-control (CC) module owns the node's resident transactional
state. It partitions data across `CcShard`s, represents each table and node
group with a `CcMap`, and serializes mutation of each shard by executing
`CcRequestBase` messages on the shard's processing context. Transactional
conflicts are represented by entry locks and wait queues, not by blocking the
shard thread.

[02-threading-model.md](02-threading-model.md) defines who may execute shard
work. [04-transaction-execution.md](04-transaction-execution.md) defines the
transaction state machines that issue CC requests.

## Ownership and module boundaries

`LocalCcShards` owns all local shards and the metadata shared across them,
including node-level catalog objects, table-range state, clocks, and background
workers. A `CcShard` owns the transaction table, resident CC maps, request
queues, transactional lock registry, cache-replacement state, and its
allocation heaps for one core.

A `CcMap` is the table-level polymorphic boundary. Typed map implementations
own ordered pages of keys and `CcEntry` values and implement point access,
scans, commit post-processing, recovery, and metadata operations for their
record model. Specialized maps provide object-command semantics and replicated
catalog, range, bucket, and cluster-configuration state. The concrete key,
record, and map types come from the engine's `CatalogFactory`; see
[05-data-model-and-catalog.md](05-data-model-and-catalog.md).

## Request lifecycle

Producers enqueue a CC request to the shard that owns the target key or
metadata. `CcShard::ProcessRequests()` calls `Execute()` serially in shard
context. The return value is an ownership decision: a completed pool-backed
request may be recycled; a request retained by a lock queue, fetch, memory wait
list, or batched continuation must remain alive until that owner re-enqueues or
aborts it.

Table-targeted requests validate the requested node-group term before touching
state and, for data paths carrying a schema version, reject a map whose schema
version differs. A request that cannot complete synchronously follows one of
three stable patterns:

- a lock conflict either fails immediately under optimistic write control or
  transfers the request to the entry's blocking queue;
- a cache or metadata miss starts an asynchronous store read and transfers the
  requester to the fetch operation until backfill completes;
- memory pressure transfers the request to a shard wait list while eviction or
  checkpoint progress makes admission possible. Each request type says how
  long it may wait (`MemoryWaitPolicy()`): a fail-fast request, such as one
  of a transaction holding a range read lock, is rejected with an
  out-of-memory failure at the first pass that finds the heap full or frees
  nothing; any request but log replay and a write forwarded to a standby is
  rejected once it has stayed parked across a configured number of concluded
  checkpoint cycles.

In all cases, waiting occurs outside `Execute()`. A resumed request continues
with the lock, fetch result, or admission opportunity granted by the component
that re-enqueued it.

Pool-backed request allocators may impose a finite reuse limit, so their
callers must handle exhaustion. Record fetches use temporary overflow requests
rather than rejecting burst misses. The shard's active-fetch map owns pooled
and overflow requests through backfill and reopen retries, and coalesces
concurrent misses for the same record. Before a pooled fetch is made reusable,
it releases retained values, archives, keys, session state, and waiter storage.

## Entry locks and isolation

`NonBlockingLock` is a transactional lock attached lazily to a resident entry.
It distinguishes read intents, read locks, write intents, and write locks:

- A read intent pins the entry and supports optimistic validation; it does not
  conflict with writers.
- A read lock prevents a conflicting write lock and is retained when the
  isolation/protocol combination requires pessimistic reads.
- A write intent reserves a future write without excluding read locks. The
  holder upgrades to a write lock during commit.
- A write lock excludes other writers and read locks while the committed value
  is installed or rolled back.

`LockTypeUtil` derives the requested mode from the operation, isolation level,
and CC protocol. Read-committed and snapshot reads normally avoid read locks;
repeatable-read and serializable reads use either read locks or read intents.
Write conflicts fail immediately under `OCC`; `OccRead` and `Locking` enqueue
the request. Queue admission prevents new readers from indefinitely delaying a
pending write-lock acquisition, and an intent holder's own upgrade is ordered
ahead of requests that cannot proceed until that intent is released.

On release, the lock grants eligible queue heads before re-enqueueing them, so
a resumed request must use the resumed-request path instead of attempting the
same acquisition again.

## Read, write, and scan flow

A read locates or creates the entry, acquires the derived lock or intent, and
returns a resident committed version. If the value is not resident, the entry
and its intent anchor the asynchronous store fetch; backfill updates the same
entry before the read resumes. Snapshot reads may select an archived version,
with durable version retrieval delegated to the store boundary.

Writes are buffered by the transaction until commit. Commit-time acquisition
takes the required write locks and returns version/timestamp bounds: each grant
reports the granting shard's clock above the reads it has served
(`last_vali_ts`), and the commit timestamp is chosen above every bound. A lock
taken on every core of every NG (`AcquireAllCc`, used by schema, range, bucket
and cluster gate operations) reports the later of the shard's last read
timestamp and its clock, so a schema operation's timestamps lie above its
catalog write lock's grant. A catalog reader publishes its commit timestamp on
release, including EloqKV's fast catalog read (`ReleaseCatalogRead`), so a later
schema operation commits above it.
Post-read requests validate optimistic reads and release read ownership;
post-write requests install the committed value or discard the pending value,
then release the write lock and wake waiters. Object-command maps use the same
lock lifecycle but apply and commit engine commands against shard-owned
objects.

A snapshot-isolation transaction starts at the later of its node's timestamp
base and the wall clock, and a newer wall clock advances the base: commits take
timestamps above their own node's base, so a start taken from another node's
base alone could miss writes that node had acknowledged. Across hosts, a
read-only transaction's start can trail another host's commits by their clock
skew, which is accepted; a write transaction spanning nodes is ordered by the
read timestamps every participant records.

Scanners are batched cursors over memory and, when needed, persistent data.
They retain the intents or locks needed to keep continuation points valid and
transfer semantic reads to the transaction read set until final validation or
abort. A client-visible batch boundary is not a transaction ownership
boundary.

## Resident-state lifecycle

CC pages and entries live on the owning shard's heap. Pages provide ordered
navigation for point and range access and also participate in shard-local cache
replacement. Entry addresses used across asynchronous steps are valid only
while a lock, intent, or explicit pin preserves the associated indirection;
page split, merge, compaction, and eviction may otherwise move or destroy the
entry.

Only state that is no longer transactionally owned and is already persistent
may be evicted. Dirty entries remain resident until checkpointing advances
their durable state. Cache misses reconstruct resident state through the store
handler, while WAL replay reconstructs it during recovery. Those durability
boundaries are described in
[07-durability-and-recovery.md](07-durability-and-recovery.md).

Shard memory has one ledger beside the allocator's figures: bytes reserved for
payloads that commit-time write-lock acquisitions will install later. A record
acquisition (`AcquireCc`) carries the coordinator's estimate of the value it
will install in post-processing (the record's `MemUsage()`, zero for a delete)
and charges it to the destination shard's heap before it attempts the write
lock, after it has located or created the entry. Reserved bytes count as
allocated in every admission decision, the heap's fullness check and the
cleaner's targets included, so a reservation and an allocation are refused by
the same test. The charge has exactly one owner at a time: the pending request
while it waits in a lock queue, then the granted write lock, to which it moves
without touching the ledger. An acquisition that finds its own transaction's
write lock on the entry takes no charge, since the lock already carries one. A
terminal failure of the request, wherever it is published, returns the charge;
releasing or resetting the write lock returns the lock's. A charge that does
not fit parks the request like any other memory shortage, without publishing
the entry pointer; one larger than the whole shard limit fails at once.

Entry admission and payload admission are separate checks. Entry lookup
(`FindEmplace()`) returns an entry already resident whatever the heap's state,
since finding it allocates nothing, and refuses only a miss on a full heap. An
acquisition then admits its payload by the reservation, so a write to a
resident key proceeds to its reservation on a full heap. A write that carries
no reservation (an object command, log replay, a batch upload, a standby
forward) asks the heap's fullness check itself before it installs into its
entry, resident or just created (`UnreservedWriteMustWait()`), and on a full
heap waits or fails as it would for a miss. Orphan lock recovery, which replays a committed
transaction's log to install its value and release a write lock its
coordinator left behind, is the exception: it only looks the key up, and once
it finds the transaction still holding the lock it installs the value on a
full heap without waiting, because the orphaned lock holds back the checkpoint
that would free memory. The meta tables and the sequence table, which entry
admission exempts from the limit because range split and the checkpointer
depend on them, are exempt from both payload checks as well; their
acquisitions reserve nothing.

A value loaded from the store is admitted where it is installed. A point read
that finds its key's value, or the historical version a snapshot needs, missing
from memory issues a store fetch and waits on it; when the store answers, the
fetch checks whether the heap is full before it installs anything, as entry
admission does; the size of what it fetched is not weighed. At that point the
waiting reads hold no lock on the key and the record is still in the store, so
the fetch can wait or be dropped safely: on a full heap it parks like any other
request, and waits no longer than the least patient read waiting on it. A
rejection applies to each waiting read by its own policy: a fail-fast read
fails with an out-of-memory error at once, a bounded read once the fetch has
waited out the checkpoint-cycle bound. The fetch keeps its record for whoever
is left, so a read that may still wait does not cause a second store read, and
log replay, standby forwarding and command upload, which buffer their commands
on the entry and wait on nothing, never lose a fetch to a read that shared it;
the record is dropped only when no one is left. A fetch that installs no value
(a missing or deleted key), a fetch that only such issuers share, and fetches
on the exempt tables do not wait. A slice loaded for a scan
is admitted the same way, batch by batch, and always fails fast because a slice
being loaded blocks a range split. A transactional write is the one install
that cannot be admitted where it happens: by then the transaction has
committed and holds the write lock that keeps the checkpoint from advancing, so
its admission is the reservation taken before the lock.

A request that finds the shard heap at its limit parks on the shard's wait
list; the park is counted, and the shard's single cleaner is scheduled if it
is idle. A cleaner that is already sweeping or deferred is left alone. A pass
starts at the LRU head and evicts in bounded batches (10 pages or 30 us, so the
shard's thread is not held up) while allocated memory is above the eviction
target (5% of the limit free), or while a request has parked since the pass
started or last released, since that request's memory did not fit however much
room the target leaves. After each batch it releases parked requests, 20 at a
time, while the heap is below the hard limit. A request that has parked since
the pass started or last released, whichever request it was, found the heap at
its limit, so the room the last release handed out is used up, and releasing
again at once would only hand the waiters the same full heap; the park count
shows whether any request has parked since then. In that case the pass keeps
evicting and releases again only once it has reclaimed `shard_clean_release_kb`
since the last release (a tx service gflag, default 256 KB, zero meaning any
amount, also a `[local]` key of the data substrate config), so the waiters
follow the pace of eviction. At the tail eviction can reclaim no more, so any
amount reclaimed since the last release is enough; with none, nothing is
released and the pass ends. A pass that reaches the tail with requests still
parked aborts fail-fast requests when the heap is full or the pass freed
nothing, and, whatever the heap's state, any request parked across
`shard_memory_wait_ckpt_cycles` (default 5) concluded checkpoint
cycles, unless it is a log replay request or a write forwarded to a standby,
which must be applied and so wait without bound. A request is stamped with the
node's concluded-cycle count when it first parks in an attempt, and the stamp
survives release and re-park, so a request that cannot fit below the limit
still ages; taking a pooled request into use again clears it. A cycle is one
checkpointer run over every node group the node hosts, concluded when each
group has flushed and advanced its timestamp, found
nothing to flush, been skipped because a write lock pins its timestamp, or
failed; a flush still running keeps the cycle open ([07](07-durability-and-recovery.md)).
Counting cycles rather than time gives a flush that many chances, on the shard
heap every hosted group shares, before a request is given up on, and it is the
only exit for a transaction that is parked while holding the lock or the
reservation another parked transaction needs.

A pass that ends asks for a checkpoint when it reached the tail with requests
still parked, however much it reclaimed, since eviction could not make the room
they need, or when it reclaimed less than `shard_clean_release_kb` over the
whole pass (not since the last release), since the sweep then found almost
nothing evictable and only a flush turns the dirty entries left into evictable
ones. It does not ask while the shard's heap is being defragmented or a data
sync operation is running on the node, nor when a flush or a range split
completed on the shard during the pass: the pass then runs once more at once,
to reach entries that became evictable behind its cursor, and that pass
decides. A pass that still has requests parked defers; the timer thread
retries it after 100 ms, doubling the delay up to 1.6 s while passes reclaim
less than `shard_clean_release_kb`, since sweeping such a chain again soon
finds no more. The backoff clears when a pass ends with nothing parked, so the
next episode of pressure starts from 100 ms again. A flush or a committed range
split runs a deferred cleaner at once and marks a running pass to run once
more; nothing rewinds a pass in progress.

## Deadlock and orphan-lock coordination

Each shard maintains a registry from transactions to the entry locks and
intents they hold. The registry is part of the correctness model: checkpointing
uses it to derive active-transaction bounds, failover recovery uses it to find
orphaned ownership, and deadlock detection uses it to build the wait-for graph.

For each transaction the registry records its latest write-lock grant and
whether it holds a write lock that keeps the checkpoint below that grant. Write
locks on data tables and catalog entries count; range, bucket and
cluster-configuration write locks, intents and read locks do not, since split
and migration operations hold them for long stretches and nothing needs them
ordered against the checkpoint. The decision is made per lock when it is
registered, as one transaction's locks span many tables. A downgrade stops the
lock from counting unless another counting lock of the transaction remains in
the shard; the commit-stage upgrade counts again from the upgrade time.

DML chooses its commit timestamp after its last write lock, so the latest grant
is also the tightest lower bound on its commit. Schema operations rely on the
exclusive section instead: index operations choose their commit timestamp at
prepare under the write lock, and create, drop and truncate choose it again
under their commit-stage write lock, above every DML that lock drained.

The deadlock service gathers local shard wait relationships and corresponding
relationships from other node-group leaders, detects cycles, and asks the
owner shard to abort a victim. Victim abort and lock release re-enter the normal
CC request and post-processing paths; they do not mutate remote shard state
directly.

## Invariants

- `Execute()` never sleeps or waits. Incomplete requests must transfer their
  lifetime to a queue or asynchronous owner before returning.
- Returning completion from `Execute()` permits immediate request reuse; no
  component may retain that request pointer afterward.
- Removing an active fetch may destroy the executing request. Release its pins
  and locks first, then return without touching it again.
- Term and schema checks precede access to table state, including resumed work.
- A resumed lock waiter already owns the granted lock.
- Cross-execution references to entries require a lock, intent, or pin that
  preserves their location and lifetime.
- The transaction-to-lock registry must remain synchronized with logical lock
  ownership; stale registry state can pin checkpoints or misdirect recovery.
- While a transaction holds a write lock on a data table or catalog entry, the
  checkpoint stays below its latest write-lock grant.
- Eviction cannot remove dirty or transactionally owned entries.
- Reserved payload bytes count as allocated in every admission decision, and
  each charge has exactly one owner, the pending acquire request or the
  granted write lock, until a terminal failure or a lock release returns it.
- A request parked for memory is not left waiting indefinitely: it is
  rejected once the configured number of checkpoint cycles have concluded
  around it. The exception is log replay: rejecting a replay request makes
  the log groups that had not finished replay again, and recovery's own
  checkpoints flush what the replay installs. A released replay request
  checks its node group term before applying anything.

## Source map

| Claim | Repository source |
|---|---|
| `LocalCcShards`, `CcShard`, and `CcMap` define the node, core, and table ownership layers | `tx_service/include/cc/local_cc_shards.h`; `tx_service/include/cc/cc_shard.h`; `tx_service/include/cc/cc_map.h` |
| CC requests execute serially on the destination shard and return a recycle decision | `tx_service/include/cc/cc_req_base.h`; `tx_service/src/cc/cc_shard.cpp` |
| Table requests validate node-group terms and schema versions | `tx_service/include/cc/cc_request.h`; `tx_service/include/cc/template_cc_map.h` |
| Lock conflicts either fail or enqueue according to the CC protocol | `tx_service/include/cc/non_blocking_lock.h`; `tx_service/src/cc/non_blocking_lock.cpp`; `tx_service/include/cc_protocol.h` |
| Write-intent upgrades and reader admission preserve progress | `tx_service/src/cc/non_blocking_lock.cpp`; `tx_service/tests/NonBlockingLock-Test.cpp` |
| Cache-miss fetches, memory waits, and backfill retain and resume requests | `tx_service/include/cc/cc_request.h`; `tx_service/include/cc/cc_req_misc.h`; `tx_service/src/cc/cc_shard.cpp` |
| Record fetches combine bounded reuse, temporary overflow, single-flight ownership, and payload release | `tx_service/include/cc/cc_req_pool.h`; `tx_service/include/cc/cc_req_misc.h`; `tx_service/src/cc/cc_req_misc.cpp`; `tx_service/include/cc/cc_shard.h`; `tx_service/src/cc/cc_shard.cpp` |
| Typed maps own pages, entries, versions, and object-command specializations | `tx_service/include/cc/cc_entry.h`; `tx_service/include/cc/template_cc_map.h`; `tx_service/include/cc/object_cc_map.h` |
| Scanners retain CC ownership across batched progress | `tx_service/include/cc/ccm_scanner.h`; `tx_service/include/cc/cc_request.h`; `tx_service/src/tx_execution.cpp` |
| Shard-local eviction admits only unowned persistent entries | `tx_service/include/cc/cc_entry.h`; `tx_service/include/cc/cc_page_clean_guard.h`; `tx_service/src/cc/cc_shard.cpp` |
| Payload reservations are charged before the write-lock attempt, owned by the request then the lock, and counted as allocated | `tx_service/include/cc/cc_shard.h` (`CcShardHeap`); `tx_service/include/cc/cc_request.h` (`AcquireCc`); `tx_service/include/cc/template_cc_map.h` (`FindEmplace`, `Execute(AcquireCc&)`); `tx_service/include/cc/non_blocking_lock.h`; `tx_service/src/cc/cc_entry.cpp`; `tx_service/tests/PayloadReservation-Test.cpp` |
| A store fetch checks shard memory after the store answers and before it installs its record | `tx_service/include/cc/cc_req_misc.h` (`FetchRecordCc`); `tx_service/src/cc/cc_req_misc.cpp` (`FetchRecordCc::Execute`, `MustWaitForMemory`, `MemoryWaitPolicy`, `AbortCcRequest`); `tx_service/tests/PayloadReservation-Test.cpp` |
| Parked requests are rejected after the configured number of concluded checkpoint cycles | `tx_service/include/cc/cc_req_base.h`; `tx_service/src/cc/cc_shard.cpp` (`EnqueueWaitListIfMemoryFull`, `AbortRequestsAfterMemoryFree`); `tx_service/src/cc/cc_req_misc.cpp` (`ShardCleanCc::Execute`); `tx_service/tests/PayloadReservation-Test.cpp` |
| Each request type says how long it may wait for memory: fail fast, bounded by checkpoint cycles, or unbounded for log replay | `tx_service/include/cc/cc_req_base.h` (`MemoryWait`, `MemoryWaitPolicy`); `tx_service/include/cc/cc_request.h` (`AcquireCc`, `ReadCc` and the other transactional requests, `ScanSliceCc`, `UploadBatchCc`, `UploadBatchSlicesCc`, `ReplayLogCc`, `RequestAborterCc`); `tx_service/include/cc/cc_req_misc.h` (`FillStoreSliceCc`); `tx_service/src/cc/cc_shard.cpp` (`AbortRequestsAfterMemoryFree`); `tx_service/src/fault/log_replay_service.cpp` (a replay error closes the node group's replay streams and the watchdog asks again) |
| The lock registry records each transaction's latest write-lock grant and whether it bounds the checkpoint | `tx_service/include/cc/cc_shard.h`; `tx_service/src/cc/cc_shard.cpp`; `tx_service/src/cc/cc_map.cpp`; `tx_service/tests/CkptLockRegistry-Test.cpp` |
| Lock grants report timestamp bounds, and catalog readers publish their commit timestamps | `tx_service/include/cc/template_cc_map.h`; `tx_service/src/cc/local_cc_handler.cpp`; `tx_service/src/tx_upsert_table_operation.cpp` |
| Snapshot transactions start at the later of the timestamp base and the wall clock | `tx_service/src/cc/cc_shard.cpp`; `tx_service/src/cc/local_cc_handler.cpp` |
| Lock ownership feeds deadlock detection and recovery bookkeeping | `tx_service/include/cc/cc_shard.h`; `tx_service/include/dead_lock_check.h`; `tx_service/src/dead_lock_check.cpp` |
