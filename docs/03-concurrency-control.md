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
  checkpoint progress makes admission possible, unless the caller requested an
  immediate out-of-memory failure.

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
takes the required write locks and returns version/timestamp bounds.
Post-read requests validate optimistic reads and release read ownership;
post-write requests install the committed value or discard the pending value,
then release the write lock and wake waiters. Object-command maps use the same
lock lifecycle but apply and commit engine commands against shard-owned
objects.

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

## Deadlock and orphan-lock coordination

Each shard maintains a registry from transactions to the entry locks and
intents they hold. The registry is part of the correctness model: checkpointing
uses it to derive active-transaction bounds, failover recovery uses it to find
orphaned ownership, and deadlock detection uses it to build the wait-for graph.

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
- Eviction cannot remove dirty or transactionally owned entries.

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
| Lock ownership feeds deadlock detection and recovery bookkeeping | `tx_service/include/cc/cc_shard.h`; `tx_service/include/dead_lock_check.h`; `tx_service/src/dead_lock_check.cpp` |
