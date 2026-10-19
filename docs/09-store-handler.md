# 09 — Store Handler

The store handler is the persistence boundary below the in-memory transaction
service. `txservice::store::DataStoreHandler` lets CC maps and transaction
operations persist and recover base records, catalogs, range metadata, table
statistics, MVCC archives, and snapshots without depending on one storage
engine or deployment shape.

The store is not the transaction commit authority. WAL establishes the commit
decision; checkpointing later makes the corresponding state durable through
this interface and only then allows log truncation or cache eviction. See
[07-durability-and-recovery.md](07-durability-and-recovery.md).

## Interface boundary

The interface groups several durable responsibilities:

- checkpoint writes and an optional explicit persistence barrier;
- point reads, bucket reads, and ordered slice scans used on cache misses;
- catalog, database, range, and statistics metadata, including a table's kv
  table without its catalog row (`CreateKvTable`, the counterpart of
  `DropKvTable`, which an index build calls before its prepare log);
- MVCC archive writes and historical-version reads;
- leader/standby lifecycle hooks and storage snapshots; and
- backend capability queries used to select shared-storage, copy, and cache
  behavior.

Data-plane reads are integrated with the shard scheduler. Operations such as
record fetch and range-slice load receive a CC request, schedule storage work,
and complete later by re-enqueueing that request on its owning shard. This
preserves shard affinity even when the I/O or RPC callback runs elsewhere.
Bulk metadata and checkpoint operations may be synchronous at the interface but
accept yield/resume hooks so coroutine callers do not have to block a worker.

`DataStoreOpStatus::Success` means the asynchronous operation was accepted, not
that its result is already installed. `Retry` is backpressure to the CC layer,
and `Error` terminates the request. Callers must preserve that distinction.

## Implementations and deployment

Startup selects one `DataStoreHandler` implementation at build time:

- the embedded RocksDB handler owns storage directly in the tx process; or
- `DataStoreServiceClient` connects to EloqDSS, whose `DataStoreService` owns
  numbered data shards backed by a pluggable `DataStore` implementation
  (RocksDB, RocksDB Cloud, or EloqStore).

The EloqStore engine is a separate submodule with its own architecture
documentation. At this layer, only its `DataStore` adapter and the
`DataStoreHandler`/EloqDSS integration contract are authoritative. A node runs
one EloqStore instance per data shard it leads, each in its own directory in
cloud mode. RocksDB backends give each data shard a database of its own, but
EloqStore keeps every shard's partitions side by side, so each instance's
storage-wide sweeps (cloud prewarm, archives for backups and standby
snapshots, branches, a global reopen) consult a filter for the partitions of
its data shard's buckets, set before every start (`SetPartitionBuckets`);
ordinary reads and writes do not.

EloqDSS can run as a separate brpc service or be colocated with the tx service.
The client resolves a KV partition to a DSS shard and current owner. A colocated
owner uses the same service operation through an in-process request path;
otherwise the client uses the protobuf RPC surface. Local bypass changes
transport cost, not storage semantics.

A read closure keeps its returned value valid until the final callback returns.
The consumer may transfer, copy, or discard that value; any unconsumed value
buffer is released before the closure returns to its pool. Pending retries keep
ownership of the closure until the operation reaches terminal completion.

## Shard ownership and retries

`DataStoreService` gates requests by per-shard ownership and lifecycle state.
Reads require an open readable shard; writes require the current writable
owner. A request that finds its shard `Starting`, opening or closing as its
node's leadership changes, waits on the shard's waiting list and runs again
once the shard leaves `Starting`. When a client reaches a former owner, the
response carries newer sharding information so the client can refresh its
versioned routing view and retry within the operation's bounded retry policy.

After a failed RPC the client sends the request again at once only if that can
reach the shard: the shard is now local, or the channel to its current owner is
healthy (`CanRetryShardNow`). brpc keeps a failed socket down until its health
check reconnects, so an immediate retry to the same address would only fail
again; the operation fails instead and its caller's retry policy applies.

With data shards bound to NGs (EloqDSS colocated with the tx service, shard *g*
for NG *g*), the node hosting a shard is the NG's leader. Every node reaches
other NGs' shards directly for shared metadata and checkpoint flushes, so each
node points a shard at its NG's leader whenever the tx service learns a newer
leader term (`DataStoreHandler::OnLeaderChanged`), even for the same node. The
shard's version in this mode is that term: every node derives the same number
for the same leadership, so a redirect's version compares with this node's. A
move whose term is below the shard's version changes nothing, checked against
the routing slot it replaces, so moves and redirects may arrive in any order.

For shared-storage backends, DSS shard migration transfers serving ownership
rather than copying the underlying dataset: the source stops accepting writes,
the target opens the shard, the topology version is published, and the source
closes after in-flight writes drain. Transaction-layer bucket migration remains
a separate concern; it changes logical NG ownership as described in
[08-range-and-bucket-management.md](08-range-and-bucket-management.md).

Hash and range partition identifiers use different mapping functions before
they reach DSS. The caller must preserve the table's partitioning kind; treating
a range id as a hash partition id can route a correct key to the wrong data
shard.

## Checkpoint durability

Checkpoint workers group dirty records by KV table and partition and submit
them through `PutAll`. Some backends stage these batches without making them
independently durable. When `NeedPersistKV` is true, `PersistKV` is the required
barrier after all batches for the checkpoint have completed.

EloqStore batch completion establishes durability, so non-MVCC flushes may
publish entry checkpoint timestamps per completed partition. RocksDB-backed
batches require `PersistKV`; MVCC also requires archive writes before entries
become clean and evictable. Lower-term tasks in a merged flush are excluded
from both datastore work and checkpoint publication. NG checkpoint timestamps
and WAL truncation still wait for the entire included round to succeed.

This distinction between entry-level progress and the NG-wide truncation
boundary is the central contract between `DataStoreHandler` and the durability
layer.

Each completed partition relinquishes its record buffers before reporting the
freed memory to data-sync admission. Buffers still needed by the flush round
retain their quota until their consumers finish, including on failure paths.

Cache misses reverse the path: the handler returns a record or streams a bounded
range slice, and the completion is installed on the owning shard. MVCC reads
may request the latest archive version visible at a timestamp rather than the
base record.

The CC layer coalesces concurrent misses for one record and owns the fetch
request until backfill or retry reaches a terminal path. Its reusable fetch
pool is bounded, but temporary overflow preserves read admission. Completion
releases retained payload storage before a pooled request becomes idle.

## Snapshots and replica lifecycle

The interface exposes snapshot creation, transport, installation, and cleanup
for standby bootstrap and backup. Concrete support is backend-dependent and is
reported through backend behavior and capability methods; generic code must not
assume every handler implements every snapshot mode.

Leader and follower hooks allow the backend to open, close, or refresh NG-local
state in step with tx-service terms. The tx layer pins NG ownership across work
that depends on that state, and lifecycle callbacks must not make an obsolete
term writable again.

## Cross-cutting invariants

- WAL decides transaction commit; the store handler supplies the later
  checkpoint durability boundary.
- Base-partition completion permits checkpoint publication only when the
  backend needs no further persistence barrier and MVCC archives are not pending.
- Asynchronous storage completion mutates CC state only after returning to the
  owning shard.
- Cache-miss coalescing retains fetch ownership through retries; pool saturation
  does not reject reads.
- Local EloqDSS bypass and remote RPC implement the same request semantics.
- Shard ownership and topology versions fence writes to a moved DSS shard.
- With data shards bound to NGs, a shard's routing version never falls below
  the newest leader term its node has learned.
- The partitioning-kind flag is part of routing correctness, not a storage
  optimization hint.

## Source map

| Claim | Repository source |
|---|---|
| `DataStoreHandler` defines the tx/storage persistence and lifecycle boundary | `tx_service/include/store/data_store_handler.h` |
| Startup selects embedded RocksDB or an EloqDSS client/backend composition | `core/src/storage_init.cpp`; `CMakeLists.txt`; `store_handler/eloq_data_store_service/CMakeLists.txt` |
| EloqDSS client routing supports local service bypass and remote RPC completion | `store_handler/data_store_service_client.h`; `store_handler/data_store_service_client.cpp`; `store_handler/data_store_service_client_closure.h`; `store_handler/data_store_service_client_closure.cpp` |
| Read closures retain values through callbacks and release unconsumed buffers before pool reuse | `store_handler/data_store_service_client_closure.h`; `store_handler/eloq_data_store_service/object_pool.h`; `tx_service/tests/ReadClosureLifetime-Test.cpp` |
| EloqDSS owns versioned data shards behind a pluggable `DataStore` interface | `store_handler/eloq_data_store_service/data_store_service.h`; `store_handler/eloq_data_store_service/data_store_service.cpp`; `store_handler/eloq_data_store_service/data_store.h`; `store_handler/eloq_data_store_service/ds_request.proto` |
| Requests for a `Starting` shard wait on its waiting list | `store_handler/eloq_data_store_service/data_store_service.h`; `store_handler/eloq_data_store_service/data_store_service.cpp` |
| The client retries at once only when the shard is reachable, and binds data shards to NG leaders by term | `store_handler/data_store_service_client.h`; `store_handler/data_store_service_client.cpp`; `store_handler/data_store_service_client_closure.h`; `store_handler/eloq_data_store_service/data_store_service_config.cpp` |
| Each EloqStore instance sweeps only its data shard's partitions | `store_handler/eloq_data_store_service/eloq_store_data_store.h`; `store_handler/eloq_data_store_service/eloq_store_data_store.cpp`; `store_handler/eloq_data_store_service/data_store.h` |
| Embedded RocksDB implements the same handler contract without DSS transport | `store_handler/rocksdb_handler.h`; `store_handler/rocksdb_handler.cpp` |
| Checkpoint data sync depends on `PutAll` and the optional persistence barrier | `tx_service/src/cc/local_cc_shards.cpp`; `tx_service/include/data_sync_task.h`; `tx_service/src/data_sync_task.cpp`; `tx_service/src/checkpointer.cpp` |
| Cache-miss requests are coalesced, retained across retry, and safely recycled or destroyed | `tx_service/include/cc/cc_req_misc.h`; `tx_service/src/cc/cc_req_misc.cpp`; `tx_service/include/cc/cc_shard.h`; `tx_service/src/cc/cc_shard.cpp` |
| Range loads and MVCC archive reads cross the same handler boundary | `tx_service/src/cc/range_slice.cpp`; `tx_service/include/store/data_store_handler.h`; `store_handler/data_store_service_scanner.cpp` |
| The in-process DSS request path is covered by store read/write/scan tests | `tx_service/tests/MemDataStore-Test.cpp`; `tx_service/tests/TTLCompactionFilter-Test.cpp` |
