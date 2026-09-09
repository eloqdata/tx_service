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
- catalog, database, range, and statistics metadata;
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
`DataStoreHandler`/EloqDSS integration contract are authoritative.

EloqDSS can run as a separate brpc service or be colocated with the tx service.
The client resolves a KV partition to a DSS shard and current owner. A colocated
owner uses the same service operation through an in-process request path;
otherwise the client uses the protobuf RPC surface. Local bypass changes
transport cost, not storage semantics.

## Shard ownership and retries

`DataStoreService` gates requests by per-shard ownership and lifecycle state.
Reads require an open readable shard; writes require the current writable
owner. When a client reaches a former owner, the response carries newer
sharding information so the client can refresh its versioned routing view and
retry within the operation's bounded retry policy.

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

The checkpointer may advance entry and NG checkpoint timestamps, make clean
entries evictable, and report a WAL truncation point only after both the writes
and any required persistence barrier succeed. This is the central contract
between `DataStoreHandler` and the durability layer.

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
- `PutAll` alone is sufficient only for a backend that does not require
  `PersistKV`.
- Asynchronous storage completion mutates CC state only after returning to the
  owning shard.
- Cache-miss coalescing retains fetch ownership through retries; pool saturation
  does not reject reads.
- Local EloqDSS bypass and remote RPC implement the same request semantics.
- Shard ownership and topology versions fence writes to a moved DSS shard.
- The partitioning-kind flag is part of routing correctness, not a storage
  optimization hint.

## Source map

| Claim | Repository source |
|---|---|
| `DataStoreHandler` defines the tx/storage persistence and lifecycle boundary | `tx_service/include/store/data_store_handler.h` |
| Startup selects embedded RocksDB or an EloqDSS client/backend composition | `core/src/storage_init.cpp`; `CMakeLists.txt`; `store_handler/eloq_data_store_service/CMakeLists.txt` |
| EloqDSS client routing supports local service bypass and remote RPC completion | `store_handler/data_store_service_client.h`; `store_handler/data_store_service_client.cpp`; `store_handler/data_store_service_client_closure.h`; `store_handler/data_store_service_client_closure.cpp` |
| EloqDSS owns versioned data shards behind a pluggable `DataStore` interface | `store_handler/eloq_data_store_service/data_store_service.h`; `store_handler/eloq_data_store_service/data_store_service.cpp`; `store_handler/eloq_data_store_service/data_store.h`; `store_handler/eloq_data_store_service/ds_request.proto` |
| Embedded RocksDB implements the same handler contract without DSS transport | `store_handler/rocksdb_handler.h`; `store_handler/rocksdb_handler.cpp` |
| Checkpoint data sync depends on `PutAll` and the optional persistence barrier | `tx_service/src/cc/local_cc_shards.cpp`; `tx_service/include/data_sync_task.h`; `tx_service/src/data_sync_task.cpp`; `tx_service/src/checkpointer.cpp` |
| Cache-miss requests are coalesced, retained across retry, and safely recycled or destroyed | `tx_service/include/cc/cc_req_misc.h`; `tx_service/src/cc/cc_req_misc.cpp`; `tx_service/include/cc/cc_shard.h`; `tx_service/src/cc/cc_shard.cpp` |
| Range loads and MVCC archive reads cross the same handler boundary | `tx_service/src/cc/range_slice.cpp`; `tx_service/include/store/data_store_handler.h`; `store_handler/data_store_service_scanner.cpp` |
| The in-process DSS request path is covered by store read/write/scan tests | `tx_service/tests/MemDataStore-Test.cpp`; `tx_service/tests/TTLCompactionFilter-Test.cpp` |
