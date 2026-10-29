# 08 — Range & Bucket Management

Buckets are the stable unit of distributed ownership; ranges are the ordered
unit used by range-partitioned tables. The two models meet at bucket metadata:
a hash-partitioned key maps directly to a bucket, while a range-partitioned key
first resolves to a range and that range identifier maps to a bucket. In both
cases the bucket names the node group (NG) that may serve the data.

This document covers the metadata and lifecycle that make those placement
decisions safe. Cluster membership and remote routing are covered by
[06-distribution-and-clustering.md](06-distribution-and-clustering.md), and the
checkpoint/replay boundary by
[07-durability-and-recovery.md](07-durability-and-recovery.md).

## Partitioning authorities

`Sharder` defines the deterministic key/range-to-bucket and bucket-to-core
mappings. `LocalCcShards` holds the fast in-memory `BucketInfo` view for each NG,
while `RangeBucketCcMap` exposes the same ownership as lockable, versioned
transactional metadata. Normal routing uses the fast view; ownership changes
switch affected buckets to the protected metadata path until the change is
committed.

For a range-partitioned table, `RangeCcMap` maps ordered key boundaries to
`RangeRecord`s. A range record links the logical interval, its persistent
partition identifier, and the bucket record that determines the owner. Reads
acquire the bucket metadata lock before the range metadata lock. That order is
global and must be preserved by new operations to avoid an internal metadata
deadlock.

## Cached range lifecycle

An active range is represented by `StoreRange`, divided into ordered
`StoreSlice`s. Slice metadata describes which portion of the range is present
in the in-memory CC map; missing portions are loaded through
`DataStoreHandler::LoadRangeSlice`. Loading is asynchronous: the initiating CC
request waits on the slice, storage completion re-enters the owning shard, and
the waiters resume only after the fill is installed there.

Ranges and loads are pinned while code depends on their identity. Split,
eviction, and ownership transitions wait for or reject conflicting pins rather
than mutating a `StoreRange` underneath a reader. Range/slice metadata uses a
separate memory budget so evicting cached range descriptions does not silently
consume the ordinary CC-map budget. Eviction drops only unpinned ranges; their
records remain recoverable from the persistent store.

Checkpoint scans reconcile each slice's durable size and specification with its
dirty records. These durable size estimates guide maintenance decisions, but
the exact sampling and threshold mechanisms are local implementation details,
not part of the architecture contract.

## Range split

A range split is a logged metadata-and-data transaction. At a high level it:

1. locks the old range and prospective ownership metadata;
2. creates pending range definitions and makes concurrent writes visible to
   both the old and new layout as required by the transition;
3. flushes the new persistent partitions and transfers cache state when a new
   range belongs to another NG;
4. commits the new boundaries and routing metadata; and
5. removes data that is no longer visible through the old range.

The publish point follows durable data synchronization, so readers cannot be
routed to a range whose base data is incomplete. A split that fails before its
commit log has installed nothing: it rolls back at commit timestamp 0 and only
releases its locks. WAL records preserve enough
stage and range metadata for recovery to rebuild pending definitions and resume
the operation after failover. Cleanup is idempotent because the committed range
layout, not the former process state, determines which keys remain visible.

## Bucket migration

Cluster scaling moves data by changing versioned bucket ownership. Migration
workers operate on bounded bucket batches but share one logical migration
status. For each batch, they lock the bucket records across NGs, install a
pending owner, synchronize durable table data and transferable cache state,
then publish the new owner and evict the old owner's copy.

The old owner remains authoritative until the synchronized data is available
to the new owner. During the transition, uploaded data is associated with the
pending bucket version so retries or delayed messages cannot overwrite a newer
ownership decision. Staged migration logs let replay resume work that crossed
a leader failure.

## Secondary-index integration

Building a secondary index traverses primary-key ranges because those ranges
are already the ownership and scan boundary for ordered data. Work is dispatched
to the NG that owns each range; generated index entries are uploaded to the NG
that owns the derived secondary key. The index transaction uses the same range
pins, remote upload boundary, data-sync durability, and staged recovery model as
other distributed metadata operations.

Each completed primary-key scan batch retains references owned by its source
shard until every index encoder has consumed them. The generator releases those
references on the source shard before resetting the scan request, waiting for
upload capacity, or sleeping for retry. Encoded index entries and the resume key
therefore require independent ownership; releasing the source batch must not
invalidate them.

An index build is a schema operation whose prepare log is its point of no
return. Under the prepare-stage write lock it creates each new index's kv table
(`DataStoreHandler::CreateKvTable`, one attempt) and writes the prepare log; a
failure of either drops the tables and fails the operation with no log. Recovery
returns an NG to the state right after the prepare log, where the dirty schema
is installed and DML writes the new index, so the kv table must exist in every
state recovery can reach. The installed dirty schema puts the prepare
timestamp on the catalog entry, so every DML that sees it commits above that
timestamp and its index writes survive replay. Data-store steps after the
prepare log are retried until they succeed, `ddl_kv_retry_interval_seconds`
apart. The kv catalog receives the new schema once, after the commit log and
under the commit-stage write lock, so no later schema operation's catalog
write can be overwritten by it. A build that finds a constraint violation
commits as rolled back at its commit-stage timestamp, drops the index's kv
tables and keeps the current schema.

Index expression details and batch encoding belong to the index implementation,
not to the range architecture.

## Cross-cutting invariants

- Bucket ownership is the final authority for both hash and range data.
- Acquire a bucket metadata lock before its range metadata lock.
- A pinned range or active slice load cannot be split, dropped, or reassigned
  underneath its users.
- Storage callbacks that fill a slice must re-enter the owning `CcShard` before
  mutating CC-map state or waking shard-owned requests.
- New range or bucket ownership is published only after required durable data
  synchronization succeeds.
- Secondary-index generation releases consumed scan batches on their source
  shard before reset, backpressure, or retry.
- A new index's kv table exists in every state that recovery of its build can
  reach.
- Split and migration replay is versioned and idempotent; delayed work from an
  older ownership version must not become visible.

## Source map

| Claim | Repository source |
|---|---|
| Key and range identifiers map through buckets to NGs and local cores | `tx_service/include/sharder.h`; `tx_service/src/sharder.cpp`; `tx_service/src/cc/local_cc_shards.cpp` |
| Bucket ownership has a fast in-memory view and a transactional CC-map view | `tx_service/include/cc/local_cc_shards.h`; `tx_service/src/cc/local_cc_shards.cpp`; `tx_service/include/cc/range_bucket_cc_map.h`; `tx_service/include/range_bucket_key_record.h` |
| Ordered range metadata links ranges to bucket ownership and fixes bucket-before-range lock order | `tx_service/include/cc/range_cc_map.h`; `tx_service/include/range_record.h`; `tx_service/src/range_record.cpp` |
| `StoreRange` and `StoreSlice` own cached-range, pin, load, and slice-spec lifecycles | `tx_service/include/cc/range_slice.h`; `tx_service/src/cc/range_slice.cpp`; `tx_service/include/cc/range_slice_type.h` |
| Slice loading and range checkpointing cross the store-handler boundary | `tx_service/include/store/data_store_handler.h`; `tx_service/include/data_sync_task.h`; `tx_service/src/cc/local_cc_shards.cpp` |
| Range split and bucket migration are staged, recoverable transaction operations | `tx_service/include/tx_operation.h`; `tx_service/src/tx_operation.cpp`; `tx_service/tx-log-protos/log.proto` |
| An index build creates its kv tables before its prepare log and writes the kv catalog once, after its commit log | `tx_service/include/tx_index_operation.h`; `tx_service/src/tx_index_operation.cpp`; `tx_service/include/store/data_store_handler.h`; `tx_service/include/cc/template_cc_map.h`; `tx_service/include/cc/catalog_cc_map.h` |
| Secondary-index build dispatches range work, releases consumed source batches, and uploads derived entries by ownership | `tx_service/include/tx_index_operation.h`; `tx_service/src/tx_index_operation.cpp`; `tx_service/include/sk_generator.h`; `tx_service/src/sk_generator.cpp`; `tx_service/include/cc/local_cc_shards.h` |
