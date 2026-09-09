# Data Model and Catalog

The tx service is API-agnostic. It coordinates keys, records, object commands,
schemas, and indexes without depending on an EloqKV, EloqSQL, or EloqDoc
runtime type. Each API engine registers a `CatalogFactory` before the data
substrate starts; the factory supplies the concrete types and construction
operations needed for request decoding, CC maps, scans, persistence, and log
replay.

This boundary lets multiple engine types share the same transaction, CC,
distribution, WAL, and store infrastructure while preserving each engine's
logical and durable data formats.

## Engine data abstractions

`TxKey` is a move-only, type-erased key handle backed by an engine-provided
`TxKeyInterface`. The interface supplies ordering, equality, hashing,
serialization, cloning, packed-key access, and memory accounting. A handle may
own its concrete key or borrow it; cloning is required when a key must outlive
the page, request, or engine object from which the borrowed view came.
Per-engine negative- and positive-infinity keys define ordered scan and range
boundaries. When a range-start sentinel must be persisted, callers use the
factory's packed negative-infinity key rather than serializing the in-memory
sentinel.

`TxRecord` is the polymorphic value boundary. It supplies serialization,
deserialization, cloning, and storage-facing encoded data. Record-oriented
engines buffer complete record replacements in a transaction write set.
`TxObject` and `TxCommand` provide a second model for command-oriented engines:
the command executes against an object on its owner shard, returns a compact
result, and retains the command image needed for commit, replication, and WAL
replay.

Commands that arrive through recovery or forwarding are applied in commit
version order and against the object version on which they were based.
Overwrite commands may establish a new replay base; other commands cannot skip
an unknown predecessor. This is a compatibility and recovery invariant, not an
implementation-specific queueing policy.

## Logical and durable identity

`TableName` combines a logical name with its table role and engine. That full
identity selects the catalog factory, CC map specialization, partitioning
behavior, and physical store mapping. Catalog, range/bucket, cluster
configuration, and sequence tables use the same type system as application
tables even when their replication or routing rules differ.

The data model has distinct serialization contexts:

- request and WAL serialization must be self-delimiting and reconstructable
  through the registered factory or schema;
- KV serialization must match the physical store's key/value encoding;
- schema and physical-catalog images are durable inputs to restart, recovery,
  and upgrade.

These formats may share engine code, but they are not interchangeable.
Changing an existing key, record, command, table-name, or catalog image is a
durable compatibility change and must be designed together with its readers,
replay path, and upgrade policy.

## CatalogFactory boundary

For each external `TableEngine`, `DataSubstrate::RegisterEngine` records one
factory and optional engine integration state. `LocalCcShards` combines the
registered external factories with built-in factories for internal hash- and
range-partitioned tables. Requests select a factory through the table's engine
identity.

The factory is responsible for reconstructing table schemas, keys, records,
commands, typed primary/secondary/range CC maps, scanners, range metadata, and
statistics. `TableSchema` is the engine-owned description used by the core: it
exposes the base table identity, key and record schemas, indexes, serialized
schema image, physical KV mapping, version, command reconstruction, and
sequence/statistics hooks.

Factories and schemas must outlive the maps and transactions that borrow them.
The data substrate controls registration and startup; the embedding engine
retains the concrete factory lifetime and semantics behind the interfaces.

## Catalog ownership and schema lifecycle

The `__catalog` CC map makes schema changes participate in transaction locking
and replication. Catalog records carry the persisted schema images and refer
to node-level schema objects. `LocalCcShards` owns a `CatalogEntry` for each
table and node group; shards borrow the current or prepared schema from that
entry rather than constructing unrelated schema objects.

A schema change has current and dirty states. Preparation installs a newer
dirty schema while the current schema remains readable; commit promotes the
dirty schema, and rejection discards it. Version comparisons make replay and
repeated delivery idempotent. DDL uses replicated catalog locks so that schema
visibility and data-path use cannot cross inconsistently.

Data-path requests carry the schema version under which their key/record was
interpreted. Typed CC maps reject nonzero versions that do not match the map's
schema version. Version zero is reserved for paths that intentionally bypass
this check, primarily internal metadata or maintenance operations; application
data paths must not use it as a convenience default.

Catalog images are persisted through the store handler and carried in schema
WAL records so startup and recovery can reconstruct both committed and
in-progress schema state. Store integration is described in
[09-store-handler.md](09-store-handler.md), and the schema transaction protocol
is coordinated through the state-machine model in
[04-transaction-execution.md](04-transaction-execution.md).

## Internal consumers

The sequence subsystem stores auto-increment and range-partition allocation
state in an internal hash table. Nodes may lease and cache a batch locally, but
advancing the authoritative sequence is a transaction against that table, so
concurrent allocation and recovery use the ordinary data and WAL contracts.

Table statistics are attached to schemas and derived from samples maintained
by typed CC maps. Responsibility for aggregating and exchanging statistics is
assigned deterministically across cores and node groups, and persisted
statistics are reconstructed through the same engine factory. Sampling and
histogram mechanics are local implementation details rather than catalog
format authority.

## Invariants

- A borrowed `TxKey`, schema, record, or statistics pointer cannot outlive its
  owner. Long-lived state must clone the value or retain shared ownership.
- In-memory sentinels are ordering identities, not durable key encodings.
- Request/WAL and KV serialization are separate contracts; recovery must use
  the format and factory that produced the image.
- Table identity includes the engine and table role, not only the name string.
- Application data CC requests carry the schema version used to interpret
  their payload and fail on mismatch.
- Object-command replay preserves base-version and commit-version ordering.
- A prepared schema does not become current until the catalog transaction
  commits; replaying preparation or completion must be idempotent.

## Source map

| Claim | Repository source |
|---|---|
| External engines register factories before startup | `core/include/data_substrate.h`; `core/src/data_substrate.cpp`; `core/src/tx_service_init.cpp` |
| `TxKey` provides type erasure, owned/borrowed lifetimes, sentinels, and serialization hooks | `tx_service/include/tx_key.h`; `tx_service/src/tx_key.cpp`; `tx_service/include/eloq_string_key_record.h` |
| `TxRecord`, `TxObject`, and `TxCommand` define record- and command-oriented value models | `tx_service/include/tx_record.h`; `tx_service/include/tx_object.h`; `tx_service/include/tx_command.h` |
| Object commands execute and commit on owner-shard objects | `tx_service/include/cc/object_cc_map.h`; `tx_service/src/tx_execution.cpp`; `tx_service/include/command_set.h` |
| Command buffering preserves object-version and commit-version order | `tx_service/include/tx_command.h`; `tx_service/include/cc/cc_entry.h`; `tx_service/tests/StandbyForward-Test.cpp` |
| Table identity carries name, role, and engine and selects partitioning behavior | `tx_service/include/type.h`; `tx_service/include/constants.h` |
| `CatalogFactory` constructs engine-specific schemas, maps, keys, records, scanners, and statistics | `tx_service/include/catalog_factory.h`; `tx_service/include/eloq_basic_catalog_factory.h`; `tx_service/src/eloq_basic_catalog_factory.cpp` |
| `LocalCcShards` owns current/dirty catalog entries shared by shard catalog records | `tx_service/include/catalog_key_record.h`; `tx_service/include/cc/local_cc_shards.h`; `tx_service/src/cc/local_cc_shards.cpp`; `tx_service/include/cc/catalog_cc_map.h` |
| Typed maps reject requests interpreted under a different schema version | `tx_service/include/cc/cc_request.h`; `tx_service/include/cc/template_cc_map.h`; `tx_service/src/tx_execution.cpp` |
| Sequences use an internal transactional table | `tx_service/include/sequences/sequences.h`; `tx_service/src/sequences/sequences.cpp` |
| Statistics are schema-bound, assigned across cores/node groups, exchanged, and persisted | `tx_service/include/statistics.h`; `tx_service/include/table_statistics.h`; `tx_service/src/cc/local_cc_shards.cpp` |
