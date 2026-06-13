# Data Model and Catalog

The tx service core is engine-agnostic: it never knows what a Redis hash or a SQL row looks like. All keys, records, schemas and commands flow through type-erased interfaces — `TxKey`/`TxKeyInterface` (`tx_service/include/tx_key.h`), `TxRecord`/`TxObject` (`tx_service/include/tx_record.h`, `tx_object.h`), `TxCommand` (`tx_service/include/tx_command.h`) and `TableSchema` (`tx_service/include/catalog_factory.h`) — and each API engine (EloqKV, EloqSQL, EloqDoc) plugs in concrete implementations by registering a `CatalogFactory` for its `TableEngine` (`core/include/data_substrate.h::RegisterEngine`). Catalog data itself is just another cc map (`__catalog`, keyed by `CatalogKey` = table name, valued by `CatalogRecord` = serialized schema image + schema pointers), versioned by a `schema_ts` that every CC request carries and that the cc maps validate. This doc covers the key/record/command model, naming, the catalog subsystem, the `Sequences` system table, and statistics. Siblings: [01-architecture-overview.md](01-architecture-overview.md), [02-threading-model.md](02-threading-model.md), [03-concurrency-control.md](03-concurrency-control.md), [04-transaction-execution.md](04-transaction-execution.md), [06-distribution-and-clustering.md](06-distribution-and-clustering.md), [07-durability-and-recovery.md](07-durability-and-recovery.md), [08-range-and-bucket-management.md](08-range-and-bucket-management.md), [09-store-handler.md](09-store-handler.md), [10-log-service.md](10-log-service.md).

## 1. Why Type Erasure: One Tx Service, Many Frontends

A single node may serve several API engines at once. Each engine has its own key encoding, record format and (for EloqKV) command set, but they all share one set of `CcShard`s, one transaction state machine, one log service and one store handler. The core therefore manipulates keys only through `TxKey` (a hand-rolled type-erasure handle, not a virtual base — keys are tiny and copied/compared on hot paths) and records through `TxRecord` virtuals, and asks the engine's `CatalogFactory` whenever it must *create* a concrete object (deserializing a remote request, replaying a log, instantiating a cc map).

Registration and dispatch:

- `DataSubstrate::RegisterEngine(engine_type, factory, system_handler, prebuilt_tables, ...)` (`core/src/data_substrate.cpp`) accepts only the external engines `EloqSql=1 / EloqKv=2 / EloqDoc=3` and stores the factory in `engines_[engine-1]` (`NUM_EXTERNAL_ENGINES = 3`, `tx_service/include/type.h`).
- `core/src/tx_service_init.cpp` passes the 3 external factories to `TxService`, and `LocalCcShards` appends its two built-in factories (`tx_service/src/cc/local_cc_shards.cpp:112`): `catalog_factory_ = {sql, kv, doc, &range_catalog_factory_, &hash_catalog_factory_}` (`EloqRangeCatalogFactory` / `EloqHashCatalogFactory` from `tx_service/include/eloq_basic_catalog_factory.h`).
- Each `CcShard` keeps the same 5-slot array and dispatches per request: `CcShard::GetCatalogFactory(TableEngine e)` returns `catalog_factory_[(int)e - 1]` (`tx_service/include/cc/cc_shard.h:546`).

| `TableEngine` (`type.h`) | value | factory slot | KV table prefix (`KvTablePrefixOf`) | partitioning (`TableName::IsHashPartitioned`) |
|---|---|---|---|---|
| `None` | 0 | — (never indexable; `e-1` would be -1) | `""` | hash |
| `EloqSql` | 1 | 0 (registered by engine) | `eloqsql_` | range |
| `EloqKv` | 2 | 1 (registered by engine) | `eloqkv_` | hash; `IsObjectTable() == true` |
| `EloqDoc` | 3 | 2 (registered by engine) | `eloqdoc_` | range |
| `InternalRange` | 4 | 3: `EloqRangeCatalogFactory` | `irange_` | range |
| `InternalHash` | 5 | 4: `EloqHashCatalogFactory` (e.g. Sequences) | `ihash_` | hash |

## 2. TxKey: Type-Erased Keys

`tx_key.h` defines `TxKeyInterface`, a manual vtable: a struct of function pointers (`delete/equal/less/hash/serialize/kv_serialize/serialize_len/clone/copy/mem_usage/set_packed_key/data/size/type/needs_defrag/to_string`) instantiated once per concrete key type via the templated constructor. Each concrete key type exposes `static const TxKeyInterface *TxKeyImpl()` returning a function-local static (see `EloqStringKey::TxKeyImpl()` in `eloq_string_key_record.h`, `CatalogKey::TxKeyImpl()` in `catalog_key_record.h`). A `TxKey` is then just `{const TxKeyInterface *interface_; uint64_t obj_addr_;}` — 16 bytes.

**Ownership modes.** `TxKey` steals the lowest bit of the object address as an ownership flag ("memory addresses are even numbers", `tx_key.h:313`):

- `TxKey(std::unique_ptr<T>)` → owning (`obj_addr_ | 1`); destructor calls `interface_->Delete`.
- `TxKey(T*)` / `TxKey(const T*)` / `GetShallowCopy()` → non-owning view.
- Move ctor/assignment transfer the bit; copy is deleted. `MoveKey<T>()` extracts the owned pointer back into a `unique_ptr`; `Release()` drops ownership without deleting (used when `TxKey` lives inside a union).

**Sentinels.** `KeyType { NegativeInf=0, PositiveInf, Normal }`. Infinities are per-type *singleton instances* (`T::NegativeInfinity()` / `T::PositiveInfinity()`) and are compared **by address** inside the concrete `operator==`/`operator<` (see `EloqStringKey`, `VoidKey`). A default-constructed `TxKey` (null `interface_`/`obj_addr_`) reports `KeyType::NegativeInf`. `TxKey::Clone()` returns a *shallow* copy for infinities — never deep-copy or serialize a sentinel.

**Contract for a concrete key type** (duck-typed; enforced by `TxKeyInterface`'s lambdas): `operator==`, `operator<`, `Hash()`, `Serialize(std::string&)` (self-delimiting, e.g. `EloqStringKey` writes a `uint16_t` length prefix), `KVSerialize()` (raw `string_view` of the bytes as stored in the KV store — no length prefix; only valid for single-blob keys), `SerializedLength()` (for WAL length estimation), `CloneTxKey()`, `Copy()`, `MemUsage()`, `SetPackedKey(data, size)`, `Data()/Size()` (packed bytes), `Type()`, `NeedsDefrag(mi_heap_t*)` (mimalloc page-utilization check), `ToString()`, plus statics `NegativeInfinity/PositiveInfinity/TxKeyImpl`.

**Packed keys.** Engines with multi-column keys encode them into one memcmp-ordered byte string ("packed key"); the core moves them around via `Data()/Size()/SetPackedKey()` and `CatalogFactory::CreateTxKey(const char*, size_t)`. `CatalogFactory::PackedNegativeInfinity()` is a real serializable stand-in for -inf (e.g. `EloqStringKey::PackedNegativeInfinity()` = single `0x00` byte) used where a sentinel must be written out (range start keys; see [08-range-and-bucket-management.md](08-range-and-bucket-management.md)).

**Helpers in `tx_key.h`:** `CompositeKey<Types...>` (tuple-based reference key; its infinities are static members compared by address) and `VoidKey` (a keyless singleton used for single-row maps, e.g. cluster config). `TxKey::PosInInterval()` returns a crude 0 / 0.5 / 1 position estimate used by histograms (concrete keys can do better).

## 3. TxRecord, RecordStatus, and the Object/Command Model

`TxRecord` (`tx_record.h`) is a classic virtual base: `Serialize/Deserialize/Clone/Copy/ToString/SerializedLength/MemUsage/Size/NeedsDefrag`, optional TTL hooks (`SetTTL/GetTTL/HasTTL/AddTTL/RemoveTTL` — default `assert(false)`), and a blob-record subprotocol (`SetEncodedBlob/EncodedBlobData/SetUnpackInfo/...`) used by store handlers ([09-store-handler.md](09-store-handler.md)). Variants in the core: `VersionTxRecord` (record + `RecordStatus` + `commit_ts_`, used for MVCC archive reads), `BlobTxRecord` (raw `value_` string + `ttl_`, used as a flush/transport container), `EloqStringRecord` (`eloq_string_key_record.h`: `encoded_blob_` + `unpack_info_`, the record type of the basic factories), and `CompositeRecord`.

`RecordStatus` (`tx_record.h`) describes what a cc map lookup knows about a key:

| Status | Meaning |
|---|---|
| `Normal` | Returned value is the newest committed version. |
| `Unknown` | CC started but the value must be fetched from the data store. |
| `Deleted` | Tombstone. |
| `RemoteUnknown` | Fire-and-forget cache-fill read sent to a remote cc map; freshness unknown. |
| `VersionUnknown` | SI read of a historical version not in memory; unknown whether it is in the base or `mvcc_archives` KV table. |
| `BaseVersionMiss` / `ArchiveVersionMiss` | SI miss where ckpt-ts tells us the version is in the base table / possibly in the archives. |
| `Invalid` | — |
| `NonExistent` / `Uncreated` | Only for temporary objects: no dirty payload & no pending command / pending command but not yet created. |

**Plain records vs objects.** SQL-style engines treat a record as an opaque value: writes ship the whole new record (`WriteEntry { TxKey, TxRecord::Uptr, commit_ts }` in `catalog_factory.h`). EloqKV instead uses the **object/command model**: a key's payload is a `TxObject` (`tx_object.h`, a `TxRecord` plus `DeserializeObject()`), and mutations are `TxCommand`s executed *in place on the owner shard*:

1. The tx ships a `TxCommand` to the owner shard via `ApplyCc`; `ObjectCcMap::Execute(ApplyCc&)` (`cc/object_cc_map.h`) runs `cmd->ExecuteOn(object)` against the committed payload, or against a cloned *dirty* payload for non-read-only commands inside a multi-command tx.
2. `ExecuteOn` returns an `ExecResult`: `Fail / Read / Write / Delete / Block / Unlock`. Only the (small) command result travels back, not the object.
3. At commit, `PostWrite` with `OperationType::CommitCommands` (`tx_execution.cpp:5348`) makes the dirty payload current via `cmd->CommitOn(obj)`.
4. **Commands, not values, are logged and forwarded.** `CmdSetEntry` (`read_write_entry.h`) accumulates `cmd->Serialize()` images per object; the WAL blob stores key + object version + TTL + command list (`tx_execution.cpp:4767+`), and during bucket migration the same serialized commands are uploaded to the bucket's new owner (`UploadTxCommands`). `IsOverwrite()` commands clear all earlier buffered commands (`ignore_previous_version_ = true`). Replay deserializes via `TableSchema::CreateTxCommand(cmd_image)` since the core cannot know the command type.

Other `TxCommand` virtuals (`tx_command.h`): `CreateObject(image)` (make the object a command applies to), `IsReadOnly/IsDelete/IsLazyDelete/IgnoreOldValue/ProceedOnNonExistentObject/ProceedOnExistentObject/IsVolatile` (volatile commands are destroyed after execution and must be cloned for commit), TTL hooks (`RetireExpiredTTLObjectCommand`, `WillSetTTL`).

**Multi-key commands.** `MultiObjectTxCommand` (MSET/multi-key DEL/...) exposes `KeyPointers()/CommandPointers()` per step plus a step machine (`IsFinished/IsLastStep/CmdSteps/IncrSteps/HandleMiddleResult/ForwardResult`), letting two-phase commands (e.g. read in step 1, write in step 2) run inside one tx.

**Blocking commands.** `BlockOperation { NoBlock, PopBlock, PopNoBlock, BlockLock, PopElement, Discard }` supports BLPOP-style semantics: `ExecuteOn` returns `ExecResult::Block`, the request parks on the entry's lock queue (`CcEntry::PushBlockCmdRequest`, `cc/cc_entry.h`), and `AblePopBlockRequest()` decides when a later write can wake it. `MultiObjectTxCommand::NumOfFinishBlockCommands()/IsExpired()` govern multi-key blocking.

**BufferedTxnCmdList** (`tx_command.h`): a per-entry deque of `TxnCmd { obj_version_, new_version_, ignore_previous_version_, valid_scope_ (TTL deadline), cmd_list_ }`, ordered by commit ts. Used when commands arrive out of order (standby replication, bucket-forwarded commands): `EmplaceTxnCmd` inserts sorted, dedupes identical commit-ts entries (asserting same base version), and discards everything before an overwrite or expired entry; `CcEntry::TryCommitBufferedCommands` applies them strictly in version order (`cc/cc_entry.h:1751`). See the standby protocol in [01-architecture-overview.md](01-architecture-overview.md)'s pointers.

## 4. TableName, TableType, TableEngine

`TableName` (`type.h`) = `{name, TableType, TableEngine}` where the name is a union of `std::string` (owning) / `std::string_view` (borrowed) discriminated by `own_string_` — the same owning/view dual as `TxKey`. Copy-construction always materializes an owning string. **Equality and ordering compare engine and type as well as the string**, and `Serialize()` appends the engine as a trailing byte (asserting engine != None). `std::hash<TableName>` hashes only the string — names that differ just by type/engine collide (fine for buckets, relevant if you assume uniqueness).

`TableType { Primary, Secondary, UniqueSecondary, Catalog, RangePartition, RangeBucket, ClusterConfig }`. The last four are *meta* tables (`TableName::IsMeta`). Secondary index names are derived from the base name by appending a prefix marker (`constants.h`): `INDEX_NAME_PREFIX = "*$$"`, `UNIQUE_INDEX_NAME_PREFIX = "*~~"` (naming convention `./DB/Table*$$Index`); `GetBaseTableNameSV()` strips them and `TableName::Type(sv)` infers the type from them.

Well-known instances (`type.h`):

| Name | string | type / engine | purpose |
|---|---|---|---|
| `catalog_ccm_name` | `__catalog` | Catalog / None | table schemas (section 5) |
| `range_bucket_ccm_name` | `__range_bucket` | RangeBucket / None | bucket→node-group map, [08](08-range-and-bucket-management.md) |
| `cluster_config_ccm_name` | `__cluster_config` | ClusterConfig / None | topology, [06](06-distribution-and-clustering.md) |
| `sequence_table_name` | `__sequence_table` | Primary / InternalHash | auto-increment & range-id sequences (kv: `ihash_sequence_table`) |
| `internal_range_table_name` / `internal_hash_table_name` | `__internal_range_table` / `__internal_hash_table` | Primary / InternalRange, InternalHash | built-in generic tables |

Constants: `total_range_buckets = total_hash_partitions = 1024 (0x400)` (`type.h:632`). Hash-partitioned engines (EloqKV, None, InternalHash) map keys to buckets by hash; the others use range partitioning ([08](08-range-and-bucket-management.md)).

## 5. The Catalog Subsystem

### CatalogFactory (`catalog_factory.h`)

| Virtual | What the engine supplies |
|---|---|
| `CreateTableSchema(name, catalog_image, version)` | Deserialize the engine's schema blob into a `TableSchema`. |
| `CreatePkCcMap / CreateSkCcMap / CreateRangeMap` | Concrete `TemplateCcMap<KeyT, ValueT, VersionedRecord, RangePartitioned>` instantiations for primary, secondary, and range-meta maps ([03](03-concurrency-control.md)). |
| `CreateTableRange(start_key, version_ts, partition_id, slices)` | Typed `TableRangeEntry` ([08](08-range-and-bucket-management.md)); hash factories return `nullptr`. |
| `CreatePkCcmScanner / CreateSkCcmScanner / CreateRangeCcmScanner` | Typed `CcScanner`s for the scan path. |
| `CreateTableStatistics(...)` (x2) | Empty stats, or stats bootstrapped from a stored sample pool (section 7). |
| `NegativeInfKey() / PositiveInfKey()` | `TxKey` views of the type's sentinels. |
| `CreateTxKey()` / `CreateTxKey(data, size)` | Default / packed-bytes key construction (deserialization paths). |
| `PackedNegativeInfinity()` | Serializable -inf (section 2). |
| `CreateTxRecord()` | Default record instance. |
| `KeyHash(buf, offset, schema)` | Hash straight from serialized bytes (avoids materializing keys for routing). |

The built-in `EloqHashCatalogFactory` / `EloqRangeCatalogFactory` (`eloq_basic_catalog_factory.h/.cpp`) bind everything to `EloqStringKey`/`EloqStringRecord` (`TemplateCcMap<EloqStringKey, EloqStringRecord, true, false>` vs `<..., true, true>` + `RangeCcMap<EloqStringKey>`), with a schema (`EloqBasicTableSchema`) that has no indexes and no statistics.

### TableSchema (`catalog_factory.h`)

`TableSchema` is the per-table catalog object: `KeySchema()` / `RecordSchema()` (from `schema.h`; `KeySchema` has `CompareKeys`, `ExtendKeyParts`, `SchemaTs`, optional `MultiKeyPaths` for Mongo-style multikey indexes; `RecordSchema::AutoIncrementIndex()` flags an auto-increment column), `SchemaImage()` (the serialized blob round-tripped through `CreateTableSchema`), `Version()` (= schema commit ts), `GetIndexes()/IndexNames()/IndexKeySchema()/IndexOffset()` (secondary indexes, `SecondaryKeySchema` = sk schema + pk schema pointer), `GetKVCatalogInfo()`, `CreateTxCommand(cmd_image)` (section 3), `CreateSkEncoder(index_name)`, `BindStatistics()/StatisticsObject()`, and `HasAutoIncrement()/GetSequenceTableName()/GetSequenceKeyAndInitRecord()` (section 6).

`KVCatalogInfo` maps the logical table to physical KV store tables: `kv_table_name_` for the primary and `kv_index_names_` (per-index map); its `Serialize/Deserialize` use length-prefixed fields with a trailing engine byte per index. `SkEncoder::AppendPackedSk(pk, record, version_ts, dest_vec)` generates 0..n packed secondary-key `WriteEntry`s per base row (0 for partial/sparse indexes, >1 for multikey), returning -1 with a `PackSkError` on engine-raised errors — used by index build and by the write path to maintain SKs.

### How catalog data is stored and versioned

- Every node group has a `CatalogCcMap : TemplateCcMap<CatalogKey, CatalogRecord, true, false>` (`cc/catalog_cc_map.h`) — the `__catalog` cc map. `CatalogKey` (`catalog_key_record.h`) wraps a `TableName` (view for lookups, owning when resident in the map). `CatalogRecord` carries `schema_` / `dirty_schema_` pointers, `schema_image_` / `dirty_schema_image_` blobs and `schema_ts_`; it serves (1) schema lookup by txs, (2) bootstrap from the data store image, and (3) installing a dirty schema during DDL (header comment, `catalog_key_record.h:348`).
- The actual schema objects live once per node in `LocalCcShards::table_catalogs_` as `CatalogEntry` (`catalog_key_record.h:153`): `schema_`/`dirty_schema_` shared_ptrs + versions, guarded by a `std::shared_mutex`, with monotonic `InitSchema/SetDirtySchema/CommitDirtySchema/RejectDirtySchema`. Shards may transiently disagree on current vs dirty, but there are at most two schema versions per node. DDL is a two-phase schema flow (`AcquireAllCc` on the catalog entry, prepare/commit log records carrying old/new catalog blobs — `tx_operation.cpp:2350`); see [04-transaction-execution.md](04-transaction-execution.md).
- **Schema version checks**: data-path CC requests carry the `schema_version_` the tx resolved; `TemplateCcMap::Execute` rejects mismatches with `CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH` (`cc/template_cc_map.h:214`, `:1482`, `:3477`). `0` means "skip the check".
- `TableKeySchemaTs` (`schema.h`) additionally tracks per-key-schema timestamps (pk + each sk), serialized into the catalog image; a missing sk entry deserializes to ts `1` ("newly added").
- Persistence of the catalog image to the KV store goes through the store handler's catalog operations ([09-store-handler.md](09-store-handler.md)); on a fresh node the catalog cc map is populated from there (or from `prebuilt_tables` passed to `RegisterEngine`).

## 6. The Sequences System Table

`Sequences` (`tx_service/include/sequences/sequences.h`) backs auto-increment columns and range-partition-id allocation. It is an ordinary hash table from the core's perspective: `__sequence_table` (Primary / InternalHash, kv name `ihash_sequence_table`, fixed `seq_schema_version_ = 100`), keyed by `EloqStringKey("autoincr_<table>"` or `"rangeid_<table>")` with an `EloqStringRecord` whose encoded blob packs `{start_val, node_step_val, rec_step_val, curr_val}` (`sequences.cpp::GenKey/GenRecord/EncodeSeqRecord`). Each node caches a `SequenceBatch` per sequence (a leased id range `[curr_id_, range_end_)`); when exhausted, one client advances the sequence via a real transaction (`ApplySequenceBatch`) while concurrent clients wait on the batch's `bthread::ConditionVariable` (`seq_being_advanced_`). Entry points: `ApplyIdOfAutoIncrColumn`, `ApplyIdOfTableRangePartition`, `DeleteSequence` (on drop table). `TableSchema::GetSequenceKeyAndInitRecord` lets DDL seed the sequence row at table creation.

## 7. Statistics (brief)

`Statistics` (`statistics.h`) is the abstract per-table stats holder; the concrete `TableStatistics<KeyT>`/`TemplateCcMapSamplePool<KeyT>` live in `table_statistics.h`. Each cc map keeps a reservoir sample (`RandomPairing`, capacity 1024) fed by realtime insert/delete sampling (the `realtime_sampling` flag wired through the `CcShard` constructor). From the sample, `DistributionSteps<KeyT>` (`distribution_steps.h`) builds an **equal-depth histogram** — at most 128 sorted step values splitting (-inf, +inf) into equal-frequency buckets; `Selectivity(min,max)` interpolates with `PosInInterval`. Stats have a home: `Statistics::LeaderCore`/`LeaderNodeGroup` hash the base table name to a core and node group. Node groups exchange sample pools as `remote::NodeGroupSamplePool` protobufs (`MakeBroadcastSamplePool` → `BroadcastIndexStatistics` in `local_cc_shards.cpp:5750`, applied via `OnRemoteStatisticsMessage`), and `MakeStoreStatistics` snapshots `{records, sample keys}` per index for persistence through the store handler.

## 8. Gotchas and Invariants

- **TxKey ownership bit**: `obj_addr_ & 1` means "owning". Never construct a `TxKey` from a misaligned (odd) pointer; never let two owning `TxKey`s reference one object. Move-assignment into an owning `TxKey` deletes the old object — `Release()` exists precisely to suppress that inside unions.
- **Keys in cc pages are not `TxKey`s**: `CcPage` stores concrete keys by value (`std::vector<KeyT> keys_`, `cc/cc_entry.h:2052+`). Any `TxKey` handed out from a map is a non-owning view whose lifetime is bounded by the page (pages split/merge/defrag — see `NeedsDefrag`). Clone before stashing.
- **Sentinels compare by address**: a deserialized copy of a -inf key is *not* -inf. Always go through `T::NegativeInfinity()` / factory `NegativeInfKey()`; use `PackedNegativeInfinity()` when the value must be serialized (range start keys).
- **Serialization compatibility**: `Serialize()` (length-prefixed, used in CC messages and the WAL) and `KVSerialize()` (raw bytes in the KV store) are distinct formats. `TableName::Serialize` appends the engine byte; `KVCatalogInfo`/`AlterTableInfo`/`TableKeySchemaTs` blobs use host-endian `size_t`/`uint32_t` length prefixes — these images live in the log and the store, so changing them breaks recovery/upgrade.
- **Schema version on every request**: forgetting to set `schema_version_` (leaving 0) silently skips the `REQUESTED_TABLE_SCHEMA_MISMATCH` check; meta-table requests legitimately use 0.
- **Object command ordering invariant**: commands on one object must apply in commit-ts order on top of the exact `obj_version_` they executed against; `BufferedTxnCmdList::EmplaceTxnCmd` asserts two commands with equal commit ts share the same base version, and replay halts (waits) on version gaps.
- **`BlobTxRecord` is a transport container**: most of its virtuals `assert(false)`; it is only safe on flush/forward paths that use `Serialize(std::string&)`/`EncodedBlobData`.
- **`std::hash<TableName>` ignores type and engine** while `operator==` does not — equal hashes across engines are expected, equality is not.
- **Default-constructed `TxKey` is NegativeInf**, not "empty/invalid"; check `KeyPtr() != nullptr` if you need to distinguish.
