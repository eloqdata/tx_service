# Architecture overview

Data Substrate is an API-agnostic database foundation embedded by API engines such as EloqKV. It combines an in-memory transactional service with pluggable replicated logging and persistent storage. API engines register concrete schema, key, record and command types, then submit `TxRequest`s without coupling the core to Redis, SQL or document semantics.

## Component responsibilities

| Component | Responsibility | Owned boundary |
|---|---|---|
| `core/` | Load configuration, register API engines, construct integrations, and order startup/shutdown | `DataSubstrate` public lifecycle |
| `tx_service/` | Execute transactions over sharded in-memory state, coordinate distribution, checkpointing and recovery | `TxRequest`, catalog and store/log interfaces |
| `store_handler/` | Adapt checkpoint and cache-miss operations to a selected persistent backend | `DataStoreHandler` asynchronous contract |
| EloqDSS | Serve persistent data over brpc or through an in-process path | `ds_request.proto` service contract |
| `eloq_log_service/` | Replicate WAL records and retain them until the engine advances a safe truncation point | log service protobuf and `LogAgent` |
| `eloq_metrics/` | Export process and subsystem metrics | metrics registry and collectors |
| API engine | Own protocol behavior and concrete data semantics | registered `CatalogFactory` and submitted requests |

The proprietary host manager participates through RPC-visible topology and leadership operations; its internal design is outside this documentation boundary.

## Lifecycle and data flow

`DataSubstrate::Init` loads process and cluster configuration. API layers then call `RegisterEngine` with their catalog factory and prebuilt tables. `Start` initializes log, metrics and storage dependencies before constructing and starting the transaction service. Shutdown reverses ownership so transaction work cannot outlive its persistence dependencies.

A foreground request follows this path:

1. An API engine submits a typed `TxRequest` to a `TransactionExecution`.
2. The transaction state machine converts it into local or remote concurrency-control requests.
3. The owning `CcShard` applies requests to catalog or data entries under transactional locking rules.
4. Commit records durable mutations in the log before exposing a successful durable outcome.
5. Checkpoint workers asynchronously flush committed state through `DataStoreHandler`; cache misses load through the same boundary.
6. Recovery combines persisted state with retained WAL. Distribution code fences requests with current ownership and leadership terms.

The engine is asynchronous: an operation that cannot progress yields and is resumed by a later completion or shard event rather than blocking the shard execution context.

## Cross-cutting invariants

- Each `CcShard` has one active execution owner at a time. Other contexts interact by enqueueing requests; direct concurrent shard access would violate the thread-per-core model.
- Transaction locks protect logical consistency and are distinct from thread synchronization. Waiters must not block a shard or brpc worker main stack.
- Leadership terms and ownership versions fence delayed local, remote and log work after topology changes.
- WAL establishes the durability boundary. Checkpoint advancement and log truncation must not move beyond state successfully persisted by every required participant.
- API-specific objects enter the core only through registered type-erased contracts. The engine must not interpret their protocol semantics.
- Storage and log implementations are replaceable behind their contracts; backend-specific retry or deployment mechanics are not transaction semantics unless they affect an exposed guarantee.

## Source map

| Claim | Repository source |
|---|---|
| Data Substrate lifecycle and integration construction | `core/include/data_substrate.h`, `core/src/data_substrate.cpp`, `core/src/log_init.cpp`, `core/src/storage_init.cpp`, `core/src/tx_service_init.cpp` |
| Transaction service, processor and shard ownership | `tx_service/include/tx_service.h`, `tx_service/include/tx_service_common.h`, `tx_service/include/cc/cc_shard.h` |
| Transaction state machine and request boundary | `tx_service/include/tx_request.h`, `tx_service/include/tx_execution.h`, `tx_service/include/tx_operation.h`, `tx_service/src/tx_execution.cpp` |
| Local and remote concurrency-control routing | `tx_service/include/cc/local_cc_handler.h`, `tx_service/include/remote/remote_cc_handler.h`, `tx_service/include/sharder.h` |
| WAL, checkpoint and recovery integration | `tx_service/include/txlog.h`, `tx_service/include/checkpointer.h`, `tx_service/include/fault/`, `tx_service/tx-log-protos/` |
| Persistent-store contract and EloqDSS boundary | `tx_service/include/store/data_store_handler.h`, `store_handler/`, `store_handler/eloq_data_store_service/ds_request.proto` |
| API-neutral catalog/type contracts | `tx_service/include/catalog_factory.h`, `tx_service/include/tx_key.h`, `tx_service/include/tx_record.h`, `tx_service/include/tx_object.h`, `tx_service/include/tx_command.h` |
