# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repo Is

Data Substrate (GitHub repo `eloqdata/tx_service`) is EloqData's API-agnostic database foundation: a distributed in-memory transaction/cache layer plus pluggable WAL and storage services. It is consumed as a git submodule by API layers — EloqKV (Redis), EloqDoc (MongoDB), EloqSQL (MySQL) — which translate protocol commands into `TxRequest`s defined here.

## Technical Docs — Read These First

`docs/` contains module-by-module design documentation (see `docs/README.md` for the index). **Before working on an unfamiliar module, read its doc** — it explains the design, flows, and invariants far faster than reading the headers: `01` overview/init, `02` threading model, `03` concurrency control, `04` transaction execution, `05` data model/catalog, `06` distribution/clustering, `07` durability/recovery, `08` range/bucket management, `09` store handler, `10` log service.

**Maintenance rule: when a code change alters behavior described in `docs/`, update the corresponding doc in the same change.** Each doc lists the source files it covers. Do not document the proprietary components (`eloq_log_service/`, `tx_service/raft_host_manager/`) beyond their RPC interface boundaries.

## Build & Test

This library is normally built as part of a parent project (the parent's CMake runs `add_subdirectory(data_substrate)`, so building eloqkv/eloqdoc/eloqsql builds this too). Standalone build for unit testing:

```bash
mkdir bld && cd bld
cmake ..
cmake --build . -j
ctest
```

Unit tests are Catch2-based, in `tx_service/tests/` (e.g. `CcEntry-Test.cpp`, `CcPage-Test.cpp`); that directory is its own CMake project. Run a single test binary directly from the build tree, or `ctest -R <name>`.

Key CMake options (top-level `CMakeLists.txt`):
- `WITH_DATA_STORE` — storage backend: `ELOQDSS_ELOQSTORE` (default), `ELOQDSS_ROCKSDB`, `ELOQDSS_ROCKSDB_CLOUD_S3/GCS`, `DYNAMODB`, `BIGTABLE`. RocksDB-based choices force a matching `WITH_LOG_STATE`; a mismatch is a fatal configure error.
- `WITH_LOG_SERVICE` / `OPEN_LOG_SERVICE` — build with the WAL log service; `OPEN_LOG_SERVICE` selects `log_service/` (open) vs `eloq_log_service/`.
- `EXT_TX_PROC_ENABLED` — external (brpc worker) threads may drive TxProcessors.
- `ELOQ_MODULE_ENABLED` — register the tx service as an Eloq module on brpc workers (requires `EXT_TX_PROC_ENABLED`).

## Architecture

Top-level layout:
- `core/` — `data_substrate` library glue: init/teardown of tx service, log service, storage, and metrics (`core/src/data_substrate.cpp` plus `*_init.cpp`). Parent projects call into this.
- `tx_service/` — the engine (namespace `txservice`). The most important directory.
- `store_handler/` — persistent storage backends behind `store::DataStoreHandler` (`kv_store.h`): `rocksdb_handler`, `dynamo_handler`, `bigtable_handler`, and `data_store_service_client` which talks to the **EloqDSS** standalone data-store service in `store_handler/eloq_data_store_service/` (its own brpc server, EloqStore or RocksDB backed).
- `log_service/`, `eloq_log_service/` — replicated (braft) WAL services; protobuf definitions shared via `tx-log-protos/`.

Inside `tx_service/`:
- **Data & CC**: data is sharded into `CcShard`s (`include/cc/cc_shard.h`), each containing `CcMap`s of `CcEntry`s. Each shard is owned by one `TxProcessor` (`include/tx_service.h`) pinned to a core; all access to a shard happens by enqueueing `CcRequest`s (`include/cc/cc_request.h`, `cc_req_misc.h`) whose `Execute()` runs on that shard — a message-passing, mostly lock-free design. Locking/isolation is via `non_blocking_lock.h`; OCC and 2PL protocols, ReadCommitted/RepeatableRead isolation (`cc_protocol.h`).
- **Transactions**: API layers submit `TxRequest`s (`tx_request.h`) to a `TransactionExecution` state machine (`tx_execution.h`, `tx_operation.h`), which issues CC requests through `local_cc_handler.h` (same node) or `remote/remote_cc_handler.h` (other nodes).
- **Distribution**: `sharder.h` tracks cluster topology / node-group leadership (raft via `raft_host_manager`); `remote/` has the brpc/streaming RPC plumbing (`cc_node_service`, `cc_stream_sender/receiver`); standby replication in `standby.h` (protocol doc: `docs/standby_replication_protocol.md`); range/bucket splitting under `range_*` files.
- **Durability**: `checkpointer.h` flushes dirty entries to the store handler; `txlog.h` / `tx_log_service.h` write WAL to the log service; recovery replays logs.
- **Misc**: `dead_lock_check.h`, `catalog_factory.h` (API layers plug in their schema/record types), `fault/` (fault injection for Debug builds), `metrics` via `eloq_metrics/`.

## Threading Model — Critical Gotcha

The tx service runs on a **custom brpc fork** (bthreads = coroutines). With `ELOQ_MODULE_ENABLED`, brpc worker N's main stack drives TxProcessor/CcShard N directly (1:1 binding) via `ProcessModulesTask()`; bthreads are bound to workers and `butex_wake` prefers bthread waiters over pthread waiters.

Consequence: a `bthread::Mutex`/`bthread::ConditionVariable` that is locked inside a CC request's `Execute()` (shard/main-stack context) **and** waited on by bthreads can permanently deadlock a worker when multiple shards/requests acquire it concurrently — the wake is routed to a bthread queued on the very worker that is blocked. Timeouts do not rescue this state.

Rules:
- In CC requests that need a completion wait, hold state in `std::atomic` and have `Wait()` poll with `bthread_usleep` backoff (canonical examples: `CkptTsCc`, `WaitableCc` in `cc_req_misc.h`).
- Single-flight execution with a single bthread waiter is structurally safe; so is a `std::thread` (pthread) waiter.
- `std::mutex` sync waits from bthreads stall a worker but don't deadlock (the native TxProcessor thread takes the shard over after ~2s); still avoid them on hot paths.

## Code Style

Google C++ style with project-specific rules in `style_guide.md` (read it before non-trivial changes). clang-format (`.clang-format` at repo root; clang-format-18 in CI). Naming: functions/classes `MyName`, locals `my_name`, members `my_name_`, enumerators `kEnumName`. Code lives in namespaces (`txservice`, `txlog`, ...). No global/static objects with non-trivial destructors.
