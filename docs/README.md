# Data Substrate technical documentation

This directory is the current-architecture authority for Data Substrate (repository name `tx_service`). It describes the API-agnostic transaction engine and its log and storage integrations. Build and contributor instructions remain in the repository [README](../README.md) and [`CLAUDE.md`](../CLAUDE.md).

## Reading order and subsystem map

Read the overview first, then the focused documents for the subsystem in scope.

| Document | Current responsibility | Primary source |
|---|---|---|
| [01 — Architecture overview](01-architecture-overview.md) | System boundary, component ownership, lifecycle and end-to-end data flow | `core/`, `tx_service/include/tx_service.h` |
| [02 — Threading model](02-threading-model.md) | Shard ownership, processor scheduling and cross-context synchronization | `tx_service/include/tx_service.h`, `tx_service/include/tx_service_common.h` |
| [03 — Concurrency control](03-concurrency-control.md) | CC maps and entries, message-driven requests, transactional locks and deadlock handling | `tx_service/include/cc/`, `tx_service/src/cc/` |
| [04 — Transaction execution](04-transaction-execution.md) | Transaction state machine, request adaptation and commit/abort lifecycle | `tx_service/include/tx_execution.h`, `tx_service/src/tx_execution.cpp` |
| [05 — Data model and catalog](05-data-model-and-catalog.md) | API-neutral key/record/command contracts and catalog registration | `tx_service/include/tx_key.h`, `tx_service/include/catalog_factory.h` |
| [06 — Distribution and clustering](06-distribution-and-clustering.md) | Node groups, ownership, terms, remote requests and topology transitions | `tx_service/include/sharder.h`, `tx_service/include/remote/`, `tx_service/include/fault/` |
| [07 — Durability and recovery](07-durability-and-recovery.md) | WAL, checkpoint, recovery and truncation boundaries | `tx_service/include/checkpointer.h`, `tx_service/include/txlog.h`, `tx_service/include/fault/` |
| [08 — Range and bucket management](08-range-and-bucket-management.md) | Partition metadata, range lifecycle, bucket migration and index build | `tx_service/include/cc/range_slice.h`, `tx_service/include/range_record.h` |
| [09 — Store handler](09-store-handler.md) | Persistent-store abstraction and EloqDSS client/service integration | `tx_service/include/store/data_store_handler.h`, `store_handler/` |
| [10 — Log service](10-log-service.md) | Replicated WAL service and the transaction node's log client | `eloq_log_service/`, `tx_service/tx-log-protos/` |
| [Standby replication protocol](standby_replication_protocol.md) | Primary-to-standby change stream, snapshot barrier and replay invariants | `tx_service/include/standby.h`, `tx_service/src/standby.cpp` |

`tx_service/raft_host_manager/` is proprietary. Current documentation may describe only the RPC-visible boundary used by the open components. The EloqStore submodule is likewise documented here only at the `DataStoreHandler`/EloqDSS integration boundary.

## Architecture authoring standard

Architecture is a compact, present-tense model of the current core design. It covers durable module boundaries, primary control and data flows, ownership and lifecycles, durable or wire contracts, external integrations, system-level correctness and safety invariants, and stable rationale.

Freshness is claim-driven. For an implementation change, identify the existing claim or core model that would otherwise become false or materially incomplete. Update architecture only when the change crosses one of those boundaries. Dedicated documentation work may correct an inaccuracy, fill a core-design gap, or consolidate sediment.

When a durable module is introduced, removed, split, or merged, update this subsystem map and the narrowest focused documents in the same change. Keep the overview at system context, high-level flow, cross-cutting invariants, and navigation.

Revise related prose into one coherent current explanation. Do not append a change narrative. Write at the highest useful abstraction and route narrower information to its durable home:

| Information | Home |
|---|---|
| Core responsibility, ownership, lifecycle, cross-module flow, durable or wire contract, system invariant, stable rationale | These current-architecture documents |
| Significant historical decision, alternatives, superseded design or architectural evolution | An ADR or explicitly historical design document |
| Supported procedure, prerequisite or operational safety boundary | Operations documentation |
| Local algorithm, runtime representation, tuning mechanism or code-level invariant | Nearby source or API documentation |
| Change motivation, before/after behavior, one-off benchmark and implementation journey | Pull request or commit |

Every architecture document must end with a `Source map` table that maps its material claims to concrete repository-relative paths. Cite directories only when the claim genuinely spans the directory. Mark an unverified boundary as `Unknown; confirm before documenting` instead of inferring intent from a name. When code and documentation disagree, code is authoritative and the stale claim must be repaired.

Material under `plans/` or `superpowers/`, if introduced, is historical change context rather than current-architecture authority. Its status must be explicit, and implemented design must be reconciled into the numbered architecture set instead of leaving historical material as the only explanation.
