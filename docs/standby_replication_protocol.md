# Standby Replication Protocol

This document describes the design and implementation of Data Substrate's
internal primary–standby replication: how committed writes on a node group
leader (the *primary*) are captured, sequenced, buffered, and streamed to
*standby* nodes, and how standbys bootstrap from a storage snapshot and
replay the stream idempotently.

Code references are given as `path:line` based on commit `887e352`. Line
numbers drift as the code evolves; symbol names are authoritative.

## 1. Overview

Standby replication keeps a near-real-time in-memory replica of a node
group on its standby nodes. It is built from three cooperating mechanisms:

1. **Incremental forwarding** — the primary captures every committed write
   at the ccmap apply/commit point and streams it to subscribed standbys as
   a per-shard ordered sequence.
2. **Snapshot bootstrap** — a new standby obtains its base state from a
   storage-layer snapshot (e.g., a RocksDB checkpoint).
3. **Idempotent replay** — the standby applies forwarded commands with
   `commit_ts` / `object_version` checks, which absorbs both the overlap
   between snapshot and stream and any out-of-order arrival.

Standby state machine:

```
                StandbyStartFollowing            ResetStandbySequenceId
  (none) ──────────────────────────▶ candidate ─────────────────────────▶ receiving stream
                                                                    (candidate standby term)
                                                                              │
                                                    snapshot fully received   │ OnSnapshotReceived
                                                                              ▼
                                                                      full standby
                                                                      (standby term)
```

Note that the primary starts streaming increments **before** the snapshot
transfer completes (§5, §6); the overlap is resolved on the standby by
versioned replay.

## 2. Roles and Terms

| Term | Meaning |
|---|---|
| Primary | Node group leader; source of the replication stream. |
| Candidate standby | A standby that has registered for the stream but has not finished snapshot bootstrap. The primary **buffers** messages for candidates without sending. |
| Subscribed standby | A standby actively receiving the forward stream. |
| Sequence group | The ordering domain of the stream. One per CPU core (= one per `CcShard`); group id equals `core_id_`. |
| Standby term | `(primary_term << 32) | subscribe_id`; identifies one subscription session end-to-end. |
| Out-of-sync | Signal from primary to standby that buffered messages it still needed were evicted; the standby must re-subscribe and re-bootstrap. |

## 3. Message Format

The forward message is `KeyObjectStandbyForwardRequest`
(`tx_service/include/proto/cc_request.proto:345`):

| Field | Description |
|---|---|
| `key` | Object key bytes. |
| `cmd_list` | Serialized `TxCommand`s. First byte is the engine command type; decoded by the engine via `TableSchema::CreateTxCommand`. |
| `commit_ts` | Commit timestamp of this write. |
| `object_version` | Object version **before** the command applied; the standby validates its replay base against this. |
| `has_overwrite` | The command list replaces the whole object (retire/recover etc.); replay discards prior state. |
| `forward_seq_id`, `forward_seq_grp` | Monotonic per-shard sequence number and its sequence group (= core id). |
| `table_name`, `table_type`, `table_engine`, `key_shard_code` | Routing and decoding metadata. |
| `primary_leader_term` | Term fencing. |
| `out_of_sync` | When set, this is a control message telling the standby it has fallen behind irrecoverably. |

Related RPCs (same file, `:530-534`): `StandbyStartFollowing`,
`ResetStandbySequenceId`, `RequestStorageSnapshotSync`, `OnSnapshotSynced`,
`UpdateStandbyCkptTs`.

## 4. Primary Node

### 4.1 Change capture

Capture happens on the `ApplyCc` execution path in
`tx_service/include/cc/object_cc_map.h` (approx. `:871-1100`). When a write
command applies to an object and the shard has subscribers
(`shard_->GetSubscribedStandbys()` non-empty), the command is packed into
the cc entry's `StandbyForwardEntry`:

- Regular commands: `StandbyForwardEntry::AddTxCommand`
  (`tx_service/src/standby.cpp:72`). Local commands are serialized via
  `cmd->Serialize()`; remote commands reuse the already-serialized
  `CommandImage()`.
- Internally generated full-object changes (TTL retirement, recovery):
  `AddOverWriteCommand`, which sets `has_overwrite`.
- Once the commit timestamp is known, the entry is finalized and handed to
  `shard_->ForwardStandbyMessage()` (`object_cc_map.h:947-965`).

Because every key is owned by exactly one shard and applies execute
serially on the shard's thread, the capture point observes **all** writes
to a key — including writes initiated by other nodes in a distributed
transaction — in apply order.

### 4.2 Sequencing and delivery

`CcShard::ForwardStandbyMessage` (`tx_service/src/cc/cc_shard.cpp:3142`):

- Each shard assigns a monotonically increasing
  `next_forward_sequence_id_`. **Ordering is guaranteed only within a
  shard**; there is no total order across shards.
- For each node in `subscribed_standby_nodes_`, the message is written to
  the brpc stream (`CcStreamSender::SendStandbyMessageToNode`) only if that
  node's `last_sent_seq_id == seq_id - 1`; a successful write advances
  `last_sent_seq_id`.
- "Sent" means **the stream write succeeded**, not that the standby
  acknowledged application. Loss is detected by the standby through
  sequence gaps (§6).

### 4.3 Forward-message buffer lifecycle

The primary-side buffer is `history_standby_msg_` (a deque) plus
`seq_id_to_entry_map_` (`tx_service/include/cc/cc_shard.h:1322-1337`).
Entries are *not* retained until the buffer fills; the lifecycle is:

1. **Fast path — never buffered.** If the message was written successfully
   to every subscribed standby and no candidate needs it (no candidate with
   `start_seq_id <= seq_id`), the entry is freed at the end of
   `ForwardStandbyMessage` without entering the buffer.
2. **Buffered when needed.** An entry enters the buffer only if some
   subscriber's stream write failed (pending retry) or a candidate is
   catching up.
3. **Watermark cleanup.** `CheckAndFreeUnneededEntries`
   (`cc_shard.cpp:3058`) computes
   `min(last_sent_seq_id + 1 over subscribers, start_seq_id over candidates)`
   and frees everything older. Invoked after a retry round
   (`ResendFailedForwardMessages`), on unsubscribe
   (`RemoveSubscribedStandby`), and when a candidate is promoted
   (`AddSubscribedStandby`).
4. **Memory-cap eviction.** Total buffered size is capped by
   `standby_buffer_memory_limit_` (~10% of node memory per shard). On
   overflow the oldest entries are evicted; if an evicted sequence is later
   needed, the affected node is declared out-of-sync (§4.4).

### 4.4 Retry and out-of-sync

- `ResendFailedForwardMessages` (`cc_shard.cpp:3294`) resends from
  `last_sent_seq_id + 1` using the buffer, at most 500 messages per round
  to avoid starving the shard.
- If a needed sequence id is missing from `seq_id_to_entry_map_` (evicted),
  `NotifyStandbyOutOfSync` (`cc_shard.cpp:3358`) sends a control message
  with `out_of_sync=true`, sets that node's `last_sent_seq_id` to
  `UINT64_MAX` (stop sending), and propagates the stop to all other shards.
  The standby must re-subscribe and re-bootstrap. Semantically this is the
  analogue of a replication-backlog overflow forcing a full resync.

## 5. Subscription Lifecycle

The standby-side driver lives in `tx_service/src/fault/cc_node.cpp`
(approx. `:900-1240`). Full sequence:

```
standby                                    primary
   │  StandbyStartFollowing                   │
   │─────────────────────────────────────────▶│ per shard: start_seq = NextForwardSequenceId()
   │◀─────────────────────────────────────────│           AddCandidateStandby(node, start_seq)
   │  resp: start_seq per shard, subscribe_id │           (candidate phase: buffer only, no send)
   │                                          │
   │  local: SubsribeToPrimaryNode(seq grps)  │
   │  standby_term = (primary_term<<32)|subscribe_id
   │  SetCandidateStandbyNodeTerm             │
   │                                          │
   │  ResetStandbySequenceId                  │
   │─────────────────────────────────────────▶│ per shard: RemoveCandidateStandby
   │                                          │           AddSubscribedStandby(node, start_seq, term)
   │                                          │           (cc_node_service.cpp:2227 — streaming begins)
   │                                          │ register subscription barrier (§6)
   │◀━━━━━━ forward stream (continuous) ━━━━━━│
   │                                          │
   │  (in parallel) snapshot transfer:        │
   │  RequestStorageSnapshotSync ────────────▶│ ship snapshot once checkpoint passes the barrier
   │◀────── snapshot files (rsync / shared) ──│
   │  OnSnapshotSynced / OnSnapshotReceived   │
   │  SetStandbyNodeTerm (full standby)       │
```

Key points:

- During the candidate phase the primary **buffers but does not send**.
- Streaming starts at `ResetStandbySequenceId`, **before** the snapshot has
  been transferred; the standby absorbs the stream while still a candidate.
- The standby term `(primary_term << 32) | subscribe_id` fences every
  message and RPC of the session.

## 6. Snapshot Synchronization and the Subscription Barrier

The snapshot is **not** a point-in-time cut at the subscription moment. Its
contract (see `cc_node_service.cpp:2252-2269` and
`tx_service/src/store/snapshot_manager.cpp`):

- **Lower bound (guaranteed).** On `ResetStandbySequenceId` the primary
  computes the max commit timestamp of active transactions across shards
  (`ActiveTxMaxTsCc`) and registers it as the *subscription barrier*
  (`RegisterSubscriptionBarrier`). The snapshot is shipped only after the
  checkpoint has advanced past the barrier, so it contains every write
  committed before the subscription moment.
- **Upper bound (none).** Writes continue while the checkpoint runs, so the
  snapshot may additionally contain data committed *after* the subscription
  moment ("fuzzy snapshot").

The overlap between the fuzzy snapshot and the forward stream is harmless
to internal standbys because replay is version-checked (§7). Consumers
without version-checking ability (e.g., a vanilla Redis replica) cannot
consume this snapshot + stream directly; alignment must then be solved on
the producer side (see invariant P5).

Snapshot transport depends on the storage backend: rsync of a RocksDB
checkpoint (`RequestStorageSnapshotSync`, `cc_node_service.cpp:1984`), or a
path handoff for shared/cloud storage (`snapshot_path` in the
`StandbyStartFollowing` response).

## 7. Standby Node

- **Receive path.** Messages arrive in
  `tx_service/src/remote/cc_stream_receiver.cpp` (`KeyObjectStandbyForwardCc`,
  approx. `:1947`) and are routed to the shard given by `forward_seq_grp`.
- **Sequence tracking.** Each shard keeps a `StandbySequenceGroup`
  (`tx_service/include/standby.h:83`): `next_expecting_standby_sequence_id_`,
  `last_consistent_standby_sequence_id_`, and
  `missing_standby_seqeunce_ids_` (the gap set).
  `UpdateLastReceivedStandbySequenceId` (`cc_shard.cpp:3452`) maintains
  contiguity; the contiguous prefix advances
  `last_standby_consistent_ts_`, the standby's consistent-read watermark.
- **Idempotent replay.** Forwarded commands run through the same `ApplyCc`
  path flagged as `is_standby_tx`. The standby compares the message's
  `object_version`/`commit_ts` with the local cc entry version: already
  reflected commands are skipped. If the base version is not yet present
  (snapshot not loaded, object not fetched), commands are parked in the cc
  entry's `BufferedTxnCmdList` (`tx_service/include/cc/cc_entry.h:348`) and
  replayed in version order after `FetchRecord` brings in the base.
- Messages with `has_overwrite` rebuild the object from scratch, removing
  any dependency on prior state.

## 8. Checkpoint Broadcast and Heartbeats

- The primary periodically broadcasts its checkpoint timestamp
  (`BrocastPrimaryCkptTs`, `tx_service/include/standby.h:136`); standbys use
  it to advance local truncation/read watermarks and report back via the
  `UpdateStandbyCkptTs` RPC.
- Subscriptions are tied to heartbeat target management
  (`AddHeartbeatTargetNode` / `RemoveHeartbeatTargetNode`); heartbeat loss
  triggers cleanup.

## 9. Protocol Invariants

Properties that external consumers (replication bridges, CDC, migration
tooling) can rely on — or must explicitly work around:

- **P1 — Per-shard FIFO.** Sequence numbers are monotonic per shard and
  messages are delivered in sequence order within a shard. No cross-shard
  total order exists.
- **P2 — Per-key ordering.** A key is owned by exactly one shard, so the
  forward order for a single key equals its apply order and its
  `commit_ts` order.
- **P3 — At-least-once + versioned idempotency.** Delivery is
  at-least-once (retries may duplicate). Correctness relies on the consumer
  performing version-checked replay using `commit_ts`/`object_version`.
  The protocol is **not** directly usable by consumers that apply blindly.
- **P4 — Bounded buffer with explicit desync.** The primary-side buffer is
  memory-capped; overflow produces an explicit out-of-sync signal, and the
  consumer must support full re-bootstrap.
- **P5 — Fuzzy snapshot.** The bootstrap snapshot guarantees only a lower
  bound (everything before the subscription barrier). Exact alignment with
  the stream requires either consumer-side version filtering or
  producer-side deduplication (record-level scan capturing per-key
  versions, with prefix filtering by `commit_ts`, or whole-object
  overwrite semantics).
- **P6 — Stream-before-snapshot.** Streaming begins as soon as the
  subscription is reset, in parallel with the snapshot transfer; buffering
  pressure during bootstrap sits on the standby (`BufferedTxnCmdList`).

## 10. Source Code Index

| Topic | Location |
|---|---|
| Forward entry / sequence group types | `tx_service/include/standby.h` |
| Command serialization into entries | `tx_service/src/standby.cpp` (`AddTxCommand`, `AddOverWriteCommand`) |
| Capture point (apply/commit) | `tx_service/include/cc/object_cc_map.h:871-1100` |
| Send / buffer / retry / out-of-sync | `tx_service/src/cc/cc_shard.cpp:3039-3460` |
| Subscription state and APIs | `tx_service/include/cc/cc_shard.h:1064-1155, 1322-1347` |
| Subscription RPC handlers | `tx_service/src/remote/cc_node_service.cpp` (`StandbyStartFollowing:1752`, `ResetStandbySequenceId:2201`, `RequestStorageSnapshotSync:1984`) |
| Standby-side subscription driver | `tx_service/src/fault/cc_node.cpp:900-1240` |
| Subscription barrier | `tx_service/src/store/snapshot_manager.cpp` (`RegisterSubscriptionBarrier:464`, `GetSubscriptionBarrier:630`) |
| Receive and replay entry points | `tx_service/src/remote/cc_stream_receiver.cpp`; `tx_service/include/cc/cc_entry.h` (`BufferedCommandList:348`) |
| Message and RPC definitions | `tx_service/include/proto/cc_request.proto:304-385, 530-534` |
