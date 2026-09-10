# Standby Replication Protocol

Standby replication maintains a warm in-memory copy of a node group's (NG's)
committed state on data-bearing candidate nodes. It combines a storage snapshot
with a continuously sequenced stream of committed commands. The snapshot gives
the standby a durable base; version-aware stream replay closes the gap without
pausing writes on the primary.

This protocol is distinct from WAL recovery. WAL remains the commit and leader
recovery authority; standby replication reduces recovery work by keeping an
additional in-memory replica close to current state.

## Roles and session identity

The NG leader is the primary for a standby session. A following node progresses
through two states:

- A candidate standby has registered a subscription but has not installed its
  storage snapshot. The primary retains the stream history it may need.
- A full standby has installed the snapshot and has a consistent incremental
  replay position. It may later provide a warm base for leader recovery.

Each subscription has a standby term derived from the primary term and a unique
subscription id. That term fences stream messages, snapshot callbacks, and
checkpoint updates from an older session. Leadership change or re-subscription
creates a new identity; delayed work from the prior identity is discarded.

## Capture and sequencing

Committed object changes are captured on the owner shard's apply path. A
forwarded command represents the command after owner-side execution, because
the standby performs commit replay and does not rerun the command's execution
phase. Whole-object replacement is available for changes that cannot safely be
expressed as an incremental command.

Each `CcShard` is an independent sequence group and assigns monotonically
increasing forward sequence ids. This provides FIFO ordering within a shard and
therefore for a key, since one key has one owner shard. There is no total order
across shards; consumers must use commit timestamps and object versions rather
than infer cross-shard serialization from arrival order.

The primary sends through the existing CC stream transport. Successful stream
write is not an application acknowledgement. The standby tracks gaps, while the
primary retains messages needed by a failed send or a bootstrapping candidate.

## Bounded backlog and resynchronization

The primary-side history is a bounded recovery backlog, not an unbounded audit
log. Entries that every subscriber has passed and no candidate still needs are
released. If a send fails, the primary retries from the subscriber's next
sequence id while the required entries remain buffered.

Memory pressure may evict an entry that a subscriber still needs. In that case
the primary sends an explicit out-of-sync indication and stops advancing the
affected session. The standby must create a new subscription and install a new
snapshot; silently skipping the missing sequence is never valid.

This bounded design prevents an unavailable standby from consuming unbounded
primary memory, at the cost of requiring full re-bootstrap after excessive lag.

## Subscription and snapshot bootstrap

Bootstrap overlaps snapshot creation with incremental forwarding:

1. The standby registers with the primary and receives a start sequence for
   every shard.
2. The primary records the candidate at those positions. Resetting the sequence
   state activates streaming and establishes the subscription barrier.
3. Checkpointing advances the storage image past that barrier, after which
   `SnapshotManager` creates and transfers a snapshot for the NG.
4. The standby installs the snapshot, reconciles the overlapping stream, and is
   promoted to full standby only after snapshot completion.

The subscription barrier is a lower bound, not an exact snapshot timestamp. It
ensures the snapshot includes writes committed before subscription, but ongoing
checkpoint work may also include later writes. Streaming begins before snapshot
installation, so snapshot and stream intentionally overlap.

Version-aware replay makes this fuzzy snapshot safe: effects already present in
the snapshot are skipped, while effects whose base version has not arrived are
buffered until the base record can be fetched or installed. A consumer that
cannot compare commit timestamps and object versions cannot safely consume this
snapshot-plus-stream protocol directly.

## Standby apply and consistency

Incoming messages are routed to their sequence group's shard and applied
through the same CC-map path with standby semantics. Each shard tracks the next
expected id, gaps, and the last contiguous sequence. Only the contiguous prefix
advances that shard's consistent timestamp; later out-of-order messages do not
make an earlier gap invisible.

Replay is version-aware and idempotent. A command whose effect is already
represented by an equal or newer local version is skipped. A command that
depends on a missing base version waits with the entry until storage fill makes
ordered replay possible. Whole-object replacement resets that dependency when
the protocol explicitly marks the message as an overwrite.

The primary also broadcasts its checkpoint progress. Standbys combine that
watermark with their contiguous stream state for cache and failover decisions;
term validation prevents checkpoint information from one session advancing
another.

## Failover boundary

Cluster leadership owns the transition from standby to candidate leader. A full
standby can retain its warm cache and request WAL replay after its minimum
consistent standby timestamp. A candidate standby whose snapshot is incomplete
cannot be trusted as a recovery base and falls back to cold recovery.

The serving leader term is still published only after every log group completes
WAL replay. Standby state reduces the replay interval but does not weaken the
term or all-log-groups recovery barriers described in
[06-distribution-and-clustering.md](06-distribution-and-clustering.md) and
[07-durability-and-recovery.md](07-durability-and-recovery.md).

## Protocol invariants

- Ordering is FIFO per shard and per key; no cross-shard total order exists.
- Session terms fence every stream and snapshot action from stale primaries or
  prior subscriptions.
- Snapshot and stream overlap is expected and requires version-aware,
  idempotent replay.
- The primary backlog is memory-bounded. Missing evicted history produces an
  explicit out-of-sync state and full re-bootstrap.
- A standby consistent timestamp advances only with the contiguous sequence
  prefix.
- Full-standby status requires snapshot installation; stream subscription alone
  is not a complete recovery base.
- Standby replication accelerates failover but never replaces WAL's commit
  decision or final recovery fence.

## Source map

| Claim | Repository source |
|---|---|
| Standby entries, sequence groups, session terms, and checkpoint broadcasts define the protocol state | `tx_service/include/standby.h`; `tx_service/src/standby.cpp` |
| Committed commands are captured on the owner-side apply path with standby replay semantics | `tx_service/include/cc/object_cc_map.h`; `tx_service/tests/StandbyForward-Test.cpp` |
| Per-shard sequencing, bounded buffering, retry, gap tracking, and out-of-sync handling live in `CcShard` | `tx_service/include/cc/cc_shard.h`; `tx_service/src/cc/cc_shard.cpp` |
| Subscription, sequence reset, snapshot, and checkpoint RPCs cross the CC control plane | `tx_service/include/proto/cc_request.proto`; `tx_service/src/remote/cc_node_service.cpp` |
| Stream receive routes standby messages back to their owning shard | `tx_service/src/remote/cc_stream_receiver.cpp`; `tx_service/src/remote/cc_stream_sender.cpp` |
| Following and standby-to-leader transitions are coordinated by `CcNode` | `tx_service/include/fault/cc_node.h`; `tx_service/src/fault/cc_node.cpp` |
| Snapshot transfer waits on the subscription/checkpoint barrier | `tx_service/include/store/snapshot_manager.h`; `tx_service/src/store/snapshot_manager.cpp`; `tx_service/src/checkpointer.cpp` |
| WAL replay remains the final leader-recovery fence | `tx_service/include/fault/log_replay_service.h`; `tx_service/src/fault/log_replay_service.cpp`; `tx_service/src/fault/cc_node.cpp` |
