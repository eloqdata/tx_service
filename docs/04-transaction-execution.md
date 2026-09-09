# Transaction Execution

`TransactionExecution` is the asynchronous coordinator for one transaction.
API engines submit `TxRequest`s; the coordinator translates each request into
one or more `TransactionOperation`s and advances them with `Forward()`. An
operation waiting on local CC work, a remote node, the log service, or the store
returns control to the owning processor instead of blocking it.

The state machine owns transaction-local coordination state, including the
read/write set, object-command set, open scanners, operation stack, timestamps,
leadership term, and pending results. It does not own table data or locks;
those remain in the destination CC shards described in
[03-concurrency-control.md](03-concurrency-control.md).

## Lifecycle and ownership

A `TxProcessor` creates or reuses a transaction coordinator and keeps it bound
to the processor's shard. Initialization registers transaction identity and
start-time state in the owner shard's transaction table and records the owner
node group's leader term. Requests are then admitted one at a time through the
coordinator's request queue.

`Forward()` processes the top operation until that operation either advances
the stack or remains pending. When no operation is active, it dequeues the next
request and lets the request install its operations. A commit or abort request
drives final post-processing; after all CC ownership is released, `Reset()`
clears transaction-local state and the coordinator returns to its processor's
reuse pool.

An object command that resets a live TTL may retain a full-object recovery
image in its result. Ordinary operation completion preserves that result until
the final reply and durability consumers have copied it. Terminal transaction
reset then releases the image before returning the coordinator to its pool,
including when WAL is disabled. WAL records and standby forwarding retain
independently owned copies.

Submitted request storage must remain live until its result completes. The
coordinator owns the reusable operation state and all buffered write/command
material for the transaction lifetime. Records and keys moved into write sets
remain owned there until commit/abort cleanup; read-set entry addresses remain
valid only because the CC layer retains the corresponding lock, intent, or pin.

## Local and remote execution

`CcHandler` is the state machine's concurrency-control boundary.
`LocalCcHandler` enqueues work to local shards; remote routing sends equivalent
requests to the current owner node. Both paths complete `CcHandlerResult`
objects, which aggregate fan-out and allow the same operation logic to wait for
multiple shards or node groups.

Only one context advances a coordinator at a time. A shard latch protects the
processor-owned state machine, while `forward_latch_` arbitrates between
`Forward()` and remote stream delivery updating an in-flight result. Responses
carry the transaction number and a monotonically changing command identifier;
late responses from a timed-out or retried operation are rejected instead of
mutating reused result state.

The transaction's node-group term fences transaction progress and destination
CC requests. A leadership change causes them to fail rather than commit under
stale ownership. Retriable remote-operation failures may re-run an
operation after routing changes, while the transaction identity and command
identifier keep old replies separate.

## Transaction footprint

`ReadWriteSet` records semantic reads, metadata reads, buffered record writes,
schema versions, and the node-group terms touched by locks. `CommandSet`
records object commands that executed on owner shards and the version/timestamp
facts needed to log and commit them. These collections are the source of
commit-time acquisition, validation, WAL construction, installation, and
cleanup.

Closing a scanner releases scanner machinery but does not discard reads whose
locks or intents are still required by the transaction. Those reads remain in
the transaction footprint until commit validation or abort cleanup. Thus scan
batch and cursor lifetimes are narrower than transaction-level CC ownership.

## Commit and abort protocol

The full decision path consists of these logical stages; a transaction skips a
stage that is irrelevant or was already satisfied while executing a command:

1. Read-lock the range or bucket metadata that determines placement of every
   write, so routing cannot change underneath commit.
2. Acquire write ownership for buffered record keys at their local or remote CC
   shards. Object commands retain the owner-shard write ownership established
   during execution and acquire any additional forwarding ownership they need.
3. Choose a commit timestamp above the transaction's start/lower bound and the
   version, lock, and read-validation bounds returned by touched entries.
4. Validate optimistic reads and release the data-read ownership that no
   longer needs to survive the decision.
5. If data WAL is enabled, serialize the record values or object-command images
   and append the transaction's data log. A successful append establishes the
   durable commit decision. An indeterminate log result is surfaced as an
   unknown transaction outcome for recovery to resolve.
6. Record the transaction outcome, install committed values or commands (or
   discard pending state on abort), release write and metadata ownership, and
   wake blocked requests.

Read-only transactions skip write acquisition and logging but still validate
and release any retained reads. Catalog-only writes use replicated
acquire-all/post-write-all operations. Schema changes, range management, and
cluster migration are composite operations built on the same stack and result
model, but their subsystem-specific protocols live in the corresponding
architecture documents rather than here.

Abort uses the same post-processing ownership map: every successfully acquired
read, intent, write lock, catalog lock, and placement lock is released through
CC requests. A recovering coordinator may start with an already determined
transaction identity and commit timestamp so it can resolve locks left by the
original coordinator; see
[07-durability-and-recovery.md](07-durability-and-recovery.md).

## Isolation model

Transactions select an isolation level and CC protocol at initialization.
`ReadCommitted`, `RepeatableRead`, and `Serializable` can use optimistic,
optimistic-read/pessimistic-write, or locking control; snapshot reads require
MVCC-capable optimistic reads. The CC layer derives concrete lock modes from
this pair, while the transaction layer retains the versions needed for
validation.

Node-local reads are reserved for metadata replicated locally. They still take
the metadata ownership required to serialize catalog, range, bucket, and
cluster changes. The local shortcut changes routing, not the consistency
contract.

## Invariants

- `Forward()` never waits synchronously for an asynchronous operation; pending
  operations yield the processor.
- A coordinator is advanced by one context at a time, and remote result
  delivery cannot race result timeout or reuse.
- Every remote response is matched to both transaction identity and command
  generation.
- Placement and schema metadata used by a write remain protected through its
  commit decision and post-processing.
- A commit is not exposed as successful before its required WAL decision.
- Object-command recovery images remain live through reply/log consumption but
  are released at terminal transaction reset.
- Reset/reuse occurs only after all transaction-owned CC state has been
  released or handed to recovery.

## Source map

| Claim | Repository source |
|---|---|
| `TransactionExecution` owns the request queue, operation stack, transaction footprint, and fencing state | `tx_service/include/tx_execution.h`; `tx_service/src/tx_execution.cpp` |
| `Forward()` advances operations without blocking the processor | `tx_service/include/tx_execution.h`; `tx_service/src/tx_execution.cpp` (`TransactionExecution::Forward`) |
| Processors create, bind, recycle, and exclusively advance coordinators | `tx_service/include/tx_service.h`; `tx_service/include/tx_service_common.h` |
| Local and remote CC implementations share the `CcHandler` contract | `tx_service/include/cc/cc_handler.h`; `tx_service/include/cc/local_cc_handler.h`; `tx_service/include/remote/remote_cc_handler.h` |
| Remote delivery is fenced by transaction number, command id, and the forward latch | `tx_service/include/tx_execution.h`; `tx_service/src/remote/cc_stream_receiver.cpp` |
| Read/write and object-command footprints drive commit and cleanup | `tx_service/include/read_write_set.h`; `tx_service/include/command_set.h`; `tx_service/include/read_write_entry.h` |
| The commit pipeline acquires placement and write ownership, sets a timestamp, validates, logs, and post-processes | `tx_service/src/tx_execution.cpp`; `tx_service/include/tx_operation.h`; `tx_service/src/tx_operation.cpp` |
| WAL failures distinguish abort from an indeterminate outcome | `tx_service/src/tx_execution.cpp` (`Process`/`PostProcess(WriteToLogOp&)`) |
| Object-command recovery images outlive operation completion and are released at terminal reset | `tx_service/include/tx_operation_result.h`; `tx_service/include/tx_execution.h`; `tx_service/src/tx_execution.cpp` |
| Lock modes derive from isolation level and CC protocol | `tx_service/include/cc_protocol.h`; `tx_service/src/tx_execution.cpp` |
| Transaction and cross-node commit behavior has integration coverage | `tx_service/tests/TxConsistency-Test.cpp`; `tx_service/tests/ClusterCrossNg-Test.cpp`; `tx_service/tests/TestNodeSmoke-Test.cpp` |
