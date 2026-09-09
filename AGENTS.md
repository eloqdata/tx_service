# AGENTS.md

Agent guidance for **tx_service**

**Read [docs/README.md](docs/README.md) before unfamiliar architecture work**;
it is the index and authoring standard for the current design.

## Build and test

This library is normally built through a parent project's
`add_subdirectory(data_substrate)`. For standalone unit tests:

```bash
mkdir -p bld && cd bld
cmake ..
cmake --build . --parallel "$(nproc)"
ctest
```

Catch2 tests live in `tx_service/tests/`, which is also a standalone CMake
project. Run one built test binary directly or use `ctest -R <name>`.

Important top-level CMake switches:

- `WITH_DATA_STORE` selects EloqStore (default), a data-store-service RocksDB
  variant, or embedded RocksDB. RocksDB selections constrain `WITH_LOG_STATE`;
  incompatible combinations fail configuration.
- `WITH_LOG_SERVICE` builds the in-tree WAL service.
- `EXT_TX_PROC_ENABLED` lets external brpc workers drive TxProcessors.
- `ELOQ_MODULE_ENABLED` registers the transaction service on brpc workers and
  requires `EXT_TX_PROC_ENABLED`.

## Critical threading boundary

With `ELOQ_MODULE_ENABLED`, a brpc worker's main stack drives its corresponding
TxProcessor/CcShard, while request handlers run as bthreads bound to workers.
Never share a `bthread::Mutex` or `bthread::ConditionVariable` between a
concurrency-control request's `Execute()` path and bthread waiters: wake-up
ordering can permanently deadlock a worker, and a timeout does not make this
safe.

For waitable CC requests, keep completion state in `std::atomic` and poll it
with `bthread_usleep` backoff. `CkptTsCc` and `WaitableCc` in
`tx_service/include/cc/cc_req_misc.h` are the canonical examples. A
single-flight operation with one bthread waiter is structurally safe; a
`std::thread` waiter is also safe. Avoid native-mutex blocking from bthreads on
hot paths even where takeover prevents permanent deadlock. See
[the threading model](docs/02-threading-model.md).

## Proprietary boundary

`tx_service/raft_host_manager/` is proprietary. Keep documentation and
open-source integration at its RPC-visible boundary; do not infer or duplicate
private internals.

## Code style

Use Google C++ style and the project rules in [style_guide.md](style_guide.md).
Format with `clang-format-18`. Use `MyName` for functions/classes,
`my_name` for locals, `my_name_` for members, and `kEnumName` for
enumerators. Keep code in a project namespace and do not add global or static
objects with non-trivial destructors.

## Final delivery

A non-trivial coding task is complete only after implementation, verification,
and reviewer-facing documentation are consistent with the final diff.

- Derive summaries and pull request text from the final merge-base diff, not
  memory or only unstaged changes.
- Report the problem, observable behavior, implementation, material design
  decisions, exact verification performed, risks, rollback, and reviewer focus.
- State unrun checks and uncertainty explicitly; never claim a test passed unless
  it was run in the current workspace.
- In Codex, use `$finish-pr` and `$respond-to-review`; in Claude Code, invoke the
  same shared skills as `/finish-pr` and `/respond-to-review`.

<!-- BEGIN bootstrap-project: engineering-standards -->
## Engineering standards

- Explain non-obvious intent, invariants, ownership, failure behavior, compatibility constraints, and performance or safety tradeoffs near the affected code. Do not restate syntax or names.
- Add or update documentation comments for public APIs when the language supports them.
- Correct stale nearby comments while changing behavior.
- Treat architecture updates as claim-driven. For implementation work, update current architecture only when a changed module boundary, core flow, ownership or lifecycle, durable or wire format, external integration, or system-level invariant makes an existing claim or core model false or materially incomplete. Leave it unchanged when no such claim exists. Dedicated documentation work may correct inaccuracies, fill core-design gaps, or consolidate sediment.
- When a change introduces, removes, splits, or merges a durable module, update the architecture taxonomy, the relevant focused documents, and the architecture index (`docs/architecture/README.md` by default, or its existing equivalent) in the same change.
- Keep the overview focused on system context, high-level flows, cross-cutting invariants, and navigation. A repository with at most one durable module may keep readable architecture detail in its overview. When multiple durable modules emerge or detail needs independent navigation, use focused documents and update the architecture index (`docs/architecture/README.md` by default, or its existing equivalent).
- Keep architecture as a compact, present-tense model of current core design and stable rationale. Put change-specific motivation and before/after explanation in the pull request or commit, and keep local algorithms, representation details, and performance mechanics near the affected code; omit those narrower details from architecture rather than cataloging them as non-architectural.
- Revise and consolidate existing architecture prose so it stands on its own without the change that produced it. Follow the authoring standard linked from `docs/README.md`.
- Treat `docs/plans/` and `docs/superpowers/` as historical change context, not authoritative descriptions of the current code.
- Before unfamiliar work, read `docs/README.md` and the relevant architecture documents.
- When code and documentation disagree, treat code as authoritative and repair the documentation in the same change.
<!-- END bootstrap-project: engineering-standards -->
