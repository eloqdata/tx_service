# AGENTS.md

Agent guidance for **tx_service** (checked out as EloqKV's `data_substrate`
submodule).

**Read [CLAUDE.md](CLAUDE.md) first** for architecture, build/test commands,
threading constraints, and code style. Read the relevant design document under
`docs/` before changing an unfamiliar module.

## Documentation and delivery

A non-trivial coding task is complete only after implementation, verification,
and reviewer-facing documentation are consistent with the final diff.

### Code comments

- Document non-obvious invariants, lock ordering, memory ordering, reader/writer
  visibility, ownership and object lifetime, WAL durability boundaries, crash
  consistency, retry/idempotency, compatibility constraints, and hot-path
  tradeoffs when relevant. Explain **why**, not syntax.
- Preserve the shard/TxProcessor execution assumptions described in `CLAUDE.md`;
  make cross-context synchronization constraints explicit where code alone is
  insufficient.
- Add documentation comments to new public APIs and externally visible types.
- Do not add comments that merely restate code. Update stale nearby comments and
  the corresponding `docs/` design document when behavior changes.
- Do not document proprietary components beyond the RPC boundaries allowed by
  `CLAUDE.md`.

### Final delivery

- Derive summaries and pull request text from the final merge-base diff, not
  memory or only unstaged changes.
- Report the problem, observable behavior, implementation, material design
  decisions, exact verification performed, risks, rollback, and reviewer focus.
- State unrun checks and uncertainty explicitly; never claim a test passed unless
  it was run in the current workspace.
- Use `$finish-pr` for completed non-trivial changes and `$respond-to-review` when
  addressing review feedback.
