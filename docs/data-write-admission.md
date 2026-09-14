# Data-write admission

`ReadWriteSet::WriteBytes(key, record)` is the accounting rule for `AddWrite`:
serialized key bytes plus serialized record bytes (zero record bytes for deletes).
Every successful call is charged, including repeated writes to an existing key;
this is not the net size of the final map. Catalog writes use `AddCatalogWrite`
and are outside this data-write counter. Exactly 62 MiB is allowed. A rejected
write leaves both the counter and the write set unchanged, and `Reset` clears it.

API engines can read `TransactionExecution::DataWriteSetBytes()` and
`ReadWriteSet::MaxWriteSetBytesCnt` to admit complete logical updates before any
of their writes are buffered. Like direct `TxUpsert`, the accessor requires the
owning API execution context with no request in flight; it introduces no shard
waits or cross-context synchronization. The API engine owns any batching and
commit policy. Neither `AddWrite` nor `TxUpsert` commits implicitly.
