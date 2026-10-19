# Log service module

`eloq_log_service/` implements Data Substrate's replicated WAL service. It owns the braft-backed log groups and the server side of the log protocol; transaction nodes use the client and protobuf contract under `tx_service/tx-log-protos/`.

The current design, durability boundary and source map are maintained in [`docs/10-log-service.md`](../docs/10-log-service.md). Build integration is defined by `build_eloq_log_service.cmake`, and module tests live under `test/`.

## Snapshot regression tests

The selected `WITH_LOG_STATE` backend's snapshot suite is included in a parent
build configured with `WITH_TESTS=ON` and `WITH_LOG_SERVICE=ON`:

```sh
cmake --build bld --target log_snapshot_tests
ctest --test-dir bld -R '^log_snapshot_tests$' --output-on-failure
```

For a standalone `eloq_log_service` configure, set `TEST_LOG_SNAPSHOT=ON` to
build the same suite. This option defaults to OFF; the default local RocksDB
`TEST_LOG_SERVICE` build does not require Catch2.

The suites use temporary storage. Local RocksDB instance tests allocate
loopback ports and cover snapshot retry, shutdown/join and restart. Cloud
stream tests do not contact object storage.
