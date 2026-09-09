# Log service module

`eloq_log_service/` implements Data Substrate's replicated WAL service. It owns the braft-backed log groups and the server side of the log protocol; transaction nodes use the client and protobuf contract under `tx_service/tx-log-protos/`.

The current design, durability boundary and source map are maintained in [`docs/10-log-service.md`](../docs/10-log-service.md). Build integration is defined by `build_eloq_log_service.cmake`, and module tests live under `test/`.
