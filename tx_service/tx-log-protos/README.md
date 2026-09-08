# Transaction/log service protocol

This directory owns the protobuf wire contract shared by transaction nodes and the log service, together with the transaction-side `LogAgent` client.

Read [`docs/10-log-service.md`](../../docs/10-log-service.md) for protocol responsibilities, durability and replay boundaries. Keep wire-compatible changes documented there; keep generated protobuf details and local client mechanics beside the source.
