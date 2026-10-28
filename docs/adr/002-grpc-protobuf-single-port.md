# ADR 002: gRPC + protobuf everywhere, one port per node

**Status:** accepted, 2026-10-28

## Context

Clients need an API; followers need to forward to the leader; raft needs a
transport; log entries and snapshots need an encoding that can evolve.

## Decision

- The client API is gRPC (`KVService`, `AdminService`) with protobuf messages
  shaped after etcd v3 (`Put`, `Get`, `Delete`, `Range` with prefix + cursor,
  `CompareAndSwap`).
- The same protobuf schema encodes the raft log `Command` and the snapshot
  stream (`SnapshotHeader` + length-prefixed `KeyValue` records). Adding an
  operation is a new field number; old entries stay decodable.
- Each node listens on **one TCP port**; `cmux` routes HTTP/2 (gRPC) to the
  API and everything else to raft. The raft advertise address is therefore
  also the address a follower forwards to, so no second address book is
  needed and `Join` needs just one address.
- Generated code is committed (`gen/`) and CI checks it is current, so
  `docker compose up` never needs protoc.

## Alternatives

- HTTP + JSON: easier with curl, but a second encoding for the log and no
  streaming/typed contract. `grpcurl` with server reflection covers the demo.
- Two ports (raft, gRPC): simpler multiplexing but a mapping from raft address
  to API address has to be replicated or discovered.

## Consequences

- `kvctl` is the demo CLI; `grpcurl` works thanks to reflection.
- The cmux matcher `HTTP2MatchHeaderFieldSendSettings` is required for
  grpc-go clients, which wait for the server's SETTINGS frame.
