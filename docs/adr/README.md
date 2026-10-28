# Architecture decision records

One file per decision, in the order they were taken. Each states the context,
the decision, the alternatives considered and the consequences. They exist so
that every "why" asked in the live review has a written answer.

| # | Decision |
|---|----------|
| [001](001-go-and-hashicorp-raft.md) | Go with hashicorp/raft rather than etcd/raft or a hand-written Raft |
| [002](002-grpc-protobuf-single-port.md) | gRPC + protobuf for the API, the log and snapshots; one port via cmux |
| [003](003-linearizable-reads.md) | Linearizable reads: ReadIndex on top of hashicorp/raft, barrier as fallback |
| [004](004-fault-injection.md) | Fault injection in the transport layer, applied symmetrically |
| [005](005-error-contract.md) | Error contract: NOT_APPLIED vs unknown outcome |
