# ADR 001: Go with hashicorp/raft

**Status:** accepted, 2026-10-28

## Context

The exercise asks for a 3-node, strongly consistent store "similar to etcd"
and explicitly allows a Raft library. The result is reviewed line by line in a
30-minute session, so every line must be explainable, and the time budget is
about 40 hours including documentation.

## Decision

Go, with `github.com/hashicorp/raft` (v1.8) and `raft-boltdb` for the log and
stable store. The application owns the state machine, the transport
wrapper, the read path and the API.

## Alternatives

- **etcd/raft**: a pure consensus core; the application must implement
  storage, the WAL, the transport, the ready loop, snapshot plumbing and
  ReadIndex handling. Closer to "literally etcd", roughly double the code,
  and all of it has to be defended live.
- **Hand-written Raft**: not what the exercise rewards; correctness proof
  would consume the whole budget.
- **Rust (openraft)**: fine library, but Porcupine and the reviewers' mental
  model are Go.

## Consequences

- Snapshots, log compaction, membership changes and leader verification come
  from the library; the project's value is in the state machine, the read
  gate, failure handling, tests and the design doc.
- `AppliedIndex()` in hashicorp/raft advances when an entry is handed to the
  FSM, not when it is applied, so the FSM tracks its own applied index (see
  ADR 003).
- Barrier and noop entries never reach `FSM.Apply`, which the read gate must
  account for.
