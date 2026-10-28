# ADR 004: Fault injection lives in the transport layer

**Status:** accepted, 2026-10-28

## Context

The Senior tier requires a partition test where the minority refuses writes;
the Stretch tier requires randomized kill / partition / pause. Tests must run
in CI (no privileges) and the same mechanism should drive the Docker demo.

## Decision

`raftnode.ChaosLayer` wraps the raft `StreamLayer`:

- **Block** a peer: refuse to dial it, drop inbound connections from its IP,
  and close its live connections so pooled streams do not keep working.
- **Freeze**: stall every read and write on every raft connection (what a
  SIGSTOP or a long GC pause looks like to peers) without touching the API.

It is exposed through `AdminService.Partition` (local to the node, never
forwarded) and used directly by the in-process harness. A cut is applied on
**both sides**; the inbound IP filter is a second line of defence for a
one-sided demo and assumes one IP per node (true in Docker; the in-process
harness binds `127.0.1.N`).

`scripts/chaos.sh` adds the real thing on Docker: `docker compose kill`,
`pause`/`unpause`, `restart`.

## Alternatives

- `iptables` / `tc` inside containers: realistic but needs `NET_ADMIN`, does
  not work in unit tests, differs per OS.
- Toxiproxy / Pumba: extra moving parts for the same effect.

## Consequences

- One code path for tests, CI and demo.
- The client-facing port is never cut by the chaos layer, which is exactly
  what makes "isolated leader keeps answering clients" testable.
