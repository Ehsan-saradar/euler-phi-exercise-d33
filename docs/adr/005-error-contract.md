# ADR 005: Error contract, NOT_APPLIED versus unknown outcome

**Status:** accepted, 2026-10-28

## Context

A client of a replicated store must know, for every failed write, whether it
can safely retry (nothing happened) or whether the write may have committed.
The Porcupine check depends on this: an operation with an unknown outcome is
modelled as "may take effect any time later", which is sound but expensive,
so false unknowns must be rare and false "not applied" must never happen.

## Decision

| gRPC status | Meaning | Client action |
|-------------|---------|---------------|
| `Unavailable` with `ErrorInfo.reason = NOT_APPLIED` | the node produced this before the proposal reached the log (no leader, not leader after one forward hop, dial failure) | retry on any node |
| `Unavailable` without the marker | transport failure: the server died or the connection broke after the request may have been sent | treat as unknown, unless the message says `connection refused` (the RPC never left the client) |
| `DeadlineExceeded` | outcome unknown: the entry was enqueued and leadership was lost, or the client deadline fired | read back before retrying, or retry if idempotent |
| `InvalidArgument` | bad key/value | fix the request |

Forwarded calls apply the same rule: a transport failure between follower and
leader is reported to the client as `DeadlineExceeded` ("leader unreachable
after forwarding; outcome unknown"), never as `Unavailable`.

## Consequences

- `api.IsNotApplied(err)` is the single helper clients use.
- The chaos recorder drops NOT_APPLIED operations from the history (they had
  no effect) and keeps unknown writes with an infinite return time.
