# ADR 003: Linearizable reads with ReadIndex, barrier as fallback

**Status:** accepted, 2026-10-28

## Context

"Every read sees the latest committed write." A read served from a replica's
state machine is only linearizable if (a) the serving node is still the
leader for the current term and (b) its state machine has applied everything
committed before the read began. A deposed leader in a minority partition
fails (a); a leader whose FSM lags fails (b).

## Decision

`Node.Linearize` implements the Raft paper §8 ReadIndex:

1. **Once per term**, commit a barrier entry. A new leader does not know the
   true commit index until it has committed an entry from its own term
   (hashicorp/raft appends a noop, but `CommitIndex()` only reflects it after
   it commits). Memoised in `termBarrier`.
2. `VerifyLeader()`: one heartbeat round confirming a majority still follows
   this leader in this term. Then re-check the term did not change.
3. `commit := CommitIndex()`.
4. Wait until the **FSM's own** applied index reaches `commit`. Because noop,
   barrier and configuration entries never reach `FSM.Apply`, the gap between
   the FSM's index and `commit` is scanned in the log store for the highest
   command entry; if there is none the FSM is already current.

`KV_READ_MODE=barrier` keeps the trivially correct alternative (append a
barrier per read) for benchmarks and as a reference in tests.

Reads with `CONSISTENCY_STALE` skip all of this and read the local FSM on any
node, returning the FSM applied index in the header so clients can reason
about staleness.

## Alternatives

- **Lease-based reads** (etcd's default): skip the heartbeat round and trust
  the leader lease, which requires bounded clock drift. Faster, weaker
  assumption; noted as a 10x optimisation.
- **Route every read through the log**: correct, simplest, and doubles write
  load on a read-heavy config store.

## Consequences

- A linearizable read costs one heartbeat round trip, no disk write.
- Follower reads are forwarded to the leader; serving ReadIndex on followers
  (as etcd does) is a documented extension.
