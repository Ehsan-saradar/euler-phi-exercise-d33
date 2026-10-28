# Live review notes

Thirty minutes: walk-through, a live change, and the 10x / 100x discussion.

## Walk-through order (10 min)

1. `docker compose up --wait`, `scripts/demo.sh` running in a side terminal.
2. `proto/kv/v1`: the API, the log command, the snapshot format (one schema).
3. `internal/fsm/fsm.go`: `Apply` is deterministic; own applied index and why
   (`AppliedIndex` in hashicorp/raft is "handed to the FSM", not "applied").
4. `internal/raftnode/node.go`: `Propose`, `Linearize` (per-term barrier,
   VerifyLeader, wait for FSM), `waitApplied` (log scan for the last command).
5. `internal/raftnode/chaos.go`: block / freeze; symmetric cuts.
6. `internal/api`: forwarding once, `NOT_APPLIED` vs unknown.
7. Tests: minority partition, snapshot catch-up, Porcupine run and its
   visualization.

## Likely live changes, rehearsed

### Leases / TTL keys (most likely, "config and leases")
- Proto: `LeaseService { Grant(ttl) -> id; KeepAlive(id); Revoke(id) }`,
  `PutRequest.lease_id`, `KeyValue.lease_id`.
- Log: `LeaseGrantOp{id, ttl_ms, granted_at_index}`, `LeaseRevokeOp{id}`,
  `LeaseKeepAliveOp{id}`. **Expiry must be a log entry.** Only the leader
  runs a ticker; when a lease's deadline (tracked on the leader in wall-clock
  terms from the last keep-alive it applied) passes, it proposes `Revoke`.
  Followers never expire anything on their own clock, so the state machine
  stays deterministic. After failover the new leader rebuilds deadlines from
  the FSM's lease table, granting every lease one full TTL of grace (etcd does
  the same).
- FSM: `leases map[id]{ttl, keys set}`; revoke deletes attached keys.
- Test: grant 1 s lease, put with it, stop keep-alives, expect key gone
  within ~2 s on every node; kill leader mid-lease, key survives for the
  grace period then expires.

### Watch (prefix)
- `rpc Watch(WatchRequest{prefix, start_index}) returns (stream Event)`.
- FSM publishes `(index, op, kv)` to an in-process broadcaster after each
  Apply; a watcher starting at an old index replays from the log store
  (bounded by compaction: if older than the last snapshot, return
  `FailedPrecondition compacted` like etcd).
- Served by any node from its own FSM (watch is naturally eventually
  consistent); `start_index` gives at-least-once resumption.

### Add a node at runtime
- Already supported: start a node with no `KV_PEERS`, `kvctl join node4 addr`.
  Show `TestJoinNewNode`. Discuss learner (non-voter) first, then promote,
  to avoid an unavailable window while the new node catches up.

### Delete-range / transactions
- `Txn{compare[], success[], failure[]}` as one log entry; FSM evaluates
  compares then applies one branch atomically. Straightforward because the
  FSM already applies entries under one lock.

### Follower reads
- Follower asks the leader for its ReadIndex (`ReadIndex` RPC), waits for its
  own FSM to reach it, serves locally. Spreads read load over all nodes;
  adds one RPC but no leader CPU for the read itself.

## What breaks at 10x and 100x

Baseline on one laptop (see `docs/bench/RESULTS.md`): one raft group, three
nodes, boltdb with fsync.

### 10x load
- **Writes**: one leader, one fsync per batch. hashicorp/raft batches
  proposals (`MaxAppendEntries` = 64) so throughput grows with concurrency
  until the disk is the limit. First steps: dedicated SSD, `raft-wal` instead
  of boltdb, bigger batches, larger values are bounded by `MaxValueBytes`.
- **Linearizable reads**: every one costs the leader a heartbeat round.
  Switch to lease-based reads (no round trip, assumes bounded clock drift) or
  follower ReadIndex to spread the load.
- **Range**: sorting matches per call stops being free. Replace the map with
  a B-tree (ordered iteration, no sort) or move the FSM to boltdb/pebble.
- **Snapshots**: `Snapshot()` copies the whole map under the read lock;
  at tens of millions of keys that pause is visible. Copy-on-write FSM or an
  on-disk FSM whose snapshot is a file copy.

### 100x load
- **Single raft group is the ceiling**: shard by key range into multiple raft
  groups (multi-raft, as in TiKV/CockroachDB) with a routing table; cross-shard
  transactions then need a 2PC layer, which is why etcd deliberately stays
  single-group and small.
- **Watch fan-out**: thousands of watchers per key need a dedicated
  broadcaster per node fed from the log, not from the API goroutines.
- **Forwarding**: 2/3 of requests make an extra hop; smart clients that learn
  the leader (as etcd clients do) remove it.
- **Membership and regions**: 5 voters across zones, learners in remote
  regions for local stale reads; cross-region commit latency bounds write
  throughput, so config writes are rare by design.

### What does not change
- Correctness: the proofs (tests, Porcupine) are per group and still hold.
- The error contract: clients still learn whether a failed write may have
  applied.
