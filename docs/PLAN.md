# D3.3 Replicated key-value store: project plan

Exercise: EulerPhi "Senior Distributed Systems Engineer: Level 3", D3.3.
Goal: a small, strongly consistent store for critical config and leases, similar to etcd.

## 1. Requirements, restated as acceptance criteria

| Tier | Requirement | How we prove it |
|------|-------------|-----------------|
| Core | 3-node cluster using Raft (library allowed) | `docker compose up` starts 3 nodes, cluster elects a leader, `/status` shows term + leader |
| Core | Every read sees the latest committed write (linearizable reads) | ReadIndex / leader-verified reads; Porcupine linearizability check over recorded histories |
| Core | Survives one node failure | Kill any one node, writes and reads still succeed on the other two |
| Senior | Leader failover with no lost committed writes | Test: write N keys with ack, kill leader, wait for new leader, read all N back |
| Senior | Partition test, minority refuses writes | Partition 1 node (or old leader) from the other 2; writes on minority return an explicit error, never a false ack |
| Senior | Snapshots and log compaction | Configurable threshold; test that log is truncated, that a restarted / lagging node restores from snapshot, and that data is identical |
| Stretch | Randomized fault testing (kill, partition, pause) with Porcupine | Nemesis loop + history recorder + Porcupine model; visualization HTML + summary table committed to the repo / CI artifact |
| Global | Runs with `docker compose up`; mocks only; README; tests; 4 to 6 page design doc with diagrams (Archify or Diagram Design) and capacity estimates | Checklist in section 8 |

## 2. Technology decisions (recommendation first, alternatives after)

### Language: Go (recommended)
- Raft libraries, Porcupine, etcd itself and the reviewers' mental model are all Go.
- Single static binary, tiny Docker image, fast tests with `-race`.
- Alternatives: Rust (`openraft`, `raft-rs`) if you are stronger in Rust; Java (`Apache Ratis`). Porcupine then needs a Go side-car or a port.

### Raft library: `hashicorp/raft` (recommended for a time-boxed take-home) vs `go.etcd.io/raft`
| | hashicorp/raft | etcd/raft |
|---|---|---|
| What you write | FSM (Apply / Snapshot / Restore), HTTP API, tests | FSM + storage + transport + tick loop + ready-loop + snapshot plumbing + ReadIndex plumbing |
| Snapshots / compaction | Built in (`SnapshotThreshold`, `SnapshotInterval`, `TrailingLogs`) | You implement (MemoryStorage + WAL + snapshotter) |
| Linearizable reads | `VerifyLeader()` + `Barrier()` (or write a no-op through the log) | Native `ReadIndex`, including follower reads |
| Membership changes | `AddVoter` / `RemoveServer`, one call | ConfChange entries, you drive them |
| Persistence | `raft-boltdb` or `raft-wal` | Your own |
| Explainability in a 30-min review | Small surface, every line is yours | More code, more to explain, but shows deeper understanding |
| Verdict | Pick this unless you have > 40h and want to impress with depth | Pick this if you want "etcd-like" to be literal and have time |

Writing Raft from scratch is NOT recommended: the exercise explicitly allows a library, and the score comes from correctness proofs and the design doc, not from re-deriving Raft.

### Client API: gRPC + protobuf (chosen)
- `KVService` (Put, Get, Delete, Range, CompareAndSwap) and `AdminService` (Status, Join, Leave, Partition, Snapshot), shaped after etcd v3.
- `Consistency` enum on reads: `LINEARIZABLE` (default) or `STALE`.
- The protobuf schema also encodes the raft log command and the snapshot stream; generated code is committed in `gen/` and checked in CI.
- `kvctl` CLI for demos and the Docker health check; `grpcurl` works through server reflection.
- One port per node: cmux routes gRPC, HTTP/1 (`/metrics`, `/healthz`, `/status`) and raft.

### Storage: `raft-boltdb` (recommended) for log + stable store, in-memory map for the state machine, file snapshot store
- The FSM is a `map[string]Entry` (value, version, create/mod revision, optional lease id) behind an `RWMutex`.
- Snapshot = encode the whole map (gob/JSON, or protobuf if gRPC). Sizes stay small for a config store; state this limit in the doc.
- Alternative: `raft-wal` (newer, faster). Alternative for the FSM: BoltDB/Pebble on disk so snapshots are cheap for large data. Mention as 10x/100x evolution.

### Node-to-node transport: `raft.NetworkTransport` (TCP) wrapped in a "chaos" transport
- A `StreamLayer` wrapper that can refuse dial / accept per peer, controlled by `POST /admin/partition {"block":["node2"]}`.
- Gives deterministic, privilege-free partitions that work identically in-process tests and in Docker.
- Plus a shell nemesis that does real `docker compose kill/pause/unpause` and `docker network disconnect/connect` for the demo (needs the Docker socket from the host, not from inside containers).

### Request routing: follower forwards writes and linearizable reads to the leader (chosen)
- Client can hit any node; a follower forwards over gRPC to the leader address learned from raft (same address as the raft one, thanks to the single port). A forwarded request is marked and never forwarded twice.
- Explicit failure modes: no leader known or quorum lost → `Unavailable` with `ErrorInfo.reason = NOT_APPLIED` after a bounded wait; a lost leader after enqueue → `DeadlineExceeded` (unknown outcome). Never a hang, never a false success.

### Cluster bootstrap: static peer list via env, every node calls `BootstrapCluster` with the identical configuration
- hashicorp/raft documents this as safe; avoids a coordinator and a "join" dance.
- Keep a `POST /admin/join` (AddVoter) endpoint anyway: "add a 4th node live" is a likely live-review request.

### Linearizable read path (be ready to explain this on a whiteboard)
1. Request arrives at node X. If X is not leader, forward to leader L.
2. Once per term L commits a barrier (Raft §8: a leader must commit an entry of its own term before serving reads).
3. L calls `VerifyLeader()` (one heartbeat round to a majority; proves no newer term exists) and re-checks the term.
4. L takes `CommitIndex()` and waits for the FSM's own applied index to reach it (noop/barrier/config entries never reach the FSM, so the gap is scanned in the log for the last command entry).
5. L reads from the FSM and responds. `KV_READ_MODE=barrier` keeps the "barrier per read" variant for comparison.
- Why not just `read from FSM on the leader`: a deposed leader in a minority partition would serve stale data.
- Why not lease-based reads: depends on bounded clock drift; fine for etcd, unnecessary here. Mention as optimization with its risk.
- Why the no-op at term start matters: a new leader must commit an entry from its own term before it can know its commit index (Raft §8). The library does this automatically; say so.

### Writes
- `Apply(cmd, timeout)` on the leader. Command = `{op, key, value, expectVersion, leaseId, reqId}` encoded with msgpack/JSON.
- Idempotency: client-supplied `reqId` is optional; store last reqId per client in the FSM if you want exactly-once semantics for retries (nice to discuss, not required).
- Minority side: `Apply` on an old leader that lost quorum never commits; it returns `ErrLeadershipLost` / timeout, which maps to 503. The leader also steps down after `LeaderLeaseTimeout`.

### Leases (the prompt says "config and leases": a small, deliberate scope)
- `POST /v1/lease {ttl_s}` → lease id; `PUT /v1/kv/{k}?lease=id`; `POST /v1/lease/{id}/keepalive`.
- Expiry must be deterministic: ONLY the leader checks expiry on a ticker and proposes a `RevokeLease` entry through Raft. Followers never expire anything on their own clock. This is exactly how etcd does it and is a great talking point.
- Keep as Phase 9 "stretch or live-change". Core/Senior do not require it.

## 3. Repository layout

```
.
├── cmd/kvnode/main.go            # wiring: config, raft, http, metrics
├── internal/
│   ├── fsm/                      # state machine: apply, snapshot, restore (+ unit tests)
│   ├── raftnode/                 # raft setup, bootstrap, chaos transport, read paths
│   ├── api/                      # HTTP handlers, forwarding, error mapping
│   ├── lease/                    # (optional) lease manager driven by the leader
│   └── cluster/                  # test harness: N in-process nodes with inmem/chaos transport
├── test/
│   ├── integration/              # failover, partition, snapshot tests (in-process)
│   ├── linearizability/          # history recorder, Porcupine model, nemesis, report
│   └── e2e/                      # docker compose driven tests (build tag e2e)
├── deploy/docker-compose.yml, Dockerfile, haproxy.cfg (optional)
├── scripts/                      # chaos.sh, demo.sh, bench.sh
├── docs/
│   ├── design.md                 # the 4 to 6 page design doc (export to PDF for submission)
│   ├── diagrams/                 # Archify / Diagram Design exports (png/svg)
│   ├── adr/                      # ADR-001 language, ADR-002 raft lib, ADR-003 read path, ...
│   ├── PLAN.md                   # this file
│   └── TIMELOG.md                # required "total time spent"
├── .github/workflows/            # ci.yml, e2e.yml, chaos.yml, release.yml
├── Makefile
└── README.md
```

## 4. Step-by-step phases

Estimates assume Go + hashicorp/raft. Double them for etcd/raft.
Status (2026-10-28): phases 0 to 8 are implemented and verified, except the
diagrams in the requested tool (sources in `docs/diagrams/README.md`). Phase 9
notes are in `docs/LIVE-REVIEW.md`; the rehearsal itself is pending.

| # | Phase | Deliverables | Est. hours |
|---|-------|--------------|-----------|
| 0 | Scaffold | go module, Makefile, golangci-lint, CI skeleton (lint + unit), Dockerfile, compose skeleton, ADR-001/002, TIMELOG.md started, GitHub repo + branch protection + issues/milestones for every row of the acceptance table | 2 |
| 1 | Single-node KV | FSM with apply/snapshot/restore, command encoding, HTTP API, `/status`, unit tests for FSM (table tests + snapshot round trip + CAS semantics) | 4 |
| 2 | 3-node cluster | static bootstrap, forwarding to leader, `/admin/join`, compose with 3 nodes + healthchecks, in-process cluster test harness (`cluster.New(t, 3)`), smoke test: write on node1, read on node3 | 4 |
| 3 | Linearizable reads | VerifyLeader + Barrier path, `consistency=stale` fallback, ADR-003, test: stale read on partitioned old leader fails, linearizable read on majority succeeds | 2 |
| 4 | Failover + partition | chaos transport + `/admin/partition`, tests: kill leader → all acked writes readable; partition minority → writes return 503 within bounded time and are not applied later; heal → converges. Shell nemesis for Docker demo | 5 |
| 5 | Snapshots + compaction | thresholds via env, test that log index gets truncated, lagging node catches up via snapshot (InstallSnapshot), restart from snapshot + remaining log, size numbers for the doc | 3 |
| 6 | Porcupine | KV model (register per key or whole-map model), history recorder client with unknown-result handling (timeouts = infinite return time), nemesis (kill, partition, pause) with seeded randomness, N runs, visualization HTML, summary table, CI job `chaos.yml` on workflow_dispatch + nightly | 6 |
| 7 | Observability + bench | Prometheus `/metrics` (apply latency, commit index, leader changes, fsync), structured logs, `bench.sh` with p50/p99 for writes and both read modes, 1 node vs 3 nodes, used as evidence in capacity estimates | 3 |
| 8 | Docs | design.md (see section 6), diagrams in the mandated tool, README (setup, architecture, trade-offs, limitations, AI tools, time), ADRs finalized, GitHub Pages with the Porcupine visualizations | 6 |
| 9 | Live-review prep | implement or rehearse: leases/TTL, watch via SSE, add a 4th node live, range queries, idempotent retries; 10x/100x narrative written down | 3 |
| | Total | | ~38 |

## 5. Test strategy

- Unit: FSM (every op, version bump rules, CAS fail/success, snapshot/restore equality, unknown op is rejected not panicked).
- Integration (in-process, no Docker, run under `-race`): cluster of 3; the four Senior scenarios above; membership add/remove; restart with persisted state.
- E2E (Docker): the same scenarios through real containers via `docker compose kill/pause` and network disconnect. Runs in CI on ubuntu-latest.
- Linearizability: Porcupine over histories from randomized runs; keep seeds in the report so a failure is reproducible.
- Property-style: randomized op sequences against a single-node FSM compared with a plain map (cheap, catches encoding bugs).

## 6. Design doc outline (target 4 to 6 pages)

1. Problem and scope (config and leases, consistency goals, non-goals such as multi-raft and auth).
2. Architecture diagram (3 nodes, client, Raft RPC, HTTP, storage) and request flow diagrams: write, linearizable read, failover, partition (process-flow diagrams required by the rules).
3. Consistency model: linearizability definition, read path options compared (log read, ReadIndex/VerifyLeader, lease read, stale), chosen path and why.
4. Data model and storage: FSM, log store, snapshot format, compaction policy, disk layout, fsync.
5. Failure handling: leader crash, follower crash, minority partition, asymmetric partition, disk full, clock jumps (why Raft does not depend on clocks except for election timeouts), what the client sees in each case.
6. Capacity estimates: key count and value sizes for config (e.g. 10k keys × 2 KB = 20 MB), leases at Phi scale (services, workers, gateway nodes renewing every few seconds → writes/s), write throughput bound (fsync-bound single group, batching, ~5k–20k writes/s measured on your machine), read throughput (leader reads vs follower stale reads), log growth per day, snapshot size and time, network between replicas, RAM.
7. What breaks at 10x and 100x: single Raft group throughput ceiling, large snapshots stalling, watch fan-out, cross-region latency; mitigations: batching, follower/ReadIndex reads, on-disk FSM, multi-raft sharding by key range, learners for read scaling, separate lease keepalive channel.
8. Test and chaos results (table with runs, seeds, outcomes, p99s).
9. Trade-offs and known limitations.

## 7. GitHub features to use

Ordered by impact per hour:

1. GitHub Actions CI: `ci.yml` (lint, `go vet`, unit + integration under `-race`, coverage summary in the job summary), `e2e.yml` (compose up, run e2e), `chaos.yml` (workflow_dispatch + nightly cron, uploads Porcupine HTML as an artifact and a markdown summary into `$GITHUB_STEP_SUMMARY`). Badges in README.
2. Work through issues, milestones (Core / Senior / Stretch / Docs) and pull requests with meaningful descriptions; squash-merge; conventional commit messages. The history itself becomes the story you tell in the review.
3. GitHub Projects board linked to the issues (shows planning, cheap).
4. Release workflow: tag `v0.x`, build multi-arch image to GHCR (`ghcr.io/<you>/kvnode`), GitHub Release with changelog; compose can `build:` locally by default and `image:` as an option.
5. GitHub Pages: publish `docs/` (design doc, diagrams, Porcupine visualizations, benchmark tables).
6. ADRs in `docs/adr` (decision log for every "why" question in the review).
7. Dependabot (gomod + actions) and CodeQL default setup; `SECURITY.md`. Minutes of work, signals maturity.
8. Dev container / Codespaces config so a reviewer can open and run it in one click.
9. Signed commits (verified badge), CODEOWNERS, PR template with a checklist that mirrors the acceptance table.
10. `github-action-benchmark` to track write p99 across commits (only if time remains).

## 8. Submission checklist

- [ ] `docker compose up` works from a clean clone, no secrets, no external services
- [ ] README: setup, architecture overview, trade-offs, limitations, AI tools (list Claude Code and everything else), total time spent
- [ ] Tests cover FSM, failover, partition, snapshots; CI green
- [ ] Design doc 4 to 6 pages with diagrams from Archify or Diagram Design and capacity estimates (PDF export in `docs/`)
- [ ] Diagrams: system architecture + process flows (write, read, failover, partition, snapshot)
- [ ] Porcupine report with seeds and visualizations
- [ ] 10x / 100x narrative prepared
- [ ] Live-change rehearsal done (leases, watch, add node)
