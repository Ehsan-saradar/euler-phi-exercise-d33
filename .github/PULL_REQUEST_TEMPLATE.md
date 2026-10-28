## What

<!-- one paragraph: the change and why -->

## Acceptance criteria touched

- [ ] Core: 3-node raft cluster, linearizable reads, survives one node failure
- [ ] Senior: leader failover, minority partition refuses writes, snapshots and compaction
- [ ] Stretch: randomized faults checked with Porcupine
- [ ] Docs: README / design doc / ADR updated

## How it was verified

- [ ] `make test` (unit + integration, race detector)
- [ ] `make chaos` (seed: )
- [ ] `docker compose up` + `scripts/demo.sh`
