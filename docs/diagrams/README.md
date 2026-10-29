# Diagrams

The exercise requires diagrams made with **Archify** or **Diagram Design**.
These were made with [Archify](https://github.com/tt-a1i/archify) (v3.2):
each diagram is a typed JSON source in [`src/`](src/), validated against
Archify's schema, rendered to a standalone HTML viewer, checked in a real
browser, and then exported from the viewer as a 4x PNG (light theme) and a
dual-theme SVG. The PNG/SVG files below are picked up by the design document
and the GitHub Pages build. The Mermaid sources further down are the
reference for what each diagram must show.

| File | Source | Diagram |
|------|--------|---------|
| `architecture.png/.svg` | `src/architecture.architecture.json` | system architecture (clients, 3 nodes, one port each, raft links, storage) |
| `flow-write.png/.svg` | `src/flow-write.sequence.json` | write path with forwarding, replication, commit, apply |
| `flow-read.png/.svg` | `src/flow-read.sequence.json` | linearizable read path (ReadIndex) and stale read |
| `flow-failover.png/.svg` | `src/flow-failover.sequence.json` | leader crash: election, acknowledged writes, in-flight writes |
| `flow-partition.png/.svg` | `src/flow-partition.sequence.json` | minority partition: lease loss, refusal, heal |
| `flow-snapshot.png/.svg` | `src/flow-snapshot.sequence.json` | snapshot, compaction, InstallSnapshot to a lagging node |

## Rebuilding

```sh
npx skills add tt-a1i/archify -g          # installs ~/.agents/skills/archify
npm install --no-save playwright-core      # drives the viewer's Export menu
export ARCHIFY_CHROME=/path/to/chrome      # any Chromium build
scripts/diagrams.sh
```

`scripts/diagrams.sh` runs `archify finalize` for every source (schema
validation, delivery, a strict artifact check and a headless-browser check;
a non-zero exit means the diagram is rejected) into the git-ignored
`.archify/` directory, then `scripts/diagrams-export.mjs` opens each HTML
viewer and saves the PNG and SVG here. Edit the JSON sources, not the
exports.

Conventions in the sources: `emphasis` arrows are the main request path,
`return` arrows are replies, `dashed` arrows are dropped, failed or
best-effort messages, and `security` arrows are the ones that decide
leadership (VerifyLeader, the AppendEntries that heals a partition). Notes
under a message stand in for Mermaid's `Note over` and self-messages, which
Archify's sequence mode does not draw.

## Architecture

```mermaid
flowchart TB
    subgraph clients
        C1[kvctl] ; C2[service clients] ; P[Prometheus]
    end
    subgraph node1 [node1 :7000]
        M1{cmux} --> G1[gRPC KV/Admin] --> A1[api: forward + errors] --> R1[raftnode]
        M1 --> H1[HTTP /metrics /status]
        M1 --> T1[raft transport + chaos layer]
        R1 --> F1[(FSM: map, versions)]
        R1 --> D1[(boltdb log, snapshots)]
    end
    subgraph node2 [node2 :7000]
        M2{cmux} --> R2[raftnode] --> F2[(FSM)]
    end
    subgraph node3 [node3 :7000]
        M3{cmux} --> R3[raftnode] --> F3[(FSM)]
    end
    C1 & C2 --> M1 & M2 & M3
    P --> H1
    T1 <--> M2
    T1 <--> M3
    M2 <--> M3
```

## Write

```mermaid
sequenceDiagram
    participant C as client
    participant F as follower (node2)
    participant L as leader (node1)
    participant F3 as follower (node3)
    C->>F: Put(k, v)
    F->>L: Put(k, v) [kv-forwarded=1]
    L->>L: encode Command, append to log (fsync)
    par replicate
        L->>F: AppendEntries
        L->>F3: AppendEntries
    end
    F-->>L: ack
    Note over L: majority -> committed
    L->>L: FSM.Apply: version+1, mod_index
    L-->>F: PutResponse{index, term, kv}
    F-->>C: PutResponse
```

## Linearizable read (ReadIndex)

```mermaid
sequenceDiagram
    participant C as client
    participant L as leader
    participant Fs as followers
    C->>L: Get(k, LINEARIZABLE)
    alt first read in this term
        L->>Fs: Barrier entry (commit an entry of own term)
        Fs-->>L: ack
    end
    L->>Fs: VerifyLeader heartbeat
    Fs-->>L: ack from majority
    L->>L: commit := CommitIndex(); wait FSM.applied >= commit
    L-->>C: GetResponse{value, version, index}
    Note over C,L: STALE: any node answers from its FSM immediately
```

## Leader failover

```mermaid
sequenceDiagram
    participant C as client
    participant L as leader (old)
    participant A as follower A
    participant B as follower B
    C->>L: Put(k1) ... acked (on majority)
    C->>L: Put(k2) in flight
    Note over L: crash
    Note over A,B: election timeout, A wins (its log has k1)
    C--xL: Put(k2): DeadlineExceeded / connection lost (unknown)
    C->>A: Get(k1) -> found (acked write survived)
    C->>A: Get(k2) -> maybe; client reads back before retrying
```

## Minority partition

```mermaid
sequenceDiagram
    participant C as client
    participant L as leader (isolated)
    participant A as follower A
    participant B as follower B
    Note over L,B: cut: L | {A, B}
    C->>L: Put(x)
    Note over L: no majority ack; lease expires -> steps down
    L-->>C: Unavailable NOT_APPLIED / DeadlineExceeded (never success)
    Note over A,B: elect A at higher term
    C->>B: Put(y) -> forwarded to A -> ok
    Note over L,B: heal
    A->>L: AppendEntries(term+1) -> L truncates x, appends y
    C->>L: Get(y) -> forwarded to A -> found
```

## Snapshot and catch-up

```mermaid
sequenceDiagram
    participant L as leader
    participant S as lagging follower
    Note over L: entries > SnapshotThreshold since last snapshot
    L->>L: FSM.Snapshot (copy map) -> Persist (stream header + KeyValue records)
    L->>L: compact log, keep TrailingLogs
    S->>L: (back online) AppendEntries reply: need index below compaction point
    L->>S: InstallSnapshot(stream)
    S->>S: FSM.Restore -> applied = snapshot index
    L->>S: AppendEntries (tail)
```
