# Diagrams

The exercise requires diagrams made with **Archify** or **Diagram Design**.
Export them here as PNG/SVG with the names below and they are picked up by
the design document and the GitHub Pages build. The Mermaid sources in this
file are the reference for what each diagram must show.

| File | Diagram |
|------|---------|
| `architecture.png` | system architecture (clients, 3 nodes, one port each, raft links, storage) |
| `flow-write.png` | write path with forwarding, replication, commit, apply |
| `flow-read.png` | linearizable read path (ReadIndex) and stale read |
| `flow-failover.png` | leader crash: election, acknowledged writes, in-flight writes |
| `flow-partition.png` | minority partition: lease loss, refusal, heal |
| `flow-snapshot.png` | snapshot, compaction, InstallSnapshot to a lagging node |

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
