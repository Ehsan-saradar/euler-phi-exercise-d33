# Porcupine runs

Appended by `make chaos` (one row per run). Columns: time, seed, read mode,
result, ops checked, ok, not-applied (dropped), unknown-outcome, faults.

| time | seed | mode | result | ops | ok | not applied | unknown | faults |
|------|------|------|--------|-----|----|-------------|---------|--------|
| 2026-10-28T08:44:08Z | 590640475 | read_index | Unknown | 2228 | 2039 | 163 | 324 | map[freeze-random:4 kill-leader:1 kill-random:1 split-random:4] |
| 2026-10-28T08:45:39Z | 590640475 | read_index | Ok | 2170 | 2140 | 514 | 43 | map[freeze-random:4 kill-leader:1 kill-random:1 split-random:4] |
| 2026-10-28T08:46:38Z | 128828502 | read_index | Ok | 2390 | 2323 | 718 | 98 | map[freeze-leader:5 freeze-random:3 isolate-leader:1 kill-leader:2 kill-random:1] |
| 2026-10-28T08:47:15Z | 116118318 | read_index | Ok | 2954 | 2862 | 1061 | 153 | map[freeze-random:5 kill-leader:4 kill-random:1 split-random:2] |
| 2026-10-30T11:54:21Z | 486797596 | read_index | Ok | 5284 | 5221 | 1645 | 94 | map[freeze-random:1 isolate-leader:1 kill-leader:2 kill-random:2 split-random:2] |
