# Benchmark (2026-10-28T08:59Z)

Host: Linux 5.15.0-143-generic x86_64, 8 CPUs. Cluster: 3 nodes in docker compose on one machine, boltdb log store with fsync, 8s per row, 1000-key space.

| mode | clients | value | ops/s | p50 | p95 | p99 | max | errors |
|------|---------|-------|-------|-----|-----|-----|-----|--------|
| put | 1 | 256 B | 46 | 18.52ms | 35.87ms | 45.43ms | 155.26ms | 0 |
| put | 16 | 256 B | 152 | 99.68ms | 156.36ms | 341.26ms | 387.69ms | 0 |
| put | 64 | 256 B | 512 | 117.77ms | 197.96ms | 234.77ms | 291.96ms | 0 |
| get | 1 | 256 B | 2420 | 430µs | 500µs | 670µs | 93.57ms | 0 |
| get | 16 | 256 B | 12651 | 1.14ms | 2.38ms | 3.49ms | 12.02ms | 0 |
| get | 64 | 256 B | 21399 | 2.63ms | 6.11ms | 8.79ms | 19.63ms | 0 |
| stale | 1 | 256 B | 5119 | 180µs | 250µs | 330µs | 14.41ms | 0 |
| stale | 16 | 256 B | 29031 | 460µs | 1.1ms | 1.83ms | 11.47ms | 0 |
| stale | 64 | 256 B | 47979 | 1.1ms | 2.94ms | 4.98ms | 13.96ms | 0 |
| mixed | 32 | 256 B | 388 | 83.87ms | 140.76ms | 174.11ms | 229.86ms | 0 |
| put | 16 | 4096 B | 140 | 106.98ms | 186.58ms | 214.34ms | 225.56ms | 0 |
