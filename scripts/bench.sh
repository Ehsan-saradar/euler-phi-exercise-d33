#!/usr/bin/env bash
# Latency/throughput numbers against the compose cluster for the design doc.
#   docker compose up -d --wait && ./scripts/bench.sh
set -euo pipefail
cd "$(dirname "$0")/.."
DURATION=${DURATION:-10s}
OUT=docs/bench/RESULTS.md
go build -o bin/kvbench ./cmd/kvbench
{
  echo "# Benchmark ($(date -u +%Y-%m-%dT%H:%MZ))"
  echo
  echo "Host: $(uname -srm), $(getconf _NPROCESSORS_ONLN) CPUs. Cluster: 3 nodes in docker compose on one machine, boltdb log store with fsync, ${DURATION} per row, 1000-key space."
  echo
  echo "| mode | clients | value | ops/s | p50 | p95 | p99 | max | errors |"
  echo "|------|---------|-------|-------|-----|-----|-----|-----|--------|"
  for mode in put get stale; do
    for clients in 1 16 64; do
      ./bin/kvbench -mode "$mode" -clients "$clients" -duration "$DURATION" -md
    done
  done
  ./bin/kvbench -mode mixed -clients 32 -duration "$DURATION" -md
  ./bin/kvbench -mode put -clients 16 -value 4096 -duration "$DURATION" -md
} | tee "$OUT"
