#!/usr/bin/env bash
# End-to-end demo against the docker compose cluster:
#   write / read across nodes, kill the leader, partition the leader,
#   snapshot. Requires: docker compose up -d --wait
set -euo pipefail
cd "$(dirname "$0")/.."

ctl() { local node=$1; shift; docker compose exec -T "$node" kvctl -addr localhost:7000 "$@"; }
leader() { ctl node1 status | sed -n 's/.*"leaderId": *"\([^"]*\)".*/\1/p'; }
step() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
others() { for n in node1 node2 node3; do [ "$n" != "$1" ] && echo "$n"; done; }

step "Cluster status"
for n in node1 node2 node3; do ctl "$n" status | tr -d '\n' | sed 's/  */ /g'; echo; done

step "Write on node1, linearizable read on node3"
ctl node1 put greeting "hello phi"
ctl node3 get greeting

L=$(leader); echo "leader is $L"
mapfile -t REST < <(others "$L")

step "Kill the leader ($L); write and read on the survivors"
docker compose kill "$L"
sleep 2
ctl "${REST[0]}" put after-failover ok
ctl "${REST[1]}" get after-failover
ctl "${REST[1]}" get greeting
docker compose start "$L"
sleep 3
ctl "$L" get after-failover

step "Partition the current leader from the other two"
L=$(leader); echo "leader is $L"; mapfile -t REST < <(others "$L")
ctl "$L" partition -block "${REST[0]},${REST[1]}"
ctl "${REST[0]}" partition -block "$L"
ctl "${REST[1]}" partition -block "$L"
sleep 2
echo "write on the isolated node (expect Unavailable / DeadlineExceeded):"
ctl "$L" -timeout 3s put isolated x || true
echo "write on the majority side:"
ctl "${REST[0]}" put majority yes
echo "heal"
for n in node1 node2 node3; do ctl "$n" partition -clear >/dev/null; done
sleep 2
ctl "$L" get majority
ctl "$L" get isolated

step "Snapshot"
for i in $(seq 1 50); do ctl node1 put "bulk/$i" "v$i" >/dev/null; done
ctl node1 snapshot
ctl node1 status | grep -E 'lastSnapshotIndex|numKeys'
echo; echo "done"
