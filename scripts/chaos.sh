#!/usr/bin/env bash
# Docker-level chaos loop: real kills, pauses (SIGSTOP) and app-level
# partitions against the compose cluster, with a writer/reader checking that
# acknowledged writes are never lost. For the formal check see `make chaos`.
#   ROUNDS=10 ./scripts/chaos.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ROUNDS=${ROUNDS:-6}
NODES=(node1 node2 node3)
ctl() { local node=$1; shift; docker compose exec -T "$node" kvctl -addr localhost:7000 "$@"; }
leader() { for n in "${NODES[@]}"; do ctl "$n" status 2>/dev/null | sed -n 's/.*"leaderId": *"\([^"]*\)".*/\1/p' | grep . && return; done; }
rand_node() { echo "${NODES[$((RANDOM % ${#NODES[@]}))]}"; }

acked=()
write_some() {
  local n tag=$1
  for i in 1 2 3 4 5; do
    n=$(rand_node)
    if ctl "$n" -timeout 2s put "chaos/$tag/$i" "$tag-$i" >/dev/null 2>&1; then acked+=("chaos/$tag/$i=$tag-$i"); fi
  done
}
verify_all() {
  local n key val missing=0
  for e in "${acked[@]}"; do
    key=${e%%=*}; val=${e#*=}
    n=$(rand_node)
    if ! ctl "$n" -timeout 5s get "$key" | grep -q "\"value\": *\"$(printf %s "$val" | base64)\""; then
      echo "MISSING acknowledged write $key on $n"; missing=1
    fi
  done
  return $missing
}

for r in $(seq 1 "$ROUNDS"); do
  L=$(leader); fault=$((RANDOM % 4))
  case $fault in
    0) echo "[$r] kill leader $L";   docker compose kill "$L";   write_some "r$r"; docker compose start "$L" ;;
    1) V=$(rand_node); echo "[$r] pause $V"; docker compose pause "$V"; write_some "r$r"; docker compose unpause "$V" ;;
    2) echo "[$r] isolate leader $L"
       for n in "${NODES[@]}"; do [ "$n" = "$L" ] || { ctl "$n" partition -block "$L" >/dev/null; ctl "$L" partition -block "$n" >/dev/null || true; }; done
       write_some "r$r"
       for n in "${NODES[@]}"; do ctl "$n" partition -clear >/dev/null || true; done ;;
    3) V=$(rand_node); echo "[$r] restart $V"; docker compose restart "$V"; write_some "r$r" ;;
  esac
  sleep 3
  verify_all && echo "[$r] ok: ${#acked[@]} acknowledged writes all present"
done
echo "chaos finished: ${#acked[@]} acknowledged writes verified"
