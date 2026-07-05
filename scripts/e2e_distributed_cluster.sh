#!/usr/bin/env bash
set -euo pipefail
set +m

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

cleanup() {
  scripts/stop_local_cluster.sh >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local name="$3"
  for _ in $(seq 1 120); do
    if [ -f "$file" ] && grep -q "$pattern" "$file"; then
      return 0
    fi
    sleep 0.1
  done
  echo "Timed out waiting for $name" >&2
  echo "== $file ==" >&2
  [ -f "$file" ] && cat "$file" >&2
  exit 1
}

start_bg() {
  local name="$1"
  shift
  "$@" > "build/logs/$name.log" 2>&1 &
  local pid=$!
  echo "$pid" > "build/pids/$name.pid"
  disown "$pid" 2>/dev/null || true
}

scripts/build_manual.sh
rm -rf data
mkdir -p build/logs build/pids
rm -f build/pids/*.pid

start_bg pd ./build/rstone-server --role pd --config config/pd.yaml --serve
wait_for_log build/logs/pd.log "PD TCP service listening" "PD"

start_bg store1 ./build/rstone-server --role store --config config/distributed_store1.yaml --serve
start_bg store2 ./build/rstone-server --role store --config config/distributed_store2.yaml --serve
start_bg store3 ./build/rstone-server --role store --config config/distributed_store3.yaml --serve
wait_for_log build/logs/store1.log "Store TCP service listening" "Store-1"
wait_for_log build/logs/store2.log "Store TCP service listening" "Store-2"
wait_for_log build/logs/store3.log "Store TCP service listening" "Store-3"

start_bg gateway ./build/rstone-server --role gateway --config config/distributed_gateway.yaml --serve
wait_for_log build/logs/gateway.log "Gateway TCP service listening" "Gateway"

./build/rstone-cli --endpoint 127.0.0.1:18080 put distributed:key v1 >/dev/null
value="$(./build/rstone-cli --endpoint 127.0.0.1:18080 get distributed:key)"
if [ "$value" != "v1" ]; then
  echo "distributed get failed: $value" >&2
  exit 1
fi

./build/rstone-cli --endpoint 127.0.0.1:8102 store-get distributed:key >/tmp/rstone_dist_get2.out
if [ "$(cat /tmp/rstone_dist_get2.out)" != "v1" ]; then
  echo "store2 follower apply failed" >&2
  exit 1
fi

./build/rstone-cli --endpoint 127.0.0.1:8103 store-get distributed:key >/tmp/rstone_dist_get3.out
if [ "$(cat /tmp/rstone_dist_get3.out)" != "v1" ]; then
  echo "store3 follower apply failed" >&2
  exit 1
fi

./build/rstone-cli --endpoint 127.0.0.1:18080 put distributed:key v2 >/dev/null
value="$(./build/rstone-cli --endpoint 127.0.0.1:18080 get distributed:key)"
if [ "$value" != "v2" ]; then
  echo "distributed overwrite failed: $value" >&2
  exit 1
fi

./build/rstone-cli --endpoint 127.0.0.1:18080 transfer-leader 1 2 >/dev/null
./build/rstone-cli --endpoint 127.0.0.1:18080 put distributed:after-transfer peer2 >/dev/null
value="$(./build/rstone-cli --endpoint 127.0.0.1:18080 get distributed:after-transfer)"
if [ "$value" != "peer2" ]; then
  echo "distributed transfer write failed: $value" >&2
  exit 1
fi

./build/rstone-cli --endpoint 127.0.0.1:8101 store-get distributed:after-transfer \
  >/tmp/rstone_dist_get1_after_transfer.out
if [ "$(cat /tmp/rstone_dist_get1_after_transfer.out)" != "peer2" ]; then
  echo "store1 apply after transfer failed" >&2
  exit 1
fi

status_output="$(./build/rstone-cli --endpoint 127.0.0.1:18080 status)"
echo "$status_output" | grep -q "pd.store_count=3"
echo "$status_output" | grep -q "pd.region0.leader_peer_id=2"

echo "RStone distributed cluster e2e passed"
