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
  : > "build/logs/$name.log"
  "$@" > "build/logs/$name.log" 2>&1 &
  local pid=$!
  echo "$pid" > "build/pids/$name.pid"
  disown "$pid" 2>/dev/null || true
}

stop_component() {
  local name="$1"
  local pid_file="build/pids/$name.pid"
  if [ ! -f "$pid_file" ]; then
    return 0
  fi
  local pid
  pid="$(cat "$pid_file")"
  kill "$pid" >/dev/null 2>&1 || true
  for _ in $(seq 1 50); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
  rm -f "$pid_file"
}

start_store() {
  local id="$1"
  start_bg "store$id" ./build/rstone-server --role store \
    --config "config/distributed_store$id.yaml" --serve
  wait_for_log "build/logs/store$id.log" "Store TCP service listening" "Store-$id"
}

expect_gateway_value() {
  local key="$1"
  local expected="$2"
  local actual
  actual="$(./build/rstone-cli --endpoint 127.0.0.1:18080 get "$key")"
  if [ "$actual" != "$expected" ]; then
    echo "unexpected gateway value for $key: expected=$expected actual=$actual" >&2
    exit 1
  fi
}

expect_store_value() {
  local endpoint="$1"
  local key="$2"
  local expected="$3"
  local actual
  actual="$(./build/rstone-cli --endpoint "$endpoint" store-get "$key")"
  if [ "$actual" != "$expected" ]; then
    echo "unexpected store value for $key on $endpoint: expected=$expected actual=$actual" >&2
    exit 1
  fi
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

./build/rstone-cli --endpoint 127.0.0.1:18080 put recovery:base base >/dev/null
expect_gateway_value recovery:base base

stop_component store3
./build/rstone-cli --endpoint 127.0.0.1:18080 put recovery:while-store3-down missed-by-store3 >/dev/null
expect_gateway_value recovery:while-store3-down missed-by-store3
start_store 3
./build/rstone-cli --endpoint 127.0.0.1:18080 put recovery:catchup-store3 catchup >/dev/null
expect_store_value 127.0.0.1:8103 recovery:while-store3-down missed-by-store3
expect_store_value 127.0.0.1:8103 recovery:catchup-store3 catchup

stop_component store1
./build/rstone-cli --endpoint 127.0.0.1:18080 transfer-leader 1 2 >/dev/null
./build/rstone-cli --endpoint 127.0.0.1:18080 put recovery:leader-down written-by-peer2 >/dev/null
expect_gateway_value recovery:leader-down written-by-peer2
start_store 1
./build/rstone-cli --endpoint 127.0.0.1:18080 put recovery:catchup-store1 catchup >/dev/null
expect_store_value 127.0.0.1:8101 recovery:leader-down written-by-peer2
expect_store_value 127.0.0.1:8101 recovery:catchup-store1 catchup

echo "RStone distributed recovery e2e passed"
