#!/usr/bin/env bash
set -euo pipefail
set +m

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local name="$3"
  for _ in $(seq 1 100); do
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

stop_component() {
  local name="$1"
  local pid_file="build/pids/$name.pid"
  if [ -f "$pid_file" ]; then
    local pid
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
      for _ in $(seq 1 50); do
        if ! kill -0 "$pid" >/dev/null 2>&1; then
          break
        fi
        sleep 0.1
      done
    fi
    rm -f "$pid_file"
  fi
}

start_pd() {
  : > build/logs/pd.log
  ./build/rstone-server --role pd --config config/pd.yaml --serve \
    > build/logs/pd.log 2>&1 &
  local pid=$!
  echo "$pid" > build/pids/pd.pid
  disown "$pid" 2>/dev/null || true
  wait_for_log build/logs/pd.log "PD TCP service listening" "PD"
}

start_store() {
  : > build/logs/store.log
  ./build/rstone-server --role store --config config/store1.yaml --serve \
    > build/logs/store.log 2>&1 &
  local pid=$!
  echo "$pid" > build/pids/store.pid
  disown "$pid" 2>/dev/null || true
  wait_for_log build/logs/store.log "Store TCP service listening" "Store"
}

start_gateway() {
  : > build/logs/gateway.log
  ./build/rstone-server --role gateway --config config/gateway.yaml --serve \
    > build/logs/gateway.log 2>&1 &
  local pid=$!
  echo "$pid" > build/pids/gateway.pid
  disown "$pid" 2>/dev/null || true
  wait_for_log build/logs/gateway.log "Gateway TCP service listening" "Gateway"
}

expect_value() {
  local key="$1"
  local expected="$2"
  local actual
  actual="$(./build/rstone-cli --endpoint 127.0.0.1:18080 get "$key")"
  if [ "$actual" != "$expected" ]; then
    echo "unexpected value for $key: expected=$expected actual=$actual" >&2
    exit 1
  fi
}

cleanup() {
  scripts/stop_local_cluster.sh >/dev/null 2>&1 || true
}
trap cleanup EXIT

rm -rf data
scripts/run_local_cluster.sh

./build/rstone-cli --endpoint 127.0.0.1:18080 put recovery:key before
expect_value recovery:key before

stop_component gateway
start_gateway
expect_value recovery:key before
./build/rstone-cli --endpoint 127.0.0.1:18080 put recovery:after-gateway after-gateway
expect_value recovery:after-gateway after-gateway

stop_component pd
start_pd
status_after_pd="$(./build/rstone-cli --endpoint 127.0.0.1:18080 status)"
echo "$status_after_pd" | grep -q "pd.store_count=3"
expect_value recovery:key before

./build/rstone-cli --endpoint 127.0.0.1:18080 put apple red
./build/rstone-cli --endpoint 127.0.0.1:18080 put zebra stripe
./build/rstone-cli --endpoint 127.0.0.1:18080 split 1 m
expect_value apple red
expect_value zebra stripe

stop_component store
start_store
expect_value apple red
expect_value zebra stripe
./build/rstone-cli --endpoint 127.0.0.1:18080 put yak large
expect_value yak large

status_after_store="$(./build/rstone-cli --endpoint 127.0.0.1:18080 status)"
echo "$status_after_store" | grep -q "store.region_count=2"

echo "RStone recovery e2e passed"
