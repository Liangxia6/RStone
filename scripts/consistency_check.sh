#!/usr/bin/env bash
set -euo pipefail
set +m

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

ENDPOINT="${1:-127.0.0.1:18080}"
HISTORY="build/consistency/history.tsv"

cleanup() {
  set +m
  scripts/stop_local_cluster.sh >/dev/null 2>&1 || true
}
trap cleanup EXIT

record() {
  local op="$1"
  local key="$2"
  local value="$3"
  local status="$4"
  local start_ns="$5"
  local end_ns="$6"
  printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$start_ns" "$end_ns" "$op" "$key" "$value" "$status" >> "$HISTORY"
}

put_value() {
  local key="$1"
  local value="$2"
  local start_ns end_ns
  start_ns="$(date +%s%N)"
  ./build/rstone-cli --endpoint "$ENDPOINT" put "$key" "$value" >/dev/null
  end_ns="$(date +%s%N)"
  record "put" "$key" "$value" "ok" "$start_ns" "$end_ns"
}

delete_key() {
  local key="$1"
  local start_ns end_ns
  start_ns="$(date +%s%N)"
  ./build/rstone-cli --endpoint "$ENDPOINT" delete "$key" >/dev/null
  end_ns="$(date +%s%N)"
  record "delete" "$key" "" "ok" "$start_ns" "$end_ns"
}

expect_get() {
  local key="$1"
  local expected="$2"
  local start_ns end_ns actual
  start_ns="$(date +%s%N)"
  local err_file="build/consistency/get.err"
  if actual="$(./build/rstone-cli --endpoint "$ENDPOINT" get "$key" 2>"$err_file")"; then
    end_ns="$(date +%s%N)"
    record "get" "$key" "$actual" "ok" "$start_ns" "$end_ns"
    if [ "$actual" != "$expected" ]; then
      echo "linearizable read failed for $key: expected=$expected actual=$actual" >&2
      exit 1
    fi
  else
    end_ns="$(date +%s%N)"
    record "get" "$key" "" "error" "$start_ns" "$end_ns"
    if [ -n "$expected" ]; then
      echo "linearizable read failed for $key: expected=$expected actual=<error>" >&2
      exit 1
    fi
  fi
}

restart_gateway() {
  local pid
  pid="$(cat build/pids/gateway.pid)"
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" 2>/dev/null || true
  rm -f build/pids/gateway.pid
  : > build/logs/gateway.log
  ./build/rstone-server --role gateway --config config/gateway.yaml --serve \
    > build/logs/gateway.log 2>&1 &
  local new_pid=$!
  echo "$new_pid" > build/pids/gateway.pid
  disown "$new_pid" 2>/dev/null || true
  for _ in $(seq 1 100); do
    if grep -q "Gateway TCP service listening" build/logs/gateway.log; then
      return 0
    fi
    sleep 0.1
  done
  echo "gateway restart did not become ready" >&2
  cat build/logs/gateway.log >&2
  exit 1
}

rm -rf data build/consistency
mkdir -p build/consistency
printf "start_ns\tend_ns\top\tkey\tvalue\tstatus\n" > "$HISTORY"

scripts/run_local_cluster.sh >/dev/null

put_value consistency:key v1
expect_get consistency:key v1
put_value consistency:key v2
expect_get consistency:key v2

./build/rstone-cli --endpoint "$ENDPOINT" transfer-leader 1 2 >/dev/null
expect_get consistency:key v2
put_value consistency:key v3
expect_get consistency:key v3

restart_gateway
expect_get consistency:key v3

put_value apple red
put_value zebra stripe
./build/rstone-cli --endpoint "$ENDPOINT" split 1 m >/dev/null
expect_get apple red
expect_get zebra stripe

put_value consistency:key v4
expect_get consistency:key v4
delete_key consistency:key
expect_get consistency:key ""

echo "RStone consistency check passed"
echo "history=$HISTORY"
