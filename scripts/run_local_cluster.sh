#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

scripts/build_manual.sh
mkdir -p build/logs build/pids
rm -f build/pids/*.pid

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

./build/rstone-server --role pd --config config/pd.yaml --serve \
  > build/logs/pd.log 2>&1 &
echo $! > build/pids/pd.pid
wait_for_log build/logs/pd.log "PD TCP service listening" "PD"

./build/rstone-server --role store --config config/store1.yaml --serve \
  > build/logs/store.log 2>&1 &
echo $! > build/pids/store.pid
wait_for_log build/logs/store.log "Store TCP service listening" "Store"

./build/rstone-server --role gateway --config config/gateway.yaml --serve \
  > build/logs/gateway.log 2>&1 &
echo $! > build/pids/gateway.pid
wait_for_log build/logs/gateway.log "Gateway TCP service listening" "Gateway"

echo "RStone local cluster started"
echo "PD log: build/logs/pd.log"
echo "Store log: build/logs/store.log"
echo "Gateway log: build/logs/gateway.log"
