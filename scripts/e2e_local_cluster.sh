#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

cleanup() {
  scripts/stop_local_cluster.sh >/dev/null 2>&1 || true
}
trap cleanup EXIT

rm -rf data
scripts/run_local_cluster.sh

./build/rstone-cli --endpoint 127.0.0.1:8081 put e2e:key value
value="$(./build/rstone-cli --endpoint 127.0.0.1:8081 get e2e:key)"
if [ "$value" != "value" ]; then
  echo "unexpected value: $value" >&2
  exit 1
fi

./build/rstone-cli --endpoint 127.0.0.1:8081 delete e2e:key
if ./build/rstone-cli --endpoint 127.0.0.1:8081 get e2e:key >/tmp/rstone_e2e_get.out 2>/tmp/rstone_e2e_get.err; then
  echo "expected deleted key to be missing" >&2
  exit 1
fi

./build/rstone-cli --endpoint 127.0.0.1:8081 put apple red
./build/rstone-cli --endpoint 127.0.0.1:8081 put zebra stripe
./build/rstone-cli --endpoint 127.0.0.1:8081 transfer-leader 1 2
./build/rstone-cli --endpoint 127.0.0.1:8081 put banana yellow
transfer_value="$(./build/rstone-cli --endpoint 127.0.0.1:8081 get banana)"
if [ "$transfer_value" != "yellow" ]; then
  echo "transfer leader verification failed: value=$transfer_value" >&2
  exit 1
fi
./build/rstone-cli --endpoint 127.0.0.1:8081 remove-peer 1 3
./build/rstone-cli --endpoint 127.0.0.1:8081 put cherry red
membership_value="$(./build/rstone-cli --endpoint 127.0.0.1:8081 get cherry)"
if [ "$membership_value" != "red" ]; then
  echo "membership verification failed: value=$membership_value" >&2
  exit 1
fi
./build/rstone-cli --endpoint 127.0.0.1:8081 split 1 m
left_value="$(./build/rstone-cli --endpoint 127.0.0.1:8081 get apple)"
right_value="$(./build/rstone-cli --endpoint 127.0.0.1:8081 get zebra)"
if [ "$left_value" != "red" ] || [ "$right_value" != "stripe" ]; then
  echo "split verification failed: left=$left_value right=$right_value" >&2
  exit 1
fi

status_output="$(./build/rstone-cli --endpoint 127.0.0.1:8081 status)"
echo "$status_output" | grep -q "pd.store_count=3"
echo "$status_output" | grep -q "store.region_count=2"

echo "RStone local cluster e2e passed"
