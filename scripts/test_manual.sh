#!/usr/bin/env bash
set -euo pipefail

"$(dirname "$0")/build_manual.sh"
./build/rstone_unit_tests_manual
