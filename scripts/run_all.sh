#!/usr/bin/env bash
# Run the daily job once per folder, each day in its own process (like a scheduler would).
# Resumable: days that already finished (dq/reconciliation partition exists) are skipped.
#
#   scripts/run_all.sh                 # all folders in date order
#   scripts/run_all.sh 2026-10-16      # from this day on
#
# Uses config/config.json (or $FALCON_CONFIG).  Needs JAVA_HOME for local Spark.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH=src
FROM="${1:-0000-00-00}"

DATASET=$(uv run --no-sync python -c "from falcon import config; print(config.load()['dataset_path'])")
WH=$(uv run --no-sync python -c "from falcon import config; print(config.load()['warehouse_path'])")

for dir in $(ls -d "$DATASET"/dt=* | sort); do
  d="${dir##*dt=}"
  [[ "$d" < "$FROM" ]] && continue
  if [[ -d "$WH/dq/reconciliation/dt=$d" ]]; then echo "skip $d (already loaded)"; continue; fi
  uv run --no-sync python -m falcon.run --date "$d" 2>&1 | tr '\r' '\n' | grep -v -E "WARN|^\[Stage|^$|setLogLevel"
done
