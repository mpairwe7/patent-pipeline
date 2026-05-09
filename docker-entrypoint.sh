#!/usr/bin/env bash
# Container entrypoint for the Patent Intelligence dashboard.
#
# - In artifact mode, require reports/patent_report.json (precomputed full-corpus
#   report payload) before starting.
# - In warehouse mode, if the DB is missing, rebuild from bundled sample TSVs.
# - Then exec the configured CMD (defaults to `streamlit run`).
set -euo pipefail

WAREHOUSE="${WAREHOUSE_DB:-/app/data/warehouse/patents.duckdb}"
REPORT_JSON="/app/reports/patent_report.json"
DATA_SOURCE="${PATENT_DASHBOARD_SOURCE:-warehouse}"

if [ "$DATA_SOURCE" = "artifacts" ]; then
  if [ ! -s "$REPORT_JSON" ]; then
    echo "[entrypoint] artifact mode requested but missing $REPORT_JSON"
    exit 1
  fi
else
  if [ ! -s "$WAREHOUSE" ]; then
    echo "[entrypoint] warehouse missing at $WAREHOUSE — rebuilding from sample data..."
    /app/.venv/bin/patent-pipeline run-all --use-sample --log-level INFO
  fi
fi

exec "$@"
