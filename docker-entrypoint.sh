#!/usr/bin/env bash
# Container entrypoint for the Patent Intelligence dashboard.
#
# - In artifact mode, require reports/patent_report.json before starting.
# - In warehouse mode (default), if the DB is missing, download the full-corpus
#   warehouse from the HF Dataset (landwind22/patent-pipeline-clean → warehouse/)
#   via scripts/fetch_clean.py, which verifies sha256 against
#   config/clean_manifest.json. Refuse to fall back to a 10K-patent sample on
#   the public Space.
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
    echo "[entrypoint] warehouse missing at $WAREHOUSE — fetching from HF..."
    /app/.venv/bin/python /app/scripts/fetch_clean.py --format warehouse
    if [ ! -s "$WAREHOUSE" ]; then
      echo "[entrypoint] warehouse download failed — aborting" >&2
      exit 1
    fi
  fi
fi

exec "$@"
