#!/usr/bin/env bash
# Container entrypoint for the Patent Intelligence dashboard.
#
# - If the warehouse is missing (volume-mounted blank /app/data, fresh checkout
#   without a build-time pipeline run), rebuild it from the bundled sample TSVs
#   so the dashboard always has something to render.
# - Then exec the configured CMD (defaults to `streamlit run`).
set -euo pipefail

WAREHOUSE="${WAREHOUSE_DB:-/app/data/warehouse/patents.duckdb}"

if [ ! -s "$WAREHOUSE" ]; then
  echo "[entrypoint] warehouse missing at $WAREHOUSE — rebuilding from sample data..."
  /app/.venv/bin/patent-pipeline run-all --use-sample --log-level INFO
fi

exec "$@"
