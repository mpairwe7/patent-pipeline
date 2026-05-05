# syntax=docker/dockerfile:1.7
#
# Patent Intelligence Dashboard — container image for Hugging Face Spaces.
# Build with either Docker or Podman:
#   podman build -t patent-dashboard .
#   docker build -t patent-dashboard .
#
# Three stages keep the final image lean:
#   1. deps     — installs Python deps into a venv via uv (cached layer)
#   2. warehouse — runs the full pipeline against the bundled sample TSVs
#                  to produce a small DuckDB warehouse + report artifacts
#   3. runtime  — copies just the venv + source + warehouse, runs Streamlit
#                  on :7860 (the port Hugging Face Spaces expects)

ARG PYTHON_VERSION=3.12-slim-bookworm

# ---------------------------------------------------------------------------
# Stage 1 — install Python dependencies into /app/.venv via uv
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION} AS deps

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/

RUN uv sync --frozen --no-dev

# ---------------------------------------------------------------------------
# Stage 2 — build the DuckDB warehouse from the bundled sample TSVs
# ---------------------------------------------------------------------------
FROM deps AS warehouse

COPY config/ ./config/
COPY sql/ ./sql/
COPY data/sample/ ./data/sample/

RUN mkdir -p data/raw data/clean data/warehouse reports/figures \
 && /app/.venv/bin/patent-pipeline run-all --use-sample --log-level INFO

# ---------------------------------------------------------------------------
# Stage 3 — minimal runtime image
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION} AS runtime

# Hugging Face Spaces runs containers as a non-root user with UID 1000.
# Create the same layout locally so podman/docker behave identically.
RUN groupadd --system --gid 1000 user \
 && useradd  --system --uid 1000 --gid 1000 --create-home --home-dir /home/user user

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/app/.venv/bin:$PATH \
    HOME=/home/user \
    STREAMLIT_SERVER_PORT=7860 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLE_CORS=false \
    STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY --from=warehouse --chown=user:user /app/.venv               /app/.venv
COPY --from=warehouse --chown=user:user /app/src                 /app/src
COPY --from=warehouse --chown=user:user /app/config              /app/config
COPY --from=warehouse --chown=user:user /app/sql                 /app/sql
COPY --from=warehouse --chown=user:user /app/data/warehouse      /app/data/warehouse
COPY --from=warehouse --chown=user:user /app/data/clean          /app/data/clean
COPY --from=warehouse --chown=user:user /app/reports             /app/reports
COPY --chown=user:user .streamlit/                               /app/.streamlit/
COPY --chown=user:user docker-entrypoint.sh                      /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
 && mkdir -p /home/user/.streamlit \
 && chown -R user:user /home/user

USER user

EXPOSE 7860

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:7860/_stcore/health', timeout=3).status==200 else 1)"

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["streamlit", "run", "src/patent_pipeline/dashboard.py"]
