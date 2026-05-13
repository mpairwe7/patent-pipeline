---
title: Global Patent Intelligence Dashboard
emoji: 🧪
colorFrom: indigo
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
license: mit
short_description: Interactive analytics over USPTO PatentsView 1976–2025.
---

# Global Patent Intelligence Data Pipeline

A production-shaped, reproducible data pipeline that ingests USPTO **PatentsView Granted Patent
Disambiguated** bulk data, cleans it with pandas, warehouses it in DuckDB, and produces
analytical reports in three formats (console, CSV, JSON) plus an interactive Streamlit dashboard.

Built for the *Cloud Computing — Data Pipeline Mini Project* and modelled on the 2026 Python data-
engineering stack: **uv + pandas (PyArrow backend) + DuckDB + Typer/Rich + Streamlit + Ruff +
GitHub Actions**.

---

## Architecture

```
┌─────────────────┐   ┌───────────────────┐   ┌─────────────────┐   ┌──────────────────┐
│   Data source   │──▶│  Python ingest    │──▶│  pandas clean   │──▶│  DuckDB load     │
│  (PatentsView)  │   │  (httpx + tqdm)   │   │  (PyArrow)      │   │  (schema.sql)    │
└─────────────────┘   └───────────────────┘   └─────────────────┘   └────────┬─────────┘
                                                                              │
                                   ┌──────────────────────────────────────────┘
                                   ▼
                        ┌───────────────────────┐
                        │  SQL analyze          │──┐
                        │  (Q1–Q7 queries)      │  │
                        └───────────────────────┘  │
                                                   ▼
                       ┌──────────┬────────────┬──────────────┬─────────────┐
                       │ Console  │  CSV       │  JSON        │  Streamlit  │
                       │ (Rich)   │  reports   │  report      │  dashboard  │
                       └──────────┴────────────┴──────────────┴─────────────┘
```

### Stack

| Concern | Tool | Notes |
|---|---|---|
| Package manager | **uv** | Reproducible lockfile (`uv.lock`), 10–100× faster than pip |
| Python | **3.12 / 3.13** | `requires-python = ">=3.12,<3.14"` |
| Ingestion | **httpx + tqdm** | Async-capable HTTP client, progress bars |
| Cleaning | **pandas 2.2 + PyArrow** | `dtype_backend="pyarrow"` for speed and proper nulls |
| Warehouse | **DuckDB 1.x** | File-based analytical SQL engine (CTE, window functions, fast COPY) |
| CLI | **Typer + Rich** | Typed sub-commands, colourful console output |
| Config | **Pydantic v2 + YAML** | Type-safe `config/pipeline.yaml` |
| Logging | **Loguru** | Structured, zero-config |
| Visualisation | **Plotly + Matplotlib** | Interactive HTML + static PNG |
| Dashboard | **Streamlit** | `patent-pipeline dashboard` |
| Lint / format | **Ruff** | Replaces black + isort + flake8 |
| Tests | **pytest** | Unit + smoke |
| CI | **GitHub Actions** | Lint + tests + full-pipeline smoke run |

---

## Quick start

```bash
# 1. Install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Clone and enter the repo
git clone <repo-url> patent-pipeline && cd patent-pipeline

# 3. Install dependencies (creates .venv automatically)
uv sync --all-extras

# 4. Run the entire pipeline on the bundled sample
uv run patent-pipeline run-all

# 5. Explore the Streamlit dashboard
uv run patent-pipeline dashboard
```

No USPTO download is required — a realistic sample is checked into `data/sample/`.

### Running on the real USPTO dataset

```bash
# Download the real disambiguated granted-patent TSVs (~GBs; needs disk)
uv run patent-pipeline ingest --no-use-sample

# Or point at a specific URL manifest
uv run patent-pipeline ingest --no-use-sample --url https://…/g_patent.tsv.zip
```

---

## Full-corpus clean dataset (hosted on Hugging Face)

The bundled `data/sample/` covers ~10K patents — enough to run the pipeline
end-to-end on a laptop in seconds. The **full-corpus clean dataset**
(1976 → 2025, 9.36 M patents, 56.8 M rows across the five tables) is
multi-GB and exceeds GitHub LFS quotas, so it's hosted as a Hugging Face
Dataset and **not** committed to this repo.

| File | CSV | Parquet (zstd) | Rows |
|---|---|---|---|
| `clean_patents` | 722 MB | 206 MB | 9,361,444 |
| `clean_inventors` | 157 MB | 75 MB | 4,257,666 |
| `clean_companies` | 35 MB | 18 MB | 567,568 |
| `clean_relationships` | 1.5 GB | 480 MB | 24,985,193 |
| `clean_cpc` | 326 MB | 102 MB | 17,668,944 |
| **Bundle** | **≈ 2.7 GB** | **≈ 881 MB** | **56.8 M** |

### Download

```bash
# Parquet bundle (~881 MB, recommended — load straight into DuckDB / pandas)
make fetch-clean
# or:
uv run python scripts/fetch_clean.py

# CSV bundle too (~2.7 GB extra)
uv run python scripts/fetch_clean.py --format both

# Verify existing local files against the manifest without re-downloading
uv run python scripts/fetch_clean.py --check
```

The script reads `config/clean_manifest.json` (committed) for the dataset
URL, file list, byte sizes, and sha256 hashes — any download is verified
before being trusted. No `huggingface_hub` SDK is required.

### Regenerate from raw

If you have the raw PatentsView TSVs in `data/raw/`, you can regenerate
the clean bundle locally instead of downloading:

```bash
uv run patent-pipeline clean --parquet     # ~12 min on a laptop SSD
```

### Publishing a new version (maintainer)

```bash
# 1. Regenerate locally
uv run patent-pipeline clean --parquet

# 2. Update sha256 + bytes in config/clean_manifest.json
sha256sum data/clean/parquet/*.parquet data/clean/*.csv

# 3. Upload to HF (one-time: `huggingface-cli login`)
huggingface-cli upload landwind22/patent-pipeline-clean \
    data/clean/parquet parquet --repo-type dataset
huggingface-cli upload landwind22/patent-pipeline-clean \
    data/clean csv --repo-type dataset --include 'clean_*.csv'

# 4. Commit the updated manifest
git add config/clean_manifest.json && git commit -m "data: refresh clean dataset manifest"
```

---

## CLI

```
uv run patent-pipeline <command> [options]

  ingest       Populate data/raw/ (from data/sample/ by default, or download)
  clean        Clean TSV → data/clean/*.csv using pandas
  load         Load clean CSVs into DuckDB at data/warehouse/patents.duckdb
  analyze      Run sql/queries/*.sql, produce console/CSV/JSON reports + figures
  dashboard    Start the Streamlit dashboard
  run-all      ingest → clean → load → analyze
  version      Print package version
```

---

## Database schema

```sql
-- sql/schema.sql (excerpt)
CREATE TABLE patents             (patent_id PK, title, abstract, filing_date, year);
CREATE TABLE inventors           (inventor_id PK, name, country);
CREATE TABLE companies           (company_id PK, name);
CREATE TABLE patent_relationships(patent_id, inventor_id, company_id);
CREATE TABLE patent_cpc          (patent_id, section, cpc_class, cpc_subclass);
```

See the full file: [`sql/schema.sql`](sql/schema.sql).

## Analytical queries

| # | File | Description |
|---|---|---|
| Q1 | [`q1_top_inventors.sql`](sql/queries/q1_top_inventors.sql) | Inventors with the most patents |
| Q2 | [`q2_top_companies.sql`](sql/queries/q2_top_companies.sql) | Companies with the most patents |
| Q3 | [`q3_top_countries.sql`](sql/queries/q3_top_countries.sql) | Countries producing the most patents (with share) |
| Q4 | [`q4_trends_over_time.sql`](sql/queries/q4_trends_over_time.sql) | Patent count per year |
| Q5 | [`q5_join_patents_inventors_companies.sql`](sql/queries/q5_join_patents_inventors_companies.sql) | Full JOIN across all entities |
| Q6 | [`q6_cte_innovation_leaders.sql`](sql/queries/q6_cte_innovation_leaders.sql) | CTE — YoY growth of top companies |
| Q7 | [`q7_rank_inventors_window.sql`](sql/queries/q7_rank_inventors_window.sql) | Window function — top-N inventors per country |

Run any query directly:

```bash
duckdb data/warehouse/patents.duckdb < sql/queries/q1_top_inventors.sql
```

---

## Outputs

After `run-all`:

```
data/clean/
  clean_patents.csv
  clean_inventors.csv
  clean_companies.csv
  clean_relationships.csv
  clean_cpc.csv

data/warehouse/
  patents.duckdb

reports/
  top_inventors.csv         # assignment deliverable
  top_companies.csv         # assignment deliverable
  country_trends.csv        # assignment deliverable
  patent_report.json        # consolidated JSON report
  figures/
    yearly_trends.png
    top_companies.png
    country_share.png
    cpc_sections.png
```

### Sample console report

```
╭─────────────── PATENT INTELLIGENCE REPORT ───────────────╮
│ Total Patents: 1,000                                     │
│ Total Inventors: 1,820                                   │
│ Total Companies: 315                                     │
│ Year Range: 2020 – 2025                                  │
╰──────────────────────────────────────────────────────────╯
TOP INVENTORS            TOP COMPANIES           TOP COUNTRIES
1. Liu Wei        42     1. IBM         112     1. US   58.1 %
2. Rajesh Kumar   38     2. Samsung      96     2. CN   18.7 %
…                        …                      …
```

---

## Project layout

```
├── config/pipeline.yaml          # runtime configuration
├── data/
│   ├── raw/                      # source TSVs (gitignored except .gitkeep)
│   ├── sample/                   # bundled sample TSVs (committed)
│   ├── clean/                    # cleaned CSVs (gitignored)
│   └── warehouse/patents.duckdb  # DuckDB file (gitignored)
├── sql/
│   ├── schema.sql
│   └── queries/q1…q7.sql
├── src/patent_pipeline/
│   ├── cli.py  config.py  ingest.py  clean.py  load.py
│   ├── analyze.py  report.py  visualize.py  dashboard.py  logging_setup.py
├── reports/                      # outputs (CSV, JSON, figures)
├── scripts/make_sample.py        # regenerate data/sample/
├── tests/                        # pytest
├── pyproject.toml  uv.lock  Makefile  .python-version
└── .github/workflows/ci.yml
```

---

## Development

```bash
uv sync --all-extras          # install with dev deps
uv run pytest                 # run tests
uv run ruff check             # lint
uv run ruff format            # format
uv run pre-commit install     # enable git hooks
```

---

## Learning outcomes (assignment checklist)

- [x] Work with real bulk-data files (PatentsView disambiguated TSVs)
- [x] Clean and process datasets with pandas
- [x] Store data in a SQL database (DuckDB)
- [x] Write analytical SQL queries (JOIN, CTE with `WITH`, window `OVER`)
- [x] Generate structured reports (Console / CSV / JSON)
- [x] Build a full, reproducible data pipeline
- [x] Bonus — Plotly visualisations
- [x] Bonus — Streamlit dashboard
- [x] Bonus — CPC category analysis

---

## Container / Hugging Face Spaces deployment

The repo ships a **multi-stage `Dockerfile`** (Podman- and Docker-compatible) that:

1. installs locked dependencies into `/app/.venv` via `uv sync --frozen`,
2. bakes in precomputed `reports/` + dashboard figure artifacts from this repo
   (full-corpus values), and
3. starts Streamlit on **port 7860** (the port Hugging Face Spaces expects) as a
   non-root UID 1000 user in artifact mode (`PATENT_DASHBOARD_SOURCE=artifacts`).

**Build & run with Podman** (or substitute `docker` — same Dockerfile):

```bash
podman build -t patent-dashboard .
podman run --rm -p 7860:7860 patent-dashboard
# → open http://localhost:7860
```

**Deploy to Hugging Face Spaces:**

1. Create a new Space → **SDK = Docker**.
2. Push this repo to the Space's git remote. Spaces reads the YAML frontmatter at
   the top of this `README.md` (`sdk: docker`, `app_port: 7860`) and builds the
   `Dockerfile` automatically.
3. First build installs dependencies and copies artifacts; subsequent rebuilds are cached.

The `docker-entrypoint.sh` validates that `reports/patent_report.json` exists in
artifact mode before starting Streamlit.

## Data source

## License

MIT — see [`LICENSE`](LICENSE).
