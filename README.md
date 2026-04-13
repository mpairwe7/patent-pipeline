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

## Data source

[USPTO PatentsView Granted Patent Disambiguated data](https://data.uspto.gov/bulkdata/datasets/pvgpatdis?fileDataFromDate=1976-01-01&fileDataToDate=2025-09-30)
— public domain. Coverage: 1976-01-01 through 2025-09-30.

## License

MIT — see [`LICENSE`](LICENSE).
