Global Patent Intelligence Data Pipeline — submission
======================================================

Dashboard
---------
  Local:   http://localhost:8501  (after `uv run patent-pipeline dashboard`)
  Source:  src/patent_pipeline/dashboard.py

GitHub repository
-----------------
  https://github.com/mpairwe7/patent-pipeline


What this project delivers
--------------------------
A production-shaped data pipeline that ingests USPTO PatentsView
Granted Patent Disambiguated bulk data (1976-01-01 to 2025-09-30),
cleans it, warehouses it in DuckDB, and produces analytical reports
in three formats (Console / CSV / JSON) plus an interactive 8-tab
Streamlit dashboard.

Real-data scale verified end-to-end:
   9,361,444  patents
   4,257,666  inventors
     567,568  companies
  24,985,193  patent ↔ inventor relationships
  17,668,944  CPC classifications
   span 1976 – 2025  (50 years)


Deliverable cross-reference (submission spec)
---------------------------------------------
[x] Code Files
    - Python scripts             src/patent_pipeline/*.py
                                  scripts/make_sample.py
    - SQL scripts                sql/queries/q1…q12.sql + sql/schema.sql

[x] Clean Data Files
    - clean_patents.csv          data/clean/clean_patents.csv
    - clean_inventors.csv        data/clean/clean_inventors.csv
    - clean_companies.csv        data/clean/clean_companies.csv
    (also clean_relationships.csv, clean_cpc.csv + Parquet equivalents)

[x] Database File
    - schema.sql                 sql/schema.sql

[x] Required reports
    - Console                    captured in reports/console_report.txt
    - top_inventors.csv          reports/top_inventors.csv
    - top_companies.csv          reports/top_companies.csv
    - country_trends.csv         reports/country_trends.csv
    - JSON report                reports/patent_report.json

[x] Bonus deliverables
    - Plotly + matplotlib graphs reports/figures/*.png  (+ *.html)
    - Streamlit dashboard        src/patent_pipeline/dashboard.py
    - CPC category analysis      sql/queries/q11_section_growth.sql
                                  sql/queries/q12_company_section_matrix.sql
                                  reports/cpc_breakdown.csv
                                  dashboard "CPC" + "Advanced" tabs


Snapshots — dashboard charts (PNG, vector + raster equivalents)
---------------------------------------------------------------
  reports/figures/yearly_trends.png         patents per year
  reports/figures/top_companies.png         top-15 companies bar
  reports/figures/country_share.png         country share donut
  reports/figures/cpc_sections.png          CPC section breakdown
  reports/figures/decade_comparison.png     5-year decade buckets
  reports/figures/company_cagr.png          top-15 by CAGR
  reports/figures/country_growth.png        first-vs-second-half growth
  reports/figures/section_growth.png        CPC section growth
  reports/figures/company_section_heatmap.png  Co × CPC heatmap
  Same set as interactive HTML in reports/figures/*.html


Snapshot — console report (verbatim CLI output)
------------------------------------------------
  reports/console_report.txt   captured via `uv run patent-pipeline analyze`


Reproduce in three commands
---------------------------
  uv sync --all-extras
  uv run patent-pipeline run-all          # bundled sample (5k patents)
  uv run patent-pipeline dashboard        # http://localhost:8501

Real PatentsView (1976-2025, ~30 GB uncompressed) instead of sample:
  uv run patent-pipeline run-all --no-use-sample --year-from 1976 --year-to 2025 --parquet


Stack
-----
  Python 3.12/3.13 · uv · pandas (PyArrow backend) · DuckDB · httpx · tqdm
  Plotly · matplotlib · Streamlit · Typer · Rich · Pydantic · Loguru · Ruff
