Global Patent Intelligence Data Pipeline — Submission
======================================================

Author : Mpairwe Lauben
Reg #  : 22/U/21345
Course : Cloud Computing — Data Pipeline Mini Project
Date   : 2026-05-05


Project links
-------------
Live dashboard (Hugging Face Space, Docker SDK):
    https://landwind22-globalpatentintelligencedatasystem.hf.space

Hugging Face Space repo (source + Dockerfile):
    https://huggingface.co/spaces/landwind22/GlobalPatentIntelligenceDataSystem

GitHub source repository:
    https://github.com/mpairwe7/patent-pipeline


About the project
-----------------
End-to-end pipeline that ingests USPTO PatentsView Granted Patent
Disambiguated bulk data (1976-01-01 → 2025-09-30), cleans it with
pandas, warehouses it in DuckDB, runs SQL analytics, and serves an
interactive Streamlit dashboard.

Stack: Python 3.12 · uv · pandas (PyArrow) · DuckDB · Typer/Rich ·
       Plotly · Streamlit · Podman/Docker · GitHub Actions


How to run
----------
    git clone https://github.com/mpairwe7/patent-pipeline.git
    cd patent-pipeline
    make install          # uv sync
    make run              # ingest -> clean -> load -> analyze
    make dashboard        # http://localhost:8501

Or pull the Docker image:
    podman build -t patent-dashboard .
    podman run --rm -p 7860:7860 patent-dashboard
    # open http://localhost:7860


Folder contents
---------------
readme.txt                       This file.

dashboard_snapshots/             19 PNG renders covering every dashboard chart
                                 (Plotly, colour-blind-safe Wong palette).
                                 Files are numbered to match the dashboard
                                 tabs (Trends → Geography → CPC → Inventors
                                 → Companies → Advanced).

  Trends tab:
    01_trends_yearly_volume.png         Patents filed per year (line + area).
    02_trends_yoy_growth.png            Year-over-year % change (green/red).
    03_trends_cumulative.png            Cumulative patents (line).

  Geography tab:
    04_geography_world_choropleth.png   World map of inventor countries.
    05_geography_country_bar.png        Top 15 inventor countries (bar).
    06_geography_country_pie.png        Top 12 country concentration (donut).

  CPC tab:
    07_cpc_section_breakdown.png        Patents per CPC section (bar).
    08_cpc_treemap.png                  Section → class hierarchy (treemap).
    09_cpc_section_trends.png           Section trends over time (multi-line).

  Inventors tab:
    10_inventors_top20.png              Top 20 inventors (bar, by country).
    11_inventors_top3_by_country.png    Top-3 per country (window function).

  Companies tab:
    12_companies_top20.png              Top 20 assignee companies (bar).
    13_companies_concentration_pie.png  Top 12 companies (donut).
    14_companies_innovation_leaders.png Top-5 companies' patents/year (line).

  Advanced tab:
    15_advanced_decade_comparison.png   Per-decade volume (bar).
    16_advanced_company_cagr.png        Top 15 by CAGR (bar).
    17_advanced_country_growth_scatter.png  Pre-/post-2000 scatter.
    18_advanced_section_growth.png      CPC section growth % (bar).
    19_advanced_company_section_heatmap.png Top 15 companies × CPC (heatmap).

console_report/                  Outputs from `patent-pipeline analyze`
                                 against the full real PatentsView corpus:
    console_report.txt           Rich-formatted console tables.
    patent_report.json           Structured JSON of all KPIs and rankings.


Headline numbers (from console_report.txt — full real corpus)
-------------------------------------------------------------
    Total Patents       9,361,444
    Total Inventors     4,257,666
    Total Companies       567,568
    Year Range          1976 – 2025
    Relationships      24,985,193

    Top inventor       Shunpei Yamazaki (JP)        6,761 patents
    Top company        Samsung Electronics Co.    171,950 patents
    Top country        United States              5,040,423 (48.6 %)


License
-------
MIT — see https://github.com/mpairwe7/patent-pipeline/blob/main/LICENSE
