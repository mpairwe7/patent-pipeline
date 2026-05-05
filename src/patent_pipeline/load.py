"""Load stage — create the DuckDB warehouse and populate it from clean
CSV (or Parquet, if available) outputs.

Parquet is preferred when present: typed, columnar, ~5-10× smaller than
the equivalent CSV on the real PatentsView bundle and loads in seconds.

After the base tables are loaded we precompute a small set of
**materialized summary tables** (``mv_*``). At the real-USPTO scale (9 M
patents, 25 M relationships) the dashboard's hot paths can't afford to
re-aggregate from scratch on every render — these views collapse the
warehouse to a few hundred thousand rows the dashboard can scan in
milliseconds.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from patent_pipeline.config import Settings
from patent_pipeline.logging_setup import logger

# (table, csv_filename, parquet_filename)
TABLE_FILES = [
    ("patents", "clean_patents.csv", "patents.parquet"),
    ("inventors", "clean_inventors.csv", "inventors.parquet"),
    ("companies", "clean_companies.csv", "companies.parquet"),
    ("patent_relationships", "clean_relationships.csv", "relationships.parquet"),
    ("patent_cpc", "clean_cpc.csv", "cpc.parquet"),
]


def _execute_schema(conn: duckdb.DuckDBPyConnection, schema_file: Path) -> None:
    sql = schema_file.read_text(encoding="utf-8")
    conn.execute(sql)


def _ingest_into(conn: duckdb.DuckDBPyConnection, table: str, src: Path) -> int:
    """Stream ``src`` (CSV or Parquet) into ``table`` using DuckDB's native
    readers. Both readers are streaming so memory stays flat regardless
    of input size.
    """
    if src.suffix == ".parquet":
        conn.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{src.as_posix()}')")
    else:
        conn.execute(
            f"""
            INSERT INTO {table}
            SELECT * FROM read_csv_auto('{src.as_posix()}', header=True, null_padding=true)
            """
        )
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info(f"loaded {table}: {count:,} rows from {src.name}")
    return count


def _build_summaries(conn: duckdb.DuckDBPyConnection) -> None:
    """Pre-aggregate the warehouse into ``mv_*`` summary tables.

    Each view collapses millions of base rows into thousands so the
    dashboard's hot queries are constant-time relative to the underlying
    data volume. Tuned for the real PatentsView span (1976-2025).
    """
    logger.info("building materialized summaries…")

    # ---- yearly base ---------------------------------------------------
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_yearly AS
        SELECT year, COUNT(DISTINCT patent_id) AS patents
        FROM patents WHERE year IS NOT NULL
        GROUP BY year ORDER BY year;
        """
    )

    # ---- country × year ------------------------------------------------
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_country_yearly AS
        SELECT i.country, p.year, COUNT(DISTINCT p.patent_id) AS patents
        FROM inventors i
        JOIN patent_relationships r ON r.inventor_id = i.inventor_id
        JOIN patents p              ON p.patent_id   = r.patent_id
        WHERE i.country IS NOT NULL AND i.country <> '' AND p.year IS NOT NULL
        GROUP BY i.country, p.year;
        """
    )

    # ---- section × year ------------------------------------------------
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_section_yearly AS
        SELECT pc.section, pc.cpc_class, p.year,
               COUNT(DISTINCT p.patent_id) AS patents
        FROM patent_cpc pc
        JOIN patents p ON p.patent_id = pc.patent_id
        WHERE pc.section IS NOT NULL AND p.year IS NOT NULL
        GROUP BY pc.section, pc.cpc_class, p.year;
        """
    )

    # ---- per-entity totals --------------------------------------------
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_company_total AS
        SELECT c.company_id, c.name AS company,
               COUNT(DISTINCT r.patent_id) AS total_patents,
               MIN(p.year) AS first_year, MAX(p.year) AS last_year
        FROM companies c
        JOIN patent_relationships r ON r.company_id = c.company_id
        JOIN patents p              ON p.patent_id  = r.patent_id
        WHERE p.year IS NOT NULL
        GROUP BY c.company_id, c.name;
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_inventor_total AS
        SELECT i.inventor_id, i.name AS inventor,
               COALESCE(i.country, '?') AS country,
               COUNT(DISTINCT r.patent_id) AS total_patents,
               MIN(p.year) AS first_year, MAX(p.year) AS last_year
        FROM inventors i
        JOIN patent_relationships r ON r.inventor_id = i.inventor_id
        JOIN patents p              ON p.patent_id   = r.patent_id
        WHERE p.year IS NOT NULL
        GROUP BY i.inventor_id, i.name, i.country;
        """
    )

    # ---- top-N per-year breakdowns (used for trend lines) -------------
    # Limit to the 200 most-prolific companies / 500 most-prolific inventors —
    # everything below is statistical noise for visualisation.
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_company_yearly AS
        WITH top_co AS (
            SELECT company_id FROM mv_company_total
            ORDER BY total_patents DESC LIMIT 500
        )
        SELECT c.name AS company, p.year,
               COUNT(DISTINCT p.patent_id) AS patents
        FROM top_co tc
        JOIN companies c            ON c.company_id  = tc.company_id
        JOIN patent_relationships r ON r.company_id  = tc.company_id
        JOIN patents p              ON p.patent_id   = r.patent_id
        WHERE p.year IS NOT NULL
        GROUP BY c.name, p.year;
        """
    )

    # ---- company × section heatmap (top 50 companies, all sections) ---
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_company_section AS
        WITH top_co AS (
            SELECT company_id, company AS company_name
            FROM mv_company_total ORDER BY total_patents DESC LIMIT 50
        )
        SELECT tc.company_name, pc.section,
               COUNT(DISTINCT pc.patent_id) AS patents
        FROM top_co tc
        JOIN patent_relationships r ON r.company_id = tc.company_id
        JOIN patent_cpc           pc ON pc.patent_id = r.patent_id
        WHERE pc.section IS NOT NULL
        GROUP BY tc.company_name, pc.section;
        """
    )

    # ---- decade-bucket comparison (Advanced tab) ----------------------
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_decade_compare AS
        WITH bucketed AS (
            SELECT
                p.patent_id,
                CASE
                    WHEN p.year BETWEEN 1976 AND 1989 THEN '1976-1989'
                    WHEN p.year BETWEEN 1990 AND 1999 THEN '1990-1999'
                    WHEN p.year BETWEEN 2000 AND 2009 THEN '2000-2009'
                    WHEN p.year BETWEEN 2010 AND 2019 THEN '2010-2019'
                    WHEN p.year BETWEEN 2020 AND 2025 THEN '2020-2025'
                    ELSE 'other'
                END AS period
            FROM patents p WHERE p.year IS NOT NULL
        )
        SELECT
            b.period,
            COUNT(DISTINCT b.patent_id)               AS patent_count,
            COUNT(DISTINCT r.inventor_id)             AS inventor_count,
            COUNT(DISTINCT r.company_id)              AS company_count,
            ROUND(
                COUNT(DISTINCT b.patent_id)
                / SUM(COUNT(DISTINCT b.patent_id)) OVER (), 4
            ) AS share
        FROM bucketed b
        LEFT JOIN patent_relationships r ON r.patent_id = b.patent_id
        WHERE b.period <> 'other'
        GROUP BY b.period ORDER BY b.period;
        """
    )

    # ---- country first-half vs second-half growth ---------------------
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_country_halfsplit AS
        WITH bounds AS (
            SELECT (MIN(year) + MAX(year)) / 2 AS y_mid
            FROM patents WHERE year IS NOT NULL
        ),
        labelled AS (
            SELECT
                i.country, p.patent_id,
                CASE WHEN p.year <= b.y_mid THEN 'first' ELSE 'second' END AS half
            FROM patents p
            JOIN patent_relationships r ON r.patent_id   = p.patent_id
            JOIN inventors            i ON i.inventor_id = r.inventor_id
            CROSS JOIN bounds b
            WHERE p.year IS NOT NULL
                  AND i.country IS NOT NULL AND i.country <> ''
        )
        SELECT
            country,
            COUNT(DISTINCT CASE WHEN half = 'first'  THEN patent_id END) AS first_half_patents,
            COUNT(DISTINCT CASE WHEN half = 'second' THEN patent_id END) AS second_half_patents,
            COUNT(DISTINCT patent_id)                                    AS total_patents
        FROM labelled GROUP BY country;
        """
    )

    # ---- CPC section first-half vs second-half growth -----------------
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_section_halfsplit AS
        WITH bounds AS (
            SELECT (MIN(year) + MAX(year)) / 2 AS y_mid
            FROM patents WHERE year IS NOT NULL
        ),
        labelled AS (
            SELECT
                pc.section, p.patent_id,
                CASE WHEN p.year <= b.y_mid THEN 'first' ELSE 'second' END AS half
            FROM patent_cpc pc
            JOIN patents p ON p.patent_id = pc.patent_id
            CROSS JOIN bounds b
            WHERE p.year IS NOT NULL AND pc.section IS NOT NULL
        )
        SELECT
            section,
            COUNT(DISTINCT CASE WHEN half = 'first'  THEN patent_id END) AS first_half_patents,
            COUNT(DISTINCT CASE WHEN half = 'second' THEN patent_id END) AS second_half_patents
        FROM labelled GROUP BY section;
        """
    )

    # ---- per-company CAGR ---------------------------------------------
    conn.execute(
        """
        CREATE OR REPLACE TABLE mv_company_cagr AS
        WITH yearly AS (
            SELECT c.company_id, c.name AS company_name, p.year,
                   COUNT(DISTINCT p.patent_id) AS patents
            FROM companies c
            JOIN patent_relationships r ON r.company_id = c.company_id
            JOIN patents              p ON p.patent_id  = r.patent_id
            WHERE p.year IS NOT NULL
            GROUP BY c.company_id, c.name, p.year
        ), endpoints AS (
            SELECT company_id, company_name,
                   MIN(year) AS first_year, MAX(year) AS last_year,
                   SUM(patents) AS total_patents
            FROM yearly GROUP BY company_id, company_name
        ), joined AS (
            SELECT e.company_id, e.company_name, e.first_year, e.last_year,
                   e.total_patents,
                   f.patents AS first_year_patents,
                   l.patents AS last_year_patents,
                   (e.last_year - e.first_year) AS span_years
            FROM endpoints e
            JOIN yearly f ON f.company_id = e.company_id AND f.year = e.first_year
            JOIN yearly l ON l.company_id = e.company_id AND l.year = e.last_year
        )
        SELECT
            company_name, first_year, last_year, span_years,
            first_year_patents, last_year_patents, total_patents,
            ROUND(POWER(CAST(last_year_patents AS DOUBLE)
                        / CAST(first_year_patents AS DOUBLE),
                        1.0 / NULLIF(span_years, 0)) - 1, 4) AS cagr
        FROM joined
        WHERE total_patents >= 20 AND span_years >= 3 AND first_year_patents > 0;
        """
    )

    # ---- size + analyze for query planner -----------------------------
    for tbl in (
        "mv_yearly",
        "mv_country_yearly",
        "mv_section_yearly",
        "mv_company_total",
        "mv_inventor_total",
        "mv_company_yearly",
        "mv_company_section",
        "mv_decade_compare",
        "mv_country_halfsplit",
        "mv_section_halfsplit",
        "mv_company_cagr",
    ):
        rows = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        logger.info(f"  {tbl:>22}: {rows:>11,} rows")
    conn.execute("ANALYZE;")


def run_load(settings: Settings) -> Path:
    db_path = settings.paths.warehouse_db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    parquet_dir = settings.paths.clean_dir / "parquet"
    logger.info(f"opening DuckDB warehouse at {db_path}")
    with duckdb.connect(db_path.as_posix()) as conn:
        _execute_schema(conn, settings.paths.schema_file)
        for table, csv_name, parquet_name in TABLE_FILES:
            parquet_path = parquet_dir / parquet_name
            csv_path = settings.paths.clean_dir / csv_name
            src = parquet_path if parquet_path.exists() else csv_path
            if not src.exists():
                logger.warning(f"skip {table}: {csv_path} (no Parquet either) missing")
                continue
            _ingest_into(conn, table, src)
        _build_summaries(conn)
    logger.info(f"warehouse ready: {db_path}")
    return db_path
