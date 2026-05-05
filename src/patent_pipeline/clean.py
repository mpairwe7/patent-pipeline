"""Clean stage — DuckDB-driven streaming transformation of raw TSVs.

Why DuckDB instead of pandas? The real PatentsView disambiguated bundle
(1976-01-01 → 2025-09-30) is multi-GB once unzipped:

  g_patent.tsv                ~ 8M rows
  g_patent_inventor.tsv       ~ 25M rows
  g_patent_assignee.tsv       ~ 12M rows
  g_cpc_current.tsv           ~ 50M rows
  g_inventor_disambiguated    ~ 5M rows
  g_assignee_disambiguated    ~ 0.5M rows
  g_location_disambiguated    ~ 0.2M rows

Loading those into pandas would OOM on a typical laptop. DuckDB's
``read_csv_auto`` reads them in **streaming** fashion — constant memory,
multi-threaded scan, fully columnar — and ``COPY (…) TO 'file.csv'``
streams the cleaned output back to disk. Year-window filtering is
pushed into SQL so we never materialise out-of-window rows.

Outputs are still the five CSVs the assignment expects (and the tests
assert):

  data/clean/clean_patents.csv
  data/clean/clean_inventors.csv
  data/clean/clean_companies.csv
  data/clean/clean_relationships.csv
  data/clean/clean_cpc.csv

If ``clean.parquet`` is enabled in config, equivalent ``.parquet`` files
are emitted alongside the CSVs — significantly faster for the load
stage on the real dataset.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from patent_pipeline.config import Settings
from patent_pipeline.logging_setup import logger

# Column whitelists — keep these in sync with sql/schema.sql.
PATENT_COLS = ["patent_id", "title", "abstract", "filing_date", "year"]
INVENTOR_COLS = ["inventor_id", "name", "country"]
COMPANY_COLS = ["company_id", "name"]
RELATIONSHIP_COLS = ["patent_id", "inventor_id", "company_id"]
CPC_COLS = ["patent_id", "section", "cpc_class", "cpc_subclass"]


def _q(path: Path) -> str:
    """SQL-quote a path so it survives spaces/single-quotes."""
    return path.as_posix().replace("'", "''")


def _read_csv_view(conn: duckdb.DuckDBPyConnection, view: str, src: Path) -> bool:
    """Register a streaming view over a TSV. Returns False if the file is
    missing (so the caller can decide to skip that entity).
    """
    if not src.exists():
        logger.warning(f"missing {src.name} — skipping {view}")
        return False
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW {view} AS
        SELECT * FROM read_csv_auto(
            '{_q(src)}',
            delim='\t',
            header=true,
            nullstr=['', 'NULL', 'null'],
            ignore_errors=false,
            sample_size=-1
        )
        """
    )
    return True


def _copy_table(
    conn: duckdb.DuckDBPyConnection, table: str, csv_path: Path, parquet_path: Path | None
) -> int:
    """Stream the contents of an in-memory table to disk. Always emits
    CSV (assignment requirement); Parquet too if requested.
    """
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(
        f"COPY {table} TO '{_q(csv_path)}' (FORMAT csv, HEADER true, DATEFORMAT '%Y-%m-%d')"
    )
    if parquet_path is not None:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(f"COPY {table} TO '{_q(parquet_path)}' (FORMAT parquet, COMPRESSION zstd)")
    rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return rows


# ---------------------------------------------------------------------------
# Per-entity SQL — kept as functions so the year-window can be parametrised.
# ---------------------------------------------------------------------------
def _columns(conn: duckdb.DuckDBPyConnection, view: str) -> set[str]:
    return set(conn.execute(f"DESCRIBE {view}").fetchdf()["column_name"].tolist())


def _build_patents(conn: duckdb.DuckDBPyConnection, min_year: int, max_year: int) -> int:
    """``g_patent.tsv`` columns: patent_id, patent_type, patent_date, patent_title,
    [optional patent_abstract], wipo_kind, num_claims, withdrawn, filename.

    The real PatentsView ``g_patent.tsv`` does NOT include the abstract — it
    lives in the separate ``g_patent_abstract.tsv`` (huge, optional). We
    detect the column at runtime so the same SQL works for sample + real.
    """
    cols = _columns(conn, "raw_patents")
    abstract_expr = (
        "regexp_replace(NULLIF(trim(CAST(patent_abstract AS VARCHAR)), ''), '\\s+', ' ', 'g')"
        if "patent_abstract" in cols
        else "CAST(NULL AS VARCHAR)"
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE patents_clean AS
        WITH parsed AS (
            SELECT
                trim(CAST(patent_id AS VARCHAR))                              AS patent_id,
                regexp_replace(trim(CAST(patent_title AS VARCHAR)),
                               '\\s+', ' ', 'g')                              AS title,
                {abstract_expr}                                               AS abstract,
                TRY_CAST(patent_date AS DATE)                                 AS filing_date,
                CAST(extract(year from TRY_CAST(patent_date AS DATE)) AS INT) AS year
            FROM raw_patents
        ),
        deduped AS (
            SELECT *, row_number() OVER (PARTITION BY patent_id ORDER BY filing_date) AS rn
            FROM parsed
            WHERE patent_id IS NOT NULL AND patent_id <> ''
              AND (year IS NULL OR year BETWEEN {min_year} AND {max_year})
        )
        SELECT patent_id, NULLIF(title, '') AS title, abstract, filing_date, year
        FROM deduped WHERE rn = 1;
        """
    )
    return conn.execute("SELECT COUNT(*) FROM patents_clean").fetchone()[0]


def _build_inventors(conn: duckdb.DuckDBPyConnection, has_locations: bool) -> int:
    """Modern PatentsView ``g_inventor_disambiguated.tsv`` is a per-(patent,
    inventor) link table with full inventor info inlined per row. We derive
    the per-inventor table by SELECT DISTINCT on inventor_id (taking the
    first observed name) and attach the most-frequent country via a join
    against the location table.
    """
    if has_locations:
        conn.execute(
            """
            CREATE OR REPLACE TABLE inv_country AS
            SELECT inv.inventor_id,
                   mode(upper(trim(CAST(loc.disambig_country AS VARCHAR)))) AS country
            FROM raw_inventors inv
            LEFT JOIN raw_location loc
                ON CAST(loc.location_id AS VARCHAR) = CAST(inv.location_id AS VARCHAR)
            WHERE inv.inventor_id IS NOT NULL
              AND trim(CAST(inv.inventor_id AS VARCHAR)) <> ''
              AND loc.disambig_country IS NOT NULL
              AND trim(CAST(loc.disambig_country AS VARCHAR)) <> ''
            GROUP BY inv.inventor_id;
            """
        )
    else:
        conn.execute("CREATE OR REPLACE TABLE inv_country (inventor_id VARCHAR, country VARCHAR);")

    conn.execute(
        """
        CREATE OR REPLACE TABLE inventors_clean AS
        WITH names AS (
            SELECT
                trim(CAST(inventor_id AS VARCHAR))                              AS inventor_id,
                trim(
                    coalesce(trim(CAST(disambig_inventor_name_first AS VARCHAR)), '') || ' ' ||
                    coalesce(trim(CAST(disambig_inventor_name_last  AS VARCHAR)), '')
                )                                                               AS name
            FROM raw_inventors
            WHERE inventor_id IS NOT NULL AND trim(CAST(inventor_id AS VARCHAR)) <> ''
        ),
        deduped AS (
            SELECT inventor_id, NULLIF(name, '') AS name,
                   row_number() OVER (PARTITION BY inventor_id ORDER BY name) AS rn
            FROM names
        )
        SELECT d.inventor_id, d.name, ic.country
        FROM deduped d
        LEFT JOIN inv_country ic ON ic.inventor_id = d.inventor_id
        WHERE d.rn = 1;
        """
    )
    return conn.execute("SELECT COUNT(*) FROM inventors_clean").fetchone()[0]


def _build_companies(conn: duckdb.DuckDBPyConnection) -> int:
    """Same idea as inventors: ``g_assignee_disambiguated.tsv`` is the
    per-(patent, assignee) link table with assignee info inlined.
    """
    conn.execute(
        """
        CREATE OR REPLACE TABLE companies_clean AS
        WITH per_row AS (
            SELECT
                trim(CAST(assignee_id AS VARCHAR))                                     AS company_id,
                coalesce(
                    NULLIF(trim(CAST(disambig_assignee_organization AS VARCHAR)), ''),
                    NULLIF(
                        trim(
                            coalesce(trim(CAST(disambig_assignee_individual_name_first
                                              AS VARCHAR)), '') || ' ' ||
                            coalesce(trim(CAST(disambig_assignee_individual_name_last
                                              AS VARCHAR)), '')
                        ), ''
                    )
                )                                                                      AS name
            FROM raw_assignees
            WHERE assignee_id IS NOT NULL AND trim(CAST(assignee_id AS VARCHAR)) <> ''
        ),
        deduped AS (
            SELECT company_id, name,
                   row_number() OVER (PARTITION BY company_id ORDER BY name) AS rn
            FROM per_row
            WHERE name IS NOT NULL
        )
        SELECT company_id, name FROM deduped WHERE rn = 1;
        """
    )
    return conn.execute("SELECT COUNT(*) FROM companies_clean").fetchone()[0]


def _build_relationships(conn: duckdb.DuckDBPyConnection) -> int:
    """The relationships fact table joins (patent, inventor) ↔ (patent,
    assignee) on patent_id. Both source files already carry patent_id, so
    this is a simple join — no separate `g_patent_inventor` / `g_patent_
    assignee` files needed in the modern PatentsView layout.
    """
    conn.execute(
        """
        CREATE OR REPLACE TABLE relationships_clean AS
        WITH pi AS (
            SELECT DISTINCT
                trim(CAST(patent_id   AS VARCHAR)) AS patent_id,
                trim(CAST(inventor_id AS VARCHAR)) AS inventor_id
            FROM raw_inventors
            WHERE patent_id IS NOT NULL AND inventor_id IS NOT NULL
              AND trim(CAST(inventor_id AS VARCHAR)) <> ''
        ),
        pa AS (
            SELECT DISTINCT
                trim(CAST(patent_id   AS VARCHAR)) AS patent_id,
                trim(CAST(assignee_id AS VARCHAR)) AS company_id
            FROM raw_assignees
            WHERE patent_id IS NOT NULL AND assignee_id IS NOT NULL
              AND trim(CAST(assignee_id AS VARCHAR)) <> ''
        )
        SELECT DISTINCT
            pi.patent_id,
            pi.inventor_id,
            CASE WHEN comp.company_id IS NOT NULL THEN pa.company_id END AS company_id
        FROM pi
        JOIN patents_clean   p   ON p.patent_id   = pi.patent_id
        JOIN inventors_clean iv  ON iv.inventor_id = pi.inventor_id
        LEFT JOIN pa             ON pa.patent_id   = pi.patent_id
        LEFT JOIN companies_clean comp ON comp.company_id = pa.company_id;
        """
    )
    return conn.execute("SELECT COUNT(*) FROM relationships_clean").fetchone()[0]


def _build_cpc(conn: duckdb.DuckDBPyConnection, has_cpc: bool) -> int:
    if not has_cpc:
        conn.execute(
            "CREATE OR REPLACE TABLE cpc_clean "
            "(patent_id VARCHAR, section VARCHAR, cpc_class VARCHAR, cpc_subclass VARCHAR);"
        )
        return 0
    conn.execute(
        """
        CREATE OR REPLACE TABLE cpc_clean AS
        SELECT DISTINCT
            trim(CAST(c.patent_id    AS VARCHAR))                          AS patent_id,
            NULLIF(trim(CAST(c.cpc_section  AS VARCHAR)), '')              AS section,
            NULLIF(trim(CAST(c.cpc_class    AS VARCHAR)), '')              AS cpc_class,
            NULLIF(trim(CAST(c.cpc_subclass AS VARCHAR)), '')              AS cpc_subclass
        FROM raw_cpc c
        JOIN patents_clean p ON p.patent_id = trim(CAST(c.patent_id AS VARCHAR))
        WHERE c.patent_id IS NOT NULL AND c.cpc_section IS NOT NULL;
        """
    )
    return conn.execute("SELECT COUNT(*) FROM cpc_clean").fetchone()[0]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_clean(settings: Settings) -> dict[str, Path]:
    """Clean every entity, persist CSVs (and optional Parquet), return map
    of name → path. Runs entirely inside a single in-memory DuckDB
    connection so the data is streamed start-to-finish.
    """
    raw = settings.paths.raw_dir
    out = settings.paths.clean_dir
    out.mkdir(parents=True, exist_ok=True)
    emit_parquet = bool(getattr(settings.clean, "parquet", False))
    parquet_dir = out / "parquet" if emit_parquet else None
    if parquet_dir is not None:
        parquet_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "patents": out / "clean_patents.csv",
        "inventors": out / "clean_inventors.csv",
        "companies": out / "clean_companies.csv",
        "relationships": out / "clean_relationships.csv",
        "cpc": out / "clean_cpc.csv",
    }
    parquet_files: dict[str, Path | None] = (
        {k: (parquet_dir / f"{k}.parquet") for k in files}
        if parquet_dir is not None
        else dict.fromkeys(files, None)
    )

    logger.info(
        f"clean: streaming with DuckDB · year window {settings.clean.min_year}-"
        f"{settings.clean.max_year} · parquet={emit_parquet}"
    )

    with duckdb.connect(":memory:") as conn:
        # Tune for streaming throughput. Low memory footprint per scan.
        conn.execute("PRAGMA threads=4;")
        conn.execute("PRAGMA preserve_insertion_order=false;")

        _read_csv_view(conn, "raw_patents", raw / "g_patent.tsv")
        _read_csv_view(conn, "raw_inventors", raw / "g_inventor_disambiguated.tsv")
        _read_csv_view(conn, "raw_assignees", raw / "g_assignee_disambiguated.tsv")
        has_locations = _read_csv_view(conn, "raw_location", raw / "g_location_disambiguated.tsv")
        has_cpc = _read_csv_view(conn, "raw_cpc", raw / "g_cpc_current.tsv")

        n_patents = _build_patents(conn, settings.clean.min_year, settings.clean.max_year)
        logger.info(f"  patents       → {n_patents:>12,} rows")
        n_inv = _build_inventors(conn, has_locations)
        logger.info(f"  inventors     → {n_inv:>12,} rows")
        n_comp = _build_companies(conn)
        logger.info(f"  companies     → {n_comp:>12,} rows")
        n_rel = _build_relationships(conn)
        logger.info(f"  relationships → {n_rel:>12,} rows")
        n_cpc = _build_cpc(conn, has_cpc)
        logger.info(f"  cpc           → {n_cpc:>12,} rows")

        # Stream each cleaned table to CSV (and optional Parquet).
        _copy_table(conn, "patents_clean", files["patents"], parquet_files["patents"])
        _copy_table(conn, "inventors_clean", files["inventors"], parquet_files["inventors"])
        _copy_table(conn, "companies_clean", files["companies"], parquet_files["companies"])
        _copy_table(
            conn, "relationships_clean", files["relationships"], parquet_files["relationships"]
        )
        _copy_table(conn, "cpc_clean", files["cpc"], parquet_files["cpc"])

    for name, path in files.items():
        logger.info(f"wrote {name}: {path}")
    return files
