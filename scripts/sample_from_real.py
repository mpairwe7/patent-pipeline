"""Build data/sample/*.tsv as a year-stratified slice of the project's
real PatentsView clean Parquet bundle (data/clean/parquet/*.parquet).

Used to refresh the bundled sample so the dashboard's 1990s/2000s decade
presets actually have data. The previous sample was synthetic and only
spanned 2010-2025; this one preserves real names, companies, and CPC
codes across 1976-2025.

Per-year sample counts are PROPORTIONAL to the real USPTO grant volume
(see ``REAL_USPTO_GRANTS_K``), so the dashboard's yearly-trends chart
mirrors the real ~5× growth from 1976 to peak (2019). Without this the
trend line is flat at 200/year, which masks the actual story.

We round-trip back into the *raw* PatentsView TSV schema (the cleaner
reads raw schema) by synthesising the columns the cleaner reads but
discards (patent_type, wipo_kind, num_claims, withdrawn, filename) and
deriving the rest from the clean parquet:

  patents      → g_patent.tsv
  inventors,
    relationships,
    locations  → g_inventor_disambiguated.tsv  + g_location_disambiguated.tsv
  companies,
    relationships → g_assignee_disambiguated.tsv
  cpc          → g_cpc_current.tsv

Run from the project root:
    uv run python scripts/sample_from_real.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PARQUET = ROOT / "data" / "clean" / "parquet"
SAMPLE = ROOT / "data" / "sample"

# Real USPTO patents granted per calendar year (in thousands), used to
# shape a proportional sample so the dashboard's yearly trend reflects
# real-world growth. Sourced from PatentsView aggregate counts; smoothed
# slightly. Keep this updated when refreshing the sample.
REAL_USPTO_GRANTS_K = {
    1976: 71, 1977: 70, 1978: 71, 1979: 52, 1980: 66, 1981: 71, 1982: 63,
    1983: 62, 1984: 73, 1985: 77, 1986: 77, 1987: 90, 1988: 84, 1989: 103,
    1990: 99, 1991: 107, 1992: 108, 1993: 110, 1994: 114, 1995: 114,
    1996: 122, 1997: 124, 1998: 163, 1999: 169, 2000: 176, 2001: 184,
    2002: 184, 2003: 187, 2004: 181, 2005: 165, 2006: 196, 2007: 182,
    2008: 186, 2009: 191, 2010: 244, 2011: 244, 2012: 277, 2013: 302,
    2014: 326, 2015: 326, 2016: 334, 2017: 343, 2018: 339, 2019: 391,
    2020: 388, 2021: 374, 2022: 343, 2023: 322, 2024: 318, 2025: 240,
}
PEAK_PER_YEAR = 200  # cap at the highest-volume year so the sample stays small
MIN_PER_YEAR = 25    # floor so even the smallest year has enough rows for charts

PEAK_REAL = max(REAL_USPTO_GRANTS_K.values())
PER_YEAR_TARGET: dict[int, int] = {
    y: max(MIN_PER_YEAR, round(c / PEAK_REAL * PEAK_PER_YEAR))
    for y, c in REAL_USPTO_GRANTS_K.items()
}

SAMPLE.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
con.execute("PRAGMA threads=4")

# Materialise the per-year quota as a small DuckDB table so we can JOIN
# against it instead of building a giant CASE WHEN.
con.execute("CREATE TABLE per_year_target (year INTEGER, n INTEGER)")
con.executemany(
    "INSERT INTO per_year_target VALUES (?, ?)", list(PER_YEAR_TARGET.items())
)

PQ_PATENTS = (PARQUET / "patents.parquet").as_posix()
PQ_INVENTORS = (PARQUET / "inventors.parquet").as_posix()
PQ_COMPANIES = (PARQUET / "companies.parquet").as_posix()
PQ_REL = (PARQUET / "relationships.parquet").as_posix()
PQ_CPC = (PARQUET / "cpc.parquet").as_posix()

# Two source modes:
#  1. parquet present (preferred) → sample directly from the real corpus
#  2. parquet absent → subsample the existing data/sample/g_patent.tsv,
#     which still contains real patent_ids from a previous parquet run.
PARQUET_AVAILABLE = (PARQUET / "patents.parquet").exists()
print(
    f"sampling proportionally to real USPTO grants (target {sum(PER_YEAR_TARGET.values()):,} patents)…"
)
if PARQUET_AVAILABLE:
    print(f"  source: real parquet at {PARQUET}")
    con.execute(
        f"""
        CREATE TABLE sampled AS
        WITH ranked AS (
            SELECT p.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY year ORDER BY hash(patent_id)
                   ) AS rn
            FROM read_parquet('{PQ_PATENTS}') p
            WHERE year BETWEEN 1976 AND 2025
        )
        SELECT r.* EXCLUDE (rn)
        FROM ranked r
        JOIN per_year_target t ON t.year = r.year
        WHERE r.rn <= t.n
        """
    )
else:
    fallback_tsv = (SAMPLE / "g_patent.tsv").as_posix()
    print(f"  source: existing sample at {fallback_tsv} (parquet missing)")
    con.execute(
        f"""
        CREATE TABLE existing_patents AS
        SELECT patent_id, patent_title AS title,
               CAST(NULL AS VARCHAR) AS abstract,
               TRY_CAST(patent_date AS DATE) AS filing_date,
               extract(year from TRY_CAST(patent_date AS DATE))::INT AS year
          FROM read_csv_auto('{fallback_tsv}', delim='\t', header=true,
                             sample_size=-1, types={{'patent_id': 'VARCHAR'}})
         WHERE TRY_CAST(patent_date AS DATE) IS NOT NULL
        """
    )
    con.execute(
        """
        CREATE TABLE sampled AS
        WITH ranked AS (
            SELECT p.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY year ORDER BY hash(patent_id)
                   ) AS rn
            FROM existing_patents p
        )
        SELECT r.* EXCLUDE (rn)
        FROM ranked r
        JOIN per_year_target t ON t.year = r.year
        WHERE r.rn <= t.n
        """
    )
total = con.execute("SELECT COUNT(*), MIN(year), MAX(year) FROM sampled").fetchone()
print(f"  → {total[0]:,} patents selected ({total[1]}-{total[2]})")
print(
    "  per-year preview:",
    {y: n for y, n in con.execute("SELECT year, COUNT(*) FROM sampled GROUP BY 1 ORDER BY 1 LIMIT 6").fetchall()},
    "…",
    {y: n for y, n in con.execute("SELECT year, COUNT(*) FROM sampled GROUP BY 1 ORDER BY 1 DESC LIMIT 6").fetchall()},
)

# Per-country synthetic location_id so the cleaner can join inventor → country.
# We don't reconstruct city/state — the dashboard only consumes country.
if PARQUET_AVAILABLE:
    con.execute(
        f"""
        CREATE TABLE country_loc AS
        SELECT country, 'loc_' || lower(country) AS location_id
        FROM (
            SELECT DISTINCT country
            FROM read_parquet('{PQ_INVENTORS}')
            WHERE country IS NOT NULL AND length(trim(country)) = 2
        )
        """
    )
else:
    # Fall back to the existing g_location_disambiguated.tsv: each row is a
    # synthetic country location from a previous parquet run.
    loc_tsv = (SAMPLE / "g_location_disambiguated.tsv").as_posix()
    con.execute(
        f"""
        CREATE TABLE country_loc AS
        SELECT disambig_country AS country, location_id
          FROM read_csv_auto('{loc_tsv}', delim='\t', header=true,
                             sample_size=-1)
         WHERE disambig_country IS NOT NULL
        """
    )


def copy_to_tsv(query: str, out: Path) -> None:
    con.execute(f"COPY ({query}) TO '{out.as_posix()}' (HEADER, DELIMITER '\t', QUOTE '\"')")
    rows = sum(1 for _ in out.open()) - 1
    print(f"  wrote {out.name}: {rows:,} rows ({out.stat().st_size / 1024:.1f} KB)")


print("writing sample TSVs…")

# Read existing companion TSVs into temp tables when parquet is missing
# (so the rest of the script can reference uniform view names regardless
# of source).
if not PARQUET_AVAILABLE:
    for tname, fname, force_types in [
        ("existing_inventors_tsv", "g_inventor_disambiguated.tsv", "{'patent_id': 'VARCHAR'}"),
        ("existing_assignees_tsv", "g_assignee_disambiguated.tsv", "{'patent_id': 'VARCHAR'}"),
        ("existing_cpc_tsv", "g_cpc_current.tsv", "{'patent_id': 'VARCHAR'}"),
    ]:
        con.execute(
            f"""
            CREATE TABLE {tname} AS
            SELECT * FROM read_csv_auto('{(SAMPLE / fname).as_posix()}',
                delim='\t', header=true, sample_size=-1, types={force_types})
            """
        )

# g_patent.tsv — synthesise the columns the cleaner reads but doesn't keep.
copy_to_tsv(
    """
    SELECT patent_id,
           'utility'                 AS patent_type,
           CAST(filing_date AS VARCHAR) AS patent_date,
           title                     AS patent_title,
           'B2'                      AS wipo_kind,
           CAST(NULL AS INTEGER)     AS num_claims,
           CAST(0 AS INTEGER)        AS withdrawn,
           CAST(NULL AS VARCHAR)     AS filename
      FROM sampled
     ORDER BY patent_date
    """,
    SAMPLE / "g_patent.tsv",
)

# g_inventor_disambiguated.tsv
if PARQUET_AVAILABLE:
    inventors_query = f"""
        WITH rel AS (
            SELECT r.* FROM read_parquet('{PQ_REL}') r
             JOIN sampled s USING (patent_id)
        )
        SELECT rel.patent_id,
               ROW_NUMBER() OVER (PARTITION BY rel.patent_id ORDER BY rel.inventor_id) AS inventor_sequence,
               inv.inventor_id,
               regexp_extract(inv.name, '^([^ ]+)', 1)               AS disambig_inventor_name_first,
               NULLIF(regexp_replace(inv.name, '^([^ ]+) ?', ''), '') AS disambig_inventor_name_last,
               CAST(NULL AS VARCHAR)                                  AS gender_code,
               cl.location_id
          FROM rel
          JOIN read_parquet('{PQ_INVENTORS}') inv USING (inventor_id)
          LEFT JOIN country_loc cl ON cl.country = inv.country
         WHERE inv.inventor_id IS NOT NULL
    """
else:
    # Fallback: filter the previous run's TSV by sampled patent_ids.
    inventors_query = """
        SELECT t.* FROM existing_inventors_tsv t
         JOIN sampled s USING (patent_id)
    """
copy_to_tsv(inventors_query, SAMPLE / "g_inventor_disambiguated.tsv")

# g_assignee_disambiguated.tsv
if PARQUET_AVAILABLE:
    assignees_query = f"""
        WITH rel AS (
            SELECT r.* FROM read_parquet('{PQ_REL}') r
             JOIN sampled s USING (patent_id)
        )
        SELECT rel.patent_id,
               ROW_NUMBER() OVER (PARTITION BY rel.patent_id ORDER BY rel.company_id) AS assignee_sequence,
               rel.company_id                          AS assignee_id,
               CAST(NULL AS VARCHAR)                   AS disambig_assignee_individual_name_first,
               CAST(NULL AS VARCHAR)                   AS disambig_assignee_individual_name_last,
               c.name                                  AS disambig_assignee_organization,
               '2'                                     AS assignee_type,
               CAST(NULL AS VARCHAR)                   AS location_id
          FROM rel
          JOIN read_parquet('{PQ_COMPANIES}') c ON c.company_id = rel.company_id
         WHERE rel.company_id IS NOT NULL
    """
else:
    assignees_query = """
        SELECT t.* FROM existing_assignees_tsv t
         JOIN sampled s USING (patent_id)
    """
copy_to_tsv(assignees_query, SAMPLE / "g_assignee_disambiguated.tsv")

# g_location_disambiguated.tsv — one row per country.
copy_to_tsv(
    """
    SELECT location_id,
           CAST(NULL AS VARCHAR) AS disambig_city,
           CAST(NULL AS VARCHAR) AS disambig_state,
           country               AS disambig_country,
           CAST(NULL AS DOUBLE)  AS latitude,
           CAST(NULL AS DOUBLE)  AS longitude,
           CAST(NULL AS VARCHAR) AS county,
           CAST(NULL AS VARCHAR) AS state_fips,
           CAST(NULL AS VARCHAR) AS county_fips
      FROM country_loc
    """,
    SAMPLE / "g_location_disambiguated.tsv",
)

# g_cpc_current.tsv
if PARQUET_AVAILABLE:
    cpc_query = f"""
        SELECT cpc.patent_id,
               ROW_NUMBER() OVER (PARTITION BY cpc.patent_id ORDER BY cpc.section, cpc.cpc_class) AS cpc_sequence,
               cpc.section       AS cpc_section,
               cpc.cpc_class     AS cpc_class,
               cpc.cpc_subclass  AS cpc_subclass,
               CAST(NULL AS VARCHAR) AS cpc_group,
               'inventional'         AS cpc_type
          FROM read_parquet('{PQ_CPC}') cpc
          JOIN sampled s USING (patent_id)
    """
else:
    cpc_query = """
        SELECT t.* FROM existing_cpc_tsv t
         JOIN sampled s USING (patent_id)
    """
copy_to_tsv(cpc_query, SAMPLE / "g_cpc_current.tsv")

print("done.")
