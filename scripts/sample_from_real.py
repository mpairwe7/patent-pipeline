"""Build data/sample/*.tsv as a year-stratified slice of the project's
real PatentsView clean Parquet bundle (data/clean/parquet/*.parquet).

Used to refresh the bundled sample so the dashboard's 1990s/2000s decade
presets actually have data. The previous sample was synthetic and only
spanned 2010-2025; this one preserves real names, companies, and CPC
codes across 1976-2025.

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
PER_YEAR = 200  # 50 years × 200 ≈ 10 K patents

SAMPLE.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
con.execute("PRAGMA threads=4")

PQ_PATENTS = (PARQUET / "patents.parquet").as_posix()
PQ_INVENTORS = (PARQUET / "inventors.parquet").as_posix()
PQ_COMPANIES = (PARQUET / "companies.parquet").as_posix()
PQ_REL = (PARQUET / "relationships.parquet").as_posix()
PQ_CPC = (PARQUET / "cpc.parquet").as_posix()

print(f"sampling {PER_YEAR} patents/year from 1976-2025…")
con.execute(
    f"""
    CREATE TABLE sampled AS
    SELECT * EXCLUDE (rn) FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY year ORDER BY hash(patent_id)
               ) AS rn
        FROM read_parquet('{PQ_PATENTS}')
        WHERE year BETWEEN 1976 AND 2025
    )
    WHERE rn <= {PER_YEAR}
    """
)
total = con.execute("SELECT COUNT(*), MIN(year), MAX(year) FROM sampled").fetchone()
print(f"  → {total[0]:,} patents selected ({total[1]}-{total[2]})")

# Per-country synthetic location_id so the cleaner can join inventor → country.
# We don't reconstruct city/state — the dashboard only consumes country.
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


def copy_to_tsv(query: str, out: Path) -> None:
    con.execute(f"COPY ({query}) TO '{out.as_posix()}' (HEADER, DELIMITER '\t', QUOTE '\"')")
    rows = sum(1 for _ in out.open()) - 1
    print(f"  wrote {out.name}: {rows:,} rows ({out.stat().st_size / 1024:.1f} KB)")


print("writing sample TSVs…")

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

# g_inventor_disambiguated.tsv — split clean `name` back into first/last.
copy_to_tsv(
    f"""
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
    """,
    SAMPLE / "g_inventor_disambiguated.tsv",
)

# g_assignee_disambiguated.tsv — companies have no country in clean data,
# so location_id is NULL (cleaner tolerates).
copy_to_tsv(
    f"""
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
    """,
    SAMPLE / "g_assignee_disambiguated.tsv",
)

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
copy_to_tsv(
    f"""
    SELECT cpc.patent_id,
           ROW_NUMBER() OVER (PARTITION BY cpc.patent_id ORDER BY cpc.section, cpc.cpc_class) AS cpc_sequence,
           cpc.section       AS cpc_section,
           cpc.cpc_class     AS cpc_class,
           cpc.cpc_subclass  AS cpc_subclass,
           CAST(NULL AS VARCHAR) AS cpc_group,
           'inventional'         AS cpc_type
      FROM read_parquet('{PQ_CPC}') cpc
      JOIN sampled s USING (patent_id)
    """,
    SAMPLE / "g_cpc_current.tsv",
)

print("done.")
