"""Clean stage — transform raw PatentsView TSVs into normalised CSVs.

The PatentsView bulk files have many columns we don't need. This module:
  1. Reads each TSV with pandas' PyArrow backend (fast, nullable types).
  2. Selects only the columns the schema cares about.
  3. Normalises strings, parses dates, coerces year to Int64.
  4. De-duplicates on primary keys.
  5. Joins location info onto inventors and assignees to derive country.
  6. Writes the five clean CSVs into data/clean/.

Input files expected (real USPTO names; sample uses the same names):
    g_patent.tsv, g_inventor_disambiguated.tsv, g_assignee_disambiguated.tsv,
    g_location_disambiguated.tsv, g_patent_inventor.tsv,
    g_patent_assignee.tsv, g_cpc_current.tsv.

Output files:
    clean_patents.csv, clean_inventors.csv, clean_companies.csv,
    clean_relationships.csv, clean_cpc.csv.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from patent_pipeline.config import Settings
from patent_pipeline.logging_setup import logger

# ---------- helpers ---------------------------------------------------------


def _read_tsv(path: Path, usecols: list[str] | None = None) -> pd.DataFrame:
    """Read a TSV with PyArrow backend; tolerate missing optional columns."""
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(
        path,
        sep="\t",
        dtype_backend="pyarrow",
        engine="pyarrow",
        na_values=["", "NULL", "null"],
    )
    if usecols:
        available = [c for c in usecols if c in df.columns]
        df = df[available]
    return df


def _strip_str(s: pd.Series) -> pd.Series:
    """Strip whitespace, collapse internal spaces, empty → NA."""
    out = s.astype("string[pyarrow]").str.strip().str.replace(r"\s+", " ", regex=True)
    return out.mask(out == "")


# ---------- per-entity cleaners --------------------------------------------


def clean_patents(raw_dir: Path, min_year: int, max_year: int) -> pd.DataFrame:
    src = raw_dir / "g_patent.tsv"
    df = _read_tsv(src, ["patent_id", "patent_date", "patent_title", "patent_abstract"])
    rows_in = len(df)

    df = df.rename(
        columns={
            "patent_date": "filing_date",
            "patent_title": "title",
            "patent_abstract": "abstract",
        }
    )
    df["patent_id"] = _strip_str(df["patent_id"])
    df["title"] = _strip_str(df["title"])
    df["abstract"] = _strip_str(df["abstract"])

    # Parse date → DATE, derive year
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce").dt.date
    df["year"] = pd.to_datetime(df["filing_date"], errors="coerce").dt.year.astype("Int64")

    # Drop rows without a primary key or outside the requested year window.
    df = df.dropna(subset=["patent_id"])
    in_range = df["year"].between(min_year, max_year, inclusive="both")
    df = df[in_range | df["year"].isna()]
    df = df.drop_duplicates(subset=["patent_id"])
    logger.info(f"patents: {rows_in} → {len(df)} rows")
    return df[["patent_id", "title", "abstract", "filing_date", "year"]]


def clean_inventors(raw_dir: Path) -> pd.DataFrame:
    df = _read_tsv(
        raw_dir / "g_inventor_disambiguated.tsv",
        ["inventor_id", "disambig_inventor_name_first", "disambig_inventor_name_last"],
    )
    rows_in = len(df)
    first = _strip_str(df["disambig_inventor_name_first"]).fillna("")
    last = _strip_str(df["disambig_inventor_name_last"]).fillna("")
    name = (first + " " + last).str.strip()
    name = name.mask(name == "")
    out = (
        pd.DataFrame(
            {
                "inventor_id": _strip_str(df["inventor_id"]),
                "name": name,
            }
        )
        .dropna(subset=["inventor_id"])
        .drop_duplicates(subset=["inventor_id"])
    )

    # country is attached later via locations; fill NA placeholder for now
    out["country"] = pd.Series([pd.NA] * len(out), dtype="string[pyarrow]")
    logger.info(f"inventors: {rows_in} → {len(out)} rows")
    return out[["inventor_id", "name", "country"]]


def clean_companies(raw_dir: Path) -> pd.DataFrame:
    df = _read_tsv(
        raw_dir / "g_assignee_disambiguated.tsv",
        [
            "assignee_id",
            "disambig_assignee_organization",
            "disambig_assignee_individual_name_first",
            "disambig_assignee_individual_name_last",
        ],
    )
    rows_in = len(df)
    org = _strip_str(df["disambig_assignee_organization"])
    first = _strip_str(
        df.get(
            "disambig_assignee_individual_name_first",
            pd.Series(pd.NA, index=df.index, dtype="string[pyarrow]"),
        )
    )
    last = _strip_str(
        df.get(
            "disambig_assignee_individual_name_last",
            pd.Series(pd.NA, index=df.index, dtype="string[pyarrow]"),
        )
    )
    indiv = (first.fillna("") + " " + last.fillna("")).str.strip().mask(lambda x: x == "")
    name = org.fillna(indiv)

    out = (
        pd.DataFrame(
            {
                "company_id": _strip_str(df["assignee_id"]),
                "name": name,
            }
        )
        .dropna(subset=["company_id", "name"])
        .drop_duplicates(subset=["company_id"])
    )
    logger.info(f"companies: {rows_in} → {len(out)} rows")
    return out[["company_id", "name"]]


def _build_location_lookup(raw_dir: Path) -> pd.DataFrame:
    df = _read_tsv(
        raw_dir / "g_location_disambiguated.tsv",
        ["location_id", "disambig_country"],
    )
    df = df.rename(columns={"disambig_country": "country"})
    df["location_id"] = _strip_str(df["location_id"])
    df["country"] = _strip_str(df["country"]).str.upper()
    return df.dropna(subset=["location_id"]).drop_duplicates("location_id")


def build_relationships(
    raw_dir: Path, patents: pd.DataFrame, inventors: pd.DataFrame, companies: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Join g_patent_inventor × g_patent_assignee on patent_id (full outer), also
    enrich each inventor with its country via the location lookup. Returns
    (relationships, inventors_with_country)."""

    pi = _read_tsv(raw_dir / "g_patent_inventor.tsv", ["patent_id", "inventor_id", "location_id"])
    pa = _read_tsv(raw_dir / "g_patent_assignee.tsv", ["patent_id", "assignee_id", "location_id"])
    loc = _build_location_lookup(raw_dir)

    for col in ("patent_id", "inventor_id", "location_id"):
        pi[col] = _strip_str(pi[col])
    for col in ("patent_id", "assignee_id", "location_id"):
        pa[col] = _strip_str(pa[col])

    pi = pi.dropna(subset=["patent_id", "inventor_id"])
    pa = pa.dropna(subset=["patent_id", "assignee_id"])

    # attach country to inventors (most frequent country across their rows)
    inv_country = (
        pi.merge(loc[["location_id", "country"]], on="location_id", how="left")
        .dropna(subset=["inventor_id", "country"])
        .groupby("inventor_id")["country"]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else pd.NA)
        .rename("country")
        .reset_index()
    )
    inventors = inventors.drop(columns=["country"]).merge(inv_country, on="inventor_id", how="left")

    # relationships: every (patent, inventor) row × every company assigned to
    # that patent; patents without an assignee keep company_id = NULL.
    pa_light = pa[["patent_id", "assignee_id"]].rename(columns={"assignee_id": "company_id"})
    rel = pi[["patent_id", "inventor_id"]].merge(pa_light, on="patent_id", how="left")

    # keep only links whose patent/inventor/company actually exist
    rel = rel[rel["patent_id"].isin(patents["patent_id"])]
    rel = rel[rel["inventor_id"].isin(inventors["inventor_id"])]
    valid_company_ids = set(companies["company_id"].dropna().tolist())
    rel.loc[~rel["company_id"].isin(valid_company_ids), "company_id"] = pd.NA
    rel = rel.drop_duplicates()

    logger.info(f"relationships: {len(rel)} rows (with company: {rel['company_id'].notna().sum()})")
    return rel[["patent_id", "inventor_id", "company_id"]], inventors


def clean_cpc(raw_dir: Path, valid_patents: pd.DataFrame) -> pd.DataFrame:
    path = raw_dir / "g_cpc_current.tsv"
    if not path.exists():
        logger.warning("g_cpc_current.tsv not found — skipping CPC cleaning")
        return pd.DataFrame(columns=["patent_id", "section", "cpc_class", "cpc_subclass"])

    df = _read_tsv(path, ["patent_id", "cpc_section", "cpc_class", "cpc_subclass"])
    rows_in = len(df)
    df = df.rename(columns={"cpc_section": "section"})
    for c in ("patent_id", "section", "cpc_class", "cpc_subclass"):
        df[c] = _strip_str(df[c])
    df = df.dropna(subset=["patent_id", "section"])
    df = df[df["patent_id"].isin(valid_patents["patent_id"])]
    df = df.drop_duplicates()
    logger.info(f"cpc: {rows_in} → {len(df)} rows")
    return df[["patent_id", "section", "cpc_class", "cpc_subclass"]]


# ---------- orchestrator ----------------------------------------------------


def run_clean(settings: Settings) -> dict[str, Path]:
    """Clean every entity, persist CSVs, return map of name → path."""
    raw = settings.paths.raw_dir
    out = settings.paths.clean_dir
    out.mkdir(parents=True, exist_ok=True)

    patents = clean_patents(raw, settings.clean.min_year, settings.clean.max_year)
    inventors = clean_inventors(raw)
    companies = clean_companies(raw)
    rel, inventors = build_relationships(raw, patents, inventors, companies)
    cpc = clean_cpc(raw, patents)

    files = {
        "patents": out / "clean_patents.csv",
        "inventors": out / "clean_inventors.csv",
        "companies": out / "clean_companies.csv",
        "relationships": out / "clean_relationships.csv",
        "cpc": out / "clean_cpc.csv",
    }
    patents.to_csv(files["patents"], index=False)
    inventors.to_csv(files["inventors"], index=False)
    companies.to_csv(files["companies"], index=False)
    rel.to_csv(files["relationships"], index=False)
    cpc.to_csv(files["cpc"], index=False)

    for name, path in files.items():
        logger.info(f"wrote {name}: {path}")
    return files
