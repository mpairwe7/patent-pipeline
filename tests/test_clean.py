"""Unit tests for the clean stage."""

from __future__ import annotations

import pandas as pd

from patent_pipeline.clean import run_clean


def test_run_clean_emits_all_csvs(tmp_settings):
    files = run_clean(tmp_settings)

    # every expected csv exists and is non-empty
    for key in ("patents", "inventors", "companies", "relationships", "cpc"):
        assert files[key].exists(), f"{key} missing"
        assert files[key].stat().st_size > 0, f"{key} is empty"


def test_patents_schema_and_year_range(tmp_settings):
    run_clean(tmp_settings)
    df = pd.read_csv(tmp_settings.paths.clean_dir / "clean_patents.csv")
    assert set(df.columns) == {"patent_id", "title", "abstract", "filing_date", "year"}
    # no duplicate primary keys
    assert df["patent_id"].is_unique
    # every non-null year is within the configured range
    years = df["year"].dropna()
    assert (years >= tmp_settings.clean.min_year).all()
    assert (years <= tmp_settings.clean.max_year).all()


def test_inventors_get_country_from_locations(tmp_settings):
    run_clean(tmp_settings)
    df = pd.read_csv(tmp_settings.paths.clean_dir / "clean_inventors.csv")
    assert {"inventor_id", "name", "country"} <= set(df.columns)
    # inventors that appear on at least one patent should have a country.
    # Orphan inventors (not in any patent_inventor row) legitimately have NA.
    assert df["country"].notna().mean() > 0.5
    # and at least a few countries should be represented
    assert df["country"].dropna().nunique() >= 3


def test_relationships_only_reference_existing_entities(tmp_settings):
    run_clean(tmp_settings)
    clean = tmp_settings.paths.clean_dir
    patents = pd.read_csv(clean / "clean_patents.csv")["patent_id"]
    inventors = pd.read_csv(clean / "clean_inventors.csv")["inventor_id"]
    companies = pd.read_csv(clean / "clean_companies.csv")["company_id"]
    rel = pd.read_csv(clean / "clean_relationships.csv")
    assert rel["patent_id"].isin(patents).all()
    assert rel["inventor_id"].isin(inventors).all()
    assert rel["company_id"].dropna().isin(companies).all()
