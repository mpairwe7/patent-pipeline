"""Integration tests — clean → load → analyze on the bundled sample."""

from __future__ import annotations

import json

import duckdb
import pandas as pd

from patent_pipeline.analyze import run_analyze
from patent_pipeline.clean import run_clean
from patent_pipeline.load import run_load
from patent_pipeline.report import write_csv_reports, write_json_report


def _prepare_warehouse(tmp_settings) -> None:
    run_clean(tmp_settings)
    run_load(tmp_settings)


def test_load_populates_all_tables(tmp_settings):
    _prepare_warehouse(tmp_settings)
    with duckdb.connect(tmp_settings.paths.warehouse_db.as_posix(), read_only=True) as c:
        for table in ("patents", "inventors", "companies", "patent_relationships", "patent_cpc"):
            rows = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert rows > 0, f"{table} is empty after load"


def test_all_queries_return_dataframes(tmp_settings):
    _prepare_warehouse(tmp_settings)
    results = run_analyze(tmp_settings)
    for key in [
        "q1_top_inventors",
        "q2_top_companies",
        "q3_top_countries",
        "q4_trends_over_time",
        "q5_join_patents_inventors_companies",
        "q6_cte_innovation_leaders",
        "q7_rank_inventors_window",
    ]:
        assert isinstance(results[key], pd.DataFrame), f"{key} not a DataFrame"
        # all queries should return at least one row on the sample
        assert not results[key].empty, f"{key} returned empty"


def test_q7_window_rank_shape(tmp_settings):
    _prepare_warehouse(tmp_settings)
    results = run_analyze(tmp_settings)
    q7 = results["q7_rank_inventors_window"]
    assert {"country", "country_rank", "inventor_name", "patent_count"} <= set(q7.columns)
    # each country returns at most 3 distinct ranks
    assert (q7.groupby("country")["country_rank"].max() <= 3).all()


def test_reports_artifacts(tmp_settings):
    _prepare_warehouse(tmp_settings)
    results = run_analyze(tmp_settings)
    csvs = write_csv_reports(results, tmp_settings)
    json_path = write_json_report(results, tmp_settings)

    for key in ("top_inventors", "top_companies", "country_trends"):
        assert csvs[key].exists() and csvs[key].stat().st_size > 0

    payload = json.loads(json_path.read_text())
    assert payload["total_patents"] > 0
    assert payload["top_inventors"], "top_inventors list empty"
    assert payload["top_countries"], "top_countries list empty"
    # shape matches the assignment's example JSON
    assert {"country", "patents", "share"} <= set(payload["top_countries"][0].keys())
