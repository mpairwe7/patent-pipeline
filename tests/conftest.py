"""Pytest fixtures — spin up a temporary settings instance pointing at a
tmp_path sandbox so tests never pollute the real data/ or warehouse.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from patent_pipeline.config import PROJECT_ROOT, Paths, Settings, load_settings


@pytest.fixture(scope="session")
def project_root() -> Path:
    return PROJECT_ROOT


@pytest.fixture()
def tmp_settings(tmp_path: Path, project_root: Path) -> Settings:
    """A Settings object where all output paths live under tmp_path."""
    raw = tmp_path / "raw"
    sample = project_root / "data" / "sample"
    for d in ("raw", "clean", "warehouse", "reports", "reports/figures"):
        (tmp_path / d).mkdir(parents=True, exist_ok=True)

    # copy sample TSVs into the sandbox's raw/ dir so tests are hermetic
    for tsv in sample.glob("*.tsv"):
        shutil.copy2(tsv, raw / tsv.name)

    base = load_settings()
    overrides = Paths(
        raw_dir=raw,
        sample_dir=sample,
        clean_dir=tmp_path / "clean",
        warehouse_dir=tmp_path / "warehouse",
        warehouse_db=tmp_path / "warehouse" / "patents.duckdb",
        sql_dir=base.paths.sql_dir,
        queries_dir=base.paths.queries_dir,
        schema_file=base.paths.schema_file,
        reports_dir=tmp_path / "reports",
        figures_dir=tmp_path / "reports" / "figures",
    )
    return Settings(
        paths=overrides,
        ingest=base.ingest,
        clean=base.clean,
        reports=base.reports,
    )
