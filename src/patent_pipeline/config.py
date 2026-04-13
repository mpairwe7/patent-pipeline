"""Typed configuration loaded from ``config/pipeline.yaml``.

Uses pydantic v2 so every path and option is validated at load time and
callers receive proper autocompletion instead of dictionary lookups.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.yaml"


class Paths(BaseModel):
    raw_dir: Path
    sample_dir: Path
    clean_dir: Path
    warehouse_dir: Path
    warehouse_db: Path
    sql_dir: Path
    queries_dir: Path
    schema_file: Path
    reports_dir: Path
    figures_dir: Path

    def absolutize(self, root: Path) -> Paths:
        data = {k: (root / v).resolve() for k, v in self.model_dump().items()}
        return Paths.model_validate(data)


class IngestCfg(BaseModel):
    use_sample: bool = True
    sources: dict[str, str] = Field(default_factory=dict)


class CleanCfg(BaseModel):
    min_year: int = 1976
    max_year: int = 2025
    dedupe: bool = True


class ReportsCfg(BaseModel):
    top_n_inventors: int = 20
    top_n_companies: int = 20
    top_n_countries: int = 15


class Settings(BaseModel):
    paths: Paths
    ingest: IngestCfg
    clean: CleanCfg
    reports: ReportsCfg
    project_root: Path = PROJECT_ROOT


@lru_cache
def load_settings(config_file: Path | None = None) -> Settings:
    cfg_path = Path(config_file) if config_file else DEFAULT_CONFIG
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    s = Settings(
        paths=Paths.model_validate(raw["paths"]).absolutize(PROJECT_ROOT),
        ingest=IngestCfg.model_validate(raw.get("ingest", {})),
        clean=CleanCfg.model_validate(raw.get("clean", {})),
        reports=ReportsCfg.model_validate(raw.get("reports", {})),
    )
    for p in (
        s.paths.raw_dir,
        s.paths.clean_dir,
        s.paths.warehouse_dir,
        s.paths.reports_dir,
        s.paths.figures_dir,
    ):
        p.mkdir(parents=True, exist_ok=True)
    return s
