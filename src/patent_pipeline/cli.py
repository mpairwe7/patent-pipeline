"""Typer CLI — ``patent-pipeline <sub-command>``."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import typer

from patent_pipeline import __version__
from patent_pipeline.analyze import run_analyze
from patent_pipeline.clean import run_clean
from patent_pipeline.config import Settings, load_settings
from patent_pipeline.ingest import ingest as run_ingest
from patent_pipeline.load import run_load
from patent_pipeline.logging_setup import configure, logger
from patent_pipeline.report import run_reports
from patent_pipeline.visualize import run_visualize

app = typer.Typer(
    help="Global Patent Intelligence Data Pipeline.",
    no_args_is_help=True,
    add_completion=False,
)


def _log(level: str) -> None:
    configure(level)


def _override(
    settings: Settings,
    *,
    year_from: int | None = None,
    year_to: int | None = None,
    parquet: bool | None = None,
) -> Settings:
    """Return a Settings copy with the requested clean-stage overrides."""
    if year_from is None and year_to is None and parquet is None:
        return settings
    new_clean = settings.clean.model_copy(
        update={
            **({"min_year": year_from} if year_from is not None else {}),
            **({"max_year": year_to} if year_to is not None else {}),
            **({"parquet": parquet} if parquet is not None else {}),
        }
    )
    return settings.model_copy(update={"clean": new_clean})


# ---------------------------------------------------------------------------
@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@app.command()
def ingest(
    use_sample: bool = typer.Option(
        True,
        "--use-sample/--no-use-sample",
        help="Copy bundled data/sample/ TSVs (default) or download from --url.",
    ),
    url: list[str] = typer.Option(
        None,
        "--url",
        help="One or more URLs to download. Repeat flag for multiple files.",
    ),
    force_refresh: bool = typer.Option(
        False,
        "--force-refresh",
        help="Re-download every file even if the manifest says it is complete.",
    ),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Populate data/raw/ from bundled sample or download URLs."""
    _log(log_level)
    settings = load_settings()
    run_ingest(settings, use_sample=use_sample, urls=url, force_refresh=force_refresh)


@app.command()
def clean(
    year_from: int | None = typer.Option(None, "--year-from", help="Earliest filing year to keep."),
    year_to: int | None = typer.Option(None, "--year-to", help="Latest filing year to keep."),
    parquet: bool = typer.Option(
        False, "--parquet/--no-parquet", help="Also emit Parquet alongside CSV."
    ),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Clean raw TSVs → data/clean/*.csv (and Parquet) via DuckDB streaming."""
    _log(log_level)
    settings = _override(load_settings(), year_from=year_from, year_to=year_to, parquet=parquet)
    run_clean(settings)


@app.command()
def load(log_level: str = typer.Option("INFO", "--log-level")) -> None:
    """Load clean CSVs/Parquet into DuckDB at data/warehouse/patents.duckdb."""
    _log(log_level)
    run_load(load_settings())


@app.command()
def analyze(log_level: str = typer.Option("INFO", "--log-level")) -> None:
    """Run every SQL query and emit console / CSV / JSON reports + figures."""
    _log(log_level)
    settings = load_settings()
    results = run_analyze(settings)
    run_reports(results, settings)
    run_visualize(results, settings)


@app.command("run-all")
def run_all(
    use_sample: bool = typer.Option(True, "--use-sample/--no-use-sample"),
    url: list[str] = typer.Option(None, "--url"),
    year_from: int | None = typer.Option(
        None, "--year-from", help="Earliest filing year to keep (e.g. 1976)."
    ),
    year_to: int | None = typer.Option(
        None, "--year-to", help="Latest filing year to keep (e.g. 2025)."
    ),
    parquet: bool = typer.Option(
        False, "--parquet/--no-parquet", help="Emit Parquet during clean (recommended for real)."
    ),
    force_refresh: bool = typer.Option(
        False, "--force-refresh", help="Ignore manifest and re-download every file."
    ),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """End-to-end: ingest → clean → load → analyze (+ reports + figures)."""
    _log(log_level)
    settings = _override(load_settings(), year_from=year_from, year_to=year_to, parquet=parquet)
    logger.info(
        f"── pipeline start · sample={use_sample} · "
        f"window={settings.clean.min_year}-{settings.clean.max_year} · parquet={parquet}"
    )
    logger.info("── 1/4 ingest ──")
    run_ingest(settings, use_sample=use_sample, urls=url, force_refresh=force_refresh)
    logger.info("── 2/4 clean ──")
    run_clean(settings)
    logger.info("── 3/4 load ──")
    run_load(settings)
    logger.info("── 4/4 analyze + report + visualize ──")
    results = run_analyze(settings)
    run_reports(results, settings)
    run_visualize(results, settings)
    logger.success("Pipeline complete ✔")


@app.command()
def dashboard(
    port: int = typer.Option(8501, "--port"),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Start the Streamlit dashboard."""
    _log(log_level)
    settings = load_settings()
    if not settings.paths.warehouse_db.exists():
        typer.echo("Warehouse not found. Run `patent-pipeline run-all` first.", err=True)
        raise typer.Exit(code=1)

    dashboard_script = Path(__file__).with_name("dashboard.py")
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(dashboard_script),
        "--server.port",
        str(port),
        "--browser.gatherUsageStats",
        "false",
    ]
    logger.info(f"launching streamlit on :{port}")
    raise typer.Exit(code=subprocess.call(cmd))


if __name__ == "__main__":
    app()
