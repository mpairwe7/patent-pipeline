"""Typer CLI — ``patent-pipeline <sub-command>``."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import typer

from patent_pipeline import __version__
from patent_pipeline.analyze import run_analyze
from patent_pipeline.clean import run_clean
from patent_pipeline.config import load_settings
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
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Populate data/raw/ from bundled sample or download URLs."""
    _log(log_level)
    settings = load_settings()
    run_ingest(settings, use_sample=use_sample, urls=url)


@app.command()
def clean(log_level: str = typer.Option("INFO", "--log-level")) -> None:
    """Clean raw TSVs → data/clean/*.csv using pandas (PyArrow backend)."""
    _log(log_level)
    run_clean(load_settings())


@app.command()
def load(log_level: str = typer.Option("INFO", "--log-level")) -> None:
    """Load clean CSVs into DuckDB at data/warehouse/patents.duckdb."""
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
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """End-to-end: ingest → clean → load → analyze (+ reports + figures)."""
    _log(log_level)
    settings = load_settings()
    logger.info("── 1/4 ingest ──")
    run_ingest(settings, use_sample=use_sample, urls=url)
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
