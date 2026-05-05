"""Report stage — produce the three required output formats.

* Console   – a Rich panel that mirrors the PDF's "PATENT REPORT" example.
* CSV files – top_inventors.csv, top_companies.csv, country_trends.csv.
* JSON file – patent_report.json aggregating the key numbers.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from patent_pipeline.config import Settings
from patent_pipeline.logging_setup import logger


def _df_to_records(
    df: pd.DataFrame, columns: dict[str, str], limit: int | None = None
) -> list[dict]:
    """Slim a frame down to the requested columns with renamed keys."""
    trimmed = df[list(columns.keys())].rename(columns=columns)
    if limit is not None:
        trimmed = trimmed.head(limit)
    return trimmed.to_dict(orient="records")


def write_csv_reports(results: dict[str, Any], settings: Settings) -> dict[str, Path]:
    reports_dir = settings.paths.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    out = {
        "top_inventors": reports_dir / "top_inventors.csv",
        "top_companies": reports_dir / "top_companies.csv",
        "country_trends": reports_dir / "country_trends.csv",
        "yearly_trends": reports_dir / "yearly_trends.csv",
        "cpc_breakdown": reports_dir / "cpc_breakdown.csv",
        # Advanced-analytics deliverables.
        "decade_comparison": reports_dir / "decade_comparison.csv",
        "company_cagr": reports_dir / "company_cagr.csv",
        "country_growth_rates": reports_dir / "country_growth_rates.csv",
        "section_growth": reports_dir / "section_growth.csv",
        "company_section_matrix": reports_dir / "company_section_matrix.csv",
    }

    inv = results["q1_top_inventors"].head(settings.reports.top_n_inventors)
    inv.to_csv(out["top_inventors"], index=False)

    comp = results["q2_top_companies"].head(settings.reports.top_n_companies)
    comp.to_csv(out["top_companies"], index=False)

    ctry = results["q3_top_countries"].head(settings.reports.top_n_countries)
    ctry.to_csv(out["country_trends"], index=False)

    results["q4_trends_over_time"].to_csv(out["yearly_trends"], index=False)
    results["cpc_breakdown"].to_csv(out["cpc_breakdown"], index=False)
    results["q8_decade_comparison"].to_csv(out["decade_comparison"], index=False)
    results["q9_company_cagr"].to_csv(out["company_cagr"], index=False)
    results["q10_country_growth_rates"].to_csv(out["country_growth_rates"], index=False)
    results["q11_section_growth"].to_csv(out["section_growth"], index=False)
    results["q12_company_section_matrix"].to_csv(out["company_section_matrix"], index=False)

    for name, path in out.items():
        logger.info(f"wrote {name}.csv → {path}")
    return out


def write_json_report(results: dict[str, Any], settings: Settings) -> Path:
    reports_dir = settings.paths.reports_dir
    totals = results["totals"]
    year_min, year_max = totals["year_range"] if totals["year_range"] else (None, None)

    payload: dict[str, Any] = {
        "total_patents": int(totals["total_patents"]),
        "total_inventors": int(totals["total_inventors"]),
        "total_companies": int(totals["total_companies"]),
        "total_relationships": int(totals["total_relationships"]),
        "year_range": {"min": year_min, "max": year_max},
        "top_inventors": _df_to_records(
            results["q1_top_inventors"],
            {"inventor_name": "name", "country": "country", "patent_count": "patents"},
            limit=settings.reports.top_n_inventors,
        ),
        "top_companies": _df_to_records(
            results["q2_top_companies"],
            {"company_name": "name", "patent_count": "patents"},
            limit=settings.reports.top_n_companies,
        ),
        "top_countries": _df_to_records(
            results["q3_top_countries"],
            {"country": "country", "patent_count": "patents", "share": "share"},
            limit=settings.reports.top_n_countries,
        ),
        "yearly_trends": _df_to_records(
            results["q4_trends_over_time"],
            {"year": "year", "patent_count": "patents"},
        ),
        "top_ranked_inventors_by_country": _df_to_records(
            results["q7_rank_inventors_window"],
            {
                "country": "country",
                "inventor_name": "name",
                "country_rank": "rank",
                "patent_count": "patents",
            },
        ),
        "cpc_breakdown": _df_to_records(
            results["cpc_breakdown"],
            {"section": "section", "section_label": "label", "patent_count": "patents"},
        ),
        "advanced_analytics": {
            "decade_comparison": results["q8_decade_comparison"].to_dict(orient="records"),
            "company_cagr": _df_to_records(
                results["q9_company_cagr"],
                {
                    "company_name": "name",
                    "first_year": "first_year",
                    "last_year": "last_year",
                    "span_years": "span_years",
                    "first_year_patents": "first_year_patents",
                    "last_year_patents": "last_year_patents",
                    "total_patents": "total_patents",
                    "cagr": "cagr",
                },
                limit=15,
            ),
            "country_growth_rates": results["q10_country_growth_rates"].to_dict(orient="records"),
            "section_growth": results["q11_section_growth"].to_dict(orient="records"),
            "company_section_matrix": results["q12_company_section_matrix"].to_dict(
                orient="records"
            ),
        },
    }

    path = reports_dir / "patent_report.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    logger.info(f"wrote JSON report → {path}")
    return path


def _print_plain_ascii(results: dict[str, Any]) -> None:
    """Assignment-spec console block — matches the PDF example verbatim."""
    totals = results["totals"]
    top_inv = results["q1_top_inventors"].head(5)
    top_comp = results["q2_top_companies"].head(5)
    top_ctry = results["q3_top_countries"].head(5)

    print("================== PATENT REPORT ===================")
    print(f"Total Patents: {int(totals['total_patents']):,}")
    print(
        "Top Inventors: "
        + " ".join(
            f"{i}. {row.inventor_name} - {int(row.patent_count)}"
            for i, row in enumerate(top_inv.itertuples(index=False), start=1)
        )
    )
    print(
        "Top Companies: "
        + " ".join(
            f"{i}. {row.company_name} - {int(row.patent_count):,}"
            for i, row in enumerate(top_comp.itertuples(index=False), start=1)
        )
    )
    print(
        "Top Countries: "
        + " ".join(
            f"{i}. {row.country}" for i, row in enumerate(top_ctry.itertuples(index=False), start=1)
        )
    )
    print("====================================================")


def print_console_report(results: dict[str, Any], settings: Settings) -> None:
    # Plain ASCII block first — matches the assignment's PDF example verbatim.
    _print_plain_ascii(results)

    console = Console()
    totals = results["totals"]
    year_min, year_max = totals["year_range"] if totals["year_range"] else ("—", "—")

    header = Table.grid(padding=(0, 2))
    header.add_column(justify="left", style="bold")
    header.add_column(justify="right", style="cyan")
    header.add_row("Total Patents", f"{int(totals['total_patents']):,}")
    header.add_row("Total Inventors", f"{int(totals['total_inventors']):,}")
    header.add_row("Total Companies", f"{int(totals['total_companies']):,}")
    header.add_row("Year Range", f"{year_min} – {year_max}")
    header.add_row("Relationships", f"{int(totals['total_relationships']):,}")

    console.print(
        Panel(
            header,
            title="[bold green]PATENT INTELLIGENCE REPORT",
            border_style="green",
            expand=False,
        )
    )

    def _table(title: str, df: pd.DataFrame, columns: list[tuple[str, str]]) -> Table:
        tbl = Table(title=title, title_style="bold magenta", header_style="bold")
        for _, header_ in columns:
            tbl.add_column(header_)
        for _, row in df.iterrows():
            tbl.add_row(*[str(row[col]) for col, _ in columns])
        return tbl

    top_inv = results["q1_top_inventors"].head(5)
    top_comp = results["q2_top_companies"].head(5)
    top_ctry = results["q3_top_countries"].head(5)

    console.print(
        _table(
            "Top Inventors",
            top_inv,
            [("inventor_name", "Inventor"), ("country", "Country"), ("patent_count", "Patents")],
        )
    )
    console.print(
        _table(
            "Top Companies",
            top_comp,
            [("company_name", "Company"), ("patent_count", "Patents")],
        )
    )
    top_ctry = top_ctry.copy()
    top_ctry["share_pct"] = (top_ctry["share"] * 100).round(1).astype(str) + " %"
    console.print(
        _table(
            "Top Countries",
            top_ctry,
            [("country", "Country"), ("patent_count", "Patents"), ("share_pct", "Share")],
        )
    )

    trend = results["q4_trends_over_time"]
    if not trend.empty:
        console.print(
            _table(
                "Patents per Year",
                trend,
                [("year", "Year"), ("patent_count", "Patents")],
            )
        )

    # ------------------------------------------------------------------
    # Advanced analytics — surfaced in the console for visibility.
    # ------------------------------------------------------------------
    decade = results.get("q8_decade_comparison")
    if decade is not None and not decade.empty:
        decade = decade.copy()
        decade["share_pct"] = (decade["share"] * 100).round(1).astype(str) + " %"
        console.print(
            _table(
                "Decade comparison",
                decade,
                [
                    ("period", "Period"),
                    ("patent_count", "Patents"),
                    ("inventor_count", "Inventors"),
                    ("company_count", "Companies"),
                    ("share_pct", "Share"),
                ],
            )
        )

    cagr = results.get("q9_company_cagr")
    if cagr is not None and not cagr.empty:
        cagr = cagr.head(5).copy()
        cagr["cagr_pct"] = (cagr["cagr"] * 100).round(2).astype(str) + " %"
        console.print(
            _table(
                "Top 5 companies by CAGR",
                cagr,
                [
                    ("company_name", "Company"),
                    ("first_year", "First yr"),
                    ("last_year", "Last yr"),
                    ("total_patents", "Total"),
                    ("cagr_pct", "CAGR"),
                ],
            )
        )

    sec = results.get("q11_section_growth")
    if sec is not None and not sec.empty:
        sec = sec.head(5).copy()
        sec["growth_pct_str"] = (sec["growth_pct"] * 100).round(1).astype(str) + " %"
        console.print(
            _table(
                "Fastest-growing CPC sections",
                sec,
                [
                    ("section", "Section"),
                    ("first_half_patents", "1st half"),
                    ("second_half_patents", "2nd half"),
                    ("growth_pct_str", "Growth"),
                ],
            )
        )


def run_reports(results: dict[str, Any], settings: Settings) -> dict[str, Path]:
    csvs = write_csv_reports(results, settings)
    json_path = write_json_report(results, settings)
    print_console_report(results, settings)
    return {**csvs, "json": json_path}
