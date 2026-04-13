"""Visualisation — Plotly (interactive HTML) + Matplotlib (static PNG).

Generates:
  reports/figures/yearly_trends.png / .html
  reports/figures/top_companies.png / .html
  reports/figures/country_share.png / .html
  reports/figures/cpc_sections.png / .html
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px

from patent_pipeline.config import Settings
from patent_pipeline.logging_setup import logger


def _savefig(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140, bbox_inches="tight")
    plt.close(fig)


def _save_plotly(fig, path_html: Path) -> None:
    path_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(path_html, include_plotlyjs="cdn")


def plot_yearly_trends(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(df["year"], df["patent_count"], marker="o", color="#4c72b0")
    ax.fill_between(df["year"], df["patent_count"], alpha=0.15, color="#4c72b0")
    ax.set_title("Patents filed per year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Patents")
    ax.grid(alpha=0.3)
    _savefig(fig, figures_dir / "yearly_trends.png")

    pfig = px.area(df, x="year", y="patent_count", title="Patents filed per year")
    _save_plotly(pfig, figures_dir / "yearly_trends.html")


def plot_top_companies(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    top = df.head(15).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.barh(top["company_name"], top["patent_count"], color="#55a868")
    ax.set_title("Top 15 companies by patent count")
    ax.set_xlabel("Patents")
    _savefig(fig, figures_dir / "top_companies.png")

    pfig = px.bar(
        df.head(15),
        x="patent_count",
        y="company_name",
        orientation="h",
        title="Top 15 companies by patent count",
    )
    pfig.update_layout(yaxis={"categoryorder": "total ascending"})
    _save_plotly(pfig, figures_dir / "top_companies.html")


def plot_country_share(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(
        df["patent_count"],
        labels=df["country"],
        autopct="%1.1f%%",
        pctdistance=0.8,
        startangle=90,
        counterclock=False,
    )
    ax.set_title("Patent share by country")
    _savefig(fig, figures_dir / "country_share.png")

    pfig = px.choropleth(
        df,
        locations="country",
        locationmode="country names",
        color="patent_count",
        title="Patents per country",
        color_continuous_scale="Viridis",
    )
    _save_plotly(pfig, figures_dir / "country_share.html")


def plot_cpc_sections(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    ordered = df.sort_values("patent_count", ascending=True)
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(ordered["section_label"], ordered["patent_count"], color="#c44e52")
    ax.set_title("Patents by CPC section")
    ax.set_xlabel("Patents")
    _savefig(fig, figures_dir / "cpc_sections.png")

    pfig = px.bar(df, x="section_label", y="patent_count", title="Patents by CPC section")
    _save_plotly(pfig, figures_dir / "cpc_sections.html")


def run_visualize(results: dict[str, Any], settings: Settings) -> list[Path]:
    figures_dir = settings.paths.figures_dir
    figures_dir.mkdir(parents=True, exist_ok=True)
    plot_yearly_trends(results["q4_trends_over_time"], figures_dir)
    plot_top_companies(results["q2_top_companies"], figures_dir)
    plot_country_share(results["q3_top_countries"], figures_dir)
    plot_cpc_sections(results["cpc_breakdown"], figures_dir)

    outputs = sorted(figures_dir.glob("*.png")) + sorted(figures_dir.glob("*.html"))
    logger.info(f"wrote {len(outputs)} figure files → {figures_dir}")
    return outputs
