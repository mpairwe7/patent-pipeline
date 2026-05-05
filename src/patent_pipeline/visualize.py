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


def plot_decade_comparison(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(df["period"], df["patent_count"], color=["#4c72b0", "#55a868", "#c44e52"])
    for x, y in zip(df["period"], df["patent_count"], strict=False):
        ax.text(x, y, f"{int(y):,}", ha="center", va="bottom", fontsize=9)
    ax.set_title("Patents per 5-year decade bucket")
    ax.set_ylabel("Patents")
    ax.grid(alpha=0.3, axis="y")
    _savefig(fig, figures_dir / "decade_comparison.png")

    pfig = px.bar(
        df,
        x="period",
        y="patent_count",
        text="patent_count",
        title="Patents per 5-year decade bucket",
    )
    _save_plotly(pfig, figures_dir / "decade_comparison.html")


def plot_company_cagr(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    top = df.head(15).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    colors = ["#55a868" if v >= 0 else "#c44e52" for v in top["cagr"]]
    ax.barh(top["company_name"], top["cagr"] * 100, color=colors)
    ax.set_title("Top 15 companies by CAGR (2010-2025)")
    ax.set_xlabel("CAGR (%)")
    ax.grid(alpha=0.3, axis="x")
    _savefig(fig, figures_dir / "company_cagr.png")

    pfig = px.bar(
        df.head(15),
        x="cagr",
        y="company_name",
        orientation="h",
        title="Top 15 companies by CAGR",
        labels={"cagr": "CAGR (fraction)", "company_name": "Company"},
    )
    pfig.update_layout(yaxis={"categoryorder": "total ascending"}, xaxis_tickformat=".1%")
    _save_plotly(pfig, figures_dir / "company_cagr.html")


def plot_country_growth(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    df_sorted = df.sort_values("growth_pct", ascending=True)
    ax.barh(df_sorted["country"], df_sorted["growth_pct"] * 100, color="#4c72b0")
    ax.set_title("Country growth rate — first half vs second half of 2010-2025")
    ax.set_xlabel("Growth (%)")
    ax.grid(alpha=0.3, axis="x")
    _savefig(fig, figures_dir / "country_growth.png")

    pfig = px.scatter(
        df,
        x="first_half_patents",
        y="second_half_patents",
        size="total_patents",
        color="growth_pct",
        hover_name="country",
        color_continuous_scale="RdYlGn",
        title="Country growth — first vs second half of span (bubble = total volume)",
    )
    _save_plotly(pfig, figures_dir / "country_growth.html")


def plot_section_growth(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    df_sorted = df.sort_values("growth_pct", ascending=True)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.barh(df_sorted["section"], df_sorted["growth_pct"] * 100, color="#8172b2")
    ax.set_title("CPC section growth rate (first half → second half)")
    ax.set_xlabel("Growth (%)")
    _savefig(fig, figures_dir / "section_growth.png")

    pfig = px.bar(
        df,
        x="section",
        y="growth_pct",
        title="CPC section growth rate",
        labels={"growth_pct": "Growth (fraction)", "section": "Section"},
    )
    pfig.update_layout(yaxis_tickformat=".0%")
    _save_plotly(pfig, figures_dir / "section_growth.html")


def plot_company_section_heatmap(df: pd.DataFrame, figures_dir: Path) -> None:
    if df.empty:
        return
    pivot = df.pivot_table(index="company_name", columns="section", values="patents", fill_value=0)
    fig, ax = plt.subplots(figsize=(10, 6))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlGnBu")
    ax.set_xticks(range(len(pivot.columns)), pivot.columns)
    ax.set_yticks(range(len(pivot.index)), pivot.index, fontsize=8)
    ax.set_title("Top 15 companies × CPC sections (patent counts)")
    fig.colorbar(im, ax=ax, label="Patents")
    _savefig(fig, figures_dir / "company_section_heatmap.png")

    pfig = px.imshow(
        pivot,
        color_continuous_scale="YlGnBu",
        labels={"x": "CPC section", "y": "Company", "color": "Patents"},
        title="Top 15 companies × CPC sections",
    )
    pfig.update_layout(height=520)
    _save_plotly(pfig, figures_dir / "company_section_heatmap.html")


def run_visualize(results: dict[str, Any], settings: Settings) -> list[Path]:
    figures_dir = settings.paths.figures_dir
    figures_dir.mkdir(parents=True, exist_ok=True)
    plot_yearly_trends(results["q4_trends_over_time"], figures_dir)
    plot_top_companies(results["q2_top_companies"], figures_dir)
    plot_country_share(results["q3_top_countries"], figures_dir)
    plot_cpc_sections(results["cpc_breakdown"], figures_dir)
    # Advanced analytics figures.
    plot_decade_comparison(results.get("q8_decade_comparison", pd.DataFrame()), figures_dir)
    plot_company_cagr(results.get("q9_company_cagr", pd.DataFrame()), figures_dir)
    plot_country_growth(results.get("q10_country_growth_rates", pd.DataFrame()), figures_dir)
    plot_section_growth(results.get("q11_section_growth", pd.DataFrame()), figures_dir)
    plot_company_section_heatmap(
        results.get("q12_company_section_matrix", pd.DataFrame()), figures_dir
    )

    outputs = sorted(figures_dir.glob("*.png")) + sorted(figures_dir.glob("*.html"))
    logger.info(f"wrote {len(outputs)} figure files → {figures_dir}")
    return outputs
