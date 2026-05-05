"""Render every dashboard chart to PNG so the submission folder shows
the complete set of dashboard visualisations.

This script reads the *built* DuckDB warehouse (default: from the
container at /tmp/warehouse_new.duckdb, falling back to the workspace
copy) and writes one PNG per chart into the target directory. The
queries mirror what dashboard.py runs at runtime, so the static images
match exactly what users see on the live HF Space dashboard.

Run:
    uv run python scripts/render_all_dashboard_charts.py \
        --warehouse /tmp/warehouse_new.duckdb \
        --out MpairweLauben_22_U_21345_DataPipeline/dashboard_snapshots
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px

WONG_PALETTE = [
    "#000000",
    "#E69F00",
    "#56B4E9",
    "#009E73",
    "#F0E442",
    "#0072B2",
    "#D55E00",
    "#CC79A7",
]
px.defaults.color_discrete_sequence = WONG_PALETTE
px.defaults.template = "plotly_white"

CPC_LABELS = {
    "A": "Human Necessities",
    "B": "Performing Operations; Transporting",
    "C": "Chemistry; Metallurgy",
    "D": "Textiles; Paper",
    "E": "Fixed Constructions",
    "F": "Mechanical Engineering",
    "G": "Physics",
    "H": "Electricity",
    "Y": "General Tagging (Emerging Tech)",
}

COUNTRY_LOOKUP = {
    "US": ("United States", "USA"), "CN": ("China", "CHN"), "JP": ("Japan", "JPN"),
    "GB": ("United Kingdom", "GBR"), "KR": ("South Korea", "KOR"), "IN": ("India", "IND"),
    "DE": ("Germany", "DEU"), "FR": ("France", "FRA"), "CA": ("Canada", "CAN"),
    "AU": ("Australia", "AUS"), "CH": ("Switzerland", "CHE"), "NL": ("Netherlands", "NLD"),
    "SE": ("Sweden", "SWE"), "IT": ("Italy", "ITA"), "ES": ("Spain", "ESP"),
    "BR": ("Brazil", "BRA"), "RU": ("Russia", "RUS"), "TW": ("Taiwan", "TWN"),
    "SG": ("Singapore", "SGP"), "IL": ("Israel", "ISR"), "FI": ("Finland", "FIN"),
    "DK": ("Denmark", "DNK"), "BE": ("Belgium", "BEL"), "AT": ("Austria", "AUT"),
    "IE": ("Ireland", "IRL"), "NO": ("Norway", "NOR"), "MX": ("Mexico", "MEX"),
    "ZA": ("South Africa", "ZAF"), "TR": ("Turkey", "TUR"), "PL": ("Poland", "POL"),
}


def save(fig, out_dir: Path, name: str, *, width: int = 1080, height: int = 540) -> None:
    out = out_dir / f"{name}.png"
    fig.update_layout(margin=dict(l=40, r=20, t=60, b=40))
    fig.write_image(out.as_posix(), width=width, height=height, scale=2)
    print(f"  wrote {out.name} ({out.stat().st_size / 1024:.0f} KB)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="/tmp/warehouse_new.duckdb")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.warehouse, read_only=True)

    print(f"reading warehouse: {args.warehouse}")
    print(f"writing PNGs to:   {out}")

    # --------------------------------------------------------------- Trends tab
    yearly = con.execute(
        "SELECT year, patents FROM mv_yearly ORDER BY year"
    ).fetch_df()
    yearly["yoy"] = yearly["patents"].pct_change()
    yearly["cumulative"] = yearly["patents"].cumsum()

    fig = px.line(yearly, x="year", y="patents", markers=True, title="Yearly volume")
    fig.update_traces(
        line=dict(color="#1f4e79", width=2.5),
        marker=dict(color="#1f4e79", size=7),
        fill="tozeroy",
        fillcolor="rgba(31, 78, 121, 0.12)",
    )
    save(fig, out, "01_trends_yearly_volume")

    yoy = yearly.dropna(subset=["yoy"]).copy()
    yoy["direction"] = yoy["yoy"].apply(lambda v: "↑ growth" if v >= 0 else "↓ decline")
    yoy["label"] = yoy["yoy"].map(lambda v: f"{v:+.1%}")
    fig = px.bar(
        yoy, x="year", y="yoy", color="direction", text="label",
        title="Year-over-year growth",
        color_discrete_map={"↑ growth": "#55a868", "↓ decline": "#c44e52"},
    )
    fig.update_layout(yaxis_tickformat=".0%", showlegend=False)
    save(fig, out, "02_trends_yoy_growth")

    fig = px.line(yearly, x="year", y="cumulative", markers=True,
                  title="Cumulative patents")
    fig.update_traces(line_color="#4c72b0")
    save(fig, out, "03_trends_cumulative")

    # ----------------------------------------------------------- Geography tab
    countries = con.execute(
        """
        SELECT country, SUM(patents) AS patents FROM mv_country_yearly
         WHERE country IS NOT NULL
         GROUP BY 1 ORDER BY patents DESC LIMIT 30
        """
    ).fetch_df()
    countries["country_name"] = countries["country"].map(
        lambda c: COUNTRY_LOOKUP.get(c, (c, c))[0]
    )
    countries["iso3"] = countries["country"].map(
        lambda c: COUNTRY_LOOKUP.get(c, (c, c))[1]
    )

    fig = px.choropleth(
        countries, locations="iso3", color="patents", locationmode="ISO-3",
        hover_name="country_name", color_continuous_scale="Viridis",
        title="Patent share by inventor country",
    )
    save(fig, out, "04_geography_world_choropleth", width=1200, height=600)

    fig = px.bar(
        countries.head(15), x="patents", y="country_name", orientation="h",
        title="Top 15 inventor countries",
        color="patents", color_continuous_scale="Blues",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
    save(fig, out, "05_geography_country_bar")

    fig = px.pie(
        countries.head(12), names="country_name", values="patents", hole=0.4,
        title="Country concentration (top 12)",
    )
    save(fig, out, "06_geography_country_pie")

    # ---------------------------------------------------------------- CPC tab
    sections = con.execute(
        """
        SELECT section, SUM(patents) AS patents FROM mv_section_yearly
         WHERE section IS NOT NULL GROUP BY 1 ORDER BY patents DESC
        """
    ).fetch_df()
    sections["label"] = sections["section"].map(
        lambda s: f"{s} — {CPC_LABELS.get(s, s)}"
    )
    fig = px.bar(
        sections, x="label", y="patents",
        title="CPC section breakdown",
        color="patents", color_continuous_scale="Teal",
    )
    fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30)
    save(fig, out, "07_cpc_section_breakdown")

    cpc_class = con.execute(
        """
        SELECT pc.section, pc.cpc_class, COUNT(DISTINCT pc.patent_id) AS patents
          FROM patent_cpc pc
         WHERE pc.section IS NOT NULL AND pc.cpc_class IS NOT NULL
         GROUP BY 1, 2 ORDER BY patents DESC LIMIT 80
        """
    ).fetch_df()
    cpc_class["section_label"] = cpc_class["section"].map(
        lambda s: f"{s} — {CPC_LABELS.get(s, s)}"
    )
    fig = px.treemap(
        cpc_class, path=[px.Constant("All"), "section_label", "cpc_class"],
        values="patents", title="CPC section → class treemap",
        color="patents", color_continuous_scale="Greens",
    )
    save(fig, out, "08_cpc_treemap", width=1200, height=700)

    section_yearly = con.execute(
        """
        SELECT year, section, patents FROM mv_section_yearly
         WHERE section IS NOT NULL ORDER BY year
        """
    ).fetch_df()
    section_yearly["section_label"] = section_yearly["section"].map(
        lambda s: f"{s} — {CPC_LABELS.get(s, s)[:28]}"
    )
    fig = px.line(
        section_yearly, x="year", y="patents", color="section_label",
        markers=False, title="CPC section trends over time",
    )
    fig.update_layout(legend_title_text="CPC section")
    save(fig, out, "09_cpc_section_trends")

    # ----------------------------------------------------------- Inventors tab
    inv = con.execute(
        """
        SELECT i.name AS inventor, i.country,
               COUNT(DISTINCT pr.patent_id) AS patents
          FROM patent_relationships pr
          JOIN inventors i ON i.inventor_id = pr.inventor_id
         WHERE i.name IS NOT NULL AND TRIM(i.name) <> ''
         GROUP BY 1, 2 ORDER BY patents DESC LIMIT 20
        """
    ).fetch_df()
    fig = px.bar(
        inv.iloc[::-1], x="patents", y="inventor", orientation="h",
        color="country", title="Top 20 inventors",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    save(fig, out, "10_inventors_top20")

    # Top-3 per country (window function rank)
    top3 = con.execute(
        """
        WITH ranked AS (
            SELECT i.country, i.name, COUNT(DISTINCT pr.patent_id) AS patents,
                   ROW_NUMBER() OVER (PARTITION BY i.country ORDER BY COUNT(DISTINCT pr.patent_id) DESC) AS rn
              FROM patent_relationships pr
              JOIN inventors i ON i.inventor_id = pr.inventor_id
             WHERE i.country IS NOT NULL AND i.name IS NOT NULL
             GROUP BY 1, 2
        )
        SELECT country, name AS inventor, patents
          FROM ranked WHERE rn <= 3
         ORDER BY country, patents DESC
        """
    ).fetch_df()
    # Limit to 12 countries with the most total to keep chart readable
    top_countries = (
        top3.groupby("country")["patents"].sum()
        .sort_values(ascending=False).head(12).index.tolist()
    )
    top3_subset = top3[top3["country"].isin(top_countries)]
    fig = px.bar(
        top3_subset, x="patents", y="country", color="inventor", orientation="h",
        title="Top-3 inventors per country (window function)",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    save(fig, out, "11_inventors_top3_by_country")

    # ----------------------------------------------------------- Companies tab
    companies = con.execute(
        """
        SELECT c.name AS company, COUNT(DISTINCT pr.patent_id) AS patents
          FROM patent_relationships pr
          JOIN companies c ON c.company_id = pr.company_id
         WHERE c.name IS NOT NULL AND TRIM(c.name) <> ''
         GROUP BY 1 ORDER BY patents DESC LIMIT 20
        """
    ).fetch_df()
    fig = px.bar(
        companies.iloc[::-1], x="patents", y="company", orientation="h",
        title="Top 20 companies", color="patents", color_continuous_scale="Blues",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
    save(fig, out, "12_companies_top20")

    fig = px.pie(
        companies.head(12), names="company", values="patents", hole=0.45,
        title="Company concentration (top 12)",
    )
    save(fig, out, "13_companies_concentration_pie")

    # Innovation leaders — top 5 companies, patents per year
    leader_names = companies.head(5)["company"].tolist()
    leaders = con.execute(
        """
        SELECT c.name AS company, p.year, COUNT(DISTINCT p.patent_id) AS patents
          FROM patents p
          JOIN patent_relationships pr ON pr.patent_id = p.patent_id
          JOIN companies c ON c.company_id = pr.company_id
         WHERE c.name IN ({})
         GROUP BY 1, 2 ORDER BY 2
        """.format(",".join("?" * len(leader_names))),
        leader_names,
    ).fetch_df()
    fig = px.line(
        leaders, x="year", y="patents", color="company", markers=True,
        title="Innovation leaders — patents per year",
    )
    save(fig, out, "14_companies_innovation_leaders", width=1200, height=540)

    # ----------------------------------------------------------- Advanced tab
    decade = con.execute(
        """
        SELECT (year / 10) * 10 AS decade, COUNT(*) AS patents
          FROM patents WHERE year IS NOT NULL GROUP BY 1 ORDER BY 1
        """
    ).fetch_df()
    decade["decade_label"] = decade["decade"].astype(int).astype(str) + "s"
    fig = px.bar(
        decade, x="decade_label", y="patents",
        title="Per-decade patent volume",
        color="patents", color_continuous_scale="Plasma",
    )
    fig.update_layout(coloraxis_showscale=False)
    save(fig, out, "15_advanced_decade_comparison")

    cagr = con.execute(
        """
        WITH y AS (
            SELECT c.name AS company, p.year,
                   COUNT(DISTINCT p.patent_id) AS n
              FROM patents p
              JOIN patent_relationships pr ON pr.patent_id = p.patent_id
              JOIN companies c ON c.company_id = pr.company_id
             WHERE c.name IS NOT NULL
             GROUP BY 1, 2
        ),
        bounds AS (
            SELECT company, MIN(year) AS y0, MAX(year) AS y1, SUM(n) AS total
              FROM y GROUP BY company
        ),
        endpoints AS (
            SELECT b.company, b.y0, b.y1, b.total,
                   y_first.n AS first_n, y_last.n AS last_n
              FROM bounds b
              JOIN y AS y_first ON y_first.company = b.company AND y_first.year = b.y0
              JOIN y AS y_last  ON y_last.company  = b.company AND y_last.year  = b.y1
        )
        SELECT company, y0, y1, total,
               CASE WHEN y1 > y0 AND first_n > 0
                    THEN power(last_n::DOUBLE / first_n, 1.0 / (y1 - y0)) - 1
                    ELSE NULL END AS cagr
          FROM endpoints
         WHERE total >= 10
         ORDER BY cagr DESC NULLS LAST LIMIT 15
        """
    ).fetch_df()
    cagr["cagr_pct"] = cagr["cagr"] * 100
    fig = px.bar(
        cagr.iloc[::-1], x="cagr_pct", y="company", orientation="h",
        title="Top 15 companies by CAGR",
        color="cagr_pct", color_continuous_scale="Sunset",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False,
                      xaxis_title="CAGR (%)")
    save(fig, out, "16_advanced_company_cagr")

    country_growth = con.execute(
        """
        WITH split AS (
            SELECT country,
                   SUM(CASE WHEN year < 2000 THEN patents ELSE 0 END) AS pre_2000,
                   SUM(CASE WHEN year >= 2000 THEN patents ELSE 0 END) AS post_2000
              FROM mv_country_yearly
             WHERE country IS NOT NULL GROUP BY 1
        )
        SELECT country, pre_2000, post_2000, post_2000 + pre_2000 AS total
          FROM split WHERE total >= 50 ORDER BY total DESC LIMIT 25
        """
    ).fetch_df()
    pre = country_growth["pre_2000"].astype(float).replace(0.0, float("nan"))
    country_growth["growth_pct"] = (
        (country_growth["post_2000"].astype(float) - pre) / pre
    ) * 100
    country_growth["country_name"] = country_growth["country"].map(
        lambda c: COUNTRY_LOOKUP.get(c, (c, c))[0]
    )
    fig = px.scatter(
        country_growth, x="pre_2000", y="post_2000",
        size="total", color="growth_pct", hover_name="country_name",
        title="Country growth: pre-2000 vs post-2000",
        color_continuous_scale="RdBu",
    )
    save(fig, out, "17_advanced_country_growth_scatter")

    section_growth = con.execute(
        """
        WITH split AS (
            SELECT section,
                   SUM(CASE WHEN year < 2000 THEN patents ELSE 0 END) AS pre_2000,
                   SUM(CASE WHEN year >= 2000 THEN patents ELSE 0 END) AS post_2000
              FROM mv_section_yearly
             WHERE section IS NOT NULL GROUP BY 1
        )
        SELECT section, pre_2000, post_2000,
               CASE WHEN pre_2000 > 0
                    THEN (post_2000 - pre_2000) * 100.0 / pre_2000
                    ELSE NULL END AS growth_pct
          FROM split ORDER BY growth_pct DESC NULLS LAST
        """
    ).fetch_df()
    section_growth["section_label"] = section_growth["section"].map(
        lambda s: f"{s} — {CPC_LABELS.get(s, s)}"
    )
    fig = px.bar(
        section_growth, x="section_label", y="growth_pct",
        title="CPC section growth (pre-2000 → post-2000, %)",
        color="growth_pct", color_continuous_scale="RdYlGn",
    )
    fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30,
                      yaxis_title="Growth (%)")
    save(fig, out, "18_advanced_section_growth")

    heat = con.execute(
        """
        WITH top_co AS (
            SELECT c.name AS company, COUNT(DISTINCT pr.patent_id) AS n
              FROM patent_relationships pr
              JOIN companies c ON c.company_id = pr.company_id
             WHERE c.name IS NOT NULL
             GROUP BY 1 ORDER BY n DESC LIMIT 15
        )
        SELECT c.name AS company, pc.section, COUNT(DISTINCT pc.patent_id) AS n
          FROM patent_cpc pc
          JOIN patent_relationships pr ON pr.patent_id = pc.patent_id
          JOIN companies c ON c.company_id = pr.company_id
         WHERE c.name IN (SELECT company FROM top_co) AND pc.section IS NOT NULL
         GROUP BY 1, 2
        """
    ).fetch_df()
    pivot = heat.pivot(index="company", columns="section", values="n").fillna(0)
    fig = px.imshow(
        pivot, aspect="auto", color_continuous_scale="YlGnBu",
        title="Top 15 companies × CPC sections (heatmap)",
        labels=dict(x="CPC section", y="Company", color="patents"),
    )
    save(fig, out, "19_advanced_company_section_heatmap", width=1100, height=620)

    print()
    print(f"done — {len(list(out.glob('*.png')))} PNG files in {out}")


if __name__ == "__main__":
    main()
