"""Render the Rich-formatted patent report as PNG screenshots.

The text version of the console output already lives at
``reports/console_report.txt``; this script captures the same Rich
tables/panels with a recording Console, exports SVG via Rich's built-in
``save_svg``, and converts each SVG to PNG with ``rsvg-convert``.

Output:
    <out>/01_console_overview.png   (header panel + top inventors/companies/countries)
    <out>/02_console_trends.png     (per-year patents + decade comparison)
    <out>/03_console_advanced.png   (CAGR + section growth)

Run:
    uv run python scripts/render_console_report.py \
        --warehouse /tmp/warehouse_new.duckdb \
        --out MpairweLauben_22_U_21345_DataPipeline/console_report
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

import duckdb
import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table


def _table(title: str, df: pd.DataFrame, columns: list[tuple[str, str]]) -> Table:
    tbl = Table(title=title, title_style="bold magenta", header_style="bold")
    for _, header in columns:
        tbl.add_column(header)
    for _, row in df.iterrows():
        tbl.add_row(*[str(row[col]) for col, _ in columns])
    return tbl


def _save_svg_to_png(con: Console, svg_path: Path, png_path: Path, title: str) -> None:
    con.save_svg(svg_path.as_posix(), title=title)
    subprocess.run(
        ["rsvg-convert", "-z", "2", svg_path.as_posix(), "-o", png_path.as_posix()],
        check=True,
    )
    svg_path.unlink()
    print(f"  wrote {png_path.name} ({png_path.stat().st_size / 1024:.0f} KB)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="/tmp/warehouse_new.duckdb")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if not shutil.which("rsvg-convert"):
        raise SystemExit("rsvg-convert not found on PATH (apt install librsvg2-bin)")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(args.warehouse, read_only=True)

    print(f"reading warehouse: {args.warehouse}")
    print(f"writing PNGs to:   {out}")

    # ---------- KPIs + leaderboards ------------------------------------
    totals = db.execute(
        """
        SELECT (SELECT COUNT(*) FROM patents)              AS total_patents,
               (SELECT COUNT(*) FROM inventors)            AS total_inventors,
               (SELECT COUNT(*) FROM companies)            AS total_companies,
               (SELECT COUNT(*) FROM patent_relationships) AS total_relationships,
               (SELECT MIN(year) FROM patents)             AS year_min,
               (SELECT MAX(year) FROM patents)             AS year_max
        """
    ).fetchone()
    top_inv = db.execute(
        """
        SELECT i.name AS inventor_name, i.country, COUNT(DISTINCT pr.patent_id) AS patent_count
          FROM patent_relationships pr
          JOIN inventors i ON i.inventor_id = pr.inventor_id
         WHERE i.name IS NOT NULL AND TRIM(i.name) <> ''
         GROUP BY 1, 2 ORDER BY patent_count DESC LIMIT 5
        """
    ).fetch_df()
    top_comp = db.execute(
        """
        SELECT c.name AS company_name, COUNT(DISTINCT pr.patent_id) AS patent_count
          FROM patent_relationships pr
          JOIN companies c ON c.company_id = pr.company_id
         WHERE c.name IS NOT NULL AND TRIM(c.name) <> ''
         GROUP BY 1 ORDER BY patent_count DESC LIMIT 5
        """
    ).fetch_df()
    top_ctry = db.execute(
        """
        SELECT country, SUM(patents) AS patent_count
          FROM mv_country_yearly WHERE country IS NOT NULL
         GROUP BY 1 ORDER BY patent_count DESC LIMIT 5
        """
    ).fetch_df()
    total_patents = totals[0] or 1
    top_ctry["share"] = top_ctry["patent_count"] / total_patents
    top_ctry["share_pct"] = (top_ctry["share"] * 100).round(1).astype(str) + " %"

    con = Console(record=True, width=110)

    header = Table.grid(padding=(0, 2))
    header.add_column(justify="left", style="bold")
    header.add_column(justify="right", style="cyan")
    header.add_row("Total Patents", f"{int(totals[0]):,}")
    header.add_row("Total Inventors", f"{int(totals[1]):,}")
    header.add_row("Total Companies", f"{int(totals[2]):,}")
    header.add_row("Year Range", f"{totals[4]} – {totals[5]}")
    header.add_row("Relationships", f"{int(totals[3]):,}")
    con.print(
        Panel(header, title="[bold green]PATENT INTELLIGENCE REPORT",
              border_style="green", expand=False)
    )
    con.print(
        _table("Top Inventors", top_inv,
               [("inventor_name", "Inventor"), ("country", "Country"),
                ("patent_count", "Patents")])
    )
    con.print(
        _table("Top Companies", top_comp,
               [("company_name", "Company"), ("patent_count", "Patents")])
    )
    con.print(
        _table("Top Countries", top_ctry,
               [("country", "Country"), ("patent_count", "Patents"),
                ("share_pct", "Share")])
    )
    _save_svg_to_png(con, out / "_tmp_overview.svg",
                     out / "01_console_overview.png",
                     "Patent Intelligence Report — Overview")

    # ---------- Trends -------------------------------------------------
    yearly = db.execute(
        "SELECT year, patents AS patent_count FROM mv_yearly ORDER BY year"
    ).fetch_df()
    decade = db.execute(
        """
        SELECT CASE WHEN year < 1990 THEN '1976-1989'
                    WHEN year < 2000 THEN '1990-1999'
                    WHEN year < 2010 THEN '2000-2009'
                    WHEN year < 2020 THEN '2010-2019'
                    ELSE '2020-2025' END AS period,
               COUNT(DISTINCT p.patent_id)        AS patent_count,
               COUNT(DISTINCT pr.inventor_id)     AS inventor_count,
               COUNT(DISTINCT pr.company_id)      AS company_count
          FROM patents p
          LEFT JOIN patent_relationships pr ON pr.patent_id = p.patent_id
         WHERE year IS NOT NULL GROUP BY 1 ORDER BY 1
        """
    ).fetch_df()
    decade["share_pct"] = (decade["patent_count"] / decade["patent_count"].sum() * 100).round(1).astype(str) + " %"

    con2 = Console(record=True, width=110)
    con2.print(
        _table("Patents per Year", yearly,
               [("year", "Year"), ("patent_count", "Patents")])
    )
    con2.print(
        _table("Decade comparison", decade,
               [("period", "Period"), ("patent_count", "Patents"),
                ("inventor_count", "Inventors"), ("company_count", "Companies"),
                ("share_pct", "Share")])
    )
    _save_svg_to_png(con2, out / "_tmp_trends.svg",
                     out / "02_console_trends.png",
                     "Patent Intelligence Report — Trends")

    # ---------- Advanced -----------------------------------------------
    cagr = db.execute(
        """
        WITH y AS (
            SELECT c.name AS company_name, p.year,
                   COUNT(DISTINCT p.patent_id) AS n
              FROM patents p
              JOIN patent_relationships pr ON pr.patent_id = p.patent_id
              JOIN companies c ON c.company_id = pr.company_id
             WHERE c.name IS NOT NULL GROUP BY 1, 2
        ),
        bounds AS (
            SELECT company_name, MIN(year) AS first_year, MAX(year) AS last_year,
                   SUM(n) AS total_patents
              FROM y GROUP BY 1
        ),
        endpoints AS (
            SELECT b.*, fy.n AS first_n, ly.n AS last_n
              FROM bounds b
              JOIN y AS fy ON fy.company_name = b.company_name AND fy.year = b.first_year
              JOIN y AS ly ON ly.company_name = b.company_name AND ly.year = b.last_year
        )
        SELECT company_name, first_year, last_year, total_patents,
               CASE WHEN last_year > first_year AND first_n > 0
                    THEN power(last_n::DOUBLE / first_n, 1.0 / (last_year - first_year)) - 1
                    ELSE NULL END AS cagr
          FROM endpoints WHERE total_patents >= 10
         ORDER BY cagr DESC NULLS LAST LIMIT 5
        """
    ).fetch_df()
    cagr["cagr_pct"] = (cagr["cagr"] * 100).round(2).astype(str) + " %"

    sec = db.execute(
        """
        WITH split AS (
            SELECT section,
                   SUM(CASE WHEN year < 2000 THEN patents ELSE 0 END) AS first_half_patents,
                   SUM(CASE WHEN year >= 2000 THEN patents ELSE 0 END) AS second_half_patents
              FROM mv_section_yearly WHERE section IS NOT NULL GROUP BY 1
        )
        SELECT section, first_half_patents, second_half_patents,
               CASE WHEN first_half_patents > 0
                    THEN (second_half_patents - first_half_patents) * 1.0 / first_half_patents
                    ELSE NULL END AS growth_pct
          FROM split ORDER BY growth_pct DESC NULLS LAST LIMIT 5
        """
    ).fetch_df()
    sec["growth_pct_str"] = (sec["growth_pct"] * 100).round(1).astype(str) + " %"

    con3 = Console(record=True, width=110)
    con3.print(
        _table("Top 5 companies by CAGR", cagr,
               [("company_name", "Company"), ("first_year", "First yr"),
                ("last_year", "Last yr"), ("total_patents", "Total"),
                ("cagr_pct", "CAGR")])
    )
    con3.print(
        _table("Fastest-growing CPC sections", sec,
               [("section", "Section"),
                ("first_half_patents", "1st half"),
                ("second_half_patents", "2nd half"),
                ("growth_pct_str", "Growth")])
    )
    _save_svg_to_png(con3, out / "_tmp_advanced.svg",
                     out / "03_console_advanced.png",
                     "Patent Intelligence Report — Advanced")

    print()
    print(f"done — 3 console PNGs in {out}")


if __name__ == "__main__":
    main()
