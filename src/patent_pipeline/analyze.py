"""Analyze stage — run the seven SQL queries plus support lookups."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from patent_pipeline.config import Settings
from patent_pipeline.logging_setup import logger

QUERY_FILES = [
    ("q1_top_inventors", "q1_top_inventors.sql"),
    ("q2_top_companies", "q2_top_companies.sql"),
    ("q3_top_countries", "q3_top_countries.sql"),
    ("q4_trends_over_time", "q4_trends_over_time.sql"),
    ("q5_join_patents_inventors_companies", "q5_join_patents_inventors_companies.sql"),
    ("q6_cte_innovation_leaders", "q6_cte_innovation_leaders.sql"),
    ("q7_rank_inventors_window", "q7_rank_inventors_window.sql"),
]


def run_query(conn: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    sql = path.read_text(encoding="utf-8")
    logger.info(f"running {path.name}")
    return conn.execute(sql).fetch_df()


def totals(conn: duckdb.DuckDBPyConnection) -> dict[str, int | tuple[int, int] | None]:
    return {
        "total_patents": conn.execute("SELECT COUNT(*) FROM patents").fetchone()[0],
        "total_inventors": conn.execute("SELECT COUNT(*) FROM inventors").fetchone()[0],
        "total_companies": conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
        "total_relationships": conn.execute("SELECT COUNT(*) FROM patent_relationships").fetchone()[
            0
        ],
        "year_range": conn.execute(
            "SELECT MIN(year), MAX(year) FROM patents WHERE year IS NOT NULL"
        ).fetchone(),
    }


def cpc_breakdown(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Bonus category analysis — patents by CPC section label."""
    labels = {
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
    df = conn.execute(
        """
        SELECT section, COUNT(DISTINCT patent_id) AS patent_count
        FROM patent_cpc
        WHERE section IS NOT NULL
        GROUP BY section
        ORDER BY patent_count DESC
        """
    ).fetch_df()
    df["section_label"] = df["section"].map(labels).fillna(df["section"])
    return df


def run_analyze(settings: Settings) -> dict[str, pd.DataFrame | dict]:
    """Open the warehouse, run every query, return a dict of result frames."""
    results: dict[str, pd.DataFrame | dict] = {}
    with duckdb.connect(settings.paths.warehouse_db.as_posix(), read_only=True) as conn:
        results["totals"] = totals(conn)
        for name, filename in QUERY_FILES:
            results[name] = run_query(conn, settings.paths.queries_dir / filename)
        results["cpc_breakdown"] = cpc_breakdown(conn)
    return results
