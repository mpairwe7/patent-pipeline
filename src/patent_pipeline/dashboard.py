"""Streamlit dashboard — 2026-grade interactive analytics over the DuckDB warehouse.

Start with:  ``uv run patent-pipeline dashboard``
Browse:      http://localhost:8501

Eight tabs (Overview · Trends · Geography · CPC · Inventors · Companies ·
Advanced · Search) gated by a radio router so only the visible tab's
queries actually run on each rerun. Filters in the sidebar (quick-range
preset, year slider, top-N, min-patents threshold, country, CPC
section/class drilldown, log-scale toggle, theme, palette) apply across
every tab and persist via URL query params (so a filter view is shareable).

Performance: every hot path reads from materialized summary tables
(``mv_*`` built in ``load._build_summaries``) so even on the real 9 M-
patent PatentsView warehouse a tab renders in well under a second.
"""

from __future__ import annotations

import contextlib
import time
from collections import deque
from datetime import UTC, datetime
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from patent_pipeline.config import load_settings

# Wong (2011) eight-colour palette — colour-blind safe across deuteranopia,
# protanopia and tritanopia. Used for every discrete chart so the
# dashboard meets WCAG perception criteria out of the box.
WONG_PALETTE: list[str] = [
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

# CPC section human-readable labels (mirrors analyze.cpc_breakdown).
CPC_LABELS: dict[str, str] = {
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

# Map ISO-2 codes stored in inventors.country to display name + ISO-3 (used by
# Plotly's choropleth with locationmode="ISO-3").
COUNTRY_LOOKUP: dict[str, tuple[str, str]] = {
    "US": ("United States", "USA"),
    "CN": ("China", "CHN"),
    "JP": ("Japan", "JPN"),
    "GB": ("United Kingdom", "GBR"),
    "KR": ("South Korea", "KOR"),
    "IN": ("India", "IND"),
    "DE": ("Germany", "DEU"),
    "FR": ("France", "FRA"),
    "CA": ("Canada", "CAN"),
    "AU": ("Australia", "AUS"),
    "CH": ("Switzerland", "CHE"),
    "NL": ("Netherlands", "NLD"),
    "SE": ("Sweden", "SWE"),
    "IT": ("Italy", "ITA"),
    "ES": ("Spain", "ESP"),
    "BR": ("Brazil", "BRA"),
    "RU": ("Russia", "RUS"),
    "TW": ("Taiwan", "TWN"),
    "SG": ("Singapore", "SGP"),
    "IL": ("Israel", "ISR"),
    "FI": ("Finland", "FIN"),
    "DK": ("Denmark", "DNK"),
    "BE": ("Belgium", "BEL"),
    "AT": ("Austria", "AUT"),
    "IE": ("Ireland", "IRL"),
    "NO": ("Norway", "NOR"),
    "MX": ("Mexico", "MEX"),
    "ZA": ("South Africa", "ZAF"),
    "TR": ("Turkey", "TUR"),
    "PL": ("Poland", "POL"),
    "PT": ("Portugal", "PRT"),
    "NZ": ("New Zealand", "NZL"),
    "MY": ("Malaysia", "MYS"),
    "TH": ("Thailand", "THA"),
    "PH": ("Philippines", "PHL"),
    "ID": ("Indonesia", "IDN"),
    "VN": ("Vietnam", "VNM"),
    "HK": ("Hong Kong", "HKG"),
    "SA": ("Saudi Arabia", "SAU"),
    "AE": ("United Arab Emirates", "ARE"),
}


# ---------------------------------------------------------------------------
# DuckDB plumbing
# ---------------------------------------------------------------------------
@st.cache_resource
def _connect() -> tuple[duckdb.DuckDBPyConnection, Any]:
    settings = load_settings()
    return duckdb.connect(settings.paths.warehouse_db.as_posix(), read_only=True), settings


@st.cache_data(ttl=600, show_spinner=False)
def _cached_query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    conn, _ = _connect()
    if params:
        return conn.execute(sql, params).fetch_df()
    return conn.execute(sql).fetch_df()


def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Cached SQL → DataFrame, with rolling per-call timing recorded into
    ``st.session_state["query_log"]`` so the dashboard can surface query
    latency in real time (status panel, bottom-right).
    """
    t0 = time.perf_counter()
    df = _cached_query(sql, params)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    log = st.session_state.setdefault("query_log", deque(maxlen=20))
    # The first 80 chars of the SQL are usually enough to identify the panel.
    log.append((sql.strip().split("\n", 1)[0][:80], elapsed_ms))
    return df


def _csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def _pagination_controls(state_key: str, total: int, page_size: int = 50) -> tuple[int, int]:
    """Render Prev / page-of-N / Next controls; return (offset, page_size).

    Persists the active page in ``st.session_state[state_key]`` and the
    URL query string so the view is shareable.
    """
    pages = max(1, (total + page_size - 1) // page_size)
    cur = int(st.session_state.get(state_key, 0))
    cur = max(0, min(cur, pages - 1))

    p1, p2, p3, p4 = st.columns([1, 2, 1, 2])
    with p1:
        if st.button(
            ":material/chevron_left: Prev",
            key=f"{state_key}_prev",
            disabled=cur == 0,
            width="stretch",
        ):
            st.session_state[state_key] = max(0, cur - 1)
            _persist_to_url(**{state_key: st.session_state[state_key]})
            st.rerun()
    with p2:
        new_cur = st.number_input(
            "Page",
            min_value=1,
            max_value=pages,
            value=cur + 1,
            step=1,
            key=f"{state_key}_input",
            label_visibility="collapsed",
        )
        if new_cur - 1 != cur:
            st.session_state[state_key] = new_cur - 1
            _persist_to_url(**{state_key: st.session_state[state_key]})
            st.rerun()
    with p3:
        if st.button(
            "Next :material/chevron_right:",
            key=f"{state_key}_next",
            disabled=cur >= pages - 1,
            width="stretch",
        ):
            st.session_state[state_key] = min(pages - 1, cur + 1)
            _persist_to_url(**{state_key: st.session_state[state_key]})
            st.rerun()
    with p4:
        st.caption(
            f"Page **{cur + 1}** of **{pages}** · "
            f"rows **{cur * page_size + 1:,} – {min(total, (cur + 1) * page_size):,}** "
            f"of **{total:,}**"
        )
    return cur * page_size, page_size


# ---------------------------------------------------------------------------
# URL-bookmarkable state — every filter survives a page reload via
# ``st.query_params`` so a colleague can paste a link and see the same view.
# ---------------------------------------------------------------------------
def _hydrate_state_from_url() -> None:
    """One-shot copy of URL params into st.session_state on first run."""
    if st.session_state.get("_hydrated"):
        return
    qp = st.query_params
    for key in ("active_tab", "preset", "theme", "sort_inv", "sort_comp"):
        if key in qp:
            st.session_state[key] = qp[key]
    for key in (
        "year_lo",
        "year_hi",
        "top_n",
        "min_patents",
        "inv_page",
        "comp_page",
        "search_page",
    ):
        if key in qp:
            with contextlib.suppress(ValueError):
                st.session_state[key] = int(qp[key])
    for key in ("countries", "sections", "classes"):
        raw = qp.get(key, "")
        if raw:
            st.session_state[key] = [v for v in raw.split(",") if v]
    if "log_scale" in qp:
        st.session_state["log_scale"] = qp["log_scale"] == "1"
    st.session_state["_hydrated"] = True


def _persist_to_url(**kv: Any) -> None:
    """Mirror live filter values into the URL query string."""
    qp = st.query_params
    for key, value in kv.items():
        if value is None or value == "" or value == []:
            qp.pop(key, None)
        elif isinstance(value, list):
            qp[key] = ",".join(map(str, value))
        elif isinstance(value, bool):
            qp[key] = "1" if value else "0"
        else:
            qp[key] = str(value)


# ---------------------------------------------------------------------------
# Sidebar filters — shared state across every tab
# ---------------------------------------------------------------------------
# Quick year-range presets — set in the sidebar. Stored as (label, min, max).
# A None bound is replaced at runtime with the warehouse min/max.
PRESETS: list[tuple[str, int | None, int | None]] = [
    ("All time", None, None),
    ("Last 5 yrs", -5, None),
    ("1990s", 1990, 1999),
    ("2000s", 2000, 2009),
    ("2010s", 2010, 2019),
    ("2020s", 2020, None),
]


def _resolve_preset(preset: str, lo: int, hi: int) -> tuple[int, int]:
    """Translate a preset label into a (year_from, year_to) tuple."""
    for label, mn, mx in PRESETS:
        if label != preset:
            continue
        ymin = lo if mn is None else (max(lo, hi + mn) if mn < 0 else max(lo, mn))
        ymax = hi if mx is None else min(hi, mx)
        return ymin, ymax
    return lo, hi


def sidebar_filters() -> dict[str, Any]:
    bounds = query("SELECT MIN(year) AS lo, MAX(year) AS hi FROM patents WHERE year IS NOT NULL")
    min_year = int(bounds["lo"].iloc[0] or 2020)
    max_year = int(bounds["hi"].iloc[0] or 2025)

    countries_df = query(
        "SELECT DISTINCT country FROM inventors "
        "WHERE country IS NOT NULL AND country <> '' ORDER BY country"
    )
    sections_df = query(
        "SELECT DISTINCT section FROM patent_cpc WHERE section IS NOT NULL ORDER BY section"
    )

    # Defaults (URL-hydrated values already in session_state if present).
    default_preset = st.session_state.get("preset", "All time")
    preset_lo_default, preset_hi_default = _resolve_preset(default_preset, min_year, max_year)
    yr_lo_def = st.session_state.get("year_lo", preset_lo_default)
    yr_hi_def = st.session_state.get("year_hi", preset_hi_default)
    top_n_def = st.session_state.get("top_n", 15)
    min_p_def = st.session_state.get("min_patents", 1)
    countries_def = st.session_state.get("countries", [])
    sections_def = st.session_state.get("sections", [])
    classes_def = st.session_state.get("classes", [])
    log_def = bool(st.session_state.get("log_scale", False))

    with st.sidebar:
        st.header(":material/tune: Filters")

        preset = st.radio(
            "Quick range",
            options=[p[0] for p in PRESETS],
            index=[p[0] for p in PRESETS].index(default_preset),
            horizontal=True,
            help="Snap the year slider to a preset window.",
        )
        preset_lo, preset_hi = _resolve_preset(preset, min_year, max_year)
        # If the user just changed the preset, reset the year slider; else
        # honour any URL-hydrated explicit value.
        if st.session_state.get("_last_preset") != preset:
            yr_lo_def, yr_hi_def = preset_lo, preset_hi
            st.session_state["_last_preset"] = preset

        year_range = st.slider(
            "Year range",
            min_value=min_year,
            max_value=max_year,
            value=(int(yr_lo_def), int(yr_hi_def)),
            help="Inclusive year window applied to every tab.",
        )

        c1, c2 = st.columns(2)
        with c1:
            top_n = st.slider(
                "Top N",
                min_value=5,
                max_value=50,
                value=int(top_n_def),
                step=5,
                help="Cap the number of rows shown in leaderboards and charts.",
            )
        with c2:
            min_patents = st.slider(
                "Min patents",
                min_value=1,
                max_value=50,
                value=int(min_p_def),
                help="Filter out entities whose total falls below this threshold.",
            )

        country_choice = st.multiselect(
            "Inventor country (ISO-2)",
            options=countries_df["country"].tolist(),
            default=[c for c in countries_def if c in countries_df["country"].tolist()],
            help="Restrict to inventors based in these countries.",
        )
        section_choice = st.multiselect(
            "CPC section",
            options=sections_df["section"].tolist(),
            default=[s for s in sections_def if s in sections_df["section"].tolist()],
            format_func=lambda s: f"{s} — {CPC_LABELS.get(s, s)}",
            help="Top-level CPC. Pick one or more to enable class drill-down.",
        )

        cpc_class_choice: list[str] = []
        if section_choice:
            placeholders = ", ".join(["?"] * len(section_choice))
            classes_df = query(
                f"""
                SELECT DISTINCT cpc_class FROM patent_cpc
                WHERE cpc_class IS NOT NULL AND section IN ({placeholders})
                ORDER BY cpc_class
                """,
                tuple(section_choice),
            )
            cpc_class_choice = st.multiselect(
                "CPC class (drill-down)",
                options=classes_df["cpc_class"].tolist(),
                default=[c for c in classes_def if c in classes_df["cpc_class"].tolist()],
                help="Narrow within the selected CPC section(s).",
            )

        log_scale = st.checkbox(
            "Logarithmic Y-axis (Trends)",
            value=log_def,
            help="Compress large ranges on the yearly bar chart.",
        )

        if st.button(":material/restart_alt: Reset all filters", width="stretch"):
            for k in (
                "preset",
                "year_lo",
                "year_hi",
                "top_n",
                "min_patents",
                "countries",
                "sections",
                "classes",
                "log_scale",
                "active_tab",
                "inv_page",
                "comp_page",
                "search_page",
                "xfilter_country",
            ):
                st.session_state.pop(k, None)
            st.query_params.clear()
            st.rerun()

        st.divider()
        st.caption(
            f"Window **{year_range[0]} – {year_range[1]}** · preset **{preset}** · "
            f"top-N **{top_n}** · min-patents **{min_patents}**"
        )

    # Cross-filter overlay: clicking a country bar elsewhere pushes a code
    # into session_state["xfilter_country"] which we merge in here.
    xfilter = st.session_state.get("xfilter_country")
    if xfilter and xfilter not in country_choice:
        country_choice = [*country_choice, xfilter]

    # Persist to URL so the view is shareable.
    _persist_to_url(
        preset=preset,
        year_lo=year_range[0],
        year_hi=year_range[1],
        top_n=top_n,
        min_patents=min_patents,
        countries=country_choice,
        sections=section_choice,
        classes=cpc_class_choice,
        log_scale=log_scale,
    )
    return {
        "year_range": year_range,
        "top_n": top_n,
        "min_patents": min_patents,
        "countries": country_choice,
        "sections": section_choice,
        "classes": cpc_class_choice,
        "log_scale": log_scale,
        "preset": preset,
    }


def _build_clauses(f: dict[str, Any]) -> tuple[str, str, str, list]:
    """Build WHERE/JOIN fragments for the active filters.

    Returns (country_clause, section_clause, section_join, extra_params).
    Use them inside a query that has aliases p (patents) and i (inventors).
    """
    country_clause = ""
    section_clause = ""
    section_join = ""
    params: list[Any] = []
    if f["countries"]:
        placeholders = ", ".join(["?"] * len(f["countries"]))
        country_clause = f" AND i.country IN ({placeholders})"
        params.extend(f["countries"])
    if f["sections"] or f.get("classes"):
        section_join = "JOIN patent_cpc pc ON pc.patent_id = p.patent_id"
        if f["sections"]:
            placeholders = ", ".join(["?"] * len(f["sections"]))
            section_clause += f" AND pc.section IN ({placeholders})"
            params.extend(f["sections"])
        if f.get("classes"):
            placeholders = ", ".join(["?"] * len(f["classes"]))
            section_clause += f" AND pc.cpc_class IN ({placeholders})"
            params.extend(f["classes"])
    return country_clause, section_clause, section_join, params


# Sort options offered to inventors / companies tabs.
SORT_OPTIONS = {
    "Patent volume (high → low)": ("patents DESC", "DESC"),
    "Patent volume (low → high)": ("patents ASC", "ASC"),
    "Name (A → Z)": ("name ASC", "ASC"),
    "Name (Z → A)": ("name DESC", "DESC"),
}


# ---------------------------------------------------------------------------
# Fast-path query helpers — read from materialized summary tables (mv_*) when
# the active filters allow it. With 9 M patents in the warehouse this drops
# initial-render time from minutes to milliseconds. CPC-filter-active queries
# fall back to the base tables (still seconds, not minutes).
# ---------------------------------------------------------------------------
def _fast_yearly(f: dict[str, Any]) -> pd.DataFrame:
    """(year, patents) honouring the active sidebar filters."""
    yr = f["year_range"]
    if f["sections"] or f.get("classes"):
        cc, sc, sj, extra = _build_clauses(f)
        return query(
            f"""
            SELECT p.year, COUNT(DISTINCT p.patent_id) AS patents
            FROM patents p
            LEFT JOIN patent_relationships r ON r.patent_id = p.patent_id
            LEFT JOIN inventors i            ON i.inventor_id = r.inventor_id
            {sj}
            WHERE p.year BETWEEN ? AND ? {cc}{sc}
            GROUP BY p.year ORDER BY p.year
            """,
            (yr[0], yr[1], *extra),
        )
    if f["countries"]:
        ph = ", ".join(["?"] * len(f["countries"]))
        return query(
            f"""
            SELECT year, CAST(SUM(patents) AS INT) AS patents FROM mv_country_yearly
            WHERE year BETWEEN ? AND ? AND country IN ({ph})
            GROUP BY year ORDER BY year
            """,
            (yr[0], yr[1], *f["countries"]),
        )
    return query(
        "SELECT year, patents FROM mv_yearly WHERE year BETWEEN ? AND ? ORDER BY year",
        (yr[0], yr[1]),
    )


def _fast_country_share(f: dict[str, Any]) -> pd.DataFrame:
    """(country, patents) with optional year-range / CPC filtering."""
    yr = f["year_range"]
    if f["sections"] or f.get("classes"):
        cc, sc, sj, extra = _build_clauses(f)
        return query(
            f"""
            SELECT i.country AS country, COUNT(DISTINCT p.patent_id) AS patents
            FROM inventors i
            JOIN patent_relationships r ON r.inventor_id = i.inventor_id
            JOIN patents p              ON p.patent_id   = r.patent_id
            {sj}
            WHERE p.year BETWEEN ? AND ?
                  AND i.country IS NOT NULL AND i.country <> ''
                  {cc}{sc}
            GROUP BY i.country ORDER BY patents DESC
            """,
            (yr[0], yr[1], *extra),
        )
    return query(
        """
        SELECT country, CAST(SUM(patents) AS INT) AS patents
        FROM mv_country_yearly
        WHERE year BETWEEN ? AND ?
        GROUP BY country
        ORDER BY patents DESC
        """,
        (yr[0], yr[1]),
    )


def _fast_top_inventors(
    f: dict[str, Any],
    top_n: int,
    min_patents: int,
    order_sql: str,
    *,
    offset: int = 0,
) -> pd.DataFrame:
    """Top-N inventors *within the active year window* (window-correct counts).

    Uses ``mv_inventor_yearly`` (one row per inventor × active year) so the
    SUM only includes patents inside ``year_range`` — fixes the previous
    bug where lifetime totals were displayed regardless of preset.
    """
    yr = f["year_range"]
    if f["sections"] or f.get("classes"):
        cc, sc, sj, extra = _build_clauses(f)
        order_by_sql = order_sql.replace("name", "inventor")
        return query(
            f"""
            SELECT i.name AS inventor,
                   COALESCE(i.country, '?') AS country,
                   COUNT(DISTINCT p.patent_id) AS patents
            FROM inventors i
            JOIN patent_relationships r ON r.inventor_id = i.inventor_id
            JOIN patents p              ON p.patent_id   = r.patent_id
            {sj}
            WHERE p.year BETWEEN ? AND ? {cc}{sc}
            GROUP BY i.name, i.country
            HAVING COUNT(DISTINCT p.patent_id) >= ?
            ORDER BY {order_by_sql}
            LIMIT ? OFFSET ?
            """,
            (yr[0], yr[1], *extra, min_patents, top_n, offset),
        )
    country_clause = ""
    params: list[Any] = []
    if f["countries"]:
        ph = ", ".join(["?"] * len(f["countries"]))
        country_clause = f" AND country IN ({ph})"
        params.extend(f["countries"])
    order_by_sql = order_sql.replace("name", "inventor")
    return query(
        f"""
        SELECT inventor, country, CAST(SUM(patents) AS BIGINT) AS patents
        FROM mv_inventor_yearly
        WHERE year BETWEEN ? AND ?
              {country_clause}
        GROUP BY inventor, country
        HAVING SUM(patents) >= ?
        ORDER BY {order_by_sql}
        LIMIT ? OFFSET ?
        """,
        (yr[0], yr[1], *params, min_patents, top_n, offset),
    )


def _count_top_inventors(f: dict[str, Any], min_patents: int) -> int:
    """Cached total count of inventors with ≥ min_patents *inside* the year window."""
    yr = f["year_range"]
    if f["sections"] or f.get("classes"):
        cc, sc, sj, extra = _build_clauses(f)
        df = query(
            f"""
            SELECT COUNT(*) AS n FROM (
                SELECT i.name, i.country
                FROM inventors i
                JOIN patent_relationships r ON r.inventor_id = i.inventor_id
                JOIN patents p              ON p.patent_id   = r.patent_id
                {sj}
                WHERE p.year BETWEEN ? AND ? {cc}{sc}
                GROUP BY i.name, i.country
                HAVING COUNT(DISTINCT p.patent_id) >= ?
            )
            """,
            (yr[0], yr[1], *extra, min_patents),
        )
        return int(df["n"].iloc[0])
    country_clause = ""
    params: list[Any] = []
    if f["countries"]:
        ph = ", ".join(["?"] * len(f["countries"]))
        country_clause = f" AND country IN ({ph})"
        params.extend(f["countries"])
    df = query(
        f"""
        SELECT COUNT(*) AS n FROM (
            SELECT inventor FROM mv_inventor_yearly
            WHERE year BETWEEN ? AND ? {country_clause}
            GROUP BY inventor, country HAVING SUM(patents) >= ?
        )
        """,
        (yr[0], yr[1], *params, min_patents),
    )
    return int(df["n"].iloc[0])


def _fast_top_companies(
    f: dict[str, Any],
    top_n: int,
    min_patents: int,
    order_sql: str,
    *,
    offset: int = 0,
) -> pd.DataFrame:
    """Top-N companies *within the active year window* — see _fast_top_inventors."""
    yr = f["year_range"]
    if f["sections"] or f.get("classes") or f["countries"]:
        cc, sc, sj, extra = _build_clauses(f)
        order_by_sql = order_sql.replace("name", "company")
        return query(
            f"""
            SELECT c.name AS company, COUNT(DISTINCT p.patent_id) AS patents
            FROM companies c
            JOIN patent_relationships r ON r.company_id = c.company_id
            JOIN patents p              ON p.patent_id   = r.patent_id
            JOIN inventors i            ON i.inventor_id = r.inventor_id
            {sj}
            WHERE p.year BETWEEN ? AND ? {cc}{sc}
            GROUP BY c.name
            HAVING COUNT(DISTINCT p.patent_id) >= ?
            ORDER BY {order_by_sql}
            LIMIT ? OFFSET ?
            """,
            (yr[0], yr[1], *extra, min_patents, top_n, offset),
        )
    order_by_sql = order_sql.replace("name", "company")
    return query(
        f"""
        SELECT company, CAST(SUM(patents) AS BIGINT) AS patents
        FROM mv_company_yearly
        WHERE year BETWEEN ? AND ?
        GROUP BY company
        HAVING SUM(patents) >= ?
        ORDER BY {order_by_sql}
        LIMIT ? OFFSET ?
        """,
        (yr[0], yr[1], min_patents, top_n, offset),
    )


def _count_top_companies(f: dict[str, Any], min_patents: int) -> int:
    yr = f["year_range"]
    if f["sections"] or f.get("classes") or f["countries"]:
        cc, sc, sj, extra = _build_clauses(f)
        df = query(
            f"""
            SELECT COUNT(*) AS n FROM (
                SELECT c.name FROM companies c
                JOIN patent_relationships r ON r.company_id = c.company_id
                JOIN patents p              ON p.patent_id   = r.patent_id
                JOIN inventors i            ON i.inventor_id = r.inventor_id
                {sj}
                WHERE p.year BETWEEN ? AND ? {cc}{sc}
                GROUP BY c.name HAVING COUNT(DISTINCT p.patent_id) >= ?
            )
            """,
            (yr[0], yr[1], *extra, min_patents),
        )
        return int(df["n"].iloc[0])
    df = query(
        """
        SELECT COUNT(*) AS n FROM (
            SELECT company FROM mv_company_yearly
            WHERE year BETWEEN ? AND ?
            GROUP BY company HAVING SUM(patents) >= ?
        )
        """,
        (yr[0], yr[1], min_patents),
    )
    return int(df["n"].iloc[0])


# ---------------------------------------------------------------------------
# Tab: Overview
# ---------------------------------------------------------------------------
def tab_overview(f: dict[str, Any]) -> None:
    yr = f["year_range"]

    yearly_filt = _fast_yearly(f)
    patents_n = int(yearly_filt["patents"].sum()) if not yearly_filt.empty else 0

    # KPI counts (only run the slow distinct-count query when CPC filter forces it).
    if f["sections"] or f.get("classes"):
        cc, sc, sj, extra = _build_clauses(f)
        kpi = query(
            f"""
            SELECT COUNT(DISTINCT i.inventor_id) AS inventors,
                   COUNT(DISTINCT c.company_id)  AS companies,
                   COUNT(DISTINCT i.country)     AS countries
            FROM patents p
            LEFT JOIN patent_relationships r ON r.patent_id = p.patent_id
            LEFT JOIN inventors i            ON i.inventor_id = r.inventor_id
            LEFT JOIN companies c            ON c.company_id = r.company_id
            {sj}
            WHERE p.year BETWEEN ? AND ? {cc}{sc}
            """,
            (yr[0], yr[1], *extra),
        )
        inventors_n = int(kpi["inventors"].iloc[0])
        companies_n = int(kpi["companies"].iloc[0])
        countries_n = int(kpi["countries"].iloc[0])
    else:
        # Fast path: derive each count from the per-year mvs so the figures
        # reflect activity *within* the active year window, not lifetime.
        inv_q = query(
            "SELECT COUNT(DISTINCT inventor) AS n FROM mv_inventor_yearly "
            "WHERE year BETWEEN ? AND ?",
            (yr[0], yr[1]),
        )
        comp_q = query(
            "SELECT COUNT(DISTINCT company) AS n FROM mv_company_yearly WHERE year BETWEEN ? AND ?",
            (yr[0], yr[1]),
        )
        if f["countries"]:
            ph = ", ".join(["?"] * len(f["countries"]))
            ctry_q = query(
                f"""
                SELECT COUNT(DISTINCT country) AS n FROM mv_country_yearly
                WHERE year BETWEEN ? AND ? AND country IN ({ph})
                """,
                (yr[0], yr[1], *f["countries"]),
            )
        else:
            ctry_q = query(
                "SELECT COUNT(DISTINCT country) AS n FROM mv_country_yearly "
                "WHERE year BETWEEN ? AND ?",
                (yr[0], yr[1]),
            )
        inventors_n = int(inv_q["n"].iloc[0])
        companies_n = int(comp_q["n"].iloc[0])
        countries_n = int(ctry_q["n"].iloc[0])

    # YoY delta from the unfiltered yearly view (cheap mv lookup).
    yearly_all = query("SELECT year, patents FROM mv_yearly ORDER BY year")
    delta_str: str | None = None
    if len(yearly_all) >= 2:
        last, prev = float(yearly_all["patents"].iloc[-1]), float(yearly_all["patents"].iloc[-2])
        if prev:
            delta_str = f"{(last - prev) / prev:+.1%} YoY"

    a, b, c, d = st.columns(4)
    a.metric("Patents", f"{patents_n:,}", delta=delta_str)
    b.metric("Inventors", f"{inventors_n:,}")
    b.caption("(distinct, post-disambiguation)")
    c.metric("Companies", f"{companies_n:,}")
    d.metric("Countries", f"{countries_n:,}")

    st.divider()

    mc1, mc2 = st.columns([2, 1])
    with mc1:
        st.subheader(":material/show_chart: Patents filed per year")
        if yearly_filt.empty:
            st.info("No patents match the current filters.")
        else:
            fig = px.area(yearly_filt, x="year", y="patents", markers=True)
            fig.update_traces(line_color="#4c72b0", fillcolor="rgba(76,114,176,0.25)")
            fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=320)
            st.plotly_chart(fig, width="stretch")

    with mc2:
        st.subheader(":material/emoji_events: Top 3 — quick view")
        top3 = _fast_top_companies(f, top_n=3, min_patents=1, order_sql="patents DESC")
        if top3.empty:
            st.write("_(no companies match)_")
        else:
            for _, row in top3.iterrows():
                st.markdown(
                    f":material/business: **{row['company']}** — {int(row['patents']):,} patents"
                )


# ---------------------------------------------------------------------------
# Tab: Trends
# ---------------------------------------------------------------------------
def tab_trends(f: dict[str, Any]) -> None:
    yearly = _fast_yearly(f)
    if yearly.empty:
        st.info("No patents match the current filters.")
        return

    yearly["yoy"] = yearly["patents"].pct_change()
    yearly["cumulative"] = yearly["patents"].cumsum()

    st.subheader(":material/bar_chart: Yearly volume")
    fig = px.bar(
        yearly,
        x="year",
        y="patents",
        text_auto=True,
        color="patents",
        color_continuous_scale="Blues",
    )
    fig.update_layout(coloraxis_showscale=False, height=380)
    if f.get("log_scale"):
        fig.update_yaxes(type="log", title_text="Patents (log)")
    st.plotly_chart(fig, width="stretch")

    g1, g2 = st.columns(2)
    with g1:
        st.subheader(":material/trending_up: Year-over-year growth")
        gdf = yearly.dropna(subset=["yoy"]).copy()
        if gdf.empty:
            st.write("_(need ≥ 2 years for YoY)_")
        else:
            gdf["direction"] = gdf["yoy"].apply(lambda v: "↑ growth" if v >= 0 else "↓ decline")
            gdf["label"] = gdf["yoy"].map(lambda v: f"{v:+.1%}")
            fig = px.bar(
                gdf,
                x="year",
                y="yoy",
                color="direction",
                text="label",
                color_discrete_map={"↑ growth": "#55a868", "↓ decline": "#c44e52"},
            )
            fig.update_layout(yaxis_tickformat=".0%", showlegend=False, height=320)
            st.plotly_chart(fig, width="stretch")
    with g2:
        st.subheader(":material/auto_stories: Cumulative patents")
        fig = px.line(yearly, x="year", y="cumulative", markers=True)
        fig.update_traces(line_color="#4c72b0")
        fig.update_layout(height=320)
        st.plotly_chart(fig, width="stretch")

    st.download_button(
        ":material/download: Download yearly trend (CSV)",
        data=_csv_bytes(yearly[["year", "patents", "yoy", "cumulative"]]),
        file_name="yearly_trends.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Tab: Geography
# ---------------------------------------------------------------------------
def tab_geography(f: dict[str, Any]) -> None:
    df = _fast_country_share(f)
    if df.empty:
        st.info("No country data matches the current filters.")
        return

    total = int(df["patents"].sum())
    df["share"] = df["patents"] / total
    df["country_name"] = df["country"].map(lambda c: COUNTRY_LOOKUP.get(c, (c, c))[0])
    df["iso3"] = df["country"].map(lambda c: COUNTRY_LOOKUP.get(c, (c, c))[1])

    st.subheader(":material/public: Patent share by inventor country")

    g1, g2 = st.columns([3, 2])
    with g1:
        fig = px.choropleth(
            df,
            locations="iso3",
            locationmode="ISO-3",
            color="patents",
            hover_name="country_name",
            hover_data={"share": ":.1%", "country": True, "iso3": False},
            color_continuous_scale="Viridis",
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=420,
            geo=dict(showframe=False, showcoastlines=True, projection_type="natural earth"),
        )
        st.plotly_chart(fig, width="stretch")
    with g2:
        top = df.head(f["top_n"]).iloc[::-1]
        # Cross-filter affordance: clicking a country bar pushes its ISO-2
        # code into st.session_state["xfilter_country"] and reruns the app.
        fig = px.bar(
            top,
            x="patents",
            y="country_name",
            orientation="h",
            text="patents",
            color="patents",
            color_continuous_scale="Viridis",
            custom_data=["country"],
        )
        fig.update_layout(
            coloraxis_showscale=False,
            height=420,
            yaxis_title="",
            title_text="Click a bar to cross-filter every tab",
        )
        sel = st.plotly_chart(
            fig,
            width="stretch",
            on_select="rerun",
            selection_mode="points",
            key="geo_country_bar",
        )
        # Newly clicked country → set as global cross-filter.
        try:
            pts = sel.selection.points if sel and hasattr(sel, "selection") else []
        except AttributeError:
            pts = []
        if pts:
            cd = pts[0].get("customdata")
            if cd:
                code = cd[0] if isinstance(cd, list) else cd
                if code and code != st.session_state.get("xfilter_country"):
                    st.session_state["xfilter_country"] = code
                    st.rerun()

    st.subheader(":material/donut_small: Concentration")
    h1, h2 = st.columns(2)
    with h1:
        pie = px.pie(df.head(12), names="country_name", values="patents", hole=0.4)
        pie.update_layout(height=380)
        st.plotly_chart(pie, width="stretch")
    with h2:
        hhi = float((df["share"] ** 2).sum() * 10000)
        top3_share = float(df.head(3)["share"].sum())
        top10_share = float(df.head(10)["share"].sum())
        st.metric(
            "Herfindahl–Hirschman Index",
            f"{hhi:,.0f}",
            help="0–10,000. <1500 = unconcentrated · 1500–2500 = moderate · >2500 = concentrated.",
        )
        st.metric("Top-3 country share", f"{top3_share:.1%}")
        st.metric("Top-10 country share", f"{top10_share:.1%}")
        st.caption(f"Distinct countries in selection: **{len(df):,}**")

    st.download_button(
        ":material/download: Download country breakdown (CSV)",
        data=_csv_bytes(df),
        file_name="country_breakdown.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Tab: CPC categories
# ---------------------------------------------------------------------------
def tab_cpc(f: dict[str, Any]) -> None:
    yr = f["year_range"]
    if f["countries"]:
        # Country filter forces a join through inventors → relationships.
        cc, sc, _sj, extra = _build_clauses(f)
        inventor_join = (
            "LEFT JOIN patent_relationships r ON r.patent_id = p.patent_id "
            "LEFT JOIN inventors i            ON i.inventor_id = r.inventor_id"
        )
        df = query(
            f"""
            SELECT pc.section AS section,
                   pc.cpc_class AS cpc_class,
                   COUNT(DISTINCT p.patent_id) AS patents
            FROM patent_cpc pc
            JOIN patents p ON p.patent_id = pc.patent_id
            {inventor_join}
            WHERE p.year BETWEEN ? AND ?
                  AND pc.section IS NOT NULL
                  {cc}{sc}
            GROUP BY pc.section, pc.cpc_class
            """,
            (yr[0], yr[1], *extra),
        )
    else:
        section_clause = ""
        class_clause = ""
        extra: list[Any] = []
        if f["sections"]:
            ph = ", ".join(["?"] * len(f["sections"]))
            section_clause = f" AND section IN ({ph})"
            extra.extend(f["sections"])
        if f.get("classes"):
            ph = ", ".join(["?"] * len(f["classes"]))
            class_clause = f" AND cpc_class IN ({ph})"
            extra.extend(f["classes"])
        df = query(
            f"""
            SELECT section, cpc_class, CAST(SUM(patents) AS INT) AS patents
            FROM mv_section_yearly
            WHERE year BETWEEN ? AND ?
            {section_clause}{class_clause}
            GROUP BY section, cpc_class
            """,
            (yr[0], yr[1], *extra),
        )
    if df.empty:
        st.info("No CPC data matches the current filters.")
        return
    df["section_label"] = df["section"].map(CPC_LABELS).fillna(df["section"])

    section_totals = (
        df.groupby(["section", "section_label"], as_index=False)["patents"]
        .sum()
        .sort_values("patents", ascending=False)
    )

    st.subheader(":material/category: CPC section breakdown")
    fig = px.bar(
        section_totals,
        x="section_label",
        y="patents",
        text="patents",
        color="section_label",
    )
    fig.update_layout(showlegend=False, height=360, xaxis_tickangle=-15, xaxis_title="")
    st.plotly_chart(fig, width="stretch")

    st.subheader(":material/account_tree: Section → class treemap")
    treemap = px.treemap(
        df,
        path=["section_label", "cpc_class"],
        values="patents",
        color="patents",
        color_continuous_scale="Tealgrn",
    )
    treemap.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=480)
    st.plotly_chart(treemap, width="stretch")

    st.subheader(":material/timeline: Section trends over time")
    if f["countries"]:
        cc, sc, _sj, extra2 = _build_clauses(f)
        trend_df = query(
            f"""
            SELECT p.year, pc.section,
                   COUNT(DISTINCT p.patent_id) AS patents
            FROM patent_cpc pc
            JOIN patents p ON p.patent_id = pc.patent_id
            LEFT JOIN patent_relationships r ON r.patent_id = p.patent_id
            LEFT JOIN inventors i            ON i.inventor_id = r.inventor_id
            WHERE p.year BETWEEN ? AND ?
                  AND pc.section IS NOT NULL
                  {cc}{sc}
            GROUP BY p.year, pc.section
            ORDER BY p.year, pc.section
            """,
            (yr[0], yr[1], *extra2),
        )
    else:
        trend_df = query(
            """
            SELECT year, section, CAST(SUM(patents) AS INT) AS patents
            FROM mv_section_yearly
            WHERE year BETWEEN ? AND ?
            GROUP BY year, section
            ORDER BY year, section
            """,
            (yr[0], yr[1]),
        )
    if not trend_df.empty:
        trend_df["section_label"] = trend_df["section"].map(CPC_LABELS).fillna(trend_df["section"])
        line = px.line(
            trend_df,
            x="year",
            y="patents",
            color="section_label",
            markers=True,
        )
        line.update_layout(height=380, legend_title="Section")
        st.plotly_chart(line, width="stretch")

    st.download_button(
        ":material/download: Download CPC breakdown (CSV)",
        data=_csv_bytes(section_totals),
        file_name="cpc_breakdown.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Tab: Inventors
# ---------------------------------------------------------------------------
def tab_inventors(f: dict[str, Any]) -> None:
    s_col, _ = st.columns([2, 4])
    with s_col:
        sort_label = st.selectbox(
            "Sort inventors by",
            options=list(SORT_OPTIONS.keys()),
            index=0,
            key="inv_sort",
            help="Server-side sort applied at SQL ORDER BY time.",
        )
    order_by, _ = SORT_OPTIONS[sort_label]
    min_patents = int(f.get("min_patents", 1))

    page_size = int(f["top_n"])
    total = _count_top_inventors(f, min_patents)
    if total == 0:
        st.info("No inventors match the current filters.")
        return
    offset, _ = _pagination_controls("inv_page", total, page_size=page_size)

    df = _fast_top_inventors(
        f, top_n=page_size, min_patents=min_patents, order_sql=order_by, offset=offset
    )

    st.subheader(
        f":material/science: Inventors {offset + 1:,}–{min(total, offset + page_size):,} "
        f"of {total:,} · min {min_patents} patents · {sort_label.lower()}"
    )
    if df.empty:
        st.info("No inventors on this page.")
        return

    g1, g2 = st.columns([3, 2])
    with g1:
        fig = px.bar(
            df.iloc[::-1],
            x="patents",
            y="inventor",
            orientation="h",
            color="country",
            text="patents",
        )
        fig.update_layout(height=520, yaxis_title="", legend_title="Country")
        st.plotly_chart(fig, width="stretch")
    with g2:
        st.dataframe(df, width="stretch", hide_index=True, height=520)

    # Top-3 per country uses mv_inventor_total when no CPC filter is active
    # — that's the demo of the window function ranker on the full warehouse.
    st.subheader(":material/military_tech: Top-3 inventors per country (window function)")
    yr = f["year_range"]
    if f["sections"] or f.get("classes"):
        cc, sc, sj, extra = _build_clauses(f)
        win_df = query(
            f"""
            WITH counts AS (
                SELECT i.name AS inventor, i.country AS country,
                       COUNT(DISTINCT p.patent_id) AS patents
                FROM inventors i
                JOIN patent_relationships r ON r.inventor_id = i.inventor_id
                JOIN patents p              ON p.patent_id   = r.patent_id
                {sj}
                WHERE p.year BETWEEN ? AND ?
                      AND i.country IS NOT NULL AND i.country <> ''
                      {cc}{sc}
                GROUP BY i.name, i.country
            ), ranked AS (
                SELECT inventor, country, patents,
                       RANK() OVER (PARTITION BY country ORDER BY patents DESC) AS rnk
                FROM counts
            )
            SELECT country, inventor, patents, rnk FROM ranked
            WHERE rnk <= 3 ORDER BY country, rnk
            """,
            (yr[0], yr[1], *extra),
        )
    else:
        # Window-correct: sum patents per (inventor, country) inside the
        # active year range, then rank within each country.
        win_df = query(
            """
            WITH counts AS (
                SELECT inventor, country, CAST(SUM(patents) AS BIGINT) AS patents
                FROM mv_inventor_yearly
                WHERE year BETWEEN ? AND ? AND country <> '?' AND country <> ''
                GROUP BY inventor, country
            ), ranked AS (
                SELECT inventor, country, patents,
                       RANK() OVER (PARTITION BY country ORDER BY patents DESC) AS rnk
                FROM counts
            )
            SELECT country, inventor, patents, rnk
            FROM ranked WHERE rnk <= 3
            ORDER BY country, rnk
            """,
            (yr[0], yr[1]),
        )
    if win_df.empty:
        st.write("_(no inventor data with country)_")
    else:
        st.dataframe(win_df, width="stretch", hide_index=True)

    st.download_button(
        ":material/download: Download top inventors (CSV)",
        data=_csv_bytes(df),
        file_name="top_inventors.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Tab: Companies
# ---------------------------------------------------------------------------
def tab_companies(f: dict[str, Any]) -> None:
    s_col, _ = st.columns([2, 4])
    with s_col:
        sort_label = st.selectbox(
            "Sort companies by",
            options=list(SORT_OPTIONS.keys()),
            index=0,
            key="comp_sort",
            help="Server-side sort applied at SQL ORDER BY time.",
        )
    order_by, _ = SORT_OPTIONS[sort_label]
    min_patents = int(f.get("min_patents", 1))

    page_size = int(f["top_n"])
    total = _count_top_companies(f, min_patents)
    if total == 0:
        st.info("No companies match the current filters.")
        return
    offset, _ = _pagination_controls("comp_page", total, page_size=page_size)

    df = _fast_top_companies(
        f, top_n=page_size, min_patents=min_patents, order_sql=order_by, offset=offset
    )
    if df.empty:
        st.info("No companies on this page.")
        return

    st.subheader(
        f":material/business: Companies {offset + 1:,}–{min(total, offset + page_size):,} "
        f"of {total:,} · min {min_patents} patents · {sort_label.lower()}"
    )
    g1, g2 = st.columns([3, 2])
    with g1:
        fig = px.bar(
            df.iloc[::-1],
            x="patents",
            y="company",
            orientation="h",
            text="patents",
            color="patents",
            color_continuous_scale="Tealgrn",
        )
        fig.update_layout(coloraxis_showscale=False, height=520, yaxis_title="")
        st.plotly_chart(fig, width="stretch")
    with g2:
        fig = px.pie(df, names="company", values="patents", hole=0.45)
        fig.update_layout(height=520, legend_title="Company")
        st.plotly_chart(fig, width="stretch")

    st.subheader(":material/rocket_launch: Innovation leaders — patents per year")
    yr = f["year_range"]
    leaders = df.head(8)["company"].tolist()
    if leaders:
        ph = ", ".join(["?"] * len(leaders))
        leaders_df = query(
            f"""
            SELECT year, company, patents FROM mv_company_yearly
            WHERE year BETWEEN ? AND ? AND company IN ({ph})
            ORDER BY year, company
            """,
            (yr[0], yr[1], *leaders),
        )
        if not leaders_df.empty:
            fig = px.line(leaders_df, x="year", y="patents", color="company", markers=True)
            fig.update_layout(height=380, legend_title="Company")
            st.plotly_chart(fig, width="stretch")

    st.download_button(
        ":material/download: Download top companies (CSV)",
        data=_csv_bytes(df),
        file_name="top_companies.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------------------------
# Tab: Advanced analytics
# ---------------------------------------------------------------------------
def tab_advanced(f: dict[str, Any]) -> None:
    """Decade comparison, CAGR leaderboards, growth bubbles, section heatmap.

    These views deliberately ignore the sidebar filters so the long-horizon
    metrics (CAGR, decade-vs-decade, first-vs-second-half) stay stable.
    """
    st.caption(
        "Filters apply only to the year-range slider where noted. "
        "Long-horizon metrics use the full warehouse span."
    )

    # ------------------------------------------------------------------
    # Decade comparison
    # ------------------------------------------------------------------
    st.subheader(":material/calendar_view_month: Decade comparison")
    decade = query(
        """
        SELECT period, patent_count AS patents,
               inventor_count AS inventors,
               company_count  AS companies,
               share
        FROM mv_decade_compare ORDER BY period
        """
    )
    if decade.empty:
        st.info("No decade buckets — re-run the pipeline.")
        return

    g1, g2 = st.columns([3, 2])
    with g1:
        fig = px.bar(
            decade,
            x="period",
            y="patents",
            text="patents",
            color="period",
            color_discrete_sequence=["#4c72b0", "#55a868", "#c44e52"],
        )
        fig.update_layout(showlegend=False, height=380, xaxis_title="")
        st.plotly_chart(fig, width="stretch")
    with g2:
        for _, row in decade.iterrows():
            st.metric(
                row["period"],
                f"{int(row['patents']):,} patents",
                delta=f"{row['share']:.1%} share",
            )

    st.divider()

    # ------------------------------------------------------------------
    # Company CAGR
    # ------------------------------------------------------------------
    st.subheader(":material/insights: Top companies by CAGR (full warehouse span)")
    cagr = query(
        """
        SELECT company_name, first_year, last_year, span_years,
               first_year_patents, last_year_patents, total_patents, cagr
        FROM mv_company_cagr
        ORDER BY cagr DESC NULLS LAST, total_patents DESC
        LIMIT ?
        """,
        (f["top_n"],),
    )
    if not cagr.empty:
        fig = px.bar(
            cagr.iloc[::-1],
            x="cagr",
            y="company_name",
            orientation="h",
            text=cagr.iloc[::-1]["cagr"].map(lambda v: f"{v:+.1%}"),
            color="cagr",
            color_continuous_scale="RdYlGn",
        )
        fig.update_layout(
            xaxis_tickformat=".0%",
            coloraxis_showscale=False,
            height=520,
            yaxis_title="",
        )
        st.plotly_chart(fig, width="stretch")
        st.dataframe(cagr, width="stretch", hide_index=True)
        st.download_button(
            ":material/download: Download company CAGR (CSV)",
            data=_csv_bytes(cagr),
            file_name="company_cagr.csv",
            mime="text/csv",
        )

    st.divider()

    # ------------------------------------------------------------------
    # Country growth — bubble plot
    # ------------------------------------------------------------------
    st.subheader(":material/trending_up: Country growth — first-half vs second-half")
    cgr = query(
        """
        SELECT country,
               first_half_patents  AS first_half,
               second_half_patents AS second_half,
               total_patents
        FROM mv_country_halfsplit
        WHERE total_patents >= 30
        ORDER BY total_patents DESC
        """
    )
    if not cgr.empty:
        cgr["growth"] = (cgr["second_half"] - cgr["first_half"]) / cgr["first_half"].replace(
            0, pd.NA
        )
        cgr["country_name"] = cgr["country"].map(lambda c: COUNTRY_LOOKUP.get(c, (c, c))[0])
        fig = px.scatter(
            cgr,
            x="first_half",
            y="second_half",
            size="total_patents",
            color="growth",
            hover_name="country_name",
            text="country",
            color_continuous_scale="RdYlGn",
            labels={
                "first_half": "First half (patents)",
                "second_half": "Second half (patents)",
                "growth": "Growth",
            },
        )
        fig.update_traces(textposition="top center")
        fig.update_layout(height=460)
        st.plotly_chart(fig, width="stretch")
        st.dataframe(
            cgr[
                ["country", "country_name", "first_half", "second_half", "total_patents", "growth"]
            ].sort_values("growth", ascending=False),
            width="stretch",
            hide_index=True,
        )

    st.divider()

    # ------------------------------------------------------------------
    # CPC section growth
    # ------------------------------------------------------------------
    st.subheader(":material/auto_graph: CPC section growth (first half → second half)")
    sg = query(
        """
        SELECT section,
               first_half_patents  AS first_half,
               second_half_patents AS second_half
        FROM mv_section_halfsplit
        ORDER BY section
        """
    )
    if not sg.empty:
        sg["growth"] = (sg["second_half"] - sg["first_half"]) / sg["first_half"].replace(0, pd.NA)
        sg["section_label"] = sg["section"].map(CPC_LABELS).fillna(sg["section"])
        h1, h2 = st.columns(2)
        with h1:
            fig = px.bar(
                sg.sort_values("growth"),
                x="growth",
                y="section_label",
                orientation="h",
                text=sg.sort_values("growth")["growth"].map(lambda v: f"{v:+.1%}"),
                color="growth",
                color_continuous_scale="RdYlGn",
            )
            fig.update_layout(
                xaxis_tickformat=".0%",
                coloraxis_showscale=False,
                height=380,
                yaxis_title="",
            )
            st.plotly_chart(fig, width="stretch")
        with h2:
            long = sg.melt(
                id_vars=["section_label"],
                value_vars=["first_half", "second_half"],
                var_name="half",
                value_name="patents",
            )
            fig = px.bar(
                long,
                x="section_label",
                y="patents",
                color="half",
                barmode="group",
                color_discrete_map={"first_half": "#4c72b0", "second_half": "#55a868"},
            )
            fig.update_layout(height=380, xaxis_title="", legend_title="Half")
            st.plotly_chart(fig, width="stretch")

    st.divider()

    # ------------------------------------------------------------------
    # Company × CPC heatmap
    # ------------------------------------------------------------------
    st.subheader(":material/grid_on: Top 15 companies × CPC sections (heatmap)")
    # Top-50 companies pre-aggregated against all sections; pick the top 15 here.
    top_companies = query(
        "SELECT company FROM mv_company_total ORDER BY total_patents DESC LIMIT 15"
    )["company"].tolist()
    if top_companies:
        ph = ", ".join(["?"] * len(top_companies))
        matrix = query(
            f"""
            SELECT company_name, section, patents
            FROM mv_company_section
            WHERE company_name IN ({ph})
            ORDER BY company_name, section
            """,
            tuple(top_companies),
        )
    else:
        matrix = pd.DataFrame()
    if not matrix.empty:
        pivot = matrix.pivot_table(
            index="company_name", columns="section", values="patents", fill_value=0
        )
        fig = px.imshow(
            pivot,
            color_continuous_scale="YlGnBu",
            labels={"x": "CPC section", "y": "Company", "color": "Patents"},
            text_auto=True,
            aspect="auto",
        )
        fig.update_layout(height=520)
        st.plotly_chart(fig, width="stretch")
        st.download_button(
            ":material/download: Download company-section matrix (CSV)",
            data=_csv_bytes(matrix),
            file_name="company_section_matrix.csv",
            mime="text/csv",
        )


# ---------------------------------------------------------------------------
# Tab: Search
# ---------------------------------------------------------------------------
def tab_search(f: dict[str, Any]) -> None:
    yr = f["year_range"]
    st.subheader(":material/search: Patent search")
    needle = st.text_input(
        "Search title or abstract (case-insensitive)",
        placeholder="e.g. carbon-capture, neural network, semiconductor…",
    )
    if not needle:
        st.caption("Type a keyword above to search the warehouse.")
        return
    needle_pat = f"%{needle}%"
    total = query(
        """
        SELECT COUNT(*) AS n FROM patents
        WHERE year BETWEEN ? AND ?
              AND (LOWER(title) LIKE LOWER(?) OR LOWER(coalesce(abstract, '')) LIKE LOWER(?))
        """,
        (yr[0], yr[1], needle_pat, needle_pat),
    )["n"].iloc[0]
    total = int(total)
    if total == 0:
        st.info("No patents match this query in the active year window.")
        return
    st.success(f":material/match_case: **{total:,}** matching patents.")
    offset, page_size = _pagination_controls("search_page", total, page_size=50)
    hits = query(
        """
        SELECT patent_id, title, year, filing_date,
               coalesce(substring(abstract, 1, 200) || '…', '') AS abstract_preview
        FROM patents
        WHERE year BETWEEN ? AND ?
              AND (LOWER(title) LIKE LOWER(?) OR LOWER(coalesce(abstract, '')) LIKE LOWER(?))
        ORDER BY filing_date DESC NULLS LAST
        LIMIT ? OFFSET ?
        """,
        (yr[0], yr[1], needle_pat, needle_pat, page_size, offset),
    )
    st.dataframe(hits, width="stretch", hide_index=True, height=420)
    st.download_button(
        ":material/download: Download this page (CSV)",
        data=_csv_bytes(hits),
        file_name=f"search_{needle[:20]}.csv".replace(" ", "_"),
        mime="text/csv",
        help="Downloads only the rows visible on the current page.",
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _warehouse_stats() -> dict[str, Any]:
    """Headline numbers for the data-freshness banner."""
    bounds = query(
        "SELECT MIN(year) AS lo, MAX(year) AS hi, COUNT(*) AS n FROM patents WHERE year IS NOT NULL"
    )
    counts = query(
        """
        SELECT
            (SELECT COUNT(*) FROM patents)              AS patents,
            (SELECT COUNT(*) FROM inventors)            AS inventors,
            (SELECT COUNT(*) FROM companies)            AS companies,
            (SELECT COUNT(*) FROM patent_relationships) AS rels
        """
    )
    return {
        "year_lo": int(bounds["lo"].iloc[0] or 0),
        "year_hi": int(bounds["hi"].iloc[0] or 0),
        "patents": int(counts["patents"].iloc[0]),
        "inventors": int(counts["inventors"].iloc[0]),
        "companies": int(counts["companies"].iloc[0]),
        "rels": int(counts["rels"].iloc[0]),
    }


TAB_ROUTES: list[tuple[str, str, str, Any]] = [
    ("overview", ":material/dashboard:", "Overview", None),  # filled in main()
    ("trends", ":material/trending_up:", "Trends", None),
    ("geography", ":material/public:", "Geography", None),
    ("cpc", ":material/category:", "CPC", None),
    ("inventors", ":material/science:", "Inventors", None),
    ("companies", ":material/business:", "Companies", None),
    ("advanced", ":material/insights:", "Advanced", None),
    ("search", ":material/search:", "Search", None),
]


def _inject_responsive_css(theme: str) -> None:
    """One-shot CSS for mobile collapse + dark-mode override."""
    dark = """
        :root, [data-testid="stAppViewContainer"] {
            --background-color: #0e1117;
            --secondary-background-color: #1a1f29;
            --text-color: #e6edf3;
        }
        body, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
            background-color: #0e1117 !important;
            color: #e6edf3 !important;
        }
        [data-testid="stSidebar"] { background-color: #1a1f29 !important; }
    """
    st.markdown(
        f"""
        <style>
        /* Mobile: collapse multi-column layouts to a single stack. */
        @media (max-width: 768px) {{
            [data-testid="stColumn"] {{ min-width: 100% !important; flex: 1 1 100% !important; }}
            [data-testid="stHorizontalBlock"] {{ flex-wrap: wrap !important; }}
        }}
        /* Tighter caption contrast for accessibility (was ~#8d96a0). */
        [data-testid="stCaptionContainer"], .stCaption {{ color: #4a5462 !important; }}
        /* Active-tab pill styling for the radio router. */
        div[role="radiogroup"] > label[data-baseweb="radio"] {{
            border: 1px solid rgba(120, 130, 140, 0.25);
            border-radius: 999px;
            padding: 4px 14px;
            margin-right: 6px;
            transition: background-color 0.15s ease;
        }}
        div[role="radiogroup"] > label[data-baseweb="radio"]:hover {{
            background-color: rgba(31, 78, 121, 0.08);
        }}
        {dark if theme == "dark" else ""}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_query_status() -> None:
    """Show the last 10 queries' latencies — visible 'real-time' signal."""
    log = list(st.session_state.get("query_log", []))
    if not log:
        return
    latencies = [ms for _, ms in log]
    p50 = sorted(latencies)[len(latencies) // 2]
    p95 = sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)]
    label = f":material/speed: queries · {len(latencies)} · p50 {p50:.0f} ms · p95 {p95:.0f} ms"
    with st.expander(label, expanded=False):
        df = pd.DataFrame(log[-10:][::-1], columns=["sql", "ms"])
        df["ms"] = df["ms"].round(1)
        st.dataframe(df, width="stretch", hide_index=True, height=240)


def _build_pdf_report(filters: dict[str, Any]) -> bytes:
    """Multi-page PDF report assembled with ``matplotlib.PdfPages``.

    No new dependencies — matplotlib is already in the project's
    requirements. Each chart becomes a vector page, plus a cover page
    with the active filter set and headline KPIs. The result is a
    self-contained, printable, embeddable PDF.
    """
    # matplotlib only — no Plotly here, since Plotly→PDF requires kaleido
    # (a 100 MB Chrome dep we don't want to add for one feature).
    import matplotlib

    matplotlib.use("Agg")  # no GUI backend in headless / Streamlit context
    import io

    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    yr = filters["year_range"]
    yearly = _fast_yearly(filters)
    countries = _fast_country_share(filters).head(15)
    countries["country_name"] = countries["country"].map(lambda c: COUNTRY_LOOKUP.get(c, (c, c))[0])
    sections = query(
        """
        SELECT section, CAST(SUM(patents) AS INT) AS patents
        FROM mv_section_yearly
        WHERE year BETWEEN ? AND ? GROUP BY section ORDER BY patents DESC
        """,
        (yr[0], yr[1]),
    )
    sections["section_label"] = sections["section"].map(CPC_LABELS).fillna(sections["section"])
    top_companies = query(
        """
        SELECT company, CAST(SUM(patents) AS BIGINT) AS patents
        FROM mv_company_yearly
        WHERE year BETWEEN ? AND ?
        GROUP BY company
        ORDER BY patents DESC LIMIT 15
        """,
        (yr[0], yr[1]),
    )

    buf = io.BytesIO()
    primary = "#1f4e79"  # navy — matches the dashboard theme
    accent = "#56B4E9"  # Wong CB-safe sky
    success = "#009E73"  # Wong CB-safe green

    with PdfPages(buf) as pdf:
        # ---- Cover page -------------------------------------------------
        fig, ax = plt.subplots(figsize=(8.5, 11))
        ax.axis("off")
        ax.text(
            0.05,
            0.95,
            "Patent Intelligence Report",
            fontsize=26,
            color=primary,
            fontweight="bold",
            transform=ax.transAxes,
        )
        ax.text(
            0.05,
            0.91,
            "USPTO PatentsView Granted Disambiguated · 1976 – 2025",
            fontsize=11,
            color="#4a5462",
            transform=ax.transAxes,
        )
        meta_lines = [
            f"Generated:  {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}",
            f"Window:     {yr[0]} – {yr[1]}  ({filters['preset']})",
            f"Top-N:      {filters['top_n']}",
            f"Min patents:{filters['min_patents']}",
        ]
        if filters["countries"]:
            meta_lines.append(f"Countries:  {', '.join(filters['countries'])}")
        if filters["sections"]:
            meta_lines.append(f"CPC sect.:  {', '.join(filters['sections'])}")
        if filters.get("classes"):
            meta_lines.append(f"CPC class:  {', '.join(filters['classes'])}")
        ax.text(
            0.05,
            0.84,
            "\n".join(meta_lines),
            fontsize=11,
            color="#1a202c",
            family="monospace",
            verticalalignment="top",
            transform=ax.transAxes,
        )

        # KPI strip
        kpi_y = 0.66
        kpi_data = [
            ("Patents", int(yearly["patents"].sum()) if not yearly.empty else 0),
            ("Countries", len(countries)),
            ("CPC sections", len(sections)),
            ("Top companies", len(top_companies)),
        ]
        for i, (label, value) in enumerate(kpi_data):
            x = 0.05 + i * 0.225
            ax.add_patch(
                plt.Rectangle(
                    (x, kpi_y),
                    0.20,
                    0.07,
                    transform=ax.transAxes,
                    facecolor="#f5f7fa",
                    edgecolor="#cbd5e0",
                )
            )
            ax.text(
                x + 0.01, kpi_y + 0.045, label, fontsize=9, color="#4a5462", transform=ax.transAxes
            )
            ax.text(
                x + 0.01,
                kpi_y + 0.012,
                f"{value:,}",
                fontsize=15,
                fontweight="bold",
                color=primary,
                transform=ax.transAxes,
            )

        ax.text(
            0.05,
            0.10,
            "Generated from the DuckDB warehouse via the Patent Intelligence "
            "dashboard.\nFilters above apply to every page in this report.",
            fontsize=9,
            color="#6b7280",
            transform=ax.transAxes,
        )
        ax.text(
            0.05,
            0.04,
            "Developer: Mpairwe Lauben  ·  mpairwelauben75@gmail.com  ·  "
            "github.com/mpairwe7/patent-pipeline",
            fontsize=8,
            color="#4a5462",
            transform=ax.transAxes,
        )
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # ---- Yearly volume ---------------------------------------------
        if not yearly.empty:
            fig, ax = plt.subplots(figsize=(11, 6))
            ax.bar(yearly["year"], yearly["patents"], color=primary)
            ax.set_title(
                f"Patents filed per year · {yr[0]} – {yr[1]}",
                fontsize=14,
                color=primary,
                fontweight="bold",
            )
            ax.set_xlabel("Year")
            ax.set_ylabel("Patents")
            ax.grid(alpha=0.3, axis="y")
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        # ---- Top countries ---------------------------------------------
        if not countries.empty:
            fig, ax = plt.subplots(figsize=(11, 6))
            top = countries.iloc[::-1]
            ax.barh(top["country_name"], top["patents"], color=accent)
            ax.set_title(
                "Top 15 countries by inventor patent count",
                fontsize=14,
                color=primary,
                fontweight="bold",
            )
            ax.set_xlabel("Patents")
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        # ---- CPC sections ----------------------------------------------
        if not sections.empty:
            fig, ax = plt.subplots(figsize=(11, 6))
            ax.bar(sections["section_label"], sections["patents"], color=success)
            ax.set_title("Patents by CPC section", fontsize=14, color=primary, fontweight="bold")
            ax.set_ylabel("Patents")
            ax.tick_params(axis="x", rotation=20)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        # ---- Top companies ---------------------------------------------
        if not top_companies.empty:
            fig, ax = plt.subplots(figsize=(11, 6))
            top = top_companies.iloc[::-1]
            # Truncate over-long company names so they fit the y axis.
            top = top.assign(
                company_short=top["company"].str.slice(0, 40)
                + top["company"].str.len().gt(40).map({True: "…", False: ""})
            )
            ax.barh(top["company_short"], top["patents"], color=primary)
            ax.set_title(
                "Top 15 companies by total patents", fontsize=14, color=primary, fontweight="bold"
            )
            ax.set_xlabel("Patents")
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        # ---- PDF metadata ---------------------------------------------
        info = pdf.infodict()
        info["Title"] = "Patent Intelligence Report"
        info["Author"] = "Mpairwe Lauben <mpairwelauben75@gmail.com>"
        info["Subject"] = f"USPTO PatentsView · {yr[0]}-{yr[1]} · preset {filters['preset']}"
        info["Keywords"] = "patents, USPTO, PatentsView, DuckDB, analytics"
        info["CreationDate"] = datetime.now(UTC)
        info["ModDate"] = datetime.now(UTC)

    return buf.getvalue()


def main() -> None:
    st.set_page_config(
        page_title="Patent Intelligence Dashboard",
        page_icon=":material/science:",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    _hydrate_state_from_url()

    _, settings = _connect()
    db_path = settings.paths.warehouse_db
    db_mtime = datetime.fromtimestamp(db_path.stat().st_mtime, tz=UTC) if db_path.exists() else None

    # Theme toggle (sidebar) — pull current theme out of session_state then
    # inject CSS before any other content renders.
    theme = st.session_state.get("theme", "light")
    _inject_responsive_css(theme)

    # ---- Header banner ----------------------------------------------------
    title_col, action_col = st.columns([5, 2])
    with title_col:
        st.title(":material/public: Global Patent Intelligence Dashboard")
        st.caption(
            "USPTO PatentsView 1976-2025 · DuckDB warehouse · materialised "
            "summary tables · click-to-cross-filter · share-via-URL"
        )
    with action_col:
        b1, b2 = st.columns(2)
        with b1:
            if st.button(
                ":material/refresh: Refresh",
                help="Clear the in-process query cache and re-read the warehouse.",
                width="stretch",
            ):
                _cached_query.clear()
                st.rerun()
        with b2:
            new_theme = "dark" if theme == "light" else "light"
            if st.button(
                f":material/{'dark_mode' if theme == 'light' else 'light_mode'}: {new_theme.title()}",
                help="Toggle light/dark theme.",
                width="stretch",
            ):
                st.session_state["theme"] = new_theme
                _persist_to_url(theme=new_theme)
                st.rerun()

    stats = _warehouse_stats()
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric(
        "Year span",
        f"{stats['year_lo']} – {stats['year_hi']}",
        help="Earliest and latest filing year present in the warehouse.",
    )
    s2.metric("Patents", f"{stats['patents']:,}", help="Distinct granted patents.")
    s3.metric("Inventors", f"{stats['inventors']:,}", help="Distinct disambiguated inventors.")
    s4.metric(
        "Companies",
        f"{stats['companies']:,}",
        help="Distinct assignees (organisations + sole-inventor names).",
    )
    s5.metric(
        "Relationships",
        f"{stats['rels']:,}",
        help="Patent ↔ inventor links (with optional company attribution).",
    )
    if db_mtime is not None:
        st.caption(
            f":material/database: Warehouse updated "
            f"**{db_mtime.strftime('%Y-%m-%d %H:%M UTC')}** "
            f"· {db_path.stat().st_size / 1e6:,.1f} MB on disk"
        )
    st.divider()

    # ---- Sidebar + filters -----------------------------------------------
    filters = sidebar_filters()

    # ---- Cross-filter chip row -------------------------------------------
    xfilter = st.session_state.get("xfilter_country")
    if xfilter:
        cf1, cf2 = st.columns([6, 1])
        with cf1:
            st.info(
                f":material/filter_alt: Cross-filter active — country "
                f"**{COUNTRY_LOOKUP.get(xfilter, (xfilter, xfilter))[0]}** "
                f"(click clear to remove).",
                icon=":material/filter_alt:",
            )
        with cf2:
            if st.button(":material/close: Clear", width="stretch"):
                st.session_state.pop("xfilter_country", None)
                st.rerun()

    # ---- Lazy tab router (radio + fragments) ------------------------------
    options = [(slug, f"{icon} {label}") for slug, icon, label, _ in TAB_ROUTES]
    labels = [lbl for _, lbl in options]
    slugs = [slug for slug, _ in options]
    default_slug = st.session_state.get("active_tab", "overview")
    if default_slug not in slugs:
        default_slug = "overview"
    selected_label = st.radio(
        "View",
        options=labels,
        index=slugs.index(default_slug),
        horizontal=True,
        label_visibility="collapsed",
        key="_tab_radio",
    )
    active_slug = slugs[labels.index(selected_label)]
    st.session_state["active_tab"] = active_slug
    _persist_to_url(active_tab=active_slug)

    # Dispatch — only the active tab's body runs.
    {
        "overview": tab_overview,
        "trends": tab_trends,
        "geography": tab_geography,
        "cpc": tab_cpc,
        "inventors": tab_inventors,
        "companies": tab_companies,
        "advanced": tab_advanced,
        "search": tab_search,
    }[active_slug](filters)

    # ---- Footer -----------------------------------------------------------
    st.divider()
    f1, f2 = st.columns([3, 2])
    with f1:
        st.caption(
            ":material/code: Built with **uv** · **DuckDB** · **Streamlit** · **Plotly** · "
            "**pandas** · **PyArrow**. "
            "Source: [PatentsView Granted Patent Disambiguated]"
            "(https://data.uspto.gov/bulkdata/datasets/pvgpatdis)."
        )
        st.caption(
            ":material/person: Developer: **Mpairwe Lauben** · "
            "[mpairwelauben75@gmail.com](mailto:mpairwelauben75@gmail.com) · "
            "[github.com/mpairwe7/patent-pipeline]"
            "(https://github.com/mpairwe7/patent-pipeline)"
        )
    with f2:
        report_bytes = _build_pdf_report(filters)
        st.download_button(
            ":material/picture_as_pdf: Download PDF report",
            data=report_bytes,
            file_name=f"patent_report_{filters['year_range'][0]}-{filters['year_range'][1]}.pdf",
            mime="application/pdf",
            help="Multi-page PDF assembled with matplotlib — vector charts, embeddable, printable.",
            width="stretch",
        )

    _render_query_status()

    st.sidebar.divider()
    st.sidebar.caption(f"Warehouse:\n`{settings.paths.warehouse_db}`")


if __name__ == "__main__":
    main()
