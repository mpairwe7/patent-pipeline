"""Streamlit dashboard — interactive view over the DuckDB warehouse.

Start with:  ``uv run patent-pipeline dashboard``
Browse:      http://localhost:8501
"""

from __future__ import annotations

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from patent_pipeline.config import load_settings


@st.cache_resource
def _connect():
    settings = load_settings()
    return duckdb.connect(settings.paths.warehouse_db.as_posix(), read_only=True), settings


@st.cache_data(ttl=600)
def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    conn, _ = _connect()
    if params:
        return conn.execute(sql, params).fetch_df()
    return conn.execute(sql).fetch_df()


def main() -> None:
    st.set_page_config(
        page_title="Patent Intelligence Dashboard",
        page_icon="🧪",
        layout="wide",
    )

    _, settings = _connect()

    st.title("🌐 Global Patent Intelligence Dashboard")
    st.caption(
        "USPTO PatentsView disambiguated data • DuckDB-backed • built for the "
        "*Cloud Computing — Data Pipeline Mini Project*"
    )

    # --- Sidebar filters -----------------------------------------------------
    year_bounds = query(
        "SELECT MIN(year) AS min_y, MAX(year) AS max_y FROM patents WHERE year IS NOT NULL"
    )
    min_year = int(year_bounds["min_y"].iloc[0] or 2020)
    max_year = int(year_bounds["max_y"].iloc[0] or 2025)

    with st.sidebar:
        st.header("Filters")
        year_range = st.slider(
            "Year range",
            min_value=min_year,
            max_value=max_year,
            value=(min_year, max_year),
        )
        top_n = st.slider("Top N", min_value=5, max_value=50, value=15)
        countries_df = query(
            "SELECT DISTINCT country FROM inventors "
            "WHERE country IS NOT NULL AND country <> '' ORDER BY country"
        )
        country_choice = st.multiselect(
            "Country filter (inventors)",
            options=countries_df["country"].tolist(),
        )

    country_clause = ""
    params: tuple = ()
    if country_choice:
        placeholders = ", ".join(["?"] * len(country_choice))
        country_clause = f" AND i.country IN ({placeholders})"
        params = tuple(country_choice)

    # --- KPIs ----------------------------------------------------------------
    totals_sql = f"""
        SELECT
            COUNT(DISTINCT p.patent_id)  AS patents,
            COUNT(DISTINCT i.inventor_id) AS inventors,
            COUNT(DISTINCT c.company_id) AS companies
        FROM patents p
        LEFT JOIN patent_relationships r ON r.patent_id = p.patent_id
        LEFT JOIN inventors i ON i.inventor_id = r.inventor_id
        LEFT JOIN companies c ON c.company_id = r.company_id
        WHERE p.year BETWEEN ? AND ?
        {country_clause}
    """
    kpi = query(totals_sql, (year_range[0], year_range[1], *params))

    c1, c2, c3 = st.columns(3)
    c1.metric("Patents", f"{int(kpi['patents'].iloc[0]):,}")
    c2.metric("Inventors", f"{int(kpi['inventors'].iloc[0]):,}")
    c3.metric("Companies", f"{int(kpi['companies'].iloc[0]):,}")

    st.divider()

    # --- Yearly trend --------------------------------------------------------
    trend = query(
        f"""
        SELECT year, COUNT(DISTINCT p.patent_id) AS patents
        FROM patents p
        LEFT JOIN patent_relationships r ON r.patent_id = p.patent_id
        LEFT JOIN inventors i            ON i.inventor_id = r.inventor_id
        WHERE year BETWEEN ? AND ? {country_clause}
        GROUP BY year ORDER BY year
        """,
        (year_range[0], year_range[1], *params),
    )
    st.subheader("📈 Patents filed per year")
    if not trend.empty:
        st.plotly_chart(
            px.area(trend, x="year", y="patents", markers=True),
            use_container_width=True,
        )

    # --- Top inventors / companies side by side ------------------------------
    left, right = st.columns(2)

    top_inventors = query(
        f"""
        SELECT i.name AS inventor, i.country, COUNT(DISTINCT p.patent_id) AS patents
        FROM inventors i
        JOIN patent_relationships r ON r.inventor_id = i.inventor_id
        JOIN patents p              ON p.patent_id  = r.patent_id
        WHERE p.year BETWEEN ? AND ? {country_clause}
        GROUP BY i.name, i.country
        ORDER BY patents DESC
        LIMIT ?
        """,
        (year_range[0], year_range[1], *params, top_n),
    )
    with left:
        st.subheader(f"🧑‍🔬 Top {top_n} inventors")
        st.dataframe(top_inventors, use_container_width=True, hide_index=True)

    top_companies = query(
        f"""
        SELECT c.name AS company, COUNT(DISTINCT p.patent_id) AS patents
        FROM companies c
        JOIN patent_relationships r ON r.company_id = c.company_id
        JOIN patents p              ON p.patent_id  = r.patent_id
        JOIN inventors i            ON i.inventor_id = r.inventor_id
        WHERE p.year BETWEEN ? AND ? {country_clause}
        GROUP BY c.name
        ORDER BY patents DESC
        LIMIT ?
        """,
        (year_range[0], year_range[1], *params, top_n),
    )
    with right:
        st.subheader(f"🏢 Top {top_n} companies")
        if not top_companies.empty:
            st.plotly_chart(
                px.bar(top_companies, x="patents", y="company", orientation="h").update_layout(
                    yaxis={"categoryorder": "total ascending"}
                ),
                use_container_width=True,
            )

    # --- Country share -------------------------------------------------------
    countries = query(
        f"""
        SELECT i.country AS country, COUNT(DISTINCT p.patent_id) AS patents
        FROM inventors i
        JOIN patent_relationships r ON r.inventor_id = i.inventor_id
        JOIN patents p              ON p.patent_id  = r.patent_id
        WHERE p.year BETWEEN ? AND ? AND i.country IS NOT NULL AND i.country <> ''
        {country_clause}
        GROUP BY i.country
        ORDER BY patents DESC
        """,
        (year_range[0], year_range[1], *params),
    )
    st.subheader("🌍 Country share")
    if not countries.empty:
        pie = px.pie(countries.head(12), names="country", values="patents", hole=0.35)
        st.plotly_chart(pie, use_container_width=True)

    # --- CPC categories ------------------------------------------------------
    cpc = query(
        """
        SELECT section, COUNT(DISTINCT pc.patent_id) AS patents
        FROM patent_cpc pc
        JOIN patents p ON p.patent_id = pc.patent_id
        WHERE p.year BETWEEN ? AND ?
        GROUP BY section ORDER BY patents DESC
        """,
        (year_range[0], year_range[1]),
    )
    if not cpc.empty:
        st.subheader("🧩 CPC section breakdown")
        st.plotly_chart(px.bar(cpc, x="section", y="patents"), use_container_width=True)

    # --- Patent search -------------------------------------------------------
    st.divider()
    st.subheader("🔎 Patent search")
    needle = st.text_input("Search title or abstract (case-insensitive)")
    if needle:
        hits = query(
            """
            SELECT patent_id, title, year, filing_date
            FROM patents
            WHERE LOWER(title) LIKE LOWER(?) OR LOWER(abstract) LIKE LOWER(?)
            ORDER BY filing_date DESC
            LIMIT 100
            """,
            (f"%{needle}%", f"%{needle}%"),
        )
        st.dataframe(hits, use_container_width=True, hide_index=True)

    st.caption(f"Warehouse: `{settings.paths.warehouse_db}`")


if __name__ == "__main__":
    main()
