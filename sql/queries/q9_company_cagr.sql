-- Q9 (advanced): Company CAGR — compound annual growth rate over the span.
-- -----------------------------------------------------------------------
-- For every company with ≥ 20 patents and a multi-year footprint, compute:
--   - first_year, first_year_count
--   - last_year, last_year_count
--   - span_years     = last_year - first_year
--   - cagr           = (last/first)^(1/span_years) - 1
-- Results are ordered by CAGR DESC.
WITH yearly AS (
    SELECT
        c.company_id,
        c.name              AS company_name,
        p.year              AS year,
        COUNT(DISTINCT p.patent_id) AS patents
    FROM companies AS c
    JOIN patent_relationships AS r ON r.company_id = c.company_id
    JOIN patents AS p              ON p.patent_id  = r.patent_id
    WHERE p.year IS NOT NULL
    GROUP BY c.company_id, c.name, p.year
),
endpoints AS (
    SELECT
        company_id,
        company_name,
        MIN(year) AS first_year,
        MAX(year) AS last_year,
        SUM(patents) AS total_patents
    FROM yearly
    GROUP BY company_id, company_name
),
joined AS (
    SELECT
        e.company_id,
        e.company_name,
        e.first_year,
        e.last_year,
        e.total_patents,
        f.patents AS first_year_patents,
        l.patents AS last_year_patents,
        (e.last_year - e.first_year) AS span_years
    FROM endpoints AS e
    JOIN yearly AS f ON f.company_id = e.company_id AND f.year = e.first_year
    JOIN yearly AS l ON l.company_id = e.company_id AND l.year = e.last_year
)
SELECT
    company_name,
    first_year,
    last_year,
    span_years,
    first_year_patents,
    last_year_patents,
    total_patents,
    ROUND(
        POWER(
            CAST(last_year_patents AS DOUBLE) / CAST(first_year_patents AS DOUBLE),
            1.0 / NULLIF(span_years, 0)
        ) - 1,
        4
    ) AS cagr
FROM joined
WHERE total_patents >= 20
  AND span_years   >= 3
  AND first_year_patents > 0
ORDER BY cagr DESC NULLS LAST, total_patents DESC
LIMIT 25;
