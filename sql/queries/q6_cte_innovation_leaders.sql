-- Q6: CTE (WITH …) — innovation leaders by year-over-year (YoY) growth.
-- ----------------------------------------------------------------------
-- Step 1:  count distinct patents per company per year.
-- Step 2:  use LAG to fetch the previous year's count.
-- Step 3:  compute YoY absolute and percentage growth.
-- Step 4:  pick the latest "complete" year (the most recent year where the
--          warehouse-wide volume is at least 60 % of the peak year — this
--          drops the tail of partial trailing years), then rank companies
--          with ≥ 2 patents in that year by YoY %.
WITH company_year_counts AS (
    SELECT
        c.company_id,
        c.name      AS company_name,
        p.year      AS year,
        COUNT(DISTINCT p.patent_id) AS patents_filed
    FROM companies AS c
    JOIN patent_relationships AS r ON r.company_id = c.company_id
    JOIN patents AS p              ON p.patent_id  = r.patent_id
    WHERE p.year IS NOT NULL
    GROUP BY c.company_id, c.name, p.year
),
with_prev AS (
    SELECT
        company_id,
        company_name,
        year,
        patents_filed,
        LAG(patents_filed) OVER (PARTITION BY company_id ORDER BY year) AS prev_year_patents
    FROM company_year_counts
),
yoy AS (
    SELECT
        company_id,
        company_name,
        year,
        patents_filed,
        prev_year_patents,
        patents_filed - COALESCE(prev_year_patents, 0) AS yoy_delta,
        CASE
            WHEN prev_year_patents IS NULL OR prev_year_patents = 0 THEN NULL
            ELSE ROUND((patents_filed - prev_year_patents) / prev_year_patents::DOUBLE, 4)
        END AS yoy_growth_pct
    FROM with_prev
),
yearly_totals AS (
    SELECT year, COUNT(DISTINCT patent_id) AS n
    FROM patents WHERE year IS NOT NULL
    GROUP BY year
),
peak_year_total AS (
    SELECT MAX(n) AS peak FROM yearly_totals
),
ref_year AS (
    -- The most-recent year whose volume ≥ 60 % of the peak — avoids the
    -- partial-year tail that 2025 typically presents.
    SELECT MAX(yt.year) AS y
    FROM yearly_totals yt CROSS JOIN peak_year_total pt
    WHERE yt.n >= pt.peak * 0.6
)
SELECT
    y.company_name,
    y.year,
    y.patents_filed,
    y.prev_year_patents,
    y.yoy_delta,
    y.yoy_growth_pct
FROM yoy AS y
CROSS JOIN ref_year AS ry
WHERE y.year = ry.y
  AND y.patents_filed >= 2
ORDER BY y.yoy_growth_pct DESC NULLS LAST, y.patents_filed DESC
LIMIT 20;
