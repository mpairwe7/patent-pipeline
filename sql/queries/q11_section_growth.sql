-- Q11 (advanced): CPC section growth — first-half vs second-half of the span.
-- ---------------------------------------------------------------------------
-- For each top-level CPC section, compute volume in the first vs second
-- half of the time series and the growth rate. Helps answer: which fields
-- of technology grew fastest across the decade?
WITH bounds AS (
    SELECT (MIN(year) + MAX(year)) / 2 AS y_mid
    FROM patents WHERE year IS NOT NULL
),
labelled AS (
    SELECT
        pc.section,
        p.patent_id,
        CASE WHEN p.year <= b.y_mid THEN 'first_half' ELSE 'second_half' END AS half
    FROM patent_cpc AS pc
    JOIN patents     AS p ON p.patent_id = pc.patent_id
    CROSS JOIN bounds AS b
    WHERE p.year IS NOT NULL AND pc.section IS NOT NULL
),
half_counts AS (
    SELECT
        section,
        COUNT(DISTINCT CASE WHEN half = 'first_half'  THEN patent_id END) AS first_half_patents,
        COUNT(DISTINCT CASE WHEN half = 'second_half' THEN patent_id END) AS second_half_patents
    FROM labelled
    GROUP BY section
)
SELECT
    section,
    first_half_patents,
    second_half_patents,
    (second_half_patents - first_half_patents)  AS delta,
    CASE
        WHEN first_half_patents = 0 THEN NULL
        ELSE ROUND(
            (second_half_patents - first_half_patents)::DOUBLE
            / first_half_patents, 4)
    END AS growth_pct,
    (first_half_patents + second_half_patents)  AS total_patents
FROM half_counts
ORDER BY growth_pct DESC NULLS LAST, total_patents DESC;
