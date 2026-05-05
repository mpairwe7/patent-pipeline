-- Q10 (advanced): Country growth rates — first-half vs second-half of the span.
-- -----------------------------------------------------------------------------
-- Splits the data into two halves (≤ midpoint vs > midpoint) and compares
-- patent volume to surface which countries grew fastest over the decade.
WITH bounds AS (
    SELECT
        MIN(year) AS y_min,
        MAX(year) AS y_max,
        (MIN(year) + MAX(year)) / 2 AS y_mid
    FROM patents WHERE year IS NOT NULL
),
labelled AS (
    SELECT
        i.country,
        p.patent_id,
        CASE WHEN p.year <= b.y_mid THEN 'first_half' ELSE 'second_half' END AS half
    FROM patents AS p
    JOIN patent_relationships AS r ON r.patent_id = p.patent_id
    JOIN inventors  AS i           ON i.inventor_id = r.inventor_id
    CROSS JOIN bounds AS b
    WHERE p.year IS NOT NULL
      AND i.country IS NOT NULL AND i.country <> ''
),
half_counts AS (
    SELECT
        country,
        COUNT(DISTINCT CASE WHEN half = 'first_half'  THEN patent_id END) AS first_half_patents,
        COUNT(DISTINCT CASE WHEN half = 'second_half' THEN patent_id END) AS second_half_patents
    FROM labelled
    GROUP BY country
)
SELECT
    country,
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
WHERE (first_half_patents + second_half_patents) >= 30
ORDER BY growth_pct DESC NULLS LAST, total_patents DESC
LIMIT 25;
