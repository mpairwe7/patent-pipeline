-- Q3: Top Countries — which countries produce the most patents, with share?
-- --------------------------------------------------------------------------
-- Country is attributed via the inventor's country.  A patent with inventors
-- from multiple countries contributes to each distinct country once.
WITH country_patents AS (
    SELECT
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM inventors AS i
    JOIN patent_relationships AS r ON r.inventor_id = i.inventor_id
    WHERE i.country IS NOT NULL AND i.country <> ''
    GROUP BY i.country
),
total AS (
    SELECT SUM(patent_count)::DOUBLE AS total_count FROM country_patents
)
SELECT
    cp.country,
    cp.patent_count,
    ROUND(cp.patent_count / t.total_count, 4) AS share
FROM country_patents AS cp
CROSS JOIN total AS t
ORDER BY cp.patent_count DESC
LIMIT 15;
