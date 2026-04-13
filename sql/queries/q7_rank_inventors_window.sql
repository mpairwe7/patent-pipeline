-- Q7: Ranking via window functions — top-3 inventors per country.
-- -----------------------------------------------------------------
-- Two window functions:
--   * RANK()       — for dense ordering with ties shared
--   * ROW_NUMBER() — for a unique ordinal within each country
WITH inventor_counts AS (
    SELECT
        i.country,
        i.inventor_id,
        i.name                         AS inventor_name,
        COUNT(DISTINCT r.patent_id)    AS patent_count
    FROM inventors AS i
    JOIN patent_relationships AS r ON r.inventor_id = i.inventor_id
    WHERE i.country IS NOT NULL AND i.country <> ''
    GROUP BY i.country, i.inventor_id, i.name
),
ranked AS (
    SELECT
        country,
        inventor_id,
        inventor_name,
        patent_count,
        RANK()       OVER (PARTITION BY country ORDER BY patent_count DESC) AS country_rank,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY patent_count DESC, inventor_name) AS country_row
    FROM inventor_counts
)
SELECT
    country,
    country_rank,
    inventor_name,
    patent_count
FROM ranked
WHERE country_row <= 3
ORDER BY country, country_rank, inventor_name;
