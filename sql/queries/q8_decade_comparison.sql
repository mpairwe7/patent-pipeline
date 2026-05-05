-- Q8 (advanced): Decade comparison — patents per 5-year bucket.
-- ----------------------------------------------------------------------
-- Buckets the 2010-2025 span into half-decades and reports volume,
-- distinct inventors, distinct companies, and share of the total. Useful
-- for showing how the pipeline now spans the previous decade as the
-- assignment requested.
WITH bucketed AS (
    SELECT
        p.patent_id,
        p.year,
        CASE
            WHEN p.year BETWEEN 2010 AND 2014 THEN '2010-2014'
            WHEN p.year BETWEEN 2015 AND 2019 THEN '2015-2019'
            WHEN p.year BETWEEN 2020 AND 2025 THEN '2020-2025'
            ELSE 'other'
        END AS bucket
    FROM patents AS p
    WHERE p.year IS NOT NULL
)
SELECT
    b.bucket                                                    AS period,
    COUNT(DISTINCT b.patent_id)                                 AS patent_count,
    COUNT(DISTINCT r.inventor_id)                               AS inventor_count,
    COUNT(DISTINCT r.company_id)                                AS company_count,
    ROUND(
        COUNT(DISTINCT b.patent_id)
        / SUM(COUNT(DISTINCT b.patent_id)) OVER (), 4
    )                                                           AS share
FROM bucketed AS b
LEFT JOIN patent_relationships AS r ON r.patent_id = b.patent_id
WHERE b.bucket <> 'other'
GROUP BY b.bucket
ORDER BY b.bucket;
