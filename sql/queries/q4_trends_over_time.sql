-- Q4: Trends Over Time — how many patents are filed each year?
-- --------------------------------------------------------------
SELECT
    year,
    COUNT(*) AS patent_count
FROM patents
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year ASC;
