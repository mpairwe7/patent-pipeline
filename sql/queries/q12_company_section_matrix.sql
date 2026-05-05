-- Q12 (advanced): Company × CPC-section matrix — heatmap-ready.
-- -------------------------------------------------------------
-- Returns one row per (top-15 company, section) with a distinct patent
-- count, suitable for rendering as a heatmap.
WITH top_companies AS (
    SELECT c.company_id, c.name AS company_name,
           COUNT(DISTINCT r.patent_id) AS total_patents
    FROM companies AS c
    JOIN patent_relationships AS r ON r.company_id = c.company_id
    GROUP BY c.company_id, c.name
    ORDER BY total_patents DESC
    LIMIT 15
)
SELECT
    tc.company_name,
    pc.section,
    COUNT(DISTINCT pc.patent_id) AS patents
FROM top_companies AS tc
JOIN patent_relationships AS r ON r.company_id = tc.company_id
JOIN patent_cpc           AS pc ON pc.patent_id = r.patent_id
WHERE pc.section IS NOT NULL
GROUP BY tc.company_name, pc.section
ORDER BY tc.company_name, pc.section;
