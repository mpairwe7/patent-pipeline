-- Q2: Top Companies — which assignees own the most distinct patents?
-- -------------------------------------------------------------------
SELECT
    c.company_id,
    c.name           AS company_name,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM companies AS c
JOIN patent_relationships AS r ON r.company_id = c.company_id
WHERE r.company_id IS NOT NULL
GROUP BY c.company_id, c.name
ORDER BY patent_count DESC, company_name ASC
LIMIT 20;
