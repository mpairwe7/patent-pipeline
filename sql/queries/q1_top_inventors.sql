-- Q1: Top Inventors — who has the most (distinct) patents?
-- ---------------------------------------------------------
SELECT
    i.inventor_id,
    i.name           AS inventor_name,
    i.country        AS country,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM inventors AS i
JOIN patent_relationships AS r ON r.inventor_id = i.inventor_id
GROUP BY i.inventor_id, i.name, i.country
ORDER BY patent_count DESC, inventor_name ASC
LIMIT 20;
