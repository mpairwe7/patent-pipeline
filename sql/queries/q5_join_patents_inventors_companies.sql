-- Q5: JOIN — combine patents with inventors and companies.
-- ---------------------------------------------------------
-- Produces a wide row per (patent, inventor, company) relationship so that
-- downstream consumers can pivot or aggregate as they wish.
SELECT
    p.patent_id,
    p.title              AS patent_title,
    p.filing_date,
    p.year,
    i.name               AS inventor_name,
    i.country            AS inventor_country,
    c.name               AS company_name
FROM patents AS p
JOIN patent_relationships AS r ON r.patent_id = p.patent_id
JOIN inventors AS i            ON i.inventor_id = r.inventor_id
LEFT JOIN companies AS c       ON c.company_id  = r.company_id
ORDER BY p.year DESC, p.patent_id, inventor_name
LIMIT 500;
