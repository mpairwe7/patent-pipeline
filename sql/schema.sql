-- ============================================================================
-- Global Patent Intelligence — Warehouse Schema (DuckDB / standard SQL)
-- ============================================================================
-- Re-runnable: drops and recreates every object so `load` is idempotent.
-- Required tables per assignment: patents, inventors, companies, relationships.
-- Extra: patent_cpc for category analysis (extra credit).
-- ============================================================================

DROP TABLE IF EXISTS patent_cpc;
DROP TABLE IF EXISTS patent_relationships;
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS inventors;
DROP TABLE IF EXISTS patents;

CREATE TABLE patents (
    patent_id    VARCHAR PRIMARY KEY,
    title        VARCHAR,
    abstract     VARCHAR,
    filing_date  DATE,
    year         INTEGER
);

CREATE TABLE inventors (
    inventor_id  VARCHAR PRIMARY KEY,
    name         VARCHAR,
    country      VARCHAR
);

CREATE TABLE companies (
    company_id   VARCHAR PRIMARY KEY,
    name         VARCHAR
);

-- Relationships fact table.  patent_id is always present; company_id may be
-- NULL for patents without an assignee (sole-inventor patents).
CREATE TABLE patent_relationships (
    patent_id    VARCHAR NOT NULL,
    inventor_id  VARCHAR NOT NULL,
    company_id   VARCHAR
);

CREATE TABLE patent_cpc (
    patent_id    VARCHAR NOT NULL,
    section      VARCHAR,
    cpc_class    VARCHAR,
    cpc_subclass VARCHAR
);

-- Indexes for analytical joins.
CREATE INDEX idx_rel_patent    ON patent_relationships(patent_id);
CREATE INDEX idx_rel_inventor  ON patent_relationships(inventor_id);
CREATE INDEX idx_rel_company   ON patent_relationships(company_id);
CREATE INDEX idx_patents_year  ON patents(year);
CREATE INDEX idx_inventors_ctry ON inventors(country);
CREATE INDEX idx_cpc_section   ON patent_cpc(section);
