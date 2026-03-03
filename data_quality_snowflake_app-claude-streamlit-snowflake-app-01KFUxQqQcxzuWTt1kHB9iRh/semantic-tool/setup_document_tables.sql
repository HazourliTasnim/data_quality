-- =============================================================================
-- Document Quality Tables Setup (v1.1)
-- Creates tables for unstructured data management
-- =============================================================================

USE DATABASE DATA_QUALITY_AI;
USE SCHEMA DATA_QUALITY_AI.CORE;

-- =============================================================================
-- Documents table - stores uploaded documents and their embeddings
-- =============================================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_AI.CORE.DOCUMENTS (
    id VARCHAR(100) PRIMARY KEY,
    filename VARCHAR(500) NOT NULL,
    text_content TEXT,
    embedding VARIANT,  -- Vector embedding as JSON array
    text_hash VARCHAR(64) NOT NULL,  -- SHA256 hash for exact duplicate detection
    file_type VARCHAR(10),  -- pdf, docx, txt, etc.
    file_size INT,  -- Size in bytes
    upload_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    uploaded_by VARCHAR(255) DEFAULT CURRENT_USER(),
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE, ARCHIVED, DUPLICATE
    duplicate_of VARCHAR(100),  -- If duplicate, references original doc ID
    metadata VARIANT,  -- Additional metadata as JSON
    COMMENT VARCHAR(1000)
);

-- Index on text_hash for fast exact duplicate detection
CREATE INDEX IF NOT EXISTS idx_documents_text_hash ON DATA_QUALITY_AI.CORE.DOCUMENTS(text_hash);

-- Index on status
CREATE INDEX IF NOT EXISTS idx_documents_status ON DATA_QUALITY_AI.CORE.DOCUMENTS(status);

-- =============================================================================
-- Document-Table Links - associates documents with database tables
-- =============================================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_AI.CORE.DOCUMENT_TABLE_LINKS (
    id VARCHAR(100) PRIMARY KEY DEFAULT UUID_STRING(),
    document_id VARCHAR(100) NOT NULL,
    database_name VARCHAR(255),
    schema_name VARCHAR(255),
    table_name VARCHAR(255),
    confidence_score FLOAT,  -- 0-1 confidence of the association
    link_type VARCHAR(50),  -- MENTIONED, DOCUMENTATION, POLICY, etc.
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(255) DEFAULT CURRENT_USER(),
    FOREIGN KEY (document_id) REFERENCES DATA_QUALITY_AI.CORE.DOCUMENTS(id)
);

-- =============================================================================
-- Document Similarity Matrix - pre-computed similarities
-- =============================================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_AI.CORE.DOCUMENT_SIMILARITIES (
    doc_id_1 VARCHAR(100),
    doc_id_2 VARCHAR(100),
    similarity_score FLOAT,  -- 0-1 similarity score
    computed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (doc_id_1, doc_id_2),
    FOREIGN KEY (doc_id_1) REFERENCES DATA_QUALITY_AI.CORE.DOCUMENTS(id),
    FOREIGN KEY (doc_id_2) REFERENCES DATA_QUALITY_AI.CORE.DOCUMENTS(id)
);

-- =============================================================================
-- Document Versions - track version history
-- =============================================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_AI.CORE.DOCUMENT_VERSIONS (
    id VARCHAR(100) PRIMARY KEY DEFAULT UUID_STRING(),
    document_id VARCHAR(100) NOT NULL,
    version_number INT NOT NULL,
    is_canonical BOOLEAN DEFAULT FALSE,  -- Is this the "official" version?
    text_content TEXT,
    changes_from_previous VARIANT,  -- JSON describing changes
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(255) DEFAULT CURRENT_USER(),
    FOREIGN KEY (document_id) REFERENCES DATA_QUALITY_AI.CORE.DOCUMENTS(id)
);

-- =============================================================================
-- Views for easy querying
-- =============================================================================

-- View: Active documents with stats
CREATE OR REPLACE VIEW DATA_QUALITY_AI.CORE.V_ACTIVE_DOCUMENTS AS
SELECT
    d.id,
    d.filename,
    d.file_type,
    d.file_size / 1024.0 AS file_size_kb,
    d.upload_date,
    d.uploaded_by,
    COUNT(DISTINCT dtl.table_name) AS linked_tables,
    COUNT(DISTINCT ds.doc_id_2) AS similar_docs_count
FROM DATA_QUALITY_AI.CORE.DOCUMENTS d
LEFT JOIN DATA_QUALITY_AI.CORE.DOCUMENT_TABLE_LINKS dtl ON d.id = dtl.document_id
LEFT JOIN DATA_QUALITY_AI.CORE.DOCUMENT_SIMILARITIES ds ON d.id = ds.doc_id_1 AND ds.similarity_score >= 0.7
WHERE d.status = 'ACTIVE'
GROUP BY d.id, d.filename, d.file_type, d.file_size, d.upload_date, d.uploaded_by;

-- View: Duplicate documents
CREATE OR REPLACE VIEW DATA_QUALITY_AI.CORE.V_DUPLICATE_DOCUMENTS AS
SELECT
    d1.id AS duplicate_id,
    d1.filename AS duplicate_filename,
    d1.upload_date AS duplicate_uploaded,
    d2.id AS original_id,
    d2.filename AS original_filename,
    d2.upload_date AS original_uploaded,
    DATEDIFF(day, d2.upload_date, d1.upload_date) AS days_apart
FROM DATA_QUALITY_AI.CORE.DOCUMENTS d1
JOIN DATA_QUALITY_AI.CORE.DOCUMENTS d2 ON d1.duplicate_of = d2.id
WHERE d1.status = 'DUPLICATE';

-- View: Document quality scores
CREATE OR REPLACE VIEW DATA_QUALITY_AI.CORE.V_DOCUMENT_QUALITY_SCORES AS
SELECT
    d.id,
    d.filename,
    CASE
        WHEN d.status = 'DUPLICATE' THEN 0
        WHEN LENGTH(d.text_content) < 100 THEN 30
        WHEN LENGTH(d.text_content) < 500 THEN 50
        WHEN d.embedding IS NULL THEN 40
        WHEN NOT EXISTS (SELECT 1 FROM DATA_QUALITY_AI.CORE.DOCUMENT_TABLE_LINKS WHERE document_id = d.id) THEN 60
        ELSE 100
    END AS quality_score,
    CASE
        WHEN d.status = 'DUPLICATE' THEN 'Duplicate - should be archived'
        WHEN LENGTH(d.text_content) < 100 THEN 'Too short - may be incomplete'
        WHEN d.embedding IS NULL THEN 'Missing embedding - reprocess needed'
        WHEN NOT EXISTS (SELECT 1 FROM DATA_QUALITY_AI.CORE.DOCUMENT_TABLE_LINKS WHERE document_id = d.id) THEN 'Not linked to any tables'
        ELSE 'OK'
    END AS quality_issue
FROM DATA_QUALITY_AI.CORE.DOCUMENTS d
WHERE d.status != 'ARCHIVED';

-- =============================================================================
-- Stored procedures for document operations
-- =============================================================================

-- Procedure: Find duplicate groups
CREATE OR REPLACE PROCEDURE DATA_QUALITY_AI.CORE.FIND_DUPLICATE_GROUPS()
RETURNS TABLE (
    group_id VARCHAR,
    doc_count INT,
    filenames VARIANT,
    avg_similarity FLOAT
)
LANGUAGE SQL
AS
$$
BEGIN
    -- This will identify groups of similar documents
    -- Implementation would use clustering on similarity scores
    RETURN TABLE(
        SELECT
            'group_' || ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS group_id,
            COUNT(*) AS doc_count,
            ARRAY_AGG(filename) AS filenames,
            AVG(similarity_score) AS avg_similarity
        FROM (
            SELECT DISTINCT
                LEAST(ds.doc_id_1, ds.doc_id_2) AS canonical_id,
                d.filename,
                ds.similarity_score
            FROM DATA_QUALITY_AI.CORE.DOCUMENT_SIMILARITIES ds
            JOIN DATA_QUALITY_AI.CORE.DOCUMENTS d ON d.id = ds.doc_id_2
            WHERE ds.similarity_score >= 0.8
        )
        GROUP BY canonical_id
        HAVING COUNT(*) > 1
    );
END;
$$;

-- =============================================================================
-- Grants
-- =============================================================================
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DATA_QUALITY_AI.CORE TO APPLICATION ROLE app_public;
GRANT SELECT ON ALL VIEWS IN SCHEMA DATA_QUALITY_AI.CORE TO APPLICATION ROLE app_public;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA DATA_QUALITY_AI.CORE TO APPLICATION ROLE app_public;

SELECT 'Document tables created successfully' AS status;
