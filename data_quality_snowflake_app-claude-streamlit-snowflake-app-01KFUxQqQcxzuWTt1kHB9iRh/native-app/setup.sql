-- =============================================================================
-- Snowflake Native App Setup Script
-- Data Quality AI - Setup and Installation
-- =============================================================================

-- Create application database if not exists
CREATE DATABASE IF NOT EXISTS DATA_QUALITY_AI;
CREATE SCHEMA IF NOT EXISTS DATA_QUALITY_AI.CORE;

-- =============================================================================
-- Create registry table for storing semantic YAML definitions
-- =============================================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_AI.CORE.SEMANTIC_VIEWS (
    NAME VARCHAR(255) PRIMARY KEY,
    VERSION INT NOT NULL DEFAULT 1,
    SOURCE_DATABASE VARCHAR(255),
    SOURCE_SCHEMA VARCHAR(255),
    SOURCE_TABLE VARCHAR(255),
    TARGET_DATABASE VARCHAR(255),
    TARGET_SCHEMA VARCHAR(255),
    TARGET_VIEW_NAME VARCHAR(255),
    YAML_DEFINITION VARIANT,
    STATUS VARCHAR(50) DEFAULT 'DRAFT',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
    COMMENT VARCHAR(1000)
);

-- =============================================================================
-- Create validation results table
-- =============================================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_AI.CORE.VALIDATION_RESULTS (
    ID VARCHAR(36) PRIMARY KEY DEFAULT UUID_STRING(),
    SEMANTIC_VIEW_NAME VARCHAR(255),
    RULE_ID VARCHAR(255),
    RULE_TYPE VARCHAR(100),
    COLUMN_NAME VARCHAR(255),
    SEVERITY VARCHAR(50),
    VIOLATION_COUNT INT,
    SAMPLE_VIOLATIONS VARIANT,
    EXECUTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXECUTED_BY VARCHAR(255) DEFAULT CURRENT_USER()
);

-- =============================================================================
-- Create user feedback table (for learning loop)
-- =============================================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_AI.CORE.USER_FEEDBACK (
    ID VARCHAR(36) PRIMARY KEY DEFAULT UUID_STRING(),
    RULE_ID VARCHAR(255),
    FEEDBACK_TYPE VARCHAR(50), -- 'ACCEPTED', 'REJECTED', 'MODIFIED'
    USER_COMMENT VARCHAR(5000),
    ORIGINAL_RULE VARIANT,
    MODIFIED_RULE VARIANT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(255) DEFAULT CURRENT_USER()
);

-- =============================================================================
-- Create helper stored procedures
-- =============================================================================

-- Procedure to validate all rules for a semantic view
CREATE OR REPLACE PROCEDURE DATA_QUALITY_AI.CORE.VALIDATE_RULES(
    SEMANTIC_VIEW_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- This will be implemented in Phase 2 (Rule Execution Engine)
    RETURN 'Rule validation will be available in the next release';
END;
$$;

-- Procedure to get rule statistics
CREATE OR REPLACE PROCEDURE DATA_QUALITY_AI.CORE.GET_RULE_STATS(
    SEMANTIC_VIEW_NAME VARCHAR
)
RETURNS TABLE (
    RULE_TYPE VARCHAR,
    COUNT INT,
    SEVERITY VARCHAR
)
LANGUAGE SQL
AS
$$
BEGIN
    -- Return statistics about rules
    RETURN TABLE(
        SELECT
            'PLACEHOLDER' AS RULE_TYPE,
            0 AS COUNT,
            'INFO' AS SEVERITY
    );
END;
$$;

-- =============================================================================
-- Grant permissions to application role
-- =============================================================================
GRANT USAGE ON DATABASE DATA_QUALITY_AI TO APPLICATION ROLE app_public;
GRANT USAGE ON SCHEMA DATA_QUALITY_AI.CORE TO APPLICATION ROLE app_public;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DATA_QUALITY_AI.CORE TO APPLICATION ROLE app_public;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA DATA_QUALITY_AI.CORE TO APPLICATION ROLE app_public;

-- =============================================================================
-- Create application views (optional - for exposing data to users)
-- =============================================================================
CREATE OR REPLACE VIEW DATA_QUALITY_AI.CORE.ACTIVE_SEMANTIC_VIEWS AS
SELECT
    NAME,
    VERSION,
    SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE AS SOURCE_TABLE,
    TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_VIEW_NAME AS TARGET_VIEW,
    STATUS,
    CREATED_AT,
    UPDATED_AT
FROM DATA_QUALITY_AI.CORE.SEMANTIC_VIEWS
WHERE STATUS = 'ACTIVE';

GRANT SELECT ON VIEW DATA_QUALITY_AI.CORE.ACTIVE_SEMANTIC_VIEWS TO APPLICATION ROLE app_public;

-- =============================================================================
-- Setup complete
-- =============================================================================
RETURN 'Data Quality AI application setup completed successfully';
