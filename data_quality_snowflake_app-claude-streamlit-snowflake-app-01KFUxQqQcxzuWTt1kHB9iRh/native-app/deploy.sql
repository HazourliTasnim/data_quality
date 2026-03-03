-- =============================================================================
-- Snowflake Native App Deployment Script
-- Data Quality AI - Package and Deploy
-- =============================================================================

-- Step 1: Create Application Package
USE ROLE ACCOUNTADMIN;

CREATE APPLICATION PACKAGE IF NOT EXISTS DATA_QUALITY_AI_PACKAGE;

USE APPLICATION PACKAGE DATA_QUALITY_AI_PACKAGE;

-- Step 2: Create a version stage for uploading files
CREATE SCHEMA IF NOT EXISTS DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT;
CREATE OR REPLACE STAGE DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stage for Data Quality AI Native App files';

-- Step 3: Upload files to stage
-- NOTE: Run these commands from SnowSQL or Snowflake UI
-- PUT file://manifest.yml @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://setup.sql @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://README.md @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://streamlit_app.py @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://snowflake_utils.py @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://semantic_yaml_spec.py @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://doc_snippets.py @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://requirements.txt @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Step 4: Verify files uploaded
LIST @DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/;

-- Step 5: Create version from uploaded files
ALTER APPLICATION PACKAGE DATA_QUALITY_AI_PACKAGE
  ADD VERSION v1_0_0
  USING '@DATA_QUALITY_AI_PACKAGE.STAGE_CONTENT.APP_STAGE/v1_0_0/';

-- Step 6: Set default version
ALTER APPLICATION PACKAGE DATA_QUALITY_AI_PACKAGE
  SET DEFAULT RELEASE DIRECTIVE
  VERSION = v1_0_0
  PATCH = 0;

-- Step 7: Create test application
USE ROLE ACCOUNTADMIN;

DROP APPLICATION IF NOT EXISTS DATA_QUALITY_AI_TEST;

CREATE APPLICATION DATA_QUALITY_AI_TEST
  FROM APPLICATION PACKAGE DATA_QUALITY_AI_PACKAGE
  USING VERSION v1_0_0;

-- Step 8: Grant necessary privileges to test app
GRANT USAGE ON DATABASE SNOWFLAKE_SAMPLE_DATA TO APPLICATION DATA_QUALITY_AI_TEST;
GRANT USAGE ON SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 TO APPLICATION DATA_QUALITY_AI_TEST;
GRANT SELECT ON ALL TABLES IN SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 TO APPLICATION DATA_QUALITY_AI_TEST;

-- Step 9: Test the application
-- Navigate to: Apps > DATA_QUALITY_AI_TEST > Streamlit

-- =============================================================================
-- Publishing to Marketplace (After Testing)
-- =============================================================================

-- Step 10: Create listing (do this through Snowflake UI)
-- 1. Go to Provider Studio in Snowsight
-- 2. Create New Listing
-- 3. Select "Native App"
-- 4. Choose DATA_QUALITY_AI_PACKAGE
-- 5. Fill in marketplace details
-- 6. Submit for review

-- Step 11: Set pricing (through Provider Studio UI)
-- Configure your pricing tiers:
-- - Starter: $500/month
-- - Professional: $1,500/month
-- - Enterprise: Custom

SELECT 'Deployment complete! Navigate to Apps > DATA_QUALITY_AI_TEST to test.' AS status;
