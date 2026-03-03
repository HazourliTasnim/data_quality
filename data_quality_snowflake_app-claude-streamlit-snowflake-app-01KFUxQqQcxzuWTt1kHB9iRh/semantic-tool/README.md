# Semantic YAML Tool

A Streamlit application to generate, edit, and validate semantic YAML definitions for Snowflake tables.

## Features

- Connect to Snowflake via SSO (externalbrowser authenticator)
- Browse databases, schemas, and tables
- Generate semantic YAML via Cortex AI or locally
- Edit YAML in an interactive editor
- Validate YAML structure
- Save definitions to Snowflake registry

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
streamlit run app.py
```

A browser window will open. Fill in your Snowflake connection details in the sidebar and click "Connect (SSO)".

## Project Structure

```
semantic-tool/
├── app.py                 # Main Streamlit application
├── snowflake_utils.py     # Snowflake connection utilities
├── semantic_yaml_spec.py  # YAML spec and local generator
├── requirements.txt       # Python dependencies
├── README.md              # This file
└── .streamlit/
    └── secrets.toml       # Secrets placeholder (not committed)
```

## Snowflake Setup (Optional)

To enable saving YAML definitions to Snowflake, run the following DDL:

```sql
-- Create schema for semantic configuration
CREATE SCHEMA IF NOT EXISTS SEMANTIC_CONFIG;

-- Create registry table
CREATE TABLE IF NOT EXISTS SEMANTIC_CONFIG.SEMANTIC_VIEW (
    VIEW_ID INTEGER AUTOINCREMENT,
    NAME STRING,
    VERSION INTEGER,
    SOURCE_DB STRING,
    SOURCE_SCHEMA STRING,
    SOURCE_TABLE STRING,
    TARGET_DB STRING,
    TARGET_SCHEMA STRING,
    TARGET_VIEW STRING,
    YAML_DEFINITION STRING,
    STATUS STRING,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Optional: Stored procedure for AI-based YAML generation
CREATE OR REPLACE PROCEDURE SEMANTIC_CONFIG.SP_GENERATE_SEMANTIC_YAML(
    P_DATABASE STRING,
    P_SCHEMA STRING,
    P_TABLE STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_columns STRING;
    v_prompt STRING;
    v_result STRING;
BEGIN
    -- Get column metadata
    SELECT LISTAGG(COLUMN_NAME || ' (' || DATA_TYPE || ')', ', ')
    INTO v_columns
    FROM IDENTIFIER(P_DATABASE || '.INFORMATION_SCHEMA.COLUMNS')
    WHERE TABLE_SCHEMA = P_SCHEMA AND TABLE_NAME = P_TABLE;

    -- Build prompt for Cortex
    v_prompt := 'Generate a semantic YAML definition for table ' ||
                P_DATABASE || '.' || P_SCHEMA || '.' || P_TABLE ||
                ' with columns: ' || v_columns ||
                '. Follow this structure: semantic_view with name, version, source, target, description, grain, primary_key, columns (with name, label, data_type, role, logical_type, description), and metrics.';

    -- Call Cortex (adjust model as needed)
    SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', v_prompt) INTO v_result;

    RETURN v_result;
END;
$$;
```

## YAML Schema

The generated YAML follows this structure:

```yaml
semantic_view:
  name: SV_TABLE_NAME
  version: 1
  source:
    database: SOURCE_DB
    schema: SOURCE_SCHEMA
    table: TABLE_NAME
  target:
    database: SEMANTIC
    schema: TARGET_SCHEMA
    view_name: SV_TABLE_NAME
  description: "Description of the semantic view"
  grain: "row"
  primary_key: []
  columns:
    - name: COLUMN_NAME
      label: "Column Label"
      data_type: "VARCHAR"
      role: "dimension"
      logical_type: "text"
      description: "Column description"
  metrics: []
```

## Porting to Streamlit-in-Snowflake

This application is designed for easy porting to Streamlit-in-Snowflake. Changes needed:

1. Replace `snowflake.connector.connect()` with `snowflake.snowpark.context.get_active_session()`
2. Remove the connection sidebar (connection is automatic in SiS)
3. Update imports to use Snowpark session methods

## License

MIT
