-- =============================================================================
-- Create Semantic YAML Generator Stored Procedure in UTIL_DB
-- Run this in Snowflake to enable AI-based YAML generation
-- =============================================================================

-- Step 1: Create the schema in UTIL_DB
CREATE SCHEMA IF NOT EXISTS UTIL_DB.SEMANTIC_CONFIG;

-- Step 2: Create the stored procedure
CREATE OR REPLACE PROCEDURE UTIL_DB.SEMANTIC_CONFIG.SP_GENERATE_SEMANTIC_YAML(
    SRC_DB STRING,
    SRC_SCHEMA STRING,
    SRC_TABLE STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
HANDLER = 'run'
AS
$$
import yaml
from snowflake.snowpark import Session

def to_title_case(name: str) -> str:
    return name.replace("_", " ").title()

def infer_role(data_type: str) -> str:
    dt_upper = data_type.upper()
    if "NUMBER" in dt_upper or "INT" in dt_upper or "FLOAT" in dt_upper or "DOUBLE" in dt_upper or "DECIMAL" in dt_upper:
        return "measure"
    return "dimension"

def infer_logical_type(data_type: str) -> str:
    dt_upper = data_type.upper()
    if "DATE" in dt_upper or "TIMESTAMP" in dt_upper:
        return "date"
    if "NUMBER" in dt_upper or "INT" in dt_upper or "FLOAT" in dt_upper or "DOUBLE" in dt_upper or "DECIMAL" in dt_upper:
        return "number"
    return "text"

def run(session: Session, SRC_DB: str, SRC_SCHEMA: str, SRC_TABLE: str) -> str:
    query = f'''
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM "{SRC_DB}".INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SRC_SCHEMA}' AND TABLE_NAME = '{SRC_TABLE}'
        ORDER BY ORDINAL_POSITION
    '''
    df = session.sql(query).collect()

    columns = []
    for row in df:
        col_name = row["COLUMN_NAME"]
        data_type = row["DATA_TYPE"]
        columns.append({
            "name": col_name,
            "label": to_title_case(col_name),
            "data_type": data_type,
            "role": infer_role(data_type),
            "logical_type": infer_logical_type(data_type),
            "description": f"TODO: describe {col_name.lower()}."
        })

    spec = {
        "semantic_view": {
            "name": f"{SRC_SCHEMA.lower()}_{SRC_TABLE.lower()}",
            "version": 1,
            "source": {
                "database": SRC_DB,
                "schema": SRC_SCHEMA,
                "table": SRC_TABLE
            },
            "target": {
                "database": "SEMANTIC",
                "schema": SRC_SCHEMA,
                "view_name": f"VW_{SRC_TABLE}"
            },
            "description": f"Semantic view for {SRC_DB}.{SRC_SCHEMA}.{SRC_TABLE}",
            "grain": "row",
            "primary_key": [],
            "columns": columns,
            "metrics": []
        }
    }

    return yaml.dump(spec, default_flow_style=False, sort_keys=False, allow_unicode=True)
$$;

-- =============================================================================
-- Test the procedure
-- =============================================================================
-- CALL UTIL_DB.SEMANTIC_CONFIG.SP_GENERATE_SEMANTIC_YAML('MYDB', 'MYSCHEMA', 'MYTABLE');
