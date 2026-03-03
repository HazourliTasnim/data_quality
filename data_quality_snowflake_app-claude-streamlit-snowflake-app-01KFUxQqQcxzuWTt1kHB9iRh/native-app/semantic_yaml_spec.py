"""
Semantic YAML specification helpers and local generator.
"""

from typing import List, Dict, Any, Optional
import yaml


# Required top-level keys in semantic YAML
REQUIRED_KEYS = ["semantic_view"]

# Required keys under semantic_view
REQUIRED_SEMANTIC_VIEW_KEYS = [
    "name",
    "version",
    "source",
    "target",
    "columns",
]

# Required keys for source
REQUIRED_SOURCE_KEYS = ["database", "schema", "table"]

# Required keys for target
REQUIRED_TARGET_KEYS = ["database", "schema", "view_name"]

# Required keys for each column
REQUIRED_COLUMN_KEYS = ["name", "data_type", "role", "logical_type"]

# Valid role values
VALID_ROLES = ["dimension", "measure", "identifier"]

# Valid logical types
VALID_LOGICAL_TYPES = [
    "identifier",
    "date",
    "datetime",
    "timestamp",
    "category",
    "text",
    "number",
    "currency",
    "percentage",
    "boolean",
]

# Mapping of Snowflake types to logical types
SNOWFLAKE_TYPE_TO_LOGICAL = {
    "NUMBER": "number",
    "DECIMAL": "number",
    "NUMERIC": "number",
    "INT": "number",
    "INTEGER": "number",
    "BIGINT": "number",
    "SMALLINT": "number",
    "TINYINT": "number",
    "BYTEINT": "number",
    "FLOAT": "number",
    "FLOAT4": "number",
    "FLOAT8": "number",
    "DOUBLE": "number",
    "DOUBLE PRECISION": "number",
    "REAL": "number",
    "VARCHAR": "text",
    "CHAR": "text",
    "CHARACTER": "text",
    "STRING": "text",
    "TEXT": "text",
    "BINARY": "text",
    "VARBINARY": "text",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "DATETIME": "datetime",
    "TIME": "datetime",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP_LTZ": "timestamp",
    "TIMESTAMP_NTZ": "timestamp",
    "TIMESTAMP_TZ": "timestamp",
    "VARIANT": "text",
    "OBJECT": "text",
    "ARRAY": "text",
}

# Columns that are typically measures (by name pattern)
MEASURE_PATTERNS = [
    "amount", "total", "sum", "count", "qty", "quantity",
    "price", "cost", "revenue", "profit", "balance",
    "avg", "average", "min", "max", "rate", "ratio",
]

# Columns that are typically identifiers (by name pattern)
IDENTIFIER_PATTERNS = [
    "id", "key", "code", "num", "number", "uuid", "guid",
]


def infer_role(column_name: str, data_type: str) -> str:
    """Infer column role based on name and type."""
    name_lower = column_name.lower()

    # Check for identifier patterns
    for pattern in IDENTIFIER_PATTERNS:
        if pattern in name_lower or name_lower.endswith(f"_{pattern}"):
            return "dimension"

    # Check for measure patterns
    for pattern in MEASURE_PATTERNS:
        if pattern in name_lower:
            return "measure"

    # Numeric types are often measures
    type_upper = data_type.upper()
    if type_upper in ["NUMBER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "INT", "INTEGER"]:
        # But not if they look like IDs
        if any(p in name_lower for p in IDENTIFIER_PATTERNS):
            return "dimension"
        return "measure"

    return "dimension"


def infer_logical_type(column_name: str, data_type: str) -> str:
    """Infer logical type from Snowflake data type."""
    type_upper = data_type.upper().split("(")[0].strip()

    # Check name patterns for identifiers
    name_lower = column_name.lower()
    for pattern in IDENTIFIER_PATTERNS:
        if pattern in name_lower or name_lower.endswith(f"_{pattern}"):
            return "identifier"

    return SNOWFLAKE_TYPE_TO_LOGICAL.get(type_upper, "text")


def to_title_case(name: str) -> str:
    """Convert column name to title case label."""
    # Replace underscores with spaces and title case
    return name.replace("_", " ").title()


def generate_semantic_yaml_local(
    database: str,
    schema: str,
    table: str,
    columns: List[Dict[str, Any]],
    primary_keys: Optional[List[str]] = None,
    description: Optional[str] = None,
    view_name: Optional[str] = None,
    target_database: str = "SEMANTIC",
    target_schema: Optional[str] = None,
    source_system: str = "GENERIC",
    business_domain: Optional[str] = None,
    entity_type: Optional[str] = None,
) -> str:
    """
    Generate semantic YAML definition locally (stub version).

    Args:
        database: Source database name
        schema: Source schema name
        table: Source table name
        columns: List of column metadata dicts from get_columns()
        primary_keys: List of primary key column names
        description: Optional table description
        view_name: Optional view name (defaults to SV_{table})
        target_database: Target database for the semantic view
        target_schema: Target schema (defaults to source schema)
        source_system: Source system identifier (e.g., SAP_SD, SFDC)
        business_domain: Business domain (e.g., Sales, Finance)
        entity_type: Entity type (e.g., SalesOrderHeader, Customer)

    Returns:
        YAML string
    """
    if target_schema is None:
        target_schema = schema

    if primary_keys is None:
        primary_keys = []

    if description is None:
        description = f"Semantic view for {database}.{schema}.{table}"

    if view_name is None:
        view_name = f"SV_{table.upper()}"

    # Build column definitions
    column_defs = []
    for col in columns:
        col_name = col["column_name"]
        data_type = col["data_type"]
        comment = col.get("comment") or ""

        col_def = {
            "name": col_name,
            "label": to_title_case(col_name),
            "data_type": data_type,
            "role": infer_role(col_name, data_type),
            "logical_type": infer_logical_type(col_name, data_type),
            "description": comment if comment else f"Column {col_name}",
        }
        column_defs.append(col_def)

    # Build the full spec with new metadata fields
    spec = {
        "semantic_view": {
            "name": view_name,
            "version": 1,
            "source_system": source_system,
            "source": {
                "database": database,
                "schema": schema,
                "table": table,
            },
            "target": {
                "database": target_database,
                "schema": target_schema,
                "view_name": view_name,
            },
            "description": description,
            "grain": "row",
            "primary_key": primary_keys,
            "columns": column_defs,
            "metrics": [],
        }
    }

    # Add optional fields if provided
    if business_domain:
        spec["semantic_view"]["business_domain"] = business_domain
    if entity_type:
        spec["semantic_view"]["entity_type"] = entity_type

    return yaml.dump(spec, default_flow_style=False, sort_keys=False, allow_unicode=True)


def validate_semantic_yaml(yaml_content: str) -> Dict[str, Any]:
    """
    Validate semantic YAML structure.

    Returns:
        Dict with keys: valid (bool), errors (list), parsed (dict or None)
    """
    errors = []
    parsed = None

    # Parse YAML
    try:
        parsed = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return {
            "valid": False,
            "errors": [f"YAML parse error: {str(e)}"],
            "parsed": None,
        }

    if not isinstance(parsed, dict):
        return {
            "valid": False,
            "errors": ["Root must be a YAML object"],
            "parsed": None,
        }

    # Check required top-level keys
    for key in REQUIRED_KEYS:
        if key not in parsed:
            errors.append(f"Missing required top-level key: {key}")

    if "semantic_view" not in parsed:
        return {"valid": False, "errors": errors, "parsed": parsed}

    sv = parsed["semantic_view"]
    if not isinstance(sv, dict):
        errors.append("semantic_view must be an object")
        return {"valid": False, "errors": errors, "parsed": parsed}

    # Check required semantic_view keys
    for key in REQUIRED_SEMANTIC_VIEW_KEYS:
        if key not in sv:
            errors.append(f"Missing required key in semantic_view: {key}")

    # Validate source
    if "source" in sv:
        source = sv["source"]
        if isinstance(source, dict):
            for key in REQUIRED_SOURCE_KEYS:
                if key not in source:
                    errors.append(f"Missing required key in source: {key}")
        else:
            errors.append("source must be an object")

    # Validate target
    if "target" in sv:
        target = sv["target"]
        if isinstance(target, dict):
            for key in REQUIRED_TARGET_KEYS:
                if key not in target:
                    errors.append(f"Missing required key in target: {key}")
        else:
            errors.append("target must be an object")

    # Validate columns
    if "columns" in sv:
        columns = sv["columns"]
        if isinstance(columns, list):
            for i, col in enumerate(columns):
                if isinstance(col, dict):
                    col_name = col.get("name", f"Column {i+1}")

                    for key in REQUIRED_COLUMN_KEYS:
                        if key not in col:
                            errors.append(f"{col_name}: missing required key '{key}'")

                    # Validate role - allow empty but flag it
                    role_val = col.get("role", "")
                    if role_val and role_val not in VALID_ROLES:
                        errors.append(f"{col_name}: invalid role '{role_val}'")
                    elif not role_val:
                        errors.append(f"{col_name}: role is empty (should be 'dimension' or 'measure')")

                    # Validate logical_type - allow empty but flag it
                    lt_val = col.get("logical_type", "")
                    if lt_val and lt_val not in VALID_LOGICAL_TYPES:
                        errors.append(f"{col_name}: invalid logical_type '{lt_val}'")
                    elif not lt_val:
                        errors.append(f"{col_name}: logical_type is empty")
                else:
                    errors.append(f"Column {i+1}: must be an object")
        else:
            errors.append("columns must be a list")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "parsed": parsed,
    }


def auto_fix_yaml(yaml_content: str) -> str:
    """
    Auto-fix common issues in semantic YAML:
    - Fill empty role with inferred value
    - Fill empty logical_type with inferred value
    - Fill missing required keys with defaults
    """
    parsed = yaml.safe_load(yaml_content)
    if not parsed or "semantic_view" not in parsed:
        return yaml_content

    sv = parsed["semantic_view"]
    columns = sv.get("columns", [])

    fixed_columns = []
    for col in columns:
        # Skip if column is not a dict (malformed)
        if not isinstance(col, dict):
            continue

        col_name = col.get("name", "UNKNOWN")
        data_type = col.get("data_type", "TEXT")

        # Fix missing name
        if not col.get("name"):
            continue  # Skip columns without names

        # Fix missing data_type
        if not col.get("data_type"):
            col["data_type"] = "TEXT"
            data_type = "TEXT"

        # Fix missing label
        if not col.get("label"):
            col["label"] = to_title_case(col_name)

        # Fix empty role
        if not col.get("role"):
            col["role"] = infer_role(col_name, data_type)

        # Fix empty logical_type
        if not col.get("logical_type"):
            col["logical_type"] = infer_logical_type(col_name, data_type)

        # Fix missing description
        if not col.get("description"):
            col["description"] = f"Column {col_name}"

        # Ensure dq_rules exists
        if "dq_rules" not in col:
            col["dq_rules"] = []

        # Fix dq_rules missing lambda_hint
        if isinstance(col.get("dq_rules"), list):
            for rule in col["dq_rules"]:
                if isinstance(rule, dict) and "lambda_hint" not in rule:
                    # Generate default lambda_hint based on rule type
                    rule_type = rule.get("type", "")
                    params = rule.get("params")
                    lambda_hint = ""

                    if rule_type == "NOT_NULL":
                        lambda_hint = f"{col_name} IS NOT NULL"
                    elif rule_type == "UNIQUE":
                        lambda_hint = f"COUNT(DISTINCT {col_name}) = COUNT({col_name})"
                    elif rule_type == "MIN_VALUE" and params:
                        min_val = params.get("min_value", 0) if isinstance(params, dict) else 0
                        lambda_hint = f"{col_name} >= {min_val}"
                    elif rule_type == "MAX_VALUE" and params:
                        max_val = params.get("max_value", 0) if isinstance(params, dict) else 0
                        lambda_hint = f"{col_name} <= {max_val}"
                    elif rule_type == "PATTERN" and params:
                        pattern = params.get("pattern", "") if isinstance(params, dict) else ""
                        lambda_hint = f"REGEXP_LIKE({col_name}, '{pattern}')"
                    elif rule_type == "ALLOWED_VALUES" and params:
                        values = params.get("values", []) if isinstance(params, dict) else []
                        if values:
                            values_str = ", ".join([f"'{v}'" for v in values])
                            lambda_hint = f"{col_name} IN ({values_str})"
                    elif rule_type == "MAX_LENGTH" and params:
                        max_len = params.get("max_length", 0) if isinstance(params, dict) else 0
                        lambda_hint = f"LENGTH({col_name}) <= {max_len}"
                    elif rule_type == "MAX_AGE_DAYS" and params:
                        max_age = params.get("max_age_days", 0) if isinstance(params, dict) else 0
                        lambda_hint = f"DATEDIFF(day, {col_name}, CURRENT_DATE()) <= {max_age}"
                    elif rule_type == "FOREIGN_KEY" and params:
                        ref_table = params.get("reference_table", "") if isinstance(params, dict) else ""
                        ref_col = params.get("reference_column", "") if isinstance(params, dict) else ""
                        lambda_hint = f"{col_name} IN (SELECT {ref_col} FROM {ref_table})"
                    elif rule_type == "LOOKUP" and params:
                        ref_table = params.get("reference_table", "") if isinstance(params, dict) else ""
                        ref_col = params.get("reference_column", "") if isinstance(params, dict) else ""
                        lambda_hint = f"{col_name} IN (SELECT {ref_col} FROM {ref_table})"
                    else:
                        lambda_hint = f"-- {rule_type} rule for {col_name}"

                    rule["lambda_hint"] = lambda_hint

        fixed_columns.append(col)

    sv["columns"] = fixed_columns
    parsed["semantic_view"] = sv

    return yaml.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)
