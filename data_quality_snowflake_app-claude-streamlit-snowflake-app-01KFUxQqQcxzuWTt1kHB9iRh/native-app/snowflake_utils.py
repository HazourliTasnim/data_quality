"""
Snowflake connection and utility functions for the Semantic YAML Tool.
"""

import re
from typing import Optional, List, Dict, Any
import snowflake.connector
from snowflake.connector import SnowflakeConnection


def parse_account_from_url(url: str) -> str:
    """
    Extract account identifier from Snowflake URL.

    Examples:
        https://mycompany.eu-central-1.snowflakecomputing.com -> mycompany.eu-central-1
        https://abc123.us-east-1.snowflakecomputing.com -> abc123.us-east-1
        mycompany.eu-central-1.snowflakecomputing.com -> mycompany.eu-central-1
    """
    url = url.strip()
    # Remove protocol if present
    url = re.sub(r'^https?://', '', url)
    # Remove trailing slash
    url = url.rstrip('/')
    # Remove .snowflakecomputing.com suffix
    account = re.sub(r'\.snowflakecomputing\.com$', '', url, flags=re.IGNORECASE)
    return account


def get_connection_from_params(
    snowflake_url: str,
    user: str,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
) -> SnowflakeConnection:
    """
    Create a Snowflake connection using externalbrowser SSO authentication.
    All parameters except URL and user are optional.
    """
    account = parse_account_from_url(snowflake_url)

    conn_params = {
        "account": account,
        "user": user,
        "authenticator": "externalbrowser",
    }

    if warehouse:
        conn_params["warehouse"] = warehouse
    if role:
        conn_params["role"] = role
    if database:
        conn_params["database"] = database
    if schema:
        conn_params["schema"] = schema

    return snowflake.connector.connect(**conn_params)


def list_warehouses(conn: SnowflakeConnection) -> List[str]:
    """List all accessible warehouses."""
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW WAREHOUSES")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def list_roles(conn: SnowflakeConnection) -> List[str]:
    """List all accessible roles."""
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW ROLES")
        return [row[1] for row in cursor.fetchall()]
    finally:
        cursor.close()


def use_warehouse(conn: SnowflakeConnection, warehouse: str) -> None:
    """Set the current warehouse."""
    cursor = conn.cursor()
    try:
        cursor.execute(f'USE WAREHOUSE "{warehouse}"')
    finally:
        cursor.close()


def use_role(conn: SnowflakeConnection, role: str) -> None:
    """Set the current role."""
    cursor = conn.cursor()
    try:
        cursor.execute(f'USE ROLE "{role}"')
    finally:
        cursor.close()


def use_database(conn: SnowflakeConnection, database: str) -> None:
    """Set the current database."""
    cursor = conn.cursor()
    try:
        cursor.execute(f'USE DATABASE "{database}"')
    finally:
        cursor.close()


def use_schema(conn: SnowflakeConnection, schema: str) -> None:
    """Set the current schema."""
    cursor = conn.cursor()
    try:
        cursor.execute(f'USE SCHEMA "{schema}"')
    finally:
        cursor.close()


def list_databases(conn: SnowflakeConnection) -> List[str]:
    """List all accessible databases."""
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()]
    finally:
        cursor.close()


def list_schemas(conn: SnowflakeConnection, database: str) -> List[str]:
    """List all schemas in a database."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW SCHEMAS IN DATABASE \"{database}\"")
        return [row[1] for row in cursor.fetchall()]
    finally:
        cursor.close()


def list_tables(conn: SnowflakeConnection, database: str, schema: str) -> List[str]:
    """List all tables in a schema."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW TABLES IN \"{database}\".\"{schema}\"")
        return [row[1] for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_columns(
    conn: SnowflakeConnection,
    database: str,
    schema: str,
    table: str
) -> List[Dict[str, Any]]:
    """
    Get column metadata for a table using INFORMATION_SCHEMA.

    Returns list of dicts with: column_name, data_type, is_nullable, ordinal_position
    """
    cursor = conn.cursor()
    try:
        query = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            ORDINAL_POSITION,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            COLUMN_DEFAULT,
            COMMENT
        FROM "{database}".INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query)
        columns = []
        for row in cursor.fetchall():
            columns.append({
                "column_name": row[0],
                "data_type": row[1],
                "is_nullable": row[2],
                "ordinal_position": row[3],
                "char_max_length": row[4],
                "numeric_precision": row[5],
                "numeric_scale": row[6],
                "column_default": row[7],
                "comment": row[8],
            })
        return columns
    finally:
        cursor.close()


def get_primary_keys(
    conn: SnowflakeConnection,
    database: str,
    schema: str,
    table: str
) -> List[str]:
    """Get primary key columns for a table."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW PRIMARY KEYS IN \"{database}\".\"{schema}\".\"{table}\"")
        return [row[4] for row in cursor.fetchall()]  # column_name is at index 4
    except Exception:
        return []
    finally:
        cursor.close()


def generate_semantic_yaml_with_cortex(
    conn: SnowflakeConnection,
    database: str,
    schema: str,
    table: str,
    model: str = "mistral-large",
    dq_columns: List[str] = None,
    source_system: str = "GENERIC",
    business_domain: Optional[str] = None,
    entity_type: Optional[str] = None,
    view_name: Optional[str] = None,
    description: Optional[str] = None
) -> str:
    """
    Generate semantic YAML using Snowflake Cortex AI directly.
    For large tables (>20 columns), generates base YAML locally and uses Cortex only for DQ rules.

    Args:
        dq_columns: List of column names to generate DQ rules for.
        source_system: Source system identifier (e.g., SAP_SD, SFDC)
        business_domain: Business domain (e.g., Sales, Finance)
        entity_type: Entity type (e.g., SalesOrderHeader, Customer)
        view_name: Optional view name (defaults to schema_table)
        description: Optional view description
    """
    import yaml as yaml_module
    from doc_snippets import build_context_prompt

    # Suggest entity type if not provided
    if not entity_type:
        entity_type = suggest_entity_type(table)

    # Default view name if not provided
    if not view_name:
        view_name = f"{schema.lower()}_{table.lower()}"

    cursor = conn.cursor()
    try:
        # Step 1: Fetch column metadata
        col_query = f'''
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM "{database}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        '''
        cursor.execute(col_query, (schema, table))
        columns = cursor.fetchall()

        if not columns:
            raise ValueError(f"No columns found for {database}.{schema}.{table}")

        num_columns = len(columns)

        # For large tables: hybrid approach - local YAML + Cortex for DQ rules only
        if num_columns > 20:
            return _generate_large_table_yaml(
                cursor, database, schema, table, columns, dq_columns, model,
                source_system, business_domain, entity_type, view_name, description
            )

        # Build context from documentation
        context = build_context_prompt(source_system, entity_type, business_domain)

        # For small/medium tables: full Cortex generation
        col_desc = "\n".join([
            f"- {row[0]}: {row[1]}, {'NOT NULL' if row[2] == 'NO' else 'NULL'}"
            for row in columns
        ])

        if num_columns <= 10:
            # Small tables: full AI generation with DQ rules
            desc_instruction = f'  description: "{description}"' if description else '  description: "Write a detailed business description here"'

            prompt = f'''You are a data quality expert creating a semantic YAML definition for a Snowflake table.

**Table Information:**
- Table: {database}.{schema}.{table}
- Source System: {source_system}
{f"- Business Domain: {business_domain}" if business_domain else ""}
{f"- Entity Type: {entity_type}" if entity_type else ""}

**Context & Documentation:**
{context if context else "No specific documentation available for this source system/entity type."}

**Columns:**
{col_desc}

**Instructions:**
1. {"Use the provided description" if description else "Generate a BUSINESS-FRIENDLY table description (2-3 sentences) that explains:"}
   {'' if description else '''- What business process or entity this table represents
   - What key information it contains
   - How it's typically used
   - DO NOT just repeat column names or say "contains data"'''}

2. For each column, write a MEANINGFUL description that explains:
   - The business purpose of the column
   - What the values represent in business terms
   - Any important constraints or patterns
   - NOT just "Column name field" - explain what it actually means!

3. Generate appropriate DQ rules based on the source system and business context.

Return ONLY valid YAML (no markdown, no code blocks):

semantic_view:
  name: {view_name}
  version: 1
  source_system: {source_system}
{f"  business_domain: {business_domain}" if business_domain else ""}
{f"  entity_type: {entity_type}" if entity_type else ""}
  source:
    database: {database}
    schema: {schema}
    table: {table}
  target:
    database: SEMANTIC
    schema: {schema}
    view_name: {view_name}
{desc_instruction}
  grain: "row"
  primary_key: []
  columns:
    - name: <COLUMN_NAME>
      label: "<Title Case label>"
      data_type: "<Snowflake type>"
      role: "dimension" or "measure"
      logical_type: "identifier/date/number/text/category"
      description: "Write meaningful business description"
      dq_rules:
        - id: "column_name_ruletype"
          type: "<NOT_NULL|UNIQUE|MIN_VALUE|MAX_VALUE|PATTERN|FOREIGN_KEY|LOOKUP>"
          severity: "CRITICAL/WARNING/INFO"
          description: "Explain why this rule is important for data quality"
          lambda_hint: "SQL expression like 'COLUMN_NAME IS NOT NULL'"
          params: null or {{key: value}}
  metrics: []

Return ONLY the YAML, no extra text.'''
        else:
            # Medium tables (11-20): generate with selected DQ columns
            dq_instruction = f"Generate DQ rules ONLY for: {', '.join(dq_columns)}. Other columns: dq_rules: []" if dq_columns else "Set dq_rules: [] for all columns."
            desc_instruction = f'  description: "{description}"' if description else '  description: "Write detailed business description"'

            prompt = f'''You are a data quality expert creating a semantic YAML definition for a Snowflake table.

**Table Information:**
- Table: {database}.{schema}.{table}
- Source System: {source_system}
{f"- Business Domain: {business_domain}" if business_domain else ""}
{f"- Entity Type: {entity_type}" if entity_type else ""}

**Context & Documentation:**
{context if context else "No specific documentation available."}

**Columns:**
{col_desc}

**Instructions:**
1. {"Use the provided description" if description else "Generate a BUSINESS-FRIENDLY table description (2-3 sentences) explaining what this table represents and its business purpose."}
2. For each column, write a MEANINGFUL description explaining its business purpose (not just "field name").
3. {dq_instruction}

Return ONLY valid YAML (no markdown, no code blocks):

semantic_view:
  name: {view_name}
  version: 1
  source_system: {source_system}
{f"  business_domain: {business_domain}" if business_domain else ""}
{f"  entity_type: {entity_type}" if entity_type else ""}
  source:
    database: {database}
    schema: {schema}
    table: {table}
  target:
    database: SEMANTIC
    schema: {schema}
    view_name: {view_name}
{desc_instruction}
  grain: "row"
  primary_key: []
  columns:
    - name: <COLUMN_NAME>
      label: "<Title Case>"
      data_type: "<type>"
      role: "dimension" or "measure"
      logical_type: "identifier/date/number/text/category"
      description: "Write meaningful business description"
      dq_rules:
        - id: "column_name_ruletype"
          type: "NOT_NULL"
          severity: "CRITICAL"
          description: "Why this rule matters"
          lambda_hint: "COLUMN_NAME IS NOT NULL"
          params: null
  metrics: []

Include ALL {num_columns} columns. For DQ rules, ALWAYS include id, type, severity, description, lambda_hint, and params fields. Return ONLY YAML.'''

        # Call Cortex
        cortex_sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)"
        cursor.execute(cortex_sql, (model, prompt))
        result = cursor.fetchone()

        if not result or not result[0]:
            raise ValueError("Cortex returned empty response")

        yaml_text = result[0].strip()

        # Clean up markdown code blocks if present
        if yaml_text.startswith("```"):
            lines = yaml_text.split("\n")
            if lines[-1].strip() == "```":
                yaml_text = "\n".join(lines[1:-1])
            else:
                yaml_text = "\n".join(lines[1:])

        # Fix any missing fields (like lambda_hint in DQ rules)
        from semantic_yaml_spec import auto_fix_yaml
        yaml_text = auto_fix_yaml(yaml_text)

        return yaml_text

    finally:
        cursor.close()


def _generate_large_table_yaml(
    cursor,
    database: str,
    schema: str,
    table: str,
    columns: list,
    dq_columns: List[str],
    model: str,
    source_system: str = "GENERIC",
    business_domain: Optional[str] = None,
    entity_type: Optional[str] = None,
    view_name: Optional[str] = None,
    description: Optional[str] = None
) -> str:
    """
    Generate YAML for large tables (>20 columns) using hybrid approach:
    1. Generate base YAML locally (all columns)
    2. Use Cortex for table & column descriptions
    3. Apply rule packs and use Cortex for DQ rules on selected columns
    4. Merge results
    """
    import yaml as yaml_module
    from doc_snippets import build_context_prompt
    import json

    # Suggest entity type if not provided
    if not entity_type:
        entity_type = suggest_entity_type(table)

    # Default view name if not provided
    if not view_name:
        view_name = f"{schema.lower()}_{table.lower()}"

    # Build context
    context = build_context_prompt(source_system, entity_type, business_domain)

    # Use provided description or generate one via AI
    if description:
        table_description = description
    else:
        # Build column summary for first 20 columns (for AI context)
        col_summary = []
        for i, row in enumerate(columns[:20]):
            col_name, data_type, is_nullable = row[0], row[1], row[2]
            col_summary.append(f"{col_name} ({data_type})")

        col_summary_text = ", ".join(col_summary)
        if len(columns) > 20:
            col_summary_text += f" ... and {len(columns) - 20} more columns"

        # Use Cortex to generate table description
        desc_prompt = f'''Generate a business-friendly description for this table.

Table: {database}.{schema}.{table}
Source System: {source_system}
{f"Business Domain: {business_domain}" if business_domain else ""}
{f"Entity Type: {entity_type}" if entity_type else ""}

Context:
{context if context else "Generic table"}

Columns: {col_summary_text}

Write a 2-3 sentence business description explaining what this table represents, what it contains, and how it's used. Be specific and meaningful, not generic.

Return ONLY the description text, no extra formatting.'''

        try:
            cursor.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)", (model, desc_prompt))
            desc_result = cursor.fetchone()
            table_description = desc_result[0].strip() if desc_result and desc_result[0] else f"Semantic view for {table} table"
        except:
            table_description = f"Semantic view for {table} table containing {len(columns)} columns"

    # Build base YAML structure locally
    base_columns = []
    for row in columns:
        col_name, data_type, is_nullable = row[0], row[1], row[2]

        # Infer role and logical_type
        role = "measure" if data_type in ("NUMBER", "FLOAT", "DECIMAL", "DOUBLE", "INTEGER", "BIGINT") and not col_name.endswith("_ID") else "dimension"
        if "_ID" in col_name or col_name.endswith("ID"):
            logical_type = "identifier"
        elif "DATE" in data_type or "TIME" in data_type:
            logical_type = "date"
        elif data_type in ("NUMBER", "FLOAT", "DECIMAL", "DOUBLE", "INTEGER", "BIGINT"):
            logical_type = "number"
        elif "BOOL" in data_type:
            logical_type = "category"
        else:
            logical_type = "text"

        # Create label from column name
        label = col_name.replace("_", " ").title()

        # Generate better column description
        col_desc = f"{label} field"
        # Add context-aware hints
        if "_id" in col_name.lower() or col_name.lower().endswith("id"):
            col_desc = f"Unique identifier for {label.lower()}"
        elif "date" in col_name.lower() or "time" in col_name.lower():
            col_desc = f"Date/time when {label.lower()} occurred"
        elif "amount" in col_name.lower() or "value" in col_name.lower() or "price" in col_name.lower():
            col_desc = f"Monetary value representing {label.lower()}"
        elif "name" in col_name.lower():
            col_desc = f"Name or label for {label.lower()}"
        elif "status" in col_name.lower() or "state" in col_name.lower():
            col_desc = f"Current status or state of {label.lower()}"

        base_columns.append({
            "name": col_name,
            "label": label,
            "data_type": data_type,
            "role": role,
            "logical_type": logical_type,
            "description": col_desc,
            "dq_rules": []
        })

    base_yaml = {
        "semantic_view": {
            "name": view_name,
            "version": 1,
            "source_system": source_system,
            "source": {
                "database": database,
                "schema": schema,
                "table": table
            },
            "target": {
                "database": "SEMANTIC",
                "schema": schema,
                "view_name": view_name
            },
            "description": table_description,
            "grain": "row",
            "primary_key": [],
            "columns": base_columns,
            "metrics": []
        }
    }

    # Add optional metadata fields
    if business_domain:
        base_yaml["semantic_view"]["business_domain"] = business_domain
    if entity_type:
        base_yaml["semantic_view"]["entity_type"] = entity_type

    # If DQ columns specified, apply rule packs + use Cortex for refinement
    if dq_columns and len(dq_columns) > 0:
        # Apply rule packs first
        rule_pack_rules = apply_rule_packs_to_columns(
            base_columns,
            source_system,
            entity_type,
            business_domain
        )

        # Use Cortex for additional/refined rules (pass base_columns for descriptions)
        dq_rules_map = _generate_dq_rules_only(cursor, dq_columns, columns, model, base_columns)

        # Merge DQ rules into base YAML
        for col in base_yaml["semantic_view"]["columns"]:
            col_name = col["name"]
            col_rules = []

            # Add rule pack rules
            if col_name in rule_pack_rules:
                col_rules.extend(rule_pack_rules[col_name])

            # Add Cortex rules (avoid duplicates)
            if col_name in dq_rules_map:
                existing_types = [r.get("type") for r in col_rules]
                for cortex_rule in dq_rules_map[col_name]:
                    if cortex_rule.get("type") not in existing_types:
                        col_rules.append(cortex_rule)

            col["dq_rules"] = col_rules

    yaml_output = yaml_module.dump(base_yaml, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # Fix any missing fields (like lambda_hint in DQ rules)
    from semantic_yaml_spec import auto_fix_yaml
    yaml_output = auto_fix_yaml(yaml_output)

    return yaml_output


def _generate_dq_rules_only(cursor, dq_columns: List[str], all_columns: list, model: str, base_columns: list = None) -> Dict[str, list]:
    """
    Use Cortex to generate DQ rules only for specified columns.
    Batches columns into groups of 10 to avoid token limits.
    Returns a dict mapping column_name -> list of dq_rules.
    """
    import json

    # Build column info for selected columns only, including descriptions if available
    selected_col_info = []
    for row in all_columns:
        if row[0] in dq_columns:
            col_info = {
                "name": row[0],
                "type": row[1],
                "nullable": row[2],
                "description": ""
            }
            # Try to get description from base_columns if available
            if base_columns:
                for base_col in base_columns:
                    if isinstance(base_col, dict) and base_col.get("name") == row[0]:
                        col_info["description"] = base_col.get("description", "")
                        break
            selected_col_info.append(col_info)

    if not selected_col_info:
        return {}

    # Batch columns into groups of 10 to avoid token limits
    batch_size = 10
    all_rules_map = {}

    for i in range(0, len(selected_col_info), batch_size):
        batch = selected_col_info[i:i+batch_size]
        col_desc = "\n".join([
            f"- {col['name']}: {col['type']}, {'NOT NULL' if col['nullable'] == 'NO' else 'NULL'}" +
            (f"\n  Description: {col['description']}" if col['description'] else "")
            for col in batch
        ])

        prompt = f'''Generate DQ rules for these columns. Return ONLY valid JSON (no markdown).

Columns:
{col_desc}

Return JSON array with this EXACT structure:
[
  {{"column": "COLUMN_NAME", "rules": [
    {{
      "type": "NOT_NULL",
      "severity": "CRITICAL",
      "description": "Why this rule is important",
      "lambda_hint": "COLUMN_NAME IS NOT NULL",
      "params": null
    }}
  ]}}
]

IMPORTANT:
- Each rule MUST have: type, severity, description, lambda_hint, params
- lambda_hint is the SQL expression to check the rule (e.g., "COL >= 0", "COL IS NOT NULL")
- Use column descriptions to create meaningful rules
- Types: NOT_NULL, UNIQUE, MIN_VALUE, MAX_VALUE, ALLOWED_VALUES, MAX_LENGTH, PATTERN
- Generate 1-2 rules per column based on the column's purpose

Return ONLY the JSON array, no extra text.'''

        cortex_sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)"
        try:
            cursor.execute(cortex_sql, (model, prompt))
            result = cursor.fetchone()

            if not result or not result[0]:
                continue

            response = result[0].strip()

            # Clean markdown
            if response.startswith("```"):
                lines = response.split("\n")
                if lines[-1].strip() == "```":
                    response = "\n".join(lines[1:-1])
                else:
                    response = "\n".join(lines[1:])

            rules_list = json.loads(response)
            for item in rules_list:
                col_name = item.get("column")
                rules = item.get("rules", [])
                # Ensure all required fields exist
                for rule in rules:
                    if "id" not in rule:
                        rule["id"] = f"{col_name}_{rule.get('type', 'rule')}".lower()
                    if "params" not in rule:
                        rule["params"] = None
                    # Generate lambda_hint if missing
                    if "lambda_hint" not in rule or not rule["lambda_hint"]:
                        rule_type = rule.get("type", "")
                        params = rule.get("params")
                        if rule_type == "NOT_NULL":
                            rule["lambda_hint"] = f"{col_name} IS NOT NULL"
                        elif rule_type == "UNIQUE":
                            rule["lambda_hint"] = f"COUNT(DISTINCT {col_name}) = COUNT({col_name})"
                        elif rule_type == "MIN_VALUE" and params:
                            min_val = params.get("min_value", 0) if isinstance(params, dict) else 0
                            rule["lambda_hint"] = f"{col_name} >= {min_val}"
                        elif rule_type == "MAX_VALUE" and params:
                            max_val = params.get("max_value", 0) if isinstance(params, dict) else 0
                            rule["lambda_hint"] = f"{col_name} <= {max_val}"
                        elif rule_type == "PATTERN" and params:
                            pattern = params.get("pattern", "") if isinstance(params, dict) else ""
                            rule["lambda_hint"] = f"REGEXP_LIKE({col_name}, '{pattern}')"
                        else:
                            rule["lambda_hint"] = f"-- {rule_type} rule for {col_name}"
                all_rules_map[col_name] = rules
        except (json.JSONDecodeError, KeyError, TypeError, Exception):
            # Skip this batch if it fails
            continue

    return all_rules_map


def call_generate_semantic_yaml(
    conn: SnowflakeConnection,
    database: str,
    schema: str,
    table: str
) -> str:
    """Alias for generate_semantic_yaml_with_cortex."""
    return generate_semantic_yaml_with_cortex(conn, database, schema, table)


def add_dq_rule_from_natural_language(
    yaml_text: str,
    column_name: str,
    nl_rule: str,
    llm_call_fn
) -> str:
    """
    Add or update a DQ rule for an existing column based on natural language.

    - Never creates new columns
    - Creates dq_rules list if missing
    - Updates existing rule if same type exists, otherwise appends
    """
    import yaml as yaml_module
    import json

    # Parse existing YAML
    parsed = yaml_module.safe_load(yaml_text)
    if not parsed or "semantic_view" not in parsed:
        raise ValueError("Invalid YAML: missing semantic_view")

    sv = parsed["semantic_view"]
    columns = sv.get("columns", [])

    # Find the target column (never create new)
    target_col = None
    target_idx = None
    for idx, col in enumerate(columns):
        if col.get("name") == column_name:
            target_col = col
            target_idx = idx
            break

    if target_col is None:
        raise ValueError(f"Column '{column_name}' not found. Cannot add rule to non-existent column.")

    # Get column metadata for context
    col_data_type = target_col.get("data_type", "unknown")
    col_logical_type = target_col.get("logical_type", "unknown")

    # Build LLM prompt
    prompt = f'''Convert this rule to JSON. Column: {column_name} ({col_data_type})
Rule: "{nl_rule}"

Types: NOT_NULL, UNIQUE, MIN_VALUE, MAX_VALUE, ALLOWED_VALUES, MAX_LENGTH, PATTERN, MAX_AGE_DAYS, FOREIGN_KEY, LOOKUP

Return ONLY JSON:
{{"id":"{column_name.lower()}_<type>","type":"<TYPE>","severity":"CRITICAL/WARNING/INFO","params":null,"description":"<what it checks>","lambda_hint":"<python expr>"}}'''

    # Call LLM
    response_text = llm_call_fn(prompt).strip()

    # Extract JSON from LLM response (handles markdown, extra text, etc.)
    def extract_json(text: str) -> str:
        """Extract the first valid JSON object from LLM response."""
        # Remove markdown code blocks
        if "```" in text:
            # Handle ```json or ``` followed by JSON
            parts = text.split("```")
            for part in parts:
                part = part.strip()
                # Skip language identifiers
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{") and part.endswith("}"):
                    return part
                # Try to find JSON even if there's extra text after
                if part.startswith("{"):
                    # Find the matching closing brace
                    brace_count = 0
                    for i, char in enumerate(part):
                        if char == "{":
                            brace_count += 1
                        elif char == "}":
                            brace_count -= 1
                            if brace_count == 0:
                                return part[:i+1]

        # No markdown blocks - try to extract JSON directly
        text = text.strip()
        if text.startswith("{"):
            # Find the matching closing brace
            brace_count = 0
            for i, char in enumerate(text):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        return text[:i+1]

        return text

    response_text = extract_json(response_text)

    # Parse JSON
    try:
        new_rule = json.loads(response_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned invalid JSON: {e}\nResponse was: {response_text[:200]}")

    # Validate required fields
    for field in ["id", "type", "severity", "description", "lambda_hint"]:
        if field not in new_rule:
            raise ValueError(f"Missing field: {field}")

    if "params" not in new_rule:
        new_rule["params"] = None

    # Ensure dq_rules list exists
    if "dq_rules" not in target_col or not isinstance(target_col["dq_rules"], list):
        target_col["dq_rules"] = []

    # Check if rule with same type exists - replace it; otherwise append
    rule_type = new_rule["type"]
    existing_idx = None
    for i, rule in enumerate(target_col["dq_rules"]):
        if isinstance(rule, dict) and rule.get("type") == rule_type:
            existing_idx = i
            break

    if existing_idx is not None:
        target_col["dq_rules"][existing_idx] = new_rule
    else:
        target_col["dq_rules"].append(new_rule)

    # Update column in list
    columns[target_idx] = target_col
    sv["columns"] = columns
    parsed["semantic_view"] = sv

    return yaml_module.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)


def add_table_level_rule_from_natural_language(
    yaml_text: str,
    column_names: List[str],
    nl_rule: str,
    llm_call_fn
) -> str:
    """
    Add a table-level cross-column DQ rule based on natural language.

    Args:
        yaml_text: The current YAML definition
        column_names: List of columns involved in this rule (2+ columns)
        nl_rule: Natural language description of the rule
        llm_call_fn: Function to call LLM (e.g., call_cortex_for_rule)

    Returns:
        Updated YAML text with the new table-level rule
    """
    import yaml as yaml_module
    import json

    if not column_names or len(column_names) < 2:
        raise ValueError("Table-level rules require at least 2 columns")

    # Parse existing YAML
    parsed = yaml_module.safe_load(yaml_text)
    if not parsed or "semantic_view" not in parsed:
        raise ValueError("Invalid YAML: missing semantic_view")

    sv = parsed["semantic_view"]

    # Ensure table_rules list exists
    if "table_rules" not in sv:
        sv["table_rules"] = []

    # Validate that all columns exist
    existing_columns = [col.get("name") for col in sv.get("columns", [])]
    for col_name in column_names:
        if col_name not in existing_columns:
            raise ValueError(f"Column '{col_name}' not found in semantic view")

    # Build LLM prompt for cross-column rules
    columns_str = ", ".join(column_names)
    prompt = f'''Convert this cross-column rule to JSON. Columns: {columns_str}
Rule: "{nl_rule}"

Rule Types:
- COMPOSITE_UNIQUE: Multiple columns together must be unique
- CROSS_COLUMN_COMPARISON: Compare values between columns (e.g., start < end)
- CONDITIONAL_REQUIRED: If column A has value, column B is required
- MUTUAL_EXCLUSIVITY: Only one of the columns can have a value
- CONDITIONAL_VALUE: If column A = value, then column B must meet condition

Return ONLY JSON:
{{"type":"<TYPE>","columns":{column_names},"severity":"CRITICAL/WARNING/INFO","description":"<what it checks>","lambda_hint":"<SQL expression using column names>"}}

Examples:
{{"type":"COMPOSITE_UNIQUE","columns":["customer_id","order_id"],"severity":"CRITICAL","description":"Customer ID and Order ID together must be unique","lambda_hint":"COUNT(*) OVER (PARTITION BY customer_id, order_id) = 1"}}
{{"type":"CROSS_COLUMN_COMPARISON","columns":["start_date","end_date"],"severity":"WARNING","description":"Start date must be before end date","lambda_hint":"start_date < end_date OR end_date IS NULL"}}
'''

    # Call LLM
    response_text = llm_call_fn(prompt).strip()

    # Extract JSON from LLM response (handles markdown, extra text, etc.)
    def extract_json(text: str) -> str:
        """Extract the first valid JSON object from LLM response."""
        # Remove markdown code blocks
        if "```" in text:
            parts = text.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{") and part.endswith("}"):
                    return part
                if part.startswith("{"):
                    brace_count = 0
                    for i, char in enumerate(part):
                        if char == "{":
                            brace_count += 1
                        elif char == "}":
                            brace_count -= 1
                            if brace_count == 0:
                                return part[:i+1]

        # No markdown blocks - try to extract JSON directly
        text = text.strip()
        if text.startswith("{"):
            brace_count = 0
            for i, char in enumerate(text):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        return text[:i+1]

        return text

    response_text = extract_json(response_text)

    # Parse JSON
    try:
        new_rule = json.loads(response_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned invalid JSON: {e}\nResponse was: {response_text[:200]}")

    # Validate required fields
    for field in ["type", "columns", "severity", "description", "lambda_hint"]:
        if field not in new_rule:
            raise ValueError(f"Missing field: {field}")

    # Ensure columns is a list
    if not isinstance(new_rule["columns"], list):
        new_rule["columns"] = column_names

    # Add ID for tracking
    rule_id = "_".join(column_names).lower() + "_" + new_rule["type"].lower()
    new_rule["id"] = rule_id

    # Check if similar rule exists (same type and columns) - replace it; otherwise append
    rule_type = new_rule["type"]
    rule_cols = set(new_rule["columns"])
    existing_idx = None

    for i, rule in enumerate(sv["table_rules"]):
        if isinstance(rule, dict) and rule.get("type") == rule_type:
            if set(rule.get("columns", [])) == rule_cols:
                existing_idx = i
                break

    if existing_idx is not None:
        sv["table_rules"][existing_idx] = new_rule
    else:
        sv["table_rules"].append(new_rule)

    parsed["semantic_view"] = sv

    return yaml_module.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)


def call_cortex_for_rule(conn: SnowflakeConnection, prompt: str, model: str = "mistral-large") -> str:
    """Call Snowflake Cortex to get LLM response for rule generation."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)", (model, prompt))
        result = cursor.fetchone()
        if not result or not result[0]:
            raise ValueError("Cortex returned empty response")
        return result[0]
    finally:
        cursor.close()


def save_semantic_yaml(
    conn: SnowflakeConnection,
    name: str,
    version: int,
    source_db: str,
    source_schema: str,
    source_table: str,
    target_db: str,
    target_schema: str,
    target_view: str,
    yaml_definition: str,
    status: str = "DRAFT"
) -> bool:
    """Save semantic YAML definition to the registry table."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO SEMANTIC_CONFIG.SEMANTIC_VIEW (
                NAME, VERSION, SOURCE_DB, SOURCE_SCHEMA, SOURCE_TABLE,
                TARGET_DB, TARGET_SCHEMA, TARGET_VIEW, YAML_DEFINITION, STATUS
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (name, version, source_db, source_schema, source_table,
              target_db, target_schema, target_view, yaml_definition, status))
        return True
    except Exception as e:
        raise e
    finally:
        cursor.close()


def execute_ddl(conn: SnowflakeConnection, ddl: str) -> bool:
    """Execute a DDL statement."""
    cursor = conn.cursor()
    try:
        cursor.execute(ddl)
        return True
    finally:
        cursor.close()


# ============================================================================
# Rule Packs and Enrichment Functions
# ============================================================================

def load_rule_packs(rule_packs_file: str = "rule_packs.yaml") -> Dict[str, Any]:
    """Load rule packs configuration from YAML file."""
    import os
    import yaml as yaml_module

    # Get the directory of this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    rule_packs_path = os.path.join(current_dir, rule_packs_file)

    try:
        with open(rule_packs_path, 'r') as f:
            return yaml_module.safe_load(f)
    except FileNotFoundError:
        return {"rule_packs": [], "entity_type_suggestions": []}


def match_column_to_pattern(column_name: str, pattern: str) -> bool:
    """
    Check if a column name matches a pattern.

    Patterns can be:
    - Exact match: "customer_id"
    - Wildcard: "*customer*", "*_id", "order_*"
    - Multiple patterns: "net_value|gross_value|amount"
    """
    import fnmatch

    column_lower = column_name.lower()

    # Handle multiple patterns separated by |
    if "|" in pattern:
        patterns = [p.strip() for p in pattern.split("|")]
        return any(match_column_to_pattern(column_name, p) for p in patterns)

    # Handle wildcard patterns
    pattern_lower = pattern.lower()
    return fnmatch.fnmatch(column_lower, pattern_lower)


def apply_rule_packs_to_columns(
    columns: list,
    source_system: str,
    entity_type: Optional[str] = None,
    business_domain: Optional[str] = None
) -> Dict[str, list]:
    """
    Apply rule packs to columns and return initial DQ rules.

    Returns:
        Dict mapping column_name -> list of rule dicts
    """
    rule_packs_data = load_rule_packs()
    rule_packs = rule_packs_data.get("rule_packs", [])

    # Find matching rule packs
    matching_packs = []

    # First, try exact match
    for pack in rule_packs:
        pack_system = pack.get("source_system")
        pack_entity = pack.get("entity_type")
        pack_domain = pack.get("business_domain")

        if pack_system == source_system:
            if entity_type and pack_entity == entity_type:
                matching_packs.append(pack)
            elif not entity_type and pack_entity == "GENERIC":
                matching_packs.append(pack)

    # If no match, fall back to GENERIC
    if not matching_packs:
        for pack in rule_packs:
            if pack.get("source_system") == "GENERIC":
                matching_packs.append(pack)

    # Apply rules from matching packs
    rules_by_column = {}

    for pack in matching_packs:
        pack_rules = pack.get("rules", [])

        for rule_template in pack_rules:
            applies_to = rule_template.get("applies_to", "")

            # Find matching columns
            for col in columns:
                col_name = col.get("name") if isinstance(col, dict) else col

                if match_column_to_pattern(col_name, applies_to):
                    if col_name not in rules_by_column:
                        rules_by_column[col_name] = []

                    # Create rule from template
                    rule_type = rule_template.get("type")
                    params = rule_template.get("params")

                    # Generate lambda_hint based on rule type
                    lambda_hint = ""
                    if rule_type == "NOT_NULL":
                        lambda_hint = f"{col_name} IS NOT NULL"
                    elif rule_type == "UNIQUE":
                        lambda_hint = f"COUNT(DISTINCT {col_name}) = COUNT({col_name})"
                    elif rule_type == "MIN_VALUE" and params:
                        min_val = params.get("min_value", 0)
                        lambda_hint = f"{col_name} >= {min_val}"
                    elif rule_type == "MAX_VALUE" and params:
                        max_val = params.get("max_value", 0)
                        lambda_hint = f"{col_name} <= {max_val}"
                    elif rule_type == "PATTERN" and params:
                        pattern = params.get("pattern", "")
                        lambda_hint = f"REGEXP_LIKE({col_name}, '{pattern}')"
                    elif rule_type == "ALLOWED_VALUES" and params:
                        values = params.get("values", [])
                        if values:
                            values_str = ", ".join([f"'{v}'" for v in values])
                            lambda_hint = f"{col_name} IN ({values_str})"
                    elif rule_type == "MAX_LENGTH" and params:
                        max_len = params.get("max_length", 0)
                        lambda_hint = f"LENGTH({col_name}) <= {max_len}"
                    elif rule_type == "MAX_AGE_DAYS" and params:
                        max_age = params.get("max_age_days", 0)
                        lambda_hint = f"DATEDIFF(day, {col_name}, CURRENT_DATE()) <= {max_age}"
                    elif rule_type == "FOREIGN_KEY" and params:
                        ref_table = params.get("reference_table", "")
                        ref_col = params.get("reference_column", "")
                        lambda_hint = f"{col_name} IN (SELECT {ref_col} FROM {ref_table})"
                    elif rule_type == "LOOKUP" and params:
                        ref_table = params.get("reference_table", "")
                        ref_col = params.get("reference_column", "")
                        lambda_hint = f"{col_name} IN (SELECT {ref_col} FROM {ref_table})"
                    else:
                        lambda_hint = f"-- {rule_type} rule for {col_name}"

                    rule = {
                        "id": f"{col_name}_{rule_type}".lower(),
                        "type": rule_type,
                        "severity": rule_template.get("severity"),
                        "description": rule_template.get("description", ""),
                        "params": params,
                        "lambda_hint": lambda_hint,
                        "source": "rule_pack"  # Mark as coming from rule pack
                    }

                    # Avoid duplicates (same type for same column)
                    existing_types = [r.get("type") for r in rules_by_column[col_name]]
                    if rule["type"] not in existing_types:
                        rules_by_column[col_name].append(rule)

    return rules_by_column


def suggest_entity_type(table_name: str) -> Optional[str]:
    """Suggest entity type based on table name patterns."""
    rule_packs_data = load_rule_packs()
    suggestions = rule_packs_data.get("entity_type_suggestions", [])

    table_lower = table_name.lower()

    for suggestion in suggestions:
        patterns = suggestion.get("patterns", [])
        for pattern in patterns:
            if match_column_to_pattern(table_name, pattern):
                return suggestion.get("entity_type")

    return None


def enrich_table_description(
    conn: SnowflakeConnection,
    yaml_content: str,
    model: str = "mistral-large"
) -> Dict[str, Any]:
    """
    Use Cortex AI to validate and enrich the table description.

    Returns:
        Dict with keys: enhanced_description, suggested_entity_type, suggested_business_domain
    """
    import yaml as yaml_module
    from doc_snippets import build_context_prompt

    # Parse YAML
    try:
        parsed = yaml_module.safe_load(yaml_content)
        sv = parsed.get("semantic_view", {})
    except:
        raise ValueError("Invalid YAML")

    # Extract metadata
    source = sv.get("source", {})
    table_name = source.get("table", "")
    source_system = sv.get("source_system", "GENERIC")
    business_domain = sv.get("business_domain")
    entity_type = sv.get("entity_type")
    current_description = sv.get("description", "")
    columns = sv.get("columns", [])

    # Build column summary
    col_summary = []
    for col in columns[:20]:  # Limit to first 20 columns
        col_name = col.get("name", "")
        data_type = col.get("data_type", "")
        col_desc = col.get("description", "")
        col_summary.append(f"- {col_name} ({data_type}): {col_desc if col_desc else 'N/A'}")

    col_summary_text = "\n".join(col_summary)
    if len(columns) > 20:
        col_summary_text += f"\n... and {len(columns) - 20} more columns"

    # Build context from documentation
    context = build_context_prompt(source_system, entity_type, business_domain)

    # Build enrichment prompt
    prompt = f"""You are a data quality expert analyzing a table's semantic definition.

Table: {table_name}
Source System: {source_system}
{f"Business Domain: {business_domain}" if business_domain else ""}
{f"Entity Type: {entity_type}" if entity_type else ""}

Current Description:
"{current_description}"

Columns:
{col_summary_text}
context_block = f"Context:\n{context}" if context else ""
{context_block}
Tasks:
1. Improve the table description to be clear, concise, and business-friendly (2-3 sentences max).
2. If entity_type is not set or seems incorrect, suggest an appropriate entity type from this list:
   [SalesOrderHeader, SalesOrderLine, Customer, Account, Opportunity, Product, Invoice, Payment, Person, Employee, Transaction, Shipment, Inventory, GENERIC]
3. If business_domain is not set, suggest one from: [Sales, Finance, HR, SupplyChain, Marketing, Customer Service, Operations, Product]

Return your response in this exact JSON format:
{{
  "enhanced_description": "your improved description here",
  "suggested_entity_type": "EntityType or null",
  "suggested_business_domain": "Domain or null"
}}

Return ONLY the JSON, no markdown formatting."""

    # Call Cortex
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)", (model, prompt))
        result = cursor.fetchone()

        if not result or not result[0]:
            raise ValueError("Cortex returned empty response")

        response = result[0].strip()

        # Clean markdown if present
        if response.startswith("```"):
            lines = response.split("\n")
            response = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

        # Parse JSON response
        import json
        result_data = json.loads(response)

        return {
            "enhanced_description": result_data.get("enhanced_description", current_description),
            "suggested_entity_type": result_data.get("suggested_entity_type"),
            "suggested_business_domain": result_data.get("suggested_business_domain")
        }

    except Exception as e:
        # Return original if enrichment fails
        return {
            "enhanced_description": current_description,
            "suggested_entity_type": None,
            "suggested_business_domain": None,
            "error": str(e)
        }
    finally:
        cursor.close()
