"""
DQ Dashboard — Data Quality Assessment & Impact Analysis

This module provides the full Data Quality Dashboard:
- Column selection for profiling
- Data filtering (by country, date, etc.) before analysis
- Correctness rate per column (null rate, invalid rate, coherence)
- External API cross-validation (VAT / SIRET / ISO)
- Business rules in natural language
- Cross-column coherence checks
- LLM similarity detection for fuzzy matching
- Downstream impact analysis
"""

import streamlit as st
import pandas as pd
import json
import yaml
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
from snowflake.connector import SnowflakeConnection

# POC Integration
from poc_integration import render_embedding_dq_section


# ============================================================================
# Backend: Column Profiling
# ============================================================================

def profile_columns(conn: SnowflakeConnection, database: str, schema: str, table: str,
                    columns: List[str], where_clause: str = None) -> List[Dict]:
    """
    Profile selected columns: null count, distinct count, empty strings, min/max, sample values.
    Returns a list of dicts, one per column.
    """
    cursor = conn.cursor()
    results = []

    # Get total row count (with optional filter)
    count_sql = f'SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"'
    if where_clause:
        count_sql += f" WHERE {where_clause}"
    cursor.execute(count_sql)
    total_rows = cursor.fetchone()[0]

    for col in columns:
        try:
            # Build profiling query
            base_filter = f" WHERE {where_clause}" if where_clause else ""

            sql = f"""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS null_count,
                SUM(CASE WHEN "{col}" IS NOT NULL AND TRIM(CAST("{col}" AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS empty_count,
                COUNT(DISTINCT "{col}") AS distinct_count,
                MIN(CAST("{col}" AS VARCHAR)) AS min_value,
                MAX(CAST("{col}" AS VARCHAR)) AS max_value
            FROM "{database}"."{schema}"."{table}"{base_filter}
            """
            cursor.execute(sql)
            row = cursor.fetchone()

            total = row[0] if row[0] else 0
            nulls = row[1] if row[1] else 0
            empties = row[2] if row[2] else 0
            distinct = row[3] if row[3] else 0
            min_val = row[4]
            max_val = row[5]

            null_rate = (nulls / total * 100) if total > 0 else 0
            empty_rate = (empties / total * 100) if total > 0 else 0
            fill_rate = 100 - null_rate
            completeness = 100 - null_rate - empty_rate

            # Sample values (top 5 most frequent)
            sample_sql = f"""
            SELECT CAST("{col}" AS VARCHAR) AS val, COUNT(*) AS cnt
            FROM "{database}"."{schema}"."{table}"{base_filter}
            WHERE "{col}" IS NOT NULL
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT 5
            """
            cursor.execute(sample_sql)
            samples = [{"value": r[0], "count": r[1]} for r in cursor.fetchall()]

            results.append({
                "column": col,
                "total_rows": total,
                "null_count": nulls,
                "null_rate": round(null_rate, 2),
                "empty_count": empties,
                "empty_rate": round(empty_rate, 2),
                "fill_rate": round(fill_rate, 2),
                "completeness": round(completeness, 2),
                "distinct_count": distinct,
                "distinct_rate": round((distinct / total * 100) if total > 0 else 0, 2),
                "min_value": str(min_val) if min_val else None,
                "max_value": str(max_val) if max_val else None,
                "top_values": samples,
                "correctness_rate": None,   # Populated later by rules/API checks
                "correctness_details": [],  # Breakdown of issues
            })
        except Exception as e:
            results.append({
                "column": col,
                "total_rows": total_rows,
                "null_count": 0,
                "null_rate": 0,
                "empty_count": 0,
                "empty_rate": 0,
                "fill_rate": 0,
                "completeness": 0,
                "distinct_count": 0,
                "distinct_rate": 0,
                "min_value": None,
                "max_value": None,
                "top_values": [],
                "correctness_rate": None,
                "correctness_details": [],
                "error": str(e)
            })

    cursor.close()
    return results


def get_filter_values(conn: SnowflakeConnection, database: str, schema: str, table: str,
                      column: str, limit: int = 200) -> List[str]:
    """Get distinct values for a filter column."""
    cursor = conn.cursor()
    sql = f"""
    SELECT DISTINCT CAST("{column}" AS VARCHAR) AS val
    FROM "{database}"."{schema}"."{table}"
    WHERE "{column}" IS NOT NULL
    ORDER BY val
    LIMIT {limit}
    """
    cursor.execute(sql)
    values = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return values


# ============================================================================
# Backend: Correctness Rate Calculation
# ============================================================================

def calculate_correctness_rate(
    conn: SnowflakeConnection,
    database: str, schema: str, table: str,
    column: str,
    checks: List[Dict],
    where_clause: str = None
) -> Dict:
    """
    Calculate correctness rate for a single column by running multiple checks.
    
    checks format: [
        {"type": "not_null", "weight": 1.0},
        {"type": "pattern", "pattern": "^[A-Z]{2}[0-9]{9}$", "weight": 1.0},
        {"type": "external_api", "provider": "vat_api", "weight": 2.0},
        {"type": "business_rule", "sql": "...", "weight": 1.0},
        {"type": "cross_column", "ref_column": "...", "weight": 1.5},
    ]
    
    Returns: {"correctness_rate": 85.3, "details": [...]}
    """
    cursor = conn.cursor()
    base_filter = f" WHERE {where_clause}" if where_clause else ""

    # Get total non-null rows
    count_sql = f'SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"{base_filter}'
    cursor.execute(count_sql)
    total_rows = cursor.fetchone()[0]

    if total_rows == 0:
        cursor.close()
        return {"correctness_rate": 0.0, "total_rows": 0, "details": []}

    details = []
    total_weight = 0.0
    weighted_score = 0.0

    for check in checks:
        check_type = check.get("type", "")
        weight = check.get("weight", 1.0)

        try:
            if check_type == "not_null":
                sql = f"""
                SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"{base_filter}
                {"AND" if where_clause else "WHERE"} "{column}" IS NOT NULL
                AND TRIM(CAST("{column}" AS VARCHAR)) != ''
                """
                cursor.execute(sql)
                valid_count = cursor.fetchone()[0]
                rate = (valid_count / total_rows * 100) if total_rows > 0 else 0

            elif check_type == "pattern":
                pattern = check.get("pattern", ".*")
                sql = f"""
                SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"{base_filter}
                {"AND" if where_clause else "WHERE"} RLIKE(CAST("{column}" AS VARCHAR), '{pattern}')
                """
                cursor.execute(sql)
                valid_count = cursor.fetchone()[0]
                rate = (valid_count / total_rows * 100) if total_rows > 0 else 0

            elif check_type == "allowed_values":
                values = check.get("values", [])
                values_str = ", ".join([f"'{v}'" for v in values])
                sql = f"""
                SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"{base_filter}
                {"AND" if where_clause else "WHERE"} CAST("{column}" AS VARCHAR) IN ({values_str})
                """
                cursor.execute(sql)
                valid_count = cursor.fetchone()[0]
                rate = (valid_count / total_rows * 100) if total_rows > 0 else 0

            elif check_type == "business_rule":
                rule_sql = check.get("sql", "1=1")
                sql = f"""
                SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"{base_filter}
                {"AND" if where_clause else "WHERE"} ({rule_sql})
                """
                cursor.execute(sql)
                valid_count = cursor.fetchone()[0]
                rate = (valid_count / total_rows * 100) if total_rows > 0 else 0

            elif check_type == "cross_column":
                ref_col = check.get("ref_column", "")
                comparison = check.get("comparison", "=")
                sql = f"""
                SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"{base_filter}
                {"AND" if where_clause else "WHERE"} "{column}" IS NOT NULL 
                AND "{ref_col}" IS NOT NULL
                AND "{column}" {comparison} "{ref_col}"
                """
                cursor.execute(sql)
                valid_count = cursor.fetchone()[0]
                # Denominator: non-null rows in both columns
                denom_sql = f"""
                SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"{base_filter}
                {"AND" if where_clause else "WHERE"} "{column}" IS NOT NULL AND "{ref_col}" IS NOT NULL
                """
                cursor.execute(denom_sql)
                denom = cursor.fetchone()[0]
                rate = (valid_count / denom * 100) if denom > 0 else 100.0

            elif check_type == "uniqueness":
                sql = f"""
                SELECT COUNT(DISTINCT "{column}") AS distinct_ct, COUNT("{column}") AS total_ct
                FROM "{database}"."{schema}"."{table}"{base_filter}
                """
                cursor.execute(sql)
                row = cursor.fetchone()
                distinct_ct = row[0] if row[0] else 0
                total_ct = row[1] if row[1] else 0
                rate = (distinct_ct / total_ct * 100) if total_ct > 0 else 100.0

            else:
                rate = 100.0  # Unknown check type

            details.append({
                "check_type": check_type,
                "label": check.get("label", check_type),
                "rate": round(rate, 2),
                "weight": weight,
                "description": check.get("description", "")
            })

            weighted_score += rate * weight
            total_weight += weight

        except Exception as e:
            details.append({
                "check_type": check_type,
                "label": check.get("label", check_type),
                "rate": 0.0,
                "weight": weight,
                "error": str(e)
            })
            total_weight += weight

    cursor.close()

    overall = (weighted_score / total_weight) if total_weight > 0 else 0.0

    return {
        "correctness_rate": round(overall, 2),
        "total_rows": total_rows,
        "details": details
    }


# ============================================================================
# Backend: Cross-Column Coherence
# ============================================================================

def check_cross_column_coherence(
    conn: SnowflakeConnection,
    database: str, schema: str, table: str,
    col_a: str, col_b: str,
    where_clause: str = None
) -> Dict:
    """
    Check coherence between two columns.
    For example: VAT number should map to a specific company name.
    Returns the incoherent rows and a coherence rate.
    """
    cursor = conn.cursor()
    base_filter = f" WHERE {where_clause}" if where_clause else ""

    # Check how many distinct (col_a, col_b) pairs exist vs distinct col_a
    sql = f"""
    SELECT
        COUNT(DISTINCT "{col_a}") AS distinct_a,
        COUNT(DISTINCT CONCAT("{col_a}", '||', "{col_b}")) AS distinct_pairs,
        COUNT(*) AS total_rows
    FROM "{database}"."{schema}"."{table}"{base_filter}
    WHERE "{col_a}" IS NOT NULL AND "{col_b}" IS NOT NULL
    """
    cursor.execute(sql)
    row = cursor.fetchone()

    distinct_a = row[0] if row[0] else 0
    distinct_pairs = row[1] if row[1] else 0
    total_rows = row[2] if row[2] else 0

    # If distinct_pairs == distinct_a, then each col_a maps to exactly one col_b (1:1)
    coherence_rate = (distinct_a / distinct_pairs * 100) if distinct_pairs > 0 else 100.0

    # Find incoherent values (col_a that maps to multiple col_b)
    incoherent_sql = f"""
    SELECT "{col_a}", LISTAGG(DISTINCT "{col_b}", ' | ') WITHIN GROUP (ORDER BY "{col_b}") AS values,
           COUNT(DISTINCT "{col_b}") AS variant_count
    FROM "{database}"."{schema}"."{table}"{base_filter}
    WHERE "{col_a}" IS NOT NULL AND "{col_b}" IS NOT NULL
    GROUP BY "{col_a}"
    HAVING COUNT(DISTINCT "{col_b}") > 1
    ORDER BY variant_count DESC
    LIMIT 50
    """
    cursor.execute(incoherent_sql)
    incoherent = []
    for r in cursor.fetchall():
        incoherent.append({
            "key_value": r[0],
            "conflicting_values": r[1],
            "variant_count": r[2]
        })

    cursor.close()

    return {
        "col_a": col_a,
        "col_b": col_b,
        "total_rows": total_rows,
        "distinct_keys": distinct_a,
        "distinct_pairs": distinct_pairs,
        "coherence_rate": round(coherence_rate, 2),
        "incoherent_count": len(incoherent),
        "incoherent_examples": incoherent
    }


# ============================================================================
# Backend: External API Validation (VAT / SIRET)
# ============================================================================

def validate_column_with_external_api(
    conn: SnowflakeConnection,
    database: str, schema: str, table: str,
    column: str,
    provider_type: str = "vat",
    where_clause: str = None,
    sample_limit: int = 100
) -> Dict:
    """
    Validate column values against an external API (VAT VIES, INSEE SIRET, etc.).
    Returns validation results with match rates.
    """
    cursor = conn.cursor()
    base_filter = f" WHERE {where_clause}" if where_clause else ""

    # Get sample values to validate
    sql = f"""
    SELECT DISTINCT CAST("{column}" AS VARCHAR) AS val
    FROM "{database}"."{schema}"."{table}"{base_filter}
    WHERE "{column}" IS NOT NULL AND TRIM(CAST("{column}" AS VARCHAR)) != ''
    LIMIT {sample_limit}
    """
    cursor.execute(sql)
    values = [row[0] for row in cursor.fetchall()]

    results = {
        "column": column,
        "provider_type": provider_type,
        "total_checked": len(values),
        "valid_count": 0,
        "invalid_count": 0,
        "error_count": 0,
        "validation_rate": 0.0,
        "details": []
    }

    if not values:
        cursor.close()
        return results

    # Use reference_data_providers if available
    try:
        from reference_data_providers import get_reference_registry, validate_with_cache

        registry = get_reference_registry()
        provider = registry.get_provider(provider_type)

        if provider:
            for val in values:
                try:
                    result = validate_with_cache(conn, provider, val)
                    is_valid = result.get("is_valid", False)
                    if is_valid:
                        results["valid_count"] += 1
                    else:
                        results["invalid_count"] += 1
                    results["details"].append({
                        "value": val,
                        "is_valid": is_valid,
                        "status": result.get("status", "UNKNOWN"),
                        "source": result.get("source", "UNKNOWN")
                    })
                except Exception as e:
                    results["error_count"] += 1
                    results["details"].append({"value": val, "error": str(e)})
        else:
            # Fallback: basic format validation
            results = _basic_format_validation(values, provider_type, results)

    except ImportError:
        # Fallback: basic format validation
        results = _basic_format_validation(values, provider_type, results)

    checked = results["valid_count"] + results["invalid_count"]
    results["validation_rate"] = round((results["valid_count"] / checked * 100) if checked > 0 else 0, 2)

    cursor.close()
    return results


def _basic_format_validation(values: List[str], provider_type: str, results: Dict) -> Dict:
    """Fallback format-level validation when API is not available."""
    import re

    patterns = {
        "vat": r"^[A-Z]{2}\d{7,12}$",
        "siret": r"^\d{14}$",
        "siren": r"^\d{9}$",
        "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        "phone": r"^[\+]?[\d\s\-\(\)]{7,20}$",
        "postal_code": r"^\d{4,10}$",
        "country_code": r"^[A-Z]{2}$",
        "iban": r"^[A-Z]{2}\d{2}[A-Z0-9]{10,30}$",
    }

    pattern = patterns.get(provider_type)
    if pattern:
        for val in values:
            is_valid = bool(re.match(pattern, val.strip().upper()))
            if is_valid:
                results["valid_count"] += 1
            else:
                results["invalid_count"] += 1
            results["details"].append({
                "value": val,
                "is_valid": is_valid,
                "status": "FORMAT_VALID" if is_valid else "FORMAT_INVALID",
                "source": "FORMAT_CHECK"
            })
    else:
        # No known pattern — all considered valid format
        results["valid_count"] = len(values)
        for val in values:
            results["details"].append({
                "value": val,
                "is_valid": True,
                "status": "NO_CHECK",
                "source": "NONE"
            })

    return results


# ============================================================================
# Backend: LLM Similarity Detection
# ============================================================================

def check_similarity_with_llm(
    conn: SnowflakeConnection,
    value_a: str,
    value_b: str,
    model: str = "mistral-large2"
) -> Dict:
    """
    Use Cortex LLM to check if two values are semantically similar.
    """
    cursor = conn.cursor()

    prompt = f"""Compare these two values and determine if they refer to the same entity.
Value A: "{value_a}"
Value B: "{value_b}"

Respond with a JSON object:
{{"is_similar": true/false, "confidence": 0.0-1.0, "explanation": "brief reason"}}
Only output the JSON, nothing else."""

    try:
        sql = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt.replace("'", "''")}')"""
        cursor.execute(sql)
        response = cursor.fetchone()[0]

        # Parse JSON from response
        response = response.strip()
        if response.startswith("```"):
            response = response.split("```")[1].strip()
            if response.startswith("json"):
                response = response[4:].strip()

        result = json.loads(response)
        cursor.close()
        return {
            "value_a": value_a,
            "value_b": value_b,
            "is_similar": result.get("is_similar", False),
            "confidence": result.get("confidence", 0.0),
            "explanation": result.get("explanation", "")
        }
    except Exception as e:
        cursor.close()
        return {
            "value_a": value_a,
            "value_b": value_b,
            "is_similar": False,
            "confidence": 0.0,
            "explanation": f"Error: {str(e)}"
        }


def batch_similarity_check(
    conn: SnowflakeConnection,
    pairs: List[Tuple[str, str]],
    model: str = "mistral-large2"
) -> List[Dict]:
    """Check similarity for multiple pairs at once using Cortex."""
    cursor = conn.cursor()

    if not pairs:
        return []

    # Build a batch prompt
    pairs_text = "\n".join([f"Pair {i+1}: \"{a}\" vs \"{b}\"" for i, (a, b) in enumerate(pairs)])
    prompt = f"""Compare each pair of values and determine if they refer to the same entity.

{pairs_text}

Respond with a JSON array:
[{{"pair": 1, "is_similar": true/false, "confidence": 0.0-1.0, "explanation": "brief reason"}}, ...]
Only output the JSON array, nothing else."""

    try:
        sql = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt.replace("'", "''")}')"""
        cursor.execute(sql)
        response = cursor.fetchone()[0]

        response = response.strip()
        if response.startswith("```"):
            response = response.split("```")[1].strip()
            if response.startswith("json"):
                response = response[4:].strip()

        results_list = json.loads(response)
        cursor.close()

        output = []
        for i, (a, b) in enumerate(pairs):
            match = next((r for r in results_list if r.get("pair") == i + 1), {})
            output.append({
                "value_a": a,
                "value_b": b,
                "is_similar": match.get("is_similar", False),
                "confidence": match.get("confidence", 0.0),
                "explanation": match.get("explanation", "")
            })
        return output

    except Exception as e:
        cursor.close()
        return [{"value_a": a, "value_b": b, "is_similar": False, "confidence": 0.0,
                 "explanation": f"Error: {str(e)}"} for a, b in pairs]


# ============================================================================
# Backend: Impact Analysis (Downstream Tables)
# ============================================================================

def analyze_downstream_impact(
    conn: SnowflakeConnection,
    source_database: str, source_schema: str, source_table: str,
    source_column: str,
    target_database: str, target_schema: str, target_table: str,
    target_join_column: str,
    error_values: List[str] = None,
    error_condition: str = None,
    where_clause: str = None,
    limit: int = 200
) -> Dict:
    """
    Analyze the downstream impact of data quality errors.
    
    For example: errors in CUSTOMERS table → impacted INVOICES.
    
    Parameters:
        error_values: Explicit list of erroneous IDs to check
        error_condition: SQL condition to identify errors
        target_join_column: Column in target table to join on
    """
    cursor = conn.cursor()
    base_filter = f" AND {where_clause}" if where_clause else ""

    try:
        if error_values:
            # Explicit list of error IDs
            vals_str = ", ".join([f"'{v}'" for v in error_values[:1000]])
            sql = f"""
            SELECT t.*
            FROM "{target_database}"."{target_schema}"."{target_table}" t
            WHERE t."{target_join_column}" IN ({vals_str})
            {base_filter}
            LIMIT {limit}
            """
        elif error_condition:
            # Dynamic: find errors in source, then check impact
            sql = f"""
            SELECT t.*
            FROM "{target_database}"."{target_schema}"."{target_table}" t
            INNER JOIN "{source_database}"."{source_schema}"."{source_table}" s
                ON t."{target_join_column}" = s."{source_column}"
            WHERE {error_condition}
            {base_filter}
            LIMIT {limit}
            """
        else:
            cursor.close()
            return {"error": "Provide either error_values or error_condition"}

        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        impacted_rows = [dict(zip(columns, row)) for row in rows]

        # Count total impacted
        if error_values:
            vals_str = ", ".join([f"'{v}'" for v in error_values[:1000]])
            count_sql = f"""
            SELECT COUNT(*)
            FROM "{target_database}"."{target_schema}"."{target_table}" t
            WHERE t."{target_join_column}" IN ({vals_str})
            {base_filter}
            """
        else:
            count_sql = f"""
            SELECT COUNT(*)
            FROM "{target_database}"."{target_schema}"."{target_table}" t
            INNER JOIN "{source_database}"."{source_schema}"."{source_table}" s
                ON t."{target_join_column}" = s."{source_column}"
            WHERE {error_condition}
            {base_filter}
            """
        cursor.execute(count_sql)
        total_impacted = cursor.fetchone()[0]

        # Total in target table
        total_sql = f'SELECT COUNT(*) FROM "{target_database}"."{target_schema}"."{target_table}"'
        cursor.execute(total_sql)
        total_target = cursor.fetchone()[0]

        cursor.close()

        return {
            "source_table": f"{source_database}.{source_schema}.{source_table}",
            "source_column": source_column,
            "target_table": f"{target_database}.{target_schema}.{target_table}",
            "target_join_column": target_join_column,
            "total_impacted": total_impacted,
            "total_target_rows": total_target,
            "impact_rate": round((total_impacted / total_target * 100) if total_target > 0 else 0, 2),
            "impacted_rows_sample": impacted_rows[:limit],
            "columns": columns
        }
    except Exception as e:
        cursor.close()
        return {"error": str(e)}


# ============================================================================
# Backend: Business Rules from Natural Language → SQL
# ============================================================================

def nl_rule_to_sql(
    conn: SnowflakeConnection,
    natural_language_rule: str,
    columns: List[Dict],
    database: str, schema: str, table: str,
    model: str = "mistral-large2"
) -> Dict:
    """
    Convert a natural language business rule to a SQL WHERE clause.
    Returns the SQL condition and a description.
    """
    cursor = conn.cursor()

    columns_desc = "\n".join([
        f"- {c.get('column_name', c.get('COLUMN_NAME', ''))}: {c.get('data_type', c.get('DATA_TYPE', 'VARCHAR'))}"
        for c in columns
    ])

    prompt = f"""You are a data quality expert. Convert this business rule to a SQL condition.

Table: "{database}"."{schema}"."{table}"
Columns:
{columns_desc}

Business Rule (natural language):
"{natural_language_rule}"

Return a JSON object:
{{"sql_condition": "the SQL WHERE clause (without WHERE keyword)", "description": "what this checks", "columns_involved": ["col1", "col2"], "severity": "CRITICAL|WARNING|INFO"}}
Only output JSON, nothing else."""

    try:
        sql = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt.replace("'", "''")}')"""
        cursor.execute(sql)
        response = cursor.fetchone()[0]

        response = response.strip()
        if response.startswith("```"):
            response = response.split("```")[1].strip()
            if response.startswith("json"):
                response = response[4:].strip()

        result = json.loads(response)
        cursor.close()
        return result
    except Exception as e:
        cursor.close()
        return {
            "sql_condition": None,
            "description": natural_language_rule,
            "error": str(e)
        }


# ============================================================================
# UI: Render the full DQ Dashboard Tab
# ============================================================================

def render_dq_dashboard_tab(conn: SnowflakeConnection):
    """Render the Data Quality Dashboard tab in the Streamlit app."""
    from utils import cached_list_databases, cached_list_schemas, cached_list_tables, cached_get_columns

    st.markdown("### Data Quality Dashboard")
    st.caption("Profile your data, assess correctness per column, cross-validate with external sources, and analyze downstream impact")
    st.write("")

    # ===== STEP 1: Table Selection =====
    with st.container(border=True):
        st.markdown("**1. Select Source Table**")
        st.write("")
        col1, col2, col3 = st.columns(3)

        with col1:
            try:
                databases = cached_list_databases(conn)
                dq_db = st.selectbox("Database", options=databases,
                                     index=databases.index(st.session_state.selected_db) if st.session_state.get("selected_db") in databases else 0,
                                     key="dq_dash_db")
            except Exception:
                dq_db = None
                st.error("Cannot load databases")

        with col2:
            dq_schema = None
            if dq_db:
                try:
                    schemas = cached_list_schemas(conn, dq_db)
                    dq_schema = st.selectbox("Schema", options=schemas,
                                             index=schemas.index(st.session_state.selected_schema) if st.session_state.get("selected_schema") in schemas else 0,
                                             key="dq_dash_schema")
                except Exception:
                    st.error("Cannot load schemas")

        with col3:
            dq_table = None
            if dq_db and dq_schema:
                try:
                    tables = cached_list_tables(conn, dq_db, dq_schema)
                    dq_table = st.selectbox("Table", options=tables,
                                            index=tables.index(st.session_state.selected_table) if st.session_state.get("selected_table") in tables else 0,
                                            key="dq_dash_table")
                except Exception:
                    st.error("Cannot load tables")

    if not (dq_db and dq_schema and dq_table):
        st.info("Select a database, schema, and table to get started.")
        return

    # Load columns
    try:
        all_columns = cached_get_columns(conn, dq_db, dq_schema, dq_table)
        column_names = [c.get("column_name", c.get("COLUMN_NAME", "")) for c in all_columns]
    except Exception as e:
        st.error(f"Cannot load columns: {e}")
        return

    # ===== STEP 2: Filtering =====
    with st.container(border=True):
        st.markdown("**2. Filter Data (Optional)**")
        st.caption("Reduce the dataset before profiling")
        st.write("")

        # Let user pick a filter column and value
        filter_col = st.selectbox("Filter by column", options=["— No filter —"] + column_names, key="dq_filter_col")

        where_clause = None
        if filter_col and filter_col != "— No filter —":
            try:
                filter_values = get_filter_values(conn, dq_db, dq_schema, dq_table, filter_col)
                selected_filter_values = st.multiselect(
                    f"Select values for **{filter_col}**",
                    options=filter_values,
                    key="dq_filter_vals"
                )
                if selected_filter_values:
                    vals_str = ", ".join([f"'{v}'" for v in selected_filter_values])
                    where_clause = f'"{filter_col}" IN ({vals_str})'
                    st.info(f"Filter active: `{where_clause}`")
            except Exception as e:
                st.warning(f"Cannot load filter values: {e}")

    # ===== STEP 3: Column Selection =====
    with st.container(border=True):
        st.markdown("**3. Select Columns to Profile**")
        st.caption("Choose which columns to analyze for data quality")
        st.write("")

        # Quick selection buttons
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            if st.button("Select All", key="dq_select_all", use_container_width=True):
                st.session_state.dq_selected_columns = column_names
        with col_b:
            if st.button("Select None", key="dq_select_none", use_container_width=True):
                st.session_state.dq_selected_columns = []
        with col_c:
            if st.button("Auto-detect Important", key="dq_auto_detect", use_container_width=True, type="primary"):
                # Heuristic: select columns likely to have quality issues
                important = [c for c in column_names if any(
                    kw in c.upper() for kw in [
                        "NAME", "EMAIL", "PHONE", "ADDRESS", "CITY", "COUNTRY", "ZIP", "POSTAL",
                        "VAT", "SIRET", "SIREN", "TVA", "ID", "CODE", "STATUS", "TYPE",
                        "COMPANY", "CUSTOMER", "CLIENT", "INVOICE", "FACTURE", "AMOUNT", "DATE"
                    ]
                )]
                st.session_state.dq_selected_columns = important if important else column_names[:10]

        # Initialize if needed
        if "dq_selected_columns" not in st.session_state:
            st.session_state.dq_selected_columns = []

        selected_columns = st.multiselect(
            "Columns to analyze",
            options=column_names,
            default=st.session_state.get("dq_selected_columns", []),
            key="dq_column_multiselect_dash"
        )

    if not selected_columns:
        st.info("Select at least one column to analyze.")
        return

    # ===== STEP 4: Run Profiling =====
    st.write("")
    run_profile = st.button("Generate Data Quality Dashboard", type="primary", use_container_width=True, key="dq_run_profile")

    if run_profile:
        with st.spinner("Profiling selected columns..."):
            profile_results = profile_columns(conn, dq_db, dq_schema, dq_table, selected_columns, where_clause)
            st.session_state.dq_profile_results = profile_results
            st.session_state.dq_source_info = {
                "database": dq_db, "schema": dq_schema, "table": dq_table,
                "where_clause": where_clause
            }

    # ===== Display Results =====
    if st.session_state.get("dq_profile_results"):
        profile_results = st.session_state.dq_profile_results
        source_info = st.session_state.get("dq_source_info", {})

        st.write("")
        st.markdown("---")

        # ----- Overview Metrics -----
        st.markdown("#### Profiling Overview")
        total_rows = profile_results[0]["total_rows"] if profile_results else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Rows", f"{total_rows:,}")
        col2.metric("Columns Analyzed", len(profile_results))

        # Average fill rate
        avg_fill = sum(p["fill_rate"] for p in profile_results) / len(profile_results) if profile_results else 0
        col3.metric("Avg Fill Rate", f"{avg_fill:.1f}%")

        # Average completeness
        avg_complete = sum(p["completeness"] for p in profile_results) / len(profile_results) if profile_results else 0
        col4.metric("Avg Completeness", f"{avg_complete:.1f}%")

        # ----- Per-Column Cards -----
        st.write("")
        st.markdown("#### Column Quality Scores")

        # Build summary dataframe
        summary_data = []
        for p in profile_results:
            # Determine quality level
            fill = p["fill_rate"]
            if fill >= 95:
                quality_icon = "●"
                quality_label = "Excellent"
            elif fill >= 80:
                quality_icon = "●"
                quality_label = "Good"
            elif fill >= 50:
                quality_icon = "●"
                quality_label = "Fair"
            else:
                quality_icon = "●"
                quality_label = "Poor"

            summary_data.append({
                "": quality_icon,
                "Column": p["column"],
                "Fill Rate": f"{p['fill_rate']:.1f}%",
                "Null Rate": f"{p['null_rate']:.1f}%",
                "Empty Rate": f"{p['empty_rate']:.1f}%",
                "Completeness": f"{p['completeness']:.1f}%",
                "Distinct": p["distinct_count"],
                "Quality": quality_label,
                "Correctness": f"{p['correctness_rate']:.1f}%" if p.get("correctness_rate") is not None else "—",
            })

        summary_df = pd.DataFrame(summary_data)
        st.dataframe(summary_df, use_container_width=True, hide_index=True,
                      column_config={
                          "": st.column_config.TextColumn("", width="small"),
                          "Column": st.column_config.TextColumn("Column", width="medium"),
                          "Fill Rate": st.column_config.TextColumn("Fill Rate", width="small"),
                          "Null Rate": st.column_config.TextColumn("Null %", width="small"),
                          "Empty Rate": st.column_config.TextColumn("Empty %", width="small"),
                          "Completeness": st.column_config.TextColumn("Completeness", width="small"),
                          "Distinct": st.column_config.NumberColumn("Distinct", width="small"),
                          "Quality": st.column_config.TextColumn("Quality", width="small"),
                          "Correctness": st.column_config.TextColumn("Correctness", width="small"),
                      })

        # ----- Bar Chart: Fill Rate per Column -----
        st.write("")
        chart_data = pd.DataFrame({
            "Column": [p["column"] for p in profile_results],
            "Fill Rate (%)": [p["fill_rate"] for p in profile_results],
            "Null Rate (%)": [p["null_rate"] for p in profile_results],
            "Empty Rate (%)": [p["empty_rate"] for p in profile_results],
        })
        chart_data = chart_data.set_index("Column")
        st.bar_chart(chart_data, color=["#2ecc71", "#e74c3c", "#f39c12"])

        # ----- Detailed Column Analysis (expandable) -----
        st.write("")
        st.markdown("#### Detailed Column Analysis")

        for p in profile_results:
            col_name = p["column"]
            fill = p["fill_rate"]
            icon = "●" if fill >= 95 else ("●" if fill >= 80 else ("●" if fill >= 50 else "●"))

            with st.expander(f"{icon} **{col_name}** — Fill: {fill:.1f}% | Null: {p['null_rate']:.1f}% | Distinct: {p['distinct_count']}", expanded=False):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Fill Rate", f"{p['fill_rate']:.1f}%")
                c2.metric("Null Rate", f"{p['null_rate']:.1f}%")
                c3.metric("Empty Rate", f"{p['empty_rate']:.1f}%")
                c4.metric("Distinct Values", f"{p['distinct_count']}")

                if p.get("min_value"):
                    st.caption(f"**Range:** {p['min_value']} → {p['max_value']}")

                if p.get("top_values"):
                    st.caption("**Top Values:**")
                    top_df = pd.DataFrame(p["top_values"])
                    st.dataframe(top_df, use_container_width=True, hide_index=True)

                if p.get("error"):
                    st.error(f"Profiling error: {p['error']}")

        # ===== STEP 5: Business Rules (Natural Language) =====
        st.write("")
        st.markdown("---")
        st.markdown("#### Business Rules (Natural Language)")
        st.caption("Add company-specific rules in plain language. The AI will convert them to SQL checks.")

        with st.container(border=True):
            nl_rule = st.text_area(
                "Business rule in natural language",
                placeholder="Describe your business rule here...",
                key="dq_nl_rule",
                height=80
            )

            if st.button("Add & Check Rule", type="primary", key="dq_add_nl_rule"):
                if nl_rule.strip():
                    with st.spinner("Converting rule to SQL..."):
                        rule_result = nl_rule_to_sql(
                            conn, nl_rule.strip(), all_columns,
                            source_info.get("database", ""),
                            source_info.get("schema", ""),
                            source_info.get("table", "")
                        )

                    if rule_result.get("sql_condition"):
                        st.success(f"Rule converted: `{rule_result['sql_condition']}`")
                        st.caption(f"Severity: {rule_result.get('severity', 'WARNING')}")
                        st.caption(f"Columns: {', '.join(rule_result.get('columns_involved', []))}")

                        # Execute the rule immediately
                        with st.spinner("Checking rule against data..."):
                            try:
                                cursor = conn.cursor()
                                base_filter = f" WHERE {source_info.get('where_clause')}" if source_info.get("where_clause") else ""

                                # Count total
                                count_sql = f"""SELECT COUNT(*) FROM "{source_info['database']}"."{source_info['schema']}"."{source_info['table']}"{base_filter}"""
                                cursor.execute(count_sql)
                                total = cursor.fetchone()[0]

                                # Count passing
                                pass_sql = f"""SELECT COUNT(*) FROM "{source_info['database']}"."{source_info['schema']}"."{source_info['table']}"{base_filter}
                                {"AND" if source_info.get("where_clause") else "WHERE"} ({rule_result['sql_condition']})"""
                                cursor.execute(pass_sql)
                                passing = cursor.fetchone()[0]

                                violation_count = total - passing
                                pass_rate = (passing / total * 100) if total > 0 else 100

                                col1, col2, col3 = st.columns(3)
                                col1.metric("Total Rows", f"{total:,}")
                                col2.metric("Passing", f"{passing:,}")
                                col3.metric("Pass Rate", f"{pass_rate:.1f}%")

                                if violation_count > 0:
                                    st.warning(f"{violation_count:,} rows violate this rule")

                                    # Show sample violations
                                    violation_sql = f"""SELECT * FROM "{source_info['database']}"."{source_info['schema']}"."{source_info['table']}"{base_filter}
                                    {"AND" if source_info.get("where_clause") else "WHERE"} NOT ({rule_result['sql_condition']})
                                    LIMIT 20"""
                                    cursor.execute(violation_sql)
                                    cols = [desc[0] for desc in cursor.description]
                                    rows = cursor.fetchall()
                                    if rows:
                                        st.caption("Sample violations:")
                                        st.dataframe(pd.DataFrame(rows, columns=cols), use_container_width=True, hide_index=True)
                                else:
                                    st.success("All rows pass this rule!")

                                cursor.close()

                                # Store rule for future reference
                                if "dq_business_rules" not in st.session_state:
                                    st.session_state.dq_business_rules = []
                                st.session_state.dq_business_rules.append({
                                    "nl_rule": nl_rule.strip(),
                                    "sql_condition": rule_result["sql_condition"],
                                    "severity": rule_result.get("severity", "WARNING"),
                                    "columns": rule_result.get("columns_involved", []),
                                    "pass_rate": pass_rate,
                                    "total_rows": total,
                                    "violations": violation_count
                                })

                            except Exception as e:
                                st.error(f"Error executing rule: {e}")
                    else:
                        st.error(f"Could not convert rule: {rule_result.get('error', 'Unknown error')}")

            # Show existing business rules
            if st.session_state.get("dq_business_rules"):
                st.write("")
                st.markdown("**Active Business Rules:**")
                for idx, rule in enumerate(st.session_state.dq_business_rules):
                    severity_icon = {"CRITICAL": "[!]", "WARNING": "[~]", "INFO": "[i]"}.get(rule["severity"], "[-]")
                    st.markdown(f"{severity_icon} **{rule['nl_rule']}** — Pass rate: {rule['pass_rate']:.1f}% ({rule['violations']} violations)")

        # ===== STEP 6: Cross-Column Coherence =====
        st.write("")
        st.markdown("---")
        st.markdown("#### Cross-Column Coherence")
        st.caption("Check if two columns are consistent")

        with st.container(border=True):
            c1, c2 = st.columns(2)
            with c1:
                coherence_col_a = st.selectbox("Key column", options=column_names, key="dq_coherence_a")
            with c2:
                remaining = [c for c in column_names if c != coherence_col_a]
                coherence_col_b = st.selectbox("Dependent column", options=remaining, key="dq_coherence_b")

            if st.button("Check Coherence", type="primary", key="dq_check_coherence"):
                with st.spinner(f"Checking coherence: {coherence_col_a} → {coherence_col_b}..."):
                    result = check_cross_column_coherence(
                        conn, source_info["database"], source_info["schema"], source_info["table"],
                        coherence_col_a, coherence_col_b,
                        source_info.get("where_clause")
                    )

                col1, col2, col3 = st.columns(3)
                col1.metric("Coherence Rate", f"{result['coherence_rate']:.1f}%")
                col2.metric("Distinct Keys", result['distinct_keys'])
                col3.metric("Incoherent Keys", result['incoherent_count'])

                if result['incoherent_examples']:
                    st.warning(f"{result['incoherent_count']} keys map to multiple values")
                    incoherent_df = pd.DataFrame(result['incoherent_examples'])
                    incoherent_df.columns = [coherence_col_a, "Conflicting Values", "# Variants"]
                    st.dataframe(incoherent_df, use_container_width=True, hide_index=True)

                    # LLM Similarity check for incoherent pairs
                    st.write("")
                    st.markdown("**AI Similarity Analysis**")
                    st.caption("Check if conflicting values are actually synonyms")

                    if st.button("Run AI Similarity Check", key="dq_run_similarity"):
                        with st.spinner("Asking AI to compare values..."):
                            # Take top 10 incoherent pairs for analysis
                            pairs_to_check = []
                            for item in result['incoherent_examples'][:10]:
                                values = item['conflicting_values'].split(' | ')
                                if len(values) >= 2:
                                    pairs_to_check.append((values[0], values[1]))

                            if pairs_to_check:
                                sim_results = batch_similarity_check(conn, pairs_to_check)
                                for sr in sim_results:
                                    icon = "[Y]" if sr["is_similar"] else "[N]"
                                    conf = f"{sr['confidence']*100:.0f}%"
                                    st.markdown(f"{icon} **\"{sr['value_a']}\"** vs **\"{sr['value_b']}\"** — Similarity: {conf} — {sr['explanation']}")
                else:
                    st.success("Perfect coherence! Each key maps to exactly one value.")

        # ===== STEP 7: External API Validation =====
        st.write("")
        st.markdown("---")
        st.markdown("#### External Source Validation")
        st.caption("Cross-validate column values with external APIs (VAT VIES, SIRET INSEE, ISO codes, etc.)")

        with st.container(border=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                api_col = st.selectbox("Column to validate", options=column_names, key="dq_api_col")
            with c2:
                api_type = st.selectbox("Validation type", options=[
                    "vat", "siret", "siren", "email", "phone", "postal_code", "country_code", "iban"
                ], key="dq_api_type")
            with c3:
                api_sample_size = st.number_input("Sample size", min_value=10, max_value=500, value=100, key="dq_api_sample")

            if st.button("Validate with External Source", type="primary", key="dq_validate_api"):
                with st.spinner(f"Validating {api_col} as {api_type}..."):
                    api_result = validate_column_with_external_api(
                        conn, source_info["database"], source_info["schema"], source_info["table"],
                        api_col, api_type, source_info.get("where_clause"), api_sample_size
                    )

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Checked", api_result['total_checked'])
                col2.metric("Valid", api_result['valid_count'])
                col3.metric("Invalid", api_result['invalid_count'])
                col4.metric("Validation Rate", f"{api_result['validation_rate']:.1f}%")

                if api_result.get("details"):
                    # Show invalid ones
                    invalid_details = [d for d in api_result["details"] if not d.get("is_valid", True)]
                    if invalid_details:
                        st.warning(f"{len(invalid_details)} invalid values found")
                        invalid_df = pd.DataFrame(invalid_details[:50])
                        st.dataframe(invalid_df, use_container_width=True, hide_index=True)

        # ===== STEP 8: Downstream Impact Analysis =====
        st.write("")
        st.markdown("---")
        st.markdown("#### Downstream Impact Analysis")
        st.caption("See which invoices / transactions are affected by data quality errors upstream")

        with st.container(border=True):
            st.markdown("**Link to downstream table**")

            c1, c2 = st.columns(2)
            with c1:
                impact_source_col = st.selectbox("Source error column",
                                                  options=column_names, key="dq_impact_src_col")
                impact_error_condition = st.text_input(
                    "Error condition (SQL)",
                    placeholder='Enter SQL condition',
                    key="dq_impact_error_cond"
                )

            with c2:
                # Target table selection
                try:
                    impact_db = st.selectbox("Target DB", options=databases if 'databases' in dir() else cached_list_databases(conn),
                                             key="dq_impact_tdb")
                    impact_schemas = cached_list_schemas(conn, impact_db) if impact_db else []
                    impact_schema = st.selectbox("Target Schema", options=impact_schemas, key="dq_impact_tschema")
                    impact_tables = cached_list_tables(conn, impact_db, impact_schema) if impact_schema else []
                    impact_table = st.selectbox("Target Table", options=impact_tables, key="dq_impact_ttable")
                except Exception:
                    impact_db = impact_schema = impact_table = None
                    st.warning("Cannot load target table list")

            if impact_table:
                try:
                    target_cols = cached_get_columns(conn, impact_db, impact_schema, impact_table)
                    target_col_names = [c.get("column_name", c.get("COLUMN_NAME", "")) for c in target_cols]
                    impact_join_col = st.selectbox("Target join column",
                                                    options=target_col_names, key="dq_impact_join_col")
                except Exception:
                    impact_join_col = None

            if st.button("Analyze Impact", type="primary", key="dq_run_impact"):
                if not impact_error_condition.strip():
                    st.warning("Please enter an error condition")
                elif not impact_table or not impact_join_col:
                    st.warning("Please select a target table and join column")
                else:
                    with st.spinner("Analyzing downstream impact..."):
                        impact_result = analyze_downstream_impact(
                            conn,
                            source_info["database"], source_info["schema"], source_info["table"],
                            impact_source_col,
                            impact_db, impact_schema, impact_table,
                            impact_join_col,
                            error_condition=impact_error_condition.strip(),
                            where_clause=source_info.get("where_clause")
                        )

                    if impact_result.get("error"):
                        st.error(f"Error: {impact_result['error']}")
                    else:
                        st.write("")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Source Errors", impact_error_condition[:40])
                        col2.metric("Impacted Records", f"{impact_result['total_impacted']:,}")
                        col3.metric("Impact Rate", f"{impact_result['impact_rate']:.1f}%")

                        st.markdown(f"**{impact_result['total_impacted']:,}** records in **{impact_result['target_table']}** are impacted by data errors in **{impact_result['source_table']}**")

                        if impact_result.get("impacted_rows_sample"):
                            st.caption(f"Showing first {len(impact_result['impacted_rows_sample'])} impacted records:")
                            impacted_df = pd.DataFrame(impact_result["impacted_rows_sample"])
                            st.dataframe(impacted_df, use_container_width=True, hide_index=True)

                            # Export
                            csv = impacted_df.to_csv(index=False)
                            st.download_button(
                                "Download Impacted Records",
                                data=csv,
                                file_name=f"impacted_{impact_table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )

        # ===== STEP 9: Correctness Rate per Column (combined score) =====
        st.write("")
        st.markdown("---")
        st.markdown("#### Calculate Correctness Rate")
        st.caption("Combine all checks (completeness, format, business rules, external APIs) into a single score per column")

        if st.button("Calculate Correctness Rate for All Columns", type="primary", key="dq_calc_correctness", use_container_width=True):
            with st.spinner("Calculating correctness rates..."):
                for i, p in enumerate(profile_results):
                    col_name = p["column"]

                    # Build checks list from available data
                    checks = [
                        {"type": "not_null", "label": "Not Null/Empty", "weight": 1.0}
                    ]

                    # Add business rules if they involve this column
                    for rule in st.session_state.get("dq_business_rules", []):
                        if col_name in rule.get("columns", []):
                            checks.append({
                                "type": "business_rule",
                                "label": rule["nl_rule"][:50],
                                "sql": rule["sql_condition"],
                                "weight": 1.5 if rule["severity"] == "CRITICAL" else 1.0,
                                "description": rule["nl_rule"]
                            })

                    result = calculate_correctness_rate(
                        conn, source_info["database"], source_info["schema"], source_info["table"],
                        col_name, checks, source_info.get("where_clause")
                    )

                    profile_results[i]["correctness_rate"] = result["correctness_rate"]
                    profile_results[i]["correctness_details"] = result["details"]

                st.session_state.dq_profile_results = profile_results

            # Show updated summary
            st.success("Correctness rates calculated!")

            correctness_data = []
            for p in profile_results:
                rate = p.get("correctness_rate", 0)
                icon = "●" if rate >= 90 else ("●" if rate >= 70 else ("●" if rate >= 50 else "●"))
                correctness_data.append({
                    "": icon,
                    "Column": p["column"],
                    "Correctness Rate": f"{rate:.1f}%",
                    "Fill Rate": f"{p['fill_rate']:.1f}%",
                    "Null Rate": f"{p['null_rate']:.1f}%",
                    "Distinct": p["distinct_count"],
                })

            corr_df = pd.DataFrame(correctness_data)
            st.dataframe(corr_df, use_container_width=True, hide_index=True)

            # Bar chart with correctness rate
            chart_corr = pd.DataFrame({
                "Column": [p["column"] for p in profile_results],
                "Correctness Rate": [p.get("correctness_rate", 0) for p in profile_results],
            }).set_index("Column")
            st.bar_chart(chart_corr, color=["#3498db"])

        # ===== Export Dashboard =====
        st.write("")
        st.markdown("---")

        if st.button("Export Full Dashboard as CSV", use_container_width=True, key="dq_export_csv"):
            export_rows = []
            for p in profile_results:
                export_rows.append({
                    "Column": p["column"],
                    "Total Rows": p["total_rows"],
                    "Null Count": p["null_count"],
                    "Null Rate (%)": p["null_rate"],
                    "Empty Count": p["empty_count"],
                    "Empty Rate (%)": p["empty_rate"],
                    "Fill Rate (%)": p["fill_rate"],
                    "Completeness (%)": p["completeness"],
                    "Distinct Count": p["distinct_count"],
                    "Distinct Rate (%)": p["distinct_rate"],
                    "Correctness Rate (%)": p.get("correctness_rate", "N/A"),
                    "Min Value": p.get("min_value", ""),
                    "Max Value": p.get("max_value", ""),
                })

            export_df = pd.DataFrame(export_rows)
            csv = export_df.to_csv(index=False)
            st.download_button(
                "Download CSV",
                data=csv,
                file_name=f"dq_dashboard_{source_info.get('table', 'table')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )            
            # ============================================================================
            # POC: Embedding-Based Anomaly Detection
            # ============================================================================
            # Render the POC section for DIM_ACCOUNT analysis
            try:
                render_embedding_dq_section(conn)
            except Exception as e:
                st.warning(f"POC section unavailable: {str(e)[:100]}")