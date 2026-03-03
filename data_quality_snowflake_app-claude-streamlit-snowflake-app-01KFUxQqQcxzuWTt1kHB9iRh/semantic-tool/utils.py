"""
Utility functions module for LeonDQ application.
Contains helper functions for caching, data conversion, and logo handling.
"""

import streamlit as st
import os
import base64
import yaml
from typing import Optional
from decimal import Decimal
from datetime import datetime, date, time

from snowflake_utils import (
    list_databases,
    list_schemas,
    list_tables,
    get_columns,
    list_warehouses,
    list_roles,
    get_connection_from_params,
)


# ============================================================================
# Logo Functions
# ============================================================================

@st.cache_data(show_spinner=False)
def get_logo_path(theme: str) -> str:
    """Get the absolute path to the logo file based on theme. Cached for performance."""
    # Get the directory where this app.py file is located
    app_dir = os.path.dirname(os.path.abspath(__file__))
    # Build path to assets folder
    assets_dir = os.path.join(app_dir, "assets")
    # Use transparent logo for both themes
    logo_filename = "Logo_transparent.png"
    logo_path = os.path.join(assets_dir, logo_filename)
    return logo_path if os.path.exists(logo_path) else None


def get_logo_data_uri(theme: str) -> Optional[str]:
    """Return a base64 data URI for the logo image so we can embed it directly in HTML."""
    path = get_logo_path(theme)
    if not path:
        return None
    try:
        with open(path, "rb") as f:
            data = f.read()
        b64 = base64.b64encode(data).decode("utf-8")
        return f"data:image/png;base64,{b64}"
    except Exception:
        return None


# ============================================================================
# Performance Optimizations - Cached Database Queries
# ============================================================================

@st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour - databases rarely change
def cached_list_databases(_conn):
    """Cached wrapper for list_databases."""
    return list_databases(_conn)


@st.cache_data(ttl=1800, show_spinner=False)  # Cache for 30 minutes - schemas change infrequently
def cached_list_schemas(_conn, database):
    """Cached wrapper for list_schemas."""
    return list_schemas(_conn, database)


@st.cache_data(ttl=600, show_spinner=False)  # Cache for 10 minutes - tables change more often
def cached_list_tables(_conn, database, schema):
    """Cached wrapper for list_tables."""
    return list_tables(_conn, database, schema)


@st.cache_data(ttl=300, show_spinner=False)  # Cache for 5 minutes - columns may change during development
def cached_get_columns(_conn, database, schema, table):
    """Cached wrapper for get_columns."""
    return get_columns(_conn, database, schema, table)


@st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour - warehouses rarely change
def cached_list_warehouses(_conn):
    """Cached wrapper for list_warehouses."""
    return list_warehouses(_conn)


@st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour - roles rarely change
def cached_list_roles(_conn):
    """Cached wrapper for list_roles."""
    return list_roles(_conn)


# ============================================================================
# Data Conversion Functions
# ============================================================================

def convert_to_serializable(value):
    """Convert Snowflake types to YAML-serializable Python types."""
    if value is None:
        return None
    elif isinstance(value, Decimal):
        # Convert Decimal to float or int
        if value % 1 == 0:
            return int(value)
        else:
            return float(value)
    elif isinstance(value, (datetime, date, time)):
        # Convert datetime objects to ISO string
        return value.isoformat()
    elif isinstance(value, (bytes, bytearray)):
        # Convert binary to string
        try:
            return value.decode('utf-8')
        except:
            return str(value)
    elif isinstance(value, (list, tuple)):
        return [convert_to_serializable(v) for v in value]
    elif isinstance(value, dict):
        return {k: convert_to_serializable(v) for k, v in value.items()}
    else:
        # For other types, convert to string as fallback
        return str(value) if not isinstance(value, (int, float, str, bool)) else value


@st.cache_data(ttl=300, show_spinner=False)
def fetch_sample_values(_conn, database, schema, table, column_name, limit=5):
    """Fetch sample values from Snowflake table for a specific column."""
    cursor = _conn.cursor()
    try:
        full_table = f"{database}.{schema}.{table}"
        cursor.execute(f"""
            SELECT DISTINCT "{column_name}"
            FROM {full_table}
            WHERE "{column_name}" IS NOT NULL
            LIMIT {limit}
        """)
        raw_values = [row[0] for row in cursor.fetchall()]
        # Convert to serializable types for YAML
        return [convert_to_serializable(v) for v in raw_values]
    except Exception as e:
        st.warning(f"Could not fetch samples for {column_name}: {str(e)}")
        return []
    finally:
        cursor.close()


# ============================================================================
# YAML History Functions
# ============================================================================

def save_to_history(yaml_content):
    """Save current YAML to history for undo/redo."""
    if not yaml_content.strip():
        return
    if (st.session_state.yaml_history and
        st.session_state.yaml_history_index >= 0 and
        st.session_state.yaml_history[st.session_state.yaml_history_index] == yaml_content):
        return
    st.session_state.yaml_history = st.session_state.yaml_history[:st.session_state.yaml_history_index + 1]
    st.session_state.yaml_history.append(yaml_content)
    st.session_state.yaml_history_index = len(st.session_state.yaml_history) - 1
    if len(st.session_state.yaml_history) > 50:
        st.session_state.yaml_history = st.session_state.yaml_history[-50:]
        st.session_state.yaml_history_index = len(st.session_state.yaml_history) - 1


def undo():
    """Go back to previous state."""
    if st.session_state.yaml_history_index > 0:
        st.session_state.yaml_history_index -= 1
        st.session_state.yaml_content = st.session_state.yaml_history[st.session_state.yaml_history_index]
        st.session_state.editor_version += 1
        st.session_state.skip_auto_save = True


def redo():
    """Go forward to next state."""
    if st.session_state.yaml_history_index < len(st.session_state.yaml_history) - 1:
        st.session_state.yaml_history_index += 1
        st.session_state.yaml_content = st.session_state.yaml_history[st.session_state.yaml_history_index]
        st.session_state.editor_version += 1
        st.session_state.skip_auto_save = True


# ============================================================================
# Snowflake Connection Functions
# ============================================================================

def connect_to_snowflake(url: str, user: str):
    """Establish Snowflake connection via SSO."""
    try:
        conn = get_connection_from_params(snowflake_url=url, user=user)
        st.session_state.connection = conn
        st.session_state.connected = True
        st.session_state.conn_params = {"url": url, "user": user}
        return True
    except Exception as e:
        st.error(f"❌ Connection failed: {str(e)}")
        return False


def disconnect():
    """Close Snowflake connection."""
    if st.session_state.connection:
        try:
            st.session_state.connection.close()
        except Exception:
            pass
    st.session_state.connection = None
    st.session_state.connected = False
    st.session_state.yaml_content = ""
    st.session_state.current_warehouse = None
    st.session_state.current_role = None


# ============================================================================
# YAML Update Functions
# ============================================================================

def update_yaml_with_filters(yaml_content, filters):
    """Update YAML content with view-level filters without regenerating."""
    try:
        parsed = yaml.safe_load(yaml_content)
        if not parsed:
            return yaml_content

        # Add or update filters in the semantic_view section
        if "semantic_view" not in parsed:
            parsed["semantic_view"] = {}

        if filters:
            parsed["semantic_view"]["filters"] = filters
        elif "filters" in parsed["semantic_view"]:
            # Remove filters if empty
            del parsed["semantic_view"]["filters"]

        # Convert back to YAML
        updated_yaml = yaml.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)
        return updated_yaml
    except Exception as e:
        st.error(f"Error updating YAML with filters: {e}")
        return yaml_content
