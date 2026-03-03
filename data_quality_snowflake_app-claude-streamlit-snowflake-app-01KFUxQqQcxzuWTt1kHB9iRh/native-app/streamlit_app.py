"""
🧩 Semantic YAML Builder - Streamlit Application

A beautiful, user-friendly tool to generate, edit, and validate semantic YAML definitions for Snowflake tables.
"""

import streamlit as st
import json
import yaml
import pandas as pd
from typing import Optional
from datetime import datetime

from snowflake_utils import (
    get_connection_from_params,
    list_databases,
    list_schemas,
    list_tables,
    get_columns,
    get_primary_keys,
    generate_semantic_yaml_with_cortex,
    save_semantic_yaml,
    list_warehouses,
    list_roles,
    use_warehouse,
    use_role,
    add_dq_rule_from_natural_language,
    add_table_level_rule_from_natural_language,
    call_cortex_for_rule,
)
from semantic_yaml_spec import (
    generate_semantic_yaml_local,
    validate_semantic_yaml,
    auto_fix_yaml,
)

# ============================================================================
# Performance Optimizations
# ============================================================================

# Cache database metadata queries to avoid repeated calls
@st.cache_data(ttl=300, show_spinner=False)  # Cache for 5 minutes
def cached_list_databases(_conn):
    """Cached wrapper for list_databases."""
    return list_databases(_conn)

@st.cache_data(ttl=300, show_spinner=False)
def cached_list_schemas(_conn, database):
    """Cached wrapper for list_schemas."""
    return list_schemas(_conn, database)

@st.cache_data(ttl=300, show_spinner=False)
def cached_list_tables(_conn, database, schema):
    """Cached wrapper for list_tables."""
    return list_tables(_conn, database, schema)

@st.cache_data(ttl=300, show_spinner=False)
def cached_get_columns(_conn, database, schema, table):
    """Cached wrapper for get_columns."""
    return get_columns(_conn, database, schema, table)

@st.cache_data(ttl=300, show_spinner=False)
def cached_list_warehouses(_conn):
    """Cached wrapper for list_warehouses."""
    return list_warehouses(_conn)

@st.cache_data(ttl=300, show_spinner=False)
def cached_list_roles(_conn):
    """Cached wrapper for list_roles."""
    return list_roles(_conn)

# ============================================================================
# Page Configuration
# ============================================================================
st.set_page_config(
    page_title="Semantic YAML Builder",
    page_icon="🧩",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# Custom CSS - Enterprise Design System
# ============================================================================
st.markdown("""
<style>
    /* ==================== Design System Variables ==================== */
    :root {
        --primary-red: #EF4444;
        --gray-900: #111827;
        --gray-700: #374151;
        --gray-500: #6B7280;
        --gray-300: #D1D5DB;
        --gray-100: #F3F4F6;
        --sidebar-bg: #F7F8FA;
    }

    /* ==================== Performance Optimizations ==================== */
    /* Hide the grey overlay that appears during reruns */
    .stApp > div[data-testid="stale-element-container"] {
        display: none !important;
    }

    /* Speed up fade effects */
    .element-container {
        transition: opacity 0.05s !important;
    }

    /* Remove dimming effect on clicks */
    [data-testid="stAppViewContainer"] > .main {
        opacity: 1 !important;
    }

    /* Disable the running indicator overlay */
    div[data-testid="stStatusWidget"] {
        opacity: 0.3 !important;
    }

    /* ==================== Typography Hierarchy ==================== */
    /* H1: 28px / Bold (700) */
    h1 {
        font-size: 28px !important;
        font-weight: 700 !important;
        color: var(--gray-900) !important;
        margin-bottom: 0.5rem !important;
        line-height: 1.2 !important;
    }

    /* H2: 20px / Semi-Bold (600) */
    h2 {
        font-size: 20px !important;
        font-weight: 600 !important;
        color: var(--gray-900) !important;
        margin-top: 1rem !important;
        margin-bottom: 0.75rem !important;
    }

    /* H3: 16px / Semi-Bold (600) */
    h3 {
        font-size: 16px !important;
        font-weight: 600 !important;
        color: var(--gray-700) !important;
        margin-top: 0.75rem !important;
        margin-bottom: 0.5rem !important;
    }

    /* Body text: 14px */
    body, .stMarkdown, .stText, p {
        font-size: 14px !important;
        line-height: 1.5 !important;
        color: var(--gray-700) !important;
    }

    /* Caption text: 12px */
    .stCaption, small {
        font-size: 12px !important;
        color: var(--gray-500) !important;
    }

    /* ==================== Sidebar Styling ==================== */
    section[data-testid="stSidebar"] {
        background-color: var(--sidebar-bg) !important;
        width: 280px !important;
        min-width: 280px !important;
    }

    section[data-testid="stSidebar"] h2 {
        margin-top: 0.5rem !important;
        margin-bottom: 0.75rem !important;
        font-size: 18px !important;
        font-weight: 600 !important;
    }

    /* Sidebar footer */
    section[data-testid="stSidebar"] .stCaption {
        text-align: center;
        opacity: 0.6;
    }

    /* ==================== Tab Styling ==================== */
    /* Professional tabs with bottom border indicator */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0px;
        background-color: transparent;
        border-bottom: 1px solid var(--gray-300);
    }

    .stTabs [data-baseweb="tab"] {
        padding: 12px 24px;
        font-weight: 500;
        font-size: 14px;
        color: var(--gray-500);
        background-color: transparent;
        border: none;
        border-bottom: 2px solid transparent;
        border-radius: 0;
        transition: all 0.2s ease;
    }

    .stTabs [data-baseweb="tab"]:hover {
        color: var(--gray-700);
        background-color: rgba(239, 68, 68, 0.05);
        border-bottom-color: var(--gray-300);
    }

    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        color: var(--primary-red);
        font-weight: 600;
        border-bottom-color: var(--primary-red);
        background-color: transparent;
    }

    /* ==================== Card System ==================== */
    /* Bordered containers with proper shadows */
    div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"] {
        border-radius: 12px;
    }

    [data-testid="stVerticalBlock"]:has(> div[style*="border"]) {
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
        border-radius: 12px;
        transition: box-shadow 0.2s ease;
    }

    [data-testid="stVerticalBlock"]:has(> div[style*="border"]):hover {
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }

    /* Container borders */
    div[data-testid="stHorizontalBlock"] > div {
        border-radius: 12px;
    }

    /* ==================== Button Styling ==================== */
    /* Primary buttons */
    .stButton > button[kind="primary"] {
        background-color: var(--primary-red);
        color: white;
        font-weight: 600;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        transition: background-color 0.2s ease;
    }

    .stButton > button[kind="primary"]:hover {
        background-color: #DC2626;
        border: none;
    }

    /* Secondary buttons */
    .stButton > button[kind="secondary"] {
        background-color: white;
        color: var(--gray-700);
        font-weight: 500;
        border: 1px solid var(--gray-300);
        border-radius: 8px;
        padding: 0.5rem 1rem;
        transition: all 0.2s ease;
    }

    .stButton > button[kind="secondary"]:hover {
        background-color: var(--gray-100);
        border-color: var(--gray-500);
    }

    /* ==================== Input Fields ==================== */
    .stTextInput > div > div > input {
        border-radius: 8px;
        border: 1px solid var(--gray-300);
        font-size: 14px;
    }

    .stTextInput > div > div > input:focus {
        border-color: var(--primary-red);
        box-shadow: 0 0 0 1px var(--primary-red);
    }

    .stSelectbox > div > div > div {
        border-radius: 8px;
        font-size: 14px;
    }

    /* ==================== Alert Boxes ==================== */
    .stSuccess, .stError, .stWarning, .stInfo {
        border-radius: 8px;
        border-left: 4px solid;
        font-size: 14px;
    }

    .stSuccess {
        border-left-color: #10B981;
    }

    .stError {
        border-left-color: var(--primary-red);
    }

    .stWarning {
        border-left-color: #F59E0B;
    }

    .stInfo {
        border-left-color: #3B82F6;
    }

    /* ==================== Code Editor ==================== */
    .stCodeBlock {
        border-radius: 8px;
        font-size: 13px;
    }

    /* ==================== Expander ==================== */
    .streamlit-expanderHeader {
        font-size: 14px;
        font-weight: 500;
        border-radius: 8px;
    }

    /* ==================== Data Tables ==================== */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# Session State Initialization
# ============================================================================
def init_session_state():
    """Initialize all session state variables."""
    defaults = {
        "connected": False,
        "connection": None,
        "yaml_content": "",
        "yaml_history": [],
        "yaml_history_index": -1,
        "editor_version": 0,
        "skip_auto_save": False,
        "selected_db": None,
        "selected_schema": None,
        "selected_table": None,
        "current_warehouse": None,
        "current_role": None,
        "show_column_selector": False,
        "available_columns": [],
        "last_generated": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# ============================================================================
# Helper Functions
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
# Sidebar - Connection Management
# ============================================================================
with st.sidebar:
    st.markdown("## Snowflake Connection")

    if st.session_state.connected:
        with st.container(border=True):
            st.success("Connected")

            if st.session_state.current_warehouse:
                st.caption(f"**Warehouse:** {st.session_state.current_warehouse}")
            if st.session_state.current_role:
                st.caption(f"**Role:** {st.session_state.current_role}")

            st.write("")  # spacing
            if st.button("Disconnect", use_container_width=True, type="secondary", key="disconnect_sidebar"):
                disconnect()
                st.rerun()

        conn = st.session_state.connection

        # Context Settings in expander
        with st.expander("Context Settings"):
            # Role selector
            try:
                roles = cached_list_roles(conn)
                current_role_idx = roles.index(st.session_state.current_role) if st.session_state.current_role in roles else 0
                selected_role = st.selectbox("Role", options=roles, index=current_role_idx, key="role_select")
                if selected_role != st.session_state.current_role:
                    use_role(conn, selected_role)
                    st.session_state.current_role = selected_role
                    st.rerun()
            except Exception as e:
                with st.expander("⚠ Role Loading Error"):
                    st.caption(str(e))

            # Warehouse selector
            try:
                warehouses = cached_list_warehouses(conn)
                current_wh_idx = warehouses.index(st.session_state.current_warehouse) if st.session_state.current_warehouse in warehouses else 0
                selected_wh = st.selectbox("Warehouse", options=warehouses, index=current_wh_idx, key="wh_select")
                if selected_wh != st.session_state.current_warehouse:
                    use_warehouse(conn, selected_wh)
                    st.session_state.current_warehouse = selected_wh
                    st.rerun()
            except Exception as e:
                with st.expander("⚠ Warehouse Loading Error"):
                    st.caption(str(e))

    else:
        st.info("Connect to get started")

        with st.container(border=True):
            url = st.text_input(
                "Snowflake URL",
                placeholder="https://mycompany.snowflakecomputing.com",
                help="Your Snowflake account URL"
            )
            user = st.text_input(
                "SSO Username",
                placeholder="your.email@company.com",
                help="Your SSO username/email"
            )

            if st.button("Connect via SSO", use_container_width=True, type="primary", key="connect_sso_sidebar"):
                if not url or not user:
                    st.error("Please fill in URL and User")
                else:
                    with st.spinner("Opening browser for SSO authentication..."):
                        if connect_to_snowflake(url, user):
                            st.success("Connected!")
                            st.rerun()

    # Footer
    st.markdown("---")
    st.caption("Semantic YAML Builder v2.0")

# ============================================================================
# Main Application Header
# ============================================================================
st.markdown("# Semantic YAML Builder")
st.caption("Generate and manage semantic view definitions for Snowflake tables")
st.write("")  # spacing

if not st.session_state.connected:
    st.info("Please connect to Snowflake using the sidebar to begin")
    st.stop()

conn = st.session_state.connection

# ============================================================================
# Main Tabs
# ============================================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "Overview",
    "Semantic Model",
    "Data Quality Rules",
    "Validate & Export"
])

# ============================================================================
# TAB 1: Overview (NEW)
# ============================================================================
with tab1:
    st.markdown("### Project Overview")
    st.write("")  # spacing

    # ===== Connection & Table Selection Card =====
    with st.container(border=True):
        st.markdown("**Source Table Selection**")
        st.write("")  # spacing

        col1, col2, col3 = st.columns(3)

        with col1:
            try:
                databases = cached_list_databases(conn)
                selected_db = st.selectbox(
                    "Database",
                    options=databases,
                    index=databases.index(st.session_state.selected_db) if st.session_state.selected_db in databases else 0,
                    key="db_select"
                )
                st.session_state.selected_db = selected_db
            except Exception as e:
                st.error(f"Error loading databases: {e}")
                selected_db = None

        with col2:
            if selected_db:
                try:
                    schemas = cached_list_schemas(conn, selected_db)
                    selected_schema = st.selectbox(
                        "Schema",
                        options=schemas,
                        index=schemas.index(st.session_state.selected_schema) if st.session_state.selected_schema in schemas else 0,
                        key="schema_select"
                    )
                    st.session_state.selected_schema = selected_schema
                except Exception as e:
                    st.error(f"Error loading schemas: {e}")
                    selected_schema = None
            else:
                selected_schema = None
                st.selectbox("Schema", options=[], disabled=True)

        with col3:
            if selected_db and selected_schema:
                try:
                    tables = cached_list_tables(conn, selected_db, selected_schema)
                    if tables:
                        selected_table = st.selectbox(
                            "Table",
                            options=tables,
                            index=tables.index(st.session_state.selected_table) if st.session_state.selected_table in databases else 0,
                            key="table_select"
                        )
                        st.session_state.selected_table = selected_table
                    else:
                        st.info("No tables found in this schema")
                        selected_table = None
                except Exception as e:
                    st.error(f"Error loading tables: {e}")
                    selected_table = None
            else:
                selected_table = None
                st.selectbox("Table", options=[], disabled=True)

    st.write("")  # spacing

    # ===== Stats & Quick Info Card =====
    if selected_db and selected_schema and selected_table:
        with st.container(border=True):
            st.markdown("**Current Project**")
            st.code(f"{selected_db}.{selected_schema}.{selected_table}", language="sql")

            st.write("")  # spacing

            # Get table and YAML info
            try:
                table_columns = cached_get_columns(conn, selected_db, selected_schema, selected_table)
                num_columns = len(table_columns)

                # Count rules if YAML exists
                total_rules = 0
                yaml_status = "Not Generated"
                if st.session_state.yaml_content.strip():
                    yaml_status = "Generated"
                    try:
                        parsed = yaml.safe_load(st.session_state.yaml_content)
                        sv = parsed.get("semantic_view", {})
                        # Count column-level rules
                        columns = sv.get("columns", [])
                        for col in columns:
                            rules = col.get("dq_rules", [])
                            if isinstance(rules, list):
                                total_rules += len(rules)
                        # Count table-level rules
                        table_rules = sv.get("table_rules", [])
                        if isinstance(table_rules, list):
                            total_rules += len(table_rules)
                    except:
                        pass

                # Display metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Table Columns", num_columns)
                with col2:
                    st.metric("YAML Status", yaml_status)
                with col3:
                    st.metric("DQ Rules", total_rules)
                with col4:
                    validation_status = "Unknown"
                    if st.session_state.yaml_content.strip():
                        result = validate_semantic_yaml(st.session_state.yaml_content)
                        validation_status = "Valid" if result["valid"] else "Invalid"
                    st.metric("Validation", validation_status)

            except Exception as e:
                st.warning(f"Could not retrieve table details: {e}")

        st.write("")  # spacing

        # ===== Quick Actions Strip =====
        with st.container(border=True):
            st.markdown("**Quick Actions**")
            st.write("")  # spacing

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                if st.button("Generate YAML", type="primary", use_container_width=True, key="generate_yaml_overview"):
                    # Navigate to Semantic Model tab (tab2)
                    st.info("Go to 'Semantic Model' tab to generate YAML")

            with col2:
                if st.button("Validate", type="secondary", use_container_width=True, key="validate_overview"):
                    if not st.session_state.yaml_content.strip():
                        st.warning("No YAML to validate. Generate YAML first.")
                    else:
                        result = validate_semantic_yaml(st.session_state.yaml_content)
                        if result["valid"]:
                            st.success("YAML is valid!")
                        else:
                            st.error("Validation failed:")
                            for error in result["errors"]:
                                st.warning(f"• {error}")

            with col3:
                if st.button("Export YAML", type="secondary", use_container_width=True, key="export_yaml_overview"):
                    if not st.session_state.yaml_content.strip():
                        st.warning("No YAML to export. Generate YAML first.")
                    else:
                        st.info("Go to 'Validate & Export' tab to download or save to Snowflake")

            with col4:
                if st.button("View Full YAML", type="secondary", use_container_width=True, key="view_full_yaml_overview"):
                    if not st.session_state.yaml_content.strip():
                        st.warning("No YAML generated yet")
                    else:
                        st.info("Go to 'Semantic Model' tab to edit the YAML")
    else:
        st.info("Select a database, schema, and table to begin")

# ============================================================================
# ============================================================================
# TAB 2: Semantic Model (Combined Generation + Editing)
# ============================================================================
with tab2:
    st.markdown("### Semantic Model")
    st.write("")  # spacing

    if not (st.session_state.selected_db and st.session_state.selected_schema and st.session_state.selected_table):
        st.warning("Please select a table in the 'Overview' tab first")
    else:
        selected_db = st.session_state.selected_db
        selected_schema = st.session_state.selected_schema
        selected_table = st.session_state.selected_table

        # Get column info
        try:
            table_columns = cached_get_columns(conn, selected_db, selected_schema, selected_table)
            num_columns = len(table_columns)
        except:
            num_columns = 0
            table_columns = []

        # ===== SPLIT LAYOUT: 35% Properties / 65% Editor =====
        col_left, col_right = st.columns([35, 65])

        # ========== LEFT COLUMN: Model Properties & Generation ==========
        with col_left:
            with st.container(border=True):
                st.markdown("**Model Properties**")
                st.write("")  # spacing

                # Initialize session state for metadata
                if "metadata_source_system" not in st.session_state:
                    st.session_state.metadata_source_system = "GENERIC"
                if "metadata_business_domain" not in st.session_state:
                    st.session_state.metadata_business_domain = ""
                if "metadata_entity_type" not in st.session_state:
                    from snowflake_utils import suggest_entity_type
                    st.session_state.metadata_entity_type = suggest_entity_type(selected_table) or ""
                if "metadata_view_name" not in st.session_state:
                    st.session_state.metadata_view_name = f"{selected_table}_semantic_view"
                if "metadata_view_description" not in st.session_state:
                    st.session_state.metadata_view_description = ""

                from doc_snippets import AVAILABLE_SOURCE_SYSTEMS, AVAILABLE_BUSINESS_DOMAINS, COMMON_ENTITY_TYPES

                # View Name
                view_name = st.text_input(
                    "View Name",
                    value=st.session_state.metadata_view_name,
                    help="The name of the semantic view to be created",
                    key="view_name_input"
                )
                st.session_state.metadata_view_name = view_name

                # View Description
                view_description = st.text_area(
                    "View Description",
                    value=st.session_state.metadata_view_description,
                    height=80,
                    help="A description of what this semantic view represents",
                    key="view_description_input"
                )
                st.session_state.metadata_view_description = view_description

                source_system = st.selectbox(
                    "Source System",
                    options=AVAILABLE_SOURCE_SYSTEMS,
                    index=AVAILABLE_SOURCE_SYSTEMS.index(st.session_state.metadata_source_system),
                    help="The source system where this table originates",
                    key="source_system_select"
                )
                st.session_state.metadata_source_system = source_system

                business_domain = st.selectbox(
                    "Business Domain",
                    options=[""] + AVAILABLE_BUSINESS_DOMAINS,
                    index=0 if not st.session_state.metadata_business_domain else AVAILABLE_BUSINESS_DOMAINS.index(st.session_state.metadata_business_domain) + 1,
                    help="The business domain this table belongs to",
                    key="business_domain_select"
                )
                st.session_state.metadata_business_domain = business_domain

                entity_type = st.selectbox(
                    "Entity Type",
                    options=[""] + COMMON_ENTITY_TYPES,
                    index=0 if not st.session_state.metadata_entity_type else (COMMON_ENTITY_TYPES.index(st.session_state.metadata_entity_type) + 1 if st.session_state.metadata_entity_type in COMMON_ENTITY_TYPES else 0),
                    help="The type of business entity this table represents",
                    key="entity_type_select"
                )
                st.session_state.metadata_entity_type = entity_type

            st.write("")  # spacing

            # Generation options
            with st.container(border=True):
                st.markdown("**Generate YAML**")
                st.write("")  # spacing

                if num_columns > 10:
                    st.caption(f"Large table ({num_columns} columns) - select columns for DQ rules")

                if st.button("Generate with AI", type="primary", use_container_width=True, key="generate_with_ai"):
                    if num_columns > 10:
                        st.session_state.show_column_selector = True
                        st.session_state.available_columns = [c["column_name"] for c in table_columns]
                        st.rerun()
                    else:
                        with st.spinner("Calling Snowflake Cortex AI..."):
                            try:
                                yaml_content = generate_semantic_yaml_with_cortex(
                                    conn, selected_db, selected_schema, selected_table,
                                    source_system=st.session_state.metadata_source_system,
                                    business_domain=st.session_state.metadata_business_domain or None,
                                    entity_type=st.session_state.metadata_entity_type or None,
                                    view_name=st.session_state.metadata_view_name or None,
                                    description=st.session_state.metadata_view_description or None
                                )
                                st.session_state.yaml_content = yaml_content
                                st.session_state.last_generated = datetime.now()
                                save_to_history(yaml_content)
                                st.success("YAML generated via AI")
                                st.rerun()
                            except Exception as e:
                                st.error(f"AI generation failed: {e}")

                st.write("")  # spacing

                if st.button("Generate Locally", use_container_width=True, key="generate_locally"):
                    with st.spinner("Generating YAML..."):
                        try:
                            columns = cached_get_columns(conn, selected_db, selected_schema, selected_table)
                            primary_keys = get_primary_keys(conn, selected_db, selected_schema, selected_table)
                            yaml_content = generate_semantic_yaml_local(
                                database=selected_db,
                                schema=selected_schema,
                                table=selected_table,
                                columns=columns,
                                primary_keys=primary_keys,
                                source_system=st.session_state.metadata_source_system,
                                business_domain=st.session_state.metadata_business_domain or None,
                                entity_type=st.session_state.metadata_entity_type or None,
                                view_name=st.session_state.metadata_view_name or None,
                                description=st.session_state.metadata_view_description or None,
                            )
                            st.session_state.yaml_content = yaml_content
                            st.session_state.last_generated = datetime.now()
                            save_to_history(yaml_content)
                            st.success("YAML generated locally")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Generation failed: {e}")

            # Column selector for large tables
            if st.session_state.show_column_selector and num_columns > 10:
                st.write("")  # spacing
                st.markdown("**Select Columns for DQ Rules**")

                with st.container(border=True):
                    # Initialize
                    if "dq_column_multiselect" not in st.session_state:
                        st.session_state.dq_column_multiselect = []

                    # Multi-select
                    selected_dq_columns = st.multiselect(
                        "Choose columns:",
                        options=st.session_state.available_columns,
                        default=st.session_state.dq_column_multiselect,
                        key="dq_column_multiselect",
                        help="Select columns for DQ rules"
                    )

                    st.caption(f"Selected: **{len(selected_dq_columns)}** / {num_columns} columns")

                    # Generate/Cancel buttons
                    if st.button("Generate YAML", type="primary", key="generate_with_selection", use_container_width=True):
                        st.session_state.show_column_selector = False
                        with st.spinner("Generating with AI..."):
                            try:
                                yaml_content = generate_semantic_yaml_with_cortex(
                                    conn, selected_db, selected_schema, selected_table,
                                    dq_columns=selected_dq_columns if selected_dq_columns else None,
                                    source_system=st.session_state.metadata_source_system,
                                    business_domain=st.session_state.metadata_business_domain or None,
                                    entity_type=st.session_state.metadata_entity_type or None,
                                    view_name=st.session_state.metadata_view_name or None,
                                    description=st.session_state.metadata_view_description or None
                                )
                                st.session_state.yaml_content = yaml_content
                                st.session_state.last_generated = datetime.now()
                                save_to_history(yaml_content)
                                st.success("YAML generated")
                                st.rerun()
                            except Exception as e:
                                st.error(f"AI generation failed: {e}")

                    if st.button("Cancel", key="cancel_selection", use_container_width=True):
                        st.session_state.show_column_selector = False
                        st.rerun()

        # ========== RIGHT COLUMN: Column Descriptions + YAML Editor ==========
        with col_right:
            if not st.session_state.yaml_content.strip():
                st.info("No YAML generated yet. Use the form on the left to generate.")
            else:
                try:
                    parsed = yaml.safe_load(st.session_state.yaml_content)
                    sv = parsed.get("semantic_view", {})
                    source = sv.get("source", {})
                    columns = sv.get("columns", [])

                    # Sync generated description back to session state
                    if sv.get("description") and not st.session_state.metadata_view_description:
                        st.session_state.metadata_view_description = sv.get("description")

                    # Sync generated view name back to session state
                    if sv.get("name") and st.session_state.metadata_view_name != sv.get("name"):
                        st.session_state.metadata_view_name = sv.get("name")

                    source_key = f"{source.get('database', '')}_{source.get('schema', '')}_{source.get('table', '')}_{st.session_state.editor_version}"

                    # ===== SECTION 1: Column Descriptions Table (TOP) =====
                    with st.container(border=True):
                        st.markdown("**📋 Column Descriptions**")
                        st.write("")  # spacing

                        if columns:
                            col_data = []
                            for col in columns:
                                col_data.append({
                                    "Column": col.get("name", ""),
                                    "Label": col.get("label", ""),
                                    "Type": col.get("data_type", ""),
                                    "Description": col.get("description", "")
                                })

                            df = pd.DataFrame(col_data)
                            st.dataframe(
                                df,
                                use_container_width=True,
                                hide_index=True,
                                height=300,
                                column_config={
                                    "Column": st.column_config.TextColumn("Column", width="medium"),
                                    "Label": st.column_config.TextColumn("Label", width="medium"),
                                    "Type": st.column_config.TextColumn("Type", width="small"),
                                    "Description": st.column_config.TextColumn("Description", width="large"),
                                }
                            )
                        else:
                            st.info("No columns found in YAML")

                    st.write("")  # spacing

                    # ===== SECTION 2: YAML Definition (BELOW, with collapsible expander) =====
                    with st.expander("📝 YAML Definition", expanded=True):
                        # Toolbar
                        toolbar_col1, toolbar_col2, toolbar_col3, toolbar_col4 = st.columns(4)

                        with toolbar_col1:
                            can_undo = st.session_state.yaml_history_index > 0
                            if st.button("↶ Undo", disabled=not can_undo, use_container_width=True, key="undo_editor"):
                                undo()
                                st.rerun()

                        with toolbar_col2:
                            can_redo = st.session_state.yaml_history_index < len(st.session_state.yaml_history) - 1
                            if st.button("↷ Redo", disabled=not can_redo, use_container_width=True, key="redo_editor"):
                                redo()
                                st.rerun()

                        with toolbar_col3:
                            if st.button("Validate", use_container_width=True, key="validate_editor"):
                                result = validate_semantic_yaml(st.session_state.yaml_content)
                                if result["valid"]:
                                    st.success("Valid!")
                                else:
                                    st.error("Invalid!")
                                    for error in result["errors"]:
                                        st.warning(f"• {error}")

                        with toolbar_col4:
                            if st.button("Auto-Fix", use_container_width=True, key="autofix_editor"):
                                try:
                                    fixed_yaml = auto_fix_yaml(st.session_state.yaml_content)
                                    st.session_state.yaml_content = fixed_yaml
                                    save_to_history(fixed_yaml)
                                    st.session_state.editor_version += 1
                                    st.success("Fixed!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed: {e}")

                        st.write("")  # spacing

                        # YAML Text Editor
                        edited_yaml = st.text_area(
                            "Edit YAML directly:",
                            value=st.session_state.yaml_content,
                            height=400,
                            key=f"yaml_editor_{source_key}",
                            help="Edit the YAML definition directly. Changes are auto-saved."
                        )

                        # Auto-save changes
                        if edited_yaml != st.session_state.yaml_content:
                            if not st.session_state.skip_auto_save:
                                save_to_history(st.session_state.yaml_content)
                                st.session_state.yaml_content = edited_yaml
                                save_to_history(edited_yaml)
                            else:
                                st.session_state.skip_auto_save = False

                except yaml.YAMLError as e:
                    st.error(f"Invalid YAML: {e}")


# TAB 3: Data Quality Rules
# ============================================================================
with tab3:
    st.markdown("### Data Quality Rules")
    st.write("")  # spacing

    if not st.session_state.yaml_content.strip():
        st.info("No YAML definition generated yet. Go to 'Semantic Model' tab to generate.")
    else:
        try:
            parsed = yaml.safe_load(st.session_state.yaml_content)
            sv = parsed.get("semantic_view", {})
            source = sv.get("source", {})
            columns = sv.get("columns", [])

            source_key = f"{source.get('database', '')}_{source.get('schema', '')}_{source.get('table', '')}_{st.session_state.editor_version}"

            if not columns:
                st.warning("No columns found in the definition")
            else:
                # Build column-level rules data
                column_rules_data = []
                for col in columns:
                    col_name = col.get("name", "")
                    dq_rules = col.get("dq_rules") or []
                    if not isinstance(dq_rules, list):
                        continue
                    for rule in dq_rules:
                        if not rule or not isinstance(rule, dict):
                            continue
                        params = rule.get("params")
                        params_str = ""
                        if params:
                            if isinstance(params, dict):
                                params_str = ", ".join([f"{k}={v}" for k, v in params.items()])
                            else:
                                params_str = str(params)
                        column_rules_data.append({
                            "scope": "Column",
                            "column": col_name,
                            "type": rule.get("type", ""),
                            "severity": rule.get("severity", ""),
                            "description": rule.get("description", ""),
                            "params": params_str,
                            "lambda_hint": rule.get("lambda_hint", ""),
                        })

                # Build table-level rules data
                table_rules_data = []
                table_rules = sv.get("table_rules", [])
                for rule in table_rules:
                    if not rule or not isinstance(rule, dict):
                        continue
                    columns_involved = rule.get("columns", [])
                    columns_str = ", ".join(columns_involved) if isinstance(columns_involved, list) else str(columns_involved)

                    params = rule.get("params")
                    params_str = ""
                    if params:
                        if isinstance(params, dict):
                            params_str = ", ".join([f"{k}={v}" for k, v in params.items()])
                        else:
                            params_str = str(params)

                    table_rules_data.append({
                        "scope": "Table",
                        "column": columns_str,
                        "type": rule.get("type", ""),
                        "severity": rule.get("severity", ""),
                        "description": rule.get("description", ""),
                        "params": params_str,
                        "lambda_hint": rule.get("lambda_hint", ""),
                    })

                # Combine all rules
                all_rules_data = column_rules_data + table_rules_data

                # Rules summary
                with st.container(border=True):
                    st.markdown("**Rules Overview**")

                    col1, col2, col3, col4 = st.columns(4)
                    critical_count = len([r for r in all_rules_data if r["severity"] == "CRITICAL"])
                    warning_count = len([r for r in all_rules_data if r["severity"] == "WARNING"])
                    info_count = len([r for r in all_rules_data if r["severity"] == "INFO"])
                    cross_column_count = len(table_rules_data)

                    col1.metric("Critical", critical_count)
                    col2.metric("Warning", warning_count)
                    col3.metric("Info", info_count)
                    col4.metric("Cross-Column", cross_column_count)

                st.write("")  # spacing

                # Rules table
                st.markdown("**All Data Quality Rules**")

                if all_rules_data:
                    rules_df = pd.DataFrame(all_rules_data)

                    def severity_color(val):
                        colors = {"CRITICAL": "▪", "WARNING": "▪", "INFO": "▪"}
                        return colors.get(val, "")

                    rules_df[""] = rules_df["severity"].apply(severity_color)
                    rules_df = rules_df[["", "scope", "column", "type", "severity", "description", "params", "lambda_hint"]]

                    st.caption("Column-level rules can be edited inline. Table-level rules (cross-column) can be deleted but not edited inline - use 'Add Rule' section below.")

                    # Column-level rule types
                    col_rule_types = ["NOT_NULL", "UNIQUE", "MIN_VALUE", "MAX_VALUE", "ALLOWED_VALUES",
                                     "MAX_LENGTH", "PATTERN", "MAX_AGE_DAYS", "FOREIGN_KEY", "LOOKUP"]
                    # Table-level rule types
                    table_rule_types = ["COMPOSITE_UNIQUE", "CROSS_COLUMN_COMPARISON", "CONDITIONAL_REQUIRED",
                                       "MUTUAL_EXCLUSIVITY", "CONDITIONAL_VALUE"]
                    all_rule_types = col_rule_types + table_rule_types

                    edited_rules_df = st.data_editor(
                        rules_df,
                        column_config={
                            "": st.column_config.TextColumn("", width="small", disabled=True),
                            "scope": st.column_config.TextColumn("Scope", width="small", disabled=True),
                            "column": st.column_config.TextColumn("Column(s)", width="medium", disabled=True),
                            "type": st.column_config.SelectboxColumn(
                                "Rule Type",
                                width="medium",
                                options=all_rule_types
                            ),
                            "severity": st.column_config.SelectboxColumn(
                                "Severity",
                                width="small",
                                options=["CRITICAL", "WARNING", "INFO"]
                            ),
                            "description": st.column_config.TextColumn("Description", width="large"),
                            "params": st.column_config.TextColumn("Parameters", width="medium"),
                            "lambda_hint": st.column_config.TextColumn("SQL Expression", width="large"),
                        },
                        hide_index=True,
                        use_container_width=True,
                        num_rows="dynamic",
                        key=f"rules_editor_{source_key}"
                    )

                    # Save rules changes
                    if not st.session_state.skip_auto_save:
                        edited_records = [col.copy() for col in columns]
                        edited_rules = edited_rules_df.to_dict(orient="records")

                        # Separate column-level and table-level rules
                        column_level_rules = [r for r in edited_rules if r["scope"] == "Column"]
                        table_level_rules = [r for r in edited_rules if r["scope"] == "Table"]

                        # Process column-level rules
                        rules_by_column = {}
                        for rule in column_level_rules:
                            col_name = rule["column"]
                            if col_name not in rules_by_column:
                                rules_by_column[col_name] = []

                            params = None
                            if rule.get("params"):
                                try:
                                    params_str = rule["params"]
                                    if params_str:
                                        params = {}
                                        for pair in params_str.split(", "):
                                            if "=" in pair:
                                                k, v = pair.split("=", 1)
                                                try:
                                                    v = int(v)
                                                except ValueError:
                                                    try:
                                                        v = float(v)
                                                    except ValueError:
                                                        pass
                                                params[k] = v
                                except Exception:
                                    params = rule["params"]

                            rules_by_column[col_name].append({
                                "id": f"{col_name}_{rule['type']}".lower(),
                                "type": rule["type"],
                                "severity": rule["severity"],
                                "description": rule["description"],
                                "params": params,
                                "lambda_hint": rule.get("lambda_hint", "")
                            })

                        # Update columns with new rules
                        for i, rec in enumerate(edited_records):
                            col_name = rec["name"]
                            rec["dq_rules"] = rules_by_column.get(col_name, [])

                        sv["columns"] = edited_records

                        # Process table-level rules
                        updated_table_rules = []
                        for rule in table_level_rules:
                            columns_str = rule["column"]
                            columns_list = [c.strip() for c in columns_str.split(",")]

                            params = None
                            if rule.get("params"):
                                try:
                                    params_str = rule["params"]
                                    if params_str:
                                        params = {}
                                        for pair in params_str.split(", "):
                                            if "=" in pair:
                                                k, v = pair.split("=", 1)
                                                try:
                                                    v = int(v)
                                                except ValueError:
                                                    try:
                                                        v = float(v)
                                                    except ValueError:
                                                        pass
                                                params[k] = v
                                except Exception:
                                    params = rule["params"]

                            rule_id = "_".join(columns_list).lower() + "_" + rule["type"].lower()
                            updated_table_rules.append({
                                "id": rule_id,
                                "type": rule["type"],
                                "columns": columns_list,
                                "severity": rule["severity"],
                                "description": rule["description"],
                                "params": params,
                                "lambda_hint": rule.get("lambda_hint", "")
                            })

                        sv["table_rules"] = updated_table_rules
                        parsed["semantic_view"] = sv

                        new_yaml = yaml.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)
                        if new_yaml != st.session_state.yaml_content:
                            save_to_history(st.session_state.yaml_content)
                            st.session_state.yaml_content = new_yaml
                            save_to_history(new_yaml)
                    else:
                        st.session_state.skip_auto_save = False
                else:
                    st.info("ℹ️ No data quality rules defined yet. Use the form below to add rules.")

                # Add rule via NL
                st.markdown("---")
                st.markdown("#### ➕ Add Rule via Natural Language")
                st.caption("Describe a data quality rule in plain English and let AI translate it to a structured rule")

                # Rule scope selector
                rule_scope = st.radio(
                    "Rule Scope",
                    ["Single Column", "Multiple Columns (Cross-Column)"],
                    key=f"rule_scope_{source_key}",
                    horizontal=True,
                    help="Single Column: Rule applies to one column only. Multiple Columns: Rule involves relationships between 2+ columns (e.g., start_date < end_date)"
                )

                st.write("")  # spacing

                column_names_list = [col.get("name", "") for col in columns]

                if rule_scope == "Single Column":
                    # Single column selection
                    col1, col2 = st.columns([1, 2])

                    with col1:
                        selected_col_for_rule = st.selectbox(
                            "🎯 Select Column",
                            options=column_names_list,
                            key=f"add_rule_col_{source_key}"
                        )

                    with col2:
                        nl_rule_input = st.text_area(
                            "📝 Describe the rule",
                            placeholder="Example: 'must always be present'\nExample: 'cannot be negative'\nExample: 'should only be NEW, ACTIVE or LOST'",
                            height=100,
                            key=f"nl_rule_input_{source_key}"
                        )

                    if st.button("🚀 Add Rule", type="primary", key=f"add_rule_btn_{source_key}"):
                        if not nl_rule_input.strip():
                            st.warning("⚠️ Please enter a rule description")
                        elif not selected_col_for_rule:
                            st.warning("⚠️ Please select a column")
                        else:
                            with st.spinner("🔄 Translating rule via AI..."):
                                try:
                                    def llm_fn(prompt):
                                        return call_cortex_for_rule(conn, prompt)

                                    updated_yaml = add_dq_rule_from_natural_language(
                                        yaml_text=st.session_state.yaml_content,
                                        column_name=selected_col_for_rule,
                                        nl_rule=nl_rule_input,
                                        llm_call_fn=llm_fn
                                    )
                                    st.session_state.yaml_content = updated_yaml
                                    save_to_history(updated_yaml)
                                    st.session_state.editor_version += 1
                                    st.session_state.skip_auto_save = True
                                    st.success(f"✅ Rule added to column '{selected_col_for_rule}'!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Failed to add rule: {e}")

                else:  # Multiple Columns (Cross-Column)
                    # Multiple column selection
                    col1, col2 = st.columns([1, 2])

                    with col1:
                        selected_cols_for_rule = st.multiselect(
                            "🎯 Select Related Columns",
                            options=column_names_list,
                            key=f"add_rule_cols_{source_key}",
                            help="Select 2 or more columns that are involved in this rule"
                        )

                        if selected_cols_for_rule and len(selected_cols_for_rule) >= 1:
                            st.caption(f"✓ {len(selected_cols_for_rule)} column{'s' if len(selected_cols_for_rule) > 1 else ''} selected")

                    with col2:
                        nl_rule_input_multi = st.text_area(
                            "📝 Describe the relationship",
                            placeholder="Example: 'customer_id and order_id together must be unique'\nExample: 'start_date must be before end_date'\nExample: 'if status is COMPLETED, then end_date must be present'",
                            height=100,
                            key=f"nl_rule_input_multi_{source_key}"
                        )

                    if st.button("🚀 Add Cross-Column Rule", type="primary", key=f"add_multi_rule_btn_{source_key}"):
                        if not nl_rule_input_multi.strip():
                            st.warning("⚠️ Please enter a rule description")
                        elif not selected_cols_for_rule or len(selected_cols_for_rule) < 2:
                            st.warning("⚠️ Please select at least 2 columns for a cross-column rule")
                        else:
                            with st.spinner("🔄 Translating cross-column rule via AI..."):
                                try:
                                    def llm_fn(prompt):
                                        return call_cortex_for_rule(conn, prompt)

                                    updated_yaml = add_table_level_rule_from_natural_language(
                                        yaml_text=st.session_state.yaml_content,
                                        column_names=selected_cols_for_rule,
                                        nl_rule=nl_rule_input_multi,
                                        llm_call_fn=llm_fn
                                    )
                                    st.session_state.yaml_content = updated_yaml
                                    save_to_history(updated_yaml)
                                    st.session_state.editor_version += 1
                                    st.session_state.skip_auto_save = True
                                    columns_str = ", ".join(selected_cols_for_rule)
                                    st.success(f"✅ Cross-column rule added for: {columns_str}")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Failed to add rule: {e}")

        except yaml.YAMLError as e:
            st.error(f"❌ Invalid YAML: {e}")

# ============================================================================
# TAB 4: Validate & Export
# ============================================================================
with tab4:
    st.markdown("### Validate & Export")
    st.write("")  # spacing

    if not st.session_state.yaml_content.strip():
        st.info("No YAML definition generated yet. Go to 'Semantic Model' tab to generate.")
    else:
        # Validation section
        with st.container(border=True):
            st.markdown("**Validation & Preview**")

            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button("Validate Definition", type="secondary", use_container_width=True, key="validate_definition_export"):
                    result = validate_semantic_yaml(st.session_state.yaml_content)
                    if result["valid"]:
                        st.success("Definition is valid and ready to use")
                    else:
                        st.error("Validation failed:")
                        for error in result["errors"]:
                            st.warning(f"• {error}")

            with col2:
                if st.button("Auto-Fix Issues", type="secondary", use_container_width=True, key="autofix_export"):
                    try:
                        fixed_yaml = auto_fix_yaml(st.session_state.yaml_content)
                        st.session_state.yaml_content = fixed_yaml
                        save_to_history(fixed_yaml)
                        st.success("Auto-fixed missing values")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Auto-fix failed: {e}")

            with col3:
                show_final = st.button("Show Final View", type="primary", use_container_width=True, key="show_final_view")

        # Final view
        if show_final:
            result = validate_semantic_yaml(st.session_state.yaml_content)
            if not result["valid"]:
                st.error("Please fix validation errors first")
            elif result["parsed"]:
                sv = result["parsed"].get("semantic_view", {})

                st.write("")  # spacing
                with st.container(border=True):
                    st.markdown("**Semantic View Summary**")

                    source = sv.get("source", {})
                    target = sv.get("target", {})

                    st.markdown(f"**{sv.get('name', 'Unnamed View')}**")
                    st.caption(sv.get('description', ''))

                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Source**")
                        st.code(f"{source.get('database')}.{source.get('schema')}.{source.get('table')}")
                    with col2:
                        st.markdown("**Target**")
                        st.code(f"{target.get('database')}.{target.get('schema')}.{target.get('view_name')}")

                    st.write("")  # spacing

                    # Columns summary
                    columns = sv.get("columns", [])
                    st.markdown(f"**Columns** ({len(columns)} total)")

                    col_summary = []
                    total_column_rules = 0
                    for col in columns:
                        rules = col.get("dq_rules", [])
                        total_column_rules += len(rules)
                        col_summary.append({
                            "Column": col.get("name"),
                            "Label": col.get("label"),
                            "Type": col.get("data_type"),
                            "Role": col.get("role"),
                            "DQ Rules": len(rules)
                        })

                    st.dataframe(pd.DataFrame(col_summary), hide_index=True, use_container_width=True)

                    # Count table-level rules
                    table_rules = sv.get("table_rules", [])
                    total_table_rules = len(table_rules) if isinstance(table_rules, list) else 0
                    total_rules = total_column_rules + total_table_rules

                    # Rules summary
                    st.markdown(f"**Data Quality Rules** ({total_rules} total: {total_column_rules} column-level, {total_table_rules} cross-column)")

                rules_summary = []
                # Column-level rules
                for col in columns:
                    for rule in col.get("dq_rules", []):
                        if isinstance(rule, dict):
                            desc = rule.get("description", "")
                            short_desc = desc[:50] + "..." if len(desc) > 50 else desc
                            rules_summary.append({
                                "Scope": "Column",
                                "Column(s)": col.get("name"),
                                "Rule": rule.get("type"),
                                "Severity": rule.get("severity"),
                                "Description": short_desc
                            })

                # Table-level rules
                for rule in sv.get("table_rules", []):
                    if isinstance(rule, dict):
                        desc = rule.get("description", "")
                        short_desc = desc[:50] + "..." if len(desc) > 50 else desc
                        columns_str = ", ".join(rule.get("columns", []))
                        rules_summary.append({
                            "Scope": "Table",
                            "Column(s)": columns_str,
                            "Rule": rule.get("type"),
                            "Severity": rule.get("severity"),
                            "Description": short_desc
                        })

                if rules_summary:
                    st.dataframe(pd.DataFrame(rules_summary), hide_index=True, use_container_width=True)
                else:
                    st.info("No data quality rules defined")

                # Download button
                st.download_button(
                    label="⬇️ Download YAML File",
                    data=st.session_state.yaml_content,
                    file_name=f"{sv.get('name', 'semantic_view')}.yaml",
                    mime="text/yaml",
                    use_container_width=True
                )

        # Save to Snowflake
        st.markdown("---")
        st.markdown("#### 💾 Save to Snowflake Registry")

        with st.expander("📦 Save to Snowflake Registry (Optional)"):
            st.caption("Save your semantic YAML definition to the Snowflake registry table for centralized management")
            st.info("⚠️ This requires a `SEMANTIC_CONFIG.SEMANTIC_VIEW` table (see README for DDL)")

            save_status = st.selectbox("📊 Status", ["DRAFT", "ACTIVE", "ARCHIVED"], index=0)

            if st.button("💾 Save to Registry", type="primary", key="save_to_registry"):
                if not st.session_state.yaml_content.strip():
                    st.warning("⚠️ No YAML content to save")
                else:
                    result = validate_semantic_yaml(st.session_state.yaml_content)
                    if not result["valid"]:
                        st.error("❌ Please fix validation errors before saving")
                    elif result["parsed"]:
                        try:
                            sv = result["parsed"]["semantic_view"]
                            save_semantic_yaml(
                                conn=conn,
                                name=sv["name"],
                                version=sv.get("version", 1),
                                source_db=sv["source"]["database"],
                                source_schema=sv["source"]["schema"],
                                source_table=sv["source"]["table"],
                                target_db=sv["target"]["database"],
                                target_schema=sv["target"]["schema"],
                                target_view=sv["target"]["view_name"],
                                yaml_definition=st.session_state.yaml_content,
                                status=save_status,
                            )
                            st.success("✅ Saved to Snowflake registry!")
                        except Exception as e:
                            st.error(f"❌ Save failed: {e}")
