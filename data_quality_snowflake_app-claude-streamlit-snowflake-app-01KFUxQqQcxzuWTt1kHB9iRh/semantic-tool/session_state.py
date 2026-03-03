"""
Session State module for LeonDQ application.
Handles initialization and management of session state variables.
"""

import streamlit as st


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
        "theme": "dark",  # "dark" or "light"
        "view_level_filters": [],
        "view_join_blocks": [],
        "view_temp_join_conditions": [],
        "dq_column_multiselect": [],
        "recently_used_databases": [],  # List of (db, timestamp) tuples
        "recently_used_schemas": [],    # List of (db, schema, timestamp) tuples
        "recently_used_tables": [],     # List of (db, schema, table, timestamp) tuples
        # DQ Dashboard state
        "dq_profile_results": None,
        "dq_source_info": {},
        "dq_selected_columns": [],
        "dq_business_rules": [],
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
