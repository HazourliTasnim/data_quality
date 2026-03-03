"""
LeonDQ - Semantic YAML Builder & Data Quality Tool for Snowflake

A modern, user-friendly platform to generate, edit, and validate semantic YAML definitions 
and manage data quality rules with AI-powered assistance.
"""

import streamlit as st
import json
import yaml
import pandas as pd
import os
from typing import Optional
from datetime import datetime

# Import Snowflake utilities
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
    auto_identify_and_create_rule,
    execute_all_rules,
)
from semantic_yaml_spec import (
    generate_semantic_yaml_local,
    validate_semantic_yaml,
    auto_fix_yaml,
)

# Import modular components
from styles import apply_base_styles, apply_light_theme, apply_dark_theme, apply_login_page_styles
from components import render_dark_card, render_header, render_section_title, render_status_badge
from utils import (
    get_logo_path,
    get_logo_data_uri,
    cached_list_databases,
    cached_list_schemas,
    cached_list_tables,
    cached_get_columns,
    cached_list_warehouses,
    cached_list_roles,
    convert_to_serializable,
    fetch_sample_values,
    save_to_history,
    undo,
    redo,
    connect_to_snowflake,
    disconnect,
    update_yaml_with_filters,
)
from session_state import init_session_state
from dq_dashboard import render_dq_dashboard_tab

# ============================================================================
# Icon Constants
# ============================================================================
WARNING_ICON = "⚠️"
DATABASE_ICON = "🗄️"
LINK_ICON = "🔗"
SEARCH_ICON = "🔍"
ROBOT_ICON = "🤖"
LIGHTBULB_ICON = "💡"
ROCKET_ICON = "🚀"
SYNC_ICON = "🔄"

# ============================================================================
# Page Configuration
# ============================================================================
st.set_page_config(
    page_title="LeonDQ - Semantic YAML Builder",
    page_icon="🧩",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# Apply Styles
# ============================================================================
apply_base_styles()

# ============================================================================
# Session State Initialization
# ============================================================================
init_session_state()

# ============================================================================
# Apply Theme
# ============================================================================
if st.session_state.theme == "light":
    apply_light_theme()
elif st.session_state.theme == "dark":
    apply_dark_theme()

# ============================================================================
# Check if Connected - Show Login or App
# ============================================================================

if not st.session_state.connected:
    # ============================================================================
    # LOGIN PAGE - Centered layout
    # ============================================================================
    st.markdown("""
    <style>
        /* DEBUG: Add visible borders to see what's being selected */

        /* Hide sidebar on login page */
        [data-testid="stSidebar"] {
            display: none !important;
        }

        /* Force the entire app container to center the login block vertically/horizontally */
        section.main {
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
            /* keep block centered but allow tighter fit on most laptop screens */
            min-height: 80vh !important;
            padding-top: 0rem !important;
            padding-bottom: 0rem !important;
            background-color: var(--background-color) !important;
        }

        /* Constrain the login block width and reduce outer padding so layout is tighter */
        section.main > div {
            max-width: 420px !important;
            width: 100% !important;
            margin: 0 auto !important;
            padding: 0.5rem  !important;
        }

        section.main .block-container {
            max-width: 420px !important;
            width: 100% !important;
            margin: 0 auto !important;
            padding: 0 !important;
            /* lift the whole login block higher to minimize top gap */
            transform: translateY(-18vh) !important;
        }

        /* Center logo and reduce vertical gap below it */
        section.main .stImage {
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
            margin: 0 0 0.25rem 0 !important;
        }

        section.main .stImage img {
            margin: 0 auto !important;
            display: block !important;
            max-width: 100% !important;
            height: auto !important;
        }

        /* Narrower login form wrapper to control form width */
        .leondq-login-form {
            max-width: 320px !important;
            margin: 0 auto !important;
        }

        /* Position theme toggle button in top-right */
        button[key="theme_toggle_login"] {
            position: fixed !important;
            top: 8px !important;
            right: 20px !important;
            z-index: 999999 !important;
        }

        /* If button selector doesn't work, try targeting its container */
        [data-testid="column"]:has(button[key="theme_toggle_login"]) {
            position: fixed !important;
            top: 8px !important;
            right: 20px !important;
            z-index: 999999 !important;
            width: auto !important;
        }
    </style>
    """, unsafe_allow_html=True)
    # Create a container for theme toggle (absolute positioned)
    col1, col2 = st.columns([10, 1])
    with col2:
        if st.button("🌙 Dark" if st.session_state.theme == "light" else "☀️ Light",
                     help="Switch theme",
                     type="secondary",
                     key="theme_toggle_login"):
            st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"
            st.rerun()

    # Logo - render centered via embedded HTML to guarantee centering
    logo_data = get_logo_data_uri(st.session_state.theme)
    if logo_data:
        st.markdown(
            f"<div style='display:flex;justify-content:center;align-items:center;margin-bottom:0.5rem;'><img src=\"{logo_data}\" style='max-width:240px;height:auto;display:block;margin:0 auto;'/></div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown("<h3 style='text-align: center;'>🧩 LeonDQ</h3>", unsafe_allow_html=True)

    # Tagline - centered below logo
    st.markdown("<p style='text-align: center; color: var(--text-muted); margin-bottom: 1rem; font-size: 14px;'>The AI assistant that cleans up your data before it costs you.</p>", unsafe_allow_html=True)

    # Small spacer and title (reduced header size)
    st.markdown("<div style='height: 0.25rem;'></div>", unsafe_allow_html=True)
    st.markdown("<h4 style='text-align: center; margin-bottom: 0.25rem; font-size: 18px;'>Connect to Snowflake</h4>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: var(--text-muted); margin-bottom: 0.5rem; font-size: 13px;'>Enter your credentials to get started</p>", unsafe_allow_html=True)

    # Put the form in a centered middle column to control width and ensure it fits one screen
    col_left, col_middle, col_right = st.columns([1, 2, 1])
    with col_middle:
        # add wrapper to constrain form width
        st.markdown("<div class='leondq-login-form'>", unsafe_allow_html=True)
        with st.container(border=True):
            url = st.text_input(
                "Snowflake URL",
                placeholder="https://mycompany.snowflakecomputing.com",
                label_visibility="collapsed"
            )
            user = st.text_input(
                "SSO Username",
                placeholder="your.email@company.com",
                label_visibility="collapsed"
            )

            # reduce extra vertical gap
            st.markdown("<div style='height: 0.25rem;'></div>", unsafe_allow_html=True)

            if st.button("Connect via SSO", width='stretch', type="primary", key="connect_sso_login"):
                if not url or not user:
                    st.error("Please fill in all fields")
                else:
                    with st.spinner("Opening browser for SSO..."):
                        if connect_to_snowflake(url, user):
                            st.success("Connected!")
                            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

else:
    # ============================================================================
    # MAIN APP - After login
    # ============================================================================
    with st.sidebar:
        # Logo at top of sidebar
        logo_path = get_logo_path(st.session_state.theme)
        if logo_path:
            st.image(logo_path, width=150)
        else:
            st.markdown("### 🧩 LeonDQ")

        # Tagline
        st.caption("The AI assistant that cleans up your data before it costs you.")
        st.write("")

        # Theme toggle button
        if st.button("🌙 Dark" if st.session_state.theme == "light" else "☀️ Light",
                     help="Switch theme",
                     type="secondary",
                     use_container_width=True,
                     key="theme_toggle_sidebar"):
            st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"
            st.rerun()

        st.markdown("---")

        # Sidebar header
        st.markdown("### Connection")
        st.caption("Snowflake workspace management")
        st.write("")

        with st.container(border=True):
            # Connection status - single line with dot and text
            st.markdown('<span class="connection-status-dot connected"></span> **Connected**', unsafe_allow_html=True)

            st.write("")  # spacing

            # PERFORMANCE: Show current context but don't load dropdowns until clicked
            st.caption(f"**Role:** {st.session_state.current_role or 'Default'}")
            st.caption(f"**Warehouse:** {st.session_state.current_warehouse or 'Default'}")

            # Button to open context settings
            if st.button("⚙️ Change Context", use_container_width=True, type="secondary", key="change_context_btn"):
                st.session_state.show_context_settings = True

            # Only show context settings if button was clicked
            if st.session_state.get("show_context_settings", False):
                with st.expander("Context Settings", expanded=True):
                    # Get connection from session state
                    conn = st.session_state.connection

                    # Role selector
                    try:
                        roles = cached_list_roles(conn)
                        current_role_idx = roles.index(st.session_state.current_role) if st.session_state.current_role in roles else 0
                        selected_role = st.selectbox("Role", options=roles, index=current_role_idx, key="role_select")
                        if selected_role != st.session_state.current_role:
                            use_role(conn, selected_role)
                            st.session_state.current_role = selected_role
                            st.session_state.show_context_settings = False
                            st.rerun()
                    except Exception as e:
                        st.caption(f"{WARNING_ICON} Could not load roles: {str(e)}", unsafe_allow_html=True)

                    # Warehouse selector
                    try:
                        warehouses = cached_list_warehouses(conn)
                        current_wh_idx = warehouses.index(st.session_state.current_warehouse) if st.session_state.current_warehouse in warehouses else 0
                        selected_wh = st.selectbox("Warehouse", options=warehouses, index=current_wh_idx, key="wh_select")
                        if selected_wh != st.session_state.current_warehouse:
                            use_warehouse(conn, selected_wh)
                            st.session_state.current_warehouse = selected_wh
                            st.session_state.show_context_settings = False
                            st.rerun()
                    except Exception as e:
                        st.caption(f"{WARNING_ICON} Could not load warehouses: {str(e)}", unsafe_allow_html=True)

                    if st.button("Done", use_container_width=True, key="done_context"):
                        st.session_state.show_context_settings = False
                        st.rerun()

            st.write("")  # spacing

            # Disconnect button
            if st.button("Disconnect", use_container_width=True, type="secondary", key="disconnect_sidebar"):
                disconnect()
                st.rerun()

        # Footer
        st.write("")
        st.markdown("---")
        st.caption("**LeonDQ** • v2.0")

    # Get connection from session state
    conn = st.session_state.connection

    # ============================================================================
    # Main Tabs
    # ============================================================================
    tab_dash, tab1, tab2, tab3, tab4, tab_dq, tab5 = st.tabs([
        "📊 Dashboard",
        "🏠 Overview",
        "🧬 Semantic Model",
        "✓ Quality Rules",
        "📊 Validate & Export",
        "🔍 DQ Dashboard",
        "📄 Document Quality"
    ])
    # ============================================================================
    # TAB 0: Dashboard
    # ============================================================================
    with tab_dash:
        st.markdown("<div style='margin-bottom: 2rem;'><h1 style='margin: 0; font-size: 2.5rem; font-weight: 700;'>📊 Dashboard</h1></div>", unsafe_allow_html=True)
        st.markdown("<p style='color: #888; font-size: 0.9rem; margin-bottom: 1.5rem;'>Overview of your data environment</p>", unsafe_allow_html=True)
        
        try:
            databases = cached_list_databases(conn)
            
            # ===== METRICS SECTION =====
            col1, col2, col3, col4 = st.columns(4, gap="large")
            
            with col1:
                db_count = len(databases) if databases else 0
                st.markdown(f"""
                <div style='background: #f8f9fa; padding: 1.2rem; border-radius: 8px; border-left: 4px solid #2c3e50; text-align: center;'>
                    <div style='font-size: 2.2rem; font-weight: 600; color: #2c3e50; margin-bottom: 0.3rem;'>{db_count}</div>
                    <div style='font-size: 0.85rem; color: #7f8c8d;'>Databases</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                all_tables = 0
                if databases:
                    for db in databases:
                        try:
                            schemas = cached_list_schemas(conn, db)
                            for schema in schemas:
                                tables = cached_list_tables(conn, db, schema)
                                all_tables += len(tables) if tables else 0
                        except:
                            pass
                st.markdown(f"""
                <div style='background: #f8f9fa; padding: 1.2rem; border-radius: 8px; border-left: 4px solid #34495e; text-align: center;'>
                    <div style='font-size: 2.2rem; font-weight: 600; color: #34495e; margin-bottom: 0.3rem;'>{all_tables}</div>
                    <div style='font-size: 0.85rem; color: #7f8c8d;'>Tables</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                last_val = "Never"
                if st.session_state.last_generated:
                    last_val = st.session_state.last_generated.strftime('%b %d')
                st.markdown(f"""
                <div style='background: #f8f9fa; padding: 1.2rem; border-radius: 8px; border-left: 4px solid #3498db; text-align: center;'>
                    <div style='font-size: 1.5rem; font-weight: 600; color: #3498db; margin-bottom: 0.3rem;'>⏱️</div>
                    <div style='font-size: 0.85rem; color: #7f8c8d;'>{last_val}</div>
                    <div style='font-size: 0.75rem; color: #95a5a6; margin-top: 0.3rem;'>Last Validation</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                rules_count = 0
                if st.session_state.yaml_content.strip():
                    try:
                        parsed = yaml.safe_load(st.session_state.yaml_content)
                        sv = parsed.get("semantic_view", {})
                        columns = sv.get("columns", [])
                        for col in columns:
                            rules = col.get("dq_rules", [])
                            if isinstance(rules, list):
                                rules_count += len(rules)
                        table_rules = sv.get("table_rules", [])
                        if isinstance(table_rules, list):
                            rules_count += len(table_rules)
                    except:
                        pass
                st.markdown(f"""
                <div style='background: #f8f9fa; padding: 1.2rem; border-radius: 8px; border-left: 4px solid #3498db; text-align: center;'>
                    <div style='font-size: 2.2rem; font-weight: 600; color: #3498db; margin-bottom: 0.3rem;'>{rules_count}</div>
                    <div style='font-size: 0.85rem; color: #7f8c8d;'>DQ Rules</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.write("")
            st.write("")
           
            # ===== MAIN CONTENT =====
            col_left, col_right = st.columns([1.3, 0.9], gap="large")
            
            with col_left:
                with st.container(border=True):
                    st.markdown("###  Database Distribution")
                    st.markdown("<p style='color: #888; font-size: 0.85rem; margin-bottom: 1rem;'>Tables per database (top 8)</p>", unsafe_allow_html=True)
                    
                    db_stats = {}
                    for db in databases:
                        try:
                            schemas = cached_list_schemas(conn, db)
                            db_table_count = 0
                            for schema in schemas:
                                tables = cached_list_tables(conn, db, schema)
                                db_table_count += len(tables) if tables else 0
                            if db_table_count > 0:
                                db_stats[db] = db_table_count
                        except:
                            pass
                    
                    if db_stats:
                        sorted_dbs = sorted(db_stats.items(), key=lambda x: x[1], reverse=True)
                        labels = [db[0] for db in sorted_dbs]
                        values = [db[1] for db in sorted_dbs]
                        
                        chart_data = pd.DataFrame({
                            'Database': labels,
                            'Tables': values
                        })
                        
                        st.bar_chart(chart_data.set_index('Database'), height=280, use_container_width=True)
                    else:
                        st.info("No databases available yet")
            
            with col_right:
                with st.container(border=True):
                    st.markdown("### 📌 Active Project")
                    
                    if st.session_state.yaml_content.strip():
                        try:
                            parsed = yaml.safe_load(st.session_state.yaml_content)
                            sv = parsed.get("semantic_view", {})
                            source = sv.get("source", {})
                            
                            st.markdown(f"<p style='font-size: 1.1rem; font-weight: 600; margin: 0.5rem 0;'>{sv.get('name', 'N/A')}</p>", unsafe_allow_html=True)
                            st.markdown("<hr style='margin: 0.7rem 0;'>", unsafe_allow_html=True)
                            
                            st.markdown(f"<p style='margin: 0.4rem 0; font-size: 0.9rem;'><strong>Source:</strong><br><code style='background: #f0f0f0; padding: 0.2rem 0.4rem; border-radius: 4px;'>{source.get('database')}.{source.get('schema')}.{source.get('table')}</code></p>", unsafe_allow_html=True)
                            
                            st.markdown(f"<p style='margin: 0.4rem 0; font-size: 0.9rem;'><strong>System:</strong> {sv.get('source_system', 'N/A')}</p>", unsafe_allow_html=True)
                            st.markdown(f"<p style='margin: 0.4rem 0; font-size: 0.9rem;'><strong>Domain:</strong> {sv.get('business_domain') or '—'}</p>", unsafe_allow_html=True)
                            st.markdown(f"<p style='margin: 0.4rem 0; font-size: 0.9rem;'><strong>Columns:</strong> {len(sv.get('columns', []))}</p>", unsafe_allow_html=True)
                            
                            is_valid = validate_semantic_yaml(st.session_state.yaml_content)["valid"]
                            status_color = "#10b981" if is_valid else "#ef4444"
                            status_text = "✓ Valid" if is_valid else "✗ Invalid"
                            st.markdown(f"<p style='margin-top: 0.7rem; color: {status_color}; font-weight: 600;'>{status_text}</p>", unsafe_allow_html=True)
                            
                            if st.session_state.last_generated:
                                st.markdown(f"<p style='margin-top: 0.7rem; color: #999; font-size: 0.8rem;'>Generated: {st.session_state.last_generated.strftime('%b %d, %H:%M')}</p>", unsafe_allow_html=True)
                        except:
                            st.warning("Error parsing YAML")
                    else:
                        st.markdown("<p style='color: #888; font-size: 0.95rem;'>No project configured yet.</p>", unsafe_allow_html=True)
                        st.markdown("<p style='color: #888; font-size: 0.9rem;'>👉 Select a table below to get started</p>", unsafe_allow_html=True)
            
            st.write("")
            st.write("")
            
            # ===== DATABASE BROWSER =====
            with st.container(border=True):
                st.markdown(f"### {DATABASE_ICON} Database Browser")
                st.markdown("<p style='color: #888; font-size: 0.85rem; margin-bottom: 1rem;'>Browse and select tables to validate</p>", unsafe_allow_html=True)
                
                tab_browser, tab_recent = st.tabs(["🏠 Browse", "⏱️ Recent"])
                
                with tab_browser:
                    col_db, col_schema, col_spacer = st.columns([1, 1, 0.5], gap="medium")
                    
                    with col_db:
                        selected_db = st.selectbox(
                            "Database",
                            options=databases,
                            key="dash_db_select",
                            placeholder="Select database..."
                        )
                        if selected_db and selected_db not in st.session_state.recently_used_databases:
                            st.session_state.recently_used_databases.insert(0, selected_db)
                            st.session_state.recently_used_databases = st.session_state.recently_used_databases[:10]
                    
                    with col_schema:
                        if selected_db:
                            schemas = cached_list_schemas(conn, selected_db)
                            selected_schema = st.selectbox(
                                "Schema",
                                options=schemas,
                                key="dash_schema_select",
                                placeholder="Select schema..."
                            )
                            if selected_schema:
                                schema_key = f"{selected_db}.{selected_schema}"
                                if schema_key not in st.session_state.recently_used_schemas:
                                    st.session_state.recently_used_schemas.insert(0, schema_key)
                                    st.session_state.recently_used_schemas = st.session_state.recently_used_schemas[:10]
                        else:
                            selected_schema = None
                            st.selectbox("Schema", options=[], disabled=True, placeholder="Select database first...")
                    
                    st.write("")
                    
                    if selected_db and selected_schema:
                        tables = cached_list_tables(conn, selected_db, selected_schema)
                        
                        if tables:
                            st.markdown(f"<p style='font-size: 0.95rem; color: #666; margin-bottom: 0.7rem;'><strong>{len(tables)}</strong> tables available</p>", unsafe_allow_html=True)
                            
                            # Create table options
                            table_options = []
                            for table in tables:
                                try:
                                    cols = cached_get_columns(conn, selected_db, selected_schema, table)
                                    num_cols = len(cols) if cols else 0
                                    table_options.append(f"{table} ({num_cols} cols)")
                                except:
                                    table_options.append(table)
                            
                            selected_table_option = st.selectbox(
                                "Select table",
                                options=table_options,
                                key="table_selector",
                                label_visibility="collapsed",
                                placeholder="Choose table..."
                            )
                            
                            if selected_table_option:
                                actual_table = selected_table_option.split(" (")[0]
                                
                                st.session_state.selected_db = selected_db
                                st.session_state.selected_schema = selected_schema
                                st.session_state.selected_table = actual_table
                                table_key = f"{selected_db}.{selected_schema}.{actual_table}"
                                if table_key not in st.session_state.recently_used_tables:
                                    st.session_state.recently_used_tables.insert(0, table_key)
                                    st.session_state.recently_used_tables = st.session_state.recently_used_tables[:10]
                                st.session_state.confirmed_table = table_key
                        else:
                            st.caption(f"📭 No tables in {selected_schema}")
                
                with tab_recent:
                    if not (st.session_state.recently_used_tables or st.session_state.recently_used_schemas or st.session_state.recently_used_databases):
                        st.info("Your recent items will appear here")
                    else:
                        if st.session_state.recently_used_tables:
                            st.markdown("**📌 Tables**")
                            cols = st.columns(len(st.session_state.recently_used_tables[:5]))
                            for idx, table_key in enumerate(st.session_state.recently_used_tables[:5]):
                                parts = table_key.split(".")
                                if len(parts) == 3:
                                    db, schema, table = parts
                                    with cols[idx]:
                                        if st.button(table, key=f"rec_t_{table_key}", use_container_width=True):
                                            st.session_state.selected_db = db
                                            st.session_state.selected_schema = schema
                                            st.session_state.selected_table = table
                                            st.session_state.confirmed_table = table_key
                                            st.rerun()
                            st.write("")
                        
                        if st.session_state.recently_used_schemas:
                            st.markdown(f"**{LINK_ICON} Schemas**")
                            cols = st.columns(len(st.session_state.recently_used_schemas[:4]))
                            for idx, schema_key in enumerate(st.session_state.recently_used_schemas[:4]):
                                parts = schema_key.split(".")
                                if len(parts) == 2:
                                    db, schema = parts
                                    with cols[idx]:
                                        if st.button(f"{db}.{schema}", key=f"rec_s_{schema_key}", use_container_width=True):
                                            st.session_state.selected_db = db
                                            st.session_state.selected_schema = schema
                                            st.rerun()
                            st.write("")
                        
                        if st.session_state.recently_used_databases:
                            st.markdown("**🗄️ Databases**")
                            cols = st.columns(len(st.session_state.recently_used_databases[:5]))
                            for idx, db in enumerate(st.session_state.recently_used_databases[:5]):
                                with cols[idx]:
                                    if st.button(db, key=f"rec_d_{db}", use_container_width=True):
                                        st.session_state.selected_db = db
                                        st.rerun()
        
        except Exception as e:
            st.error(f"Dashboard error: {e}")

    # ============================================================================
    # TAB 1: Overview
    # ============================================================================
    with tab1:
        st.markdown("### Project Overview")
        st.caption("Select a table and manage your semantic view definition")
        st.write("")

        # ===== TABLE SELECTION CARD =====
        with st.container(border=True):
            st.markdown("**Select Your Source Table**")
            st.write("")

            col1, col2, col3 = st.columns(3)

            with col1:
                try:
                    databases = cached_list_databases(conn)
                    selected_db = st.selectbox(
                        "Database",
                        options=databases,
                        index=databases.index(st.session_state.selected_db) if st.session_state.selected_db in databases else 0,
                        key="db_select",
                        label_visibility="collapsed"
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
                            key="schema_select",
                            label_visibility="collapsed"
                        )
                        st.session_state.selected_schema = selected_schema
                    except Exception as e:
                        st.error(f"Error loading schemas: {e}")
                        selected_schema = None
                else:
                    selected_schema = None
                    st.selectbox("Schema", options=[], disabled=True, label_visibility="collapsed")

            with col3:
                if selected_db and selected_schema:
                    try:
                        tables = cached_list_tables(conn, selected_db, selected_schema)
                        if tables:
                            selected_table = st.selectbox(
                                "Table",
                                options=tables,
                                index=tables.index(st.session_state.selected_table) if st.session_state.selected_table in tables else 0,
                                key="table_select",
                                label_visibility="collapsed"
                            )
                            st.session_state.selected_table = selected_table
                        else:
                            st.info("No tables in this schema")
                            selected_table = None
                    except Exception as e:
                        st.error(f"Error loading tables: {e}")
                        selected_table = None
                else:
                    selected_table = None
                    st.selectbox("Table", options=[], disabled=True, label_visibility="collapsed")

        st.write("")

        # ===== TABLE CONFIRMATION LOGIC =====
        if selected_db and selected_schema and selected_table:
            current_table_key = f"{selected_db}.{selected_schema}.{selected_table}"

            if "confirmed_table" not in st.session_state:
                st.session_state.confirmed_table = None

            table_needs_confirmation = st.session_state.confirmed_table != current_table_key

            if table_needs_confirmation:
                with st.container(border=True):
                    st.markdown("**Confirm Table Selection**")
                    st.code(f"{selected_db}.{selected_schema}.{selected_table}", language="sql")
                    st.write("")

                    col_btn1, col_btn2 = st.columns([1, 2])
                    with col_btn1:
                        if st.button("✓ Confirm", type="primary", use_container_width=True, key="confirm_table"):
                            st.session_state.confirmed_table = current_table_key
                            st.session_state.last_selected_table = selected_table
                            st.session_state.metadata_view_name = f"{selected_table}_semantic_view"

                            from snowflake_utils import suggest_entity_type
                            st.session_state.metadata_entity_type = suggest_entity_type(selected_table) or ""

                            st.success(f"Table locked: {selected_table}")
                            st.info("→ Go to 'Semantic Model' tab to generate YAML")
                            st.rerun()

                    with col_btn2:
                        st.caption("Lock in your table selection and update metadata")

                st.stop()

        # ===== STATS CARD =====
        if selected_db and selected_schema and selected_table:
            with st.container(border=True):
                st.markdown("**Current Project**")
                st.code(f"{selected_db}.{selected_schema}.{selected_table}", language="sql")

                st.write("")

                try:
                    table_columns = cached_get_columns(conn, selected_db, selected_schema, selected_table)
                    num_columns = len(table_columns)

                    total_rules = 0
                    yaml_status = "Not Generated"
                    if st.session_state.yaml_content.strip():
                        yaml_status = "Generated"
                        try:
                            parsed = yaml.safe_load(st.session_state.yaml_content)
                            sv = parsed.get("semantic_view", {})
                            columns = sv.get("columns", [])
                            for col in columns:
                                rules = col.get("dq_rules", [])
                                if isinstance(rules, list):
                                    total_rules += len(rules)
                            table_rules = sv.get("table_rules", [])
                            if isinstance(table_rules, list):
                                total_rules += len(table_rules)
                        except:
                            pass

                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Columns", num_columns)
                    col2.metric("YAML", yaml_status)
                    col3.metric("Rules", total_rules)
                
                    validation_status = "–"
                    if st.session_state.yaml_content.strip():
                        result = validate_semantic_yaml(st.session_state.yaml_content)
                        validation_status = "✓ Valid" if result["valid"] else "✗ Invalid"
                    col4.metric("Status", validation_status)

                except Exception as e:
                    st.warning(f"Could not load details: {e}")

            st.write("")

            # ===== QUICK ACTIONS =====
            with st.container(border=True):
                st.markdown("**Quick Actions**")
                st.write("")

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    if st.button("Generate", type="primary", use_container_width=True, key="gen_quick"):
                        st.info("→ Semantic Model tab")

                with col2:
                    if st.button("Validate", use_container_width=True, key="val_quick"):
                        if st.session_state.yaml_content.strip():
                            result = validate_semantic_yaml(st.session_state.yaml_content)
                            if result["valid"]:
                                st.success("✓ Valid YAML")
                            else:
                                st.error("Invalid YAML")
                        else:
                            st.warning("Generate YAML first")

                with col3:
                    if st.button("Export", use_container_width=True, key="exp_quick"):
                        if st.session_state.yaml_content.strip():
                            st.info("→ Export tab")
                        else:
                            st.warning("Generate YAML first")

                with col4:
                    if st.button("View YAML", use_container_width=True, key="view_quick"):
                        if st.session_state.yaml_content.strip():
                            st.info("→ Semantic Model tab")
        else:
            st.info("Select database, schema, and table above")

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

                    # Sync metadata from YAML if available (before rendering widgets)
                    if st.session_state.yaml_content.strip():
                        try:
                            parsed = yaml.safe_load(st.session_state.yaml_content)
                            sv = parsed.get("semantic_view", {})

                            # Sync description from YAML to widget state if widget state is empty
                            if sv.get("description"):
                                if "view_description_input" not in st.session_state or not st.session_state.view_description_input:
                                    st.session_state.view_description_input = sv.get("description")
                                    st.session_state.metadata_view_description = sv.get("description")

                            # Sync view name from YAML if different
                            if sv.get("name"):
                                if "view_name_input" not in st.session_state or st.session_state.view_name_input != sv.get("name"):
                                    st.session_state.view_name_input = sv.get("name")
                                    st.session_state.metadata_view_name = sv.get("name")
                        except:
                            pass  # If YAML parsing fails, skip sync

                    # Initialize session state for metadata
                    if "metadata_source_system" not in st.session_state:
                        st.session_state.metadata_source_system = "GENERIC"
                    if "metadata_business_domain" not in st.session_state:
                        st.session_state.metadata_business_domain = ""
                    if "metadata_entity_type" not in st.session_state:
                        from snowflake_utils import suggest_entity_type
                        st.session_state.metadata_entity_type = suggest_entity_type(selected_table) or ""

                    # Track table changes and update view_name accordingly
                    table_changed = False
                    if "last_selected_table" not in st.session_state:
                        st.session_state.last_selected_table = selected_table
                        st.session_state.metadata_view_name = f"{selected_table}_semantic_view"
                        table_changed = True
                    elif st.session_state.last_selected_table != selected_table:
                        # Table changed - update view name and entity type
                        st.session_state.last_selected_table = selected_table
                        st.session_state.metadata_view_name = f"{selected_table}_semantic_view"
                        from snowflake_utils import suggest_entity_type
                        st.session_state.metadata_entity_type = suggest_entity_type(selected_table) or ""
                        # Clear view-level filters and description for new table
                        st.session_state.view_level_filters = []
                        st.session_state.metadata_view_description = ""
                        # Also clear widget states
                        if "view_description_input" in st.session_state:
                            st.session_state.view_description_input = ""
                        if "view_name_input" in st.session_state:
                            st.session_state.view_name_input = f"{selected_table}_semantic_view"
                        table_changed = True

                    if "metadata_view_name" not in st.session_state:
                        st.session_state.metadata_view_name = f"{selected_table}_semantic_view"
                    if "metadata_view_description" not in st.session_state:
                        st.session_state.metadata_view_description = ""

                    # Initialize sample values storage
                    if "fetched_sample_values" not in st.session_state:
                        st.session_state.fetched_sample_values = {}

                    # If table just changed, trigger rerun to update text inputs with new values
                    if table_changed:
                        st.rerun()

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

                # ===== View-Level Filters Section (only shown after YAML generation) =====
                if st.session_state.yaml_content.strip():
                    with st.container(border=True):
                        st.markdown("**View-Level Filters** *(Optional)*")
                        st.caption("Apply filters to limit what data appears in this semantic view")

                    # Initialize session state for view-level filters
                        if "view_level_filters" not in st.session_state:
                            st.session_state.view_level_filters = []

                        # Show existing filters
                        if st.session_state.view_level_filters:
                            st.markdown(f"**Current Filters ({len(st.session_state.view_level_filters)}):**")
                            for idx, filter_sql in enumerate(st.session_state.view_level_filters, 1):
                                col1, col2 = st.columns([8, 1])
                                with col1:
                                    st.caption(f"Filter {idx}:")
                                    st.code(filter_sql, language="sql")
                                with col2:
                                    st.write("")  # spacing to align button
                                    if st.button("🗑️", key=f"delete_view_filter_{idx-1}", help="Remove filter"):
                                        st.session_state.view_level_filters.pop(idx-1)
                                        
                                        # Update YAML after removing filter
                                        if st.session_state.yaml_content.strip():
                                            st.session_state.yaml_content = update_yaml_with_filters(
                                                st.session_state.yaml_content,
                                                st.session_state.view_level_filters
                                            )
                                            save_to_history(st.session_state.yaml_content)
                                            st.success("✅ Filter removed from YAML")
                                        
                                        st.rerun()
                            st.write("")

                        # Add new filter
                        with st.expander("➕ Add View Filter", expanded=not st.session_state.view_level_filters):
                            filter_method = st.radio(
                                "How do you want to create the filter?",
                                ["Visual Builder", "Natural Language (AI)", "Cross-Table Join", "SQL (Advanced)"],
                                key="view_filter_method",
                                horizontal=True
                            )

                            new_filter_sql = None

                            # Option 1: Visual Builder
                            if filter_method == "🔧 Visual Builder":
                                st.markdown("**Build filter visually:**")
                                col1, col2, col3 = st.columns([3, 2, 3])
                                with col1:
                                    filter_column = st.selectbox(
                                        "Column",
                                        options=[""] + [c.get("column_name") for c in table_columns],
                                        key="view_filter_column"
                                    )
                                with col2:
                                    filter_operator = st.selectbox(
                                        "Operator",
                                        options=["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN", "NOT IN"],
                                        key="view_filter_operator"
                                    )
                                with col3:
                                    filter_value = st.text_input(
                                        "Value",
                                        placeholder="'ACTIVE', 100, '2024-01-01'",
                                        key="view_filter_value",
                                        help="Use quotes for strings, comma-separated for IN"
                                    )

                                if filter_column and filter_value:
                                    if filter_operator in ["IN", "NOT IN"]:
                                        new_filter_sql = f"{filter_column} {filter_operator} ({filter_value})"
                                    else:
                                        new_filter_sql = f"{filter_column} {filter_operator} {filter_value}"

                                    st.markdown("**Preview:**")
                                    st.code(new_filter_sql, language="sql")
                                    st.caption("👇 Click 'Add This Filter to View' below to add this filter")

                            # Option 2: Natural Language (AI)
                            elif filter_method == "Natural Language (AI)":
                                nl_filter = st.text_area(
                                    "Describe the filter in plain English:",
                                    placeholder="Examples:\n• Only active customers\n• Records created after January 2024\n• Status is not 'DELETED'",
                                    key="view_nl_filter",
                                    height=100
                                )

                                if nl_filter.strip() and st.button("Generate SQL Filter", key="view_generate_nl_filter"):
                                    with st.spinner("Generating SQL filter with AI..."):
                                        try:
                                            from snowflake_utils import generate_filter_with_ai
                                            cols_for_ai = [{"name": c.get("column_name"), "type": c.get("data_type")} for c in table_columns]

                                            # Check if semantic view exists to provide better context
                                            semantic_cols = None
                                            if st.session_state.yaml_content.strip():
                                                try:
                                                    parsed = yaml.safe_load(st.session_state.yaml_content)
                                                    semantic_cols = parsed.get("semantic_view", {}).get("columns", [])
                                                    if semantic_cols:
                                                        st.info("✨ Using semantic view metadata for better accuracy!")
                                                except:
                                                    pass

                                            new_filter_sql = generate_filter_with_ai(conn, nl_filter, cols_for_ai, semantic_columns=semantic_cols)
                                            st.markdown("**Preview:**")
                                            st.code(new_filter_sql, language="sql")
                                            st.caption("👇 Click 'Add This Filter to View' below to add this filter")
                                        except Exception as e:
                                            st.error(f"AI filter generation failed: {e}")

                            # Option 3: Cross-Table Join
                            elif filter_method == "🔗 Cross-Table Join":
                                st.markdown("**Build multi-table JOIN filter:**")
                                st.caption(f"Main table: **{selected_db}.{selected_schema}.{selected_table}**")

                                # Initialize session state for join blocks
                                if "view_join_blocks" not in st.session_state:
                                    st.session_state.view_join_blocks = []

                                # Display existing join blocks
                                if st.session_state.view_join_blocks:
                                    st.markdown(f"**JOIN Blocks ({len(st.session_state.view_join_blocks)}):**")
                                    for idx, block in enumerate(st.session_state.view_join_blocks):
                                        with st.container(border=True):
                                            col1, col2 = st.columns([9, 1])
                                            with col1:
                                                st.markdown(f"**{idx+1}. {block['join_type']} {block['table_name']}**")
                                                # Show join conditions
                                                for jc in block['join_conditions']:
                                                    st.caption(f"   ON {jc['left']} = {jc['right']}")
                                                # Show filter if exists
                                                if block.get('filter'):
                                                    st.caption(f"   WHERE {block['filter']}")
                                            with col2:
                                                if st.button("🗑️", key=f"view_del_join_block_{idx}"):
                                                    st.session_state.view_join_blocks.pop(idx)
                                                    st.rerun()

                                # Add new JOIN block
                                with st.expander("➕ Add JOIN Block", expanded=not st.session_state.view_join_blocks):
                                    st.markdown("**Step 1: Select table and join type**")

                                    ref_schemas = cached_list_schemas(conn, selected_db)
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        new_schema = st.selectbox(
                                            "Schema",
                                            options=[""] + ref_schemas,
                                            key="view_new_join_schema"
                                        )
                                    with col2:
                                        if new_schema:
                                            new_tables = cached_list_tables(conn, selected_db, new_schema)
                                            new_table = st.selectbox(
                                                "Table",
                                                options=[""] + new_tables,
                                                key="view_new_join_table"
                                            )
                                        else:
                                            new_table = None
                                            st.selectbox("Table", options=["Select schema first"], disabled=True, key="view_new_join_table_disabled")

                                    if new_schema and new_table:
                                        # Join type
                                        join_type = st.radio(
                                            "Join Type",
                                            options=["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"],
                                            horizontal=True,
                                            key="view_new_join_type"
                                        )

                                        st.markdown("**Step 2: Define join condition(s)**")
                                        st.caption("Specify how the tables connect (you can add multiple conditions)")

                                        # Get columns from both tables
                                        main_cols = [c.get("column_name") for c in table_columns]
                                        try:
                                            ref_table_cols = cached_get_columns(conn, selected_db, new_schema, new_table)
                                            ref_cols = [c["column_name"] for c in ref_table_cols]
                                        except Exception as e:
                                            st.error(f"Could not load columns from {new_table}: {e}")
                                            ref_cols = []

                                        # Initialize join conditions list
                                        if "view_temp_join_conditions" not in st.session_state:
                                            st.session_state.view_temp_join_conditions = []

                                        # Show existing join conditions
                                        if st.session_state.view_temp_join_conditions:
                                            for jc_idx, jc in enumerate(st.session_state.view_temp_join_conditions):
                                                col1, col2, col3 = st.columns([5, 5, 1])
                                                with col1:
                                                    st.text(f"{selected_table}.{jc['left']}")
                                                with col2:
                                                    st.text(f"= {new_table}.{jc['right']}")
                                                with col3:
                                                    if st.button("✖", key=f"view_del_jc_{jc_idx}"):
                                                        st.session_state.view_temp_join_conditions.pop(jc_idx)
                                                        st.rerun()

                                        # Add new join condition
                                        col1, col2, col3 = st.columns([5, 5, 2])
                                        with col1:
                                            left_col = st.selectbox(
                                                f"Column from {selected_table}",
                                                options=[""] + main_cols,
                                                key="view_jc_left"
                                            )
                                        with col2:
                                            right_col = st.selectbox(
                                                f"Column from {new_table}",
                                                options=[""] + ref_cols,
                                                key="view_jc_right"
                                            )
                                        with col3:
                                            st.write("")
                                            st.write("")
                                            if left_col and right_col and st.button("➕ Add", key="view_add_jc"):
                                                st.session_state.view_temp_join_conditions.append({
                                                    "left": left_col,
                                                    "right": right_col
                                                })
                                                st.rerun()

                                        # Optional filter
                                        if st.session_state.view_temp_join_conditions:
                                            st.markdown("**Step 3 (Optional): Add filter on joined table**")
                                            col1, col2, col3 = st.columns([3, 2, 3])
                                            with col1:
                                                filter_col = st.selectbox(
                                                    "Filter column",
                                                    options=["(no filter)"] + ref_cols,
                                                    key="view_join_filter_col"
                                                )
                                            with col2:
                                                if filter_col != "(no filter)":
                                                    filter_op = st.selectbox(
                                                        "Operator",
                                                        options=["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"],
                                                        key="view_join_filter_op"
                                                    )
                                                else:
                                                    filter_op = None
                                            with col3:
                                                if filter_col != "(no filter)":
                                                    filter_val = st.text_input(
                                                        "Value",
                                                        placeholder="'ACTIVE', 100",
                                                        key="view_join_filter_val"
                                                    )
                                                else:
                                                    filter_val = None

                                            # Add JOIN block button
                                            if st.button("✅ Add This JOIN Block", type="primary", key="view_add_join_block"):
                                                filter_str = None
                                                if filter_col != "(no filter)" and filter_val:
                                                    if filter_op == "IN":
                                                        filter_str = f"{filter_col} {filter_op} ({filter_val})"
                                                    else:
                                                        filter_str = f"{filter_col} {filter_op} {filter_val}"

                                                st.session_state.view_join_blocks.append({
                                                    "table": f"{selected_db}.{new_schema}.{new_table}",
                                                    "table_name": new_table,
                                                    "alias": f"t{len(st.session_state.view_join_blocks) + 1}",
                                                    "join_type": join_type,
                                                    "join_conditions": st.session_state.view_temp_join_conditions.copy(),
                                                    "filter": filter_str
                                                })
                                                st.session_state.view_temp_join_conditions = []
                                                st.success(f"✅ Added {join_type} {new_table}")
                                                st.rerun()

                                # Generate SQL
                                if st.session_state.view_join_blocks:
                                    st.markdown("**Preview:**")

                                    # Build SQL
                                    main_alias = "main"
                                    select_cols = [c.get("column_name") for c in table_columns]
                                    select_clause = f"{main_alias}.*"  # Or specify columns

                                    from_clause = f"FROM {selected_db}.{selected_schema}.{selected_table} {main_alias}"

                                    join_clauses = []
                                    where_clauses = []

                                    for block in st.session_state.view_join_blocks:
                                        # Build ON conditions
                                        on_conditions = []
                                        for jc in block['join_conditions']:
                                            on_conditions.append(f"{main_alias}.{jc['left']} = {block['alias']}.{jc['right']}")
                                        on_clause = " AND ".join(on_conditions)

                                        join_clauses.append(f"{block['join_type']} {block['table']} {block['alias']} ON {on_clause}")

                                        if block.get('filter'):
                                            where_clauses.append(f"{block['alias']}.{block['filter']}")

                                    where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                                    # For view-level filter, we need to return rows where these joins succeed
                                    # So we use EXISTS with the main table
                                    new_filter_sql = f"EXISTS (SELECT 1 {from_clause} {' '.join(join_clauses)} {where_clause})"

                                    st.code(new_filter_sql, language="sql")
                                    st.caption("👇 Click 'Add This Filter to View' below to add this filter")
                                else:
                                    new_filter_sql = None
                                    st.info("Add at least one JOIN block to create the filter")

                            # Option 4: SQL (Advanced)
                            elif filter_method == "💻 SQL (Advanced)":
                                new_filter_sql = st.text_area(
                                    "Enter SQL WHERE condition:",
                                    placeholder="status = 'ACTIVE' AND created_date > '2024-01-01'",
                                    key="view_manual_filter",
                                    height=80,
                                    help="Enter the condition without the WHERE keyword"
                                )

                                if new_filter_sql.strip():
                                    st.markdown("**Preview:**")
                                    st.code(new_filter_sql.strip(), language="sql")
                                    st.caption("👇 Click 'Add This Filter to View' below to add this filter")

                        # Add filter button
                        if new_filter_sql and new_filter_sql.strip():
                            st.write("")  # spacing
                            if st.button("➕ Add This Filter to View", type="primary", key="add_view_filter_btn", use_container_width=True):
                                st.session_state.view_level_filters.append(new_filter_sql.strip())
                                
                                # Update YAML directly with the new filters
                                if st.session_state.yaml_content.strip():
                                    st.session_state.yaml_content = update_yaml_with_filters(
                                        st.session_state.yaml_content,
                                        st.session_state.view_level_filters
                                    )
                                    save_to_history(st.session_state.yaml_content)
                                    st.success("✅ Filter added to YAML")
                                
                                # Clear join blocks and temp data after adding
                                if "view_join_blocks" in st.session_state:
                                    st.session_state.view_join_blocks = []
                                if "view_temp_join_conditions" in st.session_state:
                                    st.session_state.view_temp_join_conditions = []
                                st.rerun()

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
                            # STEP 1: Fetch sample values first
                            table_key = f"{selected_db}.{selected_schema}.{selected_table}"
                            sample_values_dict = {}

                            with st.spinner(f"Step 1/2: Fetching sample values from {num_columns} columns..."):
                                progress_text = st.empty()
                                for idx, col_info in enumerate(table_columns):
                                    col_name = col_info["column_name"]
                                    progress_text.text(f"Fetching samples: {col_name} ({idx+1}/{num_columns})...")

                                    samples = fetch_sample_values(conn, selected_db, selected_schema, selected_table, col_name, limit=5)
                                    if samples:
                                        sample_values_dict[col_name] = samples

                                progress_text.empty()

                            # STEP 2: Generate YAML with sample values
                            with st.spinner("Step 2/2: Generating YAML with AI (using sample values)..."):
                                try:
                                    yaml_content = generate_semantic_yaml_with_cortex(
                                        conn, selected_db, selected_schema, selected_table,
                                        source_system=st.session_state.metadata_source_system,
                                        business_domain=st.session_state.metadata_business_domain or None,
                                        entity_type=st.session_state.metadata_entity_type or None,
                                        view_name=st.session_state.metadata_view_name or None,
                                        description=st.session_state.metadata_view_description or None,
                                        sample_values=sample_values_dict,
                                        view_level_filters=st.session_state.view_level_filters if st.session_state.view_level_filters else None
                                    )
                                    st.session_state.yaml_content = yaml_content
                                    st.session_state.last_generated = datetime.now()
                                    save_to_history(yaml_content)
                                    st.success(f"✅ YAML generated with sample values from {len(sample_values_dict)} columns")
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
                                    view_level_filters=st.session_state.view_level_filters if st.session_state.view_level_filters else None
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

                            # STEP 1: Fetch sample values first
                            table_key = f"{selected_db}.{selected_schema}.{selected_table}"
                            sample_values_dict = {}

                            with st.spinner(f"Step 1/2: Fetching sample values from {num_columns} columns..."):
                                progress_text = st.empty()
                                for idx, col_info in enumerate(table_columns):
                                    col_name = col_info["column_name"]
                                    progress_text.text(f"Fetching samples: {col_name} ({idx+1}/{num_columns})...")

                                    samples = fetch_sample_values(conn, selected_db, selected_schema, selected_table, col_name, limit=5)
                                    if samples:
                                        sample_values_dict[col_name] = samples

                                progress_text.empty()

                            # STEP 2: Generate YAML with sample values
                            with st.spinner("Step 2/2: Generating YAML with AI (using sample values)..."):
                                try:
                                    yaml_content = generate_semantic_yaml_with_cortex(
                                        conn, selected_db, selected_schema, selected_table,
                                        dq_columns=selected_dq_columns if selected_dq_columns else None,
                                        source_system=st.session_state.metadata_source_system,
                                        business_domain=st.session_state.metadata_business_domain or None,
                                        entity_type=st.session_state.metadata_entity_type or None,
                                        view_name=st.session_state.metadata_view_name or None,
                                        description=st.session_state.metadata_view_description or None,
                                        sample_values=sample_values_dict,
                                        view_level_filters=st.session_state.view_level_filters if st.session_state.view_level_filters else None
                                    )
                                    st.session_state.yaml_content = yaml_content
                                    st.session_state.last_generated = datetime.now()
                                    save_to_history(yaml_content)
                                    st.success(f"✅ YAML generated with sample values from {len(sample_values_dict)} columns")
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
                                    # Get sample values if available
                                    sample_values = col.get("sample_values", [])
                                    # Convert any non-serializable types (like Decimal) to display strings
                                    if sample_values:
                                        converted_samples = [convert_to_serializable(v) for v in sample_values[:3]]
                                        sample_str = ", ".join([str(v) for v in converted_samples])
                                    else:
                                        sample_str = "N/A"

                                    col_data.append({
                                        "Column": col.get("name", ""),
                                        "Label": col.get("label", ""),
                                        "Type": col.get("data_type", ""),
                                        "Description": col.get("description", ""),
                                        "Sample Values": sample_str
                                    })

                                df = pd.DataFrame(col_data)

                                # Use data_editor for editable table
                                edited_df = st.data_editor(
                                    df,
                                    use_container_width=True,
                                    hide_index=True,
                                    height=400,
                                    column_config={
                                        "Column": st.column_config.TextColumn("Column", width="medium", disabled=True),
                                        "Label": st.column_config.TextColumn("Label", width="medium"),
                                        "Type": st.column_config.TextColumn("Type", width="small", disabled=True),
                                        "Description": st.column_config.TextColumn("Description", width="large"),
                                        "Sample Values": st.column_config.TextColumn("Sample Values", width="medium", disabled=True),
                                    },
                                    key=f"column_editor_{st.session_state.editor_version}"
                                )

                                # Check if data was edited
                                if not df.equals(edited_df):
                                    # Update YAML with edited values
                                    import yaml as yaml_module
                                    parsed = yaml_module.safe_load(st.session_state.yaml_content)

                                    for idx, row in edited_df.iterrows():
                                        col_name = row["Column"]
                                        for col in parsed["semantic_view"]["columns"]:
                                            if col.get("name") == col_name:
                                                col["label"] = row["Label"]
                                                col["description"] = row["Description"]
                                                break

                                    updated_yaml = yaml_module.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)
                                    st.session_state.yaml_content = updated_yaml
                                    save_to_history(updated_yaml)
                                    st.session_state.editor_version += 1
                                    st.rerun()

                                # Add button to fetch sample values from Snowflake
                                st.write("")  # spacing
                                if st.button("Fetch Sample Values from Snowflake", use_container_width=True, key="fetch_samples"):
                                    with st.spinner("Fetching sample values from Snowflake..."):
                                        import yaml as yaml_module
                                        parsed = yaml_module.safe_load(st.session_state.yaml_content)

                                        # Get source table info
                                        source = parsed.get("semantic_view", {}).get("source", {})
                                        db = source.get("database", "")
                                        schema = source.get("schema", "")
                                        table = source.get("table", "")

                                        if db and schema and table:
                                            # Fetch samples for each column
                                            updated_count = 0
                                            for col in parsed["semantic_view"]["columns"]:
                                                col_name = col.get("name", "")
                                                if col_name:
                                                    samples = fetch_sample_values(conn, db, schema, table, col_name, limit=5)
                                                    if samples:
                                                        col["sample_values"] = samples
                                                        updated_count += 1

                                            # Save updated YAML
                                            updated_yaml = yaml_module.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)
                                            st.session_state.yaml_content = updated_yaml
                                            save_to_history(updated_yaml)
                                            st.session_state.editor_version += 1
                                            st.success(f"✅ Fetched sample values for {updated_count} columns!")
                                            st.rerun()
                                        else:
                                            st.error("Could not determine source table. Please check your YAML configuration.")
                            else:
                                st.info("No columns found in YAML")

                        st.write("")  # spacing

                        # ===== SECTION 1.5: View-Level Filters Table (MIDDLE) =====
                        filters = sv.get("filters", [])
                        if filters:
                            with st.container(border=True):
                                st.markdown(f"**{SEARCH_ICON} View-Level Filters**")
                                st.write("")  # spacing

                                # Create editable filters table
                                filter_data = []
                                for idx, filter_sql in enumerate(filters):
                                    filter_data.append({
                                        "ID": idx + 1,
                                        "Filter": filter_sql
                                    })

                                df_filters = pd.DataFrame(filter_data)

                                # Use data_editor for editable table
                                edited_filters_df = st.data_editor(
                                    df_filters,
                                    use_container_width=True,
                                    hide_index=True,
                                    height=200,
                                    column_config={
                                        "ID": st.column_config.NumberColumn("ID", width="small", disabled=True),
                                        "Filter": st.column_config.TextColumn("Filter", width="large"),
                                    },
                                    key=f"filters_editor_{st.session_state.editor_version}"
                                )

                                # Check if filters were edited
                                if not df_filters.equals(edited_filters_df):
                                    # Update YAML with edited filters
                                    import yaml as yaml_module
                                    parsed = yaml_module.safe_load(st.session_state.yaml_content)
                                    
                                    # Update filters list
                                    updated_filters = edited_filters_df["Filter"].tolist()
                                    if updated_filters and any(f.strip() for f in updated_filters):
                                        parsed["semantic_view"]["filters"] = [f for f in updated_filters if f.strip()]
                                    elif "filters" in parsed["semantic_view"]:
                                        del parsed["semantic_view"]["filters"]
                                    
                                    updated_yaml = yaml_module.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)
                                    st.session_state.yaml_content = updated_yaml
                                    save_to_history(updated_yaml)
                                    st.session_state.editor_version += 1
                                    st.success("✅ Filters updated in YAML")
                                    st.rerun()

                                # Add button to remove all filters
                                st.write("")  # spacing
                                col_remove, col_add = st.columns(2)
                                with col_remove:
                                    if st.button("🗑️ Clear All Filters", use_container_width=True, key="clear_all_filters"):
                                        import yaml as yaml_module
                                        parsed = yaml_module.safe_load(st.session_state.yaml_content)
                                        if "filters" in parsed.get("semantic_view", {}):
                                            del parsed["semantic_view"]["filters"]
                                        updated_yaml = yaml_module.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)
                                        st.session_state.yaml_content = updated_yaml
                                        st.session_state.view_level_filters = []
                                        save_to_history(updated_yaml)
                                        st.session_state.editor_version += 1
                                        st.success("✅ All filters removed")
                                        st.rerun()

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

                    # ========== NEW: AI-Powered Auto Field Identification ==========
                    st.markdown("---")
                    st.markdown(f"#### {ROBOT_ICON} AI-Powered Rule Creation (Auto Field Detection)")
                    st.caption("Describe what you want to validate - AI will automatically identify the relevant fields")

                    with st.expander(f"{LIGHTBULB_ICON} How it works", expanded=False):
                        st.markdown("""
                        **Business users can create rules without knowing field names!**

                        Simply describe what you want to validate in natural language:
                        - "Make sure customer emails are valid"
                        - "Check that order totals match line items"
                        - "Active customers must have purchased recently"

                        AI will:
                        1. Analyze your table schema
                        2. 🎯 Identify which field(s) are relevant
                        3. 🧠 Determine the appropriate rule type
                        4. ✅ Show you what it found for your approval

                        **Examples:**
                        | Your Description | AI Identifies | Rule Type |
                        |------------------|---------------|-----------|
                        | "Emails should be valid" | `email` field | PATTERN |
                        | "Start before end" | `start_date`, `end_date` | CROSS_COLUMN_COMPARISON |
                        | "No duplicate customers" | `customer_name` | FUZZY_DUPLICATE |
                        """)

                    nl_auto_description = st.text_area(
                        "What do you want to validate?",
                        placeholder="Examples:\n• Make sure customer emails are properly formatted\n• Prices cannot be negative\n• Active customers should have recent purchases\n• Order totals must match the sum of line items\n• Start date must be before end date",
                        height=120,
                        key=f"nl_auto_desc_{source_key}",
                        help="Describe what you want to validate without worrying about field names"
                    )

                    col_btn1, col_btn2 = st.columns([1, 3])

                    with col_btn1:
                        if st.button("Auto-Identify Fields", type="primary", key=f"auto_identify_btn_{source_key}"):
                            if not nl_auto_description.strip():
                                st.warning("⚠️ Please describe what you want to validate")
                            else:
                                with st.spinner("AI is analyzing your request and identifying fields..."):
                                    try:
                                        result = auto_identify_and_create_rule(
                                            conn=conn,
                                            yaml_text=st.session_state.yaml_content,
                                            nl_description=nl_auto_description
                                        )

                                        # Store result for review
                                        st.session_state.auto_rule_result = result
                                        st.rerun()

                                    except Exception as e:
                                        st.error(f"❌ Auto-identification failed: {e}")

                    # Display identified fields and suggested rule for review
                    if "auto_rule_result" in st.session_state and st.session_state.auto_rule_result:
                        result = st.session_state.auto_rule_result

                        st.markdown("---")
                        st.markdown("##### 🎯 AI Identified the Following:")

                        with st.container(border=True):
                            col1, col2, col3 = st.columns(3)

                            with col1:
                                st.metric("Identified Fields", ", ".join(result["identified_fields"]))

                            with col2:
                                st.metric("Rule Type", result["rule_type"])

                            with col3:
                                st.metric("Category", result["rule_category"])

                            st.markdown(f"{LIGHTBULB_ICON} **AI Reasoning:** {result['explanation']}", unsafe_allow_html=True)

                            st.markdown("**Generated Rule Description:**")
                            st.code(result["nl_rule_description"], language=None)

                            col_approve, col_reject = st.columns([1, 1])

                            with col_approve:
                                if st.button("✅ Apply This Rule", type="primary", use_container_width=True, key=f"apply_auto_rule_{source_key}"):
                                    st.session_state.yaml_content = result["updated_yaml"]
                                    save_to_history(result["updated_yaml"])
                                    st.session_state.editor_version += 1
                                    st.session_state.skip_auto_save = True

                                    fields_str = ", ".join(result["identified_fields"])
                                    st.success(f"✅ Rule applied to field(s): {fields_str}")

                                    # Clear the result
                                    del st.session_state.auto_rule_result
                                    st.rerun()

                            with col_reject:
                                if st.button("❌ Try Again", use_container_width=True, key=f"reject_auto_rule_{source_key}"):
                                    del st.session_state.auto_rule_result
                                    st.rerun()

                    # Add rule via NL
                    st.markdown("---")
                    st.markdown("#### ➕ Add Rule via Natural Language (Manual Field Selection)")
                    st.caption("Select specific fields first, then describe the rule you want to apply")

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

                        # Advanced options - Filter
                        with st.expander("⚙️ Advanced Options (Optional)", expanded=False):
                            st.markdown("**Filter:** Apply this rule to a subset of data")

                            filter_method = st.radio(
                                "How do you want to create the filter?",
                                ["🤖 Natural Language (AI)", "🔧 Visual Builder", "🔗 Cross-Table Join", "💻 SQL (Advanced)"],
                                key=f"filter_method_single_{source_key}",
                                horizontal=True
                            )

                            filter_condition = ""

                            if filter_method == "🤖 Natural Language (AI)":
                                st.caption("Describe your filter in plain English - AI will convert it to SQL")

                                nl_filter = st.text_area(
                                    "Filter description",
                                    placeholder="Examples:\n• Only active customers\n• Records created after January 2024\n• Status is either NEW or PENDING\n• Amount greater than 1000",
                                    height=80,
                                    key=f"nl_filter_single_{source_key}"
                                )

                                if nl_filter.strip() and st.button("Generate SQL Filter", key=f"gen_filter_single_{source_key}"):
                                    with st.spinner("Converting to SQL..."):
                                        try:
                                            # Get available columns for context
                                            col_names = [c.get("name") for c in columns]
                                            col_context = ", ".join(col_names)

                                            prompt = f"""Convert this natural language filter to a SQL WHERE condition.

    Available columns: {col_context}

    User's filter request: "{nl_filter}"

    Return ONLY the SQL condition (no WHERE keyword, no quotes around it, just the condition).

    Examples:
    Input: "only active customers"
    Output: active = TRUE

    Input: "created after January 2024"
    Output: created_date >= '2024-01-01'

    Input: "status is NEW or PENDING"
    Output: status IN ('NEW', 'PENDING')

    Now convert the user's request:"""

                                            from snowflake_utils import call_cortex_for_rule
                                            sql_condition = call_cortex_for_rule(conn, prompt).strip()

                                            # Clean up the response
                                            sql_condition = sql_condition.strip('"').strip("'").strip()

                                            st.session_state[f"generated_filter_single_{source_key}"] = sql_condition
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Failed to generate filter: {e}")

                                if f"generated_filter_single_{source_key}" in st.session_state:
                                    filter_condition = st.session_state[f"generated_filter_single_{source_key}"]
                                    st.success(f"✓ Generated SQL: `{filter_condition}`")
                                    st.caption("You can edit this if needed:")
                                    filter_condition = st.text_input(
                                        "SQL condition (editable)",
                                        value=filter_condition,
                                        key=f"edit_filter_single_{source_key}"
                                    )

                            elif filter_method == "🔧 Visual Builder":
                                st.caption("Build your filter using dropdowns")

                                col1, col2, col3 = st.columns([3, 2, 3])

                                with col1:
                                    filter_column = st.selectbox(
                                        "Column",
                                        options=[""] + [c.get("name") for c in columns],
                                        key=f"filter_col_single_{source_key}"
                                    )

                                with col2:
                                    filter_operator = st.selectbox(
                                        "Operator",
                                        options=["=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "IS NULL", "IS NOT NULL"],
                                        key=f"filter_op_single_{source_key}"
                                    )

                                with col3:
                                    if filter_operator not in ["IS NULL", "IS NOT NULL"]:
                                        filter_value = st.text_input(
                                            "Value",
                                            placeholder="Enter value (e.g., 'ACTIVE', 100, '2024-01-01')",
                                            key=f"filter_val_single_{source_key}"
                                        )
                                    else:
                                        filter_value = None

                                # Build SQL condition
                                if filter_column:
                                    if filter_operator in ["IS NULL", "IS NOT NULL"]:
                                        filter_condition = f"{filter_column} {filter_operator}"
                                    elif filter_operator in ["IN", "NOT IN"]:
                                        if filter_value:
                                            # Parse comma-separated values
                                            values = [v.strip().strip("'\"") for v in filter_value.split(",")]
                                            values_str = ", ".join([f"'{v}'" for v in values])
                                            filter_condition = f"{filter_column} {filter_operator} ({values_str})"
                                    elif filter_value:
                                        # Auto-quote strings
                                        if not (filter_value.startswith("'") or filter_value.isdigit()):
                                            filter_value = f"'{filter_value}'"
                                        filter_condition = f"{filter_column} {filter_operator} {filter_value}"

                                    if filter_condition:
                                        st.success(f"✓ Filter: `{filter_condition}`")

                            elif filter_method == "🔗 Cross-Table Join":
                                st.caption("Filter based on data in another table")
                                st.markdown("**Simple 3-Step Process:**")

                                # Step 1: Select the linking column from current table
                                st.markdown("**Step 1:** Which column connects to the other table?")
                                join_col = st.selectbox(
                                    "Column from this table",
                                    options=[""] + [c.get("name") for c in columns],
                                    key=f"cross_join_col_single_{source_key}",
                                    help="Example: customer_id, product_id, account_id"
                                )

                                if join_col:
                                    # Step 2: Select the reference table
                                    st.markdown("**Step 2:** Select the other table")

                                    # Get list of schemas in current database
                                    ref_schemas = cached_list_schemas(conn, selected_db)

                                    col1, col2 = st.columns(2)
                                    with col1:
                                        ref_schema = st.selectbox(
                                            "Schema",
                                            options=[""] + ref_schemas,
                                            key=f"cross_ref_schema_single_{source_key}"
                                        )

                                    with col2:
                                        if ref_schema:
                                            # Get tables in selected schema
                                            ref_tables = cached_list_tables(conn, selected_db, ref_schema)
                                            ref_table_name = st.selectbox(
                                                "Table",
                                                options=[""] + ref_tables,
                                                key=f"cross_ref_table_single_{source_key}"
                                            )
                                        else:
                                            ref_table_name = None
                                            st.selectbox("Table", options=["Select schema first"], key=f"cross_ref_table_single_{source_key}_disabled", disabled=True)

                                    if ref_schema and ref_table_name:
                                        # Build fully qualified table name
                                        ref_table = f"{selected_db}.{ref_schema}.{ref_table_name}"
                                        # Step 3: Build the filter on the other table using visual builder
                                        st.markdown("**Step 3:** What condition on the other table?")
                                        st.caption("Filter which records from the other table to include")

                                        col1, col2, col3 = st.columns([3, 2, 3])

                                        with col1:
                                            ref_col_filter = st.text_input(
                                                "Column name",
                                                placeholder="country, status, type",
                                                key=f"cross_ref_col_single_{source_key}"
                                            )

                                        with col2:
                                            ref_operator = st.selectbox(
                                                "Condition",
                                                options=["=", "!=", "IN", "LIKE", "IS NOT NULL"],
                                                key=f"cross_ref_op_single_{source_key}"
                                            )

                                        with col3:
                                            if ref_operator not in ["IS NOT NULL"]:
                                                ref_value = st.text_input(
                                                    "Value",
                                                    placeholder="FR, ACTIVE, etc.",
                                                    key=f"cross_ref_val_single_{source_key}"
                                                )
                                            else:
                                                ref_value = None

                                        # Build the full filter
                                        if ref_col_filter and (ref_value or ref_operator == "IS NOT NULL"):
                                            # Build the reference condition
                                            if ref_operator == "IS NOT NULL":
                                                ref_condition = f"{ref_col_filter} IS NOT NULL"
                                            elif ref_operator == "IN":
                                                values = [v.strip().strip("'\"") for v in ref_value.split(",")]
                                                values_str = ", ".join([f"'{v}'" for v in values])
                                                ref_condition = f"{ref_col_filter} IN ({values_str})"
                                            else:
                                                if not ref_value.startswith("'"):
                                                    ref_value = f"'{ref_value}'"
                                                ref_condition = f"{ref_col_filter} {ref_operator} {ref_value}"

                                            # Assume same column name in reference table (user can edit)
                                            filter_condition = f"{join_col} IN (SELECT {join_col} FROM {ref_table} WHERE {ref_condition})"

                                            st.success(f"✓ Filter created!")
                                            st.code(filter_condition, language="sql")
                                            st.caption("💡 This will only validate rows that match records in the other table")
                                        elif ref_col_filter or ref_value:
                                            st.info("👆 Complete all fields above to create the filter")

                            else:  # SQL (Advanced)
                                st.caption("Write SQL WHERE condition manually")
                                filter_condition = st.text_input(
                                    "SQL WHERE condition",
                                    placeholder="Example: active = TRUE  or  status IN ('NEW', 'ACTIVE')  or  created_date >= '2024-01-01'",
                                    key=f"filter_condition_single_{source_key}",
                                    help="SQL condition to filter which rows this rule applies to (without the WHERE keyword)"
                                )

                            if filter_condition:
                                st.info("💡 Filters can improve performance by 10-100x on large tables!")

                        if st.button("Add Rule", type="primary", key=f"add_rule_btn_{source_key}"):
                            if not nl_rule_input.strip():
                                st.markdown(f"{WARNING_ICON} Please enter a rule description", unsafe_allow_html=True)
                            elif not selected_col_for_rule:
                                st.markdown(f"{WARNING_ICON} Please select a column", unsafe_allow_html=True)
                            else:
                                with st.spinner("Translating rule via AI..."):
                                    try:
                                        def llm_fn(prompt):
                                            return call_cortex_for_rule(conn, prompt)

                                        updated_yaml = add_dq_rule_from_natural_language(
                                            yaml_text=st.session_state.yaml_content,
                                            column_name=selected_col_for_rule,
                                            nl_rule=nl_rule_input,
                                            llm_call_fn=llm_fn
                                        )

                                        # If filter is specified, add it to the params
                                        if filter_condition and filter_condition.strip():
                                            import yaml as yaml_module
                                            parsed = yaml_module.safe_load(updated_yaml)
                                            columns = parsed["semantic_view"]["columns"]

                                            # Find the column and the last rule (the one just added)
                                            for col in columns:
                                                if col.get("name") == selected_col_for_rule:
                                                    if "dq_rules" in col and col["dq_rules"]:
                                                        last_rule = col["dq_rules"][-1]
                                                        if "params" not in last_rule or last_rule["params"] is None:
                                                            last_rule["params"] = {}
                                                        last_rule["params"]["filter"] = filter_condition.strip()
                                                    break

                                            updated_yaml = yaml_module.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)

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

                        # Advanced options - Filter for cross-column rules
                        with st.expander("⚙️ Advanced Options (Optional)", expanded=False):
                            st.markdown("**Filter:** Apply this rule to a subset of data")

                            filter_method_multi = st.radio(
                                "How do you want to create the filter?",
                                ["🤖 Natural Language (AI)", "🔧 Visual Builder", "🔗 Cross-Table Join", "💻 SQL (Advanced)"],
                                key=f"filter_method_multi_{source_key}",
                                horizontal=True
                            )

                            filter_condition_multi = ""

                            if filter_method_multi == "Natural Language (AI)":
                                st.caption("Describe your filter in plain English - AI will convert it to SQL")

                                nl_filter_multi = st.text_area(
                                    "Filter description",
                                    placeholder="Examples:\n• Only active customers\n• Records created after January 2024\n• Status is either NEW or PENDING",
                                    height=80,
                                    key=f"nl_filter_multi_{source_key}"
                                )

                                if nl_filter_multi.strip() and st.button("Generate SQL Filter", key=f"gen_filter_multi_{source_key}"):
                                    with st.spinner("Converting to SQL..."):
                                        try:
                                            col_names = [c.get("name") for c in columns]
                                            col_context = ", ".join(col_names)

                                            prompt = f"""Convert this natural language filter to a SQL WHERE condition.

    Available columns: {col_context}

    User's filter request: "{nl_filter_multi}"

    Return ONLY the SQL condition (no WHERE keyword).

    Examples:
    "only active" → active = TRUE
    "created after Jan 2024" → created_date >= '2024-01-01'
    "status is NEW or PENDING" → status IN ('NEW', 'PENDING')"""

                                            from snowflake_utils import call_cortex_for_rule
                                            sql_condition = call_cortex_for_rule(conn, prompt).strip().strip('"').strip("'").strip()

                                            st.session_state[f"generated_filter_multi_{source_key}"] = sql_condition
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Failed to generate filter: {e}")

                                if f"generated_filter_multi_{source_key}" in st.session_state:
                                    filter_condition_multi = st.session_state[f"generated_filter_multi_{source_key}"]
                                    st.success(f"✓ Generated SQL: `{filter_condition_multi}`")
                                    st.caption("You can edit this if needed:")
                                    filter_condition_multi = st.text_input(
                                        "SQL condition (editable)",
                                        value=filter_condition_multi,
                                        key=f"edit_filter_multi_{source_key}"
                                    )

                            elif filter_method_multi == "🔧 Visual Builder":
                                st.caption("Build your filter using dropdowns")

                                col1, col2, col3 = st.columns([3, 2, 3])

                                with col1:
                                    filter_column_multi = st.selectbox(
                                        "Column",
                                        options=[""] + [c.get("name") for c in columns],
                                        key=f"filter_col_multi_{source_key}"
                                    )

                                with col2:
                                    filter_operator_multi = st.selectbox(
                                        "Operator",
                                        options=["=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "IS NULL", "IS NOT NULL"],
                                        key=f"filter_op_multi_{source_key}"
                                    )

                                with col3:
                                    if filter_operator_multi not in ["IS NULL", "IS NOT NULL"]:
                                        filter_value_multi = st.text_input(
                                            "Value",
                                            placeholder="Enter value",
                                            key=f"filter_val_multi_{source_key}"
                                        )
                                    else:
                                        filter_value_multi = None

                                # Build SQL condition
                                if filter_column_multi:
                                    if filter_operator_multi in ["IS NULL", "IS NOT NULL"]:
                                        filter_condition_multi = f"{filter_column_multi} {filter_operator_multi}"
                                    elif filter_operator_multi in ["IN", "NOT IN"]:
                                        if filter_value_multi:
                                            values = [v.strip().strip("'\"") for v in filter_value_multi.split(",")]
                                            values_str = ", ".join([f"'{v}'" for v in values])
                                            filter_condition_multi = f"{filter_column_multi} {filter_operator_multi} ({values_str})"
                                    elif filter_value_multi:
                                        if not (filter_value_multi.startswith("'") or filter_value_multi.isdigit()):
                                            filter_value_multi = f"'{filter_value_multi}'"
                                        filter_condition_multi = f"{filter_column_multi} {filter_operator_multi} {filter_value_multi}"

                                    if filter_condition_multi:
                                        st.success(f"✓ Filter: `{filter_condition_multi}`")

                            elif filter_method_multi == "🔗 Cross-Table Join":
                                st.caption("Filter based on data in another table")
                                st.markdown("**Simple 3-Step Process:**")

                                # Step 1
                                st.markdown("**Step 1:** Which column connects to the other table?")
                                join_col_multi = st.selectbox(
                                    "Column from this table",
                                    options=[""] + [c.get("name") for c in columns],
                                    key=f"cross_join_col_multi_{source_key}",
                                    help="Example: customer_id, order_id"
                                )

                                if join_col_multi:
                                    # Step 2: Select the reference table
                                    st.markdown("**Step 2:** Select the other table")

                                    # Get list of schemas in current database
                                    ref_schemas_multi = cached_list_schemas(conn, selected_db)

                                    col1, col2 = st.columns(2)
                                    with col1:
                                        ref_schema_multi = st.selectbox(
                                            "Schema",
                                            options=[""] + ref_schemas_multi,
                                            key=f"cross_ref_schema_multi_{source_key}"
                                        )

                                    with col2:
                                        if ref_schema_multi:
                                            # Get tables in selected schema
                                            ref_tables_multi = cached_list_tables(conn, selected_db, ref_schema_multi)
                                            ref_table_name_multi = st.selectbox(
                                                "Table",
                                                options=[""] + ref_tables_multi,
                                                key=f"cross_ref_table_multi_{source_key}"
                                            )
                                        else:
                                            ref_table_name_multi = None
                                            st.selectbox("Table", options=["Select schema first"], key=f"cross_ref_table_multi_{source_key}_disabled", disabled=True)

                                    if ref_schema_multi and ref_table_name_multi:
                                        # Build fully qualified table name
                                        ref_table_multi = f"{selected_db}.{ref_schema_multi}.{ref_table_name_multi}"
                                        # Step 3
                                        st.markdown("**Step 3:** What condition on the other table?")
                                        st.caption("Filter which records from the other table to include")

                                        col1, col2, col3 = st.columns([3, 2, 3])

                                        with col1:
                                            ref_col_filter_multi = st.text_input(
                                                "Column name",
                                                placeholder="country, status, type",
                                                key=f"cross_ref_col_multi_{source_key}"
                                            )

                                        with col2:
                                            ref_operator_multi = st.selectbox(
                                                "Condition",
                                                options=["=", "!=", "IN", "LIKE", "IS NOT NULL"],
                                                key=f"cross_ref_op_multi_{source_key}"
                                            )

                                        with col3:
                                            if ref_operator_multi not in ["IS NOT NULL"]:
                                                ref_value_multi = st.text_input(
                                                    "Value",
                                                    placeholder="FR, ACTIVE, etc.",
                                                    key=f"cross_ref_val_multi_{source_key}"
                                                )
                                            else:
                                                ref_value_multi = None

                                        # Build the full filter
                                        if ref_col_filter_multi and (ref_value_multi or ref_operator_multi == "IS NOT NULL"):
                                            # Build the reference condition
                                            if ref_operator_multi == "IS NOT NULL":
                                                ref_condition_multi = f"{ref_col_filter_multi} IS NOT NULL"
                                            elif ref_operator_multi == "IN":
                                                values = [v.strip().strip("'\"") for v in ref_value_multi.split(",")]
                                                values_str = ", ".join([f"'{v}'" for v in values])
                                                ref_condition_multi = f"{ref_col_filter_multi} IN ({values_str})"
                                            else:
                                                if not ref_value_multi.startswith("'"):
                                                    ref_value_multi = f"'{ref_value_multi}'"
                                                ref_condition_multi = f"{ref_col_filter_multi} {ref_operator_multi} {ref_value_multi}"

                                            filter_condition_multi = f"{join_col_multi} IN (SELECT {join_col_multi} FROM {ref_table_multi} WHERE {ref_condition_multi})"

                                            st.success(f"✓ Filter created!")
                                            st.code(filter_condition_multi, language="sql")
                                            st.caption("💡 This will only validate rows that match records in the other table")
                                        elif ref_col_filter_multi or ref_value_multi:
                                            st.info("👆 Complete all fields above to create the filter")

                            else:  # SQL (Advanced)
                                st.caption("Write SQL WHERE condition manually")
                                filter_condition_multi = st.text_input(
                                    "SQL WHERE condition",
                                    placeholder="Example: active = TRUE  or  status IN ('NEW', 'ACTIVE')",
                                    key=f"filter_condition_multi_{source_key}"
                                )

                            if filter_condition_multi:
                                st.info("💡 Filters can improve performance by 10-100x on large tables!")

                        if st.button("Add Cross-Column Rule", type="primary", key=f"add_multi_rule_btn_{source_key}"):
                            if not nl_rule_input_multi.strip():
                                st.markdown(f"{WARNING_ICON} Please enter a rule description", unsafe_allow_html=True)
                            elif not selected_cols_for_rule or len(selected_cols_for_rule) < 2:
                                st.markdown(f"{WARNING_ICON} Please select at least 2 columns for a cross-column rule", unsafe_allow_html=True)
                            else:
                                with st.spinner("Translating cross-column rule via AI..."):
                                    try:
                                        def llm_fn(prompt):
                                            return call_cortex_for_rule(conn, prompt)

                                        updated_yaml = add_table_level_rule_from_natural_language(
                                            yaml_text=st.session_state.yaml_content,
                                            column_names=selected_cols_for_rule,
                                            nl_rule=nl_rule_input_multi,
                                            llm_call_fn=llm_fn
                                        )

                                        # If filter is specified, add it to the params of the newly added table rule
                                        if filter_condition_multi and filter_condition_multi.strip():
                                            import yaml as yaml_module
                                            parsed = yaml_module.safe_load(updated_yaml)
                                            table_rules = parsed["semantic_view"].get("table_rules", [])

                                            # Add filter to the last added rule
                                            if table_rules:
                                                last_rule = table_rules[-1]
                                                if "params" not in last_rule or last_rule["params"] is None:
                                                    last_rule["params"] = {}
                                                last_rule["params"]["filter"] = filter_condition_multi.strip()

                                            updated_yaml = yaml_module.dump(parsed, default_flow_style=False, sort_keys=False, allow_unicode=True)

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

                        # View-level filters section
                        filters = sv.get("filters", [])
                        if filters:
                            st.markdown(f"**View-Level Filters** ({len(filters)} total)")
                            st.caption("These filters will be applied when creating the semantic view")

                            for idx, filter_sql in enumerate(filters, 1):
                                st.markdown(f"Filter {idx}:")
                                st.code(filter_sql, language="sql")

                            # Show combined WHERE clause if multiple filters
                            if len(filters) > 1:
                                combined_where = " AND ".join([f"({f})" for f in filters])
                                st.markdown("**Combined WHERE clause:**")
                                st.code(f"WHERE {combined_where}", language="sql")
                            elif len(filters) == 1:
                                st.markdown("**WHERE clause:**")
                                st.code(f"WHERE {filters[0]}", language="sql")

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

            # ========== RULE EXECUTION & TESTING ==========
            st.markdown("---")
            st.markdown("#### 🧪 Test Data Quality Rules")
            st.caption("Run your data quality rules against actual data to find violations")

            with st.container(border=True):
                # Initialize session state for validation results
                if "validation_results" not in st.session_state:
                    st.session_state.validation_results = None

                col1, col2, col3 = st.columns([2, 1, 1])

                with col1:
                    if st.button("▶️ Run All Rules", type="primary", use_container_width=True, key="run_all_rules"):
                        if not st.session_state.yaml_content.strip():
                            st.markdown(f"{WARNING_ICON} No YAML content to validate", unsafe_allow_html=True)
                        else:
                            with st.spinner("Executing data quality rules..."):
                                try:
                                    results = execute_all_rules(
                                        conn=conn,
                                        yaml_content=st.session_state.yaml_content,
                                        limit_per_rule=100
                                    )
                                    st.session_state.validation_results = results
                                    if "error" in results:
                                        st.error(f"❌ Execution failed: {results['error']}")
                                    else:
                                        summary = results.get("summary", {})
                                        total_violations = summary.get("total_violations", 0)
                                        if total_violations == 0:
                                            st.success("✅ All rules passed! No violations found.")
                                        else:
                                            st.markdown(f"{WARNING_ICON} Found {total_violations} total violations across {summary.get('rules_with_violations', 0)} rules", unsafe_allow_html=True)
                                except Exception as e:
                                    st.error(f"❌ Failed to execute rules: {e}")

                with col2:
                    max_rows = st.number_input(
                        "Max violations per rule",
                        min_value=10,
                        max_value=1000,
                        value=100,
                        step=10,
                        help="Limit the number of violation examples returned for each rule",
                        key="max_violations_limit"
                    )

                with col3:
                    if st.button("🗑️ Clear Results", use_container_width=True, key="clear_results"):
                        st.session_state.validation_results = None
                        st.rerun()

            # Display validation results
            if st.session_state.validation_results and "error" not in st.session_state.validation_results:
                results = st.session_state.validation_results
                summary = results.get("summary", {})

                # Summary metrics
                with st.container(border=True):
                    st.markdown("**Validation Summary**")

                    col1, col2, col3, col4, col5 = st.columns(5)

                    col1.metric(
                        "Total Rules",
                        summary.get("total_rules", 0),
                        help="Total number of rules executed"
                    )

                    passed = summary.get("rules_passed", 0)
                    col2.metric(
                        "Passed",
                        passed,
                        delta=None,
                        delta_color="normal",
                        help="Rules with zero violations"
                    )

                    failed = summary.get("rules_with_violations", 0)
                    col3.metric(
                        "Failed",
                        failed,
                        delta=None,
                        delta_color="inverse",
                        help="Rules with one or more violations"
                    )

                    pass_rate = summary.get("overall_pass_rate", 100.0)
                    col4.metric(
                        "Pass Rate",
                        f"{pass_rate:.1f}%",
                        help="Percentage of rules that passed"
                    )

                    col5.metric(
                        "Total Violations",
                        summary.get("total_violations", 0),
                        help="Total number of violating rows across all rules"
                    )

                    # Severity breakdown
                    st.write("")  # spacing
                    st.markdown("**Violations by Severity**")

                    col1, col2, col3, col4 = st.columns(4)
                    critical = summary.get("critical_violations", 0)
                    warning = summary.get("warning_violations", 0)
                    info = summary.get("info_violations", 0)
                    total_rows = summary.get("total_rows", 0)

                    col1.metric("🔴 Critical", critical)
                    col2.metric("🟡 Warning", warning)
                    col3.metric("🔵 Info", info)
                    col4.metric("📊 Total Rows", total_rows)

                # Detailed results by rule
                st.write("")  # spacing
                st.markdown("**Rule Results**")

                all_rule_results = results.get("column_rules_results", []) + results.get("table_rules_results", [])

                if all_rule_results:
                    # Create results dataframe
                    results_data = []
                    for r in all_rule_results:
                        # Handle None or invalid results
                        if not isinstance(r, dict):
                            continue
                    
                        severity_icon = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}.get(r.get("severity", ""), "")
                    
                        total_rows = r.get("total_rows", 0)
                        violation_count = r.get("violation_count", 0)
                        pass_rate = r.get("pass_rate", 0)
                    
                        # Determine status: error if total_rows is 0, fail if violations > 0, pass otherwise
                        if total_rows == 0 and "error" not in r:
                            status = "⚠️ Error"
                            error_msg = "No rows evaluated"
                        elif "error" in r and r["error"]:
                            status = "⚠️ Error"
                            error_msg = r["error"]
                        elif violation_count > 0:
                            status = "❌ Fail"
                            error_msg = ""
                        else:
                            status = "✅ Pass"
                            error_msg = ""

                        results_data.append({
                            "": severity_icon,
                            "Rule": r.get("rule_type", ""),
                            "Column(s)": r.get("column", "") if "column" in r else ", ".join(r.get("columns", [])),
                            "Violations": violation_count,
                            "Total Rows": total_rows,
                            "Pass Rate": f"{pass_rate:.1f}%",
                            "Status": status,
                            "Error": error_msg
                        })

                    results_df = pd.DataFrame(results_data)

                    st.dataframe(
                        results_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "": st.column_config.TextColumn("", width="small"),
                            "Rule": st.column_config.TextColumn("Rule Type", width="medium"),
                            "Column(s)": st.column_config.TextColumn("Column(s)", width="medium"),
                            "Violations": st.column_config.NumberColumn("Violations", width="small"),
                            "Total Rows": st.column_config.NumberColumn("Total Rows", width="small"),
                            "Pass Rate": st.column_config.TextColumn("Pass Rate", width="small"),
                            "Status": st.column_config.TextColumn("Status", width="small"),
                            "Error": st.column_config.TextColumn("Error", width="large"),
                        }
                    )

                    # Show violation details
                    st.write("")  # spacing
                    st.markdown("**Violation Details**")

                    # Filter to rules with violations
                    rules_with_violations = [r for r in all_rule_results if r.get("violation_count", 0) > 0]

                    if rules_with_violations:
                        for idx, r in enumerate(rules_with_violations):
                            rule_name = r.get("rule_type", "Unknown")
                            column_name = r.get("column", "") if "column" in r else ", ".join(r.get("columns", []))
                            violation_count = r.get("violation_count", 0)
                            severity = r.get("severity", "")

                            severity_icon = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}.get(severity, "")

                            with st.expander(f"{severity_icon} {rule_name} on {column_name} ({violation_count} violations)", expanded=False):
                                violations = r.get("violations", [])
                            
                                # Handle error if present
                                if "error" in r and r["error"]:
                                    st.error(f"{WARNING_ICON} Error executing rule: {r['error']}", unsafe_allow_html=True)

                                if violations:
                                    st.caption(f"Showing up to {len(violations)} violation examples")
                                    try:
                                        violations_df = pd.DataFrame(violations)
                                        st.dataframe(violations_df, use_container_width=True, hide_index=True)
                                    except Exception as e:
                                        st.warning(f"Could not display violations table: {str(e)}")
                                        st.write(violations)
                                else:
                                    st.info("No violation examples available")

                                # Show SQL query used
                                sql_query = r.get("sql_query", "")
                                if sql_query:
                                    st.markdown("**SQL Query Used:**")
                                    st.code(sql_query, language="sql")
                    else:
                        st.success("🎉 No violations found! All rules passed.")

                    # Export results
                    st.write("")  # spacing

                    # Create CSV export
                    export_data = []
                    for r in all_rule_results:
                        export_data.append({
                            "Rule Type": r.get("rule_type", ""),
                            "Column(s)": r.get("column", "") if "column" in r else ", ".join(r.get("columns", [])),
                            "Severity": r.get("severity", ""),
                            "Violation Count": r.get("violation_count", 0),
                            "Total Rows": r.get("total_rows", 0),
                            "Pass Rate": f"{r.get('pass_rate', 0):.2f}",
                            "Status": "Pass" if r.get("violation_count", 0) == 0 else "Fail",
                            "Error": r.get("error", "")
                        })

                    export_df = pd.DataFrame(export_data)
                    csv_data = export_df.to_csv(index=False)

                    st.download_button(
                        label="📥 Download Results as CSV",
                        data=csv_data,
                        file_name=f"dq_validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )

            # Save to Snowflake
            st.markdown("---")
            st.markdown("#### 💾 Save to Snowflake Registry")

            with st.expander("📦 Save to Snowflake Registry (Optional)"):
                st.caption("Save your semantic YAML definition to the Snowflake registry table for centralized management")
                st.markdown(f"{WARNING_ICON} This requires a `SEMANTIC_CONFIG.SEMANTIC_VIEW` table (see README for DDL)", unsafe_allow_html=True)

                save_status = st.selectbox("📊 Status", ["DRAFT", "ACTIVE", "ARCHIVED"], index=0)

                if st.button("💾 Save to Registry", type="primary", key="save_to_registry"):
                    if not st.session_state.yaml_content.strip():
                        st.markdown(f"{WARNING_ICON} No YAML content to save", unsafe_allow_html=True)
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

    # ============================================================================
    # TAB DQ: Data Quality Assessment Dashboard
    # ============================================================================
    with tab_dq:
        render_dq_dashboard_tab(conn)

    # ============================================================================
    # TAB 5: Document Quality (v1.1 Feature - Beta)
    # ============================================================================
    with tab5:
        st.markdown("### 📄 Document Quality (Beta)")
        st.caption("Upload and analyze documents for AI training data preparation")
        st.write("")  # spacing

        # Import document quality module
        try:
            from document_quality import (
                parse_document_with_cortex,
                embed_text_with_cortex,
                find_similar_documents,
                store_document,
                get_document_stats
            )
        except ImportError:
            st.error("Document quality module not available. Please ensure document_quality.py is in the same directory.")
            st.stop()

        # Document stats overview
        with st.container(border=True):
            st.markdown("**Document Library Overview**")
            st.write("")  # spacing

            try:
                stats = get_document_stats(conn)
            
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Documents", stats['total_docs'])
                with col2:
                    st.metric("File Types", stats['file_types'])
                with col3:
                    st.metric("Total Size (MB)", stats['total_size_mb'])
                with col4:
                    if stats['last_upload']:
                        st.metric("Last Upload", stats['last_upload'].strftime("%Y-%m-%d"))
                    else:
                        st.metric("Last Upload", "Never")
            except Exception as e:
                st.warning(f"Could not load document stats: {e}")

        st.write("")  # spacing

        # Upload section
        with st.container(border=True):
            st.markdown("**Upload Document**")
            st.write("")  # spacing

            col1, col2 = st.columns([2, 1])

            with col1:
                uploaded_file = st.file_uploader(
                    "Choose a file",
                    type=['pdf', 'docx', 'txt', 'doc'],
                    help="Upload PDFs, Word documents, or text files for analysis"
                )

            with col2:
                st.write("")  # spacing
                st.caption("**Supported formats:**")
                st.caption("• PDF (.pdf)")
                st.caption("• Word (.docx, .doc)")
                st.caption("• Text (.txt)")

            if uploaded_file is not None:
                st.write("")  # spacing
                st.info(f"📄 File: {uploaded_file.name} ({uploaded_file.size / 1024:.2f} KB)")

                if st.button("Analyze Document", type="primary"):
                    with st.spinner("Parsing document with Snowflake Cortex..."):
                        try:
                            # Read file content
                            file_content = uploaded_file.read()
                            file_type = uploaded_file.name.split('.')[-1].lower()
                        
                            # Parse document
                            text_content = parse_document_with_cortex(
                                conn, 
                                file_content, 
                                uploaded_file.name
                            )
                        
                            if not text_content or len(text_content) < 50:
                                st.error("Could not extract meaningful text from document")
                            else:
                                st.success(f"✅ Extracted {len(text_content)} characters")

                                # Show preview
                                with st.expander("📄 Text Preview"):
                                    st.text_area(
                                        "Extracted Text (first 1000 chars)",
                                        text_content[:1000],
                                        height=200,
                                        disabled=True
                                    )

                                # Generate embedding
                                with st.spinner("Generating AI embeddings..."):
                                    embedding = embed_text_with_cortex(conn, text_content)
                                
                                    if not embedding:
                                        st.warning("Could not generate embedding")
                                    else:
                                        st.success(f"✅ Generated {len(embedding)}-dimensional embedding")

                                # Find duplicates
                                with st.spinner("Checking for similar documents..."):
                                    similar_docs = find_similar_documents(conn, embedding, threshold=0.7)

                                    if similar_docs:
                                        st.markdown(f"{WARNING_ICON} Found {len(similar_docs)} similar document(s)", unsafe_allow_html=True)
                                    
                                        with st.expander(f"{SEARCH_ICON} Similar Documents", expanded=True):
                                            for doc in similar_docs[:5]:  # Show top 5
                                                col_a, col_b, col_c = st.columns([3, 1, 1])
                                            
                                                with col_a:
                                                    st.write(f"**{doc['filename']}**")
                                                with col_b:
                                                    similarity_pct = doc['similarity']
                                                    color = "🔴" if similarity_pct > 90 else "🟡" if similarity_pct > 80 else "🟢"
                                                    st.write(f"{color} {similarity_pct}% similar")
                                                with col_c:
                                                    st.caption(doc['upload_date'].strftime("%Y-%m-%d"))

                                            st.write("")  # spacing
                                        
                                            if max([d['similarity'] for d in similar_docs]) > 90:
                                                st.error("🚨 Very high similarity detected - likely duplicate!")
                                                if st.button("Mark as Duplicate"):
                                                    st.info("Duplicate marking functionality coming soon")
                                            elif max([d['similarity'] for d in similar_docs]) > 80:
                                                st.markdown(f"{WARNING_ICON} High similarity - may be related versions", unsafe_allow_html=True)
                                            else:
                                                st.info("ℹ️ Some similarity found - possibly related documents")
                                    else:
                                        st.success("✅ No similar documents found - this is unique content")

                                # Store document
                                st.write("")  # spacing
                                if st.button("💾 Save to Document Library"):
                                    with st.spinner("Saving document..."):
                                        doc_id = store_document(
                                            conn,
                                            uploaded_file.name,
                                            text_content,
                                            embedding,
                                            file_type,
                                            uploaded_file.size
                                        )
                                    
                                        if doc_id.startswith("DUPLICATE_"):
                                            st.markdown(f"{WARNING_ICON} Exact duplicate detected! Original ID: {doc_id.replace('DUPLICATE_', '')}", unsafe_allow_html=True)
                                        elif doc_id:
                                            st.success(f"✅ Document saved! ID: {doc_id}")
                                            st.balloons()
                                        else:
                                            st.error("❌ Failed to save document")

                        except Exception as e:
                            st.error(f"❌ Error analyzing document: {e}")
                            import traceback
                            with st.expander("Error Details"):
                                st.code(traceback.format_exc())

        st.write("")  # spacing

        # Future features preview
        with st.expander(f"{ROCKET_ICON} Coming Soon in v1.2"):
            st.markdown("""
            **Advanced Document Quality Features:**
        
            - 🔗 **Automatic table linking:** AI detects which database tables are mentioned in documents
            - 📊 **Quality scoring:** Rate documents by completeness, clarity, and consistency
            - 📑 **Version detection:** Automatically identify document versions and recommend canonical version
            - 🤝 **Cross-reference validation:** Check if process docs match actual data constraints
            - 📤 **AI training data export:** Export clean, deduplicated dataset for LLM training
            - {SYNC_ICON} **Bulk upload:** Process multiple documents at once
            - 📈 **Analytics dashboard:** Visualize document quality trends over time
        
            **Want early access? Contact us!**
            """)

        # Link to current table (if one is selected)
        if st.session_state.selected_db and st.session_state.selected_schema and st.session_state.selected_table:
            st.write("")  # spacing
            with st.container(border=True):
                st.markdown("**Link Documents to Current Table**")
                st.caption(f"Current table: {st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}")
            
                st.info("ℹ️ Document-table linking feature coming in v1.2")
                st.caption("This will help you:")
                st.caption("• Find all documentation related to a table")
                st.caption("• Verify data rules match policy documents")
                st.caption("• Prepare unified training data (structured + unstructured)")
