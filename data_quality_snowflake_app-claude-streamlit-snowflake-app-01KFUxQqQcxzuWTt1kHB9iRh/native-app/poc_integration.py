"""
POC Integration Module - Embedding & Error Detection for Data Quality
Integrates with existing Streamlit app for DIM_ACCOUNT table
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
import streamlit as st

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    st.error("sentence-transformers not installed. Run: pip install sentence-transformers")
    st.stop()

# ============================================================================
# Embedding Functions
# ============================================================================

@st.cache_resource
def load_embedding_model():
    """Load the embedding model once and cache it"""
    return SentenceTransformer('all-MiniLM-L6-v2')

def create_column_embeddings(column_name, data_type, description=""):
    """Create embedding for a table column"""
    model = load_embedding_model()
    text = f"{column_name} {data_type} {description}"
    embedding = model.encode(text, convert_to_numpy=True)
    return embedding.tolist()

def calculate_similarity(embedding1, embedding2):
    """Calculate cosine similarity between two embeddings"""
    emb1 = np.array(embedding1)
    emb2 = np.array(embedding2)
    return float(np.dot(emb1, emb2) / (np.linalg.norm(emb1) * np.linalg.norm(emb2)))

# ============================================================================
# Error Detection Functions
# ============================================================================

def detect_quality_issues(column_data, column_name, column_type, reference_embeddings=None):
    """
    Detect quality issues in column data using embeddings
    Returns: list of detected issues
    """
    issues = []
    
    # Issue 1: NULL values
    null_count = sum(1 for x in column_data if x is None or x == "")
    null_pct = (null_count / len(column_data) * 100) if column_data else 0
    if null_pct > 10:
        issues.append({
            "type": "HIGH_NULLS",
            "severity": "WARNING",
            "message": f"Column has {null_pct:.1f}% NULL values",
            "count": null_count
        })
    
    # Issue 2: Type mismatch
    if column_type == "NUMBER":
        non_numeric = sum(1 for x in column_data if x and not isinstance(x, (int, float)))
        non_numeric_pct = (non_numeric / len(column_data) * 100) if column_data else 0
        if non_numeric_pct > 5:
            issues.append({
                "type": "TYPE_MISMATCH",
                "severity": "CRITICAL",
                "message": f"Column should be NUMBER but {non_numeric_pct:.1f}% are non-numeric",
                "count": non_numeric
            })
    
    # Issue 3: Duplicate values
    unique_count = len(set(str(x) for x in column_data if x))
    duplicate_pct = ((len(column_data) - unique_count) / len(column_data) * 100) if column_data else 0
    if duplicate_pct > 20:
        issues.append({
            "type": "HIGH_DUPLICATES",
            "severity": "INFO",
            "message": f"Column has only {unique_count} unique values ({100-duplicate_pct:.1f}% duplicates)",
            "count": len(column_data) - unique_count
        })
    
    # Issue 4: Column name consistency (using embeddings if available)
    if reference_embeddings:
        current_embedding = create_column_embeddings(column_name, column_type)
        for ref in reference_embeddings:
            sim = calculate_similarity(current_embedding, ref.get('embedding'))
            if sim > 0.85 and ref['name'] != column_name:
                issues.append({
                    "type": "SIMILAR_COLUMN",
                    "severity": "INFO",
                    "message": f"Column name similar to {ref['name']} ({sim:.1%} match)",
                    "count": 1
                })
    
    return issues

# ============================================================================
# Snowflake Integration
# ============================================================================

def get_dim_account_columns(conn):
    """Fetch DIM_ACCOUNT table structure from Snowflake"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'DIM_ACCOUNT'
            AND TABLE_SCHEMA = 'PUBLIC'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        cursor.close()
        
        return [
            {
                "name": col[0],
                "type": col[1],
                "position": col[2],
                "embedding": create_column_embeddings(col[0], col[1])
            }
            for col in columns
        ]
    except Exception as e:
        st.error(f"Error fetching DIM_ACCOUNT columns: {e}")
        return []

def get_dim_account_sample_data(conn, limit=100):
    """Fetch sample data from DIM_ACCOUNT"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM DIM_ACCOUNT LIMIT {limit}")
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error fetching DIM_ACCOUNT sample: {e}")
        return None

def store_embeddings_in_snowflake(conn, column_embeddings):
    """Store column embeddings in Snowflake metadata table"""
    try:
        cursor = conn.cursor()
        
        # Create metadata table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS COLUMN_EMBEDDINGS (
                TABLE_NAME VARCHAR,
                COLUMN_NAME VARCHAR,
                COLUMN_TYPE VARCHAR,
                EMBEDDING VECTOR(FLOAT, 384),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Insert embeddings
        for col in column_embeddings:
            cursor.execute("""
                INSERT INTO COLUMN_EMBEDDINGS (TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, EMBEDDING)
                VALUES ('DIM_ACCOUNT', %s, %s, %s)
            """, (col['name'], col['type'], col['embedding']))
        
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        st.error(f"Error storing embeddings: {e}")
        return False

# ============================================================================
# UI Components
# ============================================================================

def render_poc_section(conn):
    """Add POC controls to the Data Quality Rules section"""
    
    st.markdown("---")
    st.markdown("### POC: Embedding-Based Error Detection")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Analyze DIM_ACCOUNT", key="poc_analyze"):
            with st.spinner("Analyzing DIM_ACCOUNT structure..."):
                columns = get_dim_account_columns(conn)
                if columns:
                    st.session_state.dim_account_columns = columns
                    st.success(f"Loaded {len(columns)} columns from DIM_ACCOUNT")
                    
                    # Show column embeddings
                    with st.expander("View Column Embeddings", expanded=False):
                        for col in columns:
                            st.write(f"**{col['name']}** ({col['type']})")
                            embedding_preview = col['embedding'][:5]
                            st.caption(f"Embedding preview: {embedding_preview}...")
    
    with col2:
        if st.button("Detect Quality Issues", key="poc_detect"):
            if 'dim_account_columns' not in st.session_state:
                st.warning("First click 'Analyze DIM_ACCOUNT'")
            else:
                with st.spinner("Detecting quality issues..."):
                    sample_df = get_dim_account_sample_data(conn)
                    if sample_df is not None:
                        all_issues = []
                        
                        for col in st.session_state.dim_account_columns:
                            col_name = col['name']
                            col_type = col['type']
                            
                            if col_name in sample_df.columns:
                                col_data = sample_df[col_name].tolist()
                                issues = detect_quality_issues(
                                    col_data, 
                                    col_name, 
                                    col_type,
                                    st.session_state.dim_account_columns
                                )
                                
                                if issues:
                                    for issue in issues:
                                        all_issues.append({
                                            "Column": col_name,
                                            "Type": col_type,
                                            "Issue": issue['type'],
                                            "Severity": issue['severity'],
                                            "Message": issue['message']
                                        })
                        
                        if all_issues:
                            issues_df = pd.DataFrame(all_issues)
                            st.dataframe(issues_df, use_container_width=True)
                            
                            # Summary metrics
                            col_a, col_b, col_c = st.columns(3)
                            with col_a:
                                critical = len([i for i in all_issues if i['Severity'] == 'CRITICAL'])
                                st.metric("Critical", critical)
                            with col_b:
                                warnings = len([i for i in all_issues if i['Severity'] == 'WARNING'])
                                st.metric("Warnings", warnings)
                            with col_c:
                                infos = len([i for i in all_issues if i['Severity'] == 'INFO'])
                                st.metric("Info", infos)
                        else:
                            st.success("No quality issues detected!")
    
    with col3:
        if st.button("Store Embeddings", key="poc_store"):
            if 'dim_account_columns' not in st.session_state:
                st.warning("First click 'Analyze DIM_ACCOUNT'")
            else:
                with st.spinner("Storing embeddings in Snowflake..."):
                    if store_embeddings_in_snowflake(conn, st.session_state.dim_account_columns):
                        st.success(f"Stored {len(st.session_state.dim_account_columns)} column embeddings")
                    else:
                        st.error("Failed to store embeddings")
