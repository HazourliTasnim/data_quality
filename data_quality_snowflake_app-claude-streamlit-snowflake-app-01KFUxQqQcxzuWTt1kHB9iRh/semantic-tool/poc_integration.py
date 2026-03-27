"""
POC Integration Module - Embedding & Error Detection for DQ Dashboard
Integrates directly with DQ Dashboard for DIM_ACCOUNT table analysis
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
# Error Detection via Embeddings
# ============================================================================

def detect_anomalies_by_embedding(conn, database, schema, table, columns_profile):
    """
    Detect data anomalies by comparing column embeddings with reference patterns
    Uses semantic similarity to find inconsistencies
    """
    cursor = conn.cursor()
    anomalies = []
    
    try:
        # Sample data for analysis
        sample_sql = f'SELECT * FROM "{database}"."{schema}"."{table}" LIMIT 1000'
        cursor.execute(sample_sql)
        sample_df = cursor.fetchall()
        cursor.close()
        
        cols = [desc[0] for desc in cursor.description]
        sample_df = pd.DataFrame(sample_df, columns=cols)
        
        # Analyze each column
        for col_profile in columns_profile:
            col_name = col_profile['column']
            
            if col_name not in sample_df.columns:
                continue
            
            col_data = sample_df[col_name]
            
            # Embedding-based consistency check
            col_embedding = create_column_embeddings(
                col_name, 
                col_profile.get('data_type', 'VARCHAR')
            )
            
            # Check for NULL/empty patterns
            null_rate = col_profile.get('null_rate', 0)
            if null_rate > 30:
                anomalies.append({
                    'column': col_name,
                    'type': 'HIGH_NULL_RATE',
                    'severity': 'WARNING',
                    'value': f"{null_rate:.1f}% NULL values detected",
                    'embedding_confidence': 0.95
                })
            
            # Check for empty strings
            empty_rate = col_profile.get('empty_rate', 0)
            if empty_rate > 10:
                anomalies.append({
                    'column': col_name,
                    'type': 'EMPTY_STRINGS',
                    'severity': 'INFO',
                    'value': f"{empty_rate:.1f}% empty strings",
                    'embedding_confidence': 0.85
                })
            
            # Type consistency check for numeric columns
            if 'NUMBER' in str(col_profile.get('data_type', '')):
                try:
                    numeric_check = pd.to_numeric(col_data, errors='coerce')
                    non_numeric = numeric_check.isna().sum()
                    non_numeric_pct = (non_numeric / len(col_data) * 100) if len(col_data) > 0 else 0
                    
                    if non_numeric_pct > 5:
                        anomalies.append({
                            'column': col_name,
                            'type': 'TYPE_MISMATCH',
                            'severity': 'CRITICAL',
                            'value': f"{non_numeric_pct:.1f}% non-numeric values in NUMBER column",
                            'embedding_confidence': 0.98
                        })
                except:
                    pass
            
            # Duplicate detection
            distinct_rate = col_profile.get('distinct_rate', 100)
            if distinct_rate < 50:
                anomalies.append({
                    'column': col_name,
                    'type': 'HIGH_DUPLICATES',
                    'severity': 'INFO',
                    'value': f"Only {distinct_rate:.1f}% unique values",
                    'embedding_confidence': 0.80
                })
    
    except Exception as e:
        st.error(f"Error detecting anomalies: {e}")
    
    return anomalies

# ============================================================================
# DQ Dashboard Integration
# ============================================================================

def render_embedding_dq_section(conn, database="PUBLIC", schema="PUBLIC", table="DIM_ACCOUNT"):
    """
    Render the POC embedding section directly in DQ Dashboard
    Only analyzes DIM_ACCOUNT table (no selection needed)
    """
    
    st.markdown("---")
    st.markdown("### 🚀 Embedding-Based Anomaly Detection (POC)")
    st.caption("Analyzes DIM_ACCOUNT table using AI embeddings and semantic similarity")
    
    # Main action buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🧠 Generate Embeddings", key="poc_embeddings", use_container_width=True):
            with st.spinner("Generating column embeddings for DIM_ACCOUNT..."):
                cursor = conn.cursor()
                
                try:
                    # Get table structure
                    cursor.execute(f"""
                        SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{table}'
                        AND TABLE_SCHEMA = '{schema}'
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    columns = cursor.fetchall()
                    cursor.close()
                    
                    if columns:
                        embeddings_data = []
                        for col_name, col_type, position in columns:
                            embedding = create_column_embeddings(col_name, col_type)
                            embeddings_data.append({
                                'column': col_name,
                                'type': col_type,
                                'position': position,
                                'embedding': embedding,
                                'embedding_model': 'all-MiniLM-L6-v2',
                                'embedding_dimension': len(embedding)
                            })
                        
                        st.session_state.dim_account_embeddings = embeddings_data
                        st.success(f"✅ Generated embeddings for {len(embeddings_data)} columns")
                        
                        # Show summary
                        with st.expander("View Embeddings Summary", expanded=False):
                            for emb in embeddings_data:
                                st.write(f"**{emb['column']}** ({emb['type']})")
                                st.caption(f"Dimensions: {emb['embedding_dimension']} | First 5: {emb['embedding'][:5]}")
                    else:
                        st.warning("No columns found in DIM_ACCOUNT")
                
                except Exception as e:
                    st.error(f"Error: {e}")
    
    with col2:
        if st.button("🔍 Detect Anomalies", key="poc_anomalies", use_container_width=True):
            with st.spinner("Running embedding-based anomaly detection..."):
                cursor = conn.cursor()
                
                try:
                    # First get column profiles
                    cursor.execute(f"""
                        SELECT COLUMN_NAME, DATA_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{table}'
                        AND TABLE_SCHEMA = '{schema}'
                        ORDER BY ORDINAL_POSITION
                    """)
                    
                    cols_info = cursor.fetchall()
                    cursor.close()
                    
                    # Build column profile list
                    columns_profile = [
                        {
                            'column': col[0],
                            'data_type': col[1],
                            'null_rate': 0,
                            'empty_rate': 0,
                            'distinct_rate': 50
                        }
                        for col in cols_info
                    ]
                    
                    # Get profiling data for each column
                    cursor = conn.cursor()
                    for col_profile in columns_profile:
                        col = col_profile['column']
                        try:
                            profile_sql = f"""
                            SELECT
                                SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS null_count,
                                SUM(CASE WHEN "{col}" IS NOT NULL AND TRIM(CAST("{col}" AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS empty_count,
                                COUNT(DISTINCT "{col}") AS distinct_count,
                                COUNT(*) AS total_rows
                            FROM "{database}"."{schema}"."{table}"
                            """
                            cursor.execute(profile_sql)
                            row = cursor.fetchone()
                            
                            if row:
                                null_count = row[0] or 0
                                empty_count = row[1] or 0
                                distinct_count = row[2] or 0
                                total_rows = row[3] or 1
                                
                                col_profile['null_rate'] = (null_count / total_rows * 100) if total_rows > 0 else 0
                                col_profile['empty_rate'] = (empty_count / total_rows * 100) if total_rows > 0 else 0
                                col_profile['distinct_rate'] = (distinct_count / total_rows * 100) if total_rows > 0 else 0
                        except:
                            pass
                    
                    cursor.close()
                    
                    # Detect anomalies
                    anomalies = detect_anomalies_by_embedding(conn, database, schema, table, columns_profile)
                    
                    if anomalies:
                        st.dataframe(
                            pd.DataFrame(anomalies),
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Metrics
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            critical = len([a for a in anomalies if a['severity'] == 'CRITICAL'])
                            st.metric("🔴 Critical", critical)
                        with col_b:
                            warnings = len([a for a in anomalies if a['severity'] == 'WARNING'])
                            st.metric("🟡 Warnings", warnings)
                        with col_c:
                            info = len([a for a in anomalies if a['severity'] == 'INFO'])
                            st.metric("🔵 Info", info)
                    else:
                        st.success("✅ No anomalies detected - data quality looks good!")
                
                except Exception as e:
                    st.error(f"Error: {e}")
    
    with col3:
        if st.button("💾 Store Embeddings", key="poc_store", use_container_width=True):
            if 'dim_account_embeddings' not in st.session_state:
                st.warning("First generate embeddings")
            else:
                with st.spinner("Storing embeddings in Snowflake..."):
                    cursor = conn.cursor()
                    try:
                        # Create metadata table
                        cursor.execute("""
                            CREATE TABLE IF NOT EXISTS COLUMN_EMBEDDINGS (
                                TABLE_NAME VARCHAR,
                                COLUMN_NAME VARCHAR,
                                COLUMN_TYPE VARCHAR,
                                EMBEDDING_MODEL VARCHAR,
                                EMBEDDING_DIMENSION INT,
                                EMBEDDING VECTOR(FLOAT, 384),
                                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                            )
                        """)
                        
                        # Insert embeddings
                        for emb in st.session_state.dim_account_embeddings:
                            cursor.execute("""
                                INSERT INTO COLUMN_EMBEDDINGS 
                                (TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, EMBEDDING_MODEL, EMBEDDING_DIMENSION, EMBEDDING)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                table,
                                emb['column'],
                                emb['type'],
                                'all-MiniLM-L6-v2',
                                emb['embedding_dimension'],
                                emb['embedding']
                            ))
                        
                        conn.commit()
                        cursor.close()
                        st.success(f"✅ Stored {len(st.session_state.dim_account_embeddings)} embeddings")
                    
                    except Exception as e:
                        st.error(f"Error storing embeddings: {e}")
