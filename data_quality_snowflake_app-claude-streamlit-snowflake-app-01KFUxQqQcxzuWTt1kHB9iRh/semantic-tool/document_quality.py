"""
Document Quality Module - v1.1 Feature
Handles unstructured data upload, deduplication, and quality checks
"""

import streamlit as st
from typing import List, Dict, Any
import hashlib
import json
from datetime import datetime


def parse_document_with_cortex(conn, file_content: bytes, file_name: str) -> str:
    """
    Parse document using Snowflake Cortex PARSE_DOCUMENT

    Args:
        conn: Snowflake connection
        file_content: Binary content of the file
        file_name: Name of the uploaded file

    Returns:
        Extracted text from document
    """
    cursor = conn.cursor()
    try:
        # Create temporary stage if not exists
        cursor.execute("""
            CREATE STAGE IF NOT EXISTS DATA_QUALITY_AI.CORE.DOCUMENT_STAGE
            DIRECTORY = (ENABLE = TRUE)
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = NONE RECORD_DELIMITER = NONE);
        """)

        # Upload file to stage
        cursor.execute(f"PUT file://{file_name} @DATA_QUALITY_AI.CORE.DOCUMENT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        # Parse document using Cortex
        cursor.execute(f"""
            SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                @DATA_QUALITY_AI.CORE.DOCUMENT_STAGE/{file_name},
                {{}}
            ) AS parsed_content
        """)

        result = cursor.fetchone()
        if result and result[0]:
            parsed = json.loads(result[0])
            # Extract text from parsed content
            return parsed.get('text', '') or parsed.get('content', '')

        return ""
    except Exception as e:
        st.error(f"Error parsing document: {e}")
        return ""
    finally:
        cursor.close()


def embed_text_with_cortex(conn, text: str, model: str = 'e5-base-v2') -> List[float]:
    """
    Generate embeddings for text using Snowflake Cortex

    Args:
        conn: Snowflake connection
        text: Text to embed
        model: Embedding model to use

    Returns:
        Vector embedding as list of floats
    """
    cursor = conn.cursor()
    try:
        # Truncate text if too long (models have token limits)
        max_chars = 8000
        truncated_text = text[:max_chars] if len(text) > max_chars else text

        cursor.execute("""
            SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768(
                %s,
                %s
            ) AS embedding
        """, (model, truncated_text))

        result = cursor.fetchone()
        if result and result[0]:
            return json.loads(result[0])

        return []
    except Exception as e:
        st.error(f"Error generating embedding: {e}")
        return []
    finally:
        cursor.close()


def calculate_cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Calculate cosine similarity between two vectors"""
    import math

    if not vec1 or not vec2 or len(vec1) != len(vec2):
        return 0.0

    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude1 = math.sqrt(sum(a * a for a in vec1))
    magnitude2 = math.sqrt(sum(b * b for b in vec2))

    if magnitude1 == 0 or magnitude2 == 0:
        return 0.0

    return dot_product / (magnitude1 * magnitude2)


def find_similar_documents(conn, new_embedding: List[float], threshold: float = 0.7) -> List[Dict[str, Any]]:
    """
    Find similar documents using vector similarity

    Args:
        conn: Snowflake connection
        new_embedding: Embedding vector of new document
        threshold: Similarity threshold (0-1)

    Returns:
        List of similar documents with metadata
    """
    cursor = conn.cursor()
    try:
        # Query existing documents
        cursor.execute("""
            SELECT
                id,
                filename,
                upload_date,
                embedding,
                text_hash,
                file_size,
                file_type
            FROM DATA_QUALITY_AI.CORE.DOCUMENTS
            WHERE status = 'ACTIVE'
        """)

        similar_docs = []
        for row in cursor.fetchall():
            doc_id, filename, upload_date, embedding_str, text_hash, file_size, file_type = row

            if not embedding_str:
                continue

            doc_embedding = json.loads(embedding_str)
            similarity = calculate_cosine_similarity(new_embedding, doc_embedding)

            if similarity >= threshold:
                similar_docs.append({
                    'id': doc_id,
                    'filename': filename,
                    'upload_date': upload_date,
                    'similarity': round(similarity * 100, 2),
                    'text_hash': text_hash,
                    'file_size': file_size,
                    'file_type': file_type
                })

        # Sort by similarity descending
        similar_docs.sort(key=lambda x: x['similarity'], reverse=True)

        return similar_docs

    except Exception as e:
        st.error(f"Error finding similar documents: {e}")
        return []
    finally:
        cursor.close()


def store_document(conn, filename: str, text: str, embedding: List[float], file_type: str, file_size: int) -> str:
    """
    Store document metadata and embedding in database

    Args:
        conn: Snowflake connection
        filename: Original filename
        text: Extracted text
        embedding: Vector embedding
        file_type: File extension
        file_size: File size in bytes

    Returns:
        Document ID
    """
    cursor = conn.cursor()
    try:
        # Generate hash of text content for exact duplicate detection
        text_hash = hashlib.sha256(text.encode()).hexdigest()

        # Check for exact duplicates
        cursor.execute("""
            SELECT id, filename FROM DATA_QUALITY_AI.CORE.DOCUMENTS
            WHERE text_hash = %s AND status = 'ACTIVE'
        """, (text_hash,))

        existing = cursor.fetchone()
        if existing:
            return f"DUPLICATE_{existing[0]}"

        # Insert new document
        doc_id = f"DOC_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(filename) % 10000}"

        cursor.execute("""
            INSERT INTO DATA_QUALITY_AI.CORE.DOCUMENTS (
                id,
                filename,
                text_content,
                embedding,
                text_hash,
                file_type,
                file_size,
                upload_date,
                status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            doc_id,
            filename,
            text[:50000],  # Store first 50k chars
            json.dumps(embedding),
            text_hash,
            file_type,
            file_size,
            datetime.now(),
            'ACTIVE'
        ))

        conn.commit()
        return doc_id

    except Exception as e:
        st.error(f"Error storing document: {e}")
        return ""
    finally:
        cursor.close()


def link_document_to_table(conn, doc_id: str, database: str, schema: str, table: str, confidence: float):
    """
    Link a document to a database table (for future cross-referencing)

    Args:
        conn: Snowflake connection
        doc_id: Document ID
        database: Database name
        schema: Schema name
        table: Table name
        confidence: Confidence score of the link (0-1)
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO DATA_QUALITY_AI.CORE.DOCUMENT_TABLE_LINKS (
                document_id,
                database_name,
                schema_name,
                table_name,
                confidence_score,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (doc_id, database, schema, table, confidence, datetime.now()))

        conn.commit()

    except Exception as e:
        st.error(f"Error linking document to table: {e}")
    finally:
        cursor.close()


def get_document_stats(conn) -> Dict[str, Any]:
    """Get statistics about uploaded documents"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                COUNT(*) as total_docs,
                COUNT(DISTINCT file_type) as file_types,
                SUM(file_size) / 1024 / 1024 as total_size_mb,
                MAX(upload_date) as last_upload
            FROM DATA_QUALITY_AI.CORE.DOCUMENTS
            WHERE status = 'ACTIVE'
        """)

        row = cursor.fetchone()
        if row:
            return {
                'total_docs': row[0] or 0,
                'file_types': row[1] or 0,
                'total_size_mb': round(row[2] or 0, 2),
                'last_upload': row[3]
            }

        return {
            'total_docs': 0,
            'file_types': 0,
            'total_size_mb': 0,
            'last_upload': None
        }

    except Exception as e:
        return {
            'total_docs': 0,
            'file_types': 0,
            'total_size_mb': 0,
            'last_upload': None
        }
    finally:
        cursor.close()
