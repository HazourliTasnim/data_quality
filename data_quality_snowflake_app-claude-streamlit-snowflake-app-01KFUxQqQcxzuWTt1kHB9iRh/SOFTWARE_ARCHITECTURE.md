# Data Quality Tool - Software Architecture

**Version:** 1.1
**Last Updated:** 2025-11-23
**Status:** Production-Ready

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Overview](#system-overview)
3. [Architecture Principles](#architecture-principles)
4. [High-Level Architecture](#high-level-architecture)
5. [Component Architecture](#component-architecture)
6. [Data Flow](#data-flow)
7. [Technology Stack](#technology-stack)
8. [Module Details](#module-details)
9. [API & Interface Design](#api--interface-design)
10. [Deployment Architecture](#deployment-architecture)
11. [Security Architecture](#security-architecture)
12. [Performance & Scalability](#performance--scalability)
13. [Future Enhancements](#future-enhancements)

---

## Executive Summary

This document describes the software architecture of a comprehensive **Data Quality Tool** designed for Snowflake, supporting both **structured data** (tables) and **unstructured data** (documents) quality management. The tool is built for AI training data preparation, targeting the $25B+ market opportunity.

**Key Capabilities:**
- Semantic data modeling with YAML definitions
- Hybrid data quality rules (column-level + cross-column)
- Real-time rule execution and validation
- Document quality management with AI-powered deduplication
- Snowflake Native App deployment
- Enterprise-grade security and scalability

**Target Market:**
- Initial: Snowflake Marketplace (PoC)
- Future: Multi-cloud (Databricks, BigQuery, etc.)
- Revenue Goal: $10M/year without VC funding

---

## System Overview

### Purpose

The Data Quality Tool solves two critical problems:

1. **Structured Data Quality**: Traditional data quality for relational tables with support for complex cross-column relationships
2. **Unstructured Data Quality**: Document deduplication and quality management for AI training data preparation

### Key Features

| Feature Category | Capabilities |
|-----------------|--------------|
| **Data Modeling** | Semantic YAML generation, AI-powered descriptions, metadata enrichment |
| **Rule Definition** | Column-level rules, cross-column rules, natural language to rule translation, **AI auto field detection** ⭐ NEW |
| **Rule Execution** | Real-time validation, violation detection, SQL transparency |
| **Document Management** | Upload, parse, embed, deduplicate documents using vector similarity |
| **Visualization** | Interactive dashboards, violation drill-down, quality metrics |
| **Export** | YAML files, CSV results, Snowflake registry storage |
| **AI Integration** | Snowflake Cortex for LLM and embeddings (e5-base-v2) |

---

## Architecture Principles

### 1. **Hybrid Architecture**
- Supports both column-level AND table-level data quality rules
- Unified YAML schema for structured and unstructured data

### 2. **Cloud-Native**
- Built for Snowflake with Cortex AI integration
- Serverless execution model
- Multi-tenant capable

### 3. **Modular Design**
- Clear separation of concerns
- Independent modules for different capabilities
- Easy to extend and maintain

### 4. **AI-First**
- Natural language rule generation
- AI-powered semantic descriptions
- Vector similarity for document deduplication

### 5. **User-Centric**
- Interactive UI with instant feedback
- Visual results with drill-down capability
- Export options for analysis

### 6. **Performance-Optimized**
- Query caching for metadata
- Batch processing for rules
- Configurable violation limits

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         USER INTERFACE                          │
│                      (Streamlit Web App)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌────────┐│
│  │  Overview   │  │   Semantic   │  │  DQ Rules  │  │Validate││
│  │     Tab     │  │   Model Tab  │  │     Tab    │  │  Tab   ││
│  └─────────────┘  └──────────────┘  └────────────┘  └────────┘│
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              Document Quality Tab (v1.1)                   ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      APPLICATION LAYER                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐│
│  │  Snowflake Utils │  │ Semantic YAML    │  │  Document     ││
│  │     Module       │  │   Spec Module    │  │  Quality      ││
│  │                  │  │                  │  │   Module      ││
│  │ • Connections    │  │ • YAML Generator │  │ • Parser      ││
│  │ • Metadata Query │  │ • Validator      │  │ • Embeddings  ││
│  │ • Rule Execution │  │ • Auto-Fix       │  │ • Similarity  ││
│  │ • DQ Rules       │  │                  │  │ • Storage     ││
│  └──────────────────┘  └──────────────────┘  └───────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SNOWFLAKE PLATFORM                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐   │
│  │   Cortex    │  │   Tables    │  │   Document Storage   │   │
│  │     AI      │  │   (Source   │  │   (DOCUMENTS,        │   │
│  │             │  │    Data)    │  │    EMBEDDINGS)       │   │
│  │ • LLM       │  │             │  │                      │   │
│  │ • Embeddings│  │             │  │                      │   │
│  └─────────────┘  └─────────────┘  └──────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │           Registry (SEMANTIC_CONFIG schema)              │  │
│  │   • SEMANTIC_VIEW table (YAML storage)                  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Component Architecture

### Core Components

#### 1. **User Interface Layer** (`app.py`)

**Purpose:** Interactive web application for all user interactions

**Tabs:**
1. **Overview Tab**
   - Connection management
   - Quick actions dashboard
   - Recent history
   - Validation summary

2. **Semantic Model Tab**
   - Table/column selection
   - YAML generation (AI or local)
   - Metadata enrichment
   - YAML editor with undo/redo

3. **Data Quality Rules Tab**
   - Rules overview (column + table-level)
   - Inline rule editing
   - Natural language rule addition
   - Rule scope selector (single/multi-column)

4. **Validate & Export Tab**
   - YAML validation
   - **Rule execution engine** ⭐
   - Validation results dashboard
   - Violation details
   - CSV export
   - Registry storage

5. **Document Quality Tab** (Beta)
   - Document upload
   - Parsing and embedding
   - Duplicate detection
   - Quality scoring

**Key Features:**
- Session state management
- Performance caching (5-minute TTL)
- Real-time validation
- Export capabilities

#### 2. **Snowflake Utils Module** (`snowflake_utils.py`)

**Purpose:** Core business logic for Snowflake operations

**Functions:**

```python
# Connection Management
get_connection_from_params()
list_databases(), list_schemas(), list_tables()
get_columns(), get_primary_keys()
use_warehouse(), use_role()

# YAML Generation
generate_semantic_yaml_with_cortex()  # AI-powered
save_semantic_yaml()                  # Registry storage

# Rule Management
add_dq_rule_from_natural_language()        # Column-level (manual field selection)
add_table_level_rule_from_natural_language() # Cross-column (manual field selection)
auto_identify_and_create_rule()            # ⭐ NEW: Auto field identification from NL
call_cortex_for_rule()                     # LLM integration

# Rule Execution
execute_column_rule()      # Single column rule
execute_table_rule()       # Cross-column rule
execute_all_rules()        # Batch execution
```

**Rule Types Supported:**

| Category | Rule Types |
|----------|-----------|
| **Column-Level** | NOT_NULL, UNIQUE, **FUZZY_DUPLICATE** ⭐, MIN_VALUE, MAX_VALUE, ALLOWED_VALUES, MAX_LENGTH, PATTERN, MAX_AGE_DAYS, FOREIGN_KEY, **EXTERNAL_REFERENCE** ⭐ NEW, LOOKUP |
| **Table-Level** | COMPOSITE_UNIQUE, CROSS_COLUMN_COMPARISON, CONDITIONAL_REQUIRED, MUTUAL_EXCLUSIVITY, CONDITIONAL_VALUE, MULTI_TABLE_AGGREGATE ⭐, MULTI_TABLE_CONDITION ⭐ |

#### 3. **Reference Data Providers Module** (`reference_data_providers.py`) ⭐ NEW

**Purpose:** External reference data validation (INSEE SIRET, ISO codes, etc.)

**Key Components:**
```python
# Abstract Provider Interface
ReferenceDataProvider           # Base class for all providers
ReferenceDataRegistry          # Central provider registry

# Caching & Validation
ensure_cache_tables()          # Initialize Snowflake cache tables
validate_with_cache()          # Cache-first validation strategy

# Built-in Providers
ISOCountryProvider             # ISO 3166 country codes
INSEESiretProvider             # French business identifiers

# Helper Functions
register_insee_provider()      # Register INSEE with credentials
get_provider_for_field()       # Auto-detect provider from field name
```

**Provider Interface:**
```python
class ReferenceDataProvider(ABC):
    @property
    def provider_id(self) -> str           # Unique ID (e.g., 'insee_siret')
    @property
    def provider_name(self) -> str         # Human name
    @property
    def supported_fields(self) -> List[str]  # Field patterns

    def validate_single(value, context) -> Dict  # Validate one value
    def validate_batch(values, context) -> List  # Batch validation
    def get_cache_ttl() -> int                   # Cache duration
    def get_rate_limit() -> Dict                 # API limits
    def get_additional_fields(value) -> Dict     # Enrichment data
```

**Caching Architecture:**
- Snowflake tables: `REFERENCE_DATA.REFERENCE_CACHE`
- Cache-first strategy: Check cache → API call → Store result
- TTL-based expiration: 30 days for INSEE, 365 days for ISO
- Hit count tracking for analytics
- API usage tracking and cost estimation

**Supported Providers:**
- **INSEE SIRET**: French business registry validation
- **ISO Country**: ISO 3166 country code validation
- **Extensible**: Easy to add custom providers

#### 4. **Semantic YAML Spec Module** (`semantic_yaml_spec.py`)

**Purpose:** YAML schema definition and validation

**Functions:**
```python
generate_semantic_yaml_local()  # Local generation without AI
validate_semantic_yaml()        # Schema validation
auto_fix_yaml()                 # Automatic corrections
```

**YAML Schema:**
```yaml
semantic_view:
  name: "view_name"
  description: "AI-generated description"
  version: 1
  source:
    database: "DB"
    schema: "SCHEMA"
    table: "TABLE"
  target:
    database: "DB"
    schema: "SCHEMA"
    view_name: "V_TABLE"
  columns:
    - name: "column_name"
      label: "Human Label"
      data_type: "VARCHAR"
      description: "AI-generated"
      role: "dimension|measure"
      dq_rules:
        - id: "rule_id"
          type: "NOT_NULL"
          severity: "CRITICAL|WARNING|INFO"
          description: "Must not be null"
          params: {}
          lambda_hint: "SQL expression"
  table_rules:  # NEW: Cross-column rules
    - id: "rule_id"
      type: "COMPOSITE_UNIQUE"
      columns: ["col1", "col2"]
      severity: "CRITICAL"
      description: "Must be unique together"
      params: {}
      lambda_hint: "SQL expression"
```

#### 4. **Document Quality Module** (`document_quality.py`)

**Purpose:** Unstructured data management for AI training

**Functions:**
```python
parse_document_with_cortex()      # Extract text from files
embed_text_with_cortex()          # Generate 768-dim vectors
find_similar_documents()          # Vector similarity search
store_document()                  # Save to Snowflake
get_document_stats()              # Analytics
```

**Document Processing Pipeline:**
```
Upload → Parse → Hash → Embed → Similarity Check → Store → Index
```

**Supported Formats:**
- PDF, DOCX, TXT, MD
- Images with OCR (future)

**Deduplication Strategy:**
1. **Exact duplicates**: SHA256 hash comparison
2. **Near duplicates**: Cosine similarity on embeddings (threshold: 0.7)

---

## Data Flow

### 1. Structured Data Quality Workflow

```
┌─────────────┐
│   User      │
│  Selects    │
│   Table     │
└──────┬──────┘
       │
       ▼
┌─────────────┐      ┌──────────────┐
│  Metadata   │─────▶│  AI Cortex   │
│   Query     │      │  (Optional)  │
└──────┬──────┘      └──────┬───────┘
       │                    │
       │◀───────────────────┘
       ▼
┌─────────────┐
│    YAML     │
│  Generation │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    User     │
│    Adds     │
│    Rules    │
└──────┬──────┘
       │
       ▼
┌─────────────┐      ┌──────────────┐
│    Rule     │─────▶│  Snowflake   │
│  Execution  │      │    Query     │
└──────┬──────┘      └──────┬───────┘
       │                    │
       │◀───────────────────┘
       ▼
┌─────────────┐
│  Violation  │
│   Results   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Export    │
│ (CSV/YAML)  │
└─────────────┘
```

### 2. Rule Execution Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    execute_all_rules()                       │
└────────────────────────┬─────────────────────────────────────┘
                         │
         ┌───────────────┴───────────────┐
         │                               │
         ▼                               ▼
┌──────────────────┐            ┌──────────────────┐
│  Column Rules    │            │  Table Rules     │
│  (per column)    │            │  (cross-column)  │
└────────┬─────────┘            └────────┬─────────┘
         │                               │
         ▼                               ▼
┌──────────────────┐            ┌──────────────────┐
│ execute_column_  │            │ execute_table_   │
│     rule()       │            │     rule()       │
└────────┬─────────┘            └────────┬─────────┘
         │                               │
         │   ┌───────────────────────────┘
         │   │
         ▼   ▼
    ┌────────────────┐
    │  SQL Query     │
    │  Generation    │
    └────────┬───────┘
             │
             ▼
    ┌────────────────┐
    │  Snowflake     │
    │  Execution     │
    └────────┬───────┘
             │
             ▼
    ┌────────────────┐
    │   Violations   │
    │   + Metadata   │
    └────────┬───────┘
             │
             ▼
    ┌────────────────┐
    │   Summary      │
    │   Statistics   │
    └────────────────┘
```

### 3. Document Quality Workflow

```
┌─────────────┐
│    User     │
│   Uploads   │
│  Document   │
└──────┬──────┘
       │
       ▼
┌─────────────┐      ┌──────────────┐
│   Parse     │─────▶│ Cortex PARSE │
│  Document   │      │   _DOCUMENT  │
└──────┬──────┘      └──────────────┘
       │
       ▼
┌─────────────┐
│  Generate   │
│  SHA256     │
│   Hash      │
└──────┬──────┘
       │
       ▼
┌─────────────┐      ┌──────────────┐
│  Generate   │─────▶│ Cortex EMBED │
│ Embeddings  │      │    _TEXT     │
└──────┬──────┘      └──────────────┘
       │
       ▼
┌─────────────┐      ┌──────────────┐
│  Similarity │─────▶│Vector Search │
│    Check    │      │(Cosine Sim)  │
└──────┬──────┘      └──────────────┘
       │
       ▼
┌─────────────┐
│   Store     │
│  Document   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Display    │
│  Results    │
└─────────────┘
```

---

## Technology Stack

### Frontend

| Technology | Version | Purpose |
|-----------|---------|---------|
| **Streamlit** | 1.28+ | Web UI framework |
| **Pandas** | 2.0+ | Data manipulation |
| **PyYAML** | 6.0+ | YAML parsing |

### Backend

| Technology | Version | Purpose |
|-----------|---------|---------|
| **Python** | 3.9+ | Core language |
| **Snowflake Connector** | 3.0+ | Database connectivity |
| **Snowflake Cortex** | Latest | AI/ML capabilities |

### Database

| Component | Purpose |
|-----------|---------|
| **Snowflake** | Data warehouse & execution platform |
| **Cortex Complete** | LLM for natural language processing |
| **Cortex Embed** | Text embeddings (e5-base-v2, 768-dim) |

### AI Models

| Model | Purpose | Dimensions |
|-------|---------|-----------|
| **mistral-large** | Rule generation, descriptions | N/A |
| **e5-base-v2** | Document embeddings | 768 |

---

## Module Details

### snowflake_utils.py (1,887 lines)

**Structure:**
```python
# ============================================================================
# Connection Management (Lines 1-150)
# ============================================================================
- get_connection_from_params()
- list_databases(), list_schemas(), list_tables()
- get_columns(), get_primary_keys()

# ============================================================================
# YAML Generation (Lines 151-500)
# ============================================================================
- generate_semantic_yaml_with_cortex()
- Column description enrichment
- Business domain inference

# ============================================================================
# Rule Management (Lines 501-1000)
# ============================================================================
- add_dq_rule_from_natural_language()
- add_table_level_rule_from_natural_language()
- call_cortex_for_rule()

# ============================================================================
# Metadata Enrichment (Lines 1001-1372)
# ============================================================================
- enhance_column_description()
- AI-powered semantic analysis

# ============================================================================
# Rule Execution Engine (Lines 1373-1887) ⭐ NEW
# ============================================================================
- execute_column_rule()        # Column-level validation
- execute_table_rule()          # Cross-column validation
- execute_all_rules()           # Batch execution
```

**Key Algorithms:**

1. **Column Rule Execution:**
```python
def execute_column_rule(conn, database, schema, table, column_name, rule, limit=100):
    # Build SQL WHERE clause based on rule type
    if rule_type == "NOT_NULL":
        condition = f"{column_name} IS NULL"
    elif rule_type == "UNIQUE":
        # Complex CTE for duplicates
        query = """
        WITH duplicates AS (
            SELECT column, COUNT(*) as cnt
            FROM table
            GROUP BY column
            HAVING COUNT(*) > 1
        )
        SELECT t.*, d.cnt
        FROM table t JOIN duplicates d
        """
    # Execute and return violations + statistics
```

2. **Table Rule Execution:**
```python
def execute_table_rule(conn, database, schema, table, rule, limit=100):
    columns = rule.get("columns")

    if rule_type == "COMPOSITE_UNIQUE":
        # Check for duplicate combinations
        query = """
        WITH duplicates AS (
            SELECT col1, col2, COUNT(*) as cnt
            GROUP BY col1, col2
            HAVING COUNT(*) > 1
        )
        SELECT t.*, d.cnt
        FROM table t JOIN duplicates d
        """
    elif rule_type == "CROSS_COLUMN_COMPARISON":
        # e.g., start_date < end_date
        condition = f"{col1} >= {col2}"
```

### app.py (1,750+ lines)

**Structure:**
```python
# Lines 1-100: Imports and configuration
# Lines 101-300: Performance caching layer
# Lines 301-500: Session state management
# Lines 501-700: Tab 1 - Overview
# Lines 701-1000: Tab 2 - Semantic Model
# Lines 1001-1360: Tab 3 - Data Quality Rules
# Lines 1361-1750: Tab 4 - Validate & Export (with execution engine)
# Lines 1751+: Tab 5 - Document Quality
```

**Key State Management:**
```python
st.session_state.yaml_content          # Current YAML
st.session_state.yaml_history          # Undo/redo stack
st.session_state.yaml_history_index    # Current position
st.session_state.validation_results    # Rule execution results ⭐
st.session_state.editor_version        # Force UI refresh
```

### document_quality.py (312 lines)

**Structure:**
```python
# Document parsing
def parse_document_with_cortex(conn, file_content, file_name):
    # Uses Snowflake Cortex PARSE_DOCUMENT

# Embedding generation
def embed_text_with_cortex(conn, text, model='e5-base-v2'):
    # Generates 768-dimensional vectors

# Similarity search
def find_similar_documents(conn, new_embedding, threshold=0.7):
    # Cosine similarity: (A·B) / (||A|| * ||B||)

# Storage
def store_document(conn, doc_metadata):
    # Save to DOCUMENTS table
```

**Database Schema:**
```sql
-- Documents table
CREATE TABLE DOCUMENTS (
    DOC_ID VARCHAR,
    FILE_NAME VARCHAR,
    CONTENT_TEXT TEXT,
    CONTENT_HASH VARCHAR,  -- SHA256
    EMBEDDING ARRAY,       -- 768-dim vector
    UPLOAD_DATE TIMESTAMP,
    FILE_SIZE NUMBER,
    FILE_TYPE VARCHAR
);

-- Similarity tracking
CREATE TABLE DOCUMENT_SIMILARITIES (
    DOC_ID_1 VARCHAR,
    DOC_ID_2 VARCHAR,
    SIMILARITY_SCORE FLOAT,
    DETECTED_AT TIMESTAMP
);
```

---

## API & Interface Design

### Internal APIs

#### 1. Rule Execution API

```python
# Execute all rules
result = execute_all_rules(
    conn=snowflake_connection,
    yaml_content=yaml_string,
    limit_per_rule=100
)

# Returns:
{
    "summary": {
        "total_rules": 12,
        "rules_passed": 8,
        "rules_with_violations": 4,
        "total_violations": 453,
        "critical_violations": 120,
        "warning_violations": 300,
        "info_violations": 33,
        "total_rows": 10000,
        "overall_pass_rate": 66.7
    },
    "column_rules_results": [
        {
            "rule_id": "customer_id_not_null",
            "rule_type": "NOT_NULL",
            "severity": "CRITICAL",
            "column": "customer_id",
            "total_rows": 10000,
            "violation_count": 5,
            "pass_rate": 99.95,
            "violations": [...],  # Sample rows
            "sql_query": "SELECT * FROM table WHERE customer_id IS NULL"
        }
    ],
    "table_rules_results": [...]
}
```

#### 2. Document Quality API

```python
# Parse document
text = parse_document_with_cortex(conn, file_bytes, "report.pdf")

# Generate embeddings
embedding = embed_text_with_cortex(conn, text)  # Returns 768-dim array

# Find duplicates
similar_docs = find_similar_documents(conn, embedding, threshold=0.7)
# Returns: [{"doc_id": "...", "similarity": 0.85, ...}]

# Store document
doc_id = store_document(conn, {
    "file_name": "report.pdf",
    "content_text": text,
    "embedding": embedding,
    "file_size": 1024000
})
```

### External APIs (Future)

#### REST API Design (v2.0)

```
POST   /api/v1/semantic-views          # Create YAML
GET    /api/v1/semantic-views/{id}     # Retrieve YAML
PUT    /api/v1/semantic-views/{id}     # Update YAML
DELETE /api/v1/semantic-views/{id}     # Delete YAML

POST   /api/v1/rules/validate           # Execute rules
GET    /api/v1/rules/results/{job_id}   # Get results

POST   /api/v1/documents                 # Upload document
GET    /api/v1/documents/{id}/duplicates # Find duplicates
```

---

## Deployment Architecture

### Snowflake Native App (Current)

```
┌─────────────────────────────────────────────────────────┐
│                    Snowflake Account                    │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │           Native App Installation                 │ │
│  │                                                   │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │ │
│  │  │ Streamlit   │  │  Python     │  │  Setup   │ │ │
│  │  │    App      │  │  Modules    │  │   SQL    │ │ │
│  │  └─────────────┘  └─────────────┘  └──────────┘ │ │
│  │                                                   │ │
│  │  Application Files (from manifest.yml):          │ │
│  │  - streamlit_app.py                              │ │
│  │  - snowflake_utils.py                            │ │
│  │  - semantic_yaml_spec.py                         │ │
│  │  - document_quality.py                           │ │
│  │  - setup.sql                                     │ │
│  └───────────────────────────────────────────────────┘ │
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │              Customer Schema                      │ │
│  │  - SEMANTIC_CONFIG.SEMANTIC_VIEW (registry)      │ │
│  │  - DOCUMENT_LIBRARY.DOCUMENTS                    │ │
│  │  - DOCUMENT_LIBRARY.EMBEDDINGS                   │ │
│  └───────────────────────────────────────────────────┘ │
│                                                         │
│  ┌───────────────────────────────────────────────────┐ │
│  │              Cortex Services                      │ │
│  │  - COMPLETE (LLM)                                │ │
│  │  - EMBED_TEXT (e5-base-v2)                       │ │
│  │  - PARSE_DOCUMENT                                │ │
│  └───────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### Deployment Process

```bash
# 1. Package application
snow app version create v1_1_0

# 2. Test locally
snow streamlit deploy --project native-app/

# 3. Create application package
snow app create package --name DATA_QUALITY_TOOL

# 4. Upload files
snow app version upload --source ./native-app

# 5. Publish to marketplace
snow app publish --package DATA_QUALITY_TOOL --version v1_1_0
```

### Multi-Tenant Architecture (Future)

```
┌─────────────────────────────────────────────────────────┐
│                   SaaS Platform Layer                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Tenant A   │  │   Tenant B   │  │   Tenant C   │ │
│  │  (Isolated)  │  │  (Isolated)  │  │  (Isolated)  │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘ │
│         │                 │                 │         │
│         └─────────────────┼─────────────────┘         │
│                           │                           │
│  ┌────────────────────────┴─────────────────────────┐ │
│  │         Shared Service Layer                     │ │
│  │  - Authentication                                │ │
│  │  - API Gateway                                   │ │
│  │  - Billing                                       │ │
│  └──────────────────────────────────────────────────┘ │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │           Data Plane (per platform)              │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐      │  │
│  │  │Snowflake │  │Databricks│  │ BigQuery │      │  │
│  │  │ Adapter  │  │  Adapter │  │  Adapter │      │  │
│  │  └──────────┘  └──────────┘  └──────────┘      │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## Security Architecture

### Authentication & Authorization

```
┌─────────────────────────────────────────────────────┐
│                User Authentication                  │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Snowflake Native App: Uses Snowflake Auth        │
│  - Account-based authentication                    │
│  - Role-based access control (RBAC)               │
│  - No separate user management needed             │
│                                                     │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│              Data Access Control                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│  1. Schema-level isolation                         │
│  2. Row-level security (future)                    │
│  3. Column masking for PII                         │
│  4. Query result limits                            │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Data Security

| Layer | Security Measure |
|-------|-----------------|
| **Transport** | TLS 1.2+ for all connections |
| **Storage** | Snowflake encryption at rest (AES-256) |
| **Credentials** | Stored in Snowflake secrets manager |
| **API Keys** | Scoped tokens with expiration |
| **Audit** | All operations logged to Snowflake audit trail |

### Compliance

- **SOC 2 Type II**: Snowflake certified
- **GDPR**: Data residency support
- **HIPAA**: PHI support via Snowflake BAA
- **Data Retention**: Configurable retention policies

---

## Performance & Scalability

### Performance Metrics

| Metric | Target | Current |
|--------|--------|---------|
| **YAML Generation** | < 5 sec | ~3 sec |
| **Rule Execution (1k rows)** | < 10 sec | ~5 sec |
| **Rule Execution (1M rows)** | < 60 sec | ~30 sec |
| **Document Parsing** | < 5 sec/doc | ~3 sec |
| **Similarity Search** | < 2 sec | ~1 sec |
| **UI Response** | < 1 sec | ~0.5 sec |

### Optimization Strategies

#### 1. Query Caching
```python
@st.cache_data(ttl=300, show_spinner=False)
def cached_list_tables(_conn, database, schema):
    return list_tables(_conn, database, schema)
```
- 5-minute TTL for metadata
- Reduces Snowflake query costs
- Instant UI responses

#### 2. Batch Processing
```python
# Execute all rules in parallel where possible
results = execute_all_rules(conn, yaml_content)
```
- Single transaction for all rules
- Optimized SQL queries
- Configurable violation limits

#### 3. Lazy Loading
- Load violation details on-demand
- Expandable sections for large results
- Pagination for document lists

### Scalability Limits

| Resource | Current Limit | Scalable To |
|----------|--------------|-------------|
| **Tables per YAML** | 1 | 10+ (v2.0) |
| **Rules per Table** | 50 | 500+ |
| **Documents** | 10k | 10M+ |
| **Concurrent Users** | 100 | 10k+ (SaaS) |
| **Data Volume** | 100GB | 10PB+ |

---

## Future Enhancements

### Phase 2: Multi-Table Support (v1.2)

```yaml
semantic_model:  # Changed from semantic_view
  name: "customer_360"
  tables:
    - name: "customers"
      columns: [...]
      dq_rules: [...]
    - name: "orders"
      columns: [...]
      dq_rules: [...]
  cross_table_rules:  # NEW
    - type: "REFERENTIAL_INTEGRITY"
      source_table: "orders"
      source_column: "customer_id"
      target_table: "customers"
      target_column: "customer_id"
```

### Phase 3: Multi-Platform Support (v2.0)

**Adapter Pattern:**
```python
class DataPlatformAdapter(ABC):
    @abstractmethod
    def connect(self): pass

    @abstractmethod
    def execute_query(self): pass

    @abstractmethod
    def get_metadata(self): pass

class SnowflakeAdapter(DataPlatformAdapter): ...
class DatabricksAdapter(DataPlatformAdapter): ...
class BigQueryAdapter(DataPlatformAdapter): ...
```

### Phase 4: Advanced Features (v2.x)

1. **Machine Learning Integration**
   - Anomaly detection
   - Auto-generated rules from data patterns
   - Predictive quality scoring

2. **Workflow Automation**
   - Scheduled validation jobs
   - Email alerts on violations
   - Auto-remediation

3. **Advanced Analytics**
   - Quality trends over time
   - Impact analysis
   - Cost estimation

4. **Collaboration**
   - Team workspaces
   - Rule templates library
   - Version control for YAML

---

## Appendix

### File Structure

```
data_quality_snowflake_app/
├── semantic-tool/                    # Main application
│   ├── app.py                        # 1,750+ lines - UI
│   ├── snowflake_utils.py           # 1,887 lines - Core logic
│   ├── semantic_yaml_spec.py        # 442 lines - YAML schema
│   ├── document_quality.py          # 312 lines - Doc management
│   ├── setup_document_tables.sql    # 182 lines - DB schema
│   └── requirements.txt             # Dependencies
│
├── native-app/                       # Snowflake Native App
│   ├── manifest.yml                 # App metadata
│   ├── setup.sql                    # Installation script
│   ├── deploy.sql                   # Deployment automation
│   ├── streamlit_app.py             # Copy of app.py
│   ├── snowflake_utils.py           # Copy of utils
│   ├── semantic_yaml_spec.py        # Copy of spec
│   ├── README.md                    # Marketplace listing
│   └── DEPLOYMENT_GUIDE.md          # Deployment docs
│
├── DOCUMENT_QUALITY_V1.1.md         # Feature documentation
├── SCALABLE_ARCHITECTURE.md         # Long-term vision (986 lines)
├── SOFTWARE_ARCHITECTURE.md         # This file
└── README.md                         # Project overview
```

### Key Metrics Summary

| Metric | Value |
|--------|-------|
| **Total Lines of Code** | ~6,500 |
| **Python Modules** | 4 |
| **SQL Scripts** | 3 |
| **Documentation Files** | 4 |
| **Supported Rule Types** | 15 |
| **API Functions** | 30+ |
| **Database Tables** | 8 |
| **UI Tabs** | 5 |

### Dependencies

```
# requirements.txt
streamlit>=1.28.0
snowflake-connector-python>=3.0.0
pandas>=2.0.0
pyyaml>=6.0
```

### Version History

| Version | Date | Features |
|---------|------|----------|
| **v1.0** | 2025-11 | Initial release, column-level rules, basic YAML |
| **v1.1** | 2025-11 | Cross-column rules, document quality, rule execution |
| **v1.2** | TBD | Multi-table support, advanced analytics |
| **v2.0** | TBD | Multi-platform, REST API, ML integration |

---

**Document End**

For questions or contributions, please refer to the repository README or contact the development team.
