# Codebase Refactoring Guide

## Executive Summary

**Total Lines of Code:** 8,146 lines across 6 Python files

**Overall Code Quality:** Good to Very Good
- Well-structured modules with clear responsibilities
- Good use of type hints
- Reasonable docstrings
- Some areas need cleanup and modernization

---

## File-by-File Analysis

### ✅ doc_snippets.py (306 lines) - **REFACTORED**

**Status:** Completed refactoring

**Changes Made:**
1. **Eliminated duplication** - Derived `AVAILABLE_SOURCE_SYSTEMS` and `AVAILABLE_BUSINESS_DOMAINS` from their respective dictionaries (DRY principle)
2. **Better organization** - Added clear section headers
3. **Enhanced documentation** - Improved docstrings with examples
4. **Fixed inconsistency** - Added missing "Operations" and "Product" to BUSINESS_DOMAIN_INFO

**Impact:** Reduced maintenance burden, single source of truth for reference data

---

### 📋 semantic_yaml_spec.py (445 lines)

**Current Quality:** Good
**Issues Found:** Minor

#### Recommendations:

1. **Extract Magic Numbers to Constants**
```python
# BEFORE (scattered throughout)
if len(errors) > 0:
    ...
if len(st.session_state.yaml_history) > 50:
    ...

# AFTER
MAX_YAML_HISTORY_SIZE = 50
MIN_DESCRIPTION_LENGTH = 10
MAX_DESCRIPTION_LENGTH = 500

if len(st.session_state.yaml_history) > MAX_YAML_HISTORY_SIZE:
    ...
```

2. **Simplify Validation Logic**
```python
# BEFORE - nested ifs
if "semantic_view" in parsed:
    sv = parsed["semantic_view"]
    if isinstance(sv, dict):
        if "columns" in sv:
            columns = sv["columns"]
            if isinstance(columns, list):
                ...

# AFTER - early returns
if "semantic_view" not in parsed:
    return {"valid": False, "errors": ["Missing semantic_view"], "parsed": parsed}

sv = parsed["semantic_view"]
if not isinstance(sv, dict):
    return {"valid": False, "errors": ["semantic_view must be dict"], "parsed": parsed}

columns = sv.get("columns", [])
if not isinstance(columns, list):
    return {"valid": False, "errors": ["columns must be list"], "parsed": parsed}
```

3. **Type Aliases for Complex Types**
```python
from typing import TypedDict, List

class SemanticColumn(TypedDict):
    name: str
    data_type: str
    role: str
    logical_type: str
    description: str

def generate_semantic_yaml_local(
    ...
    columns: List[SemanticColumn],
    ...
) -> str:
    ...
```

---

### 🔧 snowflake_utils.py (2,836 lines) - **NEEDS REFACTORING**

**Current Quality:** Fair
**Issues Found:** Major - File is too large and has multiple responsibilities

#### Critical Issues:

1. **Single Responsibility Violation**
   - File handles: DB connections, caching, YAML generation, AI prompts, rule execution, data quality checks
   - **Solution:** Split into multiple modules

2. **Suggested File Structure:**
```
snowflake_utils/
├── __init__.py
├── connection.py       # Connection management, auth
├── metadata.py         # Schema/table/column queries
├── caching.py          # Streamlit caching decorators
├── ai_generation.py    # AI/LLM prompt generation
├── rule_execution.py   # Data quality rule execution
└── yaml_builder.py     # YAML generation functions
```

3. **Extract Repeated Patterns**

**Pattern: Cursor Management**
```python
# BEFORE - repeated 50+ times
def list_something(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT ...")
        return cursor.fetchall()
    finally:
        cursor.close()

# AFTER - Create context manager
from contextlib import contextmanager

@contextmanager
def snowflake_cursor(conn: SnowflakeConnection):
    """Context manager for Snowflake cursors with automatic cleanup."""
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()

# Usage
def list_something(conn):
    with snowflake_cursor(conn) as cursor:
        cursor.execute("SELECT ...")
        return cursor.fetchall()
```

4. **Extract Large Functions**

**Example: `generate_with_ai` function (200+ lines)**
```python
# BEFORE - Monolithic function
def generate_with_ai(conn, table, columns, ...):
    # 200 lines of:
    # - Building prompts
    # - Calling AI
    # - Parsing responses
    # - Error handling
    # - Retries
    ...

# AFTER - Split into smaller functions
def build_column_analysis_prompt(columns, samples, context):
    """Build prompt for AI column analysis."""
    ...

def call_cortex_ai(conn, prompt, model="mistral-large", max_retries=3):
    """Call Snowflake Cortex AI with retry logic."""
    ...

def parse_ai_response(response_text):
    """Parse and validate AI JSON response."""
    ...

def generate_with_ai(conn, table, columns, ...):
    """Generate semantic YAML using AI - orchestration function."""
    prompt = build_column_analysis_prompt(columns, samples, context)
    response = call_cortex_ai(conn, prompt)
    return parse_ai_response(response)
```

5. **Reduce Caching Complexity**
```python
# BEFORE - Many similar cached functions
@st.cache_data(ttl=300, show_spinner=False)
def cached_list_databases(_conn):
    return list_databases(_conn)

@st.cache_data(ttl=300, show_spinner=False)
def cached_list_schemas(_conn, database):
    return list_schemas(_conn, database)

# AFTER - Generic caching decorator
from functools import wraps

def snowflake_cached(ttl=300):
    """Decorator for caching Snowflake queries."""
    def decorator(func):
        @wraps(func)
        @st.cache_data(ttl=ttl, show_spinner=False)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Usage
@snowflake_cached(ttl=300)
def list_databases(conn):
    with snowflake_cursor(conn) as cursor:
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()]
```

---

### 🎨 app.py (3,563 lines) - **NEEDS REFACTORING**

**Current Quality:** Fair
**Issues Found:** Major - Monolithic structure, UI and business logic mixed

#### Critical Issues:

1. **Monolithic Structure**
   - Single 3,500+ line file
   - **Solution:** Split into modular components

2. **Suggested File Structure:**
```
app_modules/
├── __init__.py
├── config.py           # Constants, CSS, theme config
├── session_state.py    # Session state initialization
├── ui_components.py    # Reusable UI components
├── connection_ui.py    # Connection sidebar
├── table_selection.py  # Table selection UI
├── metadata_form.py    # Model properties form
├── filters_ui.py       # View-level filters UI
├── validation_ui.py    # Validation and testing UI
└── main.py            # Main app orchestration
```

3. **Extract CSS to Separate File**
```python
# app_modules/styles.py
"""CSS styling and theme configuration."""

DARK_THEME_CSS = """
<style>
    :root {
        --bg-canvas: #020617;
        ...
    }
</style>
"""

LIGHT_THEME_CSS = """
<style>
    :root {
        --bg-canvas: #F5F7FA;
        ...
    }
</style>
"""

def apply_theme(theme: str):
    """Apply the selected theme CSS."""
    if theme == "light":
        st.markdown(LIGHT_THEME_CSS, unsafe_allow_html=True)
    else:
        st.markdown(DARK_THEME_CSS, unsafe_allow_html=True)
```

4. **Extract Reusable UI Components**
```python
# app_modules/ui_components.py
"""Reusable UI components."""

def render_connection_status(connected: bool, details: dict = None):
    """Render connection status indicator."""
    if connected:
        st.markdown('<span class="connection-status-dot connected"></span>',
                    unsafe_allow_html=True)
        st.markdown("**Connected**")
        if details:
            st.caption(f"User: {details.get('user')}")
    else:
        st.markdown('<span class="connection-status-dot disconnected"></span>',
                    unsafe_allow_html=True)
        st.markdown("**Disconnected**")

def render_filter_preview(filter_sql: str):
    """Render SQL filter preview."""
    st.markdown("**Preview:**")
    st.code(filter_sql, language="sql")
    st.caption("👇 Click 'Add This Filter to View' below to add this filter")

def render_code_block_with_copy(code: str, language: str = "sql"):
    """Render code block with copy button."""
    st.code(code, language=language)
    # Could add copy-to-clipboard button here
```

5. **Separate Business Logic from UI**
```python
# BEFORE - Mixed in UI
if st.button("Generate with AI"):
    # 100 lines of business logic mixed with UI updates
    with st.spinner("Generating..."):
        # Fetch data
        # Call AI
        # Parse results
        # Update session state
        # Show results
        ...

# AFTER - Separated
# app_modules/yaml_generation.py
def generate_yaml_with_ai(conn, table_info, metadata):
    """Generate YAML using AI - pure business logic."""
    samples = fetch_sample_values(conn, table_info)
    context = build_context(metadata)
    response = call_ai_generation(conn, table_info, samples, context)
    return parse_and_validate_response(response)

# In UI file
if st.button("Generate with AI"):
    with st.spinner("Generating..."):
        try:
            yaml_content = generate_yaml_with_ai(
                conn,
                table_info,
                st.session_state.metadata
            )
            st.session_state.yaml_content = yaml_content
            st.success("Generated successfully!")
        except Exception as e:
            st.error(f"Generation failed: {e}")
```

---

## Cross-Cutting Improvements

### 1. Error Handling Consistency

**Create Custom Exceptions:**
```python
# exceptions.py
class SnowflakeConnectionError(Exception):
    """Raised when Snowflake connection fails."""
    pass

class YAMLValidationError(Exception):
    """Raised when YAML validation fails."""
    pass

class AIGenerationError(Exception):
    """Raised when AI generation fails."""
    pass
```

**Use Consistent Error Handling Pattern:**
```python
# BEFORE - Inconsistent
try:
    result = do_something()
except Exception as e:
    st.error(f"Error: {e}")  # Sometimes
    print(f"Error: {e}")     # Sometimes
    return None              # Sometimes
    pass                     # Sometimes

# AFTER - Consistent
from typing import Optional, Tuple

def do_something() -> Tuple[Optional[Result], Optional[str]]:
    """
    Do something and return (result, error_message).

    Returns:
        Tuple of (result, error). If successful, error is None.
        If failed, result is None and error contains message.
    """
    try:
        result = perform_operation()
        return result, None
    except SnowflakeConnectionError as e:
        return None, f"Connection failed: {e}"
    except Exception as e:
        return None, f"Unexpected error: {e}"

# Usage in UI
result, error = do_something()
if error:
    st.error(error)
else:
    st.success("Success!")
    process_result(result)
```

### 2. Logging Instead of Print Statements

```python
# BEFORE
print(f"Debug: Generated {len(columns)} columns")
print(f"Error: Failed to connect")

# AFTER
import logging

logger = logging.getLogger(__name__)

logger.debug(f"Generated {len(columns)} columns")
logger.error(f"Failed to connect: {error}", exc_info=True)

# In main app
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### 3. Configuration Management

```python
# config.py
"""Application configuration."""

from dataclasses import dataclass
from typing import Optional

@dataclass
class AppConfig:
    """Application configuration."""
    # AI Configuration
    DEFAULT_AI_MODEL: str = "mistral-large"
    AI_MAX_RETRIES: int = 3
    AI_TIMEOUT_SECONDS: int = 30

    # Cache Configuration
    CACHE_TTL_SECONDS: int = 300
    MAX_CACHE_ENTRIES: int = 100

    # YAML Configuration
    MAX_YAML_HISTORY: int = 50
    YAML_INDENT: int = 2

    # UI Configuration
    MAX_COLUMNS_BEFORE_SELECTOR: int = 10
    DEFAULT_SAMPLE_SIZE: int = 5
    MAX_VIOLATIONS_PER_RULE: int = 100

# Usage
from config import AppConfig
config = AppConfig()

if num_columns > config.MAX_COLUMNS_BEFORE_SELECTOR:
    show_column_selector()
```

### 4. Add Type Hints Everywhere

```python
# BEFORE
def process_columns(columns):
    result = []
    for col in columns:
        result.append(transform(col))
    return result

# AFTER
from typing import List, Dict, Any

def process_columns(columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process column metadata and apply transformations.

    Args:
        columns: List of column dictionaries with metadata

    Returns:
        List of transformed column dictionaries
    """
    return [transform(col) for col in columns]
```

### 5. Use Enums for Constants

```python
# BEFORE
role = "dimension"  # Could be "dimension", "measure", "identifier"
if role == "dimension":
    ...

# AFTER
from enum import Enum

class ColumnRole(str, Enum):
    """Valid column role values."""
    DIMENSION = "dimension"
    MEASURE = "measure"
    IDENTIFIER = "identifier"

role = ColumnRole.DIMENSION
if role == ColumnRole.DIMENSION:
    ...
```

---

## Testing Recommendations

### 1. Add Unit Tests
```python
# tests/test_snowflake_utils.py
import pytest
from snowflake_utils import parse_account_from_url

def test_parse_account_from_url():
    assert parse_account_from_url("https://abc.us-east-1.snowflakecomputing.com") == "abc.us-east-1"
    assert parse_account_from_url("xyz.eu-central-1.snowflakecomputing.com") == "xyz.eu-central-1"
    assert parse_account_from_url("https://test.snowflakecomputing.com/") == "test"

def test_parse_account_edge_cases():
    assert parse_account_from_url("") == ""
    assert parse_account_from_url("invalid") == "invalid"
```

### 2. Add Integration Tests for Snowflake
```python
# tests/test_integration.py
import pytest
from snowflake_utils import get_connection_from_params

@pytest.mark.integration
def test_connection_with_valid_credentials():
    conn = get_connection_from_params(
        snowflake_url="test.snowflakecomputing.com",
        user="test_user"
    )
    assert conn is not None
    conn.close()
```

---

## Documentation Improvements

### 1. Add README.md
```markdown
# Semantic YAML Builder

A Streamlit application for building semantic view definitions with AI-powered data quality rules.

## Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Configure Snowflake connection
3. Run: `streamlit run app.py`

## Architecture

- `app.py` - Main Streamlit UI
- `snowflake_utils.py` - Snowflake connection and data operations
- `semantic_yaml_spec.py` - YAML validation and generation
- `doc_snippets.py` - Documentation and context for AI
```

### 2. Add Inline Documentation
```python
# GOOD EXAMPLE - Clear intent
# Calculate weighted average considering null values
weighted_avg = sum(v * w for v, w in values if v is not None) / total_weight

# BAD EXAMPLE - Obvious comment
# Loop through columns
for col in columns:  # Don't comment the obvious
```

---

## Priority Recommendations

### High Priority (Do First):
1. ✅ **Split snowflake_utils.py** into smaller modules (connection, metadata, AI, rules)
2. ✅ **Extract CSS** from app.py into separate style file
3. ✅ **Add cursor context manager** to eliminate repetition
4. ✅ **Create config.py** with all magic numbers

### Medium Priority:
5. **Add type hints** to all functions missing them
6. **Extract large functions** (>100 lines) into smaller ones
7. **Add logging** instead of print statements
8. **Create custom exceptions** for better error handling

### Low Priority (Nice to Have):
9. Add unit tests
10. Add integration tests
11. Improve docstrings with examples
12. Add README and architecture docs

---

## Estimated Impact

| Refactoring Item | LOC Reduction | Maintainability Gain | Dev Time |
|------------------|---------------|---------------------|----------|
| Split snowflake_utils | 0 | ⭐⭐⭐⭐⭐ | 4-6 hours |
| Split app.py | 0 | ⭐⭐⭐⭐⭐ | 6-8 hours |
| Extract CSS | -200 | ⭐⭐⭐ | 1 hour |
| Cursor context mgr | -150 | ⭐⭐⭐⭐ | 2 hours |
| Config extraction | -50 | ⭐⭐⭐ | 1 hour |
| Add type hints | +100 | ⭐⭐⭐⭐ | 3-4 hours |
| Custom exceptions | +50 | ⭐⭐⭐ | 2 hours |

**Total Estimated Time:** 19-24 hours of focused development

**Expected Outcome:**
- More modular, maintainable codebase
- Easier to onboard new developers
- Faster feature development
- Better testability
- Reduced bug surface area

---

## Next Steps

1. Review this guide with the team
2. Prioritize which refactorings to tackle first
3. Create GitHub issues for each refactoring task
4. Implement incrementally (don't refactor everything at once!)
5. Add tests as you refactor each module
6. Update documentation as you go

**Remember:** The goal is not perfection, but continuous improvement. Tackle high-impact items first!
