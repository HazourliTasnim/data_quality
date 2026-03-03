"""
Refactored Snowflake Connection Module

Demonstrates best practices:
- Context managers for resource cleanup
- Clear separation of concerns
- Reusable patterns
- Better error handling
"""

import re
import logging
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Tuple
from snowflake.connector import SnowflakeConnection
import snowflake.connector

# Configure logging
logger = logging.getLogger(__name__)

# =============================================================================
# Custom Exceptions
# =============================================================================

class SnowflakeConnectionError(Exception):
    """Raised when Snowflake connection fails."""
    pass


class SnowflakeQueryError(Exception):
    """Raised when Snowflake query execution fails."""
    pass


# =============================================================================
# Resource Management
# =============================================================================

@contextmanager
def snowflake_cursor(conn: SnowflakeConnection):
    """
    Context manager for Snowflake cursors with automatic cleanup.

    This eliminates the try/finally pattern repeated throughout the codebase.

    Usage:
        with snowflake_cursor(conn) as cursor:
            cursor.execute("SELECT ...")
            results = cursor.fetchall()

    Args:
        conn: Active Snowflake connection

    Yields:
        Snowflake cursor

    Raises:
        SnowflakeQueryError: If query execution fails
    """
    cursor = conn.cursor()
    try:
        yield cursor
    except Exception as e:
        logger.error(f"Query execution failed: {e}", exc_info=True)
        raise SnowflakeQueryError(f"Query failed: {e}") from e
    finally:
        cursor.close()
        logger.debug("Cursor closed")


# =============================================================================
# Connection Management
# =============================================================================

def parse_account_from_url(url: str) -> str:
    """
    Extract account identifier from Snowflake URL.

    Args:
        url: Snowflake URL (with or without protocol)

    Returns:
        Account identifier

    Examples:
        >>> parse_account_from_url("https://mycompany.eu-central-1.snowflakecomputing.com")
        'mycompany.eu-central-1'
        >>> parse_account_from_url("abc123.us-east-1.snowflakecomputing.com")
        'abc123.us-east-1'
    """
    url = url.strip()
    # Remove protocol
    url = re.sub(r'^https?://', '', url)
    # Remove trailing slash
    url = url.rstrip('/')
    # Remove .snowflakecomputing.com suffix
    account = re.sub(r'\.snowflakecomputing\.com$', '', url, flags=re.IGNORECASE)
    return account


def create_connection(
    snowflake_url: str,
    user: str,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
) -> Tuple[Optional[SnowflakeConnection], Optional[str]]:
    """
    Create a Snowflake connection using SSO authentication.

    Args:
        snowflake_url: Snowflake account URL
        user: Username for authentication
        warehouse: Optional warehouse name
        role: Optional role name
        database: Optional database name
        schema: Optional schema name

    Returns:
        Tuple of (connection, error_message). If successful, error is None.
        If failed, connection is None and error contains message.

    Example:
        >>> conn, error = create_connection("myaccount.snowflakecomputing.com", "user@company.com")
        >>> if error:
        ...     print(f"Failed: {error}")
        ... else:
        ...     print("Connected!")
    """
    try:
        account = parse_account_from_url(snowflake_url)
        logger.info(f"Attempting connection to account: {account}, user: {user}")

        conn_params = {
            "account": account,
            "user": user,
            "authenticator": "externalbrowser",
        }

        # Add optional parameters
        if warehouse:
            conn_params["warehouse"] = warehouse
        if role:
            conn_params["role"] = role
        if database:
            conn_params["database"] = database
        if schema:
            conn_params["schema"] = schema

        conn = snowflake.connector.connect(**conn_params)
        logger.info(f"Successfully connected to {account}")
        return conn, None

    except Exception as e:
        error_msg = f"Connection failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return None, error_msg


def switch_context(
    conn: SnowflakeConnection,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Switch Snowflake session context (warehouse, role, database, schema).

    Args:
        conn: Active Snowflake connection
        warehouse: Optional warehouse to switch to
        role: Optional role to switch to
        database: Optional database to switch to
        schema: Optional schema to switch to

    Returns:
        Tuple of (success: bool, error_message: Optional[str])

    Example:
        >>> success, error = switch_context(conn, warehouse="COMPUTE_WH", database="PROD_DB")
        >>> if not success:
        ...     print(f"Failed to switch context: {error}")
    """
    try:
        with snowflake_cursor(conn) as cursor:
            if warehouse:
                cursor.execute(f"USE WAREHOUSE {warehouse}")
                logger.info(f"Switched to warehouse: {warehouse}")

            if role:
                cursor.execute(f"USE ROLE {role}")
                logger.info(f"Switched to role: {role}")

            if database:
                cursor.execute(f"USE DATABASE {database}")
                logger.info(f"Switched to database: {database}")

            if schema:
                cursor.execute(f"USE SCHEMA {schema}")
                logger.info(f"Switched to schema: {schema}")

        return True, None

    except Exception as e:
        error_msg = f"Context switch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg


# =============================================================================
# Metadata Queries
# =============================================================================

def list_warehouses(conn: SnowflakeConnection) -> List[str]:
    """
    List all accessible warehouses.

    Args:
        conn: Active Snowflake connection

    Returns:
        List of warehouse names

    Raises:
        SnowflakeQueryError: If query fails
    """
    with snowflake_cursor(conn) as cursor:
        cursor.execute("SHOW WAREHOUSES")
        return [row[0] for row in cursor.fetchall()]


def list_roles(conn: SnowflakeConnection) -> List[str]:
    """
    List all accessible roles.

    Args:
        conn: Active Snowflake connection

    Returns:
        List of role names

    Raises:
        SnowflakeQueryError: If query fails
    """
    with snowflake_cursor(conn) as cursor:
        cursor.execute("SHOW ROLES")
        return [row[1] for row in cursor.fetchall()]


def list_databases(conn: SnowflakeConnection) -> List[str]:
    """
    List all accessible databases.

    Args:
        conn: Active Snowflake connection

    Returns:
        List of database names

    Raises:
        SnowflakeQueryError: If query fails
    """
    with snowflake_cursor(conn) as cursor:
        cursor.execute("SHOW DATABASES")
        return [row[1] for row in cursor.fetchall()]


def list_schemas(conn: SnowflakeConnection, database: str) -> List[str]:
    """
    List all schemas in a database.

    Args:
        conn: Active Snowflake connection
        database: Database name

    Returns:
        List of schema names

    Raises:
        SnowflakeQueryError: If query fails
    """
    with snowflake_cursor(conn) as cursor:
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return [row[1] for row in cursor.fetchall()]


def list_tables(conn: SnowflakeConnection, database: str, schema: str) -> List[str]:
    """
    List all tables in a schema.

    Args:
        conn: Active Snowflake connection
        database: Database name
        schema: Schema name

    Returns:
        List of table names

    Raises:
        SnowflakeQueryError: If query fails
    """
    with snowflake_cursor(conn) as cursor:
        cursor.execute(f"SHOW TABLES IN {database}.{schema}")
        return [row[1] for row in cursor.fetchall()]


def get_table_columns(
    conn: SnowflakeConnection,
    database: str,
    schema: str,
    table: str
) -> List[Dict[str, Any]]:
    """
    Get column metadata for a table.

    Args:
        conn: Active Snowflake connection
        database: Database name
        schema: Schema name
        table: Table name

    Returns:
        List of column dictionaries with keys:
        - column_name: str
        - data_type: str
        - is_nullable: bool
        - comment: Optional[str]

    Raises:
        SnowflakeQueryError: If query fails
    """
    with snowflake_cursor(conn) as cursor:
        cursor.execute(f"DESCRIBE TABLE {database}.{schema}.{table}")
        columns = []
        for row in cursor.fetchall():
            columns.append({
                "column_name": row[0],
                "data_type": row[1],
                "is_nullable": row[3] == "Y",
                "comment": row[8] if len(row) > 8 else None
            })
        return columns


# =============================================================================
# Query Execution Helpers
# =============================================================================

def execute_query(
    conn: SnowflakeConnection,
    query: str,
    params: Optional[tuple] = None
) -> Tuple[Optional[List[Any]], Optional[str]]:
    """
    Execute a query and return results with error handling.

    Args:
        conn: Active Snowflake connection
        query: SQL query string
        params: Optional query parameters for parameterized queries

    Returns:
        Tuple of (results, error_message). If successful, error is None.
        If failed, results is None and error contains message.

    Example:
        >>> results, error = execute_query(conn, "SELECT * FROM table WHERE id = %s", (123,))
        >>> if error:
        ...     print(f"Query failed: {error}")
        ... else:
        ...     for row in results:
        ...         print(row)
    """
    try:
        with snowflake_cursor(conn) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            return results, None

    except Exception as e:
        error_msg = f"Query execution failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return None, error_msg


def execute_query_single(
    conn: SnowflakeConnection,
    query: str,
    params: Optional[tuple] = None
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Execute a query and return single result row.

    Args:
        conn: Active Snowflake connection
        query: SQL query string
        params: Optional query parameters

    Returns:
        Tuple of (result_row, error_message). If successful, error is None.
        If failed, result is None and error contains message.
    """
    try:
        with snowflake_cursor(conn) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchone()
            return result, None

    except Exception as e:
        error_msg = f"Query execution failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return None, error_msg


# =============================================================================
# Usage Example
# =============================================================================

if __name__ == "__main__":
    # Example usage demonstrating the improved patterns
    logging.basicConfig(level=logging.INFO)

    # Connect with error handling
    conn, error = create_connection(
        "myaccount.snowflakecomputing.com",
        "user@company.com",
        warehouse="COMPUTE_WH"
    )

    if error:
        print(f"Connection failed: {error}")
        exit(1)

    print("Connected successfully!")

    # Query with automatic cursor management
    try:
        databases = list_databases(conn)
        print(f"Found {len(databases)} databases")

        # Switch context
        success, error = switch_context(conn, database="PROD_DB", schema="PUBLIC")
        if not success:
            print(f"Context switch failed: {error}")

        # Execute custom query
        results, error = execute_query(conn, "SELECT COUNT(*) FROM my_table")
        if error:
            print(f"Query failed: {error}")
        else:
            print(f"Row count: {results[0][0]}")

    finally:
        conn.close()
        print("Connection closed")
