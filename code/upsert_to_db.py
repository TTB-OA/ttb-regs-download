"""
Database utility functions for DuckDB operations.

This module provides utility functions for database operations, particularly
for upserting (insert or update) records in DuckDB tables.

Example usage:
    from shared.upsert_to_db import upsert_to_db, DatabaseError
    
    try:
        upsert_to_db(conn, {'id': 1, 'name': 'John'}, 'users', 'id')
    except DatabaseError as e:
        logger.error(f"Database operation failed: {e}")
"""

import re
import logging
from typing import Union, Dict, Any, List, Tuple

# Module metadata
__version__ = "1.0.0"
__author__ = "TTB Regulations Download Project"
__all__ = [
    "upsert_to_db",
    "batch_upsert_to_db", 
    "DatabaseError",
    "validate_sql_identifier",
    "clean_numeric_value",
    "clean_record_values",
    "get_module_info"
]

# Module-level logger
logger = logging.getLogger(__name__)

# Compiled regex for SQL identifier validation (more efficient than recompiling)
_VALID_SQL_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

# Compiled regex for cleaning numeric strings
_NUMERIC_CLEANER = re.compile(r'[,\s]')


def clean_numeric_value(value: Any) -> Any:
    """
    Clean numeric strings by removing commas and formatting characters.
    
    Args:
        value: The value to clean (any type)
        
    Returns:
        Cleaned value (converted to int/float if possible, otherwise original)
    """
    if isinstance(value, str) and value.strip():
        # Remove commas and spaces
        cleaned = _NUMERIC_CLEANER.sub('', value.strip())
        
        # Try to convert to numeric type
        try:
            if '.' in cleaned:
                return float(cleaned)
            else:
                return int(cleaned)
        except ValueError:
            pass  # Return original value if conversion fails
    
    return value


def clean_record_values(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean all values in a record, converting numeric strings to proper types.
    
    Args:
        record: Dictionary with potentially dirty numeric strings
        
    Returns:
        Dictionary with cleaned numeric values
    """
    return {key: clean_numeric_value(value) for key, value in record.items()}


class DatabaseError(Exception):
    """
    Custom exception for database operation errors.
    
    This exception is raised when database operations fail due to
    validation errors, SQL execution errors, or connection issues.
    """
    pass


def validate_sql_identifier(identifier: str, identifier_type: str = "identifier") -> None:
    """
    Validate that a string is a safe SQL identifier.
    
    This function is exposed as part of the public API for external validation.
    
    Args:
        identifier: The string to validate
        identifier_type: Type of identifier for error messages (e.g., "table name", "column name")
    
    Raises:
        DatabaseError: If the identifier is invalid
        
    Example:
        validate_sql_identifier("user_table", "table name")  # OK
        validate_sql_identifier("user-table", "table name")  # Raises DatabaseError
    """
    if not isinstance(identifier, str):
        raise DatabaseError(f"Invalid {identifier_type}: must be a string, got {type(identifier).__name__}")
    
    if not identifier:
        raise DatabaseError(f"Invalid {identifier_type}: cannot be empty")
    
    if not _VALID_SQL_IDENTIFIER.match(identifier):
        raise DatabaseError(
            f"Invalid {identifier_type}: '{identifier}'. "
            "Must start with letter or underscore, contain only letters, numbers, and underscores."
        )


def upsert_to_db(
    conn, 
    record: Dict[str, Any], 
    table_name: str, 
    conflict_key: Union[str, List[str], Tuple[str, ...]] = 'id',
    clean_numeric_strings: bool = True,
    *,
    auto_commit: bool = True
) -> None:
    """
    Generic upsert function for DuckDB with comprehensive validation and error handling.
    
    Inserts a new record or updates an existing record in the specified table
    using the conflict_key(s) to determine uniqueness.
    
    Args:
        conn: DuckDB connection object
        record: Dictionary mapping column names to values
        table_name: Name of the target table
        conflict_key: Column name(s) to use for conflict resolution.
                     Can be a string for single column or list/tuple for multiple columns.
        clean_numeric_strings: If True (default), automatically clean numeric strings
                              by removing commas and converting to proper numeric types.
        auto_commit: Whether to automatically commit the transaction (default: True)
    
    Raises:
        DatabaseError: If validation fails or database operation encounters an error
        ValueError: If input parameters are invalid
    
    Example:
        # Single conflict key
        upsert_to_db(conn, {'id': 1, 'name': 'John'}, 'users', 'id')
        
        # With dirty numeric data (will be automatically cleaned)
        upsert_to_db(conn, {'id': 1, 'price': '1,080.50'}, 'products', 'id')
        
        # Multiple conflict keys
        upsert_to_db(conn, {'user_id': 1, 'role_id': 2, 'active': True}, 
                     'user_roles', ['user_id', 'role_id'])
                     
        # Without auto-commit (for batch operations)
        upsert_to_db(conn, record, 'table', 'id', auto_commit=False)
        conn.commit()  # Manual commit later
    """
    # Input validation
    if not record:
        raise ValueError("Record cannot be empty")
    
    if not isinstance(record, dict):
        raise ValueError(f"Record must be a dictionary, got {type(record)}")
    
    # Clean numeric strings if requested
    if clean_numeric_strings:
        record = clean_record_values(record)
    
    # Validate table name
    validate_sql_identifier(table_name, "table name")
    
    # Validate all column names
    for column_name in record.keys():
        validate_sql_identifier(column_name, "column name")
    
    # Normalize conflict_key to list for consistent processing
    if isinstance(conflict_key, str):
        conflict_keys = [conflict_key]
    elif isinstance(conflict_key, (list, tuple)):
        conflict_keys = list(conflict_key)
    else:
        raise ValueError(f"conflict_key must be string, list, or tuple, got {type(conflict_key)}")
    
    if not conflict_keys:
        raise ValueError("conflict_key cannot be empty")
    
    # Validate conflict keys
    for key in conflict_keys:
        validate_sql_identifier(key, "conflict key")
        if key not in record:
            raise ValueError(f"Conflict key '{key}' not found in record")
    
    sql = None
    try:
        # Build SQL components
        columns = ', '.join(f'"{col}"' for col in record.keys())
        placeholders = ', '.join(['?'] * len(record))
        conflict_clause = ', '.join(f'"{key}"' for key in conflict_keys)
        
        # Create update clause (exclude conflict keys from updates)
        update_columns = [col for col in record.keys() if col not in conflict_keys]
        if update_columns:
            update_clause = ', '.join(f'"{col}" = excluded."{col}"' for col in update_columns)
            sql = f"""
                INSERT INTO "{table_name}" ({columns}) 
                VALUES ({placeholders})
                ON CONFLICT({conflict_clause}) 
                DO UPDATE SET {update_clause}
            """
        else:
            # If all columns are conflict keys, just do INSERT ... ON CONFLICT DO NOTHING
            sql = f"""
                INSERT INTO "{table_name}" ({columns}) 
                VALUES ({placeholders})
                ON CONFLICT({conflict_clause}) 
                DO NOTHING
            """
        
        # Execute the query
        conn.execute(sql, list(record.values()))
        
        if auto_commit:
            conn.commit()
        
        logger.debug(f"Successfully upserted record into {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to upsert record into {table_name}: {e}")
        logger.debug(f"Record: {record}")
        logger.debug(f"SQL: {sql if sql is not None else 'SQL not generated'}")
        raise DatabaseError(f"Database operation failed: {e}") from e


# Convenience functions for batch operations
def batch_upsert_to_db(
    conn,
    records: List[Dict[str, Any]],
    table_name: str,
    conflict_key: Union[str, List[str], Tuple[str, ...]] = 'id',
    batch_size: int = 100,
    clean_numeric_strings: bool = True
) -> int:
    """
    High-performance batch upsert using temporary tables and SQL operations.
    
    This function performs batch upserts much more efficiently than individual
    upsert operations by using temporary tables and bulk SQL operations.
    
    Args:
        conn: DuckDB connection object
        records: List of dictionaries to upsert
        table_name: Name of the target table
        conflict_key: Column name(s) to use for conflict resolution
        batch_size: Number of records to process in each batch (default: 100)
        clean_numeric_strings: If True (default), automatically clean numeric strings
                              by removing commas and converting to proper numeric types.
        
    Returns:
        Total number of records processed
        
    Raises:
        DatabaseError: If any operation fails
        ValueError: If input parameters are invalid
        
    Example:
        # Batch upsert with default batch size
        count = batch_upsert_to_db(conn, records, "users", "id")
        
        # Batch upsert with custom batch size
        count = batch_upsert_to_db(conn, records, "users", ["user_id", "role_id"], 200)
    """
    if not records:
        logger.debug("No records provided for batch upsert")
        return 0
    
    # Import pandas here to avoid requiring it for simple upsert operations
    try:
        import pandas as pd
    except ImportError as e:
        raise DatabaseError("pandas is required for batch upsert operations") from e
    
    # Validate inputs using existing validation functions
    validate_sql_identifier(table_name, "table name")
    
    # Normalize conflict_key to list for consistent processing
    if isinstance(conflict_key, str):
        conflict_keys = [conflict_key]
    elif isinstance(conflict_key, (list, tuple)):
        conflict_keys = list(conflict_key)
    else:
        raise ValueError(f"conflict_key must be string, list, or tuple, got {type(conflict_key)}")
    
    if not conflict_keys:
        raise ValueError("conflict_key cannot be empty")
    
    # Validate conflict keys and record structure
    for key in conflict_keys:
        validate_sql_identifier(key, "conflict key")
    
    # Validate all column names in first record (assume consistent structure)
    if records:
        for column_name in records[0].keys():
            validate_sql_identifier(column_name, "column name")
    
    logger.debug(f"Starting batch upsert of {len(records)} records to {table_name} (batch_size={batch_size})")
    
    total_upserted = 0
    
    # Clean numeric strings if requested
    if clean_numeric_strings:
        records = [clean_record_values(record) for record in records]
    
    # Process records in batches
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        # Create a temporary DataFrame for this batch
        df = pd.DataFrame(batch)
        
        # Create a unique temporary table name for this batch using timestamp
        import time
        timestamp = int(time.time() * 1000000)  # microseconds for uniqueness
        temp_table = f"temp_{table_name}_{batch_num}_{timestamp}"
        
        try:
            # Clean up any existing temp objects with the same name first
            try:
                conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                conn.execute(f"DROP VIEW IF EXISTS {temp_table}")
            except Exception:
                pass  # Ignore cleanup errors
            
            # Insert batch into temporary table
            conn.register(temp_table, df)
            
            # Build column list for the upsert
            columns = list(df.columns)
            column_list = ', '.join(f'"{col}"' for col in columns)
            
            # Use DuckDB's INSERT OR REPLACE syntax for upsert
            # This is more efficient than ON CONFLICT for batch operations
            upsert_sql = f"""
            INSERT OR REPLACE INTO "{table_name}" ({column_list})
            SELECT {column_list} FROM {temp_table}
            """
            
            conn.execute(upsert_sql)
            total_upserted += len(batch)
            logger.debug(f"Successfully upserted batch {batch_num} ({len(batch)} records)")
            
            # Clean up temporary table
            try:
                conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except Exception:
                # Try dropping as view if table drop fails
                try:
                    conn.execute(f"DROP VIEW IF EXISTS {temp_table}")
                except Exception:
                    pass  # Ignore final cleanup errors
            
        except Exception as e:
            logger.error(f"Batch upsert failed for batch {batch_num}: {e}")
            # Fallback to individual inserts for this batch
            logger.info(f"Falling back to individual inserts for batch {batch_num}")
            for record in batch:
                try:
                    upsert_to_db(conn, record, table_name, conflict_key, auto_commit=False)
                    total_upserted += 1
                except Exception as individual_error:
                    logger.error(f"Individual insert also failed: {individual_error}")
            
            # Clean up temporary objects if they exist (both table and view)
            try:
                conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except Exception:
                pass
            try:
                conn.execute(f"DROP VIEW IF EXISTS {temp_table}")
            except Exception:
                pass
    
    # Commit all changes at once
    try:
        conn.commit()
        logger.debug(f"Batch upsert completed: {total_upserted} total records processed")
    except Exception as e:
        logger.error(f"Failed to commit batch upsert transaction: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise DatabaseError(f"Batch upsert commit failed: {e}") from e
    
    return total_upserted


def get_module_info() -> Dict[str, Union[str, List[str]]]:
    """
    Get module information for debugging and version tracking.
    
    Returns:
        Dictionary containing module metadata
    """
    return {
        "version": __version__,
        "author": __author__,
        "name": __name__,
        "public_functions": __all__
    }
