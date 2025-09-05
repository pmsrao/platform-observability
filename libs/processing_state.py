"""
Processing State Management for Platform Observability

This module provides utilities for managing processing state and progress tracking
across different data layers and pipelines.

The processing state system enables:
1. Tracking High Water Mark (HWM) for Bronze layer ingestion
2. Managing Change Data Feed (CDF) bookmarks for Silver layer processing
3. Recording processing progress for resumable operations
4. Supporting incremental processing without data duplication

Key Functions:
- get_processing_state_table(): Get the fully qualified processing state table name
- ensure_table(): Create the processing state table if it doesn't exist
- get_last_processed_timestamp(): Retrieve the last processed timestamp for a source
- commit_processing_state(): Update the processing state with new progress information

Author: Platform Observability Team
Version: 1.0
"""

from pyspark.sql import functions as F
from config import Config


def get_processing_state_table():
    """
    Get the fully qualified processing state table name using configuration.
    
    Returns:
        str: Fully qualified table name in format 'catalog.schema._bronze_hwm_processing_offsets'
    
    Example:
        >>> get_processing_state_table()
        'platform_observability.plt_bronze._bronze_hwm_processing_offsets'
    """
    return Config.get_table_name("bronze", "_bronze_hwm_processing_offsets")


def ensure_table(spark):
    """
    Create the processing state table if it doesn't exist.
    
    This function ensures the processing state table is available before any
    state tracking operations are performed.
    
    Args:
        spark: SparkSession instance for executing SQL commands
    
    Table Schema:
        - source_table: STRING PRIMARY KEY - Name of the source table
        - last_processed_timestamp: TIMESTAMP - Last processed timestamp
        - last_processed_version: BIGINT - Last processed version (for CDF)
        - updated_at: TIMESTAMP - When the state was last updated
    """
    processing_state_table = get_processing_state_table()
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {processing_state_table} (
            source_table STRING PRIMARY KEY,
            last_processed_timestamp TIMESTAMP,
            last_processed_version BIGINT,
            updated_at   TIMESTAMP
        )
    """)


def get_last_processed_timestamp(spark, source_table: str):
    """
    Retrieve the last processed timestamp for a source table.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        source_table (str): Name of the source table to get state for
    
    Returns:
        timestamp or None: Last processed timestamp, or None if no state exists
    
    Example:
        >>> get_last_processed_timestamp(spark, "system.billing.usage")
        datetime.datetime(2024, 1, 15, 10, 30, 0)
    """
    ensure_table(spark)
    processing_state_table = get_processing_state_table()
    row = spark.table(processing_state_table).where(
        F.col("source_table") == source_table
    ).select("last_processed_timestamp").first()
    return None if row is None else row[0]


def commit_processing_state(spark, source_table: str, ts, version=None):
    """
    Update the processing state with a new timestamp and version for a source table.
    
    This function uses MERGE to either update an existing state record
    or insert a new one if it doesn't exist.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        source_table (str): Name of the source table
        ts: Timestamp to set as the last processed timestamp
        version: Optional version number for CDF processing
    
    Example:
        >>> commit_processing_state(spark, "system.billing.usage", "2024-01-15 10:30:00")
    """
    ensure_table(spark)
    processing_state_table = get_processing_state_table()
    spark.sql(f"""
        MERGE INTO {processing_state_table} T
        USING (
            SELECT 
                '{source_table}' AS source_table, 
                TIMESTAMP('{ts}') AS last_ts, 
                current_timestamp() AS updated_at
        ) S
        ON T.source_table = S.source_table
        WHEN MATCHED THEN UPDATE SET 
            last_ts = S.last_ts, 
            updated_at = S.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
