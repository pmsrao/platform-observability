"""
Processing State Management for Platform Observability

This module provides utilities for managing processing state and progress tracking
across different data layers and pipelines with task-based offset management.

The processing state system enables:
1. Tracking High Water Mark (HWM) for Bronze layer ingestion
2. Managing Change Data Feed (CDF) bookmarks for Silver layer processing
3. Recording processing progress for resumable operations
4. Supporting incremental processing without data duplication
5. Task-based offset management to avoid conflicts between multiple fact tables

Key Functions:
- get_processing_state_table(): Get the fully qualified processing state table name
- ensure_table(): Create the processing state table if it doesn't exist
- get_last_processed_timestamp(): Retrieve the last processed timestamp for a source and task
- commit_processing_state(): Update the processing state with new progress information
- get_task_name(): Generate standardized task names for fact tables

Author: Platform Observability Team
Version: 2.0 (Task-based offsets)
"""

from pyspark.sql import functions as F
from config import Config


def get_processing_state_table(layer="bronze"):
    """
    Get the fully qualified processing state table name using configuration.
    
    Args:
        layer (str): Data layer ('bronze' or 'silver')
    
    Returns:
        str: Fully qualified table name
    
    Example:
        >>> get_processing_state_table("bronze")
        'platform_observability.plt_bronze._bronze_hwm_processing_offsets'
        >>> get_processing_state_table("silver")
        'platform_observability.plt_silver._cdf_processing_offsets'
    """
    config = Config.get_config()
    if layer == "bronze":
        return config.get_table_name("bronze", "_bronze_hwm_processing_offsets")
    elif layer == "silver":
        return config.get_table_name("silver", "_cdf_processing_offsets")
    else:
        raise ValueError(f"Unsupported layer: {layer}")


def get_task_name(fact_table_name):
    """
    Generate standardized task name for fact table processing.
    
    Args:
        fact_table_name (str): Name of the fact table (e.g., 'gld_fact_usage_priced_day')
    
    Returns:
        str: Standardized task name (e.g., 'task_gld_fact_usage_priced_day')
    
    Example:
        >>> get_task_name("gld_fact_usage_priced_day")
        'task_gld_fact_usage_priced_day'
    """
    return f"task_{fact_table_name}"


def ensure_table(spark, layer="bronze"):
    """
    Create the processing state table if it doesn't exist.
    
    This function ensures the processing state table is available before any
    state tracking operations are performed.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        layer (str): Data layer ('bronze' or 'silver')
    
    Table Schema (Bronze):
        - source_table: STRING - Name of the source table
        - last_processed_timestamp: TIMESTAMP - Last processed timestamp
        - last_processed_version: BIGINT - Last processed version (for CDF)
        - updated_at: TIMESTAMP - When the state was last updated
    
    Table Schema (Silver - Task-based):
        - source_table: STRING - Name of the source table
        - task_name: STRING - Task name (e.g., 'task_gld_fact_usage_priced_day')
        - last_processed_timestamp: TIMESTAMP - Last processed timestamp
        - last_processed_version: BIGINT - Last processed version (for CDF)
        - updated_at: TIMESTAMP - When the state was last updated
    """
    processing_state_table = get_processing_state_table(layer)
    
    if layer == "bronze":
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {processing_state_table} (
                source_table STRING,
                last_processed_timestamp TIMESTAMP,
                last_processed_version BIGINT,
                updated_at TIMESTAMP
            ) USING DELTA
            TBLPROPERTIES (
                'delta.feature.allowColumnDefaults' = 'supported'
            )
        """)
    elif layer == "silver":
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {processing_state_table} (
                source_table STRING,
                task_name STRING,
                last_processed_timestamp TIMESTAMP,
                last_processed_version BIGINT,
                updated_at TIMESTAMP
            ) USING DELTA
            PARTITIONED BY (source_table)
            TBLPROPERTIES (
                'delta.feature.allowColumnDefaults' = 'supported'
            )
        """)


def get_last_processed_timestamp(spark, source_table: str, task_name: str = None, layer: str = "bronze"):
    """
    Retrieve the last processed timestamp for a source table and task.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        source_table (str): Name of the source table to get state for
        task_name (str, optional): Task name for Silver layer processing
        layer (str): Data layer ('bronze' or 'silver')
    
    Returns:
        tuple: (timestamp, version) or (None, None) if no state exists
    
    Example:
        >>> get_last_processed_timestamp(spark, "system.billing.usage")
        (datetime.datetime(2024, 1, 15, 10, 30, 0), 12345)
        >>> get_last_processed_timestamp(spark, "slv_usage_txn", "task_gld_fact_usage_priced_day", "silver")
        (datetime.datetime(2024, 1, 15, 10, 30, 0), 12345)
    """
    ensure_table(spark, layer)
    processing_state_table = get_processing_state_table(layer)
    
    if layer == "bronze":
        row = spark.table(processing_state_table).where(
            F.col("source_table") == source_table
        ).select("last_processed_timestamp", "last_processed_version").first()
    elif layer == "silver":
        if not task_name:
            raise ValueError("task_name is required for Silver layer processing")
        row = spark.table(processing_state_table).where(
            (F.col("source_table") == source_table) & 
            (F.col("task_name") == task_name)
        ).select("last_processed_timestamp", "last_processed_version").first()
    
    if row is None:
        return None, None
    return row[0], row[1]


def commit_processing_state(spark, source_table: str, ts, version=None, task_name: str = None, layer: str = "bronze"):
    """
    Update the processing state with a new timestamp and version for a source table and task.
    
    This function uses MERGE to either update an existing state record
    or insert a new one if it doesn't exist.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        source_table (str): Name of the source table
        ts: Timestamp to set as the last processed timestamp
        version: Optional version number for CDF processing
        task_name (str, optional): Task name for Silver layer processing
        layer (str): Data layer ('bronze' or 'silver')
    
    Example:
        >>> commit_processing_state(spark, "system.billing.usage", "2024-01-15 10:30:00")
        >>> commit_processing_state(spark, "slv_usage_txn", "2024-01-15 10:30:00", 12345, "task_gld_fact_usage_priced_day", "silver")
    """
    ensure_table(spark, layer)
    processing_state_table = get_processing_state_table(layer)
    
    if layer == "bronze":
        spark.sql(f"""
            MERGE INTO {processing_state_table} T
            USING (
                SELECT 
                    '{source_table}' AS source_table, 
                    TIMESTAMP('{ts}') AS last_processed_timestamp, 
                    {version if version else 'NULL'} AS last_processed_version,
                    current_timestamp() AS updated_at
            ) S
            ON T.source_table = S.source_table
            WHEN MATCHED THEN UPDATE SET 
                last_processed_timestamp = S.last_processed_timestamp,
                last_processed_version = S.last_processed_version,
                updated_at = S.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
    elif layer == "silver":
        if not task_name:
            raise ValueError("task_name is required for Silver layer processing")
        spark.sql(f"""
            MERGE INTO {processing_state_table} T
            USING (
                SELECT 
                    '{source_table}' AS source_table,
                    '{task_name}' AS task_name,
                    TIMESTAMP('{ts}') AS last_processed_timestamp, 
                    {version if version else 'NULL'} AS last_processed_version,
                    current_timestamp() AS updated_at
            ) S
            ON T.source_table = S.source_table AND T.task_name = S.task_name
            WHEN MATCHED THEN UPDATE SET 
                last_processed_timestamp = S.last_processed_timestamp,
                last_processed_version = S.last_processed_version,
                updated_at = S.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
