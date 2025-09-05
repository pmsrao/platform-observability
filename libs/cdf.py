"""
Change Data Feed (CDF) Management for Platform Observability

This module provides utilities for reading and managing Change Data Feed (CDF)
from Delta tables, enabling incremental processing of data changes.

The CDF system enables:
1. Reading only new/changed records from Delta tables
2. Tracking processed versions to avoid reprocessing
3. Supporting different change types (insert, update, delete)
4. Automatic bookmark management for resumable processing

Key Functions:
- read_cdf(): Read CDF data from a Delta table with version tracking
- commit_bookmark(): Update bookmarks after successful processing
- _get_latest_version(): Get the latest version number from a Delta table
- _get_bookmark(): Retrieve the last processed version from bookmarks
- _set_bookmark(): Update bookmarks with new version information

Author: Platform Observability Team
Version: 1.0
"""

from pyspark.sql import SparkSession, DataFrame, functions as F
from config import Config

# Get configuration
config = Config.get_config()
PROCESSING_OFFSETS_TABLE = f"{config.catalog}.{config.silver_schema}._cdf_processing_offsets"


def _get_latest_version(spark: SparkSession, table_fqn: str) -> int:
    """
    Get the latest version number from a Delta table.
    
    This function queries the table history to determine the most recent
    version number, which is used to establish the end boundary for CDF reads.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        table_fqn (str): Fully qualified table name (catalog.schema.table)
    
    Returns:
        int: Latest version number, or 0 if no history exists
    
    Example:
        >>> _get_latest_version(spark, "platform_observability.plt_bronze.brz_billing_usage")
        42
    """
    try:
        # Get the latest version using DESCRIBE HISTORY
        history_df = spark.sql(f"DESCRIBE HISTORY {table_fqn}")
        if history_df.count() > 0:
            latest_version = history_df.select(F.max("version")).first()[0]
            return latest_version if latest_version is not None else 0
        return 0
    except Exception as e:
        print(f"Warning: Could not get latest version for {table_fqn}: {e}")
        return 0


def _get_processing_offset(spark: SparkSession, source_name: str) -> int:
    """
    Get the last processed version from processing offsets table.
    
    This function retrieves the processing offset that tracks the last successfully
    processed version for a given source table.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        source_name (str): Name identifier for the source table
    
    Returns:
        int: Last processed version number, or 0 if no offset exists
    
    Example:
        >>> _get_processing_offset(spark, "brz_billing_usage")
        35
    """
    try:
        offset_df = spark.sql(f"""
            SELECT last_processed_version 
            FROM {PROCESSING_OFFSETS_TABLE} 
            WHERE source_table = '{source_name}'
        """)
        
        if offset_df.count() > 0:
            last_version = offset_df.first()[0]
            return last_version if last_version is not None else 0
        return 0
    except Exception as e:
        print(f"Warning: Could not get processing offset for {source_name}: {e}")
        return 0


def _set_processing_offset(spark: SparkSession, source_name: str, version: int) -> None:
    """
    Set the processing offset for a source table.
    
    This function updates the processing offsets table with the latest processed
    version for a given source table using MERGE for upsert operations.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        source_name (str): Name identifier for the source table
        version (int): Version number to set as last processed
    
    Example:
        >>> _set_processing_offset(spark, "brz_billing_usage", 42)
    """
    try:
        # Upsert processing offset
        spark.sql(f"""
            MERGE INTO {PROCESSING_OFFSETS_TABLE} T
            USING (
                SELECT 
                    '{source_name}' as source_table, 
                    {version} as last_processed_version, 
                    CURRENT_TIMESTAMP() as updated_at
            ) S
            ON T.source_table = S.source_table
            WHEN MATCHED THEN UPDATE SET 
                T.last_processed_version = S.last_processed_version,
                T.last_processed_timestamp = CURRENT_TIMESTAMP(),
                T.updated_at = S.updated_at
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as e:
        print(f"Warning: Could not set processing offset for {source_name}: {e}")


def read_cdf(spark: SparkSession, table: str, source_name: str, change_types=("insert", "update_postimage")) -> tuple[DataFrame, int]:
    """
    Read Change Data Feed (CDF) from a Delta table and return new data + end version.
    
    This function reads only the changes that occurred after the last processed
    version, enabling incremental processing without reprocessing existing data.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        table (str): Fully qualified table name to read CDF from
        source_name (str): Name identifier for bookmark tracking
        change_types (tuple): Types of changes to include (default: insert, update_postimage)
    
    Returns:
        tuple: (DataFrame with new/changed data, end version number)
    
    Example:
        >>> df, end_ver = read_cdf(spark, "platform_observability.plt_bronze.brz_billing_usage", "brz_billing_usage")
        >>> print(f"Read {df.count()} new records up to version {end_ver}")
    """
    start_version = _get_processing_offset(spark, source_name)
    end_version = _get_latest_version(spark, table)
    
    if start_version >= end_version:
        # No new data
        return spark.createDataFrame([], spark.table(table).schema), end_version
    
    opts = {
        "readChangeDataFeed": "true",
        "startingVersion": str(start_version + 1),
        "endingVersion": str(end_version)
    }
    
    try:
        df = spark.read.format("delta").options(**opts).table(table)
        df = df.where(F.col("_change_type").isin(list(change_types)))
        return df, end_version
    except Exception as e:
        print(f"Error reading CDF from {table}: {e}")
        return spark.createDataFrame([], spark.table(table).schema), end_version


def commit_processing_offset(spark: SparkSession, source_name: str, version: int) -> None:
    """
    Commit processing offset for CDF processing.
    
    This function updates the processing offset after successful processing of CDF data,
    ensuring that the next run will start from the correct version.
    
    Args:
        spark: SparkSession instance for executing SQL commands
        source_name (str): Name identifier for the source table
        version (int): Version number that was successfully processed
    
    Example:
        >>> commit_processing_offset(spark, "brz_billing_usage", 42)
    """
    _set_processing_offset(spark, source_name, version)
