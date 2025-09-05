"""
Utility Functions for Platform Observability

This module provides common utility functions used across the platform
observability system for data processing and manipulation.

Key Functions:
- yyyymmdd(): Convert timestamp to YYYYMMDD integer format
- impacted_dates(): Generate date surrogate keys for incremental processing
- format_timestamp(): Format timestamps for display and logging
- validate_config(): Validate configuration parameters

Author: Platform Observability Team
Version: 1.0
"""

from pyspark.sql import DataFrame, functions as F
from typing import Union
from datetime import datetime


def yyyymmdd(timestamp_col) -> str:
    """
    Convert timestamp column to YYYYMMDD integer format.
    
    This function is commonly used for partitioning and date-based joins
    in the data pipeline.
    
    Args:
        timestamp_col: PySpark column expression or string column name
    
    Returns:
        str: SQL expression that converts timestamp to YYYYMMDD format
    
    Example:
        >>> df.withColumn("date_sk", yyyymmdd("timestamp_col"))
    """
    return f"int(date_format(to_date({timestamp_col}), 'yyyyMMdd'))"


def impacted_dates(df: DataFrame, date_col: str) -> DataFrame:
    """
    Generate date surrogate keys for incremental processing.
    
    This function extracts unique dates from a DataFrame and converts them
    to integer surrogate keys (YYYYMMDD format) for efficient partitioning
    and incremental processing.
    
    Args:
        df (DataFrame): Input DataFrame containing date column
        date_col (str): Name of the date column to process
    
    Returns:
        DataFrame: DataFrame with single column 'date_sk' containing unique date keys
    
    Example:
        >>> date_keys = impacted_dates(usage_df, "usage_date")
        >>> date_keys.show()
        +--------+
        | date_sk|
        +--------+
        |20240115|
        |20240116|
        +--------+
    """
    return df.selectExpr(f"int(date_format(to_date({date_col}), 'yyyyMMdd')) as date_sk").distinct()


def format_timestamp(timestamp: Union[str, datetime]) -> str:
    """
    Format timestamp for consistent display and logging.
    
    Args:
        timestamp: Timestamp as string or datetime object
    
    Returns:
        str: Formatted timestamp string
    
    Example:
        >>> format_timestamp("2024-01-15 10:30:00")
        '2024-01-15 10:30:00'
    """
    if isinstance(timestamp, str):
        return timestamp
    elif isinstance(timestamp, datetime):
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return str(timestamp)


def validate_config(config_dict: dict, required_keys: list) -> bool:
    """
    Validate configuration dictionary has required keys.
    
    Args:
        config_dict (dict): Configuration dictionary to validate
        required_keys (list): List of required configuration keys
    
    Returns:
        bool: True if all required keys are present, False otherwise
    
    Example:
        >>> config = {"catalog": "test", "schema": "test"}
        >>> validate_config(config, ["catalog", "schema"])
        True
    """
    return all(key in config_dict for key in required_keys)
