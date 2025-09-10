# Databricks notebook source
# MAGIC %md
# MAGIC # Simple DLT Pipeline for Testing
# MAGIC
# MAGIC This is a simple DLT pipeline that reads from the existing `brz_billing_list_prices` table
# MAGIC and creates a simple view for testing platform observability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import sys
import os
from datetime import datetime

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Import from libs package
from libs.path_setup import setup_paths_and_import_config

# Setup paths and import Config
Config = setup_paths_and_import_config()
config = Config.get_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Table Definition

# COMMAND ----------

import dlt

@dlt.table(
    name="current_prices",
    comment="Current pricing data extracted from billing list prices",
    table_properties={
        "quality": "silver",
        "pipeline.autoOptimize.managed": "true"
    }
)
def current_prices():
    """Extract current pricing data from billing list prices"""
    
    # Read from the existing bronze table
    source_table = f"{config.catalog}.{config.bronze_schema}.brz_billing_list_prices"
    
    return (
        spark.table(source_table)
        .filter(F.col("price_end_time") > F.current_timestamp())  # Only current prices
        .select(
            F.col("sku_name"),
            F.col("cloud"),
            F.col("currency_code"),
            F.col("usage_unit"),
            F.col("pricing.default").alias("price_usd"),
            F.col("price_start_time"),
            F.col("price_end_time"),
            F.current_timestamp().alias("_processed_at")
        )
        .distinct()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Simple View (for testing)

# COMMAND ----------

@dlt.view(
    name="pricing_summary",
    comment="Summary view of pricing data by SKU and cloud"
)
def pricing_summary():
    """Create a summary view of pricing data"""
    
    return (
        dlt.read("current_prices")
        .groupBy("sku_name", "cloud", "currency_code")
        .agg(
            F.count("*").alias("price_count"),
            F.min("price_usd").alias("min_price"),
            F.max("price_usd").alias("max_price"),
            F.avg("price_usd").alias("avg_price")
        )
        .orderBy("sku_name", "cloud")
    )
