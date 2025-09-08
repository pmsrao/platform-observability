# Databricks notebook source
# MAGIC %md
# MAGIC # Fix Silver Processing State Table
# MAGIC 
# MAGIC This notebook fixes the Silver layer processing state table (`_cdf_processing_offsets`) by dropping and recreating it with the correct Delta features enabled.
# MAGIC 
# MAGIC **Note**: Only the Silver layer has the DEFAULT clause issue. Bronze layer is working fine.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import sys
import os
from libs.logging import StructuredLogger
from config import Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("fix_processing_state_tables")

logger.info("Starting processing state tables fix", 
            catalog=config.catalog,
            bronze_schema=config.bronze_schema,
            silver_schema=config.silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Silver Processing State Table

# COMMAND ----------

# List of processing state tables to drop (only Silver layer has the issue)
processing_state_tables = [
    f"{config.catalog}.{config.silver_schema}._cdf_processing_offsets"
]

# Drop each table
for table_name in processing_state_tables:
    try:
        if spark.catalog.tableExists(table_name):
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"Successfully dropped table: {table_name}")
            print(f"✅ Dropped: {table_name}")
        else:
            logger.info(f"Table does not exist: {table_name}")
            print(f"ℹ️  Table does not exist: {table_name}")
    except Exception as e:
        logger.error(f"Error dropping table {table_name}: {str(e)}")
        print(f"❌ Error dropping {table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recreate Silver Processing State Table

# COMMAND ----------

try:
    # Import the processing state module
    from libs.processing_state import ensure_table
    
    # Only recreate silver processing state table (bronze is working fine)
    print("🔧 Creating silver processing state table...")
    ensure_table(spark, "silver")
    print("✅ Silver processing state table created")
    
    logger.info("Silver processing state table recreated successfully")
    print("🎉 Silver processing state table fixed successfully!")
    
except Exception as e:
    logger.error(f"Error recreating silver processing state table: {str(e)}")
    print(f"❌ Error recreating silver processing state table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables Created

# COMMAND ----------

# Check which table was created
logger.info("Checking created silver processing state table...")

try:
    for table_name in processing_state_tables:
        if spark.catalog.tableExists(table_name):
            logger.info(f"Processing state table exists: {table_name}")
            print(f"✅ {table_name} - EXISTS")
        else:
            logger.error(f"Processing state table missing: {table_name}")
            print(f"❌ {table_name} - MISSING")
        
except Exception as e:
    logger.error(f"Error checking processing state tables: {str(e)}")
    print(f"❌ Error checking processing state tables: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix Complete

# COMMAND ----------

logger.info("Silver processing state table fix completed successfully")
print("🎉 Silver processing state table fix completed!")
print("🚀 You can now run the Silver Layer notebook again.")
