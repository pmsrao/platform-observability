# Databricks notebook source
# MAGIC %md
# MAGIC # Drop Silver Tables
# MAGIC 
# MAGIC This notebook drops all existing silver tables to allow for clean recreation with the simplified approach.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from libs.logging import StructuredLogger
from config import Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("drop_silver_tables")

logger.info("Starting Silver tables cleanup", 
            catalog=config.catalog,
            silver_schema=config.silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Silver Tables

# COMMAND ----------

# List of all silver tables to drop (complete list from silver_tables.sql)
silver_tables = [
    # Type 1 tables (Current values only)
    "slv_workspace",
    "slv_entity_latest", 
    "slv_usage_txn",
    "slv_job_run_timeline",  # Type 1 - Run execution data
    "slv_job_task_run_timeline",  # Type 1 - Task run data
    
    # Type 2 tables (SCD2 - Historical tracking)
    "slv_jobs_scd",
    "slv_pipelines_scd", 
    "slv_price_scd",
    "slv_clusters",
    
    # Processing state tables (if they exist in silver schema)
    "_cdf_processing_offsets"
]

# Drop each table
for table_name in silver_tables:
    full_table_name = f"{config.catalog}.{config.silver_schema}.{table_name}"
    
    try:
        if spark.catalog.tableExists(full_table_name):
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            logger.info(f"Successfully dropped table: {full_table_name}")
        else:
            logger.info(f"Table does not exist: {full_table_name}")
    except Exception as e:
        logger.error(f"Error dropping table {full_table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables Dropped

# COMMAND ----------

# Check which tables still exist
logger.info("Checking remaining tables in silver schema...")

try:
    tables = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.silver_schema}")
    remaining_tables = [row.tableName for row in tables.collect()]
    
    if remaining_tables:
        logger.info(f"Remaining tables in {config.catalog}.{config.silver_schema}: {remaining_tables}")
    else:
        logger.info(f"No tables remaining in {config.catalog}.{config.silver_schema}")
        
except Exception as e:
    logger.error(f"Error checking remaining tables: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Complete

# COMMAND ----------

logger.info("Silver tables cleanup completed successfully")
print("✅ Silver tables cleanup completed!")
print("🚀 You can now run the simplified Silver Layer notebook to recreate the tables with clean schemas.")
