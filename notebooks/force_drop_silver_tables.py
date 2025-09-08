# Databricks notebook source
# MAGIC %md
# MAGIC # Force Drop Silver Tables
# MAGIC 
# MAGIC This notebook forcefully drops all silver tables including any that might be cached or locked.

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
logger = StructuredLogger("force_drop_silver_tables")

logger.info("Starting FORCE drop of Silver tables", 
            catalog=config.catalog,
            silver_schema=config.silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Force Drop Silver Tables

# COMMAND ----------

# List of silver tables to force drop
silver_tables = [
    "slv_workspace",
    "slv_entity_latest", 
    "slv_clusters",
    "slv_usage_txn",
    "slv_job_run_timeline",
    "slv_job_task_run_timeline"
]

# Force drop each table
for table_name in silver_tables:
    full_table_name = f"{config.catalog}.{config.silver_schema}.{table_name}"
    
    try:
        # Try multiple drop approaches
        logger.info(f"Attempting to drop table: {full_table_name}")
        
        # Method 1: Standard DROP TABLE
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            logger.info(f"✅ Standard DROP successful: {full_table_name}")
        except Exception as e1:
            logger.warning(f"Standard DROP failed for {full_table_name}: {str(e1)}")
            
            # Method 2: Force drop with PURGE
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name} PURGE")
                logger.info(f"✅ PURGE DROP successful: {full_table_name}")
            except Exception as e2:
                logger.warning(f"PURGE DROP failed for {full_table_name}: {str(e2)}")
                
                # Method 3: Try to drop the underlying files
                try:
                    # Get table location and try to drop files
                    table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").collect()
                    for row in table_info:
                        if "Location:" in str(row):
                            location = str(row).split("Location:")[1].strip()
                            logger.info(f"Table location: {location}")
                            # Note: We can't directly delete files from notebook, but we can try to recreate
                            break
                except Exception as e3:
                    logger.error(f"Could not get table location for {full_table_name}: {str(e3)}")
        
        # Verify table is gone
        if spark.catalog.tableExists(full_table_name):
            logger.error(f"❌ Table still exists after drop attempts: {full_table_name}")
        else:
            logger.info(f"✅ Table successfully dropped: {full_table_name}")
            
    except Exception as e:
        logger.error(f"Error in drop process for {full_table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Spark Cache

# COMMAND ----------

try:
    spark.catalog.clearCache()
    logger.info("✅ Spark catalog cache cleared")
except Exception as e:
    logger.warning(f"Could not clear Spark cache: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Tables Dropped

# COMMAND ----------

# Check which tables still exist
logger.info("Checking remaining tables in silver schema...")

try:
    tables = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.silver_schema}")
    remaining_tables = [row.tableName for row in tables.collect()]
    
    if remaining_tables:
        logger.info(f"Remaining tables in {config.catalog}.{config.silver_schema}: {remaining_tables}")
        
        # Check if any of our target tables still exist
        target_tables = [t for t in remaining_tables if t in silver_tables]
        if target_tables:
            logger.error(f"❌ Target tables still exist: {target_tables}")
        else:
            logger.info("✅ All target tables successfully dropped")
    else:
        logger.info(f"✅ No tables remaining in {config.catalog}.{config.silver_schema}")
        
except Exception as e:
    logger.error(f"Error checking remaining tables: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Force Drop Complete

# COMMAND ----------

logger.info("Force drop of Silver tables completed")
print("✅ Force drop completed!")
print("🚀 You can now run the simplified Silver Layer notebook to recreate the tables with clean schemas.")
