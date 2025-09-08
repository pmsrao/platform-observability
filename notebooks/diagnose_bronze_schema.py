# Databricks notebook source
# MAGIC %md
# MAGIC # Diagnose Bronze Schema
# MAGIC 
# MAGIC This notebook checks the actual schema of bronze tables to identify column name mismatches.

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
logger = StructuredLogger("diagnose_bronze_schema")

logger.info("Starting Bronze schema diagnosis", 
            catalog=config.catalog,
            bronze_schema=config.bronze_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Bronze Table Schemas

# COMMAND ----------

# List of bronze tables to check
bronze_tables = [
    "brz_access_workspaces_latest",
    "brz_lakeflow_jobs", 
    "brz_lakeflow_pipelines",
    "brz_compute_clusters",
    "brz_billing_usage",
    "brz_job_run_timeline",
    "brz_job_task_run_timeline"
]

# Check each table's schema
for table_name in bronze_tables:
    full_table_name = f"{config.catalog}.{config.bronze_schema}.{table_name}"
    
    try:
        if spark.catalog.tableExists(full_table_name):
            logger.info(f"Checking schema for: {full_table_name}")
            
            # Get table schema
            schema = spark.table(full_table_name).schema
            columns = [field.name for field in schema.fields]
            
            logger.info(f"Columns in {table_name}: {columns}")
            
            # Check for specific column name issues
            if "create_time" in columns:
                logger.info(f"✅ {table_name} has 'create_time' column")
            elif "created_time" in columns:
                logger.warning(f"❌ {table_name} has 'created_time' instead of 'create_time'")
            
            if "period_start_time" in columns:
                logger.info(f"✅ {table_name} has 'period_start_time' column")
            elif "start_time" in columns:
                logger.warning(f"❌ {table_name} has 'start_time' instead of 'period_start_time'")
                
            if "period_end_time" in columns:
                logger.info(f"✅ {table_name} has 'period_end_time' column")
            elif "end_time" in columns:
                logger.warning(f"❌ {table_name} has 'end_time' instead of 'period_end_time'")
                
        else:
            logger.warning(f"Table does not exist: {full_table_name}")
            
    except Exception as e:
        logger.error(f"Error checking schema for {full_table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Silver Table Schemas

# COMMAND ----------

# List of silver tables to check
silver_tables = [
    "slv_workspace",
    "slv_entity_latest", 
    "slv_clusters",
    "slv_usage_txn",
    "slv_job_run_timeline",
    "slv_job_task_run_timeline"
]

# Check each table's schema
for table_name in silver_tables:
    full_table_name = f"{config.catalog}.{config.silver_schema}.{table_name}"
    
    try:
        if spark.catalog.tableExists(full_table_name):
            logger.info(f"Checking schema for: {full_table_name}")
            
            # Get table schema
            schema = spark.table(full_table_name).schema
            columns = [field.name for field in schema.fields]
            
            logger.info(f"Columns in {table_name}: {columns}")
            
            # Check for specific column name issues
            if "create_time" in columns:
                logger.info(f"✅ {table_name} has 'create_time' column")
            elif "created_time" in columns:
                logger.warning(f"❌ {table_name} has 'created_time' instead of 'create_time'")
                
        else:
            logger.info(f"Table does not exist: {full_table_name}")
            
    except Exception as e:
        logger.error(f"Error checking schema for {full_table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosis Complete

# COMMAND ----------

logger.info("Bronze schema diagnosis completed")
print("✅ Bronze schema diagnosis completed!")
print("🔍 Check the logs above to identify any column name mismatches.")
