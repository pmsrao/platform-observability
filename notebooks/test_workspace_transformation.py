# Databricks notebook source
# MAGIC %md
# MAGIC # Test Workspace Transformation
# MAGIC 
# MAGIC This notebook tests the workspace transformation to identify the column name issue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from libs.logging import StructuredLogger
from config import Config
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("test_workspace_transformation")

logger.info("Starting workspace transformation test", 
            catalog=config.catalog,
            bronze_schema=config.bronze_schema,
            silver_schema=config.silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Bronze Data Reading

# COMMAND ----------

# Read bronze data directly
bronze_table = f"{config.catalog}.{config.bronze_schema}.brz_access_workspaces_latest"
logger.info(f"Reading data from: {bronze_table}")

# Read a small sample
df = spark.table(bronze_table).limit(5)
logger.info(f"Bronze data columns: {df.columns}")
logger.info(f"Bronze data schema: {df.dtypes}")

# Show the data
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Transformation

# COMMAND ----------

# Apply the same transformation as in the silver job
transformed_df = df.select(
    df.account_id,
    df.workspace_id,
    df.workspace_name,
    df.workspace_url,
    df.create_time,  # This should be create_time
    df.status,
    F.current_timestamp().alias("_loaded_at")
).distinct()

logger.info(f"Transformed data columns: {transformed_df.columns}")
logger.info(f"Transformed data schema: {transformed_df.dtypes}")

# Show the transformed data
transformed_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Silver Table Schema

# COMMAND ----------

# Check existing silver table schema
silver_table = f"{config.catalog}.{config.silver_schema}.slv_workspace"
logger.info(f"Checking silver table: {silver_table}")

if spark.catalog.tableExists(silver_table):
    existing_df = spark.table(silver_table).limit(1)
    logger.info(f"Existing silver table columns: {existing_df.columns}")
    logger.info(f"Existing silver table schema: {existing_df.dtypes}")
    
    # Show existing data
    existing_df.show(1, truncate=False)
else:
    logger.info("Silver table does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Write Operation

# COMMAND ----------

# Test the write operation without actually writing
logger.info("Testing write operation...")

try:
    # This will show us the exact error
    transformed_df.write.mode("append").option("mergeSchema", "true").saveAsTable(silver_table)
    logger.info("✅ Write operation successful!")
except Exception as e:
    logger.error(f"❌ Write operation failed: {str(e)}")
    # Print the full error details
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Complete

# COMMAND ----------

logger.info("Workspace transformation test completed")
print("✅ Workspace transformation test completed!")
print("🔍 Check the logs above to identify the exact issue.")
