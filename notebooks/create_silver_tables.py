# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Tables
# MAGIC 
# MAGIC This notebook creates all silver layer tables using the DDL from silver_tables.sql.
# MAGIC Run this after dropping silver tables to recreate them with clean schemas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import sys
import os
from libs.logging import StructuredLogger
from libs.sql_parameterizer import SQLParameterizer
from config import Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("create_silver_tables")

logger.info("Starting Silver tables creation", 
            catalog=config.catalog,
            silver_schema=config.silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Tables

# COMMAND ----------

try:
    # Initialize SQL parameterizer
    sql_param = SQLParameterizer(spark)
    
    # Create silver tables
    print("🚀 Creating Silver layer tables...")
    sql_param.create_silver_tables()
    
    logger.info("Silver tables created successfully")
    print("✅ Silver tables created successfully!")
    
except Exception as e:
    logger.error(f"Error creating silver tables: {str(e)}")
    print(f"❌ Error creating silver tables: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables Created

# COMMAND ----------

# Check which tables were created
logger.info("Checking created tables in silver schema...")

try:
    tables = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.silver_schema}")
    created_tables = [row.tableName for row in tables.collect()]
    
    if created_tables:
        logger.info(f"Created tables in {config.catalog}.{config.silver_schema}: {created_tables}")
        print(f"📊 Created {len(created_tables)} tables:")
        
        # Group tables by type for better display
        type1_tables = ["slv_workspace", "slv_entity_latest", "slv_usage_txn", "slv_job_run_timeline", "slv_job_task_run_timeline"]
        type2_tables = ["slv_jobs_scd", "slv_pipelines_scd", "slv_price_scd", "slv_clusters"]
        
        print("  📋 Type 1 Tables (Current values only):")
        for table in sorted([t for t in created_tables if t in type1_tables]):
            print(f"    - {table}")
            
        print("  📚 Type 2 Tables (SCD2 - Historical tracking):")
        for table in sorted([t for t in created_tables if t in type2_tables]):
            print(f"    - {table}")
            
        other_tables = [t for t in created_tables if t not in type1_tables + type2_tables]
        if other_tables:
            print("  🔧 Other Tables:")
            for table in sorted(other_tables):
                print(f"    - {table}")
    else:
        logger.info(f"No tables found in {config.catalog}.{config.silver_schema}")
        print("⚠️  No tables found in silver schema")
        
except Exception as e:
    logger.error(f"Error checking created tables: {str(e)}")
    print(f"❌ Error checking created tables: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creation Complete

# COMMAND ----------

logger.info("Silver tables creation completed successfully")
print("🎉 Silver tables creation completed!")
print("🚀 You can now run the Silver Layer notebook to populate the tables with data.")
