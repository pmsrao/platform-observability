# Databricks notebook source

# MAGIC %md
# MAGIC # Platform Observability - Cloud-Agnostic Deployment
# MAGIC 
# MAGIC This notebook provides a simplified, step-by-step deployment of the Platform Observability solution.
# MAGIC 
# MAGIC ## 🚀 Features
# MAGIC - **Cloud-Agnostic**: Works on AWS, Azure, GCP, or any Databricks environment
# MAGIC - **Environment Configuration**: Simple environment variable based configuration
# MAGIC - **HWM Architecture**: High-Water Mark approach for all layers
# MAGIC - **SCD2 Support**: Historical tracking in Gold layer
# MAGIC - **Configuration-Based**: No job parameters required
# MAGIC 
# MAGIC ## 📋 Prerequisites
# MAGIC - Databricks workspace with Unity Catalog enabled
# MAGIC - Appropriate permissions to create catalogs, schemas, and tables
# MAGIC - Environment variables set for configuration
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Configuration

# COMMAND ----------

# Import cloud-agnostic libraries
import sys
import os
import importlib
from pathlib import Path

# Import from libs package (cloud-agnostic approach)
from libs.path_setup import setup_paths_and_import_config

# Setup paths and import Config
Config = setup_paths_and_import_config()

# COMMAND ----------

# Get configuration
config = Config.get_config()

print("🔧 Configuration Loaded:")
print(f"   Environment: {Config.ENV}")
print(f"   Catalog: {config.catalog}")
print(f"   Bronze Schema: {config.bronze_schema}")
print(f"   Silver Schema: {config.silver_schema}")
print(f"   Gold Schema: {config.gold_schema}")
print(f"   Databricks Host: {config.databricks_host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bootstrap Environment

# COMMAND ----------

# Force reload modules to ensure latest changes
if 'libs.sql_manager' in sys.modules:
    importlib.reload(sys.modules['libs.sql_manager'])
if 'libs.sql_parameterizer' in sys.modules:
    importlib.reload(sys.modules['libs.sql_parameterizer'])

from libs.sql_parameterizer import SQLParameterizer
from libs.sql_manager import SQLManager

# Create SQLManager with explicit path for Databricks environment
explicit_sql_path = "/Workspace/Users/podilapalls@gmail.com/platform-observability/sql"
sql_manager = SQLManager(sql_directory=explicit_sql_path)

print("🔧 SQL Environment Setup:")
print(f"   SQL directory: {sql_manager.sql_directory}")
print(f"   SQL directory exists: {sql_manager.sql_directory.exists()}")
print(f"   Available SQL operations: {len(sql_manager.get_available_operations())}")

# Initialize SQL parameterizer
sql_param = SQLParameterizer(sql_manager_instance=sql_manager)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Create Catalog and Schemas

# COMMAND ----------

# Bootstrap catalog and schemas
sql_param.bootstrap_catalog_schemas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Create Processing State Tables

# COMMAND ----------

# Create processing state tables for HWM tracking
sql_param.create_processing_state()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Create Bronze Tables

# COMMAND ----------

# Create bronze layer tables with CDF enabled
sql_param.bootstrap_bronze_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Create Silver Tables

# COMMAND ----------

# Create silver layer tables
sql_param.create_silver_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Create Gold Tables

# COMMAND ----------

# Create gold layer tables
sql_param.create_gold_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Apply Performance Optimizations

# COMMAND ----------

# Apply performance optimizations
sql_param.apply_performance_optimizations()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 Create Gold Views

# COMMAND ----------

# Create gold layer views
sql_param.create_gold_views()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Environment Setup

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Verify catalog and schemas
print("📊 Environment Verification:")
print(f"   Catalog: {config.catalog}")
print(f"   Schemas: {config.bronze_schema}, {config.silver_schema}, {config.gold_schema}")

# Check Bronze tables
bronze_count = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.bronze_schema}").count()
print(f"   Bronze Tables: {bronze_count}")

# Check Silver tables  
silver_count = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.silver_schema}").count()
print(f"   Silver Tables: {silver_count}")

# Check Gold tables
gold_count = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.gold_schema}").count()
print(f"   Gold Tables: {gold_count}")

# Verify CDF is enabled
try:
    cdf_tables = spark.sql(f"""
        SELECT table_name
        FROM {config.catalog}.{config.bronze_schema}.INFORMATION_SCHEMA.TABLES 
        WHERE TBLPROPERTIES['delta.enableChangeDataFeed'] = 'true'
    """).count()
    print(f"   CDF-Enabled Tables: {cdf_tables}")
except:
    print(f"   CDF verification skipped (may not be supported)")

print("\n✅ Environment verification complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Deploy HWM Jobs (Configuration-Based)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Bronze HWM Ingest Job
# MAGIC 
# MAGIC Deploy the Bronze HWM Ingest Job:
# MAGIC 
# MAGIC 1. **Notebook**: `notebooks/bronze_hwm_ingest_job.py`
# MAGIC 2. **Schedule**: Daily at 2:00 AM
# MAGIC 3. **Purpose**: Ingest data from Databricks APIs with HWM tracking

# COMMAND ----------

# Display sample job configuration
bronze_job_config = {
    "name": "platform-observability-bronze-ingest",
    "notebook_path": "/Workspace/Repos/platform-observability/notebooks/bronze_hwm_ingest_job",
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",
        "timezone_id": "Asia/Kolkata"
    },
    "max_concurrent_runs": 1,
    "timeout_seconds": 3600,
    "environment": {
        "ENVIRONMENT": Config.ENV
    }
}

print("📋 Bronze Job Configuration:")
for key, value in bronze_job_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Silver HWM Build Job

# COMMAND ----------

# Display sample job configuration
silver_job_config = {
    "name": "platform-observability-silver-build",
    "notebook_path": "/Workspace/Repos/platform-observability/notebooks/silver_hwm_build_job",
    "schedule": {
        "quartz_cron_expression": "0 0 3 * * ?",
        "timezone_id": "Asia/Kolkata"
    },
    "max_concurrent_runs": 1,
    "timeout_seconds": 3600,
    "environment": {
        "ENVIRONMENT": Config.ENV
    }
}

print("📋 Silver Job Configuration:")
for key, value in silver_job_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Gold HWM Build Job

# COMMAND ----------

# Display sample job configuration
gold_job_config = {
    "name": "platform-observability-gold-build",
    "notebook_path": "/Workspace/Repos/platform-observability/notebooks/gold_hwm_build_job",
    "schedule": {
        "quartz_cron_expression": "0 0 4 * * ?",
        "timezone_id": "Asia/Kolkata"
    },
    "max_concurrent_runs": 1,
    "timeout_seconds": 3600,
    "environment": {
        "ENVIRONMENT": Config.ENV
    }
}

print("📋 Gold Job Configuration:")
for key, value in gold_job_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Health Check Job

# COMMAND ----------

# Display sample job configuration
health_job_config = {
    "name": "platform-observability-health-check",
    "notebook_path": "/Workspace/Repos/platform-observability/notebooks/health_check_job",
    "schedule": {
        "quartz_cron_expression": "0 0 */6 * * ?",
        "timezone_id": "Asia/Kolkata"
    },
    "max_concurrent_runs": 1,
    "timeout_seconds": 1800,
    "environment": {
        "ENVIRONMENT": Config.ENV
    }
}

print("📋 Health Check Job Configuration:")
for key, value in health_job_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Deployment Complete!

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ What's Been Deployed
# MAGIC 
# MAGIC | Component | Status | Description |
# MAGIC |-----------|--------|-------------|
# MAGIC | **Environment** | `{Config.ENV}` | Current environment |
# MAGIC | **Catalog** | `{config.catalog}` | Unity Catalog name |
# MAGIC | **Bronze Schema** | `{config.bronze_schema}` | Raw data layer |
# MAGIC | **Silver Schema** | `{config.silver_schema}` | Cleaned data layer |
# MAGIC | **Gold Schema** | `{config.gold_schema}` | Business-ready data layer |
# MAGIC | **Databricks Host** | `{config.databricks_host}` | Workspace URL |
# MAGIC 
# MAGIC ### 🚀 Next Steps
# MAGIC 
# MAGIC 1. **Configure Jobs**: Deploy the job configurations shown above
# MAGIC 2. **Set Environment Variables**: Ensure all required environment variables are set
# MAGIC 3. **Monitor**: Use the health check job to monitor system health
# MAGIC 4. **Customize**: Adjust configurations based on your specific needs
# MAGIC 
# MAGIC ### 📚 Documentation
# MAGIC 
# MAGIC - **Deployment Guide**: `docs/11-deployment.md`
# MAGIC - **Recent Changes**: `docs/10-recent-changes-summary.md`
# MAGIC - **Data Dictionary**: `docs/04-data-dictionary.md`
# MAGIC - **Use Cases**: `docs/07-insights-and-use-cases.md`

# COMMAND ----------
