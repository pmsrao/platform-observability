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

# Add libs to path (cloud-agnostic approach)
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    libs_dir = os.path.join(os.path.dirname(current_dir), 'libs')
    if libs_dir not in sys.path:
        sys.path.append(libs_dir)
except NameError:
    # __file__ is not available in Databricks notebooks
    pass

# For Databricks workspace - try multiple possible paths
workspace_paths = [
    '/Workspace/Repos/platform-observability/libs',
    '/Workspace/Users/podilapalls@gmail.com/platform-observability/libs'
]

for workspace_libs_path in workspace_paths:
    if workspace_libs_path not in sys.path:
        sys.path.append(workspace_libs_path)

# Import configuration
from config import Config

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

# Import SQL parameterizer for bootstrap
import os
from pathlib import Path

# Direct import - the modules should be up to date
from libs.sql_parameterizer import SQLParameterizer
from libs.sql_manager import SQLManager

# Create a new SQLManager instance with explicit path based on current working directory
# From the debug output, we know we're in /Workspace/Users/podilapalls@gmail.com/platform-observability/notebooks
# So the sql directory should be at /Workspace/Users/podilapalls@gmail.com/platform-observability/sql
explicit_sql_path = "/Workspace/Users/podilapalls@gmail.com/platform-observability/sql"

# Force reload the SQLManager class to get the latest version
import importlib
import sys
if 'libs.sql_manager' in sys.modules:
    importlib.reload(sys.modules['libs.sql_manager'])
from libs.sql_manager import SQLManager

sql_manager = SQLManager(sql_directory=explicit_sql_path)

# Test if the new method exists
print("🔍 Testing SQLManager Methods:")
print(f"   Has parameterize_sql_statements: {hasattr(sql_manager, 'parameterize_sql_statements')}")
if hasattr(sql_manager, 'parameterize_sql_statements'):
    print("   ✅ parameterize_sql_statements method is available")
else:
    print("   ❌ parameterize_sql_statements method is missing")

# Debug path configuration
print("🔍 Debugging Path Configuration:")
print(f"   Current working directory: {os.getcwd()}")
print(f"   SQL directory: {sql_manager.sql_directory}")
print(f"   SQL directory exists: {sql_manager.sql_directory.exists()}")
print(f"   Available SQL files: {sql_manager.get_available_operations()}")
print(f"   Config catalog: {sql_manager._config.catalog}")
print(f"   Config bronze schema: {sql_manager._config.bronze_schema}")
print(f"   Config silver schema: {sql_manager._config.silver_schema}")
print(f"   Config gold schema: {sql_manager._config.gold_schema}")

# Test if we can find the specific file that's failing
test_operation = "config/bootstrap_catalog_schemas"
try:
    test_path = sql_manager.get_sql_file_path(test_operation)
    print(f"✅ Found SQL file: {test_path}")
except FileNotFoundError as e:
    print(f"❌ SQL file not found: {e}")
    print("🔍 Trying alternative approaches...")
    
    # Try to find the file manually
    from pathlib import Path
    possible_locations = [
        Path.cwd() / "sql" / "config" / "bootstrap_catalog_schemas.sql",
        Path.cwd().parent / "sql" / "config" / "bootstrap_catalog_schemas.sql",
        Path("/Workspace/Repos/platform-observability/sql/config/bootstrap_catalog_schemas.sql"),
        Path("/Workspace/Users/podilapalls@gmail.com/platform-observability/sql/config/bootstrap_catalog_schemas.sql")
    ]
    
    # Try to add __file__ based path if available
    try:
        possible_locations.append(Path(__file__).parent.parent / "sql" / "config" / "bootstrap_catalog_schemas.sql")
    except NameError:
        pass
    
    for location in possible_locations:
        if location.exists():
            print(f"✅ Found file at: {location}")
            break
    else:
        print("❌ File not found in any expected location")

# COMMAND ----------

# Initialize SQL parameterizer with the new SQLManager instance
sql_param = SQLParameterizer(sql_manager_instance=sql_manager)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Create Catalog and Schemas

# COMMAND ----------

# Debug: Let's see what SQL statements are being generated
print("🔍 Debugging SQL Generation:")

# First, let's see the raw SQL content
print("Raw SQL content:")
try:
    raw_sql = sql_manager.parameterize_sql("config/bootstrap_catalog_schemas")
    print(f"Raw SQL length: {len(raw_sql)}")
    print(f"Raw SQL content: {repr(raw_sql)}")
    print("---")
except Exception as e:
    print(f"Error getting raw SQL: {e}")

# Now let's see the statements
print("Generated statements:")
try:
    statements = sql_manager.parameterize_sql_statements("config/bootstrap_catalog_schemas")
    print(f"Number of statements: {len(statements)}")
    for i, statement in enumerate(statements):
        print(f"Statement {i+1}: {repr(statement)}")
        print(f"Length: {len(statement)}")
        print("---")
except Exception as e:
    print(f"Error generating statements: {e}")

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
cdf_tables = spark.sql(f"""
    SELECT table_name
    FROM {config.catalog}.{config.bronze_schema}.INFORMATION_SCHEMA.TABLES 
    WHERE TBLPROPERTIES['delta.enableChangeDataFeed'] = 'true'
""").count()
print(f"   CDF-Enabled Tables: {cdf_tables}")

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
        "ENVIRONMENT": "dev"  # or "prod"
    }
}

print("📋 Bronze Job Configuration:")
for key, value in bronze_job_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Silver HWM Build Job

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy the Silver HWM Build Job:
# MAGIC 
# MAGIC 1. **Notebook**: `notebooks/silver_hwm_build_job.py`
# MAGIC 2. **Schedule**: Daily at 3:00 AM (after bronze)
# MAGIC 3. **Purpose**: Transform bronze data to silver with HWM tracking

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
        "ENVIRONMENT": "dev"  # or "prod"
    }
}

print("📋 Silver Job Configuration:")
for key, value in silver_job_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Gold HWM Build Job

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy the Gold HWM Build Job:
# MAGIC 
# MAGIC 1. **Notebook**: `notebooks/gold_hwm_build_job.py`
# MAGIC 2. **Schedule**: Daily at 4:00 AM (after silver)
# MAGIC 3. **Purpose**: Build gold layer with SCD2 and HWM tracking

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
        "ENVIRONMENT": "dev"  # or "prod"
    }
}

print("📋 Gold Job Configuration:")
for key, value in gold_job_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Health Check Job

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy the Health Check Job:
# MAGIC 
# MAGIC 1. **Notebook**: `notebooks/health_check_job.py`
# MAGIC 2. **Schedule**: Every 6 hours
# MAGIC 3. **Purpose**: Monitor system health and data quality

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
        "ENVIRONMENT": "dev"  # or "prod"
    }
}

print("📋 Health Check Job Configuration:")
for key, value in health_job_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Deploy Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Daily Observability Workflow

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy the Daily Observability Workflow:
# MAGIC 
# MAGIC 1. **Workflow File**: `jobs/daily_observability_workflow.json`
# MAGIC 2. **Schedule**: Daily at 1:00 AM
# MAGIC 3. **Purpose**: Orchestrate the complete daily pipeline

# COMMAND ----------

# Display workflow configuration
workflow_config = {
    "name": "platform-observability-daily-workflow",
    "schedule": {
        "quartz_cron_expression": "0 0 1 * * ?",
        "timezone_id": "Asia/Kolkata"
    },
    "tasks": [
        {
            "task_key": "bronze_ingest",
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/platform-observability/notebooks/bronze_hwm_ingest_job"
            }
        },
        {
            "task_key": "silver_build",
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/platform-observability/notebooks/silver_hwm_build_job"
            },
            "depends_on": [{"task_key": "bronze_ingest"}]
        },
        {
            "task_key": "gold_build",
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/platform-observability/notebooks/gold_hwm_build_job"
            },
            "depends_on": [{"task_key": "silver_build"}]
        }
    ]
}

print("📋 Workflow Configuration:")
print(f"   Name: {workflow_config['name']}")
print(f"   Schedule: {workflow_config['schedule']['quartz_cron_expression']}")
print(f"   Tasks: {len(workflow_config['tasks'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Testing and Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Test Data Ingestion

# COMMAND ----------

# Test bronze data ingestion
print("🧪 Testing Bronze Data Ingestion...")

try:
    # Check if bronze tables exist and have data
    bronze_tables = ["billing_usage", "compute_clusters", "job_runs"]
    for table in bronze_tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {config.get_table_name('bronze', table)}").collect()[0]['cnt']
            print(f"✅ {table}: {count} records")
        except Exception as e:
            print(f"❌ {table}: {str(e)}")
except Exception as e:
    print(f"❌ Bronze ingestion test failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Test Data Transformation

# COMMAND ----------

# Test silver data transformation
print("🧪 Testing Silver Data Transformation...")

try:
    # Check if silver tables exist and have data
    silver_tables = ["dim_compute_clusters", "fact_job_runs", "fact_billing_usage"]
    for table in silver_tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {config.get_table_name('silver', table)}").collect()[0]['cnt']
            print(f"✅ {table}: {count} records")
        except Exception as e:
            print(f"❌ {table}: {str(e)}")
except Exception as e:
    print(f"❌ Silver transformation test failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Test Gold Layer

# COMMAND ----------

# Test gold layer
print("🧪 Testing Gold Layer...")

try:
    # Check if gold tables exist and have data
    gold_tables = ["dim_compute_clusters", "fact_job_runs", "fact_billing_usage"]
    for table in gold_tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {config.get_table_name('gold', table)}").collect()[0]['cnt']
            print(f"✅ {table}: {count} records")
        except Exception as e:
            print(f"❌ {table}: {str(e)}")
except Exception as e:
    print(f"❌ Gold layer test failed: {str(e)}")

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
# MAGIC ### 📚 Documentation
# MAGIC 
# MAGIC - **Deployment Guide**: `docs/11-deployment.md`
# MAGIC - **Recent Changes**: `docs/10-recent-changes-summary.md`
# MAGIC - **Data Dictionary**: `docs/04-data-dictionary.md`
# MAGIC - **Use Cases**: `docs/07-insights-and-use-cases.md`
# MAGIC 
# MAGIC ### 🚀 Next Steps
# MAGIC 
# MAGIC 1. **Configure Jobs**: Deploy the job configurations shown above
# MAGIC 2. **Set Environment Variables**: Ensure all required environment variables are set
# MAGIC 3. **Monitor**: Use the health check job to monitor system health
# MAGIC 4. **Customize**: Adjust configurations based on your specific needs
# MAGIC 
# MAGIC ### 🔧 Configuration Summary
# MAGIC 
# MAGIC | Component | Value |
# MAGIC |-----------|-------|
# MAGIC | **Environment** | `{Config.ENV}` |
# MAGIC | **Catalog** | `{config.catalog}` |
# MAGIC | **Bronze Schema** | `{config.bronze_schema}` |
# MAGIC | **Silver Schema** | `{config.silver_schema}` |
# MAGIC | **Gold Schema** | `{config.gold_schema}` |
# MAGIC | **Databricks Host** | `{config.databricks_host}` |
# MAGIC 
# MAGIC ### 📚 Documentation
# MAGIC 
# MAGIC - **Deployment Guide**: `docs/11-deployment.md`
# MAGIC - **Recent Changes**: `docs/10-recent-changes-summary.md`
# MAGIC - **Data Dictionary**: `docs/04-data-dictionary.md`
# MAGIC - **Use Cases**: `docs/07-insights-and-use-cases.md`

# COMMAND ----------
