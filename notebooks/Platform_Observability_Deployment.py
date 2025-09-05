# Databricks notebook source

# MAGIC %md
# MAGIC # Platform Observability - Cloud-Agnostic Deployment
# MAGIC 
# MAGIC This notebook provides a simplified, step-by-step deployment of the Platform Observability solution with cloud-agnostic secrets management.
# MAGIC 
# MAGIC ## 🚀 Features
# MAGIC - **Cloud-Agnostic**: Works on AWS, Azure, GCP, or any Databricks environment
# MAGIC - **Secrets Management**: Automatic detection of cloud secrets providers
# MAGIC - **HWM Architecture**: High-Water Mark approach for all layers
# MAGIC - **SCD2 Support**: Historical tracking in Gold layer
# MAGIC - **Configuration-Based**: No job parameters required
# MAGIC 
# MAGIC ## 📋 Prerequisites
# MAGIC - Databricks workspace with Unity Catalog enabled
# MAGIC - Appropriate permissions to create catalogs, schemas, and tables
# MAGIC - Cloud credentials (optional - falls back to environment variables)
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Configuration & Secrets

# COMMAND ----------

# Import cloud-agnostic libraries
import sys
import os

# Add libs to path (cloud-agnostic approach)
current_dir = os.path.dirname(os.path.abspath(__file__))
libs_dir = os.path.join(os.path.dirname(current_dir), 'libs')
if libs_dir not in sys.path:
    sys.path.append(libs_dir)

# For Databricks workspace
workspace_libs_path = '/Workspace/Repos/platform-observability/libs'
if workspace_libs_path not in sys.path:
    sys.path.append(workspace_libs_path)

# Import configuration and secrets management
from config import Config
from secrets_manager import secrets_manager

# COMMAND ----------

# Get configuration (automatically uses secrets manager)
config = Config.get_config()

print("🔧 Configuration Loaded:")
print(f"   Environment: {config.ENV}")
print(f"   Catalog: {config.catalog}")
print(f"   Bronze Schema: {config.bronze_schema}")
print(f"   Silver Schema: {config.silver_schema}")
print(f"   Gold Schema: {config.gold_schema}")
print(f"   Databricks Host: {config.databricks_host}")

# Test secrets manager
print(f"\n🔐 Secrets Provider: {secrets_manager._active_provider.__class__.__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bootstrap Environment

# COMMAND ----------

# Import SQL parameterizer
from sql_parameterizer import SQLParameterizer

# Initialize and bootstrap entire environment
print("🏗️ Bootstrapping Platform Observability Environment...")
sql_param = SQLParameterizer()

# Full bootstrap - creates everything needed
sql_param.full_bootstrap()

print("✅ Environment bootstrap complete!")
print("   - Catalog and schemas created")
print("   - Bronze tables with CDF enabled")
print("   - Silver and Gold tables created")
print("   - Processing state tables initialized")
print("   - Performance optimizations applied")

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
# MAGIC ### 🚀 HWM Jobs Ready for Deployment
# MAGIC 
# MAGIC The following HWM jobs are **configuration-based** and ready for deployment:
# MAGIC 
# MAGIC #### **1. Bronze HWM Ingest Job**
# MAGIC - **File**: `bronze_hwm_ingest_job.py`
# MAGIC - **Purpose**: Ingest data from system tables to Bronze layer
# MAGIC - **Features**: CDF-enabled, incremental processing, data quality validation
# MAGIC 
# MAGIC #### **2. Silver HWM Build Job**
# MAGIC - **File**: `silver_hwm_build_job.py`  
# MAGIC - **Purpose**: Build Silver layer from Bronze using CDF
# MAGIC - **Features**: SCD2 support, tag processing, business logic
# MAGIC 
# MAGIC #### **3. Gold HWM Build Job**
# MAGIC - **File**: `gold_hwm_build_job.py`
# MAGIC - **Purpose**: Build Gold layer with SCD2-aware dimensions
# MAGIC - **Features**: Historical tracking, temporal accuracy, performance optimization
# MAGIC 
# MAGIC #### **4. Performance Optimization Job**
# MAGIC - **File**: `performance_optimization_job.py`
# MAGIC - **Purpose**: Optimize table performance and statistics
# MAGIC 
# MAGIC #### **5. Health Check Job**
# MAGIC - **File**: `health_check_job.py`
# MAGIC - **Purpose**: Monitor system health and data quality

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📋 Deployment Commands
# MAGIC 
# MAGIC **Deploy using Databricks CLI:**
# MAGIC 
# MAGIC ```bash
# MAGIC # Configure Databricks CLI (if not already done)
# MAGIC databricks configure --token
# MAGIC 
# MAGIC # Deploy notebooks to workspace
# MAGIC databricks workspace import_dir notebooks /Workspace/Repos/platform-observability/notebooks
# MAGIC 
# MAGIC # Deploy libraries to DBFS
# MAGIC databricks fs cp -r libs dbfs:/FileStore/platform-observability/libs
# MAGIC 
# MAGIC # Create jobs (example for Bronze job)
# MAGIC databricks jobs create --json-file bronze_job_config.json
# MAGIC ```
# MAGIC 
# MAGIC **Or deploy via Databricks UI:**
# MAGIC 1. Upload notebooks to workspace
# MAGIC 2. Upload libraries to DBFS  
# MAGIC 3. Create jobs with cloud-agnostic configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Configure Cloud-Agnostic Jobs

# COMMAND ----------

# Display sample job configuration with cloud-agnostic secrets
job_config = {
    "name": "Platform Observability - Bronze Ingest",
    "notebook_task": {
        "notebook_path": "/Workspace/Repos/platform-observability/notebooks/bronze_hwm_ingest_job"
    },
    "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
    },
    "environment": {
        "ENVIRONMENT": "prod",
        "DATABRICKS_HOST": "{{secrets/your-scope/databricks-host}}",
        "DATABRICKS_TOKEN": "{{secrets/your-scope/databricks-token}}"
    }
}

print("🔧 Sample Job Configuration (Cloud-Agnostic):")
import json
print(json.dumps(job_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Deploy Workflow

# COMMAND ----------

# Display workflow configuration
import json

try:
    with open('../jobs/daily_observability_workflow.json', 'r') as f:
        workflow = json.load(f)
    
    print("🔄 Daily Observability Workflow:")
    print(json.dumps(workflow, indent=2))
    
except FileNotFoundError:
    print("📝 Workflow file not found. Create jobs manually or use the sample configuration above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test Deployment

# COMMAND ----------

# Test Bronze ingestion (optional - uncomment to run)
# print("🧪 Testing Bronze ingestion...")
# %run bronze_hwm_ingest_job

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Validate Data Quality

# COMMAND ----------

# Check processing state
try:
    processing_state = spark.sql(f"""
        SELECT 
            table_name,
            last_processed_timestamp,
            processing_status
        FROM {config.catalog}.{config.silver_schema}._cdf_processing_offsets
        ORDER BY last_processed_timestamp DESC
        LIMIT 10
    """)
    
    print("📊 Processing State:")
    processing_state.show(truncate=False)
    
except Exception as e:
    print(f"ℹ️ Processing state table not yet populated: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Deployment Complete!

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎉 Platform Observability Successfully Deployed
# MAGIC 
# MAGIC **What's Ready:**
# MAGIC - ✅ **Cloud-Agnostic Environment**: Works on any cloud provider
# MAGIC - ✅ **Secrets Management**: Automatic provider detection
# MAGIC - ✅ **HWM Architecture**: All layers use High-Water Mark approach
# MAGIC - ✅ **SCD2 Support**: Historical tracking in Gold layer
# MAGIC - ✅ **Configuration-Based**: No job parameters required
# MAGIC - ✅ **Performance Optimized**: Z-ORDER and statistics applied
# MAGIC 
# MAGIC ### 🚀 Next Steps
# MAGIC 
# MAGIC 1. **Deploy HWM Jobs**: Use the configuration examples above
# MAGIC 2. **Deploy Workflow**: Create the daily observability workflow
# MAGIC 3. **Configure Scheduling**: Set up appropriate schedules
# MAGIC 4. **Monitor Execution**: Use Databricks UI for monitoring
# MAGIC 5. **Validate Data Flow**: Ensure data flows through all layers
# MAGIC 6. **Test SCD2**: Verify historical tracking works correctly
# MAGIC 
# MAGIC ### 🔧 Configuration Summary
# MAGIC 
# MAGIC | Component | Value |
# MAGIC |-----------|-------|
# MAGIC | **Environment** | `{config.ENV}` |
# MAGIC | **Catalog** | `{config.catalog}` |
# MAGIC | **Bronze Schema** | `{config.bronze_schema}` |
# MAGIC | **Silver Schema** | `{config.silver_schema}` |
# MAGIC | **Gold Schema** | `{config.gold_schema}` |
# MAGIC | **Secrets Provider** | `{secrets_manager._active_provider.__class__.__name__}` |
# MAGIC | **Databricks Host** | `{config.databricks_host}` |
# MAGIC 
# MAGIC ### 📚 Documentation
# MAGIC 
# MAGIC - **Deployment Guide**: `docs/05-deployment.md`
# MAGIC - **Recent Changes**: `docs/13-recent-changes-summary.md`
# MAGIC - **SCD2 Guide**: `docs/14-scd2-implementation-guide.md`
# MAGIC - **Data Dictionary**: `docs/09-data-dictionary.md`
# MAGIC 
# MAGIC ### 🆘 Support
# MAGIC 
# MAGIC For issues or questions:
# MAGIC 1. Check the troubleshooting section in deployment guide
# MAGIC 2. Review logs for detailed error messages
# MAGIC 3. Verify cloud credentials and permissions
# MAGIC 4. Test with environment variables as fallback
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC *Platform Observability - Cloud-Agnostic Deployment*  
# MAGIC *Last updated: January 2024*

# COMMAND ----------
