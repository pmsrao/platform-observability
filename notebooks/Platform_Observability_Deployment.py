# Databricks notebook source

# MAGIC %md
# MAGIC # Platform Observability Deployment Notebook
# MAGIC 
# MAGIC This notebook provides step-by-step instructions and code to deploy the Platform Observability solution.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace with Unity Catalog enabled
# MAGIC - Appropriate permissions to create catalogs, schemas, and tables
# MAGIC - Access to the platform observability code repository
# MAGIC 
# MAGIC ## Overview
# MAGIC This notebook will:
# MAGIC 1. Verify configuration
# MAGIC 2. Create catalog and schemas
# MAGIC 3. Create Bronze layer tables
# MAGIC 4. Create Silver and Gold layer tables
# MAGIC 5. Deploy HWM jobs (configuration-based)
# MAGIC 6. Create and configure workflows
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Configuration
# MAGIC 
# MAGIC First, let's verify that the configuration is properly set up and accessible.

# COMMAND ----------

# MAGIC %run ../libs/config

# COMMAND ----------

# Get the current configuration
config = get_config()
print("Current Configuration:")
print(f"Catalog: {config.catalog}")
print(f"Bronze Schema: {config.bronze_schema}")
print(f"Silver Schema: {config.silver_schema}")
print(f"Gold Schema: {config.gold_schema}")
print(f"Environment: {config.ENV}")
print(f"Gold Complete Refresh: {config.gold_complete_refresh}")
print(f"Gold Processing Strategy: {config.gold_processing_strategy}")
print(f"Overlap Hours: {config.overlap_hours}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Catalog and Schemas
# MAGIC 
# MAGIC Create the initial catalog and schemas for the platform observability solution.

# COMMAND ----------

# MAGIC %run ../libs/sql_parameterizer

# COMMAND ----------

# Initialize the SQL parameterizer
sql_param = SQLParameterizer()

# Create catalog and schemas
print("Creating catalog and schemas...")
sql_param.create_catalog_and_schemas()
print("✅ Catalog and schemas created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Bronze Layer Tables
# MAGIC 
# MAGIC Create the Bronze layer tables with CDF enabled for incremental processing.

# COMMAND ----------

# Create Bronze layer tables
print("Creating Bronze layer tables...")
sql_param.create_bronze_tables()
print("✅ Bronze layer tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Silver and Gold Layer Tables
# MAGIC 
# MAGIC Create the Silver and Gold layer tables that will be populated by the DLT pipelines.

# COMMAND ----------

# Create Silver layer tables
print("Creating Silver layer tables...")
sql_param.create_silver_tables()
print("✅ Silver layer tables created successfully!")

# COMMAND ----------

# Create Gold layer tables
print("Creating Gold layer tables...")
sql_param.create_gold_tables()
print("✅ Gold layer tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Table Creation
# MAGIC 
# MAGIC Let's verify that all tables have been created successfully.

# COMMAND ----------

# List all tables in each schema
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Check Bronze schema
print("Bronze Schema Tables:")
bronze_tables = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.bronze_schema}")
bronze_tables.show(truncate=False)

# COMMAND ----------

# Check Silver schema
print("Silver Schema Tables:")
silver_tables = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.silver_schema}")
silver_tables.show(truncate=False)

# COMMAND ----------

# Check Gold schema
print("Gold Schema Tables:")
gold_tables = spark.sql(f"SHOW TABLES IN {config.catalog}.{config.gold_schema}")
gold_tables.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Deploy HWM Jobs
# MAGIC 
# MAGIC Now let's deploy the High-Water Mark (HWM) jobs for Bronze, Silver and Gold layers.
# MAGIC 
# MAGIC **Note**: HWM jobs are deployed as Databricks Jobs and are **configuration-based** - no job parameters required.
# MAGIC The following code shows the commands you would run from your local machine.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy Bronze HWM Ingest Job
# MAGIC 
# MAGIC From your local machine, run:
# MAGIC ```bash
# MAGIC # Navigate to the notebooks directory
# MAGIC cd notebooks
# MAGIC 
# MAGIC # Deploy the Bronze HWM ingest job
# MAGIC databricks jobs create --file bronze_hwm_ingest_job.py
# MAGIC ```
# MAGIC 
# MAGIC ### Deploy Silver HWM Build Job
# MAGIC 
# MAGIC From your local machine, run:
# MAGIC ```bash
# MAGIC # Navigate to the notebooks directory
# MAGIC cd notebooks
# MAGIC 
# MAGIC # Deploy the Silver HWM build job
# MAGIC databricks jobs create --file silver_hwm_build_job.py
# MAGIC ```
# MAGIC 
# MAGIC ### Deploy Gold HWM Build Job (SCD2-Aware)
# MAGIC 
# MAGIC From your local machine, run:
# MAGIC ```bash
# MAGIC # Navigate to the notebooks directory
# MAGIC cd notebooks
# MAGIC 
# MAGIC # Deploy the Gold HWM build job with SCD2 support
# MAGIC databricks jobs create --file gold_hwm_build_job.py
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create and Configure Workflows
# MAGIC 
# MAGIC The workflow configuration is defined in `jobs/daily_observability_workflow.json`.
# MAGIC 
# MAGIC **Note**: The workflow file is already configured for HWM jobs and doesn't require pipeline IDs.

# COMMAND ----------

# Display the workflow configuration
import json

with open('../jobs/daily_observability_workflow.json', 'r') as f:
    workflow = json.load(f)

print("Workflow Configuration:")
print(json.dumps(workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### HWM Jobs Configuration
# MAGIC 
# MAGIC The workflow file `jobs/daily_observability_workflow.json` is already configured for HWM jobs:
# MAGIC 
# MAGIC 1. **Bronze Ingest Job**: `bronze_hwm_ingest_job` (configuration-based)
# MAGIC 2. **Silver Build Job**: `silver_hwm_build_job` (configuration-based)
# MAGIC 3. **Gold Build Job**: `gold_hwm_build_job` (SCD2-aware, configuration-based)
# MAGIC 4. **Performance Optimization**: `performance_optimization_job`
# MAGIC 5. **Health Check**: `health_check_job`
# MAGIC 
# MAGIC **Key Benefits**:
# MAGIC - **No Job Parameters Required**: All jobs use configuration values
# MAGIC - **Standalone Execution**: Can run without job parameters
# MAGIC - **SCD2 Support**: Gold layer preserves historical entity versions
# MAGIC - **Environment-Aware**: Different behavior for dev/prod environments
# MAGIC 
# MAGIC ### Deploy the Workflow
# MAGIC 
# MAGIC From your local machine, run:
# MAGIC ```bash
# MAGIC # Navigate to the jobs directory
# MAGIC cd jobs
# MAGIC 
# MAGIC # Create the job
# MAGIC databricks jobs create --file daily_observability_workflow.json
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Validate Deployment
# MAGIC 
# MAGIC Let's run some validation queries to ensure everything is working correctly.

# COMMAND ----------

# Check if CDF is enabled on Bronze tables
print("Checking CDF status on Bronze tables...")
bronze_cdf_check = spark.sql(f"""
SELECT 
    table_name,
    TBLPROPERTIES['delta.enableChangeDataFeed'] as cdf_enabled
FROM {config.catalog}.{config.bronze_schema}.INFORMATION_SCHEMA.TABLES 
WHERE table_schema = '{config.bronze_schema}'
""")
bronze_cdf_check.show(truncate=False)

# COMMAND ----------

# Check if processing state table exists
print("Checking processing state table...")
try:
    processing_state_check = spark.sql(f"SELECT COUNT(*) as state_count FROM {config.catalog}.{config.silver_schema}._cdf_processing_offsets")
    processing_state_check.show()
    print("✅ Processing state table exists and is accessible")
except Exception as e:
    print(f"❌ Error accessing processing state table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Test Data Ingestion
# MAGIC 
# MAGIC Let's test the Bronze layer ingestion by running a sample operation.

# COMMAND ----------

# MAGIC %run ../notebooks/bronze_hwm_ingest_job

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ **Deployment Complete!**
# MAGIC 
# MAGIC The Platform Observability solution has been successfully deployed with:
# MAGIC - Catalog and schemas created
# MAGIC - Bronze, Silver, and Gold layer tables established
# MAGIC - HWM jobs ready for deployment (configuration-based)
# MAGIC - SCD2 support in Gold layer for historical tracking
# MAGIC - Workflow configuration prepared
# MAGIC 
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Deploy HWM Jobs**: Use the Databricks CLI commands shown above
# MAGIC 2. **Deploy Workflow**: Create the job using the Databricks CLI
# MAGIC 3. **Monitor Execution**: Use the Databricks UI to monitor job execution
# MAGIC 4. **Validate Data Flow**: Ensure data is flowing correctly through all layers
# MAGIC 5. **Validate SCD2**: Check that Gold dimension tables preserve historical versions
# MAGIC 6. **Configure Scheduling**: Set up appropriate schedules for each job
# MAGIC 
# MAGIC ## Troubleshooting
# MAGIC 
# MAGIC If you encounter issues:
# MAGIC - Check the Databricks logs for detailed error messages
# MAGIC - Verify permissions and access rights
# MAGIC - Ensure all dependencies are properly installed
# MAGIC - Review the configuration settings
# MAGIC 
# MAGIC For additional help, refer to the documentation in the `docs/` folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC *This notebook was generated from the Platform Observability Getting Started guide.*
# MAGIC *Last updated: January 2024*
