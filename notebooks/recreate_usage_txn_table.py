# Databricks notebook source
# MAGIC %md
# MAGIC # Recreate slv_usage_txn Table with Updated Schema
# MAGIC 
# MAGIC This notebook recreates the slv_usage_txn table with the updated usage_metadata struct schema to match the bronze table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import sys
import os
sys.path.append('/Workspace/Users/podilapalls@gmail.com/platform-observability')

from libs.logging import StructuredLogger
from config import Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Initialize configuration
config = Config()

# Initialize logger
logger = StructuredLogger(
    application="recreate_usage_txn_table",
    environment=config.environment,
    catalog=config.catalog,
    bronze_schema=config.bronze_schema,
    silver_schema=config.silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Existing Table

# COMMAND ----------

table_name = f"{config.catalog}.{config.silver_schema}.slv_usage_txn"

try:
    if spark.catalog.tableExists(table_name):
        print(f"🗑️ Dropping existing table: {table_name}")
        spark.sql(f"DROP TABLE {table_name}")
        logger.info(f"Dropped existing table: {table_name}")
        print(f"✅ Successfully dropped {table_name}")
    else:
        print(f"ℹ️ Table {table_name} does not exist")
        logger.info(f"Table {table_name} does not exist")
except Exception as e:
    logger.error(f"Error dropping table {table_name}: {str(e)}")
    print(f"❌ Error dropping {table_name}: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Table with Updated Schema

# COMMAND ----------

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {config.catalog}.{config.silver_schema}.slv_usage_txn (
    record_id STRING,                    -- Added unique identifier from bronze
    account_id STRING,
    workspace_id STRING,                 -- Changed from BIGINT to STRING
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP,
    usage_date DATE,
    usage_quantity DECIMAL(38,18),
    entity_type STRING,
    entity_id STRING,
    job_run_id STRING,                   -- Renamed from run_id for clarity
    date_sk INT,
    list_cost_usd DECIMAL(38,18),
    duration_hours DECIMAL(38,18),
    billing_origin_product STRING,
    custom_tags MAP<STRING, STRING>,     -- Renamed from tags
    usage_metadata STRUCT<
        cluster_id:STRING,
        job_id:STRING,
        warehouse_id:STRING,
        instance_pool_id:STRING,
        node_type:STRING,
        job_run_id:STRING,
        notebook_id:STRING,
        dlt_pipeline_id:STRING,
        endpoint_name:STRING,
        endpoint_id:STRING,
        dlt_update_id:STRING,
        dlt_maintenance_id:STRING,
        run_name:STRING,
        job_name:STRING,
        notebook_path:STRING,
        central_clean_room_id:STRING,
        source_region:STRING,
        destination_region:STRING,
        app_id:STRING,
        app_name:STRING,
        metastore_id:STRING,
        private_endpoint_name:STRING,
        storage_api_type:STRING,
        budget_policy_id:STRING,
        ai_runtime_pool_id:STRING,
        ai_runtime_workload_id:STRING,
        uc_table_catalog:STRING,
        uc_table_schema:STRING,
        uc_table_name:STRING,
        database_instance_id:STRING,
        sharing_materialization_id:STRING,
        schema_id:STRING
    >,
    identity_metadata STRUCT<
        run_as:STRING,
        owned_by:STRING,
        created_by:STRING
    >,
    record_type STRING,
    ingestion_date DATE,
    product_features STRUCT<
        jobs_tier:STRING,
        sql_tier:STRING,
        dlt_tier:STRING,
        is_serverless:BOOLEAN,
        is_photon:BOOLEAN,
        serving_type:STRING,
        offering_type:STRING,
        networking:STRUCT<connectivity_type:STRING>
    >,
    usage_type STRING,
    -- NEW: Original business tags (as extracted from source)
    line_of_business_raw STRING,                -- Original value from custom_tags
    department_raw STRING,                      -- Original value from custom_tags
    cost_center_raw STRING,                     -- Original value from custom_tags
    environment_raw STRING,                     -- Original value from custom_tags
    use_case_raw STRING,                        -- Original value from custom_tags
    pipeline_name_raw STRING,                   -- Original value from custom_tags
    cluster_identifier_raw STRING,              -- Original value from custom_tags
    workflow_level_raw STRING,                  -- Original value from custom_tags
    parent_workflow_name_raw STRING,            -- Original value from custom_tags
    -- NEW: Normalized business tags (with defaults applied)
    line_of_business STRING,                    -- Normalized with 'Unknown' default
    department STRING,                          -- Normalized with 'unknown' default
    cost_center STRING,                         -- Normalized with 'unallocated' default
    environment STRING,                         -- Normalized with 'dev' default
    use_case STRING,                            -- Normalized with 'Unknown' default
    pipeline_name STRING,                       -- Normalized with 'system' default
    cluster_identifier STRING,                  -- Normalized with 'Unknown' default
    workflow_level STRING,                      -- Normalized with 'STANDALONE' default
    parent_workflow_name STRING,                -- Normalized with 'None' default
    -- NEW: Inherited cluster tags
    inherited_line_of_business STRING,
    inherited_cost_center STRING,
    inherited_workflow_level STRING,
    inherited_parent_workflow STRING,
    -- Additional runtime/compute columns
    databricks_runtime_raw STRING,
    databricks_runtime STRING,
    compute_node_type_raw STRING,
    compute_node_type STRING,
    cluster_worker_count_raw STRING,
    cluster_worker_count STRING,
    run_actor_type STRING,
    run_actor_name STRING,
    is_service_principal BOOLEAN,
    cost_attribution_key STRING,
    _loaded_at TIMESTAMP
) USING DELTA;
"""

try:
    print(f"🔧 Creating table with updated schema: {table_name}")
    spark.sql(create_table_sql)
    logger.info(f"Created table with updated schema: {table_name}")
    print(f"✅ Successfully created {table_name}")
except Exception as e:
    logger.error(f"Error creating table {table_name}: {str(e)}")
    print(f"❌ Error creating {table_name}: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table Created

# COMMAND ----------

try:
    if spark.catalog.tableExists(table_name):
        print(f"✅ Table {table_name} exists")
        logger.info(f"Table {table_name} exists")
        
        # Show table schema
        print(f"\n📋 Table Schema:")
        spark.sql(f"DESCRIBE {table_name}").show(truncate=False)
        
    else:
        print(f"❌ Table {table_name} does not exist")
        logger.error(f"Table {table_name} does not exist")
except Exception as e:
    logger.error(f"Error verifying table {table_name}: {str(e)}")
    print(f"❌ Error verifying {table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recreate Complete

# COMMAND ----------

logger.info("slv_usage_txn table recreation completed successfully")
print("🎉 slv_usage_txn table recreation completed!")
print("🚀 You can now run the Silver Layer notebook again.")
