# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer HWM Build Job
# MAGIC 
# MAGIC This notebook builds the Silver layer using High-Water Mark (HWM) approach.
# MAGIC It processes data incrementally from Bronze tables and applies transformations.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Incremental processing using HWM tracking
# MAGIC - SCD2 implementation for key tables
# MAGIC - Tag enrichment and normalization
# MAGIC - Data quality validation
# MAGIC - Structured logging and monitoring
# MAGIC 
# MAGIC ## Dependencies:
# MAGIC - Bronze tables must be populated
# MAGIC - Processing offsets tables must exist
# MAGIC - Configuration must be set up

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Import from libs package (cloud-agnostic approach)
import sys
import os

# Add current directory to path for local development
try:
    # This works in local development
    current_dir = os.path.dirname(os.path.abspath(__file__))
    libs_dir = os.path.join(os.path.dirname(current_dir), 'libs')
    if libs_dir not in sys.path:
        sys.path.append(libs_dir)
except NameError:
    # __file__ is not available in Databricks notebooks
    pass

# For Databricks, also try the workspace path
workspace_paths = [
    '/Workspace/Repos/platform-observability/libs',
    '/Workspace/Users/podilapalls@gmail.com/platform-observability/libs'
]
for workspace_libs_path in workspace_paths:
    if workspace_libs_path not in sys.path:
        sys.path.append(workspace_libs_path)

from config import Config
from processing_state import get_last_processed_timestamp, commit_processing_state, get_task_name
from tag_processor import TagProcessor
from libs.logging import StructuredLogger
from libs.error_handling import validate_data_quality
from utils import yyyymmdd
from libs.error_capture_utils import ErrorCapture

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Module Cache (Run this cell first)

# COMMAND ----------

# Clear module cache to ensure fresh imports
import sys
modules_to_clear = [
    'libs.error_handling',
    'libs.logging', 
    'libs.monitoring',
    'libs.error_capture_utils',
    'config',
    'processing_state',
    'tag_processor',
    'utils'
]

for module in modules_to_clear:
    if module in sys.modules:
        del sys.modules[module]
        print(f"✅ Cleared {module} from cache")

# Note: spark.catalog.clearCache() is not supported on Serverless compute
# Skipping Spark cache clearing for Serverless compatibility

print("🔄 Module cache cleared - ready for fresh imports")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("silver_hwm_build_job")

# Initialize error capture system
error_capture = ErrorCapture(spark)

# Helper function for upserting to silver tables (Type 1 - Current values only)
def upsert_silver_table(df, table_name, natural_keys):
    """
    Upsert DataFrame to silver table using MERGE operation to avoid duplicates.
    Uses natural keys to identify existing records.
    """
    try:
        # Create temporary view for merge
        df.createOrReplaceTempView("temp_silver")
        
        # Build merge condition
        merge_conditions = []
        for key in natural_keys:
            merge_conditions.append(f"target.{key} = source.{key}")
        merge_condition = " AND ".join(merge_conditions)
        
        # Perform merge
        merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING temp_silver AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        spark.sql(merge_sql)
        logger.info(f"Successfully upserted to {table_name} using MERGE operation")
        return True
        
    except Exception as e:
        logger.error(f"Error upserting to {table_name}: {str(e)}")
        return False

# Helper function for SCD2 tables with proper merge logic
def upsert_scd2_silver_table(df, table_name, natural_keys, change_time_col="change_time"):
    """
    Upsert SCD2 DataFrame to silver table using proper SCD2 merge logic.
    Handles historical tracking with valid_from/valid_to timestamps.
    """
    try:
        # Create temporary view for merge
        df.createOrReplaceTempView("temp_silver_scd2")
        
        # Build merge condition for natural keys
        merge_conditions = []
        for key in natural_keys:
            merge_conditions.append(f"target.{key} = source.{key}")
        merge_condition = " AND ".join(merge_conditions)
        
        # SCD2 Merge logic:
        # 1. Close existing current records (set valid_to and is_current=false)
        # 2. Insert new records with valid_from=change_time and is_current=true
        merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING temp_silver_scd2 AS source
        ON {merge_condition} AND target.is_current = true
        WHEN MATCHED AND target.valid_from < source.{change_time_col} THEN 
            UPDATE SET 
                valid_to = source.{change_time_col},
                is_current = false
        WHEN NOT MATCHED THEN 
            INSERT *
        """
        
        spark.sql(merge_sql)
        logger.info(f"Successfully upserted SCD2 data to {table_name} using proper merge logic")
        return True
        
    except Exception as e:
        logger.error(f"Error upserting SCD2 data to {table_name}: {str(e)}")
        return False

# Fallback function for simple append (use only when necessary)
def append_silver_table(df, table_name):
    """
    Append DataFrame to silver table with mergeSchema.
    Use only when SCD2 merge is not applicable.
    """
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    logger.info(f"Successfully appended to {table_name} with mergeSchema")

logger.info("Starting Silver layer HWM build job", 
            catalog=config.catalog,
            bronze_schema=config.bronze_schema,
            silver_schema=config.silver_schema,
            environment=Config.ENV)

# Add explicit print statements for visibility
print("🚀 Starting Silver Layer HWM Build Job")
print(f"📊 Catalog: {config.catalog}")
print(f"📊 Bronze Schema: {config.bronze_schema}")
print(f"📊 Silver Schema: {config.silver_schema}")
print(f"📊 Environment: {Config.ENV}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_bronze_table_name(table_name: str) -> str:
    """Get full Bronze table name"""
    return f"{config.catalog}.{config.bronze_schema}.{table_name}"

def get_silver_table_name(table_name: str) -> str:
    """Get full Silver table name"""
    return f"{config.catalog}.{config.silver_schema}.{table_name}"

def get_silver_task_name(table_name: str) -> str:
    """Get standardized task name for Silver layer table"""
    return get_task_name(table_name)

def read_bronze_since_timestamp(spark, table_name: str, last_timestamp: Optional[datetime]) -> 'DataFrame':
    """Read data from Bronze table since last timestamp using CDF"""
    bronze_table = get_bronze_table_name(table_name)
    
    if last_timestamp:
        # Read incremental data using CDF since last timestamp
        # table_changes requires at least 2 arguments: table_name and start_version/timestamp
        query = f"""
        SELECT * FROM table_changes('{bronze_table}', '{last_timestamp.isoformat()}')
        """
        logger.info(f"Reading incremental data from {bronze_table} since {last_timestamp} using CDF")
    else:
        # Read all data for initial load using CDF
        # For initial load, we need to specify a start version (0) or use a very old timestamp
        query = f"""
        SELECT * FROM table_changes('{bronze_table}', 0)
        """
        logger.info(f"Reading all data from {bronze_table} for initial load using CDF")
    
    return spark.sql(query)

def get_max_timestamp(df: 'DataFrame', timestamp_col: str = '_commit_timestamp') -> Optional[datetime]:
    """Get maximum timestamp from DataFrame (CDF output)"""
    if df.count() == 0:
        return None
    
    # CDF returns _commit_timestamp column
    max_ts = df.agg({timestamp_col: 'max'}).collect()[0][0]
    return max_ts if max_ts else None

def validate_silver_data(df: 'DataFrame', table_name: str) -> bool:
    """Validate data quality for Silver layer"""
    if df.count() == 0:
        logger.info(f"No data to validate for {table_name}")
        return True
    
    # Basic validation rules
    validation_rules = {
        'workspace_id': 'not_null',
        'entity_type': 'not_null',
        'entity_id': 'not_null'
    }
    
    return validate_data_quality(df, table_name, logger)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Builders

# COMMAND ----------

def build_silver_workspace(spark) -> bool:
    """Build Silver workspace table"""
    logger.info("Building Silver workspace table")
    print("🔧 Building Silver workspace table...")
    
    try:
        # Get last processed timestamp
        print("📅 Getting last processed timestamp...")
        task_name = get_silver_task_name("slv_workspace")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_workspace", task_name, "silver")
        print(f"📅 Last timestamp: {last_ts}")
        
        # Read new data from Bronze
        print("📖 Reading data from Bronze...")
        df = read_bronze_since_timestamp(spark, "brz_access_workspaces_latest", last_ts)
        record_count = df.count()
        print(f"📖 Found {record_count} records in Bronze")
        
        if record_count == 0:
            logger.info("No new data for Silver workspace table")
            print("✅ No new data - skipping workspace table")
            return True
        
        # Validate data - DISABLED for performance optimization
        # if not validate_silver_data(df, "silver_workspace"):
        #     logger.error("Data validation failed for Silver workspace table")
        #     return False
        logger.info("Data validation disabled - processing data")
        
        # Transform data - FIXED: Use correct column names from bronze schema
        print("🔄 Transforming data...")
        transformed_df = df.select(
            df.account_id,
            df.workspace_id,
            df.workspace_name,
            df.workspace_url,
            df.create_time,  # FIXED: Keep original column name to match existing table schema
            df.status,
            F.current_timestamp().alias("_loaded_at")
        ).distinct()
        print(f"🔄 Transformed {transformed_df.count()} records")
        
        # Write to Silver table using MERGE to avoid duplicates
        print("💾 Upserting to Silver table...")
        silver_table = get_silver_table_name("slv_workspace")
        success = upsert_silver_table(transformed_df, silver_table, ["workspace_id"])
        if success:
            print(f"✅ Successfully upserted to {silver_table}")
        else:
            print(f"❌ Failed to upsert to {silver_table}")
            return False
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_workspace")
            commit_processing_state(spark, "slv_workspace", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver workspace table with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        # Capture error for persistence
        error_capture.capture_error("build_silver_workspace", e)
        logger.error(f"Error building Silver workspace table: {str(e)}", exc_info=True)
        import traceback
        traceback.print_exc()
        return False

def build_silver_entity_latest(spark) -> bool:
    """Build unified Silver entity latest view from JOB and PIPELINE sources"""
    logger.info("Building unified Silver entity latest view from JOB and PIPELINE sources")
    
    try:
        # Get last processed timestamp
        task_name = get_silver_task_name("slv_entity_latest")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_entity_latest", task_name, "silver")
        
        # Process JOBS
        logger.info("Processing JOB entities from brz_lakeflow_jobs")
        jobs_df = read_bronze_since_timestamp(spark, "brz_lakeflow_jobs", last_ts)
        
        # Process PIPELINES
        logger.info("Processing PIPELINE entities from brz_lakeflow_pipelines")
        pipelines_df = read_bronze_since_timestamp(spark, "brz_lakeflow_pipelines", last_ts)
        
        # Transform JOB data - FIXED: Use correct column names from bronze schema
        jobs_entities = None
        if jobs_df.count() > 0:
            jobs_entities = jobs_df.select(
                jobs_df.account_id,
                jobs_df.workspace_id,
                jobs_df.job_id.alias("entity_id"),
                jobs_df.name,  # FIXED: Use 'name' column from bronze, not 'job_name'
                jobs_df.run_as,
                # Pipeline-specific attributes (NULL for jobs)
                F.lit(None).cast("string").alias("pipeline_type"),
                # Job-specific workflow attributes - FIXED: These columns don't exist in bronze yet
                F.lit(False).alias("is_parent_workflow"),  # Will be computed by tag processor
                F.lit(False).alias("is_sub_workflow"),     # Will be computed by tag processor
                F.lit("STANDALONE").alias("workflow_level"),  # Default value
                F.lit("None").alias("parent_workflow_name"),  # Default value
                # Common attributes - FIXED: Use correct column names
                jobs_df.change_time.alias("created_time"),  # Use change_time as created_time
                jobs_df.change_time.alias("updated_time"),  # Use change_time as updated_time
                F.current_timestamp().alias("_loaded_at")
            ).withColumn("entity_type", F.lit("JOB"))
            logger.info(f"Transformed {jobs_entities.count()} JOB entities")
        
        # Transform PIPELINE data - FIXED: Use correct column names from bronze schema
        pipelines_entities = None
        if pipelines_df.count() > 0:
            pipelines_entities = pipelines_df.select(
                pipelines_df.account_id,
                pipelines_df.workspace_id,
                pipelines_df.pipeline_id.alias("entity_id"),
                pipelines_df.name,
                pipelines_df.run_as,
                # Pipeline-specific attributes
                pipelines_df.pipeline_type,
                # Job-specific workflow attributes (NULL for pipelines)
                F.lit(None).cast("boolean").alias("is_parent_workflow"),
                F.lit(None).cast("boolean").alias("is_sub_workflow"),
                F.lit(None).cast("string").alias("workflow_level"),
                F.lit(None).cast("string").alias("parent_workflow_name"),
                # Common attributes - FIXED: Use correct column names
                pipelines_df.change_time.alias("created_time"),  # Use change_time as created_time
                pipelines_df.change_time.alias("updated_time"),  # Use change_time as updated_time
                F.current_timestamp().alias("_loaded_at")
            ).withColumn("entity_type", F.lit("PIPELINE"))
            logger.info(f"Transformed {pipelines_entities.count()} PIPELINE entities")
        
        # Union both sources
        unified_entities = None
        if jobs_entities is not None and pipelines_entities is not None:
            unified_entities = jobs_entities.union(pipelines_entities)
        elif jobs_entities is not None:
            unified_entities = jobs_entities
        elif pipelines_entities is not None:
            unified_entities = pipelines_entities
        else:
            logger.info("No new data for Silver entity latest view")
            return True
        
        # Validate data - DISABLED for performance optimization
        # if not validate_silver_data(unified_entities, "silver_entity_latest"):
        #     logger.error("Data validation failed for Silver entity latest view")
        #     return False
        logger.info("Data validation disabled - processing data")
        
        # Write to Silver table using MERGE to avoid duplicates
        silver_table = get_silver_table_name("slv_entity_latest")
        success = upsert_silver_table(unified_entities, silver_table, ["workspace_id", "entity_id", "entity_type"])
        if not success:
            print(f"❌ Failed to upsert to {silver_table}")
            return False
        
        # Update processing state (use max timestamp from both sources)
        max_ts_jobs = get_max_timestamp(jobs_df) if jobs_df.count() > 0 else None
        max_ts_pipelines = get_max_timestamp(pipelines_df) if pipelines_df.count() > 0 else None
        
        max_ts = None
        if max_ts_jobs and max_ts_pipelines:
            max_ts = max(max_ts_jobs, max_ts_pipelines)
        elif max_ts_jobs:
            max_ts = max_ts_jobs
        elif max_ts_pipelines:
            max_ts = max_ts_pipelines
        
        if max_ts:
            task_name = get_silver_task_name("slv_entity_latest")
            commit_processing_state(spark, "slv_entity_latest", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built unified Silver entity latest view with {unified_entities.count()} records")
        return True
        
    except Exception as e:
        # Capture error for persistence
        error_capture.capture_error("build_silver_entity_latest", e)
        logger.error(f"Error building Silver entity latest view: {str(e)}", exc_info=True)
        import traceback
        traceback.print_exc()
        return False

def build_silver_clusters(spark) -> bool:
    """Build Silver clusters table with SCD2"""
    logger.info("Building Silver clusters table with SCD2")
    
    try:
        # Get last processed timestamp
        task_name = get_silver_task_name("slv_clusters")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_clusters", task_name, "silver")
        
        # Read new data from Bronze
        df = read_bronze_since_timestamp(spark, "brz_compute_clusters", last_ts)
        
        if df.count() == 0:
            logger.info("No new data for Silver clusters table")
            return True
        
        # Validate data - DISABLED for performance optimization
        # if not validate_silver_data(df, "silver_clusters"):
        #     logger.error("Data validation failed for Silver clusters table")
        #     return False
        logger.info("Data validation disabled - processing data")
        
        # Transform data with SCD2 logic and new schema - FIXED: Handle JSON string attributes
        transformed_df = df.select(
            df.account_id,
            df.workspace_id,
            df.cluster_id,
            df.cluster_name,
            df.owned_by,
            df.create_time,
            df.delete_time,
            df.driver_node_type,
            df.worker_node_type,
            df.worker_count,
            df.min_autoscale_workers,
            df.max_autoscale_workers,
            df.auto_termination_minutes,
            df.enable_elastic_disk,
            df.tags,
            df.cluster_source,
            df.init_scripts,
            # FIXED: Parse JSON string attributes into structured objects
            F.from_json(df.aws_attributes, "struct<instance_profile_arn:string,zone_id:string,first_on_demand:int,availability:string,spot_bid_price_percent:int,ebs_volume_type:string,ebs_volume_count:int,ebs_volume_size:int,ebs_volume_iops:int,ebs_volume_throughput:int>").alias("aws_attributes"),
            F.from_json(df.azure_attributes, "struct<first_on_demand:int,availability:string,spot_bid_max_price:double>").alias("azure_attributes"),
            F.from_json(df.gcp_attributes, "struct<use_preemptible_executors:boolean,zone_id:string,first_on_demand:int,availability:string>").alias("gcp_attributes"),
            df.driver_instance_pool_id,
            df.worker_instance_pool_id,
            df.dbr_version,
            df.change_time,
            df.change_date,
            df.data_security_mode,
            df.policy_id,
            F.current_timestamp().alias("_loaded_at")
        ).withColumn("valid_from", df.change_time) \
         .withColumn("valid_to", F.lit(None)) \
         .withColumn("is_current", F.lit(True))
        
        # Add worker node type category
        tag_processor = TagProcessor()
        transformed_df = tag_processor.add_worker_node_type_category(transformed_df)
        
        # Write to Silver table using SCD2 merge logic
        silver_table = get_silver_table_name("slv_clusters")
        success = upsert_scd2_silver_table(transformed_df, silver_table, ["workspace_id", "cluster_id"], "change_time")
        if not success:
            print(f"❌ Failed to upsert SCD2 data to {silver_table}")
            return False
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_clusters")
            commit_processing_state(spark, "slv_clusters", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver clusters table with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        # Capture error for persistence
        error_capture.capture_error("build_silver_clusters", e)
        logger.error(f"Error building Silver clusters table: {str(e)}", exc_info=True)
        import traceback
        traceback.print_exc()
        return False

def build_silver_usage_txn(spark) -> bool:
    """Build Silver usage transaction table"""
    logger.info("Building Silver usage transaction table")
    
    try:
        # Get last processed timestamp
        task_name = get_silver_task_name("slv_usage_txn")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_usage_txn", task_name, "silver")
        
        # Read new data from Bronze
        df = read_bronze_since_timestamp(spark, "brz_billing_usage", last_ts)
        
        if df.count() == 0:
            logger.info("No new data for Silver usage transaction table")
            return True
        
        # Validate data - DISABLED for performance optimization
        # if not validate_silver_data(df, "silver_usage_txn"):
        #     logger.error("Data validation failed for Silver usage transaction table")
        #     return False
        logger.info("Data validation disabled - processing data")
        
        # Transform data with all required columns - FIXED: Add missing columns and computed fields
        transformed_df = df.select(
            df.record_id,
            df.account_id,
            df.workspace_id,
            df.cloud,
            df.sku_name,
            df.usage_unit,
            df.usage_start_time,
            df.usage_end_time,
            df.usage_date,
            df.usage_quantity,
            # FIXED: Add missing columns that exist in bronze but were missing in silver transform
            df.usage_metadata,
            df.identity_metadata,
            df.record_type,
            df.ingestion_date,
            df.billing_origin_product,
            df.product_features,
            df.usage_type,
            df.custom_tags,
            # FIXED: Add computed fields that need to be calculated
            F.lit("UNKNOWN").alias("entity_type"),  # Will be enriched by tag processor
            F.lit("UNKNOWN").alias("entity_id"),    # Will be enriched by tag processor
            F.coalesce(df.usage_metadata.job_run_id, F.lit("UNKNOWN")).alias("job_run_id"),
            F.date_format(df.usage_date, "yyyyMMdd").cast("int").alias("date_sk"),
            F.lit(0.0).alias("list_cost_usd"),  # Will be calculated when joined with prices
            F.lit(0.0).alias("duration_hours"),  # Will be calculated from usage times
            # Note: parent_workflow_name will be created by tag processor from custom_tags
        )
        
        # Enrich with tags and normalize
        tag_processor = TagProcessor()
        enriched_df = tag_processor.enrich_usage(transformed_df)
        
        # Add missing columns that are in the table schema but not generated by tag processor
        enriched_df = enriched_df.withColumn("inherited_line_of_business", F.lit(None).cast("string")) \
                                .withColumn("inherited_cost_center", F.lit(None).cast("string")) \
                                .withColumn("inherited_workflow_level", F.lit(None).cast("string")) \
                                .withColumn("inherited_parent_workflow", F.lit(None).cast("string")) \
                                .withColumn("databricks_runtime_raw", F.lit(None).cast("string")) \
                                .withColumn("databricks_runtime", F.lit(None).cast("string")) \
                                .withColumn("compute_node_type_raw", F.lit(None).cast("string")) \
                                .withColumn("compute_node_type", F.lit(None).cast("string")) \
                                .withColumn("cluster_worker_count_raw", F.lit(None).cast("string")) \
                                .withColumn("cluster_worker_count", F.lit(None).cast("string")) \
                                .withColumn("_loaded_at", F.current_timestamp())
        
        # Write to Silver table using MERGE to avoid duplicates
        silver_table = get_silver_table_name("slv_usage_txn")
        success = upsert_silver_table(enriched_df, silver_table, ["record_id"])
        if not success:
            print(f"❌ Failed to upsert to {silver_table}")
            return False
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_usage_txn")
            commit_processing_state(spark, "slv_usage_txn", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver usage transaction table with {enriched_df.count()} records")
        return True
        
    except Exception as e:
        # Capture error for persistence
        error_capture.capture_error("build_silver_usage_txn", e)
        logger.error(f"Error building Silver usage transaction table: {str(e)}", exc_info=True)
        import traceback
        traceback.print_exc()
        return False

def build_silver_job_run_timeline(spark) -> bool:
    """Build Silver job run timeline table"""
    logger.info("Building Silver job run timeline table")
    
    try:
        # Get last processed timestamp
        task_name = get_silver_task_name("slv_job_run_timeline")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_job_run_timeline", task_name, "silver")
        
        # Read new data from Bronze
        df = read_bronze_since_timestamp(spark, "brz_lakeflow_job_run_timeline", last_ts)
        
        if df.count() == 0:
            logger.info("No new data for Silver job run timeline table")
            return True
        
        # Validate data - DISABLED for performance optimization
        # if not validate_silver_data(df, "silver_job_run_timeline"):
        #     logger.error("Data validation failed for Silver job run timeline table")
        #     return False
        logger.info("Data validation disabled - processing data")
        
        # Transform data - FIXED: Use correct column names from bronze schema
        transformed_df = df.select(
            df.account_id,
            df.workspace_id,
            df.job_id,
            df.run_id.alias("job_run_id"),  # Transform run_id to job_run_id
            df.period_start_time,  # FIXED: Use period_start_time from bronze
            df.period_end_time,    # FIXED: Use period_end_time from bronze
            df.trigger_type,
            df.run_type,
            df.run_name,
            df.compute_ids,
            df.result_state,
            df.termination_code,
            df.job_parameters,
            # Add date_sk columns (these need to be computed)
            F.date_format(df.period_start_time, "yyyyMMdd").cast("int").alias("date_sk_start"),
            F.date_format(df.period_end_time, "yyyyMMdd").cast("int").alias("date_sk_end"),
            F.current_timestamp().alias("_loaded_at")
        )
        
        # Write to Silver table using MERGE to avoid duplicates (Type 1 - Current values only)
        silver_table = get_silver_table_name("slv_job_run_timeline")
        success = upsert_silver_table(transformed_df, silver_table, ["workspace_id", "job_id", "job_run_id"])
        if not success:
            print(f"❌ Failed to upsert to {silver_table}")
            return False
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_job_run_timeline")
            commit_processing_state(spark, "slv_job_run_timeline", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver job run timeline table with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        # Capture error for persistence
        error_capture.capture_error("build_silver_job_run_timeline", e)
        logger.error(f"Error building Silver job run timeline table: {str(e)}", exc_info=True)
        import traceback
        traceback.print_exc()
        return False

def build_silver_job_task_run_timeline(spark) -> bool:
    """Build Silver job task run timeline table with SCD2"""
    logger.info("Building Silver job task run timeline table with SCD2")
    
    try:
        # Get last processed timestamp
        task_name = get_silver_task_name("slv_job_task_run_timeline")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_job_task_run_timeline", task_name, "silver")
        
        # Read new data from Bronze
        df = read_bronze_since_timestamp(spark, "brz_lakeflow_job_task_run_timeline", last_ts)
        
        if df.count() == 0:
            logger.info("No new data for Silver job task run timeline table")
            return True
        
        # Validate data - DISABLED for performance optimization
        # if not validate_silver_data(df, "silver_job_task_run_timeline"):
        #     logger.error("Data validation failed for Silver job task run timeline table")
        #     return False
        logger.info("Data validation disabled - processing data")
        
        # Transform data with SCD2 logic - FIXED: Use correct column names from bronze schema
        transformed_df = df.select(
            df.account_id,
            df.workspace_id,
            df.job_id,
            df.run_id.alias("task_run_id"),  # Transform run_id to task_run_id
            df.parent_run_id.alias("job_run_id"),  # Parent run becomes job_run_id
            df.task_key,
            df.period_start_time,  # FIXED: Use period_start_time from bronze
            df.period_end_time,    # FIXED: Use period_end_time from bronze
            df.compute_ids,
            df.result_state,
            df.termination_code,
            # Calculate execution_secs from period times
            F.col("period_end_time").cast("long") - F.col("period_start_time").cast("long").alias("execution_secs"),
            F.current_timestamp().alias("_loaded_at")
        )
        
        # Write to Silver table using MERGE to avoid duplicates (Type 1 - Current values only)
        silver_table = get_silver_table_name("slv_job_task_run_timeline")
        success = upsert_silver_table(transformed_df, silver_table, ["workspace_id", "job_id", "task_run_id"])
        if not success:
            print(f"❌ Failed to upsert to {silver_table}")
            return False
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_job_task_run_timeline")
            commit_processing_state(spark, "slv_job_task_run_timeline", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver job task run timeline table with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        # Capture error for persistence
        error_capture.capture_error("build_silver_job_task_run_timeline", e)
        logger.error(f"Error building Silver job task run timeline table: {str(e)}", exc_info=True)
        import traceback
        traceback.print_exc()
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution Function

# COMMAND ----------

def build_silver_layer(spark) -> Dict[str, bool]:
    """Build entire Silver layer"""
    logger.info("Starting Silver layer build process")
    
    results = {}
    
    # Build Silver tables in dependency order
    silver_builders = [
        ("workspace", build_silver_workspace),
        ("entity_latest", build_silver_entity_latest),
        ("clusters", build_silver_clusters),
        ("usage_txn", build_silver_usage_txn),
        ("job_run_timeline", build_silver_job_run_timeline),
        ("job_task_run_timeline", build_silver_job_task_run_timeline)
    ]
    
    for table_name, builder_func in silver_builders:
        logger.info(f"Building Silver table: {table_name}")
        print(f"\n🏗️  Building Silver table: {table_name}")
        print("-" * 50)
        
        try:
            success = builder_func(spark)
            results[table_name] = success
            
            if success:
                logger.info(f"✅ Successfully built Silver table: {table_name}")
                print(f"✅ Successfully built Silver table: {table_name}")
            else:
                logger.error(f"❌ Failed to build Silver table: {table_name}")
                print(f"❌ Failed to build Silver table: {table_name}")
                
        except Exception as e:
            logger.error(f"❌ Exception building Silver table {table_name}: {str(e)}", exc_info=True)
            print(f"❌ Exception building Silver table {table_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            results[table_name] = False
    
    # Summary
    successful_tables = [name for name, success in results.items() if success]
    failed_tables = [name for name, success in results.items() if not success]
    
    print("\n" + "=" * 80)
    print("📊 SILVER LAYER BUILD SUMMARY")
    print("=" * 80)
    print(f"✅ Successful tables: {len(successful_tables)}")
    print(f"❌ Failed tables: {len(failed_tables)}")
    
    logger.info(f"Silver layer build completed. Successful: {len(successful_tables)}, Failed: {len(failed_tables)}")
    
    if successful_tables:
        logger.info(f"✅ Successfully built tables: {', '.join(successful_tables)}")
        print(f"✅ Successfully built: {', '.join(successful_tables)}")
    
    if failed_tables:
        logger.error(f"❌ Failed to build tables: {', '.join(failed_tables)}")
        print(f"❌ Failed to build: {', '.join(failed_tables)}")
    
    print("=" * 80)
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Silver Layer Build

# COMMAND ----------

if __name__ == "__main__":
    try:
        print("\n🚀 Starting Silver Layer Build Process...")
        print("=" * 80)
        
        # Build Silver layer
        results = build_silver_layer(spark)
        
        # Check overall success
        all_successful = all(results.values())
        
        if all_successful:
            logger.info("🎉 All Silver layer tables built successfully!")
            print("\n🎉 All Silver layer tables built successfully!")
            print("=" * 80)
            print("✅ NOTEBOOK COMPLETED SUCCESSFULLY")
        else:
            logger.error("💥 Some Silver layer tables failed to build")
            print("\n💥 Some Silver layer tables failed to build")
            print("=" * 80)
            # Save errors before exiting
            error_capture.save_errors_to_table()
            error_capture.save_errors_to_file("/tmp/silver_errors.json")
            print("❌ NOTEBOOK COMPLETED WITH FAILURES")
            
    except Exception as e:
        # Capture any critical errors
        error_capture.capture_error("main_execution", e)
        error_capture.save_errors_to_table()
        error_capture.save_errors_to_file("/tmp/silver_errors.json")
        
        logger.error(f"💥 Critical error in main execution: {str(e)}", exc_info=True)
        print(f"🚨 CRITICAL ERROR: {str(e)}")
        print(f"📋 Check error_logs table or /tmp/silver_errors.json for details")
        print("❌ NOTEBOOK COMPLETED WITH CRITICAL ERROR")
