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
current_dir = os.path.dirname(os.path.abspath(__file__))
libs_dir = os.path.join(os.path.dirname(current_dir), 'libs')
if libs_dir not in sys.path:
    sys.path.append(libs_dir)

# For Databricks, also try the workspace path
workspace_libs_path = '/Workspace/Repos/platform-observability/libs'
if workspace_libs_path not in sys.path:
    sys.path.append(workspace_libs_path)

from config import Config
from processing_state import get_last_processed_timestamp, commit_processing_state, get_task_name
from tag_processor import TagProcessor
from logging import StructuredLogger
from error_handling import validate_data_quality
from utils import yyyymmdd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("silver_hwm_build_job")

logger.info("Starting Silver layer HWM build job", {
    "catalog": config.catalog,
    "bronze_schema": config.bronze_schema,
    "silver_schema": config.silver_schema,
    "environment": config.ENV
})

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
        query = f"""
        SELECT * FROM table_changes('{bronze_table}', '{last_timestamp.isoformat()}')
        """
        logger.info(f"Reading incremental data from {bronze_table} since {last_timestamp} using CDF")
    else:
        # Read all data for initial load using CDF
        query = f"""
        SELECT * FROM table_changes('{bronze_table}')
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
    
    return validate_data_quality(df, table_name, logger, validation_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Builders

# COMMAND ----------

def build_silver_workspace(spark) -> bool:
    """Build Silver workspace table"""
    logger.info("Building Silver workspace table")
    
    try:
        # Get last processed timestamp
        task_name = get_silver_task_name("slv_workspace")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_workspace", task_name, "silver")
        
        # Read new data from Bronze
        df = read_bronze_since_timestamp(spark, "brz_access_workspaces_latest", last_ts)
        
        if df.count() == 0:
            logger.info("No new data for Silver workspace table")
            return True
        
        # Validate data
        if not validate_silver_data(df, "silver_workspace"):
            logger.error("Data validation failed for Silver workspace table")
            return False
        
        # Transform data
        transformed_df = df.select(
            df.workspace_id,
            df.workspace_name,
            df.workspace_url,
            df.region,
            df.cloud,
            df.created_time,
            df.updated_time
        ).distinct()
        
        # Write to Silver table
        silver_table = get_silver_table_name("slv_workspace")
        transformed_df.write.mode("overwrite").saveAsTable(silver_table)
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_workspace")
            commit_processing_state(spark, "slv_workspace", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver workspace table with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        logger.error(f"Error building Silver workspace table: {str(e)}", exc_info=True)
        return False

def build_silver_entity_latest(spark) -> bool:
    """Build Silver entity latest view"""
    logger.info("Building Silver entity latest view")
    
    try:
        # Get last processed timestamp
        task_name = get_silver_task_name("slv_entity_latest")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_entity_latest", task_name, "silver")
        
        # Read new data from Bronze
        df = read_bronze_since_timestamp(spark, "brz_lakeflow_jobs", last_ts)
        
        if df.count() == 0:
            logger.info("No new data for Silver entity latest view")
            return True
        
        # Validate data
        if not validate_silver_data(df, "silver_entity_latest"):
            logger.error("Data validation failed for Silver entity latest view")
            return False
        
        # Transform data
        transformed_df = df.select(
            df.workspace_id,
            df.job_id.alias("entity_id"),
            df.job_name.alias("name"),
            df.run_as,
            df.created_time,
            df.updated_time
        ).withColumn("entity_type", F.lit("JOB"))
        
        # Write to Silver table
        silver_table = get_silver_table_name("slv_entity_latest")
        transformed_df.write.mode("overwrite").saveAsTable(silver_table)
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_entity_latest")
            commit_processing_state(spark, "slv_entity_latest", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver entity latest view with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        logger.error(f"Error building Silver entity latest view: {str(e)}", exc_info=True)
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
        
        # Validate data
        if not validate_silver_data(df, "silver_clusters"):
            logger.error("Data validation failed for Silver clusters table")
            return False
        
        # Transform data with SCD2 logic and new schema
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
            df.aws_attributes,
            df.azure_attributes,
            df.gcp_attributes,
            df.driver_instance_pool_id,
            df.worker_instance_pool_id,
            df.dbr_version,
            df.change_time,
            df.change_date,
            df.data_security_mode,
            df.policy_id
        ).withColumn("valid_from", df.change_time) \
         .withColumn("valid_to", F.lit(None)) \
         .withColumn("is_current", F.lit(True))
        
        # Add worker node type category
        tag_processor = TagProcessor()
        transformed_df = tag_processor.add_worker_node_type_category(transformed_df)
        
        # Write to Silver table
        silver_table = get_silver_table_name("slv_clusters")
        transformed_df.write.mode("overwrite").saveAsTable(silver_table)
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_clusters")
            commit_processing_state(spark, "slv_clusters", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver clusters table with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        logger.error(f"Error building Silver clusters table: {str(e)}", exc_info=True)
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
        
        # Validate data
        if not validate_silver_data(df, "silver_usage_txn"):
            logger.error("Data validation failed for Silver usage transaction table")
            return False
        
        # Transform data with all required columns
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
            df.custom_tags,
            df.usage_quantity,
            df.usage_metadata,
            df.identity_metadata,
            df.record_type,
            df.ingestion_date,
            df.billing_origin_product,
            df.product_features,
            df.usage_type,
            df.entity_type,
            df.entity_id,
            df.job_run_id,
            df.date_sk,
            df.list_cost_usd,
            df.duration_hours
        )
        
        # Enrich with tags and normalize
        tag_processor = TagProcessor()
        enriched_df = tag_processor.enrich_usage(transformed_df)
        
        # Write to Silver table
        silver_table = get_silver_table_name("slv_usage_txn")
        enriched_df.write.mode("overwrite").saveAsTable(silver_table)
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_usage_txn")
            commit_processing_state(spark, "slv_usage_txn", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver usage transaction table with {enriched_df.count()} records")
        return True
        
    except Exception as e:
        logger.error(f"Error building Silver usage transaction table: {str(e)}", exc_info=True)
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
        
        # Validate data
        if not validate_silver_data(df, "silver_job_run_timeline"):
            logger.error("Data validation failed for Silver job run timeline table")
            return False
        
        # Transform data
        transformed_df = df.select(
            df.workspace_id,
            df.job_id,
            df.run_id,
            df.start_time,
            df.end_time,
            df.result_state,
            df.termination_code,
            df.date_sk_start,
            df.date_sk_end
        )
        
        # Write to Silver table
        silver_table = get_silver_table_name("slv_job_run_timeline")
        transformed_df.write.mode("overwrite").saveAsTable(silver_table)
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_job_run_timeline")
            commit_processing_state(spark, "slv_job_run_timeline", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver job run timeline table with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        logger.error(f"Error building Silver job run timeline table: {str(e)}", exc_info=True)
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
        
        # Validate data
        if not validate_silver_data(df, "silver_job_task_run_timeline"):
            logger.error("Data validation failed for Silver job task run timeline table")
            return False
        
        # Transform data with SCD2 logic
        transformed_df = df.select(
            df.workspace_id,
            df.job_id,
            df.run_id,
            df.task_key,
            df.start_time,
            df.end_time,
            df.result_state,
            df.termination_code,
            df.date_sk_start,
            df.date_sk_end
        ).withColumn("valid_from", df.start_time) \
         .withColumn("valid_to", F.lit(None)) \
         .withColumn("is_current", F.lit(True))
        
        # Write to Silver table
        silver_table = get_silver_table_name("slv_job_task_run_timeline")
        transformed_df.write.mode("overwrite").saveAsTable(silver_table)
        
        # Update processing state
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_job_task_run_timeline")
            commit_processing_state(spark, "slv_job_task_run_timeline", max_ts, task_name=task_name, layer="silver")
        
        logger.info(f"Successfully built Silver job task run timeline table with {transformed_df.count()} records")
        return True
        
    except Exception as e:
        logger.error(f"Error building Silver job task run timeline table: {str(e)}", exc_info=True)
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
        try:
            success = builder_func(spark)
            results[table_name] = success
            
            if success:
                logger.info(f"✅ Successfully built Silver table: {table_name}")
            else:
                logger.error(f"❌ Failed to build Silver table: {table_name}")
                
        except Exception as e:
            logger.error(f"❌ Exception building Silver table {table_name}: {str(e)}", exc_info=True)
            results[table_name] = False
    
    # Summary
    successful_tables = [name for name, success in results.items() if success]
    failed_tables = [name for name, success in results.items() if not success]
    
    logger.info(f"Silver layer build completed. Successful: {len(successful_tables)}, Failed: {len(failed_tables)}")
    
    if successful_tables:
        logger.info(f"✅ Successfully built tables: {', '.join(successful_tables)}")
    
    if failed_tables:
        logger.error(f"❌ Failed to build tables: {', '.join(failed_tables)}")
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Silver Layer Build

# COMMAND ----------

if __name__ == "__main__":
    # Build Silver layer
    results = build_silver_layer(spark)
    
    # Check overall success
    all_successful = all(results.values())
    
    if all_successful:
        logger.info("🎉 All Silver layer tables built successfully!")
        dbutils.notebook.exit("SUCCESS")
    else:
        logger.error("💥 Some Silver layer tables failed to build")
        dbutils.notebook.exit("FAILED")
