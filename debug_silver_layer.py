#!/usr/bin/env python3
"""
Silver Layer Debug Script
This script helps debug Silver Layer issues on Serverless compute
"""

import sys
import os
from datetime import datetime

# Add libs to path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    libs_dir = os.path.join(current_dir, 'libs')
    if libs_dir not in sys.path:
        sys.path.append(libs_dir)
except NameError:
    # For Databricks notebooks
    workspace_paths = [
        '/Workspace/Repos/platform-observability/libs',
        '/Workspace/Users/podilapalls@gmail.com/platform-observability/libs'
    ]
    for workspace_libs_path in workspace_paths:
        if workspace_libs_path not in sys.path:
            sys.path.append(workspace_libs_path)

from config import Config
from libs.logging import StructuredLogger

def debug_silver_layer():
    """Debug Silver Layer issues step by step"""
    
    logger = StructuredLogger("silver_debug")
    config = Config.get_config()
    
    logger.info("🔍 Starting Silver Layer Debug Session")
    
    try:
        # Test 1: Check if Spark is available
        logger.info("Test 1: Checking Spark availability...")
        if 'spark' in globals():
            logger.info("✅ Spark is available")
            spark_version = spark.version
            logger.info(f"Spark version: {spark_version}")
        else:
            logger.error("❌ Spark is not available")
            return False
        
        # Test 2: Check configuration
        logger.info("Test 2: Checking configuration...")
        logger.info(f"Catalog: {config.catalog}")
        logger.info(f"Bronze Schema: {config.bronze_schema}")
        logger.info(f"Silver Schema: {config.silver_schema}")
        logger.info(f"Environment: {Config.ENV}")
        
        # Test 3: Check if bronze tables exist
        logger.info("Test 3: Checking Bronze tables...")
        bronze_tables = [
            "brz_billing_usage",
            "brz_lakeflow_jobs", 
            "brz_lakeflow_pipelines",
            "brz_lakeflow_job_run_timeline",
            "brz_lakeflow_job_task_run_timeline",
            "brz_compute_clusters",
            "brz_access_workspaces_latest"
        ]
        
        for table in bronze_tables:
            try:
                full_table_name = f"{config.catalog}.{config.bronze_schema}.{table}"
                df = spark.table(full_table_name)
                count = df.count()
                logger.info(f"✅ {table}: {count} records")
            except Exception as e:
                logger.error(f"❌ {table}: {str(e)}")
        
        # Test 4: Check if silver schemas exist
        logger.info("Test 4: Checking Silver schema...")
        try:
            spark.sql(f"USE CATALOG {config.catalog}")
            spark.sql(f"USE SCHEMA {config.silver_schema}")
            logger.info("✅ Silver schema is accessible")
        except Exception as e:
            logger.error(f"❌ Silver schema issue: {str(e)}")
        
        # Test 5: Test individual transformations
        logger.info("Test 5: Testing individual transformations...")
        
        # Test billing usage transformation
        try:
            logger.info("Testing billing usage transformation...")
            billing_df = spark.table(f"{config.catalog}.{config.bronze_schema}.brz_billing_usage")
            sample_df = billing_df.limit(10)
            
            # Test the transformation logic
            from pyspark.sql import functions as F
            transformed = sample_df.select(
                sample_df.record_id,
                sample_df.account_id,
                sample_df.workspace_id,
                sample_df.cloud,
                sample_df.sku_name,
                sample_df.usage_unit,
                sample_df.usage_start_time,
                sample_df.usage_end_time,
                sample_df.usage_date,
                sample_df.usage_quantity,
                sample_df.usage_metadata,
                sample_df.identity_metadata,
                sample_df.record_type,
                sample_df.ingestion_date,
                sample_df.billing_origin_product,
                sample_df.product_features,
                sample_df.usage_type,
                sample_df.custom_tags,
                F.lit("UNKNOWN").alias("entity_type"),
                F.lit("UNKNOWN").alias("entity_id"),
                F.coalesce(sample_df.usage_metadata.job_run_id, F.lit("UNKNOWN")).alias("job_run_id"),
                F.date_format(sample_df.usage_date, "yyyyMMdd").cast("int").alias("date_sk"),
                F.lit(0.0).alias("list_cost_usd"),
                F.lit(0.0).alias("duration_hours")
            )
            
            transformed_count = transformed.count()
            logger.info(f"✅ Billing usage transformation: {transformed_count} records")
            
        except Exception as e:
            logger.error(f"❌ Billing usage transformation failed: {str(e)}")
        
        # Test job run timeline transformation
        try:
            logger.info("Testing job run timeline transformation...")
            timeline_df = spark.table(f"{config.catalog}.{config.bronze_schema}.brz_lakeflow_job_run_timeline")
            sample_timeline = timeline_df.limit(10)
            
            transformed_timeline = sample_timeline.select(
                sample_timeline.account_id,
                sample_timeline.workspace_id,
                sample_timeline.job_id,
                sample_timeline.run_id.alias("job_run_id"),
                sample_timeline.period_start_time,
                sample_timeline.period_end_time,
                sample_timeline.trigger_type,
                sample_timeline.run_type,
                sample_timeline.run_name,
                sample_timeline.compute_ids,
                sample_timeline.result_state,
                sample_timeline.termination_code,
                sample_timeline.job_parameters,
                F.date_format(sample_timeline.period_start_time, "yyyyMMdd").cast("int").alias("date_sk_start"),
                F.date_format(sample_timeline.period_end_time, "yyyyMMdd").cast("int").alias("date_sk_end"),
                F.current_timestamp().alias("_loaded_at")
            )
            
            timeline_count = transformed_timeline.count()
            logger.info(f"✅ Job run timeline transformation: {timeline_count} records")
            
        except Exception as e:
            logger.error(f"❌ Job run timeline transformation failed: {str(e)}")
        
        # Test cluster transformation
        try:
            logger.info("Testing cluster transformation...")
            cluster_df = spark.table(f"{config.catalog}.{config.bronze_schema}.brz_compute_clusters")
            sample_cluster = cluster_df.limit(10)
            
            # Test JSON parsing
            transformed_cluster = sample_cluster.select(
                sample_cluster.account_id,
                sample_cluster.workspace_id,
                sample_cluster.cluster_id,
                sample_cluster.cluster_name,
                sample_cluster.owned_by,
                sample_cluster.create_time,
                sample_cluster.delete_time,
                sample_cluster.driver_node_type,
                sample_cluster.worker_node_type,
                sample_cluster.worker_count,
                sample_cluster.min_autoscale_workers,
                sample_cluster.max_autoscale_workers,
                sample_cluster.auto_termination_minutes,
                sample_cluster.enable_elastic_disk,
                sample_cluster.tags,
                sample_cluster.cluster_source,
                sample_cluster.init_scripts,
                # Test JSON parsing
                F.from_json(sample_cluster.aws_attributes, "struct<instance_profile_arn:string,zone_id:string,first_on_demand:int,availability:string,spot_bid_price_percent:int,ebs_volume_type:string,ebs_volume_count:int,ebs_volume_size:int,ebs_volume_iops:int,ebs_volume_throughput:int>").alias("aws_attributes"),
                F.from_json(sample_cluster.azure_attributes, "struct<first_on_demand:int,availability:string,spot_bid_max_price:double>").alias("azure_attributes"),
                F.from_json(sample_cluster.gcp_attributes, "struct<use_preemptible_executors:boolean,zone_id:string,first_on_demand:int,availability:string>").alias("gcp_attributes"),
                sample_cluster.driver_instance_pool_id,
                sample_cluster.worker_instance_pool_id,
                sample_cluster.dbr_version,
                sample_cluster.change_time,
                sample_cluster.change_date,
                sample_cluster.data_security_mode,
                sample_cluster.policy_id,
                F.current_timestamp().alias("_loaded_at")
            )
            
            cluster_count = transformed_cluster.count()
            logger.info(f"✅ Cluster transformation: {cluster_count} records")
            
        except Exception as e:
            logger.error(f"❌ Cluster transformation failed: {str(e)}")
        
        logger.info("🎯 Debug session completed")
        return True
        
    except Exception as e:
        logger.error(f"💥 Debug session failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    debug_silver_layer()
