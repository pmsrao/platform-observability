# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Debug Notebook
# MAGIC 
# MAGIC This notebook helps debug Silver Layer issues on Serverless compute.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Check Spark availability and configuration
# MAGIC - Verify Bronze table existence and data
# MAGIC - Test individual Silver transformations
# MAGIC - Identify specific failure points
# MAGIC 
# MAGIC ## Usage:
# MAGIC Run this notebook before running the main Silver Layer to identify potential issues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
import os
from datetime import datetime

# Add libs to path
try:
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
from libs.logging import StructuredLogger

# Get configuration
config = Config.get_config()
logger = StructuredLogger("silver_debug")

print("🔍 Silver Layer Debug Session Initialized")
print(f"Catalog: {config.catalog}")
print(f"Bronze Schema: {config.bronze_schema}")
print(f"Silver Schema: {config.silver_schema}")
print(f"Environment: {Config.ENV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Check Spark Availability

# COMMAND ----------

logger.info("Test 1: Checking Spark availability...")
if 'spark' in globals():
    logger.info("✅ Spark is available")
    spark_version = spark.version
    logger.info(f"Spark version: {spark_version}")
    print(f"✅ Spark is available - Version: {spark_version}")
else:
    logger.error("❌ Spark is not available")
    print("❌ Spark is not available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Check Configuration

# COMMAND ----------

logger.info("Test 2: Checking configuration...")
logger.info(f"Catalog: {config.catalog}")
logger.info(f"Bronze Schema: {config.bronze_schema}")
logger.info(f"Silver Schema: {config.silver_schema}")
logger.info(f"Environment: {Config.ENV}")

print("✅ Configuration loaded successfully")
print(f"   Catalog: {config.catalog}")
print(f"   Bronze Schema: {config.bronze_schema}")
print(f"   Silver Schema: {config.silver_schema}")
print(f"   Environment: {Config.ENV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Check Bronze Tables

# COMMAND ----------

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

bronze_status = {}
for table in bronze_tables:
    try:
        full_table_name = f"{config.catalog}.{config.bronze_schema}.{table}"
        df = spark.table(full_table_name)
        count = df.count()
        logger.info(f"✅ {table}: {count} records")
        bronze_status[table] = {"status": "success", "count": count}
        print(f"✅ {table}: {count} records")
    except Exception as e:
        logger.error(f"❌ {table}: {str(e)}")
        bronze_status[table] = {"status": "error", "error": str(e)}
        print(f"❌ {table}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Check Silver Schema

# COMMAND ----------

logger.info("Test 4: Checking Silver schema...")
try:
    spark.sql(f"USE CATALOG {config.catalog}")
    spark.sql(f"USE SCHEMA {config.silver_schema}")
    logger.info("✅ Silver schema is accessible")
    print("✅ Silver schema is accessible")
except Exception as e:
    logger.error(f"❌ Silver schema issue: {str(e)}")
    print(f"❌ Silver schema issue: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Test Billing Usage Transformation

# COMMAND ----------

logger.info("Testing billing usage transformation...")
try:
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
    print(f"✅ Billing usage transformation: {transformed_count} records")
    
except Exception as e:
    logger.error(f"❌ Billing usage transformation failed: {str(e)}")
    print(f"❌ Billing usage transformation failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Test Job Run Timeline Transformation

# COMMAND ----------

logger.info("Testing job run timeline transformation...")
try:
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
    print(f"✅ Job run timeline transformation: {timeline_count} records")
    
except Exception as e:
    logger.error(f"❌ Job run timeline transformation failed: {str(e)}")
    print(f"❌ Job run timeline transformation failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Test Cluster Transformation

# COMMAND ----------

logger.info("Testing cluster transformation...")
try:
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
    print(f"✅ Cluster transformation: {cluster_count} records")
    
except Exception as e:
    logger.error(f"❌ Cluster transformation failed: {str(e)}")
    print(f"❌ Cluster transformation failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: Test CDF (Change Data Feed) Functionality

# COMMAND ----------

logger.info("Testing CDF functionality...")
try:
    # Test table_changes function with correct arguments
    test_table = f"{config.catalog}.{config.bronze_schema}.brz_billing_usage"
    
    # Test with version 0 (initial load)
    cdf_query = f"SELECT * FROM table_changes('{test_table}', 0) LIMIT 5"
    cdf_df = spark.sql(cdf_query)
    cdf_count = cdf_df.count()
    
    logger.info(f"✅ CDF test successful: {cdf_count} records")
    print(f"✅ CDF test successful: {cdf_count} records")
    
    # Show CDF columns
    print("CDF columns available:")
    for col in cdf_df.columns:
        print(f"   - {col}")
    
except Exception as e:
    logger.error(f"❌ CDF test failed: {str(e)}")
    print(f"❌ CDF test failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debug Summary

# COMMAND ----------

print("🎯 Silver Layer Debug Summary")
print("=" * 50)

# Count successful tests
successful_tests = 0
total_tests = 0

# Check Spark
if 'spark' in globals():
    successful_tests += 1
    print("✅ Spark availability: PASSED")
else:
    print("❌ Spark availability: FAILED")
total_tests += 1

# Check configuration
try:
    config = Config.get_config()
    successful_tests += 1
    print("✅ Configuration: PASSED")
except:
    print("❌ Configuration: FAILED")
total_tests += 1

# Check bronze tables
bronze_success = sum(1 for status in bronze_status.values() if status["status"] == "success")
bronze_total = len(bronze_status)
print(f"✅ Bronze tables: {bronze_success}/{bronze_total} accessible")

# Overall status
print(f"\n📊 Overall Status: {successful_tests}/{total_tests} core tests passed")
print(f"📊 Bronze Tables: {bronze_success}/{bronze_total} accessible")

if successful_tests == total_tests and bronze_success == bronze_total:
    print("🎉 All tests passed! Silver Layer should work correctly.")
else:
    print("⚠️  Some tests failed. Check the errors above before running Silver Layer.")

print("\n💡 Next Steps:")
print("1. If all tests passed, run the Silver Layer notebook")
print("2. If tests failed, fix the issues before proceeding")
print("3. Use the error viewer notebook to check for any runtime errors")