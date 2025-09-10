# Databricks notebook source
# MAGIC %md
# MAGIC # Sample DLT Pipeline for Platform Observability Testing
# MAGIC
# MAGIC This notebook creates a sample DLT pipeline that generates dummy system data
# MAGIC to test the platform observability system. Run this daily for a few days to
# MAGIC populate the bronze tables with realistic test data.
# MAGIC
# MAGIC ## Generated Data:
# MAGIC - Workspace information
# MAGIC - Job and pipeline definitions
# MAGIC - Job runs and task runs
# MAGIC - Compute clusters and node types
# MAGIC - Billing usage data
# MAGIC - List prices

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import sys
import os
from datetime import datetime, timedelta
import random
import uuid

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Import from libs package
from libs.path_setup import setup_paths_and_import_config

# Setup paths and import Config
Config = setup_paths_and_import_config()
config = Config.get_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def generate_workspace_data(spark, num_workspaces=3):
    """Generate sample workspace data"""
    workspaces = []
    for i in range(num_workspaces):
        workspaces.append({
            "account_id": "123456789",
            "workspace_id": f"workspace_{i+1}",
            "workspace_name": f"Test Workspace {i+1}",
            "workspace_url": f"https://workspace{i+1}.cloud.databricks.com",
            "create_time": datetime.now() - timedelta(days=30),
            "status": "ACTIVE",
            "row_hash": str(uuid.uuid4()),
            "_loaded_at": datetime.now()
        })
    
    return spark.createDataFrame(workspaces)

def generate_job_data(spark, num_jobs=5):
    """Generate sample job data"""
    jobs = []
    job_types = ["ETL", "ML", "Analytics", "Streaming", "Batch"]
    
    for i in range(num_jobs):
        jobs.append({
            "account_id": "123456789",
            "workspace_id": f"workspace_{(i % 3) + 1}",
            "job_id": f"job_{i+1}",
            "name": f"Sample {job_types[i % len(job_types)]} Job {i+1}",
            "description": f"Test job for {job_types[i % len(job_types)]} processing",
            "creator_id": f"user_{i+1}",
            "tags": {
                "line_of_business": random.choice(["Analytics", "ML", "Data Engineering", "UNKNOWN"]),
                "department": random.choice(["Data", "Engineering", "Analytics", "UNKNOWN"]),
                "cost_center": random.choice(["CC001", "CC002", "unallocated", "UNKNOWN"]),
                "environment": random.choice(["prod", "dev", "stage", "uat"]),
                "use_case": random.choice(["ETL", "ML Training", "Analytics", "UNKNOWN"]),
                "pipeline_name": f"pipeline_{i+1}",
                "workflow_level": random.choice(["STANDALONE", "PARENT", "SUB_WORKFLOW", "UNKNOWN"]),
                "parent_workflow_name": random.choice(["None", "WF_001", "WF_002", "UNKNOWN"])
            },
            "change_time": datetime.now() - timedelta(days=random.randint(1, 10)),
            "delete_time": None,
            "run_as": f"user_{i+1}@company.com",
            "row_hash": str(uuid.uuid4()),
            "_loaded_at": datetime.now()
        })
    
    return spark.createDataFrame(jobs)

def generate_pipeline_data(spark, num_pipelines=3):
    """Generate sample pipeline data"""
    pipelines = []
    pipeline_types = ["DLT", "Streaming", "Batch"]
    
    for i in range(num_pipelines):
        pipelines.append({
            "account_id": "123456789",
            "workspace_id": f"workspace_{(i % 3) + 1}",
            "pipeline_id": f"pipeline_{i+1}",
            "pipeline_type": pipeline_types[i % len(pipeline_types)],
            "name": f"Sample {pipeline_types[i % len(pipeline_types)]} Pipeline {i+1}",
            "created_by": f"user_{i+1}",
            "run_as": f"user_{i+1}@company.com",
            "tags": {
                "line_of_business": random.choice(["Analytics", "ML", "Data Engineering", "UNKNOWN"]),
                "department": random.choice(["Data", "Engineering", "Analytics", "UNKNOWN"]),
                "cost_center": random.choice(["CC001", "CC002", "unallocated", "UNKNOWN"]),
                "environment": random.choice(["prod", "dev", "stage", "uat"]),
                "use_case": random.choice(["ETL", "ML Training", "Analytics", "UNKNOWN"]),
                "pipeline_name": f"pipeline_{i+1}",
                "workflow_level": random.choice(["STANDALONE", "PARENT", "SUB_WORKFLOW", "UNKNOWN"]),
                "parent_workflow_name": random.choice(["None", "WF_001", "WF_002", "UNKNOWN"])
            },
            "settings": {
                "photon": random.choice([True, False]),
                "development": random.choice([True, False]),
                "continuous": random.choice([True, False]),
                "serverless": random.choice([True, False]),
                "edition": random.choice(["ADVANCED", "PREMIUM", "STANDARD"]),
                "channel": random.choice(["CURRENT", "PREVIEW"])
            },
            "configuration": {
                "key1": f"value1_{i+1}",
                "key2": f"value2_{i+1}"
            },
            "change_time": datetime.now() - timedelta(days=random.randint(1, 10)),
            "delete_time": None,
            "row_hash": str(uuid.uuid4()),
            "_loaded_at": datetime.now()
        })
    
    return spark.createDataFrame(pipelines)

def generate_cluster_data(spark, num_clusters=4):
    """Generate sample cluster data"""
    clusters = []
    node_types = ["i3.xlarge", "i3.2xlarge", "m5.large", "m5.xlarge", "r5.large", "r5.xlarge"]
    
    for i in range(num_clusters):
        clusters.append({
            "account_id": "123456789",
            "workspace_id": f"workspace_{(i % 3) + 1}",
            "cluster_id": f"cluster_{i+1}",
            "cluster_name": f"Test Cluster {i+1}",
            "owned_by": f"user_{(i % 5) + 1}@company.com",
            "create_time": datetime.now() - timedelta(days=random.randint(1, 20)),
            "delete_time": None if random.random() > 0.3 else datetime.now() - timedelta(days=random.randint(1, 5)),
            "driver_node_type": random.choice(node_types),
            "worker_node_type": random.choice(node_types),
            "worker_count": random.randint(1, 8),
            "min_autoscale_workers": random.randint(1, 4),
            "max_autoscale_workers": random.randint(4, 16),
            "auto_termination_minutes": random.randint(30, 120),
            "enable_elastic_disk": random.choice([True, False]),
            "tags": {
                "line_of_business": random.choice(["Analytics", "ML", "Data Engineering", "UNKNOWN"]),
                "department": random.choice(["Data", "Engineering", "Analytics", "UNKNOWN"]),
                "cost_center": random.choice(["CC001", "CC002", "unallocated", "UNKNOWN"]),
                "environment": random.choice(["prod", "dev", "stage", "uat"]),
                "use_case": random.choice(["ETL", "ML Training", "Analytics", "UNKNOWN"]),
                "pipeline_name": f"pipeline_{(i % 3) + 1}",
                "workflow_level": random.choice(["STANDALONE", "PARENT", "SUB_WORKFLOW", "UNKNOWN"]),
                "parent_workflow_name": random.choice(["None", "WF_001", "WF_002", "UNKNOWN"])
            },
            "cluster_source": random.choice(["JOB", "UI", "API"]),
            "init_scripts": [f"script_{i+1}.sh"] if random.random() > 0.5 else [],
            "aws_attributes": '{"instance_profile_arn": "arn:aws:iam::123456789:instance-profile/databricks", "zone_id": "us-west-2a", "first_on_demand": 1, "availability": "SPOT_WITH_FALLBACK", "spot_bid_price_percent": 50, "ebs_volume_type": "gp3", "ebs_volume_count": 1, "ebs_volume_size": 100, "ebs_volume_iops": 3000, "ebs_volume_throughput": 125}',
            "azure_attributes": '{"first_on_demand": 1, "availability": "SPOT_WITH_FALLBACK", "spot_bid_max_price": 0.5}',
            "gcp_attributes": '{"use_preemptible_executors": true, "zone_id": "us-west1-a", "first_on_demand": 1, "availability": "PREEMPTIBLE_WITH_FALLBACK"}',
            "driver_instance_pool_id": f"pool_{i+1}",
            "worker_instance_pool_id": f"pool_{i+1}",
            "dbr_version": random.choice(["13.3.x-scala2.12", "12.2.x-scala2.12", "11.3.x-scala2.12"]),
            "change_time": datetime.now() - timedelta(days=random.randint(1, 10)),
            "change_date": (datetime.now() - timedelta(days=random.randint(1, 10))).date(),
            "data_security_mode": random.choice(["SINGLE_USER", "MULTI_USER"]),
            "policy_id": f"policy_{i+1}",
            "row_hash": str(uuid.uuid4()),
            "_loaded_at": datetime.now()
        })
    
    return spark.createDataFrame(clusters)

def generate_node_types_data(spark):
    """Generate sample node types data"""
    node_types = [
        {"node_type": "i3.xlarge", "core_count": 4.0, "memory_mb": 30720, "gpu_count": 0},
        {"node_type": "i3.2xlarge", "core_count": 8.0, "memory_mb": 61440, "gpu_count": 0},
        {"node_type": "m5.large", "core_count": 2.0, "memory_mb": 8192, "gpu_count": 0},
        {"node_type": "m5.xlarge", "core_count": 4.0, "memory_mb": 16384, "gpu_count": 0},
        {"node_type": "r5.large", "core_count": 2.0, "memory_mb": 16384, "gpu_count": 0},
        {"node_type": "r5.xlarge", "core_count": 4.0, "memory_mb": 32768, "gpu_count": 0},
        {"node_type": "g4dn.xlarge", "core_count": 4.0, "memory_mb": 16384, "gpu_count": 1},
        {"node_type": "g4dn.2xlarge", "core_count": 8.0, "memory_mb": 32768, "gpu_count": 1},
    ]
    
    for node_type in node_types:
        node_type["account_id"] = "123456789"
        node_type["row_hash"] = str(uuid.uuid4())
        node_type["_loaded_at"] = datetime.now()
    
    return spark.createDataFrame(node_types)

def generate_usage_data(spark, num_records=100):
    """Generate sample billing usage data"""
    usage_records = []
    sku_names = ["DBU", "Standard_DBUs", "Premium_DBUs", "Serverless_DBUs", "Compute_DBUs"]
    clouds = ["AWS", "Azure", "GCP"]
    
    for i in range(num_records):
        start_time = datetime.now() - timedelta(hours=random.randint(1, 24))
        end_time = start_time + timedelta(minutes=random.randint(30, 180))
        
        usage_records.append({
            "record_id": str(uuid.uuid4()),
            "account_id": "123456789",
            "workspace_id": f"workspace_{(i % 3) + 1}",
            "sku_name": random.choice(sku_names),
            "cloud": random.choice(clouds),
            "usage_start_time": start_time,
            "usage_end_time": end_time,
            "usage_date": start_time.date(),
            "custom_tags": {
                "line_of_business": random.choice(["Analytics", "ML", "Data Engineering", "UNKNOWN"]),
                "department": random.choice(["Data", "Engineering", "Analytics", "UNKNOWN"]),
                "cost_center": random.choice(["CC001", "CC002", "unallocated", "UNKNOWN"]),
                "environment": random.choice(["prod", "dev", "stage", "uat"]),
                "use_case": random.choice(["ETL", "ML Training", "Analytics", "UNKNOWN"]),
                "pipeline_name": f"pipeline_{(i % 3) + 1}",
                "workflow_level": random.choice(["STANDALONE", "PARENT", "SUB_WORKFLOW", "UNKNOWN"]),
                "parent_workflow_name": random.choice(["None", "WF_001", "WF_002", "UNKNOWN"])
            },
            "usage_unit": "DBU",
            "usage_quantity": round(random.uniform(0.1, 10.0), 6),
            "usage_metadata": {
                "cluster_id": f"cluster_{(i % 4) + 1}",
                "job_id": f"job_{(i % 5) + 1}",
                "warehouse_id": f"warehouse_{(i % 2) + 1}",
                "instance_pool_id": f"pool_{(i % 3) + 1}",
                "node_type": random.choice(["i3.xlarge", "i3.2xlarge", "m5.large", "m5.xlarge"]),
                "job_run_id": f"run_{i+1}",
                "notebook_id": f"notebook_{i+1}",
                "dlt_pipeline_id": f"pipeline_{(i % 3) + 1}",
                "endpoint_name": f"endpoint_{i+1}",
                "endpoint_id": f"endpoint_{i+1}",
                "dlt_update_id": f"update_{i+1}",
                "dlt_maintenance_id": f"maintenance_{i+1}",
                "run_name": f"Run {i+1}",
                "job_name": f"Job {(i % 5) + 1}",
                "notebook_path": f"/Users/user{(i % 5) + 1}/notebook_{i+1}",
                "central_clean_room_id": f"ccr_{i+1}",
                "source_region": "us-west-2",
                "destination_region": "us-west-2",
                "app_id": f"app_{i+1}",
                "app_name": f"App {i+1}",
                "metastore_id": f"metastore_{i+1}",
                "private_endpoint_name": f"pe_{i+1}",
                "storage_api_type": "S3",
                "budget_policy_id": f"budget_{i+1}",
                "ai_runtime_pool_id": f"ai_pool_{i+1}",
                "ai_runtime_workload_id": f"ai_workload_{i+1}",
                "uc_table_catalog": f"catalog_{i+1}",
                "uc_table_schema": f"schema_{i+1}",
                "uc_table_name": f"table_{i+1}",
                "database_instance_id": f"db_{i+1}",
                "sharing_materialization_id": f"sharing_{i+1}",
                "schema_id": f"schema_{i+1}"
            },
            "identity_metadata": {
                "run_as": f"user_{(i % 5) + 1}@company.com",
                "owned_by": f"user_{(i % 5) + 1}@company.com",
                "created_by": f"user_{(i % 5) + 1}@company.com"
            },
            "record_type": "usage",
            "ingestion_date": datetime.now().date(),
            "billing_origin_product": random.choice(["JOBS", "SQL", "DLT", "MODEL_SERVING"]),
            "product_features": {
                "jobs_tier": random.choice(["STANDARD", "PREMIUM"]),
                "sql_tier": random.choice(["STANDARD", "PREMIUM"]),
                "dlt_tier": random.choice(["STANDARD", "PREMIUM"]),
                "is_serverless": random.choice([True, False]),
                "is_photon": random.choice([True, False]),
                "serving_type": random.choice(["STANDARD", "PREMIUM"]),
                "networking": {"connectivity_type": "PUBLIC"},
                "ai_runtime": {"compute_type": "CPU"},
                "model_serving": {"offering_type": "STANDARD"},
                "ai_gateway": {"feature_type": "STANDARD"},
                "serverless_gpu": {"workload_type": "INFERENCE"}
            },
            "usage_type": random.choice(["COMPUTE", "STORAGE", "NETWORK"]),
            "row_hash": str(uuid.uuid4()),
            "_loaded_at": datetime.now()
        })
    
    return spark.createDataFrame(usage_records)

def generate_pricing_data(spark):
    """Generate sample pricing data"""
    pricing_records = []
    sku_names = ["DBU", "Standard_DBUs", "Premium_DBUs", "Serverless_DBUs", "Compute_DBUs"]
    clouds = ["AWS", "Azure", "GCP"]
    
    for cloud in clouds:
        for sku in sku_names:
            pricing_records.append({
                "price_start_time": datetime.now() - timedelta(days=30),
                "price_end_time": datetime.now() + timedelta(days=30),
                "account_id": "123456789",
                "sku_name": sku,
                "cloud": cloud,
                "currency_code": "USD",
                "usage_unit": "DBU",
                "pricing": {
                    "default": round(random.uniform(0.1, 2.0), 4),
                    "promotional": {"default": round(random.uniform(0.05, 1.5), 4)},
                    "effective_list": {"default": round(random.uniform(0.1, 2.0), 4)}
                },
                "row_hash": str(uuid.uuid4()),
                "_loaded_at": datetime.now()
            })
    
    return spark.createDataFrame(pricing_records)

def generate_job_run_data(spark, num_runs=50):
    """Generate sample job run timeline data"""
    job_runs = []
    
    for i in range(num_runs):
        start_time = datetime.now() - timedelta(hours=random.randint(1, 24))
        end_time = start_time + timedelta(minutes=random.randint(10, 120))
        
        job_runs.append({
            "account_id": "123456789",
            "workspace_id": f"workspace_{(i % 3) + 1}",
            "job_id": f"job_{(i % 5) + 1}",
            "run_id": f"run_{i+1}",
            "period_start_time": start_time,
            "period_end_time": end_time,
            "trigger_type": random.choice(["MANUAL", "SCHEDULED", "API"]),
            "run_type": random.choice(["JOB", "WORKFLOW"]),
            "run_name": f"Run {i+1}",
            "compute_ids": [f"cluster_{(i % 4) + 1}"],
            "result_state": random.choice(["SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"]),
            "termination_code": random.choice(["SUCCESS", "INTERNAL_ERROR", "USER_ERROR", "SYSTEM_ERROR"]),
            "job_parameters": {
                "param1": f"value1_{i+1}",
                "param2": f"value2_{i+1}"
            },
            "row_hash": str(uuid.uuid4()),
            "_loaded_at": datetime.now()
        })
    
    return spark.createDataFrame(job_runs)

def generate_task_run_data(spark, num_tasks=100):
    """Generate sample job task run timeline data"""
    task_runs = []
    
    for i in range(num_tasks):
        start_time = datetime.now() - timedelta(hours=random.randint(1, 24))
        end_time = start_time + timedelta(minutes=random.randint(5, 60))
        
        task_runs.append({
            "account_id": "123456789",
            "workspace_id": f"workspace_{(i % 3) + 1}",
            "job_id": f"job_{(i % 5) + 1}",
            "run_id": f"task_run_{i+1}",
            "parent_run_id": f"run_{(i % 50) + 1}",
            "period_start_time": start_time,
            "period_end_time": end_time,
            "task_key": f"task_{i+1}",
            "compute_ids": [f"cluster_{(i % 4) + 1}"],
            "result_state": random.choice(["SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"]),
            "termination_code": random.choice(["SUCCESS", "INTERNAL_ERROR", "USER_ERROR", "SYSTEM_ERROR"]),
            "row_hash": str(uuid.uuid4()),
            "_loaded_at": datetime.now()
        })
    
    return spark.createDataFrame(task_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate and Write Sample Data

# COMMAND ----------

def write_sample_data():
    """Generate and write all sample data to bronze tables"""
    
    print("🚀 Starting sample data generation...")
    
    # Generate data
    print("📊 Generating workspace data...")
    workspace_df = generate_workspace_data(spark)
    
    print("📊 Generating job data...")
    job_df = generate_job_data(spark)
    
    print("📊 Generating pipeline data...")
    pipeline_df = generate_pipeline_data(spark)
    
    print("📊 Generating cluster data...")
    cluster_df = generate_cluster_data(spark)
    
    print("📊 Generating node types data...")
    node_types_df = generate_node_types_data(spark)
    
    print("📊 Generating usage data...")
    usage_df = generate_usage_data(spark, 200)  # Generate 200 usage records
    
    print("📊 Generating pricing data...")
    pricing_df = generate_pricing_data(spark)
    
    print("📊 Generating job run data...")
    job_run_df = generate_job_run_data(spark, 100)  # Generate 100 job runs
    
    print("📊 Generating task run data...")
    task_run_df = generate_task_run_data(spark, 200)  # Generate 200 task runs
    
    # Write to bronze tables
    bronze_schema = config.bronze_schema
    catalog = config.catalog
    
    print(f"💾 Writing data to {catalog}.{bronze_schema}...")
    
    # Write workspace data
    workspace_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_access_workspaces_latest")
    
    # Write job data
    job_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_lakeflow_jobs")
    
    # Write pipeline data
    pipeline_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_lakeflow_pipelines")
    
    # Write cluster data
    cluster_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_compute_clusters")
    
    # Write node types data
    node_types_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_compute_node_types")
    
    # Write usage data
    usage_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_billing_usage")
    
    # Write pricing data
    pricing_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_billing_list_prices")
    
    # Write job run data
    job_run_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_lakeflow_job_run_timeline")
    
    # Write task run data
    task_run_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_lakeflow_job_task_run_timeline")
    
    print("✅ Sample data generation completed successfully!")
    
    # Print summary
    print("\n📈 Data Summary:")
    print(f"  - Workspaces: {workspace_df.count()}")
    print(f"  - Jobs: {job_df.count()}")
    print(f"  - Pipelines: {pipeline_df.count()}")
    print(f"  - Clusters: {cluster_df.count()}")
    print(f"  - Node Types: {node_types_df.count()}")
    print(f"  - Usage Records: {usage_df.count()}")
    print(f"  - Pricing Records: {pricing_df.count()}")
    print(f"  - Job Runs: {job_run_df.count()}")
    print(f"  - Task Runs: {task_run_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Data Generation

# COMMAND ----------

if __name__ == "__main__":
    try:
        print("🎯 Sample DLT Pipeline for Platform Observability Testing")
        print("=" * 60)
        print(f"📅 Execution Time: {datetime.now()}")
        print(f"📊 Target Schema: {config.catalog}.{config.bronze_schema}")
        print("=" * 60)
        
        # Generate and write sample data
        write_sample_data()
        
        print("\n🎉 Sample data generation completed successfully!")
        print("💡 Run this notebook daily for a few days to accumulate test data")
        print("📋 Next steps:")
        print("  1. Run the bronze HWM ingest job to process the data")
        print("  2. Run the silver HWM build job to transform the data")
        print("  3. Run the gold HWM build job to create dimensions and facts")
        
    except Exception as e:
        print(f"❌ Error generating sample data: {str(e)}")
        import traceback
        traceback.print_exc()
