# Databricks notebook source
# MAGIC %md
# MAGIC # Simple DLT Pipeline for Testing
# MAGIC
# MAGIC This is a simple DLT pipeline that generates only billing usage data
# MAGIC for testing the platform observability system.

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
# MAGIC ## Generate Usage Data

# COMMAND ----------

def generate_usage_data(spark, num_records=50):
    """Generate sample billing usage data"""
    usage_records = []
    sku_names = ["DBU", "Standard_DBUs", "Premium_DBUs", "Serverless_DBUs"]
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Data to Bronze Table

# COMMAND ----------

def write_usage_data():
    """Generate and write usage data to bronze table"""
    
    print("🚀 Starting usage data generation...")
    
    # Generate usage data
    print("📊 Generating usage data...")
    usage_df = generate_usage_data(spark, 50)  # Generate 50 usage records
    
    # Write to bronze table
    bronze_schema = config.bronze_schema
    catalog = config.catalog
    
    print(f"💾 Writing data to {catalog}.{bronze_schema}.brz_billing_usage...")
    
    # Write usage data
    usage_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{bronze_schema}.brz_billing_usage")
    
    print("✅ Usage data generation completed successfully!")
    print(f"📈 Generated {usage_df.count()} usage records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Data Generation

# COMMAND ----------

if __name__ == "__main__":
    try:
        print("🎯 Simple DLT Pipeline for Testing")
        print("=" * 40)
        print(f"📅 Execution Time: {datetime.now()}")
        print(f"📊 Target Table: {config.catalog}.{config.bronze_schema}.brz_billing_usage")
        print("=" * 40)
        
        # Generate and write usage data
        write_usage_data()
        
        print("\n🎉 Sample data generation completed successfully!")
        print("💡 Run this notebook daily for a few days to accumulate test data")
        
    except Exception as e:
        print(f"❌ Error generating sample data: {str(e)}")
        import traceback
        traceback.print_exc()
