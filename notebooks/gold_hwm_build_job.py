# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer HWM Build Job
# MAGIC
# MAGIC This notebook builds the Gold layer using proper star schema design with surrogate keys.

# COMMAND ----------

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


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
from libs.logging import StructuredLogger
from gold_dimension_builder import DimensionBuilderFactory
from gold_fact_builder import FactBuilderFactory
from gold_view_builder import ViewBuilderFactory

# COMMAND ----------

# Initialize
config = Config.get_config()
logger = StructuredLogger("gold_hwm_build_job")
#spark = SparkSession.builder.appName("Gold Layer HWM Build").getOrCreate()

# COMMAND ----------

def build_gold_layer():
    """Build entire Gold layer"""
    results = {"success": False}
    
    try:
        # Build dimensions
        dimension_types = ["workspace", "entity", "cluster", "sku", "run_status", "node_type", "date"]
        for dim_type in dimension_types:
            builder = DimensionBuilderFactory.create_builder(dim_type, spark, config)
            if dim_type == "date":
                builder.build(start_date="2020-01-01", end_date="2030-12-31")
            else:
                builder.build()
        
        # Build facts
        fact_types = ["usage", "entity_cost", "run_cost", "run_status_cost", "runs_finished"]
        for fact_type in fact_types:
            builder = FactBuilderFactory.create_builder(fact_type, spark, config)
            builder.build()
        
        # Build views
        view_types = ["chargeback", "runtime_analysis"]
        for view_type in view_types:
            builder = ViewBuilderFactory.create_builder(view_type, spark, config)
            builder.build_all()
        
        results["success"] = True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        results["error"] = str(e)
    
    return results

# COMMAND ----------

# Execute build
build_results = build_gold_layer()

if build_results["success"]:
    print("✅ Gold Layer Build completed successfully!")
    print("📊 Star schema with surrogate keys implemented")
else:
    print("❌ Gold Layer Build failed")
    if "error" in build_results:
        print(f"Error: {build_results['error']}")
