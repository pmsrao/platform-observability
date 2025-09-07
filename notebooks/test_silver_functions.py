# Databricks notebook source
# MAGIC %md
# MAGIC # Test Individual Silver Layer Functions
# MAGIC 
# MAGIC This notebook tests each Silver Layer function individually to isolate issues.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Test each Silver function separately
# MAGIC - Isolate specific function failures
# MAGIC - Provide detailed error information
# MAGIC - Help identify which functions are working vs failing
# MAGIC 
# MAGIC ## Usage:
# MAGIC Run this notebook to test individual Silver Layer functions before running the full pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
import os

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
logger = StructuredLogger("silver_test")

print("🧪 Individual Silver Function Tests Initialized")
print(f"Catalog: {config.catalog}")
print(f"Bronze Schema: {config.bronze_schema}")
print(f"Silver Schema: {config.silver_schema}")
print(f"Environment: {Config.ENV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Silver Functions
# MAGIC 
# MAGIC Note: In Databricks, you can import functions from other notebooks using %run

# COMMAND ----------

# Import the Silver Layer functions
# This will import all the build_silver_* functions from the main notebook
%run /Workspace/Users/podilapalls@gmail.com/platform-observability/notebooks/silver_hwm_build_job

print("✅ Silver Layer functions imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Workspace Function

# COMMAND ----------

logger.info("Test 1: Testing build_silver_workspace...")
try:
    result = build_silver_workspace(spark)
    if result:
        logger.info("✅ Workspace function test completed successfully")
        print("✅ Workspace function test completed successfully")
    else:
        logger.error("❌ Workspace function returned False")
        print("❌ Workspace function returned False")
except Exception as e:
    logger.error(f"❌ Workspace function failed: {str(e)}")
    print(f"❌ Workspace function failed: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Entity Latest Function

# COMMAND ----------

logger.info("Test 2: Testing build_silver_entity_latest...")
try:
    result = build_silver_entity_latest(spark)
    if result:
        logger.info("✅ Entity Latest function test completed successfully")
        print("✅ Entity Latest function test completed successfully")
    else:
        logger.error("❌ Entity Latest function returned False")
        print("❌ Entity Latest function returned False")
except Exception as e:
    logger.error(f"❌ Entity Latest function failed: {str(e)}")
    print(f"❌ Entity Latest function failed: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Clusters Function

# COMMAND ----------

logger.info("Test 3: Testing build_silver_clusters...")
try:
    result = build_silver_clusters(spark)
    if result:
        logger.info("✅ Clusters function test completed successfully")
        print("✅ Clusters function test completed successfully")
    else:
        logger.error("❌ Clusters function returned False")
        print("❌ Clusters function returned False")
except Exception as e:
    logger.error(f"❌ Clusters function failed: {str(e)}")
    print(f"❌ Clusters function failed: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Usage Transaction Function

# COMMAND ----------

logger.info("Test 4: Testing build_silver_usage_txn...")
try:
    result = build_silver_usage_txn(spark)
    if result:
        logger.info("✅ Usage Transaction function test completed successfully")
        print("✅ Usage Transaction function test completed successfully")
    else:
        logger.error("❌ Usage Transaction function returned False")
        print("❌ Usage Transaction function returned False")
except Exception as e:
    logger.error(f"❌ Usage Transaction function failed: {str(e)}")
    print(f"❌ Usage Transaction function failed: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Job Run Timeline Function

# COMMAND ----------

logger.info("Test 5: Testing build_silver_job_run_timeline...")
try:
    result = build_silver_job_run_timeline(spark)
    if result:
        logger.info("✅ Job Run Timeline function test completed successfully")
        print("✅ Job Run Timeline function test completed successfully")
    else:
        logger.error("❌ Job Run Timeline function returned False")
        print("❌ Job Run Timeline function returned False")
except Exception as e:
    logger.error(f"❌ Job Run Timeline function failed: {str(e)}")
    print(f"❌ Job Run Timeline function failed: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Job Task Run Timeline Function

# COMMAND ----------

logger.info("Test 6: Testing build_silver_job_task_run_timeline...")
try:
    result = build_silver_job_task_run_timeline(spark)
    if result:
        logger.info("✅ Job Task Run Timeline function test completed successfully")
        print("✅ Job Task Run Timeline function test completed successfully")
    else:
        logger.error("❌ Job Task Run Timeline function returned False")
        print("❌ Job Task Run Timeline function returned False")
except Exception as e:
    logger.error(f"❌ Job Task Run Timeline function failed: {str(e)}")
    print(f"❌ Job Task Run Timeline function failed: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary

# COMMAND ----------

print("🎯 Individual Silver Function Test Summary")
print("=" * 50)

# Note: In a real implementation, you would track the results from each test
# For now, this provides a framework for testing individual functions

print("📊 Test Results:")
print("   - Workspace Function: Check output above")
print("   - Entity Latest Function: Check output above") 
print("   - Clusters Function: Check output above")
print("   - Usage Transaction Function: Check output above")
print("   - Job Run Timeline Function: Check output above")
print("   - Job Task Run Timeline Function: Check output above")

print("\n💡 Next Steps:")
print("1. Review the output from each test above")
print("2. If any function failed, check the error details")
print("3. Fix the issues before running the full Silver Layer")
print("4. Use the error viewer notebook to get more detailed error information")

print("\n🔍 For detailed error analysis:")
print("   Run: %run /Workspace/Users/podilapalls@gmail.com/platform-observability/notebooks/view_silver_errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Test All Functions at Once

# COMMAND ----------

# Uncomment and run this cell to test all functions in sequence
# def test_all_functions():
#     """Test all Silver Layer functions in sequence"""
#     functions = [
#         ("workspace", build_silver_workspace),
#         ("entity_latest", build_silver_entity_latest),
#         ("clusters", build_silver_clusters),
#         ("usage_txn", build_silver_usage_txn),
#         ("job_run_timeline", build_silver_job_run_timeline),
#         ("job_task_run_timeline", build_silver_job_task_run_timeline)
#     ]
#     
#     results = {}
#     for name, func in functions:
#         try:
#             result = func(spark)
#             results[name] = result
#             status = "✅ PASSED" if result else "❌ FAILED"
#             print(f"{name}: {status}")
#         except Exception as e:
#             results[name] = False
#             print(f"{name}: ❌ ERROR - {str(e)}")
#     
#     return results

# results = test_all_functions()
print("💡 To test all functions at once, uncomment and run the test_all_functions() above")