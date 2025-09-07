# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Error Viewer
# MAGIC 
# MAGIC This notebook helps view captured errors from the Silver Layer execution.
# MAGIC 
# MAGIC ## Features:
# MAGIC - View errors from error_logs table
# MAGIC - View errors from JSON backup file
# MAGIC - Formatted error display with stack traces
# MAGIC - Filter by function name or error type

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
import os
import json
from datetime import datetime, timedelta

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
logger = StructuredLogger("view_errors")

print("🔍 Silver Layer Error Viewer Initialized")
print(f"Catalog: {config.catalog}")
print(f"Bronze Schema: {config.bronze_schema}")
print(f"Environment: {Config.ENV}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Errors from Error Logs Table

# COMMAND ----------

def view_errors_from_table(hours_back=24):
    """View errors from the error_logs table"""
    try:
        table_name = f"{config.catalog}.{config.bronze_schema}.error_logs"
        
        print(f"🔍 Recent Silver Layer Errors (last {hours_back} hours):")
        print("=" * 60)
        
        # Query recent errors
        recent_errors = spark.sql(f"""
            SELECT 
                error_id,
                timestamp,
                function_name,
                error_type,
                error_message,
                stack_trace,
                context_data
            FROM {table_name}
            WHERE notebook_name = 'silver_hwm_build_job'
            AND timestamp >= current_timestamp() - INTERVAL {hours_back} HOURS
            ORDER BY timestamp DESC
            LIMIT 20
        """).collect()
        
        if not recent_errors:
            print("✅ No errors found in error_logs table")
            return []
        
        for i, error in enumerate(recent_errors, 1):
            print(f"\n🚨 Error #{i}")
            print(f"   ID: {error.error_id}")
            print(f"   Time: {error.timestamp}")
            print(f"   Function: {error.function_name}")
            print(f"   Type: {error.error_type}")
            print(f"   Message: {error.error_message}")
            if error.context_data and error.context_data != '{}':
                print(f"   Context: {error.context_data}")
            print(f"   Stack Trace:")
            # Format stack trace for better readability
            stack_lines = error.stack_trace.split('\n')
            for line in stack_lines:
                print(f"   {line}")
            print("-" * 60)
        
        return recent_errors
        
    except Exception as e:
        print(f"❌ Error querying error_logs table: {str(e)}")
        return []

# View errors from table
table_errors = view_errors_from_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Errors from JSON File

# COMMAND ----------

def view_errors_from_file():
    """View errors from the JSON file"""
    try:
        print("\n🔍 Errors from JSON file:")
        print("=" * 60)
        
        with open('/tmp/silver_errors.json', 'r') as f:
            errors = json.load(f)
        
        if not errors:
            print("✅ No errors found in JSON file")
            return []
        
        for i, error in enumerate(errors, 1):
            print(f"\n🚨 Error #{i}")
            print(f"   ID: {error['error_id']}")
            print(f"   Time: {error['timestamp']}")
            print(f"   Function: {error['function_name']}")
            print(f"   Type: {error['error_type']}")
            print(f"   Message: {error['error_message']}")
            if error.get('context_data') and error['context_data'] != '{}':
                print(f"   Context: {error['context_data']}")
            print(f"   Stack Trace:")
            # Format stack trace for better readability
            stack_lines = error['stack_trace'].split('\n')
            for line in stack_lines:
                print(f"   {line}")
            print("-" * 60)
        
        return errors
        
    except FileNotFoundError:
        print("❌ Error file not found at /tmp/silver_errors.json")
        return []
    except Exception as e:
        print(f"❌ Error reading JSON file: {str(e)}")
        return []

# View errors from file
file_errors = view_errors_from_file()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Summary and Analysis

# COMMAND ----------

def analyze_errors():
    """Analyze errors and provide summary"""
    print("\n📊 Error Analysis Summary:")
    print("=" * 60)
    
    # Combine errors from both sources
    all_errors = []
    
    # Add table errors
    for error in table_errors:
        all_errors.append({
            'function_name': error.function_name,
            'error_type': error.error_type,
            'error_message': error.error_message,
            'timestamp': error.timestamp
        })
    
    # Add file errors (avoid duplicates)
    file_error_ids = {error['error_id'] for error in file_errors}
    for error in file_errors:
        if error['error_id'] not in file_error_ids:
            all_errors.append({
                'function_name': error['function_name'],
                'error_type': error['error_type'],
                'error_message': error['error_message'],
                'timestamp': error['timestamp']
            })
    
    if not all_errors:
        print("✅ No errors found")
        return
    
    # Count errors by function
    function_counts = {}
    error_type_counts = {}
    
    for error in all_errors:
        func = error['function_name']
        error_type = error['error_type']
        
        function_counts[func] = function_counts.get(func, 0) + 1
        error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
    
    print(f"Total Errors Found: {len(all_errors)}")
    print(f"\n📈 Errors by Function:")
    for func, count in sorted(function_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"   {func}: {count} errors")
    
    print(f"\n📈 Errors by Type:")
    for error_type, count in sorted(error_type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"   {error_type}: {count} errors")
    
    # Show most recent error
    if all_errors:
        most_recent = max(all_errors, key=lambda x: x['timestamp'])
        print(f"\n🕐 Most Recent Error:")
        print(f"   Function: {most_recent['function_name']}")
        print(f"   Type: {most_recent['error_type']}")
        print(f"   Message: {most_recent['error_message']}")
        print(f"   Time: {most_recent['timestamp']}")

# Analyze errors
analyze_errors()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Error Queries

# COMMAND ----------

# Query specific function errors
def query_function_errors(function_name):
    """Query errors for a specific function"""
    try:
        table_name = f"{config.catalog}.{config.bronze_schema}.error_logs"
        
        errors = spark.sql(f"""
            SELECT * FROM {table_name}
            WHERE notebook_name = 'silver_hwm_build_job'
            AND function_name = '{function_name}'
            ORDER BY timestamp DESC
            LIMIT 5
        """).collect()
        
        if errors:
            print(f"🔍 Errors for function '{function_name}':")
            for error in errors:
                print(f"   {error.timestamp}: {error.error_message}")
        else:
            print(f"✅ No errors found for function '{function_name}'")
            
    except Exception as e:
        print(f"❌ Error querying function errors: {str(e)}")

# Example: Query errors for specific functions
print("🔍 Checking errors for common functions:")
common_functions = [
    'build_silver_workspace',
    'build_silver_entity_latest', 
    'build_silver_clusters',
    'build_silver_usage_txn',
    'build_silver_job_run_timeline',
    'build_silver_job_task_run_timeline'
]

for func in common_functions:
    query_function_errors(func)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Old Errors (Optional)

# COMMAND ----------

# Uncomment and run this cell to clear old errors (older than 7 days)
# def clear_old_errors():
#     try:
#         table_name = f"{config.catalog}.{config.bronze_schema}.error_logs"
#         
#         result = spark.sql(f"""
#             DELETE FROM {table_name}
#             WHERE timestamp < current_timestamp() - INTERVAL 7 DAYS
#         """)
#         
#         print("✅ Old errors cleared")
#     except Exception as e:
#         print(f"❌ Error clearing old errors: {str(e)}")

# clear_old_errors()
print("💡 To clear old errors, uncomment and run the clear_old_errors() function above")
