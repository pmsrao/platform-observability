#!/usr/bin/env python3
"""
Error Capture Utilities for Databricks Notebooks
This module provides utilities to capture and persist error information
that might be lost when notebooks exit.
"""

import sys
import os
import json
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

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

class ErrorCapture:
    """Capture and persist error information for debugging"""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.config = Config.get_config()
        self.logger = StructuredLogger("error_capture")
        self.errors = []
        
        # Create error logging table if it doesn't exist
        self._create_error_table()
    
    def _create_error_table(self):
        """Create error logging table if it doesn't exist"""
        try:
            if self.spark:
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.config.catalog}.{self.config.bronze_schema}.error_logs (
                    error_id STRING,
                    timestamp TIMESTAMP,
                    notebook_name STRING,
                    function_name STRING,
                    error_type STRING,
                    error_message STRING,
                    stack_trace STRING,
                    context_data STRING,
                    environment STRING
                ) USING DELTA
                """
                self.spark.sql(create_table_sql)
                self.logger.info("Error logging table created/verified")
        except Exception as e:
            self.logger.warning(f"Could not create error table: {str(e)}")
    
    def capture_error(self, 
                     function_name: str, 
                     error: Exception, 
                     context: Optional[Dict[str, Any]] = None):
        """Capture error information"""
        error_id = f"error_{int(datetime.now().timestamp() * 1000)}"
        
        error_info = {
            "error_id": error_id,
            "timestamp": datetime.now().isoformat(),
            "notebook_name": "silver_hwm_build_job",
            "function_name": function_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": traceback.format_exc(),
            "context_data": json.dumps(context or {}),
            "environment": Config.ENV
        }
        
        self.errors.append(error_info)
        
        # Log to structured logger
        self.logger.error(f"Error captured in {function_name}: {str(error)}", 
                         error_id=error_id,
                         function_name=function_name,
                         error_type=type(error).__name__)
        
        # Also print to console for immediate visibility
        print(f"🚨 ERROR CAPTURED: {function_name}")
        print(f"   Error ID: {error_id}")
        print(f"   Type: {type(error).__name__}")
        print(f"   Message: {str(error)}")
        print(f"   Stack Trace: {traceback.format_exc()}")
        
        return error_id
    
    def save_errors_to_table(self):
        """Save captured errors to the error logging table"""
        if not self.spark or not self.errors:
            return
        
        try:
            # Convert errors to DataFrame
            error_data = []
            for error in self.errors:
                error_data.append((
                    error["error_id"],
                    error["timestamp"],
                    error["notebook_name"],
                    error["function_name"],
                    error["error_type"],
                    error["error_message"],
                    error["stack_trace"],
                    error["context_data"],
                    error["environment"]
                ))
            
            # Create DataFrame and write to table
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            from pyspark.sql.functions import to_timestamp
            
            schema = StructType([
                StructField("error_id", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("notebook_name", StringType(), True),
                StructField("function_name", StringType(), True),
                StructField("error_type", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("stack_trace", StringType(), True),
                StructField("context_data", StringType(), True),
                StructField("environment", StringType(), True)
            ])
            
            error_df = self.spark.createDataFrame(error_data, schema)
            error_df = error_df.withColumn("timestamp", to_timestamp("timestamp"))
            
            # Write to error table
            table_name = f"{self.config.catalog}.{self.config.bronze_schema}.error_logs"
            error_df.write.mode("append").saveAsTable(table_name)
            
            self.logger.info(f"Saved {len(self.errors)} errors to error_logs table")
            
        except Exception as e:
            self.logger.error(f"Failed to save errors to table: {str(e)}")
    
    def save_errors_to_file(self, file_path: str = "/tmp/silver_errors.json"):
        """Save captured errors to a JSON file"""
        try:
            with open(file_path, 'w') as f:
                json.dump(self.errors, f, indent=2)
            self.logger.info(f"Saved {len(self.errors)} errors to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to save errors to file: {str(e)}")
    
    def get_recent_errors(self, hours: int = 24):
        """Get recent errors from the error logging table"""
        if not self.spark:
            return []
        
        try:
            table_name = f"{self.config.catalog}.{self.config.bronze_schema}.error_logs"
            recent_errors = self.spark.sql(f"""
                SELECT * FROM {table_name}
                WHERE timestamp >= current_timestamp() - INTERVAL {hours} HOURS
                ORDER BY timestamp DESC
            """).collect()
            
            return [row.asDict() for row in recent_errors]
        except Exception as e:
            self.logger.error(f"Failed to get recent errors: {str(e)}")
            return []

def safe_execute_with_capture(error_capture: ErrorCapture, function_name: str):
    """Decorator to safely execute functions and capture errors"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_capture.capture_error(function_name, e, {
                    "args": str(args)[:500],  # Truncate long args
                    "kwargs": str(kwargs)[:500]
                })
                raise
        return wrapper
    return decorator
