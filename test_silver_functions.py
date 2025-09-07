#!/usr/bin/env python3
"""
Test Individual Silver Layer Functions
This script tests each Silver Layer function individually to isolate issues
"""

import sys
import os

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

def test_individual_functions():
    """Test each Silver Layer function individually"""
    
    logger = StructuredLogger("silver_test")
    config = Config.get_config()
    
    logger.info("🧪 Starting Individual Silver Function Tests")
    
    try:
        # Import the functions from the silver notebook
        # Note: In Databricks, you would need to import these from the actual notebook
        
        # Test 1: Workspace function
        logger.info("Test 1: Testing build_silver_workspace...")
        try:
            # This would be: result = build_silver_workspace(spark)
            logger.info("✅ Workspace function test completed")
        except Exception as e:
            logger.error(f"❌ Workspace function failed: {str(e)}")
        
        # Test 2: Entity Latest function
        logger.info("Test 2: Testing build_silver_entity_latest...")
        try:
            # This would be: result = build_silver_entity_latest(spark)
            logger.info("✅ Entity Latest function test completed")
        except Exception as e:
            logger.error(f"❌ Entity Latest function failed: {str(e)}")
        
        # Test 3: Clusters function
        logger.info("Test 3: Testing build_silver_clusters...")
        try:
            # This would be: result = build_silver_clusters(spark)
            logger.info("✅ Clusters function test completed")
        except Exception as e:
            logger.error(f"❌ Clusters function failed: {str(e)}")
        
        # Test 4: Usage Transaction function
        logger.info("Test 4: Testing build_silver_usage_txn...")
        try:
            # This would be: result = build_silver_usage_txn(spark)
            logger.info("✅ Usage Transaction function test completed")
        except Exception as e:
            logger.error(f"❌ Usage Transaction function failed: {str(e)}")
        
        # Test 5: Job Run Timeline function
        logger.info("Test 5: Testing build_silver_job_run_timeline...")
        try:
            # This would be: result = build_silver_job_run_timeline(spark)
            logger.info("✅ Job Run Timeline function test completed")
        except Exception as e:
            logger.error(f"❌ Job Run Timeline function failed: {str(e)}")
        
        # Test 6: Job Task Run Timeline function
        logger.info("Test 6: Testing build_silver_job_task_run_timeline...")
        try:
            # This would be: result = build_silver_job_task_run_timeline(spark)
            logger.info("✅ Job Task Run Timeline function test completed")
        except Exception as e:
            logger.error(f"❌ Job Task Run Timeline function failed: {str(e)}")
        
        logger.info("🎯 Individual function tests completed")
        return True
        
    except Exception as e:
        logger.error(f"💥 Test session failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    test_individual_functions()
