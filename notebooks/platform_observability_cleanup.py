# Databricks notebook source
# MAGIC %md
# MAGIC # Platform Observability Cleanup Script
# MAGIC 
# MAGIC This notebook provides comprehensive cleanup functionality for the Platform Observability system.
# MAGIC It can remove tables, schemas, and even the entire catalog if needed.
# MAGIC 
# MAGIC ## ⚠️ WARNING: This script will permanently delete data!
# MAGIC 
# MAGIC ## Features:
# MAGIC - Clean up individual tables
# MAGIC - Clean up entire schemas (Bronze, Silver, Gold)
# MAGIC - Clean up the entire catalog
# MAGIC - Selective cleanup with confirmation prompts
# MAGIC - Dry-run mode to preview what will be deleted
# MAGIC - Comprehensive logging
# MAGIC 
# MAGIC ## Usage:
# MAGIC 1. Set the cleanup scope (tables, schemas, or catalog)
# MAGIC 2. Choose dry-run mode for safety
# MAGIC 3. Execute cleanup with confirmation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
import os
from datetime import datetime
from typing import Dict, List, Optional, Set

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Import from libs package (cloud-agnostic approach)
from libs.path_setup import setup_paths_and_import_config

# Setup paths and import Config
Config = setup_paths_and_import_config()
from libs.logging import StructuredLogger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("platform_observability_cleanup")

logger.info("Platform Observability Cleanup Script initialized", 
    catalog=config.catalog,
    bronze_schema=config.bronze_schema,
    silver_schema=config.silver_schema,
    gold_schema=config.gold_schema,
    environment=Config.ENV
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Configuration

# COMMAND ----------

# Cleanup scope configuration
CLEANUP_SCOPE = "schemas"  # Options: "tables", "schemas", "catalog"
DRY_RUN = True  # Set to False to actually perform cleanup
CONFIRM_DELETION = True  # Set to False to skip confirmation prompts

# Table patterns to exclude from cleanup (if using table-level cleanup)
EXCLUDE_PATTERNS = [
    "_processing_offsets",  # Keep processing state tables
    "_hwm_",               # Keep HWM tables
    "_cdf_"                # Keep CDF tables
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_all_tables_in_schema(spark: SparkSession, catalog: str, schema: str) -> List[str]:
    """Get all tables in a schema"""
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
        return [row.tableName for row in tables_df.collect()]
    except Exception as e:
        logger.warning(f"Could not list tables in {catalog}.{schema}: {str(e)}")
        return []

def get_all_schemas_in_catalog(spark: SparkSession, catalog: str) -> List[str]:
    """Get all schemas in a catalog"""
    try:
        schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
        return [row.databaseName for row in schemas_df.collect()]
    except Exception as e:
        logger.warning(f"Could not list schemas in {catalog}: {str(e)}")
        return []

def should_exclude_table(table_name: str, exclude_patterns: List[str]) -> bool:
    """Check if table should be excluded from cleanup"""
    return any(pattern in table_name for pattern in exclude_patterns)

def confirm_deletion(message: str) -> bool:
    """Ask for user confirmation before deletion"""
    if not CONFIRM_DELETION:
        return True
    
    print(f"\n⚠️  {message}")
    response = input("Type 'DELETE' to confirm (case sensitive): ")
    return response == "DELETE"

def log_cleanup_action(action: str, target: str, dry_run: bool = True):
    """Log cleanup actions"""
    mode = "DRY RUN" if dry_run else "EXECUTING"
    logger.info(f"{mode}: {action} - {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Functions

# COMMAND ----------

def cleanup_tables(spark: SparkSession, dry_run: bool = True) -> Dict[str, List[str]]:
    """Clean up individual tables"""
    logger.info("Starting table-level cleanup")
    
    results = {
        "deleted": [],
        "skipped": [],
        "errors": []
    }
    
    schemas = [
        (config.bronze_schema, "Bronze"),
        (config.silver_schema, "Silver"), 
        (config.gold_schema, "Gold")
    ]
    
    for schema_name, layer_name in schemas:
        logger.info(f"Processing {layer_name} layer tables in {config.catalog}.{schema_name}")
        
        tables = get_all_tables_in_schema(spark, config.catalog, schema_name)
        
        for table_name in tables:
            full_table_name = f"{config.catalog}.{schema_name}.{table_name}"
            
            # Check if table should be excluded
            if should_exclude_table(table_name, EXCLUDE_PATTERNS):
                results["skipped"].append(full_table_name)
                log_cleanup_action("SKIPPING (excluded)", full_table_name, dry_run)
                continue
            
            # Confirm deletion for each table
            if not confirm_deletion(f"Delete table {full_table_name}?"):
                results["skipped"].append(full_table_name)
                log_cleanup_action("SKIPPING (user declined)", full_table_name, dry_run)
                continue
            
            try:
                if not dry_run:
                    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                    results["deleted"].append(full_table_name)
                    log_cleanup_action("DELETED", full_table_name, False)
                else:
                    results["deleted"].append(full_table_name)
                    log_cleanup_action("WOULD DELETE", full_table_name, True)
                    
            except Exception as e:
                error_msg = f"Error deleting {full_table_name}: {str(e)}"
                results["errors"].append(error_msg)
                logger.error(error_msg)
    
    return results

def cleanup_schemas(spark: SparkSession, dry_run: bool = True) -> Dict[str, List[str]]:
    """Clean up entire schemas"""
    logger.info("Starting schema-level cleanup")
    
    results = {
        "deleted": [],
        "skipped": [],
        "errors": []
    }
    
    schemas = [
        (config.bronze_schema, "Bronze"),
        (config.silver_schema, "Silver"),
        (config.gold_schema, "Gold")
    ]
    
    for schema_name, layer_name in schemas:
        full_schema_name = f"{config.catalog}.{schema_name}"
        
        # Check if schema exists
        existing_schemas = get_all_schemas_in_catalog(spark, config.catalog)
        if schema_name not in existing_schemas:
            results["skipped"].append(full_schema_name)
            log_cleanup_action("SKIPPING (doesn't exist)", full_schema_name, dry_run)
            continue
        
        # Confirm deletion
        if not confirm_deletion(f"Delete entire {layer_name} schema {full_schema_name} and ALL its tables?"):
            results["skipped"].append(full_schema_name)
            log_cleanup_action("SKIPPING (user declined)", full_schema_name, dry_run)
            continue
        
        try:
            if not dry_run:
                spark.sql(f"DROP SCHEMA IF EXISTS {full_schema_name} CASCADE")
                results["deleted"].append(full_schema_name)
                log_cleanup_action("DELETED", full_schema_name, False)
            else:
                results["deleted"].append(full_schema_name)
                log_cleanup_action("WOULD DELETE", full_schema_name, True)
                
        except Exception as e:
            error_msg = f"Error deleting {full_schema_name}: {str(e)}"
            results["errors"].append(error_msg)
            logger.error(error_msg)
    
    return results

def cleanup_catalog(spark: SparkSession, dry_run: bool = True) -> Dict[str, List[str]]:
    """Clean up entire catalog"""
    logger.info("Starting catalog-level cleanup")
    
    results = {
        "deleted": [],
        "skipped": [],
        "errors": []
    }
    
    # Confirm deletion
    if not confirm_deletion(f"Delete ENTIRE catalog {config.catalog} and ALL its schemas and tables?"):
        results["skipped"].append(config.catalog)
        log_cleanup_action("SKIPPING (user declined)", config.catalog, dry_run)
        return results
    
    try:
        if not dry_run:
            spark.sql(f"DROP CATALOG IF EXISTS {config.catalog} CASCADE")
            results["deleted"].append(config.catalog)
            log_cleanup_action("DELETED", config.catalog, False)
        else:
            results["deleted"].append(config.catalog)
            log_cleanup_action("WOULD DELETE", config.catalog, True)
            
    except Exception as e:
        error_msg = f"Error deleting {config.catalog}: {str(e)}"
        results["errors"].append(error_msg)
        logger.error(error_msg)
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Cleanup Execution

# COMMAND ----------

def execute_cleanup(spark: SparkSession) -> Dict[str, List[str]]:
    """Execute cleanup based on configuration"""
    
    logger.info(f"Starting cleanup with scope: {CLEANUP_SCOPE}, dry_run: {DRY_RUN}")
    
    if CLEANUP_SCOPE == "tables":
        return cleanup_tables(spark, DRY_RUN)
    elif CLEANUP_SCOPE == "schemas":
        return cleanup_schemas(spark, DRY_RUN)
    elif CLEANUP_SCOPE == "catalog":
        return cleanup_catalog(spark, DRY_RUN)
    else:
        raise ValueError(f"Invalid cleanup scope: {CLEANUP_SCOPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Cleanup

# COMMAND ----------

if __name__ == "__main__":
    # Execute cleanup
    results = execute_cleanup(spark)
    
    # Print summary
    print("\n" + "="*80)
    print("CLEANUP SUMMARY")
    print("="*80)
    
    mode = "DRY RUN" if DRY_RUN else "EXECUTED"
    print(f"Mode: {mode}")
    print(f"Scope: {CLEANUP_SCOPE}")
    print(f"Timestamp: {datetime.now()}")
    
    print(f"\n✅ Deleted/Scheduled for deletion: {len(results['deleted'])}")
    for item in results['deleted']:
        print(f"  - {item}")
    
    print(f"\n⏭️  Skipped: {len(results['skipped'])}")
    for item in results['skipped']:
        print(f"  - {item}")
    
    print(f"\n❌ Errors: {len(results['errors'])}")
    for error in results['errors']:
        print(f"  - {error}")
    
    print("\n" + "="*80)
    
    # Log final results
    logger.info("Cleanup completed", 
        mode=mode,
        scope=CLEANUP_SCOPE,
        deleted_count=len(results['deleted']),
        skipped_count=len(results['skipped']),
        error_count=len(results['errors'])
    )
    
    if DRY_RUN:
        print("\n🔍 This was a DRY RUN. No data was actually deleted.")
        print("Set DRY_RUN = False to perform actual cleanup.")
    else:
        print("\n🗑️  Cleanup completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Cleanup Commands
# MAGIC 
# MAGIC ### For Development/Testing:
# MAGIC ```python
# MAGIC # Clean up all schemas (dry run)
# MAGIC CLEANUP_SCOPE = "schemas"
# MAGIC DRY_RUN = True
# MAGIC CONFIRM_DELETION = False
# MAGIC 
# MAGIC # Clean up specific tables only
# MAGIC CLEANUP_SCOPE = "tables"
# MAGIC EXCLUDE_PATTERNS = ["_processing_offsets", "_hwm_", "_cdf_"]
# MAGIC ```
# MAGIC 
# MAGIC ### For Production:
# MAGIC ```python
# MAGIC # Clean up entire catalog (with confirmation)
# MAGIC CLEANUP_SCOPE = "catalog"
# MAGIC DRY_RUN = False
# MAGIC CONFIRM_DELETION = True
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Safety Notes
# MAGIC 
# MAGIC 1. **Always run with DRY_RUN = True first** to see what will be deleted
# MAGIC 2. **Set CONFIRM_DELETION = True** for production environments
# MAGIC 3. **Use EXCLUDE_PATTERNS** to protect critical tables
# MAGIC 4. **Backup important data** before running cleanup
# MAGIC 5. **Test in development** before running in production
