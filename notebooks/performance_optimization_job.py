# Databricks notebook source
# MAGIC %md
# MAGIC # Performance Optimization Job
# MAGIC 
# MAGIC This notebook applies performance optimizations to all layers of the platform observability solution.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Z-ORDER optimization for common query patterns
# MAGIC - Statistics collection for better query planning
# MAGIC - Table compaction and optimization
# MAGIC - Performance monitoring and reporting
# MAGIC 
# MAGIC ## Dependencies:
# MAGIC - All layers (Bronze, Silver, Gold) must be populated
# MAGIC - Configuration must be set up

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
import os
from datetime import datetime
from typing import Dict, List, Optional

# Add libs to path
sys.path.append('/Workspace/Repos/platform-observability/libs')

from config import Config
from logging import get_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = get_logger(__name__, config.log_level)

logger.info("Starting performance optimization job", extra={
    "catalog": config.catalog,
    "bronze_schema": config.bronze_schema,
    "silver_schema": config.silver_schema,
    "gold_schema": config.gold_schema,
    "environment": config.environment
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Functions

# COMMAND ----------

def optimize_bronze_layer(spark) -> bool:
    """Apply performance optimizations to Bronze layer"""
    logger.info("Optimizing Bronze layer tables")
    
    try:
        # Bronze tables optimization
        bronze_optimizations = [
            f"OPTIMIZE {config.catalog}.{config.bronze_schema}.brz_billing_usage ZORDER BY (workspace_id, date_sk);",
            f"OPTIMIZE {config.catalog}.{config.bronze_schema}.brz_compute_clusters ZORDER BY (workspace_id, cluster_id);",
            f"OPTIMIZE {config.catalog}.{config.bronze_schema}.brz_lakeflow_jobs ZORDER BY (workspace_id, job_id);",
            f"OPTIMIZE {config.catalog}.{config.bronze_schema}.brz_lakeflow_job_run_timeline ZORDER BY (workspace_id, date_sk_start);",
            f"OPTIMIZE {config.catalog}.{config.bronze_schema}.brz_lakeflow_job_task_run_timeline ZORDER BY (workspace_id, date_sk_start);",
            f"OPTIMIZE {config.catalog}.{config.bronze_schema}.brz_access_workspaces_latest ZORDER BY (workspace_id);"
        ]
        
        for optimization in bronze_optimizations:
            logger.info(f"Executing: {optimization}")
            spark.sql(optimization)
        
        # Collect statistics
        bronze_stats = [
            f"ANALYZE TABLE {config.catalog}.{config.bronze_schema}.brz_billing_usage COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.bronze_schema}.brz_compute_clusters COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.bronze_schema}.brz_lakeflow_jobs COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.bronze_schema}.brz_lakeflow_job_run_timeline COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.bronze_schema}.brz_lakeflow_job_task_run_timeline COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.bronze_schema}.brz_access_workspaces_latest COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
        
        for stats in bronze_stats:
            logger.info(f"Executing: {stats}")
            spark.sql(stats)
        
        logger.info("✅ Successfully optimized Bronze layer")
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing Bronze layer: {str(e)}", exc_info=True)
        return False

def optimize_silver_layer(spark) -> bool:
    """Apply performance optimizations to Silver layer"""
    logger.info("Optimizing Silver layer tables")
    
    try:
        # Silver tables optimization
        silver_optimizations = [
            f"OPTIMIZE {config.catalog}.{config.silver_schema}.slv_workspace ZORDER BY (workspace_id);",
            f"OPTIMIZE {config.catalog}.{config.silver_schema}.slv_entity_latest ZORDER BY (workspace_id, entity_type, entity_id);",
            f"OPTIMIZE {config.catalog}.{config.silver_schema}.slv_clusters ZORDER BY (workspace_id, cluster_id);",
            f"OPTIMIZE {config.catalog}.{config.silver_schema}.slv_usage_txn ZORDER BY (date_sk, workspace_id, entity_id);",
            f"OPTIMIZE {config.catalog}.{config.silver_schema}.slv_job_run_timeline ZORDER BY (date_sk_start, workspace_id, job_id);",
            f"OPTIMIZE {config.catalog}.{config.silver_schema}.slv_job_task_run_timeline ZORDER BY (date_sk_start, workspace_id, job_id);"
        ]
        
        for optimization in silver_optimizations:
            logger.info(f"Executing: {optimization}")
            spark.sql(optimization)
        
        # Collect statistics
        silver_stats = [
            f"ANALYZE TABLE {config.catalog}.{config.silver_schema}.slv_workspace COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.silver_schema}.slv_entity_latest COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.silver_schema}.slv_clusters COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.silver_schema}.slv_usage_txn COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.silver_schema}.slv_job_run_timeline COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.silver_schema}.slv_job_task_run_timeline COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
        
        for stats in silver_stats:
            logger.info(f"Executing: {stats}")
            spark.sql(stats)
        
        logger.info("✅ Successfully optimized Silver layer")
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing Silver layer: {str(e)}", exc_info=True)
        return False

def optimize_gold_layer(spark) -> bool:
    """Apply performance optimizations to Gold layer"""
    logger.info("Optimizing Gold layer tables")
    
    try:
        # Gold dimension tables optimization
        gold_dim_optimizations = [
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_dim_workspace ZORDER BY (workspace_id);",
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_dim_entity ZORDER BY (workspace_id, entity_type, entity_id);",
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_dim_sku ZORDER BY (cloud, sku_name);",
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_dim_run_status ZORDER BY (result_state);"
        ]
        
        for optimization in gold_dim_optimizations:
            logger.info(f"Executing: {optimization}")
            spark.sql(optimization)
        
        # Gold fact tables optimization
        gold_fact_optimizations = [
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_fact_usage_priced_day ZORDER BY (date_sk, workspace_id, entity_id);",
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_fact_entity_cost ZORDER BY (date_sk, workspace_id, entity_id);",
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_fact_run_cost ZORDER BY (date_sk, workspace_id, run_id);",
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_fact_run_status_cost ZORDER BY (date_sk, workspace_id, result_state);",
            f"OPTIMIZE {config.catalog}.{config.gold_schema}.gld_fact_runs_finished_day ZORDER BY (date_sk, workspace_id, entity_id);"
        ]
        
        for optimization in gold_fact_optimizations:
            logger.info(f"Executing: {optimization}")
            spark.sql(optimization)
        
        # Collect statistics for dimensions
        gold_dim_stats = [
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_dim_workspace COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_dim_entity COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_dim_sku COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_dim_run_status COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
        
        for stats in gold_dim_stats:
            logger.info(f"Executing: {stats}")
            spark.sql(stats)
        
        # Collect statistics for facts
        gold_fact_stats = [
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_fact_usage_priced_day COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_fact_entity_cost COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_fact_run_cost COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_fact_run_status_cost COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.gold_schema}.gld_fact_runs_finished_day COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
        
        for stats in gold_fact_stats:
            logger.info(f"Executing: {stats}")
            spark.sql(stats)
        
        logger.info("✅ Successfully optimized Gold layer")
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing Gold layer: {str(e)}", exc_info=True)
        return False

def optimize_processing_offsets(spark) -> bool:
    """Apply performance optimizations to processing offsets tables"""
    logger.info("Optimizing processing offsets tables")
    
    try:
        # Processing offsets optimization
        offset_optimizations = [
            f"OPTIMIZE {config.catalog}.{config.bronze_schema}._bronze_hwm_processing_offsets ZORDER BY (source_name);",
            f"OPTIMIZE {config.catalog}.{config.bronze_schema}._cdf_processing_offsets ZORDER BY (source_name);"
        ]
        
        for optimization in offset_optimizations:
            logger.info(f"Executing: {optimization}")
            spark.sql(optimization)
        
        # Collect statistics
        offset_stats = [
            f"ANALYZE TABLE {config.catalog}.{config.bronze_schema}._bronze_hwm_processing_offsets COMPUTE STATISTICS FOR ALL COLUMNS;",
            f"ANALYZE TABLE {config.catalog}.{config.bronze_schema}._cdf_processing_offsets COMPUTE STATISTICS FOR ALL COLUMNS;"
        ]
        
        for stats in offset_stats:
            logger.info(f"Executing: {stats}")
            spark.sql(stats)
        
        logger.info("✅ Successfully optimized processing offsets tables")
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing processing offsets tables: {str(e)}", exc_info=True)
        return False

def generate_optimization_report(spark) -> Dict[str, any]:
    """Generate optimization report with table statistics"""
    logger.info("Generating optimization report")
    
    try:
        report = {
            "timestamp": datetime.now().isoformat(),
            "bronze_tables": {},
            "silver_tables": {},
            "gold_tables": {},
            "processing_offsets": {}
        }
        
        # Bronze layer statistics
        bronze_tables = [
            "brz_billing_usage", "brz_compute_clusters", "brz_lakeflow_jobs",
            "brz_lakeflow_job_run_timeline", "brz_lakeflow_job_task_run_timeline",
            "brz_access_workspaces_latest"
        ]
        
        for table in bronze_tables:
            try:
                count = spark.table(f"{config.catalog}.{config.bronze_schema}.{table}").count()
                report["bronze_tables"][table] = {"record_count": count}
            except Exception as e:
                report["bronze_tables"][table] = {"error": str(e)}
        
        # Silver layer statistics
        silver_tables = [
            "slv_workspace", "slv_entity_latest", "slv_clusters",
            "slv_usage_txn", "slv_job_run_timeline", "slv_job_task_run_timeline"
        ]
        
        for table in silver_tables:
            try:
                count = spark.table(f"{config.catalog}.{config.silver_schema}.{table}").count()
                report["silver_tables"][table] = {"record_count": count}
            except Exception as e:
                report["silver_tables"][table] = {"error": str(e)}
        
        # Gold layer statistics
        gold_tables = [
            "gld_dim_workspace", "gld_dim_entity", "gld_dim_sku", "gld_dim_run_status",
            "gld_fact_usage_priced_day", "gld_fact_entity_cost", "gld_fact_run_cost",
            "gld_fact_run_status_cost", "gld_fact_runs_finished_day"
        ]
        
        for table in gold_tables:
            try:
                count = spark.table(f"{config.catalog}.{config.gold_schema}.{table}").count()
                report["gold_tables"][table] = {"record_count": count}
            except Exception as e:
                report["gold_tables"][table] = {"error": str(e)}
        
        # Processing offsets statistics
        offset_tables = [
            "_bronze_hwm_processing_offsets", "_cdf_processing_offsets"
        ]
        
        for table in offset_tables:
            try:
                count = spark.table(f"{config.catalog}.{config.bronze_schema}.{table}").count()
                report["processing_offsets"][table] = {"record_count": count}
            except Exception as e:
                report["processing_offsets"][table] = {"error": str(e)}
        
        logger.info("✅ Successfully generated optimization report")
        return report
        
    except Exception as e:
        logger.error(f"Error generating optimization report: {str(e)}", exc_info=True)
        return {"error": str(e)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution Function

# COMMAND ----------

def run_performance_optimization(spark) -> Dict[str, bool]:
    """Run complete performance optimization process"""
    logger.info("Starting performance optimization process")
    
    results = {}
    
    # Run optimizations in order
    optimization_steps = [
        ("processing_offsets", optimize_processing_offsets),
        ("bronze_layer", optimize_bronze_layer),
        ("silver_layer", optimize_silver_layer),
        ("gold_layer", optimize_gold_layer)
    ]
    
    for step_name, optimizer_func in optimization_steps:
        logger.info(f"Running optimization step: {step_name}")
        try:
            success = optimizer_func(spark)
            results[step_name] = success
            
            if success:
                logger.info(f"✅ Successfully completed optimization step: {step_name}")
            else:
                logger.error(f"❌ Failed to complete optimization step: {step_name}")
                
        except Exception as e:
            logger.error(f"❌ Exception in optimization step {step_name}: {str(e)}", exc_info=True)
            results[step_name] = False
    
    # Generate optimization report
    try:
        report = generate_optimization_report(spark)
        results["report_generated"] = True
        logger.info("✅ Successfully generated optimization report")
    except Exception as e:
        logger.error(f"❌ Failed to generate optimization report: {str(e)}")
        results["report_generated"] = False
    
    # Summary
    successful_steps = [name for name, success in results.items() if success]
    failed_steps = [name for name, success in results.items() if not success]
    
    logger.info(f"Performance optimization completed. Successful: {len(successful_steps)}, Failed: {len(failed_steps)}")
    
    if successful_steps:
        logger.info(f"✅ Successfully completed steps: {', '.join(successful_steps)}")
    
    if failed_steps:
        logger.error(f"❌ Failed steps: {', '.join(failed_steps)}")
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Performance Optimization

# COMMAND ----------

if __name__ == "__main__":
    # Run performance optimization
    results = run_performance_optimization(spark)
    
    # Check overall success
    all_successful = all(results.values())
    
    if all_successful:
        logger.info("🎉 All performance optimization steps completed successfully!")
        dbutils.notebook.exit("SUCCESS")
    else:
        logger.error("💥 Some performance optimization steps failed")
        dbutils.notebook.exit("FAILED")
