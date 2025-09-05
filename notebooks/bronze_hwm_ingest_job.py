"""
Bronze Layer High Water Mark (HWM) Ingest Job

This notebook implements the Bronze layer ingestion strategy using High Water Mark (HWM) 
tracking for incremental data processing. It reads from Databricks system tables and 
loads data into Bronze tables with CDF enabled for downstream consumption.

Key Features:
- Incremental processing using HWM processing state
- Data quality validation with built-in rules
- Performance monitoring and structured logging
- Externalized SQL operations for maintainability
- Configurable overlap hours for data consistency

Pipeline Flow:
1. Read system tables (billing, lakeflow, access, compute)
2. Apply HWM filtering for incremental processing
3. Validate data quality
4. Execute upsert operations using external SQL
5. Update processing state for next run

Author: Platform Observability Team
Version: 1.0
"""

# Databricks notebook source
# Configuration-based parameters (no widgets required for standalone execution)

# Import configuration and utilities
from config import config
from libs.logging import StructuredLogger, PerformanceMonitor, performance_monitor
from libs.error_handling import safe_execute, validate_data_quality
from libs.monitoring import pipeline_monitor
from libs.processing_state import ensure_table, get_last_processed_timestamp, commit_processing_state
from libs.sql_manager import sql_manager

from pyspark.sql import functions as F
import time
from datetime import datetime

# =============================================================================
# INITIALIZATION & CONFIGURATION
# =============================================================================

# Initialize logging and monitoring with job context
logger = StructuredLogger("bronze_hwm_ingest_job")
logger.set_context(
    job_id=dbutils.jobs.taskValues.getCurrent().get("job_id", "unknown"),
    run_id=dbutils.jobs.taskValues.getCurrent().get("run_id", "unknown"),
    pipeline_name="bronze_hwm_ingest",
    environment=config.ENV
)

# Use overlap hours from configuration
# Overlap hours ensure we don't miss data due to timing issues
OVERLAP_HOURS = config.overlap_hours

logger.info("Bronze HWM ingest job started", {
    "overlap_hours": OVERLAP_HOURS,
    "environment": config.ENV,
    "catalog": config.catalog,
    "bronze_schema": config.bronze_schema
})

# Initialize monitoring for pipeline health tracking
pipeline_monitor.monitor_pipeline_start("bronze_hwm_ingest", dbutils.jobs.taskValues.getCurrent().get("run_id", "unknown"))

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def window_start(last_ts):
    """
    Calculate window start time with overlap for incremental processing.
    
    Args:
        last_ts: Last processed timestamp from processing state
        
    Returns:
        Timestamp representing the start of the processing window
        
    The overlap ensures we don't miss data due to:
    - Clock synchronization issues
    - Data arrival delays
    - Processing time variations
    """
    if last_ts is None:
        return F.to_timestamp(F.lit("1900-01-01"))
    return F.expr(f"timestampadd(HOUR, -{OVERLAP_HOURS}, timestamp('{last_ts}'))")

def sha256_concat(cols):
    """
    Generate SHA256 hash for row change detection.
    
    Args:
        cols: List of column names to include in hash
        
    Returns:
        SHA256 hash string for change detection
        
    This hash is used to:
    - Detect when data has actually changed
    - Avoid unnecessary updates
    - Maintain data lineage
    """
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)

def execute_sql_operation(operation: str, target_table: str, staging_view: str, logger: StructuredLogger):
    """
    Execute SQL operation using external SQL files.
    
    Args:
        operation: Name of the SQL operation (e.g., "upsert_billing_usage")
        target_table: Target table for the operation
        staging_view: Staging view containing source data
        logger: Logger instance for operation tracking
        
    This function:
    1. Loads SQL from external files
    2. Parameterizes the SQL with table names
    3. Executes the SQL operation
    4. Logs success/failure for monitoring
    """
    try:
        # Load and parameterize SQL
        sql = sql_manager.parameterize_sql(
            operation,
            target_table=target_table,
            source_table=staging_view
        )
        
        logger.debug(f"Executing SQL operation: {operation}", {
            "target_table": target_table,
            "staging_view": staging_view,
            "sql_length": len(sql)
        })
        
        # Execute the SQL
        spark.sql(sql)
        
        logger.info(f"Successfully executed SQL operation: {operation}")
        
    except Exception as e:
        logger.error(f"Failed to execute SQL operation: {operation}", {
            "error": str(e),
            "target_table": target_table,
            "staging_view": staging_view
        })
        raise

# =============================================================================
# DATA INGESTION FUNCTIONS
# =============================================================================
# Each function follows the same pattern:
# 1. Read from system table with HWM filtering
# 2. Apply business logic and transformations
# 3. Validate data quality
# 4. Execute upsert operation
    # 5. Update processing state

@performance_monitor("upsert_billing_usage")
@safe_execute(logger, "upsert_billing_usage")
def upsert_billing_usage():
    """
    Upsert billing usage data with data quality validation.
    
    Sources: system.billing.usage
            Target: brz_billing_usage
    
    Business Logic:
    - Filters by usage_end_time for incremental processing
    - Generates row hash for change detection
    - Validates data quality before processing
    - Updates processing state after successful processing
    
    Data Quality Rules:
    - Usage quantities must be non-negative
    - End time must be after start time
    - Required fields must be present
    """
    src = "system.billing.usage"
            tgt = config.get_table_name("bronze", "brz_billing_usage")
    
    logger.info(f"Processing billing usage from {src} to {tgt}")
    
    # Get last processed timestamp and calculate window start
    last = get_last_processed_timestamp(spark, src)
    ws = window_start(last)
    
    # Read and process data with HWM filtering
    stg = (spark.table(src)
           .where(F.col("usage_end_time") > ws)
           .withColumn("row_hash", sha256_concat([
               "workspace_id", "cloud", "sku_name", "usage_unit",
               "usage_start_time", "usage_end_time",
               "usage_metadata.job_run_id", "usage_metadata.dlt_pipeline_id",
               "usage_quantity"
           ])))
    
    # Validate data quality before upsert
    if not validate_data_quality(stg, "billing_usage", logger):
        raise ValueError("Data quality validation failed for billing usage")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_usage")
    execute_sql_operation("upsert_billing_usage", tgt, "stg_usage", logger)
    
    # Update processing state for next incremental run
    mx = stg.select(F.max("usage_end_time").alias("mx")).first().mx
    if mx is not None:
        commit_processing_state(spark, src, mx)
        logger.info(f"Updated processing state for {src} to {mx}")
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} billing usage records")
    return record_count

@performance_monitor("upsert_list_prices")
@safe_execute(logger, "upsert_list_prices")
def upsert_list_prices():
    """
    Upsert list prices data for cost calculations.
    
    Sources: system.billing.list_prices
            Target: brz_billing_list_prices
    
    Business Logic:
    - Filters by price_start_time for incremental processing
    - Handles price changes over time
    - Used downstream for cost calculations
    """
    src = "system.billing.list_prices"
            tgt = config.get_table_name("bronze", "brz_billing_list_prices")
    
    logger.info(f"Processing list prices from {src} to {tgt}")
    
    last = get_last_processed_timestamp(spark, src)
    ws = window_start(last)
    
    stg = spark.table(src).where(F.col("price_start_time") > ws)
    
    # Validate data quality
    if not validate_data_quality(stg, "list_prices", logger):
        raise ValueError("Data quality validation failed for list prices")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_prices")
    execute_sql_operation("upsert_list_prices", tgt, "stg_prices", logger)
    
    mx = stg.select(F.max("price_start_time").alias("mx")).first().mx
    if mx is not None:
        commit_processing_state(spark, src, mx)
        logger.info(f"Updated processing state for {src} to {mx}")
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} list price records")
    return record_count

@performance_monitor("upsert_job_run_timeline")
@safe_execute(logger, "upsert_job_run_timeline")
def upsert_job_run_timeline():
    """
    Upsert job run timeline data for performance analysis.
    
    Sources: system.lakeflow.job_run_timeline
            Target: brz_lakeflow_job_run_timeline
    
    Business Logic:
    - Filters by period_end_time for incremental processing
    - Tracks job execution status and timing
    - Used for cost allocation and performance monitoring
    """
    src = "system.lakeflow.job_run_timeline"
            tgt = config.get_table_name("bronze", "brz_lakeflow_job_run_timeline")
    
    logger.info(f"Processing job run timeline from {src} to {tgt}")
    
    last = get_last_processed_timestamp(spark, src)
    ws = window_start(last)
    
    stg = (spark.table(src)
           .where(F.col("period_end_time") > ws)
           .withColumn("row_hash", sha256_concat([
               "workspace_id","job_id","run_id","period_start_time","period_end_time","result_state","termination_code"
           ])))
    
    # Validate data quality
    if not validate_data_quality(stg, "job_run_timeline", logger):
        raise ValueError("Data quality validation failed for job run timeline")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_job_run")
    execute_sql_operation("upsert_job_run_timeline", tgt, "stg_job_run", logger)
    
    mx = stg.select(F.max("period_end_time").alias("mx")).first().mx
    if mx is not None:
        commit_processing_state(spark, src, mx)
        logger.info(f"Updated processing state for {src} to {mx}")
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} job run timeline records")
    return record_count

@performance_monitor("upsert_job_task_run_timeline")
@safe_execute(logger, "upsert_job_task_run_timeline")
def upsert_job_task_run_timeline():
    """
    Upsert job task run timeline data for granular performance analysis.
    
    Sources: system.lakeflow.job_task_run_timeline
            Target: brz_lakeflow_job_task_run_timeline
    
    Business Logic:
    - Filters by period_end_time for incremental processing
    - Provides task-level execution insights
    - Used for performance optimization and debugging
    """
    src = "system.lakeflow.job_task_run_timeline"
            tgt = config.get_table_name("bronze", "brz_lakeflow_job_task_run_timeline")
    
    logger.info(f"Processing job task run timeline from {src} to {tgt}")
    
    last = get_last_processed_timestamp(spark, src)
    ws = window_start(last)
    
    stg = (spark.table(src)
           .where(F.col("period_end_time") > ws)
           .withColumn("row_hash", sha256_concat([
               "workspace_id","job_id","run_id","task_key","period_start_time","period_end_time","result_state","retry_attempt"
           ])))
    
    # Validate data quality
    if not validate_data_quality(stg, "job_task_run_timeline", logger):
        raise ValueError("Data quality validation failed for job task run timeline")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_job_task_run")
    execute_sql_operation("upsert_job_task_run_timeline", tgt, "stg_job_task_run", logger)
    
    mx = stg.select(F.max("period_end_time").alias("mx")).first().mx
    if mx is not None:
        commit_processing_state(spark, src, mx)
        logger.info(f"Updated processing state for {src} to {mx}")
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} job task run timeline records")
    return record_count

@performance_monitor("upsert_lakeflow_jobs")
@safe_execute(logger, "upsert_lakeflow_jobs")
def upsert_lakeflow_jobs():
    """
    Upsert lakeflow jobs metadata for entity tracking.
    
    Sources: system.lakeflow.jobs
            Target: brz_lakeflow_jobs
    
    Business Logic:
    - Filters by change_time for incremental processing
    - Tracks job configuration changes
    - Used for entity identification and cost allocation
    """
    src = "system.lakeflow.jobs"
            tgt = config.get_table_name("bronze", "bronze_lakeflow_jobs")
    
    logger.info(f"Processing lakeflow jobs from {src} to {tgt}")
    
    last = get_last_processed_timestamp(spark, src)
    ws = window_start(last)
    
    stg = (spark.table(src)
           .where(F.col("change_time") > ws)
           .withColumn("row_hash", sha256_concat([
               "workspace_id","job_id","name","run_as","change_time"
           ])))
    
    # Validate data quality
    if not validate_data_quality(stg, "lakeflow_jobs", logger):
        raise ValueError("Data quality validation failed for lakeflow jobs")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_jobs")
    execute_sql_operation("upsert_lakeflow_jobs", tgt, "stg_jobs", logger)
    
    mx = stg.select(F.max("change_time").alias("mx")).first().mx
    if mx is not None:
        commit_processing_state(spark, src, mx)
        logger.info(f"Updated processing state for {src} to {mx}")
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} lakeflow jobs records")
    return record_count

@performance_monitor("upsert_lakeflow_pipelines")
@safe_execute(logger, "upsert_lakeflow_pipelines")
def upsert_lakeflow_pipelines():
    """
    Upsert lakeflow pipelines metadata for entity tracking.
    
    Sources: system.lakeflow.pipelines
            Target: brz_lakeflow_pipelines
    
    Business Logic:
    - Filters by change_time for incremental processing
    - Tracks pipeline configuration changes
    - Used for entity identification and cost allocation
    """
    src = "system.lakeflow.pipelines"
            tgt = config.get_table_name("bronze", "brz_lakeflow_pipelines")
    
    logger.info(f"Processing lakeflow pipelines from {src} to {tgt}")
    
    last = get_last_processed_timestamp(spark, src)
    ws = window_start(last)
    
    stg = (spark.table(src)
           .where(F.col("change_time") > ws)
           .withColumn("row_hash", sha256_concat([
               "workspace_id","pipeline_id","name","run_as","change_time"
           ])))
    
    # Validate data quality
    if not validate_data_quality(stg, "lakeflow_pipelines", logger):
        raise ValueError("Data quality validation failed for lakeflow pipelines")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_pipelines")
    execute_sql_operation("upsert_lakeflow_pipelines", tgt, "stg_pipelines", logger)
    
    mx = stg.select(F.max("change_time").alias("mx")).first().mx
    if mx is not None:
        commit_processing_state(spark, src, mx)
        logger.info(f"Updated processing state for {src} to {mx}")
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} lakeflow pipelines records")
    return record_count

@performance_monitor("upsert_compute_clusters")
@safe_execute(logger, "upsert_compute_clusters")
def upsert_compute_clusters():
    """
    Upsert compute clusters data for infrastructure tracking.
    
    Sources: system.compute.clusters
            Target: brz_compute_clusters
    
    Business Logic:
    - Filters by created_time for incremental processing
    - Tracks cluster configuration and status
    - Used for cost allocation and policy compliance
    """
    src = "system.compute.clusters"
            tgt = config.get_table_name("bronze", "brz_compute_clusters")
    
    logger.info(f"Processing compute clusters from {src} to {tgt}")
    
    last = get_last_processed_timestamp(spark, src)
    ws = window_start(last)
    
    stg = (spark.table(src)
           .where(F.col("created_time") > ws)
           .withColumn("row_hash", sha256_concat([
               "cluster_id","cluster_name","cluster_source","policy_id","spark_version","data_security_mode",
               "driver_node_type_id","node_type_id","min_workers","max_workers","num_workers"
           ])))
    
    # Validate data quality
    if not validate_data_quality(stg, "compute_clusters", logger):
        raise ValueError("Data quality validation failed for compute clusters")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_clusters")
    execute_sql_operation("upsert_compute_clusters", tgt, "stg_clusters", logger)
    
    mx = stg.select(F.max("created_time").alias("mx")).first().mx
    if mx is not None:
        commit_processing_state(spark, src, mx)
        logger.info(f"Updated processing state for {src} to {mx}")
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} compute clusters records")
    return record_count

@performance_monitor("upsert_compute_node_types")
@safe_execute(logger, "upsert_compute_node_types")
def upsert_compute_node_types():
    """
    Upsert compute node types data (full upsert).
    
    Sources: system.compute.node_types
            Target: brz_compute_node_types
    
    Business Logic:
    - Full upsert (no HWM filtering) since node types are reference data
    - Tracks node type specifications and capabilities
    - Used for cost calculations and capacity planning
    """
    src = "system.compute.node_types"
            tgt = config.get_table_name("bronze", "bronze_compute_node_types")
    
    logger.info(f"Processing compute node types from {src} to {tgt}")
    
    stg = spark.table(src).withColumn("row_hash", sha256_concat([
        "node_type_id","node_info.instance_type","node_info.memory_mb","node_info.num_cores"
    ]))
    
    # Validate data quality
    if not validate_data_quality(stg, "compute_node_types", logger):
        raise ValueError("Data quality validation failed for compute node types")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_node_types")
    execute_sql_operation("upsert_compute_node_types", tgt, "stg_node_types", logger)
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} compute node types records")
    return record_count

@performance_monitor("upsert_access_workspaces")
@safe_execute(logger, "upsert_access_workspaces")
def upsert_access_workspaces():
    """
    Upsert access workspaces data for workspace tracking.
    
    Sources: system.access.workspaces_latest
            Target: brz_access_workspaces
    
    Business Logic:
    - Full upsert (no HWM filtering) since workspace data is reference data
    - Tracks workspace metadata and access information
    - Used for workspace identification and cost allocation
    """
    src = "system.access.workspaces_latest"
            tgt = config.get_table_name("bronze", "brz_access_workspaces_latest")
    
    logger.info(f"Processing access workspaces from {src} to {tgt}")
    
    stg = spark.table(src).withColumn("row_hash", sha256_concat([
        "workspace_id","workspace_name","workspace_url"
    ]))
    
    # Validate data quality
    if not validate_data_quality(stg, "access_workspaces", logger):
        raise ValueError("Data quality validation failed for access workspaces")
    
    # Create staging view and execute SQL operation
    stg.createOrReplaceTempView("stg_workspaces")
    execute_sql_operation("upsert_access_workspaces", tgt, "stg_workspaces", logger)
    
    record_count = stg.count()
    logger.info(f"Successfully processed {record_count} access workspaces records")
    return record_count

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function orchestrating the entire bronze ingestion process.
    
    Execution Flow:
    1. Validate available SQL operations
    2. Ensure processing state table exists
    3. Process all data sources sequentially
    4. Monitor pipeline completion
    5. Log final results and metrics
    
    Error Handling:
    - Individual source failures are logged but don't stop the pipeline
    - Overall pipeline failure is tracked for monitoring
    - Performance metrics are captured for optimization
    """
    start_time = time.time()
    total_records = 0
    
    try:
        logger.info("Starting bronze HWM ingest job")
        
        # Validate SQL operations are available before processing
        available_operations = sql_manager.get_available_operations()
        logger.info(f"Available SQL operations: {available_operations}")
        
        # Ensure processing state table exists for HWM tracking
        ensure_table()
        
        # Define all data sources and their processing functions
        # Each source is processed independently for fault isolation
        sources = [
            ("billing_usage", upsert_billing_usage),
            ("list_prices", upsert_list_prices),
            ("job_run_timeline", upsert_job_run_timeline),
            ("job_task_run_timeline", upsert_job_task_run_timeline),
            ("lakeflow_jobs", upsert_lakeflow_jobs),
            ("lakeflow_pipelines", upsert_lakeflow_pipelines),
            ("compute_clusters", upsert_compute_clusters),
            ("compute_node_types", upsert_compute_node_types),
            ("access_workspaces", upsert_access_workspaces)
        ]
        
        # Process each source sequentially
        for source_name, source_func in sources:
            try:
                logger.info(f"Processing source: {source_name}")
                record_count = source_func()
                total_records += record_count
                logger.info(f"Completed processing {source_name}: {record_count} records")
            except Exception as e:
                logger.error(f"Failed to process source {source_name}: {str(e)}")
                raise
        
        duration = time.time() - start_time
        
        # Monitor pipeline completion for health tracking
        pipeline_monitor.monitor_pipeline_completion(
            pipeline_name="bronze_hwm_ingest",
            run_id=dbutils.jobs.taskValues.getCurrent().get("run_id", "unknown"),
            success=True,
            duration_seconds=duration,
            records_processed=total_records
        )
        
        logger.info("Bronze HWM ingest job completed successfully", {
            "total_records_processed": total_records,
            "duration_seconds": round(duration, 2),
            "overlap_hours": OVERLAP_HOURS
        })
        
        return total_records
        
    except Exception as e:
        duration = time.time() - start_time
        
        # Monitor pipeline failure for alerting
        pipeline_monitor.monitor_pipeline_completion(
            pipeline_name="bronze_hwm_ingest",
            run_id=dbutils.jobs.taskValues.getCurrent().get("run_id", "unknown"),
            success=False,
            duration_seconds=duration,
            records_processed=total_records
        )
        
        logger.error(f"Bronze HWM ingest job failed: {str(e)}", {
            "total_records_processed": total_records,
            "duration_seconds": round(duration, 2),
            "error": str(e)
        })
        raise

# Execute main function when notebook is run
if __name__ == "__main__":
    main()
