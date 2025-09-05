"""
DLT Pipeline: Silver Layer Construction

This pipeline consumes Bronze layer data via Change Data Feed (CDF) and builds
the Silver layer with SCD2 support and business logic enrichment.

Pipeline Flow:
1. Read CDF data from Bronze tables
2. Build Silver layer with SCD2 dimensions and enriched facts
3. Commit bookmarks for incremental processing

Author: Platform Observability Team
Version: 1.0
"""

from libs.tag_processor import TagProcessor

# DLT Silver: consume Bronze via CDF and build curated Silver layer
import dlt
from pyspark.sql import functions as F, Window

from libs.cdf import read_cdf, commit_processing_offset
from libs.data_enrichment import with_entity_and_run, join_prices
from libs.utils import yyyymmdd
from config import Config

# Get configuration for catalog and schema names
config = Config.get_config()
CAT = config.catalog
BRZ = f"{CAT}.{config.bronze_schema}"

# =============================================================================
# PHASE 1: READ CDF DATA FROM BRONZE SOURCES
# =============================================================================
# Read Change Data Feed from all Bronze tables to get only new/changed records
# This enables incremental processing and reduces compute costs

def read_cdf_data():
    """Read CDF data from all Bronze sources"""
    global usage_cdf, prices_cdf, run_cdf, task_cdf, jobs_cdf, pipes_cdf, ws_cdf, clusters_cdf
    global usage_end_ver, prices_end_ver, run_end_ver, task_end_ver, jobs_end_ver, pipes_end_ver, ws_end_ver, clusters_end_ver
    
    # Read CDF from Bronze sources with automatic bookmark management
    usage_cdf, usage_end_ver = read_cdf(spark, f"{BRZ}.brz_billing_usage", "brz_billing_usage")
    prices_cdf, prices_end_ver = read_cdf(spark, f"{BRZ}.brz_billing_list_prices", "brz_billing_list_prices")
    run_cdf,    run_end_ver    = read_cdf(spark, f"{BRZ}.brz_lakeflow_job_run_timeline", "brz_lakeflow_job_run_timeline")
    task_cdf,   task_end_ver   = read_cdf(spark, f"{BRZ}.brz_lakeflow_job_task_run_timeline", "brz_lakeflow_job_task_run_timeline")
    jobs_cdf,   jobs_end_ver   = read_cdf(spark, f"{BRZ}.brz_lakeflow_jobs", "brz_lakeflow_jobs")
    pipes_cdf,  pipes_end_ver  = read_cdf(spark, f"{BRZ}.brz_lakeflow_pipelines", "brz_lakeflow_pipelines")
    ws_cdf,     ws_end_ver     = read_cdf(spark, f"{BRZ}.brz_access_workspaces_latest", "brz_access_workspaces_latest")
    clusters_cdf, clusters_end_ver = read_cdf(spark, f"{BRZ}.brz_compute_clusters", "brz_compute_clusters")
    
    # Remove CDF metadata columns from the dataframes for processing
    # These columns are only needed for CDF tracking, not for business logic
    global usage_new, prices_new, run_new, task_new, jobs_new, pipes_new, ws_new, clusters_new
    usage_new = usage_cdf.drop("_change_type","_commit_version","_commit_timestamp")
    prices_new = prices_cdf.drop("_change_type","_commit_version","_commit_timestamp")
    run_new = run_cdf.drop("_change_type","_commit_version","_commit_timestamp")
    task_new = task_cdf.drop("_change_type","_commit_version","_commit_timestamp")
    jobs_new = jobs_cdf.drop("_change_type","_commit_version","_commit_timestamp")
    pipes_new = pipes_cdf.drop("_change_type","_commit_version","_commit_timestamp")
    ws_new = ws_cdf.drop("_change_type","_commit_version","_commit_timestamp")
    clusters_new = clusters_cdf.drop("_change_type","_commit_version","_commit_timestamp")

# =============================================================================
# PHASE 2: SILVER LAYER - DIMENSIONS
# =============================================================================
# Build slowly changing dimensions (SCD2) and reference data
# SCD2 tracks historical changes with valid_from/valid_to timestamps

@dlt.table(name="slv_workspace", comment="Workspace snapshot (current)")
def slv_workspace():
    """
    Workspace dimension table containing current workspace information.
    This is a Type 1 dimension (current values only) since workspace details
    don't change frequently and historical tracking isn't required.
    """
    return ws_new.select("workspace_id","workspace_name","workspace_url").distinct()

@dlt.table(name="slv_jobs_scd", comment="SCD2 for jobs with workflow hierarchy")
def slv_jobs_scd():
    """
    Enhanced jobs table with workflow hierarchy and cluster association
    """
    src = jobs_new.select(
        "workspace_id", "job_id", "name", "run_as", "change_time",
        "parent_workflow_id", "workflow_type", "cluster_id", "tags"
    )
    w = Window.partitionBy("workspace_id","job_id").orderBy("change_time")
    next_change = F.lead("change_time").over(w)
    return (src
        .withColumn("valid_from", F.col("change_time"))
        .withColumn("valid_to", F.expr("CASE WHEN lead(change_time) OVER (PARTITION BY workspace_id, job_id ORDER BY change_time) IS NULL THEN TIMESTAMP('2999-12-31') ELSE (lead(change_time) OVER (PARTITION BY workspace_id, job_id ORDER BY change_time) - INTERVAL 1 MICROSECOND) END"))
        .withColumn("is_current", next_change.isNull())
        .withColumn("is_parent_workflow", F.col("workflow_type") == F.lit("PARENT"))
        .withColumn("is_sub_workflow", F.col("workflow_type") == F.lit("SUB_WORKFLOW"))
        .drop("change_time")
    )

@dlt.table(name="slv_pipelines_scd", comment="SCD2 for pipelines from CDF")
def slv_pipelines_scd():
    """
    Slowly Changing Dimension Type 2 (SCD2) for pipelines.
    Similar to jobs, tracks pipeline configuration changes over time.
    """
    src = pipes_new.select("workspace_id","pipeline_id","name","run_as","change_time")
    w = Window.partitionBy("workspace_id","pipeline_id").orderBy("change_time")
    next_change = F.lead("change_time").over(w)
    return (src
        .withColumn("valid_from", F.col("change_time"))
        .withColumn("valid_to", F.expr("CASE WHEN lead(change_time) OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time) IS NULL THEN TIMESTAMP('2999-12-31') ELSE (lead(change_time) OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time) - INTERVAL 1 MICROSECOND) END"))
        .withColumn("is_current", next_change.isNull())
        .drop("change_time")
    )

@dlt.table(name="slv_clusters", comment="Cluster SCD2 table with entity association and runtime analysis")
def slv_clusters():
    """
    Enhanced cluster SCD2 table with runtime analysis and optimization insights
    """
    return clusters_new.select(
        "workspace_id", "cluster_id", "cluster_name", "cluster_type",
        "associated_entity_id", "associated_entity_type", "tags",
        "spark_version", "cluster_source", "node_type_id",
        "min_workers", "max_workers", "driver_node_type_id",
        "autoscale_enabled"
    ).withColumn(
        "major_version",
        F.regexp_extract(F.col("spark_version"), r"(\d+)\.", 1).cast("int")
    ).withColumn(
        "minor_version",
        F.regexp_extract(F.col("spark_version"), r"\.(\d+)\.", 1).cast("int")
    ).withColumn(
        "runtime_age_months",
        F.months_between(F.current_date(), F.lit("2023-01-01"))  # Placeholder - should use actual release dates
    ).withColumn(
        "is_lts",
        F.when(F.col("major_version") >= 13, True).otherwise(False)  # Simplified LTS logic
    ).withColumn(
        "valid_from", F.col("created_time")
    ).withColumn(
        "valid_to", F.expr("CASE WHEN lead(created_time) OVER (PARTITION BY cluster_id ORDER BY created_time) IS NULL THEN TIMESTAMP('2999-12-31') ELSE (lead(created_time) OVER (PARTITION BY cluster_id ORDER BY created_time) - INTERVAL 1 MICROSECOND) END")
    ).withColumn(
        "is_current", F.expr("lead(created_time) OVER (PARTITION BY cluster_id ORDER BY created_time) IS NULL")
    )

@dlt.view(name="slv_entity_latest_v")
def slv_entity_latest_v():
    """
    Unified view of current jobs and pipelines.
    Combines current versions of both entity types for easy access.
    Used downstream for entity lookups and joins.
    """
    jobs = dlt.read("slv_jobs_scd").where("is_current").select(F.lit("JOB").alias("entity_type"), "workspace_id", F.col("job_id").alias("entity_id"), "name","run_as")
    pipes = dlt.read("slv_pipelines_scd").where("is_current").select(F.lit("PIPELINE").alias("entity_type"), "workspace_id", F.col("pipeline_id").alias("entity_id"), "name","run_as")
    return jobs.unionByName(pipes)



# =============================================================================
# PHASE 3: SILVER LAYER - PRICING & USAGE
# =============================================================================
# Build fact tables with pricing information and business logic

@dlt.table(name="slv_price_scd", comment="Price intervals")
def slv_price_scd():
    """
    Price dimension table with time-based pricing.
    Handles price changes over time with start/end timestamps.
    Defaults to far-future date for current prices.
    """
    p = prices_new.select("cloud","sku_name","usage_unit","pricing.default".alias("price_usd"),"price_start_time","price_end_time")
    return p.withColumn("price_end_time", F.coalesce(F.col("price_end_time"), F.to_timestamp(F.lit("2999-12-31"))))

@dlt.table(name="slv_usage_txn", comment="Usage + pricing + entity/run + date_sk + normalized business tags + workflow hierarchy")
@dlt.expect("nonneg_qty","usage_quantity >= 0")
@dlt.expect("valid_time","usage_end_time >= usage_start_time")
def slv_usage_txn():
    """
    Enhanced usage fact table with workflow hierarchy and cluster context
    
    Data Quality Expectations:
    - nonneg_qty: Usage quantities must be non-negative
    - valid_time: End time must be after start time
    
    Enrichment Steps:
    1. Add entity and run information
    2. Join with pricing data
    3. Join with cluster information
    4. Apply tag normalization and workflow hierarchy
    5. Use cluster-inherited tags when job tags are missing
    """
    u = with_entity_and_run(usage_new)
    u = join_prices(u, dlt.read("silver_price_scd"))
    
    # Enrich with cluster and workflow hierarchy
    u = u.join(
        dlt.read("slv_clusters"),
        (u.workspace_id == F.col("cluster.workspace_id")) &
        (u.usage_metadata.cluster_id == F.col("cluster.cluster_id")),
        "left"
    )
    
    # Enrich with normalized tags and workflow hierarchy
    u = TagProcessor.enrich_usage(u)
    u = TagProcessor.enrich_workflow_hierarchy(u)
    
    # Use cluster-inherited tags when job tags are missing
    u = u.withColumn(
        "line_of_business",
        F.coalesce(F.col("line_of_business"), F.col("inherited_line_of_business"))
    ).withColumn(
        "cost_center",
        F.coalesce(F.col("cost_center"), F.col("inherited_cost_center"))
    ).withColumn(
        "workflow_level",
        F.coalesce(F.col("workflow_level"), F.col("inherited_workflow_level"))
    )
    
    return u

@dlt.view(name="slv_usage_tags_exploded", comment="Exploded tags view for ad-hoc slicing (no aggregation; use EXISTS/semi-joins)")
def slv_usage_tags_exploded():
    """
    Exploded tags view for flexible querying.
    Each tag becomes a separate row, enabling complex tag-based filtering
    without requiring aggregation or complex string operations.
    """
    return TagProcessor.exploded_tags_view(dlt.read("slv_usage_txn"))

@dlt.table(name="slv_job_run_timeline", comment="Run timeline from CDF")
def slv_job_run_timeline():
    """
    Job run timeline with date surrogate keys.
    Converts timestamps to integer date keys for efficient partitioning
    and joins with date dimension tables.
    """
    r = run_new.select("workspace_id","job_id","run_id","period_start_time","period_end_time","result_state","termination_code")
    return (r
        .withColumn("date_sk_start", yyyymmdd("period_start_time"))
        .withColumn("date_sk_end",   yyyymmdd("period_end_time"))
    )

@dlt.table(name="slv_job_task_run_timeline", comment="Task timeline SCD2 from CDF")
def slv_job_task_run_timeline():
    """
    Task-level run timeline SCD2 with execution metrics.
    Calculates execution duration and provides task-level insights.
    """
    t = task_new.select("workspace_id","job_id","run_id","task_key","period_start_time","period_end_time","result_state","retry_attempt")
    return (t
        .withColumn("execution_secs", (F.col("period_end_time").cast("double") - F.col("period_start_time").cast("double")))
        .withColumn("date_sk", yyyymmdd("period_end_time"))
        .withColumn("valid_from", F.col("period_end_time"))
        .withColumn("valid_to", F.expr("CASE WHEN lead(period_end_time) OVER (PARTITION BY workspace_id, job_id, run_id, task_key ORDER BY period_end_time) IS NULL THEN TIMESTAMP('2999-12-31') ELSE (lead(period_end_time) OVER (PARTITION BY workspace_id, job_id, run_id, task_key ORDER BY period_end_time) - INTERVAL 1 MICROSECOND) END"))
        .withColumn("is_current", F.expr("lead(period_end_time) OVER (PARTITION BY workspace_id, job_id, run_id, task_key ORDER BY period_end_time) IS NULL"))
    )

@dlt.view(name="slv_usage_run_enriched_v")
def slv_usage_run_enriched_v():
    """
    Enriched usage view with run information.
    Joins usage data with run timeline to provide run-level context
    for cost analysis and performance monitoring.
    
    Join Logic:
    - Matches workspace, job, and run IDs
    - Uses hour-level time windows for flexible matching
    - Left join preserves usage records even without run context
    """
    u = dlt.read("slv_usage_txn").alias("u")
    r = dlt.read("slv_job_run_timeline").alias("r")
    return (u.join(
        r,
        (F.col("u.workspace_id") == F.col("r.workspace_id")) &
        (F.col("u.usage_metadata.job_id") == F.col("r.job_id")) &
        (F.col("u.usage_metadata.job_run_id") == F.col("r.run_id")) &
        (F.date_trunc("hour", F.col("u.usage_start_time")) >= F.date_trunc("hour", F.col("r.period_start_time"))) &
        (F.date_trunc("hour", F.col("u.usage_start_time")) <  (F.date_trunc("hour", F.col("r.period_end_time")) + F.expr("INTERVAL 1 HOUR"))) ,
        "left"))

# =============================================================================
# PHASE 4: COMMIT CDF BOOKMARKS
# =============================================================================
# Update bookmarks to track processed versions for next incremental run
# This ensures we only process new changes in subsequent runs

def commit_processing_offsets():
    """Commit all CDF processing offsets"""
    try:
        commit_processing_offset(spark, "brz_billing_usage", usage_end_ver)
        commit_processing_offset(spark, "brz_billing_list_prices", prices_end_ver)
        commit_processing_offset(spark, "brz_lakeflow_job_run_timeline", run_end_ver)
        commit_processing_offset(spark, "brz_lakeflow_job_task_run_timeline", task_end_ver)
        commit_processing_offset(spark, "brz_lakeflow_jobs", jobs_end_ver)
        commit_processing_offset(spark, "brz_lakeflow_pipelines", pipes_end_ver)
        commit_processing_offset(spark, "brz_access_workspaces_latest", ws_end_ver)
        commit_processing_offset(spark, "brz_compute_clusters", clusters_end_ver)
        print("✅ All CDF processing offsets committed successfully")
    except Exception as e:
        print(f"❌ Failed to commit processing offsets: {str(e)}")
        raise

# =============================================================================
# MAIN PIPELINE EXECUTION
# =============================================================================

def main():
    """Main pipeline execution function"""
    print("🚀 Starting Silver DLT pipeline...")
    
    try:
        # Phase 1: Read CDF data
        print("📖 Reading CDF data from Bronze sources...")
        read_cdf_data()
        
        # Phase 2-3: Silver layer is built automatically by DLT decorators
        
        # Phase 4: Commit processing offsets
        print("💾 Committing CDF processing offsets...")
        commit_processing_offsets()
        
        print("🎉 Silver DLT pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        raise

# Execute main function when pipeline is run
if __name__ == "__main__":
    main()
