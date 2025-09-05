# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer HWM Build Job
# MAGIC 
# MAGIC This notebook builds the Gold layer using High-Water Mark (HWM) approach.
# MAGIC It processes data incrementally from Silver tables and creates fact/dimension tables.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Incremental processing using HWM tracking
# MAGIC - MERGE operations for fact tables
# MAGIC - Dimension table maintenance
# MAGIC - Data quality validation
# MAGIC - Structured logging and monitoring
# MAGIC 
# MAGIC ## Dependencies:
# MAGIC - Silver tables must be populated
# MAGIC - Processing offsets tables must exist
# MAGIC - Configuration must be set up

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, sum, max, count, countDistinct
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, lag, row_number
from pyspark.sql.functions import date_format, to_date, unix_timestamp, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, quarter
from pyspark.sql.functions import current_timestamp, current_date
from pyspark.sql.functions import isnan, isnull, otherwise
from pyspark.sql.functions import array, explode, collect_list, collect_set
from pyspark.sql.functions import udf

# Import from libs package (cloud-agnostic approach)
import sys
import os

# Add current directory to path for local development
current_dir = os.path.dirname(os.path.abspath(__file__))
libs_dir = os.path.join(os.path.dirname(current_dir), 'libs')
if libs_dir not in sys.path:
    sys.path.append(libs_dir)

# For Databricks, also try the workspace path
workspace_libs_path = '/Workspace/Repos/platform-observability/libs'
if workspace_libs_path not in sys.path:
    sys.path.append(workspace_libs_path)

from config import Config
from processing_state import get_last_processed_timestamp, commit_processing_state
from tag_processor import TagProcessor
from logging import StructuredLogger
from error_handling import validate_data_quality
from utils import yyyymmdd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("gold_hwm_build_job")

logger.info("Starting Gold layer HWM build job", {
    "catalog": config.catalog,
    "silver_schema": config.silver_schema,
    "gold_schema": config.gold_schema,
    "environment": config.ENV
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_silver_table_name(table_name: str) -> str:
    """Get full Silver table name"""
    return f"{config.catalog}.{config.silver_schema}.{table_name}"

def get_gold_table_name(table_name: str) -> str:
    """Get full Gold table name"""
    return f"{config.catalog}.{config.gold_schema}.{table_name}"

def read_silver_since_timestamp(spark, table_name: str, last_timestamp: Optional[datetime], strategy: str = "updated_time") -> 'DataFrame':
    """
    Read data from Silver table since last timestamp using the best strategy
    
    Args:
        spark: Spark session
        table_name: Silver table name
        last_timestamp: Last processed timestamp
        strategy: Strategy for reading incremental data
            - "updated_time": Use updated_time column (recommended)
            - "date_sk": Use date_sk column (legacy)
            - "hybrid": Use both for maximum accuracy
    """
    silver_table = get_silver_table_name(table_name)
    
    if last_timestamp:
        if strategy == "updated_time":
            # Best approach: Use updated_time for true incremental processing
            query = f"""
            SELECT * FROM {silver_table}
            WHERE updated_time > '{last_timestamp.isoformat()}'
            ORDER BY updated_time
            """
            logger.info(f"Reading incremental data from {silver_table} since {last_timestamp} using updated_time strategy")
        
        elif strategy == "date_sk":
            # Legacy approach: Convert timestamp to date_sk
            last_date_sk = int(last_timestamp.strftime('%Y%m%d'))
            query = f"""
            SELECT * FROM {silver_table}
            WHERE date_sk >= {last_date_sk}
            ORDER BY date_sk, updated_time
            """
            logger.info(f"Reading incremental data from {silver_table} since date_sk {last_date_sk} using date_sk strategy")
        
        elif strategy == "hybrid":
            # Hybrid approach: Use both for maximum accuracy
            last_date_sk = int(last_timestamp.strftime('%Y%m%d'))
            query = f"""
            SELECT * FROM {silver_table}
            WHERE (date_sk > {last_date_sk}) 
               OR (date_sk = {last_date_sk} AND updated_time > '{last_timestamp.isoformat()}')
            ORDER BY date_sk, updated_time
            """
            logger.info(f"Reading incremental data from {silver_table} using hybrid strategy (date_sk + updated_time)")
        
        else:
            # Default to updated_time
            query = f"""
            SELECT * FROM {silver_table}
            WHERE updated_time > '{last_timestamp.isoformat()}'
            ORDER BY updated_time
            """
            logger.info(f"Reading incremental data from {silver_table} since {last_timestamp} using default updated_time strategy")
    else:
        # Read all data for initial load
        query = f"SELECT * FROM {silver_table}"
        logger.info(f"Reading all data from {silver_table} for initial load")
    
    return spark.sql(query)

def get_max_date_sk(df: 'DataFrame', date_sk_col: str = 'date_sk') -> Optional[int]:
    """Get maximum date_sk from DataFrame"""
    if df.count() == 0:
        return None
    
    max_date_sk = df.agg({date_sk_col: 'max'}).collect()[0][0]
    return max_date_sk if max_date_sk else None

def validate_gold_data(df: 'DataFrame', table_name: str) -> bool:
    """Validate data quality for Gold layer"""
    if df.count() == 0:
        logger.info(f"No data to validate for {table_name}")
        return True
    
    # Basic validation rules
    validation_rules = {
        'date_sk': 'not_null',
        'workspace_id': 'not_null'
    }
    
    return validate_data_quality(df, table_name, logger, validation_rules)

def get_impacted_dates(spark, start_date_sk: int, end_date_sk: int) -> List[int]:
    """Get list of impacted date_sk values"""
    dates = []
    current_date_sk = start_date_sk
    
    while current_date_sk <= end_date_sk:
        dates.append(current_date_sk)
        # Move to next date (assuming date_sk is in YYYYMMDD format)
        current_date = datetime.strptime(str(current_date_sk), '%Y%m%d')
        next_date = current_date + timedelta(days=1)
        current_date_sk = int(next_date.strftime('%Y%m%d'))
    
    return dates

def merge_into_fact_table(spark, df: 'DataFrame', table_name: str, table_type: str) -> None:
    """
    Merge data into fact table to handle overlapping days correctly.
    This prevents data duplication when processing the same date_sk multiple times.
    
    Args:
        spark: Spark session
        df: DataFrame with new data
        table_name: Full table name to merge into
        table_type: Type of fact table for logging
    """
    if df.count() == 0:
        logger.info(f"No data to merge into {table_type}")
        return
    
    # Get unique date_sk values to process
    date_sk_values = df.select("date_sk").distinct().collect()
    date_sk_list = [str(row.date_sk) for row in date_sk_values]
    
    logger.info(f"Merging {df.count()} records for {table_type} with date_sk: {date_sk_list}")
    
    # Create temporary view for the new data
    df.createOrReplaceTempView("new_data")
    
    # Build MERGE statement based on table type
    if table_type == "gld_fact_usage_priced_day":
        merge_sql = f"""
        MERGE INTO {table_name} T
        USING new_data S
        ON T.date_sk = S.date_sk 
           AND T.workspace_id = S.workspace_id 
           AND T.entity_type = S.entity_type 
           AND T.entity_id = S.entity_id 
           AND T.cloud = S.cloud 
           AND T.sku_name = S.sku_name 
           AND T.usage_unit = S.usage_unit
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    elif table_type == "gld_fact_entity_cost":
        merge_sql = f"""
        MERGE INTO {table_name} T
        USING new_data S
        ON T.date_sk = S.date_sk 
           AND T.workspace_id = S.workspace_id 
           AND T.entity_type = S.entity_type 
           AND T.entity_id = S.entity_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    elif table_type == "gld_fact_run_cost":
        merge_sql = f"""
        MERGE INTO {table_name} T
        USING new_data S
        ON T.date_sk = S.date_sk 
           AND T.workspace_id = S.workspace_id 
           AND T.entity_type = S.entity_type 
           AND T.entity_id = S.entity_id 
           AND T.run_id = S.run_id 
           AND T.cloud = S.cloud 
           AND T.sku_name = S.sku_name 
           AND T.usage_unit = S.usage_unit
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    elif table_type == "gld_fact_run_status_cost":
        merge_sql = f"""
        MERGE INTO {table_name} T
        USING new_data S
        ON T.date_sk = S.date_sk 
           AND T.workspace_id = S.workspace_id 
           AND T.entity_type = S.entity_type 
           AND T.entity_id = S.entity_id 
           AND T.run_id = S.run_id 
           AND COALESCE(T.result_state, '') = COALESCE(S.result_state, '') 
           AND COALESCE(T.termination_code, '') = COALESCE(S.termination_code, '')
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    elif table_type == "gld_fact_runs_finished_day":
        merge_sql = f"""
        MERGE INTO {table_name} T
        USING new_data S
        ON T.date_sk = S.date_sk 
           AND T.workspace_id = S.workspace_id 
           AND T.entity_type = S.entity_type 
           AND T.entity_id = S.entity_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    else:
        # Default merge for unknown table types
        merge_sql = f"""
        MERGE INTO {table_name} T
        USING new_data S
        ON T.date_sk = S.date_sk
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    
    # Execute MERGE
    try:
        spark.sql(merge_sql)
        logger.info(f"✅ Successfully merged data into {table_type}")
    except Exception as e:
        logger.error(f"❌ Error merging data into {table_type}: {str(e)}")
        raise

def build_usage_facts_with_scd2_alignment(spark, usage_df):
    """Build usage facts with proper SCD2 dimension alignment"""
    logger.info("Building usage facts with SCD2 dimension alignment")
    
    try:
        # Create temp view for the usage data
        usage_df.createOrReplaceTempView("usage_data")
        
        # Join with SCD2 dimensions to get the correct version for each date
        usage_with_dimensions_df = spark.sql(f"""
            SELECT 
                u.*,
                -- Job dimension (SCD2 aligned)
                j.job_name,
                j.job_type,
                j.owner,
                j.valid_from as job_valid_from,
                j.valid_to as job_valid_to,
                -- Pipeline dimension (SCD2 aligned)
                p.pipeline_name,
                p.pipeline_type,
                p.valid_from as pipeline_valid_from,
                p.valid_to as pipeline_valid_to,
                -- Cluster dimension (SCD2 aligned)
                c.cluster_name,
                c.spark_version,
                c.valid_from as cluster_valid_from,
                c.valid_to as cluster_valid_to
            FROM usage_data u
            LEFT JOIN {get_silver_table_name("slv_jobs_scd")} j
                ON u.job_id = j.job_id
                AND u.date_sk BETWEEN j.valid_from AND COALESCE(j.valid_to, 99991231)
            LEFT JOIN {get_silver_table_name("slv_pipelines_scd")} p
                ON u.pipeline_id = p.pipeline_id
                AND u.date_sk BETWEEN p.valid_from AND COALESCE(p.valid_to, 99991231)
            LEFT JOIN {get_silver_table_name("slv_clusters")} c
                ON u.cluster_id = c.cluster_id
                AND u.date_sk BETWEEN c.valid_from AND COALESCE(c.valid_to, 99991231)
        """)
        
        # Group by dimensions and aggregate
        usage_fact_df = usage_with_dimensions_df.groupBy(
            "date_sk", "workspace_id", "entity_type", "entity_id", 
            "cloud", "sku_name", "usage_unit",
            "job_id", "pipeline_id", "cluster_id"
        ).agg(
            sum("usage_quantity").alias("usage_quantity"),
            sum("list_cost_usd").alias("list_cost_usd"),
            sum("duration_hours").alias("duration_hours"),
            max("line_of_business").alias("line_of_business"),
            max("department").alias("department"),
            max("cost_center").alias("cost_center"),
            max("environment").alias("environment"),
            max("use_case").alias("use_case"),
            max("workflow_level").alias("workflow_level"),
            max("parent_workflow_name").alias("parent_workflow_name")
        )
        
        logger.info(f"✅ Successfully built usage facts with SCD2 alignment: {usage_fact_df.count()} records")
        return usage_fact_df
        
    except Exception as e:
        logger.error(f"Error building usage facts with SCD2 alignment: {str(e)}", exc_info=True)
        raise

def delete_existing_data_for_dates(spark, table_name: str, date_sk_list: List[int]) -> None:
    """
    Delete existing data for specific date_sk values before merging.
    This is useful for complete refresh scenarios or when data needs to be reprocessed.
    
    Args:
        spark: Spark session
        table_name: Full table name to delete from
        date_sk_list: List of date_sk values to delete
    """
    if not date_sk_list:
        return
    
    # Convert date_sk list to SQL IN clause
    date_sk_str = ",".join(map(str, date_sk_list))
    
    delete_sql = f"""
    DELETE FROM {table_name}
    WHERE date_sk IN ({date_sk_str})
    """
    
    try:
        logger.info(f"Deleting existing data for date_sk: {date_sk_list} from {table_name}")
        spark.sql(delete_sql)
        logger.info(f"✅ Successfully deleted existing data for date_sk: {date_sk_list}")
    except Exception as e:
        logger.error(f"❌ Error deleting existing data: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table Builders

# COMMAND ----------

def build_gold_dimensions_scd2_aware(spark) -> bool:
    """Build Gold dimension tables with SCD2 versioning preserved"""
    logger.info("Building Gold dimension tables with SCD2 awareness")
    
    try:
        # Build workspace dimension (no SCD2, direct copy)
        workspace_df = spark.table(get_silver_table_name("slv_workspace"))
        gold_workspace = get_gold_table_name("gld_dim_workspace")
        workspace_df.write.mode("overwrite").saveAsTable(gold_workspace)
        logger.info(f"Built workspace dimension with {workspace_df.count()} records")
        
        # Build jobs dimension (SCD2 - preserve all versions using MERGE)
        jobs_df = spark.table(get_silver_table_name("slv_jobs_scd"))
        gold_jobs = get_gold_table_name("gld_dim_job")
        
        # Use MERGE to handle SCD2 updates incrementally
        jobs_df.createOrReplaceTempView("new_jobs")
        
        merge_sql = f"""
        MERGE INTO {gold_jobs} T
        USING new_jobs S
        ON T.job_id = S.job_id AND T.valid_from = S.valid_from
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        spark.sql(merge_sql)
        logger.info(f"Built job dimension with {jobs_df.count()} records (all SCD2 versions preserved)")
        
        # Build pipelines dimension (SCD2 - preserve all versions using MERGE)
        pipelines_df = spark.table(get_silver_table_name("slv_pipelines_scd"))
        gold_pipelines = get_gold_table_name("gld_dim_pipeline")
        
        pipelines_df.createOrReplaceTempView("new_pipelines")
        
        merge_sql = f"""
        MERGE INTO {gold_pipelines} T
        USING new_pipelines S
        ON T.pipeline_id = S.pipeline_id AND T.valid_from = S.valid_from
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        spark.sql(merge_sql)
        logger.info(f"Built pipeline dimension with {pipelines_df.count()} records (all SCD2 versions preserved)")
        
        # Build clusters dimension (SCD2 - preserve all versions using MERGE)
        clusters_df = spark.table(get_silver_table_name("slv_clusters"))
        gold_clusters = get_gold_table_name("gld_dim_cluster")
        
        clusters_df.createOrReplaceTempView("new_clusters")
        
        merge_sql = f"""
        MERGE INTO {gold_clusters} T
        USING new_clusters S
        ON T.cluster_id = S.cluster_id AND T.valid_from = S.valid_from
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        spark.sql(merge_sql)
        logger.info(f"Built cluster dimension with {clusters_df.count()} records (all SCD2 versions preserved)")
        
        # Build SKU dimension (no SCD2, direct copy)
        sku_df = spark.table(get_silver_table_name("slv_usage_txn")) \
                      .select("cloud", "sku_name", "usage_unit") \
                      .distinct()
        gold_sku = get_gold_table_name("gld_dim_sku")
        
        sku_df.write.mode("overwrite").saveAsTable(gold_sku)
        logger.info(f"Built SKU dimension with {sku_df.count()} records")
        
        # Build run status dimension (no SCD2, direct copy)
        status_df = spark.table(get_silver_table_name("slv_job_run_timeline")) \
                        .select("result_state", "termination_code") \
                        .distinct()
        gold_status = get_gold_table_name("gld_dim_run_status")
        
        status_df.write.mode("overwrite").saveAsTable(gold_status)
        logger.info(f"Built run status dimension with {status_df.count()} records")
        
        # Build node type dimension (no SCD2, direct copy)
        node_type_df = spark.table(get_silver_table_name("slv_compute_node_types")) \
                            .select("node_type_id", "node_type_name", "cloud", "category") \
                            .distinct()
        gold_node_type = get_gold_table_name("gld_dim_node_type")
        
        node_type_df.write.mode("overwrite").saveAsTable(gold_node_type)
        logger.info(f"Built node type dimension with {node_type_df.count()} records")
        
        logger.info("✅ Successfully built all Gold dimension tables with SCD2 versions preserved")
        return True
        
    except Exception as e:
        logger.error(f"Error building Gold dimension tables: {str(e)}", exc_info=True)
        return False

def build_gold_facts(spark, complete_refresh: bool = False, strategy: str = "updated_time") -> bool:
    """
    Build Gold fact tables
    
    Args:
        spark: Spark session
        complete_refresh: If True, delete existing data for overlapping dates before merging
        strategy: Strategy for reading incremental data
            - "updated_time": Use updated_time column (recommended)
            - "date_sk": Use date_sk column (legacy)
            - "hybrid": Use both for maximum accuracy
    """
    logger.info("Building Gold fact tables")
    
    try:
        # Get last processed timestamp for facts
        last_timestamp = get_last_processed_timestamp(spark, "gold_facts")
        
        # Read new data from Silver usage transaction with specified strategy
        usage_df = read_silver_since_timestamp(spark, "slv_usage_txn", last_timestamp, strategy)
        
        # If complete refresh is requested, delete existing data for overlapping dates
        if complete_refresh:
            date_sk_values = usage_df.select("date_sk").distinct().collect()
            date_sk_list = [row.date_sk for row in date_sk_values]
            
            # Delete existing data for these dates from all fact tables
            fact_tables = [
                "gld_fact_usage_priced_day",
                "gld_fact_entity_cost", 
                "gld_fact_run_cost",
                "gld_fact_run_status_cost",
                "gld_fact_runs_finished_day"
            ]
            
            for table in fact_tables:
                table_name = get_gold_table_name(table)
                delete_existing_data_for_dates(spark, table_name, date_sk_list)
            
            logger.info(f"Complete refresh: Deleted existing data for date_sk: {date_sk_list}")
        
        # Validate data
        if not validate_gold_data(usage_df, "gold_facts"):
            logger.error("Data validation failed for Gold fact tables")
            return False
        
        # Build usage fact table with SCD2 dimension alignment
        usage_fact_df = build_usage_facts_with_scd2_alignment(spark, usage_df)
        
        # Write to Gold fact table using MERGE to handle overlapping days
        gold_usage_fact = get_gold_table_name("gld_fact_usage_priced_day")
        merge_into_fact_table(spark, usage_fact_df, gold_usage_fact, "gld_fact_usage_priced_day")
        
        # Build entity cost fact table
        entity_cost_df = usage_df.groupBy("date_sk", "workspace_id", "entity_type", "entity_id") \
                                .agg(
                                    sum("list_cost_usd").alias("list_cost_usd"),
                                    countDistinct("run_id").alias("runs_count")
                                )
        
        gold_entity_cost = get_gold_table_name("gld_fact_entity_cost")
        merge_into_fact_table(spark, entity_cost_df, gold_entity_cost, "gld_fact_entity_cost")
        
        # Build run cost fact table
        run_cost_df = usage_df.groupBy("date_sk", "workspace_id", "entity_type", "entity_id", 
                                      "run_id", "cloud", "sku_name", "usage_unit") \
                              .agg(
                                  sum("list_cost_usd").alias("list_cost_usd"),
                                  sum("usage_quantity").alias("usage_quantity"),
                                  sum("duration_hours").alias("duration_hours")
                              )
        
        gold_run_cost = get_gold_table_name("gld_fact_run_cost")
        merge_into_fact_table(spark, run_cost_df, gold_run_cost, "gld_fact_run_cost")
        
        # Build run status cost fact table
        run_status_df = usage_df.join(
            spark.table(get_silver_table_name("slv_job_run_timeline")),
            (usage_df.workspace_id == col("slv_job_run_timeline.workspace_id")) &
            (usage_df.run_id == col("slv_job_run_timeline.run_id")),
            "left"
        ).groupBy("date_sk", "workspace_id", "entity_type", "entity_id", "run_id", 
                  "result_state", "termination_code") \
         .agg(sum("list_cost_usd").alias("result_state_cost_usd"))
        
        gold_run_status_cost = get_gold_table_name("gld_fact_run_status_cost")
        merge_into_fact_table(spark, run_status_df, gold_run_status_cost, "gld_fact_run_status_cost")
        
        # Build runs finished fact table
        runs_finished_df = spark.table(get_silver_table_name("slv_job_run_timeline")) \
                               .groupBy("date_sk_end", "workspace_id") \
                               .agg(
                                   count("*").alias("finished_runs"),
                                   sum(when(col("result_state") == "SUCCESS", 1).otherwise(0)).alias("success_runs"),
                                   sum(when(col("result_state") == "FAILED", 1).otherwise(0)).alias("failed_runs"),
                                   sum(when(col("result_state") == "CANCELED", 1).otherwise(0)).alias("cancelled_runs")
                               ) \
                               .withColumn("entity_type", lit("JOB")) \
                               .withColumn("entity_id", lit("ALL"))
        
        gold_runs_finished = get_gold_table_name("gld_fact_runs_finished_day")
        merge_into_fact_table(spark, runs_finished_df, gold_runs_finished, "gld_fact_runs_finished_day")
        
        # Update processing state with timestamp for better incremental processing
        max_timestamp = get_max_timestamp(usage_df, "updated_time")
        if max_timestamp:
            commit_processing_state(spark, "gold_facts", max_timestamp)
        
        logger.info(f"✅ Successfully built all Gold fact tables with data up to timestamp {max_timestamp}")
        return True
        
    except Exception as e:
        logger.error(f"Error building Gold fact tables: {str(e)}", exc_info=True)
        return False

def build_gold_views(spark) -> bool:
    """Build Gold business views"""
    logger.info("Building Gold business views")
    
    try:
        # Build cost trends view
        cost_trends_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_cost_trends")} AS
        SELECT 
            date_sk,
            workspace_id,
            SUM(list_cost_usd) as daily_cost,
            AVG(list_cost_usd) OVER (PARTITION BY workspace_id ORDER BY date_sk ROWS 7 PRECEDING) as rolling_7d_avg_cost
        FROM {get_gold_table_name("gld_fact_usage_priced_day")}
        GROUP BY date_sk, workspace_id
        """
        spark.sql(cost_trends_sql)
        
        # Build cost anomalies view
        cost_anomalies_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_cost_anomalies")} AS
        SELECT 
            date_sk,
            workspace_id,
            daily_cost,
            rolling_7d_avg_cost,
            CASE 
                WHEN daily_cost > rolling_7d_avg_cost * 2 THEN 'HIGH_COST_SPIKE'
                WHEN daily_cost < rolling_7d_avg_cost * 0.5 THEN 'LOW_COST_DROP'
                ELSE 'NORMAL'
            END as cost_anomaly_type
        FROM {get_gold_table_name("v_cost_trends")}
        WHERE daily_cost > rolling_7d_avg_cost * 2 
           OR daily_cost < rolling_7d_avg_cost * 0.5
        """
        spark.sql(cost_anomalies_sql)
        
        logger.info("✅ Successfully built Gold business views")
        return True
        
    except Exception as e:
        logger.error(f"Error building Gold business views: {str(e)}", exc_info=True)
        return False

def build_gold_chargeback_views(spark) -> bool:
    """Build Gold chargeback views"""
    logger.info("Building Gold chargeback views")
    
    try:
        # Build tag coverage summary view
        tag_coverage_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_tag_coverage_summary")} AS
        SELECT 
            workspace_id,
            COUNT(*) as total_records,
            COUNT(CASE WHEN cost_center IS NOT NULL THEN 1 END) as cost_center_coverage,
            COUNT(CASE WHEN line_of_business IS NOT NULL THEN 1 END) as lob_coverage,
            COUNT(CASE WHEN department IS NOT NULL THEN 1 END) as dept_coverage,
            ROUND(COUNT(CASE WHEN cost_center IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as cost_center_pct,
            ROUND(COUNT(CASE WHEN line_of_business IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as lob_pct,
            ROUND(COUNT(CASE WHEN department IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as dept_pct
        FROM {get_gold_table_name("gld_fact_usage_priced_day")}
        GROUP BY workspace_id
        """
        spark.sql(tag_coverage_sql)
        
        # Build cost center analysis view
        cost_center_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_cost_center_analysis")} AS
        SELECT 
            cost_center,
            workspace_id,
            SUM(list_cost_usd) as total_cost,
            COUNT(*) as record_count,
            AVG(list_cost_usd) as avg_cost_per_record
        FROM {get_gold_table_name("gld_fact_usage_priced_day")}
        WHERE cost_center IS NOT NULL
        GROUP BY cost_center, workspace_id
        ORDER BY total_cost DESC
        """
        spark.sql(cost_center_sql)
        
        # Build workflow hierarchy cost view
        workflow_cost_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_workflow_hierarchy_cost")} AS
        SELECT 
            workflow_level,
            parent_workflow_name,
            workspace_id,
            SUM(list_cost_usd) as total_cost,
            COUNT(*) as record_count
        FROM {get_gold_table_name("gld_fact_usage_priced_day")}
        WHERE workflow_level IS NOT NULL
        GROUP BY workflow_level, parent_workflow_name, workspace_id
        ORDER BY total_cost DESC
        """
        spark.sql(workflow_cost_sql)
        
        logger.info("✅ Successfully built Gold chargeback views")
        return True
        
    except Exception as e:
        logger.error(f"Error building Gold chargeback views: {str(e)}", exc_info=True)
        return False

def build_gold_runtime_analysis_views(spark) -> bool:
    """Build Gold runtime analysis views"""
    logger.info("Building Gold runtime analysis views")
    
    try:
        # Build runtime version analysis view
        runtime_analysis_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_runtime_version_analysis")} AS
        SELECT 
            c.spark_version,
            c.cluster_source,
            COUNT(*) as cluster_count,
            SUM(f.list_cost_usd) as total_cost,
            AVG(f.list_cost_usd) as avg_cost_per_cluster
        FROM {get_gold_table_name("gld_fact_usage_priced_day")} f
        JOIN {get_silver_table_name("slv_clusters")} c 
            ON f.cluster_identifier = c.cluster_id
        GROUP BY c.spark_version, c.cluster_source
        ORDER BY total_cost DESC
        """
        spark.sql(runtime_analysis_sql)
        
        # Build node type analysis view
        node_type_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_node_type_analysis")} AS
        SELECT 
            c.node_type_id,
            c.driver_node_type_id,
            COUNT(*) as cluster_count,
            SUM(f.list_cost_usd) as total_cost,
            AVG(f.list_cost_usd) as avg_cost_per_cluster
        FROM {get_gold_table_name("gld_fact_usage_priced_day")} f
        JOIN {get_silver_table_name("slv_clusters")} c 
            ON f.cluster_identifier = c.cluster_id
        GROUP BY c.node_type_id, c.driver_node_type_id
        ORDER BY total_cost DESC
        """
        spark.sql(node_type_sql)
        
        # Build runtime modernization opportunities view
        modernization_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_runtime_modernization_opportunities")} AS
        SELECT 
            c.spark_version,
            c.workspace_id,
            COUNT(*) as cluster_count,
            SUM(f.list_cost_usd) as total_cost,
            'Consider upgrading to latest runtime' as recommendation
        FROM {get_gold_table_name("gld_fact_usage_priced_day")} f
        JOIN {get_silver_table_name("slv_clusters")} c 
            ON f.cluster_identifier = c.cluster_id
        WHERE c.spark_version NOT LIKE '%latest%'
        GROUP BY c.spark_version, c.workspace_id
        ORDER BY total_cost DESC
        """
        spark.sql(modernization_sql)
        
        logger.info("✅ Successfully built Gold runtime analysis views")
        return True
        
    except Exception as e:
        logger.error(f"Error building Gold runtime analysis views: {str(e)}", exc_info=True)
        return False

def build_gold_policy_compliance_views(spark) -> bool:
    """Build Gold policy compliance views"""
    logger.info("Building Gold policy compliance views")
    
    try:
        # Build policy compliance summary view
        compliance_sql = f"""
        CREATE OR REPLACE VIEW {get_gold_table_name("v_policy_compliance_summary")} AS
        SELECT 
            workspace_id,
            COUNT(*) as total_records,
            COUNT(CASE WHEN cost_center IS NOT NULL THEN 1 END) as compliant_records,
            COUNT(CASE WHEN cost_center IS NULL THEN 1 END) as non_compliant_records,
            ROUND(COUNT(CASE WHEN cost_center IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as compliance_rate
        FROM {get_gold_table_name("gld_fact_usage_priced_day")}
        GROUP BY workspace_id
        ORDER BY compliance_rate ASC
        """
        spark.sql(compliance_sql)
        
        logger.info("✅ Successfully built Gold policy compliance views")
        return True
        
    except Exception as e:
        logger.error(f"Error building Gold policy compliance views: {str(e)}", exc_info=True)
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution Function

# COMMAND ----------

def build_gold_layer(spark, complete_refresh: bool = False, strategy: str = "updated_time") -> Dict[str, bool]:
    """
    Build entire Gold layer
    
    Args:
        spark: Spark session
        complete_refresh: If True, perform complete refresh of fact tables for overlapping dates
        strategy: Strategy for reading incremental data
            - "updated_time": Use updated_time column (recommended)
            - "date_sk": Use date_sk column (legacy)
            - "hybrid": Use both for maximum accuracy
    """
    logger.info("Starting Gold layer build process")
    
    if complete_refresh:
        logger.info("🔄 Complete refresh mode enabled - will delete and reprocess overlapping dates")
    
    logger.info(f"📅 Processing strategy: {strategy}")
    
    results = {}
    
    # Build Gold tables in dependency order
    gold_builders = [
        ("dimensions", build_gold_dimensions_scd2_aware),  # SCD2-aware dimension building
        ("facts", lambda s: build_gold_facts(s, complete_refresh, strategy)),  # SCD2-aligned facts
        ("business_views", build_gold_views),
        ("chargeback_views", build_gold_chargeback_views),
        ("runtime_analysis_views", build_gold_runtime_analysis_views),
        ("policy_compliance_views", build_gold_policy_compliance_views)
    ]
    
    for component_name, builder_func in gold_builders:
        logger.info(f"Building Gold component: {component_name}")
        try:
            success = builder_func(spark)
            results[component_name] = success
            
            if success:
                logger.info(f"✅ Successfully built Gold component: {component_name}")
            else:
                logger.error(f"❌ Failed to build Gold component: {component_name}")
                
        except Exception as e:
            logger.error(f"❌ Exception building Gold component {component_name}: {str(e)}", exc_info=True)
            results[component_name] = False
    
    # Summary
    successful_components = [name for name, success in results.items() if success]
    failed_components = [name for name, success in results.items() if not success]
    
    logger.info(f"Gold layer build completed. Successful: {len(successful_components)}, Failed: {len(failed_components)}")
    
    if successful_components:
        logger.info(f"✅ Successfully built components: {', '.join(successful_components)}")
    
    if failed_components:
        logger.error(f"❌ Failed to build components: {', '.join(failed_components)}")
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Gold Layer Build

# COMMAND ----------

if __name__ == "__main__":
    # Get configuration values instead of widgets for standalone notebook execution
    complete_refresh = config.gold_complete_refresh
    strategy = config.gold_processing_strategy
    
    # Validate strategy
    valid_strategies = ["updated_time", "date_sk", "hybrid"]
    if strategy not in valid_strategies:
        logger.warning(f"Invalid strategy '{strategy}' in config, using default 'updated_time'")
        strategy = "updated_time"
    
    logger.info(f"Starting Gold layer build with SCD2 awareness")
    logger.info(f"Configuration: complete_refresh={complete_refresh}, strategy={strategy}")
    logger.info(f"Environment: {config.ENV}")
    
    # Build Gold layer with SCD2 awareness
    results = build_gold_layer(spark, complete_refresh, strategy)
    
    # Check overall success
    all_successful = all(results.values())
    
    if all_successful:
        logger.info("🎉 All Gold layer components built successfully with SCD2 implementation!")
        logger.info("📊 SCD2 benefits: Historical tracking, temporal accuracy, audit trail, and business intelligence support")
        dbutils.notebook.exit("SUCCESS")
    else:
        logger.error("💥 Some Gold layer components failed to build")
        dbutils.notebook.exit("FAILED")
