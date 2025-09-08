# Databricks notebook source
# MAGIC %md
# MAGIC # Health Check Job
# MAGIC 
# MAGIC This notebook performs comprehensive health checks on all layers of the platform observability solution.
# MAGIC 
# MAGIC ## Features:
# MAGIC - Data freshness checks
# MAGIC - Record count validation
# MAGIC - Data quality assessment
# MAGIC - Performance metrics collection
# MAGIC - Health status reporting
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
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Import from libs package (cloud-agnostic approach)
from libs.path_setup import setup_paths_and_import_config

# Setup paths and import Config
Config = setup_paths_and_import_config()
from logging import StructuredLogger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Logging

# COMMAND ----------

# Get configuration
config = Config.get_config()
logger = StructuredLogger("health_check_job")

logger.info("Starting health check job", 
            catalog=config.catalog,
            bronze_schema=config.bronze_schema,
            silver_schema=config.silver_schema,
            gold_schema=config.gold_schema,
            environment=Config.ENV)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Health Check Functions

# COMMAND ----------

def check_data_freshness(spark) -> Dict[str, Any]:
    """Check data freshness across all layers"""
    logger.info("Checking data freshness")
    
    try:
        freshness_report = {
            "bronze_layer": {},
            "silver_layer": {},
            "gold_layer": {},
            "overall_status": "HEALTHY"
        }
        
        # Bronze layer freshness
        bronze_tables = [
            "brz_billing_usage", "brz_compute_clusters", "brz_lakeflow_jobs",
            "brz_lakeflow_job_run_timeline", "brz_lakeflow_job_task_run_timeline",
            "brz_access_workspaces_latest"
        ]
        
        for table in bronze_tables:
            try:
                # Get latest timestamp
                latest_ts = spark.sql(f"""
                    SELECT MAX(_commit_timestamp) as latest_ts 
                    FROM {config.catalog}.{config.bronze_schema}.{table}
                """).collect()[0][0]
                
                if latest_ts:
                    hours_old = (datetime.now() - latest_ts).total_seconds() / 3600
                    status = "FRESH" if hours_old < 24 else "STALE" if hours_old < 48 else "VERY_STALE"
                    
                    freshness_report["bronze_layer"][table] = {
                        "latest_timestamp": latest_ts.isoformat(),
                        "hours_old": round(hours_old, 2),
                        "status": status
                    }
                    
                    if status != "FRESH":
                        freshness_report["overall_status"] = "WARNING"
                else:
                    freshness_report["bronze_layer"][table] = {"status": "NO_DATA"}
                    
            except Exception as e:
                freshness_report["bronze_layer"][table] = {"status": "ERROR", "error": str(e)}
                freshness_report["overall_status"] = "CRITICAL"
        
        # Silver layer freshness
        silver_tables = [
            "slv_workspace", "slv_entity_latest", "slv_clusters",
            "slv_usage_txn", "slv_job_run_timeline", "slv_job_task_run_timeline"
        ]
        
        for table in silver_tables:
            try:
                # Get latest date_sk
                latest_date = spark.sql(f"""
                    SELECT MAX(date_sk) as latest_date 
                    FROM {config.catalog}.{config.silver_schema}.{table}
                """).collect()[0][0]
                
                if latest_date:
                    # Convert date_sk to datetime
                    latest_dt = datetime.strptime(str(latest_date), '%Y%m%d')
                    hours_old = (datetime.now() - latest_dt).total_seconds() / 3600
                    status = "FRESH" if hours_old < 24 else "STALE" if hours_old < 48 else "VERY_STALE"
                    
                    freshness_report["silver_layer"][table] = {
                        "latest_date_sk": latest_date,
                        "latest_date": latest_dt.isoformat(),
                        "hours_old": round(hours_old, 2),
                        "status": status
                    }
                    
                    if status != "FRESH":
                        freshness_report["overall_status"] = "WARNING"
                else:
                    freshness_report["silver_layer"][table] = {"status": "NO_DATA"}
                    
            except Exception as e:
                freshness_report["silver_layer"][table] = {"status": "ERROR", "error": str(e)}
                freshness_report["overall_status"] = "CRITICAL"
        
        # Gold layer freshness
        gold_tables = [
            "gld_fact_usage_priced_day", "gld_fact_entity_cost", "gld_fact_run_cost",
            "gld_fact_run_status_cost", "gld_fact_runs_finished_day"
        ]
        
        for table in gold_tables:
            try:
                # Get latest date_sk
                latest_date = spark.sql(f"""
                    SELECT MAX(date_sk) as latest_date 
                    FROM {config.catalog}.{config.gold_schema}.{table}
                """).collect()[0][0]
                
                if latest_date:
                    # Convert date_sk to datetime
                    latest_dt = datetime.strptime(str(latest_date), '%Y%m%d')
                    hours_old = (datetime.now() - latest_dt).total_seconds() / 3600
                    status = "FRESH" if hours_old < 24 else "STALE" if hours_old < 48 else "VERY_STALE"
                    
                    freshness_report["gold_layer"][table] = {
                        "latest_date_sk": latest_date,
                        "latest_date": latest_dt.isoformat(),
                        "hours_old": round(hours_old, 2),
                        "status": status
                    }
                    
                    if status != "FRESH":
                        freshness_report["overall_status"] = "WARNING"
                else:
                    freshness_report["gold_layer"][table] = {"status": "NO_DATA"}
                    
            except Exception as e:
                freshness_report["gold_layer"][table] = {"status": "ERROR", "error": str(e)}
                freshness_report["overall_status"] = "CRITICAL"
        
        logger.info("✅ Successfully completed data freshness check")
        return freshness_report
        
    except Exception as e:
        logger.error(f"Error checking data freshness: {str(e)}", exc_info=True)
        return {"error": str(e), "overall_status": "CRITICAL"}

def check_record_counts(spark) -> Dict[str, Any]:
    """Check record counts across all layers"""
    logger.info("Checking record counts")
    
    try:
        count_report = {
            "bronze_layer": {},
            "silver_layer": {},
            "gold_layer": {},
            "overall_status": "HEALTHY"
        }
        
        # Bronze layer counts
        bronze_tables = [
            "brz_billing_usage", "brz_compute_clusters", "brz_lakeflow_jobs",
            "brz_lakeflow_job_run_timeline", "brz_lakeflow_job_task_run_timeline",
            "brz_access_workspaces_latest"
        ]
        
        for table in bronze_tables:
            try:
                count = spark.table(f"{config.catalog}.{config.bronze_schema}.{table}").count()
                status = "HEALTHY" if count > 0 else "EMPTY"
                
                count_report["bronze_layer"][table] = {
                    "record_count": count,
                    "status": status
                }
                
                if status == "EMPTY":
                    count_report["overall_status"] = "WARNING"
                    
            except Exception as e:
                count_report["bronze_layer"][table] = {"status": "ERROR", "error": str(e)}
                count_report["overall_status"] = "CRITICAL"
        
        # Silver layer counts
        silver_tables = [
            "slv_workspace", "slv_entity_latest", "slv_clusters",
            "slv_usage_txn", "slv_job_run_timeline", "slv_job_task_run_timeline"
        ]
        
        for table in silver_tables:
            try:
                count = spark.table(f"{config.catalog}.{config.silver_schema}.{table}").count()
                status = "HEALTHY" if count > 0 else "EMPTY"
                
                count_report["silver_layer"][table] = {
                    "record_count": count,
                    "status": status
                }
                
                if status == "EMPTY":
                    count_report["overall_status"] = "WARNING"
                    
            except Exception as e:
                count_report["silver_layer"][table] = {"status": "ERROR", "error": str(e)}
                count_report["overall_status"] = "CRITICAL"
        
        # Gold layer counts
        gold_tables = [
            "gld_dim_workspace", "gld_dim_entity", "gld_dim_sku", "gld_dim_run_status",
            "gld_fact_usage_priced_day", "gld_fact_entity_cost", "gld_fact_run_cost",
            "gld_fact_run_status_cost", "gld_fact_runs_finished_day"
        ]
        
        for table in gold_tables:
            try:
                count = spark.table(f"{config.catalog}.{config.gold_schema}.{table}").count()
                status = "HEALTHY" if count > 0 else "EMPTY"
                
                count_report["gold_layer"][table] = {
                    "record_count": count,
                    "status": status
                }
                
                if status == "EMPTY":
                    count_report["overall_status"] = "WARNING"
                    
            except Exception as e:
                count_report["gold_layer"][table] = {"status": "ERROR", "error": str(e)}
                count_report["overall_status"] = "CRITICAL"
        
        logger.info("✅ Successfully completed record count check")
        return count_report
        
    except Exception as e:
        logger.error(f"Error checking record counts: {str(e)}", exc_info=True)
        return {"error": str(e), "overall_status": "CRITICAL"}

def check_data_quality(spark) -> Dict[str, Any]:
    """Check data quality across all layers"""
    logger.info("Checking data quality")
    
    try:
        quality_report = {
            "bronze_layer": {},
            "silver_layer": {},
            "gold_layer": {},
            "overall_status": "HEALTHY"
        }
        
        # Bronze layer quality checks
        bronze_quality_checks = [
            {
                "table": "brz_billing_usage",
                "checks": [
                    "SELECT COUNT(*) as null_workspace_id FROM {table} WHERE workspace_id IS NULL",
                    "SELECT COUNT(*) as negative_usage FROM {table} WHERE usage_quantity < 0",
                    "SELECT COUNT(*) as invalid_dates FROM {table} WHERE usage_start_time > usage_end_time"
                ]
            }
        ]
        
        for check_config in bronze_quality_checks:
            table = check_config["table"]
            quality_report["bronze_layer"][table] = {}
            
            for i, check in enumerate(check_config["checks"]):
                try:
                    check_sql = check.format(table=f"{config.catalog}.{config.bronze_schema}.{table}")
                    result = spark.sql(check_sql).collect()[0][0]
                    
                    quality_report["bronze_layer"][table][f"check_{i+1}"] = {
                        "result": result,
                        "status": "PASS" if result == 0 else "FAIL"
                    }
                    
                    if result > 0:
                        quality_report["overall_status"] = "WARNING"
                        
                except Exception as e:
                    quality_report["bronze_layer"][table][f"check_{i+1}"] = {
                        "status": "ERROR",
                        "error": str(e)
                    }
                    quality_report["overall_status"] = "CRITICAL"
        
        # Silver layer quality checks
        silver_quality_checks = [
            {
                "table": "slv_usage_txn",
                "checks": [
                    "SELECT COUNT(*) as null_workspace_id FROM {table} WHERE workspace_id IS NULL",
                    "SELECT COUNT(*) as null_date_sk FROM {table} WHERE date_sk IS NULL",
                    "SELECT COUNT(*) as negative_cost FROM {table} WHERE list_cost_usd < 0"
                ]
            }
        ]
        
        for check_config in silver_quality_checks:
            table = check_config["table"]
            quality_report["silver_layer"][table] = {}
            
            for i, check in enumerate(check_config["checks"]):
                try:
                    check_sql = check.format(table=f"{config.catalog}.{config.silver_schema}.{table}")
                    result = spark.sql(check_sql).collect()[0][0]
                    
                    quality_report["silver_layer"][table][f"check_{i+1}"] = {
                        "result": result,
                        "status": "PASS" if result == 0 else "FAIL"
                    }
                    
                    if result > 0:
                        quality_report["overall_status"] = "WARNING"
                        
                except Exception as e:
                    quality_report["silver_layer"][table][f"check_{i+1}"] = {
                        "status": "ERROR",
                        "error": str(e)
                    }
                    quality_report["overall_status"] = "CRITICAL"
        
        # Gold layer quality checks
        gold_quality_checks = [
            {
                "table": "gld_fact_usage_priced_day",
                "checks": [
                    "SELECT COUNT(*) as null_workspace_id FROM {table} WHERE workspace_id IS NULL",
                    "SELECT COUNT(*) as null_date_sk FROM {table} WHERE date_sk IS NULL",
                    "SELECT COUNT(*) as negative_cost FROM {table} WHERE list_cost_usd < 0"
                ]
            }
        ]
        
        for check_config in gold_quality_checks:
            table = check_config["table"]
            quality_report["gold_layer"][table] = {}
            
            for i, check in enumerate(check_config["checks"]):
                try:
                    check_sql = check.format(table=f"{config.catalog}.{config.gold_schema}.{table}")
                    result = spark.sql(check_sql).collect()[0][0]
                    
                    quality_report["gold_layer"][table][f"check_{i+1}"] = {
                        "result": result,
                        "status": "PASS" if result == 0 else "FAIL"
                    }
                    
                    if result > 0:
                        quality_report["overall_status"] = "WARNING"
                        
                except Exception as e:
                    quality_report["gold_layer"][table][f"check_{i+1}"] = {
                        "status": "ERROR",
                        "error": str(e)
                    }
                    quality_report["overall_status"] = "CRITICAL"
        
        logger.info("✅ Successfully completed data quality check")
        return quality_report
        
    except Exception as e:
        logger.error(f"Error checking data quality: {str(e)}", exc_info=True)
        return {"error": str(e), "overall_status": "CRITICAL"}

def check_processing_offsets(spark) -> Dict[str, Any]:
    """Check processing offsets health"""
    logger.info("Checking processing offsets")
    
    try:
        offset_report = {
            "bronze_hwm_offsets": {},
            "cdf_offsets": {},
            "overall_status": "HEALTHY"
        }
        
        # Check Bronze HWM offsets
        try:
            hwm_offsets = spark.table(f"{config.catalog}.{config.bronze_schema}._bronze_hwm_processing_offsets")
            hwm_count = hwm_offsets.count()
            
            if hwm_count > 0:
                # Check if any offsets are too old
                old_offsets = hwm_offsets.filter(
                    col("last_processed_timestamp") < (datetime.now() - timedelta(hours=72))
                ).count()
                
                status = "HEALTHY" if old_offsets == 0 else "WARNING"
                offset_report["bronze_hwm_offsets"] = {
                    "total_offsets": hwm_count,
                    "old_offsets": old_offsets,
                    "status": status
                }
                
                if status == "WARNING":
                    offset_report["overall_status"] = "WARNING"
            else:
                offset_report["bronze_hwm_offsets"] = {"status": "NO_OFFSETS"}
                
        except Exception as e:
            offset_report["bronze_hwm_offsets"] = {"status": "ERROR", "error": str(e)}
            offset_report["overall_status"] = "CRITICAL"
        
        # Check CDF offsets
        try:
            cdf_offsets = spark.table(f"{config.catalog}.{config.silver_schema}._cdf_processing_offsets")
            cdf_count = cdf_offsets.count()
            
            if cdf_count > 0:
                # Check if any offsets are too old
                old_offsets = cdf_offsets.filter(
                    col("last_processed_timestamp") < (datetime.now() - timedelta(hours=72))
                ).count()
                
                status = "HEALTHY" if old_offsets == 0 else "WARNING"
                offset_report["cdf_offsets"] = {
                    "total_offsets": cdf_count,
                    "old_offsets": old_offsets,
                    "status": status
                }
                
                if status == "WARNING":
                    offset_report["overall_status"] = "WARNING"
            else:
                offset_report["cdf_offsets"] = {"status": "NO_OFFSETS"}
                
        except Exception as e:
            offset_report["cdf_offsets"] = {"status": "ERROR", "error": str(e)}
            offset_report["overall_status"] = "CRITICAL"
        
        logger.info("✅ Successfully completed processing offsets check")
        return offset_report
        
    except Exception as e:
        logger.error(f"Error checking processing offsets: {str(e)}", exc_info=True)
        return {"error": str(e), "overall_status": "CRITICAL"}

def generate_health_summary(spark) -> Dict[str, Any]:
    """Generate comprehensive health summary"""
    logger.info("Generating health summary")
    
    try:
        # Run all health checks
        freshness_report = check_data_freshness(spark)
        count_report = check_record_counts(spark)
        quality_report = check_data_quality(spark)
        offset_report = check_processing_offsets(spark)
        
        # Determine overall health status
        statuses = [
            freshness_report.get("overall_status", "UNKNOWN"),
            count_report.get("overall_status", "UNKNOWN"),
            quality_report.get("overall_status", "UNKNOWN"),
            offset_report.get("overall_status", "UNKNOWN")
        ]
        
        if "CRITICAL" in statuses:
            overall_status = "CRITICAL"
        elif "WARNING" in statuses:
            overall_status = "WARNING"
        else:
            overall_status = "HEALTHY"
        
        # Generate summary
        summary = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": overall_status,
            "checks": {
                "data_freshness": freshness_report,
                "record_counts": count_report,
                "data_quality": quality_report,
                "processing_offsets": offset_report
            },
            "recommendations": []
        }
        
        # Add recommendations based on findings
        if overall_status == "CRITICAL":
            summary["recommendations"].append("Immediate attention required - critical issues detected")
        elif overall_status == "WARNING":
            summary["recommendations"].append("Review warnings and address data quality issues")
        else:
            summary["recommendations"].append("System is healthy - continue monitoring")
        
        # Add specific recommendations
        if freshness_report.get("overall_status") == "WARNING":
            summary["recommendations"].append("Some tables have stale data - check pipeline execution")
        
        if quality_report.get("overall_status") == "WARNING":
            summary["recommendations"].append("Data quality issues detected - review validation rules")
        
        if offset_report.get("overall_status") == "WARNING":
            summary["recommendations"].append("Processing offsets are old - check job execution")
        
        logger.info(f"✅ Successfully generated health summary with status: {overall_status}")
        return summary
        
    except Exception as e:
        logger.error(f"Error generating health summary: {str(e)}", exc_info=True)
        return {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "CRITICAL",
            "error": str(e),
            "recommendations": ["System health check failed - investigate immediately"]
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution Function

# COMMAND ----------

def run_health_checks(spark) -> Dict[str, Any]:
    """Run complete health check process"""
    logger.info("Starting health check process")
    
    try:
        # Generate comprehensive health summary
        health_summary = generate_health_summary(spark)
        
        # Log summary
        logger.info(f"Health check completed with overall status: {health_summary['overall_status']}")
        
        # Log recommendations
        if health_summary.get("recommendations"):
            for rec in health_summary["recommendations"]:
                logger.info(f"Recommendation: {rec}")
        
        return health_summary
        
    except Exception as e:
        logger.error(f"Error running health checks: {str(e)}", exc_info=True)
        return {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "CRITICAL",
            "error": str(e),
            "recommendations": ["Health check process failed - investigate immediately"]
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Health Checks

# COMMAND ----------

if __name__ == "__main__":
    # Run health checks
    health_summary = run_health_checks(spark)
    
    # Check overall health status
    overall_status = health_summary.get("overall_status", "UNKNOWN")
    
    if overall_status == "HEALTHY":
        logger.info("🎉 System is healthy!")
        dbutils.notebook.exit("HEALTHY")
    elif overall_status == "WARNING":
        logger.warning("⚠️ System has warnings - review recommendations")
        dbutils.notebook.exit("WARNING")
    else:
        logger.error("💥 System has critical issues - immediate attention required")
        dbutils.notebook.exit("CRITICAL")
