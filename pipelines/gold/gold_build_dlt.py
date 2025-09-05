"""
DLT Pipeline: Gold Layer Construction

This pipeline consumes Silver layer data and builds the Gold layer with
dimensional models for BI and analytics.

Pipeline Flow:
1. Read data from Silver layer tables
2. Build Gold layer with star schema (dimensions + facts)
3. Apply business logic and aggregations

Author: Platform Observability Team
Version: 1.0
"""

from libs.utils import impacted_dates
from libs.sql_manager import sql_manager

# DLT Gold: consume Silver and build dimensional model
import dlt
from pyspark.sql import functions as F

from config import Config

# Get configuration for catalog and schema names
config = Config.get_config()
CAT = config.catalog
SLV = f"{CAT}.{config.silver_schema}"
GLD = f"{CAT}.{config.gold_schema}"

# =============================================================================
# PHASE 1: READ DATA FROM SILVER LAYER
# =============================================================================
# Read data from Silver layer tables for dimensional modeling

def read_silver_data():
    """Read data from Silver layer tables"""
    global usage_df, run_df, task_df, workspace_df, entity_df
    
    # Read from Silver layer
    usage_df = dlt.read("slv_usage_txn")
    run_df = dlt.read("slv_job_run_timeline")
    task_df = dlt.read("slv_job_task_run_timeline")
    workspace_df = dlt.read("slv_workspace")
    entity_df = dlt.read("slv_entity_latest_v")

# =============================================================================
# PHASE 2: BUILD GOLD LAYER
# =============================================================================
# Build star schema with dimensions and facts for BI and analytics

def build_gold_layer():
    """Build Gold layer using external SQL files"""
    
    # Calculate impacted dates for incremental processing
    # Only process partitions that have new/changed data
    imp_usage_dates = impacted_dates(usage_df, "usage_date")
    imp_run_dates   = impacted_dates(run_df,   "period_end_time")
    imp_task_dates  = impacted_dates(task_df,  "period_end_time")
    imp_dates = imp_usage_dates.union(imp_run_dates).union(imp_task_dates).distinct()
    imp_dates.createOrReplaceTempView("imp_dates")
    
    # Build dimensions using external SQL
    build_gold_dimensions()
    
    # Build facts using external SQL
    build_gold_facts()

def build_gold_dimensions():
    """Build Gold layer dimensions using external SQL files"""
    try:
        # Execute dimension creation and merge operations
        sql = sql_manager.parameterize_sql_with_catalog_schema("gold/gold_dimensions")
        spark.sql(sql)
        print("✅ Gold dimensions built successfully")
    except Exception as e:
        print(f"❌ Failed to build gold dimensions: {str(e)}")
        raise

def build_gold_facts():
    """Build Gold layer facts using external SQL files"""
    try:
        # Execute fact table creation and merge operations
        sql = sql_manager.parameterize_sql_with_catalog_schema("gold/gold_facts")
        spark.sql(sql)
        print("✅ Gold facts built successfully")
    except Exception as e:
        print(f"❌ Failed to build gold facts: {str(e)}")
        raise

# =============================================================================
# MAIN PIPELINE EXECUTION
# =============================================================================

def main():
    """Main pipeline execution function"""
    print("🚀 Starting Gold DLT pipeline...")
    
    try:
        # Phase 1: Read Silver data
        print("📖 Reading data from Silver layer...")
        read_silver_data()
        
        # Phase 2: Build Gold layer
        print("🏗️ Building Gold layer...")
        build_gold_layer()
        
        print("🎉 Gold DLT pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {str(e)}")
        raise

# Execute main function when pipeline is run
if __name__ == "__main__":
    main()
