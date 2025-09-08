# Platform Observability - Getting Started Guide

## Table of Contents
- [1. Prerequisites](#1-prerequisites)
- [2. Quick Start](#2-quick-start)
- [3. Detailed Deployment](#3-detailed-deployment)
- [4. Production Deployment](#4-production-deployment)
- [5. Validation](#5-validation)
- [6. Related Documentation](#6-related-documentation)

## 1. Prerequisites

- **Databricks Workspace** with Unity Catalog enabled
- **Python 3.9+** environment
- **Databricks CLI** configured (optional, for advanced deployment)
- **Access to system tables** (billing, lakeflow, compute, access)

## 2. Quick Start

### 2.1 Clone and Setup
```bash
git clone <repository-url>
cd platform-observability
pip install -r requirements.txt
```

### 2.2 Environment Configuration
The system uses `config.py` for configuration management. You can override settings using environment variables:

```bash
# Set environment variables (optional - config.py has defaults)
export ENVIRONMENT=dev
export CATALOG=platform_observability
export BRONZE_SCHEMA=plt_bronze
export SILVER_SCHEMA=plt_silver
export GOLD_SCHEMA=plt_gold
export OVERLAP_HOURS=48
export TIMEZONE=Asia/Kolkata
export LOG_LEVEL=INFO
# Gold layer processing configuration
export GOLD_COMPLETE_REFRESH=false
export GOLD_PROCESSING_STRATEGY=updated_time
```

**Note**: The `env.example` file is provided for reference but is not required. The system will use sensible defaults from `config.py`. All notebooks are **configuration-based** and can run standalone without job parameters.

### 2.3 Bootstrap the System
```python
from libs.sql_parameterizer import bootstrap_platform_observability

# Bootstrap the entire system
bootstrap_platform_observability(spark)
```

## 3. Detailed Deployment

### 3.1 Phase 1: Bootstrap Infrastructure

#### 3.1.1 Create Catalog and Schemas
```python
# Execute catalog and schema creation
from libs.sql_parameterizer import SQLParameterizer

parameterizer = SQLParameterizer(spark)
parameterizer.bootstrap_catalog_schemas()
```

**What it creates:**
- `platform_observability` catalog
- `plt_bronze` schema for raw data
- `plt_silver` schema for curated data
- `plt_gold` schema for dimensional models

#### 3.1.2 Create Control Tables
```python
# Create processing state tables
parameterizer.create_processing_state()
```

**What it creates:**
- `_cdf_processing_offsets` table for Silver layer CDF tracking
- `_bronze_hwm_processing_offsets` table for Bronze layer HWM tracking

### 3.2 Phase 2: Deploy Bronze Layer

#### 3.2.1 Create Bronze Tables
```python
# Execute bronze table creation
parameterizer.bootstrap_bronze_tables()
```

**What it creates:**
- `bronze/bronze_tables.sql` - All Bronze tables with CDF enabled
- Tables for billing, lakeflow, compute, and access data

#### 3.2.2 Deploy Bronze HWM Ingest Job
```python
# Execute the bronze HWM ingest job
# notebooks/bronze_hwm_ingest_job.py

# This will create:
# - brz_billing_usage
# - brz_billing_list_prices
# - brz_lakeflow_jobs
# - brz_lakeflow_pipelines
# - brz_lakeflow_job_run_timeline
# - brz_lakeflow_job_task_run_timeline
# - brz_access_workspaces_latest
# - brz_compute_clusters
# - brz_compute_node_types
```

**Expected Output:**
```
🚀 Starting Bronze HWM ingest job...
📖 Reading data from system tables...
💾 Committing processing state...
🎉 Bronze HWM ingest job completed successfully!
```

### 3.3 Phase 3: Deploy Silver Layer

#### 3.3.1 Create Silver Tables
```python
# Execute silver table creation
parameterizer.create_silver_tables()
```

**What it creates:**
- `silver/silver_tables.sql` - All Silver layer tables (workspace, jobs SCD2, pipelines SCD2, pricing, usage, run timeline, task timeline)

#### 3.3.2 Run Silver HWM Build Job
```python
# Execute the silver HWM build job
# notebooks/silver_hwm_build_job.py

# This will create:
# - slv_workspace (dimension)
# - slv_jobs_scd (SCD2 dimension)
# - slv_pipelines_scd (SCD2 dimension)
# - slv_usage_txn (fact table)
# - slv_job_run_timeline (fact table)
# - slv_job_task_run_timeline (fact table)
# - slv_clusters (SCD2 dimension)
```

**Expected Output:**
```
🚀 Starting Silver HWM build job...
📖 Reading CDF data from Bronze sources...
💾 Committing processing state...
🎉 Silver HWM build job completed successfully!
```

### 3.4 Phase 4: Deploy Gold Layer

#### 3.4.1 Create Gold Tables
```python
# Execute gold table creation
parameterizer.create_gold_tables()
```

**What it creates:**
- `gold/gold_dimensions.sql` - Dimension tables (workspace, entity, SKU, run status)
- `gold/gold_facts.sql` - Fact tables (usage, entity cost, run cost, run status cost, runs finished)

#### 3.4.2 Run Gold HWM Build Job (SCD2-Aware)
```python
# Execute the gold HWM build job with SCD2 support
# notebooks/gold_hwm_build_job.py

# This will build the dimensional model from Silver layer data with SCD2 awareness:
# - gld_dim_workspace (dimension)
# - gld_dim_entity (SCD2 dimension - all versions preserved for jobs & pipelines)
# - gld_dim_cluster (SCD2 dimension - all versions preserved)
# - gld_dim_sku (dimension)
# - gld_dim_run_status (dimension)
# - gld_dim_node_type (dimension)
# - gld_dim_date (dimension)
# - gld_fact_usage_priced_day (fact table with SCD2 alignment)
# - gld_fact_entity_cost (fact table)
# - gld_fact_run_cost (fact table)
# - gld_fact_run_status_cost (fact table)
# - gld_fact_runs_finished_day (fact table)
```

**Expected Output:**
```
🚀 Starting Gold layer build with SCD2 awareness...
📖 Reading data from Silver layer...
🏗️ Building Gold layer with SCD2 support...
✅ Gold dimensions built successfully (SCD2 versions preserved)
✅ Gold facts built successfully (SCD2 aligned)
🎉 Gold HWM build job completed successfully with SCD2 implementation!
```

### 3.5 Phase 5: Finalize System

#### 3.5.1 Apply Performance Optimizations
```python
# Apply Z-ORDER and statistics
parameterizer.apply_performance_optimizations()
```

#### 3.5.2 Create Business Views
```python
# Create business-ready views
parameterizer.create_gold_views()
```

**What it creates:**
- `gold/gold_views.sql` - Cost trends and anomaly detection
- `gold/policy_compliance.sql` - Policy compliance and governance
- `gold/gold_chargeback_views.sql` - Cost allocation and chargeback

## 4. Production Deployment

### 4.1 Schedule Daily Workflow
Use the provided workflow configuration for HWM jobs:

```json
{
  "name": "Platform Observability — Daily HWM Build",
  "schedule": {
    "quartz_cron_expression": "0 30 5 ? * *",
    "timezone_id": "Asia/Kolkata"
  },
  "tasks": [
    {
      "task_key": "bronze_hwm_ingest",
      "notebook_task": {
        "notebook_path": "/notebooks/bronze_hwm_ingest_job"
      }
    },
    {
      "task_key": "silver_hwm_build",
      "depends_on": [{"task_key": "bronze_hwm_ingest"}],
      "notebook_task": {
        "notebook_path": "/notebooks/silver_hwm_build_job"
      }
    },
    {
      "task_key": "gold_hwm_build",
      "depends_on": [{"task_key": "silver_hwm_build"}],
      "notebook_task": {
        "notebook_path": "/notebooks/gold_hwm_build_job"
      }
    },
    {
      "task_key": "performance_optimization",
      "depends_on": [{"task_key": "gold_hwm_build"}],
      "notebook_task": {
        "notebook_path": "/notebooks/performance_optimization_job"
      }
    },
    {
      "task_key": "health_check",
      "depends_on": [{"task_key": "performance_optimization"}],
      "notebook_task": {
        "notebook_path": "/notebooks/health_check_job"
      }
    }
  ]
}
```

### 4.2 Monitor Pipeline Health
- Check HWM job execution logs
- Monitor processing state tables for processing progress
- Review data quality metrics

### 4.3 Validate Data
```sql
-- Check Silver layer data
SELECT COUNT(*) FROM platform_observability.plt_silver.slv_usage_txn;

-- Check Gold layer data
SELECT COUNT(*) FROM platform_observability.plt_gold.gld_fact_usage_priced_day;

-- Check SCD2 dimension data (should have multiple versions for changed entities)
SELECT job_id, COUNT(*) as version_count 
FROM platform_observability.plt_gold.gld_dim_job 
GROUP BY job_id 
HAVING COUNT(*) > 1;
```

## 5. Validation

### 5.1 Common Issues

1. **Permission Errors**: Ensure workspace has access to system tables
2. **CDF Not Enabled**: Verify Delta tables have CDF enabled
3. **Processing State Issues**: Check processing state table schemas and permissions
4. **Configuration Issues**: Verify configuration values are properly set

### 5.2 Debug Commands

```python
# Check configuration
from config import Config
config = Config.get_config()
print(f"Catalog: {config.catalog}")
print(f"Bronze Schema: {config.bronze_schema}")
print(f"Gold Complete Refresh: {config.gold_complete_refresh}")
print(f"Gold Processing Strategy: {config.gold_processing_strategy}")

# List available SQL operations
from libs.sql_manager import sql_manager
operations = sql_manager.get_available_operations()
print("Available operations:", operations)
```

## 6. Related Documentation

- [01-overview.md](01-overview.md) - Solution overview and architecture
- [03-parameterization.md](03-parameterization.md) - Configuration and SQL management
- [04-data-dictionary.md](04-data-dictionary.md) - Complete data model documentation
- [08-scd2-temporal-join-example.md](08-scd2-temporal-join-example.md) - SCD2 implementation guide
- [09-task-based-processing.md](09-task-based-processing.md) - Task-based processing guide
- [10-recent-changes-summary.md](10-recent-changes-summary.md) - Recent changes and migration summary
- [11-deployment.md](11-deployment.md) - Production deployment and workflow setup

## Next Steps

1. **Customize Configuration**: Modify `config.py` for your environment
2. **Add Data Sources**: Extend Bronze layer for additional system tables
3. **Enhance Business Logic**: Customize Silver layer transformations
4. **Build Dashboards**: Use Gold layer for BI and analytics

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review pipeline execution logs
3. Validate data quality metrics
4. Consult the comprehensive documentation
