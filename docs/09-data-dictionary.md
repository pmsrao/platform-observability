# Data Dictionary

## Overview

This document provides a comprehensive reference for all data models, tables, and attributes in the Platform Observability solution. It covers the Bronze (raw), Silver (curated), and Gold (analytics) layers.

## Table of Contents

1. [Bronze Layer Tables](#bronze-layer-tables)
2. [Silver Layer Tables](#silver-layer-tables)
3. [Gold Layer Tables](#gold-layer-tables)
4. [Tag Processing and Inheritance](#tag-processing-and-inheritance)
5. [Data Quality and Validation](#data-quality-and-validation)

## Bronze Layer Tables

### `brz_billing_usage`
**Purpose**: Raw usage data from Databricks billing system (`system.billing.usage`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `workspace_id` | BIGINT | Unique workspace identifier | Workspace-level aggregation |
| `cloud` | STRING | Cloud provider (AWS, Azure, GCP) | Multi-cloud cost analysis |
| `sku_name` | STRING | SKU identifier | Cost attribution by service |
| `usage_unit` | STRING | Unit of measurement | Usage normalization |
| `usage_start_time` | TIMESTAMP | Usage period start | Time-based analysis |
| `usage_end_time` | TIMESTAMP | Usage period end | Duration calculations |
| `usage_quantity` | DOUBLE | Amount consumed | Cost and usage metrics |
| `entity_type` | STRING | Entity type (JOB, PIPELINE, CLUSTER) | Entity-level cost attribution |
| `entity_id` | STRING | Entity identifier | Entity-specific analysis |
| `run_id` | STRING | Job/pipeline run identifier | Run-level cost tracking |
| `billing_origin_product` | STRING | Product origin | Service categorization |

### `brz_billing_list_prices`
**Purpose**: Raw pricing data for cost calculations (`system.billing.list_prices`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `cloud` | STRING | Cloud provider | Multi-cloud pricing |
| `sku_name` | STRING | SKU identifier | Service-specific pricing |
| `usage_unit` | STRING | Unit of measurement | Price normalization |
| `price_usd` | DOUBLE | Price in USD | Cost calculations |
| `price_start_time` | TIMESTAMP | Price validity start | Historical pricing analysis |
| `price_end_time` | TIMESTAMP | Price validity end | Price change tracking |

### `brz_lakeflow_jobs`
**Purpose**: Raw job metadata and configuration (`system.lakeflow.jobs`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `workspace_id` | BIGINT | Workspace identifier | Workspace-level job analysis |
| `job_id` | BIGINT | Unique job identifier | Job-specific tracking |
| `name` | STRING | Job name | Job identification |
| `run_as` | STRING | Job owner/runner | Ownership attribution |
| `change_time` | TIMESTAMP | Last configuration change | Change tracking |
| `parent_workflow_id` | BIGINT | Parent workflow identifier | Workflow hierarchy |
| `workflow_type` | STRING | Workflow classification | Workflow analysis |
| `cluster_id` | STRING | Associated cluster | Cluster-job relationship |
| `tags` | MAP<STRING, STRING> | Job-level tags | Business context |

### `brz_compute_clusters`
**Purpose**: Raw cluster configuration and metadata (`system.compute.clusters`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `workspace_id` | BIGINT | Workspace identifier | Workspace-level analysis |
| `cluster_id` | STRING | Unique cluster identifier | Cluster-specific tracking |
| `cluster_name` | STRING | Cluster name | Cluster identification |
| `cluster_type` | STRING | JOB_CLUSTER or ALL_PURPOSE | Cluster classification |
| `associated_entity_id` | BIGINT | Job/Pipeline ID | Entity association |
| `associated_entity_type` | STRING | JOB or PIPELINE | Entity type |
| `tags` | MAP<STRING, STRING> | Cluster-level tags | Business context |
| `databricks_runtime_version` | STRING | Runtime version | Runtime analysis |
| `runtime_environment` | STRING | Python, Scala, SQL, ML | Environment tracking |
| `node_type_id` | STRING | Primary node type | Infrastructure analysis |
| `min_workers` | INT | Minimum worker count | Sizing analysis |
| `max_workers` | INT | Maximum worker count | Auto-scaling analysis |
| `driver_node_type_id` | STRING | Driver node type (if different) | Mixed node type analysis |
| `autoscale_enabled` | BOOLEAN | Auto-scaling status | Optimization insights |



## Silver Layer Tables

### `slv_workspace`
**Purpose**: Curated workspace information
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `workspace_id` | BIGINT | Unique workspace identifier | Primary key |
| `workspace_name` | STRING | Workspace name | Workspace identification |
| `workspace_url` | STRING | Workspace URL | Access information |

### `silver_jobs_scd`
**Purpose**: Jobs with SCD2 support for historical tracking
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `workspace_id` | BIGINT | Workspace identifier | Workspace-level analysis |
| `job_id` | BIGINT | Unique job identifier | Primary key |
| `name` | STRING | Job name | Job identification |
| `run_as` | STRING | Job owner/runner | Ownership tracking |
| `valid_from` | TIMESTAMP | SCD2 validity start | Historical tracking |
| `valid_to` | TIMESTAMP | SCD2 validity end | Historical tracking |
| `is_current` | BOOLEAN | Current record flag | Current vs historical |
| `parent_workflow_id` | BIGINT | Parent workflow identifier | Workflow hierarchy |
| `workflow_type` | STRING | Workflow classification | Workflow analysis |
| `cluster_id` | STRING | Associated cluster | Cluster relationship |
| `tags` | MAP<STRING, STRING> | Job-level tags | Business context |
| `is_parent_workflow` | BOOLEAN | Parent workflow flag | Workflow classification |
| `is_sub_workflow` | BOOLEAN | Sub-workflow flag | Workflow classification |
| `workflow_level` | STRING | PARENT/SUB_WORKFLOW/STANDALONE | Workflow level |
| `parent_workflow_name` | STRING | Parent workflow name | Workflow identification |

### `slv_job_task_run_timeline`
**Purpose**: Task-level execution timeline SCD2 for granular performance analysis
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `workspace_id` | BIGINT | Workspace identifier | Workspace analysis |
| `job_id` | BIGINT | Job identifier | Job analysis |
| `run_id` | BIGINT | Run identifier | Run analysis |
| `task_key` | STRING | Task identifier | Task analysis |
| `period_start_time` | TIMESTAMP | Task start time | Time analysis |
| `period_end_time` | TIMESTAMP | Task end time | Time analysis |
| `result_state` | STRING | Task result | Status analysis |
| `retry_attempt` | INT | Retry count | Reliability analysis |
| `execution_secs` | DOUBLE | Execution duration | Performance analysis |
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `valid_from` | TIMESTAMP | Validity start | SCD2 tracking |
| `valid_to` | TIMESTAMP | Validity end | SCD2 tracking |
| `is_current` | BOOLEAN | Current version flag | SCD2 filtering |
| `_loaded_at` | TIMESTAMP | Load timestamp | Audit trail |

### `slv_clusters`
**Purpose**: Enhanced cluster SCD2 information with business context
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `workspace_id` | BIGINT | Workspace identifier | Workspace-level analysis |
| `cluster_id` | STRING | Unique cluster identifier | Primary key |
| `cluster_name` | STRING | Cluster name | Cluster identification |
| `cluster_type` | STRING | JOB_CLUSTER or ALL_PURPOSE | Cluster classification |
| `associated_entity_id` | BIGINT | Job/Pipeline ID | Entity association |
| `associated_entity_type` | STRING | JOB or PIPELINE | Entity type |
| `tags` | MAP<STRING, STRING> | Cluster-level tags | Business context |
| `inherited_line_of_business` | STRING | Inherited from job | Cost attribution |
| `inherited_department` | STRING | Inherited from job | Department tracking |
| `inherited_cost_center` | STRING | Inherited from job | Cost center allocation |
| `inherited_environment` | STRING | Inherited from job | Environment tracking |
| `inherited_use_case` | STRING | Inherited from job | Use case analysis |
| `inherited_workflow_level` | STRING | Inherited from job | Workflow context |
| `inherited_parent_workflow` | STRING | Inherited from job | Parent workflow |
| `spark_version` | STRING | Runtime version | Runtime analysis |
| `cluster_source` | STRING | UI, API, JOB | Cluster creation source |
| `node_type_id` | STRING | Primary node type | Infrastructure analysis |
| `min_workers` | INT | Minimum worker count | Sizing analysis |
| `max_workers` | INT | Maximum worker count | Auto-scaling analysis |
| `driver_node_type_id` | STRING | Driver node type (if different) | Mixed node type analysis |
| `autoscale_enabled` | BOOLEAN | Auto-scaling status | Optimization insights |
| `major_version` | INT | Computed major version | Version analysis |
| `minor_version` | INT | Computed minor version | Version analysis |
| `runtime_age_months` | INT | Runtime age in months | Modernization planning |
| `is_lts` | BOOLEAN | LTS version flag | Support level |
| `valid_from` | TIMESTAMP | SCD2 validity start | Historical tracking |
| `valid_to` | TIMESTAMP | SCD2 validity end | Historical tracking |
| `is_current` | BOOLEAN | Current record flag | SCD2 filtering |

### `slv_usage_txn`
**Purpose**: Enriched usage transactions with business context
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `workspace_id` | BIGINT | Workspace identifier | Workspace-level analysis |
| `cloud` | STRING | Cloud provider | Multi-cloud analysis |
| `sku_name` | STRING | SKU identifier | Service analysis |
| `usage_unit` | STRING | Unit of measurement | Usage normalization |
| `usage_start_time` | TIMESTAMP | Usage period start | Time analysis |
| `usage_end_time` | TIMESTAMP | Usage period end | Duration analysis |
| `usage_quantity` | DOUBLE | Amount consumed | Usage metrics |
| `entity_type` | STRING | Entity type | Entity analysis |
| `entity_id` | STRING | Entity identifier | Entity tracking |
| `run_id` | STRING | Job/pipeline run identifier | Run analysis |
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `list_cost_usd` | DOUBLE | Calculated cost | Cost analysis |
| `duration_hours` | DOUBLE | Duration in hours | Time analysis |
| `billing_origin_product` | STRING | Product origin | Service categorization |
| `tags` | MAP<STRING, STRING> | Raw tags | Tag analysis |
| `line_of_business` | STRING | Normalized business unit | Business analysis |
| `department` | STRING | Normalized department | Department analysis |
| `cost_center` | STRING | Normalized cost center | Cost allocation |
| `environment` | STRING | Normalized environment | Environment analysis |
| `use_case` | STRING | Normalized use case | Use case analysis |
| `pipeline_name` | STRING | Normalized pipeline name | Pipeline analysis |
| `cluster_identifier` | STRING | Normalized cluster identifier | Cluster analysis |
| `workflow_level` | STRING | Workflow hierarchy level | Workflow analysis |
| `parent_workflow_name` | STRING | Parent workflow name | Workflow analysis |
| `inherited_line_of_business` | STRING | Inherited from cluster | Cost attribution |
| `inherited_cost_center` | STRING | Inherited from cluster | Cost allocation |
| `inherited_workflow_level` | STRING | Inherited from cluster | Workflow context |
| `inherited_parent_workflow` | STRING | Inherited from cluster | Parent workflow |

## Gold Layer Tables

### **SCD2 Implementation in Gold Layer**

The Gold layer implements **Slowly Changing Dimension Type 2 (SCD2)** to preserve complete historical information about entities while maintaining temporal accuracy in fact table joins.

**Key Benefits**:
- **Historical Analysis**: Track how entities (jobs, pipelines, clusters) change over time
- **Temporal Accuracy**: Ensure facts reference the correct dimension version for each date
- **Audit Trail**: Maintain complete history of entity changes
- **Business Intelligence**: Support time-based analysis and reporting

**SCD2 Tables**:
- `gld_dim_job`: All versions of jobs preserved using MERGE operations
- `gld_dim_pipeline`: All versions of pipelines preserved using MERGE operations  
- `gld_dim_cluster`: All versions of clusters preserved using MERGE operations

**Non-SCD2 Tables**:
- `gld_dim_workspace`: Current workspace information only
- `gld_dim_sku`: Current SKU information only
- `gld_dim_run_status`: Current status information only
- `gld_dim_node_type`: Current node type information only

### `gld_fact_usage_priced_day`
**Purpose**: Daily aggregated usage facts for analytics with SCD2 dimension alignment
**SCD Type**: Type 1 (current values only) with SCD2 dimension joins

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `workspace_id` | BIGINT | Workspace identifier | Workspace analysis |
| `entity_type` | STRING | Entity type | Entity analysis |
| `entity_id` | STRING | Entity identifier | Entity tracking |
| `cloud` | STRING | Cloud provider | Multi-cloud analysis |
| `sku_name` | STRING | SKU identifier | Service analysis |
| `usage_unit` | STRING | Unit of measurement | Usage normalization |
| `usage_quantity` | DOUBLE | Daily aggregated usage | Usage metrics |
| `list_cost_usd` | DOUBLE | Daily aggregated cost | Cost metrics |
| `duration_hours` | DOUBLE | Daily aggregated duration | Time metrics |
| `line_of_business` | STRING | Business unit | Business analysis |
| `department` | STRING | Department | Department analysis |
| `cost_center` | STRING | Cost center | Cost allocation |
| `environment` | STRING | Environment | Environment analysis |
| `use_case` | STRING | Use case | Use case analysis |
| `pipeline_name` | STRING | Pipeline name | Pipeline analysis |
| `cluster_identifier` | STRING | Cluster identifier | Cluster analysis |
| `workflow_level` | STRING | Workflow level | Workflow analysis |
| `parent_workflow_name` | STRING | Parent workflow | Workflow analysis |

## Tag Processing and Inheritance

### Tag Normalization
The system processes raw tags into normalized business columns:

| Source Tag | Normalized Column | Default Value | Business Purpose |
|------------|-------------------|---------------|------------------|
| `project` | `line_of_business` | "Unknown" | Business unit allocation |
| `sub_project` | `department` | "general" | Team/department tracking |
| `cost_center` | `cost_center` | "unallocated" | Financial cost center |
| `environment` | `environment` | "dev" | Environment-based costing |
| `data_product` | `use_case` | "Unknown" | Use case analysis |
| `job_pipeline` | `pipeline_name` | "system" | Pipeline cost tracking |
| `cluster_name` | `cluster_identifier` | "Unknown" | Infrastructure costing |
| `workflow_level` | `workflow_level` | "STANDALONE" | Workflow hierarchy |
| `parent_workflow` | `parent_workflow_name` | "None" | Parent workflow |
| `runtime_version` | `databricks_runtime` | "Unknown" | Runtime analysis |
| `node_type` | `compute_node_type` | "Unknown" | Infrastructure analysis |
| `cluster_size` | `cluster_worker_count` | "Unknown" | Sizing analysis |

### Tag Inheritance Hierarchy
```
Job Tags → Cluster Tags → Usage Records
    ↓           ↓           ↓
line_of_business → inherited_line_of_business → line_of_business (coalesced)
cost_center → inherited_cost_center → cost_center (coalesced)
workflow_level → inherited_workflow_level → workflow_level (coalesced)
```

**Business Logic**: When job-level tags are missing, the system automatically inherits tags from the associated cluster, ensuring complete cost attribution.

## Data Quality and Validation

### CDF (Change Data Feed) Processing
- **Bronze Layer**: All tables have CDF enabled for incremental processing
- **Silver Layer**: SCD2 support for historical tracking where applicable
- **Gold Layer**: Incremental MERGE operations for fact tables

### Data Validation Rules
1. **Required Fields**: All primary keys and business identifiers must be non-null
2. **Tag Normalization**: Raw tags are validated and normalized with defaults
3. **Cost Calculations**: Usage quantities and prices are validated for positive values
4. **Time Consistency**: Start times must be before end times
5. **Entity Relationships**: Entity IDs must reference valid entities in the system

### Performance Optimizations
1. **Partitioning**: Date-based partitioning on `date_sk` columns
2. **Z-Ordering**: Optimized on frequently queried columns
3. **Auto-Optimization**: Delta Lake auto-optimization enabled
4. **CDF Tracking**: Efficient incremental processing
5. **Surrogate Keys**: Integer date keys for efficient joins
