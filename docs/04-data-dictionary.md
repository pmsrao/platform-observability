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
| `record_id` | STRING | Unique record identifier from source | Primary key for upserts |
| `account_id` | STRING | Account identifier | Account-level aggregation |
| `workspace_id` | STRING | Unique workspace identifier | Workspace-level aggregation |
| `sku_name` | STRING | SKU identifier | Cost attribution by service |
| `cloud` | STRING | Cloud provider (AWS, Azure, GCP) | Multi-cloud cost analysis |
| `usage_start_time` | TIMESTAMP | Usage period start | Time-based analysis |
| `usage_end_time` | TIMESTAMP | Usage period end | Duration calculations |
| `usage_date` | DATE | Usage date | Date-based analysis |
| `custom_tags` | MAP<STRING, STRING> | Custom tags from source | Tag-based analysis |
| `usage_unit` | STRING | Unit of measurement | Usage normalization |
| `usage_quantity` | DECIMAL(38,18) | Amount consumed | Cost and usage metrics |
| `usage_metadata` | STRUCT | Detailed usage metadata (22 fields) | Granular analysis |
| `identity_metadata` | STRUCT | Identity information | Ownership tracking |
| `record_type` | STRING | Record type | Record classification |
| `ingestion_date` | DATE | Data ingestion date | Data freshness tracking |
| `billing_origin_product` | STRING | Product origin | Service categorization |
| `product_features` | STRUCT | Product feature flags | Feature-based analysis |
| `usage_type` | STRING | Usage type | Usage classification |

### `brz_billing_list_prices`
**Purpose**: Raw pricing data for cost calculations (`system.billing.list_prices`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `price_start_time` | TIMESTAMP | Price validity start | Historical pricing analysis |
| `price_end_time` | TIMESTAMP | Price validity end | Price change tracking |
| `account_id` | STRING | Account identifier | Account-level pricing |
| `cloud` | STRING | Cloud provider | Multi-cloud pricing |
| `sku_name` | STRING | SKU identifier | Service-specific pricing |
| `currency_code` | STRING | Currency code | Multi-currency support |
| `usage_unit` | STRING | Unit of measurement | Price normalization |
| `pricing` | STRUCT | Pricing structure | Cost calculations |

### `brz_lakeflow_jobs`
**Purpose**: Raw job metadata and configuration (`system.lakeflow.jobs`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level job analysis |
| `job_id` | STRING | Unique job identifier | Job-specific tracking |
| `name` | STRING | Job name | Job identification |
| `description` | STRING | Job description | Job documentation |
| `creator_id` | STRING | Job creator | Ownership tracking |
| `tags` | MAP<STRING, STRING> | Job-level tags | Business context |
| `change_time` | TIMESTAMP | Last configuration change | Change tracking |
| `delete_time` | TIMESTAMP | Deletion timestamp | Lifecycle tracking |
| `run_as` | STRING | Job owner/runner | Ownership attribution |

### `brz_lakeflow_pipelines`
**Purpose**: Raw pipeline metadata and configuration (`system.lakeflow.pipelines`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `pipeline_id` | STRING | Unique pipeline identifier | Pipeline-specific tracking |
| `pipeline_type` | STRING | Pipeline type | Pipeline classification |
| `name` | STRING | Pipeline name | Pipeline identification |
| `created_by` | STRING | Pipeline creator | Ownership tracking |
| `run_as` | STRING | Pipeline runner | Execution context |
| `tags` | MAP<STRING, STRING> | Pipeline-level tags | Business context |
| `settings` | STRUCT | Pipeline settings | Configuration analysis |
| `configuration` | MAP<STRING, STRING> | Pipeline configuration | Configuration tracking |
| `change_time` | TIMESTAMP | Last configuration change | Change tracking |
| `delete_time` | TIMESTAMP | Deletion timestamp | Lifecycle tracking |

### `brz_lakeflow_job_run_timeline`
**Purpose**: Raw job run execution timeline (`system.lakeflow.job_run_timeline`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `job_id` | STRING | Job identifier | Job analysis |
| `job_run_id` | STRING | Job run identifier | Run-specific tracking |
| `period_start_time` | TIMESTAMP | Run start time | Time analysis |
| `period_end_time` | TIMESTAMP | Run end time | Duration analysis |
| `trigger_type` | STRING | Run trigger type | Trigger analysis |
| `run_type` | STRING | Run type | Run classification |
| `run_name` | STRING | Run name | Run identification |
| `compute_ids` | ARRAY<STRING> | Compute resource IDs | Resource tracking |
| `result_state` | STRING | Run result | Status analysis |
| `termination_code` | STRING | Termination reason | Failure analysis |
| `job_parameters` | MAP<STRING, STRING> | Job parameters | Parameter analysis |

### `brz_lakeflow_job_task_run_timeline`
**Purpose**: Raw task run execution timeline (`system.lakeflow.job_task_run_timeline`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `job_id` | STRING | Job identifier | Job analysis |
| `task_run_id` | STRING | Task run identifier | Task-specific tracking |
| `job_run_id` | STRING | Parent job run ID | Parent-child relationship |
| `parent_run_id` | STRING | Parent run ID | Hierarchy tracking |
| `period_start_time` | TIMESTAMP | Task start time | Time analysis |
| `period_end_time` | TIMESTAMP | Task end time | Duration analysis |
| `task_key` | STRING | Task identifier | Task identification |
| `compute_ids` | ARRAY<STRING> | Compute resource IDs | Resource tracking |
| `result_state` | STRING | Task result | Status analysis |
| `termination_code` | STRING | Termination reason | Failure analysis |

### `brz_access_workspaces_latest`
**Purpose**: Raw workspace information (`system.access.workspaces_latest`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace identification |
| `workspace_name` | STRING | Workspace name | Workspace identification |
| `workspace_url` | STRING | Workspace URL | Access information |
| `create_time` | TIMESTAMP | Workspace creation time | Lifecycle tracking |
| `status` | STRING | Workspace status | Status monitoring |

### `brz_compute_clusters`
**Purpose**: Raw cluster configuration and metadata (`system.compute.clusters`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `cluster_id` | STRING | Unique cluster identifier | Cluster-specific tracking |
| `cluster_name` | STRING | Cluster name | Cluster identification |
| `owned_by` | STRING | Cluster owner | Ownership tracking |
| `create_time` | TIMESTAMP | Cluster creation time | Lifecycle tracking |
| `delete_time` | TIMESTAMP | Cluster deletion time | Lifecycle tracking |
| `driver_node_type` | STRING | Driver node type | Infrastructure analysis |
| `worker_node_type` | STRING | Worker node type | Infrastructure analysis |
| `worker_count` | BIGINT | Total worker count | Sizing analysis |
| `min_autoscale_workers` | BIGINT | Minimum autoscale workers | Auto-scaling analysis |
| `max_autoscale_workers` | BIGINT | Maximum autoscale workers | Auto-scaling analysis |
| `auto_termination_minutes` | BIGINT | Auto-termination timeout | Cost optimization |
| `enable_elastic_disk` | BOOLEAN | Elastic disk enabled | Storage analysis |
| `tags` | MAP<STRING, STRING> | Cluster-level tags | Business context |
| `cluster_source` | STRING | Cluster creation source | Source tracking |
| `init_scripts` | ARRAY<STRING> | Initialization scripts | Configuration tracking |
| `aws_attributes` | STRUCT | AWS-specific attributes | Cloud provider analysis |
| `azure_attributes` | STRUCT | Azure-specific attributes | Cloud provider analysis |
| `gcp_attributes` | STRUCT | GCP-specific attributes | Cloud provider analysis |
| `driver_instance_pool_id` | STRING | Driver instance pool | Resource pooling |
| `worker_instance_pool_id` | STRING | Worker instance pool | Resource pooling |
| `dbr_version` | STRING | Databricks runtime version | Runtime analysis |
| `change_time` | TIMESTAMP | Last configuration change | Change tracking |
| `change_date` | DATE | Change date | Date-based analysis |
| `data_security_mode` | STRING | Data security mode | Security analysis |
| `policy_id` | STRING | Policy identifier | Policy compliance |

### `brz_compute_node_types`
**Purpose**: Raw node type information (`system.compute.node_types`)
**Incremental Processing**: Yes (CDF enabled)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `node_type` | STRING | Node type identifier | Node type identification |
| `core_count` | DOUBLE | Number of CPU cores | Compute capacity analysis |
| `memory_mb` | BIGINT | Memory in megabytes | Memory capacity analysis |
| `gpu_count` | BIGINT | Number of GPUs | GPU capacity analysis |

## Silver Layer Tables

### `slv_workspace`
**Purpose**: Curated workspace information
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Unique workspace identifier | Primary key |
| `workspace_name` | STRING | Workspace name | Workspace identification |
| `workspace_url` | STRING | Workspace URL | Access information |
| `create_time` | TIMESTAMP | Workspace creation time | Lifecycle tracking |
| `status` | STRING | Workspace status | Status monitoring |

### `slv_jobs_scd`
**Purpose**: Jobs with SCD2 support for historical tracking
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `job_id` | STRING | Unique job identifier | Primary key |
| `name` | STRING | Job name | Job identification |
| `description` | STRING | Job description | Job documentation |
| `creator_id` | STRING | Job creator | Ownership tracking |
| `run_as` | STRING | Job owner/runner | Ownership attribution |
| `valid_from` | TIMESTAMP | SCD2 validity start | Historical tracking |
| `valid_to` | TIMESTAMP | SCD2 validity end | Historical tracking |
| `is_current` | BOOLEAN | Current record flag | Current vs historical |
| `tags` | MAP<STRING, STRING> | Job-level tags | Business context |
| `is_parent_workflow` | BOOLEAN | Parent workflow flag | Workflow classification |
| `is_sub_workflow` | BOOLEAN | Sub-workflow flag | Workflow classification |
| `workflow_level` | STRING | PARENT/SUB_WORKFLOW/STANDALONE | Workflow level |
| `parent_workflow_name` | STRING | Parent workflow name | Workflow identification |

### `slv_pipelines_scd`
**Purpose**: Pipelines with SCD2 support for historical tracking
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `pipeline_id` | STRING | Unique pipeline identifier | Primary key |
| `pipeline_type` | STRING | Pipeline type | Pipeline classification |
| `name` | STRING | Pipeline name | Pipeline identification |
| `created_by` | STRING | Pipeline creator | Ownership tracking |
| `run_as` | STRING | Pipeline runner | Execution context |
| `tags` | MAP<STRING, STRING> | Pipeline-level tags | Business context |
| `settings` | STRUCT | Pipeline settings | Configuration analysis |
| `configuration` | MAP<STRING, STRING> | Pipeline configuration | Configuration tracking |
| `valid_from` | TIMESTAMP | SCD2 validity start | Historical tracking |
| `valid_to` | TIMESTAMP | SCD2 validity end | Historical tracking |
| `is_current` | BOOLEAN | Current record flag | Current vs historical |

### `slv_price_scd`
**Purpose**: Pricing data with SCD2 support for historical tracking
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level pricing |
| `cloud` | STRING | Cloud provider | Multi-cloud pricing |
| `sku_name` | STRING | SKU identifier | Service-specific pricing |
| `usage_unit` | STRING | Unit of measurement | Price normalization |
| `currency_code` | STRING | Currency code | Multi-currency support |
| `price_usd` | DECIMAL(38,18) | Price in USD | Cost calculations |
| `price_start_time` | TIMESTAMP | Price validity start | Historical pricing analysis |
| `price_end_time` | TIMESTAMP | Price validity end | Price change tracking |
| `valid_from` | TIMESTAMP | SCD2 validity start | Historical tracking |
| `valid_to` | TIMESTAMP | SCD2 validity end | Historical tracking |
| `is_current` | BOOLEAN | Current record flag | Current vs historical |

### `slv_usage_txn`
**Purpose**: Enriched usage transactions with business context
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `record_id` | STRING | Unique record identifier | Primary key |
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `cloud` | STRING | Cloud provider | Multi-cloud analysis |
| `sku_name` | STRING | SKU identifier | Service analysis |
| `usage_unit` | STRING | Unit of measurement | Usage normalization |
| `usage_start_time` | TIMESTAMP | Usage period start | Time analysis |
| `usage_end_time` | TIMESTAMP | Usage period end | Duration analysis |
| `usage_date` | DATE | Usage date | Date-based analysis |
| `usage_quantity` | DECIMAL(38,18) | Amount consumed | Usage metrics |
| `entity_type` | STRING | Entity type | Entity analysis |
| `entity_id` | STRING | Entity identifier | Entity tracking |
| `job_run_id` | STRING | Job run identifier | Run analysis |
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `list_cost_usd` | DECIMAL(38,18) | Calculated cost | Cost analysis |
| `duration_hours` | DECIMAL(38,18) | Duration in hours | Time analysis |
| `billing_origin_product` | STRING | Product origin | Service categorization |
| `custom_tags` | MAP<STRING, STRING> | Raw tags | Tag analysis |
| `usage_metadata` | STRUCT | Detailed usage metadata | Granular analysis |
| `identity_metadata` | STRUCT | Identity information | Ownership tracking |
| `record_type` | STRING | Record type | Record classification |
| `ingestion_date` | DATE | Data ingestion date | Data freshness tracking |
| `product_features` | STRUCT | Product feature flags | Feature-based analysis |
| `usage_type` | STRING | Usage type | Usage classification |
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

### `slv_job_run_timeline`
**Purpose**: Job run execution timeline
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `job_id` | STRING | Job identifier | Job analysis |
| `job_run_id` | STRING | Job run identifier | Run-specific tracking |
| `period_start_time` | TIMESTAMP | Run start time | Time analysis |
| `period_end_time` | TIMESTAMP | Run end time | Duration analysis |
| `trigger_type` | STRING | Run trigger type | Trigger analysis |
| `run_type` | STRING | Run type | Run classification |
| `run_name` | STRING | Run name | Run identification |
| `compute_ids` | ARRAY<STRING> | Compute resource IDs | Resource tracking |
| `result_state` | STRING | Run result | Status analysis |
| `termination_code` | STRING | Termination reason | Failure analysis |
| `job_parameters` | MAP<STRING, STRING> | Job parameters | Parameter analysis |
| `date_sk_start` | INT | Start date surrogate key | Date partitioning |
| `date_sk_end` | INT | End date surrogate key | Date partitioning |

### `slv_job_task_run_timeline`
**Purpose**: Task-level execution timeline SCD2 for granular performance analysis
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace analysis |
| `job_id` | STRING | Job identifier | Job analysis |
| `task_run_id` | STRING | Task run identifier | Task-specific tracking |
| `job_run_id` | STRING | Parent job run ID | Parent-child relationship |
| `parent_run_id` | STRING | Parent run ID | Hierarchy tracking |
| `task_key` | STRING | Task identifier | Task identification |
| `period_start_time` | TIMESTAMP | Task start time | Time analysis |
| `period_end_time` | TIMESTAMP | Task end time | Time analysis |
| `compute_ids` | ARRAY<STRING> | Compute resource IDs | Resource tracking |
| `result_state` | STRING | Task result | Status analysis |
| `termination_code` | STRING | Termination reason | Failure analysis |
| `execution_secs` | DECIMAL(38,18) | Execution duration | Performance analysis |
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `valid_from` | TIMESTAMP | Validity start | SCD2 tracking |
| `valid_to` | TIMESTAMP | Validity end | SCD2 tracking |
| `is_current` | BOOLEAN | Current version flag | SCD2 filtering |

### `slv_clusters`
**Purpose**: Enhanced cluster SCD2 information with business context
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `cluster_id` | STRING | Unique cluster identifier | Primary key |
| `cluster_name` | STRING | Cluster name | Cluster identification |
| `owned_by` | STRING | Cluster owner | Ownership tracking |
| `create_time` | TIMESTAMP | Cluster creation time | Lifecycle tracking |
| `delete_time` | TIMESTAMP | Cluster deletion time | Lifecycle tracking |
| `driver_node_type` | STRING | Driver node type | Infrastructure analysis |
| `worker_node_type` | STRING | Worker node type | Infrastructure analysis |
| `worker_count` | BIGINT | Total worker count | Sizing analysis |
| `min_autoscale_workers` | BIGINT | Minimum autoscale workers | Auto-scaling analysis |
| `max_autoscale_workers` | BIGINT | Maximum autoscale workers | Auto-scaling analysis |
| `auto_termination_minutes` | BIGINT | Auto-termination timeout | Cost optimization |
| `enable_elastic_disk` | BOOLEAN | Elastic disk enabled | Storage analysis |
| `tags` | MAP<STRING, STRING> | Cluster-level tags | Business context |
| `cluster_source` | STRING | Cluster creation source | Source tracking |
| `init_scripts` | ARRAY<STRING> | Initialization scripts | Configuration tracking |
| `aws_attributes` | STRUCT | AWS-specific attributes | Cloud provider analysis |
| `azure_attributes` | STRUCT | Azure-specific attributes | Cloud provider analysis |
| `gcp_attributes` | STRUCT | GCP-specific attributes | Cloud provider analysis |
| `driver_instance_pool_id` | STRING | Driver instance pool | Resource pooling |
| `worker_instance_pool_id` | STRING | Worker instance pool | Resource pooling |
| `dbr_version` | STRING | Databricks runtime version | Runtime analysis |
| `change_time` | TIMESTAMP | Last configuration change | Change tracking |
| `change_date` | DATE | Change date | Date-based analysis |
| `data_security_mode` | STRING | Data security mode | Security analysis |
| `policy_id` | STRING | Policy identifier | Policy compliance |
| `inherited_line_of_business` | STRING | Inherited from job | Cost attribution |
| `inherited_department` | STRING | Inherited from job | Department tracking |
| `inherited_cost_center` | STRING | Inherited from job | Cost center allocation |
| `inherited_environment` | STRING | Inherited from job | Environment tracking |
| `inherited_use_case` | STRING | Inherited from job | Use case analysis |
| `inherited_workflow_level` | STRING | Inherited from job | Workflow context |
| `inherited_parent_workflow` | STRING | Inherited from job | Parent workflow |
| `major_version` | INT | Computed major version | Version analysis |
| `minor_version` | INT | Computed minor version | Version analysis |
| `runtime_age_months` | INT | Runtime age in months | Modernization planning |
| `is_lts` | BOOLEAN | LTS version flag | Support level |
| `valid_from` | TIMESTAMP | SCD2 validity start | Historical tracking |
| `valid_to` | TIMESTAMP | SCD2 validity end | Historical tracking |
| `is_current` | BOOLEAN | Current record flag | SCD2 filtering |

## Gold Layer Tables

### **SCD2 Implementation in Gold Layer**

The Gold layer implements **Slowly Changing Dimension Type 2 (SCD2)** to preserve complete historical information about entities while maintaining temporal accuracy in fact table joins.

**Key Benefits**:
- **Historical Analysis**: Track how entities (jobs, pipelines, clusters) change over time
- **Temporal Accuracy**: Ensure facts reference the correct dimension version for each date
- **Audit Trail**: Maintain complete history of entity changes
- **Business Intelligence**: Support time-based analysis and reporting

**SCD2 Tables**:
- `gld_dim_entity`: All versions of entities (jobs, pipelines) preserved using MERGE operations
- `gld_dim_cluster`: All versions of clusters preserved using MERGE operations

**Non-SCD2 Tables**:
- `gld_dim_workspace`: Current workspace information only
- `gld_dim_sku`: Current SKU information only
- `gld_dim_run_status`: Current status information only
- `gld_dim_node_type`: Current node type information only

### `gld_dim_workspace`
**Purpose**: Workspace dimension for analytics
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Primary key |
| `workspace_name` | STRING | Workspace name | Workspace identification |
| `workspace_url` | STRING | Workspace URL | Access information |
| `status` | STRING | Workspace status | Status monitoring |
| `region` | STRING | Workspace region | Regional analysis |
| `cloud` | STRING | Cloud provider | Multi-cloud analysis |
| `created_time` | TIMESTAMP | Creation time | Lifecycle tracking |
| `updated_time` | TIMESTAMP | Last update time | Change tracking |

### `gld_dim_entity` (SCD2)
**Purpose**: Entity dimension for analytics with historical tracking
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `entity_key` | BIGINT | Surrogate key (auto-generated) | Primary key |
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `entity_type` | STRING | Entity type | Entity classification |
| `entity_id` | STRING | Entity identifier | Natural key |
| `name` | STRING | Entity name | Entity identification |
| `description` | STRING | Entity description | Entity documentation |
| `creator_id` | STRING | Entity creator | Ownership tracking |
| `run_as` | STRING | Entity runner | Execution context |
| `workspace_name` | STRING | Workspace name | Workspace identification |
| `workspace_url` | STRING | Workspace URL | Access information |
| `created_time` | TIMESTAMP | Creation time | Lifecycle tracking |
| `updated_time` | TIMESTAMP | Last update time | Change tracking |
| `valid_from` | TIMESTAMP | SCD2 validity start | Historical tracking |
| `valid_to` | TIMESTAMP | SCD2 validity end | Historical tracking |
| `is_current` | BOOLEAN | Current record flag | Current vs historical |

### `gld_dim_cluster` (SCD2)
**Purpose**: Cluster dimension for analytics with historical tracking
**SCD Type**: Type 2 (historical tracking)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `cluster_key` | BIGINT | Surrogate key (auto-generated) | Primary key |
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace-level analysis |
| `cluster_id` | STRING | Cluster identifier | Natural key |
| `cluster_name` | STRING | Cluster name | Cluster identification |
| `owned_by` | STRING | Cluster owner | Ownership tracking |
| `create_time` | TIMESTAMP | Cluster creation time | Lifecycle tracking |
| `delete_time` | TIMESTAMP | Cluster deletion time | Lifecycle tracking |
| `driver_node_type` | STRING | Driver node type | Infrastructure analysis |
| `worker_node_type` | STRING | Worker node type | Infrastructure analysis |
| `worker_count` | INT | Current worker count | Capacity analysis |
| `min_autoscale_workers` | INT | Minimum autoscale workers | Scaling analysis |
| `max_autoscale_workers` | INT | Maximum autoscale workers | Scaling analysis |
| `auto_termination_minutes` | INT | Auto-termination timeout | Cost optimization |
| `enable_elastic_disk` | BOOLEAN | Elastic disk enabled | Storage analysis |
| `cluster_source` | STRING | Cluster source (JOB/UI/API) | Usage pattern analysis |
| `init_scripts` | STRING | Initialization scripts | Configuration tracking |
| `aws_attributes` | STRUCT | AWS-specific attributes | Cloud-specific analysis |
| `azure_attributes` | STRUCT | Azure-specific attributes | Cloud-specific analysis |
| `gcp_attributes` | STRUCT | GCP-specific attributes | Cloud-specific analysis |
| `driver_instance_pool_id` | STRING | Driver instance pool ID | Resource management |
| `worker_instance_pool_id` | STRING | Worker instance pool ID | Resource management |
| `dbr_version` | STRING | Databricks Runtime version | Performance analysis |
| `change_time` | TIMESTAMP | Last change time | Change tracking |
| `change_date` | DATE | Last change date | Change tracking |
| `data_security_mode` | STRING | Data security mode | Security analysis |
| `policy_id` | STRING | Cluster policy ID | Policy compliance |
| `worker_node_type_category` | STRING | Categorized node type | Node type analysis |
| `valid_from` | TIMESTAMP | SCD2 validity start | Historical tracking |
| `valid_to` | TIMESTAMP | SCD2 validity end | Historical tracking |
| `is_current` | BOOLEAN | Current record flag | Current vs historical |

### `gld_dim_sku`
**Purpose**: SKU dimension for analytics
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `cloud` | STRING | Cloud provider | Multi-cloud analysis |
| `sku_name` | STRING | SKU identifier | Primary key |
| `usage_unit` | STRING | Unit of measurement | Usage normalization |
| `currency_code` | STRING | Currency code | Multi-currency support |
| `billing_origin_product` | STRING | Product origin | Service categorization |
| `current_price_usd` | DECIMAL(38,18) | Current price in USD | Cost calculations |
| `price_effective_date` | DATE | Price effective date | Price tracking |

### `gld_dim_node_type`
**Purpose**: Node type dimension for analytics
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `account_id` | STRING | Account identifier | Account-level analysis |
| `node_type` | STRING | Node type identifier | Primary key |
| `core_count` | DOUBLE | Number of CPU cores | Compute capacity analysis |
| `memory_mb` | BIGINT | Memory in megabytes | Memory capacity analysis |
| `gpu_count` | BIGINT | Number of GPUs | GPU capacity analysis |
| `cloud` | STRING | Cloud provider | Multi-cloud analysis |
| `category` | STRING | Node type category | Node classification |

### `gld_fact_usage_priced_day`
**Purpose**: Daily aggregated usage facts for analytics with SCD2 dimension alignment
**SCD Type**: Type 1 (current values only) with SCD2 dimension joins

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `date_key` | INT | Date surrogate key | Date partitioning |
| `workspace_key` | BIGINT | Workspace surrogate key | Workspace analysis |
| `entity_key` | BIGINT | Entity surrogate key | Entity analysis |
| `cluster_key` | BIGINT | Cluster surrogate key | Infrastructure analysis |
| `sku_key` | BIGINT | SKU surrogate key | Service analysis |
| `usage_quantity` | DECIMAL(38,18) | Usage quantity | Usage measurement |
| `list_cost_usd` | DECIMAL(38,18) | List cost in USD | Cost analysis |
| `duration_hours` | DECIMAL(38,18) | Duration in hours | Performance analysis |
| `job_run_id` | STRING | Job run identifier | Run tracking |
| `cloud` | STRING | Cloud provider | Multi-cloud analysis |
| `line_of_business` | STRING | Business unit | Business analysis |
| `department` | STRING | Department | Organizational analysis |
| `cost_center` | STRING | Cost center | Cost allocation |
| `environment` | STRING | Environment | Environment analysis |
| `use_case` | STRING | Use case | Use case analysis |
| `pipeline_name` | STRING | Pipeline name | Pipeline analysis |
| `cluster_identifier` | STRING | Cluster identifier | Cluster analysis |
| `workflow_level` | STRING | Workflow level | Workflow analysis |
| `parent_workflow_name` | STRING | Parent workflow name | Workflow hierarchy |

### `gld_fact_entity_cost`
**Purpose**: Daily cost by entity
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace analysis |
| `entity_type` | STRING | Entity type | Entity classification |
| `entity_id` | STRING | Entity identifier | Entity tracking |
| `list_cost_usd` | DECIMAL(38,18) | Daily cost | Cost metrics |
| `runs_count` | BIGINT | Number of runs | Activity metrics |

### `gld_fact_run_cost`
**Purpose**: Cost by individual run
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace analysis |
| `entity_type` | STRING | Entity type | Entity classification |
| `entity_id` | STRING | Entity identifier | Entity tracking |
| `job_run_id` | STRING | Job run identifier | Run tracking |
| `cloud` | STRING | Cloud provider | Multi-cloud analysis |
| `sku_name` | STRING | SKU identifier | Service analysis |
| `usage_unit` | STRING | Unit of measurement | Usage normalization |
| `list_cost_usd` | DECIMAL(38,18) | Run cost | Cost metrics |
| `usage_quantity` | DECIMAL(38,18) | Run usage | Usage metrics |
| `duration_hours` | DECIMAL(38,18) | Run duration | Time metrics |

### `gld_fact_run_status_cost`
**Purpose**: Cost by run status
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace analysis |
| `entity_type` | STRING | Entity type | Entity classification |
| `entity_id` | STRING | Entity identifier | Entity tracking |
| `job_run_id` | STRING | Job run identifier | Run tracking |
| `result_state` | STRING | Run result | Status analysis |
| `termination_code` | STRING | Termination reason | Failure analysis |
| `result_state_cost_usd` | DECIMAL(38,18) | Cost by status | Cost analysis |

### `gld_fact_runs_finished_day`
**Purpose**: Run completion metrics by day
**SCD Type**: Type 1 (current values only)

| Column | Type | Description | Business Purpose |
|--------|------|-------------|------------------|
| `date_sk` | INT | Date surrogate key | Date partitioning |
| `account_id` | STRING | Account identifier | Account-level analysis |
| `workspace_id` | STRING | Workspace identifier | Workspace analysis |
| `entity_type` | STRING | Entity type | Entity classification |
| `entity_id` | STRING | Entity identifier | Entity tracking |
| `finished_runs` | BIGINT | Total finished runs | Activity metrics |
| `success_runs` | BIGINT | Successful runs | Success metrics |
| `failed_runs` | BIGINT | Failed runs | Failure metrics |
| `cancelled_runs` | BIGINT | Cancelled runs | Cancellation metrics |

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

### Schema Alignment with Databricks System Tables
All Bronze layer tables are now aligned with the actual Databricks system table schemas:
- **Data Types**: Using correct data types (STRING for IDs, DECIMAL for financial data)
- **Column Names**: Using exact column names from source system tables
- **Complete Schema**: Capturing all fields available in source tables
- **Unique Identifiers**: Using proper unique identifiers like `record_id` for reliable upserts