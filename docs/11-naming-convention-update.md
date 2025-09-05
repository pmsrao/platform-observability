# Naming Convention Update Summary

## Overview

This document summarizes the comprehensive naming convention updates made to the Platform Observability solution to align with actual Databricks system table structures and improve consistency across all layers.

## Key Changes Made

### 1. Bronze Layer Table Names

**Previous Naming**: Used incorrect `sys_` prefix and `_raw` suffix
**New Naming**: `brz_[source_schema]_[table_name]` format

| Previous Name | New Name | Source Schema | Source Table |
|---------------|----------|---------------|--------------|
| `bronze_sys_billing_usage_raw` | `brz_billing_usage` | `system.billing` | `usage` |
| `bronze_sys_billing_list_prices_raw` | `brz_billing_list_prices` | `system.billing` | `list_prices` |
| `bronze_lakeflow_jobs_raw` | `brz_lakeflow_jobs` | `system.lakeflow` | `jobs` |
| `bronze_lakeflow_pipelines_raw` | `brz_lakeflow_pipelines` | `system.lakeflow` | `pipelines` |
| `bronze_lakeflow_job_run_timeline_raw` | `brz_lakeflow_job_run_timeline` | `system.lakeflow` | `job_run_timeline` |
| `bronze_lakeflow_job_task_run_timeline_raw` | `brz_lakeflow_job_task_run_timeline` | `system.lakeflow` | `job_task_run_timeline` |
| `bronze_system_compute_clusters_raw` | `brz_compute_clusters` | `system.compute` | `clusters` |
| `bronze_system_compute_node_types_raw` | `brz_compute_node_types` | `system.compute` | `node_types` |
| `bronze_access_workspaces_latest_raw` | `brz_access_workspaces_latest` | `system.access` | `workspaces_latest` |

### 2. Silver Layer Table Names

**Previous Naming**: Used `silver_` prefix
**New Naming**: `slv_[table_name]` format

| Previous Name | New Name | Purpose |
|---------------|----------|---------|
| `silver_workspace` | `slv_workspace` | Workspace information |
| `silver_jobs_scd` | `slv_jobs_scd` | Jobs with SCD2 support |
| `silver_pipelines_scd` | `slv_pipelines_scd` | Pipelines with SCD2 support |
| `silver_price_scd` | `slv_price_scd` | Price history |
| `silver_usage_txn` | `slv_usage_txn` | Usage transactions |
| `silver_job_run_timeline` | `slv_job_run_timeline` | Job run timeline |
| `silver_job_task_run_timeline` | `slv_job_task_run_timeline` | Task timeline |
| `silver_clusters` | `slv_clusters` | Cluster information |
| `silver_entity_latest_v` | `slv_entity_latest_v` | Entity latest view |
| `silver_usage_tags_exploded` | `slv_usage_tags_exploded` | Exploded tags view |
| `silver_usage_run_enriched_v` | `slv_usage_run_enriched_v` | Usage-run enriched view |

### 3. Gold Layer Table Names

**Previous Naming**: Used descriptive names without prefix
**New Naming**: `gld_[table_name]` format

| Previous Name | New Name | Purpose |
|---------------|----------|---------|
| `fact_usage_priced_day` | `gld_fact_usage_priced_day` | Daily usage facts |
| `fact_entity_cost` | `gld_fact_entity_cost` | Entity cost facts |
| `fact_run_cost` | `gld_fact_run_cost` | Run cost facts |
| `fact_run_status_cost` | `gld_fact_run_status_cost` | Run status cost facts |
| `fact_runs_finished_day` | `gld_fact_runs_finished_day` | Daily run completion facts |

### 4. Removed Tables

**Table Removed**: `bronze_runtime_versions_raw` and `silver_runtime_versions`
**Reason**: Runtime information is available directly from `system.compute.clusters.spark_version`

### 5. SCD2 Implementation

**Tables Updated to SCD2**:
- `slv_job_task_run_timeline`: Added `valid_from`, `valid_to`, `is_current` columns
- `slv_clusters`: Added `valid_from`, `valid_to`, `is_current` columns

**Reason**: Historical tracking for clusters and task executions enables better audit trails and change analysis

### 6. Bookmarks to Processing State Rename

**Concept Renamed**: `bookmarks` → `processing_state` / `processing_offsets`
**Reason**: More descriptive naming that better reflects the purpose of tracking processing progress

**Files Renamed**:
- `sql/config/bookmarks.sql` → `sql/config/processing_offsets.sql`
- `libs/bookmarks.py` → `libs/processing_state.py`

**Table Names Updated**:
- `_cdf_bookmarks` → `_cdf_processing_offsets`
- `_bronze_hwm_bookmarks` → `_bronze_hwm_processing_offsets`

**Function Names Updated**:
- `get_last_ts` → `get_last_processed_timestamp`
- `commit_last_ts` → `commit_processing_state`
- `_get_bookmark` → `_get_processing_offset`
- `commit_bookmark` → `commit_processing_offset`

### 7. Updated Schema Fields

**Clusters Table Updates**:
- `databricks_runtime_version` → `spark_version`
- `runtime_environment` → `cluster_source`
- Removed `worker_node_type_id` (redundant with `node_type_id`)

## Implementation Details

### Files Updated

1. **SQL Files**:
   - `sql/bronze/bronze_tables_bootstrap.sql`
   - `sql/silver/silver_tables.sql`
   - `sql/gold/gold_facts.sql`
   - `sql/config/processing_offsets.sql` (renamed from bookmarks.sql)

2. **DLT Pipelines**:
   - `pipelines/silver/silver_build_dlt.py`
   - `pipelines/gold/gold_build_dlt.py`

3. **Ingest Job**:
   - `notebooks/bronze_hwm_ingest_job.py`

4. **Library Files**:
   - `libs/processing_state.py` (renamed from bookmarks.py)
   - `libs/cdf.py`

5. **Documentation**:
   - `docs/09-data-dictionary.md`
   - `docs/11-naming-convention-update.md` (this document)

### Key Benefits

1. **Accuracy**: Names now reflect actual Databricks system table structure
2. **Consistency**: Uniform naming pattern across all layers
3. **Clarity**: Clear indication of data source and layer
4. **Maintainability**: Easier to understand and maintain
5. **Elimination of Redundancy**: Removed unnecessary runtime versions table

### Data Source Mapping

| Bronze Table | Databricks Source | Purpose |
|--------------|-------------------|---------|
| `brz_billing_usage` | `system.billing.usage` | Usage data for cost analysis |
| `brz_billing_list_prices` | `system.billing.list_prices` | Pricing data for cost calculations |
| `brz_lakeflow_jobs` | `system.lakeflow.jobs` | Job metadata and configuration |
| `brz_lakeflow_pipelines` | `system.lakeflow.pipelines` | Pipeline metadata and configuration |
| `brz_compute_clusters` | `system.compute.clusters` | Cluster configuration and runtime info |
| `brz_compute_node_types` | `system.compute.node_types` | Node type specifications |
| `brz_access_workspaces_latest` | `system.access.workspaces_latest` | Workspace metadata |

## Summary of All Changes

### Bronze Layer
- ✅ Updated table names to use `brz_[source_schema]_[table_name]` format
- ✅ Corrected `brz_access_workspaces` to `brz_access_workspaces_latest`
- ✅ Removed `bronze_runtime_versions_raw` table
- ✅ Added `brz_compute_node_types` table

### Silver Layer
- ✅ Updated table names to use `slv_[table_name]` format
- ✅ Implemented SCD2 for `slv_job_task_run_timeline` and `slv_clusters`
- ✅ Updated schema fields (`spark_version`, `cluster_source`)
- ✅ Removed `silver_runtime_versions` table

### Gold Layer
- ✅ Updated table names to use `gld_[table_name]` format
- ✅ All tables now read from updated Silver table names

### Processing State Management
- ✅ Renamed `bookmarks` concept to `processing_state`/`processing_offsets`
- ✅ Updated all function names and table names
- ✅ Updated all import statements and function calls

### Documentation
- ✅ Updated data dictionary with SCD2 information
- ✅ Updated naming convention document
- ✅ All documentation reflects current state

## Migration Notes

### For Existing Deployments

1. **Table Recreation**: Existing tables will need to be recreated with new names
2. **Data Migration**: Data from old tables should be migrated to new tables
3. **Job Updates**: All DLT pipelines and ingest jobs have been updated
4. **View Updates**: Gold layer views automatically use new table names

### For New Deployments

1. **Fresh Start**: Use the updated naming convention from the beginning
2. **Consistent Structure**: All layers follow the same naming pattern
3. **Clear Lineage**: Easy to trace data from source to consumption

## Future Considerations

1. **Runtime Analysis**: Runtime information is now sourced directly from clusters
2. **Node Type Analysis**: Simplified to use primary and driver node types only
3. **Tag Processing**: Maintains all existing tag processing capabilities
4. **Workflow Hierarchy**: Enhanced workflow and cost center support remains intact

## Conclusion

The naming convention update provides a more accurate, consistent, and maintainable structure for the Platform Observability solution. All changes maintain backward compatibility in terms of functionality while improving the overall architecture and clarity of the data model.
