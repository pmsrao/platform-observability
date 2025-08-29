USE CATALOG platform_observability;

CREATE SCHEMA IF NOT EXISTS platform_observability.plt_bronze;

-- Each CTAS copies only the schema (0 rows) and enables CDF
CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_sys_billing_usage_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true, delta.autoOptimize.optimizeWrite=true, delta.autoOptimize.autoCompact=true)
AS SELECT * FROM system.billing.usage WHERE 1=0;

CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_sys_billing_list_prices_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true)
AS SELECT * FROM system.billing.list_prices WHERE 1=0;

CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_lakeflow_job_run_timeline_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true)
AS SELECT * FROM system.lakeflow.job_run_timeline WHERE 1=0;

CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_lakeflow_job_task_run_timeline_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true)
AS SELECT * FROM system.lakeflow.job_task_run_timeline WHERE 1=0;

CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_lakeflow_jobs_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true)
AS SELECT * FROM system.lakeflow.jobs WHERE 1=0;

CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_lakeflow_pipelines_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true)
AS SELECT * FROM system.lakeflow.pipelines WHERE 1=0;

CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_access_workspaces_latest_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true)
AS SELECT * FROM system.access.workspaces_latest WHERE 1=0;

CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_system_compute_clusters_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true)
AS SELECT * FROM system.compute.clusters WHERE 1=0;

CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_system_compute_node_types_raw
TBLPROPERTIES (delta.enableChangeDataFeed=true)
AS SELECT * FROM system.compute.node_types WHERE 1=0;
