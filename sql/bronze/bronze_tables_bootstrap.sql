-- Bronze Tables Bootstrap
-- This file creates empty bronze tables with CDF enabled
-- Naming convention: brz_[source_schema]_[table_name]

-- Billing Usage Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_billing_usage (
    workspace_id BIGINT,
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP,
    usage_quantity DOUBLE,
    billing_origin_product STRING,
    tags MAP<STRING, STRING>,
    usage_metadata STRUCT<job_id:STRING, job_run_id:STRING, cluster_id:STRING>,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- List Prices Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_billing_list_prices (
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    pricing STRUCT<default:DOUBLE, tiered:ARRAY<STRUCT<min:DOUBLE, max:DOUBLE, price:DOUBLE>>>,
    price_start_time TIMESTAMP,
    price_end_time TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Lakeflow Jobs Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_lakeflow_jobs (
    workspace_id BIGINT,
    job_id BIGINT,
    name STRING,
    run_as STRING,
    change_time TIMESTAMP,
    -- NEW: Workflow hierarchy fields
    parent_workflow_id BIGINT,           -- Parent workflow ID
    workflow_type STRING,                -- 'PARENT' or 'SUB_WORKFLOW'
    cluster_id STRING,                   -- Associated cluster
    tags MAP<STRING, STRING>,            -- Job-level tags
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Lakeflow Pipelines Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_lakeflow_pipelines (
    workspace_id BIGINT,
    pipeline_id BIGINT,
    name STRING,
    run_as STRING,
    change_time TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Job Run Timeline Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_lakeflow_job_run_timeline (
    workspace_id BIGINT,
    job_id BIGINT,
    run_id BIGINT,
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    result_state STRING,
    termination_code STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Job Task Run Timeline Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_lakeflow_job_task_run_timeline (
    workspace_id BIGINT,
    job_id BIGINT,
    run_id BIGINT,
    task_key STRING,
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    result_state STRING,
    retry_attempt INT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Access Workspaces Latest Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_access_workspaces_latest (
    workspace_id BIGINT,
    workspace_name STRING,
    workspace_url STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Compute Clusters Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_compute_clusters (
    workspace_id BIGINT,
    cluster_id STRING,
    cluster_name STRING,
    cluster_type STRING,                 -- 'JOB_CLUSTER' or 'ALL_PURPOSE'
    associated_entity_id BIGINT,         -- Job/Pipeline ID
    associated_entity_type STRING,       -- 'JOB' or 'PIPELINE'
    tags MAP<STRING, STRING>,            -- Cluster-level tags
    -- NEW: Runtime and Node Type Information
    spark_version STRING,                -- Databricks runtime version (e.g., "13.3.x-scala2.12")
    cluster_source STRING,               -- Cluster source (e.g., "UI", "API", "JOB")
    node_type_id STRING,                 -- Primary node type (e.g., "Standard_DS3_v2")
    min_workers INT,                     -- Minimum number of worker nodes
    max_workers INT,                     -- Maximum number of worker nodes
    driver_node_type_id STRING,          -- Driver node type (if different from primary)
    autoscale_enabled BOOLEAN,           -- Whether autoscaling is enabled
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Compute Node Types Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_compute_node_types (
    node_type_id STRING,                 -- Node type identifier (e.g., "Standard_DS3_v2")
    node_type_name STRING,               -- Human-readable node type name
    vcpus INT,                           -- Number of virtual CPUs
    memory_gb DOUBLE,                    -- Memory in GB
    storage_gb DOUBLE,                   -- Storage in GB
    gpu_count INT,                       -- Number of GPUs (if any)
    gpu_type STRING,                     -- GPU type (if any)
    cloud_provider STRING,               -- Cloud provider (AWS, Azure, GCP)
    region STRING,                       -- Cloud region
    cost_per_hour_usd DOUBLE,            -- Cost per hour in USD
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
