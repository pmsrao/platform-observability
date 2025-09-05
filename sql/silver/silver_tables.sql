-- Silver Layer Tables
-- This file contains DDL statements for creating and maintaining Silver layer tables
-- Silver layer focuses on curated, business-ready data with SCD2 support
-- Naming convention: slv_[table_name]

-- Workspace Table (Type 1 - Current values only)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_workspace (
    workspace_id BIGINT,
    workspace_name STRING,
    workspace_url STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Jobs SCD2 Table (Type 2 - Historical tracking)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_jobs_scd (
    workspace_id BIGINT,
    job_id BIGINT,
    name STRING,
    run_as STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    -- NEW: Workflow hierarchy fields
    parent_workflow_id BIGINT,
    workflow_type STRING,
    cluster_id STRING,
    tags MAP<STRING, STRING>,
    -- NEW: Computed workflow fields
    is_parent_workflow BOOLEAN,
    is_sub_workflow BOOLEAN,
    workflow_level STRING,
    parent_workflow_name STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Pipelines SCD2 Table (Type 2 - Historical tracking)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_pipelines_scd (
    workspace_id BIGINT,
    pipeline_id BIGINT,
    name STRING,
    run_as STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Price Table (Type 2 - Historical pricing)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_price_scd (
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    price_usd DOUBLE,
    price_start_time TIMESTAMP,
    price_end_time TIMESTAMP,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Usage Transaction Table (Enriched usage data)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_usage_txn (
    workspace_id BIGINT,
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP,
    usage_quantity DOUBLE,
    entity_type STRING,
    entity_id STRING,
    run_id STRING,
    date_sk INT,
    list_cost_usd DOUBLE,
    duration_hours DOUBLE,
    billing_origin_product STRING,
    tags MAP<STRING, STRING>,
    -- NEW: Normalized business tags
    line_of_business STRING,
    department STRING,
    cost_center STRING,
    environment STRING,
    use_case STRING,
    pipeline_name STRING,
    cluster_identifier STRING,
    -- NEW: Workflow hierarchy tags
    workflow_level STRING,
    parent_workflow_name STRING,
    -- NEW: Inherited cluster tags
    inherited_line_of_business STRING,
    inherited_cost_center STRING,
    inherited_workflow_level STRING,
    inherited_parent_workflow STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Job Run Timeline Table (Run execution data)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_job_run_timeline (
    workspace_id BIGINT,
    job_id BIGINT,
    run_id BIGINT,
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    result_state STRING,
    termination_code STRING,
    date_sk_start INT,
    date_sk_end INT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Job Task Run Timeline SCD2 Table (Type 2 - Historical tracking)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_job_task_run_timeline (
    workspace_id BIGINT,
    job_id BIGINT,
    run_id BIGINT,
    task_key STRING,
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    result_state STRING,
    retry_attempt INT,
    execution_secs DOUBLE,
    date_sk INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Clusters SCD2 Table (Type 2 - Historical tracking)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_clusters (
    workspace_id BIGINT,
    cluster_id STRING,
    cluster_name STRING,
    cluster_type STRING,
    associated_entity_id BIGINT,
    associated_entity_type STRING,
    tags MAP<STRING, STRING>,
    -- NEW: Inherited job tags for cost attribution
    inherited_line_of_business STRING,
    inherited_department STRING,
    inherited_cost_center STRING,
    inherited_environment STRING,
    inherited_use_case STRING,
    inherited_workflow_level STRING,
    inherited_parent_workflow STRING,
    -- NEW: Runtime and Node Type Information
    spark_version STRING,                -- Renamed from databricks_runtime_version
    cluster_source STRING,               -- Renamed from runtime_environment
    node_type_id STRING,                 -- Primary node type (most common)
    min_workers INT,
    max_workers INT,
    driver_node_type_id STRING,          -- Driver node type (if different from primary)
    autoscale_enabled BOOLEAN,
    -- NEW: Computed runtime fields
    major_version INT,
    minor_version INT,
    runtime_age_months INT,
    is_lts BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;


