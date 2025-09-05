-- Silver Layer Tables
-- This file contains DDL statements for creating and maintaining Silver layer tables
-- Silver layer focuses on curated, business-ready data with SCD2 support
-- Naming convention: slv_[table_name]

-- Workspace Table (Type 1 - Current values only)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_workspace (
    account_id STRING,
    workspace_id STRING,                 -- Changed from BIGINT to STRING
    workspace_name STRING,
    workspace_url STRING,
    create_time TIMESTAMP,
    status STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Jobs SCD2 Table (Type 2 - Historical tracking)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_jobs_scd (
    account_id STRING,
    workspace_id STRING,                 -- Changed from BIGINT to STRING
    job_id STRING,                       -- Changed from BIGINT to STRING
    name STRING,
    description STRING,
    creator_id STRING,
    run_as STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
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
    account_id STRING,
    workspace_id STRING,                 -- Changed from BIGINT to STRING
    pipeline_id STRING,                  -- Changed from BIGINT to STRING
    pipeline_type STRING,
    name STRING,
    created_by STRING,
    run_as STRING,
    tags MAP<STRING, STRING>,
    settings STRUCT<
        photon:BOOLEAN,
        development:BOOLEAN,
        continuous:BOOLEAN,
        serverless:BOOLEAN,
        edition:STRING,
        channel:STRING
    >,
    configuration MAP<STRING, STRING>,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Price Table (Type 2 - Historical pricing)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_price_scd (
    account_id STRING,
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    currency_code STRING,
    price_usd DECIMAL(38,18),
    price_start_time TIMESTAMP,
    price_end_time TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Usage Transaction Table (Enriched usage data)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_usage_txn (
    record_id STRING,                    -- Added unique identifier from bronze
    account_id STRING,
    workspace_id STRING,                 -- Changed from BIGINT to STRING
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP,
    usage_date DATE,
    usage_quantity DECIMAL(38,18),
    entity_type STRING,
    entity_id STRING,
    job_run_id STRING,                   -- Renamed from run_id for clarity
    date_sk INT,
    list_cost_usd DECIMAL(38,18),
    duration_hours DECIMAL(38,18),
    billing_origin_product STRING,
    custom_tags MAP<STRING, STRING>,     -- Renamed from tags
    usage_metadata STRUCT<
        cluster_id:STRING,
        job_id:STRING,
        warehouse_id:STRING,
        instance_pool_id:STRING,
        node_type:STRING,
        job_run_id:STRING,
        notebook_id:STRING,
        dlt_pipeline_id:STRING,
        endpoint_name:STRING,
        endpoint_id:STRING,
        dlt_update_id:STRING,
        dlt_maintenance_id:STRING,
        metastore_id:STRING,
        run_name:STRING,
        job_name:STRING,
        notebook_path:STRING,
        central_clean_room_id:STRING,
        source_region:STRING,
        destination_region:STRING,
        app_id:STRING,
        app_name:STRING,
        private_endpoint_name:STRING,
        budget_policy_id:STRING
    >,
    identity_metadata STRUCT<
        run_as:STRING,
        owned_by:STRING,
        created_by:STRING
    >,
    record_type STRING,
    ingestion_date DATE,
    product_features STRUCT<
        jobs_tier:STRING,
        sql_tier:STRING,
        dlt_tier:STRING,
        is_serverless:BOOLEAN,
        is_photon:BOOLEAN,
        serving_type:STRING,
        offering_type:STRING,
        networking:STRUCT<connectivity_type:STRING>
    >,
    usage_type STRING,
    -- NEW: Original business tags (as extracted from source)
    line_of_business_raw STRING,                -- Original value from custom_tags
    department_raw STRING,                      -- Original value from custom_tags
    cost_center_raw STRING,                     -- Original value from custom_tags
    environment_raw STRING,                     -- Original value from custom_tags
    use_case_raw STRING,                        -- Original value from custom_tags
    pipeline_name_raw STRING,                   -- Original value from custom_tags
    cluster_identifier_raw STRING,              -- Original value from custom_tags
    workflow_level_raw STRING,                  -- Original value from custom_tags
    parent_workflow_name_raw STRING,            -- Original value from custom_tags
    -- NEW: Normalized business tags (with defaults applied)
    line_of_business STRING,                    -- Normalized with 'Unknown' default
    department STRING,                          -- Normalized with 'unknown' default
    cost_center STRING,                         -- Normalized with 'unallocated' default
    environment STRING,                         -- Normalized with 'dev' default
    use_case STRING,                            -- Normalized with 'Unknown' default
    pipeline_name STRING,                       -- Normalized with 'system' default
    cluster_identifier STRING,                  -- Normalized with 'Unknown' default
    workflow_level STRING,                      -- Normalized with 'STANDALONE' default
    parent_workflow_name STRING,                -- Normalized with 'None' default
    -- NEW: Inherited cluster tags
    inherited_line_of_business STRING,
    inherited_cost_center STRING,
    inherited_workflow_level STRING,
    inherited_parent_workflow STRING,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Job Run Timeline Table (Run execution data)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_job_run_timeline (
    account_id STRING,
    workspace_id STRING,                 -- Changed from BIGINT to STRING
    job_id STRING,                       -- Changed from BIGINT to STRING
    job_run_id STRING,                   -- Renamed from run_id for clarity
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    trigger_type STRING,
    run_type STRING,
    run_name STRING,
    compute_ids ARRAY<STRING>,
    result_state STRING,
    termination_code STRING,
    job_parameters MAP<STRING, STRING>,
    date_sk_start INT,
    date_sk_end INT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Job Task Run Timeline SCD2 Table (Type 2 - Historical tracking)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_job_task_run_timeline (
    account_id STRING,
    workspace_id STRING,                 -- Changed from BIGINT to STRING
    job_id STRING,                       -- Changed from BIGINT to STRING
    task_run_id STRING,                  -- Renamed from run_id for clarity
    job_run_id STRING,                   -- Parent job run ID
    parent_run_id STRING,                -- Parent run ID
    task_key STRING,
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    compute_ids ARRAY<STRING>,
    result_state STRING,
    termination_code STRING,
    execution_secs DECIMAL(38,18),
    date_sk INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Clusters SCD2 Table (Type 2 - Historical tracking)
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}.slv_clusters (
    account_id STRING,
    workspace_id STRING,                 -- Changed from BIGINT to STRING
    cluster_id STRING,
    cluster_name STRING,
    owned_by STRING,
    create_time TIMESTAMP,
    delete_time TIMESTAMP,
    driver_node_type STRING,             -- Renamed from driver_node_type_id
    worker_node_type STRING,             -- Renamed from node_type_id
    worker_count BIGINT,                 -- Total worker count
    min_autoscale_workers BIGINT,        -- Renamed from min_workers
    max_autoscale_workers BIGINT,        -- Renamed from max_workers
    auto_termination_minutes BIGINT,
    enable_elastic_disk BOOLEAN,
    tags MAP<STRING, STRING>,
    cluster_source STRING,
    init_scripts ARRAY<STRING>,
    aws_attributes STRUCT,
    azure_attributes STRUCT,
    gcp_attributes STRUCT,
    driver_instance_pool_id STRING,
    worker_instance_pool_id STRING,
    dbr_version STRING,                  -- Renamed from spark_version
    change_time TIMESTAMP,
    change_date DATE,
    data_security_mode STRING,
    policy_id STRING,
    -- NEW: Node type categorization (retain original + add categorized)
    worker_node_type_category STRING,    -- Categorized node type (General Purpose, Memory Optimized, etc.)
    -- NEW: Inherited job tags for cost attribution
    inherited_line_of_business STRING,
    inherited_department STRING,
    inherited_cost_center STRING,
    inherited_environment STRING,
    inherited_use_case STRING,
    inherited_workflow_level STRING,
    inherited_parent_workflow STRING,
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


