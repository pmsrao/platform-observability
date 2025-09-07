-- Bronze Tables Bootstrap
-- This file creates empty bronze tables with CDF enabled
-- Naming convention: brz_[source_schema]_[table_name]

-- Billing Usage Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_billing_usage (
    record_id STRING,                    -- Unique identifier from source
    account_id STRING,                   -- Account identifier
    workspace_id STRING,                 -- Workspace identifier (STRING, not BIGINT)
    sku_name STRING,
    cloud STRING,
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP,
    usage_date DATE,
    custom_tags MAP<STRING, STRING>,     -- Custom tags from source
    usage_unit STRING,
    usage_quantity DECIMAL(38,18),       -- DECIMAL, not DOUBLE
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
        run_name:STRING,
        job_name:STRING,
        notebook_path:STRING,
        central_clean_room_id:STRING,
        source_region:STRING,
        destination_region:STRING,
        app_id:STRING,
        app_name:STRING,
        metastore_id:STRING,
        private_endpoint_name:STRING,
        storage_api_type:STRING,
        budget_policy_id:STRING,
        ai_runtime_pool_id:STRING,
        ai_runtime_workload_id:STRING,
        uc_table_catalog:STRING,
        uc_table_schema:STRING,
        uc_table_name:STRING,
        database_instance_id:STRING,
        sharing_materialization_id:STRING,
        schema_id:STRING
    >,
    identity_metadata STRUCT<
        run_as:STRING,
        owned_by:STRING,
        created_by:STRING
    >,
    record_type STRING,
    ingestion_date DATE,
    billing_origin_product STRING,
    product_features STRUCT<
        jobs_tier:STRING,
        sql_tier:STRING,
        dlt_tier:STRING,
        is_serverless:BOOLEAN,
        is_photon:BOOLEAN,
        serving_type:STRING,
        networking:STRUCT<connectivity_type:STRING>,
        ai_runtime:STRUCT<compute_type:STRING>,
        model_serving:STRUCT<offering_type:STRING>,
        ai_gateway:STRUCT<feature_type:STRING>,
        serverless_gpu:STRUCT<workload_type:STRING>
    >,
    usage_type STRING,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- List Prices Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_billing_list_prices (
    price_start_time TIMESTAMP,
    price_end_time TIMESTAMP,
    account_id STRING,
    sku_name STRING,
    cloud STRING,
    currency_code STRING,
    usage_unit STRING,
    pricing STRUCT<
        default:DECIMAL(38,18),
        promotional:STRUCT<default:DECIMAL(38,18)>,
        effective_list:STRUCT<default:DECIMAL(38,18)>
    >,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Lakeflow Jobs Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_lakeflow_jobs (
    account_id STRING,
    workspace_id STRING,                 -- STRING, not BIGINT
    job_id STRING,                       -- STRING, not BIGINT
    name STRING,
    description STRING,
    creator_id STRING,
    tags MAP<STRING, STRING>,
    change_time TIMESTAMP,
    delete_time TIMESTAMP,
    run_as STRING,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Lakeflow Pipelines Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_lakeflow_pipelines (
    account_id STRING,
    workspace_id STRING,                 -- STRING, not BIGINT
    pipeline_id STRING,                  -- STRING, not BIGINT
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
    change_time TIMESTAMP,
    delete_time TIMESTAMP,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Job Run Timeline Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_lakeflow_job_run_timeline (
    account_id STRING,
    workspace_id STRING,                 -- STRING, not BIGINT
    job_id STRING,                       -- STRING, not BIGINT
    run_id STRING,                       -- Original run_id from source
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    trigger_type STRING,
    run_type STRING,
    run_name STRING,
    compute_ids ARRAY<STRING>,
    result_state STRING,
    termination_code STRING,
    job_parameters MAP<STRING, STRING>,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Job Task Run Timeline Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_lakeflow_job_task_run_timeline (
    account_id STRING,
    workspace_id STRING,                 -- STRING, not BIGINT
    job_id STRING,                       -- STRING, not BIGINT
    run_id STRING,                       -- Original run_id from source
    parent_run_id STRING,                -- Parent run ID
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    task_key STRING,
    compute_ids ARRAY<STRING>,
    result_state STRING,
    termination_code STRING,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Access Workspaces Latest Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_access_workspaces_latest (
    account_id STRING,
    workspace_id STRING,                 -- STRING, not BIGINT
    workspace_name STRING,
    workspace_url STRING,
    create_time TIMESTAMP,
    status STRING,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Compute Clusters Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_compute_clusters (
    account_id STRING,
    workspace_id STRING,                 -- STRING, not BIGINT
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
    aws_attributes STRUCT<
        instance_profile_arn:STRING,
        zone_id:STRING,
        first_on_demand:INT,
        availability:STRING,
        spot_bid_price_percent:INT,
        ebs_volume_type:STRING,
        ebs_volume_count:INT,
        ebs_volume_size:INT,
        ebs_volume_iops:INT,
        ebs_volume_throughput:INT
    >,
    azure_attributes STRUCT<
        first_on_demand:INT,
        availability:STRING,
        spot_bid_max_price:DOUBLE
    >,
    gcp_attributes STRUCT<
        use_preemptible_executors:BOOLEAN,
        zone_id:STRING,
        first_on_demand:INT,
        availability:STRING
    >,
    driver_instance_pool_id STRING,
    worker_instance_pool_id STRING,
    dbr_version STRING,                  -- Renamed from spark_version
    change_time TIMESTAMP,
    change_date DATE,
    data_security_mode STRING,
    policy_id STRING,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Compute Node Types Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.brz_compute_node_types (
    account_id STRING,
    node_type STRING,                    -- Renamed from node_type_id to match source
    core_count DOUBLE,                   -- Renamed from vcpus to match source
    memory_mb BIGINT,                    -- Renamed from memory_gb and changed type to match source
    gpu_count BIGINT,                    -- Changed type to match source
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
