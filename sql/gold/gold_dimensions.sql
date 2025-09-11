-- Gold Layer Dimensions
-- This file contains DDL statements for creating Gold layer dimension tables
-- Note: These tables are populated by the Gold HWM job using DataFrame operations

-- Workspace Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_workspace (
    workspace_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    account_id STRING,
    workspace_id STRING,                 -- Natural key (changed from BIGINT to STRING)
    workspace_name STRING,
    workspace_url STRING,
    status STRING,
    created_time TIMESTAMP,
    updated_time TIMESTAMP
)
USING DELTA;

-- Entity Dimension (SCD2)
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_entity (
    entity_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    account_id STRING,
    workspace_id STRING,                 -- Natural key (changed from BIGINT to STRING)
    entity_type STRING,
    entity_id STRING,                    -- Natural key
    name STRING,
    description STRING,
    creator_id STRING,
    run_as STRING,
    created_time TIMESTAMP,
    updated_time TIMESTAMP,
    -- SCD2 columns
    valid_from TIMESTAMP,                -- When this version became valid
    valid_to TIMESTAMP,                  -- When this version became invalid (NULL for current)
    is_current BOOLEAN DEFAULT TRUE      -- Flag for current version
)
USING DELTA
PARTITIONED BY (entity_type)
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- SKU Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_sku(
    sku_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    account_id STRING,
    cloud STRING,
    sku_name STRING,                    -- Natural key
    usage_unit STRING,
    currency_code STRING,
    current_price_usd DECIMAL(38,18),
    price_effective_from TIMESTAMP,
    price_effective_till TIMESTAMP
)
USING DELTA
PARTITIONED BY (cloud);

-- Run Status Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_run_status (
    run_status_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    result_state STRING,                    -- Natural key
    termination_code STRING
)
USING DELTA;

-- Cluster Dimension (SCD2)
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_cluster (
    cluster_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    account_id STRING,
    workspace_id STRING,                 -- Natural key
    cluster_id STRING,                   -- Natural key
    cluster_name STRING,
    owned_by STRING,
    create_time TIMESTAMP,
    delete_time TIMESTAMP,
    driver_node_type STRING,
    worker_node_type STRING,
    worker_count INT,
    min_autoscale_workers INT,
    max_autoscale_workers INT,
    auto_termination_minutes INT,
    enable_elastic_disk BOOLEAN,
    cluster_source STRING,
    init_scripts STRING,
    driver_instance_pool_id STRING,
    worker_instance_pool_id STRING,
    dbr_version STRING,                  -- Databricks Runtime version
    change_time TIMESTAMP,
    change_date DATE,
    data_security_mode STRING,
    policy_id STRING,
    worker_node_type_category STRING,    -- Categorized node type (General Purpose, Memory Optimized, etc.)
    -- SCD2 columns
    valid_from TIMESTAMP,                -- When this version became valid
    valid_to TIMESTAMP,                  -- When this version became invalid (NULL for current)
    is_current BOOLEAN DEFAULT TRUE      -- Flag for current version
)
USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);

-- Node Type Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_node_type (
    node_type_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    account_id STRING,
    node_type STRING,                    -- Natural key (renamed from node_type_id to match source)
    core_count DOUBLE,                   -- Renamed from vcpus to match source
    memory_mb BIGINT,                    -- Renamed from memory_gb and changed type
    gpu_count BIGINT,                    -- Changed type to match source
    category STRING,                     -- Node type category from Silver layer
)
USING DELTA;

-- Date Dimension (if not exists)
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_date (
    date_key INT,                        -- Renamed from date_sk for consistency
    date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    day_of_year INT,
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    is_quarter_end BOOLEAN,
    is_year_end BOOLEAN
)
USING DELTA
PARTITIONED BY (year, month);
