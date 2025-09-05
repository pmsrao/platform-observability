-- Gold Layer Dimensions
-- This file contains DDL statements for creating Gold layer dimension tables
-- Note: These tables are populated by the Gold HWM job using DataFrame operations

-- Workspace Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_workspace (
    workspace_id BIGINT,
    workspace_name STRING,
    workspace_url STRING,
    region STRING,
    cloud STRING,
    created_time TIMESTAMP,
    updated_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (cloud);

-- Entity Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_entity (
    workspace_id BIGINT,
    entity_type STRING,
    entity_id STRING,
    name STRING,
    run_as STRING,
    workspace_name STRING,
    workspace_url STRING,
    created_time TIMESTAMP,
    updated_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (entity_type, cloud);

-- SKU Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_sku (
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    billing_origin_product STRING
)
USING DELTA
PARTITIONED BY (cloud);

-- Run Status Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_run_status (
    result_state STRING,
    termination_code STRING
)
USING DELTA;

-- Node Type Dimension
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_node_type (
    node_type_id STRING,
    node_type_name STRING,
    cloud STRING,
    category STRING
)
USING DELTA
PARTITIONED BY (cloud);

-- Date Dimension (if not exists)
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_dim_date (
    date_sk INT,
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
