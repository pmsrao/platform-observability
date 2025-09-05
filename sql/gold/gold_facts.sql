-- Gold Layer Facts
-- This file contains DDL statements for creating Gold layer fact tables
-- Note: These tables are populated by the Gold HWM job using DataFrame operations

-- Usage Fact Table - Daily aggregated usage with pricing and normalized tags
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_usage_priced_day (
    date_sk INT,
    workspace_id BIGINT,
    entity_type STRING,
    entity_id STRING,
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    usage_quantity DOUBLE,
    list_cost_usd DOUBLE,
    duration_hours DOUBLE,
    line_of_business STRING,
    department STRING,
    cost_center STRING,
    environment STRING,
    use_case STRING,
    pipeline_name STRING,
    cluster_identifier STRING,
    workflow_level STRING,
    parent_workflow_name STRING
)
USING DELTA
PARTITIONED BY (date_sk, cloud);

-- Entity Cost Fact Table - Daily cost by entity
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_entity_cost (
    date_sk INT,
    workspace_id BIGINT,
    entity_type STRING,
    entity_id STRING,
    list_cost_usd DOUBLE,
    runs_count BIGINT
)
USING DELTA
PARTITIONED BY (date_sk, entity_type);

-- Run Cost Fact Table - Cost by individual run
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_run_cost (
    date_sk INT,
    workspace_id BIGINT,
    entity_type STRING,
    entity_id STRING,
    run_id STRING,
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    list_cost_usd DOUBLE,
    usage_quantity DOUBLE,
    duration_hours DOUBLE
)
USING DELTA
PARTITIONED BY (date_sk, cloud);

-- Run Status Cost Fact Table - Cost by run status
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_run_status_cost (
    date_sk INT,
    workspace_id BIGINT,
    entity_type STRING,
    entity_id STRING,
    run_id STRING,
    result_state STRING,
    termination_code STRING,
    result_state_cost_usd DOUBLE
)
USING DELTA
PARTITIONED BY (date_sk, result_state);

-- Runs Finished Fact Table - Run completion metrics by day
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_runs_finished_day (
    date_sk INT,
    workspace_id BIGINT,
    entity_type STRING,
    entity_id STRING,
    finished_runs BIGINT,
    success_runs BIGINT,
    failed_runs BIGINT,
    cancelled_runs BIGINT
)
USING DELTA
PARTITIONED BY (date_sk, entity_type);
