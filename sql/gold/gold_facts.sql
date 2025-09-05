-- Gold Layer Facts
-- This file contains DDL statements for creating Gold layer fact tables
-- Note: These tables are populated by the Gold HWM job using DataFrame operations

-- Usage Fact Table - Daily aggregated usage with pricing and normalized tags
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_usage_priced_day (
    date_key INT,
    workspace_key BIGINT,
    entity_key BIGINT,
    cluster_key BIGINT,
    sku_key BIGINT,
    usage_quantity DECIMAL(38,18),
    list_cost_usd DECIMAL(38,18),
    duration_hours DECIMAL(38,18),
    job_run_id STRING,
    cloud STRING,
    usage_unit STRING,
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
PARTITIONED BY (date_key, cloud);

-- Entity Cost Fact Table - Daily cost by entity
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_entity_cost (
    date_key INT,
    workspace_key BIGINT,
    entity_key BIGINT,
    list_cost_usd DECIMAL(38,18),
    runs_count BIGINT
)
USING DELTA
PARTITIONED BY (date_key);

-- Run Cost Fact Table - Cost by individual run
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_run_cost (
    date_key INT,
    workspace_key BIGINT,
    entity_key BIGINT,
    cluster_key BIGINT,
    sku_key BIGINT,
    job_run_id STRING,
    cloud STRING,
    usage_unit STRING,
    list_cost_usd DECIMAL(38,18),
    usage_quantity DECIMAL(38,18),
    duration_hours DECIMAL(38,18)
)
USING DELTA
PARTITIONED BY (date_key, cloud);

-- Run Status Cost Fact Table - Cost by run status
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_run_status_cost (
    date_key INT,
    workspace_key BIGINT,
    entity_key BIGINT,
    run_status_key BIGINT,
    job_run_id STRING,
    result_state_cost_usd DECIMAL(38,18)
)
USING DELTA
PARTITIONED BY (date_key);

-- Runs Finished Fact Table - Run completion metrics by day
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_runs_finished_day (
    date_key INT,
    workspace_key BIGINT,
    entity_key BIGINT,
    finished_runs BIGINT,
    success_runs BIGINT,
    failed_runs BIGINT,
    cancelled_runs BIGINT
)
USING DELTA
PARTITIONED BY (date_key);
