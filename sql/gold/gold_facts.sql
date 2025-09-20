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
    record_id STRING,                    -- Unique identifier from billing.usage for traceability
    job_run_id STRING,
    cloud STRING,
    usage_unit STRING,
    line_of_business STRING,
    department STRING,
    cost_center STRING,
    environment STRING,
    use_case STRING,
    pipeline_name STRING,
    workflow_level STRING,
    parent_workflow_name STRING,
    -- MEASURES
    usage_quantity DECIMAL(38,18),
    usage_cost DECIMAL(38,18),
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (date_key, cloud);

-- Entity Cost Fact Table - Daily cost by entity
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_entity_cost (
    date_key INT,
    workspace_key BIGINT,
    entity_key BIGINT,
    -- MEASURES
    usage_cost DECIMAL(38,18),
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
    -- MEASURES
    usage_cost DECIMAL(38,18),
    usage_quantity DECIMAL(38,18)
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
    -- MEASURES
    usage_cost DECIMAL(38,18)
)
USING DELTA
PARTITIONED BY (date_key);

-- Runs Finished Fact Table - Run completion metrics by day
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_runs_finished_day (
    date_key INT,
    workspace_key BIGINT,
    entity_key BIGINT,
    -- MEASURES
    finished_runs BIGINT,
    success_runs BIGINT,
    failed_runs BIGINT,
    cancelled_runs BIGINT
)
USING DELTA
PARTITIONED BY (date_key);

-- Billing Usage Fact Table - Detailed billing usage with additional attributes
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.gld_fact_billing_usage (
    record_id STRING,                    -- Unique identifier from billing.usage (unique in this table)
    date_key INT,                        -- Date surrogate key
    workspace_key BIGINT,                -- Workspace dimension key
    entity_key BIGINT,                   -- Entity dimension key
    cluster_key BIGINT,                  -- Cluster dimension key
    sku_key BIGINT,                      -- SKU dimension key
    job_run_id STRING,                   -- Job run identifier
    cloud STRING,                        -- Cloud provider
    billing_origin_product STRING,       -- Product origin (JOBS, SQL, etc.)
    usage_unit STRING,                   -- Unit of measurement
    usage_type STRING,                   -- Usage type classification
    is_serverless STRING,                -- Serverless flag ('Y' or 'N')
    cost_center STRING,                  -- Cost center for chargeback
    line_of_business STRING,             -- Business unit
    department STRING,                   -- Department
    use_case STRING,                     -- Use case classification
    pipeline_name STRING,                -- Pipeline name
    workflow_level STRING,               -- Workflow hierarchy level
    parent_workflow_name STRING,         -- Parent workflow name
    -- MEASURES
    usage_quantity DECIMAL(38,18),       -- Usage quantity
    usage_cost DECIMAL(38,18),           -- Calculated usage cost
    usage_start_time TIMESTAMP,          -- Usage period start
    usage_end_time TIMESTAMP             -- Usage period end
)
USING DELTA
PARTITIONED BY (date_key)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
