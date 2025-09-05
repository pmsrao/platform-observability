-- Processing Offset Tables for CDF and HWM Tracking
-- This file creates tables for tracking processed data versions and offsets

-- CDF Processing Offsets Table (Task-based)
-- Each task maintains its own processing state to avoid conflicts
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}._cdf_processing_offsets (
    source_table STRING,                    -- Source table name (e.g., 'slv_usage_txn')
    task_name STRING,                       -- Task name (e.g., 'task_gld_fact_usage_priced_day')
    last_processed_version BIGINT,          -- Last processed Delta version
    last_processed_timestamp TIMESTAMP,     -- Last processed timestamp
    updated_at TIMESTAMP
) USING DELTA
CLUSTER BY (source_table);

-- Bronze High Water Mark Processing Offsets Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}._bronze_hwm_processing_offsets (
    source_table STRING,
    last_processed_timestamp TIMESTAMP,
    last_processed_version BIGINT,
    updated_at TIMESTAMP
) USING DELTA
CLUSTER BY (source_table);

-- Insert initial processing offsets for system tables
INSERT INTO {catalog}.{bronze_schema}._bronze_hwm_processing_offsets (source_table, last_processed_timestamp, last_processed_version, updated_at)
VALUES 
    ('system.billing.usage', NULL, NULL, current_timestamp()),
    ('system.billing.list_prices', NULL, NULL, current_timestamp()),
    ('system.lakeflow.job_run_timeline', NULL, NULL, current_timestamp()),
    ('system.lakeflow.job_task_run_timeline', NULL, NULL, current_timestamp()),
    ('system.lakeflow.jobs', NULL, NULL, current_timestamp()),
    ('system.lakeflow.pipelines', NULL, NULL, current_timestamp()),
    ('system.compute.clusters', NULL, NULL, current_timestamp()),
    ('system.compute.node_types', NULL, NULL, current_timestamp()),
    ('system.access.workspaces_latest', NULL, NULL, current_timestamp());
