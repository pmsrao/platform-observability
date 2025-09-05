-- Processing Offset Tables for CDF and HWM Tracking
-- This file creates tables for tracking processed data versions and offsets

-- CDF Processing Offsets Table
CREATE TABLE IF NOT EXISTS {catalog}.{silver_schema}._cdf_processing_offsets (
    source_table STRING,
    last_processed_version BIGINT,
    last_processed_timestamp TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Bronze High Water Mark Processing Offsets Table
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}._bronze_hwm_processing_offsets (
    source_table STRING,
    last_processed_timestamp TIMESTAMP,
    last_processed_version BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA;

-- Insert initial processing offsets for system tables
INSERT INTO {catalog}.{bronze_schema}._bronze_hwm_processing_offsets (source_table, last_processed_timestamp, last_processed_version)
VALUES 
    ('system.billing.usage', NULL, NULL),
    ('system.billing.list_prices', NULL, NULL),
    ('system.lakeflow.job_run_timeline', NULL, NULL),
    ('system.lakeflow.job_task_run_timeline', NULL, NULL),
    ('system.lakeflow.jobs', NULL, NULL),
    ('system.lakeflow.pipelines', NULL, NULL),
    ('system.compute.clusters', NULL, NULL),
    ('system.compute.node_types', NULL, NULL),
    ('system.access.workspaces_latest', NULL, NULL)
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP();
