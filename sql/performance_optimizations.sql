-- Performance Optimizations for Platform Observability
-- This file contains optimizations for better query performance and cost efficiency

USE CATALOG platform_observability;

-- 1. OPTIMIZE and Z-ORDER for common query patterns
-- Optimize Bronze tables for better read performance
OPTIMIZE plt_bronze.bronze_sys_billing_usage_raw 
ZORDER BY (workspace_id, usage_end_time, sku_name);

OPTIMIZE plt_bronze.bronze_lakeflow_job_run_timeline_raw 
ZORDER BY (workspace_id, job_id, period_end_time);

OPTIMIZE plt_bronze.bronze_lakeflow_job_task_run_timeline_raw 
ZORDER BY (workspace_id, job_id, run_id, period_end_time);

-- 2. Partition Gold fact tables by date_sk for better query performance
-- Note: These tables should already be partitioned by date_sk, but we can optimize further

-- Optimize usage fact table
OPTIMIZE plt_gold.fact_usage_priced_day 
ZORDER BY (workspace_id, entity_id, date_sk);

-- Optimize run cost fact table  
OPTIMIZE plt_gold.fact_run_cost 
ZORDER BY (workspace_id, entity_id, date_sk);

-- Optimize task runtime fact table
OPTIMIZE plt_gold.fact_task_runtime_day 
ZORDER BY (workspace_id, entity_id, date_sk);

-- 3. Set table properties for better performance
ALTER TABLE plt_bronze.bronze_sys_billing_usage_raw 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.optimizeWrite.enabled' = 'true'
);

ALTER TABLE plt_bronze.bronze_lakeflow_job_run_timeline_raw 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.optimizeWrite.enabled' = 'true'
);

ALTER TABLE plt_bronze.bronze_lakeflow_job_task_run_timeline_raw 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.optimizeWrite.enabled' = 'true'
);

-- 4. Create optimized views for common query patterns
CREATE OR REPLACE VIEW plt_gold.vw_daily_cost_summary AS
SELECT 
    date_sk,
    workspace_id,
    entity_type,
    SUM(list_cost_usd) as total_cost_usd,
    COUNT(DISTINCT entity_id) as active_entities,
    COUNT(DISTINCT run_id) as total_runs
FROM plt_gold.fact_usage_priced_day
GROUP BY date_sk, workspace_id, entity_type;

-- 5. Create materialized view for expensive aggregations (if supported)
-- This would be a table that gets refreshed periodically
CREATE OR REPLACE TABLE plt_gold.mv_monthly_cost_trend AS
SELECT 
    SUBSTR(CAST(date_sk AS STRING), 1, 6) as month_sk,
    workspace_id,
    entity_type,
    SUM(list_cost_usd) as monthly_cost_usd,
    AVG(list_cost_usd) as avg_daily_cost_usd,
    COUNT(DISTINCT date_sk) as active_days,
    COUNT(DISTINCT entity_id) as active_entities
FROM plt_gold.fact_usage_priced_day
GROUP BY SUBSTR(CAST(date_sk AS STRING), 1, 6), workspace_id, entity_type;

-- 6. Optimize the materialized view
OPTIMIZE plt_gold.mv_monthly_cost_trend 
ZORDER BY (month_sk, workspace_id, entity_type);

-- 7. Set properties for the materialized view
ALTER TABLE plt_gold.mv_monthly_cost_trend 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 8. Create indexes for frequently queried columns (if supported)
-- Note: This is a placeholder - actual syntax depends on Databricks version
-- CREATE INDEX idx_workspace_entity ON plt_gold.fact_usage_priced_day (workspace_id, entity_id);

-- 9. Statistics collection for better query planning
ANALYZE TABLE plt_gold.fact_usage_priced_day COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE plt_gold.fact_run_cost COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE plt_gold.fact_task_runtime_day COMPUTE STATISTICS FOR ALL COLUMNS;

-- 10. Cache frequently accessed tables
CACHE TABLE plt_gold.dim_workspace;
CACHE TABLE plt_gold.dim_entity;
CACHE TABLE plt_gold.dim_date;

-- 11. Set cluster properties for better performance
-- These would be set in the cluster configuration, not in SQL
-- spark.sql.adaptive.enabled=true
-- spark.sql.adaptive.coalescePartitions.enabled=true
-- spark.sql.adaptive.skewJoin.enabled=true
-- spark.sql.adaptive.localShuffleReader.enabled=true

-- 12. Create optimized view for policy compliance queries
CREATE OR REPLACE VIEW plt_gold.vw_policy_compliance_summary AS
SELECT 
    workspace_id,
    COUNT(*) as total_clusters,
    SUM(CASE WHEN is_compliant THEN 1 ELSE 0 END) as compliant_clusters,
    ROUND(SUM(CASE WHEN is_compliant THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as compliance_rate,
    STRING_AGG(DISTINCT violations, ', ') as all_violations
FROM plt_gold.vw_cluster_policy_compliance_current
GROUP BY workspace_id;

-- 13. Optimize Silver tables for CDF reads
OPTIMIZE plt_silver.silver_usage_txn 
ZORDER BY (workspace_id, date_sk, entity_id);

OPTIMIZE plt_silver.silver_job_run_timeline 
ZORDER BY (workspace_id, date_sk, job_id);

-- 14. Set properties for Silver tables
ALTER TABLE plt_silver.silver_usage_txn 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 15. Create summary statistics table for monitoring
CREATE OR REPLACE TABLE plt_gold.daily_summary_stats (
    date_sk INT,
    workspace_id STRING,
    total_records BIGINT,
    total_cost_usd DOUBLE,
    avg_processing_time_seconds DOUBLE,
    failed_runs_count BIGINT,
    successful_runs_count BIGINT,
    updated_at TIMESTAMP
) USING DELTA
PARTITIONED BY (date_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 16. Optimize the summary stats table
OPTIMIZE plt_gold.daily_summary_stats 
ZORDER BY (date_sk, workspace_id);

-- 17. Create view for cost optimization insights
CREATE OR REPLACE VIEW plt_gold.vw_cost_optimization_insights AS
WITH daily_costs AS (
    SELECT 
        date_sk,
        workspace_id,
        entity_type,
        entity_id,
        SUM(list_cost_usd) as daily_cost_usd,
        COUNT(*) as daily_runs
    FROM plt_gold.fact_usage_priced_day
    GROUP BY date_sk, workspace_id, entity_type, entity_id
),
cost_trends AS (
    SELECT 
        workspace_id,
        entity_type,
        entity_id,
        AVG(daily_cost_usd) as avg_daily_cost,
        STDDEV(daily_cost_usd) as cost_volatility,
        MAX(daily_cost_usd) as max_daily_cost,
        MIN(daily_cost_usd) as min_daily_cost
    FROM daily_costs
    GROUP BY workspace_id, entity_type, entity_id
)
SELECT 
    *,
    CASE 
        WHEN cost_volatility > avg_daily_cost * 0.5 THEN 'HIGH_VOLATILITY'
        WHEN cost_volatility > avg_daily_cost * 0.2 THEN 'MEDIUM_VOLATILITY'
        ELSE 'LOW_VOLATILITY'
    END as cost_stability,
    CASE 
        WHEN avg_daily_cost > 100 THEN 'HIGH_COST'
        WHEN avg_daily_cost > 50 THEN 'MEDIUM_COST'
        ELSE 'LOW_COST'
    END as cost_category
FROM cost_trends
ORDER BY avg_daily_cost DESC;

-- 18. Final optimization for all tables
-- This should be run periodically (e.g., daily after data loads)
-- OPTIMIZE ALL TABLES IN SCHEMA plt_bronze;
-- OPTIMIZE ALL TABLES IN SCHEMA plt_silver;
-- OPTIMIZE ALL TABLES IN SCHEMA plt_gold;
