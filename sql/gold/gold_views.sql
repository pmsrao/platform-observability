-- Gold Layer Views
-- This file contains business-ready views for BI tools and dashboards

-- Cost Trend Analysis View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_trends AS
SELECT 
    f.date_key,
    w.workspace_id,
    e.entity_type,
    e.entity_id,
    SUM(f.list_cost_usd) as daily_cost,
    AVG(f.list_cost_usd) OVER (PARTITION BY w.workspace_id, e.entity_type, e.entity_id ORDER BY f.date_key ROWS 7 PRECEDING) as rolling_7day_avg_cost
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, w.workspace_id, e.entity_type, e.entity_id;

-- Anomaly Detection View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_anomalies AS
SELECT 
    f.*,
    CASE 
        WHEN f.list_cost_usd > (avg_cost.avg_cost * 2) THEN 'HIGH_COST_ANOMALY'
        WHEN f.list_cost_usd < (avg_cost.avg_cost * 0.1) THEN 'LOW_COST_ANOMALY'
        ELSE 'NORMAL'
    END as anomaly_flag
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
JOIN (
    SELECT w2.workspace_id, e2.entity_type, e2.entity_id, AVG(f2.list_cost_usd) as avg_cost
    FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f2
    JOIN {catalog}.{gold_schema}.gld_dim_workspace w2 ON f2.workspace_key = w2.workspace_key
    JOIN {catalog}.{gold_schema}.gld_dim_entity e2 ON f2.entity_key = e2.entity_key
    GROUP BY w2.workspace_id, e2.entity_type, e2.entity_id
) avg_cost ON w.workspace_id = avg_cost.workspace_id 
    AND e.entity_type = avg_cost.entity_type 
    AND e.entity_id = avg_cost.entity_id;
