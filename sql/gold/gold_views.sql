-- Gold Layer Views
-- This file contains business-ready views for BI tools and dashboards

-- Cost Trend Analysis View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_trends AS
SELECT 
    date_sk,
    workspace_id,
    entity_type,
    entity_id,
    SUM(list_cost_usd) as daily_cost,
    AVG(list_cost_usd) OVER (PARTITION BY workspace_id, entity_type, entity_id ORDER BY date_sk ROWS 7 PRECEDING) as rolling_7day_avg_cost
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day
GROUP BY date_sk, workspace_id, entity_type, entity_id;

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
JOIN (
    SELECT workspace_id, entity_type, entity_id, AVG(list_cost_usd) as avg_cost
    FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day
    GROUP BY workspace_id, entity_type, entity_id
) avg_cost ON f.workspace_id = avg_cost.workspace_id 
    AND f.entity_type = avg_cost.entity_type 
    AND f.entity_id = avg_cost.entity_id;
