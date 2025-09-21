-- Gold Layer Views
-- This file contains business-ready views for BI tools and dashboards

-- Cost Trend Analysis View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_trends AS
SELECT 
    f.date_key,
    w.workspace_id,
    f.billing_origin_product billing_origin_product,
    f.usage_unit,
    nvl(e.entity_id, 'NA') job_pipeline_id,
    e.name job_name,
    sku.sku_name,
    round(SUM(f.usage_cost), 4) as daily_cost,
    round(AVG(SUM(f.usage_cost)) OVER (PARTITION BY w.workspace_id, f.usage_unit, billing_origin_product, nvl(e.entity_id, 'NA') ORDER BY f.date_key ROWS 7 PRECEDING) , 4) as rolling_7day_avg_cost,
    round(SUM(f.usage_quantity), 4) as daily_usage_qty,
    round(AVG(SUM(f.usage_quantity)) OVER (PARTITION BY w.workspace_id, f.usage_unit, billing_origin_product, nvl(e.entity_id, 'NA') ORDER BY f.date_key ROWS 7 PRECEDING), 4) as rolling_7day_avg_usage_qty
FROM {catalog}.{gold_schema}.gld_fact_billing_usage f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
LEFT JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
LEFT JOIN {catalog}.{gold_schema}.gld_dim_sku sku ON f.sku_key = sku.sku_key
GROUP BY all;

-- Anomaly Detection View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_anomalies AS
SELECT 
    f.*,
    CASE 
        WHEN f.usage_cost > (avg_cost.avg_cost * 2) THEN 'HIGH_COST_ANOMALY'
        WHEN f.usage_cost < (avg_cost.avg_cost * 0.1) THEN 'LOW_COST_ANOMALY'
        ELSE 'NORMAL'
    END as anomaly_flag
FROM {catalog}.{gold_schema}.gld_fact_billing_usage f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
LEFT JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
JOIN (
    SELECT w2.workspace_id, f2.billing_origin_product, e2.entity_type, nvl(e2.entity_id, 'NA') entity_id, AVG(f2.usage_cost) as avg_cost
    FROM {catalog}.{gold_schema}.gld_fact_billing_usage f2
    JOIN {catalog}.{gold_schema}.gld_dim_workspace w2 ON f2.workspace_key = w2.workspace_key
    LEFT JOIN {catalog}.{gold_schema}.gld_dim_entity e2 ON f2.entity_key = e2.entity_key
    GROUP BY w2.workspace_id, f2.billing_origin_product, e2.entity_type, nvl(e2.entity_id, 'NA')
) avg_cost ON w.workspace_id = avg_cost.workspace_id 
    AND e.entity_type = avg_cost.entity_type 
    AND nvl(e.entity_id, 'NA') = avg_cost.entity_id
    AND f.billing_origin_product = avg_cost.billing_origin_product;
