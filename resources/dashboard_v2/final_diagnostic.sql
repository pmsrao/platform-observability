-- Final diagnostic queries to check data availability
-- Run these in Databricks to verify data exists

-- 1. Check if the view exists and has data
SELECT COUNT(*) as total_records,
       MIN(date_key) as min_date,
       MAX(date_key) as max_date,
       COUNT(DISTINCT workspace_id) as unique_workspaces
FROM platform_observability.plt_gold.v_cost_trends;

-- 2. Check data in the current date range (last 30 days)
SELECT COUNT(*) as records_in_range,
       MIN(date_key) as min_date,
       MAX(date_key) as max_date
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format(current_date() - interval 30 days, 'yyyyMMdd')
  AND date_key <= date_format(current_date(), 'yyyyMMdd');

-- 3. Check workspace data specifically
SELECT DISTINCT workspace_id, COUNT(*) as record_count
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format(current_date() - interval 30 days, 'yyyyMMdd')
  AND date_key <= date_format(current_date(), 'yyyyMMdd')
GROUP BY workspace_id
ORDER BY record_count DESC;

-- 4. Test the exact query from the dashboard
SELECT 
    COUNT(DISTINCT date_key) as days_analyzed,
    ROUND(SUM(daily_cost), 2) as total_cost_usd,
    ROUND(AVG(daily_cost), 2) as avg_daily_cost,
    ROUND(SUM(daily_usage_qty), 2) as total_usage_quantity,
    COUNT(DISTINCT workspace_id) as workspaces_analyzed,
    COUNT(DISTINCT billing_origin_product) as workload_types,
    COUNT(DISTINCT sku_name) as compute_types
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format('2025-08-21', 'yyyyMMdd')
  AND date_key <= date_format('2025-09-20', 'yyyyMMdd')
  AND IF('<ALL WORKSPACES>' = '<ALL WORKSPACES>', true, workspace_id = '<ALL WORKSPACES>');

-- 5. Check if the fact table has data
SELECT COUNT(*) as fact_records,
       MIN(date_key) as min_date,
       MAX(date_key) as max_date
FROM platform_observability.plt_gold.gld_fact_billing_usage;
