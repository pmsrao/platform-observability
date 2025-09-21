-- Debug Data Availability Check
-- Run these queries to verify data exists

-- 1. Check if gld_fact_billing_usage table exists and has data
SELECT 
    'gld_fact_billing_usage' as table_name,
    COUNT(*) as total_records,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(DISTINCT workspace_id) as distinct_workspaces
FROM platform_observability.plt_gold.gld_fact_billing_usage;

-- 2. Check if v_cost_trends view exists and has data
SELECT 
    'v_cost_trends' as view_name,
    COUNT(*) as total_records,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(DISTINCT workspace_id) as distinct_workspaces
FROM platform_observability.plt_gold.v_cost_trends;

-- 3. Check data in the last 30 days (our dashboard date range)
SELECT 
    'v_cost_trends_last_30_days' as view_name,
    COUNT(*) as total_records,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(DISTINCT workspace_id) as distinct_workspaces
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format(current_date() - interval 30 days, 'yyyyMMdd') 
  AND date_key <= date_format(current_date(), 'yyyyMMdd');

-- 4. Check specific workspace data
SELECT 
    'workspace_data' as check_type,
    workspace_id,
    COUNT(*) as record_count,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format(current_date() - interval 30 days, 'yyyyMMdd') 
  AND date_key <= date_format(current_date(), 'yyyyMMdd')
GROUP BY workspace_id
ORDER BY record_count DESC;

-- 5. Test a dashboard query directly
SELECT 
    'dashboard_query_test' as test_type,
    COUNT(DISTINCT date_key) as days_analyzed,
    ROUND(SUM(daily_cost), 2) as total_cost_usd,
    ROUND(AVG(daily_cost), 2) as avg_daily_cost,
    COUNT(DISTINCT workspace_id) as workspaces_analyzed
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format('2024-08-21', 'yyyyMMdd') 
  AND date_key <= date_format('2024-09-20', 'yyyyMMdd')
  AND IF('<ALL_WORKSPACES>' = '<ALL_WORKSPACES>', true, workspace_id = '<ALL_WORKSPACES>');
