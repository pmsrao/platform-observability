-- Test queries to check what data exists in the system
-- Run these in Databricks to diagnose the "No data" issue

-- 1. Check if the fact table exists and has data
SELECT COUNT(*) as total_records, 
       MIN(date_key) as min_date, 
       MAX(date_key) as max_date
FROM platform_observability.plt_gold.gld_fact_billing_usage;

-- 2. Check if the view exists and has data
SELECT COUNT(*) as total_records,
       MIN(date_key) as min_date,
       MAX(date_key) as max_date
FROM platform_observability.plt_gold.v_cost_trends;

-- 3. Check workspace data
SELECT DISTINCT workspace_id, COUNT(*) as record_count
FROM platform_observability.plt_gold.v_cost_trends
GROUP BY workspace_id
ORDER BY record_count DESC;

-- 4. Check date range in the data (last 30 days)
SELECT date_key, COUNT(*) as daily_records
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format(current_date() - interval 30 days, 'yyyyMMdd') 
  AND date_key <= date_format(current_date(), 'yyyyMMdd')
GROUP BY date_key
ORDER BY date_key;

-- 5. Check if the old fact table has data (fallback)
SELECT COUNT(*) as total_records,
       MIN(date_key) as min_date,
       MAX(date_key) as max_date
FROM platform_observability.plt_gold.gld_fact_usage_priced_day;
