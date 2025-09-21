-- Diagnostic queries to understand what data exists
-- Run these in Databricks to identify the data source issue

-- 1. Check if our custom fact table has data
SELECT COUNT(*) as fact_table_records,
       MIN(date_key) as min_date,
       MAX(date_key) as max_date
FROM platform_observability.plt_gold.gld_fact_billing_usage;

-- 2. Check if our custom view has data  
SELECT COUNT(*) as view_records,
       MIN(date_key) as min_date,
       MAX(date_key) as max_date
FROM platform_observability.plt_gold.v_cost_trends;

-- 3. Check if system tables have data (like LakeFlow dashboard)
SELECT COUNT(*) as system_usage_records,
       MIN(usage_date) as min_date,
       MAX(usage_date) as max_date
FROM system.billing.usage
WHERE usage_date >= current_date() - interval 30 days;

-- 4. Check workspace data in system tables
SELECT DISTINCT workspace_id, COUNT(*) as record_count
FROM system.billing.usage
WHERE usage_date >= current_date() - interval 30 days
GROUP BY workspace_id
ORDER BY record_count DESC
LIMIT 10;

-- 5. Check if our gold layer views are even created
SHOW TABLES IN platform_observability.plt_gold;

-- 6. Sample data from system tables (like LakeFlow uses)
SELECT 
    usage_date,
    workspace_id,
    billing_origin_product,
    usage_unit,
    usage_quantity,
    sku_name
FROM system.billing.usage
WHERE usage_date >= current_date() - interval 7 days
LIMIT 10;
