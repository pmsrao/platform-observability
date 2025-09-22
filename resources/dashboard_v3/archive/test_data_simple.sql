-- Test if our data exists
SELECT 'Testing gld_fact_billing_usage' as test_name, COUNT(*) as record_count
FROM platform_observability.plt_gold.gld_fact_billing_usage
WHERE usage_start_time >= current_date() - interval 30 days;

SELECT 'Testing v_cost_trends' as test_name, COUNT(*) as record_count  
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format(current_date() - interval 30 days, 'yyyyMMdd');

-- Test date range
SELECT 'Date range test' as test_name, 
       current_date() as today,
       current_date() - interval 30 days as last_30_days,
       date_format(current_date() - interval 30 days, 'yyyyMMdd') as date_key_start,
       date_format(current_date(), 'yyyyMMdd') as date_key_end;
