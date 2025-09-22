#!/usr/bin/env python3
"""
Data Diagnostic Script - Test if our data exists and queries work
"""

import json
from datetime import datetime, timedelta

def test_data_queries():
    """Test the actual queries from our dashboard"""
    
    # Calculate the date range we're using
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    default_start = start_date.strftime("%Y-%m-%dT00:00:00.000")
    default_end = end_date.strftime("%Y-%m-%dT00:00:00.000")
    
    print(f"🔍 Testing data queries with date range: {default_start} to {default_end}")
    print()
    
    # Test queries from our dashboard
    test_queries = [
        {
            "name": "Test 1: Basic fact table count",
            "sql": f"""
SELECT COUNT(*) as record_count
FROM platform_observability.plt_gold.gld_fact_billing_usage 
WHERE usage_start_time >= '{default_start}' 
  AND usage_start_time <= '{default_end}'
"""
        },
        {
            "name": "Test 2: Cost trends view count", 
            "sql": f"""
SELECT COUNT(*) as record_count
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format('{default_start}', 'yyyyMMdd') 
  AND date_key <= date_format('{default_end}', 'yyyyMMdd')
"""
        },
        {
            "name": "Test 3: Date key format test",
            "sql": f"""
SELECT 
    date_format('{default_start}', 'yyyyMMdd') as start_date_key,
    date_format('{default_end}', 'yyyyMMdd') as end_date_key,
    current_date() as today,
    date_format(current_date(), 'yyyyMMdd') as today_key
"""
        },
        {
            "name": "Test 4: Sample data from fact table",
            "sql": f"""
SELECT 
    usage_start_time,
    usage_end_time,
    usage_cost,
    billing_origin_product,
    usage_unit
FROM platform_observability.plt_gold.gld_fact_billing_usage 
WHERE usage_start_time >= '{default_start}' 
  AND usage_start_time <= '{default_end}'
LIMIT 5
"""
        },
        {
            "name": "Test 5: Sample data from cost trends view",
            "sql": f"""
SELECT 
    date_key,
    daily_cost,
    billing_origin_product,
    usage_unit
FROM platform_observability.plt_gold.v_cost_trends
WHERE date_key >= date_format('{default_start}', 'yyyyMMdd') 
  AND date_key <= date_format('{default_end}', 'yyyyMMdd')
LIMIT 5
"""
        }
    ]
    
    print("📋 Test queries to run in Databricks:")
    print("=" * 60)
    
    for i, test in enumerate(test_queries, 1):
        print(f"\n{i}. {test['name']}")
        print("-" * 40)
        print(test['sql'].strip())
        print()
    
    print("=" * 60)
    print("💡 Instructions:")
    print("1. Run these queries in Databricks to check if data exists")
    print("2. If Test 1 & 2 return 0 records, the data doesn't exist in this date range")
    print("3. If Test 4 & 5 return data, then the issue is with dashboard parameter binding")
    print("4. Check the date format in Test 3 to ensure it matches your data")

if __name__ == "__main__":
    test_data_queries()
