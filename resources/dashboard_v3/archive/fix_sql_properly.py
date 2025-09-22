#!/usr/bin/env python3
"""
Fix SQL properly - reconstruct the queries correctly
"""

import json
import os
from datetime import datetime, timedelta

def fix_sql_properly():
    """Fix SQL by reconstructing queries properly"""
    
    # Load the dashboard
    with open('dbv4_final_working.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing SQL by reconstructing queries properly...")
    
    # Define the correct SQL queries for each dataset
    correct_queries = {
        "dataset_001": [
            "SELECT",
            "    COUNT(DISTINCT date_key) as days_analyzed,",
            "    ROUND(SUM(daily_cost), 2) as total_cost_usd,",
            "    ROUND(AVG(daily_cost), 2) as avg_daily_cost,",
            "    ROUND(SUM(daily_usage_qty), 2) as total_usage_quantity,",
            "    COUNT(DISTINCT workspace_id) as workspaces_analyzed,",
            "    COUNT(DISTINCT billing_origin_product) as workload_types,",
            "    COUNT(DISTINCT sku_name) as compute_types",
            "FROM platform_observability.plt_gold.v_cost_trends",
            "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')",
            "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')",
            "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)"
        ],
        "dataset_002": [
            "SELECT",
            "    usage_unit,",
            "    ROUND(SUM(daily_cost), 2) as total_cost,",
            "    ROUND(SUM(daily_usage_qty), 2) as total_usage_qty,",
            "    COUNT(DISTINCT workspace_id) as workspaces_count",
            "FROM platform_observability.plt_gold.v_cost_trends",
            "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')",
            "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')",
            "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)",
            "GROUP BY usage_unit",
            "ORDER BY total_cost DESC"
        ],
        "dataset_003": [
            "SELECT",
            "    sku_name,",
            "    ROUND(SUM(daily_cost), 2) as total_cost,",
            "    ROUND(SUM(daily_usage_qty), 2) as total_usage_qty,",
            "    COUNT(DISTINCT workspace_id) as workspaces_count",
            "FROM platform_observability.plt_gold.v_cost_trends",
            "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')",
            "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')",
            "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)",
            "GROUP BY sku_name",
            "ORDER BY total_cost DESC",
            "LIMIT 10"
        ],
        "dataset_004": [
            "SELECT",
            "    job_pipeline_id,",
            "    job_name,",
            "    ROUND(SUM(daily_cost), 2) as total_cost,",
            "    ROUND(SUM(daily_usage_qty), 2) as total_usage_qty",
            "FROM platform_observability.plt_gold.v_cost_trends",
            "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')",
            "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')",
            "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)",
            "GROUP BY job_pipeline_id, job_name",
            "ORDER BY total_cost DESC",
            "LIMIT 10"
        ],
        "dataset_005": [
            "SELECT",
            "    date_key,",
            "    ROUND(SUM(daily_cost), 2) as daily_cost,",
            "    ROUND(SUM(daily_usage_qty), 2) as daily_usage_qty",
            "FROM platform_observability.plt_gold.v_cost_trends",
            "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')",
            "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')",
            "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)",
            "GROUP BY date_key",
            "ORDER BY date_key"
        ],
        "dataset_006": [
            "SELECT",
            "    billing_origin_product as workload_type,",
            "    ROUND(SUM(daily_cost), 2) as cost,",
            "    ROUND(SUM(daily_usage_qty), 2) as usage_qty",
            "FROM platform_observability.plt_gold.v_cost_trends",
            "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')",
            "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')",
            "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)",
            "GROUP BY billing_origin_product",
            "ORDER BY cost DESC"
        ],
        "dataset_007": [
            "SELECT",
            "    workspace_id,",
            "    job_pipeline_id,",
            "    job_name,",
            "    billing_origin_product as workload_type,",
            "    ROUND(usage_cost, 2) as cost,",
            "    anomaly_flag,",
            "    usage_start_time",
            "FROM platform_observability.plt_gold.v_cost_anomalies",
            "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')",
            "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')",
            "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)",
            "  AND anomaly_flag != 'NORMAL'",
            "ORDER BY usage_cost DESC",
            "LIMIT 20"
        ],
        "dataset_008": [
            "SELECT DISTINCT workspace_id",
            "FROM platform_observability.plt_gold.v_cost_trends",
            "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')",
            "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')",
            "UNION ALL",
            "SELECT '<ALL_WORKSPACES>' as workspace_id"
        ]
    }
    
    # Fix each dataset
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        if dataset_name in correct_queries:
            dataset["queryLines"] = correct_queries[dataset_name]
            print(f"✅ Fixed {dataset_name} SQL query")
        else:
            print(f"⚠️  Warning: No correct query defined for {dataset_name}")
    
    # Save the fixed dashboard
    output_path = "dbv4_final_working_fixed.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 SQL properly fixed! Dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_sql_properly()
