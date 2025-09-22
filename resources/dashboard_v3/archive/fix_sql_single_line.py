#!/usr/bin/env python3
"""
Fix SQL by using single-line queries instead of queryLines
"""

import json
import os

def fix_sql_single_line():
    """Fix SQL by using single-line queries"""
    
    # Load the dashboard
    with open('lakeflow_exact_copy_fixed.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Converting to single-line SQL queries...")
    
    # Fix the workspace filter dataset
    for dataset in dashboard["datasets"]:
        if dataset["name"] == "workspace_filter_dataset":
            # Replace queryLines with single query
            workspace_sql = """SELECT DISTINCT workspace_id
FROM platform_observability.plt_gold.gld_fact_billing_usage
WHERE usage_start_time >= :param_start_date
  AND usage_start_time <= :param_end_date
UNION ALL
SELECT '<ALL_WORKSPACES>' as workspace_id"""
            
            # Remove queryLines and add query
            if "queryLines" in dataset:
                del dataset["queryLines"]
            dataset["query"] = workspace_sql
            print("✅ Fixed workspace filter dataset SQL")
            break
    
    # Fix the cost summary dataset
    for dataset in dashboard["datasets"]:
        if dataset["name"] == "cost_summary_dataset":
            # Replace queryLines with single query
            cost_sql = """SELECT
  COUNT(DISTINCT date_key) as days_analyzed,
  ROUND(SUM(usage_cost), 2) as total_cost_usd,
  ROUND(AVG(usage_cost), 2) as avg_daily_cost,
  ROUND(SUM(usage_quantity), 2) as total_usage_quantity,
  COUNT(DISTINCT workspace_key) as workspaces_analyzed,
  COUNT(DISTINCT billing_origin_product) as workload_types,
  COUNT(DISTINCT sku_key) as compute_types
FROM platform_observability.plt_gold.gld_fact_billing_usage
WHERE usage_start_time >= :param_start_date
  AND usage_start_time <= :param_end_date
  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_key IN (
    SELECT workspace_key FROM platform_observability.plt_gold.gld_dim_workspace
    WHERE workspace_id = :param_workspace
  ))"""
            
            # Remove queryLines and add query
            if "queryLines" in dataset:
                del dataset["queryLines"]
            dataset["query"] = cost_sql
            print("✅ Fixed cost summary dataset SQL")
            break
    
    # Save the fixed dashboard
    output_path = "lakeflow_exact_copy_single_line.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"💾 Single-line SQL dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_sql_single_line()
