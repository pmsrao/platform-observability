#!/usr/bin/env python3
"""
Fix column names to match the actual fact table schema
"""

import json
import os

def fix_column_names():
    """Fix column names to match fact table schema"""
    
    # Load the dashboard
    with open('lakeflow_exact_copy_single_line.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing column names to match fact table schema...")
    
    # Fix the workspace filter dataset
    for dataset in dashboard["datasets"]:
        if dataset["name"] == "workspace_filter_dataset":
            # Use workspace_key from fact table and join with dimension to get workspace_id
            workspace_sql = """SELECT DISTINCT w.workspace_id
FROM platform_observability.plt_gold.gld_fact_billing_usage f
JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key
WHERE f.usage_start_time >= :param_start_date
  AND f.usage_start_time <= :param_end_date
UNION ALL
SELECT '<ALL_WORKSPACES>' as workspace_id"""
            
            dataset["query"] = workspace_sql
            print("✅ Fixed workspace filter dataset - using correct column names")
            break
    
    # Fix the cost summary dataset
    for dataset in dashboard["datasets"]:
        if dataset["name"] == "cost_summary_dataset":
            # Use correct column names from fact table
            cost_sql = """SELECT
  COUNT(DISTINCT f.date_key) as days_analyzed,
  ROUND(SUM(f.usage_cost), 2) as total_cost_usd,
  ROUND(AVG(f.usage_cost), 2) as avg_daily_cost,
  ROUND(SUM(f.usage_quantity), 2) as total_usage_quantity,
  COUNT(DISTINCT f.workspace_key) as workspaces_analyzed,
  COUNT(DISTINCT f.billing_origin_product) as workload_types,
  COUNT(DISTINCT f.sku_key) as compute_types
FROM platform_observability.plt_gold.gld_fact_billing_usage f
WHERE f.usage_start_time >= :param_start_date
  AND f.usage_start_time <= :param_end_date
  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, f.workspace_key IN (
    SELECT w.workspace_key FROM platform_observability.plt_gold.gld_dim_workspace w
    WHERE w.workspace_id = :param_workspace
  ))"""
            
            dataset["query"] = cost_sql
            print("✅ Fixed cost summary dataset - using correct column names")
            break
    
    # Save the fixed dashboard
    output_path = "lakeflow_exact_copy_correct_columns.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"💾 Corrected column names dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_column_names()
