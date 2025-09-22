#!/usr/bin/env python3
"""
Fix SQL spacing and widget structure issues
"""

import json
import os

def fix_sql_and_structure():
    """Fix SQL spacing and widget structure"""
    
    # Load the dashboard
    with open('exact_lakeflow_copy.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing SQL spacing and widget structure...")
    
    # Fix the workspace filter dataset - add proper spacing
    for dataset in dashboard["datasets"]:
        if dataset["name"] == "workspace_filter_dataset":
            dataset["queryLines"] = [
                "SELECT DISTINCT w.workspace_id",
                "FROM platform_observability.plt_gold.gld_fact_billing_usage f",
                "JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key",
                "WHERE f.usage_start_time >= :param_start_date",
                "  AND f.usage_start_time <= :param_end_date",
                "UNION ALL",
                "SELECT '<ALL_WORKSPACES>' as workspace_id"
            ]
            print("✅ Fixed workspace filter dataset SQL spacing")
            break
    
    # Fix the cost summary dataset - add proper spacing
    for dataset in dashboard["datasets"]:
        if dataset["name"] == "cost_summary_dataset":
            dataset["queryLines"] = [
                "SELECT",
                "  COUNT(DISTINCT f.date_key) as days_analyzed,",
                "  ROUND(SUM(f.usage_cost), 2) as total_cost_usd,",
                "  ROUND(AVG(f.usage_cost), 2) as avg_daily_cost,",
                "  ROUND(SUM(f.usage_quantity), 2) as total_usage_quantity,",
                "  COUNT(DISTINCT f.workspace_key) as workspaces_analyzed,",
                "  COUNT(DISTINCT f.billing_origin_product) as workload_types,",
                "  COUNT(DISTINCT f.sku_key) as compute_types",
                "FROM platform_observability.plt_gold.gld_fact_billing_usage f",
                "WHERE f.usage_start_time >= :param_start_date",
                "  AND f.usage_start_time <= :param_end_date",
                "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, f.workspace_key IN (",
                "    SELECT w.workspace_key FROM platform_observability.plt_gold.gld_dim_workspace w",
                "    WHERE w.workspace_id = :param_workspace",
                "  ))"
            ]
            print("✅ Fixed cost summary dataset SQL spacing")
            break
    
    # Fix the workspace filter widget structure
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            if widget["name"] == "filter_workspace":
                # Fix the main query structure
                widget["queries"][0]["query"]["fields"] = [
                    {
                        "name": "workspace_id",
                        "expression": "`workspace_id`"
                    },
                    {
                        "name": "workspace_id_associativity",
                        "expression": "COUNT_IF(`associative_filter_predicate_group`)"
                    }
                ]
                print("✅ Fixed workspace filter widget structure")
                break
    
    # Save the fixed dashboard
    output_path = "exact_lakeflow_copy_fixed.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"💾 Fixed dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_sql_and_structure()
