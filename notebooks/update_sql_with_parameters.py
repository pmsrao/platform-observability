#!/usr/bin/env python3
"""
Script to update SQL queries with parameter support while maintaining fact/dimension model
"""

import json
import sys
from pathlib import Path

def load_json_file(file_path):
    """Load JSON file with error handling."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {file_path} - {e}")
        sys.exit(1)

def update_query_with_parameters(query_lines):
    """Update SQL query lines to use parameters while maintaining fact/dimension model."""
    updated_lines = []
    
    for line in query_lines:
        # Replace hardcoded date filters with parameters
        if "date_key >= date_format(date_sub(current_date(), 30), 'yyyyMMdd')" in line:
            updated_lines.append("  WHERE f.date_key >= date_format(:param_start_date, 'yyyyMMdd')")
            updated_lines.append("    AND f.date_key <= date_format(:param_end_date, 'yyyyMMdd')")
            updated_lines.append("    AND (:param_workspace = '<ALL WORKSPACES>' OR w.workspace_name = :param_workspace)")
            updated_lines.append("    AND (:param_cost_center = '<ALL COST CENTERS>' OR f.cost_center = :param_cost_center)")
            updated_lines.append("    AND (:param_environment = '<ALL ENVIRONMENTS>' OR f.environment = :param_environment)")
        elif "date_key >= date_format(date_sub(current_date(), 90), 'yyyyMMdd')" in line:
            updated_lines.append("  WHERE f.date_key >= date_format(:param_start_date, 'yyyyMMdd')")
            updated_lines.append("    AND f.date_key <= date_format(:param_end_date, 'yyyyMMdd')")
            updated_lines.append("    AND (:param_workspace = '<ALL WORKSPACES>' OR w.workspace_name = :param_workspace)")
            updated_lines.append("    AND (:param_cost_center = '<ALL COST CENTERS>' OR f.cost_center = :param_cost_center)")
            updated_lines.append("    AND (:param_environment = '<ALL ENVIRONMENTS>' OR f.environment = :param_environment)")
        elif "WHERE date_key >= date_format(date_sub(current_date(), 30), 'yyyyMMdd')" in line:
            updated_lines.append("WHERE f.date_key >= date_format(:param_start_date, 'yyyyMMdd')")
            updated_lines.append("  AND f.date_key <= date_format(:param_end_date, 'yyyyMMdd')")
            updated_lines.append("  AND (:param_workspace = '<ALL WORKSPACES>' OR w.workspace_name = :param_workspace)")
            updated_lines.append("  AND (:param_cost_center = '<ALL COST CENTERS>' OR f.cost_center = :param_cost_center)")
            updated_lines.append("  AND (:param_environment = '<ALL ENVIRONMENTS>' OR f.environment = :param_environment)")
        elif "WHERE date_key >= date_format(date_sub(current_date(), 90), 'yyyyMMdd')" in line:
            updated_lines.append("WHERE f.date_key >= date_format(:param_start_date, 'yyyyMMdd')")
            updated_lines.append("  AND f.date_key <= date_format(:param_end_date, 'yyyyMMdd')")
            updated_lines.append("  AND (:param_workspace = '<ALL WORKSPACES>' OR w.workspace_name = :param_workspace)")
            updated_lines.append("  AND (:param_cost_center = '<ALL COST CENTERS>' OR f.cost_center = :param_cost_center)")
            updated_lines.append("  AND (:param_environment = '<ALL ENVIRONMENTS>' OR f.environment = :param_environment)")
        else:
            # Add fact table alias and workspace join where needed
            if "FROM platform_observability.plt_gold.gld_fact_usage_priced_day" in line and "f" not in line:
                updated_lines.append(line.replace("FROM platform_observability.plt_gold.gld_fact_usage_priced_day", "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f"))
            elif "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f" in line:
                updated_lines.append(line)
            else:
                updated_lines.append(line)
    
    return updated_lines

def add_workspace_join_if_needed(query_lines):
    """Add workspace dimension join if not already present."""
    has_workspace_join = any("gld_dim_workspace" in line for line in query_lines)
    has_workspace_filter = any("param_workspace" in line for line in query_lines)
    
    if has_workspace_filter and not has_workspace_join:
        # Find the FROM line and add JOIN after it
        updated_lines = []
        for i, line in enumerate(query_lines):
            updated_lines.append(line)
            if "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f" in line:
                updated_lines.append("  JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key")
        return updated_lines
    
    return query_lines

def update_sql_queries_with_parameters():
    """Update all SQL queries with parameter support."""
    
    # Load the original SQL queries
    queries_file = Path("../resources/dashboard/comprehensive_dashboard_sql_queries.json")
    queries_data = load_json_file(queries_file)
    
    # Define standard parameters for all datasets
    standard_parameters = [
        {
            "displayName": "param_start_date",
            "keyword": "param_start_date",
            "dataType": "DATE"
        },
        {
            "displayName": "param_end_date",
            "keyword": "param_end_date",
            "dataType": "DATE"
        },
        {
            "displayName": "param_workspace",
            "keyword": "param_workspace",
            "dataType": "STRING"
        },
        {
            "displayName": "param_cost_center",
            "keyword": "param_cost_center",
            "dataType": "STRING"
        },
        {
            "displayName": "param_environment",
            "keyword": "param_environment",
            "dataType": "STRING"
        }
    ]
    
    # Update each dataset
    for dataset_id, dataset_info in queries_data["datasets"].items():
        print(f"Updating {dataset_id}: {dataset_info['displayName']}")
        
        # Update query lines with parameters
        updated_query_lines = update_query_with_parameters(dataset_info["queryLines"])
        updated_query_lines = add_workspace_join_if_needed(updated_query_lines)
        
        # Update the dataset
        queries_data["datasets"][dataset_id]["queryLines"] = updated_query_lines
        queries_data["datasets"][dataset_id]["parameters"] = standard_parameters
    
    # Save the updated queries
    output_path = Path("../resources/dashboard/comprehensive_dashboard_sql_queries_v2.json")
    with open(output_path, 'w') as f:
        json.dump(queries_data, f, indent=2)
    
    print(f"✅ Updated SQL queries with parameters saved to: {output_path}")
    return output_path

if __name__ == "__main__":
    update_sql_queries_with_parameters()
