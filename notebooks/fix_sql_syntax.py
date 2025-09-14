#!/usr/bin/env python3
"""
Script to fix SQL syntax issues in the parameterized queries
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

def fix_sql_syntax(query_lines):
    """Fix common SQL syntax issues."""
    fixed_lines = []
    
    for i, line in enumerate(query_lines):
        # Fix missing table alias in FROM clause
        if "FROM platform_observability.plt_gold.gld_fact_usage_priced_day" in line and " f" not in line:
            line = line.replace("FROM platform_observability.plt_gold.gld_fact_usage_priced_day", "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f")
        
        # Fix missing workspace join when workspace parameter is used
        if ":param_workspace" in line and "w.workspace_name" in line:
            # Check if workspace join exists in the query
            has_workspace_join = any("gld_dim_workspace" in l for l in query_lines)
            if not has_workspace_join:
                # Find the FROM line and add JOIN after it
                for j, from_line in enumerate(query_lines):
                    if "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f" in from_line:
                        # Insert JOIN after FROM line
                        if j + 1 < len(query_lines) and "JOIN" not in query_lines[j + 1]:
                            query_lines.insert(j + 1, "  JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key")
                        break
        
        # Fix duplicate WHERE clauses
        if "WHERE cost_center != 'unallocated'" in line and "WHERE f.date_key" in query_lines[i-1]:
            line = line.replace("WHERE cost_center != 'unallocated'", "  AND cost_center != 'unallocated'")
        
        # Fix missing table alias in WHERE clauses
        if "WHERE f.date_key" in line and "FROM platform_observability.plt_gold.gld_fact_usage_priced_day" in query_lines[i-1]:
            # This is correct, keep as is
            pass
        
        fixed_lines.append(line)
    
    return fixed_lines

def fix_all_sql_queries():
    """Fix SQL syntax issues in all datasets."""
    
    # Load the SQL queries
    queries_file = Path("../resources/dashboard/comprehensive_dashboard_sql_queries_v2.json")
    queries_data = load_json_file(queries_file)
    
    # Fix each dataset
    for dataset_id, dataset_info in queries_data["datasets"].items():
        print(f"Fixing {dataset_id}: {dataset_info['displayName']}")
        
        # Fix query lines
        fixed_query_lines = fix_sql_syntax(dataset_info["queryLines"])
        
        # Update the dataset
        queries_data["datasets"][dataset_id]["queryLines"] = fixed_query_lines
    
    # Save the fixed queries
    output_path = Path("../resources/dashboard/comprehensive_dashboard_sql_queries_v2_fixed.json")
    with open(output_path, 'w') as f:
        json.dump(queries_data, f, indent=2)
    
    print(f"✅ Fixed SQL queries saved to: {output_path}")
    return output_path

if __name__ == "__main__":
    fix_all_sql_queries()
