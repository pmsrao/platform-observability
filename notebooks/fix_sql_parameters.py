#!/usr/bin/env python3
"""
Fix SQL parameter binding issues in dashboard_sql_v2.json
- Add missing JOIN clauses to workspace dimension table
- Fix parameter references
- Ensure proper table aliases
"""

import json
from pathlib import Path

def fix_sql_queries():
    """Fix SQL queries by adding proper JOINs and fixing parameter issues."""
    
    # Load the current SQL queries
    sql_file = Path("../resources/dashboard/dashboard_sql_v2.json")
    with open(sql_file, 'r') as f:
        data = json.load(f)
    
    # Fix each dataset
    for dataset_id, dataset_info in data["datasets"].items():
        print(f"Fixing {dataset_id}: {dataset_info['displayName']}")
        
        # Get the query lines
        query_lines = dataset_info["queryLines"]
        query_text = "\n".join(query_lines)
        
        # Fix common issues
        fixed_query = fix_query_issues(query_text, dataset_id)
        
        # Update the query lines
        data["datasets"][dataset_id]["queryLines"] = fixed_query.split("\n")
    
    # Save the fixed queries
    output_file = Path("../resources/dashboard/dashboard_sql_v2_fixed.json")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Fixed SQL queries saved to: {output_file}")
    return output_file

def fix_query_issues(query_text, dataset_id):
    """Fix common SQL query issues."""
    
    # Fix 1: Add missing JOIN to workspace dimension table
    if "w.workspace_name" in query_text and "JOIN.*gld_dim_workspace" not in query_text:
        # Find the FROM clause and add JOIN after it
        if "FROM platform_observability.plt_gold.gld_fact_usage_priced_day" in query_text:
            query_text = query_text.replace(
                "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f",
                "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f\n  JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key"
            )
        elif "FROM platform_observability.plt_gold.gld_fact_usage_priced_day" in query_text:
            query_text = query_text.replace(
                "FROM platform_observability.plt_gold.gld_fact_usage_priced_day",
                "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f\n  JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key"
            )
    
    # Fix 2: Add table alias 'f' if missing
    if "FROM platform_observability.plt_gold.gld_fact_usage_priced_day" in query_text and "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f" not in query_text:
        query_text = query_text.replace(
            "FROM platform_observability.plt_gold.gld_fact_usage_priced_day",
            "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f"
        )
    
    # Fix 3: Fix duplicate WHERE clauses
    if "WHERE cost_center != 'unallocated'" in query_text and "WHERE f.date_key" in query_text:
        # Remove the standalone WHERE clause and merge conditions
        query_text = query_text.replace(
            "WHERE cost_center != 'unallocated' \n  WHERE f.date_key",
            "WHERE f.date_key"
        )
        # Add the cost_center condition to the main WHERE clause
        query_text = query_text.replace(
            "AND (:param_environment = '<ALL ENVIRONMENTS>' OR f.environment = :param_environment)",
            "AND (:param_environment = '<ALL ENVIRONMENTS>' OR f.environment = :param_environment)\n  AND cost_center != 'unallocated'"
        )
    
    # Fix 4: Fix duplicate JOIN clauses
    if "LEFT JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key" in query_text:
        # Remove duplicate workspace JOIN
        query_text = query_text.replace(
            "LEFT JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key ",
            ""
        )
    
    # Fix 5: Fix missing table aliases in WHERE clauses
    if "WHERE f.date_key" in query_text and "f.date_key" not in query_text.split("WHERE")[1].split("AND")[0]:
        # This is already handled by the JOIN fix above
        pass
    
    # Fix 6: Ensure proper parameter formatting
    # Parameters should be properly formatted as :param_name
    
    return query_text

if __name__ == "__main__":
    fix_sql_queries()
