#!/usr/bin/env python3
"""
Clean SQL queries by removing parameter placeholders and fixing JOIN issues.
This creates clean SQL that can be validated independently.
"""

import json
from pathlib import Path

def clean_sql_queries():
    """Remove parameter placeholders and fix SQL issues to create clean, validatable SQL."""
    
    # Load the current SQL queries
    sql_file = Path("../resources/dashboard/dashboard_sql_v2.json")
    with open(sql_file, 'r') as f:
        data = json.load(f)
    
    # Clean each dataset
    for dataset_id, dataset_info in data["datasets"].items():
        print(f"Cleaning {dataset_id}: {dataset_info['displayName']}")
        
        # Get the query lines
        query_lines = dataset_info["queryLines"]
        query_text = "\n".join(query_lines)
        
        # Clean the query
        cleaned_query = clean_query(query_text, dataset_id)
        
        # Update the query lines
        data["datasets"][dataset_id]["queryLines"] = cleaned_query.split("\n")
        
        # Remove parameters array since we're not using parameter placeholders
        if "parameters" in data["datasets"][dataset_id]:
            del data["datasets"][dataset_id]["parameters"]
    
    # Save the cleaned queries
    output_file = Path("../resources/dashboard/dashboard_sql_v2_clean.json")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Clean SQL queries saved to: {output_file}")
    return output_file

def clean_query(query_text, dataset_id):
    """Clean SQL query by removing parameters and fixing issues."""
    
    # Fix 1: Add missing JOIN to workspace dimension table where needed
    if "w.workspace_name" in query_text and "JOIN.*gld_dim_workspace" not in query_text:
        if "FROM platform_observability.plt_gold.gld_fact_usage_priced_day f" in query_text:
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
    
    # Fix 3: Remove all parameter placeholders and replace with reasonable defaults
    # Date parameters - use last 30 days
    query_text = query_text.replace(":param_start_date", "date_sub(current_date(), 30)")
    query_text = query_text.replace(":param_end_date", "current_date()")
    
    # String parameters - use 'ALL' values
    query_text = query_text.replace(":param_workspace", "'<ALL WORKSPACES>'")
    query_text = query_text.replace(":param_cost_center", "'<ALL COST CENTERS>'")
    query_text = query_text.replace(":param_environment", "'<ALL ENVIRONMENTS>'")
    
    # Fix 4: Fix duplicate WHERE clauses
    if "WHERE cost_center != 'unallocated'" in query_text and "WHERE f.date_key" in query_text:
        # Remove the standalone WHERE clause and merge conditions
        query_text = query_text.replace(
            "WHERE cost_center != 'unallocated' \n  WHERE f.date_key",
            "WHERE f.date_key"
        )
        # Add the cost_center condition to the main WHERE clause
        query_text = query_text.replace(
            "AND ('<ALL ENVIRONMENTS>' = '<ALL ENVIRONMENTS>' OR f.environment = '<ALL ENVIRONMENTS>')",
            "AND ('<ALL ENVIRONMENTS>' = '<ALL ENVIRONMENTS>' OR f.environment = '<ALL ENVIRONMENTS>')\n  AND cost_center != 'unallocated'"
        )
    
    # Fix 5: Fix duplicate JOIN clauses
    if "LEFT JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key" in query_text:
        # Remove duplicate workspace JOIN
        query_text = query_text.replace(
            "LEFT JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key ",
            ""
        )
    
    # Fix 6: Fix date format function calls
    query_text = query_text.replace(
        "date_format(date_sub(current_date(), 30), 'yyyyMMdd')",
        "date_format(date_sub(current_date(), 30), 'yyyyMMdd')"
    )
    query_text = query_text.replace(
        "date_format(current_date(), 'yyyyMMdd')",
        "date_format(current_date(), 'yyyyMMdd')"
    )
    
    return query_text

if __name__ == "__main__":
    clean_sql_queries()
