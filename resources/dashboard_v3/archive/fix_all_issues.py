#!/usr/bin/env python3
"""
Fix all validation issues: SQL spacing and 2024 dates
"""

import json
import os
from datetime import datetime, timedelta

def fix_all_issues():
    """Fix all validation issues"""
    
    # Load the dashboard
    with open('dbv4_full_dashboard_sql_fixed.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing all validation issues...")
    
    # 1. Fix SQL Spacing Issues
    print("1. 🔧 Fixing SQL spacing issues...")
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        print(f"   Fixing {dataset_name}...")
        
        # Get the current queryLines
        query_lines = dataset["queryLines"]
        
        # Fix spacing issues properly
        fixed_lines = []
        for line in query_lines:
            line = line.strip()
            
            # Fix specific spacing issues
            if line == "SELECT":
                line = "SELECT"
            elif line.startswith("SELECT "):
                line = line
            elif line.startswith("SELECT"):
                line = "SELECT " + line[6:]
            
            # Fix other keywords
            line = line.replace(" FROM ", " FROM ")
            line = line.replace(" WHERE ", " WHERE ")
            line = line.replace(" AND ", " AND ")
            line = line.replace(" OR ", " OR ")
            line = line.replace(" JOIN ", " JOIN ")
            line = line.replace(" ON ", " ON ")
            line = line.replace(" UNION ALL ", " UNION ALL ")
            line = line.replace(" ORDER BY ", " ORDER BY ")
            line = line.replace(" GROUP BY ", " GROUP BY ")
            line = line.replace(" LIMIT ", " LIMIT ")
            
            # Clean up multiple spaces
            while "  " in line:
                line = line.replace("  ", " ")
            
            line = line.strip()
            fixed_lines.append(line)
        
        # Update the dataset
        dataset["queryLines"] = fixed_lines
    
    # 2. Fix 2024 Dates
    print("2. 🔧 Fixing 2024 dates...")
    current_date = datetime.now()
    start_date = (current_date - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000")
    end_date = current_date.strftime("%Y-%m-%dT00:00:00.000")
    
    for dataset in dashboard["datasets"]:
        for param in dataset.get("parameters", []):
            if param["keyword"] in ["param_start_date", "param_end_date"]:
                default_value = param["defaultSelection"]["values"]["values"][0]["value"]
                if "2024" in default_value:
                    if "start" in param["keyword"]:
                        param["defaultSelection"]["values"]["values"][0]["value"] = start_date
                        print(f"   Fixed {dataset['name']} start date: {start_date}")
                    else:
                        param["defaultSelection"]["values"]["values"][0]["value"] = end_date
                        print(f"   Fixed {dataset['name']} end date: {end_date}")
    
    # 3. Fix Widget Default Dates
    print("3. 🔧 Fixing widget default dates...")
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            widget_name = widget["name"]
            
            if "start_date" in widget_name:
                if "selection" in widget["spec"] and "defaultSelection" in widget["spec"]["selection"]:
                    widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = start_date
                    print(f"   Fixed {widget_name} default: {start_date}")
            elif "end_date" in widget_name:
                if "selection" in widget["spec"] and "defaultSelection" in widget["spec"]["selection"]:
                    widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = end_date
                    print(f"   Fixed {widget_name} default: {end_date}")
    
    # Save the fixed dashboard
    output_path = "dbv4_final_validated.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 All issues fixed! Dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_all_issues()
