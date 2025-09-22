#!/usr/bin/env python3
"""
Fix remaining issues: UNION ALL spacing, 2024 dates, and usage_start_time
"""

import json
import os
from datetime import datetime, timedelta

def fix_remaining_issues():
    """Fix remaining issues in the dashboard"""
    
    # Load the dashboard
    with open('dbv4_final_working.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing remaining issues...")
    
    # Calculate proper dates (start_date = 1 month ago, end_date = today)
    current_date = datetime.now()
    start_date = (current_date - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000")
    end_date = current_date.strftime("%Y-%m-%dT00:00:00.000")
    
    print(f"📅 Using dates: {start_date} to {end_date}")
    
    # Fix each dataset
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        print(f"🔧 Fixing {dataset_name}...")
        
        # Fix queryLines
        query_lines = dataset["queryLines"]
        fixed_lines = []
        
        for i, line in enumerate(query_lines):
            line = line.strip()
            
            # Fix UNION ALL spacing issue
            if line == "UNION ALL" and i + 1 < len(query_lines):
                next_line = query_lines[i + 1].strip()
                if not next_line.startswith(" "):
                    line = "UNION ALL "
            
            # Fix usage_start_time in dataset_007 (cost_anomalies)
            if dataset_name == "dataset_007" and "usage_start_time" in line:
                print(f"   ⚠️  Removing usage_start_time from {dataset_name}")
                continue  # Skip this line
            
            fixed_lines.append(line)
        
        # Update the dataset
        dataset["queryLines"] = fixed_lines
        
        # Fix 2024 dates in parameters
        for param in dataset.get("parameters", []):
            if param["keyword"] in ["param_start_date", "param_end_date"]:
                default_value = param["defaultSelection"]["values"]["values"][0]["value"]
                if "2024" in default_value:
                    if "start" in param["keyword"]:
                        param["defaultSelection"]["values"]["values"][0]["value"] = start_date
                        print(f"   📅 Fixed {dataset_name} start date: {start_date}")
                    else:
                        param["defaultSelection"]["values"]["values"][0]["value"] = end_date
                        print(f"   📅 Fixed {dataset_name} end date: {end_date}")
    
    # Fix widget default dates
    print("🔧 Fixing widget default dates...")
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            widget_name = widget["name"]
            
            if "start_date" in widget_name:
                if "selection" in widget["spec"] and "defaultSelection" in widget["spec"]["selection"]:
                    widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = start_date
                    print(f"   📅 Fixed {widget_name} default: {start_date}")
            elif "end_date" in widget_name:
                if "selection" in widget["spec"] and "defaultSelection" in widget["spec"]["selection"]:
                    widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = end_date
                    print(f"   📅 Fixed {widget_name} default: {end_date}")
    
    # Save the fixed dashboard
    output_path = "dbv4_final_working_issues_fixed.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 All issues fixed! Dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_remaining_issues()
