#!/usr/bin/env python3
"""
Fix SELECTCOUNT issue properly
"""

import json
import os

def fix_selectcount_issue():
    """Fix SELECTCOUNT issue properly"""
    
    # Load the dashboard
    with open('dbv4_final_working_issues_fixed.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing SELECTCOUNT issue properly...")
    
    # Fix each dataset's queryLines
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        print(f"🔧 Fixing {dataset_name}...")
        
        # Get the current queryLines
        query_lines = dataset["queryLines"]
        
        # Fix SELECT spacing issue
        fixed_lines = []
        for i, line in enumerate(query_lines):
            line = line.strip()
            
            # Fix SELECT spacing - if line is just "SELECT", add space
            if line == "SELECT":
                line = "SELECT "
            
            # Fix FROM spacing - if line starts with "FROM" and previous line doesn't end with space
            if line.startswith("FROM") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Fix WHERE spacing
            if line.startswith("WHERE") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Fix AND spacing
            if line.startswith("AND") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Fix UNION ALL spacing
            if line == "UNION ALL":
                line = "UNION ALL "
            
            fixed_lines.append(line)
        
        # Update the dataset
        dataset["queryLines"] = fixed_lines
        print(f"✅ Fixed {len(fixed_lines)} lines for {dataset_name}")
    
    # Save the fixed dashboard
    output_path = "dbv4_final_working_all_fixed.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 SELECTCOUNT issue fixed! Dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_selectcount_issue()
