#!/usr/bin/env python3
"""
Fix SQL spacing issues FINALLY - add proper spaces between keywords
"""

import json
import os

def fix_sql_spacing_final():
    """Fix SQL spacing by adding proper spaces between keywords"""
    
    # Load the dashboard
    with open('dbv4_final_working.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing SQL spacing issues FINALLY...")
    
    # Fix each dataset's queryLines
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        print(f"🔧 Fixing {dataset_name}...")
        
        # Get the current queryLines
        query_lines = dataset["queryLines"]
        
        # Fix spacing issues by adding spaces where needed
        fixed_lines = []
        for i, line in enumerate(query_lines):
            line = line.strip()
            
            # Add space after SELECT if next line doesn't start with space
            if line == "SELECT" and i + 1 < len(query_lines):
                next_line = query_lines[i + 1].strip()
                if not next_line.startswith(" "):
                    line = "SELECT "
            
            # Add space before FROM if previous line doesn't end with space
            if line.startswith("FROM") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Add space before WHERE if previous line doesn't end with space
            if line.startswith("WHERE") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Add space before AND if previous line doesn't end with space
            if line.startswith("AND") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Add space before UNION ALL if previous line doesn't end with space
            if line.startswith("UNION ALL") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Add space before ORDER BY if previous line doesn't end with space
            if line.startswith("ORDER BY") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Add space before GROUP BY if previous line doesn't end with space
            if line.startswith("GROUP BY") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Add space before LIMIT if previous line doesn't end with space
            if line.startswith("LIMIT") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            fixed_lines.append(line)
        
        # Update the dataset
        dataset["queryLines"] = fixed_lines
        print(f"✅ Fixed {len(fixed_lines)} lines for {dataset_name}")
    
    # Save the fixed dashboard
    output_path = "dbv4_final_working_spacing_fixed.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 SQL spacing FINALLY fixed! Dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_sql_spacing_final()
