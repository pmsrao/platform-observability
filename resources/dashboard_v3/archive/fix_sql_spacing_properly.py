#!/usr/bin/env python3
"""
Fix SQL spacing issues properly - following the learnings
"""

import json
import os

def fix_sql_spacing_properly():
    """Fix SQL spacing issues in the dbv4 dashboard"""
    
    # Load the dashboard
    with open('dbv4_full_dashboard_fixed.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing SQL spacing issues properly...")
    
    # Fix each dataset's queryLines
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        print(f"🔧 Fixing SQL spacing for {dataset_name}...")
        
        # Get the current queryLines
        query_lines = dataset["queryLines"]
        
        # Fix spacing issues
        fixed_lines = []
        for line in query_lines:
            # Remove any existing whitespace
            line = line.strip()
            
            # Add proper spacing after keywords
            line = line.replace("SELECT", "SELECT ")
            line = line.replace("FROM", " FROM ")
            line = line.replace("WHERE", " WHERE ")
            line = line.replace("AND", " AND ")
            line = line.replace("OR", " OR ")
            line = line.replace("JOIN", " JOIN ")
            line = line.replace("ON", " ON ")
            line = line.replace("UNION ALL", " UNION ALL ")
            line = line.replace("ORDER BY", " ORDER BY ")
            line = line.replace("GROUP BY", " GROUP BY ")
            line = line.replace("HAVING", " HAVING ")
            line = line.replace("LIMIT", " LIMIT ")
            
            # Clean up multiple spaces
            while "  " in line:
                line = line.replace("  ", " ")
            
            # Remove leading/trailing spaces
            line = line.strip()
            
            fixed_lines.append(line)
        
        # Update the dataset
        dataset["queryLines"] = fixed_lines
        print(f"✅ Fixed {len(fixed_lines)} lines for {dataset_name}")
    
    # Save the fixed dashboard
    output_path = "dbv4_full_dashboard_sql_fixed.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 SQL spacing fixed dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_sql_spacing_properly()
