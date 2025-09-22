#!/usr/bin/env python3
"""
Clean up dbv3 files and create one final working version
"""

import json
import os
import shutil
from datetime import datetime, timedelta

def cleanup_and_create_final():
    """Clean up old files and create final working version"""
    
    print("🧹 Cleaning up old dbv3 files...")
    
    # List of files to keep (the working one)
    keep_file = "exact_lakeflow_copy_fixed.lvdash.json"
    
    # List of files to remove
    files_to_remove = [
        "dbv3_field_mappings_fixed.lvdash.json",
        "dbv3_final_out.lvdash.json", 
        "dbv3_fixed_final_out.lvdash.json",
        "dbv3_proper_out.lvdash.json",
        "exact_lakeflow_copy.lvdash.json",
        "lakeflow_exact_copy.lvdash.json",
        "lakeflow_exact_copy_correct_columns.lvdash.json",
        "lakeflow_exact_copy_fixed.lvdash.json",
        "lakeflow_exact_copy_single_line.lvdash.json"
    ]
    
    # Remove old files
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
            print(f"🗑️  Removed: {file}")
    
    # Rename the working file to final name
    final_name = "dbv3_final_working.lvdash.json"
    if os.path.exists(keep_file):
        shutil.move(keep_file, final_name)
        print(f"✅ Renamed {keep_file} to {final_name}")
    
    # Load the final dashboard to verify it's working
    with open(final_name, 'r') as f:
        dashboard = json.load(f)
    
    print(f"\n📊 Final Dashboard Summary:")
    print(f"   File: {final_name}")
    print(f"   Size: {os.path.getsize(final_name) / 1024:.2f} KB")
    print(f"   Datasets: {len(dashboard['datasets'])}")
    print(f"   Pages: {len(dashboard['pages'])}")
    
    # Count widgets
    total_widgets = 0
    for page in dashboard['pages']:
        total_widgets += len(page['layout'])
    print(f"   Widgets: {total_widgets}")
    
    # Show dataset names
    print(f"\n📋 Datasets:")
    for dataset in dashboard['datasets']:
        print(f"   - {dataset['name']}: {dataset['displayName']}")
    
    # Show widget types
    print(f"\n🎯 Widgets:")
    for page in dashboard['pages']:
        for widget_layout in page['layout']:
            widget = widget_layout['widget']
            widget_type = widget['spec']['widgetType']
            print(f"   - {widget['name']}: {widget_type}")
    
    print(f"\n✅ Cleanup complete! Final working dashboard: {final_name}")
    
    return final_name

if __name__ == "__main__":
    cleanup_and_create_final()
