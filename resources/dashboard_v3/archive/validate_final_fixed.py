#!/usr/bin/env python3
"""
Validate the properly fixed dashboard
"""

import json
import os

def validate_dashboard():
    """Validate the dashboard against all learnings"""
    
    # Load the dashboard
    with open('dbv4_final_working_fixed.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔍 Validating properly fixed dashboard...")
    
    issues = []
    warnings = []
    
    # 1. Validate SQL Spacing
    print("\n1. 🔍 Checking SQL Spacing...")
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        query_lines = dataset["queryLines"]
        
        for i, line in enumerate(query_lines):
            line = line.strip()
            
            # Check for missing spaces after keywords
            keywords = ["SELECT", "FROM", "WHERE", "AND", "OR", "JOIN", "ON", "UNION ALL", "ORDER BY", "GROUP BY", "LIMIT"]
            for keyword in keywords:
                if keyword in line and not line.endswith(keyword + " "):
                    # Check if keyword is followed by space or newline
                    if keyword + " " not in line and keyword + "\n" not in line:
                        issues.append(f"❌ {dataset_name} line {i+1}: Missing space after '{keyword}' in: {line}")
    
    # 2. Validate Widget Structure
    print("\n2. 🔍 Checking Widget Structure...")
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            widget_name = widget["name"]
            widget_type = widget["spec"]["widgetType"]
            version = widget["spec"]["version"]
            
            # Check widget type and version compatibility
            if widget_type == "table" and version != 1:
                issues.append(f"❌ {widget_name}: Table widget should have version 1, got {version}")
            elif widget_type in ["line", "pie", "bar"] and version != 3:
                issues.append(f"❌ {widget_name}: Chart widget should have version 3, got {version}")
            elif widget_type.startswith("filter-") and version != 2:
                issues.append(f"❌ {widget_name}: Filter widget should have version 2, got {version}")
    
    # 3. Validate Parameter Binding
    print("\n3. 🔍 Checking Parameter Binding...")
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            widget_name = widget["name"]
            widget_type = widget["spec"]["widgetType"]
            
            if widget_type.startswith("filter-"):
                if "queries" not in widget or len(widget["queries"]) == 0:
                    issues.append(f"❌ {widget_name}: Filter widget has no queries")
                
                if "encodings" not in widget["spec"] or "fields" not in widget["spec"]["encodings"]:
                    issues.append(f"❌ {widget_name}: Filter widget has no encodings.fields")
    
    # 4. Validate Placeholder Replacement
    print("\n4. 🔍 Checking Placeholder Replacement...")
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        query_lines = dataset["queryLines"]
        
        full_query = " ".join(query_lines)
        if "{catalog}" in full_query or "{gold_schema}" in full_query:
            issues.append(f"❌ {dataset_name}: Contains unreplaced placeholders")
    
    # 5. Validate Date Values
    print("\n5. 🔍 Checking Date Values...")
    for dataset in dashboard["datasets"]:
        for param in dataset.get("parameters", []):
            if param["keyword"] in ["param_start_date", "param_end_date"]:
                default_value = param["defaultSelection"]["values"]["values"][0]["value"]
                if "2024" in default_value:
                    warnings.append(f"⚠️  {dataset_name}: Uses 2024 date: {default_value}")
    
    # Print results
    print(f"\n📊 Validation Results:")
    print(f"   Datasets: {len(dashboard['datasets'])}")
    print(f"   Widgets: {sum(len(page['layout']) for page in dashboard['pages'])}")
    
    if issues:
        print(f"\n❌ Issues Found ({len(issues)}):")
        for issue in issues:
            print(f"   {issue}")
    else:
        print(f"\n✅ No critical issues found!")
    
    if warnings:
        print(f"\n⚠️  Warnings ({len(warnings)}):")
        for warning in warnings:
            print(f"   {warning}")
    
    if not issues and not warnings:
        print(f"\n🎉 Dashboard validation PASSED! Ready for import.")
    elif not issues:
        print(f"\n✅ Dashboard validation PASSED with warnings. Ready for import.")
    else:
        print(f"\n❌ Dashboard validation FAILED. Fix issues before import.")
    
    return len(issues) == 0

if __name__ == "__main__":
    validate_dashboard()
