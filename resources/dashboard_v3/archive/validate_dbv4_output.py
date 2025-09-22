#!/usr/bin/env python3
"""
Comprehensive validation of dbv4 output against DASHBOARD_LEARNINGS_MASTER.md
"""

import json
import os
from datetime import datetime, timedelta

def validate_dbv4_output():
    """Comprehensive validation of dbv4 output"""
    
    # Load the dashboard
    with open('dbv4_out.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔍 Comprehensive validation of dbv4 output...")
    
    issues = []
    warnings = []
    
    # Calculate expected dates
    current_date = datetime.now()
    expected_start_date = (current_date - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000")
    expected_end_date = current_date.strftime("%Y-%m-%dT00:00:00.000")
    
    print(f"📅 Expected dates: {expected_start_date} to {expected_end_date}")
    
    # 1. Validate SQL Concatenation (CRITICAL)
    print("\n1. 🔍 Checking SQL Concatenation...")
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        query_lines = dataset["queryLines"]
        concatenated_sql = "".join(query_lines)
        
        # Check for concatenation issues
        if "SELECTCOUNT" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: SELECTCOUNT found")
        if "FROMplatform_observability" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: FROMplatform_observability found")
        if "UNIONALL" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: UNIONALL found")
        if "WHEREdate_key" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: WHEREdate_key found")
        if "ANDdate_key" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: ANDdate_key found")
        if "ORDERBY" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: ORDERBY found")
        if "GROUPBY" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: GROUPBY found")
    
    # 2. Validate Widget Structure (CRITICAL)
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
            
            # Check widget encodings
            if widget_type == "table":
                if "encodings" not in widget["spec"] or "columns" not in widget["spec"]["encodings"]:
                    issues.append(f"❌ {widget_name}: Table widget missing encodings.columns")
                elif len(widget["spec"]["encodings"]["columns"]) == 0:
                    issues.append(f"❌ {widget_name}: Table widget has empty columns array")
            elif widget_type in ["line", "pie", "bar"]:
                if "encodings" not in widget["spec"]:
                    issues.append(f"❌ {widget_name}: Chart widget missing encodings")
                else:
                    encodings = widget["spec"]["encodings"]
                    if widget_type == "line" and ("x" not in encodings or "y" not in encodings):
                        issues.append(f"❌ {widget_name}: Line chart missing x or y encoding")
                    elif widget_type == "pie" and ("theta" not in encodings or "color" not in encodings):
                        issues.append(f"❌ {widget_name}: Pie chart missing theta or color encoding")
    
    # 3. Validate Parameter Binding (CRITICAL)
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
    
    # 4. Validate Placeholder Replacement (CRITICAL)
    print("\n4. 🔍 Checking Placeholder Replacement...")
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        query_lines = dataset["queryLines"]
        
        full_query = " ".join(query_lines)
        if "{catalog}" in full_query or "{gold_schema}" in full_query:
            issues.append(f"❌ {dataset_name}: Contains unreplaced placeholders")
    
    # 5. Validate Date Values (CRITICAL)
    print("\n5. 🔍 Checking Date Values...")
    for dataset in dashboard["datasets"]:
        for param in dataset.get("parameters", []):
            if param["keyword"] in ["param_start_date", "param_end_date"]:
                default_value = param["defaultSelection"]["values"]["values"][0]["value"]
                if "2024" in default_value:
                    issues.append(f"❌ {dataset_name}: Still has 2024 date: {default_value}")
    
    # 6. Validate Widget Default Dates
    print("\n6. 🔍 Checking Widget Default Dates...")
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            widget_name = widget["name"]
            
            if "start_date" in widget_name:
                if "selection" in widget["spec"] and "defaultSelection" in widget["spec"]["selection"]:
                    default_value = widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"]
                    if "2024" in default_value:
                        issues.append(f"❌ {widget_name}: Still has 2024 date: {default_value}")
            elif "end_date" in widget_name:
                if "selection" in widget["spec"] and "defaultSelection" in widget["spec"]["selection"]:
                    default_value = widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"]
                    if "2024" in default_value:
                        issues.append(f"❌ {widget_name}: Still has 2024 date: {default_value}")
    
    # Print results
    print(f"\n📊 Comprehensive Validation Results:")
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
        print(f"\n🎉 ALL VALIDATIONS PASSED! Dashboard is ready for import!")
        print(f"✅ SQL concatenation: PASSED")
        print(f"✅ Widget structure: PASSED")
        print(f"✅ Parameter binding: PASSED")
        print(f"✅ Placeholder replacement: PASSED")
        print(f"✅ Date values: PASSED")
    elif not issues:
        print(f"\n✅ Critical validations PASSED with warnings. Dashboard is ready for import!")
    else:
        print(f"\n❌ Validation FAILED. Fix issues before import.")
    
    return len(issues) == 0

if __name__ == "__main__":
    validate_dbv4_output()
