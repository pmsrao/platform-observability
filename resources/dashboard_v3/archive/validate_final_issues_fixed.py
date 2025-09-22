#!/usr/bin/env python3
"""
Final validation of all issues fixed
"""

import json
import os
from datetime import datetime, timedelta

def validate_final_issues_fixed():
    """Final validation of all issues fixed"""
    
    # Load the dashboard
    with open('dbv4_final_working_issues_fixed.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔍 Final validation of all issues fixed...")
    
    issues = []
    warnings = []
    
    # Calculate expected dates
    current_date = datetime.now()
    expected_start_date = (current_date - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000")
    expected_end_date = current_date.strftime("%Y-%m-%dT00:00:00.000")
    
    print(f"📅 Expected dates: {expected_start_date} to {expected_end_date}")
    
    # 1. Validate SQL Concatenation
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
    
    # 2. Validate Date Values
    print("\n2. 🔍 Checking Date Values...")
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        for param in dataset.get("parameters", []):
            if param["keyword"] in ["param_start_date", "param_end_date"]:
                default_value = param["defaultSelection"]["values"]["values"][0]["value"]
                if "2024" in default_value:
                    issues.append(f"❌ {dataset_name}: Still has 2024 date: {default_value}")
                elif "start" in param["keyword"] and default_value != expected_start_date:
                    warnings.append(f"⚠️  {dataset_name}: Start date not 30 days ago: {default_value}")
                elif "end" in param["keyword"] and default_value != expected_end_date:
                    warnings.append(f"⚠️  {dataset_name}: End date not today: {default_value}")
    
    # 3. Validate Column Names
    print("\n3. 🔍 Checking Column Names...")
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        query_lines = dataset["queryLines"]
        full_query = " ".join(query_lines)
        
        # Check for usage_start_time in dataset_007
        if dataset_name == "dataset_007" and "usage_start_time" in full_query:
            issues.append(f"❌ {dataset_name}: Still contains usage_start_time")
    
    # 4. Validate Widget Default Dates
    print("\n4. 🔍 Checking Widget Default Dates...")
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
    print(f"\n📊 Final Validation Results:")
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
    elif not issues:
        print(f"\n✅ Critical validations PASSED with warnings. Dashboard is ready for import!")
    else:
        print(f"\n❌ Validation FAILED. Fix issues before import.")
    
    return len(issues) == 0

if __name__ == "__main__":
    validate_final_issues_fixed()
