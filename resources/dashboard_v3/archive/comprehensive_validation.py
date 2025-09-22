#!/usr/bin/env python3
"""
Comprehensive validation of dbv4 output - detailed analysis
"""

import json
import os
from datetime import datetime, timedelta

def comprehensive_validation():
    """Comprehensive validation of dbv4 output"""
    
    # Load the dashboard
    with open('dbv4_out.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔍 COMPREHENSIVE VALIDATION OF DBV4 OUTPUT")
    print("=" * 60)
    
    issues = []
    warnings = []
    
    # 1. Validate Dashboard Structure
    print("\n1. 📋 DASHBOARD STRUCTURE VALIDATION")
    print("-" * 40)
    
    if "pages" not in dashboard:
        issues.append("❌ Missing 'pages' in dashboard")
    else:
        print(f"✅ Pages: {len(dashboard['pages'])}")
        
        for i, page in enumerate(dashboard["pages"]):
            if "layout" not in page:
                issues.append(f"❌ Page {i}: Missing 'layout'")
            else:
                print(f"✅ Page {i}: {len(page['layout'])} widgets")
    
    if "datasets" not in dashboard:
        issues.append("❌ Missing 'datasets' in dashboard")
    else:
        print(f"✅ Datasets: {len(dashboard['datasets'])}")
    
    # 2. Validate Datasets
    print("\n2. 📊 DATASET VALIDATION")
    print("-" * 40)
    
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        print(f"\n📋 {dataset_name}:")
        
        # Check required fields
        if "displayName" not in dataset:
            issues.append(f"❌ {dataset_name}: Missing displayName")
        else:
            print(f"   ✅ displayName: {dataset['displayName']}")
        
        if "queryLines" not in dataset:
            issues.append(f"❌ {dataset_name}: Missing queryLines")
        else:
            print(f"   ✅ queryLines: {len(dataset['queryLines'])} lines")
            
            # Check SQL concatenation
            concatenated = "".join(dataset['queryLines'])
            if "SELECTCOUNT" in concatenated:
                issues.append(f"❌ {dataset_name}: SELECTCOUNT concatenation issue")
            if "FROMplatform_observability" in concatenated:
                issues.append(f"❌ {dataset_name}: FROMplatform_observability concatenation issue")
            if "UNIONALL" in concatenated:
                issues.append(f"❌ {dataset_name}: UNIONALL concatenation issue")
        
        if "parameters" not in dataset:
            warnings.append(f"⚠️  {dataset_name}: No parameters")
        else:
            print(f"   ✅ parameters: {len(dataset['parameters'])}")
    
    # 3. Validate Widgets
    print("\n3. 🎛️  WIDGET VALIDATION")
    print("-" * 40)
    
    widget_count = 0
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            widget_name = widget["name"]
            widget_type = widget["spec"]["widgetType"]
            version = widget["spec"]["version"]
            widget_count += 1
            
            print(f"\n🎛️  {widget_name} ({widget_type}, v{version}):")
            
            # Check widget structure
            if "queries" not in widget:
                issues.append(f"❌ {widget_name}: Missing queries")
            else:
                print(f"   ✅ queries: {len(widget['queries'])}")
            
            if "spec" not in widget:
                issues.append(f"❌ {widget_name}: Missing spec")
            else:
                spec = widget["spec"]
                
                # Check widget type and version compatibility
                if widget_type == "table" and version != 1:
                    issues.append(f"❌ {widget_name}: Table should be version 1, got {version}")
                elif widget_type in ["line", "pie", "bar"] and version != 3:
                    issues.append(f"❌ {widget_name}: Chart should be version 3, got {version}")
                elif widget_type.startswith("filter-") and version != 2:
                    issues.append(f"❌ {widget_name}: Filter should be version 2, got {version}")
                
                # Check encodings
                if "encodings" not in spec:
                    issues.append(f"❌ {widget_name}: Missing encodings")
                else:
                    encodings = spec["encodings"]
                    
                    if widget_type == "table":
                        if "columns" not in encodings:
                            issues.append(f"❌ {widget_name}: Table missing columns")
                        else:
                            columns = encodings["columns"]
                            print(f"   ✅ columns: {len(columns)}")
                            
                            if len(columns) == 0:
                                issues.append(f"❌ {widget_name}: Empty columns array")
                            else:
                                for i, col in enumerate(columns):
                                    if "fieldName" not in col:
                                        issues.append(f"❌ {widget_name}: Column {i} missing fieldName")
                                    else:
                                        print(f"      ✅ Column {i}: {col['fieldName']}")
                    
                    elif widget_type in ["line", "pie", "bar"]:
                        if widget_type == "line":
                            if "x" not in encodings or "y" not in encodings:
                                issues.append(f"❌ {widget_name}: Line chart missing x or y")
                            else:
                                print(f"   ✅ x: {encodings['x']['fieldName']}")
                                print(f"   ✅ y: {encodings['y']['fieldName']}")
                        elif widget_type == "pie":
                            if "theta" not in encodings or "color" not in encodings:
                                issues.append(f"❌ {widget_name}: Pie chart missing theta or color")
                            else:
                                print(f"   ✅ theta: {encodings['theta']['fieldName']}")
                                print(f"   ✅ color: {encodings['color']['fieldName']}")
                    
                    elif widget_type.startswith("filter-"):
                        if "fields" not in encodings:
                            issues.append(f"❌ {widget_name}: Filter missing fields")
                        else:
                            fields = encodings["fields"]
                            print(f"   ✅ fields: {len(fields)}")
                            
                            for i, field in enumerate(fields):
                                if "fieldName" in field:
                                    print(f"      ✅ Field {i}: fieldName={field['fieldName']}")
                                elif "parameterName" in field:
                                    print(f"      ✅ Field {i}: parameterName={field['parameterName']}")
    
    # 4. Validate Parameter Binding
    print("\n4. 🔗 PARAMETER BINDING VALIDATION")
    print("-" * 40)
    
    # Check if all datasets have parameter queries
    dataset_names = [d["name"] for d in dashboard["datasets"]]
    
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            widget_name = widget["name"]
            
            if "queries" in widget:
                for query in widget["queries"]:
                    if "query" in query and "datasetName" in query["query"]:
                        dataset_name = query["query"]["datasetName"]
                        if dataset_name not in dataset_names:
                            issues.append(f"❌ {widget_name}: References non-existent dataset {dataset_name}")
    
    # 5. Summary
    print("\n5. 📊 VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total Widgets: {widget_count}")
    print(f"Total Datasets: {len(dashboard['datasets'])}")
    
    if issues:
        print(f"\n❌ CRITICAL ISSUES ({len(issues)}):")
        for issue in issues:
            print(f"   {issue}")
    else:
        print(f"\n✅ No critical issues found!")
    
    if warnings:
        print(f"\n⚠️  WARNINGS ({len(warnings)}):")
        for warning in warnings:
            print(f"   {warning}")
    
    if not issues:
        print(f"\n🎉 COMPREHENSIVE VALIDATION PASSED!")
        print("Dashboard is ready for import.")
    else:
        print(f"\n❌ VALIDATION FAILED!")
        print("Fix issues before importing.")
    
    return len(issues) == 0

if __name__ == "__main__":
    comprehensive_validation()
