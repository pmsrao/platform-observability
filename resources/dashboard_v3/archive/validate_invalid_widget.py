#!/usr/bin/env python3
"""
Validate for 'invalid widget definition' errors
"""

import json
import os

def validate_widget_definitions(dashboard_path: str):
    """Validate widget definitions for common issues that cause 'invalid widget definition'"""
    print(f"🔍 Validating widget definitions in: {dashboard_path}")
    
    with open(dashboard_path, 'r') as f:
        dashboard = json.load(f)
    
    errors = []
    
    for page in dashboard.get("pages", []):
        for widget_layout in page.get("layout", []):
            widget = widget_layout.get("widget", {})
            widget_name = widget.get("name", "unknown")
            spec = widget.get("spec", {})
            
            # Check widget type and version compatibility
            widget_type = spec.get("widgetType", "unknown")
            version = spec.get("version", "unknown")
            
            # Check queries
            queries = widget.get("queries", [])
            if not queries:
                errors.append(f"Widget '{widget_name}': No queries defined")
                continue
            
            for i, query in enumerate(queries):
                if "name" not in query:
                    errors.append(f"Widget '{widget_name}' query {i}: Missing 'name'")
                if "query" not in query:
                    errors.append(f"Widget '{widget_name}' query {i}: Missing 'query' object")
                elif "datasetName" not in query["query"]:
                    errors.append(f"Widget '{widget_name}' query {i}: Missing 'datasetName'")
            
            # Check encodings based on widget type
            encodings = spec.get("encodings", {})
            if not encodings:
                errors.append(f"Widget '{widget_name}': No encodings defined")
                continue
            
            if widget_type == "table":
                if version != 1:
                    errors.append(f"Widget '{widget_name}': Table widget should have version 1, got {version}")
                if "columns" not in encodings:
                    errors.append(f"Widget '{widget_name}': Table widget missing 'columns' in encodings")
                else:
                    for j, column in enumerate(encodings["columns"]):
                        if "fieldName" not in column:
                            errors.append(f"Widget '{widget_name}' column {j}: Missing 'fieldName'")
                        if "displayName" not in column:
                            errors.append(f"Widget '{widget_name}' column {j}: Missing 'displayName'")
            
            elif widget_type == "line":
                if version != 3:
                    errors.append(f"Widget '{widget_name}': Line widget should have version 3, got {version}")
                if "x" not in encodings or "y" not in encodings:
                    errors.append(f"Widget '{widget_name}': Line widget missing 'x' or 'y' in encodings")
                else:
                    if "fieldName" not in encodings["x"]:
                        errors.append(f"Widget '{widget_name}': Line widget 'x' missing 'fieldName'")
                    if "fieldName" not in encodings["y"]:
                        errors.append(f"Widget '{widget_name}': Line widget 'y' missing 'fieldName'")
            
            elif widget_type == "bar":
                if version != 3:
                    errors.append(f"Widget '{widget_name}': Bar widget should have version 3, got {version}")
                if "x" not in encodings or "y" not in encodings:
                    errors.append(f"Widget '{widget_name}': Bar widget missing 'x' or 'y' in encodings")
                else:
                    if "fieldName" not in encodings["x"]:
                        errors.append(f"Widget '{widget_name}': Bar widget 'x' missing 'fieldName'")
                    if "fieldName" not in encodings["y"]:
                        errors.append(f"Widget '{widget_name}': Bar widget 'y' missing 'fieldName'")
            
            elif widget_type == "pie":
                if version != 3:
                    errors.append(f"Widget '{widget_name}': Pie widget should have version 3, got {version}")
                if "theta" not in encodings or "color" not in encodings:
                    errors.append(f"Widget '{widget_name}': Pie widget missing 'theta' or 'color' in encodings")
                else:
                    if "fieldName" not in encodings["theta"]:
                        errors.append(f"Widget '{widget_name}': Pie widget 'theta' missing 'fieldName'")
                    if "fieldName" not in encodings["color"]:
                        errors.append(f"Widget '{widget_name}': Pie widget 'color' missing 'fieldName'")
            
            elif "filter" in widget_type:
                if version != 2:
                    errors.append(f"Widget '{widget_name}': Filter widget should have version 2, got {version}")
                if "fields" not in encodings:
                    errors.append(f"Widget '{widget_name}': Filter widget missing 'fields' in encodings")
                else:
                    for j, field in enumerate(encodings["fields"]):
                        if "parameterName" not in field and "fieldName" not in field:
                            errors.append(f"Widget '{widget_name}' field {j}: Missing 'parameterName' or 'fieldName'")
                        if "queryName" not in field:
                            errors.append(f"Widget '{widget_name}' field {j}: Missing 'queryName'")
    
    if errors:
        print(f"❌ Found {len(errors)} potential 'invalid widget definition' issues:")
        for error in errors:
            print(f"   - {error}")
    else:
        print("✅ No 'invalid widget definition' issues found!")
    
    return errors

if __name__ == "__main__":
    dashboard_path = "dbv3_fixed_final_out.lvdash.json"
    if os.path.exists(dashboard_path):
        validate_widget_definitions(dashboard_path)
    else:
        print(f"❌ Dashboard file not found: {dashboard_path}")
