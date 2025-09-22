#!/usr/bin/env python3
"""
Fix field mappings to match actual SQL query column names
"""

import json
import os
from datetime import datetime, timedelta

def fix_field_mappings():
    """Fix field mappings in the dashboard"""
    
    # Read the current dashboard
    with open('dbv3_fixed_final_out.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔧 Fixing field mappings...")
    
    # Define correct field mappings for each dataset
    field_mappings = {
        "dataset_001": {
            "columns": [
                {"fieldName": "days_analyzed", "displayName": "Days Analyzed"},
                {"fieldName": "total_cost_usd", "displayName": "Total Cost (USD)"},
                {"fieldName": "avg_daily_cost", "displayName": "Avg Daily Cost"},
                {"fieldName": "total_usage_quantity", "displayName": "Total Usage"},
                {"fieldName": "workspaces_analyzed", "displayName": "Workspaces"},
                {"fieldName": "workload_types", "displayName": "Workload Types"},
                {"fieldName": "compute_types", "displayName": "Compute Types"}
            ]
        },
        "dataset_002": {
            "line": {
                "x": {"fieldName": "date", "scale": {"type": "temporal"}, "displayName": "Date"},
                "y": {"fieldName": "daily_cost", "scale": {"type": "quantitative"}, "format": {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 2}}, "displayName": "Daily Cost"},
                "color": {"fieldName": "workload_type", "scale": {"type": "categorical"}, "displayName": "Workload Type"}
            }
        },
        "dataset_003": {
            "bar": {
                "x": {"fieldName": "usage_unit", "scale": {"type": "ordinal"}, "displayName": "Usage Unit"},
                "y": {"fieldName": "total_cost", "scale": {"type": "quantitative"}, "format": {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 2}}, "displayName": "Total Cost"},
                "color": {"fieldName": "workload_type", "scale": {"type": "categorical"}, "displayName": "Workload Type"}
            }
        },
        "dataset_004": {
            "pie": {
                "theta": {"fieldName": "total_cost", "scale": {"type": "quantitative"}, "format": {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 2}}, "displayName": "Total Cost"},
                "color": {"fieldName": "workload_type", "scale": {"type": "categorical"}, "displayName": "Workload Type"}
            }
        },
        "dataset_005": {
            "columns": [
                {"fieldName": "compute_type", "displayName": "Compute Type"},
                {"fieldName": "workload_type", "displayName": "Workload Type"},
                {"fieldName": "total_cost", "displayName": "Total Cost"},
                {"fieldName": "total_usage", "displayName": "Total Usage"},
                {"fieldName": "workspace_count", "displayName": "Workspaces"}
            ]
        }
    }
    
    # Update widgets with correct field mappings
    for page in dashboard["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            widget_name = widget["name"]
            spec = widget.get("spec", {})
            widget_type = spec.get("widgetType", "")
            
            # Skip filter widgets
            if "filter" in widget_name:
                continue
            
            # Get the dataset name from the first query
            queries = widget.get("queries", [])
            if not queries:
                continue
            
            dataset_name = queries[0]["query"]["datasetName"]
            
            if dataset_name in field_mappings:
                mapping = field_mappings[dataset_name]
                
                if widget_type == "table" and "columns" in mapping:
                    # Update table columns
                    encodings = spec.get("encodings", {})
                    new_columns = []
                    for i, col_mapping in enumerate(mapping["columns"]):
                        column = {
                            "fieldName": col_mapping["fieldName"],
                            "booleanValues": ["false", "true"],
                            "imageUrlTemplate": "{{ @ }}",
                            "imageTitleTemplate": "{{ @ }}",
                            "imageWidth": "",
                            "imageHeight": "",
                            "linkUrlTemplate": "{{ @ }}",
                            "linkTextTemplate": "{{ @ }}",
                            "linkTitleTemplate": "{{ @ }}",
                            "linkOpenInNewTab": True,
                            "type": "string",
                            "displayAs": "string",
                            "visible": True,
                            "order": i,
                            "title": col_mapping["fieldName"],
                            "allowSearch": False,
                            "alignContent": "left",
                            "allowHTML": True,
                            "highlightLinks": False,
                            "useMonospaceFont": False,
                            "preserveWhitespace": False,
                            "displayName": col_mapping["displayName"]
                        }
                        new_columns.append(column)
                    
                    encodings["columns"] = new_columns
                    print(f"✅ Fixed table widget '{widget_name}' with {len(new_columns)} columns")
                
                elif widget_type in ["line", "bar"] and widget_type in mapping:
                    # Update chart encodings
                    encodings = spec.get("encodings", {})
                    encodings.update(mapping[widget_type])
                    print(f"✅ Fixed {widget_type} widget '{widget_name}' with x/y/color encodings")
                
                elif widget_type == "pie" and "pie" in mapping:
                    # Update pie encodings
                    encodings = spec.get("encodings", {})
                    encodings.update(mapping["pie"])
                    print(f"✅ Fixed pie widget '{widget_name}' with theta/color encodings")
    
    # Save the fixed dashboard
    output_path = "dbv3_field_mappings_fixed.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"💾 Fixed dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    
    return output_path

if __name__ == "__main__":
    fix_field_mappings()
