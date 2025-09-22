#!/usr/bin/env python3
"""
Create dbv4 full dashboard with all widgets using proven LakeFlow structure
"""

import json
import os
from datetime import datetime, timedelta

def create_dbv4_full_dashboard():
    """Create full dashboard with all widgets using proven structure"""
    
    print("🏗️ Creating dbv4 full dashboard with all widgets...")
    
    # Load the backup files
    backup_dir = "backup_20250921_225223"
    
    with open(f"{backup_dir}/dbv3_sql_queries.json", 'r') as f:
        sql_queries = json.load(f)
    
    with open(f"{backup_dir}/dbv3_template.json", 'r') as f:
        template = json.load(f)
    
    print(f"✅ Loaded backup files from {backup_dir}")
    
    # Create datasets with proper queryLines and spacing
    datasets = []
    parameter_queries = []
    
    # Process each dataset from the backup
    for dataset_key, dataset_data in sql_queries["datasets"].items():
        dataset_name = dataset_data["name"]
        display_name = dataset_data["displayName"]
        query_lines = dataset_data["queryLines"]
        parameters = dataset_data.get("parameters", [])
        
        # Fix queryLines spacing and replace placeholders
        fixed_query_lines = []
        for line in query_lines:
            # Replace placeholders
            line = line.replace("{catalog}", "platform_observability")
            line = line.replace("{gold_schema}", "plt_gold")
            # Add proper spacing
            fixed_query_lines.append(line.strip())
        
        # Create dataset
        dataset = {
            "name": dataset_name,
            "displayName": display_name,
            "queryLines": fixed_query_lines,
            "parameters": parameters
        }
        datasets.append(dataset)
        
        # Create parameter queries for this dataset
        for param in parameters:
            param_name = param["keyword"]
            query_name = f"parameter_dashboards/dbv4_full/datasets/{dataset_name}_{param_name}"
            parameter_queries.append({
                "name": query_name,
                "query": {
                    "datasetName": dataset_name,
                    "parameters": [{"name": param_name, "keyword": param_name}],
                    "disaggregated": False
                }
            })
    
    print(f"✅ Created {len(datasets)} datasets")
    print(f"✅ Created {len(parameter_queries)} parameter queries")
    
    # Create the full dashboard
    dashboard = {
        "datasets": datasets,
        "pages": [
            {
                "name": "dbv4_full_page",
                "displayName": "Cost & Usage Overview - Full Dashboard",
                "layout": []
            }
        ]
    }
    
    # Process widgets from template
    for widget_layout in template["pages"][0]["layout"]:
        widget = widget_layout["widget"]
        widget_name = widget["name"]
        widget_type = widget["spec"]["widgetType"]
        
        print(f"🔧 Processing widget: {widget_name} ({widget_type})")
        
        # Create widget based on type
        if widget_type == "filter-date-picker":
            # Date picker filter
            param_name = "param_start_date" if "start" in widget_name else "param_end_date"
            widget_queries = [q for q in parameter_queries if param_name in q["name"]]
            
            new_widget = {
                "name": widget_name,
                "queries": widget_queries,
                "spec": {
                    "version": 2,
                    "widgetType": "filter-date-picker",
                    "encodings": {
                        "fields": [
                            {
                                "parameterName": param_name,
                                "queryName": q["name"]
                            } for q in widget_queries
                        ]
                    },
                    "selection": {
                        "defaultSelection": {
                            "values": {
                                "dataType": "DATE",
                                "values": [{"value": "2025-08-22T00:00:00.000"}]
                            }
                        }
                    }
                }
            }
            
        elif widget_type == "filter-single-select":
            # Workspace filter
            if "workspace" in widget_name:
                # Find workspace dataset
                workspace_dataset = None
                for dataset in datasets:
                    if "workspace" in dataset["name"].lower():
                        workspace_dataset = dataset
                        break
                
                if workspace_dataset:
                    # Create main query for workspace dropdown
                    main_query = {
                        "name": "workspace_main_query",
                        "query": {
                            "datasetName": workspace_dataset["name"],
                            "fields": [
                                {
                                    "name": "workspace_id",
                                    "expression": "`workspace_id`"
                                },
                                {
                                    "name": "workspace_id_associativity",
                                    "expression": "COUNT_IF(`associative_filter_predicate_group`)"
                                }
                            ],
                            "disaggregated": False
                        }
                    }
                    
                    # Get parameter queries
                    param_queries = [q for q in parameter_queries if "param_workspace" in q["name"]]
                    
                    new_widget = {
                        "name": widget_name,
                        "queries": [main_query] + param_queries,
                        "spec": {
                            "version": 2,
                            "widgetType": "filter-single-select",
                            "encodings": {
                                "fields": [
                                    {
                                        "fieldName": "workspace_id",
                                        "displayName": "workspace_id",
                                        "queryName": "workspace_main_query"
                                    }
                                ] + [
                                    {
                                        "parameterName": "param_workspace",
                                        "queryName": q["name"]
                                    } for q in param_queries
                                ]
                            },
                            "selection": {
                                "defaultSelection": {
                                    "values": {
                                        "dataType": "STRING",
                                        "values": [{"value": "<ALL_WORKSPACES>"}]
                                    }
                                }
                            }
                        }
                    }
                else:
                    print(f"⚠️  Warning: No workspace dataset found for {widget_name}")
                    continue
                    
        elif widget_type == "table":
            # Table widget
            dataset_name = f"dataset_{widget_name.split('_')[-1].zfill(3)}"
            
            # Find the dataset
            target_dataset = None
            for dataset in datasets:
                if dataset["name"] == dataset_name:
                    target_dataset = dataset
                    break
            
            if target_dataset:
                # Create main query
                main_query = {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "disaggregated": False
                    }
                }
                
                new_widget = {
                    "name": widget_name,
                    "queries": [main_query],
                    "spec": {
                        "version": 1,
                        "widgetType": "table",
                        "encodings": {
                            "columns": []
                        }
                    }
                }
                
                # Add columns based on dataset (simplified for now)
                if "cost_summary" in dataset_name:
                    new_widget["spec"]["encodings"]["columns"] = [
                        {
                            "fieldName": "days_analyzed",
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
                            "order": 0,
                            "title": "days_analyzed",
                            "allowSearch": False,
                            "alignContent": "left",
                            "allowHTML": True,
                            "highlightLinks": False,
                            "useMonospaceFont": False,
                            "preserveWhitespace": False,
                            "displayName": "Days Analyzed"
                        },
                        {
                            "fieldName": "total_cost_usd",
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
                            "order": 1,
                            "title": "total_cost_usd",
                            "allowSearch": False,
                            "alignContent": "left",
                            "allowHTML": True,
                            "highlightLinks": False,
                            "useMonospaceFont": False,
                            "preserveWhitespace": False,
                            "displayName": "Total Cost (USD)"
                        }
                    ]
                
            else:
                print(f"⚠️  Warning: Dataset {dataset_name} not found for {widget_name}")
                continue
                
        else:
            print(f"⚠️  Warning: Unsupported widget type {widget_type} for {widget_name}")
            continue
        
        # Add widget to layout
        dashboard["pages"][0]["layout"].append({
            "widget": new_widget,
            "position": widget_layout["position"]
        })
    
    # Save the dashboard
    output_path = "dbv4_full_dashboard.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 Full dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    print(f"📊 Datasets: {len(datasets)}")
    print(f"📊 Widgets: {len(dashboard['pages'][0]['layout'])}")
    
    return output_path

if __name__ == "__main__":
    create_dbv4_full_dashboard()
