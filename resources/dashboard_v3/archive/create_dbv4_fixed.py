#!/usr/bin/env python3
"""
Create dbv4 full dashboard with proper dataset mapping and all widget types
"""

import json
import os
from datetime import datetime, timedelta

def create_dbv4_fixed():
    """Create full dashboard with proper mapping"""
    
    print("🏗️ Creating dbv4 full dashboard with proper mapping...")
    
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
    
    # Create dataset mapping
    dataset_mapping = {}
    for dataset in datasets:
        dataset_mapping[dataset["name"]] = dataset
    
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
    
    # Process widgets from template with proper mapping
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
            # Workspace filter - use dataset_008 (workspace filter)
            workspace_dataset = dataset_mapping.get("dataset_008")
            
            if workspace_dataset:
                # Create main query for workspace dropdown
                main_query = {
                    "name": "workspace_main_query",
                    "query": {
                        "datasetName": "dataset_008",
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
                print(f"⚠️  Warning: Workspace dataset (dataset_008) not found")
                continue
                
        elif widget_type == "table":
            # Map widget names to dataset names
            widget_to_dataset = {
                "cost_summary_kpis": "dataset_001",
                "cost_breakdown_usage_unit": "dataset_002", 
                "top_compute_skus": "dataset_003",
                "top_jobs_pipelines": "dataset_004",
                "cost_anomalies": "dataset_007"
            }
            
            dataset_name = widget_to_dataset.get(widget_name)
            if not dataset_name:
                print(f"⚠️  Warning: No dataset mapping for {widget_name}")
                continue
                
            target_dataset = dataset_mapping.get(dataset_name)
            if not target_dataset:
                print(f"⚠️  Warning: Dataset {dataset_name} not found")
                continue
            
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
            
            # Add basic columns (will be populated by Databricks)
            new_widget["spec"]["encodings"]["columns"] = [
                {
                    "fieldName": "placeholder",
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
                    "title": "placeholder",
                    "allowSearch": False,
                    "alignContent": "left",
                    "allowHTML": True,
                    "highlightLinks": False,
                    "useMonospaceFont": False,
                    "preserveWhitespace": False,
                    "displayName": "Data"
                }
            ]
                
        elif widget_type == "line":
            # Line chart widget
            dataset_name = "dataset_005"  # daily_cost_trend
            
            target_dataset = dataset_mapping.get(dataset_name)
            if not target_dataset:
                print(f"⚠️  Warning: Dataset {dataset_name} not found for {widget_name}")
                continue
            
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
                    "version": 3,
                    "widgetType": "line",
                    "encodings": {
                        "x": {
                            "fieldName": "date",
                            "displayName": "Date"
                        },
                        "y": {
                            "fieldName": "daily_cost",
                            "displayName": "Daily Cost"
                        }
                    }
                }
            }
            
        elif widget_type == "pie":
            # Pie chart widget
            dataset_name = "dataset_006"  # workload_distribution
            
            target_dataset = dataset_mapping.get(dataset_name)
            if not target_dataset:
                print(f"⚠️  Warning: Dataset {dataset_name} not found for {widget_name}")
                continue
            
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
                    "version": 3,
                    "widgetType": "pie",
                    "encodings": {
                        "theta": {
                            "fieldName": "cost",
                            "displayName": "Cost"
                        },
                        "color": {
                            "fieldName": "workload_type",
                            "displayName": "Workload Type"
                        }
                    }
                }
            }
            
        else:
            print(f"⚠️  Warning: Unsupported widget type {widget_type} for {widget_name}")
            continue
        
        # Add widget to layout
        dashboard["pages"][0]["layout"].append({
            "widget": new_widget,
            "position": widget_layout["position"]
        })
    
    # Save the dashboard
    output_path = "dbv4_full_dashboard_fixed.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 Full dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    print(f"📊 Datasets: {len(datasets)}")
    print(f"📊 Widgets: {len(dashboard['pages'][0]['layout'])}")
    
    return output_path

if __name__ == "__main__":
    create_dbv4_fixed()
