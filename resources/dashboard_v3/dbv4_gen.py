#!/usr/bin/env python3
"""
DBV4 Dashboard Generator - Clean naming convention
"""

import json
import os
from datetime import datetime, timedelta

def create_dbv4_dashboard():
    """Create dbv4 dashboard using clean naming convention"""
    
    print("🏗️ Creating dbv4 dashboard with clean naming...")
    
    # Load the source files
    with open('dbv4_sql.json', 'r') as f:
        sql_queries = json.load(f)
    
    with open('dbv4_template.json', 'r') as f:
        template = json.load(f)
    
    print(f"✅ Loaded source files: dbv4_sql.json and dbv4_template.json")
    
    # Calculate proper dates (start_date = 30 days ago, end_date = today)
    current_date = datetime.now()
    start_date = (current_date - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000")
    end_date = current_date.strftime("%Y-%m-%dT00:00:00.000")
    
    print(f"📅 Using dates: {start_date} to {end_date}")
    
    # Update template filter defaults
    for widget_layout in template["pages"][0]["layout"]:
        widget = widget_layout["widget"]
        if widget["spec"]["widgetType"] == "filter-date-picker":
            if "start" in widget["name"]:
                widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = start_date
            elif "end" in widget["name"]:
                widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = end_date
    
    # Create datasets with proper queryLines and spacing
    datasets = []
    parameter_queries = []
    
    # Process each dataset from the source
    for dataset_key, dataset_data in sql_queries["datasets"].items():
        dataset_name = dataset_data["name"]
        display_name = dataset_data["displayName"]
        query_lines = dataset_data["queryLines"]
        parameters = dataset_data.get("parameters", [])
        
        # Fix queryLines spacing and replace placeholders
        fixed_query_lines = []
        for i, line in enumerate(query_lines):
            # Replace placeholders
            line = line.replace("{catalog}", "platform_observability")
            line = line.replace("{gold_schema}", "plt_gold")
            # Ensure proper spacing for concatenation
            line = line.strip()
            
            # Add proper spacing for SQL keywords
            if line == "SELECT":
                line = "SELECT "
            elif line.startswith("FROM") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            elif line.startswith("WHERE") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            elif line.startswith("AND") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            elif line == "UNION ALL":
                line = " UNION ALL "
            elif line.startswith("ORDER BY") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            elif line.startswith("GROUP BY") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            elif line.startswith("LIMIT") and i > 0:
                prev_line = query_lines[i - 1].strip()
                if not prev_line.endswith(" "):
                    line = " " + line
            
            # Ensure last line has space at the end for dynamic filters
            if i == len(query_lines) - 1:
                if not line.endswith(" "):
                    line = line + " "
            
            fixed_query_lines.append(line)
        
        # Fix 2024 dates in parameters
        fixed_parameters = []
        for param in parameters:
            if param["keyword"] in ["param_start_date", "param_end_date"]:
                default_value = param["defaultSelection"]["values"]["values"][0]["value"]
                if "2024" in default_value:
                    if "start" in param["keyword"]:
                        param["defaultSelection"]["values"]["values"][0]["value"] = start_date
                    else:
                        param["defaultSelection"]["values"]["values"][0]["value"] = end_date
            fixed_parameters.append(param)
        
        # Create dataset
        dataset = {
            "name": dataset_name,
            "displayName": display_name,
            "queryLines": fixed_query_lines,
            "parameters": fixed_parameters
        }
        datasets.append(dataset)
        
        # Create parameter queries for this dataset
        for param in fixed_parameters:
            param_name = param["keyword"]
            query_name = f"parameter_dashboards/dbv4/datasets/{dataset_name}_{param_name}"
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
                "name": "dbv4_page",
                "displayName": "Cost & Usage Overview",
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
                                "values": [{"value": start_date if "start" in widget_name else end_date}]
                            }
                        }
                    },
                    "frame": {
                        "showTitle": True,
                        "title": "Start Date" if "start" in widget_name else "End Date"
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
                        },
                        "frame": {
                            "showTitle": True,
                            "title": "Workspace"
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
                "cost_breakdown_usage_unit": "dataset_003", 
                "top_compute_skus": "dataset_005",
                "top_jobs_pipelines": "dataset_006",
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
            
            # Define field names based on dataset - using actual column names from SQL
            field_mapping = {
                "dataset_001": ["days_analyzed", "total_cost_usd", "avg_daily_cost", "total_usage_quantity", "workspaces_analyzed"],
                "dataset_003": ["usage_unit", "workload_type", "total_cost", "total_quantity"],
                "dataset_004": ["workload_type", "total_cost", "total_usage"],
                "dataset_005": ["compute_type", "workload_type", "total_cost", "total_usage"],
                "dataset_006": ["job_pipeline_id", "job_name", "workload_type", "total_cost", "total_usage"],
                "dataset_007": ["workspace_id", "job_pipeline_id", "job_name", "workload_type", "cost"]
            }
            
            field_names = field_mapping.get(dataset_name, ["data"])
            
            # Create main query with fields array
            main_query = {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": [
                        {
                            "name": field_name,
                            "expression": f"`{field_name}`"
                        } for field_name in field_names
                    ],
                    "disaggregated": False
                }
            }
            
            # Create column encodings for actual fields - minimal structure
            columns = []
            for i, field_name in enumerate(field_names):
                columns.append({
                    "fieldName": field_name
                })
            
            new_widget = {
                "name": widget_name,
                "displayName": target_dataset.get("displayName", widget_name),
                "queries": [main_query],
                "spec": {
                    "version": 1,
                    "widgetType": "table",
                    "encodings": {
                        "columns": columns
                    }
                },
                "frame": {
                    "showTitle": True,
                    "title": target_dataset.get("displayName", widget_name)
                }
            }
                
        elif widget_type == "line":
            # Line chart widget
            dataset_name = "dataset_002"  # daily_cost_trend
            
            target_dataset = dataset_mapping.get(dataset_name)
            if not target_dataset:
                print(f"⚠️  Warning: Dataset {dataset_name} not found for {widget_name}")
                continue
            
            # Create main query with fields array
            main_query = {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": [
                        {
                            "name": "date",
                            "expression": "`date`"
                        },
                        {
                            "name": "daily_cost",
                            "expression": "`daily_cost`"
                        }
                    ],
                    "disaggregated": False
                }
            }
            
            new_widget = {
                "name": widget_name,
                "displayName": target_dataset.get("displayName", widget_name),
                "queries": [main_query],
                "spec": {
                    "version": 3,
                    "widgetType": "line",
                    "encodings": {
                        "x": {
                            "fieldName": "date",
                            "scale": {
                                "type": "temporal"
                            },
                            "displayName": "Date"
                        },
                        "y": {
                            "fieldName": "daily_cost",
                            "scale": {
                                "type": "quantitative"
                            },
                            "format": {
                                "type": "number-plain",
                                "abbreviation": "compact",
                                "decimalPlaces": {
                                    "type": "max",
                                    "places": 2
                                }
                            },
                            "displayName": "Daily Cost"
                        }
                    }
                },
                "frame": {
                    "showTitle": True,
                    "title": target_dataset.get("displayName", widget_name)
                }
            }
            
        elif widget_type == "pie":
            # Pie chart widget
            dataset_name = "dataset_004"  # workload_distribution
            
            target_dataset = dataset_mapping.get(dataset_name)
            if not target_dataset:
                print(f"⚠️  Warning: Dataset {dataset_name} not found for {widget_name}")
                continue
            
            # Create main query with fields array
            main_query = {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": [
                        {
                            "name": "workload_type",
                            "expression": "`workload_type`"
                        },
                        {
                            "name": "total_cost",
                            "expression": "`total_cost`"
                        }
                    ],
                    "disaggregated": False
                }
            }
            
            new_widget = {
                "name": widget_name,
                "displayName": target_dataset.get("displayName", widget_name),
                "queries": [main_query],
                "spec": {
                    "version": 3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "workload_type",
                            "scale": {
                                "type": "ordinal"
                            },
                            "displayName": "Workload Type"
                        },
                        "y": {
                            "fieldName": "total_cost",
                            "scale": {
                                "type": "quantitative"
                            },
                            "format": {
                                "type": "number-plain",
                                "abbreviation": "compact",
                                "decimalPlaces": {
                                    "type": "max",
                                    "places": 2
                                }
                            },
                            "displayName": "Total Cost"
                        }
                    }
                },
                "frame": {
                    "showTitle": True,
                    "title": target_dataset.get("displayName", widget_name)
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
    output_path = "dbv4_out.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"\n💾 Dashboard saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    print(f"📊 Datasets: {len(datasets)}")
    print(f"📊 Widgets: {len(dashboard['pages'][0]['layout'])}")
    
    return output_path

if __name__ == "__main__":
    create_dbv4_dashboard()
