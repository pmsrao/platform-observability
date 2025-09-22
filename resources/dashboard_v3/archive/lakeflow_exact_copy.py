#!/usr/bin/env python3
"""
Create an exact copy of LakeFlow dashboard structure but with our data
"""

import json
import os
from datetime import datetime, timedelta

def create_exact_lakeflow_copy():
    """Create dashboard that exactly matches LakeFlow structure"""
    
    # Load the LakeFlow dashboard
    lakeflow_path = "../dashboard/LakeFlow System Tables Dashboard v0.1.lvdash.json"
    with open(lakeflow_path, 'r') as f:
        lakeflow = json.load(f)
    
    print("🔍 Analyzing LakeFlow structure...")
    
    # Extract the exact structure of one working widget from LakeFlow
    working_widget = None
    for page in lakeflow["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            if widget["spec"]["widgetType"] == "table":
                working_widget = widget
                break
        if working_widget:
            break
    
    if not working_widget:
        print("❌ Could not find a working table widget in LakeFlow")
        return
    
    print(f"✅ Found working widget: {working_widget['name']}")
    
    # Create our datasets
    datasets = [
        {
            "name": "workspace_filter_dataset",
            "displayName": "Workspace Filter Data",
            "queryLines": [
                "SELECT DISTINCT workspace_id",
                "FROM platform_observability.plt_gold.gld_fact_billing_usage",
                "WHERE usage_start_time >= :param_start_date",
                "  AND usage_start_time <= :param_end_date",
                "UNION ALL",
                "SELECT '<ALL_WORKSPACES>' as workspace_id"
            ],
            "parameters": [
                {
                    "displayName": "Start Date",
                    "keyword": "param_start_date",
                    "dataType": "DATE",
                    "defaultSelection": {
                        "values": {
                            "dataType": "DATE",
                            "values": [{"value": "2025-08-22T00:00:00.000"}]
                        }
                    }
                },
                {
                    "displayName": "End Date",
                    "keyword": "param_end_date",
                    "dataType": "DATE",
                    "defaultSelection": {
                        "values": {
                            "dataType": "DATE",
                            "values": [{"value": "2025-09-21T00:00:00.000"}]
                        }
                    }
                }
            ]
        },
        {
            "name": "cost_summary_dataset",
            "displayName": "Cost Summary",
            "queryLines": [
                "SELECT",
                "  COUNT(DISTINCT date_key) as days_analyzed,",
                "  ROUND(SUM(usage_cost), 2) as total_cost_usd,",
                "  ROUND(AVG(usage_cost), 2) as avg_daily_cost,",
                "  ROUND(SUM(usage_quantity), 2) as total_usage_quantity,",
                "  COUNT(DISTINCT workspace_key) as workspaces_analyzed,",
                "  COUNT(DISTINCT billing_origin_product) as workload_types,",
                "  COUNT(DISTINCT sku_key) as compute_types",
                "FROM platform_observability.plt_gold.gld_fact_billing_usage",
                "WHERE usage_start_time >= :param_start_date",
                "  AND usage_start_time <= :param_end_date",
                "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_key IN (",
                "    SELECT workspace_key FROM platform_observability.plt_gold.gld_dim_workspace",
                "    WHERE workspace_id = :param_workspace",
                "  ))"
            ],
            "parameters": [
                {
                    "displayName": "Start Date",
                    "keyword": "param_start_date",
                    "dataType": "DATE",
                    "defaultSelection": {
                        "values": {
                            "dataType": "DATE",
                            "values": [{"value": "2025-08-22T00:00:00.000"}]
                        }
                    }
                },
                {
                    "displayName": "End Date",
                    "keyword": "param_end_date",
                    "dataType": "DATE",
                    "defaultSelection": {
                        "values": {
                            "dataType": "DATE",
                            "values": [{"value": "2025-09-21T00:00:00.000"}]
                        }
                    }
                },
                {
                    "displayName": "Workspace",
                    "keyword": "param_workspace",
                    "dataType": "STRING",
                    "defaultSelection": {
                        "values": {
                            "dataType": "STRING",
                            "values": [{"value": "<ALL_WORKSPACES>"}]
                        }
                    }
                }
            ]
        }
    ]
    
    # Create parameter queries exactly like LakeFlow
    parameter_queries = []
    for dataset in datasets:
        for param in dataset.get("parameters", []):
            param_name = param["keyword"]
            query_name = f"parameter_dashboards/exact_copy/datasets/{dataset['name']}_{param_name}"
            parameter_queries.append({
                "name": query_name,
                "query": {
                    "datasetName": dataset["name"],
                    "parameters": [{"name": param_name, "keyword": param_name}],
                    "disaggregated": False
                }
            })
    
    # Create dashboard with exact LakeFlow structure
    dashboard = {
        "datasets": datasets,
        "pages": [
            {
                "name": "exact_copy_page",
                "displayName": "Exact LakeFlow Copy",
                "layout": [
                    # Date filters - exact LakeFlow structure
                    {
                        "widget": {
                            "name": "filter_start_date",
                            "queries": [q for q in parameter_queries if "param_start_date" in q["name"]],
                            "spec": {
                                "version": 2,
                                "widgetType": "filter-date-picker",
                                "encodings": {
                                    "fields": [
                                        {
                                            "parameterName": "param_start_date",
                                            "queryName": q["name"]
                                        } for q in parameter_queries if "param_start_date" in q["name"]
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
                        },
                        "position": {"x": 0, "y": 0, "width": 2, "height": 1}
                    },
                    {
                        "widget": {
                            "name": "filter_end_date",
                            "queries": [q for q in parameter_queries if "param_end_date" in q["name"]],
                            "spec": {
                                "version": 2,
                                "widgetType": "filter-date-picker",
                                "encodings": {
                                    "fields": [
                                        {
                                            "parameterName": "param_end_date",
                                            "queryName": q["name"]
                                        } for q in parameter_queries if "param_end_date" in q["name"]
                                    ]
                                },
                                "selection": {
                                    "defaultSelection": {
                                        "values": {
                                            "dataType": "DATE",
                                            "values": [{"value": "2025-09-21T00:00:00.000"}]
                                        }
                                    }
                                }
                            }
                        },
                        "position": {"x": 2, "y": 0, "width": 2, "height": 1}
                    },
                    # Workspace filter - exact LakeFlow structure
                    {
                        "widget": {
                            "name": "filter_workspace",
                            "queries": [
                                {
                                    "name": "workspace_main_query",
                                    "query": {
                                        "datasetName": "workspace_filter_dataset",
                                        "fields": [
                                            {
                                                "name": "workspace_id",
                                                "expression": "`workspace_id`"
                                            }
                                        ],
                                        "disaggregated": False
                                    }
                                }
                            ] + [q for q in parameter_queries if "param_workspace" in q["name"]],
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
                                        } for q in parameter_queries if "param_workspace" in q["name"]
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
                        },
                        "position": {"x": 4, "y": 0, "width": 2, "height": 1}
                    },
                    # Cost summary table - exact LakeFlow structure
                    {
                        "widget": {
                            "name": "cost_summary_table",
                            "queries": [
                                {
                                    "name": "cost_summary_query",
                                    "query": {
                                        "datasetName": "cost_summary_dataset",
                                        "disaggregated": False
                                    }
                                }
                            ],
                            "spec": {
                                "version": 1,
                                "widgetType": "table",
                                "encodings": {
                                    "columns": [
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
                                }
                            }
                        },
                        "position": {"x": 0, "y": 1, "width": 6, "height": 3}
                    }
                ]
            }
        ]
    }
    
    # Save the dashboard
    output_path = "lakeflow_exact_copy.lvdash.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    file_size_kb = os.path.getsize(output_path) / 1024
    print(f"💾 Exact LakeFlow copy saved to: {output_path}")
    print(f"📊 File size: {file_size_kb:.2f} KB")
    print(f"📊 Datasets: {len(datasets)}")
    print(f"📊 Widgets: 4 (3 filters + 1 table)")
    
    return output_path

if __name__ == "__main__":
    create_exact_lakeflow_copy()
