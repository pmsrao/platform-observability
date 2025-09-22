#!/usr/bin/env python3
"""
Create an exact copy of LakeFlow dashboard structure
"""

import json
import os

def create_exact_lakeflow_copy():
    """Create dashboard that exactly matches LakeFlow structure"""
    
    # Create datasets with queryLines (like LakeFlow)
    datasets = [
        {
            "name": "workspace_filter_dataset",
            "displayName": "Workspace Filter Data",
            "queryLines": [
                "SELECT DISTINCT w.workspace_id",
                "FROM platform_observability.plt_gold.gld_fact_billing_usage f",
                "JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key",
                "WHERE f.usage_start_time >= :param_start_date",
                "  AND f.usage_start_time <= :param_end_date",
                "UNION ALL",
                "SELECT '<ALL_WORKSPACES>' as workspace_id"
            ],
            "parameters": [
                {
                    "displayName": "param_start_date",
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
                    "displayName": "param_end_date",
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
                "  COUNT(DISTINCT f.date_key) as days_analyzed,",
                "  ROUND(SUM(f.usage_cost), 2) as total_cost_usd,",
                "  ROUND(AVG(f.usage_cost), 2) as avg_daily_cost,",
                "  ROUND(SUM(f.usage_quantity), 2) as total_usage_quantity,",
                "  COUNT(DISTINCT f.workspace_key) as workspaces_analyzed,",
                "  COUNT(DISTINCT f.billing_origin_product) as workload_types,",
                "  COUNT(DISTINCT f.sku_key) as compute_types",
                "FROM platform_observability.plt_gold.gld_fact_billing_usage f",
                "WHERE f.usage_start_time >= :param_start_date",
                "  AND f.usage_start_time <= :param_end_date",
                "  AND IF(:param_workspace = '<ALL_WORKSPACES>', true, f.workspace_key IN (",
                "    SELECT w.workspace_key FROM platform_observability.plt_gold.gld_dim_workspace w",
                "    WHERE w.workspace_id = :param_workspace",
                "  ))"
            ],
            "parameters": [
                {
                    "displayName": "param_start_date",
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
                    "displayName": "param_end_date",
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
                    "displayName": "param_workspace",
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
            query_name = f"parameter_dashboards/exact_lakeflow_copy/datasets/{dataset['name']}_{param_name}"
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
                "name": "exact_lakeflow_page",
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
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "cost_summary_dataset",
                                        "fields": [
                                            {
                                                "name": "days_analyzed",
                                                "expression": "`days_analyzed`"
                                            },
                                            {
                                                "name": "total_cost_usd",
                                                "expression": "`total_cost_usd`"
                                            },
                                            {
                                                "name": "avg_daily_cost",
                                                "expression": "`avg_daily_cost`"
                                            },
                                            {
                                                "name": "total_usage_quantity",
                                                "expression": "`total_usage_quantity`"
                                            },
                                            {
                                                "name": "workspaces_analyzed",
                                                "expression": "`workspaces_analyzed`"
                                            },
                                            {
                                                "name": "workload_types",
                                                "expression": "`workload_types`"
                                            },
                                            {
                                                "name": "compute_types",
                                                "expression": "`compute_types`"
                                            }
                                        ],
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
    output_path = "exact_lakeflow_copy.lvdash.json"
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
