#!/usr/bin/env python3
"""
Comprehensive Platform Observability Dashboard Generator

This script generates a comprehensive Databricks dashboard JSON file based on
the documented use cases in 10-insights-and-use-cases.md, covering:

- Finance & Cost Management Teams
- Platform Engineers & DevOps  
- Data Engineers & Analysts
- Business Stakeholders & Product Owners

The dashboard includes 6 tabs with 20+ widgets covering all documented use cases.
"""

import json
import sys
from pathlib import Path

def load_json_file(file_path):
    """Load JSON file with error handling."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {file_path} - {e}")
        sys.exit(1)

def create_dataset_entry(dataset_id, dataset_info):
    """Create a dataset entry for the dashboard."""
    return {
        "name": dataset_id,
        "displayName": dataset_info["displayName"],
        "queryLines": dataset_info["queryLines"]
    }

def create_filter_widget(widget_name, title, dataset_name, field_name, widget_type="filter-single-select"):
    """Create a filter widget."""
    return {
        "widget": {
            "name": widget_name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {
                                "name": field_name,
                                "expression": f"`{field_name}`"
                            }
                        ],
                        "disaggregated": True
                    }
                }
            ],
            "spec": {
                "version": 2,
                "widgetType": widget_type,
                "encodings": {
                    "fields": [
                        {
                            "fieldName": field_name,
                            "displayName": field_name,
                            "queryName": "main_query"
                        }
                    ]
                },
                "selection": {
                    "defaultSelection": {
                        "values": {
                            "dataType": "STRING",
                            "values": [
                                {
                                    "value": "<ALL>"
                                }
                            ]
                        }
                    }
                },
                "frame": {
                    "showTitle": True,
                    "title": title
                }
            }
        }
    }

def create_table_widget(widget_name, title, dataset_name, fields):
    """Create a table widget."""
    columns = []
    for i, field in enumerate(fields):
        columns.append({
            "fieldName": field,
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
            "order": i
        })
    
    return {
        "widget": {
            "name": widget_name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {
                                "name": field,
                                "expression": f"`{field}`"
                            } for field in fields
                        ],
                        "disaggregated": True
                    }
                }
            ],
            "spec": {
                "version": 1,
                "widgetType": "table",
                "encodings": {
                    "columns": columns
                },
                "frame": {
                    "showTitle": True,
                    "title": title
                }
            }
        }
    }

def create_chart_widget(widget_name, title, dataset_name, x_field, y_field, chart_type="line"):
    """Create a chart widget (line, bar, etc.)."""
    return {
        "widget": {
            "name": widget_name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {
                                "name": x_field,
                                "expression": f"`{x_field}`"
                            },
                            {
                                "name": y_field,
                                "expression": f"`{y_field}`"
                            }
                        ],
                        "disaggregated": True
                    }
                }
            ],
            "spec": {
                "version": 3,
                "widgetType": chart_type,
                "encodings": {
                    "x": {
                        "fieldName": x_field,
                        "scale": {
                            "type": "temporal" if "date" in x_field.lower() else "ordinal"
                        },
                        "axis": {
                            "title": x_field.replace("_", " ").title()
                        },
                        "displayName": x_field.replace("_", " ").title()
                    },
                    "y": {
                        "fieldName": y_field,
                        "scale": {
                            "type": "quantitative"
                        },
                        "axis": {
                            "title": y_field.replace("_", " ").title()
                        },
                        "displayName": y_field.replace("_", " ").title()
                    }
                },
                "frame": {
                    "showTitle": True,
                    "title": title
                }
            }
        }
    }

def create_counter_widget(widget_name, title, dataset_name, value_field, format_type="number", style_config=None):
    """Create a counter widget for KPI displays."""
    spec = {
        "version": 2,
        "widgetType": "counter",
        "encodings": {
            "value": {
                "fieldName": value_field,
                "displayName": value_field
            }
        },
        "frame": {
            "showTitle": True,
            "title": title
        }
    }
    
    # Add format if specified
    if format_type:
        spec["encodings"]["value"]["format"] = {
            "type": format_type
        }
    
    # Add style if specified
    if style_config:
        spec["encodings"]["value"]["style"] = style_config
    
    return {
        "widget": {
            "name": widget_name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {
                                "name": value_field,
                                "expression": f"`{value_field}`"
                            }
                        ],
                        "disaggregated": True
                    }
                }
            ],
            "spec": spec
        }
    }

def generate_comprehensive_dashboard():
    """Generate the comprehensive dashboard JSON."""
    
    # Load SQL queries and template
    queries_file = Path("../resources/dashboard/comprehensive_dashboard_sql_queries.json")
    template_file = Path("../resources/dashboard/comprehensive_dashboard_template.json")
    
    queries_data = load_json_file(queries_file)
    template_data = load_json_file(template_file)
    
    # Create datasets
    datasets = []
    for dataset_id, dataset_info in queries_data["datasets"].items():
        datasets.append(create_dataset_entry(dataset_id, dataset_info))
    
    # Define page layouts with comprehensive widgets
    pages = [
        {
            "name": "overview-page-001",
            "displayName": "Overview",
            "layout": [
                # Filters
                {
                    **create_filter_widget("filter_date_range", "Date Range", "dataset_007", "workspace_name", "filter-date-picker"),
                    "position": {"x": 0, "y": 0, "width": 3, "height": 1}
                },
                {
                    **create_filter_widget("filter_workspace", "Workspace", "dataset_007", "workspace_name"),
                    "position": {"x": 3, "y": 0, "width": 3, "height": 1}
                },
                # KPI Counter widgets
                {
                    **create_counter_widget("overview_total_cost_kpi", "Total Cost (30 days)", "dataset_019", 
                                          "total_cost_usd", "number-currency"),
                    "position": {"x": 0, "y": 1, "width": 3, "height": 2}
                },
                {
                    **create_counter_widget("overview_active_workspaces_kpi", "Active Workspaces", "dataset_019", 
                                          "active_workspaces", "number"),
                    "position": {"x": 3, "y": 1, "width": 3, "height": 2}
                },
                {
                    **create_counter_widget("overview_active_entities_kpi", "Active Entities", "dataset_019", 
                                          "active_entities", "number"),
                    "position": {"x": 6, "y": 1, "width": 3, "height": 2}
                },
                {
                    **create_counter_widget("overview_avg_daily_cost_kpi", "Avg Daily Cost", "dataset_019", 
                                          "avg_daily_cost", "number-currency"),
                    "position": {"x": 9, "y": 1, "width": 3, "height": 2}
                },
                # Charts and tables
                {
                    **create_chart_widget("overview_cost_trend", "Cost Trend (30 Days)", "dataset_002", 
                                        "date_key", "daily_cost_usd", "line"),
                    "position": {"x": 0, "y": 3, "width": 6, "height": 5}
                },
                {
                    **create_table_widget("overview_total_cost_summary", "Cost Summary Details", "dataset_001", 
                                        ["metric", "value", "unit"]),
                    "position": {"x": 6, "y": 3, "width": 6, "height": 5}
                },
                {
                    **create_table_widget("overview_top_cost_centers", "Top Cost Centers", "dataset_003", 
                                        ["cost_center", "line_of_business", "total_cost_usd", "active_entities", "cost_per_entity"]),
                    "position": {"x": 0, "y": 8, "width": 12, "height": 5}
                }
            ],
            "pageType": "PAGE_TYPE_CANVAS"
        },
        {
            "name": "finance-page-002",
            "displayName": "Finance & Cost Management",
            "layout": [
                # Cost allocation
                {
                    **create_table_widget("finance_cost_allocation", "Cost Allocation by Business Unit", "dataset_008", 
                                        ["line_of_business", "cost_center", "department", "total_cost_usd", "active_entities", "cost_per_entity"]),
                    "position": {"x": 0, "y": 0, "width": 12, "height": 6}
                },
                # Budget tracking
                {
                    **create_chart_widget("finance_budget_tracking", "Budget vs Actual Tracking", "dataset_009", 
                                        "month", "actual_cost", "line"),
                    "position": {"x": 0, "y": 6, "width": 6, "height": 6}
                },
                # Monthly breakdown
                {
                    **create_table_widget("finance_monthly_breakdown", "Monthly Cost Breakdown", "dataset_005", 
                                        ["month", "cost_center", "line_of_business", "total_cost_usd", "active_entities", "cost_per_entity"]),
                    "position": {"x": 6, "y": 6, "width": 6, "height": 6}
                },
                # Cost center analysis
                {
                    **create_table_widget("finance_cost_center_analysis", "Cost Center Analysis", "dataset_018", 
                                        ["cost_center", "line_of_business", "department", "month", "monthly_cost", "cost_change_pct"]),
                    "position": {"x": 0, "y": 12, "width": 12, "height": 6}
                }
            ],
            "pageType": "PAGE_TYPE_CANVAS"
        },
        {
            "name": "platform-page-003",
            "displayName": "Platform Engineering",
            "layout": [
                # Runtime modernization
                {
                    **create_table_widget("platform_runtime_modernization", "Runtime Modernization Opportunities", "dataset_010", 
                                        ["upgrade_priority", "version_category", "cluster_count", "avg_runtime_age", "affected_runtimes"]),
                    "position": {"x": 0, "y": 0, "width": 12, "height": 6}
                },
                # Cluster optimization
                {
                    **create_table_widget("platform_cluster_optimization", "Cluster Sizing Optimization", "dataset_011", 
                                        ["cluster_name", "worker_node_type_category", "min_autoscale_workers", "max_autoscale_workers", "avg_duration", "total_cost"]),
                    "position": {"x": 0, "y": 6, "width": 6, "height": 6}
                },
                # Node type analysis
                {
                    **create_chart_widget("platform_node_type_analysis", "Node Type Analysis", "dataset_012", 
                                        "worker_node_type_category", "total_cost", "bar"),
                    "position": {"x": 6, "y": 6, "width": 6, "height": 6}
                },
                # Runtime health
                {
                    **create_chart_widget("platform_runtime_health", "Runtime Health Distribution", "dataset_004", 
                                        "version_category", "total_clusters", "bar"),
                    "position": {"x": 0, "y": 12, "width": 6, "height": 6}
                },
                # Workflow hierarchy
                {
                    **create_table_widget("platform_workflow_hierarchy", "Workflow Hierarchy Cost", "dataset_013", 
                                        ["workflow_level", "parent_workflow_name", "line_of_business", "entity_count", "total_cost_usd", "cost_per_entity"]),
                    "position": {"x": 6, "y": 12, "width": 6, "height": 6}
                }
            ],
            "pageType": "PAGE_TYPE_CANVAS"
        },
        {
            "name": "data-quality-page-004",
            "displayName": "Data Quality & Governance",
            "layout": [
                # Tag quality analysis
                {
                    **create_table_widget("data_quality_tag_analysis", "Tag Quality Analysis", "dataset_014", 
                                        ["workspace_name", "total_records", "cost_center_tagged", "lob_tagged", "cost_center_coverage_pct", "lob_coverage_pct"]),
                    "position": {"x": 0, "y": 0, "width": 12, "height": 8}
                },
                # Usage patterns
                {
                    **create_chart_widget("data_quality_usage_patterns", "Usage Patterns by Hour", "dataset_015", 
                                        "hour_of_day", "usage_count", "line"),
                    "position": {"x": 0, "y": 8, "width": 6, "height": 6}
                },
                # Job performance
                {
                    **create_table_widget("data_quality_job_performance", "Job Performance Analysis", "dataset_006", 
                                        ["job_name", "workspace_name", "total_runs", "avg_duration_hours", "total_cost_usd", "cost_per_run"]),
                    "position": {"x": 6, "y": 8, "width": 6, "height": 6}
                }
            ],
            "pageType": "PAGE_TYPE_CANVAS"
        },
        {
            "name": "business-page-005",
            "displayName": "Business Intelligence",
            "layout": [
                # Business unit performance
                {
                    **create_table_widget("business_unit_performance", "Business Unit Performance", "dataset_016", 
                                        ["line_of_business", "use_case", "active_entities", "total_cost_usd", "total_usage", "cost_per_entity"]),
                    "position": {"x": 0, "y": 0, "width": 12, "height": 6}
                },
                # Project cost tracking
                {
                    **create_table_widget("business_project_tracking", "Project Cost Tracking", "dataset_017", 
                                        ["month", "line_of_business", "sub_project", "project_cost_usd", "active_entities", "active_workspaces"]),
                    "position": {"x": 0, "y": 6, "width": 12, "height": 6}
                }
            ],
            "pageType": "PAGE_TYPE_CANVAS"
        },
        {
            "name": "operations-page-006",
            "displayName": "Operations & Performance",
            "layout": [
                # Runtime health
                {
                    **create_chart_widget("operations_runtime_health", "Runtime Health - Cluster Distribution", "dataset_004", 
                                        "version_category", "total_clusters", "bar"),
                    "position": {"x": 0, "y": 0, "width": 6, "height": 6}
                },
                # Job performance
                {
                    **create_table_widget("operations_job_performance", "Job Performance Analysis", "dataset_006", 
                                        ["job_name", "workspace_name", "total_runs", "avg_duration_hours", "total_cost_usd", "cost_per_run"]),
                    "position": {"x": 6, "y": 0, "width": 6, "height": 6}
                },
                # Usage patterns
                {
                    **create_chart_widget("operations_usage_patterns", "Usage Patterns by Environment", "dataset_015", 
                                        "environment", "total_usage", "bar"),
                    "position": {"x": 0, "y": 6, "width": 6, "height": 6}
                },
                # Cost trend
                {
                    **create_chart_widget("operations_cost_trend", "Cost Trend Analysis", "dataset_002", 
                                        "date_key", "daily_cost_usd", "line"),
                    "position": {"x": 6, "y": 6, "width": 6, "height": 6}
                }
            ],
            "pageType": "PAGE_TYPE_CANVAS"
        }
    ]
    
    # Create the final dashboard structure
    dashboard = {
        "datasets": datasets,
        "pages": pages,
        "uiSettings": template_data["uiSettings"]
    }
    
    # Save the comprehensive dashboard
    output_path = Path("../resources/dashboard/platform_observability_comprehensive_dashboard.lvdash.json")
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    print(f"✅ Comprehensive dashboard generated successfully!")
    print(f"📁 Output file: {output_path}")
    print(f"📊 Dashboard includes:")
    print(f"   - {len(datasets)} datasets")
    print(f"   - {len(pages)} pages")
    print(f"   - 20+ widgets covering all documented use cases")
    print(f"   - Finance & Cost Management")
    print(f"   - Platform Engineering & DevOps")
    print(f"   - Data Quality & Governance")
    print(f"   - Business Intelligence")
    print(f"   - Operations & Performance")
    print(f"   - Overview with filters")
    
    return output_path

if __name__ == "__main__":
    generate_comprehensive_dashboard()
