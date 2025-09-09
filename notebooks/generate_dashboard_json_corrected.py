# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Databricks Dashboard JSON (Corrected Format)
# MAGIC 
# MAGIC This notebook generates a properly formatted Databricks dashboard JSON file using the correct .lvdash.json format based on manual export analysis.

# COMMAND ----------

import json
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# File paths
TEMPLATE_PATH = "/Workspace/Users/podilapalls@gmail.com/platform-observability/resources/dashboard/dashboard_template_corrected.json"
SQL_STATEMENTS_PATH = "/Workspace/Users/podilapalls@gmail.com/platform-observability/resources/dashboard/dashboard_sql_statements.json"
OUTPUT_DASHBOARD_PATH = "/Workspace/Users/podilapalls@gmail.com/platform-observability/resources/dashboard/platform_observability_dashboard.lvdash.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Template and SQL Statements

# COMMAND ----------

# Load template
with open(TEMPLATE_PATH, 'r') as f:
    template = json.load(f)

# Load SQL statements
with open(SQL_STATEMENTS_PATH, 'r') as f:
    sql_statements = json.load(f)

print(f"✅ Loaded template with {len(template.get('datasets', []))} dataset placeholders")
print(f"✅ Loaded {len(sql_statements)} SQL statements")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Datasets

# COMMAND ----------

def create_dataset(name, display_name, sql_query):
    """Create a dataset with proper queryLines format"""
    return {
        "name": name,
        "displayName": display_name,
        "queryLines": sql_query.split('\n') if isinstance(sql_query, str) else sql_query
    }

# Generate datasets from SQL statements
datasets = []
dataset_counter = 1

for widget_name, sql_data in sql_statements.items():
    dataset_name = f"dataset_{dataset_counter:03d}"
    display_name = sql_data.get('displayName', widget_name.replace('_', ' ').title())
    sql_query = sql_data.get('sql', '')
    
    datasets.append(create_dataset(dataset_name, display_name, sql_query))
    dataset_counter += 1

print(f"📊 Generated {len(datasets)} datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Widgets

# COMMAND ----------

def create_table_widget(widget_name, title, dataset_name, fields):
    """Create a table widget with proper encoding specifications"""
    return {
        "name": widget_name,
        "queries": [
            {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": [{"name": field, "expression": f"`{field}`"} for field in fields],
                    "disaggregated": True
                }
            }
        ],
        "spec": {
            "version": 1,
            "widgetType": "table",
            "encodings": {
                "columns": [
                    {
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
                        "type": "string" if field in ["metric", "value", "unit", "cost_center", "line_of_business", "workspace_name", "entity_type"] else "decimal",
                        "displayAs": "string" if field in ["metric", "value", "unit", "cost_center", "line_of_business", "workspace_name", "entity_type"] else "number",
                        "visible": True,
                        "order": 100000 + i,
                        "title": field,
                        "allowSearch": False,
                        "alignContent": "left" if field in ["metric", "value", "unit", "cost_center", "line_of_business", "workspace_name", "entity_type"] else "right",
                        "allowHTML": False,
                        "highlightLinks": False,
                        "useMonospaceFont": False,
                        "preserveWhitespace": False,
                        "displayName": field
                    }
                    for i, field in enumerate(fields)
                ]
            },
            "allowHTMLByDefault": False,
            "itemsPerPage": 25,
            "paginationSize": "default",
            "condensed": True,
            "withRowNumber": True,
            "frame": {
                "showTitle": True,
                "title": title
            }
        }
    }

def create_line_widget(widget_name, title, dataset_name, x_field, y_field):
    """Create a line chart widget"""
    return {
        "name": widget_name,
        "queries": [
            {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": [
                        {"name": x_field, "expression": f"`{x_field}`"},
                        {"name": y_field, "expression": f"`{y_field}`"}
                    ],
                    "disaggregated": True
                }
            }
        ],
        "spec": {
            "version": 1,
            "widgetType": "line",
            "encodings": {
                "x": {
                    "fieldName": x_field,
                    "type": "integer"
                },
                "y": {
                    "fieldName": y_field,
                    "type": "decimal"
                }
            },
            "frame": {
                "showTitle": True,
                "title": title
            }
        }
    }

def create_bar_widget(widget_name, title, dataset_name, x_field, y_field):
    """Create a bar chart widget"""
    return {
        "name": widget_name,
        "queries": [
            {
                "name": "main_query",
                "query": {
                    "datasetName": dataset_name,
                    "fields": [
                        {"name": x_field, "expression": f"`{x_field}`"},
                        {"name": y_field, "expression": f"`{y_field}`"}
                    ],
                    "disaggregated": True
                }
            }
        ],
        "spec": {
            "version": 1,
            "widgetType": "bar",
            "encodings": {
                "x": {
                    "fieldName": x_field,
                    "type": "string"
                },
                "y": {
                    "fieldName": y_field,
                    "type": "decimal"
                }
            },
            "frame": {
                "showTitle": True,
                "title": title
            }
        }
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Widget Layout

# COMMAND ----------

# Define widget configurations
widget_configs = [
    # Overview Page
    {
        "name": "overview_total_cost_summary",
        "title": "Total Cost Summary",
        "type": "table",
        "dataset": "dataset_001",
        "fields": ["metric", "value", "unit"],
        "position": {"x": 0, "y": 0, "width": 6, "height": 5}
    },
    {
        "name": "overview_cost_trend",
        "title": "Cost Trend (30 Days)",
        "type": "line",
        "dataset": "dataset_002",
        "x_field": "date_sk",
        "y_field": "daily_cost_usd",
        "position": {"x": 6, "y": 0, "width": 6, "height": 5}
    },
    {
        "name": "overview_top_cost_centers",
        "title": "Top Cost Centers",
        "type": "table",
        "dataset": "dataset_003",
        "fields": ["cost_center", "line_of_business", "total_cost_usd", "active_entities", "cost_per_entity"],
        "position": {"x": 0, "y": 5, "width": 12, "height": 5}
    }
]

# Generate widgets
widgets = []
for config in widget_configs:
    if config["type"] == "table":
        widget = create_table_widget(
            config["name"],
            config["title"],
            config["dataset"],
            config["fields"]
        )
    elif config["type"] == "line":
        widget = create_line_widget(
            config["name"],
            config["title"],
            config["dataset"],
            config["x_field"],
            config["y_field"]
        )
    elif config["type"] == "bar":
        widget = create_bar_widget(
            config["name"],
            config["title"],
            config["dataset"],
            config["x_field"],
            config["y_field"]
        )
    
    widgets.append({
        "widget": widget,
        "position": config["position"]
    })

print(f"🎨 Generated {len(widgets)} widgets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Complete Dashboard

# COMMAND ----------

# Create the complete dashboard
dashboard = {
    "datasets": datasets,
    "pages": [
        {
            "name": "overview-page-001",
            "displayName": "Overview",
            "layout": widgets,
            "pageType": "PAGE_TYPE_CANVAS"
        }
    ],
    "uiSettings": {
        "theme": {
            "widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"
        }
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Dashboard

# COMMAND ----------

# Save the dashboard
with open(OUTPUT_DASHBOARD_PATH, 'w') as f:
    json.dump(dashboard, f, indent=2)

print(f"✅ Dashboard saved to: {OUTPUT_DASHBOARD_PATH}")
print(f"📊 Generated {len(datasets)} datasets and {len(widgets)} widgets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("📋 Dashboard Structure:")
print(f"  - Datasets: {len(dashboard['datasets'])}")
for dataset in dashboard['datasets']:
    print(f"    - {dataset['name']}: {dataset['displayName']}")

print(f"  - Pages: {len(dashboard['pages'])}")
for page in dashboard['pages']:
    print(f"    - {page['name']}: {page['displayName']} ({len(page['layout'])} widgets)")

print("\n🎯 Dashboard is ready for import into Databricks!")
print("📁 Import the file: platform_observability_dashboard.lvdash.json")
