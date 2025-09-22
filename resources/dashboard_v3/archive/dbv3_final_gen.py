#!/usr/bin/env python3
"""
Dashboard V3 Final Generator - Fixed query structure
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any

class DashboardV3FinalGenerator:
    def __init__(self, catalog: str = "platform_observability", gold_schema: str = "plt_gold"):
        self.catalog = catalog
        self.gold_schema = gold_schema
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
    def read_sql_queries(self) -> Dict[str, Any]:
        """Read the SQL queries configuration"""
        queries_path = os.path.join(self.base_dir, "../dashboard_v2/dbv2_sql_queries.json")
        with open(queries_path, 'r') as f:
            return json.load(f)
    
    def update_default_dates(self, datasets: Dict[str, Any]) -> Dict[str, Any]:
        """Update default dates to last 30 days"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        default_start = start_date.strftime("%Y-%m-%dT00:00:00.000")
        default_end = end_date.strftime("%Y-%m-%dT00:00:00.000")
        
        print(f"📅 Using date range: {default_start} to {default_end}")
        
        # Update all datasets with new default dates
        for dataset_key, dataset in datasets.items():
            if "parameters" in dataset:
                for param in dataset["parameters"]:
                    if param["keyword"] == "param_start_date":
                        param["defaultSelection"]["values"]["values"][0]["value"] = default_start
                    elif param["keyword"] == "param_end_date":
                        param["defaultSelection"]["values"]["values"][0]["value"] = default_end
        
        return datasets
    
    def replace_placeholders(self, query_lines: List[str]) -> List[str]:
        """Replace catalog and schema placeholders in SQL query lines"""
        updated_query_lines = []
        for line in query_lines:
            line = line.replace("{catalog}", self.catalog)
            line = line.replace("{gold_schema}", self.gold_schema)
            updated_query_lines.append(line)
        return updated_query_lines
    
    def create_parameter_queries(self, datasets: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create parameter queries following LakeFlow pattern"""
        parameter_queries = []
        
        # Group parameters by name to create multiple queries per parameter
        param_groups = {}
        for dataset_key, dataset in datasets.items():
            dataset_name = dataset["name"]
            if "parameters" in dataset:
                for param in dataset["parameters"]:
                    param_name = param["keyword"]
                    if param_name not in param_groups:
                        param_groups[param_name] = []
                    param_groups[param_name].append(dataset_name)
        
        # Create parameter queries for each parameter group
        for param_name, dataset_names in param_groups.items():
            for dataset_name in dataset_names:
                query_name = f"parameter_dashboards/dbv3/datasets/{dataset_name}_{param_name}"
                
                parameter_query = {
                    "name": query_name,
                    "query": {
                        "datasetName": dataset_name,
                        "parameters": [
                            {
                                "name": param_name,
                                "keyword": param_name
                            }
                        ],
                        "disaggregated": False
                    }
                }
                parameter_queries.append(parameter_query)
        
        return parameter_queries
    
    def create_table_column(self, field_name: str, display_name: str, order: int = 0) -> Dict[str, Any]:
        """Create a table column following LakeFlow structure"""
        return {
            "fieldName": field_name,
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
            "order": order,
            "title": field_name,
            "allowSearch": False,
            "alignContent": "left",
            "allowHTML": True,
            "highlightLinks": False,
            "useMonospaceFont": False,
            "preserveWhitespace": False,
            "displayName": display_name
        }
    
    def create_line_encoding(self, x_field: str, y_field: str, color_field: str = None) -> Dict[str, Any]:
        """Create line chart encoding following LakeFlow structure"""
        encoding = {
            "x": {
                "fieldName": x_field,
                "scale": {"type": "temporal"},
                "displayName": "Date"
            },
            "y": {
                "fieldName": y_field,
                "scale": {"type": "quantitative"},
                "format": {
                    "type": "number-plain",
                    "abbreviation": "compact",
                    "decimalPlaces": {
                        "type": "max",
                        "places": 2
                    }
                },
                "displayName": y_field.replace("_", " ").title()
            }
        }
        
        if color_field:
            encoding["color"] = {
                "fieldName": color_field,
                "scale": {"type": "categorical"},
                "displayName": color_field.replace("_", " ").title()
            }
        
        return encoding
    
    def create_bar_encoding(self, x_field: str, y_field: str, color_field: str = None) -> Dict[str, Any]:
        """Create bar chart encoding following LakeFlow structure"""
        encoding = {
            "x": {
                "fieldName": x_field,
                "scale": {"type": "ordinal"},
                "displayName": x_field.replace("_", " ").title()
            },
            "y": {
                "fieldName": y_field,
                "scale": {"type": "quantitative"},
                "format": {
                    "type": "number-plain",
                    "abbreviation": "compact",
                    "decimalPlaces": {
                        "type": "max",
                        "places": 2
                    }
                },
                "displayName": y_field.replace("_", " ").title()
            }
        }
        
        if color_field:
            encoding["color"] = {
                "fieldName": color_field,
                "scale": {"type": "categorical"},
                "displayName": color_field.replace("_", " ").title()
            }
        
        return encoding
    
    def create_pie_encoding(self, theta_field: str, color_field: str) -> Dict[str, Any]:
        """Create pie chart encoding following LakeFlow structure"""
        return {
            "theta": {
                "fieldName": theta_field,
                "scale": {"type": "quantitative"},
                "format": {
                    "type": "number-plain",
                    "abbreviation": "compact",
                    "decimalPlaces": {
                        "type": "max",
                        "places": 2
                    }
                },
                "displayName": theta_field.replace("_", " ").title()
            },
            "color": {
                "fieldName": color_field,
                "scale": {"type": "categorical"},
                "displayName": color_field.replace("_", " ").title()
            }
        }
    
    def generate_dashboard(self) -> str:
        """Generate the complete dashboard following LakeFlow structure"""
        print("🚀 Starting Dashboard V3 Final Generation...")
        print(f"📊 Using catalog: {self.catalog}")
        print(f"📊 Using gold schema: {self.gold_schema}")
        
        # Read SQL queries
        print("📖 Reading SQL queries...")
        sql_data = self.read_sql_queries()
        datasets = self.update_default_dates(sql_data["datasets"])
        
        # Convert datasets to list format
        dataset_list = []
        for dataset_key, dataset_info in datasets.items():
            dataset_entry = {
                "name": dataset_info["name"],
                "displayName": dataset_info["displayName"],
                "queryLines": self.replace_placeholders(dataset_info["queryLines"])
            }
            
            # Add parameters if they exist
            if "parameters" in dataset_info and dataset_info["parameters"]:
                dataset_entry["parameters"] = dataset_info["parameters"]
            
            dataset_list.append(dataset_entry)
        
        # Create parameter queries
        parameter_queries = self.create_parameter_queries(datasets)
        
        # Calculate default dates
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        default_start = start_date.strftime("%Y-%m-%dT00:00:00.000")
        default_end = end_date.strftime("%Y-%m-%dT00:00:00.000")
        
        # Create dashboard following LakeFlow structure
        dashboard = {
            "datasets": dataset_list,
            "pages": [
                {
                    "name": "dbv3_page_001",
                    "displayName": "Cost & Usage Overview",
                    "layout": [
                        # Date Filters
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
                                                "values": [{"value": default_start}]
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
                                                "values": [{"value": default_end}]
                                            }
                                        }
                                    }
                                }
                            },
                            "position": {"x": 2, "y": 0, "width": 2, "height": 1}
                        },
                        {
                            "widget": {
                                "name": "filter_workspace",
                                "queries": [
                                    {
                                        "name": "workspace_query",
                                        "query": {
                                            "datasetName": "dataset_008",
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
                                                "queryName": "workspace_query"
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
                        # Data Widgets
                        {
                            "widget": {
                                "name": "cost_summary_kpis",
                                "queries": [
                                    {
                                        "name": "dataset_001",
                                        "query": {
                                            "datasetName": "dataset_001",
                                            "disaggregated": False
                                        }
                                    }
                                ],
                                "spec": {
                                    "version": 1,
                                    "widgetType": "table",
                                    "encodings": {
                                        "columns": [
                                            self.create_table_column("days_analyzed", "Days Analyzed", 0),
                                            self.create_table_column("total_cost_usd", "Total Cost (USD)", 1),
                                            self.create_table_column("avg_daily_cost", "Avg Daily Cost", 2),
                                            self.create_table_column("total_usage_quantity", "Total Usage", 3),
                                            self.create_table_column("workspaces_analyzed", "Workspaces", 4),
                                            self.create_table_column("workload_types", "Workload Types", 5),
                                            self.create_table_column("compute_types", "Compute Types", 6)
                                        ]
                                    }
                                }
                            },
                            "position": {"x": 0, "y": 1, "width": 6, "height": 2}
                        },
                        {
                            "widget": {
                                "name": "daily_cost_trend",
                                "queries": [
                                    {
                                        "name": "dataset_002",
                                        "query": {
                                            "datasetName": "dataset_002",
                                            "disaggregated": False
                                        }
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "line",
                                    "encodings": self.create_line_encoding("date", "daily_cost", "workload_type")
                                }
                            },
                            "position": {"x": 0, "y": 3, "width": 6, "height": 3}
                        },
                        {
                            "widget": {
                                "name": "cost_breakdown_by_usage_unit",
                                "queries": [
                                    {
                                        "name": "dataset_003",
                                        "query": {
                                            "datasetName": "dataset_003",
                                            "disaggregated": False
                                        }
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "bar",
                                    "encodings": self.create_bar_encoding("usage_unit", "total_cost", "workload_type")
                                }
                            },
                            "position": {"x": 0, "y": 6, "width": 6, "height": 3}
                        },
                        {
                            "widget": {
                                "name": "workload_type_distribution",
                                "queries": [
                                    {
                                        "name": "dataset_004",
                                        "query": {
                                            "datasetName": "dataset_004",
                                            "disaggregated": False
                                        }
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "pie",
                                    "encodings": self.create_pie_encoding("total_cost", "workload_type")
                                }
                            },
                            "position": {"x": 0, "y": 9, "width": 3, "height": 3}
                        },
                        {
                            "widget": {
                                "name": "top_compute_skus",
                                "queries": [
                                    {
                                        "name": "dataset_005",
                                        "query": {
                                            "datasetName": "dataset_005",
                                            "disaggregated": False
                                        }
                                    }
                                ],
                                "spec": {
                                    "version": 1,
                                    "widgetType": "table",
                                    "encodings": {
                                        "columns": [
                                            self.create_table_column("compute_type", "Compute Type", 0),
                                            self.create_table_column("workload_type", "Workload Type", 1),
                                            self.create_table_column("total_cost", "Total Cost", 2),
                                            self.create_table_column("total_usage", "Total Usage", 3),
                                            self.create_table_column("workspace_count", "Workspaces", 4)
                                        ]
                                    }
                                }
                            },
                            "position": {"x": 3, "y": 9, "width": 3, "height": 3}
                        }
                    ]
                }
            ]
        }
        
        # Write output file
        output_path = os.path.join(self.base_dir, "dbv3_final_out.lvdash.json")
        print(f"💾 Writing dashboard to {output_path}...")
        with open(output_path, 'w') as f:
            json.dump(dashboard, f, indent=2)
        
        # Get file size
        file_size_kb = os.path.getsize(output_path) / 1024
        
        print("✅ Dashboard V3 final generation completed successfully!")
        print(f"📊 Output file: {output_path}")
        print("🎯 Ready to import into Databricks!")
        print()
        print("📈 Generation Statistics:")
        print(f"   datasets_count: {len(dashboard['datasets'])}")
        print(f"   pages_count: {len(dashboard['pages'])}")
        print(f"   widgets_count: {sum(len(page['layout']) for page in dashboard['pages'])}")
        print(f"   file_size_kb: {file_size_kb:.2f}")
        print(f"   generated_at: {datetime.now().isoformat()}")
        print(f"   catalog: {self.catalog}")
        print(f"   gold_schema: {self.gold_schema}")
        
        return output_path

def main():
    """Main function"""
    generator = DashboardV3FinalGenerator()
    generator.generate_dashboard()

if __name__ == "__main__":
    main()
