#!/usr/bin/env python3
"""
Simple Test Dashboard Generator - Fixed validation errors
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any

class SimpleTestFixedGenerator:
    def __init__(self, catalog: str = "platform_observability", gold_schema: str = "plt_gold"):
        self.catalog = catalog
        self.gold_schema = gold_schema
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
    def generate_simple_dashboard(self) -> str:
        """Generate a simple dashboard with just 2 widgets"""
        print("🚀 Generating Simple Test Dashboard (Fixed)...")
        
        # Calculate dates
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        default_start = start_date.strftime("%Y-%m-%dT00:00:00.000")
        default_end = end_date.strftime("%Y-%m-%dT00:00:00.000")
        
        print(f"📅 Using date range: {default_start} to {default_end}")
        
        # Simple dashboard structure with fixed validation issues
        dashboard = {
            "datasets": [
                {
                    "name": "simple_dataset_001",
                    "displayName": "Simple Cost Summary",
                    "queryLines": [
                        "SELECT ",
                        "    COUNT(*) as total_records,",
                        "    ROUND(SUM(usage_cost), 2) as total_cost,",
                        "    COUNT(DISTINCT workspace_key) as workspaces",
                        "FROM platform_observability.plt_gold.gld_fact_billing_usage ",
                        "WHERE usage_start_time >= :param_start_date ",
                        "  AND usage_start_time <= :param_end_date"
                    ],
                    "parameters": [
                        {
                            "displayName": "Start Date",
                            "keyword": "param_start_date",
                            "dataType": "DATE",
                            "defaultSelection": {
                                "values": {
                                    "dataType": "DATE",
                                    "values": [{"value": default_start}]
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
                                    "values": [{"value": default_end}]
                                }
                            }
                        }
                    ]
                },
                {
                    "name": "simple_dataset_002",
                    "displayName": "Simple Cost Trend",
                    "queryLines": [
                        "SELECT ",
                        "    date_key,",
                        "    ROUND(SUM(usage_cost), 2) as daily_cost",
                        "FROM platform_observability.plt_gold.gld_fact_billing_usage ",
                        "WHERE usage_start_time >= :param_start_date ",
                        "  AND usage_start_time <= :param_end_date ",
                        "GROUP BY date_key ",
                        "ORDER BY date_key"
                    ],
                    "parameters": [
                        {
                            "displayName": "Start Date",
                            "keyword": "param_start_date",
                            "dataType": "DATE",
                            "defaultSelection": {
                                "values": {
                                    "dataType": "DATE",
                                    "values": [{"value": default_start}]
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
                                    "values": [{"value": default_end}]
                                }
                            }
                        }
                    ]
                }
            ],
            "pages": [
                {
                    "name": "SimpleTestPage",
                    "displayName": "Simple Test Page",
                    "layout": [
                        {
                            "widget": {
                                "name": "filter_start_date",
                                "queries": [
                                    {
                                        "name": "parameter_dashboards/simple/datasets/simple_dataset_001_param_start_date",
                                        "query": {
                                            "datasetName": "simple_dataset_001",
                                            "parameters": [{"name": "param_start_date", "keyword": "param_start_date"}],
                                            "disaggregated": False
                                        }
                                    },
                                    {
                                        "name": "parameter_dashboards/simple/datasets/simple_dataset_002_param_start_date", 
                                        "query": {
                                            "datasetName": "simple_dataset_002",
                                            "parameters": [{"name": "param_start_date", "keyword": "param_start_date"}],
                                            "disaggregated": False
                                        }
                                    }
                                ],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "filter-date-picker",
                                    "encodings": {
                                        "fields": [
                                            {
                                                "parameterName": "param_start_date",
                                                "queryName": "parameter_dashboards/simple/datasets/simple_dataset_001_param_start_date"
                                            },
                                            {
                                                "parameterName": "param_start_date", 
                                                "queryName": "parameter_dashboards/simple/datasets/simple_dataset_002_param_start_date"
                                            }
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
                            "position": {"x": 0, "y": 0, "width": 3, "height": 1}
                        },
                        {
                            "widget": {
                                "name": "filter_end_date",
                                "queries": [
                                    {
                                        "name": "parameter_dashboards/simple/datasets/simple_dataset_001_param_end_date",
                                        "query": {
                                            "datasetName": "simple_dataset_001", 
                                            "parameters": [{"name": "param_end_date", "keyword": "param_end_date"}],
                                            "disaggregated": False
                                        }
                                    },
                                    {
                                        "name": "parameter_dashboards/simple/datasets/simple_dataset_002_param_end_date",
                                        "query": {
                                            "datasetName": "simple_dataset_002",
                                            "parameters": [{"name": "param_end_date", "keyword": "param_end_date"}],
                                            "disaggregated": False
                                        }
                                    }
                                ],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "filter-date-picker", 
                                    "encodings": {
                                        "fields": [
                                            {
                                                "parameterName": "param_end_date",
                                                "queryName": "parameter_dashboards/simple/datasets/simple_dataset_001_param_end_date"
                                            },
                                            {
                                                "parameterName": "param_end_date",
                                                "queryName": "parameter_dashboards/simple/datasets/simple_dataset_002_param_end_date"
                                            }
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
                            "position": {"x": 3, "y": 0, "width": 3, "height": 1}
                        },
                        {
                            "widget": {
                                "name": "simple_summary",
                                "queries": [{"name": "simple_dataset_001"}],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "table",
                                    "encodings": {
                                        "fields": [
                                            {"fieldName": "total_records", "queryName": "simple_dataset_001"},
                                            {"fieldName": "total_cost", "queryName": "simple_dataset_001"},
                                            {"fieldName": "workspaces", "queryName": "simple_dataset_001"}
                                        ]
                                    }
                                }
                            },
                            "position": {"x": 0, "y": 1, "width": 6, "height": 2}
                        },
                        {
                            "widget": {
                                "name": "simple_trend",
                                "queries": [{"name": "simple_dataset_002"}],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "line",
                                    "encodings": {
                                        "fields": [
                                            {"fieldName": "date_key", "queryName": "simple_dataset_002"},
                                            {"fieldName": "daily_cost", "queryName": "simple_dataset_002"}
                                        ]
                                    }
                                }
                            },
                            "position": {"x": 0, "y": 3, "width": 6, "height": 3}
                        }
                    ]
                }
            ]
        }
        
        # Write output file
        output_path = os.path.join(self.base_dir, "simple_test_fixed_out.lvdash.json")
        print(f"💾 Writing fixed simple dashboard to {output_path}...")
        with open(output_path, 'w') as f:
            json.dump(dashboard, f, indent=2)
        
        file_size_kb = os.path.getsize(output_path) / 1024
        print(f"✅ Fixed simple test dashboard generated!")
        print(f"📊 Output file: {output_path}")
        print(f"📈 File size: {file_size_kb:.2f} KB")
        print(f"📅 Date range: {default_start} to {default_end}")
        
        return output_path

def main():
    generator = SimpleTestFixedGenerator()
    generator.generate_simple_dashboard()

if __name__ == "__main__":
    main()
