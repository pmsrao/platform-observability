#!/usr/bin/env python3
"""
Dashboard V3 Corrected Generator - Fixed widget structure to match LakeFlow
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any

class DashboardV3CorrectedGenerator:
    def __init__(self, catalog: str = "platform_observability", gold_schema: str = "plt_gold"):
        self.catalog = catalog
        self.gold_schema = gold_schema
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
    def read_template(self) -> Dict[str, Any]:
        """Read the dashboard template"""
        template_path = os.path.join(self.base_dir, "dbv3_template.json")
        with open(template_path, 'r') as f:
            return json.load(f)
    
    def read_sql_queries(self) -> Dict[str, Any]:
        """Read the SQL queries configuration"""
        queries_path = os.path.join(self.base_dir, "dbv3_sql_queries.json")
        with open(queries_path, 'r') as f:
            return json.load(f)
    
    def update_default_dates(self, datasets: Dict[str, Any]) -> Dict[str, Any]:
        """Update default dates to last 30 days"""
        # Calculate last 30 days from today
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
    
    def update_template_filter_defaults(self, template: Dict[str, Any]) -> None:
        """Update default values in template filter widgets"""
        # Calculate last 30 days from today
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        default_start = start_date.strftime("%Y-%m-%dT00:00:00.000")
        default_end = end_date.strftime("%Y-%m-%dT00:00:00.000")
        
        # Update filter widgets in template
        for page in template["pages"]:
            for widget_layout in page["layout"]:
                widget = widget_layout["widget"]
                
                if "filter" in widget["name"]:
                    if "start_date" in widget["name"]:
                        widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = default_start
                    elif "end_date" in widget["name"]:
                        widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = default_end
                    elif "workspace" in widget["name"]:
                        widget["spec"]["selection"]["defaultSelection"]["values"]["values"][0]["value"] = "<ALL_WORKSPACES>"
    
    def fix_widget_structure(self, template: Dict[str, Any]) -> None:
        """Fix widget structure to match LakeFlow pattern"""
        for page in template["pages"]:
            for widget_layout in page["layout"]:
                widget = widget_layout["widget"]
                
                # Fix table widgets
                if widget["spec"]["widgetType"] == "table":
                    widget["spec"]["version"] = 1
                    if "encodings" in widget["spec"] and "fields" in widget["spec"]["encodings"]:
                        # Convert fields to columns format
                        columns = []
                        for field in widget["spec"]["encodings"]["fields"]:
                            column = {
                                "fieldName": field["fieldName"],
                                "booleanValues": ["false", "true"],
                                "imageUrlTemplate": "{{ @ }}",
                                "imageTitleTemplate": "{{ @ }}",
                                "imageWidth": "",
                                "imageHeight": "",
                                "linkUrlTemplate": "{{ @ }}",
                                "linkTextTemplate": "{{ @ }}",
                                "linkTitleTemplate": "{{ @ }}"
                            }
                            if "displayName" in field:
                                column["displayName"] = field["displayName"]
                            columns.append(column)
                        widget["spec"]["encodings"]["columns"] = columns
                        del widget["spec"]["encodings"]["fields"]
                
                # Fix line widgets
                elif widget["spec"]["widgetType"] == "line":
                    widget["spec"]["version"] = 3
                    if "encodings" in widget["spec"] and "fields" in widget["spec"]["encodings"]:
                        # Convert fields to x/y format
                        x_field = None
                        y_field = None
                        for field in widget["spec"]["encodings"]["fields"]:
                            if field["fieldName"] in ["date_key", "usage_date", "daily_date"]:
                                x_field = field
                            else:
                                y_field = field
                        
                        if x_field and y_field:
                            widget["spec"]["encodings"] = {
                                "x": {
                                    "fieldName": x_field["fieldName"],
                                    "scale": {"type": "ordinal"},
                                    "displayName": x_field.get("displayName", "Date")
                                },
                                "y": {
                                    "fieldName": y_field["fieldName"],
                                    "scale": {"type": "quantitative"},
                                    "format": {"type": "number-plain", "precision": 2},
                                    "displayName": y_field.get("displayName", "Value")
                                }
                            }
                
                # Fix bar widgets
                elif widget["spec"]["widgetType"] == "bar":
                    widget["spec"]["version"] = 3
                    if "encodings" in widget["spec"] and "fields" in widget["spec"]["encodings"]:
                        # Convert fields to x/y format
                        x_field = None
                        y_field = None
                        for field in widget["spec"]["encodings"]["fields"]:
                            if field["fieldName"] in ["billing_origin_product", "usage_unit", "sku_name", "workload_type"]:
                                x_field = field
                            else:
                                y_field = field
                        
                        if x_field and y_field:
                            widget["spec"]["encodings"] = {
                                "x": {
                                    "fieldName": x_field["fieldName"],
                                    "scale": {"type": "ordinal"},
                                    "displayName": x_field.get("displayName", "Category")
                                },
                                "y": {
                                    "fieldName": y_field["fieldName"],
                                    "scale": {"type": "quantitative"},
                                    "format": {"type": "number-plain", "precision": 2},
                                    "displayName": y_field.get("displayName", "Value")
                                }
                            }
                
                # Fix pie widgets
                elif widget["spec"]["widgetType"] == "pie":
                    widget["spec"]["version"] = 3
                    if "encodings" in widget["spec"] and "fields" in widget["spec"]["encodings"]:
                        # Convert fields to theta/color format
                        theta_field = None
                        color_field = None
                        for field in widget["spec"]["encodings"]["fields"]:
                            if field["fieldName"] in ["total_cost", "total_usage", "count"]:
                                theta_field = field
                            else:
                                color_field = field
                        
                        if theta_field and color_field:
                            widget["spec"]["encodings"] = {
                                "theta": {
                                    "fieldName": theta_field["fieldName"],
                                    "scale": {"type": "quantitative"},
                                    "format": {"type": "number-plain", "precision": 2},
                                    "displayName": theta_field.get("displayName", "Value")
                                },
                                "color": {
                                    "fieldName": color_field["fieldName"],
                                    "scale": {"type": "ordinal"},
                                    "displayName": color_field.get("displayName", "Category")
                                }
                            }
    
    def inject_sql_into_template(self, template: Dict[str, Any], sql_data: Dict[str, Any]) -> Dict[str, Any]:
        """Inject SQL datasets into template following LakeFlow pattern"""
        # Update default dates
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
        
        # Add parameter queries
        parameter_queries = self.create_parameter_queries(datasets)
        
        # Inject into template
        template["datasets"] = dataset_list
        
        # Update template filter widget default values
        self.update_template_filter_defaults(template)
        
        # Fix widget structure to match LakeFlow
        self.fix_widget_structure(template)
        
        # Add parameter queries to filter widgets following LakeFlow pattern
        for page in template["pages"]:
            for widget_layout in page["layout"]:
                widget = widget_layout["widget"]
                
                # Add parameter queries to filter widgets
                if "filter" in widget["name"]:
                    if "start_date" in widget["name"]:
                        start_date_queries = [q for q in parameter_queries if "param_start_date" in q["name"]]
                        widget["queries"] = start_date_queries
                        # Add parameterName entries to encodings for date picker (LakeFlow pattern)
                        if "spec" in widget and "encodings" in widget["spec"] and "fields" in widget["spec"]["encodings"]:
                            # Clear existing fields and add only parameterName entries
                            widget["spec"]["encodings"]["fields"] = []
                            for sq in start_date_queries:
                                param_entry = {
                                    "parameterName": "param_start_date",
                                    "queryName": sq["name"]
                                }
                                widget["spec"]["encodings"]["fields"].append(param_entry)
                        print(f"🔍 Fixed start_date filter: {len(start_date_queries)} parameter queries")
                    elif "end_date" in widget["name"]:
                        end_date_queries = [q for q in parameter_queries if "param_end_date" in q["name"]]
                        widget["queries"] = end_date_queries
                        # Add parameterName entries to encodings for date picker (LakeFlow pattern)
                        if "spec" in widget and "encodings" in widget["spec"] and "fields" in widget["spec"]["encodings"]:
                            # Clear existing fields and add only parameterName entries
                            widget["spec"]["encodings"]["fields"] = []
                            for eq in end_date_queries:
                                param_entry = {
                                    "parameterName": "param_end_date",
                                    "queryName": eq["name"]
                                }
                                widget["spec"]["encodings"]["fields"].append(param_entry)
                        print(f"🔍 Fixed end_date filter: {len(end_date_queries)} parameter queries")
                    elif "workspace" in widget["name"]:
                        # Preserve existing main query and add parameter queries
                        existing_queries = widget.get("queries", [])
                        param_queries = [q for q in parameter_queries if "param_workspace" in q["name"]]
                        widget["queries"] = existing_queries + param_queries
                        
                        # Add parameterName entries to encodings for workspace filter
                        if "spec" in widget and "encodings" in widget["spec"] and "fields" in widget["spec"]["encodings"]:
                            # Add parameterName for the main query
                            main_query_param_entry = {
                                "parameterName": "param_workspace",
                                "queryName": "workspace_query"
                            }
                            widget["spec"]["encodings"]["fields"].append(main_query_param_entry)
                            
                            # Add parameterName entries for parameter queries
                            for pq in param_queries:
                                param_entry = {
                                    "parameterName": "param_workspace",
                                    "queryName": pq["name"]
                                }
                                widget["spec"]["encodings"]["fields"].append(param_entry)
                        print(f"🔍 Fixed workspace filter: {len(existing_queries)} existing + {len(param_queries)} parameter queries")
        
        return template
    
    def validate_dashboard(self, dashboard: Dict[str, Any]) -> List[str]:
        """Validate dashboard structure"""
        errors = []
        
        # Check required top-level keys
        required_keys = ["datasets", "pages"]
        for key in required_keys:
            if key not in dashboard:
                errors.append(f"Missing required key: {key}")
        
        # Check datasets
        if "datasets" in dashboard:
            if not isinstance(dashboard["datasets"], list):
                errors.append("datasets must be a list")
            else:
                for i, dataset in enumerate(dashboard["datasets"]):
                    if "name" not in dataset:
                        errors.append(f"Dataset {i} missing 'name' field")
                    if "queryLines" not in dataset:
                        errors.append(f"Dataset {i} missing 'queryLines' field")
        
        # Check pages
        if "pages" in dashboard:
            if not isinstance(dashboard["pages"], list):
                errors.append("pages must be a list")
            else:
                for i, page in enumerate(dashboard["pages"]):
                    if "layout" not in page:
                        errors.append(f"Page {i} missing 'layout' field")
                    else:
                        for j, widget_layout in enumerate(page["layout"]):
                            if "widget" not in widget_layout:
                                errors.append(f"Page {i}, Widget {j} missing 'widget' field")
        
        return errors
    
    def generate_dashboard(self) -> str:
        """Generate the complete dashboard"""
        print("🚀 Starting Dashboard V3 Corrected Generation...")
        print(f"📊 Using catalog: {self.catalog}")
        print(f"📊 Using gold schema: {self.gold_schema}")
        
        # Read template and SQL queries
        print("📖 Reading template and SQL queries...")
        template = self.read_template()
        sql_data = self.read_sql_queries()
        
        # Inject SQL into template
        print("🔄 Injecting SQL into template...")
        dashboard = self.inject_sql_into_template(template, sql_data)
        
        # Validate dashboard
        print("✅ Validating dashboard structure...")
        errors = self.validate_dashboard(dashboard)
        if errors:
            print("❌ Validation errors found:")
            for error in errors:
                print(f"   - {error}")
            raise ValueError("Dashboard validation failed")
        
        # Write output file
        output_path = os.path.join(self.base_dir, "dbv3_corrected_out.lvdash.json")
        print(f"💾 Writing dashboard to {output_path}...")
        with open(output_path, 'w') as f:
            json.dump(dashboard, f, indent=2)
        
        # Get file size
        file_size_kb = os.path.getsize(output_path) / 1024
        
        print("✅ Dashboard V3 corrected generation completed successfully!")
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
    generator = DashboardV3CorrectedGenerator()
    generator.generate_dashboard()

if __name__ == "__main__":
    main()
