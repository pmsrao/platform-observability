#!/usr/bin/env python3
"""
Dashboard V2 Generator Script

This script generates the enhanced Platform Observability dashboard
leveraging the new gold views with billing_origin_product, usage_unit,
sku_name, and job_pipeline_id attributes.

Usage:
    python dbv2_gen.py

Output:
    dbv2_out.lvdash.json - Ready to import into Databricks
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any

class DashboardV2Generator:
    """Generator for Platform Observability Dashboard V2"""
    
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.template_file = os.path.join(self.base_dir, "dbv2_template.json")
        self.sql_file = os.path.join(self.base_dir, "dbv2_sql_queries.json")
        self.output_file = os.path.join(self.base_dir, "dbv2_out.lvdash.json")
        
        # Default date range (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        self.default_start_date = start_date.strftime("%Y-%m-%dT00:00:00.000")
        self.default_end_date = end_date.strftime("%Y-%m-%dT00:00:00.000")
        
        # Catalog and schema placeholders
        self.catalog = "platform_observability"
        self.gold_schema = "plt_gold"
    
    def read_template(self) -> Dict[str, Any]:
        """Read dashboard template"""
        try:
            with open(self.template_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Template file not found: {self.template_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in template file: {e}")
    
    def read_sql_queries(self) -> Dict[str, Any]:
        """Read SQL queries"""
        try:
            with open(self.sql_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"SQL queries file not found: {self.sql_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in SQL queries file: {e}")
    
    def update_default_dates(self, datasets: Dict[str, Any]) -> Dict[str, Any]:
        """Update default dates to current dates"""
        for dataset_key, dataset in datasets.items():
            if "parameters" in dataset:
                for param in dataset["parameters"]:
                    if param["keyword"] == "param_start_date":
                        param["defaultSelection"]["values"]["values"][0]["value"] = self.default_start_date
                    elif param["keyword"] == "param_end_date":
                        param["defaultSelection"]["values"]["values"][0]["value"] = self.default_end_date
        return datasets
    
    def create_parameter_queries(self, datasets: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create parameter queries following LakeFlow pattern"""
        parameter_queries = []
        
        for dataset_key, dataset in datasets.items():
            dataset_name = dataset["name"]
            if "parameters" in dataset:
                for param in dataset["parameters"]:
                    param_name = param["keyword"]
                    query_name = f"parameter_dashboards/dbv2/datasets/{dataset_name}_{param_name}"
                    
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
    
    def inject_sql_into_template(self, template: Dict[str, Any], sql_data: Dict[str, Any]) -> Dict[str, Any]:
        """Inject SQL datasets into template"""
        # Update default dates
        datasets = self.update_default_dates(sql_data["datasets"])
        
        # Convert datasets to list format
        dataset_list = []
        for dataset_key, dataset_info in datasets.items():
            dataset_entry = {
                "name": dataset_info["name"],
                "displayName": dataset_info["displayName"],
                "queryLines": dataset_info["queryLines"]
            }
            
            # Add parameters if they exist
            if "parameters" in dataset_info and dataset_info["parameters"]:
                dataset_entry["parameters"] = dataset_info["parameters"]
            
            dataset_list.append(dataset_entry)
        
        # Add parameter queries
        parameter_queries = self.create_parameter_queries(datasets)
        
        # Inject into template
        template["datasets"] = dataset_list
        
        # Add parameter queries to filter widgets
        for page in template["pages"]:
            for widget_layout in page["layout"]:
                widget = widget_layout["widget"]
                
                # Add parameter queries to filter widgets
                if "filter" in widget["name"]:
                    if "param_start_date" in widget["name"] or "param_end_date" in widget["name"]:
                        widget["queries"] = [q for q in parameter_queries if widget["name"].split("_")[-1] in q["name"]]
        
        return template
    
    def validate_dashboard(self, dashboard: Dict[str, Any]) -> List[str]:
        """Validate dashboard structure"""
        errors = []
        
        # Check required fields
        if "datasets" not in dashboard:
            errors.append("Missing 'datasets' field")
        if "pages" not in dashboard:
            errors.append("Missing 'pages' field")
        
        # Check datasets
        if "datasets" in dashboard:
            for i, dataset in enumerate(dashboard["datasets"]):
                if "name" not in dataset:
                    errors.append(f"Dataset {i} missing 'name' field")
                if "queryLines" not in dataset:
                    errors.append(f"Dataset {i} missing 'queryLines' field")
        
        # Check pages
        if "pages" in dashboard:
            for i, page in enumerate(dashboard["pages"]):
                if "layout" not in page:
                    errors.append(f"Page {i} missing 'layout' field")
                
                if "layout" in page:
                    for j, widget_layout in enumerate(page["layout"]):
                        if "widget" not in widget_layout:
                            errors.append(f"Page {i}, layout {j} missing 'widget' field")
                        if "position" not in widget_layout:
                            errors.append(f"Page {i}, layout {j} missing 'position' field")
        
        return errors
    
    def generate_dashboard(self) -> None:
        """Main generation function"""
        print("🚀 Starting Dashboard V2 Generation...")
        
        # Read template and SQL
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
            return
        
        # Write output
        print(f"💾 Writing dashboard to {self.output_file}...")
        with open(self.output_file, 'w') as f:
            json.dump(dashboard, f, indent=2)
        
        print("✅ Dashboard V2 generation completed successfully!")
        print(f"📊 Output file: {self.output_file}")
        print("🎯 Ready to import into Databricks!")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get generation statistics"""
        if not os.path.exists(self.output_file):
            return {"error": "Dashboard not generated yet"}
        
        with open(self.output_file, 'r') as f:
            dashboard = json.load(f)
        
        stats = {
            "datasets_count": len(dashboard.get("datasets", [])),
            "pages_count": len(dashboard.get("pages", [])),
            "widgets_count": sum(len(page.get("layout", [])) for page in dashboard.get("pages", [])),
            "file_size_kb": round(os.path.getsize(self.output_file) / 1024, 2),
            "generated_at": datetime.now().isoformat()
        }
        
        return stats

def main():
    """Main entry point"""
    generator = DashboardV2Generator()
    
    try:
        generator.generate_dashboard()
        
        # Print statistics
        stats = generator.get_stats()
        print("\n📈 Generation Statistics:")
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
    except Exception as e:
        print(f"❌ Error generating dashboard: {e}")
        raise

if __name__ == "__main__":
    main()
