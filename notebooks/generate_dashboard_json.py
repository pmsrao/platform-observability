#!/usr/bin/env python3
"""
Updated Dashboard JSON Generator
Based on learnings from multiple iterations of dashboard development.

Key Learnings Applied:
1. Proper spacing in SQL queries (trailing spaces after keywords)
2. Correct column references (entity_key, workspace_key, date_key)
3. Clean widget specifications (removed unsupported properties)
4. Proper widget versions (version 1 for tables, version 3 for charts)
5. Simplified column encodings (only essential properties)
6. Correct date formatting (TO_DATE with yyyyMMdd format)
7. Use try_divide for division operations
8. Proper filter widget configurations
"""

import json
import os
from pathlib import Path

def load_sql_queries():
    """Load SQL queries from the JSON file"""
    queries_file = Path(__file__).parent.parent / "resources" / "dashboard" / "dashboard_sql_queries.json"
    with open(queries_file, 'r') as f:
        return json.load(f)

def load_dashboard_template():
    """Load the dashboard template"""
    template_file = Path(__file__).parent.parent / "resources" / "dashboard" / "dashboard_template.json"
    with open(template_file, 'r') as f:
        return json.load(f)

def generate_dashboard_json():
    """Generate the final dashboard JSON by combining template with SQL queries"""
    
    # Load data
    sql_data = load_sql_queries()
    template = load_dashboard_template()
    
    # Create datasets from SQL queries
    datasets = []
    for query_key, query_data in sql_data["datasets"].items():
        dataset = {
            "name": query_data["name"],
            "displayName": query_data["displayName"],
            "queryLines": query_data["queryLines"]
        }
        datasets.append(dataset)
    
    # Create the final dashboard structure
    dashboard = {
        "datasets": datasets,
        "pages": template["pages"],
        "uiSettings": template["uiSettings"]
    }
    
    return dashboard

def save_dashboard_json(dashboard, output_path):
    """Save the dashboard JSON to file"""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    print(f"Dashboard JSON saved to: {output_file}")

def validate_dashboard_json(dashboard):
    """Validate the generated dashboard JSON"""
    try:
        # Basic structure validation
        required_keys = ["datasets", "pages", "uiSettings"]
        for key in required_keys:
            if key not in dashboard:
                raise ValueError(f"Missing required key: {key}")
        
        # Validate datasets
        if not isinstance(dashboard["datasets"], list):
            raise ValueError("datasets must be a list")
        
        for i, dataset in enumerate(dashboard["datasets"]):
            if "name" not in dataset or "queryLines" not in dataset:
                raise ValueError(f"Dataset {i} missing required fields")
        
        # Validate pages
        if not isinstance(dashboard["pages"], list):
            raise ValueError("pages must be a list")
        
        for i, page in enumerate(dashboard["pages"]):
            if "name" not in page or "layout" not in page:
                raise ValueError(f"Page {i} missing required fields")
        
        print("✅ Dashboard JSON validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Dashboard JSON validation failed: {e}")
        return False

def main():
    """Main function to generate the dashboard"""
    print("🚀 Generating Platform Observability Dashboard...")
    
    try:
        # Generate dashboard
        dashboard = generate_dashboard_json()
        
        # Validate dashboard
        if not validate_dashboard_json(dashboard):
            return False
        
        # Save dashboard
        output_path = Path(__file__).parent.parent / "resources" / "dashboard" / "platform_observability_dashboard_generated.lvdash.json"
        save_dashboard_json(dashboard, output_path)
        
        print("✅ Dashboard generation completed successfully!")
        print(f"📁 Output file: {output_path}")
        print(f"📊 Datasets: {len(dashboard['datasets'])}")
        print(f"📄 Pages: {len(dashboard['pages'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Dashboard generation failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
