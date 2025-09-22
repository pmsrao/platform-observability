#!/usr/bin/env python3
"""
Dashboard Structure Validator - Validate generated dashboard against LakeFlow structure
"""

import json
import os
from typing import Dict, List, Any

class DashboardStructureValidator:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.lakeflow_path = os.path.join(self.base_dir, "../dashboard/LakeFlow System Tables Dashboard v0.1.lvdash.json")
        
    def load_dashboard(self, path: str) -> Dict[str, Any]:
        """Load a dashboard JSON file"""
        with open(path, 'r') as f:
            return json.load(f)
    
    def validate_table_widget(self, widget: Dict[str, Any]) -> List[str]:
        """Validate table widget structure"""
        errors = []
        
        spec = widget.get("spec", {})
        if spec.get("widgetType") != "table":
            return errors
        
        if spec.get("version") != 1:
            errors.append(f"Table widget {widget.get('name', 'unknown')} should have version 1, got {spec.get('version')}")
        
        encodings = spec.get("encodings", {})
        if "columns" not in encodings:
            errors.append(f"Table widget {widget.get('name', 'unknown')} should have 'columns' in encodings")
        elif "fields" in encodings:
            errors.append(f"Table widget {widget.get('name', 'unknown')} should not have 'fields' in encodings")
        
        # Validate column structure
        for i, column in enumerate(encodings.get("columns", [])):
            required_fields = ["fieldName", "booleanValues", "imageUrlTemplate", "displayName"]
            for field in required_fields:
                if field not in column:
                    errors.append(f"Table widget {widget.get('name', 'unknown')} column {i} missing '{field}'")
        
        return errors
    
    def validate_line_widget(self, widget: Dict[str, Any]) -> List[str]:
        """Validate line widget structure"""
        errors = []
        
        spec = widget.get("spec", {})
        if spec.get("widgetType") != "line":
            return errors
        
        if spec.get("version") != 3:
            errors.append(f"Line widget {widget.get('name', 'unknown')} should have version 3, got {spec.get('version')}")
        
        encodings = spec.get("encodings", {})
        if "x" not in encodings or "y" not in encodings:
            errors.append(f"Line widget {widget.get('name', 'unknown')} should have 'x' and 'y' in encodings")
        elif "fields" in encodings:
            errors.append(f"Line widget {widget.get('name', 'unknown')} should not have 'fields' in encodings")
        
        return errors
    
    def validate_bar_widget(self, widget: Dict[str, Any]) -> List[str]:
        """Validate bar widget structure"""
        errors = []
        
        spec = widget.get("spec", {})
        if spec.get("widgetType") != "bar":
            return errors
        
        if spec.get("version") != 3:
            errors.append(f"Bar widget {widget.get('name', 'unknown')} should have version 3, got {spec.get('version')}")
        
        encodings = spec.get("encodings", {})
        if "x" not in encodings or "y" not in encodings:
            errors.append(f"Bar widget {widget.get('name', 'unknown')} should have 'x' and 'y' in encodings")
        elif "fields" in encodings:
            errors.append(f"Bar widget {widget.get('name', 'unknown')} should not have 'fields' in encodings")
        
        return errors
    
    def validate_pie_widget(self, widget: Dict[str, Any]) -> List[str]:
        """Validate pie widget structure"""
        errors = []
        
        spec = widget.get("spec", {})
        if spec.get("widgetType") != "pie":
            return errors
        
        if spec.get("version") != 3:
            errors.append(f"Pie widget {widget.get('name', 'unknown')} should have version 3, got {spec.get('version')}")
        
        encodings = spec.get("encodings", {})
        if "theta" not in encodings or "color" not in encodings:
            errors.append(f"Pie widget {widget.get('name', 'unknown')} should have 'theta' and 'color' in encodings")
        elif "fields" in encodings:
            errors.append(f"Pie widget {widget.get('name', 'unknown')} should not have 'fields' in encodings")
        
        return errors
    
    def validate_filter_widget(self, widget: Dict[str, Any]) -> List[str]:
        """Validate filter widget structure"""
        errors = []
        
        spec = widget.get("spec", {})
        widget_type = spec.get("widgetType", "")
        
        if "filter" not in widget_type:
            return errors
        
        if spec.get("version") != 2:
            errors.append(f"Filter widget {widget.get('name', 'unknown')} should have version 2, got {spec.get('version')}")
        
        encodings = spec.get("encodings", {})
        if "fields" not in encodings:
            errors.append(f"Filter widget {widget.get('name', 'unknown')} should have 'fields' in encodings")
        
        # Validate field structure
        for i, field in enumerate(encodings.get("fields", [])):
            if "parameterName" not in field and "fieldName" not in field:
                errors.append(f"Filter widget {widget.get('name', 'unknown')} field {i} should have 'parameterName' or 'fieldName'")
            if "queryName" not in field:
                errors.append(f"Filter widget {widget.get('name', 'unknown')} field {i} should have 'queryName'")
        
        return errors
    
    def validate_widget_queries(self, widget: Dict[str, Any]) -> List[str]:
        """Validate widget queries structure"""
        errors = []
        
        queries = widget.get("queries", [])
        if not queries:
            errors.append(f"Widget {widget.get('name', 'unknown')} should have 'queries' array")
            return errors
        
        for i, query in enumerate(queries):
            if "name" not in query:
                errors.append(f"Widget {widget.get('name', 'unknown')} query {i} missing 'name'")
            if "query" not in query:
                errors.append(f"Widget {widget.get('name', 'unknown')} query {i} missing 'query' object")
            elif "datasetName" not in query["query"]:
                errors.append(f"Widget {widget.get('name', 'unknown')} query {i} missing 'datasetName'")
        
        return errors
    
    def validate_dashboard(self, dashboard_path: str) -> Dict[str, Any]:
        """Validate entire dashboard structure"""
        print(f"🔍 Validating dashboard: {dashboard_path}")
        
        dashboard = self.load_dashboard(dashboard_path)
        validation_results = {
            "total_errors": 0,
            "widget_errors": {},
            "structure_errors": [],
            "summary": {}
        }
        
        # Validate top-level structure
        required_keys = ["datasets", "pages"]
        for key in required_keys:
            if key not in dashboard:
                validation_results["structure_errors"].append(f"Missing required key: {key}")
        
        # Validate datasets
        datasets = dashboard.get("datasets", [])
        validation_results["summary"]["datasets_count"] = len(datasets)
        
        for i, dataset in enumerate(datasets):
            required_dataset_fields = ["name", "displayName", "queryLines"]
            for field in required_dataset_fields:
                if field not in dataset:
                    validation_results["structure_errors"].append(f"Dataset {i} missing '{field}'")
        
        # Validate pages and widgets
        pages = dashboard.get("pages", [])
        validation_results["summary"]["pages_count"] = len(pages)
        validation_results["summary"]["widgets_count"] = 0
        
        for page in pages:
            for widget_layout in page.get("layout", []):
                widget = widget_layout.get("widget", {})
                widget_name = widget.get("name", "unknown")
                validation_results["summary"]["widgets_count"] += 1
                
                # Validate widget queries
                query_errors = self.validate_widget_queries(widget)
                if query_errors:
                    validation_results["widget_errors"][widget_name] = query_errors
                
                # Validate widget structure based on type
                spec = widget.get("spec", {})
                widget_type = spec.get("widgetType", "unknown")
                
                if widget_type == "table":
                    errors = self.validate_table_widget(widget)
                elif widget_type == "line":
                    errors = self.validate_line_widget(widget)
                elif widget_type == "bar":
                    errors = self.validate_bar_widget(widget)
                elif widget_type == "pie":
                    errors = self.validate_pie_widget(widget)
                elif "filter" in widget_type:
                    errors = self.validate_filter_widget(widget)
                else:
                    errors = [f"Unknown widget type: {widget_type}"]
                
                if errors:
                    if widget_name not in validation_results["widget_errors"]:
                        validation_results["widget_errors"][widget_name] = []
                    validation_results["widget_errors"][widget_name].extend(errors)
        
        # Count total errors
        validation_results["total_errors"] = len(validation_results["structure_errors"])
        for widget_errors in validation_results["widget_errors"].values():
            validation_results["total_errors"] += len(widget_errors)
        
        return validation_results
    
    def print_validation_results(self, results: Dict[str, Any]):
        """Print validation results in a readable format"""
        print(f"\n📊 Validation Results:")
        print(f"   Total Errors: {results['total_errors']}")
        print(f"   Datasets: {results['summary']['datasets_count']}")
        print(f"   Pages: {results['summary']['pages_count']}")
        print(f"   Widgets: {results['summary']['widgets_count']}")
        
        if results["structure_errors"]:
            print(f"\n❌ Structure Errors:")
            for error in results["structure_errors"]:
                print(f"   - {error}")
        
        if results["widget_errors"]:
            print(f"\n❌ Widget Errors:")
            for widget_name, errors in results["widget_errors"].items():
                print(f"   Widget '{widget_name}':")
                for error in errors:
                    print(f"     - {error}")
        
        if results["total_errors"] == 0:
            print(f"\n✅ Dashboard structure is valid!")
        else:
            print(f"\n❌ Dashboard has {results['total_errors']} validation errors")

def main():
    validator = DashboardStructureValidator()
    
    # Validate the generated dashboard
    dashboard_path = os.path.join(validator.base_dir, "dbv3_proper_out.lvdash.json")
    if os.path.exists(dashboard_path):
        results = validator.validate_dashboard(dashboard_path)
        validator.print_validation_results(results)
    else:
        print(f"❌ Dashboard file not found: {dashboard_path}")

if __name__ == "__main__":
    main()
