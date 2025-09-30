#!/usr/bin/env python3
"""
Comprehensive Dashboard Validation Script
Validates SQL queries, widget structure, parameter binding, and field mappings
"""

import json
import re
from typing import Dict, List, Any

def validate_sql_queries(dashboard_json: Dict[str, Any]) -> List[str]:
    """Validate SQL query structure and spacing"""
    errors = []
    
    for dataset in dashboard_json.get("datasets", []):
        dataset_name = dataset.get("name", "unknown")
        query_lines = dataset.get("queryLines", [])
        
        if not query_lines:
            errors.append(f"❌ Dataset {dataset_name} has no queryLines")
            continue
            
        # Check spacing
        for i, line in enumerate(query_lines):
            if line.strip() and not line.endswith(' '):
                errors.append(f"❌ Dataset {dataset_name}, Line {i+1} missing trailing space: '{line}'")
        
        # Check parameter usage
        full_query = ''.join(query_lines)
        if ':param_' not in full_query:
            errors.append(f"❌ Dataset {dataset_name} missing parameters")
        
        # Check for common SQL issues
        if 'SELECT' not in full_query.upper():
            errors.append(f"❌ Dataset {dataset_name} missing SELECT statement")
        
        if 'FROM' not in full_query.upper():
            errors.append(f"❌ Dataset {dataset_name} missing FROM clause")
    
    return errors

def validate_widget_structure(dashboard_json: Dict[str, Any]) -> List[str]:
    """Validate widget configurations"""
    errors = []
    
    for page in dashboard_json.get("pages", []):
        page_name = page.get("name", "unknown")
        
        for widget_layout in page.get("layout", []):
            widget = widget_layout.get("widget", {})
            widget_name = widget.get("name", "unknown")
            
            # Check required properties
            required = ["name", "displayName", "queries", "spec"]
            for prop in required:
                if prop not in widget:
                    errors.append(f"❌ Widget {widget_name} missing {prop}")
            
            # Check widget type specific requirements
            spec = widget.get("spec", {})
            widget_type = spec.get("widgetType", "unknown")
            
            if widget_type == "table":
                encodings = spec.get("encodings", {})
                if "columns" not in encodings:
                    errors.append(f"❌ Table widget {widget_name} missing columns in encodings")
                else:
                    columns = encodings["columns"]
                    if not columns:
                        errors.append(f"❌ Table widget {widget_name} has empty columns array")
            
            elif widget_type in ["line", "bar"]:
                encodings = spec.get("encodings", {})
                if "x" not in encodings or "y" not in encodings:
                    errors.append(f"❌ Chart widget {widget_name} missing x/y encodings")
                else:
                    # Check scale properties
                    x_encoding = encodings["x"]
                    y_encoding = encodings["y"]
                    
                    if "scale" not in x_encoding:
                        errors.append(f"❌ Chart widget {widget_name} x-axis missing scale property")
                    if "scale" not in y_encoding:
                        errors.append(f"❌ Chart widget {widget_name} y-axis missing scale property")
            
            elif widget_type.startswith("filter-"):
                encodings = spec.get("encodings", {})
                if "fields" not in encodings:
                    errors.append(f"❌ Filter widget {widget_name} missing fields in encodings")
            
            # Check frame/title structure
            if "frame" not in widget:
                errors.append(f"❌ Widget {widget_name} missing frame structure")
            else:
                frame = widget["frame"]
                if "showTitle" not in frame or "title" not in frame:
                    errors.append(f"❌ Widget {widget_name} frame missing showTitle or title")
    
    return errors

def validate_parameter_binding(dashboard_json: Dict[str, Any]) -> List[str]:
    """Validate parameter queries and bindings"""
    errors = []
    
    # Collect all parameter queries
    parameter_queries = []
    for page in dashboard_json.get("pages", []):
        for widget_layout in page.get("layout", []):
            widget = widget_layout.get("widget", {})
            if "queries" in widget:
                parameter_queries.extend(widget["queries"])
    
    # Validate parameter query structure
    for query in parameter_queries:
        query_name = query.get("name", "unknown")
        query_obj = query.get("query", {})
        
        if "parameters" in query_obj:
            params = query_obj["parameters"]
            for param in params:
                if "name" not in param or "keyword" not in param:
                    errors.append(f"❌ Parameter query {query_name} has invalid parameter structure: {param}")
        
        if "datasetName" not in query_obj:
            errors.append(f"❌ Parameter query {query_name} missing datasetName")
    
    return errors

def validate_field_mappings(dashboard_json: Dict[str, Any]) -> List[str]:
    """Validate field names match SQL columns"""
    errors = []
    
    # Extract field names from datasets (simplified)
    dataset_fields = {}
    for dataset in dashboard_json.get("datasets", []):
        dataset_name = dataset.get("name", "unknown")
        query_lines = dataset.get("queryLines", [])
        full_query = ''.join(query_lines)
        
        # Extract column names from SELECT clause (simplified regex)
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', full_query, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            # Split by comma and extract column names
            columns = []
            for col in select_clause.split(','):
                col = col.strip()
                # Handle AS aliases
                if ' AS ' in col.upper():
                    col = col.split(' AS ')[-1].strip()
                # Remove backticks and quotes
                col = col.replace('`', '').replace('"', '').replace("'", "")
                if col:
                    columns.append(col)
            dataset_fields[dataset_name] = columns
    
    # Check widget field mappings
    for page in dashboard_json.get("pages", []):
        for widget_layout in page.get("layout", []):
            widget = widget_layout.get("widget", {})
            widget_name = widget.get("name", "unknown")
            
            if "queries" in widget:
                for query in widget["queries"]:
                    query_obj = query.get("query", {})
                    dataset_name = query_obj.get("datasetName", "unknown")
                    
                    if "fields" in query_obj:
                        available_fields = dataset_fields.get(dataset_name, [])
                        for field in query_obj["fields"]:
                            field_name = field.get("name", "unknown")
                            if field_name not in available_fields:
                                errors.append(f"❌ Widget {widget_name} field '{field_name}' not found in dataset {dataset_name}")
    
    return errors

def validate_date_parameters(dashboard_json: Dict[str, Any]) -> List[str]:
    """Validate date parameters are current"""
    errors = []
    
    # Check for hardcoded 2024 dates
    dashboard_str = json.dumps(dashboard_json)
    if "2024" in dashboard_str:
        errors.append("❌ Found hardcoded 2024 dates - should use dynamic dates")
    
    # Check date format consistency
    date_pattern = r'"value":\s*"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})"'
    dates = re.findall(date_pattern, dashboard_str)
    
    if dates:
        # Check if dates are reasonable (not too far in past/future)
        from datetime import datetime
        current_year = datetime.now().year
        
        for date_str in dates:
            try:
                date_obj = datetime.fromisoformat(date_str)
                if abs(date_obj.year - current_year) > 1:
                    errors.append(f"❌ Date {date_str} is more than 1 year from current year")
            except ValueError:
                errors.append(f"❌ Invalid date format: {date_str}")
    
    return errors

def validate_dashboard_completeness(dashboard_json: Dict[str, Any]) -> List[str]:
    """Validate overall dashboard completeness"""
    errors = []
    
    # Check required top-level properties
    required_props = ["datasets", "pages"]
    for prop in required_props:
        if prop not in dashboard_json:
            errors.append(f"❌ Dashboard missing {prop}")
    
    # Check datasets exist
    datasets = dashboard_json.get("datasets", [])
    if not datasets:
        errors.append("❌ Dashboard has no datasets")
    
    # Check pages exist
    pages = dashboard_json.get("pages", [])
    if not pages:
        errors.append("❌ Dashboard has no pages")
    
    # Check widgets exist
    total_widgets = 0
    for page in pages:
        total_widgets += len(page.get("layout", []))
    
    if total_widgets == 0:
        errors.append("❌ Dashboard has no widgets")
    
    return errors

def main():
    """Main validation function"""
    print("🔍 Starting comprehensive dashboard validation...")
    
    try:
        with open('dbv4_out.lvdash.json', 'r') as f:
            dashboard_json = json.load(f)
    except FileNotFoundError:
        print("❌ dbv4_out.lvdash.json not found")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        return
    
    all_errors = []
    
    # Run all validation checks
    print("\n📊 Validating dashboard completeness...")
    all_errors.extend(validate_dashboard_completeness(dashboard_json))
    
    print("🗃️ Validating SQL queries...")
    all_errors.extend(validate_sql_queries(dashboard_json))
    
    print("🎨 Validating widget structure...")
    all_errors.extend(validate_widget_structure(dashboard_json))
    
    print("🔗 Validating parameter binding...")
    all_errors.extend(validate_parameter_binding(dashboard_json))
    
    print("📋 Validating field mappings...")
    all_errors.extend(validate_field_mappings(dashboard_json))
    
    print("📅 Validating date parameters...")
    all_errors.extend(validate_date_parameters(dashboard_json))
    
    # Report results
    print(f"\n📊 Validation Results:")
    print(f"Total checks: {len(all_errors)}")
    
    if all_errors:
        print(f"❌ Found {len(all_errors)} issues:")
        for error in all_errors:
            print(f"  {error}")
    else:
        print("✅ All validations passed! Dashboard is ready for deployment.")
    
    # Summary statistics
    datasets = dashboard_json.get("datasets", [])
    pages = dashboard_json.get("pages", [])
    total_widgets = sum(len(page.get("layout", [])) for page in pages)
    
    print(f"\n📈 Dashboard Statistics:")
    print(f"  Datasets: {len(datasets)}")
    print(f"  Pages: {len(pages)}")
    print(f"  Widgets: {total_widgets}")
    
    # Widget type breakdown
    widget_types = {}
    for page in pages:
        for widget_layout in page.get("layout", []):
            widget_type = widget_layout.get("widget", {}).get("spec", {}).get("widgetType", "unknown")
            widget_types[widget_type] = widget_types.get(widget_type, 0) + 1
    
    print(f"  Widget Types:")
    for widget_type, count in widget_types.items():
        print(f"    {widget_type}: {count}")

if __name__ == "__main__":
    main()

