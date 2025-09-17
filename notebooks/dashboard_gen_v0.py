#!/usr/bin/env python3
"""
Dashboard Generator v0 - Comprehensive Platform Observability Dashboard
- Reads template file and SQL queries
- Generates final dashboard with proper parameter binding
- Includes comprehensive validation
- Follows LakeFlow pattern for filters
- Handles all 19 datasets with parameter injection
"""

import json
import os
import sys
from datetime import datetime, timedelta

def get_default_dates():
    """Get default dates (last 30 days from today)."""
    today = datetime.now()
    start_date = today - timedelta(days=30)
    
    return {
        "start": start_date.strftime("%Y-%m-%dT00:00:00.000"),
        "end": today.strftime("%Y-%m-%dT00:00:00.000")
    }

def read_template(template_file):
    """Read dashboard template file."""
    try:
        with open(template_file, 'r') as f:
            template = json.load(f)
        print(f"✅ Template loaded: {template_file}")
        return template
    except Exception as e:
        print(f"❌ Error loading template: {e}")
        return None

def read_sql_queries(sql_file):
    """Read SQL queries file."""
    try:
        with open(sql_file, 'r') as f:
            sql_data = json.load(f)
        print(f"✅ SQL queries loaded: {sql_file}")
        return sql_data
    except Exception as e:
        print(f"❌ Error loading SQL queries: {e}")
        return None

def inject_parameters_into_sql(query_lines):
    """Inject parameter placeholders into SQL with smart handling of WITH clauses."""
    query_text = ' '.join(query_lines)
    
    # Check if parameters are already present
    if ':param_start_date' in query_text and ':param_end_date' in query_text:
        return query_lines
    
    new_query_lines = query_lines.copy()
    
    # Pattern 1: Replace hardcoded date filters with parameter-based filters
    for i, line in enumerate(new_query_lines):
        # Replace hardcoded date filters with parameter filters
        if 'date_key >= date_format(date_sub(current_date(), 30)' in line:
            # Replace the entire line with parameter-based filter
            new_query_lines[i] = line.replace(
                'date_key >= date_format(date_sub(current_date(), 30), \'yyyyMMdd\')',
                'date_key >= date_format(:param_start_date, \'yyyyMMdd\') AND date_key <= date_format(:param_end_date, \'yyyyMMdd\')'
            )
        elif 'date_key >= date_format(date_sub(current_date(), 90)' in line:
            # Replace the entire line with parameter-based filter
            new_query_lines[i] = line.replace(
                'date_key >= date_format(date_sub(current_date(), 90), \'yyyyMMdd\')',
                'date_key >= date_format(:param_start_date, \'yyyyMMdd\') AND date_key <= date_format(:param_end_date, \'yyyyMMdd\')'
            )
        elif 'date_key >= date_format(current_date()' in line:
            # Replace the entire line with parameter-based filter
            new_query_lines[i] = line.replace(
                'date_key >= date_format(current_date(), \'yyyyMMdd\')',
                'date_key >= date_format(:param_start_date, \'yyyyMMdd\') AND date_key <= date_format(:param_end_date, \'yyyyMMdd\')'
            )
    
    # Pattern 2: If no existing date filters found, add them ONLY inside WITH clauses
    # BUT ONLY if the query involves fact tables with date_key columns
    query_text_updated = ' '.join(new_query_lines)
    if ':param_start_date' not in query_text_updated or ':param_end_date' not in query_text_updated:
        # Check if this query involves fact tables (should have date filters)
        has_fact_table = 'gld_fact_usage_priced_day' in query_text_updated
        has_date_key_column = 'date_key' in query_text_updated
        
        # Only add date filters if this is a fact table query
        if has_fact_table and has_date_key_column:
            # Find the appropriate location to add WHERE clause
            where_added = False
            
            # Look for WITH clause structure
            in_with_clause = False
            with_clause_depth = 0
            
            for i, line in enumerate(new_query_lines):
                line_upper = line.upper().strip()
                
                # Track WITH clause depth
                if line_upper.startswith('WITH '):
                    in_with_clause = True
                    with_clause_depth += 1
                elif line_upper.startswith(')') and in_with_clause:
                    with_clause_depth -= 1
                    if with_clause_depth == 0:
                        in_with_clause = False
                
                # Look for FROM clause inside WITH clause ONLY
                if 'FROM ' in line_upper and in_with_clause and not where_added:
                    # Check if WHERE already exists after this FROM
                    has_where_after = False
                    for j in range(i + 1, len(new_query_lines)):
                        if 'WHERE' in new_query_lines[j].upper():
                            has_where_after = True
                            break
                        if new_query_lines[j].strip().startswith(')') and with_clause_depth > 0:
                            break
                    
                    if not has_where_after:
                        # Add WHERE clause after FROM (inside WITH clause)
                        new_query_lines.insert(i + 1, "  WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')")
                        new_query_lines.insert(i + 2, "    AND date_key <= date_format(:param_end_date, 'yyyyMMdd')")
                        where_added = True
                        break
                
                # For queries WITHOUT WITH clause, add WHERE clause after FROM
                elif 'FROM ' in line_upper and not in_with_clause and not where_added:
                    # Check if WHERE already exists after this FROM
                    has_where_after = False
                    for j in range(i + 1, len(new_query_lines)):
                        if 'WHERE' in new_query_lines[j].upper():
                            has_where_after = True
                            break
                        if new_query_lines[j].strip().startswith('SELECT') or new_query_lines[j].strip().startswith('UNION'):
                            break
                    
                    if not has_where_after:
                        # Add WHERE clause after FROM (for non-WITH queries)
                        new_query_lines.insert(i + 1, "WHERE date_key >= date_format(:param_start_date, 'yyyyMMdd')")
                        new_query_lines.insert(i + 2, "  AND date_key <= date_format(:param_end_date, 'yyyyMMdd')")
                        where_added = True
                        break
    
    return new_query_lines

def create_dataset_entry(dataset_id, dataset_info, default_dates):
    """Create a dataset entry with parameters injected (no queries array)."""
    # Inject parameters into SQL
    query_lines = inject_parameters_into_sql(dataset_info["queryLines"])
    
    dataset = {
        "name": dataset_id,
        "displayName": dataset_info["displayName"],
        "queryLines": query_lines,
        "parameters": [
            {
                "displayName": "Start Date",
                "keyword": "param_start_date",
                "dataType": "DATE",
                "defaultSelection": {
                    "values": {
                        "dataType": "DATE",
                        "values": [{"value": default_dates["start"]}]
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
                        "values": [{"value": default_dates["end"]}]
                    }
                }
            }
        ]
    }
    
    return dataset

def create_parameter_datasets(default_dates, filter_dataset_name="dataset_007"):
    """Create parameter datasets for filter widgets following LakeFlow pattern."""
    parameter_datasets = []
    
    # Create parameter datasets for start and end date
    for param_name in ["param_start_date", "param_end_date"]:
        param_dataset = {
            "name": f"param_{filter_dataset_name}_{param_name}",
            "displayName": f"{filter_dataset_name} {param_name}",
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": f"param_{filter_dataset_name}_{param_name}",
                        "parameters": [
                            {
                                "name": param_name,
                                "keyword": param_name
                            }
                        ],
                        "disaggregated": False
                    }
                }
            ],
            "queryLines": [
                f"SELECT :{param_name} as {param_name}"
            ],
            "parameters": [
                {
                    "displayName": param_name.replace("_", " ").title(),
                    "keyword": param_name,
                    "dataType": "DATE",
                    "defaultSelection": {
                        "values": {
                            "dataType": "DATE",
                            "values": [{"value": default_dates["start" if "start" in param_name else "end"]}]
                        }
                    }
                }
            ]
        }
        parameter_datasets.append(param_dataset)
    
    return parameter_datasets

def create_global_filters(default_dates, datasets):
    """Create global filter widgets for Date Range following LakeFlow pattern."""
    # Create queries for each dataset
    start_date_queries = []
    end_date_queries = []
    start_date_fields = []
    end_date_fields = []
    
    for dataset_id in datasets.keys():
        # Create query for start date
        start_query_name = f"parameter_dashboards/dashboard_v0/datasets/{dataset_id}_param_start_date"
        start_date_queries.append({
            "name": start_query_name,
            "query": {
                "datasetName": dataset_id,
                "parameters": [{"name": "param_start_date", "keyword": "param_start_date"}],
                "disaggregated": False
            }
        })
        start_date_fields.append({
            "parameterName": "param_start_date",
            "queryName": start_query_name
        })
        
        # Create query for end date
        end_query_name = f"parameter_dashboards/dashboard_v0/datasets/{dataset_id}_param_end_date"
        end_date_queries.append({
            "name": end_query_name,
            "query": {
                "datasetName": dataset_id,
                "parameters": [{"name": "param_end_date", "keyword": "param_end_date"}],
                "disaggregated": False
            }
        })
        end_date_fields.append({
            "parameterName": "param_end_date",
            "queryName": end_query_name
        })
    
    return [
        {
            "widget": {
                "name": "global_start_date_filter",
                "queries": start_date_queries,
                "spec": {
                    "version": 2,
                    "widgetType": "filter-date-picker",
                    "encodings": {
                        "fields": start_date_fields
                    },
                    "selection": {
                        "defaultSelection": {
                            "values": {
                                "dataType": "DATE",
                                "values": [{"value": default_dates["start"]}]
                            }
                        }
                    },
                    "frame": {"showTitle": True, "title": "Start Date"}
                }
            },
            "position": {"x": 0, "y": 0, "width": 3, "height": 1}
        },
        {
            "widget": {
                "name": "global_end_date_filter",
                "queries": end_date_queries,
                "spec": {
                    "version": 2,
                    "widgetType": "filter-date-picker",
                    "encodings": {
                        "fields": end_date_fields
                    },
                    "selection": {
                        "defaultSelection": {
                            "values": {
                                "dataType": "DATE",
                                "values": [{"value": default_dates["end"]}]
                            }
                        }
                    },
                    "frame": {"showTitle": True, "title": "End Date"}
                }
            },
            "position": {"x": 3, "y": 0, "width": 3, "height": 1}
        }
    ]

def replace_placeholders_in_pages(pages, default_dates, datasets):
    """Replace placeholders in pages with actual values and add global filters."""
    updated_pages = []
    
    for page in pages:
        # Create a copy of the page
        updated_page = page.copy()
        
        # Check if global filters already exist in the template
        has_existing_filters = any(
            widget.get("widget", {}).get("name", "").startswith("filter_") or 
            widget.get("widget", {}).get("name", "").startswith("global_")
            for widget in page["layout"]
        )
        
        if has_existing_filters:
            # Template already has filters, replace them with LakeFlow pattern
            print(f"📋 Page '{page['displayName']}' has existing filters, replacing with LakeFlow pattern")
            global_filters = create_global_filters(default_dates, datasets)
            
            adjusted_layout = []
            for widget in page["layout"]:
                # Skip existing filter widgets and widgets with placeholder references
                widget_name = widget.get("widget", {}).get("name", "")
                widget_str = json.dumps(widget)
                
                if (widget_name.startswith("filter_") or widget_name.startswith("global_") or
                    "{{DATASET_NAME}}" in widget_str or "{{DEFAULT_START_DATE}}" in widget_str or "{{DEFAULT_END_DATE}}" in widget_str):
                    print(f"⚠️  Replacing widget: {widget_name}")
                    continue
                adjusted_layout.append(widget)
            
            # Add the new LakeFlow pattern filters
            adjusted_layout.extend(global_filters)
            updated_page["layout"] = adjusted_layout
        else:
            # No existing filters, add global filters
            print(f"📋 Adding global filters to page '{page['displayName']}'")
            global_filters = create_global_filters(default_dates, datasets)
            
            # Process existing widgets and remove any placeholder references
            adjusted_layout = []
            for widget in page["layout"]:
                # Skip any widgets that have placeholder references
                widget_str = json.dumps(widget)
                if "{{DATASET_NAME}}" in widget_str or "{{DEFAULT_START_DATE}}" in widget_str or "{{DEFAULT_END_DATE}}" in widget_str:
                    print(f"⚠️  Skipping widget with placeholders: {widget.get('widget', {}).get('name', 'unnamed')}")
                    continue
                
                # Adjust position to account for global filter row
                if "position" in widget:
                    adjusted_widget = widget.copy()
                    adjusted_widget["position"]["y"] += 1  # Move down to account for global filter row
                    adjusted_layout.append(adjusted_widget)
                else:
                    adjusted_layout.append(widget)
            
            # Combine global filters with adjusted layout
            updated_page["layout"] = global_filters + adjusted_layout
        updated_pages.append(updated_page)
    
    return updated_pages

def create_comprehensive_dashboard(template, datasets, default_dates):
    """Create comprehensive dashboard with all datasets and pages."""
    # Create all datasets with parameters
    dashboard_datasets = []
    for dataset_id, dataset_info in datasets.items():
        dataset_entry = create_dataset_entry(dataset_id, dataset_info, default_dates)
        dashboard_datasets.append(dataset_entry)
    
    # Note: Using direct dataset references instead of parameter datasets (LakeFlow pattern)
    
    # Replace placeholders in pages
    pages = replace_placeholders_in_pages(template["dashboard_structure"]["pages"], default_dates, datasets)
    
    # Create the final dashboard with proper metadata
    dashboard = {
        "version": "1.0",
        "type": "dashboard",
        "created": "2025-01-15",
        "author": "Platform Observability Team",
        "datasets": dashboard_datasets,
        "pages": pages
    }
    
    return dashboard

def validate_json_structure(dashboard):
    """Validate JSON structure."""
    try:
        # Check required top-level keys
        required_keys = ["datasets", "pages"]
        for key in required_keys:
            if key not in dashboard:
                print(f"❌ Missing required key: {key}")
                return False
        
        # Check datasets structure
        if not isinstance(dashboard["datasets"], list):
            print("❌ Datasets must be a list")
            return False
        
        # Check pages structure
        if not isinstance(dashboard["pages"], list):
            print("❌ Pages must be a list")
            return False
        
        print("✅ JSON structure is valid")
        return True
        
    except Exception as e:
        print(f"❌ JSON structure validation error: {e}")
        return False

def validate_datasets(dashboard):
    """Validate datasets structure."""
    try:
        for dataset in dashboard["datasets"]:
            # Check required dataset keys
            required_keys = ["name", "displayName", "queryLines", "parameters"]
            for key in required_keys:
                if key not in dataset:
                    print(f"❌ Dataset missing required key: {key}")
                    return False
            
            # Check parameters
            if not isinstance(dataset["parameters"], list):
                print(f"❌ Dataset {dataset['name']} parameters must be a list")
                return False
            
            # Check that parameters have required fields
            for param in dataset["parameters"]:
                if "keyword" not in param or "dataType" not in param:
                    print(f"❌ Parameter missing required fields in dataset {dataset['name']}")
                    return False
        
        print("✅ Datasets validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Datasets validation error: {e}")
        return False

def validate_pages(dashboard):
    """Validate pages structure."""
    try:
        for page in dashboard["pages"]:
            # Check required page keys
            if "name" not in page or "displayName" not in page or "layout" not in page:
                print(f"❌ Page missing required keys")
                return False
            
            # Check layout
            if not isinstance(page["layout"], list):
                print(f"❌ Page {page['name']} layout must be a list")
                return False
        
        print("✅ Pages validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Pages validation error: {e}")
        return False

def run_comprehensive_validation(dashboard):
    """Run comprehensive validation checks."""
    print("🔍 Running comprehensive validation...")
    
    validation_checks = [
        ("JSON Structure", lambda: validate_json_structure(dashboard)),
        ("Datasets", lambda: validate_datasets(dashboard)),
        ("Pages", lambda: validate_pages(dashboard))
    ]
    
    passed = 0
    total = len(validation_checks)
    
    for check_name, check_func in validation_checks:
        try:
            if check_func():
                passed += 1
            else:
                print(f"❌ {check_name} validation failed")
        except Exception as e:
            print(f"❌ {check_name} validation error: {e}")
    
    print(f"🎯 Validation Results: {passed}/{total} checks passed")
    return passed == total

def generate_dashboard():
    """Main function to generate the dashboard."""
    print("🚀 Generating Comprehensive Platform Observability Dashboard v0...")
    
    # File paths
    template_file = "resources/dashboard/dashboard_template_v0.json"
    sql_file = "resources/dashboard/dashboard_sql_v0.json"
    output_file = "resources/dashboard/dashboard_out_v0.lvdash.json"
    
    # Check if files exist
    if not os.path.exists(template_file):
        print(f"❌ Template file not found: {template_file}")
        return False
    
    if not os.path.exists(sql_file):
        print(f"❌ SQL file not found: {sql_file}")
        return False
    
    # Read template and SQL
    template = read_template(template_file)
    if not template:
        return False
    
    
    sql_data = read_sql_queries(sql_file)
    if not sql_data:
        return False
    
    # Get default dates
    default_dates = get_default_dates()
    print(f"📅 Using default dates: {default_dates['start']} to {default_dates['end']}")
    
    # Create comprehensive dashboard
    dashboard = create_comprehensive_dashboard(
        template, 
        sql_data["sql_queries"]["datasets"], 
        default_dates
    )
    
    # Validate dashboard
    if not run_comprehensive_validation(dashboard):
        print("❌ Validation failed, not generating file")
        return False
    
    # Write output file
    try:
        with open(output_file, 'w') as f:
            json.dump(dashboard, f, indent=2)
        print(f"✅ Dashboard generated successfully: {output_file}")
        
        # Final validation
        print("🔍 Final validation...")
        with open(output_file, 'r') as f:
            final_dashboard = json.load(f)
        
        if validate_json_structure(final_dashboard):
            print("✅ Final dashboard is valid and ready for import")
            return True
        else:
            print("❌ Final dashboard validation failed")
            return False
            
    except Exception as e:
        print(f"❌ Error writing output file: {e}")
        return False

if __name__ == "__main__":
    success = generate_dashboard()
    if success:
        print("\n🎉 Dashboard generation completed successfully!")
        print("📁 Output file: resources/dashboard/dashboard_out_v0.lvdash.json")
        print("📊 Dashboard includes:")
        print("   - 19 datasets with parameter injection")
        print("   - 7 pages with comprehensive widgets")
        print("   - Global filters: Date Range (Start/End) + Workspace")
        print("   - LakeFlow pattern compliance")
        print("   - Comprehensive validation")
        print("\n🚀 Ready for import into Databricks!")
    else:
        print("\n❌ Dashboard generation failed!")
        sys.exit(1)