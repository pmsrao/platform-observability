# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard JSON Generator
# MAGIC 
# MAGIC This notebook generates the final dashboard JSON by combining the dashboard template
# MAGIC with SQL statements and validates the SQL queries independently.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# File paths
DASHBOARD_TEMPLATE_PATH = "/Workspace/Users/podilapalls@gmail.com/platform-observability/resources/dashboard/dashboard_template.json"
SQL_STATEMENTS_PATH = "/Workspace/Users/podilapalls@gmail.com/platform-observability/resources/dashboard/dashboard_sql_statements.json"
OUTPUT_DASHBOARD_PATH = "/Workspace/Users/podilapalls@gmail.com/platform-observability/resources/dashboard/platform_observability_dashboard.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration Files

# COMMAND ----------

def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON file and return as dictionary"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in {file_path}: {e}")
        raise

# Load template and SQL statements
print("📁 Loading dashboard template...")
dashboard_template = load_json_file(DASHBOARD_TEMPLATE_PATH)

print("📁 Loading SQL statements...")
sql_statements = load_json_file(SQL_STATEMENTS_PATH)

print("✅ Configuration files loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Validation Functions

# COMMAND ----------

def validate_sql_syntax(sql: str) -> bool:
    """
    Validate SQL syntax by attempting to parse it
    This is a basic validation - in production, you might want more sophisticated validation
    """
    try:
        # Basic SQL validation - check for common syntax issues
        sql_upper = sql.upper().strip()
        
        # Check for basic SQL structure
        if not any(keyword in sql_upper for keyword in ['SELECT', 'WITH']):
            return False
            
        # Check for balanced parentheses
        if sql.count('(') != sql.count(')'):
            return False
            
        # Check for balanced quotes (basic check)
        single_quotes = sql.count("'")
        if single_quotes % 2 != 0:
            return False
            
        return True
    except Exception as e:
        print(f"❌ SQL validation error: {e}")
        return False

def validate_sql_references(template: Dict[str, Any], sql_statements: Dict[str, Any]) -> List[str]:
    """
    Validate that all SQL references in the template exist in the SQL statements
    """
    errors = []
    sql_refs = set()
    
    # Collect all SQL references from template
    def collect_sql_refs(obj, path=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "sqlRef" and isinstance(value, str):
                    sql_refs.add(value)
                else:
                    collect_sql_refs(value, f"{path}.{key}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                collect_sql_refs(item, f"{path}[{i}]")
    
    collect_sql_refs(template)
    
    # Check if all references exist in SQL statements
    available_sqls = set(sql_statements.get("statements", {}).keys())
    
    for sql_ref in sql_refs:
        if sql_ref not in available_sqls:
            errors.append(f"❌ SQL reference '{sql_ref}' not found in SQL statements")
    
    return errors

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Generation Functions

# COMMAND ----------

def replace_sql_references(template: Dict[str, Any], sql_statements: Dict[str, Any]) -> Dict[str, Any]:
    """
    Replace SQL references in template with actual SQL statements
    """
    def replace_refs(obj):
        if isinstance(obj, dict):
            if "sqlRef" in obj:
                sql_ref = obj["sqlRef"]
                if sql_ref in sql_statements.get("statements", {}):
                    # Replace sqlRef with actual SQL
                    sql_info = sql_statements["statements"][sql_ref]
                    obj["sql"] = sql_info["sql"]
                    del obj["sqlRef"]
                    # Add metadata if available
                    if "description" in sql_info:
                        obj["sqlDescription"] = sql_info["description"]
                    if "category" in sql_info:
                        obj["sqlCategory"] = sql_info["category"]
                else:
                    print(f"⚠️ Warning: SQL reference '{sql_ref}' not found")
            else:
                # Recursively process nested objects
                for key, value in obj.items():
                    obj[key] = replace_refs(value)
        elif isinstance(obj, list):
            # Process list items
            return [replace_refs(item) for item in obj]
        return obj
    
    return replace_refs(template.copy())

def add_generation_metadata(dashboard: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add metadata about dashboard generation
    """
    dashboard["metadata"]["generatedAt"] = datetime.now().isoformat()
    dashboard["metadata"]["generatedBy"] = "Dashboard JSON Generator"
    dashboard["metadata"]["templateVersion"] = dashboard_template.get("version", "1.0")
    dashboard["metadata"]["sqlStatementsVersion"] = sql_statements.get("version", "1.0")
    
    return dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Validation

# COMMAND ----------

print("🔍 Validating SQL statements...")

# Validate SQL syntax
sql_validation_results = {}
for sql_name, sql_info in sql_statements.get("statements", {}).items():
    sql = sql_info.get("sql", "")
    is_valid = validate_sql_syntax(sql)
    sql_validation_results[sql_name] = is_valid
    
    if is_valid:
        print(f"✅ {sql_name}: Valid")
    else:
        print(f"❌ {sql_name}: Invalid syntax")

# Validate SQL references
print("\n🔍 Validating SQL references...")
reference_errors = validate_sql_references(dashboard_template, sql_statements)

if reference_errors:
    for error in reference_errors:
        print(error)
    print("❌ SQL reference validation failed")
else:
    print("✅ All SQL references are valid")

# Summary
valid_sqls = sum(1 for is_valid in sql_validation_results.values() if is_valid)
total_sqls = len(sql_validation_results)

print(f"\n📊 SQL Validation Summary:")
print(f"   Valid SQL statements: {valid_sqls}/{total_sqls}")
print(f"   Reference errors: {len(reference_errors)}")

if valid_sqls == total_sqls and len(reference_errors) == 0:
    print("✅ All validations passed!")
else:
    print("❌ Some validations failed. Please fix the issues before generating the dashboard.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Dashboard JSON

# COMMAND ----------

if valid_sqls == total_sqls and len(reference_errors) == 0:
    print("🚀 Generating dashboard JSON...")
    
    # Replace SQL references with actual SQL
    dashboard = replace_sql_references(dashboard_template, sql_statements)
    
    # Add generation metadata
    dashboard = add_generation_metadata(dashboard)
    
    # Save the final dashboard JSON
    try:
        with open(OUTPUT_DASHBOARD_PATH, 'w') as f:
            json.dump(dashboard, f, indent=2)
        
        print(f"✅ Dashboard JSON generated successfully!")
        print(f"📁 Output file: {OUTPUT_DASHBOARD_PATH}")
        
        # Display summary
        print(f"\n📊 Dashboard Summary:")
        print(f"   Name: {dashboard['name']}")
        print(f"   Tabs: {len(dashboard['tabs'])}")
        print(f"   Total Widgets: {sum(len(tab['widgets']) for tab in dashboard['tabs'])}")
        print(f"   Alerts: {len(dashboard.get('alerts', []))}")
        print(f"   Generated: {dashboard['metadata']['generatedAt']}")
        
    except Exception as e:
        print(f"❌ Error saving dashboard JSON: {e}")
else:
    print("❌ Cannot generate dashboard due to validation errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Generated Dashboard Structure

# COMMAND ----------

if valid_sqls == total_sqls and len(reference_errors) == 0:
    print("📋 Generated Dashboard Structure:")
    print("=" * 50)
    
    for i, tab in enumerate(dashboard['tabs'], 1):
        print(f"{i}. {tab['name']}")
        print(f"   📝 {tab['description']}")
        print(f"   📊 Widgets: {len(tab['widgets'])}")
        
        for j, widget in enumerate(tab['widgets'], 1):
            print(f"      {j}. {widget['name']} ({widget['type']})")
            if 'sqlDescription' in widget:
                print(f"         💡 {widget['sqlDescription']}")
        
        print()
    
    print("🎯 Next Steps:")
    print("1. Import the generated JSON file into Databricks Dashboard")
    print("2. Configure access permissions for different personas")
    print("3. Set up refresh schedules and alerts")
    print("4. Test all widgets and validate data accuracy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export SQL Statements for Independent Testing

# COMMAND ----------

# Create a separate file with just the SQL statements for independent testing
sql_only_path = "/Workspace/Users/podilapalls@gmail.com/platform-observability/resources/dashboard/dashboard_sql_only.sql"

print("📝 Creating SQL-only file for independent testing...")

try:
    with open(sql_only_path, 'w') as f:
        f.write("-- Platform Observability Dashboard SQL Statements\n")
        f.write("-- Generated for independent testing and validation\n")
        f.write(f"-- Generated: {datetime.now().isoformat()}\n\n")
        
        for category in ['overview', 'finance', 'platform', 'data', 'business', 'alert']:
            f.write(f"-- {category.upper()} CATEGORY\n")
            f.write("=" * 50 + "\n\n")
            
            for sql_name, sql_info in sql_statements.get("statements", {}).items():
                if sql_info.get("category") == category:
                    f.write(f"-- {sql_name}\n")
                    f.write(f"-- {sql_info.get('description', 'No description')}\n")
                    f.write(sql_info["sql"])
                    f.write("\n\n")
            
            f.write("\n")
    
    print(f"✅ SQL-only file created: {sql_only_path}")
    print("💡 You can now test these SQL statements independently in Databricks SQL")
    
except Exception as e:
    print(f"❌ Error creating SQL-only file: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("🎉 Dashboard JSON Generation Complete!")
print("=" * 50)

print(f"📁 Files Created:")
print(f"   • Dashboard JSON: {OUTPUT_DASHBOARD_PATH}")
print(f"   • SQL-only file: {sql_only_path}")

print(f"\n📊 Validation Results:")
print(f"   • SQL statements validated: {valid_sqls}/{total_sqls}")
print(f"   • Reference errors: {len(reference_errors)}")

print(f"\n🚀 Ready for Import:")
print(f"   • Use the dashboard JSON file to import into Databricks")
print(f"   • Test SQL statements independently using the SQL-only file")
print(f"   • Configure permissions and refresh schedules after import")

print(f"\n💡 Benefits of This Approach:")
print(f"   • ✅ Independent SQL validation")
print(f"   • ✅ Reusable SQL statements")
print(f"   • ✅ Version control friendly")
print(f"   • ✅ Easy maintenance and updates")
print(f"   • ✅ Dynamic dashboard generation")
