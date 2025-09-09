# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Silver Transformations
# MAGIC 
# MAGIC This notebook validates that all silver table transformations include all required columns from the DDL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check DDL vs Transformations

# COMMAND ----------

# Read the DDL file to get all column definitions
with open('/Workspace/Users/podilapalls@gmail.com/platform-observability/sql/silver/silver_tables.sql', 'r') as f:
    ddl_content = f.read()

print("🔍 Checking DDL for all silver tables...")

# Extract table definitions
import re

# Find all CREATE TABLE statements
table_pattern = r'CREATE TABLE.*?slv_(\w+).*?\((.*?)\) USING DELTA;'
tables = re.findall(table_pattern, ddl_content, re.DOTALL)

print(f"Found {len(tables)} silver tables in DDL:")
for table_name, columns in tables:
    print(f"  - slv_{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Each Table's Columns

# COMMAND ----------

for table_name, columns in tables:
    print(f"\n📋 Table: slv_{table_name}")
    print("Columns in DDL:")
    
    # Parse columns (simple approach - split by comma and clean)
    column_lines = [line.strip() for line in columns.split('\n') if line.strip() and not line.strip().startswith('--')]
    
    for line in column_lines:
        if line and not line.startswith('--'):
            # Extract column name (first word)
            col_name = line.split()[0] if line.split() else ""
            if col_name:
                print(f"  - {col_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ DDL validation complete!")
print("\nNext steps:")
print("1. Compare these columns with the transformations in silver_hwm_build_job.py")
print("2. Ensure all columns are included in the .select() statements")
print("3. Add any missing columns with appropriate defaults")
