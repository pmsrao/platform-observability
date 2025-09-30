# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Mismatch Diagnosis
# MAGIC 
# MAGIC This notebook diagnoses the schema mismatch issue in the billing usage ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Source Schema

# COMMAND ----------

# Check the current schema of the source table
print("=== SOURCE TABLE SCHEMA (system.billing.usage) ===")
source_schema = spark.table("system.billing.usage").schema
for field in source_schema.fields:
    if field.name == "usage_metadata":
        print(f"Field: {field.name}")
        print(f"Type: {field.dataType}")
        print(f"Nullable: {field.nullable}")
        print("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Target Schema

# COMMAND ----------

# Check the current schema of the target table
print("=== TARGET TABLE SCHEMA (brz_billing_usage) ===")
try:
    target_schema = spark.table("platform_observability.plt_bronze.brz_billing_usage").schema
    for field in target_schema.fields:
        if field.name == "usage_metadata":
            print(f"Field: {field.name}")
            print(f"Type: {field.dataType}")
            print(f"Nullable: {field.nullable}")
            print("---")
except Exception as e:
    print(f"Error reading target table: {e}")
    print("Target table may not exist yet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Schemas

# COMMAND ----------

# Compare the usage_metadata struct schemas
print("=== SCHEMA COMPARISON ===")

# Get source usage_metadata fields
source_usage_metadata = None
for field in source_schema.fields:
    if field.name == "usage_metadata":
        source_usage_metadata = field.dataType
        break

# Get target usage_metadata fields  
target_usage_metadata = None
try:
    target_schema = spark.table("platform_observability.plt_bronze.brz_billing_usage").schema
    for field in target_schema.fields:
        if field.name == "usage_metadata":
            target_usage_metadata = field.dataType
            break
except:
    print("Target table doesn't exist - will show expected schema from bronze_tables.sql")

if source_usage_metadata and target_usage_metadata:
    print("SOURCE usage_metadata fields:")
    for field in source_usage_metadata.fields:
        print(f"  {field.name}: {field.dataType}")
    
    print("\nTARGET usage_metadata fields:")
    for field in target_usage_metadata.fields:
        print(f"  {field.name}: {field.dataType}")
    
    # Find differences
    source_fields = {f.name: f.dataType for f in source_usage_metadata.fields}
    target_fields = {f.name: f.dataType for f in target_usage_metadata.fields}
    
    print("\n=== DIFFERENCES ===")
    
    # Fields in source but not in target
    missing_in_target = set(source_fields.keys()) - set(target_fields.keys())
    if missing_in_target:
        print(f"Fields in source but missing in target: {missing_in_target}")
    
    # Fields in target but not in source
    missing_in_source = set(target_fields.keys()) - set(source_fields.keys())
    if missing_in_source:
        print(f"Fields in target but missing in source: {missing_in_source}")
    
    # Type mismatches
    type_mismatches = []
    for field_name in set(source_fields.keys()) & set(target_fields.keys()):
        if str(source_fields[field_name]) != str(target_fields[field_name]):
            type_mismatches.append((field_name, source_fields[field_name], target_fields[field_name]))
    
    if type_mismatches:
        print("Type mismatches:")
        for field_name, source_type, target_type in type_mismatches:
            print(f"  {field_name}: source={source_type} vs target={target_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Check

# COMMAND ----------

# Check a sample of the source data to see what fields are actually present
print("=== SAMPLE SOURCE DATA ===")
sample_data = spark.table("system.billing.usage").select("usage_metadata").limit(5)
sample_data.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Schema Evolution SQL

# COMMAND ----------

# Generate ALTER TABLE statements to add missing fields
if source_usage_metadata and target_usage_metadata:
    source_fields = {f.name: f.dataType for f in source_usage_metadata.fields}
    target_fields = {f.name: f.dataType for f in target_usage_metadata.fields}
    
    missing_in_target = set(source_fields.keys()) - set(target_fields.keys())
    
    if missing_in_target:
        print("=== ALTER TABLE STATEMENTS TO ADD MISSING FIELDS ===")
        for field_name in sorted(missing_in_target):
            field_type = source_fields[field_name]
            print(f"ALTER TABLE platform_observability.plt_bronze.brz_billing_usage ALTER COLUMN usage_metadata.{field_name} ADD COLUMN {field_name} {field_type};")
    else:
        print("No missing fields found - schema mismatch might be due to type differences")
