# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Updated DDL
# MAGIC 
# MAGIC This notebook generates the exact DDL needed to fix the schema mismatch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Current Source Schema

# COMMAND ----------

# Get the current schema from the source table
print("=== GETTING CURRENT SOURCE SCHEMA ===")
source_df = spark.table("system.billing.usage")
source_schema = source_df.schema

# Find the problematic fields
usage_metadata_schema = None
product_features_schema = None

for field in source_schema.fields:
    if field.name == "usage_metadata":
        usage_metadata_schema = field.dataType
    elif field.name == "product_features":
        product_features_schema = field.dataType

print("=== CURRENT usage_metadata SCHEMA ===")
if usage_metadata_schema:
    print(f"Type: {usage_metadata_schema}")
    print(f"Fields: {len(usage_metadata_schema.fields)}")
    for field in usage_metadata_schema.fields:
        print(f"  {field.name}: {field.dataType}")

print("\n=== CURRENT product_features SCHEMA ===")
if product_features_schema:
    print(f"Type: {product_features_schema}")
    print(f"Fields: {len(product_features_schema.fields)}")
    for field in product_features_schema.fields:
        print(f"  {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Updated DDL

# COMMAND ----------

def schema_to_sql(data_type):
    """Convert Spark DataType to SQL string"""
    if isinstance(data_type, StructType):
        fields = []
        for field in data_type.fields:
            field_sql = f"{field.name}:{schema_to_sql(field.dataType)}"
            fields.append(field_sql)
        return f"STRUCT<{','.join(fields)}>"
    elif isinstance(data_type, MapType):
        key_type = schema_to_sql(data_type.keyType)
        value_type = schema_to_sql(data_type.valueType)
        return f"MAP<{key_type},{value_type}>"
    elif isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    else:
        return str(data_type).upper()

# Generate the updated DDL
if usage_metadata_schema and product_features_schema:
    usage_metadata_sql = schema_to_sql(usage_metadata_schema)
    product_features_sql = schema_to_sql(product_features_schema)
    
    updated_ddl = f"""
-- Updated Bronze Tables Bootstrap
-- This file creates empty bronze tables with CDF enabled
-- Naming convention: brz_[source_schema]_[table_name]

-- Billing Usage Table
CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.brz_billing_usage (
    record_id STRING,                    -- Unique identifier from source
    account_id STRING,                   -- Account identifier
    workspace_id STRING,                 -- Workspace identifier (STRING, not BIGINT)
    sku_name STRING,
    cloud STRING,
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP,
    usage_date DATE,
    custom_tags MAP<STRING, STRING>,     -- Custom tags from source
    usage_unit STRING,
    usage_quantity DECIMAL(38,18),       -- DECIMAL, not DOUBLE
    usage_metadata {usage_metadata_sql},
    identity_metadata STRUCT<
        run_as:STRING,
        owned_by:STRING,
        created_by:STRING
    >,
    record_type STRING,
    ingestion_date DATE,
    billing_origin_product STRING,
    product_features {product_features_sql},
    usage_type STRING,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
"""
    
    print("=== UPDATED DDL ===")
    print(updated_ddl)
    
    # Save to a file for easy copy-paste
    print("\n=== COPY THIS DDL TO EXECUTE ===")
    print("=" * 80)
    print(updated_ddl)
    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Just the ALTER TABLE statements

# COMMAND ----------

# If you prefer to alter the existing table instead of recreating
if usage_metadata_schema and product_features_schema:
    print("=== ALTERNATIVE: ALTER TABLE STATEMENTS ===")
    print("If you prefer to alter the existing table instead of recreating:")
    print()
    print("-- First, drop the existing table")
    print("DROP TABLE IF EXISTS platform_observability.plt_bronze.brz_billing_usage;")
    print()
    print("-- Then run the CREATE TABLE statement above")
    print("-- This is the safest approach for development")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Fix Commands

# COMMAND ----------

print("=== QUICK FIX COMMANDS ===")
print("1. Drop existing table:")
print("   DROP TABLE IF EXISTS platform_observability.plt_bronze.brz_billing_usage;")
print()
print("2. Run the CREATE TABLE statement from above")
print()
print("3. Test the fix:")
print("   SELECT COUNT(*) FROM platform_observability.plt_bronze.brz_billing_usage;")
print()
print("4. Run your bronze ingestion job again")
