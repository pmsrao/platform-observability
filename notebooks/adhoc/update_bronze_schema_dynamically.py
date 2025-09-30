# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Bronze Schema Update
# MAGIC 
# MAGIC This notebook dynamically updates the bronze table schema to match the current source schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, DecimalType, MapType, BooleanType
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Current Source Schema

# COMMAND ----------

# Get the current schema from the source table
print("=== GETTING CURRENT SOURCE SCHEMA ===")
source_df = spark.table("system.billing.usage")
source_schema = source_df.schema

print("Source table schema:")
source_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract usage_metadata Schema

# COMMAND ----------

# Find the usage_metadata field and extract its schema
usage_metadata_schema = None
for field in source_schema.fields:
    if field.name == "usage_metadata":
        usage_metadata_schema = field.dataType
        break

if usage_metadata_schema:
    print("=== CURRENT usage_metadata SCHEMA ===")
    print(f"Type: {usage_metadata_schema}")
    print(f"Fields: {len(usage_metadata_schema.fields)}")
    
    # List all fields in the usage_metadata struct
    print("\nFields in usage_metadata:")
    for field in usage_metadata_schema.fields:
        print(f"  {field.name}: {field.dataType}")
else:
    print("❌ usage_metadata field not found in source schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Updated CREATE TABLE Statement

# COMMAND ----------

# Generate the CREATE TABLE statement with the current schema
def generate_create_table_sql(usage_metadata_schema):
    """Generate CREATE TABLE SQL with current usage_metadata schema"""
    
    # Convert the schema to SQL string
    def schema_to_sql(data_type):
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
    
    usage_metadata_sql = schema_to_sql(usage_metadata_schema)
    
    create_table_sql = f"""
CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.brz_billing_usage (
    record_id STRING,
    account_id STRING,
    workspace_id STRING,
    sku_name STRING,
    cloud STRING,
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP,
    usage_date DATE,
    custom_tags MAP<STRING, STRING>,
    usage_unit STRING,
    usage_quantity DECIMAL(38,18),
    usage_metadata {usage_metadata_sql},
    identity_metadata STRUCT<
        run_as:STRING,
        owned_by:STRING,
        created_by:STRING
    >,
    record_type STRING,
    ingestion_date DATE,
    billing_origin_product STRING,
    product_features STRUCT<
        sql_tier:STRING,
        dlt_tier:STRING,
        is_serverless:BOOLEAN,
        is_photon:BOOLEAN,
        serving_type:STRING,
        networking:STRUCT<connectivity_type:STRING>,
        ai_runtime:STRUCT<compute_type:STRING>,
        model_serving:STRUCT<offering_type:STRING>,
        ai_gateway:STRUCT<feature_type:STRING>,
        serverless_gpu:STRUCT<workload_type:STRING>
    >,
    usage_type STRING,
    row_hash STRING,
    _loaded_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
"""
    return create_table_sql

if usage_metadata_schema:
    create_table_sql = generate_create_table_sql(usage_metadata_schema)
    print("=== GENERATED CREATE TABLE SQL ===")
    print(create_table_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Bronze Table Schema

# COMMAND ----------

if usage_metadata_schema:
    print("=== UPDATING BRONZE TABLE SCHEMA ===")
    
    # Drop existing table if it exists
    try:
        spark.sql("DROP TABLE IF EXISTS platform_observability.plt_bronze.brz_billing_usage")
        print("✅ Dropped existing table")
    except Exception as e:
        print(f"Warning: Error dropping table: {e}")
    
    # Create table with updated schema
    try:
        spark.sql(create_table_sql)
        print("✅ Created table with updated schema")
        
        # Verify the table was created
        new_table = spark.table("platform_observability.plt_bronze.brz_billing_usage")
        print("✅ Table created successfully")
        print("\nNew table schema:")
        new_table.printSchema()
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        print("You may need to check the generated SQL for syntax errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Schema Compatibility

# COMMAND ----------

print("=== TESTING SCHEMA COMPATIBILITY ===")

try:
    # Test that we can read from source and the schemas match
    source_sample = spark.table("system.billing.usage").limit(1)
    target_table = spark.table("platform_observability.plt_bronze.brz_billing_usage")
    
    # Check if the usage_metadata schemas are compatible
    source_usage_metadata = None
    target_usage_metadata = None
    
    for field in source_sample.schema.fields:
        if field.name == "usage_metadata":
            source_usage_metadata = field.dataType
            break
    
    for field in target_table.schema.fields:
        if field.name == "usage_metadata":
            target_usage_metadata = field.dataType
            break
    
    if source_usage_metadata and target_usage_metadata:
        if str(source_usage_metadata) == str(target_usage_metadata):
            print("✅ Schemas are compatible!")
            print("✅ You can now run the bronze ingestion job")
        else:
            print("❌ Schemas are still not compatible")
            print(f"Source: {source_usage_metadata}")
            print(f"Target: {target_usage_metadata}")
    
except Exception as e:
    print(f"❌ Error testing compatibility: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Updated Schema to File

# COMMAND ----------

# Save the updated schema to a file for future reference
if usage_metadata_schema:
    schema_info = {
        "usage_metadata_schema": str(usage_metadata_schema),
        "fields": [
            {
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable
            }
            for field in usage_metadata_schema.fields
        ],
        "total_fields": len(usage_metadata_schema.fields)
    }
    
    # Save to a JSON file
    schema_json = json.dumps(schema_info, indent=2)
    
    # Write to a file (you can save this to DBFS or a shared location)
    print("=== SCHEMA INFORMATION ===")
    print(schema_json)
    
    # You can also save this to a file for future reference
    # dbutils.fs.put("/tmp/usage_metadata_schema.json", schema_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps

# COMMAND ----------

print("=== NEXT STEPS ===")
print("1. ✅ Bronze table schema has been updated")
print("2. 🔄 Run the bronze ingestion job: bronze_hwm_ingest_job.py")
print("3. 📊 Monitor the job execution")
print("4. 🎯 If successful, proceed with silver layer processing")

print("\n=== TO RUN THE INGESTION JOB ===")
print("Execute the notebook: bronze_hwm_ingest_job.py")
print("Or run the specific function: upsert_billing_usage()")

print("\n=== MONITORING ===")
print("Check the job logs for any remaining issues")
print("Verify data is being ingested correctly")
