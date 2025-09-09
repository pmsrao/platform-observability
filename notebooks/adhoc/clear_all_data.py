# Databricks notebook source
# MAGIC %md
# MAGIC # Clear All Data - Silver and Gold Tables
# MAGIC 
# MAGIC This notebook clears all data from Silver and Gold tables and resets processing state.
# MAGIC 
# MAGIC **⚠️ WARNING: This will delete ALL data from Silver and Gold layers!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import from libs package (cloud-agnostic approach)
from libs.path_setup import setup_paths_and_import_config

# Setup paths and import Config
Config = setup_paths_and_import_config()
config = Config.get_config()

print(f"📋 Configuration loaded:")
print(f"   Catalog: {config.catalog}")
print(f"   Bronze Schema: {config.bronze_schema}")
print(f"   Silver Schema: {config.silver_schema}")
print(f"   Gold Schema: {config.gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Clear Silver Tables

# COMMAND ----------

# Silver tables to clear
silver_tables = [
    "slv_workspace",
    "slv_entity_latest", 
    "slv_clusters",
    "slv_usage_txn",
    "slv_job_run_timeline",
    "slv_job_task_run_timeline",
    "slv_jobs_scd",
    "slv_pipelines_scd",
    "slv_price_scd"
]

print("🗑️ Clearing Silver tables...")
for table in silver_tables:
    try:
        table_name = f"{config.catalog}.{config.silver_schema}.{table}"
        print(f"   Clearing {table_name}...")
        spark.sql(f"DELETE FROM {table_name}")
        print(f"   ✅ {table_name} cleared")
    except Exception as e:
        print(f"   ❌ Error clearing {table_name}: {str(e)}")

print("✅ Silver tables cleared!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Clear Gold Tables

# COMMAND ----------

# Gold tables to clear
gold_tables = [
    "gld_dim_workspace",
    "gld_dim_cluster", 
    "gld_dim_entity",
    "gld_dim_sku",
    "gld_fact_usage_priced_day",
    "gld_fact_entity_cost",
    "gld_fact_run_cost",
    "gld_fact_run_status_cost",
    "gld_fact_runs_finished_day"
]

print("🗑️ Clearing Gold tables...")
for table in gold_tables:
    try:
        table_name = f"{config.catalog}.{config.gold_schema}.{table}"
        print(f"   Clearing {table_name}...")
        spark.sql(f"DELETE FROM {table_name}")
        print(f"   ✅ {table_name} cleared")
    except Exception as e:
        print(f"   ❌ Error clearing {table_name}: {str(e)}")

print("✅ Gold tables cleared!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Reset Processing State

# COMMAND ----------

# Clear processing state tables
processing_tables = [
    "_cdf_processing_offsets"
]

print("🔄 Resetting processing state...")
for table in processing_tables:
    try:
        table_name = f"{config.catalog}.{config.silver_schema}.{table}"
        print(f"   Clearing {table_name}...")
        spark.sql(f"DELETE FROM {table_name}")
        print(f"   ✅ {table_name} cleared")
    except Exception as e:
        print(f"   ❌ Error clearing {table_name}: {str(e)}")

print("✅ Processing state reset!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Tables are Empty

# COMMAND ----------

print("🔍 Verifying tables are empty...")

# Check Silver tables
print("\n📊 Silver Tables:")
for table in silver_tables:
    try:
        table_name = f"{config.catalog}.{config.silver_schema}.{table}"
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
        print(f"   {table}: {count} records")
    except Exception as e:
        print(f"   {table}: Error - {str(e)}")

# Check Gold tables
print("\n📊 Gold Tables:")
for table in gold_tables:
    try:
        table_name = f"{config.catalog}.{config.gold_schema}.{table}"
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
        print(f"   {table}: {count} records")
    except Exception as e:
        print(f"   {table}: Error - {str(e)}")

# Check processing state
print("\n📊 Processing State:")
for table in processing_tables:
    try:
        table_name = f"{config.catalog}.{config.silver_schema}.{table}"
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
        print(f"   {table}: {count} records")
    except Exception as e:
        print(f"   {table}: Error - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Summary

# COMMAND ----------

print("🎉 CLEAR ALL DATA COMPLETED!")
print("\n📋 Summary:")
print("   ✅ Silver tables cleared")
print("   ✅ Gold tables cleared") 
print("   ✅ Processing state reset")
print("\n🚀 Next Steps:")
print("   1. Run Bronze HWM Ingest Job")
print("   2. Run Silver HWM Build Job")
print("   3. Run Gold HWM Build Job")
print("\n💡 All tables are now ready for fresh data processing!")

# COMMAND ----------
