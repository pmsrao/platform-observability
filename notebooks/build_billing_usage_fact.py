# Databricks notebook source
# MAGIC %md
# MAGIC # Build Billing Usage Fact Table
# MAGIC 
# MAGIC This notebook builds the `gld_fact_billing_usage` fact table from the Silver layer.
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC The `gld_fact_billing_usage` table provides detailed billing usage information with additional attributes:
# MAGIC - **Unique by record_id**: Each record_id from billing.usage is unique in this table
# MAGIC - **Additional attributes**: billing_origin_product, is_serverless, usage_type
# MAGIC - **Business context**: All tag-derived attributes for analytics convenience
# MAGIC - **SCD2-aligned**: Proper temporal joins with dimension tables
# MAGIC 
# MAGIC ## Key Features
# MAGIC - Incremental processing using task-based offsets
# MAGIC - SCD2 temporal joins for historical accuracy
# MAGIC - Comprehensive debugging and logging
# MAGIC - Cost calculation with pricing data
# MAGIC - Deduplication by record_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
import sys
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DecimalType

# Add libs to path
sys.path.append('/Workspace/Repos/your-repo/platform-observability/libs')

# Import custom modules
from config import Config
from billing_usage_fact_builder import BillingUsageFactBuilder, BillingUsageFactBuilderFactory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration and Spark Session

# COMMAND ----------

# Get configuration
config = Config.get_config()
print(f"Environment: {config}")
print(f"Catalog: {config.catalog}")
print(f"Gold Schema: {config.gold_schema}")
print(f"Silver Schema: {config.silver_schema}")

# Get Spark session
spark = SparkSession.getActiveSession()
print(f"Spark Session: {spark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Prerequisites

# COMMAND ----------

# Check if required tables exist
required_tables = [
    f"{config.catalog}.{config.silver_schema}.slv_usage_txn",
    f"{config.catalog}.{config.silver_schema}.slv_price_scd",
    f"{config.catalog}.{config.gold_schema}.gld_dim_workspace",
    f"{config.catalog}.{config.gold_schema}.gld_dim_entity",
    f"{config.catalog}.{config.gold_schema}.gld_dim_cluster",
    f"{config.catalog}.{config.gold_schema}.gld_dim_sku"
]

print("Checking required tables...")
for table in required_tables:
    try:
        count = spark.table(table).count()
        print(f"✅ {table}: {count:,} records")
    except Exception as e:
        print(f"❌ {table}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Fact Table (if not exists)

# COMMAND ----------

# Create the fact table using SQL parameterizer
from sql_parameterizer import SQLParameterizer

sql_param = SQLParameterizer(spark, config)
print("Creating gld_fact_billing_usage table...")

try:
    # Execute the DDL for the fact table
    sql_param.execute_bootstrap_sql("gold/gold_facts", 
                                   catalog=config.catalog, 
                                   gold_schema=config.gold_schema)
    print("✅ Fact table DDL executed successfully")
except Exception as e:
    print(f"❌ Error creating fact table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the Fact Table

# COMMAND ----------

# Create the fact builder using factory
print("Initializing BillingUsageFactBuilder...")
builder = BillingUsageFactBuilderFactory.create_builder(spark, config)

# Build the fact table
print("Building gld_fact_billing_usage fact table...")
result = builder.build()

if result:
    print("✅ Successfully built gld_fact_billing_usage fact table")
else:
    print("❌ Failed to build gld_fact_billing_usage fact table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Results

# COMMAND ----------

# Check the fact table
fact_table = f"{config.catalog}.{config.gold_schema}.gld_fact_billing_usage"

try:
    # Get record count
    total_count = spark.table(fact_table).count()
    print(f"Total records in {fact_table}: {total_count:,}")
    
    # Show sample data
    print("\nSample data:")
    spark.table(fact_table).select(
        "record_id", "date_key", "workspace_key", "entity_key", 
        "billing_origin_product", "is_serverless", "usage_cost", "usage_quantity"
    ).show(10, truncate=False)
    
    # Check for duplicates by record_id
    duplicate_count = (spark.table(fact_table)
                      .groupBy("record_id")
                      .count()
                      .filter("count > 1")
                      .count())
    
    if duplicate_count == 0:
        print("✅ No duplicate record_ids found")
    else:
        print(f"⚠️ Found {duplicate_count} duplicate record_ids")
    
    # Check data quality
    null_record_ids = spark.table(fact_table).filter("record_id IS NULL").count()
    if null_record_ids == 0:
        print("✅ No null record_ids found")
    else:
        print(f"⚠️ Found {null_record_ids} null record_ids")
        
except Exception as e:
    print(f"❌ Error validating results: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Use the builder's built-in validation methods
print("Running comprehensive data quality checks using builder validation...")

try:
    # Get validation results from the builder
    validation_results = builder.validate_data_quality()
    
    print("Data Quality Validation Results:")
    print("=" * 50)
    
    # Check for duplicates
    duplicate_count = validation_results.get("duplicate_record_ids", 0)
    print(f"Duplicate record_ids: {duplicate_count}")
    
    # Check for null record_ids
    null_count = validation_results.get("null_record_ids", 0)
    print(f"Null record_ids: {null_count}")
    
    # Check for negative costs
    negative_costs = validation_results.get("negative_costs", 0)
    print(f"Records with negative costs: {negative_costs}")
    
    # Check for negative quantities
    negative_quantities = validation_results.get("negative_quantities", 0)
    print(f"Records with negative quantities: {negative_quantities}")
    
    # Check is_serverless distribution
    serverless_dist = validation_results.get("serverless_distribution", {})
    print("is_serverless value distribution:")
    for value, count in serverless_dist.items():
        print(f"  {value}: {count:,}")
    
    # Check billing_origin_product distribution
    product_dist = validation_results.get("product_distribution", {})
    print("billing_origin_product distribution (top 10):")
    for product, count in product_dist.items():
        print(f"  {product}: {count:,}")
    
    # Check date range
    date_range = validation_results.get("date_range", {})
    if date_range:
        print(f"Date range: {date_range.get('min')} to {date_range.get('max')}")
    
    # Overall validation status
    overall_status = validation_results.get("overall_status", False)
    print(f"Overall validation status: {'✅ PASSED' if overall_status else '❌ FAILED'}")
    
except Exception as e:
    print(f"❌ Error in data quality checks: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Statistics

# COMMAND ----------

# Get processing statistics from the builder
print("Getting processing statistics...")

try:
    stats = builder.get_processing_stats()
    
    print("Processing Statistics:")
    print("=" * 50)
    
    # Total records
    total_records = stats.get("total_records", 0)
    print(f"Total records: {total_records:,}")
    
    # Daily counts (last 30 days)
    daily_counts = stats.get("daily_counts", {})
    print("Daily record counts (last 30 days):")
    for date_key, count in list(daily_counts.items())[:10]:  # Show first 10
        print(f"  {date_key}: {count:,}")
    
    # Cost statistics
    cost_stats = stats.get("cost_statistics", {})
    if cost_stats:
        print("Cost Statistics:")
        print(f"  Total cost: ${cost_stats.get('total_cost', 0):,.2f}")
        print(f"  Average cost: ${cost_stats.get('avg_cost', 0):,.4f}")
        print(f"  Min cost: ${cost_stats.get('min_cost', 0):,.4f}")
        print(f"  Max cost: ${cost_stats.get('max_cost', 0):,.4f}")
    
    # Usage quantity statistics
    quantity_stats = stats.get("quantity_statistics", {})
    if quantity_stats:
        print("Usage Quantity Statistics:")
        print(f"  Total quantity: {quantity_stats.get('total_quantity', 0):,.2f}")
        print(f"  Average quantity: {quantity_stats.get('avg_quantity', 0):,.4f}")
        print(f"  Min quantity: {quantity_stats.get('min_quantity', 0):,.4f}")
        print(f"  Max quantity: {quantity_stats.get('max_quantity', 0):,.4f}")
    
except Exception as e:
    print(f"❌ Error getting processing stats: {str(e)}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization

# COMMAND ----------

# Optimize the table for better query performance
print("Optimizing table for performance...")

try:
    # Z-ORDER by commonly queried columns
    spark.sql(f"""
        OPTIMIZE {fact_table} 
        ZORDER BY (date_key, workspace_key, entity_key, billing_origin_product)
    """)
    print("✅ Table optimized with Z-ORDER")
    
    # Collect statistics
    spark.sql(f"""
        ANALYZE TABLE {fact_table} 
        COMPUTE STATISTICS FOR ALL COLUMNS
    """)
    print("✅ Statistics collected")
    
except Exception as e:
    print(f"❌ Error optimizing table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("BILLING USAGE FACT TABLE BUILD SUMMARY")
print("=" * 80)
print(f"Table: {fact_table}")
print(f"Build Status: {'✅ SUCCESS' if result else '❌ FAILED'}")
print(f"Total Records: {total_count:,}")
print(f"Unique record_ids: {'✅ YES' if duplicate_count == 0 else '❌ NO'}")
print(f"Data Quality: {'✅ PASSED' if negative_costs == 0 and negative_quantities == 0 else '⚠️ ISSUES FOUND'}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Schedule the job**: Add this notebook to your daily workflow
# MAGIC 2. **Monitor performance**: Check processing times and resource usage
# MAGIC 3. **Create dashboards**: Use this fact table for billing analytics
# MAGIC 4. **Set up alerts**: Monitor for data quality issues
# MAGIC 
# MAGIC ## Usage Examples
# MAGIC 
# MAGIC ```sql
# MAGIC -- Top cost drivers by billing origin product
# MAGIC SELECT 
# MAGIC     billing_origin_product,
# MAGIC     SUM(usage_cost) as total_cost,
# MAGIC     COUNT(*) as record_count
# MAGIC FROM platform_observability.plt_gold.gld_fact_billing_usage
# MAGIC WHERE date_key >= 20240101
# MAGIC GROUP BY billing_origin_product
# MAGIC ORDER BY total_cost DESC;
# MAGIC 
# MAGIC -- Serverless vs non-serverless usage
# MAGIC SELECT 
# MAGIC     is_serverless,
# MAGIC     SUM(usage_cost) as total_cost,
# MAGIC     AVG(usage_cost) as avg_cost
# MAGIC FROM platform_observability.plt_gold.gld_fact_billing_usage
# MAGIC WHERE date_key >= 20240101
# MAGIC GROUP BY is_serverless;
# MAGIC ```
