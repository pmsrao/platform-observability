"""
Billing Usage Fact Builder

This module provides the BillingUsageFactBuilder class for building the 
gld_fact_billing_usage fact table from the Silver layer.

The BillingUsageFactBuilder creates a detailed billing usage fact table with:
- Unique record_id from billing.usage (unique in this table)
- Additional billing attributes (billing_origin_product, is_serverless, usage_type)
- Business context attributes for analytics convenience
- SCD2-aligned temporal joins with dimension tables
- Comprehensive debugging and monitoring

Author: Platform Observability Team
Version: 1.0
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DecimalType

from config import Config
from .processing_state import get_last_processed_timestamp, commit_processing_state, get_task_name


class FactBuilder:
    """Base class for building fact tables"""
    
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config
        self.catalog = config.catalog
        self.gold_schema = config.gold_schema
        self.silver_schema = config.silver_schema
    
    def get_table_name(self, table_name: str) -> str:
        """Get fully qualified table name"""
        return f"{self.catalog}.{self.gold_schema}.{table_name}"
    
    def get_task_name(self, fact_table_name: str) -> str:
        """Get standardized task name for this fact table"""
        return get_task_name(fact_table_name)
    
    def get_incremental_data(self, source_table: str, fact_table_name: str) -> DataFrame:
        """
        Get incremental data from Silver layer using task-based processing state.
        
        Args:
            source_table (str): Source table name (e.g., 'slv_usage_txn')
            fact_table_name (str): Fact table name (e.g., 'gld_fact_billing_usage')
        
        Returns:
            DataFrame: Incremental data to process
        """
        task_name = self.get_task_name(fact_table_name)
        
        # Get last processed state for this task
        last_timestamp, last_version = get_last_processed_timestamp(
            self.spark, source_table, task_name, "silver"
        )
        
        # Read from Silver table
        silver_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.{source_table}")
        
        if last_timestamp is None:
            # First run - process all data
            print(f"First run for {task_name} - processing all data")
            return silver_df
        else:
            # Incremental run - process only new data
            print(f"Incremental run for {task_name} - processing data after {last_timestamp}")
            return silver_df.filter(F.col("ingestion_date") > last_timestamp)
    
    def commit_processing_state(self, source_table: str, fact_table_name: str, 
                              max_timestamp, max_version=None):
        """
        Commit processing state for this task.
        
        Args:
            source_table (str): Source table name
            fact_table_name (str): Fact table name
            max_timestamp: Maximum timestamp processed
            max_version: Maximum version processed (optional)
        """
        task_name = self.get_task_name(fact_table_name)
        commit_processing_state(
            self.spark, source_table, max_timestamp, max_version, task_name, "silver"
        )
        print(f"Committed processing state for {task_name}: {max_timestamp}")
    
    def get_dimension_key(self, dimension_table: str, natural_key_conditions: Dict[str, str]) -> DataFrame:
        """Get surrogate key from dimension table based on natural key conditions"""
        conditions = []
        for key, value in natural_key_conditions.items():
            conditions.append(f"{key} = '{value}'")
        
        where_clause = " AND ".join(conditions)
        
        return self.spark.sql(f"""
            SELECT * FROM {self.catalog}.{self.gold_schema}.{dimension_table}
            WHERE {where_clause}
        """)
    
    def upsert_fact(self, df: DataFrame, table_name: str, business_keys: List[str]) -> bool:
        """
        Upsert fact table using business keys
        
        Args:
            df (DataFrame): DataFrame to upsert
            table_name (str): Target table name
            business_keys (List[str]): List of business key columns for merge condition
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            full_table_name = self.get_table_name(table_name)
            
            # Create temporary view for merge
            df.createOrReplaceTempView("temp_fact")
            
            # Build merge condition
            merge_conditions = []
            for key in business_keys:
                merge_conditions.append(f"target.{key} = source.{key}")
            merge_condition = " AND ".join(merge_conditions)
            
            # Perform merge
            merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING temp_fact AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            
            self.spark.sql(merge_sql)
            return True
            
        except Exception as e:
            print(f"Error upserting fact {table_name}: {str(e)}")
            return False


class BillingUsageFactBuilder(FactBuilder):
    """
    Builder for billing usage fact table with detailed billing attributes.
    
    This builder creates the gld_fact_billing_usage fact table which provides:
    - Unique record_id from billing.usage (unique in this table)
    - Additional billing attributes (billing_origin_product, is_serverless, usage_type)
    - Business context attributes for analytics convenience
    - SCD2-aligned temporal joins with dimension tables
    - Comprehensive debugging and monitoring
    """
    
    def build(self) -> bool:
        """
        Build billing usage fact table from Silver layer.
        
        This method:
        1. Gets incremental data from slv_usage_txn using task-based processing state
        2. Joins with pricing data to calculate usage_cost
        3. Joins with dimension tables using SCD2 temporal logic
        4. Deduplicates by record_id to ensure uniqueness
        5. Transforms columns for the billing usage fact table
        6. Upserts the fact table using record_id as the unique key
        7. Commits processing state for incremental processing
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print("Starting BillingUsageFactBuilder.build()...")
            
            # Get incremental data using task-based processing state
            silver_df = self.get_incremental_data("slv_usage_txn", "gld_fact_billing_usage")
            
            # Check if there's any data to process
            record_count = silver_df.count()
            print(f"Found {record_count} records to process for gld_fact_billing_usage")
            
            if record_count == 0:
                print("No new data to process for gld_fact_billing_usage")
                return True
            
            # Get price data for cost calculation
            print("Loading price data for cost calculation...")
            silver_price_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_price_scd")
            
            # Get dimension keys
            print("Loading dimension tables...")
            workspace_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_workspace")
            entity_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_entity")
            cluster_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_cluster").drop("cloud")
            sku_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_sku")
            
            print(f"Dimension table counts - Workspace: {workspace_dim.count()}, Entity: {entity_dim.count()}, Cluster: {cluster_dim.count()}, SKU: {sku_dim.count()}")
            
            # Join with price data to calculate usage_cost
            print("Joining with price data to calculate usage cost...")
            silver_with_cost = (silver_df.join(silver_price_df, 
                                            (silver_df.sku_name == silver_price_df.sku_name) & 
                                            (silver_df.account_id == silver_price_df.account_id) & 
                                            (silver_df.usage_unit == silver_price_df.usage_unit) & 
                                            (silver_df.usage_start_time >= silver_price_df.price_start_time) & 
                                            ((silver_df.usage_start_time < silver_price_df.price_end_time) | 
                                             silver_price_df.price_end_time.isNull()))
                             .select(silver_df["*"], silver_price_df["price_usd"])
                             .withColumn("usage_cost", F.col("usage_quantity") * F.col("price_usd")))
            
            print(f"Records after price join: {silver_with_cost.count()}")

            c_hr = (
                cluster_dim
                    .withColumn("valid_from_hr", F.date_trunc("hour", F.col("valid_from")))
                    .withColumn("valid_to_hr",   F.date_trunc("hour", F.col("valid_to")))
                    .alias("c_hr")
            )

            # Far-future cap for open-ended SCD2 rows 
            far_future = F.lit("9999-12-31 00:00:00").cast("timestamp")
            
            # Join with dimensions using SCD2 temporal logic
            print("Joining with dimension tables using SCD2 temporal logic...")
            fact_df = (silver_with_cost.alias("slv_usage_txn")
                # Workspace dimension join (no SCD2 needed - workspace doesn't change frequently)
                .join(workspace_dim, 
                      (silver_with_cost.workspace_id == workspace_dim.workspace_id) & 
                      (silver_with_cost.account_id == workspace_dim.account_id), 
                      "left")
                # Entity dimension join with SCD2 temporal logic
                .join(entity_dim, 
                      (silver_with_cost.workspace_id == entity_dim.workspace_id) & 
                      (silver_with_cost.entity_type == entity_dim.entity_type) & 
                      (silver_with_cost.entity_id == entity_dim.entity_id) &
                      # SCD2 temporal condition: fact date must be within dimension validity period
                      (silver_with_cost.usage_start_time >= entity_dim.valid_from) &
                      ((entity_dim.valid_to.isNull()) | (silver_with_cost.usage_start_time < entity_dim.valid_to)), 
                      "left")
                # Cluster dimension join with SCD2 temporal logic
                .join(
                    c_hr,
                    on=[
                        F.col("silver_with_cost.workspace_id") == F.col("c.workspace_id"),
                        F.col("silver_with_cost.cluster_id")   == F.col("c.cluster_id"),
                        (F.col("silver_with_cost.usage_start_time") >= F.col("c.valid_from_hr")) &
                        (F.col("silver_with_cost.usage_start_time")   <  F.coalesce(F.col("c.valid_to_hr"), far_future))
                    ],
                    how="left"
                )
                .join(cluster_dim, [
                      (silver_with_cost.workspace_id == cluster_dim.workspace_id) & 
                      (silver_with_cost.cluster_id == cluster_dim.cluster_id) &
                      # SCD2 temporal condition: fact date must be within dimension validity period
                      ((silver_with_cost.billing_origin_product == "JOBS") | 
                      ((silver_with_cost.usage_start_time < F.coalesce(cluster_dim.valid_to, F.lit("9999-12-31")))
                & (silver_with_cost.usage_end_time > cluster_dim.valid_from)))], 
                      "left")
                # SKU dimension join with temporal pricing logic
                .join(sku_dim, 
                      (silver_with_cost.sku_name == sku_dim.sku_name) & 
                      (silver_with_cost.cloud == sku_dim.cloud) &
                      (silver_with_cost.usage_unit == sku_dim.usage_unit) &
                      # Temporal condition: usage_start_time >= price_effective_from and (price_effective_from is null or usage_start_time < price_effective_till)
                      (silver_with_cost.usage_start_time >= sku_dim.price_effective_from) &
                      ((sku_dim.price_effective_till.isNull()) | (silver_with_cost.usage_start_time < sku_dim.price_effective_till)), 
                      "left")
            )
            
            print(f"Records after dimension joins: {fact_df.count()}")
            
            # Deduplicate by record_id (since record_id should be unique in this table)
            print("Deduplicating records by record_id...")
            w = Window.partitionBy("record_id").orderBy(F.col("usage_start_time").desc())
            deduped_df = (fact_df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))
            
            print(f"Records after deduplication: {deduped_df.count()}")
            
            # Select and transform columns for the billing usage fact table
            print("Transforming columns for billing usage fact table...")
            fact_df = deduped_df.select(
                    # PRIMARY KEY (unique identifier)
                    F.col("record_id"),
                    # FOREIGN KEYS (surrogate keys to dimensions)
                    F.col("date_sk").alias("date_key"),
                    F.col("workspace_key"),
                    F.col("entity_key"),
                    F.col("cluster_key"),
                    F.col("sku_key"),
                    # DEGENERATE DIMENSIONS
                    F.col("job_run_id"),
                    F.col("slv_usage_txn.cloud"),
                    F.col("slv_usage_txn.billing_origin_product"),
                    F.col("slv_usage_txn.usage_unit"),
                    F.col("slv_usage_txn.usage_type"),
                    F.when(F.col("slv_usage_txn.product_features.is_serverless") == True, "Y")
                     .otherwise("N").alias("is_serverless"),
                    # BUSINESS CONTEXT (for analytics convenience)
                    F.col("slv_usage_txn.cost_center"),
                    F.col("line_of_business"),
                    F.col("department"),
                    F.col("use_case"),
                    # SERVERLESS FLAG (mapped from product_features.is_serverless)
                    F.col("pipeline_name"),
                    F.col("workflow_level"),
                    F.col("parent_workflow_name"),
                    # MEASURES
                    F.col("usage_quantity"),
                    F.col("usage_cost"),
                    F.col("usage_start_time"),
                    F.col("usage_end_time")
                )
            
            print(f"Final fact table record count: {fact_df.count()}")
            
            # Show sample data for debugging
            print("Sample of transformed data:")
            fact_df.select("record_id", "date_key", "workspace_key", "entity_key", "billing_origin_product", "is_serverless", "usage_cost").show(5, truncate=False)
            
            # Upsert fact using record_id as the unique key
            print("Upserting fact table...")
            result = self.upsert_fact(fact_df, "gld_fact_billing_usage", ["record_id"])
            
            if result:
                # Commit processing state
                max_timestamp = silver_df.select(F.max("ingestion_date")).collect()[0][0]
                print(f"Committing processing state with max timestamp: {max_timestamp}")
                self.commit_processing_state("slv_usage_txn", "gld_fact_billing_usage", max_timestamp)
                print("Successfully built gld_fact_billing_usage fact table")
            else:
                print("Failed to upsert gld_fact_billing_usage fact table")
            
            return result
            
        except Exception as e:
            print(f"Error building billing usage fact: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def validate_data_quality(self) -> Dict[str, any]:
        """
        Validate data quality of the billing usage fact table.
        
        Returns:
            Dict[str, any]: Dictionary containing validation results
        """
        try:
            fact_table = self.get_table_name("gld_fact_billing_usage")
            df = self.spark.table(fact_table)
            
            validation_results = {}
            
            # Check for duplicates by record_id
            duplicate_count = (df.groupBy("record_id")
                              .count()
                              .filter("count > 1")
                              .count())
            validation_results["duplicate_record_ids"] = duplicate_count
            
            # Check for null record_ids
            null_record_ids = df.filter("record_id IS NULL").count()
            validation_results["null_record_ids"] = null_record_ids
            
            # Check for negative costs
            negative_costs = df.filter("usage_cost < 0").count()
            validation_results["negative_costs"] = negative_costs
            
            # Check for negative quantities
            negative_quantities = df.filter("usage_quantity < 0").count()
            validation_results["negative_quantities"] = negative_quantities
            
            # Check is_serverless values
            serverless_values = df.groupBy("is_serverless").count().collect()
            validation_results["serverless_distribution"] = {row["is_serverless"]: row["count"] for row in serverless_values}
            
            # Check billing_origin_product distribution
            product_dist = df.groupBy("billing_origin_product").count().orderBy("count", ascending=False).collect()
            validation_results["product_distribution"] = {row["billing_origin_product"]: row["count"] for row in product_dist[:10]}
            
            # Check date range
            date_range = df.select(F.min("date_key"), F.max("date_key")).collect()[0]
            validation_results["date_range"] = {"min": date_range[0], "max": date_range[1]}
            
            # Overall validation status
            validation_results["overall_status"] = (
                duplicate_count == 0 and 
                null_record_ids == 0 and 
                negative_costs == 0 and 
                negative_quantities == 0
            )
            
            return validation_results
            
        except Exception as e:
            print(f"Error validating data quality: {str(e)}")
            return {"error": str(e), "overall_status": False}
    
    def get_processing_stats(self) -> Dict[str, any]:
        """
        Get processing statistics for the billing usage fact table.
        
        Returns:
            Dict[str, any]: Dictionary containing processing statistics
        """
        try:
            fact_table = self.get_table_name("gld_fact_billing_usage")
            df = self.spark.table(fact_table)
            
            stats = {}
            
            # Total record count
            stats["total_records"] = df.count()
            
            # Record count by date
            daily_counts = (df.groupBy("date_key")
                            .count()
                            .orderBy("date_key", ascending=False)
                            .limit(30)
                            .collect())
            stats["daily_counts"] = {row["date_key"]: row["count"] for row in daily_counts}
            
            # Cost statistics
            cost_stats = df.select(
                F.sum("usage_cost").alias("total_cost"),
                F.avg("usage_cost").alias("avg_cost"),
                F.min("usage_cost").alias("min_cost"),
                F.max("usage_cost").alias("max_cost")
            ).collect()[0]
            stats["cost_statistics"] = {
                "total_cost": cost_stats["total_cost"],
                "avg_cost": cost_stats["avg_cost"],
                "min_cost": cost_stats["min_cost"],
                "max_cost": cost_stats["max_cost"]
            }
            
            # Usage quantity statistics
            quantity_stats = df.select(
                F.sum("usage_quantity").alias("total_quantity"),
                F.avg("usage_quantity").alias("avg_quantity"),
                F.min("usage_quantity").alias("min_quantity"),
                F.max("usage_quantity").alias("max_quantity")
            ).collect()[0]
            stats["quantity_statistics"] = {
                "total_quantity": quantity_stats["total_quantity"],
                "avg_quantity": quantity_stats["avg_quantity"],
                "min_quantity": quantity_stats["min_quantity"],
                "max_quantity": quantity_stats["max_quantity"]
            }
            
            return stats
            
        except Exception as e:
            print(f"Error getting processing stats: {str(e)}")
            return {"error": str(e)}


class BillingUsageFactBuilderFactory:
    """Factory for creating BillingUsageFactBuilder instances"""
    
    @staticmethod
    def create_builder(spark: SparkSession, config: Config) -> BillingUsageFactBuilder:
        """
        Create a BillingUsageFactBuilder instance.
        
        Args:
            spark (SparkSession): Spark session instance
            config (Config): Configuration instance
        
        Returns:
            BillingUsageFactBuilder: Configured builder instance
        """
        return BillingUsageFactBuilder(spark, config)
