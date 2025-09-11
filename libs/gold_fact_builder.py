"""
Gold Layer Fact Builder

This module provides classes for building Gold layer fact tables
using proper star schema design with surrogate key references.
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
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
            fact_table_name (str): Fact table name (e.g., 'gld_fact_usage_priced_day')
        
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


class UsageFactBuilder(FactBuilder):
    """Builder for usage fact table"""
    
    def build(self) -> bool:
        """Build usage fact table from Silver layer"""
        try:
            # Get incremental data using task-based processing state
            silver_df = self.get_incremental_data("slv_usage_txn", "gld_fact_usage_priced_day")
            
            # Check if there's any data to process
            if silver_df.count() == 0:
                print("No new data to process for gld_fact_usage_priced_day")
                return True
            
            # Get price data for cost calculation
            silver_price_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_price_scd")
            
            # Get dimension keys
            workspace_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_workspace")
            entity_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_entity")
            cluster_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_cluster").drop("cloud")
            sku_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_sku")
            
            # Join with price data to calculate usage_cost
            silver_with_cost = (silver_df.join(silver_price_df, 
                                            (silver_df.sku_name == silver_price_df.sku_name) & 
                                            (silver_df.account_id == silver_price_df.account_id) & 
                                            (silver_df.usage_unit == silver_price_df.usage_unit) & 
                                            (silver_df.usage_start_time >= silver_price_df.price_start_time) & 
                                            ((silver_df.usage_start_time < silver_price_df.price_end_time) | 
                                             silver_price_df.price_end_time.isNull()))
                             .select( silver_df["*"],  silver_price_df["price_usd"])
                             .withColumn("usage_cost", F.col("usage_quantity") * F.col("price_usd")))
            
            # Join with dimensions using SCD2 temporal logic
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
                      (F.to_date(silver_with_cost.usage_start_time) >= entity_dim.valid_from) &
                      ((entity_dim.valid_to.isNull()) | (F.to_date(silver_with_cost.usage_start_time) < entity_dim.valid_to)), 
                      "left")
                # Cluster dimension join with SCD2 temporal logic
                .join(cluster_dim, 
                      (silver_with_cost.workspace_id == cluster_dim.workspace_id) & 
                      (silver_with_cost.cluster_id == cluster_dim.cluster_id) &
                      # SCD2 temporal condition: fact date must be within dimension validity period
                      (F.to_date(silver_with_cost.usage_start_time) >= cluster_dim.valid_from) &
                      ((cluster_dim.valid_to.isNull()) | (F.to_date(silver_with_cost.usage_start_time) < cluster_dim.valid_to)), 
                      "left")
                # SKU dimension join with temporal pricing logic
                .join(sku_dim, 
                      (silver_with_cost.sku_name == sku_dim.sku_name) & 
                      (silver_with_cost.cloud == sku_dim.cloud) &
                      (silver_with_cost.usage_unit == sku_dim.usage_unit) &
                      # Temporal condition: usage_start_time <= price_effective_from and > price_effective_till or price_effective_till is null
                      (silver_with_cost.usage_start_time <= sku_dim.price_effective_from) &
                      ((sku_dim.price_effective_till.isNull()) | (silver_with_cost.usage_start_time > sku_dim.price_effective_till)), 
                      "left")
                .select(
                    # FOREIGN KEYS (surrogate keys to dimensions)
                    F.col("date_sk").alias("date_key"),
                    F.col("workspace_key"),
                    F.col("entity_key"),
                    F.col("cluster_key"),
                    F.col("sku_key"),
                    # MEASURES (numeric values to be aggregated)
                    F.col("record_id"),                    # Unique identifier from billing.usage
                    # DEGENERATE DIMENSIONS
                    F.col("job_run_id"),
                    F.col("slv_usage_txn.cloud"),
                    F.col("slv_usage_txn.usage_unit"),
                    # BUSINESS CONTEXT (for analytics convenience)
                    F.col("line_of_business"),
                    F.col("department"),
                    F.col("cost_center"),
                    F.col("environment"),
                    F.col("use_case"),
                    F.col("pipeline_name"),
                    F.col("workflow_level"),
                    F.col("parent_workflow_name"),
                    # MEASURES
                    F.col("usage_quantity"),
                    F.col("usage_cost")
                )
            )
            
            # Upsert fact
            result = self.upsert_fact(fact_df, "gld_fact_usage_priced_day", 
                                    ["date_key", "workspace_key", "entity_key", "cluster_key", "sku_key"])
            
            if result:
                # Commit processing state
                max_timestamp = silver_df.select(F.max("ingestion_date")).collect()[0][0]
                self.commit_processing_state("slv_usage_txn", "gld_fact_usage_priced_day", max_timestamp)
            
            return result
            
        except Exception as e:
            print(f"Error building usage fact: {str(e)}")
            return False


class EntityCostFactBuilder(FactBuilder):
    """Builder for entity cost fact table"""
    
    def build(self) -> bool:
        """Build entity cost fact table from Silver layer"""
        try:
            # Read from Silver usage transaction table and aggregate
            silver_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_usage_txn")
            silver_price_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_price_scd")
            
            # Get dimension keys
            workspace_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_workspace")
            entity_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_entity")
            
            # Aggregate by entity
            aggregated_df = (silver_df.join(silver_price_df, 
                                            (silver_df.sku_name == silver_price_df.sku_name) & 
                                            (silver_df.account_id == silver_price_df.account_id) & 
                                            (silver_df.usage_unit == silver_price_df.usage_unit) & 
                                            (silver_df.usage_start_time >= silver_price_df.price_start_time) & 
                                            ((silver_df.usage_start_time < silver_price_df.price_end_time) | 
                                             silver_price_df.price_end_time.isNull()))
                             .select( silver_df["*"],  silver_price_df["price_usd"])
                             .withColumn("usage_cost", F.col("usage_quantity") * F.col("price_usd"))
                             .select(
                                 silver_df.date_sk, 
                                 silver_df.account_id, 
                                 silver_df.workspace_id, 
                                 silver_df.entity_type, 
                                 silver_df.entity_id,
                                 silver_df.job_run_id,
                                 F.col("usage_cost")
                             )
                             .groupBy("date_sk", "account_id", "workspace_id", "entity_type", "entity_id")
                             .agg(
                                 F.sum("usage_cost").alias("usage_cost"),
                                 F.countDistinct("job_run_id").alias("runs_count")
                             )
            )
            
            # Join with dimensions using SCD2 temporal logic
            fact_df = (aggregated_df
                .join(workspace_dim, 
                      (aggregated_df.workspace_id == workspace_dim.workspace_id) & 
                      (aggregated_df.account_id == workspace_dim.account_id), 
                      "left")
                .join(entity_dim, 
                      (aggregated_df.workspace_id == entity_dim.workspace_id) & 
                      (aggregated_df.entity_type == entity_dim.entity_type) & 
                      (aggregated_df.entity_id == entity_dim.entity_id) &
                      # SCD2 temporal condition: use date_sk for temporal join
                      (aggregated_df.date_sk >= F.date_format(entity_dim.valid_from, "yyyyMMdd").cast("int")) &
                      ((entity_dim.valid_to.isNull()) | (aggregated_df.date_sk < F.date_format(entity_dim.valid_to, "yyyyMMdd").cast("int"))), 
                      "left")
                .select(
                    F.col("date_sk").alias("date_key"),
                    F.col("workspace_key"),
                    F.col("entity_key"),
                    F.col("usage_cost"),
                    F.col("runs_count")
                )
            )
            
            # Upsert fact
            return self.upsert_fact(fact_df, "gld_fact_entity_cost", 
                                  ["date_key", "workspace_key", "entity_key"])
            
        except Exception as e:
            print(f"Error building entity cost fact: {str(e)}")
            return False


class RunCostFactBuilder(FactBuilder):
    """Builder for run cost fact table"""
    
    def build(self) -> bool:
        """Build run cost fact table from Silver layer"""
        try:
            # Get incremental data using task-based processing state
            silver_df = self.get_incremental_data("slv_usage_txn", "gld_fact_run_cost")
            
            # Check if there's any data to process
            if silver_df.count() == 0:
                print("No new data to process for gld_fact_run_cost")
                return True
            
            # Get price data for cost calculation
            silver_price_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_price_scd")
            
            # Get dimension keys
            workspace_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_workspace")
            entity_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_entity")
            cluster_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_cluster")
            sku_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_sku")
            
            # Join with price data to calculate usage_cost
            silver_with_cost = (silver_df.join(silver_price_df, 
                                            (silver_df.sku_name == silver_price_df.sku_name) & 
                                            (silver_df.account_id == silver_price_df.account_id) & 
                                            (silver_df.usage_unit == silver_price_df.usage_unit) & 
                                            (silver_df.usage_start_time >= silver_price_df.price_start_time) & 
                                            ((silver_df.usage_start_time < silver_price_df.price_end_time) | 
                                             silver_price_df.price_end_time.isNull()))
                             .select(silver_df["*"],  silver_price_df["price_usd"])
                             .withColumn("usage_cost", F.col("usage_quantity") * F.col("price_usd")))
            
            # Join with dimensions using SCD2 temporal logic
            fact_df = (silver_with_cost.alias("slv_usage_txn")
                .join(workspace_dim, 
                      (silver_with_cost.workspace_id == workspace_dim.workspace_id) & 
                      (silver_with_cost.account_id == workspace_dim.account_id), 
                      "left")
                .join(entity_dim, 
                      (silver_with_cost.workspace_id == entity_dim.workspace_id) & 
                      (silver_with_cost.entity_type == entity_dim.entity_type) & 
                      (silver_with_cost.entity_id == entity_dim.entity_id) &
                      # SCD2 temporal condition: fact date must be within dimension validity period
                      (F.to_date(silver_with_cost.usage_start_time) >= entity_dim.valid_from) &
                      ((entity_dim.valid_to.isNull()) | (F.to_date(silver_with_cost.usage_start_time) < entity_dim.valid_to)), 
                      "left")
                .join(cluster_dim, 
                      (silver_with_cost.workspace_id == cluster_dim.workspace_id) & 
                      (silver_with_cost.cluster_id == cluster_dim.cluster_id) &
                      # SCD2 temporal condition: fact date must be within dimension validity period
                      (F.to_date(silver_with_cost.usage_start_time) >= cluster_dim.valid_from) &
                      ((cluster_dim.valid_to.isNull()) | (F.to_date(silver_with_cost.usage_start_time) < cluster_dim.valid_to)), 
                      "left")
                .join(sku_dim, 
                      (silver_with_cost.sku_name == sku_dim.sku_name) & 
                      (silver_with_cost.cloud == sku_dim.cloud) &
                      (silver_with_cost.usage_unit == sku_dim.usage_unit) &
                      # Temporal condition: usage_start_time <= price_effective_from and > price_effective_till or price_effective_till is null
                      (silver_with_cost.usage_start_time <= sku_dim.price_effective_from) &
                      ((sku_dim.price_effective_till.isNull()) | (silver_with_cost.usage_start_time > sku_dim.price_effective_till)), 
                      "left")
                .select(
                    F.col("date_sk").alias("date_key"),
                    F.col("workspace_key"),
                    F.col("entity_key"),
                    F.col("cluster_key"),
                    F.col("sku_key"),
                    F.col("job_run_id"),
                    F.col("slv_usage_txn.cloud"),
                    F.col("slv_usage_txn.usage_unit"),
                    # MEASURES
                    F.col("usage_cost"),
                    F.col("usage_quantity")
                )
            )
            
            # Upsert fact
            result = self.upsert_fact(fact_df, "gld_fact_run_cost", 
                                    ["date_key", "workspace_key", "entity_key", "cluster_key", "sku_key", "job_run_id"])
            
            if result:
                # Commit processing state
                max_timestamp = silver_df.select(F.max("ingestion_date")).collect()[0][0]
                self.commit_processing_state("slv_usage_txn", "gld_fact_run_cost", max_timestamp)
            
            return result
            
        except Exception as e:
            print(f"Error building run cost fact: {str(e)}")
            return False


class RunStatusCostFactBuilder(FactBuilder):
    """Builder for run status cost fact table"""
    
    def build(self) -> bool:
        """Build run status cost fact table from Silver layer"""
        try:
            # Read from Silver job run timeline table and join with usage data to get costs
            silver_usage_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_usage_txn")
            silver_price_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_price_scd")
            silver_job_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_job_run_timeline")
            
            # Calculate usage cost by joining usage with price data
            usage_with_cost = (silver_usage_df.join(silver_price_df, 
                                            (silver_usage_df.sku_name == silver_price_df.sku_name) & 
                                            (silver_usage_df.account_id == silver_price_df.account_id) & 
                                            (silver_usage_df.usage_unit == silver_price_df.usage_unit) & 
                                            (silver_usage_df.usage_start_time >= silver_price_df.price_start_time) & 
                                            ((silver_usage_df.usage_start_time < silver_price_df.price_end_time) | 
                                             silver_price_df.price_end_time.isNull()))
                            .select( silver_df["*"],  silver_price_df["price_usd"])
                             .withColumn("usage_cost", F.col("usage_quantity") * F.col("price_usd")))
            
            # Aggregate usage cost by job run
            run_costs = (usage_with_cost
                        .groupBy("workspace_id", "account_id", "job_run_id")
                        .agg(F.sum("usage_cost").alias("usage_cost")))
            
            # Join job run timeline with calculated costs
            silver_df = (silver_job_df
                        .withColumnRenamed("date_sk_end", "date_sk")
                        .withColumn("entity_type", F.when(F.col("run_type")=="JOB_RUN", "JOB").otherwise(F.col("run_type")))
                        .withColumnRenamed("job_id", "entity_id")
                        .join(run_costs, 
                              (silver_job_df.workspace_id == run_costs.workspace_id) & 
                              (silver_job_df.account_id == run_costs.account_id) & 
                              (silver_job_df.job_run_id == run_costs.job_run_id), 
                              "left")
                        .withColumn("usage_cost", F.coalesce(F.col("usage_cost"), F.lit(0.0))))
            
            # Get dimension keys
            workspace_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_workspace")
            entity_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_entity")
            run_status_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_run_status")
            
            # Join with dimensions using SCD2 temporal logic
            fact_df = (silver_df
                .join(workspace_dim, 
                      (silver_df.workspace_id == workspace_dim.workspace_id) & 
                      (silver_df.account_id == workspace_dim.account_id), 
                      "left")
                .join(entity_dim, 
                      (silver_df.workspace_id == entity_dim.workspace_id) & 
                      (silver_df.entity_type == entity_dim.entity_type) & 
                      (silver_df.entity_id == entity_dim.entity_id) &
                      # SCD2 temporal condition: use date_sk for temporal join
                      (silver_df.date_sk >= F.date_format(entity_dim.valid_from, "yyyyMMdd").cast("int")) &
                      ((entity_dim.valid_to.isNull()) | (silver_df.date_sk < F.date_format(entity_dim.valid_to, "yyyyMMdd").cast("int"))), 
                      "left")
                .join(run_status_dim, 
                      silver_df.result_state == run_status_dim.result_state, 
                      "left")
                .select(
                    F.col("date_sk").alias("date_key"),
                    F.col("workspace_key"),
                    F.col("entity_key"),
                    F.col("run_status_key"),
                    F.col("job_run_id"),
                    F.col("usage_cost")
                )
            )
            
            # Upsert fact
            return self.upsert_fact(fact_df, "gld_fact_run_status_cost", 
                                  ["date_key", "workspace_key", "entity_key", "run_status_key", "job_run_id"])
            
        except Exception as e:
            print(f"Error building run status cost fact: {str(e)}")
            return False


class RunsFinishedFactBuilder(FactBuilder):
    """Builder for runs finished fact table"""
    
    def build(self) -> bool:
        """Build runs finished fact table from Silver layer"""
        try:
            # Read from Silver job run timeline table
            silver_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_job_run_timeline")
            
            # Get dimension keys
            workspace_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_workspace")
            entity_dim = self.spark.table(f"{self.catalog}.{self.gold_schema}.gld_dim_entity")
            
            # Aggregate by entity and date
            aggregated_df = (silver_df.withColumnRenamed("date_sk_end", "date_sk")
                             .withColumn("entity_type", F.when(F.col("run_type")=="JOB_RUN", "JOB").otherwise(F.col("run_type")))
                .withColumnRenamed("job_id", "entity_id")
                .groupBy("date_sk", "account_id", "workspace_id", "entity_type", "entity_id")
                .agg(
                    F.count("*").alias("finished_runs"),
                    F.sum(F.when(F.col("result_state") == "SUCCESS", 1).otherwise(0)).alias("success_runs"),
                    F.sum(F.when(F.col("result_state") == "FAILED", 1).otherwise(0)).alias("failed_runs"),
                    F.sum(F.when(F.col("result_state") == "CANCELLED", 1).otherwise(0)).alias("cancelled_runs")
                )
            )
            
            # Join with dimensions using SCD2 temporal logic
            fact_df = (aggregated_df
                .join(workspace_dim, 
                      (aggregated_df.workspace_id == workspace_dim.workspace_id) & 
                      (aggregated_df.account_id == workspace_dim.account_id), 
                      "left")
                .join(entity_dim, 
                      (aggregated_df.workspace_id == entity_dim.workspace_id) & 
                      (aggregated_df.entity_type == entity_dim.entity_type) & 
                      (aggregated_df.entity_id == entity_dim.entity_id) &
                      # SCD2 temporal condition: use date_sk for temporal join
                      (aggregated_df.date_sk >= F.date_format(entity_dim.valid_from, "yyyyMMdd").cast("int")) &
                      ((entity_dim.valid_to.isNull()) | (aggregated_df.date_sk < F.date_format(entity_dim.valid_to, "yyyyMMdd").cast("int"))), 
                      "left")
                .select(
                    F.col("date_sk").alias("date_key"),
                    F.col("workspace_key"),
                    F.col("entity_key"),
                    F.col("finished_runs"),
                    F.col("success_runs"),
                    F.col("failed_runs"),
                    F.col("cancelled_runs")
                )
            )
            
            # Upsert fact
            return self.upsert_fact(fact_df, "gld_fact_runs_finished_day", 
                                  ["date_key", "workspace_key", "entity_key"])
            
        except Exception as e:
            print(f"Error building runs finished fact: {str(e)}")
            return False


class FactBuilderFactory:
    """Factory for creating fact builders"""
    
    @staticmethod
    def create_builder(builder_type: str, spark: SparkSession, config: Config) -> FactBuilder:
        """Create appropriate fact builder"""
        builders = {
            "usage": UsageFactBuilder,
            "entity_cost": EntityCostFactBuilder,
            "run_cost": RunCostFactBuilder,
            "run_status_cost": RunStatusCostFactBuilder,
            "runs_finished": RunsFinishedFactBuilder
        }
        
        if builder_type not in builders:
            raise ValueError(f"Unknown builder type: {builder_type}")
        
        return builders[builder_type](spark, config)
