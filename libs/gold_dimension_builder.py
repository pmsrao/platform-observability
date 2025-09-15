"""
Gold Layer Dimension Builder

This module provides classes for building Gold layer dimension tables
using proper star schema design with surrogate keys.
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DecimalType

from config import Config


class DimensionBuilder:
    """Base class for building dimension tables"""
    
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config
        self.catalog = config.catalog
        self.gold_schema = config.gold_schema
        self.silver_schema = config.silver_schema
    
    def get_table_name(self, table_name: str) -> str:
        """Get fully qualified table name"""
        return f"{self.catalog}.{self.gold_schema}.{table_name}"
    
    def upsert_dimension(self, df: DataFrame, table_name: str, natural_keys: List[str]) -> bool:
        """
        Upsert dimension table using natural keys
        """
        try:
            full_table_name = self.get_table_name(table_name)
            
            # Create temporary view for merge
            df.createOrReplaceTempView("temp_dimension")
            
            # Build merge condition
            merge_conditions = []
            for key in natural_keys:
                merge_conditions.append(f"target.{key} = source.{key}")
            merge_condition = " AND ".join(merge_conditions)
            
            # Get table schema to identify surrogate key columns (identity columns)
            table_schema = self.spark.sql(f"DESCRIBE {full_table_name}").collect()
            surrogate_key_columns = []
            for row in table_schema:
                if "identity" in row["col_name"].lower() or (row["col_name"].endswith("_key") and row["col_name"]!="date_key"):
                    surrogate_key_columns.append(row["col_name"])
            
            # Build UPDATE SET clause excluding surrogate keys
            source_columns = [col for col in df.columns if col not in surrogate_key_columns]
            update_set_clause = ", ".join([f"{col} = source.{col}" for col in source_columns])
            insert_set_clause = ", ".join([f"source.{col}" for col in source_columns])
            insert_columns = ", ".join([f"{col}" for col in source_columns])
            
            # Perform merge
            merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING temp_dimension AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_set_clause})
            """
            
            self.spark.sql(merge_sql)
            return True
            
        except Exception as e:
            print(f"Error upserting dimension {table_name}: {str(e)}")
            return False


class WorkspaceDimensionBuilder(DimensionBuilder):
    """Builder for workspace dimension table"""
    
    def build(self) -> bool:
        """Build workspace dimension from Silver layer"""
        try:
            # Read from Silver workspace table
            silver_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_workspace")
            
            # Transform to dimension format (removed region and cloud attributes)
            dim_df = silver_df.select(
                F.col("account_id"),
                F.col("workspace_id"),
                F.col("workspace_name"),
                F.col("workspace_url"),
                F.col("status"),
                F.col("create_time").alias("created_time"),
                F.col("_loaded_at").alias("updated_time")
            ).distinct()
            
            # Upsert dimension
            return self.upsert_dimension(dim_df, "gld_dim_workspace", ["workspace_id"])
            
        except Exception as e:
            print(f"Error building workspace dimension: {str(e)}")
            return False


class EntityDimensionBuilder(DimensionBuilder):
    """Builder for entity dimension table"""
    
    def build(self) -> bool:
        """Build entity dimension from Silver layer with SCD2"""
        try:
            # Read from Silver entity latest view
            silver_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_entity_latest")
            
            # Transform to dimension format with SCD2 logic
            dim_df = silver_df.select(
                F.col("account_id"),
                F.col("workspace_id"),
                F.col("entity_type"),
                F.col("entity_id"),
                F.col("name"),
                F.col("run_as"),
                F.col("created_time"),
                F.col("updated_time"),
                F.col("creator_id"),
                # SCD2 columns
                F.col("created_time").alias("valid_from"),  # Version becomes valid from creation
                F.lit(None).cast("timestamp").alias("valid_to"),  # No end date for current version
                F.lit(True).alias("is_current")  # Current version flag
            ).distinct()
            
            # For SCD2, we need to handle updates differently
            # This is a simplified version - in production, you'd want more sophisticated SCD2 logic
            return self.upsert_dimension_scd2(dim_df, "gld_dim_entity", ["workspace_id", "entity_type", "entity_id"])
            
        except Exception as e:
            print(f"Error building entity dimension: {str(e)}")
            return False
    
    def upsert_dimension_scd2(self, df: DataFrame, table_name: str, natural_keys: List[str]) -> bool:
        """
        Upsert dimension table using SCD2 logic
        """
        try:
            full_table_name = self.get_table_name(table_name)
            
            # Create temporary view for merge
            df.createOrReplaceTempView("temp_dimension")
            
            # Build merge condition for natural keys
            merge_conditions = []
            for key in natural_keys:
                merge_conditions.append(f"target.{key} = source.{key}")
            merge_condition = " AND ".join(merge_conditions)
            
            # Get table schema to identify surrogate key columns (identity columns)
            table_schema = self.spark.sql(f"DESCRIBE {full_table_name}").collect()
            surrogate_key_columns = []
            for row in table_schema:
                if "identity" in row["col_name"].lower() or row["col_name"].endswith("_key"):
                    surrogate_key_columns.append(row["col_name"])
            
            # Build UPDATE SET clause excluding surrogate keys for SCD2 close operation
            source_columns = [col for col in df.columns if col not in surrogate_key_columns]
            update_set_clause = ", ".join([f"{col} = source.{col}" for col in source_columns])
            insert_set_clause = ", ".join([f"source.{col}" for col in source_columns])
            insert_columns = ", ".join([f"{col}" for col in source_columns])
            
            # SCD2 merge logic
            merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING temp_dimension AS source
            ON {merge_condition} AND target.is_current = true
            WHEN MATCHED AND (
                target.name != source.name OR 
                target.run_as != source.run_as OR
                target.updated_time != source.updated_time
            ) THEN 
                UPDATE SET 
                    valid_to = source.updated_time,
                    is_current = false
            WHEN NOT MATCHED THEN 
                INSERT ({insert_columns}) VALUES ({insert_set_clause})
            """
            
            self.spark.sql(merge_sql)
            
            # Insert new version for updated records
            insert_new_version_sql = f"""
            INSERT INTO {full_table_name} ({insert_columns})
            SELECT 
                source.*
            FROM temp_dimension source
            WHERE EXISTS (
                SELECT 1 FROM {full_table_name} target
                WHERE {merge_condition} 
                AND target.is_current = false
                AND target.valid_to = source.updated_time
            )
            """
            
            self.spark.sql(insert_new_version_sql)
            return True
            
        except Exception as e:
            print(f"Error upserting SCD2 dimension {table_name}: {str(e)}")
            return False


class SKUDimensionBuilder(DimensionBuilder):
    """Builder for SKU dimension table"""
    
    def build(self) -> bool:
        """Build SKU dimension from Silver layer"""
        try:
            # Read from Silver usage transaction table
            silver_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_price_scd")
            
            # Transform to dimension format
            dim_df = silver_df.select(
                F.col("account_id"),
                F.col("cloud"),
                F.col("sku_name"),
                F.col("usage_unit"),
                F.col("currency_code"),  # Default currency
                F.col("price_usd").cast(DecimalType(38, 18)).alias("current_price_usd"), 
                F.to_date(F.col("price_start_time")).alias("price_effective_from"),
                F.to_date(F.col("price_end_time")).alias("price_effective_till")
            ).distinct()
            
            # Upsert dimension
            return self.upsert_dimension(dim_df, "gld_dim_sku", ["sku_name", "cloud", "account_id", "usage_unit", "currency_code", "price_effective_from"])
            
        except Exception as e:
            print(f"Error building SKU dimension: {str(e)}")
            return False


class RunStatusDimensionBuilder(DimensionBuilder):
    """Builder for run status dimension table"""
    
    def build(self) -> bool:
        """Build run status dimension from Silver layer"""
        try:
            # Read from Silver job run timeline table
            silver_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_job_run_timeline")
            
            # Transform to dimension format
            dim_df = silver_df.select(
                F.col("result_state"),
                F.col("termination_code")
            ).distinct()
            
            # Upsert dimension
            return self.upsert_dimension(dim_df, "gld_dim_run_status", ["result_state"])
            
        except Exception as e:
            print(f"Error building run status dimension: {str(e)}")
            return False


class ClusterDimensionBuilder(DimensionBuilder):
    """Builder for cluster dimension table with SCD2"""
    
    def build(self) -> bool:
        """Build cluster dimension from Silver layer with SCD2, including both clusters and warehouses"""
        try:
            # Read from Silver clusters table
            clusters_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_clusters")
            
            # Read from Silver warehouses table
            warehouses_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_warehouses")
            
            # Transform clusters data
            clusters_dim = clusters_df.select(
                F.col("account_id"),
                F.col("workspace_id"),
                F.col("cluster_id"),
                F.col("cluster_name"),
                F.col("owned_by"),
                F.col("create_time"),
                F.col("delete_time"),
                F.col("driver_node_type"),
                F.col("worker_node_type"),
                F.col("worker_count"),
                F.col("min_autoscale_workers"),
                F.col("max_autoscale_workers"),
                F.col("auto_termination_minutes"),
                F.col("enable_elastic_disk"),
                F.col("cluster_source"),
                F.lit(None).cast("string").alias("cluster_type"),
                F.lit(None).cast("string").alias("warehouse_size"),
                F.col("init_scripts"),
                F.col("driver_instance_pool_id"),
                F.col("worker_instance_pool_id"),
                F.col("dbr_version"),
                F.col("major_version"),
                F.col("minor_version"),
                F.col("is_photon_enabled"),
                F.col("is_ml_enabled"),
                F.col("change_time"),
                F.col("change_date"),
                F.col("data_security_mode"),
                F.col("policy_id"),
                F.col("worker_node_type_category"),
                # SCD2 columns
                F.col("change_time").alias("valid_from"),
                F.col("valid_to").alias("valid_to"),  
                F.col("is_current").alias("is_current")
            )
            
            # Transform warehouses data with mapping
            warehouses_dim = warehouses_df.select(
                F.col("account_id"),
                F.col("workspace_id"),
                F.col("warehouse_id").alias("cluster_id"),  # warehouse_id -> cluster_id
                F.col("warehouse_name").alias("cluster_name"),  # warehouse_name -> cluster_name
                F.lit(None).cast("string").alias("owned_by"),  # No owned_by for warehouses
                F.col("change_time").alias("create_time"),  # Use change_time as create_time
                F.col("delete_time"),
                F.lit(None).cast("string").alias("driver_node_type"),  # No driver_node_type for warehouses
                F.lit(None).cast("string").alias("worker_node_type"),  # No worker_node_type for warehouses
                F.lit(None).cast("int").alias("worker_count"),  # No worker_count for warehouses
                F.col("min_clusters").alias("min_autoscale_workers"),  # min_clusters -> min_autoscale_workers
                F.col("max_clusters").alias("max_autoscale_workers"),  # max_clusters -> max_autoscale_workers
                F.col("auto_stop_minutes").alias("auto_termination_minutes"),  # auto_stop_minutes -> auto_termination_minutes
                F.lit(None).cast("boolean").alias("enable_elastic_disk"),  # No enable_elastic_disk for warehouses
                F.lit("WAREHOUSE").alias("cluster_source"),  # Constant value "WAREHOUSE"
                F.col("warehouse_type").alias("cluster_type"),  # warehouse_type -> cluster_type
                F.col("warehouse_size"),  # warehouse_size -> warehouse_size
                F.lit(None).cast("string").alias("init_scripts"),  # No init_scripts for warehouses
                F.lit(None).cast("string").alias("driver_instance_pool_id"),  # No driver_instance_pool_id for warehouses
                F.lit(None).cast("string").alias("worker_instance_pool_id"),  # No worker_instance_pool_id for warehouses
                F.lit(None).cast("string").alias("dbr_version"),  # No dbr_version for warehouses
                F.lit(None).cast("int").alias("major_version"),  # No major_version for warehouses
                F.lit(None).cast("int").alias("minor_version"),  # No minor_version for warehouses
                F.lit(None).cast("boolean").alias("is_photon_enabled"),  # No is_photon_enabled for warehouses
                F.lit(None).cast("boolean").alias("is_ml_enabled"),  # No is_ml_enabled for warehouses
                F.col("change_time"),
                F.to_date(F.col("change_time")).alias("change_date"),
                F.lit(None).cast("string").alias("data_security_mode"),  # No data_security_mode for warehouses
                F.lit(None).cast("string").alias("policy_id"),  # No policy_id for warehouses
                F.lit(None).cast("string").alias("worker_node_type_category"),  # No worker_node_type_category for warehouses
                # SCD2 columns
                F.col("change_time").alias("valid_from"),
                F.col("valid_to").alias("valid_to"),  
                F.col("is_current").alias("is_current")
            )
            
            # Union clusters and warehouses
            dim_df = clusters_dim.union(warehouses_dim)
            
            # For SCD2, we need to handle updates differently
            return self.upsert_dimension(dim_df, "gld_dim_cluster", ["workspace_id", "cluster_id", "valid_from"])
            
        except Exception as e:
            print(f"Error building cluster dimension: {str(e)}")
            return False
    
   

class NodeTypeDimensionBuilder(DimensionBuilder):
    """Builder for node type dimension table"""
    
    def build(self) -> bool:
        """Build node type dimension from Silver layer"""
        try:
            # Read from Silver compute node type SCD2 table
            silver_df = self.spark.table(f"{self.catalog}.{self.silver_schema}.slv_compute_node_type_scd")
            
            # Filter for current records only
            current_df = silver_df.filter(F.col("is_current") == True)
            
            # Transform to dimension format
            dim_df = current_df.select(
                F.col("account_id"),
                F.col("node_type"),
                F.col("core_count"),
                F.col("memory_mb"),
                F.col("gpu_count"),
                F.col("category")
            ).distinct()
            
            # Upsert dimension
            return self.upsert_dimension(dim_df, "gld_dim_node_type", ["node_type"])
            
        except Exception as e:
            print(f"Error building node type dimension: {str(e)}")
            return False


class DateDimensionBuilder(DimensionBuilder):
    """Builder for date dimension table"""
    
    def build(self, start_date: str = "2020-01-01", end_date: str = "2030-12-31") -> bool:
        """Build date dimension table"""
        try:
            # Generate date range
            date_df = self.spark.sql(f"""
                SELECT 
                    date_format(date, 'yyyyMMdd') as date_key,
                    date as date,
                    year(date) as year,
                    month(date) as month,
                    day(date) as day,
                    quarter(date) as quarter,
                    dayofweek(date) as day_of_week,
                    dayofyear(date) as day_of_year,
                    CASE WHEN dayofweek(date) IN (1, 7) THEN true ELSE false END as is_weekend,
                    CASE WHEN day(date) = day(last_day(date)) THEN true ELSE false END as is_month_end,
                    CASE WHEN month(date) IN (3, 6, 9, 12) AND day(date) = day(last_day(date)) THEN true ELSE false END as is_quarter_end,
                    CASE WHEN month(date) = 12 AND day(date) = 31 THEN true ELSE false END as is_year_end
                FROM (
                    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
                )
            """)
            
            # Upsert dimension
            return self.upsert_dimension(date_df, "gld_dim_date", ["date_key"])
            
        except Exception as e:
            print(f"Error building date dimension: {str(e)}")
            return False


class DimensionBuilderFactory:
    """Factory for creating dimension builders"""
    
    @staticmethod
    def create_builder(builder_type: str, spark: SparkSession, config: Config) -> DimensionBuilder:
        """Create appropriate dimension builder"""
        builders = {
            "workspace": WorkspaceDimensionBuilder,
            "entity": EntityDimensionBuilder,
            "cluster": ClusterDimensionBuilder,
            "sku": SKUDimensionBuilder,
            "run_status": RunStatusDimensionBuilder,
            "node_type": NodeTypeDimensionBuilder,
            "date": DateDimensionBuilder
        }
        
        if builder_type not in builders:
            raise ValueError(f"Unknown builder type: {builder_type}")
        
        return builders[builder_type](spark, config)
