from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Business tag normalization and enrichment
class TagProcessor:

    @staticmethod
    def normalize_environment(col: F.Column) -> F.Column:
        return (F.when(F.lower(col).isin("production", "prod"), F.lit("prod"))
                 .when(F.lower(col).isin("staging", "stage"), F.lit("stage"))
                 .when(F.lower(col).isin("user_acceptance", "uat"), F.lit("uat"))
                 .when(F.lower(col).isin("development", "dev"), F.lit("dev"))
                 .otherwise(F.lower(col)))


    @staticmethod
    def add_worker_node_type_category(df: DataFrame, node_type_col: str = "worker_node_type", category_col: str = "worker_node_type_category") -> DataFrame:
        """
        Add worker node type category based on node type patterns
        """
        return df.withColumn(
            category_col,
            F.when(F.col(node_type_col).like("%Standard_DS%"), F.lit("General Purpose (DS)"))
             .when(F.col(node_type_col).like("%Standard_E%"), F.lit("Memory Optimized (E)"))
             .when(F.col(node_type_col).like("%Standard_F%"), F.lit("Compute Optimized (F)"))
             .when(F.col(node_type_col).like("%Standard_L%"), F.lit("Storage Optimized (L)"))
             .when(F.col(node_type_col).like("%Standard_N%"), F.lit("GPU Enabled (N)"))
             .when(F.col(node_type_col).like("%Standard_M%"), F.lit("Memory Optimized (M)"))
             .when(F.col(node_type_col).like("c%"), F.lit("Compute Optimized (C-family)"))
             .when(F.col(node_type_col).like("m%"), F.lit("General Purpose (M-family)"))
             .when(F.col(node_type_col).like("r%"), F.lit("Memory Optimized (R-family)"))
             .when(F.col(node_type_col).like("i%"), F.lit("Storage Optimized (I-family)"))
             .when(F.col(node_type_col).like("g%"), F.lit("GPU Enabled (G-family)"))
             .when(F.col(node_type_col).like("p%"), F.lit("GPU Enabled (P-family)"))
             .when(F.col(node_type_col).like("t%"), F.lit("Burstable (T-family)"))
             .otherwise(F.lit("Other"))
        )

    @classmethod
    def enrich_usage(cls, usage_df: DataFrame) -> DataFrame:
        """
        Enrich usage data with specific tag processing:
        Extract columns from custom_tags
        """
        result = usage_df
        
        # 1. Extract raw columns from custom_tags
        columns = ["line_of_business","department","cost_center","use_case","pipeline_name","workflow_level","parent_workflow_name"]
        
        for col in columns:
            result = result.withColumn(
                col,
                F.col(f"custom_tags.{col}")
            )
        
        # 2. Handle environment separately from custom_tags.environment
        result = result.withColumn(
            "environment",
            F.coalesce(F.col("custom_tags.environment"), F.lit("dev"))
        )
        
        # 3. Normalize environment
        result = result.withColumn("environment", cls.normalize_environment(F.col("environment")))
        
        return result

    @staticmethod
    def exploded_tags_view(base: DataFrame) -> DataFrame:
        # Returns a distinct row-per-tag view for slicing without duplication
        if "custom_tags" not in base.columns:
            return base.limit(0)
        return (base
            .where(F.col("custom_tags").isNotNull())
            .select("workspace_id", "entity_type", "entity_id", "job_run_id", "date_sk", F.map_entries("custom_tags").alias("kv"))
            .select("workspace_id", "entity_type", "entity_id", "job_run_id", "date_sk", F.col("kv.key").alias("tag_key"), F.col("kv.value").alias("tag_value"))
            .distinct()
        )
