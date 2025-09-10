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
             .otherwise(F.lit("Other"))
        )

    @classmethod
    def enrich_usage(cls, usage_df: DataFrame) -> DataFrame:
        """
        Enrich usage data with specific tag processing:
        1. Extract raw columns from custom_tags
        2. Populate inherited columns with nulls
        3. Create coalesced columns from raw and inherited
        """
        result = usage_df
        
        # 1. Extract raw columns from custom_tags
        raw_columns = {
            "line_of_business": "line_of_business_raw",
            "department": "department_raw", 
            "cost_center": "cost_center_raw",
            "use_case": "use_case_raw",
            "pipeline_name": "pipeline_name_raw",
            "workflow_level": "workflow_level_raw",
            "parent_workflow_name": "parent_workflow_name_raw"
        }
        
        for tag_key, raw_col in raw_columns.items():
            result = result.withColumn(
                raw_col,
                F.col(f"custom_tags.{tag_key}")
            )
        
        # 2. Populate inherited columns with nulls
        inherited_columns = [
            "inherited_line_of_business",
            "inherited_cost_center", 
            "inherited_workflow_level",
            "inherited_parent_workflow_name"
        ]
        
        for inherited_col in inherited_columns:
            result = result.withColumn(inherited_col, F.lit(None).cast("string"))
        
        # 3. Create coalesced columns (raw + inherited + defaults)
        coalesced_columns = {
            "line_of_business": ("line_of_business_raw", "inherited_line_of_business", "UNKNOWN"),
            "department": ("department_raw", None, "UNKNOWN"),
            "cost_center": ("cost_center_raw", "inherited_cost_center", "UNKNOWN"),
            "use_case": ("use_case_raw", None, "UNKNOWN"),
            "pipeline_name": ("pipeline_name_raw", None, "UNKNOWN"),
            "workflow_level": ("workflow_level_raw", "inherited_workflow_level", "UNKNOWN"),
            "parent_workflow_name": ("parent_workflow_name_raw", "inherited_parent_workflow_name", "UNKNOWN")
        }
        
        for norm_col, (raw_col, inherited_col, default) in coalesced_columns.items():
            if inherited_col:
                result = result.withColumn(
                    norm_col,
                    F.coalesce(F.col(raw_col), F.col(inherited_col), F.lit(default))
                )
            else:
                result = result.withColumn(
                    norm_col,
                    F.coalesce(F.col(raw_col), F.lit(default))
                )
        
        # 4. Handle environment separately from custom_tags.environment
        result = result.withColumn(
            "environment",
            F.coalesce(F.col("custom_tags.environment"), F.lit("dev"))
        )
        
        # 5. Normalize environment
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
