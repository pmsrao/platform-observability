from typing import Dict, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Business tag normalization and enrichment
class TagProcessor:
    # Map incoming tag keys to normalized columns
    REQUIRED_TAGS: Dict[str, str] = {
        "project": "line_of_business",
        "sub_project": "department",
        "cost_center": "cost_center",
        "environment": "environment",
        "data_product": "use_case",
        "job_pipeline": "pipeline_name",
        "cluster_name": "cluster_identifier",
        # NEW: Workflow hierarchy tags
        "workflow_level": "workflow_level",
        "parent_workflow": "parent_workflow_name",
        # NEW: Runtime and infrastructure tags
        "runtime_version": "databricks_runtime",
        "node_type": "compute_node_type",
        "cluster_size": "cluster_worker_count",
    }

    # Defaults per your request
    DEFAULTS: Dict[str, str] = {
        "line_of_business": "Unknown",
        "department": "general",
        "cost_center": "unallocated",
        "environment": "dev",
        "use_case": "Unknown",
        "pipeline_name": "system",
        "cluster_identifier": "Unknown",
        # NEW: Workflow defaults
        "workflow_level": "STANDALONE",
        "parent_workflow": "None",
        # NEW: Runtime and infrastructure defaults
        "databricks_runtime": "Unknown",
        "compute_node_type": "Unknown",
        "cluster_worker_count": "Unknown",
    }

    VALID_ENVS = ["prod", "stage", "uat", "dev", "production", "staging", "development", "user_acceptance"]

    @staticmethod
    def normalize_environment(col: F.Column) -> F.Column:
        return (F.when(F.lower(col).isin("production", "prod"), F.lit("prod"))
                 .when(F.lower(col).isin("staging", "stage"), F.lit("stage"))
                 .when(F.lower(col).isin("user_acceptance", "uat"), F.lit("uat"))
                 .when(F.lower(col).isin("development", "dev"), F.lit("dev"))
                 .otherwise(F.lower(col)))

    @classmethod
    def extract_from_map(cls, df: DataFrame, map_col: str = "custom_tags") -> DataFrame:
        result = df
        if map_col not in df.columns:
            # Apply defaults if map not present
            for _, norm_col in cls.REQUIRED_TAGS.items():
                # Create both raw and normalized columns
                result = result.withColumn(f"{norm_col}_raw", F.lit(None))
                result = result.withColumn(norm_col, F.lit(cls.DEFAULTS[norm_col]))
            return result

        for src_key, norm_col in cls.REQUIRED_TAGS.items():
            # Extract original value (raw)
            result = result.withColumn(
                f"{norm_col}_raw",
                F.coalesce(
                    F.col(f"{map_col}.{src_key}"),
                    F.col(f"{map_col}.{norm_col}")
                )
            )
            # Create normalized value with defaults
            result = result.withColumn(
                norm_col,
                F.coalesce(
                    F.col(f"{map_col}.{src_key}"),
                    F.col(f"{map_col}.{norm_col}"),
                    F.lit(cls.DEFAULTS[norm_col])
                )
            )
        # Env normalization (only for normalized column)
        result = result.withColumn("environment", cls.normalize_environment(F.col("environment")))
        return result

    @classmethod
    def apply_job_pipeline_fallbacks(cls, df: DataFrame, job_tags_col: str = None) -> DataFrame:
        # If job/pipeline-level tags exist, use them where usage-level is default/empty
        result = df
        return result

    @classmethod
    def attach_cluster_identifier(cls, df: DataFrame, cluster_source_col: str = None, cluster_name_col: str = None) -> DataFrame:
        # cluster_identifier = Job_Cluster if cluster_source == 'JOB', else cluster_name, else Unknown
        result = df
        if cluster_source_col and cluster_name_col:
            result = result.withColumn(
                "cluster_identifier",
                F.when(F.col(cluster_source_col) == F.lit("JOB"), F.lit("Job_Cluster"))
                 .when(F.col(cluster_name_col).isNotNull() & (F.col(cluster_name_col) != F.lit("")), F.col(cluster_name_col))
                 .otherwise(F.lit(cls.DEFAULTS["cluster_identifier"]))
            )
        return result

    @classmethod
    def ensure_defaults(cls, df: DataFrame) -> DataFrame:
        # Trim/lower basic string tags and apply defaults where null/empty
        result = df
        for _, norm_col in cls.REQUIRED_TAGS.items():
            # Ensure normalized columns have defaults where null/empty
            result = result.withColumn(
                norm_col,
                F.when(F.col(norm_col).isNull() | (F.trim(F.col(norm_col)) == F.lit("")), F.lit(cls.DEFAULTS[norm_col]))
                 .otherwise(F.col(norm_col))
            )
            # Ensure raw columns are properly trimmed (but keep original values)
            if f"{norm_col}_raw" in df.columns:
                result = result.withColumn(
                    f"{norm_col}_raw",
                    F.when(F.col(f"{norm_col}_raw").isNull(), F.lit(None))
                     .otherwise(F.trim(F.col(f"{norm_col}_raw")))
                )
        result = result.withColumn("environment", cls.normalize_environment(F.col("environment")))
        return result

    @staticmethod
    def add_identity_fields(df: DataFrame) -> DataFrame:
        # From identity_metadata.* derive run_actor fields
        has_identity = "identity_metadata" in df.columns
        result = df
        if has_identity:
            result = (result
                .withColumn("run_actor_type", F.coalesce(F.col("identity_metadata.principal_type"), F.lit("unknown")))
                .withColumn("run_actor_name", F.coalesce(F.col("identity_metadata.user_name"), F.col("identity_metadata.service_principal_application_id")))
                .withColumn("is_service_principal", F.col("run_actor_type") == F.lit("service_principal"))
            )
        else:
            result = (result
                .withColumn("run_actor_type", F.lit("unknown"))
                .withColumn("run_actor_name", F.lit(None))
                .withColumn("is_service_principal", F.lit(False))
            )
        return result

    @staticmethod
    def add_cost_attribution_key(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "cost_attribution_key",
            F.concat_ws(
                "|",
                F.col("line_of_business"),
                F.col("department"),
                F.col("cost_center"),
                F.col("environment"),
                F.col("use_case"),
                F.col("pipeline_name"),
                F.col("cluster_identifier"),
                F.col("workflow_level"),
                F.col("parent_workflow_name"),
            )
        )

    @classmethod
    def enrich_workflow_hierarchy(cls, df: DataFrame) -> DataFrame:
        """
        Enrich with workflow hierarchy information
        """
        return df.withColumn(
            "workflow_level",
            F.when(F.col("is_parent_workflow"), F.lit("PARENT"))
             .when(F.col("is_sub_workflow"), F.lit("SUB_WORKFLOW"))
             .otherwise(F.lit("STANDALONE"))
        ).withColumn(
            "parent_workflow_name",
            F.when(F.col("parent_workflow_id").isNotNull(), 
                   F.concat_ws("_", F.lit("WF"), F.col("parent_workflow_id")))
             .otherwise(F.lit("None"))
        )

    @classmethod
    def enrich_cluster_with_job_tags(cls, cluster_df: DataFrame, job_df: DataFrame) -> DataFrame:
        """
        Enrich cluster data with job-level tags for cost attribution
        """
        return cluster_df.join(
            job_df,
            (cluster_df.workspace_id == job_df.workspace_id) &
            (cluster_df.associated_entity_id == job_df.job_id) &
            (cluster_df.associated_entity_type == F.lit("JOB")) &
            (job_df.is_current == True),
            "left"
        ).select(
            cluster_df["*"],
            # Inherit job tags for cost attribution
            F.col("job.line_of_business").alias("inherited_line_of_business"),
            F.col("job.department").alias("inherited_department"),
            F.col("job.cost_center").alias("inherited_cost_center"),
            F.col("job.environment").alias("inherited_environment"),
            F.col("job.use_case").alias("inherited_use_case"),
            F.col("job.workflow_level").alias("inherited_workflow_level"),
            F.col("job.parent_workflow_name").alias("inherited_parent_workflow")
        )

    @staticmethod
    def add_worker_node_type_category(df: DataFrame) -> DataFrame:
        """
        Add worker node type category based on node type patterns
        """
        return df.withColumn(
            "worker_node_type_category",
            F.when(F.col("worker_node_type").like("%Standard_DS%"), F.lit("General Purpose (DS)"))
             .when(F.col("worker_node_type").like("%Standard_E%"), F.lit("Memory Optimized (E)"))
             .when(F.col("worker_node_type").like("%Standard_F%"), F.lit("Compute Optimized (F)"))
             .when(F.col("worker_node_type").like("%Standard_L%"), F.lit("Storage Optimized (L)"))
             .when(F.col("worker_node_type").like("%Standard_N%"), F.lit("GPU Enabled (N)"))
             .when(F.col("worker_node_type").like("%Standard_M%"), F.lit("Memory Optimized (M)"))
             .otherwise(F.lit("Other"))
        )

    @classmethod
    def enrich_usage(cls, usage_df: DataFrame) -> DataFrame:
        # 1) Extract usage-level tags
        u = cls.extract_from_map(usage_df, "custom_tags")
        # 2) Ensure defaults
        u = cls.ensure_defaults(u)
        # 3) Identity fields
        u = cls.add_identity_fields(u)
        # 4) Build attribution key
        u = cls.add_cost_attribution_key(u)
        return u

    @staticmethod
    def exploded_tags_view(base: DataFrame) -> DataFrame:
        # Returns a distinct row-per-tag view for slicing without duplication
        if "custom_tags" not in base.columns:
            return base.limit(0)
        return (base
            .where(F.col("custom_tags").isNotNull())
            .select("workspace_id", "entity_type", "entity_id", "run_id", "date_sk", F.map_entries("custom_tags").alias("kv"))
            .select("workspace_id", "entity_type", "entity_id", "run_id", "date_sk", F.col("kv.key").alias("tag_key"), F.col("kv.value").alias("tag_value"))
            .distinct()
        )
