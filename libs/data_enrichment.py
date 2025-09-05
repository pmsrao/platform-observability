"""
Data Enrichment Utilities for Platform Observability

This module provides data enrichment functions that are used across
the Silver and Gold layers to enhance data with business logic and
contextual information.

The data enrichment functions handle:
1. Entity and run identification from usage metadata
2. Price joining and cost calculation
3. Business logic application and data transformation
4. Data quality enhancement and validation

Key Functions:
- with_entity_and_run(): Extract entity type, ID, and run information from usage metadata
- join_prices(): Join usage data with pricing information and calculate costs
- enrich_usage_data(): Apply business rules and enrich usage data

Note: These functions work with PySpark DataFrames and use the global spark session
from the DLT pipeline context.

Author: Platform Observability Team
Version: 1.0
"""

from pyspark.sql import DataFrame, functions as F
from .utils import yyyymmdd


def with_entity_and_run(df: DataFrame) -> DataFrame:
    """
    Extract entity type, ID, and run information from usage metadata.
    
    This function analyzes the usage metadata to determine whether the usage
    is associated with a job or pipeline, and extracts the relevant identifiers.
    
    Args:
        df (DataFrame): Input DataFrame with usage_metadata column containing job/pipeline info
    
    Returns:
        DataFrame: Input DataFrame with additional columns:
            - entity_type: "JOB" or "PIPELINE"
            - entity_id: ID of the job or pipeline
            - run_id: ID of the specific run
            - serverless_flag: Whether the product is serverless
    
    Example:
        >>> enriched_df = with_entity_and_run(usage_df)
        >>> enriched_df.select("entity_type", "entity_id", "run_id").show()
    """
    df = (df
        .withColumn("entity_type", 
            F.when(F.col("usage_metadata.job_id").isNotNull(), F.lit("JOB"))
            .when(F.col("usage_metadata.dlt_pipeline_id").isNotNull(), F.lit("PIPELINE")))
        .withColumn("entity_id", 
            F.when(F.col("entity_type") == "JOB", F.col("usage_metadata.job_id"))
            .when(F.col("entity_type") == "PIPELINE", F.col("usage_metadata.dlt_pipeline_id")))
        .withColumn("run_id", 
            F.when(F.col("entity_type") == "JOB", F.col("usage_metadata.job_run_id"))
            .when(F.col("entity_type") == "PIPELINE", F.col("usage_metadata.dlt_pipeline_id")))
        .withColumn("serverless_flag", F.col("product_features.is_serverless"))
    )
    return df


def join_prices(usage_df: DataFrame, price_df: DataFrame) -> DataFrame:
    """
    Join usage data with pricing information and calculate costs.
    
    This function performs a time-based join between usage data and pricing data,
    ensuring that the correct price is applied based on when the usage occurred.
    
    Args:
        usage_df (DataFrame): Usage data with cloud, sku_name, usage_unit, and timestamps
        price_df (DataFrame): Pricing data with cloud, sku_name, usage_unit, and time ranges
    
    Returns:
        DataFrame: Usage data enriched with:
            - price_usd: Unit price for the SKU
            - list_cost_usd: Calculated cost (usage_quantity * price_usd)
            - has_price: Boolean indicating if pricing was found
            - date_sk: Date surrogate key for partitioning
            - duration_hours: Usage duration in hours
    
    Example:
        >>> priced_df = join_prices(usage_df, price_df)
        >>> priced_df.select("list_cost_usd", "duration_hours").show()
    """
    p = (price_df
         .selectExpr(
             "cloud",
             "sku_name", 
             "usage_unit",
             "pricing.default as price_usd",
             "coalesce(price_end_time, timestamp('2999-12-31')) as price_end_time",
             "price_start_time"
         ))
    
    u = usage_df.alias("u").join(
        p.alias("p"),
        (F.col("u.cloud") == F.col("p.cloud")) &
        (F.col("u.sku_name") == F.col("p.sku_name")) &
        (F.col("u.usage_unit") == F.col("p.usage_unit")) &
        (F.col("u.usage_end_time") >= F.col("p.price_start_time")) &
        (F.col("u.usage_end_time") <= F.col("p.price_end_time")),
        "left"
    )
    
    u = (u
         .withColumn("list_cost_usd", 
             F.coalesce(F.col("u.usage_quantity") * F.col("p.price_usd"), F.lit(0.0)))
         .withColumn("has_price", F.col("p.price_usd").isNotNull())
         .withColumn("date_sk", yyyymmdd(F.col("u.usage_date")))
         .withColumn("duration_hours", 
             (F.col("u.usage_end_time").cast("double") - F.col("u.usage_start_time").cast("double")) / 3600.0)
    )
    return u


def enrich_usage_data(usage_df: DataFrame) -> DataFrame:
    """
    Apply business rules and enrich usage data with additional context.
    
    This function applies business logic to enhance usage data with
    derived fields and business context.
    
    Args:
        usage_df (DataFrame): Usage data to enrich
    
    Returns:
        DataFrame: Enriched usage data with additional business context
    
    Example:
        >>> enriched_df = enrich_usage_data(usage_df)
    """
    # Apply business rules and enrichments
    enriched_df = (usage_df
        .withColumn("is_peak_hours", 
            F.hour(F.col("usage_start_time")).between(9, 17))
        .withColumn("is_weekend", 
            F.dayofweek(F.col("usage_start_time")).isin(1, 7))
        .withColumn("cost_per_hour", 
            F.when(F.col("duration_hours") > 0, 
                   F.col("list_cost_usd") / F.col("duration_hours"))
            .otherwise(F.lit(0.0)))
    )
    
    return enriched_df
