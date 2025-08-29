from pyspark.sql import functions as F
from .utils import yyyymmdd

def with_entity_and_run(df):
    df = (df
        .withColumn("entity_type", F.when(F.col("usage_metadata.job_id").isNotNull(), F.lit("JOB"))
                                     .when(F.col("usage_metadata.dlt_pipeline_id").isNotNull(), F.lit("PIPELINE")))
        .withColumn("entity_id", F.when(F.col("entity_type") == "JOB", F.col("usage_metadata.job_id"))
                                   .when(F.col("entity_type") == "PIPELINE", F.col("usage_metadata.dlt_pipeline_id")))
        .withColumn("run_id", F.when(F.col("entity_type") == "JOB", F.col("usage_metadata.job_run_id"))
                                 .when(F.col("entity_type") == "PIPELINE", F.col("usage_metadata.dlt_pipeline_id")))
        .withColumn("serverless_flag", F.col("product_features.is_serverless"))
    )
    return df

def join_prices(usage_df, price_df):
    p = (price_df
         .selectExpr("cloud","sku_name","usage_unit","pricing.default as price_usd","coalesce(price_end_time, timestamp('2999-12-31')) as price_end_time","price_start_time"))
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
         .withColumn("list_cost_usd", F.coalesce(F.col("u.usage_quantity") * F.col("p.price_usd"), F.lit(0.0)))
         .withColumn("has_price", F.col("p.price_usd").isNotNull())
         .withColumn("date_sk", yyyymmdd(F.col("u.usage_date")))
         .withColumn("duration_hours", (F.col("u.usage_end_time").cast("double") - F.col("u.usage_start_time").cast("double")) / 3600.0)
    )
    return u

def impacted_dates(df, date_col: str):
    return df.selectExpr(f"int(date_format(to_date({date_col}), 'yyyyMMdd')) as date_sk").distinct()
