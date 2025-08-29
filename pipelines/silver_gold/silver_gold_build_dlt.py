from libs.tag_processor import TagProcessor

# DLT Silver+Gold: consume Bronze via CDF and incrementally MERGE into Gold
import dlt
from pyspark.sql import functions as F, Window

from libs.cdf import read_cdf, commit_bookmark
from libs.transform import with_entity_and_run, join_prices, impacted_dates
from libs.utils import yyyymmdd

CAT = "platform_observability"
BRZ = f"{CAT}.plt_bronze"
GLD = f"{CAT}.plt_gold"

# Read CDF from Bronze sources
usage_cdf, usage_end_ver = read_cdf(spark, f"{BRZ}.bronze_sys_billing_usage_raw", "bronze_sys_billing_usage_raw")
prices_cdf, prices_end_ver = read_cdf(spark, f"{BRZ}.bronze_sys_billing_list_prices_raw", "bronze_sys_billing_list_prices_raw")
run_cdf,    run_end_ver    = read_cdf(spark, f"{BRZ}.bronze_lakeflow_job_run_timeline_raw", "bronze_lakeflow_job_run_timeline_raw")
task_cdf,   task_end_ver   = read_cdf(spark, f"{BRZ}.bronze_lakeflow_job_task_run_timeline_raw", "bronze_lakeflow_job_task_run_timeline_raw")
jobs_cdf,   jobs_end_ver   = read_cdf(spark, f"{BRZ}.bronze_lakeflow_jobs_raw", "bronze_lakeflow_jobs_raw")
pipes_cdf,  pipes_end_ver  = read_cdf(spark, f"{BRZ}.bronze_lakeflow_pipelines_raw", "bronze_lakeflow_pipelines_raw")
ws_cdf,     ws_end_ver     = read_cdf(spark, f"{BRZ}.bronze_access_workspaces_latest_raw", "bronze_access_workspaces_latest_raw")

usage_new = usage_cdf.drop("_change_type","_commit_version","_commit_timestamp")
prices_new = prices_cdf.drop("_change_type","_commit_version","_commit_timestamp")
run_new = run_cdf.drop("_change_type","_commit_version","_commit_timestamp")
task_new = task_cdf.drop("_change_type","_commit_version","_commit_timestamp")
jobs_new = jobs_cdf.drop("_change_type","_commit_version","_commit_timestamp")
pipes_new = pipes_cdf.drop("_change_type","_commit_version","_commit_timestamp")
ws_new = ws_cdf.drop("_change_type","_commit_version","_commit_timestamp")

# Silver — Dimensions
@dlt.table(name="silver_workspace", comment="Workspace snapshot (current)")
def silver_workspace():
    return ws_new.select("workspace_id","workspace_name","workspace_url").distinct()

@dlt.table(name="silver_jobs_scd", comment="SCD2 for jobs from CDF")
def silver_jobs_scd():
    src = jobs_new.select("workspace_id","job_id","name","run_as","change_time")
    w = Window.partitionBy("workspace_id","job_id").orderBy("change_time")
    next_change = F.lead("change_time").over(w)
    return (src
        .withColumn("valid_from", F.col("change_time"))
        .withColumn("valid_to", F.expr("CASE WHEN lead(change_time) OVER (PARTITION BY workspace_id, job_id ORDER BY change_time) IS NULL THEN TIMESTAMP('2999-12-31') ELSE (lead(change_time) OVER (PARTITION BY workspace_id, job_id ORDER BY change_time) - INTERVAL 1 MICROSECOND) END"))
        .withColumn("is_current", next_change.isNull())
        .drop("change_time")
    )

@dlt.table(name="silver_pipelines_scd", comment="SCD2 for pipelines from CDF")
def silver_pipelines_scd():
    src = pipes_new.select("workspace_id","pipeline_id","name","run_as","change_time")
    w = Window.partitionBy("workspace_id","pipeline_id").orderBy("change_time")
    next_change = F.lead("change_time").over(w)
    return (src
        .withColumn("valid_from", F.col("change_time"))
        .withColumn("valid_to", F.expr("CASE WHEN lead(change_time) OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time) IS NULL THEN TIMESTAMP('2999-12-31') ELSE (lead(change_time) OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time) - INTERVAL 1 MICROSECOND) END"))
        .withColumn("is_current", next_change.isNull())
        .drop("change_time")
    )

@dlt.view(name="silver_entity_latest_v")
def silver_entity_latest_v():
    jobs = dlt.read("silver_jobs_scd").where("is_current").select(F.lit("JOB").alias("entity_type"), "workspace_id", F.col("job_id").alias("entity_id"), "name","run_as")
    pipes = dlt.read("silver_pipelines_scd").where("is_current").select(F.lit("PIPELINE").alias("entity_type"), "workspace_id", F.col("pipeline_id").alias("entity_id"), "name","run_as")
    return jobs.unionByName(pipes)

# Silver — Pricing & usage
@dlt.table(name="silver_price_scd", comment="Price intervals")
def silver_price_scd():
    p = prices_new.select("cloud","sku_name","usage_unit","pricing.default".alias("price_usd"),"price_start_time","price_end_time")
    return p.withColumn("price_end_time", F.coalesce(F.col("price_end_time"), F.to_timestamp(F.lit("2999-12-31"))))

@dlt.table(name="silver_usage_txn", comment="Usage + pricing + entity/run + date_sk + normalized business tags")
@dlt.expect("nonneg_qty","usage_quantity >= 0")
@dlt.expect("valid_time","usage_end_time >= usage_start_time")
def silver_usage_txn():
    u = with_entity_and_run(usage_new)
    u = join_prices(u, dlt.read("silver_price_scd"))
    # Enrich with normalized tags and identity fields (use_case default Unknown; job clusters -> Job_Cluster handled downstream when cluster info is joined)
    u = TagProcessor.enrich_usage(u)
    return u

@dlt.view(name="silver_usage_tags_exploded", comment="Exploded tags view for ad-hoc slicing (no aggregation; use EXISTS/semi-joins)")
def silver_usage_tags_exploded():
    return TagProcessor.exploded_tags_view(dlt.read("silver_usage_txn"))

@dlt.table(name="silver_job_run_timeline", comment="Run timeline from CDF")
def silver_job_run_timeline():
    r = run_new.select("workspace_id","job_id","run_id","period_start_time","period_end_time","result_state","termination_code")
    return (r
        .withColumn("date_sk_start", yyyymmdd("period_start_time"))
        .withColumn("date_sk_end",   yyyymmdd("period_end_time"))
    )

@dlt.table(name="silver_job_task_run_timeline", comment="Task timeline from CDF")
def silver_job_task_run_timeline():
    t = task_new.select("workspace_id","job_id","run_id","task_key","period_start_time","period_end_time","result_state","retry_attempt")
    return (t
        .withColumn("execution_secs", (F.col("period_end_time").cast("double") - F.col("period_start_time").cast("double")))
        .withColumn("date_sk", yyyymmdd("period_end_time"))
    )

@dlt.view(name="silver_usage_run_enriched_v")
def silver_usage_run_enriched_v():
    u = dlt.read("silver_usage_txn").alias("u")
    r = dlt.read("silver_job_run_timeline").alias("r")
    return (u.join(
        r,
        (F.col("u.workspace_id") == F.col("r.workspace_id")) &
        (F.col("u.usage_metadata.job_id") == F.col("r.job_id")) &
        (F.col("u.usage_metadata.job_run_id") == F.col("r.run_id")) &
        (F.date_trunc("hour", F.col("u.usage_start_time")) >= F.date_trunc("hour", F.col("r.period_start_time"))) &
        (F.date_trunc("hour", F.col("u.usage_start_time")) <  (F.date_trunc("hour", F.col("r.period_end_time")) + F.expr("INTERVAL 1 HOUR"))) ,
        "left"))

# GOLD — compute impacted dates and MERGE
imp_usage_dates = impacted_dates(usage_new, "usage_date")
imp_run_dates   = impacted_dates(run_new,   "period_end_time")
imp_task_dates  = impacted_dates(task_new,  "period_end_time")
imp_dates = imp_usage_dates.union(imp_run_dates).union(imp_task_dates).distinct()
imp_dates.createOrReplaceTempView("imp_dates")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.dim_workspace AS
SELECT 0 as workspace_id, '' as workspace_name, '' as workspace_url WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.dim_workspace T
USING (SELECT DISTINCT workspace_id, workspace_name, workspace_url FROM LIVE.silver_workspace) S
ON T.workspace_id = S.workspace_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.dim_entity AS
SELECT 0 AS workspace_id, '' AS entity_type, '' AS entity_id, '' AS name, '' AS run_as, '' AS workspace_name, '' AS workspace_url WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.dim_entity T
USING (
  SELECT DISTINCT e.workspace_id, e.entity_type, e.entity_id, e.name, e.run_as, w.workspace_name, w.workspace_url
  FROM LIVE.silver_entity_latest_v e
  LEFT JOIN LIVE.silver_workspace w USING (workspace_id)
) S
ON  T.workspace_id=S.workspace_id AND T.entity_type=S.entity_type AND T.entity_id=S.entity_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.dim_sku AS
SELECT '' AS cloud, '' AS sku_name, '' AS usage_unit, '' AS billing_origin_product WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.dim_sku T
USING (SELECT DISTINCT cloud, sku_name, usage_unit, billing_origin_product FROM LIVE.silver_usage_txn) S
ON  T.cloud=S.cloud AND T.sku_name=S.sku_name AND T.usage_unit=S.usage_unit
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.dim_run_status AS
SELECT '' AS result_state, '' AS termination_code WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.dim_run_status T
USING (SELECT DISTINCT result_state, termination_code FROM LIVE.silver_job_run_timeline) S
ON  T.result_state=S.result_state AND coalesce(T.termination_code,'')=coalesce(S.termination_code,'')
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.fact_usage_priced_day AS
SELECT 0 as date_sk, 0 as workspace_id, '' as entity_type, '' as entity_id, '' as cloud, '' as sku_name, '' as usage_unit, 0.0 as usage_quantity, 0.0 as list_cost_usd, 0.0 as duration_hours WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.fact_usage_priced_day T
USING (
  SELECT u.date_sk, u.workspace_id, u.entity_type, u.entity_id, u.cloud, u.sku_name, u.usage_unit,
         SUM(u.usage_quantity) AS usage_quantity, SUM(u.list_cost_usd) AS list_cost_usd, SUM(u.duration_hours) AS duration_hours
  FROM LIVE.silver_usage_txn u
  JOIN imp_dates d ON d.date_sk = u.date_sk
  GROUP BY ALL
) S
ON  T.date_sk=S.date_sk AND T.workspace_id=S.workspace_id AND T.entity_type=S.entity_type AND T.entity_id=S.entity_id AND T.cloud=S.cloud AND T.sku_name=S.sku_name AND T.usage_unit=S.usage_unit
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.fact_entity_cost AS
SELECT 0 as date_sk, 0 as workspace_id, '' as entity_type, '' as entity_id, 0.0 as list_cost_usd, 0 as runs_count WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.fact_entity_cost T
USING (
  SELECT u.date_sk, u.workspace_id, u.entity_type, u.entity_id,
         SUM(u.list_cost_usd) AS list_cost_usd,
         COUNT(DISTINCT u.run_id) AS runs_count
  FROM LIVE.silver_usage_txn u
  JOIN imp_dates d ON d.date_sk = u.date_sk
  GROUP BY ALL
) S
ON  T.date_sk=S.date_sk AND T.workspace_id=S.workspace_id AND T.entity_type=S.entity_type AND T.entity_id=S.entity_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.fact_run_cost AS
SELECT 0 as date_sk, 0 as workspace_id, '' as entity_type, '' as entity_id, '' as run_id, '' as cloud, '' as sku_name, '' as usage_unit, 0.0 as list_cost_usd, 0.0 as usage_quantity, 0.0 as duration_hours WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.fact_run_cost T
USING (
  SELECT u.date_sk, u.workspace_id, u.entity_type, u.entity_id, u.run_id, u.cloud, u.sku_name, u.usage_unit,
         SUM(u.list_cost_usd) AS list_cost_usd, SUM(u.usage_quantity) AS usage_quantity, SUM(u.duration_hours) AS duration_hours
  FROM LIVE.silver_usage_txn u
  JOIN imp_dates d ON d.date_sk = u.date_sk
  GROUP BY ALL
) S
ON  T.date_sk=S.date_sk AND T.workspace_id=S.workspace_id AND T.entity_type=S.entity_type AND T.entity_id=S.entity_id AND T.run_id=S.run_id AND T.cloud=S.cloud AND T.sku_name=S.sku_name AND T.usage_unit=S.usage_unit
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.fact_run_status_cost AS
SELECT 0 as date_sk, 0 as workspace_id, '' as entity_type, '' as entity_id, '' as run_id, '' as result_state, '' as termination_code, 0.0 as result_state_cost_usd WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.fact_run_status_cost T
USING (
  SELECT u.date_sk, u.workspace_id, u.entity_type, u.entity_id, u.run_id, r.result_state, r.termination_code,
         SUM(u.list_cost_usd) AS result_state_cost_usd
  FROM LIVE.silver_usage_run_enriched_v u
  LEFT JOIN LIVE.silver_job_run_timeline r
    ON u.workspace_id=r.workspace_id AND u.usage_metadata.job_id=r.job_id AND u.usage_metadata.job_run_id=r.run_id
  JOIN imp_dates d ON d.date_sk = u.date_sk
  GROUP BY ALL
) S
ON  T.date_sk=S.date_sk AND T.workspace_id=S.workspace_id AND T.entity_type=S.entity_type AND T.entity_id=S.entity_id AND T.run_id=S.run_id AND coalesce(T.result_state,'')=coalesce(S.result_state,'') AND coalesce(T.termination_code,'')=coalesce(S.termination_code,'')
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GLD}.fact_runs_finished_day AS
SELECT 0 as date_sk, 0 as workspace_id, '' as entity_type, '' as entity_id, 0 as finished_runs, 0 as success_runs, 0 as failed_runs, 0 as cancelled_runs WHERE 1=0
""")

spark.sql(f"""
MERGE INTO {GLD}.fact_runs_finished_day T
USING (
  SELECT r.date_sk_end AS date_sk, r.workspace_id, 'JOB' AS entity_type, r.job_id AS entity_id,
         COUNT(1) AS finished_runs,
         SUM(CASE WHEN r.result_state='SUCCESS'  THEN 1 ELSE 0 END) AS success_runs,
         SUM(CASE WHEN r.result_state='FAILED'   THEN 1 ELSE 0 END) AS failed_runs,
         SUM(CASE WHEN r.result_state='CANCELED' THEN 1 ELSE 0 END) AS cancelled_runs
  FROM LIVE.silver_job_run_timeline r
  JOIN imp_dates d ON d.date_sk = r.date_sk_end
  GROUP BY ALL
) S
ON  T.date_sk=S.date_sk AND T.workspace_id=S.workspace_id AND T.entity_type=S.entity_type AND T.entity_id=S.entity_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# Commit CDF bookmarks
commit_bookmark(spark, "bronze_sys_billing_usage_raw", usage_end_ver)
commit_bookmark(spark, "bronze_sys_billing_list_prices_raw", prices_end_ver)
commit_bookmark(spark, "bronze_lakeflow_job_run_timeline_raw", run_end_ver)
commit_bookmark(spark, "bronze_lakeflow_job_task_run_timeline_raw", task_end_ver)
commit_bookmark(spark, "bronze_lakeflow_jobs_raw", jobs_end_ver)
commit_bookmark(spark, "bronze_lakeflow_pipelines_raw", pipes_end_ver)
commit_bookmark(spark, "bronze_access_workspaces_latest_raw", ws_end_ver)
