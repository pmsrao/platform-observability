from pyspark.sql import functions as F

BOOKMARK_TABLE = "platform_observability.plt_silver._cdf_bookmarks"

def _get_latest_version(spark, table_fqn: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table_fqn} LIMIT 1").select("version").first()[0]

def _get_bookmark(spark, source_table: str):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {BOOKMARK_TABLE} (source_table STRING PRIMARY KEY, last_version BIGINT, updated_at TIMESTAMP)")
    row = spark.table(BOOKMARK_TABLE).where(F.col("source_table") == source_table).select("last_version").first()
    return None if row is None else int(row[0])

def _set_bookmark(spark, source_table: str, version: int):
    spark.sql(f"""        MERGE INTO {BOOKMARK_TABLE} AS T
    USING (SELECT '{source_table}' AS source_table, {version} AS last_version, current_timestamp() AS updated_at) AS S
    ON T.source_table = S.source_table
    WHEN MATCHED THEN UPDATE SET last_version = S.last_version, updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT *
    """)

def read_cdf(spark, table_fqn: str, source_name: str, change_types=("insert","update_postimage")):
    start_version = _get_bookmark(spark, source_name)
    end_version   = _get_latest_version(spark, table_fqn)
    opts = {
        "readChangeFeed": "true",
        "startingVersion": str(start_version + 1 if start_version is not None else 0),
        "endingVersion": str(end_version)
    }
    df = spark.read.format("delta").options(**opts).table(table_fqn)
    df = df.where(F.col("_change_type").isin(list(change_types)))
    return df, end_version

def commit_bookmark(spark, source_name: str, end_version: int):
    _set_bookmark(spark, source_name, end_version)
