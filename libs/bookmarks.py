from pyspark.sql import functions as F
from config import Config

def get_bookmarks_table():
    """Get the fully qualified bookmarks table name using configuration"""
    return Config.get_table_name("bronze", "_bronze_bookmarks")

def ensure_table(spark):
    bookmarks_table = get_bookmarks_table()
    spark.sql(f"""        CREATE TABLE IF NOT EXISTS {bookmarks_table} (
      source_table STRING PRIMARY KEY,
      last_ts      TIMESTAMP,
      updated_at   TIMESTAMP
    )
    """)

def get_last_ts(spark, source_table: str):
    ensure_table(spark)
    bookmarks_table = get_bookmarks_table()
    row = spark.table(bookmarks_table).where(F.col("source_table") == source_table).select("last_ts").first()
    return None if row is None else row[0]

def commit_last_ts(spark, source_table: str, ts):
    ensure_table(spark)
    bookmarks_table = get_bookmarks_table()
    spark.sql(f"""        MERGE INTO {bookmarks_table} T
    USING (SELECT '{source_table}' AS source_table, TIMESTAMP('{ts}') AS last_ts, current_timestamp() AS updated_at) S
    ON T.source_table = S.source_table
    WHEN MATCHED THEN UPDATE SET last_ts = S.last_ts, updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT *
    """)
