-- High-water mark (HWM) bookmarks for Bronze incremental pulls from system.*
CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze._bronze_bookmarks (
  source_table STRING PRIMARY KEY,
  last_ts      TIMESTAMP,
  updated_at   TIMESTAMP
) TBLPROPERTIES (delta.autoOptimize.optimizeWrite=true, delta.autoOptimize.autoCompact=true);

-- Optional: seed rows (otherwise created on first commit)
-- INSERT INTO platform_observability.plt_bronze._bronze_bookmarks VALUES
--   ('system.billing.usage', TIMESTAMP('1900-01-01'), current_timestamp());
