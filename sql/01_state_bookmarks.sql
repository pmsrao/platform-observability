-- Stores last processed table version per Bronze target for CDF-driven Silver/Gold
CREATE TABLE IF NOT EXISTS platform_observability.plt_silver._cdf_bookmarks (
  source_table STRING PRIMARY KEY,
  last_version BIGINT,
  updated_at   TIMESTAMP
) TBLPROPERTIES (delta.autoOptimize.optimizeWrite=true, delta.autoOptimize.autoCompact=true);
