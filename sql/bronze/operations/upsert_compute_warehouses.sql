-- Upsert Compute Warehouses
-- This operation performs an upsert (merge) operation for compute warehouses data
-- It handles both new records and updates to existing records

MERGE INTO {target_table} AS target
USING {source_table} AS source
ON target.warehouse_id = source.warehouse_id 
   AND target.workspace_id = source.workspace_id
   AND target.account_id = source.account_id
   AND target.change_time = source.change_time
WHEN MATCHED AND target.row_hash != source.row_hash THEN
  UPDATE SET
    warehouse_name = source.warehouse_name,
    warehouse_type = source.warehouse_type,
    warehouse_channel = source.warehouse_channel,
    warehouse_size = source.warehouse_size,
    min_clusters = source.min_clusters,
    max_clusters = source.max_clusters,
    auto_stop_minutes = source.auto_stop_minutes,
    tags = source.tags,
    change_time = source.change_time,
    delete_time = source.delete_time,
    row_hash = source.row_hash,
    _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    warehouse_id,
    workspace_id,
    account_id,
    warehouse_name,
    warehouse_type,
    warehouse_channel,
    warehouse_size,
    min_clusters,
    max_clusters,
    auto_stop_minutes,
    tags,
    change_time,
    delete_time,
    row_hash,
    _loaded_at
  )
  VALUES (
    source.warehouse_id,
    source.workspace_id,
    source.account_id,
    source.warehouse_name,
    source.warehouse_type,
    source.warehouse_channel,
    source.warehouse_size,
    source.min_clusters,
    source.max_clusters,
    source.auto_stop_minutes,
    source.tags,
    source.change_time,
    source.delete_time,
    source.row_hash,
    CURRENT_TIMESTAMP()
  )
