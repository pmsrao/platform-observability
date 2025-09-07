-- Upsert compute node types data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.node_type = S.node_type
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.core_count = S.core_count,
        T.memory_mb = S.memory_mb,
        T.gpu_count = S.gpu_count,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        account_id, node_type, core_count, memory_mb, gpu_count,
        row_hash, _loaded_at
    )
    VALUES (
        S.account_id, S.node_type, S.core_count, S.memory_mb, S.gpu_count,
        S.row_hash, CURRENT_TIMESTAMP()
    )
