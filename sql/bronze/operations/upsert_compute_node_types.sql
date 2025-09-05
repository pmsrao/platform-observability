-- Upsert compute node types data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.node_type_id = S.node_type_id
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.node_type_name = S.node_type_name,
        T.vcpus = S.vcpus,
        T.memory_gb = S.memory_gb,
        T.storage_gb = S.storage_gb,
        T.gpu_count = S.gpu_count,
        T.gpu_type = S.gpu_type,
        T.cloud_provider = S.cloud_provider,
        T.region = S.region,
        T.cost_per_hour_usd = S.cost_per_hour_usd,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        node_type_id, node_type_name, vcpus, memory_gb, storage_gb,
        gpu_count, gpu_type, cloud_provider, region, cost_per_hour_usd,
        row_hash, _loaded_at
    )
    VALUES (
        S.node_type_id, S.node_type_name, S.vcpus, S.memory_gb, S.storage_gb,
        S.gpu_count, S.gpu_type, S.cloud_provider, S.region, S.cost_per_hour_usd,
        S.row_hash, CURRENT_TIMESTAMP()
    )
