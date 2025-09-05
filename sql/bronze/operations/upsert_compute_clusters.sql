-- Upsert compute clusters data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
   AND T.cluster_id = S.cluster_id
   AND T.created_time = S.created_time
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.cluster_name = S.cluster_name,
        T.cluster_type = S.cluster_type,
        T.associated_entity_id = S.associated_entity_id,
        T.associated_entity_type = S.associated_entity_type,
        T.tags = S.tags,
        T.spark_version = S.spark_version,
        T.cluster_source = S.cluster_source,
        T.node_type_id = S.node_type_id,
        T.min_workers = S.min_workers,
        T.max_workers = S.max_workers,
        T.driver_node_type_id = S.driver_node_type_id,
        T.autoscale_enabled = S.autoscale_enabled,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        workspace_id, cluster_id, cluster_name, cluster_type,
        associated_entity_id, associated_entity_type, tags,
        spark_version, cluster_source, node_type_id,
        min_workers, max_workers, driver_node_type_id, autoscale_enabled,
        row_hash, _loaded_at
    )
    VALUES (
        S.workspace_id, S.cluster_id, S.cluster_name, S.cluster_type,
        S.associated_entity_id, S.associated_entity_type, S.tags,
        S.spark_version, S.cluster_source, S.node_type_id,
        S.min_workers, S.max_workers, S.driver_node_type_id, S.autoscale_enabled,
        S.row_hash, CURRENT_TIMESTAMP()
    )
