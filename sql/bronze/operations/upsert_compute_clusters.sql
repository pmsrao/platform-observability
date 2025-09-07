-- Upsert compute clusters data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
   AND T.cluster_id = S.cluster_id
   AND T.create_time = S.create_time
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.cluster_name = S.cluster_name,
        T.owned_by = S.owned_by,
        T.delete_time = S.delete_time,
        T.driver_node_type = S.driver_node_type,
        T.worker_node_type = S.worker_node_type,
        T.worker_count = S.worker_count,
        T.min_autoscale_workers = S.min_autoscale_workers,
        T.max_autoscale_workers = S.max_autoscale_workers,
        T.auto_termination_minutes = S.auto_termination_minutes,
        T.enable_elastic_disk = S.enable_elastic_disk,
        T.tags = S.tags,
        T.cluster_source = S.cluster_source,
        T.init_scripts = S.init_scripts,
        T.aws_attributes = CAST(S.aws_attributes AS STRING),
        T.azure_attributes = CAST(S.azure_attributes AS STRING),
        T.gcp_attributes = CAST(S.gcp_attributes AS STRING),
        T.driver_instance_pool_id = S.driver_instance_pool_id,
        T.worker_instance_pool_id = S.worker_instance_pool_id,
        T.dbr_version = S.dbr_version,
        T.change_time = S.change_time,
        T.change_date = S.change_date,
        T.data_security_mode = S.data_security_mode,
        T.policy_id = S.policy_id,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        account_id, workspace_id, cluster_id, cluster_name, owned_by,
        create_time, delete_time, driver_node_type, worker_node_type,
        worker_count, min_autoscale_workers, max_autoscale_workers,
        auto_termination_minutes, enable_elastic_disk, tags, cluster_source,
        init_scripts, aws_attributes, azure_attributes, gcp_attributes,
        driver_instance_pool_id, worker_instance_pool_id, dbr_version,
        change_time, change_date, data_security_mode, policy_id,
        row_hash, _loaded_at
    )
    VALUES (
        S.account_id, S.workspace_id, S.cluster_id, S.cluster_name, S.owned_by,
        S.create_time, S.delete_time, S.driver_node_type, S.worker_node_type,
        S.worker_count, S.min_autoscale_workers, S.max_autoscale_workers,
        S.auto_termination_minutes, S.enable_elastic_disk, S.tags, S.cluster_source,
        S.init_scripts, CAST(S.aws_attributes AS STRING), CAST(S.azure_attributes AS STRING), CAST(S.gcp_attributes AS STRING),
        S.driver_instance_pool_id, S.worker_instance_pool_id, S.dbr_version,
        S.change_time, S.change_date, S.data_security_mode, S.policy_id,
        S.row_hash, CURRENT_TIMESTAMP()
    )
