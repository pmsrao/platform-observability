-- Upsert billing usage data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
   AND T.cloud = S.cloud
   AND T.sku_name = S.sku_name
   AND T.usage_unit = S.usage_unit
   AND T.usage_start_time = S.usage_start_time
   AND T.usage_end_time = S.usage_end_time
   AND T.usage_metadata.job_run_id = S.usage_metadata.job_run_id
   AND T.usage_metadata.dlt_pipeline_id = S.usage_metadata.dlt_pipeline_id
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.usage_quantity = S.usage_quantity,
        T.billing_origin_product = S.billing_origin_product,
        T.tags = S.tags,
        T.usage_metadata = S.usage_metadata,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        workspace_id, cloud, sku_name, usage_unit,
        usage_start_time, usage_end_time, usage_quantity,
        billing_origin_product, tags, usage_metadata,
        row_hash, _loaded_at
    )
    VALUES (
        S.workspace_id, S.cloud, S.sku_name, S.usage_unit,
        S.usage_start_time, S.usage_end_time, S.usage_quantity,
        S.billing_origin_product, S.tags, S.usage_metadata,
        S.row_hash, CURRENT_TIMESTAMP()
    )
