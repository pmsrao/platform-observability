-- Upsert billing usage data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.record_id = S.record_id
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.account_id = S.account_id,
        T.workspace_id = S.workspace_id,
        T.sku_name = S.sku_name,
        T.cloud = S.cloud,
        T.usage_start_time = S.usage_start_time,
        T.usage_end_time = S.usage_end_time,
        T.usage_date = S.usage_date,
        T.custom_tags = S.custom_tags,
        T.usage_unit = S.usage_unit,
        T.usage_quantity = S.usage_quantity,
        T.usage_metadata = S.usage_metadata,
        T.identity_metadata = S.identity_metadata,
        T.record_type = S.record_type,
        T.ingestion_date = S.ingestion_date,
        T.billing_origin_product = S.billing_origin_product,
        T.product_features = S.product_features,
        T.usage_type = S.usage_type,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        record_id, account_id, workspace_id, sku_name, cloud,
        usage_start_time, usage_end_time, usage_date, custom_tags,
        usage_unit, usage_quantity, usage_metadata, identity_metadata,
        record_type, ingestion_date, billing_origin_product,
        product_features, usage_type, row_hash, _loaded_at
    )
    VALUES (
        S.record_id, S.account_id, S.workspace_id, S.sku_name, S.cloud,
        S.usage_start_time, S.usage_end_time, S.usage_date, S.custom_tags,
        S.usage_unit, S.usage_quantity, S.usage_metadata, S.identity_metadata,
        S.record_type, S.ingestion_date, S.billing_origin_product,
        S.product_features, S.usage_type, S.row_hash, CURRENT_TIMESTAMP()
    )
