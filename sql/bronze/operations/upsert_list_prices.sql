-- Upsert list prices data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.cloud = S.cloud
   AND T.sku_name = S.sku_name
   AND T.usage_unit = S.usage_unit
   AND T.price_start_time = S.price_start_time
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.pricing = S.pricing,
        T.price_end_time = S.price_end_time,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        cloud, sku_name, usage_unit, pricing,
        price_start_time, price_end_time,
        row_hash, _loaded_at
    )
    VALUES (
        S.cloud, S.sku_name, S.usage_unit, S.pricing,
        S.price_start_time, S.price_end_time,
        S.row_hash, CURRENT_TIMESTAMP()
    )
