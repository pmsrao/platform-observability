-- Upsert list prices data into Bronze layer
-- Parameters: {target_table}, {source_table}
MERGE INTO {target_table} T
USING {source_table} S
ON  T.cloud=S.cloud AND T.sku_name=S.sku_name AND T.usage_unit=S.usage_unit
AND T.price_start_time = S.price_start_time AND coalesce(T.price_end_time, timestamp('2999-12-31')) = coalesce(S.price_end_time, timestamp('2999-12-31'))
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
