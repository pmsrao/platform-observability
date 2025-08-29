-- Upsert billing usage data into Bronze layer
-- Parameters: {target_table}, {source_table}
MERGE INTO {target_table} T
USING {source_table} S
ON  T.workspace_id = S.workspace_id
AND T.cloud        = S.cloud
AND T.sku_name     = S.sku_name
AND T.usage_unit   = S.usage_unit
AND T.usage_start_time = S.usage_start_time
AND T.usage_end_time   = S.usage_end_time
AND coalesce(T.usage_metadata.job_run_id,'')      = coalesce(S.usage_metadata.job_run_id,'')
AND coalesce(T.usage_metadata.dlt_pipeline_id,'') = coalesce(S.usage_metadata.dlt_pipeline_id,'')
WHEN MATCHED AND T.row_hash <> S.row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
