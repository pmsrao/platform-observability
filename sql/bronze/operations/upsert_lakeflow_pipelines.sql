-- Upsert lakeflow pipelines data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
   AND T.pipeline_id = S.pipeline_id
   AND T.change_time = S.change_time
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.name = S.name,
        T.run_as = S.run_as,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        workspace_id , account_id , pipeline_id, name, run_as, change_time,
        row_hash, _loaded_at
    )
    VALUES (
        S.workspace_id, S.account_id, S.pipeline_id, S.name, S.run_as, S.change_time,
        S.row_hash, CURRENT_TIMESTAMP()
    )
