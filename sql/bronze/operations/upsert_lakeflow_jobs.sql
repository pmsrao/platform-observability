-- Upsert lakeflow jobs data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
   AND T.job_id = S.job_id
   AND T.change_time = S.change_time
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.name = S.name,
        T.description = S.description,
        T.creator_id = S.creator_id,
        T.tags = S.tags,
        T.delete_time = S.delete_time,
        T.run_as = S.run_as,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        account_id, workspace_id, job_id, name, description, creator_id,
        tags, change_time, delete_time, run_as, row_hash, _loaded_at
    )
    VALUES (
        S.account_id, S.workspace_id, S.job_id, S.name, S.description, S.creator_id,
        S.tags, S.change_time, S.delete_time, S.run_as, S.row_hash, CURRENT_TIMESTAMP()
    )
