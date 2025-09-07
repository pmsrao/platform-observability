-- Upsert access workspaces data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.workspace_name = S.workspace_name,
        T.workspace_url = S.workspace_url,
        T.create_time = S.create_time,
        T.status = S.status,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        account_id, workspace_id, workspace_name, workspace_url,
        create_time, status, row_hash, _loaded_at
    )
    VALUES (
        S.account_id, S.workspace_id, S.workspace_name, S.workspace_url,
        S.create_time, S.status, S.row_hash, CURRENT_TIMESTAMP()
    )
