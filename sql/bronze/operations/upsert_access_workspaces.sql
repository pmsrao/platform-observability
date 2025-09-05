-- Upsert access workspaces data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.workspace_name = S.workspace_name,
        T.workspace_url = S.workspace_url,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        workspace_id, workspace_name, workspace_url,
        row_hash, _loaded_at
    )
    VALUES (
        S.workspace_id, S.workspace_name, S.workspace_url,
        S.row_hash, CURRENT_TIMESTAMP()
    )
