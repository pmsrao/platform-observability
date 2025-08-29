-- Upsert access workspaces data into Bronze layer
-- Parameters: {target_table}, {source_table}
MERGE INTO {target_table} T
USING {source_table} S
ON  T.workspace_id=S.workspace_id
WHEN MATCHED AND T.row_hash <> S.row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
