-- Upsert compute node types data into Bronze layer
-- Parameters: {target_table}, {source_table}
MERGE INTO {target_table} T
USING {source_table} S
ON  T.node_type_id=S.node_type_id
WHEN MATCHED AND T.row_hash <> S.row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
