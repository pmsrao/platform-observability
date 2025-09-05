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
        T.run_as = S.run_as,
        T.parent_workflow_id = S.parent_workflow_id,
        T.workflow_type = S.workflow_type,
        T.cluster_id = S.cluster_id,
        T.tags = S.tags,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        workspace_id, job_id, name, run_as, change_time,
        parent_workflow_id, workflow_type, cluster_id, tags,
        row_hash, _loaded_at
    )
    VALUES (
        S.workspace_id, S.job_id, S.name, S.run_as, S.change_time,
        S.parent_workflow_id, S.workflow_type, S.cluster_id, S.tags,
        S.row_hash, CURRENT_TIMESTAMP()
    )
