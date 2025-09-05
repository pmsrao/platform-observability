-- Upsert job task run timeline data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.account_id = S.account_id
   AND T.workspace_id = S.workspace_id
   AND T.job_id = S.job_id
   AND T.task_run_id = S.task_run_id
   AND T.task_key = S.task_key
   AND T.period_start_time = S.period_start_time
   AND T.period_end_time = S.period_end_time
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.job_run_id = S.job_run_id,
        T.parent_run_id = S.parent_run_id,
        T.compute_ids = S.compute_ids,
        T.result_state = S.result_state,
        T.termination_code = S.termination_code,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        account_id, workspace_id, job_id, task_run_id, job_run_id, parent_run_id,
        task_key, period_start_time, period_end_time, compute_ids,
        result_state, termination_code, row_hash, _loaded_at
    )
    VALUES (
        S.account_id, S.workspace_id, S.job_id, S.task_run_id, S.job_run_id, S.parent_run_id,
        S.task_key, S.period_start_time, S.period_end_time, S.compute_ids,
        S.result_state, S.termination_code, S.row_hash, CURRENT_TIMESTAMP()
    )
