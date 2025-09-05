-- Upsert job run timeline data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.account_id = S.account_id
   AND T.workspace_id = S.workspace_id
   AND T.job_id = S.job_id
   AND T.job_run_id = S.job_run_id
   AND T.period_start_time = S.period_start_time
   AND T.period_end_time = S.period_end_time
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.trigger_type = S.trigger_type,
        T.run_type = S.run_type,
        T.run_name = S.run_name,
        T.compute_ids = S.compute_ids,
        T.result_state = S.result_state,
        T.termination_code = S.termination_code,
        T.job_parameters = S.job_parameters,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        account_id, workspace_id, job_id, job_run_id, period_start_time, period_end_time,
        trigger_type, run_type, run_name, compute_ids, result_state, termination_code,
        job_parameters, row_hash, _loaded_at
    )
    VALUES (
        S.account_id, S.workspace_id, S.job_id, S.job_run_id, S.period_start_time, S.period_end_time,
        S.trigger_type, S.run_type, S.run_name, S.compute_ids, S.result_state, S.termination_code,
        S.job_parameters, S.row_hash, CURRENT_TIMESTAMP()
    )
