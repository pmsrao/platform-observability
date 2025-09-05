-- Upsert job run timeline data from staging to bronze table
-- This operation handles incremental updates using MERGE

MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
   AND T.job_id = S.job_id
   AND T.run_id = S.run_id
   AND T.period_start_time = S.period_start_time
   AND T.period_end_time = S.period_end_time
WHEN MATCHED AND T.row_hash != S.row_hash THEN
    UPDATE SET
        T.result_state = S.result_state,
        T.termination_code = S.termination_code,
        T._loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        workspace_id, job_id, run_id, period_start_time, period_end_time,
        result_state, termination_code,
        row_hash, _loaded_at
    )
    VALUES (
        S.workspace_id, S.job_id, S.run_id, S.period_start_time, S.period_end_time,
        S.result_state, S.termination_code,
        S.row_hash, CURRENT_TIMESTAMP()
    )
