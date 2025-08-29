-- Upsert job task run timeline data into Bronze layer
-- Parameters: {target_table}, {source_table}
MERGE INTO {target_table} T
USING {source_table} S
ON  T.workspace_id=S.workspace_id AND T.job_id=S.job_id AND T.run_id=S.run_id AND T.task_key=S.task_key
AND T.period_start_time=S.period_start_time AND T.period_end_time=S.period_end_time
WHEN MATCHED AND T.row_hash <> S.row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
