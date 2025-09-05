-- Performance Optimizations
-- This file applies performance tuning to all tables

-- Bronze Layer Optimizations
OPTIMIZE {catalog}.{bronze_schema}.brz_billing_usage ZORDER BY (workspace_id, usage_start_time);
OPTIMIZE {catalog}.{bronze_schema}.brz_lakeflow_jobs ZORDER BY (workspace_id, job_id);
OPTIMIZE {catalog}.{bronze_schema}.brz_lakeflow_pipelines ZORDER BY (workspace_id, pipeline_id);
OPTIMIZE {catalog}.{bronze_schema}.brz_lakeflow_job_run_timeline ZORDER BY (workspace_id, job_id, period_start_time);
OPTIMIZE {catalog}.{bronze_schema}.brz_lakeflow_job_task_run_timeline ZORDER BY (workspace_id, job_id, run_id);

-- Silver Layer Optimizations
OPTIMIZE {catalog}.{silver_schema}.slv_workspace ZORDER BY (workspace_id);
OPTIMIZE {catalog}.{silver_schema}.slv_jobs_scd ZORDER BY (workspace_id, job_id, valid_from);
OPTIMIZE {catalog}.{silver_schema}.slv_pipelines_scd ZORDER BY (workspace_id, pipeline_id, valid_from);
OPTIMIZE {catalog}.{silver_schema}.slv_usage_txn ZORDER BY (workspace_id, date_sk, entity_id);
OPTIMIZE {catalog}.{silver_schema}.slv_job_run_timeline ZORDER BY (workspace_id, job_id, date_sk_start);
OPTIMIZE {catalog}.{silver_schema}.slv_job_task_run_timeline ZORDER BY (workspace_id, job_id, task_run_id, date_sk);

-- Gold Layer Optimizations
OPTIMIZE {catalog}.{gold_schema}.gld_dim_workspace ZORDER BY (workspace_id);
OPTIMIZE {catalog}.{gold_schema}.gld_dim_entity ZORDER BY (workspace_id, entity_type, entity_id);
OPTIMIZE {catalog}.{gold_schema}.gld_dim_sku ZORDER BY (cloud, sku_name);
OPTIMIZE {catalog}.{gold_schema}.gld_dim_run_status ZORDER BY (result_state);
OPTIMIZE {catalog}.{gold_schema}.gld_fact_usage_priced_day ZORDER BY (date_key, workspace_key, entity_key);
OPTIMIZE {catalog}.{gold_schema}.gld_fact_entity_cost ZORDER BY (date_key, workspace_key, entity_key);
OPTIMIZE {catalog}.{gold_schema}.gld_fact_run_cost ZORDER BY (date_key, workspace_key, job_run_id);
OPTIMIZE {catalog}.{gold_schema}.gld_fact_run_status_cost ZORDER BY (date_key, workspace_key, run_status_key);
OPTIMIZE {catalog}.{gold_schema}.gld_fact_runs_finished_day ZORDER BY (date_key, workspace_key, entity_key);

-- Collect Statistics
ANALYZE TABLE {catalog}.{bronze_schema}.brz_billing_usage COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE {catalog}.{silver_schema}.slv_usage_txn COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE {catalog}.{gold_schema}.gld_fact_usage_priced_day COMPUTE STATISTICS FOR ALL COLUMNS;
