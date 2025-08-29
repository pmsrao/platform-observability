# Silver Layer Data Dictionary

This document describes the Silver layer tables/views, their columns, types, and purpose.

## Tables

### silver_workspace
- workspace_id (BIGINT): Databricks workspace identifier
- workspace_name (STRING): Friendly name of the workspace
- workspace_url (STRING): Workspace URL

### silver_jobs_scd (SCD2)
- workspace_id (BIGINT): Workspace id
- job_id (BIGINT): Job identifier
- name (STRING): Job name
- run_as (STRING): Identity configured to run the job
- valid_from (TIMESTAMP): Start of SCD validity
- valid_to (TIMESTAMP): End of SCD validity
- is_current (BOOLEAN): Current version flag

### silver_pipelines_scd (SCD2)
- workspace_id (BIGINT): Workspace id
- pipeline_id (STRING): DLT pipeline identifier
- name (STRING): Pipeline name
- run_as (STRING): Identity configured to run the pipeline
- valid_from (TIMESTAMP): Start of SCD validity
- valid_to (TIMESTAMP): End of SCD validity
- is_current (BOOLEAN): Current version flag

### silver_price_scd
- cloud (STRING): Cloud provider
- sku_name (STRING): Billing SKU
- usage_unit (STRING): Unit of usage
- price_usd (DOUBLE): Unit list price USD
- price_start_time (TIMESTAMP): Price effective start
- price_end_time (TIMESTAMP): Price effective end (default 2999-12-31 when open)

### silver_usage_txn
Enriched usage transactions with normalized tags and identity.
- workspace_id (BIGINT): Workspace id
- entity_type (STRING): JOB or PIPELINE
- entity_id (STRING): Entity identifier
- run_id (STRING): Run identifier
- cloud (STRING): Cloud provider
- sku_name (STRING): Billing SKU
- usage_unit (STRING): Unit of usage
- usage_quantity (DOUBLE): Usage quantity
- list_cost_usd (DOUBLE): Estimated list cost in USD
- duration_hours (DOUBLE): Duration in hours for the usage window
- usage_start_time (TIMESTAMP): Usage start
- usage_end_time (TIMESTAMP): Usage end
- date_sk (INT): YYYYMMDD surrogate key
- usage_metadata (STRUCT): Source metadata (job_id, job_run_id, dlt_pipeline_id, task_key, ...)
- identity_metadata (STRUCT): Source identity metadata (principal_type, user_name, service_principal_application_id)
- line_of_business (STRING): Normalized tag from `project` (default Unknown)
- department (STRING): Normalized tag from `sub_project` (default general)
- environment (STRING): Normalized env (prod|stage|uat|dev; default dev)
- use_case (STRING): Normalized tag from `data_product` (default Unknown)
- pipeline_name (STRING): Normalized tag from `job_pipeline` (default system)
- cluster_identifier (STRING): Cluster name; when cluster_source==JOB → Job_Cluster; default Unknown
- cost_attribution_key (STRING): `lob|dept|env|use_case|pipeline|cluster`
- run_actor_type (STRING): user|service_principal|unknown
- run_actor_name (STRING): User name or service principal application id
- is_service_principal (BOOLEAN): True when actor type is service_principal

### silver_job_run_timeline
- workspace_id (BIGINT): Workspace id
- job_id (BIGINT): Job id
- run_id (BIGINT): Run id
- period_start_time (TIMESTAMP): Window start
- period_end_time (TIMESTAMP): Window end
- result_state (STRING): Run result state
- termination_code (STRING): Termination code
- date_sk_start (INT): YYYYMMDD start
- date_sk_end (INT): YYYYMMDD end

### silver_job_task_run_timeline
- workspace_id (BIGINT): Workspace id
- job_id (BIGINT): Job id
- run_id (BIGINT): Run id
- task_key (STRING): Task key
- period_start_time (TIMESTAMP): Window start
- period_end_time (TIMESTAMP): Window end
- result_state (STRING): Task result state
- retry_attempt (INT): Retry attempt
- execution_secs (DOUBLE): Duration in seconds
- date_sk (INT): YYYYMMDD end

## Views

### silver_entity_latest_v (VIEW)
- entity_type (STRING): JOB or PIPELINE
- workspace_id (BIGINT): Workspace id
- entity_id (STRING): Entity id
- name (STRING): Latest display name
- run_as (STRING): Latest run_as

### silver_usage_run_enriched_v (VIEW)
Join of `silver_usage_txn` to `silver_job_run_timeline` using workspace/job/run and aligned time windows; used for run-state cost analyses.

### silver_usage_tags_exploded (VIEW)
Convenience view to explore tags without cost duplication.
- workspace_id (BIGINT)
- entity_type (STRING)
- entity_id (STRING)
- run_id (STRING)
- date_sk (INT)
- tag_key (STRING)
- tag_value (STRING)

Notes:
- Use `silver_usage_txn` for all cost aggregations to avoid duplication.
- Use `silver_usage_tags_exploded` only for filtering via semi-join/EXISTS.
