# Gold Layer Data Dictionary

This document describes the Gold layer tables/views for chargeback and operations.

## Tables (existing core facts/dims retained)

### dim_workspace
- workspace_id (BIGINT)
- workspace_name (STRING)
- workspace_url (STRING)

### dim_entity
- workspace_id (BIGINT)
- entity_type (STRING)
- entity_id (STRING)
- name (STRING)
- run_as (STRING)
- workspace_name (STRING)
- workspace_url (STRING)

### dim_sku
- cloud (STRING)
- sku_name (STRING)
- usage_unit (STRING)
- billing_origin_product (STRING)

### dim_run_status
- result_state (STRING)
- termination_code (STRING)

### fact_usage_priced_day
- date_sk (INT)
- workspace_id (BIGINT)
- entity_type (STRING)
- entity_id (STRING)
- cloud (STRING)
- sku_name (STRING)
- usage_unit (STRING)
- usage_quantity (DOUBLE)
- list_cost_usd (DOUBLE)
- duration_hours (DOUBLE)

### fact_entity_cost
- date_sk (INT)
- workspace_id (BIGINT)
- entity_type (STRING)
- entity_id (STRING)
- list_cost_usd (DOUBLE)
- runs_count (BIGINT)

### fact_run_cost
- date_sk (INT)
- workspace_id (BIGINT)
- entity_type (STRING)
- entity_id (STRING)
- run_id (STRING)
- cloud (STRING)
- sku_name (STRING)
- usage_unit (STRING)
- list_cost_usd (DOUBLE)
- usage_quantity (DOUBLE)
- duration_hours (DOUBLE)

### fact_run_status_cost
- date_sk (INT)
- workspace_id (BIGINT)
- entity_type (STRING)
- entity_id (STRING)
- run_id (STRING)
- result_state (STRING)
- termination_code (STRING)
- result_state_cost_usd (DOUBLE)

### fact_runs_finished_day
- date_sk (INT)
- workspace_id (BIGINT)
- entity_type (STRING)
- entity_id (STRING)
- finished_runs (BIGINT)
- success_runs (BIGINT)
- failed_runs (BIGINT)
- cancelled_runs (BIGINT)

## Views (chargeback)

### vw_cost_by_business_unit
- date_sk (INT)
- workspace_id (BIGINT)
- line_of_business (STRING)
- department (STRING)
- environment (STRING)
- use_case (STRING)
- pipeline_name (STRING)
- cluster_identifier (STRING)
- total_cost_usd (DOUBLE)
- total_usage_quantity (DOUBLE)
- records_count (BIGINT)

### vw_cost_by_environment
- date_sk (INT)
- environment (STRING)
- line_of_business (STRING)
- environment_cost_usd (DOUBLE)
- environment_usage_quantity (DOUBLE)

### vw_pipeline_cost_by_business
- date_sk (INT)
- workspace_id (BIGINT)
- pipeline_name (STRING)
- line_of_business (STRING)
- department (STRING)
- environment (STRING)
- use_case (STRING)
- pipeline_cost_usd (DOUBLE)
- pipeline_usage_quantity (DOUBLE)

### vw_cluster_cost_by_business
- date_sk (INT)
- workspace_id (BIGINT)
- cluster_identifier (STRING)
- line_of_business (STRING)
- department (STRING)
- environment (STRING)
- use_case (STRING)
- cluster_cost_usd (DOUBLE)
- cluster_usage_quantity (DOUBLE)

### vw_cost_by_actor_type
- date_sk (INT)
- environment (STRING)
- line_of_business (STRING)
- run_actor_type (STRING)
- run_actor_name (STRING)
- total_cost_usd (DOUBLE)
- total_usage_quantity (DOUBLE)

### vw_tag_coverage
- date_sk (INT)
- environment (STRING)
- pct_lob_present (DOUBLE)
- pct_use_case_present (DOUBLE)
- pct_cluster_present (DOUBLE)

Notes:
- All views source from `plt_silver.silver_usage_txn` to avoid duplication.
- Use `vw_tag_coverage` to track coverage of key tags over time.
