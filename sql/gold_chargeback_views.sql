-- Gold Chargeback Views (tag-driven)
-- These views read from normalized Silver tables and do not alter existing facts

USE CATALOG platform_observability;

-- 1) Cost by Business Unit (daily)
CREATE OR REPLACE VIEW plt_gold.vw_cost_by_business_unit AS
SELECT 
  u.date_sk,
  u.workspace_id,
  u.line_of_business,
  u.department,
  u.environment,
  u.use_case,
  u.pipeline_name,
  u.cluster_identifier,
  SUM(u.list_cost_usd)     AS total_cost_usd,
  SUM(u.usage_quantity)    AS total_usage_quantity,
  COUNT(1)                 AS records_count
FROM plt_silver.silver_usage_txn u
GROUP BY u.date_sk, u.workspace_id, u.line_of_business, u.department, u.environment, u.use_case, u.pipeline_name, u.cluster_identifier;

-- 2) Cost by Environment (daily)
CREATE OR REPLACE VIEW plt_gold.vw_cost_by_environment AS
SELECT 
  u.date_sk,
  u.environment,
  u.line_of_business,
  SUM(u.list_cost_usd)  AS environment_cost_usd,
  SUM(u.usage_quantity) AS environment_usage_quantity
FROM plt_silver.silver_usage_txn u
GROUP BY u.date_sk, u.environment, u.line_of_business;

-- 3) Pipeline Cost by Business Context (daily)
CREATE OR REPLACE VIEW plt_gold.vw_pipeline_cost_by_business AS
SELECT 
  u.date_sk,
  u.workspace_id,
  u.pipeline_name,
  u.line_of_business,
  u.department,
  u.environment,
  u.use_case,
  SUM(u.list_cost_usd)  AS pipeline_cost_usd,
  SUM(u.usage_quantity) AS pipeline_usage_quantity
FROM plt_silver.silver_usage_txn u
GROUP BY u.date_sk, u.workspace_id, u.pipeline_name, u.line_of_business, u.department, u.environment, u.use_case;

-- 4) Cluster Cost by Business Context (daily)
CREATE OR REPLACE VIEW plt_gold.vw_cluster_cost_by_business AS
SELECT 
  u.date_sk,
  u.workspace_id,
  u.cluster_identifier,
  u.line_of_business,
  u.department,
  u.environment,
  u.use_case,
  SUM(u.list_cost_usd)  AS cluster_cost_usd,
  SUM(u.usage_quantity) AS cluster_usage_quantity
FROM plt_silver.silver_usage_txn u
GROUP BY u.date_sk, u.workspace_id, u.cluster_identifier, u.line_of_business, u.department, u.environment, u.use_case;

-- 5) Actor Type Cost Split (Service Principal vs User)
CREATE OR REPLACE VIEW plt_gold.vw_cost_by_actor_type AS
SELECT 
  u.date_sk,
  u.environment,
  u.line_of_business,
  u.run_actor_type,
  COALESCE(u.run_actor_name, 'Unknown') AS run_actor_name,
  SUM(u.list_cost_usd)  AS total_cost_usd,
  SUM(u.usage_quantity) AS total_usage_quantity
FROM plt_silver.silver_usage_txn u
GROUP BY u.date_sk, u.environment, u.line_of_business, u.run_actor_type, COALESCE(u.run_actor_name, 'Unknown');

-- 6) Tag Coverage Snapshot (based on presence of key tags)
CREATE OR REPLACE VIEW plt_gold.vw_tag_coverage AS
WITH base AS (
  SELECT 
    date_sk,
    environment,
    SUM(CASE WHEN line_of_business IS NOT NULL AND line_of_business <> 'Unknown' THEN 1 ELSE 0 END) AS lob_present,
    SUM(CASE WHEN use_case IS NOT NULL AND use_case <> 'Unknown' THEN 1 ELSE 0 END) AS use_case_present,
    SUM(CASE WHEN cluster_identifier IS NOT NULL AND cluster_identifier <> 'Unknown' THEN 1 ELSE 0 END) AS cluster_present,
    COUNT(1) AS total_rows
  FROM plt_silver.silver_usage_txn
  GROUP BY date_sk, environment
)
SELECT 
  date_sk,
  environment,
  ROUND(100.0 * lob_present     / NULLIF(total_rows,0), 2) AS pct_lob_present,
  ROUND(100.0 * use_case_present/ NULLIF(total_rows,0), 2) AS pct_use_case_present,
  ROUND(100.0 * cluster_present / NULLIF(total_rows,0), 2) AS pct_cluster_present
FROM base;
