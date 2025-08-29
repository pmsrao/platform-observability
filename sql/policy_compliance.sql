-- Policy compliance views & baseline controls
-- Catalog: platform_observability
-- Schemas: plt_bronze, plt_silver, plt_gold

USE CATALOG platform_observability;

-- Baseline controls
CREATE TABLE IF NOT EXISTS platform_observability.plt_gold.policy_baseline (
  id                       INT,
  require_cluster_policy   BOOLEAN,
  allowed_security_modes   ARRAY<STRING>,
  required_tag_keys        ARRAY<STRING>,
  updated_at               TIMESTAMP
) TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);

INSERT INTO platform_observability.plt_gold.policy_baseline
SELECT 1,
       true,                                                -- require policy
       array('SINGLE_USER','USER_ISOLATION'),
       array('environment','owner','cost_center'),
       current_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM platform_observability.plt_gold.policy_baseline);

-- Cluster policy-compliance (current clusters)
CREATE OR REPLACE VIEW platform_observability.plt_gold.vw_cluster_policy_compliance_current AS
WITH pb AS (
  SELECT * FROM platform_observability.plt_gold.policy_baseline ORDER BY updated_at DESC LIMIT 1
),
cluster_ws AS (
  SELECT cluster_id, any_value(workspace_id) AS workspace_id
  FROM   platform_observability.plt_bronze.bronze_system_compute_clusters_raw
  GROUP BY cluster_id
),
cur AS (
  SELECT c.cluster_id,
         cw.workspace_id,
         c.cluster_name,
         c.cluster_source,
         c.policy_id,
         c.spark_version,
         c.data_security_mode,
         c.driver_node_type_id,
         c.node_type_id       AS worker_node_type_id,
         c.min_workers,
         c.max_workers,
         c.num_workers
  FROM platform_observability.plt_silver.silver_clusters_scd c
  LEFT JOIN cluster_ws cw USING (cluster_id)
  WHERE c.is_current
)
SELECT
  cur.workspace_id,
  cur.cluster_id,
  cur.cluster_name,
  cur.cluster_source,
  cur.policy_id,
  cur.data_security_mode,
  cur.spark_version,
  cur.driver_node_type_id,
  cur.worker_node_type_id,
  cur.min_workers,
  cur.max_workers,
  cur.num_workers,
  CASE WHEN pb.require_cluster_policy AND cur.policy_id IS NULL THEN false ELSE true END         AS policy_ok,
  CASE WHEN array_contains(pb.allowed_security_modes, cur.data_security_mode) THEN true ELSE false END AS security_mode_ok,
  CASE WHEN dnt.is_deprecated THEN false ELSE true END                                           AS driver_node_type_ok,
  CASE WHEN wnt.is_deprecated THEN false ELSE true END                                           AS worker_node_type_ok,
  array_remove(array(
    CASE WHEN pb.require_cluster_policy AND cur.policy_id IS NULL THEN 'MISSING_CLUSTER_POLICY' END,
    CASE WHEN NOT array_contains(pb.allowed_security_modes, cur.data_security_mode) THEN 'DISALLOWED_SECURITY_MODE' END,
    CASE WHEN dnt.is_deprecated THEN 'DEPRECATED_DRIVER_NODE_TYPE' END,
    CASE WHEN wnt.is_deprecated THEN 'DEPRECATED_WORKER_NODE_TYPE' END
  ), NULL) AS violations,
  size(array_remove(array(
    CASE WHEN pb.require_cluster_policy AND cur.policy_id IS NULL THEN 'MISSING_CLUSTER_POLICY' END,
    CASE WHEN NOT array_contains(pb.allowed_security_modes, cur.data_security_mode) THEN 'DISALLOWED_SECURITY_MODE' END,
    CASE WHEN dnt.is_deprecated THEN 'DEPRECATED_DRIVER_NODE_TYPE' END,
    CASE WHEN wnt.is_deprecated THEN 'DEPRECATED_WORKER_NODE_TYPE' END
  ), NULL)) AS violation_count,
  (policy_ok AND security_mode_ok AND driver_node_type_ok AND worker_node_type_ok) AS is_compliant,
  current_timestamp() AS computed_at
FROM cur
CROSS JOIN pb
LEFT JOIN platform_observability.plt_silver.silver_node_types dnt ON dnt.node_type_id = cur.driver_node_type_id
LEFT JOIN platform_observability.plt_silver.silver_node_types wnt ON wnt.node_type_id = cur.worker_node_type_id;

-- Required tag coverage (per day, per entity)
CREATE OR REPLACE VIEW platform_observability.plt_gold.vw_required_tags_coverage_day AS
WITH pb AS (
  SELECT * FROM platform_observability.plt_gold.policy_baseline ORDER BY updated_at DESC LIMIT 1
),
keys AS (
  SELECT explode(pb.required_tag_keys) AS tag_key FROM pb
),
runs_by_day AS (
  SELECT date_sk, workspace_id, entity_type, entity_id,
         COUNT(DISTINCT run_id) AS runs
  FROM   platform_observability.plt_silver.silver_usage_txn
  WHERE  run_id IS NOT NULL
  GROUP BY date_sk, workspace_id, entity_type, entity_id
),
run_with_tag AS (
  SELECT t.date_sk, t.workspace_id, t.entity_type, t.entity_id, t.tag_key,
         COUNT(DISTINCT t.run_id) AS runs_with_tag
  FROM   platform_observability.plt_silver.silver_usage_tags t
  WHERE  t.run_id IS NOT NULL
  GROUP BY t.date_sk, t.workspace_id, t.entity_type, t.entity_id, t.tag_key
)
SELECT r.date_sk,
       r.workspace_id,
       r.entity_type,
       r.entity_id,
       k.tag_key,
       COALESCE(t.runs_with_tag, 0) AS runs_with_tag,
       r.runs AS total_runs,
       CASE WHEN r.runs = 0 THEN 1.0 ELSE COALESCE(t.runs_with_tag, 0) / r.runs END AS coverage_ratio
FROM runs_by_day r
CROSS JOIN keys k
LEFT JOIN run_with_tag t
  ON  r.date_sk     = t.date_sk
  AND r.workspace_id= t.workspace_id
  AND r.entity_type = t.entity_type
  AND r.entity_id   = t.entity_id
  AND k.tag_key     = t.tag_key;

CREATE OR REPLACE VIEW platform_observability.plt_gold.vw_required_tags_missing_day AS
SELECT *
FROM platform_observability.plt_gold.vw_required_tags_coverage_day
WHERE coverage_ratio < 1.0;

-- Summary: violations by workspace
CREATE OR REPLACE VIEW platform_observability.plt_gold.vw_policy_violations_summary AS
SELECT workspace_id,
       'CLUSTER' AS area,
       v.violation AS violation_type,
       COUNT(*) AS count
FROM platform_observability.plt_gold.vw_cluster_policy_compliance_current c
LATERAL VIEW explode(c.violations) v AS violation
GROUP BY workspace_id, v.violation
UNION ALL
SELECT workspace_id,
       'USAGE_TAGS' AS area,
       concat('MISSING_TAG:', tag_key) AS violation_type,
       COUNT(*) AS count
FROM platform_observability.plt_gold.vw_required_tags_missing_day
GROUP BY workspace_id, tag_key;

-- Optional distribution helper
CREATE OR REPLACE VIEW platform_observability.plt_gold.vw_clusters_security_mode_distribution AS
SELECT workspace_id, data_security_mode, COUNT(*) AS clusters
FROM platform_observability.plt_gold.vw_cluster_policy_compliance_current
GROUP BY workspace_id, data_security_mode;
