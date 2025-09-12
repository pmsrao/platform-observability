-- Gold Layer Runtime Analysis Views
-- This file contains views for analyzing Databricks Runtime versions, node types, and optimization opportunities

-- Runtime Version Analysis View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_runtime_version_analysis AS
with cl_major as (
  select workspace_id, max(major_version) cl_max_version from {catalog}.{gold_schema}.gld_dim_cluster
  group by workspace_id
)
SELECT 
    f.date_key,
    c.dbr_version,
    c.major_version,
    c.minor_version,
    c.cluster_source,
    'NA' is_lts,
    c.is_current,

    -- Business context (from Silver layer)
    f.line_of_business,
    f.department,
    f.cost_center,
    f.environment,
    -- Usage metrics
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    COUNT(DISTINCT c.cluster_id) as active_clusters,
    SUM(f.usage_cost) as total_cost_usd,
    SUM(f.usage_quantity) as total_usage_quantity,
    -- SUM(f.duration_hours) as total_duration_hours,
    AVG(f.usage_cost) as avg_cost_per_entity,
    -- Runtime health indicators
    CASE 
        WHEN (cl_major.cl_max_version - c.major_version) > 4 THEN 'CRITICAL - End of Support'
        WHEN (cl_major.cl_max_version - c.major_version) > 3 THEN 'HIGH - Support Ending Soon'
        WHEN (cl_major.cl_max_version - c.major_version) > 2 THEN 'MEDIUM - Plan for Upgrade'
        WHEN (cl_major.cl_max_version - c.major_version) > 1 THEN 'MEDIUM - Consider Upgrade'
        ELSE 'LOW - Current and Supported'
    END as upgrade_priority
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
LEFT JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
JOIN cl_major ON cl_major.workspace_id = w.workspace_id
LEFT JOIN {catalog}.{gold_schema}.gld_dim_cluster c ON f.cluster_key = c.cluster_key
GROUP BY f.date_key,   cl_major.cl_max_version,       c.dbr_version, c.major_version, c.minor_version,
         c.cluster_source, c.is_current, f.line_of_business, f.department, f.cost_center, f.environment;

-- Node Type Analysis View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_node_type_analysis AS
SELECT 
    f.date_key,
    c.worker_node_type,
    c.driver_node_type,
    c.worker_node_type as node_type_id,
    -- Node type categorization (from Silver layer)
    c.worker_node_type_category as node_family,
    -- Cluster sizing analysis
    c.min_autoscale_workers,
    c.max_autoscale_workers,
    case when c.min_autoscale_workers IS NULL then false else true end as autoscale_enabled,
    CASE 
        WHEN c.max_autoscale_workers = c.min_autoscale_workers THEN 'Fixed Size'
        WHEN c.min_autoscale_workers IS NOT NULL THEN 'Auto-scaling'
        ELSE 'Manual Scaling'
    END as scaling_type,
    -- Business context
    f.line_of_business,
    f.department,
    f.cost_center,
    -- Usage metrics
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    COUNT(DISTINCT c.cluster_id) as active_clusters,
    SUM(f.usage_cost) as total_cost_usd,
    SUM(f.usage_quantity) as total_usage_quantity,
    -- SUM(f.duration_hours) as total_duration_hours,
    AVG(f.usage_cost) as avg_cost_per_entity,
    -- Optimization insights
    CASE 
        WHEN c.max_autoscale_workers = c.min_autoscale_workers AND c.max_autoscale_workers > 10 THEN 'Consider Auto-scaling'
        WHEN c.max_autoscale_workers = c.min_autoscale_workers AND c.max_autoscale_workers = 1 THEN 'Single Node - Monitor Performance'
        WHEN c.min_autoscale_workers IS NULL THEN 'Enable Auto-scaling'
        ELSE 'Optimal Configuration'
    END as optimization_suggestion
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
LEFT JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
JOIN {catalog}.{gold_schema}.gld_dim_cluster c ON f.cluster_key = c.cluster_key
GROUP BY f.date_key, c.worker_node_type, c.driver_node_type, c.worker_node_type_category,
         c.min_autoscale_workers, c.max_autoscale_workers,
         f.line_of_business, f.department, f.cost_center;





