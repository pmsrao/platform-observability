-- Gold Layer Runtime Analysis Views
-- This file contains views for analyzing Databricks Runtime versions, node types, and optimization opportunities

-- Runtime Version Analysis View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_runtime_version_analysis AS
SELECT 
    f.date_key,
    c.dbr_version,
    c.major_version,
    c.minor_version,
    c.cluster_source,
    c.is_lts,
    c.is_current,
    c.runtime_age_months,
    c.months_to_eos,
    c.is_supported,
    -- Business context (from Silver layer)
    f.line_of_business,
    f.department,
    f.cost_center,
    f.environment,
    -- Usage metrics
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    COUNT(DISTINCT c.cluster_id) as active_clusters,
    SUM(f.list_cost_usd) as total_cost_usd,
    SUM(f.usage_quantity) as total_usage_quantity,
    SUM(f.duration_hours) as total_duration_hours,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    -- Runtime health indicators
    CASE 
        WHEN c.is_supported = false THEN 'CRITICAL - End of Support'
        WHEN c.months_to_eos <= 3 THEN 'HIGH - Support Ending Soon'
        WHEN c.months_to_eos <= 6 THEN 'MEDIUM - Plan for Upgrade'
        WHEN c.runtime_age_months >= 24 THEN 'MEDIUM - Consider Upgrade'
        ELSE 'LOW - Current and Supported'
    END as upgrade_priority
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{silver_schema}.slv_clusters c ON f.cluster_identifier = c.cluster_id
GROUP BY f.date_key,          c.dbr_version, c.major_version, c.minor_version,
         c.cluster_source, c.is_lts, c.is_current, c.runtime_age_months,
         c.months_to_eos, c.is_supported, f.line_of_business, f.department, f.cost_center, f.environment;

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
    c.autoscale_enabled,
    CASE 
        WHEN c.max_autoscale_workers = c.min_autoscale_workers THEN 'Fixed Size'
        WHEN c.autoscale_enabled = true THEN 'Auto-scaling'
        ELSE 'Manual Scaling'
    END as scaling_type,
    -- Business context
    f.line_of_business,
    f.department,
    f.cost_center,
    -- Usage metrics
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    COUNT(DISTINCT c.cluster_id) as active_clusters,
    SUM(f.list_cost_usd) as total_cost_usd,
    SUM(f.usage_quantity) as total_usage_quantity,
    SUM(f.duration_hours) as total_duration_hours,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    -- Optimization insights
    CASE 
        WHEN c.max_autoscale_workers = c.min_autoscale_workers AND c.max_autoscale_workers > 10 THEN 'Consider Auto-scaling'
        WHEN c.max_autoscale_workers = c.min_autoscale_workers AND c.max_autoscale_workers = 1 THEN 'Single Node - Monitor Performance'
        WHEN c.autoscale_enabled = false AND c.max_autoscale_workers > c.min_autoscale_workers THEN 'Enable Auto-scaling'
        ELSE 'Optimal Configuration'
    END as optimization_suggestion
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{silver_schema}.slv_clusters c ON f.cluster_identifier = c.cluster_id
GROUP BY f.date_key, c.driver_node_type_id, c.node_type_id,
         c.min_autoscale_workers, c.max_autoscale_workers, c.autoscale_enabled,
         f.line_of_business, f.department, f.cost_center;

-- Runtime Modernization Opportunities View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_runtime_modernization_opportunities AS
SELECT 
    f.date_key,
    c.workspace_id,
    c.cluster_id,
    c.cluster_name,
    c.dbr_version,
    c.major_version,
    c.minor_version,
    c.cluster_source,
    c.worker_node_type,
    c.driver_node_type,
    c.min_autoscale_workers,
    c.max_autoscale_workers,
    c.autoscale_enabled,
    -- Business context
    f.line_of_business,
    f.department,
    f.cost_center,
    -- Runtime health assessment
    CASE 
        WHEN c.is_supported = false THEN 'CRITICAL'
        WHEN c.months_to_eos <= 3 THEN 'HIGH'
        WHEN c.months_to_eos <= 6 THEN 'MEDIUM'
        WHEN c.runtime_age_months >= 24 THEN 'MEDIUM'
        ELSE 'LOW'
    END as upgrade_urgency,
    -- Cost impact
    SUM(f.list_cost_usd) as monthly_cost_usd,
    SUM(f.duration_hours) as monthly_hours,
    -- Modernization recommendations
    CASE 
        WHEN c.major_version < 13 THEN 'Upgrade to Runtime 13+ for latest features and security'
        WHEN c.major_version = 13 AND c.minor_version < 3 THEN 'Upgrade to Runtime 13.3+ for LTS support'
        WHEN c.major_version = 14 AND c.minor_version < 2 THEN 'Upgrade to Runtime 14.2+ for latest LTS'
        ELSE 'Runtime is current'
    END as runtime_recommendation,
    CASE 
        WHEN c.autoscale_enabled = false AND c.max_autoscale_workers > c.min_autoscale_workers THEN 'Enable auto-scaling for cost optimization'
        WHEN c.max_autoscale_workers = c.min_autoscale_workers AND c.max_autoscale_workers > 10 THEN 'Consider auto-scaling for variable workloads'
        WHEN c.max_autoscale_workers = 1 THEN 'Monitor performance - consider multi-node for heavy workloads'
        ELSE 'Node configuration is optimal'
    END as scaling_recommendation,
    -- Estimated savings (placeholder for cost optimization calculations)
    CASE 
        WHEN c.major_version < 13 THEN 'High - Newer runtimes offer 10-20% performance improvements'
        WHEN c.autoscale_enabled = false THEN 'Medium - Auto-scaling can reduce costs by 15-30%'
        ELSE 'Low - Current configuration is optimal'
    END as potential_savings
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{silver_schema}.slv_clusters c ON f.cluster_identifier = c.cluster_id
WHERE c.dbr_version IS NOT NULL
GROUP BY f.date_key, c.workspace_id, c.cluster_id, c.cluster_name,
         c.dbr_version, c.major_version, c.minor_version,
         c.cluster_source, c.node_type_id, c.driver_node_type_id,
         c.min_autoscale_workers, c.max_autoscale_workers, c.autoscale_enabled,
         f.line_of_business, f.department, f.cost_center;

-- Runtime Performance Trends View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_runtime_performance_trends AS
SELECT 
    f.date_key,
    c.dbr_version,
    c.major_version,
    c.minor_version,
    c.cluster_source,
    c.worker_node_type,
    -- Performance metrics
    COUNT(DISTINCT f.entity_id) as active_jobs,
    SUM(f.list_cost_usd) as total_cost_usd,
    SUM(f.usage_quantity) as total_usage_quantity,
    SUM(f.duration_hours) as total_duration_hours,
    -- Cost efficiency metrics
    AVG(f.list_cost_usd / NULLIF(f.duration_hours, 0)) as cost_per_hour,
    AVG(f.list_cost_usd / NULLIF(f.usage_quantity, 0)) as cost_per_unit,
    -- Runtime comparison (placeholder for performance benchmarking)
    CASE 
        WHEN c.major_version >= 14 THEN 'Latest Generation'
        WHEN c.major_version >= 13 THEN 'Current Generation'
        WHEN c.major_version >= 12 THEN 'Previous Generation'
        ELSE 'Legacy'
    END as runtime_generation,
    -- Node type efficiency (from Silver layer)
    c.worker_node_type_category as node_optimization_type
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{silver_schema}.slv_clusters c ON f.cluster_identifier = c.cluster_id
WHERE c.dbr_version IS NOT NULL
GROUP BY f.date_key, c.dbr_version, c.major_version, c.minor_version,
         c.cluster_source, c.node_type_id;

-- Cluster Sizing Optimization View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cluster_sizing_optimization AS
SELECT 
    f.date_key,
    c.workspace_id,
    c.cluster_id,
    c.cluster_name,
    c.cluster_name,
    c.worker_node_type,
    c.min_autoscale_workers,
    c.max_autoscale_workers,
    c.autoscale_enabled,
    -- Current usage patterns
    COUNT(DISTINCT f.entity_id) as active_jobs,
    SUM(f.list_cost_usd) as total_cost_usd,
    SUM(f.duration_hours) as total_duration_hours,
    AVG(f.list_cost_usd / NULLIF(f.duration_hours, 0)) as cost_per_hour,
    -- Sizing analysis
    CASE 
        WHEN c.max_autoscale_workers = c.min_autoscale_workers THEN 'Fixed Size'
        WHEN c.autoscale_enabled = true THEN 'Auto-scaling'
        ELSE 'Manual Scaling'
    END as current_scaling,
    -- Optimization recommendations
    CASE 
        WHEN c.max_autoscale_workers = c.min_autoscale_workers AND c.max_autoscale_workers > 10 THEN 'Consider auto-scaling for variable workloads'
        WHEN c.max_autoscale_workers = c.min_autoscale_workers AND c.max_autoscale_workers = 1 THEN 'Monitor performance - consider multi-node for heavy workloads'
        WHEN c.autoscale_enabled = false AND c.max_autoscale_workers > c.min_autoscale_workers THEN 'Enable auto-scaling for cost optimization'
        ELSE 'Current configuration is optimal'
    END as sizing_recommendation,
    -- Cost optimization potential
    CASE 
        WHEN c.autoscale_enabled = false AND c.max_autoscale_workers > c.min_autoscale_workers THEN 'Medium - Auto-scaling can reduce costs by 15-30%'
        WHEN c.max_autoscale_workers = c.min_autoscale_workers AND c.max_autoscale_workers > 10 THEN 'Medium - Consider auto-scaling for variable workloads'
        ELSE 'Low - Current configuration is optimal'
    END as optimization_potential
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{silver_schema}.slv_clusters c ON f.cluster_identifier = c.cluster_id
GROUP BY f.date_key, c.workspace_id, c.cluster_id, c.cluster_name,
         c.worker_node_type, c.min_autoscale_workers, c.max_autoscale_workers, c.autoscale_enabled;
