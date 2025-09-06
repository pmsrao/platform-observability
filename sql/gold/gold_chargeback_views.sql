-- Gold Layer Chargeback Views
-- This file contains cost allocation and chargeback views

-- Cost Allocation by Entity View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_allocation AS
SELECT 
    f.date_key,
    w.workspace_id,
    w.workspace_name,
    e.entity_type,
    e.entity_id,
    e.name as entity_name,
    SUM(f.list_cost_usd) as total_cost_usd,
    SUM(f.usage_quantity) as total_usage_quantity,
    SUM(f.duration_hours) as total_duration_hours,
    COUNT(DISTINCT f.job_run_id) as total_runs,
    AVG(f.list_cost_usd) as avg_cost_per_run
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, w.workspace_id, w.workspace_name, e.entity_type, e.entity_id, e.name;

-- Department Cost Summary View (Tag-based, not workspace prefix)
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_department_cost_summary AS
SELECT 
    f.department as department_code,
    w.workspace_name,
    f.date_key,
    SUM(f.list_cost_usd) as department_total_cost,
    COUNT(DISTINCT e.entity_id) as active_entities,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    COUNT(DISTINCT w.workspace_id) as active_workspaces
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.department, w.workspace_name, f.date_key;

-- Cost Trend by SKU View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_sku_cost_trends AS
SELECT 
    f.date_key,
    f.cloud,
    s.sku_name,
    f.usage_unit,
    SUM(f.list_cost_usd) as total_cost_usd,
    SUM(f.usage_quantity) as total_usage_quantity,
    AVG(f.list_cost_usd / NULLIF(f.usage_quantity, 0)) as avg_cost_per_unit,
    COUNT(DISTINCT w.workspace_id) as active_workspaces
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_sku s ON f.sku_key = s.sku_key
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
GROUP BY f.date_key, f.cloud, s.sku_name, f.usage_unit;

-- Enhanced Tag-Based Chargeback Views

-- Line of Business Cost Summary
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_line_of_business_cost AS
SELECT 
    f.date_key,
    f.line_of_business,
    f.department,
    f.environment,
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, f.line_of_business, f.department, f.environment;

-- Environment Cost Analysis
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_environment_cost AS
SELECT 
    f.date_key,
    f.environment,
    f.line_of_business,
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, f.environment, f.line_of_business;

-- Use Case Cost Analysis
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_use_case_cost AS
SELECT 
    f.date_key,
    f.use_case,
    f.line_of_business,
    f.department,
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, f.use_case, f.line_of_business, f.department;

-- Cluster Cost Analysis
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cluster_cost AS
SELECT 
    f.date_key,
    f.cluster_identifier,
    f.line_of_business,
    f.department,
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, f.cluster_identifier, f.line_of_business, f.department;

-- Tag Quality and Missing Tags Visibility View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_tag_quality_analysis AS
SELECT 
    f.date_key,
    w.workspace_id,
    w.workspace_name,
    e.entity_type,
    e.entity_id,
    -- Tag completeness indicators (based on default values)
    CASE WHEN f.line_of_business = 'Unknown' THEN 'Missing' ELSE 'Present' END as line_of_business_status,
    CASE WHEN f.department = 'unknown' THEN 'Missing' ELSE 'Present' END as department_status,
    CASE WHEN f.cost_center = 'unallocated' THEN 'Missing' ELSE 'Present' END as cost_center_status,
    CASE WHEN f.environment = 'dev' THEN 'Missing' ELSE 'Present' END as environment_status,
    CASE WHEN f.use_case = 'Unknown' THEN 'Missing' ELSE 'Present' END as use_case_status,
    CASE WHEN f.pipeline_name = 'system' THEN 'Missing' ELSE 'Present' END as pipeline_name_status,
    CASE WHEN f.cluster_identifier = 'Unknown' THEN 'Missing' ELSE 'Present' END as cluster_identifier_status,
    CASE WHEN f.workflow_level = 'STANDALONE' THEN 'Missing' ELSE 'Present' END as workflow_level_status,
    CASE WHEN f.parent_workflow_name = 'None' THEN 'Missing' ELSE 'Present' END as parent_workflow_status,
    -- Count of missing tags
    CASE WHEN f.line_of_business = 'Unknown' THEN 1 ELSE 0 END +
    CASE WHEN f.department = 'unknown' THEN 1 ELSE 0 END +
    CASE WHEN f.cost_center = 'unallocated' THEN 1 ELSE 0 END +
    CASE WHEN f.environment = 'dev' THEN 1 ELSE 0 END +
    CASE WHEN f.use_case = 'Unknown' THEN 1 ELSE 0 END +
    CASE WHEN f.pipeline_name = 'system' THEN 1 ELSE 0 END +
    CASE WHEN f.cluster_identifier = 'Unknown' THEN 1 ELSE 0 END +
    CASE WHEN f.workflow_level = 'STANDALONE' THEN 1 ELSE 0 END +
    CASE WHEN f.parent_workflow_name = 'None' THEN 1 ELSE 0 END as missing_tags_count,
    -- Tag values for analysis
    f.line_of_business,
    f.department,
    f.cost_center,
    f.environment,
    f.use_case,
    f.pipeline_name,
    f.cluster_identifier,
    f.workflow_level,
    f.parent_workflow_name,
    -- Cost and usage data
    f.list_cost_usd,
    f.usage_quantity,
    f.duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key;

-- Tag Coverage Summary View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_tag_coverage_summary AS
SELECT 
    f.date_key,
    -- Overall tag coverage
    COUNT(*) as total_records,
    SUM(CASE WHEN f.line_of_business != 'Unknown' THEN 1 ELSE 0 END) as records_with_line_of_business,
    SUM(CASE WHEN f.department != 'unknown' THEN 1 ELSE 0 END) as records_with_department,
    SUM(CASE WHEN f.cost_center != 'unallocated' THEN 1 ELSE 0 END) as records_with_cost_center,
    SUM(CASE WHEN f.environment != 'dev' THEN 1 ELSE 0 END) as records_with_environment,
    SUM(CASE WHEN f.use_case != 'Unknown' THEN 1 ELSE 0 END) as records_with_use_case,
    SUM(CASE WHEN f.pipeline_name != 'system' THEN 1 ELSE 0 END) as records_with_pipeline_name,
    SUM(CASE WHEN f.cluster_identifier != 'Unknown' THEN 1 ELSE 0 END) as records_with_cluster_identifier,
    SUM(CASE WHEN f.workflow_level != 'STANDALONE' THEN 1 ELSE 0 END) as records_with_workflow_level,
    SUM(CASE WHEN f.parent_workflow_name != 'None' THEN 1 ELSE 0 END) as records_with_parent_workflow,
    -- Coverage percentages
    ROUND(SUM(CASE WHEN f.line_of_business != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as line_of_business_coverage_pct,
    ROUND(SUM(CASE WHEN f.department != 'unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as department_coverage_pct,
    ROUND(SUM(CASE WHEN f.cost_center != 'unallocated' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cost_center_coverage_pct,
    ROUND(SUM(CASE WHEN f.environment != 'dev' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as environment_coverage_pct,
    ROUND(SUM(CASE WHEN f.use_case != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as use_case_coverage_pct,
    ROUND(SUM(CASE WHEN f.pipeline_name != 'system' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pipeline_name_coverage_pct,
    ROUND(SUM(CASE WHEN f.cluster_identifier != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cluster_identifier_coverage_pct,
    ROUND(SUM(CASE WHEN f.workflow_level != 'STANDALONE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as workflow_level_coverage_pct,
    ROUND(SUM(CASE WHEN f.parent_workflow_name != 'None' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as parent_workflow_coverage_pct,
    -- Total cost for context
    SUM(f.list_cost_usd) as total_cost_usd
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
GROUP BY f.date_key;

-- Enhanced Chargeback Views with New Tags

-- Cost Center Analysis View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_center_analysis AS
SELECT 
    f.date_key,
    f.cost_center,
    f.line_of_business,
    f.department,
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, f.cost_center, f.line_of_business, f.department;

-- Workflow Hierarchy Cost Analysis
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_workflow_hierarchy_cost AS
SELECT 
    f.date_key,
    f.workflow_level,
    f.parent_workflow_name,
    f.cost_center,
    f.line_of_business,
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, f.workflow_level, f.parent_workflow_name, f.cost_center, f.line_of_business;

-- Cluster Cost Attribution by Workflow
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cluster_workflow_cost AS
SELECT 
    f.date_key,
    f.cluster_identifier,
    f.workflow_level,
    f.parent_workflow_name,
    f.cost_center,
    COUNT(DISTINCT w.workspace_id) as active_workspaces,
    COUNT(DISTINCT e.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
GROUP BY f.date_key, f.cluster_identifier, f.workflow_level, f.parent_workflow_name, f.cost_center;
