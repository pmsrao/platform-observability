-- Gold Layer Chargeback Views
-- This file contains cost allocation and chargeback views

-- Cost Allocation by Entity View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_allocation AS
SELECT 
    date_sk,
    workspace_id,
    workspace_name,
    entity_type,
    entity_id,
    entity_name,
    SUM(list_cost_usd) as total_cost_usd,
    SUM(usage_quantity) as total_usage_quantity,
    SUM(duration_hours) as total_duration_hours,
    COUNT(DISTINCT run_id) as total_runs,
    AVG(list_cost_usd) as avg_cost_per_run
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.dim_workspace w ON f.workspace_id = w.workspace_id
JOIN {catalog}.{gold_schema}.dim_entity e ON f.workspace_id = e.workspace_id 
    AND f.entity_type = e.entity_type 
    AND f.entity_id = e.entity_id
GROUP BY date_sk, workspace_id, workspace_name, entity_type, entity_id, entity_name;

-- Department Cost Summary View (Tag-based, not workspace prefix)
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_department_cost_summary AS
SELECT 
    COALESCE(f.department, 'unknown') as department_code,
    w.workspace_name,
    f.date_sk,
    SUM(f.list_cost_usd) as department_total_cost,
    COUNT(DISTINCT f.entity_id) as active_entities,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    COUNT(DISTINCT f.workspace_id) as active_workspaces
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.dim_workspace w ON f.workspace_id = w.workspace_id
GROUP BY COALESCE(f.department, 'unknown'), w.workspace_name, f.date_sk;

-- Cost Trend by SKU View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_sku_cost_trends AS
SELECT 
    date_sk,
    cloud,
    sku_name,
    usage_unit,
    SUM(list_cost_usd) as total_cost_usd,
    SUM(usage_quantity) as total_usage_quantity,
    AVG(list_cost_usd / NULLIF(usage_quantity, 0)) as avg_cost_per_unit,
    COUNT(DISTINCT workspace_id) as active_workspaces
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
JOIN {catalog}.{gold_schema}.dim_sku s ON f.cloud = s.cloud 
    AND f.sku_name = s.sku_name 
    AND f.usage_unit = s.usage_unit
GROUP BY date_sk, cloud, sku_name, usage_unit;

-- Enhanced Tag-Based Chargeback Views

-- Line of Business Cost Summary
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_line_of_business_cost AS
SELECT 
    f.date_sk,
    COALESCE(f.line_of_business, 'Unknown') as line_of_business,
    COALESCE(f.department, 'unknown') as department,
    COALESCE(f.environment, 'dev') as environment,
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.line_of_business, 'Unknown'), COALESCE(f.department, 'unknown'), COALESCE(f.environment, 'dev');

-- Environment Cost Analysis
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_environment_cost AS
SELECT 
    f.date_sk,
    COALESCE(f.environment, 'dev') as environment,
    COALESCE(f.line_of_business, 'Unknown') as line_of_business,
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.environment, 'dev'), COALESCE(f.line_of_business, 'Unknown');

-- Use Case Cost Analysis
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_use_case_cost AS
SELECT 
    f.date_sk,
    COALESCE(f.use_case, 'Unknown') as use_case,
    COALESCE(f.line_of_business, 'Unknown') as line_of_business,
    COALESCE(f.department, 'unknown') as department,
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.use_case, 'Unknown'), COALESCE(f.line_of_business, 'Unknown'), COALESCE(f.department, 'unknown');

-- Cluster Cost Analysis
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cluster_cost AS
SELECT 
    f.date_sk,
    COALESCE(f.cluster_identifier, 'Unknown') as cluster_identifier,
    COALESCE(f.line_of_business, 'Unknown') as line_of_business,
    COALESCE(f.department, 'unknown') as department,
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.cluster_identifier, 'Unknown'), COALESCE(f.line_of_business, 'Unknown'), COALESCE(f.department, 'unknown');

-- Tag Quality and Missing Tags Visibility View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_tag_quality_analysis AS
SELECT 
    f.date_sk,
    f.workspace_id,
    w.workspace_name,
    f.entity_type,
    f.entity_id,
    -- Tag completeness indicators
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
JOIN {catalog}.{gold_schema}.dim_workspace w ON f.workspace_id = w.workspace_id;

-- Tag Coverage Summary View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_tag_coverage_summary AS
SELECT 
    f.date_sk,
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
GROUP BY f.date_sk;

-- Enhanced Chargeback Views with New Tags

-- Cost Center Analysis View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cost_center_analysis AS
SELECT 
    f.date_sk,
    COALESCE(f.cost_center, 'unallocated') as cost_center,
    COALESCE(f.line_of_business, 'Unknown') as line_of_business,
    COALESCE(f.department, 'unknown') as department,
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.cost_center, 'unallocated'), 
         COALESCE(f.line_of_business, 'Unknown'), COALESCE(f.department, 'unknown');

-- Workflow Hierarchy Cost Analysis
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_workflow_hierarchy_cost AS
SELECT 
    f.date_sk,
    COALESCE(f.workflow_level, 'STANDALONE') as workflow_level,
    COALESCE(f.parent_workflow_name, 'None') as parent_workflow_name,
    COALESCE(f.cost_center, 'unallocated') as cost_center,
    COALESCE(f.line_of_business, 'Unknown') as line_of_business,
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.workflow_level, 'STANDALONE'), 
         COALESCE(f.parent_workflow_name, 'None'), COALESCE(f.cost_center, 'unallocated'),
         COALESCE(f.line_of_business, 'Unknown');

-- Cluster Cost Attribution by Workflow
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_cluster_workflow_cost AS
SELECT 
    f.date_sk,
    COALESCE(f.cluster_identifier, 'Unknown') as cluster_identifier,
    COALESCE(f.workflow_level, 'STANDALONE') as workflow_level,
    COALESCE(f.parent_workflow_name, 'None') as parent_workflow_name,
    COALESCE(f.cost_center, 'unallocated') as cost_center,
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.cluster_identifier, 'Unknown'), 
         COALESCE(f.workflow_level, 'STANDALONE'), COALESCE(f.parent_workflow_name, 'None'),
         COALESCE(f.cost_center, 'unallocated');
