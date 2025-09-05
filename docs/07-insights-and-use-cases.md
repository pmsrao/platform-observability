# Insights and Use Cases Guide

## Overview

This document provides a comprehensive guide to the insights and use cases available in the Platform Observability solution. It's organized by persona and insight type to help users quickly find relevant information.

## Table of Contents

1. [Persona-Based Use Cases](#persona-based-use-cases)
2. [Insight Categories](#insight-categories)
3. [Implementation Examples](#implementation-examples)
4. [Best Practices](#best-practices)

## Persona-Based Use Cases

### 🏢 **Finance & Cost Management Teams**

#### **Cost Allocation & Chargeback**
- **Use Case**: Allocate Databricks costs to business units, departments, and cost centers
- **Key Views**: `v_cost_center_analysis`, `v_line_of_business_cost`, `v_department_cost_summary`
- **Business Value**: Accurate cost attribution for budgeting and chargeback
- **Example Query**:
```sql
-- Monthly cost by cost center and line of business
SELECT 
    DATE_FORMAT(date_sk, 'yyyy-MM') as month,
    cost_center,
    line_of_business,
    SUM(list_cost_usd) as total_cost_usd
FROM gold.gld_fact_usage_priced_day
WHERE cost_center != 'unallocated'
GROUP BY DATE_FORMAT(date_sk, 'yyyy-MM'), cost_center, line_of_business
ORDER BY month, total_cost_usd DESC;
```

#### **Budget Tracking & Forecasting**
- **Use Case**: Monitor spending against budgets and forecast future costs
- **Key Views**: `v_cost_trends`, `v_workspace_cost_summary`
- **Business Value**: Proactive cost management and budget planning
- **Example Query**:
```sql
-- Daily cost trends for budget monitoring
SELECT 
    date_sk,
    SUM(list_cost_usd) as daily_cost_usd,
    AVG(SUM(list_cost_usd)) OVER (ORDER BY date_sk ROWS 7 PRECEDING) as rolling_7day_avg
FROM gold.gld_fact_usage_priced_day
GROUP BY date_sk
ORDER BY date_sk;
```

### 🚀 **Platform Engineers & DevOps**

#### **Runtime Modernization & Optimization**
- **Use Case**: Identify clusters using outdated runtimes and plan upgrades
- **Key Views**: `v_runtime_version_analysis`, `v_runtime_modernization_opportunities`
- **Business Value**: Reduce security risks, improve performance, optimize costs
- **Example Query**:
```sql
-- Clusters using end-of-support runtimes
SELECT 
    cluster_id,
    cluster_name,
    databricks_runtime_version,
    months_to_eos,
    is_supported,
    upgrade_priority
FROM gold.v_runtime_modernization_opportunities
WHERE is_supported = false OR months_to_eos <= 6
ORDER BY months_to_eos ASC;
```

#### **Cluster Sizing & Performance Optimization**
- **Use Case**: Analyze cluster sizing patterns and identify optimization opportunities with detailed infrastructure insights
- **Key Views**: `v_cluster_sizing_optimization`, `v_node_type_analysis`, cluster dimension analysis
- **Business Value**: Right-size clusters, reduce waste, improve performance, optimize infrastructure costs

##### **Node Type Analysis**
```sql
-- Analyze costs by node type category
SELECT 
    c.worker_node_type_category,
    c.dbr_version,
    SUM(f.list_cost_usd) as total_cost,
    AVG(f.duration_hours) as avg_duration
FROM gld_fact_usage_priced_day f
JOIN gld_dim_cluster c ON f.cluster_key = c.cluster_key
WHERE f.date_key >= 20250101
GROUP BY c.worker_node_type_category, c.dbr_version
ORDER BY total_cost DESC;
```

##### **DBR Version Performance Analysis**
```sql
-- Compare performance across DBR versions
SELECT 
    c.dbr_version,
    COUNT(DISTINCT f.job_run_id) as total_runs,
    AVG(f.duration_hours) as avg_duration,
    SUM(f.list_cost_usd) as total_cost,
    SUM(f.list_cost_usd) / COUNT(DISTINCT f.job_run_id) as cost_per_run
FROM gld_fact_usage_priced_day f
JOIN gld_dim_cluster c ON f.cluster_key = c.cluster_key
WHERE f.date_key >= 20250101
GROUP BY c.dbr_version
ORDER BY cost_per_run;
```

##### **Cluster Configuration Optimization**
```sql
-- Identify clusters with suboptimal autoscaling
SELECT 
    c.cluster_name,
    c.worker_node_type_category,
    c.min_autoscale_workers,
    c.max_autoscale_workers,
    AVG(f.duration_hours) as avg_duration,
    SUM(f.list_cost_usd) as total_cost
FROM gld_fact_usage_priced_day f
JOIN gld_dim_cluster c ON f.cluster_key = c.cluster_key
WHERE f.date_key >= 20250101
GROUP BY c.cluster_name, c.worker_node_type_category, 
         c.min_autoscale_workers, c.max_autoscale_workers
HAVING (c.max_autoscale_workers - c.min_autoscale_workers) > 5
ORDER BY total_cost DESC;
```

##### **Historical Cluster Evolution (SCD2)**
```sql
-- Track cluster configuration changes over time
SELECT 
    c.cluster_name,
    c.worker_node_type,
    c.dbr_version,
    c.valid_from,
    c.valid_to,
    c.is_current,
    DATEDIFF(COALESCE(c.valid_to, CURRENT_DATE()), c.valid_from) as days_active
FROM gld_dim_cluster c
WHERE c.cluster_name = 'data-processing-cluster'
ORDER BY c.valid_from;
```

#### **Workflow Hierarchy Management**
- **Use Case**: Understand and optimize workflow relationships and dependencies
- **Key Views**: `v_workflow_hierarchy_cost`, `v_cluster_workflow_cost`
- **Business Value**: Better resource planning, dependency management, cost attribution
- **Example Query**:
```sql
-- Workflow hierarchy cost analysis
SELECT 
    workflow_level,
    parent_workflow_name,
    COUNT(DISTINCT entity_id) as entity_count,
    SUM(list_cost_usd) as total_cost_usd,
    AVG(list_cost_usd) as avg_cost_per_entity
FROM gold.gld_fact_usage_priced_day
WHERE workflow_level != 'STANDALONE'
GROUP BY workflow_level, parent_workflow_name
ORDER BY total_cost_usd DESC;
```

### 📊 **Data Engineers & Analysts**

#### **Data Quality & Tag Coverage Analysis**
- **Use Case**: Monitor tag quality and identify areas for improvement
- **Key Views**: `v_tag_quality_analysis`, `v_tag_coverage_summary`
- **Business Value**: Improve data quality, ensure complete cost attribution
- **Example Query**:
```sql
-- Tag coverage analysis by workspace
SELECT 
    workspace_id,
    tag_name,
    total_records,
    tagged_records,
    coverage_percentage,
    missing_records
FROM gold.v_tag_coverage_summary
WHERE coverage_percentage < 90
ORDER BY coverage_percentage ASC;
```

#### **Usage Pattern Analysis**
- **Use Case**: Understand usage patterns across different dimensions
- **Key Views**: `v_usage_patterns`, `v_entity_usage_summary`
- **Business Value**: Optimize resource allocation, identify trends
- **Example Query**:
```sql
-- Usage patterns by environment and time
SELECT 
    environment,
    HOUR(usage_start_time) as hour_of_day,
    COUNT(*) as usage_count,
    SUM(usage_quantity) as total_usage,
    AVG(usage_quantity) as avg_usage
FROM silver.slv_usage_txn
GROUP BY environment, HOUR(usage_start_time)
ORDER BY environment, hour_of_day;
```

### 🎯 **Business Stakeholders & Product Owners**

#### **Business Unit Performance Analysis**
- **Use Case**: Track performance and costs by business unit and use case
- **Key Views**: `v_line_of_business_cost`, `v_use_case_analysis`
- **Business Value**: Business unit accountability, ROI analysis
- **Example Query**:
```sql
-- Business unit cost and usage summary
SELECT 
    line_of_business,
    use_case,
    COUNT(DISTINCT entity_id) as active_entities,
    SUM(list_cost_usd) as total_cost_usd,
    SUM(usage_quantity) as total_usage,
    AVG(list_cost_usd) as avg_cost_per_entity
FROM gold.gld_fact_usage_priced_day
WHERE line_of_business != 'Unknown'
GROUP BY line_of_business, use_case
ORDER BY total_cost_usd DESC;
```

#### **Project & Initiative Tracking**
- **Use Case**: Monitor costs and performance for specific projects or initiatives
- **Key Views**: `v_project_cost_summary`, `v_initiative_tracking`
- **Business Value**: Project cost tracking, initiative success measurement
- **Example Query**:
```sql
-- Project cost tracking by month
SELECT 
    DATE_FORMAT(date_sk, 'yyyy-MM') as month,
    line_of_business,
    sub_project,
    SUM(list_cost_usd) as project_cost_usd,
    COUNT(DISTINCT entity_id) as active_entities
FROM gold.gld_fact_usage_priced_day
WHERE line_of_business = 'DataPlatform' AND sub_project = 'Migration2024'
GROUP BY DATE_FORMAT(date_sk, 'yyyy-MM'), line_of_business, sub_project
ORDER BY month;
```

## Insight Categories

### 💰 **Billing & Cost Insights**

#### **Cost Attribution**
- **Purpose**: Accurately attribute costs to business units, projects, and teams
- **Key Metrics**: Cost by cost center, line of business, department, environment
- **Business Impact**: Improved budgeting, accurate chargeback, cost transparency

#### **Cost Optimization**
- **Purpose**: Identify cost reduction opportunities and optimization strategies
- **Key Metrics**: Cost trends, usage patterns, resource efficiency
- **Business Impact**: Reduced operational costs, improved resource utilization

#### **Budget Management**
- **Purpose**: Monitor spending against budgets and forecast future costs
- **Key Metrics**: Budget vs actual, cost projections, spending alerts
- **Business Impact**: Better financial planning, proactive cost management

### ⚡ **Performance & Infrastructure Insights**

#### **Runtime Analysis**
- **Purpose**: Monitor runtime versions and plan modernization initiatives
- **Key Metrics**: Runtime distribution, end-of-support dates, upgrade priorities
- **Business Impact**: Reduced security risks, improved performance, cost optimization

#### **Cluster Optimization**
- **Purpose**: Optimize cluster sizing and configuration for better performance
- **Key Metrics**: Node type usage, auto-scaling patterns, sizing efficiency
- **Business Impact**: Improved performance, reduced waste, better resource planning

#### **Workflow Management**
- **Purpose**: Understand and optimize workflow relationships and dependencies
- **Key Metrics**: Workflow hierarchy, dependency chains, resource allocation
- **Business Impact**: Better resource planning, improved efficiency, cost attribution

### 🏷️ **Data Quality & Governance Insights**

#### **Tag Quality Monitoring**
- **Purpose**: Ensure data quality and complete cost attribution
- **Key Metrics**: Tag coverage, missing tags, data completeness
- **Business Impact**: Improved data quality, accurate reporting, better decision making

#### **Compliance & Auditing**
- **Purpose**: Track changes and maintain audit trails for compliance
- **Key Metrics**: Change history, data lineage, access patterns
- **Business Impact**: Regulatory compliance, audit readiness, data governance

### 📈 **Operational & Business Insights**

#### **Usage Pattern Analysis**
- **Purpose**: Understand how resources are used across the organization
- **Key Metrics**: Usage trends, peak usage times, resource utilization
- **Business Impact**: Better resource planning, capacity management, cost optimization

#### **Business Unit Accountability**
- **Purpose**: Track performance and costs by business unit and team
- **Key Metrics**: Cost per business unit, usage efficiency, ROI analysis
- **Business Impact**: Business unit accountability, resource optimization, strategic planning

## Implementation Examples

### **Cost Center Analysis Dashboard**
```sql
-- Create a comprehensive cost center dashboard
WITH cost_summary AS (
    SELECT 
        cost_center,
        line_of_business,
        department,
        DATE_FORMAT(date_sk, 'yyyy-MM') as month,
        SUM(list_cost_usd) as monthly_cost,
        COUNT(DISTINCT entity_id) as active_entities
    FROM gold.gld_fact_usage_priced_day
    WHERE cost_center != 'unallocated'
    GROUP BY cost_center, line_of_business, department, DATE_FORMAT(date_sk, 'yyyy-MM')
)
SELECT 
    cost_center,
    line_of_business,
    department,
    month,
    monthly_cost,
    active_entities,
    monthly_cost / active_entities as cost_per_entity,
    LAG(monthly_cost) OVER (PARTITION BY cost_center ORDER BY month) as prev_month_cost,
    (monthly_cost - LAG(monthly_cost) OVER (PARTITION BY cost_center ORDER BY month)) / 
        LAG(monthly_cost) OVER (PARTITION BY cost_center ORDER BY month) * 100 as cost_change_pct
FROM cost_summary
ORDER BY cost_center, month;
```

### **Runtime Modernization Planning**
```sql
-- Identify clusters for runtime modernization
WITH runtime_analysis AS (
    SELECT 
        c.cluster_id,
        c.cluster_name,
        c.databricks_runtime_version,
        c.major_version,
        c.minor_version,
        c.runtime_age_months,
        c.is_lts,
        c.is_current,
        r.months_to_eos,
        r.is_supported,
        CASE
            WHEN r.is_supported = false THEN 'CRITICAL - End of Support'
            WHEN r.months_to_eos <= 3 THEN 'HIGH - Support Ending Soon'
            WHEN r.months_to_eos <= 6 THEN 'MEDIUM - Plan for Upgrade'
            WHEN c.runtime_age_months >= 24 THEN 'MEDIUM - Consider Upgrade'
            ELSE 'LOW - Current and Supported'
        END as upgrade_priority
    FROM silver.slv_clusters c
)
SELECT 
    upgrade_priority,
    COUNT(*) as cluster_count,
    AVG(runtime_age_months) as avg_runtime_age,
    STRING_AGG(DISTINCT spark_version, ', ') as affected_runtimes
FROM runtime_analysis
GROUP BY upgrade_priority
ORDER BY 
    CASE upgrade_priority
        WHEN 'CRITICAL - End of Support' THEN 1
        WHEN 'HIGH - Support Ending Soon' THEN 2
        WHEN 'MEDIUM - Plan for Upgrade' THEN 3
        WHEN 'MEDIUM - Consider Upgrade' THEN 4
        ELSE 5
    END;
```

### **Workflow Cost Attribution**
```sql
-- Analyze costs by workflow hierarchy
WITH workflow_costs AS (
    SELECT 
        workflow_level,
        parent_workflow_name,
        line_of_business,
        cost_center,
        DATE_FORMAT(date_sk, 'yyyy-MM') as month,
        SUM(list_cost_usd) as total_cost,
        COUNT(DISTINCT entity_id) as entity_count,
        COUNT(DISTINCT workspace_id) as workspace_count
    FROM gold.gld_fact_usage_priced_day
    WHERE workflow_level != 'STANDALONE'
    GROUP BY workflow_level, parent_workflow_name, line_of_business, cost_center, DATE_FORMAT(date_sk, 'yyyy-MM')
)
SELECT 
    month,
    workflow_level,
    parent_workflow_name,
    line_of_business,
    cost_center,
    total_cost,
    entity_count,
    workspace_count,
    total_cost / entity_count as cost_per_entity,
    total_cost / workspace_count as cost_per_workspace
FROM workflow_costs
ORDER BY month DESC, total_cost DESC;
```

## Best Practices

### **Cost Management**
1. **Regular Monitoring**: Set up daily/weekly cost monitoring dashboards
2. **Tag Strategy**: Implement consistent tagging strategy across all resources
3. **Budget Alerts**: Configure alerts for budget thresholds and unusual spending
4. **Cost Reviews**: Conduct monthly cost review meetings with stakeholders

### **Performance Optimization**
1. **Runtime Updates**: Plan quarterly runtime modernization reviews
2. **Cluster Sizing**: Regularly review and optimize cluster configurations
3. **Auto-scaling**: Enable auto-scaling where appropriate to reduce waste
4. **Performance Monitoring**: Track performance metrics and identify bottlenecks

### **Data Quality**
1. **Tag Validation**: Implement automated tag validation and alerts
2. **Coverage Monitoring**: Track tag coverage and address missing tags
3. **Data Lineage**: Maintain clear data lineage for audit purposes
4. **Regular Audits**: Conduct quarterly data quality audits

### **Reporting & Communication**
1. **Stakeholder Dashboards**: Create role-based dashboards for different audiences
2. **Regular Updates**: Provide weekly/monthly updates to stakeholders
3. **Actionable Insights**: Focus on insights that drive action and improvement
4. **Success Metrics**: Define and track success metrics for the observability program

## Next Steps

1. **Review Current State**: Assess current tag coverage and data quality
2. **Define Priorities**: Identify high-impact use cases for immediate implementation
3. **Create Dashboards**: Build role-based dashboards for key stakeholders
4. **Establish Processes**: Set up regular review and optimization processes
5. **Measure Success**: Track improvements in cost, performance, and data quality
