# Tag Processing and Chargeback Model

## Overview

This document explains the tag processing system, hierarchy, and chargeback model implemented in the Platform Observability solution. The system provides comprehensive cost allocation and chargeback capabilities based on normalized business tags rather than workspace naming conventions.

## Table of Contents

1. [Tag Hierarchy and Injection Points](#tag-hierarchy-and-injection-points)
2. [Tag Processing Flow](#tag-processing-flow)
3. [Normalized Tag Schema](#normalized-tag-schema)
4. [Chargeback Views](#chargeback-views)
5. [Tag Quality Monitoring](#tag-quality-monitoring)
6. [Implementation Details](#implementation-details)

## Tag Hierarchy and Injection Points

### **Tag Injection Hierarchy**

The tag processing follows a **multi-level hierarchy** where tags can be injected at different levels:

```
1. WORKSPACE LEVEL (Bronze Layer)
   ├── workspace_id, workspace_name, workspace_url
   └── Basic workspace metadata

2. ENTITY LEVEL (Bronze Layer)
   ├── job_id, pipeline_id, name, run_as
   └── Entity configuration and ownership

3. USAGE LEVEL (Bronze Layer)
   ├── tags MAP<STRING, STRING> ← PRIMARY TAG SOURCE
   ├── usage_metadata (job_id, job_run_id, cluster_id)
   └── Runtime usage context

4. CLUSTER LEVEL (Bronze Layer)
   ├── cluster_id, cluster_name
   └── Infrastructure context
```

### **Where Tags Are Injected**

#### **A. Bronze Layer - Raw Data Ingestion**
```sql
-- In bronze_sys_billing_usage_raw table
tags MAP<STRING, STRING>  -- Primary source of business tags
usage_metadata STRUCT<job_id:STRING, job_run_id:STRING, cluster_id:STRING>
```

**Tag Sources:**
- **Cloud Provider Billing**: AWS tags, Azure tags, GCP labels
- **Databricks Workspace**: Workspace-level tags
- **Job/Pipeline Configuration**: Job and pipeline metadata
- **Cluster Configuration**: Cluster tags and metadata

#### **B. Silver Layer - Tag Normalization**
```python
# TagProcessor.enrich_usage() processes tags from Bronze
u = TagProcessor.extract_from_map(usage_df, "custom_tags")
u = TagProcessor.ensure_defaults(u)
u = TagProcessor.add_identity_fields(u)
u = TagProcessor.add_cost_attribution_key(u)
```

#### **C. Gold Layer - Tag-Based Chargeback**
```sql
-- Normalized tags are included in fact tables
line_of_business, department, environment, use_case, pipeline_name, cluster_identifier
```

## Tag Processing Flow

### **1. Tag Extraction (Bronze → Silver)**
```
Bronze Layer: tags MAP<STRING, STRING>
    ↓
TagProcessor.extract_from_map()
    ↓
Normalized columns with defaults
    ↓
Silver Layer: silver_usage_txn table
```

### **2. Workflow Hierarchy Processing**
```
Bronze Layer: Jobs with parent_workflow_id, workflow_type
    ↓
TagProcessor.enrich_workflow_hierarchy()
    ↓
workflow_level (PARENT/SUB_WORKFLOW/STANDALONE)
parent_workflow_name (WF_<id> or None)
    ↓
Silver Layer: Enhanced workflow context
```

### **3. Cluster-Job Tag Inheritance**
```
Job Layer: Tags applied to jobs
    ↓
TagProcessor.enrich_cluster_with_job_tags()
    ↓
Cluster inherits job tags as inherited_* fields
    ↓
Usage records get cluster context + inherited tags
```

### **2. Tag Normalization (Silver Layer)**
```python
# Required tag mapping
REQUIRED_TAGS = {
    "project": "line_of_business",      # Business unit
    "sub_project": "department",        # Department/team
    "environment": "environment",       # Dev/Stage/Prod
    "data_product": "use_case",         # Use case/purpose
    "job_pipeline": "pipeline_name",    # Pipeline identifier
    "cluster_name": "cluster_identifier" # Cluster context
}
```

### **3. Tag Aggregation (Silver → Gold)**
```
Silver Layer: Individual usage records with tags
    ↓
Gold Layer: Daily aggregated facts with MAX(tags)
    ↓
Chargeback views: Tag-based cost allocation
```

## Normalized Tag Schema

### **Required Tags and Defaults**

| **Source Tag Key** | **Normalized Column** | **Default Value** | **Business Purpose** |
|-------------------|----------------------|-------------------|---------------------|
| `project` | `line_of_business` | "Unknown" | Business unit allocation |
| `sub_project` | `department` | "general" | Team/department tracking |
| `cost_center` | `cost_center` | "unallocated" | Financial cost center allocation |
| `environment` | `environment` | "dev" | Environment-based costing |
| `data_product` | `use_case` | "Unknown" | Use case analysis |
| `job_pipeline` | `pipeline_name` | "system" | Pipeline cost tracking |
| `cluster_name` | `cluster_identifier` | "Unknown" | Infrastructure costing |
| `workflow_level` | `workflow_level` | "STANDALONE" | Workflow hierarchy level |
| `parent_workflow` | `parent_workflow_name` | "None" | Parent workflow identifier |

### **Tag Validation Rules**

#### **Environment Normalization**
```python
VALID_ENVS = ["prod", "stage", "uat", "dev", "production", "staging", "development", "user_acceptance"]

def normalize_environment(col):
    return (F.when(F.lower(col).isin("production", "prod"), F.lit("prod"))
             .when(F.lower(col).isin("staging", "stage"), F.lit("stage"))
             .when(F.lower(col).isin("user_acceptance", "uat"), F.lit("uat"))
             .when(F.lower(col).isin("development", "dev"), F.lit("dev"))
             .otherwise(F.lower(col)))
```

#### **Cluster Identifier Logic**
```python
def attach_cluster_identifier(df, cluster_source_col, cluster_name_col):
    return df.withColumn(
        "cluster_identifier",
        F.when(F.col(cluster_source_col) == F.lit("JOB"), F.lit("Job_Cluster"))
         .when(F.col(cluster_name_col).isNotNull() & (F.col(cluster_name_col) != F.lit("")), F.col(cluster_name_col))
         .otherwise(F.lit("Unknown"))
    )
```

## Chargeback Views

### **1. Core Cost Allocation Views**

#### **A. Cost Allocation by Entity**
```sql
CREATE OR REPLACE VIEW v_cost_allocation AS
SELECT 
    date_sk, workspace_id, workspace_name, entity_type, entity_id, entity_name,
    SUM(list_cost_usd) as total_cost_usd,
    SUM(usage_quantity) as total_usage_quantity,
    SUM(duration_hours) as total_duration_hours,
    COUNT(DISTINCT run_id) as total_runs,
    AVG(list_cost_usd) as avg_cost_per_run
FROM gld_fact_usage_priced_day f
JOIN dim_workspace w ON f.workspace_id = w.workspace_id
JOIN dim_entity e ON f.workspace_id = e.workspace_id 
    AND f.entity_type = e.entity_type 
    AND f.entity_id = e.entity_id
GROUP BY date_sk, workspace_id, workspace_name, entity_type, entity_id, entity_name;
```

#### **B. Department Cost Summary (Tag-Based)**
```sql
CREATE OR REPLACE VIEW v_department_cost_summary AS
SELECT 
    COALESCE(f.department, 'unknown') as department_code,  -- Uses normalized tag, not workspace prefix
    w.workspace_name,
    f.date_sk,
    SUM(f.list_cost_usd) as department_total_cost,
    COUNT(DISTINCT f.entity_id) as active_entities,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    COUNT(DISTINCT f.workspace_id) as active_workspaces
FROM gld_fact_usage_priced_day f
JOIN dim_workspace w ON f.workspace_id = w.workspace_id
GROUP BY COALESCE(f.department, 'unknown'), w.workspace_name, f.date_sk;
```

### **2. Enhanced Tag-Based Views**

#### **A. Line of Business Cost Analysis**
```sql
CREATE OR REPLACE VIEW v_line_of_business_cost AS
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
FROM gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.line_of_business, 'Unknown'), 
         COALESCE(f.department, 'unknown'), COALESCE(f.environment, 'dev');
```

#### **B. Environment Cost Analysis**
```sql
CREATE OR REPLACE VIEW v_environment_cost AS
SELECT 
    f.date_sk,
    COALESCE(f.environment, 'dev') as environment,
    COALESCE(f.line_of_business, 'Unknown') as line_of_business,
    COUNT(DISTINCT f.workspace_id) as active_workspaces,
    COUNT(DISTINCT f.entity_id) as active_entities,
    SUM(f.list_cost_usd) as total_cost_usd,
    AVG(f.list_cost_usd) as avg_cost_per_entity,
    SUM(f.duration_hours) as total_duration_hours
FROM gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.environment, 'dev'), 
         COALESCE(f.line_of_business, 'Unknown');
```

### **3. SKU and Infrastructure Views**

#### **A. SKU Cost Trends**
```sql
CREATE OR REPLACE VIEW v_sku_cost_trends AS
SELECT 
    date_sk, cloud, sku_name, usage_unit,
    SUM(list_cost_usd) as total_cost_usd,
    SUM(usage_quantity) as total_usage_quantity,
    AVG(list_cost_usd / NULLIF(usage_quantity, 0)) as avg_cost_per_unit,
    COUNT(DISTINCT workspace_id) as active_workspaces
FROM gld_fact_usage_priced_day f
JOIN dim_sku s ON f.cloud = s.cloud AND f.sku_name = s.sku_name AND f.usage_unit = s.usage_unit
GROUP BY date_sk, cloud, sku_name, usage_unit;
```

#### **B. Cluster Cost Analysis**
```sql
CREATE OR REPLACE VIEW v_cluster_cost AS
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
FROM gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.cluster_identifier, 'Unknown'), 
         COALESCE(f.line_of_business, 'Unknown'), COALESCE(f.department, 'unknown');
```

### **4. Enhanced Workflow and Cost Center Views**

#### **A. Cost Center Analysis**
```sql
CREATE OR REPLACE VIEW v_cost_center_analysis AS
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
FROM gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.cost_center, 'unallocated'), 
         COALESCE(f.line_of_business, 'Unknown'), COALESCE(f.department, 'unknown');
```

#### **B. Workflow Hierarchy Cost Analysis**
```sql
CREATE OR REPLACE VIEW v_workflow_hierarchy_cost AS
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
FROM gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.workflow_level, 'STANDALONE'), 
         COALESCE(f.parent_workflow_name, 'None'), COALESCE(f.cost_center, 'unallocated'),
         COALESCE(f.line_of_business, 'Unknown');
```

#### **C. Cluster Cost Attribution by Workflow**
```sql
CREATE OR REPLACE VIEW v_cluster_workflow_cost AS
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
FROM gld_fact_usage_priced_day f
GROUP BY f.date_sk, COALESCE(f.cluster_identifier, 'Unknown'), 
         COALESCE(f.workflow_level, 'STANDALONE'), COALESCE(f.parent_workflow_name, 'None'),
         COALESCE(f.cost_center, 'unallocated');
```

## Tag Quality Monitoring

### **1. Tag Quality Analysis View**

```sql
CREATE OR REPLACE VIEW v_tag_quality_analysis AS
SELECT 
    f.date_sk, f.workspace_id, w.workspace_name, f.entity_type, f.entity_id,
    -- Tag completeness indicators
    CASE WHEN f.line_of_business = 'Unknown' THEN 'Missing' ELSE 'Present' END as line_of_business_status,
    CASE WHEN f.department = 'unknown' THEN 'Missing' ELSE 'Present' END as department_status,
    CASE WHEN f.environment = 'dev' THEN 'Missing' ELSE 'Present' END as environment_status,
    CASE WHEN f.use_case = 'Unknown' THEN 'Missing' ELSE 'Present' END as use_case_status,
    CASE WHEN f.pipeline_name = 'system' THEN 'Missing' ELSE 'Present' END as pipeline_name_status,
    CASE WHEN f.cluster_identifier = 'Unknown' THEN 'Missing' ELSE 'Present' END as cluster_identifier_status,
    -- Count of missing tags
    CASE WHEN f.line_of_business = 'Unknown' THEN 1 ELSE 0 END +
    CASE WHEN f.department = 'unknown' THEN 1 ELSE 0 END +
    CASE WHEN f.environment = 'dev' THEN 1 ELSE 0 END +
    CASE WHEN f.use_case = 'Unknown' THEN 1 ELSE 0 END +
    CASE WHEN f.pipeline_name = 'system' THEN 1 ELSE 0 END +
    CASE WHEN f.cluster_identifier = 'Unknown' THEN 1 ELSE 0 END as missing_tags_count,
    -- Tag values and cost data
    f.line_of_business, f.department, f.environment, f.use_case, f.pipeline_name, f.cluster_identifier,
    f.list_cost_usd, f.usage_quantity, f.duration_hours
FROM gld_fact_usage_priced_day f
JOIN dim_workspace w ON f.workspace_id = w.workspace_id;
```

### **2. Tag Coverage Summary View**

```sql
CREATE OR REPLACE VIEW v_tag_coverage_summary AS
SELECT 
    f.date_sk,
    -- Overall tag coverage
    COUNT(*) as total_records,
    SUM(CASE WHEN f.line_of_business != 'Unknown' THEN 1 ELSE 0 END) as records_with_line_of_business,
    SUM(CASE WHEN f.department != 'unknown' THEN 1 ELSE 0 END) as records_with_department,
    SUM(CASE WHEN f.environment != 'dev' THEN 1 ELSE 0 END) as records_with_environment,
    SUM(CASE WHEN f.use_case != 'Unknown' THEN 1 ELSE 0 END) as records_with_use_case,
    SUM(CASE WHEN f.pipeline_name != 'system' THEN 1 ELSE 0 END) as records_with_pipeline_name,
    SUM(CASE WHEN f.cluster_identifier != 'Unknown' THEN 1 ELSE 0 END) as records_with_cluster_identifier,
    -- Coverage percentages
    ROUND(SUM(CASE WHEN f.line_of_business != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as line_of_business_coverage_pct,
    ROUND(SUM(CASE WHEN f.department != 'unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as department_coverage_pct,
    ROUND(SUM(CASE WHEN f.environment != 'dev' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as environment_coverage_pct,
    ROUND(SUM(CASE WHEN f.use_case != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as use_case_coverage_pct,
    ROUND(SUM(CASE WHEN f.pipeline_name != 'system' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pipeline_name_coverage_pct,
    ROUND(SUM(CASE WHEN f.cluster_identifier != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cluster_identifier_coverage_pct,
    -- Total cost for context
    SUM(f.list_cost_usd) as total_cost_usd
FROM gld_fact_usage_priced_day f
GROUP BY f.date_sk;
```

## Implementation Details

### **1. Tag Processing in Silver Layer**

```python
# In silver_build_dlt.py
@dlt.table(name="silver_usage_txn", comment="Usage + pricing + entity/run + date_sk + normalized business tags")
@dlt.expect("nonneg_qty","usage_quantity >= 0")
@dlt.expect("valid_time","usage_end_time >= usage_start_time")
def silver_usage_txn():
    u = with_entity_and_run(usage_new)
    u = join_prices(u, dlt.read("silver_price_scd"))
    # Enrich with normalized tags and identity fields
    u = TagProcessor.enrich_usage(u)
    return u
```

### **2. Tag Aggregation in Gold Layer**

```sql
-- In gold_facts.sql
SELECT u.date_sk, u.workspace_id, u.entity_type, u.entity_id, u.cloud, u.sku_name, u.usage_unit,
       SUM(u.usage_quantity) AS usage_quantity, 
       SUM(u.list_cost_usd) AS list_cost_usd, 
       SUM(u.duration_hours) AS duration_hours,
       -- Include normalized tags for chargeback analysis
       MAX(u.line_of_business) AS line_of_business,
       MAX(u.department) AS department,
       MAX(u.environment) AS environment,
       MAX(u.use_case) AS use_case,
       MAX(u.pipeline_name) AS pipeline_name,
       MAX(u.cluster_identifier) AS cluster_identifier
FROM LIVE.silver_usage_txn u
JOIN imp_dates d ON d.date_sk = u.date_sk
GROUP BY u.date_sk, u.workspace_id, u.entity_type, u.entity_id, u.cloud, u.sku_name, u.usage_unit
```

### **3. Cost Attribution Key**

```python
# Built from normalized tags for consistent cost allocation
def add_cost_attribution_key(df):
    return df.withColumn(
        "cost_attribution_key",
        F.concat_ws(
            "|",
            F.col("line_of_business"),
            F.col("department"),
            F.col("environment"),
            F.col("use_case"),
            F.col("pipeline_name"),
            F.col("cluster_identifier"),
        )
    )
```

## Benefits of This Approach

### **1. No Workspace Prefix Dependency**
- **Eliminates hardcoded assumptions** about workspace naming conventions
- **Uses actual business tags** for cost allocation
- **Flexible and maintainable** chargeback model

### **2. Comprehensive Tag Coverage**
- **6 required tags** provide complete cost attribution
- **Default values** ensure data consistency
- **Tag quality monitoring** identifies missing or invalid tags

### **3. Flexible Chargeback Views**
- **Multiple aggregation levels** (entity, department, environment, use case)
- **Tag-based grouping** instead of workspace assumptions
- **Real-time visibility** into tag quality and coverage

### **4. Production Ready**
- **Incremental processing** via CDF and MERGE statements
- **Data quality expectations** ensure data integrity
- **Comprehensive monitoring** and alerting capabilities

## Next Steps

1. **Review tag requirements** based on your business needs
2. **Customize tag mappings** if different tag keys are used
3. **Implement tag injection** at the appropriate levels (workspace, job, cluster)
4. **Monitor tag quality** using the provided views
5. **Extend chargeback views** for additional business requirements

---

*This document provides a comprehensive overview of the tag processing and chargeback system. For implementation details, refer to the specific code files mentioned throughout.*
