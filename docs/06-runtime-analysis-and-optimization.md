# Runtime Analysis and Optimization Guide

## Overview

This guide explains the **Databricks Runtime analysis and optimization** features implemented in the Platform Observability solution. These features provide comprehensive insights into runtime versions, node types, cluster sizing, and optimization opportunities.

## Table of Contents

1. [Runtime Analysis Features](#runtime-analysis-features)
2. [Data Model Enhancements](#data-model-enhancements)
3. [Analysis Views](#analysis-views)
4. [Optimization Recommendations](#optimization-recommendations)
5. [Implementation Details](#implementation-details)
6. [Usage Examples](#usage-examples)

## Runtime Analysis Features

### **Key Capabilities**

✅ **Runtime Version Tracking**: Monitor Databricks Runtime versions across all clusters
✅ **Node Type Analysis**: Analyze compute node types and optimization opportunities  
✅ **Cluster Sizing Insights**: Understand min/max workers and auto-scaling usage
✅ **Modernization Opportunities**: Identify upgrade priorities and cost savings
✅ **Performance Trends**: Track runtime performance and cost efficiency
✅ **Business Context**: Correlate runtime usage with business units and cost centers

### **Business Value**

- **Cost Optimization**: Identify opportunities for auto-scaling and node type optimization
- **Risk Mitigation**: Track end-of-support dates and upgrade priorities
- **Performance Improvement**: Leverage newer runtime versions for better performance
- **Capacity Planning**: Understand cluster sizing patterns and optimization needs

## Data Model Enhancements

### **1. Enhanced Bronze Layer**

#### **A. Clusters Table (`bronze_clusters_raw`)**
```sql
-- NEW: Runtime and Node Type Information
dbr_version STRING,                   -- e.g., "13.3.x-scala2.12", "14.0.x-scala2.12"
runtime_environment STRING,          -- e.g., "python", "scala", "sql", "ml"
node_type_id STRING,                 -- e.g., "Standard_DS3_v2", "Standard_E4s_v3"
min_workers INT,                     -- Minimum number of worker nodes
max_workers INT,                     -- Maximum number of worker nodes
driver_node_type_id STRING,          -- Driver node type
worker_node_type_id STRING,          -- Worker node type
autoscale_enabled BOOLEAN,           -- Whether autoscaling is enabled
```

#### **B. Runtime Versions Table (`bronze_runtime_versions_raw`)**
```sql
runtime_version STRING,              -- Full runtime version string
major_version INT,                   -- Major version number (e.g., 13, 14)
minor_version INT,                   -- Minor version number (e.g., 3, 0)
patch_version STRING,                -- Patch version (e.g., "x", "1")
scala_version STRING,                -- Scala version (e.g., "2.12", "2.13")
runtime_type STRING,                 -- "LTS", "STABLE", "BETA"
release_date DATE,                   -- Release date
end_of_support_date DATE,            -- End of support date
is_current BOOLEAN,                  -- Whether this is the current version
is_lts BOOLEAN,                      -- Whether this is LTS version
```

### **2. Enhanced Silver Layer**

#### **A. Clusters Table (`slv_clusters`)**
```sql
-- NEW: Computed runtime fields
major_version INT,                   -- Extracted from runtime version
minor_version INT,                   -- Extracted from runtime version
runtime_age_months INT,              -- Age since release
is_lts BOOLEAN,                      -- LTS version indicator
is_current BOOLEAN,                  -- Current version indicator
```

#### **B. Runtime Versions Table (`silver_runtime_versions`)**
```sql
-- NEW: Computed fields for analysis
runtime_age_months INT,              -- Age since release
months_to_eos INT,                   -- Months until end of support
is_supported BOOLEAN,                -- Whether still supported
```

### **3. Enhanced Gold Layer**

#### **A. Runtime Analysis Views**
- `v_runtime_version_analysis`: Comprehensive runtime health analysis
- `v_node_type_analysis`: Node type optimization insights
- `v_runtime_modernization_opportunities`: Upgrade recommendations
- `v_runtime_performance_trends`: Performance and cost trends
- `v_cluster_sizing_optimization`: Cluster sizing recommendations

## Analysis Views

### **1. Runtime Version Analysis (`v_runtime_version_analysis`)**

**Purpose**: Comprehensive analysis of runtime versions across the platform

**Key Metrics**:
- Runtime version distribution and usage
- Support lifecycle status (supported, end-of-support, LTS)
- Business context correlation (line of business, department, cost center)
- Upgrade priority assessment

**Sample Query**:
```sql
SELECT 
    dbr_version,
    major_version,
    minor_version,
    upgrade_priority,
    COUNT(DISTINCT active_clusters) as cluster_count,
    SUM(total_cost_usd) as total_cost
FROM v_runtime_version_analysis
WHERE date_sk = 20241201
GROUP BY dbr_version, major_version, minor_version, upgrade_priority
ORDER BY upgrade_priority DESC, total_cost DESC;
```

### **2. Node Type Analysis (`v_node_type_analysis`)**

**Purpose**: Analyze compute node types and optimization opportunities

**Key Metrics**:
- Node family categorization (General Purpose, Memory Optimized, Compute Optimized)
- Scaling type analysis (Fixed Size, Auto-scaling, Manual Scaling)
- Optimization suggestions for cost and performance

**Sample Query**:
```sql
SELECT 
    node_family,
    scaling_type,
    optimization_suggestion,
    COUNT(DISTINCT active_clusters) as cluster_count,
    AVG(cost_per_hour) as avg_cost_per_hour
FROM v_node_type_analysis
WHERE date_sk = 20241201
GROUP BY node_family, scaling_type, optimization_suggestion
ORDER BY cluster_count DESC;
```

### **3. Runtime Modernization Opportunities (`v_runtime_modernization_opportunities`)**

**Purpose**: Identify specific clusters and jobs that need runtime upgrades

**Key Metrics**:
- Upgrade urgency assessment (CRITICAL, HIGH, MEDIUM, LOW)
- Specific upgrade recommendations
- Cost impact and potential savings
- Business context for prioritization

**Sample Query**:
```sql
SELECT 
    cluster_name,
    dbr_version,
    upgrade_urgency,
    runtime_recommendation,
    scaling_recommendation,
    monthly_cost_usd,
    potential_savings
FROM v_runtime_modernization_opportunities
WHERE date_sk = 20241201
  AND upgrade_urgency IN ('CRITICAL', 'HIGH')
ORDER BY upgrade_urgency, monthly_cost_usd DESC;
```

### **4. Cluster Sizing Optimization (`v_cluster_sizing_optimization`)**

**Purpose**: Analyze cluster sizing patterns and optimization opportunities

**Key Metrics**:
- Current scaling configuration (Fixed Size, Auto-scaling, Manual Scaling)
- Sizing recommendations for cost optimization
- Optimization potential assessment

**Sample Query**:
```sql
SELECT 
    cluster_name,
    current_scaling,
    sizing_recommendation,
    optimization_potential,
    total_cost_usd,
    cost_per_hour
FROM v_cluster_sizing_optimization
WHERE date_sk = 20241201
  AND optimization_potential != 'Low - Current configuration is optimal'
ORDER BY total_cost_usd DESC;
```

## Optimization Recommendations

### **1. Runtime Upgrade Priorities**

#### **CRITICAL Priority**
- **End of Support**: Runtimes that have reached end-of-support
- **Security Risks**: Outdated runtimes with security vulnerabilities
- **Action**: Immediate upgrade required

#### **HIGH Priority**
- **Support Ending Soon**: Runtimes reaching end-of-support within 3 months
- **Performance Impact**: Significant performance improvements available
- **Action**: Plan upgrade within 1-2 months

#### **MEDIUM Priority**
- **Support Ending**: Runtimes reaching end-of-support within 6 months
- **Aging Runtimes**: Runtimes older than 24 months
- **Action**: Plan upgrade within 3-6 months

#### **LOW Priority**
- **Current and Supported**: Recent runtimes with ongoing support
- **LTS Versions**: Long-term support versions
- **Action**: Monitor for future upgrade opportunities

### **2. Node Type Optimization**

#### **Auto-scaling Recommendations**
```sql
-- Clusters that should enable auto-scaling
SELECT cluster_name, worker_node_type_id, min_workers, max_workers
FROM v_cluster_sizing_optimization
WHERE sizing_recommendation LIKE '%Enable auto-scaling%'
  AND autoscale_enabled = false;
```

#### **Node Family Optimization**
- **General Purpose (DS)**: Good for most workloads
- **Memory Optimized (E)**: Better for memory-intensive workloads
- **Compute Optimized (F)**: Better for CPU-intensive workloads
- **Storage Optimized (L)**: Better for I/O-intensive workloads

### **3. Cluster Sizing Optimization**

#### **Fixed Size Clusters**
- **Small Clusters (1-2 workers)**: Monitor performance, consider multi-node for heavy workloads
- **Large Clusters (10+ workers)**: Consider auto-scaling for variable workloads

#### **Auto-scaling Benefits**
- **Cost Reduction**: 15-30% potential cost savings
- **Performance**: Better resource utilization
- **Flexibility**: Adapt to variable workloads

## Implementation Details

### **1. HWM Job Enhancements**

#### **A. Runtime Processing in Silver Layer**
```python
def build_silver_clusters(spark):
    return clusters_new.select(
        # ... existing fields ...
        "dbr_version", "runtime_environment", "node_type_id",
        "min_workers", "max_workers", "driver_node_type_id", "worker_node_type_id",
        "autoscale_enabled"
    ).withColumn(
        "major_version",
        F.regexp_extract(F.col("dbr_version"), r"(\d+)\.", 1).cast("int")
    ).withColumn(
        "minor_version",
        F.regexp_extract(F.col("dbr_version"), r"\.(\d+)\.", 1).cast("int")
    )
```

#### **B. Runtime Versions Processing**
```python
def build_silver_runtime_versions(spark):
    return spark.table("bronze_runtime_versions_raw").withColumn(
        "runtime_age_months",
        F.months_between(F.current_date(), F.col("release_date"))
    ).withColumn(
        "months_to_eos",
        F.months_between(F.col("end_of_support_date"), F.current_date())
    )
```

### **2. Tag Processor Enhancements**

#### **A. Runtime Tags**
```python
REQUIRED_TAGS: Dict[str, str] = {
    # ... existing tags ...
    "runtime_version": "databricks_runtime",      # Databricks Runtime version
    "node_type": "compute_node_type",             # Compute node type
    "cluster_size": "cluster_worker_count",       # Cluster worker count
}
```

#### **B. Runtime Defaults**
```python
DEFAULTS: Dict[str, str] = {
    # ... existing defaults ...
    "databricks_runtime": "Unknown",
    "compute_node_type": "Unknown",
    "cluster_worker_count": "Unknown",
}
```

## Usage Examples

### **1. Runtime Health Dashboard**

```sql
-- Overall runtime health summary
SELECT 
    upgrade_priority,
    COUNT(DISTINCT active_clusters) as cluster_count,
    SUM(total_cost_usd) as total_cost,
    AVG(runtime_age_months) as avg_runtime_age
FROM v_runtime_version_analysis
WHERE date_sk = 20241201
GROUP BY upgrade_priority
ORDER BY 
    CASE upgrade_priority 
        WHEN 'CRITICAL - End of Support' THEN 1
        WHEN 'HIGH - Support Ending Soon' THEN 2
        WHEN 'MEDIUM - Plan for Upgrade' THEN 3
        ELSE 4
    END;
```

### **2. Node Type Optimization Report**

```sql
-- Node type optimization opportunities
SELECT 
    node_family,
    scaling_type,
    optimization_suggestion,
    COUNT(DISTINCT active_clusters) as cluster_count,
    SUM(total_cost_usd) as total_cost,
    AVG(cost_per_hour) as avg_cost_per_hour
FROM v_node_type_analysis
WHERE date_sk = 20241201
  AND optimization_suggestion != 'Optimal Configuration'
GROUP BY node_family, scaling_type, optimization_suggestion
ORDER BY total_cost DESC;
```

### **3. Modernization Roadmap**

```sql
-- Modernization roadmap by business unit
SELECT 
    line_of_business,
    upgrade_urgency,
    COUNT(DISTINCT cluster_id) as cluster_count,
    SUM(monthly_cost_usd) as total_monthly_cost,
    STRING_AGG(DISTINCT runtime_recommendation, '; ') as recommendations
FROM v_runtime_modernization_opportunities
WHERE date_sk = 20241201
GROUP BY line_of_business, upgrade_urgency
ORDER BY line_of_business, upgrade_urgency;
```

### **4. Cost Optimization Analysis**

```sql
-- Cost optimization opportunities
SELECT 
    cluster_name,
    current_scaling,
    sizing_recommendation,
    optimization_potential,
    monthly_cost_usd,
    potential_savings
FROM v_cluster_sizing_optimization
WHERE date_sk = 20241201
  AND optimization_potential LIKE '%Medium%'
ORDER BY monthly_cost_usd DESC;
```

## Benefits

### **1. Cost Optimization**
- **Auto-scaling**: 15-30% potential cost savings
- **Node Type Optimization**: Better performance per dollar
- **Runtime Modernization**: 10-20% performance improvements

### **2. Risk Mitigation**
- **End-of-Support Tracking**: Avoid security and compatibility issues
- **Upgrade Planning**: Proactive modernization roadmap
- **Performance Monitoring**: Identify optimization opportunities

### **3. Operational Excellence**
- **Resource Optimization**: Better cluster sizing and utilization
- **Performance Improvement**: Leverage latest runtime features
- **Capacity Planning**: Data-driven infrastructure decisions

## Next Steps

1. **Data Ingestion**: Populate runtime and node type information in Bronze layer
2. **Runtime Catalog**: Maintain current runtime version reference data
3. **Monitoring**: Set up alerts for critical runtime health issues
4. **Optimization**: Implement auto-scaling and node type recommendations
5. **Modernization**: Execute runtime upgrade roadmap

---

*This guide provides comprehensive coverage of runtime analysis and optimization features. For implementation details, refer to the specific code files mentioned throughout.*
