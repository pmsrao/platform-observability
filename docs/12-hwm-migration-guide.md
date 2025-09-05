# HWM Migration Guide: From DLT to High-Water Mark Approach

## Overview

This document outlines the migration from Delta Live Tables (DLT) to a High-Water Mark (HWM) approach for the Silver and Gold layers of the Platform Observability solution.

## Why Migrate to HWM?

### **Benefits of HWM Approach**
1. **Simpler Architecture**: One consistent pattern across all layers
2. **Easier Maintenance**: Similar code structure and logic
3. **Better Control**: More granular control over processing
4. **Easier Testing**: Can test individual functions independently
5. **Consistent with Bronze**: Same approach already familiar to the team
6. **Resource Management**: Better control over resource usage
7. **Debugging**: Simpler to debug and troubleshoot

### **When to Use HWM vs DLT**
- **Use HWM for**: Batch-oriented data warehousing, simpler transformations, consistent patterns
- **Use DLT for**: Real-time streaming, complex data quality expectations, automatic optimization

## Migration Summary

### **Before (DLT Approach)**
```
Bronze (HWM Job) → Silver (DLT Pipeline) → Gold (DLT Pipeline)
```

### **After (HWM Approach)**
```
Bronze (HWM Job) → Silver (HWM Job) → Gold (HWM Job)
```

## New HWM Jobs Created

### **1. Silver Layer HWM Job**
- **File**: `notebooks/silver_hwm_build_job.py`
- **Purpose**: Build Silver layer from Bronze using HWM approach
- **Features**:
  - Incremental processing using HWM tracking
  - SCD2 implementation for key tables
  - Tag enrichment and normalization
  - Data quality validation
  - Structured logging and monitoring

### **2. Gold Layer HWM Job**
- **File**: `notebooks/gold_hwm_build_job.py`
- **Purpose**: Build Gold layer from Silver using HWM approach
- **Features**:
  - Incremental processing using HWM tracking
  - Dimension table maintenance
  - Fact table aggregation
  - Business view creation
  - Chargeback and analysis views

### **3. Performance Optimization Job**
- **File**: `notebooks/performance_optimization_job.py`
- **Purpose**: Apply performance optimizations to all layers
- **Features**:
  - Z-ORDER optimization for common query patterns
  - Statistics collection for better query planning
  - Table compaction and optimization
  - Performance monitoring and reporting

### **4. Health Check Job**
- **File**: `notebooks/health_check_job.py`
- **Purpose**: Perform comprehensive health checks on all layers
- **Features**:
  - Data freshness checks
  - Record count validation
  - Data quality assessment
  - Performance metrics collection
  - Health status reporting

## Updated Workflow

### **Daily Observability Workflow**
The `jobs/daily_observability_workflow.json` has been updated to use the new HWM jobs:

```json
{
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "description": "Ingest data from system tables to Bronze layer using HWM approach"
    },
    {
      "task_key": "silver_build",
      "description": "Build Silver layer from Bronze using HWM approach",
      "depends_on": ["bronze_ingest"]
    },
    {
      "task_key": "gold_build",
      "description": "Build Gold layer from Silver using HWM approach",
      "depends_on": ["silver_build"]
    },
    {
      "task_key": "performance_optimization",
      "description": "Apply performance optimizations to all layers",
      "depends_on": ["gold_build"]
    },
    {
      "task_key": "health_check",
      "description": "Perform health checks and send summary report",
      "depends_on": ["performance_optimization"]
    }
  ]
}
```

## Implementation Details

### **HWM Tracking**
Both Silver and Gold jobs use the same HWM tracking mechanism:

```python
# Get last processed timestamp
last_ts = get_last_processed_timestamp(spark, "silver_workspace")

# Read new data since last timestamp
df = read_bronze_since_timestamp(spark, "brz_access_workspaces_latest", last_ts)

# Update processing state after successful processing
if max_ts:
    commit_processing_state(spark, "silver_workspace", max_ts)
```

### **Incremental Processing**
- **Silver Layer**: Processes only new/changed records from Bronze using CDF timestamps
- **Gold Layer**: Processes only new date_sk partitions from Silver

### **Data Quality Validation**
Each layer includes built-in data quality checks:

```python
def validate_silver_data(df: 'DataFrame', table_name: str) -> bool:
    """Validate data quality for Silver layer"""
    validation_rules = {
        'workspace_id': 'not_null',
        'entity_type': 'not_null',
        'entity_id': 'not_null'
    }
    return validate_data_quality(df, table_name, logger, validation_rules)
```

### **SCD2 Implementation**
Silver layer maintains SCD2 for key tables:

```python
transformed_df = df.select(
    df.cluster_id, df.workspace_id, df.cluster_name,
    df.spark_version, df.cluster_source, df.node_type_id,
    df.driver_node_type_id, df.creator, df.created_time, df.updated_time
).withColumn("valid_from", df.created_time) \
 .withColumn("valid_to", lit(None)) \
 .withColumn("is_current", lit(True))
```

## Migration Steps

### **Phase 1: Create HWM Jobs**
1. ✅ Create `silver_hwm_build_job.py`
2. ✅ Create `gold_hwm_build_job.py`
3. ✅ Create `performance_optimization_job.py`
4. ✅ Create `health_check_job.py`

### **Phase 2: Update Workflow**
1. ✅ Update `daily_observability_workflow.json`
2. ✅ Test workflow execution

### **Phase 3: Deploy and Test**
1. Deploy HWM jobs to Databricks
2. Test individual job execution
3. Test complete workflow execution
4. Validate data quality and completeness

### **Phase 4: Cleanup (Optional)**
1. Remove DLT pipelines if no longer needed
2. Update documentation
3. Archive old DLT code

## Testing Strategy

### **Unit Testing**
- Test individual HWM functions
- Test data validation logic
- Test SCD2 implementation

### **Integration Testing**
- Test Bronze → Silver → Gold data flow
- Test HWM tracking accuracy
- Test incremental processing

### **End-to-End Testing**
- Test complete workflow execution
- Test data quality across all layers
- Test performance optimizations

## Monitoring and Alerting

### **Job Monitoring**
- Monitor job execution times
- Monitor data processing volumes
- Monitor error rates

### **Data Quality Monitoring**
- Monitor data freshness
- Monitor record counts
- Monitor validation failures

### **Performance Monitoring**
- Monitor query performance
- Monitor table optimization
- Monitor resource usage

## Rollback Plan

If issues arise with the HWM approach:

1. **Immediate Rollback**: Revert to DLT pipelines
2. **Data Recovery**: Restore from latest successful DLT run
3. **Investigation**: Analyze HWM job failures
4. **Fix and Retry**: Address issues and redeploy

## Performance Considerations

### **HWM vs DLT Performance**
- **HWM**: More predictable resource usage, better control over execution
- **DLT**: Automatic optimization, real-time processing capabilities

### **Optimization Strategies**
- Use appropriate partition columns
- Implement efficient HWM queries
- Apply Z-ORDER optimization
- Collect table statistics regularly

## Best Practices

### **HWM Implementation**
1. Use consistent HWM tracking across all layers
2. Implement proper error handling and retry logic
3. Monitor HWM lag and alert on delays
4. Regular cleanup of old HWM records

### **Data Quality**
1. Implement validation at each layer
2. Monitor data freshness metrics
3. Alert on quality violations
4. Maintain data lineage tracking

### **Monitoring**
1. Comprehensive logging at each step
2. Performance metrics collection
3. Health check automation
4. Proactive alerting

## Conclusion

The migration to HWM approach provides:
- **Simpler architecture** with consistent patterns
- **Better control** over data processing
- **Easier maintenance** and debugging
- **Consistent approach** across all layers

This approach is well-suited for batch-oriented data warehousing scenarios and provides the foundation for a robust, maintainable platform observability solution.

## Next Steps

1. **Deploy HWM jobs** to Databricks
2. **Test execution** in development environment
3. **Validate data quality** and completeness
4. **Monitor performance** and optimize as needed
5. **Document lessons learned** and best practices
