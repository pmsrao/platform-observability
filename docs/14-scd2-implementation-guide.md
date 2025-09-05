# SCD2 Implementation Guide for Gold Layer

This document explains how Slowly Changing Dimension Type 2 (SCD2) is implemented in the Gold layer of the Platform Observability solution.

## 🎯 **Overview**

### **What is SCD2?**
SCD2 (Slowly Changing Dimension Type 2) is a data modeling technique that preserves the complete history of dimension changes by creating new records for each change while maintaining the old versions.

### **Why SCD2 in Gold Layer?**
- **Historical Analysis**: Track how entities (jobs, pipelines, clusters) change over time
- **Temporal Accuracy**: Ensure facts reference the correct dimension version for each date
- **Audit Trail**: Maintain complete history of entity changes
- **Business Intelligence**: Support time-based analysis and reporting

## 📊 **SCD2 Structure in Silver Layer**

### **Silver Table Schema**
```sql
-- Example: slv_jobs_scd table
CREATE TABLE slv_jobs_scd (
    job_id STRING,
    job_name STRING,
    job_type STRING,
    owner STRING,
    valid_from INT,        -- date_sk when this version became active
    valid_to INT,          -- date_sk when this version expired (NULL for current)
    is_current BOOLEAN,    -- flag for current version
    created_time TIMESTAMP,
    updated_time TIMESTAMP
)
```

### **Example Data**
```
job_id    | job_name      | job_type | owner  | valid_from | valid_to | is_current
----------|---------------|----------|--------|------------|----------|------------
1123453   | Data Pipeline | ETL      | John   | 20250101   | 20250815 | false
1123453   | Data Pipeline | ETL      | Sarah  | 20250816   | NULL     | true
```

## 🏗️ **Gold Layer SCD2 Implementation**

### **1. Dimension Table Building**

#### **SCD2-Aware Dimension Building**
```python
def build_gold_dimensions_scd2_aware(spark) -> bool:
    """Build Gold dimension tables with SCD2 versioning preserved"""
    
    # Jobs dimension (SCD2 - preserve all versions using MERGE)
    jobs_df = spark.table(get_silver_table_name("slv_jobs_scd"))
    gold_jobs = get_gold_table_name("gld_dim_job")
    
    # Use MERGE to handle SCD2 updates incrementally
    jobs_df.createOrReplaceTempView("new_jobs")
    
    merge_sql = f"""
    MERGE INTO {gold_jobs} T
    USING new_jobs S
    ON T.job_id = S.job_id AND T.valid_from = S.valid_from
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
```

#### **Key Benefits**
- **Incremental Updates**: Only process changed dimension records
- **Version Preservation**: Maintain all SCD2 versions
- **Efficient Processing**: Use MERGE operations instead of complete table rebuilds
- **Data Integrity**: Ensure no duplicate versions are created

### **2. Fact Table SCD2 Alignment**

#### **Temporal Join Logic**
```python
def build_usage_facts_with_scd2_alignment(spark, usage_df):
    """Build usage facts with proper SCD2 dimension alignment"""
    
    # Join with SCD2 dimensions to get the correct version for each date
    usage_with_dimensions_df = spark.sql(f"""
        SELECT 
            u.*,
            -- Job dimension (SCD2 aligned)
            j.job_name,
            j.job_type,
            j.owner,
            j.valid_from as job_valid_from,
            j.valid_to as job_valid_to,
            -- Pipeline dimension (SCD2 aligned)
            p.pipeline_name,
            p.pipeline_type,
            p.valid_from as pipeline_valid_from,
            p.valid_to as pipeline_valid_to,
            -- Cluster dimension (SCD2 aligned)
            c.cluster_name,
            c.spark_version,
            c.valid_from as cluster_valid_from,
            c.valid_to as cluster_valid_to
        FROM usage_data u
        LEFT JOIN {get_silver_table_name("slv_jobs_scd")} j
            ON u.job_id = j.job_id
            AND u.date_sk BETWEEN j.valid_from AND COALESCE(j.valid_to, 99991231)
        LEFT JOIN {get_silver_table_name("slv_pipelines_scd")} p
            ON u.pipeline_id = p.pipeline_id
            AND u.date_sk BETWEEN p.valid_from AND COALESCE(p.valid_to, 99991231)
        LEFT JOIN {get_silver_table_name("slv_clusters")} c
            ON u.cluster_id = c.cluster_id
            AND u.date_sk BETWEEN c.valid_from AND COALESCE(c.valid_to, 99991231)
    """)
```

#### **Temporal Join Logic Explained**
```sql
-- For each fact record, find the dimension version that was active on that date
u.date_sk BETWEEN j.valid_from AND COALESCE(j.valid_to, 99991231)
```

**Example**:
- **Fact date_sk**: 20250810 (August 10, 2025)
- **Job Version 1**: valid_from: 20250101, valid_to: 20250815
- **Job Version 2**: valid_from: 20250816, valid_to: NULL
- **Result**: Fact joins to Job Version 1 (because 20250810 is between 20250101 and 20250815)

## 🔄 **Data Flow and Processing**

### **1. Silver to Gold Flow**
```
Silver SCD2 Tables → Gold Dimensions (all versions preserved)
         ↓
Silver Fact Tables → Gold Facts (SCD2 aligned)
         ↓
Gold Layer Complete (temporally accurate)
```

### **2. Processing Steps**
1. **Dimension Building**: Load all SCD2 versions from Silver to Gold using MERGE
2. **Fact Building**: Join facts with dimensions using temporal logic
3. **Data Validation**: Ensure temporal alignment and referential integrity
4. **Performance Optimization**: Apply Z-ORDER and statistics collection

## 📈 **Expected Results**

### **Gold Dimension Tables**
- **`gld_dim_job`**: Contains all versions of each job (e.g., 2 records for job_id 1123453)
- **`gld_dim_pipeline`**: Contains all versions of each pipeline
- **`gld_dim_cluster`**: Contains all versions of each cluster

### **Gold Fact Tables**
- **Facts before 2025-08-15**: Associated with job Version 1 (John as owner)
- **Facts from 2025-08-16**: Associated with job Version 2 (Sarah as owner)
- **Temporal Accuracy**: Each fact references the correct dimension version for its date

## 🎯 **Use Cases and Benefits**

### **1. Historical Analysis**
```sql
-- Analyze job ownership changes over time
SELECT 
    job_id,
    job_name,
    owner,
    valid_from,
    valid_to,
    DATEDIFF(valid_to, valid_from) as days_active
FROM gld_dim_job
WHERE job_id = '1123453'
ORDER BY valid_from;
```

### **2. Cost Attribution by Time Period**
```sql
-- Attribute costs to the correct job owner for each time period
SELECT 
    f.date_sk,
    f.job_id,
    j.owner,
    SUM(f.list_cost_usd) as total_cost
FROM gld_fact_usage_priced_day f
JOIN gld_dim_job j ON f.job_id = j.job_id
    AND f.date_sk BETWEEN j.valid_from AND COALESCE(j.valid_to, 99991231)
GROUP BY f.date_sk, f.job_id, j.owner
ORDER BY f.date_sk;
```

### **3. Change Impact Analysis**
```sql
-- Analyze the impact of job ownership changes
SELECT 
    j1.owner as previous_owner,
    j2.owner as current_owner,
    j1.valid_to as change_date,
    COUNT(f.job_id) as facts_affected
FROM gld_dim_job j1
JOIN gld_dim_job j2 ON j1.job_id = j2.job_id
    AND j1.valid_to = j2.valid_from - 1
JOIN gld_fact_usage_priced_day f ON f.job_id = j1.job_id
    AND f.date_sk = j1.valid_to
GROUP BY j1.owner, j2.owner, j1.valid_to;
```

## 🔧 **Configuration and Monitoring**

### **1. SCD2 Configuration**
```python
# In gold_hwm_build_job.py
def build_gold_layer(spark, complete_refresh: bool = False, strategy: str = "updated_time") -> Dict[str, bool]:
    """Build entire Gold layer with SCD2 awareness"""
    
    gold_builders = [
        ("dimensions", build_gold_dimensions_scd2_aware),  # SCD2-aware dimension building
        ("facts", lambda s: build_gold_facts(s, complete_refresh, strategy)),  # SCD2-aligned facts
        # ... other components
    ]
```

### **2. Monitoring SCD2 Processing**
```python
# Log SCD2 processing details
logger.info(f"Built job dimension with {jobs_df.count()} records (all SCD2 versions preserved)")
logger.info(f"Built pipeline dimension with {pipelines_df.count()} records (all SCD2 versions preserved)")
logger.info(f"Built cluster dimension with {clusters_df.count()} records (all SCD2 versions preserved)")
```

## 🚨 **Important Considerations**

### **1. Data Volume Impact**
- **SCD2 dimensions**: Will grow over time as entities change
- **Fact table joins**: May become more complex with temporal logic
- **Performance**: Monitor query performance as SCD2 versions increase

### **2. Data Quality**
- **Temporal gaps**: Ensure no gaps exist between valid_from and valid_to
- **Overlapping periods**: Validate that no overlapping valid periods exist
- **Referential integrity**: Ensure all facts can join to appropriate dimension versions

### **3. Maintenance**
- **Version cleanup**: Consider archiving old versions if they're no longer needed
- **Performance tuning**: Regular Z-ORDER optimization and statistics collection
- **Monitoring**: Track SCD2 version growth and processing performance

## 📚 **Related Documentation**

- [Gold HWM Build Job](notebooks/gold_hwm_build_job.py) - Implementation details
- [Data Dictionary](09-data-dictionary.md) - Complete data model documentation
- [Recent Changes Summary](13-recent-changes-summary.md) - Overview of SCD2 implementation
- [HWM Migration Guide](12-hwm-migration-guide.md) - Migration from DLT to HWM

## 🎯 **Next Steps**

1. **Deploy**: Use the updated Gold HWM build job with SCD2 awareness
2. **Validate**: Verify that facts align with correct dimension versions
3. **Monitor**: Track SCD2 processing performance and data quality
4. **Optimize**: Apply performance tuning based on actual usage patterns

---

*This document was last updated: January 2024*
*Version: 14-scd2-implementation-guide*
