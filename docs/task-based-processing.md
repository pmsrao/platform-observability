# Task-Based Processing State Management

## Overview

The Platform Observability solution uses **task-based processing state management** to ensure reliable, independent, and scalable incremental processing across all data layers (Bronze, Silver, and Gold).

## Problem Statement

The original HWM approach had a **critical flaw** where multiple fact tables reading from the same Silver table would conflict on offset management.

### **❌ Original Flawed Approach:**
```sql
-- Single entry per source table (PROBLEMATIC)
source_table | last_processed_timestamp | last_processed_version
slv_usage_txn | 2025-01-15 10:30:00 | 12345
```

### **The Issue:**
1. **`gld_fact_usage_priced_day`** processes `slv_usage_txn` → commits offset
2. **`gld_fact_run_cost`** processes `slv_usage_txn` → **NO DATA** (offset already committed!)
3. **`gld_fact_entity_cost`** processes `slv_usage_txn` → **NO DATA** (offset already committed!)

## ✅ Solution: Task-Based Processing State

### **New Approach:**
```sql
-- Task-based entries (CORRECT)
source_table | task_name | last_processed_timestamp | last_processed_version
slv_usage_txn | task_gld_fact_usage_priced_day | 2025-01-15 10:30:00 | 12345
slv_usage_txn | task_gld_fact_run_cost | 2025-01-15 10:30:00 | 12345
slv_usage_txn | task_gld_fact_entity_cost | 2025-01-15 10:30:00 | 12345
```

### **How It Works:**
1. **Each task** maintains its own processing state
2. **Independent processing** - no conflicts between tasks
3. **Standardized task names** - `task_<table_name>`
4. **Complete data processing** - all tasks get all data

## Implementation Details

### **1. Updated Processing State Table Schema:**
```sql
-- Processing offsets (Task-based)
CREATE TABLE IF NOT EXISTS {catalog}.{schema}._cdf_processing_offsets (
    source_table STRING,                    -- Source table name
    task_name STRING,                       -- Task name (e.g., 'task_slv_usage_txn')
    last_processed_timestamp TIMESTAMP,     -- Last processed timestamp
    last_processed_version BIGINT,          -- Last processed Delta version
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
PARTITIONED BY (source_table);
```

### **2. Standardized Task Naming:**
```python
def get_task_name(table_name):
    """Generate standardized task name for table processing."""
    return f"task_{table_name}"

# Examples:
# slv_usage_txn → task_slv_usage_txn
# gld_fact_usage_priced_day → task_gld_fact_usage_priced_day
# gld_fact_run_cost → task_gld_fact_run_cost
```

### **3. Task-Based Processing Logic:**
```python
def get_incremental_data(self, source_table: str, task_name: str) -> DataFrame:
    """Get incremental data using task-based processing state."""
    
    # Get last processed state for THIS specific task
    last_timestamp, last_version = get_last_processed_timestamp(
        self.spark, source_table, task_name, layer
    )
    
    # Read from source table
    source_df = self.spark.table(f"{self.catalog}.{self.schema}.{source_table}")
    
    if last_timestamp is None:
        # First run - process all data
        print(f"First run for {task_name} - processing all data")
        return source_df
    else:
        # Incremental run - process only new data
        print(f"Incremental run for {task_name} - processing data after {last_timestamp}")
        return source_df.filter(F.col("ingestion_date") > last_timestamp)
```

## Silver Layer Implementation

### **Silver Layer Processing:**
Each Silver table maintains its own processing state using standardized task names:

```python
def get_silver_task_name(table_name: str) -> str:
    """Get standardized task name for Silver layer table"""
    return get_task_name(table_name)

# Examples:
# slv_workspace → task_slv_workspace
# slv_entity_latest → task_slv_entity_latest
# slv_clusters → task_slv_clusters
# slv_usage_txn → task_slv_usage_txn
```

### **Updated Silver Processing Logic:**
```python
def build_silver_usage_txn(spark) -> bool:
    """Build Silver usage transaction table"""
    try:
        # Get last processed timestamp (task-based)
        task_name = get_silver_task_name("slv_usage_txn")
        last_ts, _ = get_last_processed_timestamp(spark, "slv_usage_txn", task_name, "silver")
        
        # Read new data from Bronze
        df = read_bronze_since_timestamp(spark, "brz_billing_usage", last_ts)
        
        # Process and transform data
        transformed_df = process_silver_data(df)
        
        # Write to Silver table
        silver_table = get_silver_table_name("slv_usage_txn")
        transformed_df.write.mode("overwrite").saveAsTable(silver_table)
        
        # Update processing state (task-based)
        max_ts = get_max_timestamp(df)
        if max_ts:
            task_name = get_silver_task_name("slv_usage_txn")
            commit_processing_state(spark, "slv_usage_txn", max_ts, task_name=task_name, layer="silver")
        
        return True
    except Exception as e:
        logger.error(f"Error building Silver usage transaction table: {str(e)}")
        return False
```

## Gold Layer Implementation

### **Gold Layer Processing:**
Each Gold fact table maintains its own processing state for reading from Silver tables:

```python
class UsageFactBuilder:
    def build(self):
        """Build usage fact table with task-based processing."""
        # Get incremental data using task-based state
        silver_data = self.get_incremental_data("slv_usage_txn", "task_gld_fact_usage_priced_day")
        
        # Join with dimensions and build fact table
        fact_data = self.join_with_dimensions(silver_data)
        
        # Write to Gold fact table
        self.upsert_fact(fact_data)
        
        # Commit processing state for this specific task
        self.commit_processing_state("slv_usage_txn", "task_gld_fact_usage_priced_day")
```

### **Independent State Commits:**
```python
def commit_processing_state(self, source_table: str, task_name: str, 
                          max_timestamp, max_version=None):
    """Commit processing state for THIS specific task."""
    commit_processing_state(
        self.spark, source_table, max_timestamp, max_version, task_name, layer
    )
    print(f"Committed processing state for {task_name}: {max_timestamp}")
```

## Example Scenario

### **Initial State (All Tasks):**
```sql
-- No processing state exists yet
SELECT * FROM _cdf_processing_offsets WHERE source_table = 'slv_usage_txn';
-- Result: No rows
```

### **First Run - All Tasks:**
```python
# Silver layer processes Bronze data
task_name = "task_slv_usage_txn"
# Processes ALL Bronze data (no previous state)
# Commits: 2025-01-15 10:30:00

# Gold layer processes Silver data
task_name = "task_gld_fact_usage_priced_day"
# Processes ALL Silver data (no previous state)
# Commits: 2025-01-15 10:30:00

task_name = "task_gld_fact_run_cost"
# Processes ALL Silver data (no previous state)
# Commits: 2025-01-15 10:30:00
```

### **Processing State After First Run:**
```sql
source_table | task_name | last_processed_timestamp | last_processed_version
slv_usage_txn | task_slv_usage_txn | 2025-01-15 10:30:00 | 12345
slv_usage_txn | task_gld_fact_usage_priced_day | 2025-01-15 10:30:00 | 12345
slv_usage_txn | task_gld_fact_run_cost | 2025-01-15 10:30:00 | 12345
```

### **Second Run - New Data Available:**
```python
# New data arrives: 2025-01-15 11:00:00 to 2025-01-15 12:00:00

# Silver layer processes new Bronze data
task_name = "task_slv_usage_txn"
# Reads last state: 2025-01-15 10:30:00
# Processes Bronze data AFTER 2025-01-15 10:30:00
# Commits: 2025-01-15 12:00:00

# Gold layer processes new Silver data
task_name = "task_gld_fact_usage_priced_day"
# Reads last state: 2025-01-15 10:30:00
# Processes Silver data AFTER 2025-01-15 10:30:00
# Commits: 2025-01-15 12:00:00

task_name = "task_gld_fact_run_cost"
# Reads last state: 2025-01-15 10:30:00
# Processes Silver data AFTER 2025-01-15 10:30:00
# Commits: 2025-01-15 12:00:00
```

## Key Benefits

### **✅ Independent Processing:**
- Each task maintains its own processing state
- No conflicts between multiple tasks reading same source
- All tasks process all available data

### **✅ Fault Tolerance:**
- If one task fails, others continue processing
- Individual task recovery without affecting others
- Granular error handling and retry logic

### **✅ Scalability:**
- Easy to add new tasks without conflicts
- Standardized task naming convention
- Clear separation of concerns

### **✅ Monitoring:**
- Track processing state per task
- Identify which tasks are behind
- Monitor processing performance per task

## Query Examples

### **Check Processing State:**
```sql
-- View all processing states
SELECT 
    source_table,
    task_name,
    last_processed_timestamp,
    last_processed_version,
    updated_at
FROM _cdf_processing_offsets
ORDER BY source_table, task_name;

-- Check specific task status
SELECT * FROM _cdf_processing_offsets 
WHERE source_table = 'slv_usage_txn' 
  AND task_name = 'task_gld_fact_usage_priced_day';
```

### **Identify Lagging Tasks:**
```sql
-- Find tasks that haven't processed recent data
SELECT 
    source_table,
    task_name,
    last_processed_timestamp,
    DATEDIFF(CURRENT_TIMESTAMP(), last_processed_timestamp) as hours_behind
FROM _cdf_processing_offsets
WHERE last_processed_timestamp < DATE_SUB(CURRENT_TIMESTAMP(), 2)
ORDER BY hours_behind DESC;
```

### **Reset Task State:**
```sql
-- Reset a specific task to reprocess all data
DELETE FROM _cdf_processing_offsets 
WHERE source_table = 'slv_usage_txn' 
  AND task_name = 'task_gld_fact_usage_priced_day';
```

## Cross-Layer Integration

### **Bronze → Silver → Gold Processing Flow:**
```python
# Bronze layer: Task-based processing for Bronze table ingestion
BronzeIngestJob: task_brz_billing_usage → processes source data → commits Bronze state

# Silver layer: Task-based processing for Silver table transformation
SilverBuildJob: task_slv_usage_txn → processes Bronze data → commits Silver state

# Gold layer: Task-based processing for Gold fact table building
UsageFactBuilder: task_gld_fact_usage_priced_day → processes Silver data → commits Gold state
RunCostFactBuilder: task_gld_fact_run_cost → processes Silver data → commits Gold state
EntityCostFactBuilder: task_gld_fact_entity_cost → processes Silver data → commits Gold state
```

### **Consistent State Management:**
- **Bronze Layer**: Task-based processing for Bronze table ingestion
- **Silver Layer**: Task-based processing for Silver table transformation  
- **Gold Layer**: Task-based processing for Gold fact table building

The task-based approach ensures **reliable, independent, and scalable** processing across all data layers! 🚀
