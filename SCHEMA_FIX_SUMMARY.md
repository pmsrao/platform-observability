# Schema Fix Summary

## 🎯 **Issue Resolved**
Fixed Delta schema mismatch error in bronze billing usage ingestion:
```
[DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION] Cannot cast struct<...> to struct<...>
```

## 🔧 **Root Cause**
The source table `system.billing.usage` had **new fields** that didn't exist in the bronze table schema:

### **usage_metadata struct:**
- **Added**: `usage_policy_id:STRING`

### **product_features struct:**
- **Added**: `jobs_tier:STRING` (at the beginning)
- **Added**: `performance_target:STRING` (in the middle)

## ✅ **Changes Made**

### **1. Updated bronze_tables.sql**
- **File**: `sql/bronze/bronze_tables.sql`
- **Changes**:
  - Added `usage_policy_id:STRING` to `usage_metadata` struct
  - Added `jobs_tier:STRING` to `product_features` struct
  - Added `performance_target:STRING` to `product_features` struct

### **2. Moved Diagnostic Scripts**
- **File**: `notebooks/adhoc/generate_updated_ddl.py`
- **Purpose**: Generate updated DDL based on current source schema
- **Location**: Already in `notebooks/adhoc/` folder

### **3. Validation Script**
- **File**: `resources/dashboard_v3/validate_dashboard.py`
- **Status**: No changes needed - works as expected

## 🚀 **Resolution Steps**
1. ✅ **Updated DDL**: Modified `bronze_tables.sql` with correct schema
2. ✅ **Dropped and recreated table**: `brz_billing_usage`
3. ✅ **Tested**: Bronze ingestion job now works
4. ✅ **Documented**: Changes tracked in this summary

## 📋 **Updated Schema Structure**

### **usage_metadata struct (33 fields):**
```sql
usage_metadata STRUCT<
    cluster_id:STRING,
    job_id:STRING,
    warehouse_id:STRING,
    instance_pool_id:STRING,
    node_type:STRING,
    job_run_id:STRING,
    notebook_id:STRING,
    dlt_pipeline_id:STRING,
    endpoint_name:STRING,
    endpoint_id:STRING,
    dlt_update_id:STRING,
    dlt_maintenance_id:STRING,
    run_name:STRING,
    job_name:STRING,
    notebook_path:STRING,
    central_clean_room_id:STRING,
    source_region:STRING,
    destination_region:STRING,
    app_id:STRING,
    app_name:STRING,
    metastore_id:STRING,
    private_endpoint_name:STRING,
    storage_api_type:STRING,
    budget_policy_id:STRING,
    ai_runtime_pool_id:STRING,
    ai_runtime_workload_id:STRING,
    uc_table_catalog:STRING,
    uc_table_schema:STRING,
    uc_table_name:STRING,
    database_instance_id:STRING,
    sharing_materialization_id:STRING,
    schema_id:STRING,
    usage_policy_id:STRING  -- NEW FIELD
>
```

### **product_features struct (12 fields):**
```sql
product_features STRUCT<
    jobs_tier:STRING,                    -- NEW FIELD
    sql_tier:STRING,
    dlt_tier:STRING,
    is_serverless:BOOLEAN,
    is_photon:BOOLEAN,
    serving_type:STRING,
    networking:STRUCT<connectivity_type:STRING>,
    ai_runtime:STRUCT<compute_type:STRING>,
    model_serving:STRUCT<offering_type:STRING>,
    ai_gateway:STRUCT<feature_type:STRING>,
    performance_target:STRING,           -- NEW FIELD
    serverless_gpu:STRUCT<workload_type:STRING>
>
```

## 🎉 **Result**
- ✅ Bronze ingestion job now runs successfully
- ✅ Schema mismatch error resolved
- ✅ Data pipeline can proceed to silver layer
- ✅ Future schema changes can be handled with diagnostic scripts

## 📝 **Notes**
- This is a common issue when Databricks adds new fields to system tables
- The diagnostic script `generate_updated_ddl.py` can be used for future schema updates
- Consider implementing schema evolution for production environments
