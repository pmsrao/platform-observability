# **Platform Observability System - Build Instructions**

*Generated from reverse engineering the existing codebase*

## **Project Overview**
Build a comprehensive observability platform that unifies usage, cost, and run health data across Databricks Jobs, DLT Pipelines, clusters, and node types. The system should enable platform teams, FinOps, and data engineers to identify cost drivers, track run health, detect performance regressions, and enforce policy compliance.

## **Source System Tables Required**
You need access to these Databricks system tables with their complete schemas:

### **Billing Tables:**
- `system.billing.usage` - Raw usage data with 50+ fields including usage_metadata struct
- `system.billing.list_prices` - Pricing data with effective date ranges

### **Lakeflow Tables:**
- `system.lakeflow.jobs` - Job metadata and configuration
- `system.lakeflow.pipelines` - Pipeline metadata and settings
- `system.lakeflow.job_run_timeline` - Job execution timeline
- `system.lakeflow.job_task_run_timeline` - Task-level execution details

### **Access & Compute Tables:**
- `system.access.workspaces_latest` - Workspace information
- `system.compute.clusters` - Cluster configuration and metadata
- `system.compute.node_types` - Node type specifications

> **Note**: Complete schema definitions are available in `dbr_schema.json` in this same folder.

## **Architecture Requirements**

### **Data Architecture:**
- **Medallion Architecture**: Bronze (raw) → Silver (curated) → Gold (analytics)
- **Catalog Structure**: `platform_observability.plt_bronze`, `plt_silver`, `plt_gold`
- **Time Key**: Use `date_key` (YYYYMMDD format) as canonical time dimension
- **Processing Strategy**: High Water Mark (HWM) with Change Data Feed (CDF) for incremental processing

### **Key Design Patterns:**
1. **Bronze Layer**: 1:1 copies of system tables with CDF enabled
2. **Silver Layer**: SCD2 (Slowly Changing Dimension Type 2) for historical tracking
3. **Gold Layer**: Star schema with facts and dimensions, SCD2-aligned joins
4. **Incremental Processing**: Only process new/changed records using CDF versions

## **Core Components to Build**

### **1. Configuration System**
- Environment-aware configuration (dev/prod)
- Centralized parameter management
- SQL file externalization for maintainability
- Type-safe configuration with dataclasses

**Key Files to Create:**
- `config.py` - Environment-aware configuration management
- `env.example` - Environment configuration template
- `requirements.txt` - Python dependencies

### **2. Processing State Management**
- High Water Mark tracking for Bronze ingestion
- CDF bookmark management for Silver processing
- Task-based offset management for Gold layer
- Resumable operations with state persistence

**Key Files to Create:**
- `libs/processing_state.py` - Processing state management utilities
- `libs/cdf.py` - Change Data Feed operations
- `sql/config/processing_offsets.sql` - Processing state table DDL

### **3. Data Quality & Validation**
- Built-in data quality rules (non-negative quantities, valid time ranges)
- Tag normalization and inheritance hierarchy
- Data quality scoring and monitoring
- Graceful error handling with detailed logging

**Key Files to Create:**
- `libs/error_handling.py` - Data quality validation and error handling
- `libs/logging.py` - Structured logging and performance monitoring
- `libs/monitoring.py` - Monitoring and alerting system

### **4. Bronze Layer (Raw Data)**
- Create tables with exact schema from source system tables
- Enable CDF on all tables for downstream consumption
- Implement HWM-based ingestion with overlap handling
- Support for late-arriving data

**Key Files to Create:**
- `sql/bronze/bronze_tables.sql` - Bronze table DDL with CDF enabled
- `notebooks/bronze_hwm_ingest_job.py` - Bronze ingestion job
- `libs/sql_manager.py` - SQL file management utility

### **5. Silver Layer (Curated Data)**
- **CDF-Driven Processing**: Read only new/changed records from Bronze using Change Data Feed
- **SCD2 Implementation**: Preserve historical versions of jobs, pipelines, and clusters
- **Tag Processing**: Inheritance hierarchy (Job → Cluster → Usage) with normalization
- **Pricing Joins**: Effective date ranges for accurate cost calculations
- **Timeline Normalization**: Standardize date_key across all timeline data

**Silver Layer Processing Approach:**
1. **CDF Reading**: Read only changed records from Bronze tables since last processed version
2. **Entity SCD2**: Create SCD2 records for jobs, pipelines, and clusters with valid_from/valid_to
3. **Tag Explosion**: Convert MAP tags to normalized key-value pairs with inheritance
4. **Pricing Enrichment**: Join usage data with effective pricing for cost calculations
5. **Timeline Processing**: Normalize run timelines with date_key standardization

**Key Files to Create:**
- `sql/silver/silver_tables.sql` - Silver table DDL with SCD2 support
- `notebooks/silver_hwm_build_job.py` - Silver layer build job (CDF-driven)
- `libs/tag_processor.py` - Tag processing and normalization
- `libs/data_enrichment.py` - Data transformations and enrichment

### **6. Gold Layer (Analytics)**
- **Dimensions**: workspace, entity, cluster, SKU, run_status, node_type
- **Facts**: usage_priced_day, entity_cost, run_cost, run_status_cost, runs_finished_day
- **Views**: policy compliance, tag coverage, latency trends
- SCD2-aligned temporal joins for historical accuracy

**Key Files to Create:**
- `sql/gold/gold_dimensions.sql` - Dimension table DDL
- `sql/gold/gold_facts.sql` - Fact table DDL
- `sql/gold/gold_views.sql` - Business-ready views
- `sql/gold/gold_chargeback_views.sql` - Cost allocation views
- `sql/gold/gold_runtime_analysis_views.sql` - Runtime optimization views
- `sql/gold/policy_compliance.sql` - Policy compliance views
- `notebooks/gold_hwm_build_job.py` - Gold layer build job
- `libs/gold_dimension_builder.py` - Dimension building utilities
- `libs/gold_fact_builder.py` - Fact building utilities
- `libs/gold_view_builder.py` - View building utilities

### **7. Monitoring & Alerting**
- Structured logging with JSON format
- Performance metrics tracking
- Automated alerts for failures and thresholds
- Health checks for system status

**Key Files to Create:**
- `notebooks/health_check_job.py` - Health monitoring job
- `notebooks/performance_optimization_job.py` - Performance optimization job

### **8. Dashboard System**
- Modular dashboard architecture
- SQL-driven visualization
- Multi-persona views (Finance, Platform, Data Engineering)
- Automated dashboard generation and deployment

**Key Files to Create:**
- `notebooks/generate_dashboard_json.py` - Dashboard generation
- `notebooks/deploy_dashboard.py` - Dashboard deployment
- `resources/dashboard/dashboard_template.json` - Dashboard structure
- `resources/dashboard/dashboard_sql_queries.json` - Dashboard SQL statements

## **Implementation Steps**

### **Phase 1: Foundation**
1. Set up configuration system with environment support
2. Create processing state tables for HWM and CDF tracking
3. Bootstrap Bronze tables with CDF enabled
4. Implement basic logging and error handling

**Commands:**
```bash
# Create environment configuration
cp env.example .env
# Edit .env with your values

# Install dependencies
pip install -r requirements.txt

# Bootstrap database objects
python -c "from libs.sql_parameterizer import SQLParameterizer; sp = SQLParameterizer(); sp.bootstrap_catalog_schemas(); sp.create_processing_state(); sp.bootstrap_bronze_tables()"
```

### **Phase 2: Data Processing**
1. Build Bronze HWM ingestion job
2. Create Silver layer with SCD2 support
3. Implement tag processing and inheritance
4. Build Gold layer with star schema

**Commands:**
```bash
# Create Silver tables
python -c "from libs.sql_parameterizer import SQLParameterizer; sp = SQLParameterizer(); sp.create_silver_tables()"

# Create Gold tables
python -c "from libs.sql_parameterizer import SQLParameterizer; sp = SQLParameterizer(); sp.create_gold_tables()"

# Apply performance optimizations
python -c "from libs.sql_parameterizer import SQLParameterizer; sp = SQLParameterizer(); sp.apply_performance_optimizations()"
```

### **Phase 3: Analytics & Monitoring**
1. Create business views and dashboards
2. Implement monitoring and alerting
3. Add performance optimizations
4. Set up automated deployment

### **Phase 4: Operations**
1. Create health check jobs
2. Implement data quality monitoring
3. Set up CI/CD pipelines
4. Create operational dashboards

## **Critical Technical Details**

### **Data Types & Schema Alignment:**
- Use STRING for all ID fields (not BIGINT)
- Use DECIMAL(38,18) for financial data
- Preserve exact column names from source tables
- Handle complex nested structures (STRUCT, MAP, ARRAY)

### **Processing Logic:**
- Bronze: Daily HWM ingestion with 48-72 hour overlap
- Silver: CDF-driven incremental processing with SCD2
- Gold: Date-partitioned incremental updates with SCD2 joins

### **Tag Processing Hierarchy:**
```
Job Tags → Cluster Tags → Usage Records
    ↓           ↓           ↓
line_of_business → inherited_line_of_business → line_of_business (coalesced)
cost_center → inherited_cost_center → cost_center (coalesced)
workflow_level → inherited_workflow_level → workflow_level (coalesced)
parent_workflow → inherited_parent_workflow → parent_workflow_name (coalesced)
```

### **SCD2 Implementation:**
- **Entity Dimension**: All versions of jobs/pipelines preserved
- **Cluster Dimension**: All versions of clusters preserved
- **Temporal Joins**: Facts reference correct dimension version for each date
- **MERGE Operations**: Idempotent upserts for dimension updates

### **Performance Optimizations:**
- Z-ORDER on frequently queried columns
- Date-based partitioning on date_key
- Auto-optimization and compaction
- Materialized views for expensive aggregations

### **Processing State Management:**

#### **Bronze Layer - HWM (High Water Mark) Approach:**
- **Purpose**: Track the last processed timestamp for each source system table
- **Implementation**: Timestamp-based offsets stored in `_bronze_hwm_processing_offsets` table
- **Overlap Handling**: 48-72 hour overlap to handle late-arriving data
- **Processing Logic**: 
  ```python
  # Get last processed timestamp
  last_timestamp = get_last_processed_timestamp(spark, "system.billing.usage")
  
  # Query source with overlap
  if last_timestamp:
      query = f"SELECT * FROM system.billing.usage WHERE ingestion_date > '{last_timestamp}'"
  else:
      query = f"SELECT * FROM system.billing.usage WHERE ingestion_date >= current_date() - {overlap_days}"
  ```

#### **Silver Layer - CDF (Change Data Feed) Approach:**
- **Purpose**: Process only new/changed records from Bronze tables using CDF
- **Implementation**: Version-based offsets stored in `_cdf_processing_offsets` table
- **Task-Based Offsets**: Separate offsets for each fact table to avoid conflicts
- **Processing Logic**:
  ```python
  # Read CDF from Bronze table
  df, end_version = read_cdf(spark, "platform_observability.plt_bronze.brz_billing_usage", "brz_billing_usage")
  
  # Process only new/changed records
  df = df.where(F.col("_change_type").isin(["insert", "update_postimage"]))
  
  # Commit processing offset
  commit_processing_offset(spark, "brz_billing_usage", end_version)
  ```

#### **Gold Layer - Date-Partitioned Incremental Processing:**
- **Purpose**: Update only impacted date partitions in fact tables
- **Implementation**: Task-based offsets with date_key filtering
- **SCD2 Alignment**: Facts reference correct dimension versions for each date
- **Processing Logic**:
  ```python
  # Get incremental data for specific task
  task_name = "task_gld_fact_usage_priced_day"
  last_timestamp, last_version = get_last_processed_timestamp(spark, "slv_usage_txn", task_name, "silver")
  
  # Filter by date_key for partition-level updates
  if last_timestamp:
      df = silver_df.filter(F.col("ingestion_date") > last_timestamp)
  ```

### **CDF Implementation Details:**

#### **Bronze Layer CDF Setup:**
```sql
-- Enable CDF on all Bronze tables
ALTER TABLE platform_observability.plt_bronze.brz_billing_usage 
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

#### **Silver Layer CDF Reading:**
```python
def read_cdf(spark, table, source_name, change_types=("insert", "update_postimage")):
    start_version = _get_processing_offset(spark, source_name)
    end_version = _get_latest_version(spark, table)
    
    if start_version >= end_version:
        return spark.createDataFrame([], spark.table(table).schema), end_version
    
    opts = {
        "readChangeDataFeed": "true",
        "startingVersion": str(start_version + 1),
        "endingVersion": str(end_version)
    }
    
    df = spark.read.format("delta").options(**opts).table(table)
    df = df.where(F.col("_change_type").isin(list(change_types)))
    return df, end_version
```

#### **Task-Based Offset Management:**
- **Problem**: Multiple fact tables reading from same Silver table would conflict
- **Solution**: Separate offsets per task (e.g., `task_gld_fact_usage_priced_day`)
- **Implementation**: 
  ```python
  def get_task_name(fact_table_name):
      return f"task_{fact_table_name}"
  
  # Each fact table has its own processing state
  task_name = get_task_name("gld_fact_usage_priced_day")
  last_timestamp, last_version = get_last_processed_timestamp(spark, "slv_usage_txn", task_name, "silver")
  ```

### **Processing State Tables:**

#### **Bronze HWM Processing Offsets:**
```sql
CREATE TABLE platform_observability.plt_bronze._bronze_hwm_processing_offsets (
    source_table STRING,
    last_processed_timestamp TIMESTAMP,
    last_processed_version BIGINT,
    updated_at TIMESTAMP
) USING DELTA;
```

#### **Silver CDF Processing Offsets:**
```sql
CREATE TABLE platform_observability.plt_silver._cdf_processing_offsets (
    source_table STRING,
    task_name STRING,
    last_processed_timestamp TIMESTAMP,
    last_processed_version BIGINT,
    updated_at TIMESTAMP
) USING DELTA
PARTITIONED BY (source_table);
```

## **File Structure to Create**

```
platform-observability/
├── config.py                                    # Environment-aware configuration
├── requirements.txt                             # Python dependencies
├── env.example                                  # Environment configuration template
├── libs/
│   ├── __init__.py
│   ├── cdf.py                                   # CDF operations
│   ├── data_enrichment.py                       # Data transformations
│   ├── error_handling.py                        # Data quality validation
│   ├── gold_dimension_builder.py                # Dimension building utilities
│   ├── gold_fact_builder.py                     # Fact building utilities
│   ├── gold_view_builder.py                     # View building utilities
│   ├── logging.py                               # Structured logging
│   ├── monitoring.py                            # Monitoring and alerting
│   ├── path_setup.py                            # Path management
│   ├── processing_state.py                      # Processing state management
│   ├── sql_manager.py                           # SQL file management
│   ├── sql_parameterizer.py                     # SQL parameterization
│   ├── tag_processor.py                         # Tag processing
│   └── utils.py                                 # Utility functions
├── sql/
│   ├── config/
│   │   ├── bootstrap_catalog_schemas.sql        # Catalog and schema creation
│   │   ├── performance_optimizations.sql        # Performance tuning
│   │   └── processing_offsets.sql               # Processing state tables
│   ├── bronze/
│   │   └── bronze_tables.sql                    # Bronze table DDL
│   ├── silver/
│   │   └── silver_tables.sql                    # Silver table DDL
│   └── gold/
│       ├── gold_dimensions.sql                  # Dimension tables
│       ├── gold_facts.sql                       # Fact tables
│       ├── gold_views.sql                       # Business views
│       ├── gold_chargeback_views.sql            # Cost allocation views
│       ├── gold_runtime_analysis_views.sql      # Runtime optimization views
│       └── policy_compliance.sql                # Policy compliance views
├── notebooks/
│   ├── bronze_hwm_ingest_job.py                 # Bronze ingestion job
│   ├── silver_hwm_build_job.py                  # Silver layer build job
│   ├── gold_hwm_build_job.py                    # Gold layer build job
│   ├── health_check_job.py                      # Health monitoring job
│   ├── performance_optimization_job.py          # Performance optimization job
│   ├── generate_dashboard_json.py               # Dashboard generation
│   ├── deploy_dashboard.py                      # Dashboard deployment
│   └── platform_observability_deploy.py         # Main deployment notebook
├── jobs/
│   └── daily_observability_workflow.json        # Daily workflow configuration
├── resources/
│   ├── dbr_schema.json                          # Source system table schemas
│   └── dashboard/
│       ├── dashboard_template.json              # Dashboard structure
│       ├── dashboard_sql_queries.json           # Dashboard SQL statements
│       └── dashboard_development_knowledge.md   # Dashboard development guide
└── tests/
    ├── test_config.py                           # Configuration tests
    ├── test_logging.py                          # Logging tests
    ├── test_sql_manager.py                      # SQL manager tests
    └── test_error_handling.py                   # Error handling tests
```

## **Expected Outcomes**
- **Cost Visibility**: Identify top cost drivers and reduce waste
- **Run Health**: Track failures and their cost impact
- **Performance**: Detect task latency regressions
- **Compliance**: Enforce policy compliance and measure tag coverage
- **Chargeback**: Generate accurate cost allocation reports

## **Scheduling & Operations**
- **Schedule**: Daily at 05:30 Asia/Kolkata
- **Processing**: Incremental with late-arrival handling
- **Monitoring**: Comprehensive health checks and alerting
- **Deployment**: Automated CI/CD with environment separation

This system should process data daily, handle late arrivals gracefully, and provide comprehensive observability across the entire Databricks platform.

---

*This document was generated by reverse engineering the existing platform-observability codebase to provide clear build instructions for recreating the system from scratch.*
