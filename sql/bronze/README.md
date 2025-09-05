# Bronze Layer SQL Files

This folder contains all Bronze layer SQL operations for the Platform Observability system.

## Structure

```
bronze/
├── README.md                           # This file
├── bronze_tables.sql         # Initial table creation with CDF enabled
└── operations/                         # Individual upsert operations
    ├── upsert_billing_usage.sql        # Billing usage data ingestion
    ├── upsert_list_prices.sql          # Pricing data ingestion
    ├── upsert_lakeflow_jobs.sql        # Lakeflow jobs data ingestion
    ├── upsert_lakeflow_pipelines.sql   # Lakeflow pipelines data ingestion
    ├── upsert_job_run_timeline.sql     # Job run timeline ingestion
    ├── upsert_job_task_run_timeline.sql # Job task run timeline ingestion
    ├── upsert_compute_clusters.sql     # Compute clusters data ingestion
    ├── upsert_compute_node_types.sql   # Compute node types data ingestion
    └── upsert_access_workspaces.sql   # Workspace access data ingestion
```

## Purpose

The Bronze layer serves as the **raw data ingestion layer** where:
- Data is ingested from various system tables
- Change Data Feed (CDF) is enabled for incremental processing
- Minimal transformation is applied (only schema alignment)
- Data quality checks are basic (existence, format)

## Key Characteristics

- **CDF Enabled**: All tables have `delta.enableChangeDataFeed = 'true'`
- **Auto-optimization**: Tables use `delta.autoOptimize.optimizeWrite = 'true'`
- **Raw Format**: Data is stored in its original form from system tables
- **Incremental Processing**: Uses High Water Mark (HWM) for efficient ingestion

## Usage

### 1. Bootstrap Tables
```sql
-- Execute via SQLParameterizer
parameterizer.bootstrap_bronze_tables()
```

### 2. Individual Operations
```sql
-- Execute specific upsert operations
parameterizer.execute_bootstrap_sql("bronze/operations/upsert_billing_usage")
```

## Data Flow

```
System Tables → Bronze Layer → Silver Layer (via CDF)
     ↓              ↓              ↓
Raw Data    →   CDF Enabled  →  Curated Data
```

## Maintenance

- Tables are automatically optimized via `performance_optimizations.sql`
- CDF processing offsets are managed by the Silver layer pipeline
- HWM processing offsets track ingestion progress for each source table
