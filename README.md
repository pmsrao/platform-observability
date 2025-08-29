# Platform Observability (CDF-driven) 🚀

**Catalog**: `platform_observability`  
**Schemas**: `plt_bronze`, `plt_silver`, `plt_gold`

## ✨ What This Repo Does

- **Bronze (Job)**: Daily **High-Water Mark (HWM)** ingestion from `system.*` tables with CDF enabled for downstream consumption
- **Silver (DLT)**: Processes **only new/changed records** via Change Data Feed (CDF) with per-source version bookmarks
- **Gold**: Incrementally **MERGEs** only **impacted `date_sk`** partitions into facts/dims + views for compliance/latency insights
- **Monitoring & Alerting**: Comprehensive observability with structured logging, performance metrics, and automated alerts
- **Data Quality**: Built-in validation rules and monitoring for data integrity
- **CI/CD**: Automated deployment pipelines for dev and production environments
- **Code Quality**: **Externalized SQL files** for better maintainability and version control

## 🏗️ Architecture Overview

```
System Tables (billing, lakeflow, access, compute)
        │
        ▼  (DLT Pipeline A — Bronze, CDF enabled)
platform_observability.plt_bronze.*  ───────────────►  Bronze with CDF
        │                                  ▲
        │                                  │ CDF versions (bookmarks)
        ▼
(DLT Pipeline B — Silver/Gold, CDF read)
        │
        ├─► Silver: entity SCD2, pricing, tags, timelines, cluster/node types
        │
        └─► Gold: dims & facts (MERGE only impacted date_sk)
                 ├─ dim_date, dim_workspace, dim_entity, dim_sku, dim_run_status, dim_node_type
                 ├─ fact_usage_priced_day, fact_entity_cost, fact_run_cost
                 ├─ fact_run_status_cost, fact_runs_finished_day, fact_usage_by_node_type_day
                 └─ views: policy compliance, tag coverage, latency trend/anomaly
```

## 📁 Enhanced Repository Structure

```
platform-observability/
├─ README.md                                    # This file
├─ config.py                                    # 🆕 Environment-aware configuration
├─ requirements.txt                             # 🆕 Python dependencies
├─ env.example                                  # 🆕 Environment configuration template
├─ docs/
│  └─ Platform Observability — Solution Overview & Usage Guide (CDF-driven) v0.1.md
├─ sql/
│  ├─ 00_bootstrap_catalog_schemas.sql         # catalog & schema scaffold
│  ├─ 01_state_bookmarks.sql                   # CDF bookmarks (Silver/Gold)
│  ├─ 02_bronze_high_watermarks.sql            # HWM bookmarks (Bronze)
│  ├─ 03_bronze_tables_bootstrap.sql           # empty Bronze tables (CDF ON)
│  ├─ gold_views.sql                           # trend/anomaly helper views
│  ├─ policy_compliance.sql                    # policy baseline & compliance views
│  ├─ performance_optimizations.sql             # 🆕 Performance tuning & optimizations
│  └─ bronze_operations/                       # 🆕 Externalized SQL operations
│     ├─ upsert_billing_usage.sql              # Billing usage upsert
│     ├─ upsert_list_prices.sql                # List prices upsert
│     ├─ upsert_job_run_timeline.sql           # Job run timeline upsert
│     ├─ upsert_job_task_run_timeline.sql      # Job task run timeline upsert
│     ├─ upsert_lakeflow_jobs.sql              # Lakeflow jobs upsert
│     ├─ upsert_lakeflow_pipelines.sql         # Lakeflow pipelines upsert
│     ├─ upsert_compute_clusters.sql           # Compute clusters upsert
│     ├─ upsert_compute_node_types.sql         # Compute node types upsert
│     └─ upsert_access_workspaces.sql          # Access workspaces upsert
├─ libs/
│  ├─ utils.py                                 # utility functions
│  ├─ cdf.py                                   # CDF operations
│  ├─ transform.py                             # data transformations
│  ├─ bookmarks.py                             # bookmark management
│  ├─ logging.py                               # 🆕 Structured logging & performance monitoring
│  ├─ error_handling.py                        # 🆕 Error handling & data quality validation
│  ├─ monitoring.py                            # 🆕 Monitoring & alerting system
│  └─ sql_manager.py                           # 🆕 SQL file management utility
├─ pipelines/
│  └─ silver_gold/
│     └─ silver_gold_build_dlt.py              # DLT Pipeline-B (target: plt_silver → builds Gold)
├─ notebooks/
│  └─ bronze_hwm_ingest_job.py                 # 🆕 Enhanced Bronze ingest Job with monitoring
├─ jobs/
│  ├─ workflow_bronze_job_plus_dlt.json        # Daily workflow: Bronze Job -> DLT
│  └─ daily_observability_workflow.json        # (optional) legacy DLT-only workflow
├─ tests/                                       # 🆕 Comprehensive test suite
│  ├─ test_config.py                           # Configuration tests
│  ├─ test_logging.py                          # Logging & monitoring tests
│  ├─ test_error_handling.py                   # Error handling tests
│  └─ test_sql_manager.py                      # 🆕 SQL manager tests
└─ .github/workflows/                           # 🆕 CI/CD automation
    └─ deploy.yml                               # GitHub Actions deployment workflow
```

## 🚀 New Features & Improvements

### 1. **Environment-Aware Configuration** ⚙️
- **Centralized config management** with environment-specific overrides
- **Dev/Prod separation** with different settings (overlap hours, log levels, alerts)
- **Environment variable support** for flexible deployment
- **Type-safe configuration** with dataclasses

### 2. **Structured Logging & Performance Monitoring** 📊
- **JSON-structured logs** for easy parsing and analysis
- **Performance metrics tracking** (execution time, records processed, success rates)
- **Correlation IDs** for tracing requests across components
- **Automatic performance monitoring** with decorators

### 3. **Comprehensive Error Handling & Data Quality** ✅
- **Built-in data quality rules** (non-negative quantities, valid time ranges, required fields)
- **Graceful error handling** with detailed logging
- **Data quality scoring** and monitoring
- **Safe execution decorators** for critical operations

### 4. **Monitoring & Alerting System** 🚨
- **Real-time monitoring** of pipeline health and performance
- **Automated alerts** for failures, cost thresholds, and data freshness
- **Health checks** for overall system status
- **Extensible alert handlers** (Slack, Teams, email)

### 5. **Performance Optimizations** ⚡
- **Z-ORDER optimization** for common query patterns
- **Table properties** for auto-optimization and compaction
- **Materialized views** for expensive aggregations
- **Partitioning strategies** for better query performance

### 6. **CI/CD Automation** 🔄
- **Automated testing** (linting, type checking, unit tests)
- **Security scanning** with Bandit
- **Environment-specific deployments** (dev/prod)
- **Automated releases** and notifications

### 7. **Code Quality & Maintainability** 🧹
- **Externalized SQL files** for better version control and maintenance
- **SQL file manager** with parameterization and caching
- **Separation of concerns** between Python logic and SQL operations
- **Easier SQL debugging** and modification without touching Python code

## 🛠️ Setup & Configuration

### 1. **Environment Setup**
```bash
# Copy environment template
cp env.example .env

# Update with your values
ENVIRONMENT=dev
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
```

### 2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

### 3. **Database Setup**
```sql
-- Run in sequence:
-- 1. Bootstrap catalog and schemas
RUN sql/00_bootstrap_catalog_schemas.sql

-- 2. Create CDF bookmarks
RUN sql/01_state_bookmarks.sql

-- 3. Create HWM bookmarks
RUN sql/02_bronze_high_watermarks.sql

-- 4. Bootstrap Bronze tables
RUN sql/03_bronze_tables_bootstrap.sql

-- 5. Apply performance optimizations
RUN sql/performance_optimizations.sql
```

### 4. **Create Databricks Resources**
- **Job**: `notebooks/bronze_hwm_ingest_job.py` (parameter: `overlap_hours`, default: 48)
- **DLT Pipeline**: `pipelines/silver_gold/silver_gold_build_dlt.py` (target: `plt_silver`)
- **Workflow**: Import `jobs/workflow_bronze_job_plus_dlt.json`

### 5. **Schedule & Monitor**
- **Schedule**: Daily at 05:30 Asia/Kolkata
- **Monitoring**: Check logs and alerts via structured logging
- **Health Checks**: Use built-in health monitoring system

## 🔍 Monitoring & Observability

### **Pipeline Health Dashboard**
```python
from libs.monitoring import health_checker

# Check overall system health
health_status = health_checker.check_system_health()
print(f"System Status: {health_status['overall_status']}")
```

### **Performance Metrics**
```python
from libs.logging import performance_monitor

@performance_monitor("my_operation")
def my_function():
    # Your code here
    pass
```

### **Data Quality Validation**
```python
from libs.error_handling import validate_data_quality

# Validate DataFrame before processing
if not validate_data_quality(df, "table_name", logger):
    raise ValueError("Data quality validation failed")
```

### **SQL File Management**
```python
from libs.sql_manager import sql_manager

# Load and parameterize SQL
sql = sql_manager.parameterize_sql(
    "upsert_billing_usage",
    target_table="platform_observability.plt_bronze.bronze_sys_billing_usage_raw",
    source_table="stg_usage"
)

# Execute SQL
spark.sql(sql)
```

## 🧪 Testing

### **Run All Tests**
```bash
pytest tests/ -v --cov=libs --cov-report=html
```

### **Run Specific Test Categories**
```bash
# Configuration tests
pytest tests/test_config.py -v

# Logging tests
pytest tests/test_logging.py -v

# Error handling tests
pytest tests/test_error_handling.py -v

# SQL manager tests
pytest tests/test_sql_manager.py -v
```

### **Code Quality Checks**
```bash
# Linting
flake8 libs/ tests/

# Type checking
mypy libs/ --ignore-missing-imports

# Security scanning
bandit -r libs/
```

## 🚀 Deployment

### **Automated Deployment (Recommended)**
- **Push to `develop`**: Automatic deployment to dev environment
- **Push to `main`**: Automatic deployment to production
- **Manual deployment**: Use GitHub Actions workflow dispatch

### **Manual Deployment**
```bash
# Set environment
export ENVIRONMENT=prod
export DATABRICKS_HOST=your-host
export DATABRICKS_TOKEN=your-token

# Deploy using Databricks CLI
databricks fs cp libs/ dbfs:/platform-observability/prod/libs/ --recursive
databricks fs cp notebooks/ dbfs:/platform-observability/prod/notebooks/ --recursive
databricks fs cp sql/ dbfs:/platform-observability/prod/sql/ --recursive
```

## 📈 Performance Tuning

### **Automatic Optimizations**
- **Z-ORDER**: Optimized for common query patterns
- **Auto-compaction**: Automatic file compaction
- **Statistics**: Automatic statistics collection
- **Caching**: Frequently accessed tables cached

### **Manual Optimizations**
```sql
-- Optimize specific tables
OPTIMIZE plt_gold.fact_usage_priced_day ZORDER BY (workspace_id, date_sk);

-- Collect statistics
ANALYZE TABLE plt_gold.fact_usage_priced_day COMPUTE STATISTICS FOR ALL COLUMNS;
```

## 🚨 Alerting & Notifications

### **Built-in Alerts**
- **Pipeline failures** with detailed error information
- **Cost threshold violations** for budget management
- **Data freshness alerts** for stale data detection
- **Performance degradation** warnings

### **Custom Alerts**
```python
from libs.monitoring import monitoring_system, AlertSeverity

# Create custom alert
monitoring_system.create_alert(
    title="Custom Alert",
    message="Custom message",
    severity=AlertSeverity.WARNING,
    source="custom_monitor"
)
```

## 🔧 Configuration Options

### **Environment-Specific Settings**
| Setting | Dev | Prod |
|---------|-----|------|
| Overlap Hours | 72 | 48 |
| Log Level | DEBUG | INFO |
| Alerts | Disabled | Enabled |
| Monitoring | Basic | Full |

### **Performance Settings**
- **Auto-optimize**: Enabled by default
- **Auto-compact**: Enabled by default
- **Z-ORDER**: Applied to common query patterns
- **Partitioning**: By `date_sk` for time-based queries

## 📊 Data Quality Rules

### **Built-in Validations**
1. **Non-negative quantities** for usage data
2. **Valid time ranges** (end > start)
3. **Required fields** not null
4. **Data freshness** within expected thresholds

### **Custom Rules**
```python
from libs.error_handling import DataQualityRule, DataQualityMonitor

# Add custom rule
custom_rule = DataQualityRule(
    name="custom_validation",
    description="Custom validation rule",
    rule_type="custom",
    severity="warning",
    rule_function=my_validation_function
)

monitor = DataQualityMonitor(logger)
monitor.add_rule(custom_rule)
```

## 🗄️ SQL File Management

### **Benefits of Externalized SQL**
- **Version Control**: SQL changes tracked separately from Python code
- **Maintainability**: SQL experts can modify queries without Python knowledge
- **Reusability**: SQL files can be shared across different Python modules
- **Testing**: SQL can be validated independently of Python logic
- **Documentation**: SQL files serve as self-documenting data operations

### **SQL File Structure**
```
sql/bronze_operations/
├─ upsert_billing_usage.sql           # Parameterized with {target_table}, {source_table}
├─ upsert_list_prices.sql             # Parameterized with {target_table}, {source_table}
├─ upsert_job_run_timeline.sql        # Parameterized with {target_table}, {source_table}
└─ ...                                # Additional operations
```

### **SQL Parameterization**
```sql
-- Example: upsert_billing_usage.sql
MERGE INTO {target_table} T
USING {source_table} S
ON T.workspace_id = S.workspace_id
-- ... rest of merge logic
```

### **Using SQL Manager**
```python
from libs.sql_manager import sql_manager

# List available operations
operations = sql_manager.get_available_operations()
print(f"Available SQL operations: {operations}")

# Load and parameterize SQL
sql = sql_manager.parameterize_sql(
    "upsert_billing_usage",
    target_table="my_target_table",
    source_table="my_source_view"
)

# Execute
spark.sql(sql)
```

## 🔐 Security & Compliance

### **Security Features**
- **Environment isolation** (dev/prod separation)
- **Audit logging** for all operations
- **Encryption** support for sensitive data
- **Access control** via Databricks permissions

### **Compliance Monitoring**
- **Policy compliance** views for clusters
- **Tag coverage** monitoring
- **Security mode** validation
- **Node type** deprecation checks

## 📚 Additional Resources

- **Documentation**: See `docs/` folder for detailed guides
- **Examples**: Check `notebooks/` for usage examples
- **Tests**: Run `tests/` for validation
- **Issues**: Report bugs and feature requests via GitHub

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch
3. **Make** your changes
4. **Add** tests for new functionality
5. **Run** the test suite
6. **Submit** a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**🎯 Ready to deploy?** Start with the [Setup & Configuration](#️-setup--configuration) section above!
