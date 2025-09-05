# Platform Observability (CDF-driven) 🚀

**Catalog**: `platform_observability`  
**Schemas**: `plt_bronze`, `plt_silver`, `plt_gold`

## ✨ What This Repo Does

- **Bronze (Job)**: Daily **High-Water Mark (HWM)** ingestion from `system.*` tables with CDF enabled for downstream consumption
- **Silver (HWM)**: Processes **only new/changed records** via Change Data Feed (CDF) with per-source version processing offsets
- **Gold (HWM)**: Incrementally **updates** only **impacted `date_sk`** partitions into facts/dims + views for compliance/latency insights
- **Monitoring & Alerting**: Comprehensive observability with structured logging, performance metrics, and automated alerts
- **Data Quality**: Built-in validation rules and monitoring for data integrity
- **CI/CD**: Automated deployment pipelines for dev and production environments
- **Code Quality**: **Externalized SQL files** for better maintainability and version control

## 🏗️ Architecture Overview

The solution follows a modern data architecture with three layers with **SCD2 (Slowly Changing Dimension Type 2)** support in the Gold layer:

### **SCD2 Benefits**
- **Historical Tracking**: Preserve complete history of entity changes (jobs, pipelines, clusters)
- **Temporal Accuracy**: Ensure facts reference correct dimension versions for each date
- **Audit Trail**: Maintain complete change history for compliance and analysis
- **Business Intelligence**: Support time-based analysis and reporting

```
System Tables (billing, lakeflow, access, compute)
        │
        ▼  (HWM Job — Bronze, CDF enabled)
platform_observability.plt_bronze.*  ───────────────►  Bronze with CDF
        │                                  ▲
        │                                  │ CDF versions (processing offsets)
        ▼
(HWM Jobs — Silver & Gold, incremental processing)
        │
        ├─► Silver: entity SCD2, pricing, tags, timelines, cluster/node types
        │
        └─► Gold: dims & facts (incremental updates by date_sk)
                 ├─ gld_dim_date, gld_dim_workspace, gld_dim_entity, gld_dim_sku, gld_dim_run_status, gld_dim_node_type
├─ gld_fact_usage_priced_day, gld_fact_entity_cost, gld_fact_run_cost
├─ gld_fact_run_status_cost, gld_fact_runs_finished_day, gld_fact_usage_by_node_type_day
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
│  ├─ config/                                  # Configuration and control tables
│  │  ├─ processing_offsets.sql                # CDF and HWM processing offsets
│  │  └─ performance_optimizations.sql         # Performance tuning & optimizations
│  ├─ bronze/                                  # Bronze layer DDL
│  │  └─ bronze_tables.sql           # Bronze tables with CDF enabled
│  ├─ silver/                                  # Silver layer DDL
│  │  └─ silver_tables.sql                     # Silver tables (SCD2)
│  └─ gold/                                    # Gold layer DDL and views
│     ├─ gold_dimensions.sql                   # Dimension tables
│     ├─ gold_facts.sql                        # Fact tables
│     ├─ gold_views.sql                        # Business-ready views
│     ├─ gold_chargeback_views.sql             # Cost allocation views
│     ├─ gold_runtime_analysis_views.sql       # Runtime optimization views
│     └─ policy_compliance.sql                 # Policy compliance views
├─ libs/
│  ├─ utils.py                                 # utility functions
│  ├─ cdf.py                                   # CDF operations
│  ├─ data_enrichment.py                       # data transformations and enrichment
│  ├─ processing_state.py                      # processing state management
│  ├─ tag_processor.py                         # tag processing and normalization
│  ├─ logging.py                               # 🆕 Structured logging & performance monitoring
│  ├─ error_handling.py                        # 🆕 Error handling & data quality validation
│  ├─ monitoring.py                            # 🆕 Monitoring & alerting system
│  └─ sql_manager.py                           # 🆕 SQL file management utility
├─ notebooks/
│  ├─ bronze_hwm_ingest_job.py                 # 🆕 Enhanced Bronze ingest Job with monitoring
│  ├─ silver_hwm_build_job.py                  # 🆕 Silver layer HWM build job
│  ├─ gold_hwm_build_job.py                    # 🆕 Gold layer HWM build job
│  └─ platform_observability_deployment.py    # 🆕 Main deployment notebook
├─ jobs/
│  └─ daily_observability_workflow.json        # Daily workflow configuration
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
-- 1. Create processing offsets tables
RUN sql/config/processing_offsets.sql

-- 2. Bootstrap Bronze tables
RUN sql/bronze/bronze_tables.sql

-- 3. Create Silver tables
RUN sql/silver/silver_tables.sql

-- 4. Create Gold tables and views
RUN sql/gold/gold_dimensions.sql
RUN sql/gold/gold_facts.sql
RUN sql/gold/gold_views.sql
RUN sql/gold/gold_chargeback_views.sql
RUN sql/gold/gold_runtime_analysis_views.sql
RUN sql/gold/policy_compliance.sql

-- 5. Apply performance optimizations
RUN sql/config/performance_optimizations.sql
```

### 4. **Create Databricks Resources**
- **Jobs**: 
  - `notebooks/bronze_hwm_ingest_job.py` (configuration-based: `overlap_hours` from config)
  - `notebooks/silver_hwm_build_job.py` (Silver layer build)
  - `notebooks/gold_hwm_build_job.py` (Gold layer build with SCD2 support)
  - `notebooks/performance_optimization_job.py` (Performance optimization)
  - `notebooks/health_check_job.py` (Health monitoring)
- **Workflow**: Import `jobs/daily_observability_workflow.json`
- **Note**: All notebooks can run standalone without job parameters (configuration-based)

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
    "gold_facts",
    catalog="platform_observability",
    gold_schema="plt_gold"
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
OPTIMIZE plt_gold.gld_fact_usage_priced_day ZORDER BY (workspace_id, date_sk);

-- Collect statistics
ANALYZE TABLE plt_gold.gld_fact_usage_priced_day COMPUTE STATISTICS FOR ALL COLUMNS;
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
sql/
├─ config/                             # Configuration and control tables
├─ bronze/                             # Bronze layer DDL
├─ silver/                             # Silver layer DDL
└─ gold/                               # Gold layer DDL and views
    ├─ gold_dimensions.sql             # Dimension tables
    ├─ gold_facts.sql                  # Fact tables
    ├─ gold_views.sql                  # Business views
    ├─ gold_chargeback_views.sql       # Cost allocation views
    ├─ gold_runtime_analysis_views.sql # Runtime optimization views
    └─ policy_compliance.sql           # Policy compliance views
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
