# Platform Observability - Parameterization Guide

## Table of Contents
- [1. Overview](#1-overview)
- [2. Configuration Management](#2-configuration-management)
- [3. SQL File Organization](#3-sql-file-organization)
- [4. Usage Examples](#4-usage-examples)
- [5. Migration Guide](#5-migration-guide)
- [6. Related Documentation](#6-related-documentation)

## 1. Overview

This guide explains the parameterized configuration and SQL management system that enables environment-specific deployments without code changes.

### 1.1 Key Benefits

- **Environment Agnostic**: Same code works across dev, staging, and production
- **Centralized Configuration**: All settings managed in one place
- **SQL Externalization**: DDL and business logic stored in separate SQL files
- **Automatic Parameterization**: Catalog and schema names injected automatically
- **Easy Maintenance**: Update configuration without touching code

### 1.2 Architecture

```
config.py → EnvironmentConfig → SQL Files → Parameterized Execution
    ↓              ↓              ↓              ↓
Environment    Schema Names   Placeholders   Runtime Values
Variables     Table Names    {catalog}      platform_observability
              Log Levels     {bronze_schema} plt_bronze
              Timezones      {silver_schema} plt_silver
                            {gold_schema}    plt_gold
```

## 2. Configuration Management

### 2.1 Environment Configuration

The system uses a hierarchical configuration approach:

```python
from config import Config

# Get configuration for current environment
config = Config.get_config()

# Access configuration values
catalog = config.catalog                    # platform_observability
bronze_schema = config.bronze_schema        # plt_bronze
silver_schema = config.silver_schema        # plt_silver
gold_schema = config.gold_schema           # plt_gold
log_level = config.log_level               # INFO
timezone = config.timezone                 # Asia/Kolkata
```

### 2.2 Environment Variables

Override default configuration using environment variables:

```bash
# Set environment-specific values
export ENVIRONMENT=prod
export CATALOG=platform_observability_prod
export BRONZE_SCHEMA=plt_bronze_prod
export SILVER_SCHEMA=plt_silver_prod
export GOLD_SCHEMA=plt_gold_prod
export LOG_LEVEL=WARNING
export TIMEZONE=UTC
```

### 2.3 Configuration Hierarchy

1. **Environment Variables** (highest priority)
2. **Environment-Specific Config** (dev, staging, prod)
3. **Default Values** (lowest priority)

## 3. SQL File Organization

### 3.1 Folder Structure

```
sql/
├── config/                    # Configuration and control files
│   ├── bootstrap_catalog_schemas.sql
│   ├── bookmarks.sql
│   └── performance_optimizations.sql
├── bronze/                    # Bronze layer operations
│   ├── bronze_tables.sql
│   └── operations/            # Individual upsert operations
│       ├── upsert_billing_usage.sql
│       ├── upsert_lakeflow_jobs.sql
│       └── ...
├── silver/                    # Silver layer DDL
│   └── silver_tables.sql
└── gold/                      # Gold layer DDL and views
    ├── gold_dimensions.sql
    ├── gold_facts.sql
    ├── gold_views.sql
    ├── policy_compliance.sql
    └── gold_chargeback_views.sql
```

### 3.2 SQL Parameterization

SQL files use placeholders that are automatically replaced:

```sql
-- Example: sql/bronze/bronze_tables.sql
CREATE TABLE IF NOT EXISTS {catalog}.{bronze_schema}.bronze_sys_billing_usage_raw (
    workspace_id BIGINT,
    cloud STRING,
    sku_name STRING,
    usage_unit STRING,
    usage_start_time TIMESTAMP,
    usage_end_time TIMESTAMP,
    usage_quantity DOUBLE,
    billing_origin_product STRING,
    tags MAP<STRING, STRING>,
    usage_metadata STRUCT<job_id:STRING, job_run_id:STRING, cluster_id:STRING>,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

### 3.3 Available Placeholders

| Placeholder | Description | Example Value |
|-------------|-------------|---------------|
| `{catalog}` | Main catalog name | `platform_observability` |
| `{bronze_schema}` | Bronze layer schema | `plt_bronze` |
| `{silver_schema}` | Silver layer schema | `plt_silver` |
| `{gold_schema}` | Gold layer schema | `plt_gold` |

## 4. Usage Examples

### 4.1 Basic Configuration

```python
from config import Config

# Get configuration
config = Config.get_config()

# Generate table names
billing_table = f"{config.catalog}.{config.bronze_schema}.bronze_sys_billing_usage_raw"
print(f"Billing table: {billing_table}")
# Output: platform_observability.plt_bronze.bronze_sys_billing_usage_raw
```

### 4.2 SQL File Execution

```python
from libs.sql_manager import sql_manager

# Execute parameterized SQL
sql = sql_manager.parameterize_sql_with_catalog_schema("bronze/bronze_tables")
spark.sql(sql)

# Get available operations
operations = sql_manager.get_available_operations()
print("Available operations:", operations)
```

### 4.3 Bootstrap Operations

```python
from libs.sql_parameterizer import SQLParameterizer

# Initialize parameterizer
parameterizer = SQLParameterizer(spark)

# Bootstrap entire system
parameterizer.full_bootstrap()

# Or execute individual steps
parameterizer.bootstrap_catalog_schemas()
parameterizer.bootstrap_bronze_tables()
parameterizer.create_bookmarks()
parameterizer.create_silver_tables()
parameterizer.create_gold_tables()
parameterizer.apply_performance_optimizations()
parameterizer.create_gold_views()
```

### 4.4 Environment-Specific Configuration

```python
from config import Config

# Create custom environment configuration
custom_config = Config.get_config(environment="staging")

# Use custom configuration
print(f"Staging catalog: {custom_config.catalog}")
print(f"Staging bronze schema: {custom_config.bronze_schema}")
```

## 5. Migration Guide

### 5.1 From Hardcoded Values

**Before (Hardcoded):**
```python
# Old way - hardcoded values
catalog = "platform_observability"
bronze_schema = "plt_bronze"
table_name = f"{catalog}.{bronze_schema}.bronze_sys_billing_usage_raw"
```

**After (Parameterized):**
```python
# New way - parameterized
from config import Config
config = Config.get_config()
table_name = f"{config.catalog}.{config.bronze_schema}.bronze_sys_billing_usage_raw"
```

### 5.2 From Inline SQL

**Before (Inline SQL):**
```python
# Old way - inline SQL
sql = f"""
CREATE TABLE IF NOT EXISTS platform_observability.plt_bronze.bronze_sys_billing_usage_raw (
    workspace_id BIGINT,
    cloud STRING
)
"""
```

**After (Externalized SQL):**
```python
# New way - externalized SQL
from libs.sql_manager import sql_manager
sql = sql_manager.parameterize_sql_with_catalog_schema("bronze/bronze_tables")
```

### 5.3 Testing Configuration

```python
# Test configuration in different environments
environments = ["dev", "staging", "prod"]

for env in environments:
    config = Config.get_config(environment=env)
    print(f"\n{env.upper()} Environment:")
    print(f"  Catalog: {config.catalog}")
    print(f"  Bronze Schema: {config.bronze_schema}")
    print(f"  Silver Schema: {config.silver_schema}")
    print(f"  Gold Schema: {config.gold_schema}")
```

## 6. Related Documentation

- [01-overview.md](01-overview.md) - Solution overview and architecture
- [02-getting-started.md](02-getting-started.md) - Step-by-step deployment guide
- [04-data-dictionary.md](04-data-dictionary.md) - Complete data model documentation
- [11-deployment.md](11-deployment.md) - Production deployment and workflow setup

## Troubleshooting

### Common Issues

1. **Configuration Not Found**: Ensure environment variables are set correctly
2. **SQL File Not Found**: Check file paths in `sql/` directory
3. **Placeholder Not Replaced**: Verify placeholder syntax `{catalog}`, `{schema}`
4. **Permission Errors**: Ensure proper access to catalog and schemas

### Debug Commands

```python
# Check current configuration
from config import Config
config = Config.get_config()
print(f"Current config: {config}")

# List available SQL operations
from libs.sql_manager import sql_manager
operations = sql_manager.get_available_operations()
print(f"Available operations: {operations}")

# Test SQL parameterization
sql = sql_manager.parameterize_sql_with_catalog_schema("config/bootstrap_catalog_schemas")
print(f"Parameterized SQL: {sql}")
```

## Best Practices

1. **Always use placeholders** in SQL files instead of hardcoded values
2. **Test configuration** in multiple environments before deployment
3. **Use environment variables** for production overrides
4. **Keep SQL files organized** in logical folder structure
5. **Validate configuration** before executing SQL operations
6. **Document custom configurations** for team members
