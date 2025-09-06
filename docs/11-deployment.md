# Platform Observability - Cloud-Agnostic Deployment Guide

## Table of Contents
- [1. Overview](#1-overview)
- [2. Cloud-Agnostic Features](#2-cloud-agnostic-features)
- [3. Prerequisites](#3-prerequisites)
- [4. Environment Setup](#4-environment-setup)
- [5. Secrets Management](#5-secrets-management)
- [6. Pipeline Deployment](#6-pipeline-deployment)
- [7. Workflow Configuration](#7-workflow-configuration)
- [8. Monitoring & Alerting](#8-monitoring--alerting)
- [9. Troubleshooting](#9-troubleshooting)
- [10. Related Documentation](#10-related-documentation)

## 1. Overview

This guide covers the **cloud-agnostic** production deployment of the Platform Observability solution, including HWM job setup, workflow orchestration, secrets management, and operational considerations. The solution is designed to run in any Databricks environment across different cloud providers (AWS, Azure, GCP) without requiring cloud-specific configurations.

## 2. Cloud-Agnostic Features

### **Key Features**
- **Cloud-Agnostic Secrets Management**: Automatically detects and uses the appropriate secrets provider
- **Environment Detection**: Automatically configures based on available cloud services
- **Fallback Mechanisms**: Gracefully falls back to environment variables when cloud services are unavailable
- **No Hardcoded Dependencies**: No Azure Key Vault or other cloud-specific hardcoded references

### **Supported Cloud Providers**

#### **AWS**
- **Secrets Manager**: For storing sensitive configuration
- **Parameter Store**: Alternative for configuration parameters
- **Environment Variables**: Fallback for development

#### **Azure**
- **Key Vault**: For storing secrets and certificates
- **Environment Variables**: Fallback for development

#### **GCP**
- **Secret Manager**: For storing sensitive data
- **Environment Variables**: Fallback for development

#### **Local Development**
- **Environment Variables**: Primary method for local development

### **Deployment Architecture**

```
Production Environment
├── Unity Catalog: platform_observability
├── Schemas: plt_bronze, plt_silver, plt_gold
├── HWM Jobs: Bronze, Silver, Gold (SCD2-aware)
├── Workflow: Daily orchestration
└── Monitoring: Health checks and alerts
```

### 1.2 Deployment Phases

1. **Environment Preparation** - Catalog, schemas, and control tables
2. **Job Deployment** - HWM job creation and configuration
3. **Workflow Setup** - Production scheduling and orchestration
4. **Validation** - Data quality and system health verification
5. **Monitoring** - Operational monitoring and alerting

## 3. Prerequisites

### 3.1 System Requirements

- **Databricks Runtime**: 13.0+ with Unity Catalog enabled
- **Workspace Access**: Admin or Account Admin privileges
- **System Tables**: Access to billing, lakeflow, compute, and access tables
- **Storage**: Sufficient storage for Bronze, Silver, and Gold layers
- **Compute**: HWM job compute resources

### 2.2 Access Requirements

```bash
# Required permissions
- Unity Catalog: CREATE CATALOG, CREATE SCHEMA
- System Tables: SELECT on all required system tables
- Storage: CREATE TABLE, INSERT, UPDATE, DELETE
- Jobs: CREATE JOB, MANAGE JOB, RUN JOB
- Jobs: CREATE JOB, MANAGE JOB
```

### 2.3 Configuration Files

Ensure these files are available:
- `config.py` - Configuration management
- `libs/` - All library modules
- `sql/` - SQL files for table creation
- `notebooks/` - HWM job definitions
- `jobs/` - Workflow configuration

## 4. Environment Setup

### 4.1 Prepare Environment

```bash
# Clone the repository
git clone <repository-url>
cd platform-observability

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp env.example .env
```

### 4.2 Configure Environment

Edit `.env` file with your specific configuration:

```bash
# Basic configuration
ENVIRONMENT=dev
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token

# Optional: Cloud-specific configuration
# AWS
AWS_DEFAULT_REGION=us-west-2

# Azure
AZURE_KEY_VAULT_URL=https://your-vault.vault.azure.net/

# GCP
GOOGLE_CLOUD_PROJECT=your-project-id
```

## 5. Secrets Management

### 5.1 Automatic Detection

The solution automatically detects the best available secrets provider:

1. **Environment Variables** (always available)
2. **AWS Secrets Manager** (if AWS credentials are configured)
3. **Azure Key Vault** (if Azure credentials and vault URL are configured)
4. **GCP Secret Manager** (if GCP credentials and project are configured)

### 5.2 Cloud Secrets Configuration

For production environments, you can store sensitive data in cloud secrets:

#### **AWS Secrets Manager**
```bash
# Store as JSON in AWS Secrets Manager
{
  "DATABRICKS_TOKEN": "your-token",
  "SLACK_WEBHOOK_URL": "your-webhook",
  "TEAMS_WEBHOOK_URL": "your-webhook"
}
```

#### **Azure Key Vault**
```bash
# Set vault URL
export AZURE_KEY_VAULT_URL=https://your-vault.vault.azure.net/

# Store secrets in Key Vault
az keyvault secret set --vault-name your-vault --name "DATABRICKS-TOKEN" --value "your-token"
```

#### **GCP Secret Manager**
```bash
# Set project ID
export GOOGLE_CLOUD_PROJECT=your-project-id

# Create secrets
gcloud secrets create DATABRICKS_TOKEN --data-file=- <<< "your-token"
```

### 5.3 Manual Configuration

You can also manually set environment variables:

```bash
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export ENVIRONMENT="dev"  # or "prod"
```

## 6. Pipeline Deployment

### 6.1 Deploy to Databricks

#### **Option A: Using Databricks CLI**
```bash
# Configure Databricks CLI
databricks configure --token

# Deploy notebooks
databricks workspace import_dir notebooks /Workspace/Repos/platform-observability/notebooks

# Deploy libraries
databricks fs cp -r libs dbfs:/FileStore/platform-observability/libs
```

#### **Option B: Using Databricks UI**
1. Upload notebooks to your workspace
2. Upload library files to DBFS
3. Configure job parameters

### 6.2 Bootstrap Production Environment

```python
from libs.sql_parameterizer import SQLParameterizer

# Initialize parameterizer
parameterizer = SQLParameterizer(spark)

# Bootstrap entire production environment
parameterizer.full_bootstrap()
```

**What this creates:**
- Production catalog and schemas
- Control tables (bookmarks, processing state)
- Bronze, Silver, and Gold layer tables
- Performance optimizations
- Business views

### 3.2 Environment Configuration

Set production environment variables:

```bash
# Production environment configuration
export ENVIRONMENT=prod
export CATALOG=platform_observability_prod
export BRONZE_SCHEMA=plt_bronze_prod
export SILVER_SCHEMA=plt_silver_prod
export GOLD_SCHEMA=plt_gold_prod
export LOG_LEVEL=WARNING
export TIMEZONE=UTC
export OVERLAP_HOURS=72
```

### 3.3 Security Configuration

```python
# Set up access control
spark.sql(f"""
GRANT SELECT ON CATALOG {config.catalog} TO `account users`
GRANT SELECT ON SCHEMA {config.catalog}.{config.bronze_schema} TO `account users`
GRANT SELECT ON SCHEMA {config.catalog}.{config.silver_schema} TO `account users`
GRANT SELECT ON SCHEMA {config.catalog}.{config.gold_schema} TO `account users`
""")
```

## 7. Job Deployment

### 7.1 Bronze HWM Ingest Job

#### 7.1.1 Create Job

1. Go to **Databricks Workspace > Workflows > Jobs**
2. Click **Create Job**
3. Configure job settings:

```
Job Name: Platform_Observability_Bronze_HWM_Ingest
Type: Notebook
Notebook Path: /notebooks/bronze_hwm_ingest_job
Cluster: Auto (or specify cluster)
```

#### 7.1.2 Job Configuration

```json
{
  "name": "Platform Observability - Bronze Ingest",
  "notebook_task": {
    "notebook_path": "/Workspace/Repos/platform-observability/notebooks/bronze_hwm_ingest_job"
  },
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2
  },
  "environment": {
    "ENVIRONMENT": "prod",
    "DATABRICKS_HOST": "{{secrets/your-scope/databricks-host}}",
    "DATABRICKS_TOKEN": "{{secrets/your-scope/databricks-token}}"
  }
}
```

#### 4.1.3 Deploy and Test

```python
# Test bronze HWM ingest job
# This will create all bronze tables with CDF enabled
# Verify data ingestion and processing state tracking
# Configuration-based execution (no job parameters required)
```

### 4.2 Silver HWM Build Job

#### 4.2.1 Create Job

1. Go to **Databricks Workspace > Workflows > Jobs**
2. Click **Create Job**
3. Configure job settings:

```
Job Name: Platform_Observability_Silver_HWM_Build
Type: Notebook
Notebook Path: /notebooks/silver_hwm_build_job
Cluster: Auto (or specify cluster)
```

#### 4.2.2 Job Configuration

```json
{
  "name": "Platform_Observability_Silver_HWM_Build",
  "type": "notebook",
  "notebook_path": "/notebooks/silver_hwm_build_job",
  "timeout_seconds": 3600,
  "retry_on_timeout": true,
  "max_retries": 3,
  "new_cluster": {
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
    "autoscale": {
      "min_workers": 1,
      "max_workers": 4
    },
    "spark_version": "13.3.x-scala2.12"
  }
}
```

#### 4.2.3 Deploy and Test

```python
# Test silver HWM build job
# This will read from bronze via CDF and build silver layer
# Verify SCD2 functionality and business logic
# Configuration-based execution (no job parameters required)
```

### 4.3 Gold HWM Build Job (SCD2-Aware)

#### 4.3.1 Create Job

1. Go to **Databricks Workspace > Workflows > Jobs**
2. Click **Create Job**
3. Configure job settings:

```
Job Name: Platform_Observability_Gold_HWM_Build
Type: Notebook
Notebook Path: /notebooks/gold_hwm_build_job
Cluster: Auto (or specify cluster)
```

#### 4.3.2 Job Configuration

```json
{
  "name": "Platform_Observability_Gold_HWM_Build",
  "type": "notebook",
  "notebook_path": "/notebooks/gold_hwm_build_job",
  "timeout_seconds": 3600,
  "retry_on_timeout": true,
  "max_retries": 3,
  "new_cluster": {
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
    "autoscale": {
      "min_workers": 1,
      "max_workers": 4
    },
    "spark_version": "13.3.x-scala2.12"
  }
}
```

#### 4.3.3 Deploy and Test

```python
# Test gold HWM build job with SCD2 support
# This will build dimensional model from silver layer
# Verify SCD2 dimension tables preserve historical versions
# Verify fact tables align with correct dimension versions
# Configuration-based execution (no job parameters required)
```

## 5. Workflow Configuration

### 5.1 Create Production Workflow

1. Go to **Databricks Workspace > Workflows**
2. Click **Create Workflow**
3. Configure workflow settings:

```
Workflow Name: Platform_Observability_Production
```

### 5.2 Workflow Tasks

#### 5.2.1 Bronze Task

```json
{
  "task_key": "bronze_hwm_ingest",
  "notebook_task": {
    "notebook_path": "/notebooks/bronze_hwm_ingest_job"
  },
  "timeout_seconds": 3600,
  "retry_on_timeout": true,
  "max_retries": 3
}
```

#### 5.2.2 Silver Task

```json
{
  "task_key": "silver_hwm_build",
  "depends_on": [
    {
      "task_key": "bronze_hwm_ingest"
    }
  ],
  "notebook_task": {
    "notebook_path": "/notebooks/silver_hwm_build_job"
  },
  "timeout_seconds": 3600,
  "retry_on_timeout": true,
  "max_retries": 3
}
```

#### 5.2.3 Gold Task

```json
{
  "task_key": "gold_hwm_build",
  "depends_on": [
    {
      "task_key": "silver_hwm_build"
    }
  ],
  "notebook_task": {
    "notebook_path": "/notebooks/gold_hwm_build_job"
  },
  "timeout_seconds": 3600,
  "retry_on_timeout": true,
  "max_retries": 3
}
```

### 5.3 Workflow Schedule

```json
{
  "schedule": {
    "quartz_cron_expression": "0 30 5 ? * *",
    "timezone_id": "UTC",
    "pause_status": "UNPAUSED"
  },
  "max_concurrent_runs": 1,
  "max_clusters": 10
}
```

### 5.4 Workflow Notifications

```json
{
  "notifications": [
    {
      "on_success": ["user@company.com"],
      "on_failure": ["admin@company.com", "oncall@company.com"],
      "on_timeout": ["admin@company.com"]
    }
  ]
}
```

## 6. Monitoring & Alerting

### 6.1 Job Health Monitoring

#### 6.1.1 HWM Job Metrics

Monitor these key metrics:
- **Job Status**: Running, Failed, Completed
- **Processing Time**: Duration of each job run
- **Data Quality**: Records processed, failed expectations
- **Resource Usage**: CPU, memory, storage utilization
- **SCD2 Processing**: Dimension version tracking, temporal alignment

#### 6.1.2 Data Quality Monitoring

```python
# Monitor data quality metrics
from libs.monitoring import monitor_data_quality

# Check bronze layer quality
bronze_quality = monitor_data_quality("bronze")
print(f"Bronze quality score: {bronze_quality}")

# Check silver layer quality
silver_quality = monitor_data_quality("silver")
print(f"Silver quality score: {silver_quality}")

# Check gold layer quality
gold_quality = monitor_data_quality("gold")
print(f"Gold quality score: {gold_quality}")
```

### 6.2 Alerting Configuration

#### 6.2.1 Failure Alerts

```python
# Configure failure alerts
alerts = {
    "job_failure": {
        "condition": "job_status == 'FAILED'",
        "notification": "admin@company.com",
        "severity": "HIGH"
    },
    "data_quality_breach": {
        "condition": "quality_score < 0.95",
        "notification": "data-team@company.com",
        "severity": "MEDIUM"
    },
    "processing_delay": {
        "condition": "processing_time > 3600",
        "notification": "platform-team@company.com",
        "severity": "LOW"
    },
    "scd2_validation_failure": {
        "condition": "scd2_temporal_alignment == 'FAILED'",
        "notification": "data-team@company.com",
        "severity": "MEDIUM"
    }
}
```

#### 6.2.2 Performance Alerts

```python
# Monitor performance metrics
performance_metrics = {
    "bronze_hwm_ingest_time": "target < 1800 seconds",
    "silver_hwm_build_time": "target < 3600 seconds",
    "gold_hwm_build_time": "target < 1800 seconds",
    "total_workflow_time": "target < 7200 seconds",
    "scd2_merge_operations": "target < 300 seconds"
}
```

### 6.3 Dashboard Creation

#### 6.3.1 Operational Dashboard

Create a dashboard with:
- **Job Status**: Current status of all HWM jobs
- **Processing Metrics**: Records processed, processing time
- **Data Quality**: Quality scores and failed expectations
- **Resource Usage**: Cluster utilization and costs
- **Error Rates**: Failure rates and error types
- **SCD2 Metrics**: Dimension version counts, temporal alignment status

#### 6.3.2 Business Dashboard

Create a dashboard with:
- **Cost Analysis**: Daily, weekly, monthly costs
- **Usage Trends**: Resource utilization patterns
- **Performance Metrics**: Job performance and SCD2 processing
- **Compliance Status**: Policy compliance metrics
- **Historical Analysis**: Entity change tracking and temporal trends

## 7. Troubleshooting

### 7.1 Common Issues

#### 7.1.1 Job Failures

**Issue**: HWM job fails to start
**Solution**:
```python
# Check job configuration
from config import Config
config = Config.get_config()
print(f"Environment: {Config.ENV}")
print(f"Gold Complete Refresh: {config.gold_complete_refresh}")
print(f"Gold Processing Strategy: {config.gold_processing_strategy}")

# Verify target schema exists
spark.sql("SHOW SCHEMAS IN platform_observability_prod").show()
```

**Issue**: Job fails during execution
**Solution**:
```python
# Check job logs
job_logs = spark.sql("SELECT * FROM job_logs WHERE job_name = 'job_name'")
print(job_logs.show())

# Verify source table access
spark.sql("SELECT COUNT(*) FROM system.billing.usage").show()

# Check processing state tables
spark.sql("SELECT * FROM _cdf_processing_offsets").show()
```

#### 7.1.2 Data Quality Issues

**Issue**: High failure rate in data quality expectations
**Solution**:
```python
# Review failed expectations
failed_expectations = spark.sql("""
    SELECT * FROM slv_usage_txn 
    WHERE _expectation_failures IS NOT NULL
""")
print(failed_expectations.show())

# Check data source quality
source_quality = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN usage_quantity < 0 THEN 1 END) as negative_quantity,
        COUNT(CASE WHEN usage_end_time < usage_start_time THEN 1 END) as invalid_time
    FROM brz_billing_usage
""")
print(source_quality.show())
```

#### 7.1.3 Performance Issues

**Issue**: Job processing time exceeds SLA
**Solution**:
```python
# Check cluster configuration
cluster_config = spark.sql("DESCRIBE CLUSTER cluster_id")
print(cluster_config.show())

# Monitor resource usage
resource_usage = spark.sql("""
    SELECT 
        cluster_id,
        avg(cpu_usage) as avg_cpu,
        avg(memory_usage) as avg_memory
    FROM cluster_metrics
    GROUP BY cluster_id
""")
print(resource_usage.show())
```

### 7.2 Debug Commands

#### 7.2.1 Configuration Verification

```python
# Verify configuration
from config import Config
config = Config.get_config()
print(f"Environment: {Config.ENV}")
print(f"Catalog: {config.catalog}")
print(f"Bronze Schema: {config.bronze_schema}")

# Check SQL operations
from libs.sql_manager import sql_manager
operations = sql_manager.get_available_operations()
print(f"Available operations: {operations}")
```

#### 7.2.2 Data Verification

```python
# Verify data flow
bronze_count = spark.table(f"{config.catalog}.{config.bronze_schema}.brz_billing_usage").count()
silver_count = spark.table(f"{config.catalog}.{config.silver_schema}.slv_usage_txn").count()
gold_count = spark.table(f"{config.catalog}.{config.gold_schema}.gld_fact_usage_priced_day").count()

print(f"Bronze records: {bronze_count}")
print(f"Silver records: {silver_count}")
print(f"Gold records: {gold_count}")

# Verify SCD2 dimension data
scd2_job_versions = spark.sql(f"""
    SELECT job_id, COUNT(*) as version_count 
    FROM {config.catalog}.{config.gold_schema}.gld_dim_job 
    GROUP BY job_id 
    HAVING COUNT(*) > 1
""")
print(f"SCD2 job versions: {scd2_job_versions.count()}")
```

#### 7.2.3 Job Status

```python
# Check job status
job_status = spark.sql("""
    SELECT 
        job_name,
        status,
        last_updated,
        error_message
    FROM system.jobs
    WHERE job_name LIKE '%Platform_Observability%'
""")
print(job_status.show())

# Check processing state
processing_state = spark.sql("""
    SELECT 
        table_name,
        last_processed_timestamp,
        processing_status
    FROM _cdf_processing_offsets
    ORDER BY last_processed_timestamp DESC
""")
print(processing_state.show())
```

## 8. Related Documentation

- [01-overview.md](01-overview.md) - Solution overview and architecture
- [02-getting-started.md](02-getting-started.md) - Step-by-step deployment guide
- [03-parameterization.md](03-parameterization.md) - Configuration and SQL management
- [04-data-dictionary.md](04-data-dictionary.md) - Complete data model documentation
- [08-scd2-temporal-join-example.md](08-scd2-temporal-join-example.md) - SCD2 implementation guide
- [09-task-based-processing.md](09-task-based-processing.md) - Task-based processing guide
- [10-recent-changes-summary.md](10-recent-changes-summary.md) - Recent changes and migration summary

## Next Steps

1. **Deploy to Production**: Follow the deployment steps above
2. **Configure Monitoring**: Set up dashboards and alerting
3. **Validate Data**: Verify data quality and job health
4. **Validate SCD2**: Check that Gold dimension tables preserve historical versions
5. **Train Users**: Educate teams on using the observability data
6. **Optimize Performance**: Monitor and tune job performance
7. **Scale Operations**: Extend to additional workspaces or regions

## Support

For production issues:
1. Check job logs and error messages
2. Review data quality metrics
3. Consult troubleshooting section
4. Contact platform team for critical issues
5. Use monitoring dashboards for operational insights
6. Validate SCD2 processing and temporal alignment
