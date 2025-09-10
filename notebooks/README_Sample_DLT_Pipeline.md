# Sample DLT Pipeline for Platform Observability Testing

This directory contains a sample DLT (Delta Live Tables) pipeline designed to generate realistic test data for the platform observability system.

## Overview

The sample DLT pipeline generates dummy system data across all the bronze layer tables to help you test and validate the platform observability system. By running this pipeline daily for a few days, you'll accumulate enough data to test the entire data flow from bronze to gold layers.

## Generated Data

The pipeline generates the following types of data:

### 1. **Workspace Data** (`brz_access_workspaces_latest`)
- 3 test workspaces with realistic names and URLs
- Account information and creation timestamps
- Active status and metadata

### 2. **Job Data** (`brz_lakeflow_jobs`)
- 5 sample jobs with different types (ETL, ML, Analytics, Streaming, Batch)
- Realistic job names, descriptions, and metadata
- Tag information for cost attribution testing
- Creator and ownership information

### 3. **Pipeline Data** (`brz_lakeflow_pipelines`)
- 3 sample pipelines (DLT, Streaming, Batch)
- Configuration settings and metadata
- Tag information for cost attribution testing

### 4. **Cluster Data** (`brz_compute_clusters`)
- 4 sample clusters with different configurations
- Various node types and scaling settings
- Cloud provider attributes (AWS, Azure, GCP)
- Tag information for cost attribution testing

### 5. **Node Types Data** (`brz_compute_node_types`)
- 8 different node types with CPU, memory, and GPU specifications
- Realistic AWS instance types (i3, m5, r5, g4dn series)

### 6. **Usage Data** (`brz_billing_usage`)
- 200 usage records with realistic timestamps
- Various SKU types and cloud providers
- Rich metadata including job runs, clusters, and tags
- Realistic usage quantities and durations

### 7. **Pricing Data** (`brz_billing_list_prices`)
- Pricing information for all SKU types across cloud providers
- Time-based pricing with start/end dates
- Different pricing tiers (default, promotional, effective)

### 8. **Job Run Data** (`brz_lakeflow_job_run_timeline`)
- 100 job run records with execution details
- Various trigger types and result states
- Realistic execution times and parameters

### 9. **Task Run Data** (`brz_lakeflow_job_task_run_timeline`)
- 200 task run records with detailed execution information
- Parent-child relationships with job runs
- Task-specific metadata and results

## Usage Instructions

### Option 1: Run as Notebook (Recommended for Testing)

1. **Open the notebook**: Navigate to `/notebooks/sample_dlt_pipeline.py` in your Databricks workspace
2. **Configure the environment**: Ensure your configuration is set up correctly
3. **Run the notebook**: Execute all cells to generate and write sample data
4. **Repeat daily**: Run this notebook once per day for 3-5 days to accumulate data

### Option 2: Run as DLT Pipeline

1. **Create a DLT pipeline**:
   - Go to Databricks Workflows → Delta Live Tables
   - Click "Create Pipeline"
   - Use the configuration from `pipeline_configs/sample_dlt_pipeline.json`

2. **Configure the pipeline**:
   - Update the `storage_location` in the JSON config
   - Adjust cluster settings as needed
   - Set the target catalog and schema

3. **Run the pipeline**:
   - Start the pipeline manually or schedule it
   - Monitor the execution in the DLT UI

## Data Characteristics

### Realistic Timestamps
- All data is generated with timestamps from the last 24 hours
- Job runs and task runs have realistic execution durations
- Usage data spans different time periods

### Tag Diversity
- Random selection from predefined tag values
- Mix of "UNKNOWN" and specific values to test normalization
- Consistent tagging across related entities

### Data Relationships
- Jobs and pipelines are associated with specific workspaces
- Usage records reference actual job IDs and cluster IDs
- Job runs and task runs maintain proper parent-child relationships

### Volume Scaling
- Adjustable record counts for different data types
- Configurable number of workspaces, jobs, and clusters
- Realistic usage patterns and frequencies

## Customization

You can customize the generated data by modifying the following parameters in the notebook:

```python
# Adjust data volumes
num_workspaces = 3
num_jobs = 5
num_pipelines = 3
num_clusters = 4
num_usage_records = 200
num_job_runs = 100
num_task_runs = 200

# Modify tag values
job_types = ["ETL", "ML", "Analytics", "Streaming", "Batch"]
environments = ["prod", "dev", "stage", "uat"]
node_types = ["i3.xlarge", "i3.2xlarge", "m5.large", "m5.xlarge"]
```

## Data Validation

After running the pipeline, you can validate the generated data:

```sql
-- Check data counts
SELECT 'workspaces' as table_name, count(*) as record_count FROM platform_observability.plt_bronze.brz_access_workspaces_latest
UNION ALL
SELECT 'jobs', count(*) FROM platform_observability.plt_bronze.brz_lakeflow_jobs
UNION ALL
SELECT 'pipelines', count(*) FROM platform_observability.plt_bronze.brz_lakeflow_pipelines
UNION ALL
SELECT 'clusters', count(*) FROM platform_observability.plt_bronze.brz_compute_clusters
UNION ALL
SELECT 'usage', count(*) FROM platform_observability.plt_bronze.brz_billing_usage;

-- Check data freshness
SELECT 
  table_name,
  max(_loaded_at) as latest_load_time,
  count(*) as total_records
FROM (
  SELECT 'workspaces' as table_name, _loaded_at FROM platform_observability.plt_bronze.brz_access_workspaces_latest
  UNION ALL
  SELECT 'jobs', _loaded_at FROM platform_observability.plt_bronze.brz_lakeflow_jobs
  UNION ALL
  SELECT 'usage', _loaded_at FROM platform_observability.plt_bronze.brz_billing_usage
) t
GROUP BY table_name;
```

## Next Steps

After generating sample data:

1. **Run Bronze HWM Ingest Job**: Process the raw data into bronze tables
2. **Run Silver HWM Build Job**: Transform data with business logic and SCD2
3. **Run Gold HWM Build Job**: Create dimensions and facts for analytics
4. **Generate Dashboards**: Use the gold layer data for reporting and analysis

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure your user has write access to the target catalog and schema
2. **Schema Mismatches**: Verify that the bronze table schemas match the generated data
3. **Memory Issues**: Reduce the number of records if you encounter memory problems
4. **Configuration Errors**: Check that your `config.py` is properly set up

### Debugging

Enable debug logging by adding this to the notebook:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Support

For issues or questions about the sample DLT pipeline, refer to the main platform observability documentation or contact the development team.
