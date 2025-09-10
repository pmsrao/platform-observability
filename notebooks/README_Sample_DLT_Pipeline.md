# Simple DLT Pipeline for Platform Observability Testing

This directory contains a simple DLT (Delta Live Tables) pipeline designed to generate billing usage test data for the platform observability system.

## Overview

The simple DLT pipeline generates only billing usage data in the bronze layer to help you test and validate the platform observability system. By running this pipeline daily for a few days, you'll accumulate enough usage data to test the data flow from bronze to gold layers.

## Generated Data

The pipeline generates only one type of data:

### **Usage Data** (`brz_billing_usage`)
- 50 usage records per run with realistic timestamps
- Various SKU types (DBU, Standard_DBUs, Premium_DBUs, Serverless_DBUs)
- Multiple cloud providers (AWS, Azure, GCP)
- Rich metadata including job runs, clusters, and tags
- Realistic usage quantities and durations
- Complete tag information for cost attribution testing

## Usage Instructions

### Option 1: Run as Notebook (Recommended for Testing)

1. **Open the notebook**: Navigate to `/notebooks/sample_dlt_pipeline.py` in your Databricks workspace
2. **Configure the environment**: Ensure your configuration is set up correctly
3. **Run the notebook**: Execute all cells to generate and write usage data
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
- All usage data is generated with timestamps from the last 24 hours
- Realistic usage durations (30-180 minutes)
- Proper start and end time relationships

### Tag Diversity
- Random selection from predefined tag values
- Mix of "UNKNOWN" and specific values to test normalization
- Complete tag coverage for cost attribution testing

### Rich Metadata
- Complete usage_metadata structure with all required fields
- Identity metadata for user tracking
- Product features and billing information
- Realistic SKU and cloud provider distribution

### Volume Scaling
- 50 usage records per run (easily adjustable)
- Realistic usage patterns and frequencies
- Configurable record counts

## Customization

You can customize the generated data by modifying the following parameters in the notebook:

```python
# Adjust data volume
num_records = 50  # Number of usage records per run

# Modify tag values
sku_names = ["DBU", "Standard_DBUs", "Premium_DBUs", "Serverless_DBUs"]
clouds = ["AWS", "Azure", "GCP"]
environments = ["prod", "dev", "stage", "uat"]
node_types = ["i3.xlarge", "i3.2xlarge", "m5.large", "m5.xlarge"]
```

## Data Validation

After running the pipeline, you can validate the generated data:

```sql
-- Check usage data count
SELECT count(*) as usage_records FROM platform_observability.plt_bronze.brz_billing_usage;

-- Check data freshness
SELECT 
  max(_loaded_at) as latest_load_time,
  count(*) as total_records,
  count(DISTINCT workspace_id) as unique_workspaces,
  count(DISTINCT sku_name) as unique_skus
FROM platform_observability.plt_bronze.brz_billing_usage;

-- Check tag distribution
SELECT 
  custom_tags.line_of_business,
  custom_tags.environment,
  count(*) as record_count
FROM platform_observability.plt_bronze.brz_billing_usage
GROUP BY custom_tags.line_of_business, custom_tags.environment
ORDER BY record_count DESC;
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
