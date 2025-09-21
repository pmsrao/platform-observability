# Dashboard V2 - Enhanced Platform Observability Dashboard

## Overview

Dashboard V2 is an enhanced version of the Platform Observability dashboard that leverages the new gold views with additional attributes:
- **billing_origin_product** - Workload type (JOBS, DLT, SQL, etc.)
- **usage_unit** - DBU vs Network Egress costs
- **sku_name** - Compute type visibility
- **job_pipeline_id** - Actual job/pipeline names instead of generic entity IDs

## File Structure

```
resources/dashboard_v2/
├── dbv2_sql_queries.json      # Enhanced SQL queries with new attributes
├── dbv2_template.json         # Dashboard template with widget layouts
├── dbv2_gen.py               # Generator script
├── dbv2_out.lvdash.json      # Generated dashboard (output)
└── README.md                 # This documentation
```

## Features

### Enhanced Widgets

1. **Cost Summary KPIs** - Days analyzed, total cost, workload types, compute types
2. **Daily Cost Trend by Workload** - Line chart showing JOBS, DLT, SQL, etc.
3. **Cost Breakdown by Usage Unit** - DBU vs Network Egress costs
4. **Workload Type Distribution** - Pie chart of workload types
5. **Top Compute SKUs** - Most expensive compute types
6. **Top Jobs/Pipelines** - Highest cost jobs and pipelines
7. **Cost Anomalies** - Enhanced anomaly detection with context

### Common Filters

- **Date Range Filter** - Start and end date pickers (default: last 30 days)
- **Workspace Filter** - Single select dropdown (default: "All Workspaces")

## Usage

### 1. Generate Dashboard

```bash
cd resources/dashboard_v2
python dbv2_gen.py
```

### 2. Import to Databricks

1. Copy the generated `dbv2_out.lvdash.json` file
2. In Databricks, go to Dashboards → Import Dashboard
3. Upload the JSON file
4. The dashboard should import without errors

## Technical Implementation

### LakeFlow Pattern
- **Separate Filter Widgets** - Date range and workspace filters
- **Parameter Queries** - Each filter widget has parameter queries
- **Proper Parameter Binding** - Uses `:param_start_date`, `:param_end_date`, `:param_workspace`

### Widget Versions
- **Version 1** - Table widgets
- **Version 2** - Filter widgets
- **Version 3** - Chart widgets (line, bar, pie)

### SQL Enhancements
- **Enhanced Views** - Uses `v_cost_trends` and `v_cost_anomalies`
- **New Attributes** - Leverages billing_origin_product, usage_unit, sku_name, job_pipeline_id
- **Proper Filtering** - Date range and workspace filtering
- **Current Dates** - Defaults to last 30 days

## Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  FILTERS (y=0)                                              │
│  Start Date | End Date | Workspace                          │
├─────────────────────────────────────────────────────────────┤
│  Cost Summary KPIs (Table) - y=1                           │
├─────────────────────────────────────────────────────────────┤
│  Daily Cost Trend by Workload (Line Chart) - y=2           │
├─────────────────────────────────────────────────────────────┤
│  Cost by Usage Unit (Bar Chart) - y=3 | Workload Dist (Pie) - y=3 │
├─────────────────────────────────────────────────────────────┤
│  Top Compute SKUs (Table) - y=4                            │
├─────────────────────────────────────────────────────────────┤
│  Top Jobs/Pipelines (Table) - y=5                          │
├─────────────────────────────────────────────────────────────┤
│  Cost Anomalies (Table) - y=6                              │
└─────────────────────────────────────────────────────────────┘
```

## Key Insights Provided

### Workload Analysis
- **Workload Type Distribution** - See which workloads (JOBS, DLT, SQL) consume most resources
- **Usage Unit Breakdown** - Distinguish between DBU costs and Network Egress costs
- **Compute SKU Analysis** - Identify which compute types are most expensive

### Job/Pipeline Context
- **Actual Names** - See real job and pipeline names instead of generic entity IDs
- **Cost Attribution** - Understand which specific jobs/pipelines drive costs
- **Activity Patterns** - See how many days each job/pipeline was active

### Enhanced Anomaly Detection
- **Contextual Anomalies** - Anomalies now include workload type, compute type, and job context
- **Better Classification** - Distinguish between cost spikes and unusual low costs
- **Actionable Insights** - Know exactly which workload caused the anomaly

## Customization

### Adding New Widgets
1. Add SQL query to `dbv2_sql_queries.json`
2. Add widget specification to `dbv2_template.json`
3. Run `python dbv2_gen.py`

### Modifying Existing Queries
1. Update SQL in `dbv2_sql_queries.json`
2. Run `python dbv2_gen.py`

### Adding New Filters
1. Add parameter to datasets in `dbv2_sql_queries.json`
2. Add filter widget to template
3. Update generator script if needed

## Validation

The generator script includes comprehensive validation:
- ✅ JSON structure validation
- ✅ Required fields checking
- ✅ Widget connection validation
- ✅ Parameter binding verification
- ✅ File size and statistics reporting

## Statistics

Generated dashboard includes:
- **8 datasets** with enhanced SQL queries
- **1 page** with comprehensive layout
- **10 widgets** (3 filters + 7 data widgets)
- **33.77 KB** file size
- **Current dates** as defaults

## Troubleshooting

### Common Issues
1. **Import Errors** - Check JSON syntax and widget versions
2. **Filter Issues** - Verify parameter binding and LakeFlow pattern
3. **Data Issues** - Ensure gold views are populated and accessible

### Validation
- Run `python dbv2_gen.py` to validate structure
- Check console output for any errors
- Verify file size and statistics

## Future Enhancements

### Planned Features
- **Multiple Pages** - Add dedicated pages for different analysis types
- **Drill-down Capabilities** - Click-through from summary to detailed views
- **Export Functionality** - Export data and charts
- **Alerting Integration** - Connect with anomaly detection systems

### Additional Widgets
- **Cost Forecasting** - Predict future costs based on trends
- **Resource Utilization** - Show compute efficiency metrics
- **Cost Optimization** - Suggest optimization opportunities
- **Compliance Views** - Show policy compliance and governance metrics

---

**Generated**: September 2025  
**Version**: 2.0  
**Author**: Platform Observability Team
