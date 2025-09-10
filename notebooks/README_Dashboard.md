# Platform Observability Dashboard - Updated System

## Overview

This updated dashboard system incorporates all learnings from multiple iterations of development and testing. It provides a clean, modular approach to generating Databricks dashboards that import without errors.

## Key Learnings Applied

### 1. SQL Query Formatting
- **Trailing Spaces**: All SQL keywords must have trailing spaces (`FROM `, `WHERE `, `GROUP BY `, `ORDER BY `, `UNION ALL `)
- **Date Formatting**: Use `TO_DATE(CAST(date_key AS STRING), 'yyyyMMdd')` for proper date conversion
- **Division Operations**: Use `try_divide()` instead of `/` to prevent division by zero errors
- **Column References**: Use correct column names (`entity_key`, `workspace_key`, `date_key`)

### 2. Widget Specifications
- **Table Widgets**: Use `version: 1` and remove unsupported properties
- **Chart Widgets**: Use `version: 3` for line and bar charts
- **Column Encodings**: Only include essential properties (remove `numberFormat`, `allowSearch`, `alignContent`, etc.)
- **Order Values**: Use simple integers (0, 1, 2, etc.) instead of complex values

### 3. Filter Widgets
- **Date Picker**: Use `filter-date-picker` with proper parameter binding
- **Single Select**: Use `filter-single-select` with field binding and default selections

## File Structure

```
resources/dashboard/
├── dashboard_sql_queries.json          # All working SQL queries
├── dashboard_template_updated.json     # Clean dashboard template
├── platform_observability_dashboard_generated.lvdash.json  # Generated dashboard
└── platform_observability_dashboard.lvdash.json           # Current working dashboard

notebooks/
├── generate_dashboard_json_updated.py  # Updated generator script
└── README_Dashboard_Updated.md         # This documentation
```

## Usage

### 1. Generate Dashboard
```bash
cd notebooks
python generate_dashboard_json_updated.py
```

### 2. Import to Databricks
1. Copy the generated `platform_observability_dashboard_generated.lvdash.json` file
2. In Databricks, go to Dashboards → Import Dashboard
3. Upload the JSON file
4. The dashboard should import without errors

## Dashboard Structure

### Overview Tab
- **Date Range Filter**: Filter by date range
- **Workspace Filter**: Filter by workspace (defaults to "All")
- **Total Cost Summary**: KPI metrics table
- **Cost Trend**: Line chart showing daily costs
- **Top Cost Centers**: Table with cost center analysis

### Finance Tab
- **Monthly Cost Breakdown**: Detailed monthly cost analysis by cost center and line of business

### Operations Tab
- **Runtime Health**: Bar chart showing cluster distribution by DBR version
- **Job Performance Analysis**: Table with job execution metrics and costs

## SQL Queries Included

1. **Total Cost Summary**: 30-day cost metrics with KPIs
2. **Cost Trend**: Daily cost trends with 7-day rolling average
3. **Top Cost Centers**: Cost center analysis with entity counts
4. **Runtime Health**: DBR version distribution and cluster analysis
5. **Monthly Cost Breakdown**: 3-month cost trends by cost center
6. **Job Performance Analysis**: Job execution metrics and costs
7. **Filter Parameters**: Workspace list for filtering

## Key Features

### ✅ Error-Free Import
- All SQL syntax issues resolved
- Proper widget specifications
- Clean column encodings
- Correct data type handling

### ✅ Comprehensive Filtering
- Date range filtering
- Workspace selection
- Default values for better UX

### ✅ Multi-Tab Structure
- Overview: High-level metrics and trends
- Finance: Detailed cost analysis
- Operations: Runtime health and job performance

### ✅ Proper Data Sources
- Uses correct dimension tables (`gld_dim_entity` instead of `gld_dim_job`)
- Proper column references (`entity_key`, `workspace_key`, `date_key`)
- Efficient queries with appropriate joins

## Customization

### Adding New Queries
1. Add the SQL query to `dashboard_sql_queries.json`
2. Add the dataset to the template in `dashboard_template_updated.json`
3. Create the widget specification
4. Run the generator script

### Modifying Existing Queries
1. Update the SQL in `dashboard_sql_queries.json`
2. Ensure proper spacing and formatting
3. Run the generator script

### Adding New Widgets
1. Add the widget specification to the appropriate page in the template
2. Ensure proper positioning and sizing
3. Use correct widget versions and encodings
4. Run the generator script

## Troubleshooting

### Common Issues
1. **SQL Syntax Errors**: Check for missing trailing spaces after keywords
2. **Column Not Found**: Verify column names match the actual table schema
3. **Widget Import Errors**: Ensure widget specifications match Databricks requirements
4. **Date Format Errors**: Use proper date conversion functions

### Validation
The generator script includes validation to ensure:
- Proper JSON structure
- Required fields are present
- Datasets and pages are correctly formatted

## Best Practices

1. **Always test queries** in Databricks SQL editor before adding to dashboard
2. **Use proper spacing** in SQL queries to avoid parsing errors
3. **Keep widget specifications simple** - only include supported properties
4. **Use meaningful field names** and display names
5. **Test dashboard import** after any changes
6. **Document any customizations** for future reference

## Support

For issues or questions:
1. Check the SQL queries in Databricks SQL editor
2. Validate JSON syntax
3. Review widget specifications against Databricks documentation
4. Test with minimal dashboard first, then add complexity

This updated system provides a solid foundation for creating and maintaining Databricks dashboards with minimal errors and maximum functionality.
