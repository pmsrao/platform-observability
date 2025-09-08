# Platform Observability Dashboard - Modular Approach

This directory contains a modular approach to creating and managing Databricks Dashboards with separated concerns for better maintainability and flexibility.

## 📁 File Structure

```
notebooks/
├── generate_dashboard_json.py       # Generator script (Databricks notebook)
├── deploy_dashboard.py              # Deployment instructions and utilities
└── README_Dashboard.md              # This documentation

resources/dashboard/
├── dashboard_template.json          # Dashboard structure with SQL references
├── dashboard_sql_statements.json    # All SQL statements with metadata
├── platform_observability_dashboard.json  # Generated final dashboard (output)
└── dashboard_sql_only.sql           # SQL statements for independent testing
```

## 🎯 Benefits of This Approach

### ✅ **Separation of Concerns**
- **Dashboard Structure**: Defined in `dashboard_template.json`
- **SQL Logic**: Managed in `dashboard_sql_statements.json`
- **Generation Logic**: Handled by `generate_dashboard_json.py`

### ✅ **Independent SQL Validation**
- Test SQL queries before dashboard creation
- Validate syntax and references
- Ensure data availability and accuracy

### ✅ **Reusable Components**
- Same SQL statement can be used in multiple widgets
- Easy to create dashboard variants
- Consistent query logic across widgets

### ✅ **Version Control Friendly**
- Track changes to SQL separately from dashboard structure
- Easy to review and merge changes
- Clear history of modifications

### ✅ **Dynamic Generation**
- Generate different dashboard versions
- A/B testing capabilities
- Environment-specific configurations

## 🚀 Quick Start

### 1. Generate Dashboard JSON

Run the generator notebook to create the final dashboard JSON:

```python
# In Databricks, run the generate_dashboard_json.py notebook
# This will:
# - Validate all SQL statements
# - Check SQL references
# - Generate the final dashboard JSON in resources/dashboard/
# - Create a SQL-only file for testing
```

### 2. Test SQL Statements

Use the generated SQL-only file to test queries independently:

```sql
-- Test individual queries in Databricks SQL
-- File: resources/dashboard/dashboard_sql_only.sql
```

### 3. Deploy Dashboard

Follow the deployment instructions in `deploy_dashboard.py`:

- **Manual Import**: Upload `resources/dashboard/platform_observability_dashboard.json` to Databricks Dashboard UI
- **Programmatic**: Use Databricks REST API (advanced)

## 📊 Dashboard Structure

### Tabs and Personas

| Tab | Target Persona | Key Metrics |
|-----|----------------|-------------|
| **Overview** | All Users | Total cost, active workspaces, cost trends |
| **Finance** | Finance Team | Cost allocation, chargeback, budget tracking |
| **Platform** | Platform Engineers | Runtime analysis, cluster sizing, optimization |
| **Data** | Data Engineers | Tag coverage, usage patterns, data quality |
| **Business** | Business Stakeholders | ROI analysis, department performance |

### Widget Types

- **Tables**: Detailed data with sorting and search
- **Line Charts**: Trend analysis with time series
- **Bar Charts**: Comparative analysis
- **Pie Charts**: Distribution analysis

## 🔧 Customization

### Adding New Widgets

1. **Add SQL Statement**:
   ```json
   // In dashboard_sql_statements.json
   "new_widget_sql": {
     "description": "Description of the new widget",
     "category": "overview|finance|platform|data|business|alert",
     "sql": "SELECT ... FROM ..."
   }
   ```

2. **Add Widget Reference**:
   ```json
   // In dashboard_template.json
   {
     "id": "new_widget",
     "name": "New Widget",
     "type": "table",
     "query": {
       "sqlRef": "new_widget_sql"
     }
   }
   ```

3. **Regenerate Dashboard**:
   ```python
   # Run generate_dashboard_json.py
   ```

### Modifying Existing Widgets

1. **Update SQL**: Modify the SQL in `dashboard_sql_statements.json`
2. **Update Structure**: Modify widget properties in `dashboard_template.json`
3. **Regenerate**: Run the generator script

### Creating Dashboard Variants

1. **Copy Template**: Create a new template file
2. **Modify Structure**: Adjust tabs, widgets, or layout
3. **Use Same SQL**: Reference existing SQL statements
4. **Generate Variant**: Create a new dashboard JSON

## 🧪 Testing and Validation

### SQL Validation

The generator script performs several validations:

- **Syntax Validation**: Basic SQL syntax checking
- **Reference Validation**: Ensures all SQL references exist
- **Balance Checking**: Validates parentheses and quotes

### Data Validation

Before deployment, ensure:

- ✅ Gold layer data is fresh and complete
- ✅ All referenced tables exist
- ✅ Column names and types match queries
- ✅ Data ranges are appropriate

### Performance Testing

- Test queries with production data volumes
- Optimize slow-performing queries
- Consider materialized views for complex aggregations

## 📈 Maintenance

### Regular Updates

1. **Monthly**: Review and update SQL queries for new requirements
2. **Quarterly**: Assess dashboard usage and user feedback
3. **Annually**: Major dashboard redesign and optimization

### Monitoring

Track these metrics:

- **Usage**: Daily active users, widget views, session duration
- **Performance**: Query execution times, refresh success rates
- **Business Impact**: Cost reduction, data quality improvements

### Troubleshooting

Common issues and solutions:

| Issue | Cause | Solution |
|-------|-------|----------|
| Widget not loading | SQL error | Test query independently |
| Access denied | Permission issue | Check table ACLs |
| Slow performance | Large datasets | Optimize queries, add filters |
| Stale data | Refresh issues | Check Gold layer pipeline |

## 🔐 Security and Access Control

### Table Permissions

Ensure dashboard users have appropriate access:

```sql
-- Example: Grant read access to Gold layer tables
GRANT SELECT ON platform_observability.plt_gold.gld_fact_usage_priced_day TO `dashboard_users`;
GRANT SELECT ON platform_observability.plt_gold.gld_dim_cluster TO `dashboard_users`;
```

### Dashboard Access

Configure access by persona:

- **Overview**: All authenticated users
- **Finance**: Finance team members
- **Platform**: Platform engineers and DevOps
- **Data**: Data engineers and analysts
- **Business**: Business stakeholders and executives

## 🚀 Advanced Features

### Dynamic Parameters

Consider adding parameterized queries for:

- Date range selection
- Workspace filtering
- Cost center filtering
- Environment selection

### Real-time Updates

For real-time monitoring:

- Use streaming data sources
- Implement incremental refresh
- Set up alerting mechanisms

### Machine Learning Integration

Future enhancements:

- Cost prediction models
- Anomaly detection
- Optimization recommendations
- Trend forecasting

## 📚 Resources

### Documentation

- [Databricks Dashboard Documentation](https://docs.databricks.com/dashboards/index.html)
- [SQL Widget Configuration](https://docs.databricks.com/dashboards/widgets.html)
- [Dashboard Permissions](https://docs.databricks.com/security/access-control/dashboard-acl.html)

### Best Practices

- Keep SQL queries simple and efficient
- Use appropriate refresh intervals
- Implement proper error handling
- Monitor dashboard performance
- Collect user feedback regularly

## 🤝 Contributing

### Adding New Features

1. Create feature branch
2. Update SQL statements and template
3. Test thoroughly
4. Submit pull request
5. Update documentation

### Reporting Issues

When reporting issues, include:

- Dashboard version
- Error messages
- Steps to reproduce
- Expected vs actual behavior
- Environment details

## 📞 Support

For questions or issues:

1. Check this documentation
2. Review troubleshooting section
3. Test SQL statements independently
4. Contact platform observability team

---

**Last Updated**: 2025-01-08  
**Version**: 1.0  
**Maintainer**: Platform Observability Team
