# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard Deployment Script
# MAGIC 
# MAGIC This notebook provides instructions and utilities for deploying the generated dashboard JSON to Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Deployment Instructions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: Manual Import (Recommended)
# MAGIC 
# MAGIC 1. **Navigate to Databricks Dashboards**:
# MAGIC    - Go to your Databricks workspace
# MAGIC    - Click on "Dashboards" in the left sidebar
# MAGIC 
# MAGIC 2. **Import Dashboard**:
# MAGIC    - Click "Create Dashboard"
# MAGIC    - Select "Import from JSON" option
# MAGIC    - Upload the generated `platform_observability_dashboard.json` file
# MAGIC 
# MAGIC 3. **Configure Dashboard**:
# MAGIC    - Review and adjust widget positions if needed
# MAGIC    - Set refresh intervals for each widget
# MAGIC    - Configure access permissions
# MAGIC 
# MAGIC 4. **Test Dashboard**:
# MAGIC    - Verify all widgets load correctly
# MAGIC    - Check data accuracy and freshness
# MAGIC    - Test different user access levels

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Programmatic Deployment (Advanced)

# COMMAND ----------

# MAGIC %md
# MAGIC If you need to deploy programmatically, you can use the Databricks REST API:

# COMMAND ----------

import requests
import json
import os

def deploy_dashboard_via_api(dashboard_json_path: str, workspace_url: str, access_token: str):
    """
    Deploy dashboard using Databricks REST API
    Note: This requires appropriate API permissions and may need adjustment based on your Databricks setup
    """
    
    # Load dashboard JSON
    with open(dashboard_json_path, 'r') as f:
        dashboard_data = json.load(f)
    
    # API endpoint for dashboard creation
    api_url = f"{workspace_url}/api/2.0/sql/dashboards"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Prepare payload
    payload = {
        "name": dashboard_data["name"],
        "tags": dashboard_data.get("tags", []),
        "is_favorite": False,
        "is_archived": False
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=payload)
        response.raise_for_status()
        
        dashboard_id = response.json()["id"]
        print(f"✅ Dashboard created with ID: {dashboard_id}")
        
        # Add widgets (this would require additional API calls)
        # Note: The exact API for adding widgets may vary
        print("⚠️ Note: Widget creation requires additional API calls")
        print("💡 Consider using the manual import method for full functionality")
        
        return dashboard_id
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API deployment failed: {e}")
        return None

# Example usage (commented out - requires actual credentials)
# dashboard_id = deploy_dashboard_via_api(
#     dashboard_json_path="/Workspace/Users/podilapalls@gmail.com/platform-observability/resources/dashboard/platform_observability_dashboard.json",
#     workspace_url="https://your-workspace.cloud.databricks.com",
#     access_token="your-access-token"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Deployment Checklist

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Data Layer Validation
# MAGIC 
# MAGIC Before deploying the dashboard, ensure:
# MAGIC 
# MAGIC 1. **Gold Layer is Built**:
# MAGIC    - Run the Gold Layer notebook to populate fact and dimension tables
# MAGIC    - Verify all required tables exist: `gld_fact_usage_priced_day`, `gld_dim_cluster`, etc.
# MAGIC 
# MAGIC 2. **Data Freshness**:
# MAGIC    - Check that data is up-to-date in the Gold layer
# MAGIC    - Verify date ranges in queries match your data availability
# MAGIC 
# MAGIC 3. **Schema Validation**:
# MAGIC    - Ensure all referenced columns exist in the tables
# MAGIC    - Verify data types match query expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ SQL Query Testing
# MAGIC 
# MAGIC Test all SQL queries independently:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test a sample query to verify data availability
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(DISTINCT workspace_id) as unique_workspaces,
# MAGIC   MIN(date_key) as earliest_date,
# MAGIC   MAX(date_key) as latest_date
# MAGIC FROM platform_observability.plt_gold.gld_fact_usage_priced_day;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Access Control Setup
# MAGIC 
# MAGIC Configure access permissions for different personas:

# COMMAND ----------

# MAGIC %md
# MAGIC | Persona | Tab Access | Permissions |
# MAGIC |---------|------------|-------------|
# MAGIC | **All Users** | Overview | Read-only |
# MAGIC | **Finance Team** | Overview, Finance | Read-only |
# MAGIC | **Platform Engineers** | Overview, Platform | Read-only |
# MAGIC | **Data Engineers** | Overview, Data | Read-only |
# MAGIC | **Business Stakeholders** | Overview, Business | Read-only |
# MAGIC | **Platform Admins** | All Tabs | Full access |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Deployment Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 Refresh Schedules
# MAGIC 
# MAGIC Configure appropriate refresh intervals:

# COMMAND ----------

# MAGIC %md
# MAGIC | Tab | Recommended Refresh | Reason |
# MAGIC |-----|-------------------|--------|
# MAGIC | **Overview** | 1 hour | Real-time monitoring |
# MAGIC | **Finance** | 4 hours | Daily reporting |
# MAGIC | **Platform** | 2 hours | Infrastructure monitoring |
# MAGIC | **Data** | 6 hours | Data quality monitoring |
# MAGIC | **Business** | 8 hours | Business reporting |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚨 Alert Configuration
# MAGIC 
# MAGIC Set up alerts for key metrics:

# COMMAND ----------

# MAGIC %md
# MAGIC | Alert | Threshold | Action |
# MAGIC |-------|-----------|--------|
# MAGIC | **High Unallocated Cost** | > 20% of total | Email + Slack |
# MAGIC | **Legacy Runtime Usage** | Any clusters | Email |
# MAGIC | **Low Tag Coverage** | < 80% | Email + Slack |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues and Solutions

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Widget Not Loading
# MAGIC - **Cause**: SQL query error or missing data
# MAGIC - **Solution**: Test the SQL query independently in Databricks SQL
# MAGIC - **Check**: Table permissions and data availability

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Access Denied
# MAGIC - **Cause**: Insufficient table permissions
# MAGIC - **Solution**: Grant appropriate permissions to dashboard users
# MAGIC - **Check**: Table ACLs and workspace access

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Slow Performance
# MAGIC - **Cause**: Large datasets or inefficient queries
# MAGIC - **Solution**: Optimize SQL queries, add filters, or use materialized views
# MAGIC - **Check**: Query execution plans and data volumes

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Stale Data
# MAGIC - **Cause**: Gold layer not updated or refresh schedule issues
# MAGIC - **Solution**: Check Gold layer job status and refresh schedules
# MAGIC - **Check**: Data pipeline execution and timestamps

# COMMAND ----------

# MAGIC %md
# MAGIC ## Success Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC Track these metrics to measure dashboard success:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Usage Metrics
# MAGIC - **Daily Active Users**: Number of unique users accessing the dashboard daily
# MAGIC - **Widget Views**: Most frequently accessed widgets and tabs
# MAGIC - **Session Duration**: Average time users spend on the dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💰 Business Impact
# MAGIC - **Cost Reduction**: Decrease in unallocated costs
# MAGIC - **Data Quality**: Improvement in tag coverage percentages
# MAGIC - **Runtime Modernization**: Reduction in legacy runtime usage

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 User Satisfaction
# MAGIC - **Feedback Scores**: User ratings and feedback
# MAGIC - **Support Tickets**: Reduction in data-related support requests
# MAGIC - **Adoption Rate**: Percentage of target users actively using the dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps

# COMMAND ----------

# MAGIC %md
# MAGIC After successful deployment:

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **User Training**: Conduct training sessions for each persona group
# MAGIC 2. **Documentation**: Create user guides and best practices
# MAGIC 3. **Feedback Collection**: Set up regular feedback collection mechanisms
# MAGIC 4. **Continuous Improvement**: Plan quarterly dashboard reviews and updates
# MAGIC 5. **Advanced Features**: Consider adding machine learning insights and predictions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("🎉 Dashboard Deployment Ready!")
print("=" * 50)

print("📁 Files Available:")
print("   • platform_observability_dashboard.json - Ready for import")
print("   • dashboard_sql_only.sql - For independent SQL testing")

print("\n🚀 Deployment Options:")
print("   1. Manual Import (Recommended)")
print("   2. Programmatic API (Advanced)")

print("\n✅ Pre-Deployment Checklist:")
print("   • Gold layer data is fresh and complete")
print("   • SQL queries tested independently")
print("   • Access permissions configured")
print("   • Refresh schedules planned")

print("\n🎯 Success Metrics:")
print("   • User adoption and engagement")
print("   • Business impact (cost reduction, data quality)")
print("   • User satisfaction and feedback")

print("\n💡 Ready to deploy your comprehensive Platform Observability Dashboard!")
