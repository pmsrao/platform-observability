# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Query Notebook
# MAGIC 
# MAGIC This notebook contains sample queries for all available system tables in Databricks.
# MAGIC Each section provides basic queries to explore the data structure and content.
# MAGIC 
# MAGIC ## Available System Tables:
# MAGIC - `system.billing.usage` - Usage and billing data
# MAGIC - `system.billing.list_prices` - Pricing information
# MAGIC - `system.lakeflow.job_run_timeline` - Job run timeline data
# MAGIC - `system.lakeflow.job_task_run_timeline` - Job task run timeline data
# MAGIC - `system.lakeflow.jobs` - Job definitions and metadata
# MAGIC - `system.lakeflow.pipelines` - Pipeline definitions and metadata
# MAGIC - `system.access.workspaces_latest` - Workspace information
# MAGIC - `system.compute.clusters` - Cluster configurations and metadata
# MAGIC - `system.compute.node_types` - Node type specifications

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. System Billing Usage
# MAGIC 
# MAGIC Query usage and billing data with custom tags and metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Basic usage data with recent records
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   workspace_id,
# MAGIC   sku_name,
# MAGIC   cloud,
# MAGIC   usage_start_time,
# MAGIC   usage_end_time,
# MAGIC   usage_date,
# MAGIC   usage_unit,
# MAGIC   usage_quantity,
# MAGIC   custom_tags,
# MAGIC   usage_metadata.cluster_id,
# MAGIC   usage_metadata.job_id,
# MAGIC   usage_metadata.job_name,
# MAGIC   usage_metadata.notebook_path,
# MAGIC   identity_metadata.run_as,
# MAGIC   identity_metadata.owned_by,
# MAGIC   record_type,
# MAGIC   billing_origin_product,
# MAGIC   usage_type
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY usage_start_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Usage summary by SKU and workspace
# MAGIC SELECT 
# MAGIC   workspace_id,
# MAGIC   sku_name,
# MAGIC   cloud,
# MAGIC   COUNT(*) as record_count,
# MAGIC   SUM(usage_quantity) as total_usage,
# MAGIC   MIN(usage_start_time) as earliest_usage,
# MAGIC   MAX(usage_start_time) as latest_usage
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY workspace_id, sku_name, cloud
# MAGIC ORDER BY total_usage DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. System Billing List Prices
# MAGIC 
# MAGIC Query pricing information for different SKUs and regions.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current pricing information
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   sku_name,
# MAGIC   cloud,
# MAGIC   currency_code,
# MAGIC   usage_unit,
# MAGIC   pricing.default as default_price,
# MAGIC   pricing.promotional.default as promotional_price,
# MAGIC   pricing.effective_list.default as effective_list_price,
# MAGIC   price_start_time,
# MAGIC   price_end_time
# MAGIC FROM system.billing.list_prices
# MAGIC WHERE price_start_time <= current_timestamp()
# MAGIC   AND (price_end_time IS NULL OR price_end_time > current_timestamp())
# MAGIC ORDER BY sku_name, cloud;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Price history for specific SKUs
# MAGIC SELECT 
# MAGIC   sku_name,
# MAGIC   cloud,
# MAGIC   pricing.default as price,
# MAGIC   price_start_time,
# MAGIC   price_end_time
# MAGIC FROM system.billing.list_prices
# MAGIC WHERE sku_name LIKE '%DBU%'
# MAGIC ORDER BY sku_name, price_start_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. LakeFlow Job Run Timeline
# MAGIC 
# MAGIC Query job run execution timeline and status information.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent job runs with status
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   workspace_id,
# MAGIC   job_id,
# MAGIC   run_id,
# MAGIC   run_name,
# MAGIC   period_start_time,
# MAGIC   period_end_time,
# MAGIC   trigger_type,
# MAGIC   run_type,
# MAGIC   compute_ids,
# MAGIC   result_state,
# MAGIC   termination_code,
# MAGIC   job_parameters
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_start_time >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC ORDER BY period_start_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Job run summary by result state
# MAGIC SELECT 
# MAGIC   workspace_id,
# MAGIC   result_state,
# MAGIC   trigger_type,
# MAGIC   COUNT(*) as run_count,
# MAGIC   AVG(UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) as avg_duration_seconds
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY workspace_id, result_state, trigger_type
# MAGIC ORDER BY run_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. LakeFlow Job Task Run Timeline
# MAGIC 
# MAGIC Query individual task execution within job runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Task run details for recent job runs
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   workspace_id,
# MAGIC   job_id,
# MAGIC   run_id,
# MAGIC   job_run_id,
# MAGIC   parent_run_id,
# MAGIC   task_key,
# MAGIC   period_start_time,
# MAGIC   period_end_time,
# MAGIC   compute_ids,
# MAGIC   result_state,
# MAGIC   termination_code
# MAGIC FROM system.lakeflow.job_task_run_timeline
# MAGIC WHERE period_start_time >= current_timestamp() - INTERVAL 3 DAYS
# MAGIC ORDER BY period_start_time DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Task performance analysis
# MAGIC SELECT 
# MAGIC   workspace_id,
# MAGIC   task_key,
# MAGIC   result_state,
# MAGIC   COUNT(*) as task_count,
# MAGIC   AVG(UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) as avg_duration_seconds,
# MAGIC   MIN(UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) as min_duration_seconds,
# MAGIC   MAX(UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) as max_duration_seconds
# MAGIC FROM system.lakeflow.job_task_run_timeline
# MAGIC WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY workspace_id, task_key, result_state
# MAGIC ORDER BY task_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. LakeFlow Jobs
# MAGIC 
# MAGIC Query job definitions, metadata, and configuration.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Active jobs with metadata
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   workspace_id,
# MAGIC   job_id,
# MAGIC   name,
# MAGIC   description,
# MAGIC   creator_id,
# MAGIC   tags,
# MAGIC   change_time,
# MAGIC   run_as
# MAGIC FROM system.lakeflow.jobs
# MAGIC WHERE delete_time IS NULL
# MAGIC ORDER BY change_time DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Job count by workspace and creator
# MAGIC SELECT 
# MAGIC   workspace_id,
# MAGIC   creator_id,
# MAGIC   COUNT(*) as job_count,
# MAGIC   COUNT(CASE WHEN delete_time IS NULL THEN 1 END) as active_jobs,
# MAGIC   COUNT(CASE WHEN delete_time IS NOT NULL THEN 1 END) as deleted_jobs
# MAGIC FROM system.lakeflow.jobs
# MAGIC GROUP BY workspace_id, creator_id
# MAGIC ORDER BY job_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. LakeFlow Pipelines
# MAGIC 
# MAGIC Query pipeline definitions, settings, and metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Active pipelines with settings
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   workspace_id,
# MAGIC   pipeline_id,
# MAGIC   pipeline_type,
# MAGIC   name,
# MAGIC   created_by,
# MAGIC   run_as,
# MAGIC   tags,
# MAGIC   settings.photon,
# MAGIC   settings.development,
# MAGIC   settings.continuous,
# MAGIC   settings.serverless,
# MAGIC   settings.edition,
# MAGIC   settings.channel,
# MAGIC   change_time
# MAGIC FROM system.lakeflow.pipelines
# MAGIC WHERE delete_time IS NULL
# MAGIC ORDER BY change_time DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pipeline settings analysis
# MAGIC SELECT 
# MAGIC   workspace_id,
# MAGIC   pipeline_type,
# MAGIC   settings.edition,
# MAGIC   settings.channel,
# MAGIC   COUNT(*) as pipeline_count,
# MAGIC   COUNT(CASE WHEN settings.photon = true THEN 1 END) as photon_enabled,
# MAGIC   COUNT(CASE WHEN settings.serverless = true THEN 1 END) as serverless_enabled,
# MAGIC   COUNT(CASE WHEN settings.continuous = true THEN 1 END) as continuous_pipelines
# MAGIC FROM system.lakeflow.pipelines
# MAGIC WHERE delete_time IS NULL
# MAGIC GROUP BY workspace_id, pipeline_type, settings.edition, settings.channel
# MAGIC ORDER BY pipeline_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. System Access Workspaces Latest
# MAGIC 
# MAGIC Query workspace information and status.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All workspaces with status
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   workspace_id,
# MAGIC   workspace_name,
# MAGIC   workspace_url,
# MAGIC   create_time,
# MAGIC   status
# MAGIC FROM system.access.workspaces_latest
# MAGIC ORDER BY create_time DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workspace status summary
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   status,
# MAGIC   COUNT(*) as workspace_count,
# MAGIC   MIN(create_time) as earliest_workspace,
# MAGIC   MAX(create_time) as latest_workspace
# MAGIC FROM system.access.workspaces_latest
# MAGIC GROUP BY account_id, status
# MAGIC ORDER BY workspace_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. System Compute Clusters
# MAGIC 
# MAGIC Query cluster configurations, node types, and metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Active clusters with configuration details
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   workspace_id,
# MAGIC   cluster_id,
# MAGIC   cluster_name,
# MAGIC   owned_by,
# MAGIC   create_time,
# MAGIC   driver_node_type,
# MAGIC   worker_node_type,
# MAGIC   worker_count,
# MAGIC   min_autoscale_workers,
# MAGIC   max_autoscale_workers,
# MAGIC   auto_termination_minutes,
# MAGIC   enable_elastic_disk,
# MAGIC   tags,
# MAGIC   cluster_source,
# MAGIC   dbr_version,
# MAGIC   data_security_mode
# MAGIC FROM system.compute.clusters
# MAGIC WHERE delete_time IS NULL
# MAGIC ORDER BY create_time DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cluster configuration analysis
# MAGIC SELECT 
# MAGIC   workspace_id,
# MAGIC   dbr_version,
# MAGIC   data_security_mode,
# MAGIC   cluster_source,
# MAGIC   COUNT(*) as cluster_count,
# MAGIC   AVG(worker_count) as avg_worker_count,
# MAGIC   AVG(min_autoscale_workers) as avg_min_workers,
# MAGIC   AVG(max_autoscale_workers) as avg_max_workers,
# MAGIC   COUNT(CASE WHEN enable_elastic_disk = true THEN 1 END) as elastic_disk_enabled
# MAGIC FROM system.compute.clusters
# MAGIC WHERE delete_time IS NULL
# MAGIC GROUP BY workspace_id, dbr_version, data_security_mode, cluster_source
# MAGIC ORDER BY cluster_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Node type usage analysis
# MAGIC SELECT 
# MAGIC   workspace_id,
# MAGIC   driver_node_type,
# MAGIC   worker_node_type,
# MAGIC   COUNT(*) as cluster_count,
# MAGIC   SUM(worker_count) as total_workers,
# MAGIC   AVG(worker_count) as avg_workers_per_cluster
# MAGIC FROM system.compute.clusters
# MAGIC WHERE delete_time IS NULL
# MAGIC GROUP BY workspace_id, driver_node_type, worker_node_type
# MAGIC ORDER BY cluster_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. System Compute Node Types
# MAGIC 
# MAGIC Query node type specifications and capabilities.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All available node types with specifications
# MAGIC SELECT 
# MAGIC   account_id,
# MAGIC   node_type,
# MAGIC   core_count,
# MAGIC   memory_mb,
# MAGIC   gpu_count,
# MAGIC   ROUND(memory_mb / 1024, 2) as memory_gb
# MAGIC FROM system.compute.node_types
# MAGIC ORDER BY core_count, memory_mb;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Node type categories
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN gpu_count > 0 THEN 'GPU'
# MAGIC     WHEN core_count >= 16 THEN 'High CPU'
# MAGIC     WHEN memory_mb >= 65536 THEN 'High Memory'
# MAGIC     ELSE 'Standard'
# MAGIC   END as node_category,
# MAGIC   COUNT(*) as node_type_count,
# MAGIC   AVG(core_count) as avg_cores,
# MAGIC   AVG(memory_mb) as avg_memory_mb,
# MAGIC   AVG(gpu_count) as avg_gpu_count
# MAGIC FROM system.compute.node_types
# MAGIC GROUP BY 
# MAGIC   CASE 
# MAGIC     WHEN gpu_count > 0 THEN 'GPU'
# MAGIC     WHEN core_count >= 16 THEN 'High CPU'
# MAGIC     WHEN memory_mb >= 65536 THEN 'High Memory'
# MAGIC     ELSE 'Standard'
# MAGIC   END
# MAGIC ORDER BY node_type_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cross-Table Analysis Queries
# MAGIC 
# MAGIC Queries that combine data from multiple system tables for comprehensive analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Job usage and cost analysis (combining jobs, job runs, and billing)
# MAGIC WITH job_usage AS (
# MAGIC   SELECT 
# MAGIC     j.workspace_id,
# MAGIC     j.job_id,
# MAGIC     j.name as job_name,
# MAGIC     j.creator_id,
# MAGIC     j.tags,
# MAGIC     COUNT(jrt.run_id) as total_runs,
# MAGIC     COUNT(CASE WHEN jrt.result_state = 'SUCCESS' THEN 1 END) as successful_runs,
# MAGIC     COUNT(CASE WHEN jrt.result_state = 'FAILED' THEN 1 END) as failed_runs
# MAGIC   FROM system.lakeflow.jobs j
# MAGIC   LEFT JOIN system.lakeflow.job_run_timeline jrt 
# MAGIC     ON j.job_id = jrt.job_id 
# MAGIC     AND j.workspace_id = jrt.workspace_id
# MAGIC     AND jrt.period_start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC   WHERE j.delete_time IS NULL
# MAGIC   GROUP BY j.workspace_id, j.job_id, j.name, j.creator_id, j.tags
# MAGIC )
# MAGIC SELECT 
# MAGIC   workspace_id,
# MAGIC   job_name,
# MAGIC   creator_id,
# MAGIC   total_runs,
# MAGIC   successful_runs,
# MAGIC   failed_runs,
# MAGIC   ROUND(successful_runs * 100.0 / NULLIF(total_runs, 0), 2) as success_rate_pct
# MAGIC FROM job_usage
# MAGIC WHERE total_runs > 0
# MAGIC ORDER BY total_runs DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Workspace resource utilization summary
# MAGIC WITH workspace_summary AS (
# MAGIC   SELECT 
# MAGIC     w.workspace_id,
# MAGIC     w.workspace_name,
# MAGIC     w.status as workspace_status,
# MAGIC     COUNT(DISTINCT c.cluster_id) as total_clusters,
# MAGIC     COUNT(DISTINCT j.job_id) as total_jobs,
# MAGIC     COUNT(DISTINCT p.pipeline_id) as total_pipelines,
# MAGIC     SUM(c.worker_count) as total_workers
# MAGIC   FROM system.access.workspaces_latest w
# MAGIC   LEFT JOIN system.compute.clusters c ON w.workspace_id = c.workspace_id AND c.delete_time IS NULL
# MAGIC   LEFT JOIN system.lakeflow.jobs j ON w.workspace_id = j.workspace_id AND j.delete_time IS NULL
# MAGIC   LEFT JOIN system.lakeflow.pipelines p ON w.workspace_id = p.workspace_id AND p.delete_time IS NULL
# MAGIC   GROUP BY w.workspace_id, w.workspace_name, w.status
# MAGIC )
# MAGIC SELECT 
# MAGIC   workspace_name,
# MAGIC   workspace_status,
# MAGIC   total_clusters,
# MAGIC   total_jobs,
# MAGIC   total_pipelines,
# MAGIC   total_workers,
# MAGIC   CASE 
# MAGIC     WHEN total_clusters > 0 THEN ROUND(total_workers * 1.0 / total_clusters, 2)
# MAGIC     ELSE 0 
# MAGIC   END as avg_workers_per_cluster
# MAGIC FROM workspace_summary
# MAGIC ORDER BY total_workers DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes and Usage Tips
# MAGIC 
# MAGIC ### Performance Considerations:
# MAGIC - Use date filters to limit data volume (e.g., `WHERE usage_date >= current_date() - INTERVAL 7 DAYS`)
# MAGIC - Add `LIMIT` clauses for initial exploration
# MAGIC - Consider using `DISTINCT` for unique value analysis
# MAGIC 
# MAGIC ### Common Filtering Patterns:
# MAGIC - **Recent data**: `WHERE [date_column] >= current_date() - INTERVAL 7 DAYS`
# MAGIC - **Active resources**: `WHERE delete_time IS NULL`
# MAGIC - **Specific workspace**: `WHERE workspace_id = 'your-workspace-id'`
# MAGIC - **Failed runs**: `WHERE result_state = 'FAILED'`
# MAGIC 
# MAGIC ### Data Relationships:
# MAGIC - Jobs and Job Runs: `system.lakeflow.jobs.job_id = system.lakeflow.job_run_timeline.job_id`
# MAGIC - Job Runs and Task Runs: `system.lakeflow.job_run_timeline.run_id = system.lakeflow.job_task_run_timeline.run_id`
# MAGIC - Usage and Jobs: `system.billing.usage.usage_metadata.job_id = system.lakeflow.jobs.job_id`
# MAGIC - Clusters and Workspaces: `system.compute.clusters.workspace_id = system.access.workspaces_latest.workspace_id`
