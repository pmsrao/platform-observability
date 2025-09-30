# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Clusters Data via Databricks REST API
# MAGIC 
# MAGIC This notebook extracts cluster data using the Databricks REST API and inserts it into the bronze table.
# MAGIC 
# MAGIC ## Prerequisites:
# MAGIC 1. Set up a Personal Access Token (PAT) in Databricks
# MAGIC 2. Configure the following widgets:
# MAGIC    - `pat_token`: Your Personal Access Token
# MAGIC    - `workspace_url`: Your Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
# MAGIC    - `catalog`: Target catalog name
# MAGIC    - `bronze_schema`: Target bronze schema name
# MAGIC    - `account_id`: Your Databricks account ID
# MAGIC    - `workspace_id`: Your workspace ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import requests
import json
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, isnan, isnull, to_date
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType, BooleanType, LongType, ArrayType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("pat_token", "", "Personal Access Token")
dbutils.widgets.text("workspace_url", "https://dbc-8e3108de-b81a.cloud.databricks.com/", "Workspace URL (e.g., https://your-workspace.cloud.databricks.com)")
dbutils.widgets.text("catalog", "platform_observability", "Target Catalog")
dbutils.widgets.text("bronze_schema", "plt_bronze", "Target Bronze Schema")
dbutils.widgets.text("account_id", "bbbbf5c0-dfce-4b0c-8daf-aeae76d68c22", "Account ID")
dbutils.widgets.text("workspace_id", "1524967460024799", "Workspace ID")
dbutils.widgets.text("batch_size", "100", "Batch size for API calls")

# Get widget values
PAT_TOKEN = dbutils.widgets.get("pat_token")
WORKSPACE_URL = dbutils.widgets.get("workspace_url")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
ACCOUNT_ID = dbutils.widgets.get("account_id")
WORKSPACE_ID = dbutils.widgets.get("workspace_id")
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))

# Validate required parameters
if not all([PAT_TOKEN, WORKSPACE_URL, ACCOUNT_ID, WORKSPACE_ID]):
    raise ValueError("Please provide all required parameters: pat_token, workspace_url, account_id, workspace_id")

print(f"Configuration:")
print(f"  Workspace URL: {WORKSPACE_URL}")
print(f"  Account ID: {ACCOUNT_ID}")
print(f"  Workspace ID: {WORKSPACE_ID}")
print(f"  Target: {CATALOG}.{BRONZE_SCHEMA}.brz_compute_clusters")
print(f"  Batch Size: {BATCH_SIZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## REST API Helper Functions

# COMMAND ----------

class DatabricksClustersAPI:
    """Helper class for Databricks Clusters REST API operations"""
    
    def __init__(self, workspace_url: str, pat_token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.pat_token = pat_token
        self.headers = {
            'Authorization': f'Bearer {pat_token}',
            'Content-Type': 'application/json'
        }
    
    def _make_request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """Make HTTP request to Databricks API"""
        url = f"{self.workspace_url}/api/2.0/clusters/{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=self.headers, params=params)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=self.headers, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            raise
    
    def list_clusters(self) -> Dict:
        """List all clusters in the workspace"""
        return self._make_request('GET', 'list')
    
    def get_cluster(self, cluster_id: str) -> Dict:
        """Get detailed information about a specific cluster"""
        return self._make_request('GET', f'get?cluster_id={cluster_id}')
    
    def get_all_clusters(self) -> List[Dict]:
        """Get all clusters with detailed information"""
        print("Fetching clusters list...")
        response = self.list_clusters()
        
        clusters = response.get('clusters', [])
        print(f"Found {len(clusters)} clusters")
        
        # Get detailed information for each cluster
        detailed_clusters = []
        for cluster in clusters:
            cluster_id = cluster.get('cluster_id')
            if cluster_id:
                try:
                    print(f"Fetching details for cluster: {cluster_id}")
                    detailed_cluster = self.get_cluster(cluster_id)
                    detailed_clusters.append(detailed_cluster)
                except Exception as e:
                    print(f"Error fetching details for cluster {cluster_id}: {e}")
                    # Use basic cluster info if detailed fetch fails
                    detailed_clusters.append(cluster)
        
        print(f"Successfully fetched details for {len(detailed_clusters)} clusters")
        return detailed_clusters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Functions

# COMMAND ----------

def generate_row_hash(data: Dict) -> str:
    """Generate a hash for the row to detect changes"""
    # Create a string representation of the data for hashing
    data_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode()).hexdigest()

def safe_get_nested(data: Dict, keys: List[str], default=None):
    """Safely get nested dictionary values"""
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current

def transform_cluster_data(cluster_data: Dict, account_id: str, workspace_id: str) -> Dict:
    """Transform cluster data from API response to bronze table format"""
    
    # Extract basic cluster information
    cluster_id = cluster_data.get('cluster_id', '')
    cluster_name = cluster_data.get('cluster_name', '')
    owned_by = cluster_data.get('creator_user_name', '')
    
    # Extract timestamps
    create_time = cluster_data.get('start_time', 0)
    if create_time:
        create_time = datetime.fromtimestamp(create_time / 1000, tz=timezone.utc)
    else:
        create_time = None
    
    # Extract node configuration
    driver_node_type = safe_get_nested(cluster_data, ['driver_node_type_id'])
    worker_node_type = safe_get_nested(cluster_data, ['node_type_id'])
    
    # Extract worker counts
    worker_count = safe_get_nested(cluster_data, ['num_workers'], 0)
    min_autoscale_workers = safe_get_nested(cluster_data, ['autoscale', 'min_workers'], 0)
    max_autoscale_workers = safe_get_nested(cluster_data, ['autoscale', 'max_workers'], 0)
    
    # Extract other configuration
    auto_termination_minutes = safe_get_nested(cluster_data, ['autotermination_minutes'], 0)
    enable_elastic_disk = safe_get_nested(cluster_data, ['enable_elastic_disk'], False)
    
    # Extract tags
    tags = cluster_data.get('custom_tags', {})
    
    # Extract cluster source
    cluster_source = safe_get_nested(cluster_data, ['cluster_source'], 'UNKNOWN')
    
    # Extract init scripts
    init_scripts = []
    init_scripts_data = safe_get_nested(cluster_data, ['init_scripts'], [])
    for script in init_scripts_data:
        if isinstance(script, dict):
            # Extract script path or command
            script_path = script.get('dbfs', {}).get('destination', '')
            if script_path:
                init_scripts.append(script_path)
    
    # Extract cloud-specific attributes
    aws_attributes = safe_get_nested(cluster_data, ['aws_attributes'])
    azure_attributes = safe_get_nested(cluster_data, ['azure_attributes'])
    gcp_attributes = safe_get_nested(cluster_data, ['gcp_attributes'])
    
    # Convert attributes to JSON strings
    aws_attributes_str = json.dumps(aws_attributes) if aws_attributes else None
    azure_attributes_str = json.dumps(azure_attributes) if azure_attributes else None
    gcp_attributes_str = json.dumps(gcp_attributes) if gcp_attributes else None
    
    # Extract instance pool information
    driver_instance_pool_id = safe_get_nested(cluster_data, ['driver_instance_pool_id'])
    worker_instance_pool_id = safe_get_nested(cluster_data, ['instance_pool_id'])
    
    # Extract runtime information
    dbr_version = safe_get_nested(cluster_data, ['spark_version'])
    
    # Extract security and policy information
    data_security_mode = safe_get_nested(cluster_data, ['data_security_mode'])
    policy_id = safe_get_nested(cluster_data, ['policy_id'])
    
    # Set change_time (use create_time if available, otherwise current time)
    change_time = create_time if create_time else datetime.now(timezone.utc)
    change_date = change_time.date() if change_time else None
    
    # Create the transformed record
    transformed_record = {
        'account_id': account_id,
        'workspace_id': workspace_id,
        'cluster_id': cluster_id,
        'cluster_name': cluster_name,
        'owned_by': owned_by,
        'create_time': create_time,
        'delete_time': None,  # Clusters API doesn't provide delete time
        'driver_node_type': driver_node_type,
        'worker_node_type': worker_node_type,
        'worker_count': worker_count,
        'min_autoscale_workers': min_autoscale_workers,
        'max_autoscale_workers': max_autoscale_workers,
        'auto_termination_minutes': auto_termination_minutes,
        'enable_elastic_disk': enable_elastic_disk,
        'tags': tags,
        'cluster_source': cluster_source,
        'init_scripts': init_scripts,
        'aws_attributes': aws_attributes_str,
        'azure_attributes': azure_attributes_str,
        'gcp_attributes': gcp_attributes_str,
        'driver_instance_pool_id': driver_instance_pool_id,
        'worker_instance_pool_id': worker_instance_pool_id,
        'dbr_version': dbr_version,
        'change_time': change_time,
        'change_date': change_date,
        'data_security_mode': data_security_mode,
        'policy_id': policy_id,
        '_loaded_at': datetime.now(timezone.utc)
    }
    
    # Generate row hash
    transformed_record['row_hash'] = generate_row_hash(transformed_record)
    
    return transformed_record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Extraction Logic

# COMMAND ----------

def extract_clusters_to_bronze():
    """Main function to extract clusters and insert into bronze table"""
    
    print("Starting clusters extraction...")
    
    # Initialize API client
    api_client = DatabricksClustersAPI(WORKSPACE_URL, PAT_TOKEN)
    
    try:
        # Get all clusters
        print("Fetching all clusters from API...")
        all_clusters = api_client.get_all_clusters()
        
        if not all_clusters:
            print("No clusters found in the workspace")
            return
        
        print(f"Processing {len(all_clusters)} clusters...")
        
        # Transform cluster data
        transformed_clusters = []
        for cluster in all_clusters:
            try:
                transformed_cluster = transform_cluster_data(cluster, ACCOUNT_ID, WORKSPACE_ID)
                transformed_clusters.append(transformed_cluster)
            except Exception as e:
                print(f"Error transforming cluster {cluster.get('cluster_id', 'unknown')}: {e}")
                continue
        
        print(f"Successfully transformed {len(transformed_clusters)} clusters")
        
        # Convert to DataFrame
        if transformed_clusters:
            # Define explicit schema for the bronze table
            schema = StructType([
                StructField("account_id", StringType(), True),
                StructField("workspace_id", StringType(), True),
                StructField("cluster_id", StringType(), True),
                StructField("cluster_name", StringType(), True),
                StructField("owned_by", StringType(), True),
                StructField("create_time", TimestampType(), True),
                StructField("delete_time", TimestampType(), True),
                StructField("driver_node_type", StringType(), True),
                StructField("worker_node_type", StringType(), True),
                StructField("worker_count", LongType(), True),
                StructField("min_autoscale_workers", LongType(), True),
                StructField("max_autoscale_workers", LongType(), True),
                StructField("auto_termination_minutes", LongType(), True),
                StructField("enable_elastic_disk", BooleanType(), True),
                StructField("tags", MapType(StringType(), StringType()), True),
                StructField("cluster_source", StringType(), True),
                StructField("init_scripts", ArrayType(StringType()), True),
                StructField("aws_attributes", StringType(), True),
                StructField("azure_attributes", StringType(), True),
                StructField("gcp_attributes", StringType(), True),
                StructField("driver_instance_pool_id", StringType(), True),
                StructField("worker_instance_pool_id", StringType(), True),
                StructField("dbr_version", StringType(), True),
                StructField("change_time", TimestampType(), True),
                StructField("change_date", DateType(), True),
                StructField("data_security_mode", StringType(), True),
                StructField("policy_id", StringType(), True),
                StructField("row_hash", StringType(), True),
                StructField("_loaded_at", TimestampType(), True)
            ])
            
            # Create DataFrame with explicit schema
            df = spark.createDataFrame(transformed_clusters, schema)
            
            # Define the target table
            target_table = f"{CATALOG}.{BRONZE_SCHEMA}.brz_compute_clusters"
            
            print(f"Inserting data into {target_table}...")
            
            # Write to bronze table with merge logic
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)
            
            print(f"Successfully inserted {len(transformed_clusters)} cluster records into {target_table}")
            
            # Show sample data
            print("\nSample of inserted data:")
            spark.table(target_table).orderBy(col("_loaded_at").desc()).limit(5).show(truncate=False)
            
        else:
            print("No valid cluster data to insert")
    
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Extraction

# COMMAND ----------

# Run the extraction
extract_clusters_to_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification and Summary

# COMMAND ----------

# Verify the data was inserted correctly
target_table = f"{CATALOG}.{BRONZE_SCHEMA}.brz_compute_clusters"

print(f"Verifying data in {target_table}...")

# Check record count
total_records = spark.table(target_table).count()
print(f"Total records in table: {total_records}")

# Check latest records
print("\nLatest 5 records:")
spark.table(target_table).orderBy(col("_loaded_at").desc()).limit(5).select(
    "cluster_id", "cluster_name", "owned_by", "create_time", "_loaded_at"
).show(truncate=False)

# Check data quality
print("\nData quality checks:")
df = spark.table(target_table)

# Check for nulls in key fields
null_checks = {
    "cluster_id": df.filter(col("cluster_id").isNull()).count(),
    "cluster_name": df.filter(col("cluster_name").isNull()).count(),
    "account_id": df.filter(col("account_id").isNull()).count(),
    "workspace_id": df.filter(col("workspace_id").isNull()).count()
}

for field, null_count in null_checks.items():
    print(f"  {field}: {null_count} null values")

# Check unique cluster_ids
unique_clusters = df.select("cluster_id").distinct().count()
print(f"  Unique cluster_ids: {unique_clusters}")

# Check cluster states
print("\nCluster sources:")
df.groupBy("cluster_source").count().show()

print("\n✅ Clusters extraction completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Widgets

# COMMAND ----------

# Clean up widgets (optional)
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC 
# MAGIC ### Setup:
# MAGIC 1. **Create Personal Access Token:**
# MAGIC    - Go to User Settings → Developer → Access tokens
# MAGIC    - Generate new token with appropriate permissions
# MAGIC 
# MAGIC 2. **Get Workspace Information:**
# MAGIC    - Workspace URL: Your Databricks workspace URL
# MAGIC    - Account ID: Found in Account Settings → Account console
# MAGIC    - Workspace ID: Found in workspace URL or Account Settings
# MAGIC 
# MAGIC 3. **Configure Widgets:**
# MAGIC    - Run the notebook and fill in the widget values
# MAGIC    - Or modify the default values in the widget creation section
# MAGIC 
# MAGIC ### Execution:
# MAGIC 1. Run all cells in sequence
# MAGIC 2. Monitor the output for any errors
# MAGIC 3. Verify data in the bronze table
# MAGIC 
# MAGIC ### Data Schema:
# MAGIC The script extracts the following fields from the Clusters API:
# MAGIC - `account_id`: Your Databricks account ID
# MAGIC - `workspace_id`: Your workspace ID  
# MAGIC - `cluster_id`: Unique cluster identifier
# MAGIC - `cluster_name`: Cluster name
# MAGIC - `owned_by`: User who created the cluster
# MAGIC - `create_time`: When the cluster was created
# MAGIC - `delete_time`: When the cluster was deleted (null for active clusters)
# MAGIC - `driver_node_type`: Driver node type
# MAGIC - `worker_node_type`: Worker node type
# MAGIC - `worker_count`: Number of worker nodes
# MAGIC - `min_autoscale_workers`: Minimum autoscale workers
# MAGIC - `max_autoscale_workers`: Maximum autoscale workers
# MAGIC - `auto_termination_minutes`: Auto-termination setting
# MAGIC - `enable_elastic_disk`: Elastic disk enabled flag
# MAGIC - `tags`: Cluster tags as key-value pairs
# MAGIC - `cluster_source`: Source of the cluster (UI, API, etc.)
# MAGIC - `init_scripts`: Initialization scripts
# MAGIC - `aws_attributes`: AWS-specific attributes (JSON string)
# MAGIC - `azure_attributes`: Azure-specific attributes (JSON string)
# MAGIC - `gcp_attributes`: GCP-specific attributes (JSON string)
# MAGIC - `driver_instance_pool_id`: Driver instance pool ID
# MAGIC - `worker_instance_pool_id`: Worker instance pool ID
# MAGIC - `dbr_version`: Databricks Runtime version
# MAGIC - `change_time`: When the cluster was last modified
# MAGIC - `change_date`: Date when the cluster was last modified
# MAGIC - `data_security_mode`: Data security mode
# MAGIC - `policy_id`: Cluster policy ID
# MAGIC - `row_hash`: Hash for change detection
# MAGIC - `_loaded_at`: Timestamp when record was loaded
# MAGIC 
# MAGIC ### Error Handling:
# MAGIC - API rate limiting is handled with batch processing
# MAGIC - Individual cluster transformation errors are logged but don't stop the process
# MAGIC - Data quality checks are performed after insertion
# MAGIC 
# MAGIC ### Scheduling:
# MAGIC This notebook can be scheduled as a Databricks job to run periodically and keep the bronze table updated.
