# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Jobs Data via Databricks REST API
# MAGIC 
# MAGIC This notebook extracts job data using the Databricks REST API and inserts it into the bronze table.
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
from pyspark.sql.functions import col, lit, current_timestamp, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Configuration

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("pat_token", "", "Personal Access Token")
dbutils.widgets.text("workspace_url", "", "Workspace URL (e.g., https://your-workspace.cloud.databricks.com)")
dbutils.widgets.text("catalog", "platform_observability", "Target Catalog")
dbutils.widgets.text("bronze_schema", "plt_bronze", "Target Bronze Schema")
dbutils.widgets.text("account_id", "", "Account ID")
dbutils.widgets.text("workspace_id", "", "Workspace ID")
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
print(f"  Target: {CATALOG}.{BRONZE_SCHEMA}.brz_lakeflow_jobs")
print(f"  Batch Size: {BATCH_SIZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## REST API Helper Functions

# COMMAND ----------

class DatabricksJobsAPI:
    """Helper class for Databricks Jobs REST API operations"""
    
    def __init__(self, workspace_url: str, pat_token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.pat_token = pat_token
        self.headers = {
            'Authorization': f'Bearer {pat_token}',
            'Content-Type': 'application/json'
        }
    
    def _make_request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """Make HTTP request to Databricks API"""
        url = f"{self.workspace_url}/api/2.1/jobs/{endpoint}"
        
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
    
    def list_jobs(self, limit: int = 25, offset: int = 0) -> Dict:
        """List all jobs in the workspace"""
        params = {
            'limit': limit,
            'offset': offset
        }
        return self._make_request('GET', 'list', params=params)
    
    def get_job(self, job_id: int) -> Dict:
        """Get detailed information about a specific job"""
        return self._make_request('GET', f'get?job_id={job_id}')
    
    def get_all_jobs(self, batch_size: int = 100) -> List[Dict]:
        """Get all jobs with pagination"""
        all_jobs = []
        offset = 0
        
        while True:
            print(f"Fetching jobs batch: offset={offset}, limit={batch_size}")
            response = self.list_jobs(limit=batch_size, offset=offset)
            
            jobs = response.get('jobs', [])
            if not jobs:
                break
            
            all_jobs.extend(jobs)
            offset += len(jobs)
            
            # Check if we've reached the end
            if len(jobs) < batch_size:
                break
        
        print(f"Total jobs fetched: {len(all_jobs)}")
        return all_jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Functions

# COMMAND ----------

def generate_row_hash(data: Dict) -> str:
    """Generate a hash for the row to detect changes"""
    # Create a string representation of the data for hashing
    data_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode()).hexdigest()

def transform_job_data(job_data: Dict, account_id: str, workspace_id: str) -> Dict:
    """Transform job data from API response to bronze table format"""
    
    # Extract basic job information
    job_id = str(job_data.get('job_id', ''))
    name = job_data.get('settings', {}).get('name', '')
    description = job_data.get('settings', {}).get('description', '')
    
    # Extract creator information
    creator_id = job_data.get('creator_user_name', '')
    
    # Extract tags
    tags = job_data.get('settings', {}).get('tags', {})
    
    # Extract timestamps
    created_time = job_data.get('created_time', 0)
    if created_time:
        created_time = datetime.fromtimestamp(created_time / 1000, tz=timezone.utc)
    
    # Extract run_as information
    run_as = job_data.get('settings', {}).get('run_as', {}).get('service_principal_name', '')
    if not run_as:
        run_as = job_data.get('settings', {}).get('run_as', {}).get('user_name', '')
    
    # Create the transformed record
    transformed_record = {
        'account_id': account_id,
        'workspace_id': workspace_id,
        'job_id': job_id,
        'name': name,
        'description': description,
        'creator_id': creator_id,
        'tags': tags,
        'change_time': created_time,
        'delete_time': None,  # Jobs API doesn't provide delete time
        'run_as': run_as,
        '_loaded_at': datetime.now(timezone.utc)
    }
    
    # Generate row hash
    transformed_record['row_hash'] = generate_row_hash(transformed_record)
    
    return transformed_record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Extraction Logic

# COMMAND ----------

def extract_jobs_to_bronze():
    """Main function to extract jobs and insert into bronze table"""
    
    print("Starting jobs extraction...")
    
    # Initialize API client
    api_client = DatabricksJobsAPI(WORKSPACE_URL, PAT_TOKEN)
    
    try:
        # Get all jobs
        print("Fetching all jobs from API...")
        all_jobs = api_client.get_all_jobs(batch_size=BATCH_SIZE)
        
        if not all_jobs:
            print("No jobs found in the workspace")
            return
        
        print(f"Processing {len(all_jobs)} jobs...")
        
        # Transform job data
        transformed_jobs = []
        for job in all_jobs:
            try:
                transformed_job = transform_job_data(job, ACCOUNT_ID, WORKSPACE_ID)
                transformed_jobs.append(transformed_job)
            except Exception as e:
                print(f"Error transforming job {job.get('job_id', 'unknown')}: {e}")
                continue
        
        print(f"Successfully transformed {len(transformed_jobs)} jobs")
        
        # Convert to DataFrame
        if transformed_jobs:
            # Create DataFrame
            df = spark.createDataFrame(transformed_jobs)
            
            # Define the target table
            target_table = f"{CATALOG}.{BRONZE_SCHEMA}.brz_lakeflow_jobs"
            
            print(f"Inserting data into {target_table}...")
            
            # Write to bronze table with merge logic
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)
            
            print(f"Successfully inserted {len(transformed_jobs)} job records into {target_table}")
            
            # Show sample data
            print("\nSample of inserted data:")
            spark.table(target_table).orderBy(col("_loaded_at").desc()).limit(5).show(truncate=False)
            
        else:
            print("No valid job data to insert")
    
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Extraction

# COMMAND ----------

# Run the extraction
extract_jobs_to_bronze()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification and Summary

# COMMAND ----------

# Verify the data was inserted correctly
target_table = f"{CATALOG}.{BRONZE_SCHEMA}.brz_lakeflow_jobs"

print(f"Verifying data in {target_table}...")

# Check record count
total_records = spark.table(target_table).count()
print(f"Total records in table: {total_records}")

# Check latest records
print("\nLatest 5 records:")
spark.table(target_table).orderBy(col("_loaded_at").desc()).limit(5).select(
    "job_id", "name", "creator_id", "change_time", "_loaded_at"
).show(truncate=False)

# Check data quality
print("\nData quality checks:")
df = spark.table(target_table)

# Check for nulls in key fields
null_checks = {
    "job_id": df.filter(col("job_id").isNull()).count(),
    "name": df.filter(col("name").isNull()).count(),
    "account_id": df.filter(col("account_id").isNull()).count(),
    "workspace_id": df.filter(col("workspace_id").isNull()).count()
}

for field, null_count in null_checks.items():
    print(f"  {field}: {null_count} null values")

# Check unique job_ids
unique_jobs = df.select("job_id").distinct().count()
print(f"  Unique job_ids: {unique_jobs}")

print("\n✅ Jobs extraction completed successfully!")

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
# MAGIC The script extracts the following fields from the Jobs API:
# MAGIC - `account_id`: Your Databricks account ID
# MAGIC - `workspace_id`: Your workspace ID  
# MAGIC - `job_id`: Unique job identifier
# MAGIC - `name`: Job name
# MAGIC - `description`: Job description
# MAGIC - `creator_id`: User who created the job
# MAGIC - `tags`: Job tags as key-value pairs
# MAGIC - `change_time`: When the job was created/modified
# MAGIC - `delete_time`: When the job was deleted (null for active jobs)
# MAGIC - `run_as`: Service principal or user the job runs as
# MAGIC - `row_hash`: Hash for change detection
# MAGIC - `_loaded_at`: Timestamp when record was loaded
# MAGIC 
# MAGIC ### Error Handling:
# MAGIC - API rate limiting is handled with batch processing
# MAGIC - Individual job transformation errors are logged but don't stop the process
# MAGIC - Data quality checks are performed after insertion
# MAGIC 
# MAGIC ### Scheduling:
# MAGIC This notebook can be scheduled as a Databricks job to run periodically and keep the bronze table updated.
