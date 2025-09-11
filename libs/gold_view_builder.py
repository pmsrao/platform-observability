"""
Gold Layer View Builder

This module provides classes for building Gold layer views
using proper star schema design with surrogate key joins.
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config import Config


class ViewBuilder:
    """Base class for building views"""
    
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config
        self.catalog = config.catalog
        self.gold_schema = config.gold_schema
    
    def get_view_name(self, view_name: str) -> str:
        """Get fully qualified view name"""
        return f"{self.catalog}.{self.gold_schema}.{view_name}"
    
    def create_view(self, sql: str, view_name: str) -> bool:
        """Create or replace view"""
        try:
            full_view_name = self.get_view_name(view_name)
            create_sql = f"CREATE OR REPLACE VIEW {full_view_name} AS {sql}"
            print(f"Creating view {view_name}")
            self.spark.sql(create_sql)
            return True
        except Exception as e:
            print(f"Error creating view {view_name}: {str(e)}")
            return False


class ChargebackViewBuilder(ViewBuilder):
    """Builder for chargeback views"""
    
    def build_all(self) -> Dict[str, bool]:
        """Build all chargeback views"""
        results = {}
        
        # Department Cost Summary View
        results["department_cost_summary"] = self.create_view("""
            SELECT 
                f.department as department_code,
                w.workspace_name,
                f.date_key,
                SUM(f.usage_cost) as department_total_cost,
                COUNT(DISTINCT e.entity_id) as active_entities,
                AVG(f.usage_cost) as avg_cost_per_entity,
                COUNT(DISTINCT w.workspace_id) as active_workspaces
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.department, w.workspace_name, f.date_key
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_department_cost_summary")
        
        # Line of Business Cost Summary
        results["line_of_business_cost"] = self.create_view("""
            SELECT 
                f.date_key,
                f.line_of_business,
                f.department,
                f.environment,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                AVG(f.usage_cost) as avg_cost_per_entity,
                SUM(f.duration_hours) as total_duration_hours
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.date_key, f.line_of_business, f.department, f.environment
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_line_of_business_cost")
        
        # Environment Cost Analysis
        results["environment_cost"] = self.create_view("""
            SELECT 
                f.date_key,
                f.environment,
                f.line_of_business,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                AVG(f.usage_cost) as avg_cost_per_entity,
                SUM(f.duration_hours) as total_duration_hours
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.date_key, f.environment, f.line_of_business
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_environment_cost")
        
        # Use Case Cost Analysis
        results["use_case_cost"] = self.create_view("""
            SELECT 
                f.date_key,
                f.use_case,
                f.line_of_business,
                f.department,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                AVG(f.usage_cost) as avg_cost_per_entity,
                SUM(f.duration_hours) as total_duration_hours
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.date_key, f.use_case, f.line_of_business, f.department
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_use_case_cost")
        
        # Cluster Cost Analysis
        results["cluster_cost"] = self.create_view("""
            SELECT 
                f.date_key,
                f.cluster_identifier,
                f.line_of_business,
                f.department,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                AVG(f.usage_cost) as avg_cost_per_entity,
                SUM(f.duration_hours) as total_duration_hours
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.date_key, f.cluster_identifier, f.line_of_business, f.department
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_cluster_cost")
        
        # Tag Quality Analysis
        results["tag_quality_analysis"] = self.create_view("""
            SELECT 
                f.date_key,
                w.workspace_id,
                w.workspace_name,
                e.entity_type,
                e.entity_id,
                -- Tag completeness indicators (based on default values)
                CASE WHEN f.line_of_business = 'Unknown' THEN 'Missing' ELSE 'Present' END as line_of_business_status,
                CASE WHEN f.department = 'unknown' THEN 'Missing' ELSE 'Present' END as department_status,
                CASE WHEN f.cost_center = 'unallocated' THEN 'Missing' ELSE 'Present' END as cost_center_status,
                CASE WHEN f.environment = 'dev' THEN 'Missing' ELSE 'Present' END as environment_status,
                CASE WHEN f.use_case = 'Unknown' THEN 'Missing' ELSE 'Present' END as use_case_status,
                CASE WHEN f.pipeline_name = 'system' THEN 'Missing' ELSE 'Present' END as pipeline_name_status,
                CASE WHEN f.cluster_identifier = 'Unknown' THEN 'Missing' ELSE 'Present' END as cluster_identifier_status,
                CASE WHEN f.workflow_level = 'STANDALONE' THEN 'Missing' ELSE 'Present' END as workflow_level_status,
                CASE WHEN f.parent_workflow_name = 'None' THEN 'Missing' ELSE 'Present' END as parent_workflow_status,
                -- Count of missing tags
                CASE WHEN f.line_of_business = 'Unknown' THEN 1 ELSE 0 END +
                CASE WHEN f.department = 'unknown' THEN 1 ELSE 0 END +
                CASE WHEN f.cost_center = 'unallocated' THEN 1 ELSE 0 END +
                CASE WHEN f.environment = 'dev' THEN 1 ELSE 0 END +
                CASE WHEN f.use_case = 'Unknown' THEN 1 ELSE 0 END +
                CASE WHEN f.pipeline_name = 'system' THEN 1 ELSE 0 END +
                CASE WHEN f.cluster_identifier = 'Unknown' THEN 1 ELSE 0 END +
                CASE WHEN f.workflow_level = 'STANDALONE' THEN 1 ELSE 0 END +
                CASE WHEN f.parent_workflow_name = 'None' THEN 1 ELSE 0 END as missing_tags_count,
                -- Tag values for analysis
                f.line_of_business,
                f.department,
                f.cost_center,
                f.environment,
                f.use_case,
                f.pipeline_name,
                f.cluster_identifier,
                f.workflow_level,
                f.parent_workflow_name,
                f.usage_cost
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_tag_quality_analysis")
        
        # Tag Coverage Summary
        results["tag_coverage_summary"] = self.create_view("""
            SELECT 
                f.date_key,
                COUNT(*) as total_records,
                SUM(CASE WHEN f.line_of_business != 'Unknown' THEN 1 ELSE 0 END) as records_with_line_of_business,
                SUM(CASE WHEN f.department != 'unknown' THEN 1 ELSE 0 END) as records_with_department,
                SUM(CASE WHEN f.cost_center != 'unallocated' THEN 1 ELSE 0 END) as records_with_cost_center,
                SUM(CASE WHEN f.environment != 'dev' THEN 1 ELSE 0 END) as records_with_environment,
                SUM(CASE WHEN f.use_case != 'Unknown' THEN 1 ELSE 0 END) as records_with_use_case,
                SUM(CASE WHEN f.pipeline_name != 'system' THEN 1 ELSE 0 END) as records_with_pipeline_name,
                SUM(CASE WHEN f.cluster_identifier != 'Unknown' THEN 1 ELSE 0 END) as records_with_cluster_identifier,
                SUM(CASE WHEN f.workflow_level != 'STANDALONE' THEN 1 ELSE 0 END) as records_with_workflow_level,
                SUM(CASE WHEN f.parent_workflow_name != 'None' THEN 1 ELSE 0 END) as records_with_parent_workflow,
                -- Coverage percentages
                ROUND(SUM(CASE WHEN f.line_of_business != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as line_of_business_coverage_pct,
                ROUND(SUM(CASE WHEN f.department != 'unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as department_coverage_pct,
                ROUND(SUM(CASE WHEN f.cost_center != 'unallocated' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cost_center_coverage_pct,
                ROUND(SUM(CASE WHEN f.environment != 'dev' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as environment_coverage_pct,
                ROUND(SUM(CASE WHEN f.use_case != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as use_case_coverage_pct,
                ROUND(SUM(CASE WHEN f.pipeline_name != 'system' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pipeline_name_coverage_pct,
                ROUND(SUM(CASE WHEN f.cluster_identifier != 'Unknown' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cluster_identifier_coverage_pct,
                ROUND(SUM(CASE WHEN f.workflow_level != 'STANDALONE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as workflow_level_coverage_pct,
                ROUND(SUM(CASE WHEN f.parent_workflow_name != 'None' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as parent_workflow_coverage_pct
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            GROUP BY f.date_key
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_tag_coverage_summary")
        
        # Cost Center Analysis
        results["cost_center_analysis"] = self.create_view("""
            SELECT 
                f.date_key,
                f.cost_center,
                f.line_of_business,
                f.department,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                AVG(f.usage_cost) as avg_cost_per_entity,
                SUM(f.duration_hours) as total_duration_hours
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.date_key, f.cost_center, f.line_of_business, f.department
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_cost_center_analysis")
        
        # Workflow Hierarchy Cost Analysis
        results["workflow_hierarchy_cost"] = self.create_view("""
            SELECT 
                f.date_key,
                f.workflow_level,
                f.parent_workflow_name,
                f.cost_center,
                f.line_of_business,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                AVG(f.usage_cost) as avg_cost_per_entity,
                SUM(f.duration_hours) as total_duration_hours
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.date_key, f.workflow_level, f.parent_workflow_name, f.cost_center, f.line_of_business
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_workflow_hierarchy_cost")
        
        # Cluster Cost Attribution by Workflow
        results["cluster_workflow_cost"] = self.create_view("""
            SELECT 
                f.date_key,
                f.cluster_identifier,
                f.workflow_level,
                f.parent_workflow_name,
                f.cost_center,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                AVG(f.usage_cost) as avg_cost_per_entity,
                SUM(f.duration_hours) as total_duration_hours
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.date_key, f.cluster_identifier, f.workflow_level, f.parent_workflow_name, f.cost_center
        """.format(catalog=self.catalog, gold_schema=self.gold_schema), "v_cluster_workflow_cost")
        
        return results


class RuntimeAnalysisViewBuilder(ViewBuilder):
    """Builder for runtime analysis views"""
    
    def build_all(self) -> Dict[str, bool]:
        """Build all runtime analysis views"""
        results = {}
        
        # Runtime Version Analysis
        results["runtime_version_analysis"] = self.create_view("""
            SELECT 
                f.date_key,
                c.dbr_version,
                c.major_version,
                c.minor_version,
                c.cluster_source,
                c.is_lts,
                c.is_current,
                c.runtime_age_months,
                NULL as months_to_eos,
                NULL as is_supported,
                f.line_of_business,
                f.department,
                f.cost_center,
                f.environment,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                SUM(f.duration_hours) as total_duration_hours,
                -- Upgrade priority logic
                CASE 
                    WHEN months_to_eos <= 3 THEN 'Critical'
                    WHEN months_to_eos <= 6 THEN 'High'
                    WHEN months_to_eos <= 12 THEN 'Medium'
                    ELSE 'Low'
                END as upgrade_priority
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{silver_schema}.slv_clusters c ON f.cluster_identifier = c.cluster_id
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            GROUP BY f.date_key, c.dbr_version, c.major_version, c.minor_version,
                     c.cluster_source, c.is_lts, c.is_current, c.runtime_age_months,
                     months_to_eos, is_supported, f.line_of_business, f.department, f.cost_center, f.environment
        """.format(catalog=self.catalog, gold_schema=self.gold_schema, silver_schema=self.config.silver_schema), "v_runtime_version_analysis")
        
        # Node Type Analysis
        results["node_type_analysis"] = self.create_view("""
            SELECT 
                f.date_key,
                c.worker_node_type,
                c.driver_node_type,
                c.worker_node_type as node_type_id,
                -- Node type categorization (from Silver layer)
                c.worker_node_type_category as node_family,
                -- Cluster sizing analysis
                c.min_autoscale_workers,
                c.max_autoscale_workers,
                case when c.worker_count is null then true else false end as autoscale_enabled,
                CASE 
                    WHEN c.worker_count is not null THEN 'Fixed'
                    WHEN c.worker_count is null THEN 'Auto-scaling'
                    ELSE 'Manual'
                END as scaling_type,
                -- Business context (from Silver layer)
                f.line_of_business,
                f.department,
                f.cost_center,
                f.environment,
                COUNT(DISTINCT w.workspace_id) as active_workspaces,
                COUNT(DISTINCT e.entity_id) as active_entities,
                SUM(f.usage_cost) as total_cost_usd,
                SUM(f.duration_hours) as total_duration_hours,
                AVG(f.duration_hours) as avg_duration_hours
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{silver_schema}.slv_clusters c ON f.cluster_identifier = c.cluster_id
            JOIN {catalog}.{gold_schema}.gld_dim_workspace w ON f.workspace_key = w.workspace_key
            JOIN {catalog}.{gold_schema}.gld_dim_entity e ON f.entity_key = e.entity_key
            GROUP BY f.date_key, c.worker_node_type, c.driver_node_type, c.worker_node_type,
                     c.worker_node_type_category, c.min_autoscale_workers, c.max_autoscale_workers,
                     autoscale_enabled, c.worker_count, f.line_of_business, f.department, f.cost_center, f.environment
        """.format(catalog=self.catalog, gold_schema=self.gold_schema, silver_schema=self.config.silver_schema), "v_node_type_analysis")
        
        # Runtime Optimization Opportunities
        results["runtime_optimization_opportunities"] = self.create_view("""
            SELECT 
                f.date_key,
                c.dbr_version,
                c.major_version,
                c.minor_version,
                c.cluster_source,
                c.is_lts,
                c.is_current,
                c.runtime_age_months,
                NULL as months_to_eos,
                NULL as is_supported,
                -- Runtime generation classification
                CASE 
                    WHEN c.major_version >= 14 THEN 'Latest Generation'
                    WHEN c.major_version >= 13 THEN 'Current Generation'
                    WHEN c.major_version >= 12 THEN 'Previous Generation'
                    ELSE 'Legacy'
                END as runtime_generation,
                -- Node type efficiency (from Silver layer)
                c.worker_node_type_category as node_optimization_type
            FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
            JOIN {catalog}.{silver_schema}.slv_clusters c ON f.cluster_identifier = c.cluster_id
            WHERE c.dbr_version IS NOT NULL
            GROUP BY f.date_key, c.dbr_version, c.major_version, c.minor_version,c.is_lts,c.is_current,
                c.runtime_age_months,c.cluster_source, c.worker_node_type_category
        """.format(catalog=self.catalog, gold_schema=self.gold_schema, silver_schema=self.config.silver_schema), "v_runtime_optimization_opportunities")
        
        return results


class ViewBuilderFactory:
    """Factory for creating view builders"""
    
    @staticmethod
    def create_builder(builder_type: str, spark: SparkSession, config: Config) -> ViewBuilder:
        """Create appropriate view builder"""
        builders = {
            "chargeback": ChargebackViewBuilder,
            "runtime_analysis": RuntimeAnalysisViewBuilder
        }
        
        if builder_type not in builders:
            raise ValueError(f"Unknown builder type: {builder_type}")
        
        return builders[builder_type](spark, config)
