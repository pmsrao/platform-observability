"""
SQL Parameterizer Utility

This module provides utilities for parameterizing and executing SQL files
with proper catalog and schema values from configuration.
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from config import Config
from .sql_manager import sql_manager


class SQLParameterizer:
    """Utility class for parameterizing and executing SQL files"""
    
    def __init__(self, spark: SparkSession = None, sql_manager_instance = None):
        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        self.spark = spark
        self.config = Config.get_config()
        self.sql_manager = sql_manager_instance if sql_manager_instance is not None else sql_manager
    
    def execute_bootstrap_sql(self, operation: str, **kwargs) -> None:
        """Execute bootstrap SQL operations with proper parameterization"""
        # Add catalog and schema parameters if not provided
        if "catalog" not in kwargs:
            kwargs["catalog"] = self.config.catalog
        if "bronze_schema" not in kwargs:
            kwargs["bronze_schema"] = self.config.bronze_schema
        if "silver_schema" not in kwargs:
            kwargs["silver_schema"] = self.config.silver_schema
        if "gold_schema" not in kwargs:
            kwargs["gold_schema"] = self.config.gold_schema
        
        # Get parameterized SQL statements
        statements = self.sql_manager.parameterize_sql_statements(operation, **kwargs)
        
        # Execute each statement separately
        for i, statement in enumerate(statements):
            if statement.strip():
                print(f"   Executing statement {i+1}/{len(statements)}: {statement[:50]}...")
                try:
                    self.spark.sql(statement)
                    print(f"   ✅ Statement {i+1} executed successfully")
                except Exception as e:
                    print(f"   ❌ Statement {i+1} failed: {e}")
                    print(f"   Statement content: {repr(statement)}")
                    raise
    
    def bootstrap_catalog_schemas(self) -> None:
        """Bootstrap catalog and schemas"""
        print("Bootstrapping catalog and schemas...")
        self.execute_bootstrap_sql("config/bootstrap_catalog_schemas")
        print("✅ Catalog and schemas created successfully")
    
    def bootstrap_bronze_tables(self) -> None:
        """Bootstrap bronze tables with CDF enabled"""
        print("Bootstrapping bronze tables...")
        self.execute_bootstrap_sql("bronze/bronze_tables")
        print("✅ Bronze tables created successfully")
    
    def create_processing_state(self) -> None:
        """Create processing state tables for CDF and HWM tracking"""
        print("Creating processing state tables...")
        self.execute_bootstrap_sql("config/processing_offsets")
        print("✅ Processing state tables created successfully")
    
    def create_silver_tables(self) -> None:
        """Create silver layer tables"""
        print("Creating silver layer tables...")
        self.execute_bootstrap_sql("silver/silver_tables")
        print("✅ Silver layer tables created successfully")
    
    def create_gold_tables(self) -> None:
        """Create gold layer tables"""
        print("Creating gold layer tables...")
        self.execute_bootstrap_sql("gold/gold_dimensions")
        self.execute_bootstrap_sql("gold/gold_facts")
        print("✅ Gold layer tables created successfully")
    
    def apply_performance_optimizations(self) -> None:
        """Apply performance optimizations"""
        print("Applying performance optimizations...")
        self.execute_bootstrap_sql("config/performance_optimizations")
        print("✅ Performance optimizations applied successfully")
    
    def create_gold_views(self) -> None:
        """Create gold layer views"""
        print("Creating gold layer views...")
        self.execute_bootstrap_sql("gold/gold_views")
        self.execute_bootstrap_sql("gold/policy_compliance")
        self.execute_bootstrap_sql("gold/gold_chargeback_views")
        print("✅ Gold layer views created successfully")
    
    def full_bootstrap(self) -> None:
        """Perform full bootstrap of the platform observability system"""
        print("🚀 Starting full bootstrap of platform observability system...")
        
        try:
            # Step 1: Create catalog and schemas
            self.bootstrap_catalog_schemas()
            
            # Step 2: Create processing state tables
            self.create_processing_state()
            
            # Step 3: Create bronze tables
            self.bootstrap_bronze_tables()
            
            # Step 4: Create silver tables
            self.create_silver_tables()
            
            # Step 5: Create gold tables
            self.create_gold_tables()
            
            # Step 6: Apply performance optimizations
            self.apply_performance_optimizations()
            
            # Step 7: Create gold views
            self.create_gold_views()
            
            print("🎉 Full bootstrap completed successfully!")
            
        except Exception as e:
            print(f"❌ Bootstrap failed: {str(e)}")
            raise
    
    def get_parameterized_sql(self, operation: str, **kwargs) -> str:
        """Get parameterized SQL content for review"""
        return self.sql_manager.parameterize_sql_with_catalog_schema(operation, **kwargs)
    
    def list_available_operations(self) -> list:
        """List all available SQL operations"""
        return self.sql_manager.get_available_operations()


def bootstrap_platform_observability(spark: SparkSession = None) -> None:
    """Convenience function to bootstrap the entire platform observability system"""
    parameterizer = SQLParameterizer(spark)
    parameterizer.full_bootstrap()


if __name__ == "__main__":
    # This can be run as a standalone script for testing
    print("SQL Parameterizer Utility")
    print("Available operations:")
    
    operations = sql_manager.get_available_operations()
    for op in operations:
        print(f"  - {op}")
    
    print(f"\nConfiguration:")
    config = Config.get_config()
    print(f"  Catalog: {config.catalog}")
    print(f"  Bronze Schema: {config.bronze_schema}")
    print(f"  Silver Schema: {config.silver_schema}")
    print(f"  Gold Schema: {config.gold_schema}")
    print(f"  Environment: {Config.ENV}")
    print(f"  Log Level: {config.log_level}")
