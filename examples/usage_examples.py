"""
Usage Examples for Platform Observability

This file demonstrates how to use the new parameterized configuration
and SQL management system.
"""

from pyspark.sql import SparkSession
from config import Config
from libs.sql_manager import sql_manager
from libs.sql_parameterizer import SQLParameterizer, bootstrap_platform_observability


def example_1_basic_config_usage():
    """Example 1: Basic configuration usage"""
    print("=== Example 1: Basic Configuration Usage ===")
    
    # Get configuration for current environment
    config = Config.get_config()
    
    print(f"Current Environment: {Config.ENV}")
    print(f"Catalog: {config.catalog}")
    print(f"Bronze Schema: {config.bronze_schema}")
    print(f"Silver Schema: {config.silver_schema}")
    print(f"Gold Schema: {config.gold_schema}")
    print(f"Overlap Hours: {config.overlap_hours}")
    print(f"Log Level: {config.log_level}")
    print(f"Monitoring Enabled: {config.enable_monitoring}")
    print(f"Alerts Enabled: {config.enable_alerts}")
    print()


def example_2_table_name_generation():
    """Example 2: Table name generation"""
    print("=== Example 2: Table Name Generation ===")
    
    # Using config instance methods
    config = Config.get_config()
    bronze_table = config.get_table_name("bronze", "brz_billing_usage")
silver_table = config.get_table_name("silver", "slv_usage_txn")
    gold_table = config.get_table_name("gold", "gld_fact_usage_priced_day")
    
    print(f"Bronze Table: {bronze_table}")
    print(f"Silver Table: {silver_table}")
    print(f"Gold Table: {gold_table}")
    print()


def example_3_sql_operations():
    """Example 3: SQL operations with parameterization"""
    print("=== Example 3: SQL Operations with Parameterization ===")
    
    # List available SQL operations
    operations = sql_manager.get_available_operations()
    print(f"Available SQL operations: {operations}")
    
    # Get parameterized SQL for bronze operations
    print("\n--- Bronze Operations ---")
    for op in ["upsert_billing_usage", "upsert_list_prices"]:
        if op in operations:
            sql = sql_manager.parameterize_sql_with_catalog_schema(
                op,
                target_table="my_target_table",
                source_table="my_source_view"
            )
            print(f"\n{op}:")
            print(sql[:200] + "..." if len(sql) > 200 else sql)
    
    # Get parameterized SQL for bootstrap operations
    print("\n--- Bootstrap Operations ---")
    bootstrap_ops = ["processing_offsets", "bronze_tables_bootstrap"]
    for op in bootstrap_ops:
        if op in operations:
            sql = sql_manager.parameterize_sql_with_catalog_schema(op)
            print(f"\n{op}:")
            print(sql[:200] + "..." if len(sql) > 200 else sql)
    print()


def example_4_environment_specific_config():
    """Example 4: Environment-specific configuration"""
    print("=== Example 4: Environment-Specific Configuration ===")
    
    # Show how configuration changes based on environment
    environments = ["dev", "prod"]
    
    for env in environments:
        print(f"\n--- {env.upper()} Environment ---")
        
        # Temporarily set environment
        import os
        original_env = os.environ.get("ENVIRONMENT")
        os.environ["ENVIRONMENT"] = env
        
        try:
            config = Config.get_config()
            print(f"Overlap Hours: {config.overlap_hours}")
            print(f"Log Level: {config.log_level}")
            print(f"Alerts Enabled: {config.enable_alerts}")
        finally:
            # Restore original environment
            if original_env:
                os.environ["ENVIRONMENT"] = original_env
            else:
                os.environ.pop("ENVIRONMENT", None)
    print()


def example_5_sql_parameterizer_usage():
    """Example 5: SQL Parameterizer usage"""
    print("=== Example 5: SQL Parameterizer Usage ===")
    
    # This would typically be run in a Databricks environment
    print("Note: This example shows the interface but requires a SparkSession")
    print("to actually execute SQL operations.")
    
    # Example of how to use the parameterizer
    print("\nInterface:")
    print("- SQLParameterizer(spark).bootstrap_catalog_schemas()")
    print("- SQLParameterizer(spark).bootstrap_bronze_tables()")
    print("- SQLParameterizer(spark).create_processing_state()")
    print("- SQLParameterizer(spark).apply_performance_optimizations()")
    print("- SQLParameterizer(spark).create_gold_views()")
    print("- SQLParameterizer(spark).full_bootstrap()")
    print()


def example_6_custom_environment():
    """Example 6: Custom environment configuration"""
    print("=== Example 6: Custom Environment Configuration ===")
    
    # Show how to override specific values
    import os
    
    # Set custom values
    os.environ.update({
        "ENVIRONMENT": "custom",
        "CATALOG": "my_custom_catalog",
        "BRONZE_SCHEMA": "my_bronze",
        "SILVER_SCHEMA": "my_silver",
        "GOLD_SCHEMA": "my_gold",
        "OVERLAP_HOURS": "120",
        "TIMEZONE": "America/New_York",
        "LOG_LEVEL": "DEBUG"
    })
    
    try:
        config = Config.get_config()
        print(f"Custom Catalog: {config.catalog}")
        print(f"Custom Bronze Schema: {config.bronze_schema}")
        print(f"Custom Silver Schema: {config.silver_schema}")
        print(f"Custom Gold Schema: {config.gold_schema}")
        print(f"Custom Overlap Hours: {config.overlap_hours}")
        print(f"Custom Timezone: {config.timezone}")
        print(f"Custom Log Level: {config.log_level}")
        
        # Show table names with custom config
        custom_table = config.get_table_name("bronze", "test_table")
        print(f"Custom Table Name: {custom_table}")
        
    finally:
        # Clean up environment variables
        for key in ["ENVIRONMENT", "CATALOG", "BRONZE_SCHEMA", "SILVER_SCHEMA", 
                   "GOLD_SCHEMA", "OVERLAP_HOURS", "TIMEZONE", "LOG_LEVEL"]:
            os.environ.pop(key, None)
    print()


def example_7_cdf_usage():
    """Example 7: CDF usage with the new implementation"""
    print("=== Example 7: CDF Usage ===")
    
    print("CDF functions available:")
    print("- read_cdf(spark, table, source_name, change_types)")
    print("- commit_processing_state(spark, source_name, version)")
    print()
    
    print("Example usage:")
    print("from libs.cdf import read_cdf, commit_processing_offset")
    print()
    print("# Read CDF data")
    print("df, end_version = read_cdf(spark, 'table_name', 'source_name')")
    print()
    print("# Commit processing state after processing")
print("commit_processing_offset(spark, 'source_name', end_version)")
    print()


def main():
    """Run all examples"""
    print("🚀 Platform Observability - Usage Examples")
    print("=" * 50)
    
    example_1_basic_config_usage()
    example_2_table_name_generation()
    example_3_sql_operations()
    example_4_environment_specific_config()
    example_5_sql_parameterizer_usage()
    example_6_custom_environment()
    example_7_cdf_usage()
    
    print("✅ All examples completed!")
    print("\nTo use in Databricks:")
    print("1. Import the required modules")
    print("2. Create a SQLParameterizer instance with your SparkSession")
    print("3. Call the bootstrap methods as needed")
    print("4. Use the config methods for table names and other configuration")
    print("5. Use CDF functions for change data feed operations")


if __name__ == "__main__":
    main()
