import os
import pytest
from unittest.mock import patch
from config import Config, EnvironmentConfig

class TestConfig:
    """Test configuration management"""
    
    def test_default_config(self):
        """Test default configuration values"""
        with patch.dict(os.environ, {}, clear=True):
            config = Config.get_config()
            
            assert config.catalog == "platform_observability"
            assert config.bronze_schema == "plt_bronze"
            assert config.silver_schema == "plt_silver"
            assert config.gold_schema == "plt_gold"
            assert config.overlap_hours == 48
            assert config.timezone == "Asia/Kolkata"
            assert config.log_level == "INFO"
            assert config.enable_monitoring is True
            assert config.enable_alerts is True
    
    def test_dev_environment(self):
        """Test development environment configuration"""
        with patch.dict(os.environ, {"ENVIRONMENT": "dev"}, clear=True):
            config = Config.get_config()
            
            assert config.overlap_hours == 72
            assert config.log_level == "DEBUG"
            assert config.enable_alerts is False
    
    def test_prod_environment(self):
        """Test production environment configuration"""
        with patch.dict(os.environ, {"ENVIRONMENT": "prod"}, clear=True):
            config = Config.get_config()
            
            assert config.overlap_hours == 48
            assert config.log_level == "INFO"
            assert config.enable_alerts is True
    
    def test_environment_variable_override(self):
        """Test environment variable overrides"""
        with patch.dict(os.environ, {
            "ENVIRONMENT": "dev",
            "OVERLAP_HOURS": "96",
            "TIMEZONE": "UTC",
            "LOG_LEVEL": "ERROR"
        }, clear=True):
            config = Config.get_config()
            
            assert config.overlap_hours == 96
            assert config.timezone == "UTC"
            assert config.log_level == "ERROR"
    
    def test_invalid_environment(self):
        """Test handling of invalid environment"""
        with patch.dict(os.environ, {"ENVIRONMENT": "invalid"}, clear=True):
            config = Config.get_config()
            
            # Should fall back to default values
            assert config.overlap_hours == 48
            assert config.log_level == "INFO"
    
    def test_environment_config_dataclass(self):
        """Test EnvironmentConfig dataclass"""
        config = EnvironmentConfig(
            catalog="test_catalog",
            bronze_schema="test_bronze",
            silver_schema="test_silver",
            gold_schema="test_gold",
            overlap_hours=24,
            timezone="UTC",
            log_level="DEBUG",
            enable_monitoring=True,
            enable_alerts=False
        )
        
        assert config.catalog == "test_catalog"
        assert config.bronze_schema == "test_bronze"
        assert config.silver_schema == "test_silver"
        assert config.gold_schema == "test_gold"
        assert config.overlap_hours == 24
        assert config.timezone == "UTC"
        assert config.log_level == "DEBUG"
        assert config.enable_monitoring is True
        assert config.enable_alerts is False
    
    def test_environment_config_table_name_methods(self):
        """Test EnvironmentConfig table and schema name methods"""
        config = EnvironmentConfig(
            catalog="test_catalog",
            bronze_schema="test_bronze",
            silver_schema="test_silver",
            gold_schema="test_gold",
            overlap_hours=24,
            timezone="UTC",
            log_level="DEBUG",
            enable_monitoring=True,
            enable_alerts=False
        )
        
        # Test table name generation
        assert config.get_table_name("bronze", "test_table") == "test_catalog.test_bronze.test_table"
        assert config.get_table_name("silver", "test_table") == "test_catalog.test_silver.test_table"
        assert config.get_table_name("gold", "test_table") == "test_catalog.test_gold.test_table"
        
        # Test schema name generation
        assert config.get_schema_name("bronze") == "test_catalog.test_bronze"
        assert config.get_schema_name("silver") == "test_catalog.test_silver"
        assert config.get_schema_name("gold") == "test_catalog.test_gold"
    
    def test_invalid_schema_name(self):
        """Test error handling for invalid schema names"""
        config = EnvironmentConfig(
            catalog="test_catalog",
            bronze_schema="test_bronze",
            silver_schema="test_silver",
            gold_schema="test_gold",
            overlap_hours=24,
            timezone="UTC",
            log_level="DEBUG",
            enable_monitoring=True,
            enable_alerts=False
        )
        
        # Test invalid schema names
        with pytest.raises(ValueError, match="Invalid schema 'invalid'. Must be one of: bronze, silver, gold"):
            config.get_table_name("invalid", "test_table")
        
        with pytest.raises(ValueError, match="Invalid schema 'invalid'. Must be one of: bronze, silver, gold"):
            config.get_schema_name("invalid")
