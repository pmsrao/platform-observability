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
            
            assert config.ENV == "dev"
            assert config.overlap_hours == 72
            assert config.log_level == "DEBUG"
            assert config.enable_alerts is False
    
    def test_prod_environment(self):
        """Test production environment configuration"""
        with patch.dict(os.environ, {"ENVIRONMENT": "prod"}, clear=True):
            config = Config.get_config()
            
            assert config.ENV == "prod"
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
    
    def test_get_table_name(self):
        """Test table name generation"""
        with patch.dict(os.environ, {"ENVIRONMENT": "dev"}, clear=True):
            table_name = Config.get_table_name("bronze", "test_table")
            assert table_name == "platform_observability.plt_bronze.test_table"
    
    def test_get_schema_name(self):
        """Test schema name generation"""
        with patch.dict(os.environ, {"ENVIRONMENT": "dev"}, clear=True):
            schema_name = Config.get_schema_name("bronze")
            assert schema_name == "platform_observability.plt_bronze"
    
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
