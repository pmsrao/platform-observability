import os
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class EnvironmentConfig:
    """Environment-specific configuration"""
    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    overlap_hours: int
    timezone: str
    log_level: str
    enable_monitoring: bool
    enable_alerts: bool
    # Gold layer processing configuration
    gold_complete_refresh: bool
    gold_processing_strategy: str
    # Databricks configuration
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    
    def get_table_name(self, schema: str, table: str) -> str:
        """Get fully qualified table name from config instance"""
        schema_attr = f"{schema}_schema"
        if not hasattr(self, schema_attr):
            raise ValueError(f"Invalid schema '{schema}'. Must be one of: bronze, silver, gold")
        return f"{self.catalog}.{getattr(self, schema_attr)}.{table}"
    
    def get_schema_name(self, schema: str) -> str:
        """Get fully qualified schema name from config instance"""
        schema_attr = f"{schema}_schema"
        if not hasattr(self, schema_attr):
            raise ValueError(f"Invalid schema '{schema}'. Must be one of: bronze, silver, gold")
        return f"{self.catalog}.{getattr(self, schema_attr)}"

class Config:
    """Centralized configuration management"""
    
    # Valid environments
    VALID_ENVIRONMENTS = ["dev", "prod", "test"]
    
    # Environment detection
    ENV = os.getenv("ENVIRONMENT", "dev").lower()
    
    # Validate environment
    if ENV not in VALID_ENVIRONMENTS:
        raise ValueError(f"Invalid environment: {ENV}. Must be one of {VALID_ENVIRONMENTS}")
    
    # Base configuration
    BASE_CONFIG = {
        "catalog": "platform_observability",
        "bronze_schema": "plt_bronze",
        "silver_schema": "plt_silver", 
        "gold_schema": "plt_gold",
        "overlap_hours": 48,
        "timezone": "Asia/Kolkata",
        "log_level": "INFO",
        "enable_monitoring": True,
        "enable_alerts": True,
        # Gold layer processing configuration
        "gold_complete_refresh": False,
        "gold_processing_strategy": "updated_time",
        # Databricks configuration
        "databricks_host": os.getenv("DATABRICKS_HOST"),
        "databricks_token": os.getenv("DATABRICKS_TOKEN")
    }
    
    # Environment-specific overrides
    ENV_OVERRIDES = {
        "dev": {
            "overlap_hours": 72,  # More overlap in dev for testing
            "log_level": "DEBUG",
            "enable_alerts": False,  # No alerts in dev
            "gold_complete_refresh": True,  # Complete refresh in dev for testing
            "gold_processing_strategy": "hybrid",  # Use hybrid strategy in dev
        },
        "prod": {
            "overlap_hours": 48,
            "log_level": "INFO", 
            "enable_alerts": True,
            "gold_complete_refresh": False,  # Incremental processing in prod
            "gold_processing_strategy": "updated_time",  # Use updated_time strategy in prod
        }
    }
    
    @classmethod
    def get_config(cls) -> EnvironmentConfig:
        """Get configuration for current environment"""
        config = cls.BASE_CONFIG.copy()
        
        if cls.ENV in cls.ENV_OVERRIDES:
            config.update(cls.ENV_OVERRIDES[cls.ENV])
        
        return EnvironmentConfig(**config)

# Global config instance
config = Config.get_config()
