import os
from typing import Dict, Any
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

class Config:
    """Centralized configuration management"""
    
    # Environment detection
    ENV = os.getenv("ENVIRONMENT", "dev").lower()
    
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
        "enable_alerts": True
    }
    
    # Environment-specific overrides
    ENV_OVERRIDES = {
        "dev": {
            "overlap_hours": 72,  # More overlap in dev for testing
            "log_level": "DEBUG",
            "enable_alerts": False,  # No alerts in dev
        },
        "prod": {
            "overlap_hours": 48,
            "log_level": "INFO", 
            "enable_alerts": True,
        }
    }
    
    @classmethod
    def get_config(cls) -> EnvironmentConfig:
        """Get configuration for current environment"""
        config = cls.BASE_CONFIG.copy()
        
        if cls.ENV in cls.ENV_OVERRIDES:
            config.update(cls.ENV_OVERRIDES[cls.ENV])
        
        # Environment variable overrides (highest priority)
        config.update({
            "catalog": os.getenv("CATALOG", config["catalog"]),
            "bronze_schema": os.getenv("BRONZE_SCHEMA", config["bronze_schema"]),
            "silver_schema": os.getenv("SILVER_SCHEMA", config["silver_schema"]),
            "gold_schema": os.getenv("GOLD_SCHEMA", config["gold_schema"]),
            "overlap_hours": int(os.getenv("OVERLAP_HOURS", config["overlap_hours"])),
            "timezone": os.getenv("TIMEZONE", config["timezone"]),
            "log_level": os.getenv("LOG_LEVEL", config["log_level"]),
            "enable_monitoring": os.getenv("ENABLE_MONITORING", str(config["enable_monitoring"])).lower() == "true",
            "enable_alerts": os.getenv("ENABLE_ALERTS", str(config["enable_alerts"])).lower() == "true"
        })
        
        return EnvironmentConfig(**config)
    
    @classmethod
    def get_table_name(cls, schema: str, table: str) -> str:
        """Get fully qualified table name"""
        config = cls.get_config()
        return f"{config.catalog}.{getattr(config, f'{schema}_schema')}.{table}"
    
    @classmethod
    def get_schema_name(cls, schema: str) -> str:
        """Get fully qualified schema name"""
        config = cls.get_config()
        return f"{config.catalog}.{getattr(config, f'{schema}_schema')}"

# Global config instance
config = Config.get_config()
