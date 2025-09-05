"""
Platform Observability Library Package

This package provides core functionality for the Platform Observability solution
including configuration management, logging, monitoring, data processing, and utilities.
"""

__version__ = "1.0.0"
__author__ = "Platform Observability Team"

# Import key classes for easy access
from config import Config, EnvironmentConfig
from .sql_manager import SQLManager, sql_manager

__all__ = [
    # Configuration
    "Config",
    "EnvironmentConfig",
    
    # SQL Management
    "SQLManager",
    "sql_manager"
]
