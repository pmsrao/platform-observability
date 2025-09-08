"""
Path Setup Utility for Platform Observability

This module provides centralized path setup functionality to eliminate code duplication
across notebooks and Python files. It handles both local development and Databricks
workspace environments.

Usage:
    from libs.path_setup import setup_paths
    setup_paths()
    
    # Now you can import from config and other libs
    from config import Config
"""

import sys
import os
from pathlib import Path


def setup_paths():
    """
    Setup Python paths for both local development and Databricks workspace environments.
    
    This function:
    1. Adds the libs directory to sys.path for local development
    2. Adds common Databricks workspace paths for notebook execution
    3. Handles the case where __file__ is not available (Databricks notebooks)
    
    Returns:
        list: List of paths that were added to sys.path
    """
    added_paths = []
    
    # Add libs to path for local development
    try:
        # This works in local development
        current_dir = os.path.dirname(os.path.abspath(__file__))
        libs_dir = os.path.dirname(current_dir)  # Go up one level from libs/ to project root
        if libs_dir not in sys.path:
            sys.path.append(libs_dir)
            added_paths.append(libs_dir)
    except NameError:
        # __file__ is not available in Databricks notebooks
        pass
    
    # For Databricks workspace - try multiple possible paths
    workspace_paths = [
        '/Workspace/Repos/platform-observability',
        '/Workspace/Repos/accelerators/platform-observability'
        '/Workspace/Users/podilapalls@gmail.com/platform-observability'
    ]
    
    for workspace_path in workspace_paths:
        if workspace_path not in sys.path:
            sys.path.append(workspace_path)
            added_paths.append(workspace_path)
    
    return added_paths


def setup_paths_and_import_config():
    """
    Setup paths and import Config in one call.
    
    This is a convenience function for the most common use case.
    
    Returns:
        Config: The Config class instance
    """
    setup_paths()
    from config import Config
    return Config


# Auto-setup when module is imported
if __name__ != "__main__":
    setup_paths()
