import os
from typing import Dict, Any, Optional
from pathlib import Path
from config import Config

class SQLManager:
    """Manages SQL file operations and parameter substitution"""
    
    def __init__(self, sql_directory: str = "sql"):
        self.sql_directory = Path(sql_directory)
        self._sql_cache: Dict[str, str] = {}
        self._config = Config.get_config()
    
    def get_sql_file_path(self, operation: str) -> Path:
        """Get the full path to a SQL file using explicit path"""
        # Handle explicit paths (e.g., "config/bootstrap_catalog_schemas", "bronze/bronze_tables_bootstrap")
        if "/" in operation:
            # Explicit path provided
            sql_file_path = self.sql_directory / f"{operation}.sql"
        else:
            # Fallback to root sql directory for backward compatibility
            sql_file_path = self.sql_directory / f"{operation}.sql"
        
        if not sql_file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
        
        return sql_file_path
    
    def load_sql(self, operation: str) -> str:
        """Load SQL content from file with caching"""
        if operation not in self._sql_cache:
            sql_file = self.get_sql_file_path(operation)
            
            if not sql_file.exists():
                raise FileNotFoundError(f"SQL file not found: {sql_file}")
            
            with open(sql_file, 'r') as f:
                self._sql_cache[operation] = f.read()
        
        return self._sql_cache[operation]
    
    def parameterize_sql(self, operation: str, **kwargs) -> str:
        """Load SQL and substitute parameters"""
        sql_content = self.load_sql(operation)
        
        # Simple parameter substitution
        for key, value in kwargs.items():
            placeholder = f"{{{key}}}"
            if placeholder in sql_content:
                sql_content = sql_content.replace(placeholder, str(value))
        
        return sql_content
    
    def parameterize_sql_with_catalog_schema(self, operation: str, **kwargs) -> str:
        """Load SQL and substitute parameters including catalog and schema placeholders"""
        sql_content = self.load_sql(operation)
        
        # Add catalog and schema placeholders if not provided
        if "catalog" not in kwargs:
            kwargs["catalog"] = self._config.catalog
        if "bronze_schema" not in kwargs:
            kwargs["bronze_schema"] = self._config.bronze_schema
        if "silver_schema" not in kwargs:
            kwargs["silver_schema"] = self._config.silver_schema
        if "gold_schema" not in kwargs:
            kwargs["gold_schema"] = self._config.gold_schema
        
        # Simple parameter substitution
        for key, value in kwargs.items():
            placeholder = f"{{{key}}}"
            if placeholder in sql_content:
                sql_content = sql_content.replace(placeholder, str(value))
        
        return sql_content
    
    def get_parameterized_table_name(self, schema: str, table: str) -> str:
        """Get parameterized table name for SQL operations"""
        return self._config.get_table_name(schema, table)
    
    def get_parameterized_schema_name(self, schema: str) -> str:
        """Get parameterized schema name for SQL operations"""
        return self._config.get_schema_name(schema)
    
    def get_available_operations(self) -> list:
        """Get list of available SQL operations with explicit paths"""
        operations = []
        
        # Check specific subdirectories and include the path prefix
        for subdir in ["bronze", "bronze/operations", "silver", "gold", "config"]:
            dir_path = self.sql_directory / subdir
            if dir_path.exists():
                for sql_file in dir_path.glob("*.sql"):
                    # Include the subdirectory path in the operation name
                    relative_path = sql_file.relative_to(self.sql_directory)
                    operation_name = str(relative_path.with_suffix(''))  # Remove .sql extension
                    operations.append(operation_name)
        
        return sorted(operations)
    
    def validate_sql(self, operation: str) -> bool:
        """Validate that a SQL operation exists and is readable"""
        try:
            sql_file = self.get_sql_file_path(operation)
            return sql_file.exists() and sql_file.is_file()
        except Exception:
            return False
    
    def reload_sql(self, operation: Optional[str] = None):
        """Reload SQL files (useful for development)"""
        if operation:
            if operation in self._sql_cache:
                del self._sql_cache[operation]
        else:
            self._sql_cache.clear()
    
    def get_sql_with_placeholders(self, operation: str) -> str:
        """Get SQL content with catalog/schema placeholders for documentation"""
        sql_content = self.load_sql(operation)
        
        # Replace hardcoded catalog/schema with placeholders for documentation
        sql_content = sql_content.replace(self._config.catalog, "{catalog}")
        sql_content = sql_content.replace(self._config.bronze_schema, "{bronze_schema}")
        sql_content = sql_content.replace(self._config.silver_schema, "{silver_schema}")
        sql_content = sql_content.replace(self._config.gold_schema, "{gold_schema}")
        
        return sql_content

# Global SQL manager instance
sql_manager = SQLManager()
