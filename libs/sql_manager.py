import os
from typing import Dict, Any, Optional
from pathlib import Path

class SQLManager:
    """Manages SQL file operations and parameter substitution"""
    
    def __init__(self, sql_directory: str = "sql"):
        self.sql_directory = Path(sql_directory)
        self._sql_cache: Dict[str, str] = {}
    
    def get_sql_file_path(self, operation: str) -> Path:
        """Get the full path to a SQL file"""
        return self.sql_directory / "bronze_operations" / f"{operation}.sql"
    
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
    
    def get_available_operations(self) -> list:
        """Get list of available SQL operations"""
        bronze_ops_dir = self.sql_directory / "bronze_operations"
        if not bronze_ops_dir.exists():
            return []
        
        operations = []
        for sql_file in bronze_ops_dir.glob("*.sql"):
            operations.append(sql_file.stem)
        
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

# Global SQL manager instance
sql_manager = SQLManager()
