import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, mock_open

from libs.sql_manager import SQLManager

class TestSQLManager:
    """Test SQL file manager functionality"""
    
    def test_sql_manager_initialization(self):
        """Test SQL manager initialization"""
        manager = SQLManager("test_sql_dir")
        assert manager.sql_directory == Path("test_sql_dir")
        assert manager._sql_cache == {}
    
    def test_get_sql_file_path(self):
        """Test SQL file path generation"""
        manager = SQLManager("test_sql_dir")
        path = manager.get_sql_file_path("test_operation")
        expected = Path("test_sql_dir") / "bronze_operations" / "test_operation.sql"
        assert path == expected
    
    def test_load_sql_file_not_found(self):
        """Test loading non-existent SQL file"""
        manager = SQLManager("nonexistent_dir")
        
        with pytest.raises(FileNotFoundError):
            manager.load_sql("test_operation")
    
    def test_load_sql_file_success(self):
        """Test successful SQL file loading"""
        # Create temporary directory with SQL file
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_content = "SELECT * FROM test_table"
            sql_file.write_text(sql_content)
            
            manager = SQLManager(temp_dir)
            loaded_sql = manager.load_sql("test_operation")
            
            assert loaded_sql == sql_content
            assert "test_operation" in manager._sql_cache
    
    def test_load_sql_caching(self):
        """Test SQL file caching"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_content = "SELECT * FROM test_table"
            sql_file.write_text(sql_content)
            
            manager = SQLManager(temp_dir)
            
            # First load
            first_load = manager.load_sql("test_operation")
            
            # Modify file content
            sql_file.write_text("SELECT * FROM modified_table")
            
            # Second load should return cached content
            second_load = manager.load_sql("test_operation")
            
            assert first_load == second_load
            assert first_load == "SELECT * FROM test_table"
    
    def test_parameterize_sql(self):
        """Test SQL parameterization"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_content = "MERGE INTO {target_table} USING {source_table}"
            sql_file.write_text(sql_content)
            
            manager = SQLManager(temp_dir)
            parameterized_sql = manager.parameterize_sql(
                "test_operation",
                target_table="test.target",
                source_table="test.source"
            )
            
            expected = "MERGE INTO test.target USING test.source"
            assert parameterized_sql == expected
    
    def test_parameterize_sql_no_parameters(self):
        """Test SQL parameterization with no parameters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_content = "SELECT * FROM test_table"
            sql_file.write_text(sql_content)
            
            manager = SQLManager(temp_dir)
            parameterized_sql = manager.parameterize_sql("test_operation")
            
            assert parameterized_sql == sql_content
    
    def test_get_available_operations(self):
        """Test getting available SQL operations"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            # Create multiple SQL files
            operations = ["op1", "op2", "op3"]
            for op in operations:
                sql_file = sql_dir / f"{op}.sql"
                sql_file.write_text(f"-- {op} SQL")
            
            manager = SQLManager(temp_dir)
            available_ops = manager.get_available_operations()
            
            assert set(available_ops) == set(operations)
    
    def test_get_available_operations_empty_directory(self):
        """Test getting operations from empty directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = SQLManager(temp_dir)
            available_ops = manager.get_available_operations()
            
            assert available_ops == []
    
    def test_validate_sql(self):
        """Test SQL validation"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_file.write_text("SELECT * FROM test_table")
            
            manager = SQLManager(temp_dir)
            
            # Valid operation
            assert manager.validate_sql("test_operation") is True
            
            # Invalid operation
            assert manager.validate_sql("nonexistent_operation") is False
    
    def test_reload_sql_specific_operation(self):
        """Test reloading specific SQL operation"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_file.write_text("SELECT * FROM test_table")
            
            manager = SQLManager(temp_dir)
            
            # Load SQL to populate cache
            manager.load_sql("test_operation")
            assert "test_operation" in manager._sql_cache
            
            # Reload specific operation
            manager.reload_sql("test_operation")
            assert "test_operation" not in manager._sql_cache
    
    def test_reload_sql_all_operations(self):
        """Test reloading all SQL operations"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_file.write_text("SELECT * FROM test_table")
            
            manager = SQLManager(temp_dir)
            
            # Load SQL to populate cache
            manager.load_sql("test_operation")
            assert len(manager._sql_cache) > 0
            
            # Reload all operations
            manager.reload_sql()
            assert len(manager._sql_cache) == 0
    
    def test_parameterize_sql_with_special_characters(self):
        """Test SQL parameterization with special characters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_content = "SELECT * FROM {table_name} WHERE id = {id}"
            sql_file.write_text(sql_content)
            
            manager = SQLManager(temp_dir)
            parameterized_sql = manager.parameterize_sql(
                "test_operation",
                table_name="test.table",
                id="123"
            )
            
            expected = "SELECT * FROM test.table WHERE id = 123"
            assert parameterized_sql == expected
    
    def test_parameterize_sql_missing_parameters(self):
        """Test SQL parameterization with missing parameters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze_operations"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_content = "SELECT * FROM {target_table} JOIN {source_table}"
            sql_file.write_text(sql_content)
            
            manager = SQLManager(temp_dir)
            parameterized_sql = manager.parameterize_sql(
                "test_operation",
                target_table="test.target"
                # Missing source_table parameter
            )
            
            # Should leave placeholder unchanged
            expected = "SELECT * FROM test.target JOIN {source_table}"
            assert parameterized_sql == expected
    
    def test_sql_manager_with_relative_paths(self):
        """Test SQL manager with relative paths"""
        manager = SQLManager(".")
        path = manager.get_sql_file_path("test_operation")
        expected = Path(".") / "bronze_operations" / "test_operation.sql"
        assert path == expected
    
    def test_sql_manager_with_absolute_paths(self):
        """Test SQL manager with absolute paths"""
        temp_dir = tempfile.mkdtemp()
        try:
            manager = SQLManager(temp_dir)
            path = manager.get_sql_file_path("test_operation")
            expected = Path(temp_dir) / "bronze_operations" / "test_operation.sql"
            assert path == expected
        finally:
            import shutil
            shutil.rmtree(temp_dir)
