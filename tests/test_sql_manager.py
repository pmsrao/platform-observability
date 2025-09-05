import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, mock_open

from libs.sql_manager import SQLManager

class TestSQLManager:
    """Test cases for SQLManager class"""
    
    def test_init(self):
        """Test SQLManager initialization"""
        manager = SQLManager("test_sql_dir")
        assert manager.sql_directory == Path("test_sql_dir")
        assert manager._sql_cache == {}
    
    def test_get_sql_file_path(self):
        """Test getting SQL file path"""
        manager = SQLManager("test_sql_dir")
        path = manager.get_sql_file_path("test_operation")
        expected = Path("test_sql_dir") / "bronze" / "test_operation.sql"
        assert path == expected
    
    def test_get_sql_file_path_not_found(self):
        """Test getting SQL file path when file doesn't exist"""
        manager = SQLManager("test_sql_dir")
        path = manager.get_sql_file_path("nonexistent")
        expected = Path("test_sql_dir") / "nonexistent.sql"
        assert path == expected
    
    def test_load_sql_file_not_found(self):
        """Test loading SQL file that doesn't exist"""
        manager = SQLManager("test_sql_dir")
        
        with pytest.raises(FileNotFoundError):
            manager.load_sql("test_operation")
    
    def test_load_sql_file_success(self):
        """Test successful SQL file loading"""
        # Create temporary directory with SQL file
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze"
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
            sql_dir = Path(temp_dir) / "bronze"
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
            sql_dir = Path(temp_dir) / "bronze"
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
            sql_dir = Path(temp_dir) / "bronze"
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
            sql_dir = Path(temp_dir) / "bronze"
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
    
    def test_get_available_operations_mixed_directories(self):
        """Test getting operations from mixed directory structure"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create bronze directory with SQL files
            bronze_dir = Path(temp_dir) / "bronze"
            bronze_dir.mkdir()
            
            bronze_ops = ["bronze_op1", "bronze_op2"]
            for op in bronze_ops:
                sql_file = bronze_dir / f"{op}.sql"
                sql_file.write_text(f"-- {op} SQL")
            
            # Create root SQL directory with SQL files
            root_sql_dir = Path(temp_dir)
            root_ops = ["root_op1", "root_op2"]
            for op in root_ops:
                sql_file = root_sql_dir / f"{op}.sql"
                sql_file.write_text(f"-- {op} SQL")
            
            manager = SQLManager(temp_dir)
            available_ops = manager.get_available_operations()
            
            expected_ops = bronze_ops + root_ops
            assert set(available_ops) == set(expected_ops)
    
    def test_clear_cache(self):
        """Test clearing SQL cache"""
        manager = SQLManager("test_sql_dir")
        
        # Add some items to cache
        manager._sql_cache["test1"] = "content1"
        manager._sql_cache["test2"] = "content2"
        
        assert len(manager._sql_cache) == 2
        
        # Clear cache
        manager.clear_cache()
        
        assert len(manager._sql_cache) == 0
    
    def test_parameterize_sql_with_complex_parameters(self):
        """Test SQL parameterization with complex parameter values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_content = """
            SELECT * FROM {table_name} 
            WHERE {date_column} >= '{start_date}' 
            AND {date_column} <= '{end_date}'
            AND {status_column} IN ({status_values})
            """
            sql_file.write_text(sql_content)
            
            manager = SQLManager(temp_dir)
            parameterized_sql = manager.parameterize_sql(
                "test_operation",
                table_name="my_table",
                date_column="created_date",
                start_date="2023-01-01",
                end_date="2023-12-31",
                status_column="status",
                status_values="'active', 'pending'"
            )
            
            expected = """
            SELECT * FROM my_table 
            WHERE created_date >= '2023-01-01' 
            AND created_date <= '2023-12-31'
            AND status IN ('active', 'pending')
            """
            assert parameterized_sql.strip() == expected.strip()
    
    def test_parameterize_sql_missing_parameters(self):
        """Test SQL parameterization with missing parameters"""
        with tempfile.TemporaryDirectory() as temp_dir:
            sql_dir = Path(temp_dir) / "bronze"
            sql_dir.mkdir()
            
            sql_file = sql_dir / "test_operation.sql"
            sql_content = "SELECT * FROM {table_name} WHERE {condition}"
            sql_file.write_text(sql_content)
            
            manager = SQLManager(temp_dir)
            
            # Should raise KeyError for missing parameters
            with pytest.raises(KeyError):
                manager.parameterize_sql("test_operation", table_name="test_table")
    
    def test_sql_file_path_resolution_order(self):
        """Test SQL file path resolution order (bronze first, then root)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create both directories
            bronze_dir = Path(temp_dir) / "bronze"
            bronze_dir.mkdir()
            
            # Create file in bronze directory
            bronze_file = bronze_dir / "test_operation.sql"
            bronze_file.write_text("-- Bronze operation")
            
            # Create file with same name in root directory
            root_file = Path(temp_dir) / "test_operation.sql"
            root_file.write_text("-- Root operation")
            
            manager = SQLManager(temp_dir)
            path = manager.get_sql_file_path("test_operation")
            
            # Should return bronze directory path (priority)
            expected = Path(temp_dir) / "bronze" / "test_operation.sql"
            assert path == expected
    
    def test_sql_file_path_fallback_to_root(self):
        """Test SQL file path fallback to root when not in bronze directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create only root SQL file
            root_file = Path(temp_dir) / "test_operation.sql"
            root_file.write_text("-- Root operation")
            
            manager = SQLManager(temp_dir)
            path = manager.get_sql_file_path("test_operation")
            
            # Should return root directory path
            expected = Path(temp_dir) / "test_operation.sql"
            assert path == expected
