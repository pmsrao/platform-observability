"""
Test SQL Manager functionality
"""

import unittest
import sys
import os
from pathlib import Path

# Add libs to path for testing
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'libs'))

from sql_manager import sql_manager


class TestSQLManager(unittest.TestCase):
    """Test SQL Manager functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.sql_manager = sql_manager
    
    def test_get_available_operations(self):
        """Test getting available SQL operations"""
        operations = self.sql_manager.get_available_operations()
        
        # Should have operations from different directories
        self.assertIsInstance(operations, list)
        self.assertGreater(len(operations), 0)
        
        # Check for expected operations
        expected_operations = [
            'config/bootstrap_catalog_schemas',
            'bronze/bronze_tables',
            'silver/silver_tables',
            'gold/gold_dimensions'
        ]
        
        for expected_op in expected_operations:
            self.assertIn(expected_op, operations, f"Expected operation {expected_op} not found")
    
    def test_get_sql_file_path_explicit_paths(self):
        """Test getting SQL file paths with explicit folder paths"""
        test_cases = [
            ('config/bootstrap_catalog_schemas', 'sql/config/bootstrap_catalog_schemas.sql'),
            ('bronze/bronze_tables', 'sql/bronze/bronze_tables.sql'),
            ('silver/silver_tables', 'sql/silver/silver_tables.sql'),
            ('gold/gold_dimensions', 'sql/gold/gold_dimensions.sql'),
            ('bronze/operations/upsert_billing_usage', 'sql/bronze/operations/upsert_billing_usage.sql')
        ]
        
        for operation, expected_path in test_cases:
            with self.subTest(operation=operation):
                try:
                    actual_path = self.sql_manager.get_sql_file_path(operation)
                    self.assertTrue(actual_path.exists(), f"File {actual_path} does not exist")
                    self.assertEqual(str(actual_path), expected_path)
                except FileNotFoundError as e:
                    self.fail(f"File not found for operation {operation}: {e}")
    
    def test_get_sql_file_path_nonexistent(self):
        """Test getting SQL file path for non-existent operation"""
        with self.assertRaises(FileNotFoundError):
            self.sql_manager.get_sql_file_path('nonexistent/operation')
    
    def test_load_sql(self):
        """Test loading SQL content"""
        # Test loading a known SQL file
        try:
            sql_content = self.sql_manager.load_sql('config/bootstrap_catalog_schemas')
            self.assertIsInstance(sql_content, str)
            self.assertGreater(len(sql_content), 0)
        except FileNotFoundError:
            self.skipTest("SQL file not found - skipping load test")
    
    def test_parameterize_sql(self):
        """Test SQL parameterization"""
        # Test with a simple parameter
        try:
            sql_content = self.sql_manager.load_sql('config/bootstrap_catalog_schemas')
            if '{catalog}' in sql_content:
                parameterized_sql = self.sql_manager.parameterize_sql(
                    'config/bootstrap_catalog_schemas', 
                    catalog='test_catalog'
                )
                self.assertIn('test_catalog', parameterized_sql)
                self.assertNotIn('{catalog}', parameterized_sql)
        except FileNotFoundError:
            self.skipTest("SQL file not found - skipping parameterization test")
    
    def test_validate_sql(self):
        """Test SQL validation"""
        # Test valid operation
        self.assertTrue(self.sql_manager.validate_sql('config/bootstrap_catalog_schemas'))
        
        # Test invalid operation
        self.assertFalse(self.sql_manager.validate_sql('nonexistent/operation'))


if __name__ == '__main__':
    unittest.main()