import pytest
import json
import time
from unittest.mock import Mock, patch
from datetime import datetime

from libs.logging import (
    StructuredLogger, 
    PerformanceMonitor, 
    performance_monitor,
    LogContext,
    PerformanceMetrics
)

class TestStructuredLogger:
    """Test structured logging functionality"""
    
    def test_logger_initialization(self):
        """Test logger initialization"""
        logger = StructuredLogger("test_logger")
        assert logger.logger.name == "test_logger"
        assert logger.context is None
    
    def test_set_context(self):
        """Test setting logging context"""
        logger = StructuredLogger("test_logger")
        logger.set_context("job123", "run456", "test_pipeline", "dev")
        
        assert logger.context is not None
        assert logger.context.job_id == "job123"
        assert logger.context.run_id == "run456"
        assert logger.context.pipeline_name == "test_pipeline"
        assert logger.context.environment == "dev"
        assert logger.context.correlation_id is not None
    
    def test_format_message_with_context(self):
        """Test message formatting with context"""
        logger = StructuredLogger("test_logger")
        logger.set_context("job123", "run456", "test_pipeline", "dev")
        
        message = logger._format_message("INFO", "Test message", {"key": "value"})
        log_entry = json.loads(message)
        
        assert log_entry["level"] == "INFO"
        assert log_entry["message"] == "Test message"
        assert log_entry["extra"]["key"] == "value"
        assert log_entry["job_id"] == "job123"
        assert log_entry["run_id"] == "run456"
        assert log_entry["pipeline_name"] == "test_pipeline"
        assert log_entry["environment"] == "dev"
        assert "correlation_id" in log_entry
    
    def test_format_message_without_context(self):
        """Test message formatting without context"""
        logger = StructuredLogger("test_logger")
        
        message = logger._format_message("INFO", "Test message", {"key": "value"})
        log_entry = json.loads(message)
        
        assert log_entry["level"] == "INFO"
        assert log_entry["message"] == "Test message"
        assert log_entry["extra"]["key"] == "value"
        assert "job_id" not in log_entry
    
    def test_log_levels(self):
        """Test all log levels"""
        logger = StructuredLogger("test_logger")
        
        # Mock the underlying logger
        with patch.object(logger.logger, 'info') as mock_info, \
             patch.object(logger.logger, 'warning') as mock_warning, \
             patch.object(logger.logger, 'error') as mock_error, \
             patch.object(logger.logger, 'debug') as mock_debug, \
             patch.object(logger.logger, 'critical') as mock_critical:
            
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")
            logger.debug("Debug message")
            logger.critical("Critical message")
            
            assert mock_info.called
            assert mock_warning.called
            assert mock_error.called
            assert mock_debug.called
            assert mock_critical.called

class TestPerformanceMonitor:
    """Test performance monitoring functionality"""
    
    def test_monitor_initialization(self):
        """Test performance monitor initialization"""
        logger = StructuredLogger("test_logger")
        monitor = PerformanceMonitor(logger)
        
        assert monitor.logger == logger
        assert monitor.metrics == {}
    
    def test_start_operation(self):
        """Test starting an operation"""
        logger = StructuredLogger("test_logger")
        monitor = PerformanceMonitor(logger)
        
        with patch.object(logger, 'info') as mock_info:
            operation_id = monitor.start_operation("test_operation")
            
            assert operation_id.startswith("test_operation_")
            assert operation_id in monitor.metrics
            assert mock_info.called
    
    def test_complete_operation(self):
        """Test completing an operation"""
        logger = StructuredLogger("test_logger")
        monitor = PerformanceMonitor(logger)
        
        operation_id = monitor.start_operation("test_operation")
        
        with patch.object(logger, 'info') as mock_info:
            monitor.complete_operation(
                operation_id=operation_id,
                records_processed=100,
                records_failed=5,
                records_success=95,
                error_count=1,
                warning_count=2
            )
            
            metrics = monitor.metrics[operation_id]
            assert metrics.records_processed == 100
            assert metrics.records_failed == 5
            assert metrics.records_success == 95
            assert metrics.error_count == 1
            assert metrics.warning_count == 2
            assert metrics.end_time is not None
            assert metrics.duration_seconds is not None
            assert mock_info.called
    
    def test_complete_nonexistent_operation(self):
        """Test completing a non-existent operation"""
        logger = StructuredLogger("test_logger")
        monitor = PerformanceMonitor(logger)
        
        # Should not raise an error
        monitor.complete_operation("nonexistent_id", records_processed=100)
    
    def test_performance_summary_logging(self):
        """Test performance summary logging"""
        logger = StructuredLogger("test_logger")
        monitor = PerformanceMonitor(logger)
        
        operation_id = monitor.start_operation("test_operation")
        time.sleep(0.1)  # Small delay to ensure duration > 0
        
        with patch.object(logger, 'info') as mock_info:
            monitor.complete_operation(operation_id, records_processed=100)
            
            # Should log performance summary
            assert mock_info.call_count >= 2  # At least start + completion + summary
    
    def test_metrics_completion(self):
        """Test metrics completion functionality"""
        metrics = PerformanceMetrics(start_time=datetime.utcnow())
        
        assert metrics.end_time is None
        assert metrics.duration_seconds is None
        
        completed_metrics = metrics.complete()
        
        assert completed_metrics.end_time is not None
        assert completed_metrics.duration_seconds is not None
        assert completed_metrics.duration_seconds >= 0

class TestPerformanceMonitorDecorator:
    """Test performance monitor decorator"""
    
    def test_performance_monitor_decorator_success(self):
        """Test successful operation with decorator"""
        logger = StructuredLogger("test_logger")
        
        @performance_monitor("test_operation")
        def test_function():
            return "success"
        
        with patch.object(logger, 'info') as mock_info:
            result = test_function()
            
            assert result == "success"
            assert mock_info.called
    
    def test_performance_monitor_decorator_failure(self):
        """Test failed operation with decorator"""
        logger = StructuredLogger("test_logger")
        
        @performance_monitor("test_operation")
        def test_function():
            raise ValueError("Test error")
        
        with patch.object(logger, 'error') as mock_error:
            with pytest.raises(ValueError):
                test_function()
            
            assert mock_error.called
    
    def test_performance_monitor_with_dataframe(self):
        """Test performance monitor with DataFrame-like object"""
        logger = StructuredLogger("test_logger")
        
        # Mock DataFrame with count method
        mock_df = Mock()
        mock_df.count.return_value = 100
        
        @performance_monitor("test_operation")
        def test_function():
            return mock_df
        
        with patch.object(logger, 'info') as mock_info:
            result = test_function()
            
            assert result == mock_df
            assert mock_info.called

class TestLogContext:
    """Test log context dataclass"""
    
    def test_log_context_creation(self):
        """Test log context creation"""
        context = LogContext(
            job_id="job123",
            run_id="run456",
            pipeline_name="test_pipeline",
            environment="dev",
            timestamp="2024-01-01T00:00:00",
            correlation_id="corr123"
        )
        
        assert context.job_id == "job123"
        assert context.run_id == "run456"
        assert context.pipeline_name == "test_pipeline"
        assert context.environment == "dev"
        assert context.timestamp == "2024-01-01T00:00:00"
        assert context.correlation_id == "corr123"
    
    def test_log_context_asdict(self):
        """Test log context serialization"""
        from dataclasses import asdict
        
        context = LogContext(
            job_id="job123",
            run_id="run456",
            pipeline_name="test_pipeline",
            environment="dev",
            timestamp="2024-01-01T00:00:00",
            correlation_id="corr123"
        )
        
        context_dict = asdict(context)
        
        assert context_dict["job_id"] == "job123"
        assert context_dict["run_id"] == "run456"
        assert context_dict["pipeline_name"] == "test_pipeline"
        assert context_dict["environment"] == "dev"
        assert context_dict["timestamp"] == "2024-01-01T00:00:00"
        assert context_dict["correlation_id"] == "corr123"
