import logging
import json
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from functools import wraps
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession

@dataclass
class LogContext:
    """Context information for structured logging"""
    job_id: str
    run_id: str
    pipeline_name: str
    environment: str
    timestamp: str
    correlation_id: str

@dataclass
class PerformanceMetrics:
    """Performance metrics for monitoring"""
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    records_processed: int = 0
    records_failed: int = 0
    records_success: int = 0
    error_count: int = 0
    warning_count: int = 0
    
    def complete(self):
        """Mark metrics as complete and calculate duration"""
        self.end_time = datetime.utcnow()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        return self

class StructuredLogger:
    """Structured logger for Databricks platform observability"""
    
    def __init__(self, name: str, spark: Optional[SparkSession] = None):
        self.logger = logging.getLogger(name)
        self.spark = spark
        self.context: Optional[LogContext] = None
        
    def set_context(self, job_id: str, run_id: str, pipeline_name: str, environment: str):
        """Set logging context for the current execution"""
        self.context = LogContext(
            job_id=job_id,
            run_id=run_id,
            pipeline_name=pipeline_name,
            environment=environment,
            timestamp=datetime.utcnow().isoformat(),
            correlation_id=str(uuid.uuid4())
        )
    
    def _format_message(self, level: str, message: str, extra: Optional[Dict[str, Any]] = None) -> str:
        """Format log message as structured JSON"""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "extra": extra or {}
        }
        
        if self.context:
            log_entry.update(asdict(self.context))
        
        return json.dumps(log_entry)
    
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log info message"""
        self.logger.info(self._format_message("INFO", message, extra))
    
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log warning message"""
        self.logger.warning(self._format_message("WARNING", message, extra))
    
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log error message"""
        self.logger.error(self._format_message("ERROR", message, extra))
    
    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log debug message"""
        self.logger.debug(self._format_message("DEBUG", message, extra))
    
    def critical(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log critical message"""
        self.logger.critical(self._format_message("CRITICAL", message, extra))

class PerformanceMonitor:
    """Monitor and track performance metrics"""
    
    def __init__(self, logger: StructuredLogger):
        self.logger = logger
        self.metrics: Dict[str, PerformanceMetrics] = {}
    
    def start_operation(self, operation_name: str) -> str:
        """Start monitoring an operation"""
        operation_id = f"{operation_name}_{uuid.uuid4().hex[:8]}"
        self.metrics[operation_id] = PerformanceMetrics(
            start_time=datetime.utcnow()
        )
        
        self.logger.info(f"Started operation: {operation_name}", {
            "operation_id": operation_id,
            "operation_name": operation_name
        })
        
        return operation_id
    
    def complete_operation(self, operation_id: str, 
                          records_processed: int = 0,
                          records_failed: int = 0,
                          records_success: int = 0,
                          error_count: int = 0,
                          warning_count: int = 0):
        """Complete monitoring an operation"""
        if operation_id in self.metrics:
            metrics = self.metrics[operation_id]
            metrics.records_processed = records_processed
            metrics.records_failed = records_failed
            metrics.records_success = records_success
            metrics.error_count = error_count
            metrics.warning_count = warning_count
            
            completed_metrics = metrics.complete()
            
            self.logger.info(f"Completed operation: {operation_id}", {
                "operation_id": operation_id,
                "performance_metrics": asdict(completed_metrics)
            })
            
            # Log performance summary
            self._log_performance_summary(operation_id, completed_metrics)
    
    def _log_performance_summary(self, operation_id: str, metrics: PerformanceMetrics):
        """Log performance summary for monitoring"""
        if metrics.duration_seconds:
            throughput = metrics.records_processed / metrics.duration_seconds if metrics.duration_seconds > 0 else 0
            
            self.logger.info("Performance Summary", {
                "operation_id": operation_id,
                "duration_seconds": round(metrics.duration_seconds, 2),
                "records_per_second": round(throughput, 2),
                "success_rate": round((metrics.records_success / max(metrics.records_processed, 1)) * 100, 2),
                "error_rate": round((metrics.records_failed / max(metrics.records_processed, 1)) * 100, 2)
            })

def performance_monitor(operation_name: str):
    """Decorator to automatically monitor function performance"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract logger from args if available
            logger = None
            for arg in args:
                if hasattr(arg, 'logger') and isinstance(arg.logger, StructuredLogger):
                    logger = arg.logger
                    break
            
            if not logger:
                # Create default logger
                logger = StructuredLogger(f"{func.__module__}.{func.__name__}")
            
            monitor = PerformanceMonitor(logger)
            operation_id = monitor.start_operation(operation_name)
            
            try:
                start_time = time.time()
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Try to extract record counts from result if it's a DataFrame
                records_processed = 0
                if hasattr(result, 'count'):
                    try:
                        records_processed = result.count()
                    except:
                        pass
                
                monitor.complete_operation(
                    operation_id=operation_id,
                    records_processed=records_processed,
                    records_success=records_processed,
                    error_count=0
                )
                
                return result
                
            except Exception as e:
                monitor.complete_operation(
                    operation_id=operation_id,
                    error_count=1
                )
                logger.error(f"Operation failed: {operation_name}", {
                    "error": str(e),
                    "operation_id": operation_id
                })
                raise
        
        return wrapper
    return decorator

# Global logger instance
logger = StructuredLogger("platform_observability")
