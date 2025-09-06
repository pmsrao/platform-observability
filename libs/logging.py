import logging
import json
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from functools import wraps
from dataclasses import dataclass, asdict
from config import Config

# Get configuration
config = Config.get_config()

@dataclass
class LogContext:
    """Context information for structured logging"""
    job_id: str
    job_run_id: str
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
    """Structured logging with JSON format and performance monitoring"""
    
    def __init__(self, name: str = None):
        self.name = name or f"{config.catalog}_observability"
        self.logger = logging.getLogger(self.name)
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(getattr(logging, config.log_level.upper()))
    
    def _make_serializable(self, obj):
        """Convert object to JSON-serializable format"""
        if obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (list, tuple)):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {str(k): self._make_serializable(v) for k, v in obj.items()}
        elif hasattr(obj, '__dict__'):
            # Handle objects with __dict__ attribute
            return self._make_serializable(obj.__dict__)
        else:
            # Convert to string for non-serializable objects
            return str(obj)
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Log structured message with additional context"""
        # Ensure all kwargs are serializable
        serializable_kwargs = self._make_serializable(kwargs)
        
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level.upper(),
            "message": message,
            "application": self.name,
            "environment": Config.ENV,
            **serializable_kwargs
        }
        
        try:
            json_log = json.dumps(log_entry)
            if level.upper() == "DEBUG":
                self.logger.debug(json_log)
            elif level.upper() == "INFO":
                self.logger.info(json_log)
            elif level.upper() == "WARNING":
                self.logger.warning(json_log)
            elif level.upper() == "ERROR":
                self.logger.error(json_log)
            elif level.upper() == "CRITICAL":
                self.logger.critical(json_log)
        except Exception as e:
            # Fallback to simple logging if JSON serialization fails
            fallback_msg = f"{message} | {str(serializable_kwargs)}"
            if level.upper() == "DEBUG":
                self.logger.debug(fallback_msg)
            elif level.upper() == "INFO":
                self.logger.info(fallback_msg)
            elif level.upper() == "WARNING":
                self.logger.warning(fallback_msg)
            elif level.upper() == "ERROR":
                self.logger.error(fallback_msg)
            elif level.upper() == "CRITICAL":
                self.logger.critical(fallback_msg)
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self._log_structured("DEBUG", message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self._log_structured("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self._log_structured("WARNING", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self._log_structured("ERROR", message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self._log_structured("CRITICAL", message, **kwargs)

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
            # Create default logger for the operation
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
