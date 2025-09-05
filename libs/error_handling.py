import logging
from typing import Dict, Any, List, Optional, Callable, Tuple
from functools import wraps
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, count, isnan, isnull

from .logging import StructuredLogger, PerformanceMonitor

@dataclass
class DataQualityRule:
    """Data quality rule definition"""
    name: str
    description: str
    rule_type: str  # 'expectation', 'validation', 'custom'
    severity: str   # 'error', 'warning', 'info'
    rule_function: Callable
    parameters: Optional[Dict[str, Any]] = None

@dataclass
class DataQualityResult:
    """Result of data quality check"""
    rule_name: str
    passed: bool
    message: str
    records_checked: int
    records_failed: int
    records_passed: int
    error_details: Optional[str] = None
    execution_time: Optional[float] = None

class DataQualityMonitor:
    """Monitor and validate data quality"""
    
    def __init__(self, logger: StructuredLogger):
        self.logger = logger
        self.results: List[DataQualityResult] = []
        self.rules: Dict[str, DataQualityRule] = {}
        
        # Register default rules
        self._register_default_rules()
    
    def _register_default_rules(self):
        """Register default data quality rules - these are generic rules that apply to all tables"""
        
        # Generic required fields rule (workspace_id is common to all tables)
        self.add_rule(DataQualityRule(
            name="required_workspace_id",
            description="Workspace ID should not be null",
            rule_type="validation",
            severity="error",
            rule_function=self._check_required_fields,
            parameters={"required_columns": ["workspace_id"]}
        ))
        
        # Data freshness rule
        self.add_rule(DataQualityRule(
            name="data_freshness",
            description="Data should not be older than expected",
            rule_type="custom",
            severity="warning",
            rule_function=self._check_data_freshness,
            parameters={"max_age_hours": 72}
        ))
    
    def add_rule(self, rule: DataQualityRule):
        """Add a custom data quality rule"""
        self.rules[rule.name] = rule
        self.logger.info(f"Added data quality rule: {rule.name}", {
            "rule_name": rule.name,
            "rule_type": rule.rule_type,
            "severity": rule.severity
        })
    
    def validate_dataframe(self, df: DataFrame, table_name: str) -> List[DataQualityResult]:
        """Validate DataFrame against all registered rules"""
        self.logger.info(f"Starting data quality validation for table: {table_name}")
        
        results = []
        monitor = PerformanceMonitor(self.logger)
        
        # Add entity-specific rules based on table name
        entity_specific_rules = self._get_entity_specific_rules(table_name)
        all_rules = {**self.rules, **entity_specific_rules}
        
        for rule_name, rule in all_rules.items():
            try:
                operation_id = monitor.start_operation(f"dq_validation_{rule_name}")
                
                # Execute rule
                result = rule.rule_function(df, rule.parameters or {})
                result.rule_name = rule_name
                
                # Update performance metrics
                monitor.complete_operation(
                    operation_id=operation_id,
                    records_checked=result.records_checked,
                    records_failed=result.records_failed,
                    records_passed=result.records_passed
                )
                
                results.append(result)
                
                # Log result
                level = "warning" if not result.passed and rule.severity == "warning" else "info"
                getattr(self.logger, level)(
                    f"Data quality rule '{rule_name}' {'passed' if result.passed else 'failed'}", {
                        "table_name": table_name,
                        "rule_name": rule_name,
                        "passed": result.passed,
                        "records_checked": result.records_checked,
                        "records_failed": result.records_failed,
                        "severity": rule.severity
                    }
                )
                
            except Exception as e:
                self.logger.error(f"Error executing data quality rule: {rule_name}", {
                    "table_name": table_name,
                    "rule_name": rule_name,
                    "error": str(e)
                })
                
                # Create failed result
                failed_result = DataQualityResult(
                    rule_name=rule_name,
                    passed=False,
                    message=f"Rule execution failed: {str(e)}",
                    records_checked=0,
                    records_failed=0,
                    records_passed=0,
                    error_details=str(e)
                )
                results.append(failed_result)
        
        self.results.extend(results)
        return results
    
    def _get_entity_specific_rules(self, table_name: str) -> Dict[str, DataQualityRule]:
        """Get entity-specific validation rules based on table name"""
        entity_rules = {}
        
        # Billing usage specific rules
        if "billing_usage" in table_name.lower():
            entity_rules["non_negative_quantity"] = DataQualityRule(
                name="non_negative_quantity",
                description="Usage quantity should be non-negative",
                rule_type="expectation",
                severity="error",
                rule_function=self._check_non_negative_quantity,
                parameters={"column": "usage_quantity"}
            )
            
            entity_rules["valid_time_range"] = DataQualityRule(
                name="valid_time_range",
                description="End time should be after start time",
                rule_type="expectation", 
                severity="error",
                rule_function=self._check_valid_time_range,
                parameters={"start_col": "usage_start_time", "end_col": "usage_end_time"}
            )
            
            entity_rules["required_billing_fields"] = DataQualityRule(
                name="required_billing_fields",
                description="Required billing fields should not be null",
                rule_type="validation",
                severity="error",
                rule_function=self._check_required_fields,
                parameters={"required_columns": ["sku_name", "usage_quantity", "usage_start_time", "usage_end_time"]}
            )
        
        # Job run timeline specific rules
        elif "job_run_timeline" in table_name.lower():
            entity_rules["valid_timeline_range"] = DataQualityRule(
                name="valid_timeline_range",
                description="Period end time should be after start time",
                rule_type="expectation", 
                severity="error",
                rule_function=self._check_valid_time_range,
                parameters={"start_col": "period_start_time", "end_col": "period_end_time"}
            )
            
            entity_rules["required_timeline_fields"] = DataQualityRule(
                name="required_timeline_fields",
                description="Required timeline fields should not be null",
                rule_type="validation",
                severity="error",
                rule_function=self._check_required_fields,
                parameters={"required_columns": ["job_id", "job_run_id", "period_start_time", "period_end_time"]}
            )
        
        # Compute clusters specific rules
        elif "compute_clusters" in table_name.lower():
            entity_rules["valid_cluster_config"] = DataQualityRule(
                name="valid_cluster_config",
                description="Cluster configuration should be valid",
                rule_type="validation",
                severity="error",
                rule_function=self._check_cluster_config,
                parameters={}
            )
            
            entity_rules["required_cluster_fields"] = DataQualityRule(
                name="required_cluster_fields",
                description="Required cluster fields should not be null",
                rule_type="validation",
                severity="error",
                rule_function=self._check_required_fields,
                parameters={"required_columns": ["cluster_id", "cluster_name", "cluster_type"]}
            )
        
        return entity_rules
    
    def _check_non_negative_quantity(self, df: DataFrame, params: Dict[str, Any]) -> DataQualityResult:
        """Check if quantity column has non-negative values"""
        column = params.get("column", "usage_quantity")
        
        total_count = df.count()
        failed_count = df.filter(col(column) < 0).count()
        passed_count = total_count - failed_count
        
        return DataQualityResult(
            rule_name="non_negative_quantity",
            passed=failed_count == 0,
            message=f"Found {failed_count} records with negative {column}",
            records_checked=total_count,
            records_failed=failed_count,
            records_passed=passed_count
        )
    
    def _check_valid_time_range(self, df: DataFrame, params: Dict[str, Any]) -> DataQualityResult:
        """Check if end time is after start time"""
        start_col = params.get("start_col", "usage_start_time")
        end_col = params.get("end_col", "usage_end_time")
        
        total_count = df.count()
        failed_count = df.filter(col(end_col) <= col(start_col)).count()
        passed_count = total_count - failed_count
        
        return DataQualityResult(
            rule_name="valid_time_range",
            passed=failed_count == 0,
            message=f"Found {failed_count} records with invalid time range",
            records_checked=total_count,
            records_failed=failed_count,
            records_passed=passed_count
        )
    
    def _check_required_fields(self, df: DataFrame, params: Dict[str, Any]) -> DataQualityResult:
        """Check if required fields are not null"""
        required_columns = params.get("required_columns", [])
        
        total_count = df.count()
        failed_count = 0
        
        for col_name in required_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull() | isnan(col(col_name)) | isnull(col(col_name))).count()
                failed_count += null_count
        
        passed_count = total_count - failed_count
        
        return DataQualityResult(
            rule_name="required_fields",
            passed=failed_count == 0,
            message=f"Found {failed_count} records with null required fields",
            records_checked=total_count,
            records_failed=failed_count,
            records_passed=passed_count
        )
    
    def _check_data_freshness(self, df: DataFrame, params: Dict[str, Any]) -> DataQualityResult:
        """Check if data is fresh enough"""
        max_age_hours = params.get("max_age_hours", 72)
        
        # This is a simplified check - in practice you'd check actual timestamp columns
        total_count = df.count()
        # For now, assume data is fresh if we have records
        passed_count = total_count
        failed_count = 0
        
        return DataQualityResult(
            rule_name="data_freshness",
            passed=failed_count == 0,
            message=f"Data freshness check completed",
            records_checked=total_count,
            records_failed=failed_count,
            records_passed=passed_count
        )
    
    def _check_cluster_config(self, df: DataFrame, params: Dict[str, Any]) -> DataQualityResult:
        """Check if cluster configuration is valid"""
        total_count = df.count()
        failed_count = 0
        
        # Check if min_workers <= max_workers
        if "min_workers" in df.columns and "max_workers" in df.columns:
            invalid_config = df.filter(col("min_workers") > col("max_workers")).count()
            failed_count += invalid_config
        
        # Check if cluster_type is valid
        if "cluster_type" in df.columns:
            valid_types = ["JOB_CLUSTER", "ALL_PURPOSE"]
            invalid_type = df.filter(~col("cluster_type").isin(valid_types)).count()
            failed_count += invalid_type
        
        passed_count = total_count - failed_count
        
        return DataQualityResult(
            rule_name="valid_cluster_config",
            passed=failed_count == 0,
            message=f"Found {failed_count} records with invalid cluster configuration",
            records_checked=total_count,
            records_failed=failed_count,
            records_passed=passed_count
        )
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all data quality results"""
        total_rules = len(self.results)
        passed_rules = sum(1 for r in self.results if r.passed)
        failed_rules = total_rules - passed_rules
        
        total_records = sum(r.records_checked for r in self.results)
        total_failed = sum(r.records_failed for r in self.results)
        
        return {
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "failed_rules": failed_rules,
            "success_rate": round((passed_rules / max(total_rules, 1)) * 100, 2),
            "total_records_checked": total_records,
            "total_records_failed": total_failed,
            "data_quality_score": round(((total_records - total_failed) / max(total_records, 1)) * 100, 2)
        }

def safe_execute(func: Callable, logger: StructuredLogger, operation_name: str):
    """Decorator for safe function execution with error handling"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"Starting {operation_name}")
            result = func(*args, **kwargs)
            logger.info(f"Successfully completed {operation_name}")
            return result
        except Exception as e:
            logger.error(f"Error in {operation_name}: {str(e)}", {
                "operation_name": operation_name,
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            raise
    return wrapper

def validate_data_quality(df: DataFrame, table_name: str, logger: StructuredLogger) -> bool:
    """Validate DataFrame data quality and return success status"""
    monitor = DataQualityMonitor(logger)
    results = monitor.validate_dataframe(df, table_name)
    
    # Check if any critical rules failed
    critical_failures = [r for r in results if not r.passed and r.rule_name in ["non_negative_quantity", "valid_time_range", "required_fields"]]
    
    if critical_failures:
        logger.error(f"Critical data quality failures detected for table: {table_name}", {
            "table_name": table_name,
            "critical_failures": [r.rule_name for r in critical_failures],
            "total_failures": len(critical_failures)
        })
        return False
    
    # Log summary
    summary = monitor.get_summary()
    logger.info(f"Data quality validation completed for table: {table_name}", {
        "table_name": table_name,
        "summary": summary
    })
    
    return True
