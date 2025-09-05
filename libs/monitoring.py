import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum

from .logging import StructuredLogger, PerformanceMonitor

class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class AlertStatus(Enum):
    """Alert status"""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"

@dataclass
class Alert:
    """Alert definition"""
    id: str
    title: str
    message: str
    severity: AlertSeverity
    source: str
    timestamp: datetime
    status: AlertStatus = AlertStatus.ACTIVE
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class Metric:
    """Metric definition"""
    name: str
    value: float
    unit: str
    timestamp: datetime
    tags: Dict[str, str]
    metadata: Optional[Dict[str, Any]] = None

class MonitoringSystem:
    """Central monitoring system for platform observability"""
    
    def __init__(self, logger: StructuredLogger):
        self.logger = logger
        self.alerts: List[Alert] = []
        self.metrics: List[Metric] = []
        self.alert_handlers: List[Callable] = []
        self.metric_handlers: List[Callable] = []
        
        # Register default handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default monitoring handlers"""
        # Log-based alert handler
        self.register_alert_handler(self._log_alert)
        
        # Log-based metric handler  
        self.register_metric_handler(self._log_metric)
    
    def register_alert_handler(self, handler: Callable):
        """Register an alert handler function"""
        self.alert_handlers.append(handler)
        self.logger.info(f"Registered alert handler: {handler.__name__}")
    
    def register_metric_handler(self, handler: Callable):
        """Register a metric handler function"""
        self.metric_handlers.append(handler)
        self.logger.info(f"Registered metric handler: {handler.__name__}")
    
    def create_alert(self, title: str, message: str, severity: AlertSeverity, 
                     source: str, metadata: Optional[Dict[str, Any]] = None) -> Alert:
        """Create and trigger a new alert"""
        alert = Alert(
            id=f"alert_{int(time.time())}_{len(self.alerts)}",
            title=title,
            message=message,
            severity=severity,
            source=source,
            timestamp=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        self.alerts.append(alert)
        
        # Trigger all alert handlers
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                self.logger.error(f"Error in alert handler {handler.__name__}: {str(e)}")
        
        self.logger.info(f"Created alert: {title}", {
            "alert_id": alert.id,
            "severity": alert.severity.value,
            "source": alert.source
        })
        
        return alert
    
    def create_metric(self, name: str, value: float, unit: str, 
                      tags: Dict[str, str], metadata: Optional[Dict[str, Any]] = None) -> Metric:
        """Create and record a new metric"""
        metric = Metric(
            name=name,
            value=value,
            unit=unit,
            timestamp=datetime.utcnow(),
            tags=tags,
            metadata=metadata or {}
        )
        
        self.metrics.append(metric)
        
        # Trigger all metric handlers
        for handler in self.metric_handlers:
            try:
                handler(metric)
            except Exception as e:
                self.logger.error(f"Error in metric handler {handler.__name__}: {str(e)}")
        
        return metric
    
    def _log_alert(self, alert: Alert):
        """Default alert handler - logs to structured logger"""
        log_level = alert.severity.value
        getattr(self.logger, log_level)(
            f"ALERT: {alert.title}",
            {
                "alert_id": alert.id,
                "alert_title": alert.title,
                "alert_message": alert.message,
                "alert_severity": alert.severity.value,
                "alert_source": alert.source,
                "alert_timestamp": alert.timestamp.isoformat(),
                "alert_metadata": alert.metadata
            }
        )
    
    def _log_metric(self, metric: Metric):
        """Default metric handler - logs to structured logger"""
        self.logger.info(
            f"METRIC: {metric.name} = {metric.value} {metric.unit}",
            {
                "metric_name": metric.name,
                "metric_value": metric.value,
                "metric_unit": metric.unit,
                "metric_timestamp": metric.timestamp.isoformat(),
                "metric_tags": metric.tags,
                "metric_metadata": metric.metadata
            }
        )
    
    def get_active_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get active alerts, optionally filtered by severity"""
        alerts = [a for a in self.alerts if a.status == AlertStatus.ACTIVE]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        return alerts
    
    def acknowledge_alert(self, alert_id: str, user: str):
        """Acknowledge an alert"""
        for alert in self.alerts:
            if alert.id == alert_id:
                alert.status = AlertStatus.ACKNOWLEDGED
                alert.acknowledged_by = user
                alert.acknowledged_at = datetime.utcnow()
                
                self.logger.info(f"Alert acknowledged: {alert.title}", {
                    "alert_id": alert.id,
                    "acknowledged_by": user,
                    "acknowledged_at": alert.acknowledged_at.isoformat()
                })
                break
    
    def resolve_alert(self, alert_id: str, user: str):
        """Resolve an alert"""
        for alert in self.alerts:
            if alert.id == alert_id:
                alert.status = AlertStatus.RESOLVED
                alert.resolved_by = user
                alert.resolved_at = datetime.utcnow()
                
                self.logger.info(f"Alert resolved: {alert.title}", {
                    "alert_id": alert.id,
                    "resolved_by": user,
                    "resolved_at": alert.resolved_at.isoformat()
                })
                break

class PipelineMonitor:
    """Monitor specific pipeline metrics and health"""
    
    def __init__(self, logger: StructuredLogger, monitoring_system: MonitoringSystem):
        self.logger = logger
        self.monitoring = monitoring_system
        self.performance_monitor = PerformanceMonitor(logger)
    
    def monitor_pipeline_start(self, pipeline_name: str, run_id: str):
        """Monitor pipeline start"""
        self.logger.info(f"Pipeline started: {pipeline_name}", {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "event": "pipeline_start"
        })
        
        # Create metric for pipeline start
        self.monitoring.create_metric(
            name="pipeline_start_count",
            value=1,
            unit="count",
            tags={"pipeline_name": pipeline_name, "run_id": run_id},
            metadata={"event": "pipeline_start"}
        )
    
    def monitor_pipeline_completion(self, pipeline_name: str, run_id: str, 
                                   success: bool, duration_seconds: float,
                                   records_processed: int = 0):
        """Monitor pipeline completion"""
        status = "success" if success else "failure"
        
        self.logger.info(f"Pipeline completed: {pipeline_name}", {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "status": status,
            "duration_seconds": duration_seconds,
            "records_processed": records_processed,
            "event": "pipeline_completion"
        })
        
        # Create metrics
        self.monitoring.create_metric(
            name="pipeline_duration_seconds",
            value=duration_seconds,
            unit="seconds",
            tags={"pipeline_name": pipeline_name, "run_id": run_id, "status": status}
        )
        
        self.monitoring.create_metric(
            name="pipeline_records_processed",
            value=records_processed,
            unit="count",
            tags={"pipeline_name": pipeline_name, "run_id": run_id, "status": status}
        )
        
        # Create alert for failures
        if not success:
            self.monitoring.create_alert(
                title=f"Pipeline {pipeline_name} failed",
                message=f"Pipeline {pipeline_name} (run {run_id}) failed after {duration_seconds:.2f} seconds",
                severity=AlertSeverity.ERROR,
                source="pipeline_monitor",
                metadata={
                    "pipeline_name": pipeline_name,
                    "run_id": run_id,
                    "duration_seconds": duration_seconds
                }
            )
    
    def monitor_data_freshness(self, table_name: str, last_update: datetime, 
                               expected_freshness_hours: int = 24):
        """Monitor data freshness and alert if stale"""
        now = datetime.utcnow()
        age_hours = (now - last_update).total_seconds() / 3600
        
        if age_hours > expected_freshness_hours:
            self.monitoring.create_alert(
                title=f"Data freshness alert: {table_name}",
                message=f"Table {table_name} data is {age_hours:.1f} hours old (expected < {expected_freshness_hours} hours)",
                severity=AlertSeverity.WARNING,
                source="data_freshness_monitor",
                metadata={
                    "table_name": table_name,
                    "age_hours": age_hours,
                    "expected_freshness_hours": expected_freshness_hours,
                    "last_update": last_update.isoformat()
                }
            )
        
        # Create metric for data age
        self.monitoring.create_metric(
            name="data_age_hours",
            value=age_hours,
            unit="hours",
            tags={"table_name": table_name},
            metadata={"last_update": last_update.isoformat()}
        )
    
    def monitor_cost_thresholds(self, cost_usd: float, threshold_usd: float, 
                                pipeline_name: str, run_id: str):
        """Monitor costs and alert if thresholds exceeded"""
        if cost_usd > threshold_usd:
            self.monitoring.create_alert(
                title=f"Cost threshold exceeded: {pipeline_name}",
                message=f"Pipeline {pipeline_name} (run {run_id}) cost ${cost_usd:.2f} exceeded threshold ${threshold_usd:.2f}",
                severity=AlertSeverity.WARNING,
                source="cost_monitor",
                metadata={
                    "pipeline_name": pipeline_name,
                    "run_id": run_id,
                    "cost_usd": cost_usd,
                    "threshold_usd": threshold_usd
                }
            )
        
        # Create cost metric
        self.monitoring.create_metric(
            name="pipeline_cost_usd",
            value=cost_usd,
            unit="USD",
            tags={"pipeline_name": pipeline_name, "run_id": run_id}
        )

class HealthChecker:
    """Check overall system health"""
    
    def __init__(self, logger: StructuredLogger, monitoring_system: MonitoringSystem):
        self.logger = logger
        self.monitoring = monitoring_system
    
    def check_system_health(self) -> Dict[str, Any]:
        """Perform comprehensive system health check"""
        health_status = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": "healthy",
            "checks": {}
        }
        
        # Check active alerts
        active_alerts = self.monitoring.get_active_alerts()
        critical_alerts = [a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]
        error_alerts = [a for a in active_alerts if a.severity == AlertSeverity.ERROR]
        
        health_status["checks"]["alerts"] = {
            "total_active": len(active_alerts),
            "critical": len(critical_alerts),
            "errors": len(error_alerts),
            "status": "healthy" if len(critical_alerts) == 0 else "unhealthy"
        }
        
        # Check data freshness (simplified)
        health_status["checks"]["data_freshness"] = {
            "status": "healthy",  # Would check actual table freshness
            "last_check": datetime.utcnow().isoformat()
        }
        
        # Determine overall status
        if len(critical_alerts) > 0:
            health_status["overall_status"] = "critical"
        elif len(error_alerts) > 0:
            health_status["overall_status"] = "warning"
        
        self.logger.info("System health check completed", health_status)
        return health_status

# Global monitoring instance
monitoring_system = MonitoringSystem(logger)
pipeline_monitor = PipelineMonitor(logger, monitoring_system)
health_checker = HealthChecker(logger, monitoring_system)
