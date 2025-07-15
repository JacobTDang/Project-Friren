"""
Advanced Performance Monitoring System

Provides comprehensive monitoring of the Friren trading system performance
after the hardcoded value elimination and 3-scenario assignment integration.

Monitors:
- Market data calculation performance
- Strategy assignment efficiency
- 3-scenario coordination metrics
- Memory and CPU usage
- Decision making latency
- Database performance
- Redis communication health
"""

import time
import psutil
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import deque, defaultdict
import json

# Performance tracking data structures
@dataclass
class PerformanceMetric:
    """Individual performance metric"""
    timestamp: datetime
    component: str
    operation: str
    duration_ms: float
    success: bool
    memory_mb: float
    cpu_percent: float
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SystemHealthSnapshot:
    """Complete system health snapshot"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    disk_usage_percent: float
    redis_healthy: bool
    database_healthy: bool
    active_processes: int
    total_operations: int
    avg_operation_time_ms: float
    error_rate_percent: float
    market_data_quality: str
    assignment_success_rate: float

class AdvancedPerformanceMonitor:
    """
    Advanced monitoring system for the transformed Friren trading system.
    
    Tracks performance metrics, system health, and operational efficiency
    to ensure optimal production performance.
    """
    
    def __init__(self, max_metrics_history: int = 10000):
        self.logger = logging.getLogger(__name__)
        self.max_metrics_history = max_metrics_history
        
        # Performance data storage
        self.metrics_history: deque = deque(maxlen=max_metrics_history)
        self.component_stats: Dict[str, Dict] = defaultdict(lambda: {
            'total_operations': 0,
            'total_duration_ms': 0.0,
            'success_count': 0,
            'error_count': 0,
            'avg_duration_ms': 0.0,
            'success_rate': 100.0,
            'last_operation': None
        })
        
        # System health tracking
        self.health_snapshots: deque = deque(maxlen=1440)  # 24 hours of minute snapshots
        self.alert_thresholds = {
            'cpu_warning': 70.0,
            'cpu_critical': 85.0,
            'memory_warning': 75.0,
            'memory_critical': 90.0,
            'error_rate_warning': 5.0,
            'error_rate_critical': 10.0,
            'operation_time_warning': 1000.0,  # 1 second
            'operation_time_critical': 5000.0  # 5 seconds
        }
        
        # Component-specific monitoring
        self.market_data_performance = {
            'calculations_per_minute': 0,
            'avg_calculation_time_ms': 0.0,
            'data_quality_distribution': defaultdict(int),
            'symbols_processed': set(),
            'last_quality_check': None
        }
        
        self.assignment_performance = {
            'assignments_per_minute': 0,
            'scenario_distribution': defaultdict(int),
            'avg_assignment_time_ms': 0.0,
            'validation_success_rate': 100.0,
            'database_write_success_rate': 100.0
        }
        
        self.redis_performance = {
            'messages_per_minute': 0,
            'avg_message_latency_ms': 0.0,
            'connection_health': True,
            'queue_depths': defaultdict(int),
            'last_health_check': None
        }
        
        # Performance monitoring thread
        self.monitoring_active = False
        self.monitoring_thread = None
        
        self.logger.info("AdvancedPerformanceMonitor initialized")
    
    def start_monitoring(self, check_interval_seconds: int = 60):
        """Start continuous performance monitoring"""
        if self.monitoring_active:
            self.logger.warning("Performance monitoring already active")
            return
        
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(check_interval_seconds,),
            daemon=True
        )
        self.monitoring_thread.start()
        self.logger.info(f"Performance monitoring started with {check_interval_seconds}s interval")
    
    def stop_monitoring(self):
        """Stop continuous performance monitoring"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5.0)
        self.logger.info("Performance monitoring stopped")
    
    def record_operation(self, component: str, operation: str, 
                        duration_ms: float, success: bool, 
                        metadata: Optional[Dict] = None):
        """Record a performance metric for an operation"""
        
        # Get current system metrics
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        cpu_percent = process.cpu_percent()
        
        # Create performance metric
        metric = PerformanceMetric(
            timestamp=datetime.now(),
            component=component,
            operation=operation,
            duration_ms=duration_ms,
            success=success,
            memory_mb=memory_mb,
            cpu_percent=cpu_percent,
            metadata=metadata or {}
        )
        
        # Store metric
        self.metrics_history.append(metric)
        
        # Update component statistics
        self._update_component_stats(component, duration_ms, success)
        
        # Check for performance alerts
        self._check_performance_alerts(metric)
    
    def record_market_data_calculation(self, symbol: str, calculation_time_ms: float, 
                                     data_quality: str, success: bool):
        """Record market data calculation performance"""
        
        self.record_operation(
            component='market_data',
            operation='calculate_metrics',
            duration_ms=calculation_time_ms,
            success=success,
            metadata={
                'symbol': symbol,
                'data_quality': data_quality
            }
        )
        
        # Update market data specific metrics
        if success:
            self.market_data_performance['symbols_processed'].add(symbol)
            self.market_data_performance['data_quality_distribution'][data_quality] += 1
            
            # Update averages
            current_avg = self.market_data_performance['avg_calculation_time_ms']
            total_ops = self.component_stats['market_data']['total_operations']
            new_avg = ((current_avg * (total_ops - 1)) + calculation_time_ms) / total_ops
            self.market_data_performance['avg_calculation_time_ms'] = new_avg
    
    def record_strategy_assignment(self, symbol: str, scenario: str, 
                                 assignment_time_ms: float, success: bool,
                                 validation_passed: bool, database_success: bool):
        """Record strategy assignment performance"""
        
        self.record_operation(
            component='strategy_assignment',
            operation=f'assign_{scenario}',
            duration_ms=assignment_time_ms,
            success=success,
            metadata={
                'symbol': symbol,
                'scenario': scenario,
                'validation_passed': validation_passed,
                'database_success': database_success
            }
        )
        
        # Update assignment specific metrics
        self.assignment_performance['scenario_distribution'][scenario] += 1
        
        if validation_passed:
            val_success_count = sum(1 for m in self.metrics_history 
                                  if m.component == 'strategy_assignment' 
                                  and m.metadata.get('validation_passed'))
            val_total_count = sum(1 for m in self.metrics_history 
                                if m.component == 'strategy_assignment')
            self.assignment_performance['validation_success_rate'] = (val_success_count / val_total_count) * 100
        
        if database_success:
            db_success_count = sum(1 for m in self.metrics_history 
                                 if m.component == 'strategy_assignment' 
                                 and m.metadata.get('database_success'))
            db_total_count = sum(1 for m in self.metrics_history 
                               if m.component == 'strategy_assignment')
            self.assignment_performance['database_write_success_rate'] = (db_success_count / db_total_count) * 100
    
    def record_redis_operation(self, operation: str, latency_ms: float, success: bool,
                             queue_name: Optional[str] = None):
        """Record Redis operation performance"""
        
        self.record_operation(
            component='redis',
            operation=operation,
            duration_ms=latency_ms,
            success=success,
            metadata={
                'queue_name': queue_name
            }
        )
        
        # Update Redis specific metrics
        if queue_name:
            self.redis_performance['queue_depths'][queue_name] += 1
        
        # Update average latency
        redis_ops = [m for m in self.metrics_history if m.component == 'redis']
        if redis_ops:
            avg_latency = sum(m.duration_ms for m in redis_ops) / len(redis_ops)
            self.redis_performance['avg_message_latency_ms'] = avg_latency
    
    def get_system_health(self) -> SystemHealthSnapshot:
        """Get current system health snapshot"""
        
        # System metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Calculate operational metrics
        recent_metrics = [m for m in self.metrics_history 
                         if m.timestamp > datetime.now() - timedelta(minutes=5)]
        
        if recent_metrics:
            total_operations = len(recent_metrics)
            avg_operation_time = sum(m.duration_ms for m in recent_metrics) / len(recent_metrics)
            error_count = sum(1 for m in recent_metrics if not m.success)
            error_rate = (error_count / total_operations) * 100
        else:
            total_operations = 0
            avg_operation_time = 0.0
            error_rate = 0.0
        
        # Check Redis health
        redis_healthy = self._check_redis_health()
        
        # Check database health
        database_healthy = self._check_database_health()
        
        # Get market data quality
        market_data_quality = self._get_current_market_data_quality()
        
        # Calculate assignment success rate
        assignment_success_rate = self._calculate_assignment_success_rate()
        
        return SystemHealthSnapshot(
            timestamp=datetime.now(),
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            disk_usage_percent=disk.percent,
            redis_healthy=redis_healthy,
            database_healthy=database_healthy,
            active_processes=len(psutil.pids()),
            total_operations=total_operations,
            avg_operation_time_ms=avg_operation_time,
            error_rate_percent=error_rate,
            market_data_quality=market_data_quality,
            assignment_success_rate=assignment_success_rate
        )
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        
        health = self.get_system_health()
        
        return {
            'system_health': {
                'cpu_percent': health.cpu_percent,
                'memory_percent': health.memory_percent,
                'disk_usage_percent': health.disk_usage_percent,
                'redis_healthy': health.redis_healthy,
                'database_healthy': health.database_healthy,
                'error_rate_percent': health.error_rate_percent
            },
            'market_data_performance': self.market_data_performance.copy(),
            'assignment_performance': self.assignment_performance.copy(),
            'redis_performance': self.redis_performance.copy(),
            'component_statistics': dict(self.component_stats),
            'alert_status': self._get_current_alerts(),
            'uptime_metrics': self._calculate_uptime_metrics(),
            'timestamp': datetime.now().isoformat()
        }
    
    def export_metrics(self, filepath: str):
        """Export performance metrics to JSON file"""
        
        export_data = {
            'export_timestamp': datetime.now().isoformat(),
            'metrics_count': len(self.metrics_history),
            'performance_summary': self.get_performance_summary(),
            'detailed_metrics': [
                {
                    'timestamp': m.timestamp.isoformat(),
                    'component': m.component,
                    'operation': m.operation,
                    'duration_ms': m.duration_ms,
                    'success': m.success,
                    'memory_mb': m.memory_mb,
                    'cpu_percent': m.cpu_percent,
                    'metadata': m.metadata
                }
                for m in list(self.metrics_history)[-1000:]  # Last 1000 metrics
            ]
        }
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        self.logger.info(f"Performance metrics exported to {filepath}")
    
    def _update_component_stats(self, component: str, duration_ms: float, success: bool):
        """Update component-specific statistics"""
        
        stats = self.component_stats[component]
        stats['total_operations'] += 1
        stats['total_duration_ms'] += duration_ms
        
        if success:
            stats['success_count'] += 1
        else:
            stats['error_count'] += 1
        
        stats['avg_duration_ms'] = stats['total_duration_ms'] / stats['total_operations']
        stats['success_rate'] = (stats['success_count'] / stats['total_operations']) * 100
        stats['last_operation'] = datetime.now()
    
    def _check_performance_alerts(self, metric: PerformanceMetric):
        """Check for performance alerts based on thresholds"""
        
        alerts = []
        
        # CPU alerts
        if metric.cpu_percent > self.alert_thresholds['cpu_critical']:
            alerts.append(f"CRITICAL: CPU usage {metric.cpu_percent:.1f}% exceeds critical threshold")
        elif metric.cpu_percent > self.alert_thresholds['cpu_warning']:
            alerts.append(f"WARNING: CPU usage {metric.cpu_percent:.1f}% exceeds warning threshold")
        
        # Memory alerts
        if metric.memory_mb > 800:  # Approaching 1GB limit for t3.micro
            alerts.append(f"WARNING: Memory usage {metric.memory_mb:.1f}MB approaching system limit")
        
        # Operation time alerts
        if metric.duration_ms > self.alert_thresholds['operation_time_critical']:
            alerts.append(f"CRITICAL: {metric.component}.{metric.operation} took {metric.duration_ms:.1f}ms")
        elif metric.duration_ms > self.alert_thresholds['operation_time_warning']:
            alerts.append(f"WARNING: {metric.component}.{metric.operation} took {metric.duration_ms:.1f}ms")
        
        # Log alerts
        for alert in alerts:
            self.logger.warning(f"PERFORMANCE ALERT: {alert}")
    
    def _monitoring_loop(self, check_interval_seconds: int):
        """Main monitoring loop"""
        
        while self.monitoring_active:
            try:
                # Take system health snapshot
                health = self.get_system_health()
                self.health_snapshots.append(health)
                
                # Update per-minute metrics
                self._update_per_minute_metrics()
                
                # Check for system-level alerts
                self._check_system_alerts(health)
                
                # Sleep until next check
                time.sleep(check_interval_seconds)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(check_interval_seconds)
    
    def _update_per_minute_metrics(self):
        """Update per-minute operational metrics"""
        
        one_minute_ago = datetime.now() - timedelta(minutes=1)
        recent_metrics = [m for m in self.metrics_history if m.timestamp > one_minute_ago]
        
        # Market data calculations per minute
        market_data_ops = [m for m in recent_metrics if m.component == 'market_data']
        self.market_data_performance['calculations_per_minute'] = len(market_data_ops)
        
        # Assignments per minute
        assignment_ops = [m for m in recent_metrics if m.component == 'strategy_assignment']
        self.assignment_performance['assignments_per_minute'] = len(assignment_ops)
        
        # Redis messages per minute
        redis_ops = [m for m in recent_metrics if m.component == 'redis']
        self.redis_performance['messages_per_minute'] = len(redis_ops)
    
    def _check_redis_health(self) -> bool:
        """Check Redis connection health"""
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, decode_responses=True)
            r.ping()
            return True
        except Exception:
            return False
    
    def _check_database_health(self) -> bool:
        """Check database connection health"""
        try:
            import os
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'infrastructure.settings')
            import django
            django.setup()
            
            from django.db import connection
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            return True
        except Exception:
            return False
    
    def _get_current_market_data_quality(self) -> str:
        """Get current market data quality assessment"""
        
        recent_market_ops = [m for m in self.metrics_history 
                           if m.component == 'market_data' 
                           and m.timestamp > datetime.now() - timedelta(minutes=5)]
        
        if not recent_market_ops:
            return 'unknown'
        
        quality_counts = defaultdict(int)
        for op in recent_market_ops:
            quality = op.metadata.get('data_quality', 'unknown')
            quality_counts[quality] += 1
        
        # Return most common quality level
        return max(quality_counts.items(), key=lambda x: x[1])[0]
    
    def _calculate_assignment_success_rate(self) -> float:
        """Calculate recent assignment success rate"""
        
        recent_assignments = [m for m in self.metrics_history 
                            if m.component == 'strategy_assignment' 
                            and m.timestamp > datetime.now() - timedelta(minutes=10)]
        
        if not recent_assignments:
            return 100.0
        
        success_count = sum(1 for a in recent_assignments if a.success)
        return (success_count / len(recent_assignments)) * 100
    
    def _get_current_alerts(self) -> List[str]:
        """Get current active alerts"""
        
        alerts = []
        health = self.get_system_health()
        
        if health.cpu_percent > self.alert_thresholds['cpu_warning']:
            alerts.append(f"High CPU usage: {health.cpu_percent:.1f}%")
        
        if health.memory_percent > self.alert_thresholds['memory_warning']:
            alerts.append(f"High memory usage: {health.memory_percent:.1f}%")
        
        if health.error_rate_percent > self.alert_thresholds['error_rate_warning']:
            alerts.append(f"High error rate: {health.error_rate_percent:.1f}%")
        
        if not health.redis_healthy:
            alerts.append("Redis connection unhealthy")
        
        if not health.database_healthy:
            alerts.append("Database connection unhealthy")
        
        return alerts
    
    def _calculate_uptime_metrics(self) -> Dict[str, Any]:
        """Calculate system uptime metrics"""
        
        if not self.health_snapshots:
            return {'uptime_hours': 0, 'availability_percent': 100.0}
        
        # Estimate uptime based on health snapshots
        uptime_hours = len(self.health_snapshots) / 60  # Assuming 1-minute intervals
        
        # Calculate availability based on operational metrics
        total_operations = sum(stats['total_operations'] for stats in self.component_stats.values())
        total_successes = sum(stats['success_count'] for stats in self.component_stats.values())
        
        availability_percent = (total_successes / total_operations * 100) if total_operations > 0 else 100.0
        
        return {
            'uptime_hours': uptime_hours,
            'availability_percent': availability_percent,
            'total_operations': total_operations,
            'successful_operations': total_successes
        }
    
    def _check_system_alerts(self, health: SystemHealthSnapshot):
        """Check for system-level alerts"""
        
        # Critical system alerts
        if health.cpu_percent > self.alert_thresholds['cpu_critical']:
            self.logger.critical(f"CRITICAL SYSTEM ALERT: CPU usage {health.cpu_percent:.1f}%")
        
        if health.memory_percent > self.alert_thresholds['memory_critical']:
            self.logger.critical(f"CRITICAL SYSTEM ALERT: Memory usage {health.memory_percent:.1f}%")
        
        if health.error_rate_percent > self.alert_thresholds['error_rate_critical']:
            self.logger.critical(f"CRITICAL SYSTEM ALERT: Error rate {health.error_rate_percent:.1f}%")
        
        if not health.redis_healthy:
            self.logger.error("SYSTEM ALERT: Redis connection failed")
        
        if not health.database_healthy:
            self.logger.error("SYSTEM ALERT: Database connection failed")


# Global performance monitor instance
_performance_monitor: Optional[AdvancedPerformanceMonitor] = None

def get_performance_monitor() -> AdvancedPerformanceMonitor:
    """Get global performance monitor instance"""
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = AdvancedPerformanceMonitor()
    return _performance_monitor

def start_performance_monitoring(check_interval_seconds: int = 60):
    """Start global performance monitoring"""
    monitor = get_performance_monitor()
    monitor.start_monitoring(check_interval_seconds)

def record_operation(component: str, operation: str, duration_ms: float, 
                    success: bool, metadata: Optional[Dict] = None):
    """Record operation performance globally"""
    monitor = get_performance_monitor()
    monitor.record_operation(component, operation, duration_ms, success, metadata)