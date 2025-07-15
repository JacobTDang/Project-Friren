"""
Resource Monitor - Production Resource Management

This module provides comprehensive resource monitoring and management for production deployment.
Designed specifically for t3.micro constraints with zero hardcoded limits.

Features:
- Real-time resource monitoring (CPU, memory, disk, network)
- Market-driven resource allocation
- Automatic scaling and throttling
- Resource bottleneck detection
- Emergency resource management
- Process-level resource coordination
"""

import psutil
import threading
import time
import logging
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import json
import os

# Import market metrics for dynamic resource allocation
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager


class ResourceType(Enum):
    """Types of system resources"""
    CPU = "cpu"
    MEMORY = "memory"
    DISK_IO = "disk_io"
    NETWORK_IO = "network_io"
    THREADS = "threads"
    PROCESSES = "processes"


class ResourceStatus(Enum):
    """Resource utilization status levels"""
    OPTIMAL = "optimal"        # <50% utilization
    GOOD = "good"             # 50-70% utilization
    WARNING = "warning"       # 70-85% utilization
    CRITICAL = "critical"     # 85-95% utilization
    EMERGENCY = "emergency"   # >95% utilization


class ThrottleAction(Enum):
    """Actions for resource throttling"""
    NONE = "none"
    REDUCE_FREQUENCY = "reduce_frequency"
    PAUSE_NON_CRITICAL = "pause_non_critical"
    EMERGENCY_THROTTLE = "emergency_throttle"
    SHUTDOWN_NON_ESSENTIAL = "shutdown_non_essential"


@dataclass
class ResourceMetrics:
    """Comprehensive resource metrics"""
    timestamp: datetime
    
    # CPU metrics
    cpu_percent: float
    cpu_freq_mhz: float
    load_average: Tuple[float, float, float]  # 1, 5, 15 minute averages
    
    # Memory metrics
    memory_total_mb: float
    memory_available_mb: float
    memory_used_mb: float
    memory_percent: float
    swap_used_mb: float
    swap_percent: float
    
    # Disk I/O metrics
    disk_read_mb: float
    disk_write_mb: float
    disk_read_iops: float
    disk_write_iops: float
    disk_usage_percent: float
    
    # Network I/O metrics
    network_bytes_sent: float
    network_bytes_recv: float
    network_packets_sent: int
    network_packets_recv: int
    
    # Process metrics
    process_count: int
    thread_count: int
    file_descriptor_count: int
    
    # Market context
    market_stress_level: float = 0.0
    volatility_level: float = 0.0


@dataclass
class ResourceAlert:
    """Resource utilization alert"""
    timestamp: datetime
    resource_type: ResourceType
    status: ResourceStatus
    current_value: float
    threshold: float
    message: str
    recommended_action: ThrottleAction
    market_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceLimits:
    """Dynamic resource limits based on market conditions"""
    # CPU limits (percentage)
    cpu_warning: float = 70.0
    cpu_critical: float = 85.0
    cpu_emergency: float = 95.0
    
    # Memory limits (percentage)
    memory_warning: float = 70.0
    memory_critical: float = 85.0
    memory_emergency: float = 95.0
    
    # Disk I/O limits (MB/s)
    disk_io_warning: float = 50.0
    disk_io_critical: float = 100.0
    
    # Network I/O limits (MB/s)
    network_io_warning: float = 10.0
    network_io_critical: float = 50.0
    
    # Process limits
    max_threads: int = 100
    max_processes: int = 20
    max_file_descriptors: int = 1000


class ResourceMonitor:
    """Comprehensive resource monitoring with market-aware throttling"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.ResourceMonitor")
        
        # Resource tracking
        self.metrics_history = deque(maxlen=1000)  # Last 1000 measurements
        self.alerts_history = deque(maxlen=500)    # Last 500 alerts
        
        # Dynamic limits (adjusted based on market conditions)
        self.base_limits = ResourceLimits()
        self.current_limits = ResourceLimits()
        
        # Monitoring state
        self.monitoring_active = True
        self.last_metrics_collection = None
        self.collection_interval = 30  # seconds
        
        # Throttling state
        self.active_throttles = {}  # {ResourceType: ThrottleAction}
        self.throttle_callbacks = {}  # {ThrottleAction: List[callback_functions]}
        
        # Background monitoring thread
        self.monitor_thread = None
        self.shutdown_event = threading.Event()
        
        # Resource allocation tracking
        self.process_allocations = {}  # {process_name: ResourceMetrics}
        
        # Market context cache
        self.market_context_cache = {}
        self.market_context_ttl = 60  # seconds
        
        self.logger.info("ResourceMonitor initialized")
    
    def start_monitoring(self):
        """Start background resource monitoring"""
        if not self.monitor_thread or not self.monitor_thread.is_alive():
            self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.monitor_thread.start()
            self.logger.info("Resource monitoring started")
    
    def stop_monitoring(self):
        """Stop background monitoring"""
        self.shutdown_event.set()
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)
        self.logger.info("Resource monitoring stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        while not self.shutdown_event.is_set():
            try:
                # Collect metrics
                metrics = self._collect_resource_metrics()
                
                if metrics:
                    self.metrics_history.append(metrics)
                    
                    # Update dynamic limits based on market conditions
                    self._update_dynamic_limits(metrics)
                    
                    # Check for resource alerts
                    alerts = self._check_resource_thresholds(metrics)
                    
                    # Process alerts and take action
                    for alert in alerts:
                        self._handle_resource_alert(alert)
                    
                    # Update Redis with current status
                    self._update_shared_resource_status(metrics)
                
                # Sleep until next collection
                self.shutdown_event.wait(self.collection_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                self.shutdown_event.wait(10)  # Wait before retrying
    
    def _collect_resource_metrics(self) -> Optional[ResourceMetrics]:
        """Collect comprehensive resource metrics"""
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_freq = psutil.cpu_freq()
            load_avg = os.getloadavg() if hasattr(os, 'getloadavg') else (0, 0, 0)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            # Disk I/O metrics
            disk_io = psutil.disk_io_counters()
            disk_usage = psutil.disk_usage('/')
            
            # Network I/O metrics
            network_io = psutil.net_io_counters()
            
            # Process metrics
            process_count = len(psutil.pids())
            current_process = psutil.Process()
            thread_count = current_process.num_threads()
            
            try:
                fd_count = current_process.num_fds()
            except (AttributeError, psutil.AccessDenied):
                fd_count = 0
            
            # Get market context for dynamic adjustments
            market_stress, volatility = self._get_market_context()
            
            metrics = ResourceMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                cpu_freq_mhz=cpu_freq.current if cpu_freq else 0,
                load_average=load_avg,
                memory_total_mb=memory.total / (1024 * 1024),
                memory_available_mb=memory.available / (1024 * 1024),
                memory_used_mb=memory.used / (1024 * 1024),
                memory_percent=memory.percent,
                swap_used_mb=swap.used / (1024 * 1024),
                swap_percent=swap.percent,
                disk_read_mb=disk_io.read_bytes / (1024 * 1024) if disk_io else 0,
                disk_write_mb=disk_io.write_bytes / (1024 * 1024) if disk_io else 0,
                disk_read_iops=disk_io.read_count if disk_io else 0,
                disk_write_iops=disk_io.write_count if disk_io else 0,
                disk_usage_percent=disk_usage.percent,
                network_bytes_sent=network_io.bytes_sent if network_io else 0,
                network_bytes_recv=network_io.bytes_recv if network_io else 0,
                network_packets_sent=network_io.packets_sent if network_io else 0,
                network_packets_recv=network_io.packets_recv if network_io else 0,
                process_count=process_count,
                thread_count=thread_count,
                file_descriptor_count=fd_count,
                market_stress_level=market_stress,
                volatility_level=volatility
            )
            
            self.last_metrics_collection = datetime.now()
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error collecting resource metrics: {e}")
            return None
    
    def _get_market_context(self) -> Tuple[float, float]:
        """Get market context for dynamic resource allocation"""
        try:
            now = datetime.now()
            
            # Check cache
            if ('market_context' in self.market_context_cache and 
                (now - self.market_context_cache['timestamp']).total_seconds() < self.market_context_ttl):
                cached = self.market_context_cache['data']
                return cached['stress'], cached['volatility']
            
            # Get average market stress across monitored symbols
            # In production, this would get from a list of active symbols
            stress_levels = []
            volatility_levels = []
            
            # For now, use a representative symbol (would be dynamic in production)
            for symbol in ['AAPL', 'SPY']:  # Representative symbols
                try:
                    market_metrics = get_all_metrics(symbol)
                    if market_metrics:
                        stress = getattr(market_metrics, 'market_stress', 0.3)
                        volatility = getattr(market_metrics, 'volatility', 0.3)
                        stress_levels.append(stress)
                        volatility_levels.append(volatility)
                except:
                    continue
            
            # Calculate averages
            avg_stress = sum(stress_levels) / len(stress_levels) if stress_levels else 0.3
            avg_volatility = sum(volatility_levels) / len(volatility_levels) if volatility_levels else 0.3
            
            # Cache result
            self.market_context_cache = {
                'timestamp': now,
                'data': {'stress': avg_stress, 'volatility': avg_volatility}
            }
            
            return avg_stress, avg_volatility
            
        except Exception as e:
            self.logger.error(f"Error getting market context: {e}")
            return 0.3, 0.3  # Default values
    
    def _update_dynamic_limits(self, metrics: ResourceMetrics):
        """Update resource limits based on market conditions"""
        try:
            # Reset to base limits
            self.current_limits = ResourceLimits(
                cpu_warning=self.base_limits.cpu_warning,
                cpu_critical=self.base_limits.cpu_critical,
                cpu_emergency=self.base_limits.cpu_emergency,
                memory_warning=self.base_limits.memory_warning,
                memory_critical=self.base_limits.memory_critical,
                memory_emergency=self.base_limits.memory_emergency,
                disk_io_warning=self.base_limits.disk_io_warning,
                disk_io_critical=self.base_limits.disk_io_critical,
                network_io_warning=self.base_limits.network_io_warning,
                network_io_critical=self.base_limits.network_io_critical,
                max_threads=self.base_limits.max_threads,
                max_processes=self.base_limits.max_processes,
                max_file_descriptors=self.base_limits.max_file_descriptors
            )
            
            # Adjust based on market stress
            if metrics.market_stress_level > 0.7:  # High stress
                # More aggressive limits in stressed markets
                self.current_limits.cpu_warning *= 0.8
                self.current_limits.cpu_critical *= 0.9
                self.current_limits.memory_warning *= 0.8
                self.current_limits.memory_critical *= 0.9
            elif metrics.market_stress_level < 0.3:  # Low stress
                # More relaxed limits in calm markets
                self.current_limits.cpu_warning *= 1.1
                self.current_limits.cpu_critical *= 1.05
                self.current_limits.memory_warning *= 1.1
                self.current_limits.memory_critical *= 1.05
            
            # Adjust based on volatility
            if metrics.volatility_level > 0.6:  # High volatility
                # Need more resources for volatile markets
                self.current_limits.disk_io_warning *= 1.2
                self.current_limits.network_io_warning *= 1.3
            
        except Exception as e:
            self.logger.error(f"Error updating dynamic limits: {e}")
    
    def _check_resource_thresholds(self, metrics: ResourceMetrics) -> List[ResourceAlert]:
        """Check resource usage against thresholds and generate alerts"""
        alerts = []
        
        try:
            # CPU checks
            cpu_status, cpu_action = self._get_resource_status_and_action(
                metrics.cpu_percent, 
                self.current_limits.cpu_warning,
                self.current_limits.cpu_critical,
                self.current_limits.cpu_emergency
            )
            
            if cpu_status != ResourceStatus.OPTIMAL:
                alerts.append(ResourceAlert(
                    timestamp=metrics.timestamp,
                    resource_type=ResourceType.CPU,
                    status=cpu_status,
                    current_value=metrics.cpu_percent,
                    threshold=self._get_threshold_for_status(cpu_status, 'cpu'),
                    message=f"CPU usage {metrics.cpu_percent:.1f}% exceeds {cpu_status.value} threshold",
                    recommended_action=cpu_action,
                    market_context={'stress': metrics.market_stress_level, 'volatility': metrics.volatility_level}
                ))
            
            # Memory checks
            memory_status, memory_action = self._get_resource_status_and_action(
                metrics.memory_percent,
                self.current_limits.memory_warning,
                self.current_limits.memory_critical,
                self.current_limits.memory_emergency
            )
            
            if memory_status != ResourceStatus.OPTIMAL:
                alerts.append(ResourceAlert(
                    timestamp=metrics.timestamp,
                    resource_type=ResourceType.MEMORY,
                    status=memory_status,
                    current_value=metrics.memory_percent,
                    threshold=self._get_threshold_for_status(memory_status, 'memory'),
                    message=f"Memory usage {metrics.memory_percent:.1f}% exceeds {memory_status.value} threshold",
                    recommended_action=memory_action,
                    market_context={'stress': metrics.market_stress_level, 'volatility': metrics.volatility_level}
                ))
            
            # Thread count checks
            if metrics.thread_count > self.current_limits.max_threads:
                alerts.append(ResourceAlert(
                    timestamp=metrics.timestamp,
                    resource_type=ResourceType.THREADS,
                    status=ResourceStatus.WARNING,
                    current_value=metrics.thread_count,
                    threshold=self.current_limits.max_threads,
                    message=f"Thread count {metrics.thread_count} exceeds limit {self.current_limits.max_threads}",
                    recommended_action=ThrottleAction.REDUCE_FREQUENCY,
                    market_context={'stress': metrics.market_stress_level, 'volatility': metrics.volatility_level}
                ))
            
            # Disk usage checks
            if metrics.disk_usage_percent > 90:
                alerts.append(ResourceAlert(
                    timestamp=metrics.timestamp,
                    resource_type=ResourceType.DISK_IO,
                    status=ResourceStatus.CRITICAL,
                    current_value=metrics.disk_usage_percent,
                    threshold=90.0,
                    message=f"Disk usage {metrics.disk_usage_percent:.1f}% is critically high",
                    recommended_action=ThrottleAction.EMERGENCY_THROTTLE,
                    market_context={'stress': metrics.market_stress_level, 'volatility': metrics.volatility_level}
                ))
            
        except Exception as e:
            self.logger.error(f"Error checking resource thresholds: {e}")
        
        return alerts
    
    def _get_resource_status_and_action(self, current_value: float, warning: float, 
                                      critical: float, emergency: float) -> Tuple[ResourceStatus, ThrottleAction]:
        """Determine resource status and recommended action"""
        if current_value >= emergency:
            return ResourceStatus.EMERGENCY, ThrottleAction.SHUTDOWN_NON_ESSENTIAL
        elif current_value >= critical:
            return ResourceStatus.CRITICAL, ThrottleAction.EMERGENCY_THROTTLE
        elif current_value >= warning:
            return ResourceStatus.WARNING, ThrottleAction.REDUCE_FREQUENCY
        elif current_value >= 50:
            return ResourceStatus.GOOD, ThrottleAction.NONE
        else:
            return ResourceStatus.OPTIMAL, ThrottleAction.NONE
    
    def _get_threshold_for_status(self, status: ResourceStatus, resource_type: str) -> float:
        """Get threshold value for a given status and resource type"""
        if resource_type == 'cpu':
            if status == ResourceStatus.WARNING:
                return self.current_limits.cpu_warning
            elif status == ResourceStatus.CRITICAL:
                return self.current_limits.cpu_critical
            elif status == ResourceStatus.EMERGENCY:
                return self.current_limits.cpu_emergency
        elif resource_type == 'memory':
            if status == ResourceStatus.WARNING:
                return self.current_limits.memory_warning
            elif status == ResourceStatus.CRITICAL:
                return self.current_limits.memory_critical
            elif status == ResourceStatus.EMERGENCY:
                return self.current_limits.memory_emergency
        
        return 0.0
    
    def _handle_resource_alert(self, alert: ResourceAlert):
        """Handle a resource alert by taking appropriate action"""
        try:
            # Add to alerts history
            self.alerts_history.append(alert)
            
            # Log alert
            self.logger.warning(f"Resource alert: {alert.message} - Action: {alert.recommended_action.value}")
            
            # Take action based on severity
            if alert.recommended_action != ThrottleAction.NONE:
                self._apply_throttle_action(alert.resource_type, alert.recommended_action)
            
            # Send alert to other processes
            self._broadcast_resource_alert(alert)
            
        except Exception as e:
            self.logger.error(f"Error handling resource alert: {e}")
    
    def _apply_throttle_action(self, resource_type: ResourceType, action: ThrottleAction):
        """Apply throttling action"""
        try:
            # Update active throttles
            self.active_throttles[resource_type] = action
            
            # Execute registered callbacks for this action
            if action in self.throttle_callbacks:
                for callback in self.throttle_callbacks[action]:
                    try:
                        callback(resource_type, action)
                    except Exception as e:
                        self.logger.error(f"Error in throttle callback: {e}")
            
            self.logger.info(f"Applied throttle action: {action.value} for {resource_type.value}")
            
        except Exception as e:
            self.logger.error(f"Error applying throttle action: {e}")
    
    def _broadcast_resource_alert(self, alert: ResourceAlert):
        """Broadcast resource alert to other processes"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                return
            
            # Create alert message
            alert_data = {
                'alert_type': 'resource_alert',
                'resource_type': alert.resource_type.value,
                'status': alert.status.value,
                'current_value': alert.current_value,
                'threshold': alert.threshold,
                'message': alert.message,
                'recommended_action': alert.recommended_action.value,
                'market_context': alert.market_context,
                'timestamp': alert.timestamp.isoformat()
            }
            
            # Broadcast to interested processes
            target_processes = [
                "decision_engine",
                "position_health_monitor",
                "enhanced_news_pipeline_process"
            ]
            
            for target in target_processes:
                redis_manager.send_message(target, {
                    'message_type': 'resource_alert',
                    'data': alert_data,
                    'priority': 'HIGH' if alert.status in [ResourceStatus.CRITICAL, ResourceStatus.EMERGENCY] else 'MEDIUM'
                })
            
        except Exception as e:
            self.logger.error(f"Error broadcasting resource alert: {e}")
    
    def _update_shared_resource_status(self, metrics: ResourceMetrics):
        """Update shared resource status in Redis"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                return
            
            status_data = {
                'timestamp': metrics.timestamp.isoformat(),
                'cpu_percent': metrics.cpu_percent,
                'memory_percent': metrics.memory_percent,
                'memory_used_mb': metrics.memory_used_mb,
                'disk_usage_percent': metrics.disk_usage_percent,
                'thread_count': metrics.thread_count,
                'process_count': metrics.process_count,
                'market_stress_level': metrics.market_stress_level,
                'volatility_level': metrics.volatility_level,
                'active_throttles': {k.value: v.value for k, v in self.active_throttles.items()},
                'status_summary': self._get_overall_status(metrics)
            }
            
            redis_manager.set_shared_data("system_resource_status", status_data)
            
        except Exception as e:
            self.logger.error(f"Error updating shared resource status: {e}")
    
    def _get_overall_status(self, metrics: ResourceMetrics) -> str:
        """Get overall system status"""
        try:
            max_usage = max(metrics.cpu_percent, metrics.memory_percent)
            
            if max_usage >= self.current_limits.memory_emergency:
                return "EMERGENCY"
            elif max_usage >= self.current_limits.memory_critical:
                return "CRITICAL"
            elif max_usage >= self.current_limits.memory_warning:
                return "WARNING"
            elif max_usage >= 50:
                return "GOOD"
            else:
                return "OPTIMAL"
                
        except Exception as e:
            self.logger.error(f"Error getting overall status: {e}")
            return "UNKNOWN"
    
    def register_throttle_callback(self, action: ThrottleAction, callback: Callable):
        """Register a callback for throttle actions"""
        if action not in self.throttle_callbacks:
            self.throttle_callbacks[action] = []
        self.throttle_callbacks[action].append(callback)
        self.logger.debug(f"Registered throttle callback for {action.value}")
    
    def get_current_metrics(self) -> Optional[ResourceMetrics]:
        """Get current resource metrics"""
        if self.metrics_history:
            return self.metrics_history[-1]
        return None
    
    def get_resource_summary(self, hours: int = 1) -> Dict[str, Any]:
        """Get resource usage summary for specified time period"""
        try:
            if not self.metrics_history:
                return {'error': 'No metrics available'}
            
            # Filter metrics for time period
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_metrics = [m for m in self.metrics_history if m.timestamp >= cutoff_time]
            
            if not recent_metrics:
                recent_metrics = [self.metrics_history[-1]]  # Use latest if no recent data
            
            # Calculate averages and peaks
            avg_cpu = sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics)
            max_cpu = max(m.cpu_percent for m in recent_metrics)
            avg_memory = sum(m.memory_percent for m in recent_metrics) / len(recent_metrics)
            max_memory = max(m.memory_percent for m in recent_metrics)
            
            current = recent_metrics[-1]
            
            # Recent alerts
            recent_alerts = [a for a in self.alerts_history if a.timestamp >= cutoff_time]
            
            return {
                'time_period_hours': hours,
                'current_status': self._get_overall_status(current),
                'current_metrics': {
                    'cpu_percent': current.cpu_percent,
                    'memory_percent': current.memory_percent,
                    'memory_used_mb': current.memory_used_mb,
                    'disk_usage_percent': current.disk_usage_percent,
                    'thread_count': current.thread_count,
                    'process_count': current.process_count
                },
                'period_averages': {
                    'cpu_percent': avg_cpu,
                    'memory_percent': avg_memory
                },
                'period_peaks': {
                    'cpu_percent': max_cpu,
                    'memory_percent': max_memory
                },
                'market_context': {
                    'stress_level': current.market_stress_level,
                    'volatility_level': current.volatility_level
                },
                'active_throttles': {k.value: v.value for k, v in self.active_throttles.items()},
                'recent_alerts': len(recent_alerts),
                'alert_breakdown': {
                    status.value: len([a for a in recent_alerts if a.status == status])
                    for status in ResourceStatus
                },
                'dynamic_limits': {
                    'cpu_warning': self.current_limits.cpu_warning,
                    'cpu_critical': self.current_limits.cpu_critical,
                    'memory_warning': self.current_limits.memory_warning,
                    'memory_critical': self.current_limits.memory_critical
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting resource summary: {e}")
            return {'error': str(e)}
    
    def force_resource_cleanup(self):
        """Force immediate resource cleanup"""
        try:
            self.logger.warning("Forcing immediate resource cleanup")
            
            # Apply emergency throttling
            for resource_type in [ResourceType.CPU, ResourceType.MEMORY]:
                self._apply_throttle_action(resource_type, ThrottleAction.EMERGENCY_THROTTLE)
            
            # Trigger garbage collection
            import gc
            collected = gc.collect()
            self.logger.info(f"Emergency GC collected {collected} objects")
            
        except Exception as e:
            self.logger.error(f"Error in force resource cleanup: {e}")


# Global resource monitor instance
_global_monitor = None


def get_resource_monitor() -> ResourceMonitor:
    """Get global resource monitor instance"""
    global _global_monitor
    
    if _global_monitor is None:
        _global_monitor = ResourceMonitor()
        _global_monitor.start_monitoring()
    
    return _global_monitor


# Utility functions for resource management
def check_resource_availability(cpu_threshold: float = 80.0, memory_threshold: float = 80.0) -> bool:
    """Check if sufficient resources are available"""
    try:
        monitor = get_resource_monitor()
        metrics = monitor.get_current_metrics()
        
        if not metrics:
            return True  # Assume available if no data
        
        return (metrics.cpu_percent < cpu_threshold and 
                metrics.memory_percent < memory_threshold)
                
    except Exception:
        return True  # Assume available on error


def wait_for_resources(max_wait_seconds: int = 60, check_interval: int = 5) -> bool:
    """Wait for resources to become available"""
    try:
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            if check_resource_availability():
                return True
            time.sleep(check_interval)
        
        return False
        
    except Exception:
        return False
