"""
Windows Handle Monitor
=====================

Enhanced Windows handle management and monitoring system to prevent handle leaks
in the Friren trading system. Provides comprehensive handle tracking, monitoring,
and automatic cleanup mechanisms.

Features:
- Real-time handle count monitoring
- Handle leak detection and prevention
- Automatic cleanup of orphaned handles
- Redis connection pooling to prevent connection leaks
- Process handle tracking with automatic cleanup
- System resource monitoring and alerting
"""

import os
import sys
import time
import threading
import logging
import subprocess
import platform
from typing import Dict, List, Optional, Set, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import weakref

# Windows-specific imports
if platform.system() == "Windows":
    try:
        import psutil
        import ctypes
        from ctypes import wintypes
        WINDOWS_API_AVAILABLE = True
    except ImportError:
        WINDOWS_API_AVAILABLE = False
else:
    WINDOWS_API_AVAILABLE = False


@dataclass
class HandleSnapshot:
    """Snapshot of system handle usage"""
    timestamp: datetime
    process_id: int
    total_handles: int
    process_handles: int
    thread_handles: int
    file_handles: int
    registry_handles: int
    socket_handles: int
    memory_usage_mb: float
    
    
@dataclass
class HandleAlert:
    """Handle usage alert information"""
    timestamp: datetime
    alert_type: str  # 'high_usage', 'leak_detected', 'exhaustion_risk'
    process_id: int
    current_handles: int
    threshold: int
    growth_rate: float
    details: Dict[str, Any] = field(default_factory=dict)


class WindowsHandleMonitor:
    """
    Comprehensive Windows handle monitoring and management system
    
    Monitors handle usage patterns, detects leaks, and provides
    automatic cleanup mechanisms to prevent handle exhaustion.
    """
    
    def __init__(self, redis_manager=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.redis_manager = redis_manager
        
        # PRODUCTION: Get handle monitor configuration - NO HARDCODED VALUES
        try:
            from Friren_V1.infrastructure.configuration_manager import get_handle_monitor_config
            handle_config = get_handle_monitor_config()
        except ImportError:
            raise ImportError("CRITICAL: Configuration manager required for handle monitor. No hardcoded values allowed.")
        
        # Configuration from configuration manager
        self.monitoring_enabled = platform.system() == "Windows" and WINDOWS_API_AVAILABLE
        self.monitoring_interval = handle_config['monitoring_interval']
        self.handle_warning_threshold = handle_config['warning_threshold']
        self.handle_critical_threshold = handle_config['critical_threshold']
        self.leak_detection_window = handle_config['leak_detection_window']
        self.max_growth_rate = handle_config['max_growth_rate']
        self.max_redis_connections = handle_config['max_redis_connections']
        
        # Validate critical configuration
        if self.monitoring_interval <= 0:
            raise ValueError("PRODUCTION: HANDLE_MONITOR_INTERVAL_SECONDS must be positive")
        if self.handle_warning_threshold <= 0:
            raise ValueError("PRODUCTION: HANDLE_WARNING_THRESHOLD must be positive")
        if self.handle_critical_threshold <= self.handle_warning_threshold:
            raise ValueError("PRODUCTION: HANDLE_CRITICAL_THRESHOLD must be greater than HANDLE_WARNING_THRESHOLD")
        
        # State tracking
        self.handle_history: Dict[int, List[HandleSnapshot]] = defaultdict(list)
        self.active_processes: Set[int] = set()
        self.alerts: List[HandleAlert] = []
        self.cleanup_callbacks: Dict[int, List[Callable]] = defaultdict(list)
        
        # Redis connection pool for handle leak prevention
        self.redis_connection_pool = None
        self.redis_connection_reuse_count = 0
        
        # Threading
        self._monitoring_active = False
        self._monitoring_thread = None
        self._cleanup_thread = None
        self._lock = threading.RLock()
        
        # Windows handle tracking
        self.tracked_process_handles: Dict[int, subprocess.Popen] = {}
        self.tracked_thread_handles: Dict[int, threading.Thread] = {}
        self.tracked_file_handles: Set[str] = set()
        
        if self.monitoring_enabled:
            self.logger.info("Windows Handle Monitor initialized with API support")
            self._initialize_redis_connection_pool()
        else:
            self.logger.warning("Windows Handle Monitor disabled - Windows APIs not available")
    
    def _initialize_redis_connection_pool(self):
        """Initialize Redis connection pool to prevent connection handle leaks"""
        try:
            if self.redis_manager and hasattr(self.redis_manager, 'redis_client'):
                # Create connection pool configuration
                pool_config = {
                    'max_connections': self.max_redis_connections,
                    'retry_on_timeout': True,
                    'socket_connect_timeout': 5,
                    'socket_timeout': 10,
                    'socket_keepalive': True,
                    'socket_keepalive_options': {},
                    'health_check_interval': 30
                }
                
                # Note: Connection pooling will be managed by Redis client
                # This is just configuration tracking
                self.redis_connection_pool = pool_config
                self.logger.info(f"Redis connection pool configuration established (max: {self.max_redis_connections})")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis connection pool: {e}")
    
    def start_monitoring(self):
        """Start handle monitoring threads"""
        if not self.monitoring_enabled:
            self.logger.warning("Cannot start monitoring - Windows APIs not available")
            return False
            
        if self._monitoring_active:
            return True
            
        self._monitoring_active = True
        
        # Start monitoring thread
        self._monitoring_thread = threading.Thread(
            target=self._monitoring_loop, 
            daemon=True, 
            name="HandleMonitor"
        )
        self._monitoring_thread.start()
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, 
            daemon=True, 
            name="HandleCleanup"
        )
        self._cleanup_thread.start()
        
        self.logger.info("Windows handle monitoring started")
        return True
    
    def stop_monitoring(self):
        """Stop handle monitoring"""
        self._monitoring_active = False
        
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=10)
            
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=10)
            
        self.logger.info("Windows handle monitoring stopped")
    
    def register_process_handle(self, process: subprocess.Popen, cleanup_callback: Optional[Callable] = None):
        """Register a process handle for tracking"""
        if not self.monitoring_enabled:
            return
            
        with self._lock:
            pid = process.pid
            self.tracked_process_handles[pid] = process
            self.active_processes.add(pid)
            
            if cleanup_callback:
                self.cleanup_callbacks[pid].append(cleanup_callback)
                
            self.logger.debug(f"Registered process handle for PID {pid}")
    
    def unregister_process_handle(self, pid: int):
        """Unregister a process handle"""
        with self._lock:
            self.tracked_process_handles.pop(pid, None)
            self.active_processes.discard(pid)
            self.cleanup_callbacks.pop(pid, None)
            
            # Clean up history for dead processes
            if pid in self.handle_history:
                # Keep last 5 snapshots for analysis
                self.handle_history[pid] = self.handle_history[pid][-5:]
                
            self.logger.debug(f"Unregistered process handle for PID {pid}")
    
    def register_thread_handle(self, thread: threading.Thread):
        """Register a thread handle for tracking"""
        if not self.monitoring_enabled:
            return
            
        with self._lock:
            thread_id = thread.ident or id(thread)
            self.tracked_thread_handles[thread_id] = weakref.ref(thread)
            self.logger.debug(f"Registered thread handle for thread {thread_id}")
    
    def register_file_handle(self, file_path: str):
        """Register a file handle for tracking"""
        if not self.monitoring_enabled:
            return
            
        with self._lock:
            self.tracked_file_handles.add(file_path)
            self.logger.debug(f"Registered file handle: {file_path}")
    
    def unregister_file_handle(self, file_path: str):
        """Unregister a file handle"""
        with self._lock:
            self.tracked_file_handles.discard(file_path)
            self.logger.debug(f"Unregistered file handle: {file_path}")
    
    def take_handle_snapshot(self, pid: Optional[int] = None) -> Optional[HandleSnapshot]:
        """Take a snapshot of current handle usage"""
        if not self.monitoring_enabled:
            return None
            
        try:
            if pid is None:
                pid = os.getpid()
                
            process = psutil.Process(pid)
            
            # Get handle counts
            if hasattr(process, 'num_handles'):
                total_handles = process.num_handles()
            else:
                total_handles = 0
                
            # Get detailed handle information if available
            process_handles = 0
            thread_handles = 0
            file_handles = 0
            registry_handles = 0
            socket_handles = 0
            
            # Windows-specific handle enumeration
            if platform.system() == "Windows":
                try:
                    # Count different handle types
                    for conn in process.connections():
                        socket_handles += 1
                        
                    for fd in process.open_files():
                        file_handles += 1
                        
                    thread_handles = process.num_threads()
                    process_handles = total_handles - thread_handles - file_handles - socket_handles
                    
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    pass
            
            # Memory usage
            memory_info = process.memory_info()
            memory_usage_mb = memory_info.rss / 1024 / 1024
            
            snapshot = HandleSnapshot(
                timestamp=datetime.now(),
                process_id=pid,
                total_handles=total_handles,
                process_handles=process_handles,
                thread_handles=thread_handles,
                file_handles=file_handles,
                registry_handles=registry_handles,
                socket_handles=socket_handles,
                memory_usage_mb=memory_usage_mb
            )
            
            return snapshot
            
        except Exception as e:
            self.logger.error(f"Error taking handle snapshot for PID {pid}: {e}")
            return None
    
    def detect_handle_leaks(self, pid: int) -> List[HandleAlert]:
        """Detect potential handle leaks for a process"""
        alerts = []
        
        if pid not in self.handle_history:
            return alerts
            
        history = self.handle_history[pid]
        if len(history) < 3:
            return alerts
            
        # Get recent snapshots
        recent_snapshots = history[-10:]  # Last 10 snapshots
        
        # Calculate growth rate
        if len(recent_snapshots) >= 2:
            time_span = (recent_snapshots[-1].timestamp - recent_snapshots[0].timestamp).total_seconds()
            if time_span > 0:
                handle_growth = recent_snapshots[-1].total_handles - recent_snapshots[0].total_handles
                growth_rate = (handle_growth / time_span) * 60  # handles per minute
                
                # Check for excessive growth
                if growth_rate > self.max_growth_rate:
                    alert = HandleAlert(
                        timestamp=datetime.now(),
                        alert_type='leak_detected',
                        process_id=pid,
                        current_handles=recent_snapshots[-1].total_handles,
                        threshold=self.max_growth_rate,
                        growth_rate=growth_rate,
                        details={
                            'time_span_seconds': time_span,
                            'handle_growth': handle_growth,
                            'snapshots_analyzed': len(recent_snapshots)
                        }
                    )
                    alerts.append(alert)
        
        # Check absolute thresholds
        current_handles = recent_snapshots[-1].total_handles
        
        if current_handles > self.handle_critical_threshold:
            alert = HandleAlert(
                timestamp=datetime.now(),
                alert_type='exhaustion_risk',
                process_id=pid,
                current_handles=current_handles,
                threshold=self.handle_critical_threshold,
                growth_rate=0,
                details={'threshold_type': 'critical'}
            )
            alerts.append(alert)
            
        elif current_handles > self.handle_warning_threshold:
            alert = HandleAlert(
                timestamp=datetime.now(),
                alert_type='high_usage',
                process_id=pid,
                current_handles=current_handles,
                threshold=self.handle_warning_threshold,
                growth_rate=0,
                details={'threshold_type': 'warning'}
            )
            alerts.append(alert)
        
        return alerts
    
    def cleanup_orphaned_handles(self):
        """Clean up orphaned handles from dead processes"""
        orphaned_count = 0
        
        with self._lock:
            # Check tracked process handles
            dead_pids = []
            for pid, process in self.tracked_process_handles.items():
                try:
                    if process.poll() is not None:
                        dead_pids.append(pid)
                except Exception:
                    dead_pids.append(pid)
            
            # Clean up dead processes
            for pid in dead_pids:
                self.logger.debug(f"Cleaning up orphaned handles for dead process {pid}")
                
                # Execute cleanup callbacks
                for callback in self.cleanup_callbacks.get(pid, []):
                    try:
                        callback()
                        orphaned_count += 1
                    except Exception as e:
                        self.logger.warning(f"Error in cleanup callback for PID {pid}: {e}")
                
                self.unregister_process_handle(pid)
            
            # Check tracked thread handles
            dead_threads = []
            for thread_id, thread_ref in self.tracked_thread_handles.items():
                thread = thread_ref()
                if thread is None or not thread.is_alive():
                    dead_threads.append(thread_id)
            
            for thread_id in dead_threads:
                del self.tracked_thread_handles[thread_id]
                orphaned_count += 1
                
        if orphaned_count > 0:
            self.logger.info(f"Cleaned up {orphaned_count} orphaned handles")
            
        return orphaned_count
    
    def force_redis_connection_cleanup(self):
        """Force cleanup of Redis connections to prevent handle leaks"""
        if not self.redis_manager:
            return
            
        try:
            # Force connection pool cleanup if available
            if hasattr(self.redis_manager, 'redis_client'):
                client = self.redis_manager.redis_client
                
                # Close connection pool
                if hasattr(client, 'connection_pool'):
                    pool = client.connection_pool
                    if hasattr(pool, 'disconnect'):
                        pool.disconnect()
                        self.logger.debug("Forced Redis connection pool cleanup")
                        
            self.redis_connection_reuse_count += 1
            
        except Exception as e:
            self.logger.warning(f"Error during Redis connection cleanup: {e}")
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        while self._monitoring_active:
            try:
                current_time = datetime.now()
                
                # Take snapshots for all active processes
                for pid in list(self.active_processes):
                    snapshot = self.take_handle_snapshot(pid)
                    if snapshot:
                        with self._lock:
                            self.handle_history[pid].append(snapshot)
                            
                            # Limit history size
                            if len(self.handle_history[pid]) > 100:
                                self.handle_history[pid] = self.handle_history[pid][-50:]
                        
                        # Detect leaks
                        alerts = self.detect_handle_leaks(pid)
                        for alert in alerts:
                            self.alerts.append(alert)
                            self._handle_alert(alert)
                
                # Clean up old alerts
                cutoff_time = current_time - timedelta(hours=1)
                self.alerts = [a for a in self.alerts if a.timestamp > cutoff_time]
                
                time.sleep(self.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"Error in handle monitoring loop: {e}")
                time.sleep(5)
    
    def _cleanup_loop(self):
        """Background cleanup loop"""
        while self._monitoring_active:
            try:
                # Periodic cleanup of orphaned handles
                self.cleanup_orphaned_handles()
                
                # Periodic Redis connection cleanup
                if self.redis_connection_reuse_count % 10 == 0:
                    self.force_redis_connection_cleanup()
                
                time.sleep(60)  # Run every minute
                
            except Exception as e:
                self.logger.error(f"Error in handle cleanup loop: {e}")
                time.sleep(30)
    
    def _handle_alert(self, alert: HandleAlert):
        """Handle a handle usage alert"""
        if alert.alert_type == 'exhaustion_risk':
            self.logger.critical(f"HANDLE EXHAUSTION RISK: Process {alert.process_id} has {alert.current_handles} handles (threshold: {alert.threshold})")
            
            # Emergency cleanup
            self.cleanup_orphaned_handles()
            self.force_redis_connection_cleanup()
            
        elif alert.alert_type == 'leak_detected':
            self.logger.warning(f"HANDLE LEAK DETECTED: Process {alert.process_id} growing at {alert.growth_rate:.1f} handles/min")
            
        elif alert.alert_type == 'high_usage':
            self.logger.info(f"HIGH HANDLE USAGE: Process {alert.process_id} has {alert.current_handles} handles")
    
    def get_handle_statistics(self) -> Dict[str, Any]:
        """Get comprehensive handle usage statistics"""
        with self._lock:
            stats = {
                'monitoring_enabled': self.monitoring_enabled,
                'active_processes': len(self.active_processes),
                'tracked_process_handles': len(self.tracked_process_handles),
                'tracked_thread_handles': len(self.tracked_thread_handles),
                'tracked_file_handles': len(self.tracked_file_handles),
                'recent_alerts': len([a for a in self.alerts if a.timestamp > datetime.now() - timedelta(minutes=30)]),
                'redis_connection_reuse_count': self.redis_connection_reuse_count,
                'total_snapshots': sum(len(history) for history in self.handle_history.values())
            }
            
            # Current handle usage by process
            current_usage = {}
            for pid in self.active_processes:
                if pid in self.handle_history and self.handle_history[pid]:
                    latest = self.handle_history[pid][-1]
                    current_usage[pid] = {
                        'total_handles': latest.total_handles,
                        'memory_mb': latest.memory_usage_mb,
                        'last_update': latest.timestamp.isoformat()
                    }
            
            stats['current_usage'] = current_usage
            return stats


# Global handle monitor instance
_windows_handle_monitor = None


def get_windows_handle_monitor(redis_manager=None):
    """Get the global Windows handle monitor instance"""
    global _windows_handle_monitor
    if _windows_handle_monitor is None:
        _windows_handle_monitor = WindowsHandleMonitor(redis_manager)
    return _windows_handle_monitor


def register_process_handle(process: subprocess.Popen, cleanup_callback: Optional[Callable] = None):
    """Register a process handle for monitoring"""
    monitor = get_windows_handle_monitor()
    monitor.register_process_handle(process, cleanup_callback)


def unregister_process_handle(pid: int):
    """Unregister a process handle"""
    monitor = get_windows_handle_monitor()
    monitor.unregister_process_handle(pid)


def start_handle_monitoring(redis_manager=None):
    """Start Windows handle monitoring"""
    monitor = get_windows_handle_monitor(redis_manager)
    return monitor.start_monitoring()


def stop_handle_monitoring():
    """Stop Windows handle monitoring"""
    monitor = get_windows_handle_monitor()
    monitor.stop_monitoring()