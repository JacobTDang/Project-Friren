#!/usr/bin/env python3
"""
memory_monitor.py

Comprehensive Memory Monitoring and Leak Prevention System

Features:
- Real-time memory tracking for all processes
- Memory leak detection and automatic cleanup
- Threshold-based alerts and emergency shutdown
- Redis-based memory state sharing
- Process-specific memory limits
- Automatic garbage collection optimization
- Memory usage analytics and reporting
"""

import gc
import os
import sys
import time
import psutil
import threading
import logging
import tracemalloc
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import weakref
from collections import defaultdict, deque

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager

class MemoryAlert(Enum):
    """Memory alert levels"""
    LOW = "low"           # 60% of limit
    MEDIUM = "medium"     # 75% of limit
    HIGH = "high"         # 85% of limit
    CRITICAL = "critical" # 95% of limit
    EMERGENCY = "emergency" # Over limit

@dataclass
class MemorySnapshot:
    """Single memory measurement snapshot"""
    timestamp: datetime
    process_id: str
    pid: int
    memory_mb: float
    memory_percent: float
    cpu_percent: float
    threads_count: int
    open_files: int
    connections: int
    gc_objects: int

    # Memory details
    rss_mb: float  # Resident Set Size
    vms_mb: float  # Virtual Memory Size
    shared_mb: float
    text_mb: float
    data_mb: float

    # Python-specific
    python_objects: int
    python_memory_mb: float

    # Custom tracking
    custom_counters: Dict[str, Any]

@dataclass
class MemoryStats:
    """Memory statistics over time"""
    process_id: str
    start_time: datetime
    current_memory_mb: float
    peak_memory_mb: float
    average_memory_mb: float
    memory_growth_rate: float  # MB per minute
    total_snapshots: int
    alert_level: MemoryAlert
    last_cleanup: Optional[datetime]
    leak_detected: bool
    leak_rate: float  # MB per minute if leak detected

class MemoryMonitor:
    """
    Comprehensive memory monitoring system

    Tracks memory usage, detects leaks, and performs automatic cleanup
    """

    def __init__(self,
                 process_id: str,
                 memory_limit_mb: float = 200,   # 200MB per process (1GB total / 5 processes)
                 check_interval: int = 30,       # Check every 30 seconds
                 history_size: int = 100,        # Keep 100 snapshots
                 cleanup_threshold: float = 0.85): # Cleanup at 85% of limit

        self.process_id = process_id
        self.memory_limit_mb = memory_limit_mb
        self.check_interval = check_interval
        self.history_size = history_size
        self.cleanup_threshold = cleanup_threshold

        # Memory tracking
        self.snapshots = deque(maxlen=history_size)
        self.start_time = datetime.now()
        self.peak_memory = 0.0
        self.last_cleanup = None
        self.cleanup_count = 0

        # Leak detection
        self.leak_detection_window = 10  # Look at last 10 snapshots
        self.leak_threshold = 50.0  # 50MB growth = potential leak

        # Monitoring thread
        self._stop_event = threading.Event()
        self._monitor_thread = None
        self._is_monitoring = False

        # Custom counters for tracking specific objects
        self.custom_counters = {}
        self._counter_callbacks = {}

        # Redis for sharing memory state
        self.redis_manager = None

        # Alert callbacks
        self.alert_callbacks = []
        self.emergency_callbacks = []

        # Process handle for detailed monitoring
        try:
            self.process = psutil.Process()
        except:
            self.process = None

        # Enable tracemalloc for Python memory tracking
        if not tracemalloc.is_tracing():
            tracemalloc.start()

        # Setup logging
        self.logger = logging.getLogger(f"memory_monitor.{process_id}")

        self.logger.info(f"Memory Monitor initialized for {process_id}")
        self.logger.info(f"Memory limit: {memory_limit_mb}MB, Check interval: {check_interval}s")

    def start_monitoring(self):
        """Start the memory monitoring thread"""
        if self._is_monitoring:
            return

        try:
            # Initialize Redis connection
            self.redis_manager = get_trading_redis_manager()

            self._is_monitoring = True
            self._stop_event.clear()

            # Start monitoring thread
            self._monitor_thread = threading.Thread(
                target=self._monitoring_loop,
                daemon=True,
                name=f"MemoryMonitor-{self.process_id}"
            )
            self._monitor_thread.start()

            self.logger.info(f"Memory monitoring started for {self.process_id}")

            # Take initial snapshot
            self.take_snapshot()

        except Exception as e:
            self.logger.error(f"Failed to start memory monitoring: {e}")
            self._is_monitoring = False

    def stop_monitoring(self):
        """Stop the memory monitoring thread"""
        if not self._is_monitoring:
            return

        self._stop_event.set()
        self._is_monitoring = False

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=60)  # Increased to 60 seconds for proper cleanup
            
            # Force thread termination if it doesn't stop gracefully
            if self._monitor_thread.is_alive():
                self.logger.warning(f"Memory monitor thread for {self.process_id} did not stop gracefully after 60s")
                # Note: Python threads cannot be force-terminated, but we can flag this for investigation

        self.logger.info(f"Memory monitoring stopped for {self.process_id}")

    def _monitoring_loop(self):
        """Main monitoring loop"""
        while not self._stop_event.is_set():
            try:
                # Take memory snapshot
                snapshot = self.take_snapshot()

                # Analyze snapshot
                stats = self.get_memory_stats()

                # Check for alerts
                self._check_alerts(snapshot, stats)

                # Check for memory leaks
                self._check_for_leaks(stats)

                # Auto cleanup if needed
                if self._should_auto_cleanup(snapshot):
                    self.cleanup_memory()

                # Update Redis state
                self._update_redis_state(snapshot, stats)

            except Exception as e:
                self.logger.error(f"Error in memory monitoring loop: {e}")

            # Wait for next check
            self._stop_event.wait(self.check_interval)

    def take_snapshot(self) -> MemorySnapshot:
        """Take a comprehensive memory snapshot"""
        try:
            timestamp = datetime.now()

            # Get process info
            if self.process:
                memory_info = self.process.memory_info()
                memory_percent = self.process.memory_percent()
                cpu_percent = self.process.cpu_percent()

                # Extended memory info
                try:
                    memory_full = self.process.memory_full_info()
                    rss_mb = memory_full.rss / 1024 / 1024
                    
                    # CRITICAL FIX: For main process, include ALL child processes
                    if self.process_id == "main_process":
                        try:
                            for child in self.process.children(recursive=True):
                                try:
                                    child_memory = child.memory_info().rss / 1024 / 1024
                                    rss_mb += child_memory
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    continue
                        except Exception as e:
                            self.logger.debug(f"Could not get child process memory: {e}")
                    
                    vms_mb = memory_full.vms / 1024 / 1024
                    shared_mb = getattr(memory_full, 'shared', 0) / 1024 / 1024
                    text_mb = getattr(memory_full, 'text', 0) / 1024 / 1024
                    data_mb = getattr(memory_full, 'data', 0) / 1024 / 1024
                except:
                    rss_mb = memory_info.rss / 1024 / 1024
                    
                    # CRITICAL FIX: Fallback also needs child process memory for main process
                    if self.process_id == "main_process":
                        try:
                            for child in self.process.children(recursive=True):
                                try:
                                    child_memory = child.memory_info().rss / 1024 / 1024
                                    rss_mb += child_memory
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    continue
                        except Exception as e:
                            self.logger.debug(f"Could not get child process memory: {e}")
                    vms_mb = memory_info.vms / 1024 / 1024
                    shared_mb = text_mb = data_mb = 0

                # Process details
                try:
                    threads_count = self.process.num_threads()
                    open_files = len(self.process.open_files())
                    connections = len(self.process.connections())
                except:
                    threads_count = open_files = connections = 0
            else:
                # Fallback if process handle not available
                memory_info = psutil.virtual_memory()
                memory_percent = memory_info.percent
                cpu_percent = 0
                rss_mb = vms_mb = shared_mb = text_mb = data_mb = 0
                threads_count = open_files = connections = 0

            memory_mb = rss_mb if rss_mb > 0 else (memory_info.rss / 1024 / 1024 if hasattr(memory_info, 'rss') else 0)

            # Python memory tracking
            try:
                if tracemalloc.is_tracing():
                    current, peak = tracemalloc.get_traced_memory()
                    python_memory_mb = current / 1024 / 1024
                else:
                    python_memory_mb = 0
            except:
                python_memory_mb = 0

            # Garbage collection info
            gc_objects = len(gc.get_objects())
            python_objects = sum(gc.get_count())

            # Update custom counters
            custom_counters = {}
            for name, callback in self._counter_callbacks.items():
                try:
                    custom_counters[name] = callback()
                except Exception as e:
                    self.logger.warning(f"Custom counter '{name}' failed: {e}")
                    custom_counters[name] = 0

            # Add static custom counters
            custom_counters.update(self.custom_counters)

            # Create snapshot
            snapshot = MemorySnapshot(
                timestamp=timestamp,
                process_id=self.process_id,
                pid=os.getpid(),
                memory_mb=memory_mb,
                memory_percent=memory_percent,
                cpu_percent=cpu_percent,
                threads_count=threads_count,
                open_files=open_files,
                connections=connections,
                gc_objects=gc_objects,
                rss_mb=rss_mb,
                vms_mb=vms_mb,
                shared_mb=shared_mb,
                text_mb=text_mb,
                data_mb=data_mb,
                python_objects=python_objects,
                python_memory_mb=python_memory_mb,
                custom_counters=custom_counters
            )

            # Add to history
            self.snapshots.append(snapshot)

            # Update peak memory
            if memory_mb > self.peak_memory:
                self.peak_memory = memory_mb

            return snapshot

        except Exception as e:
            self.logger.error(f"Failed to take memory snapshot: {e}")
            # Return minimal snapshot
            return MemorySnapshot(
                timestamp=datetime.now(),
                process_id=self.process_id,
                pid=os.getpid(),
                memory_mb=0,
                memory_percent=0,
                cpu_percent=0,
                threads_count=0,
                open_files=0,
                connections=0,
                gc_objects=0,
                rss_mb=0,
                vms_mb=0,
                shared_mb=0,
                text_mb=0,
                data_mb=0,
                python_objects=0,
                python_memory_mb=0,
                custom_counters={}
            )

    def get_memory_stats(self) -> MemoryStats:
        """Calculate memory statistics from snapshots"""
        if not self.snapshots:
            return MemoryStats(
                process_id=self.process_id,
                start_time=self.start_time,
                current_memory_mb=0,
                peak_memory_mb=0,
                average_memory_mb=0,
                memory_growth_rate=0,
                total_snapshots=0,
                alert_level=MemoryAlert.LOW,
                last_cleanup=self.last_cleanup,
                leak_detected=False,
                leak_rate=0
            )

        current_snapshot = self.snapshots[-1]
        current_memory = current_snapshot.memory_mb

        # Calculate average
        avg_memory = sum(s.memory_mb for s in self.snapshots) / len(self.snapshots)

        # Calculate growth rate (MB per minute)
        growth_rate = 0
        if len(self.snapshots) >= 2:
            time_diff = (current_snapshot.timestamp - self.snapshots[0].timestamp).total_seconds() / 60
            if time_diff > 0:
                memory_diff = current_memory - self.snapshots[0].memory_mb
                growth_rate = memory_diff / time_diff

        # Determine alert level
        usage_percent = current_memory / self.memory_limit_mb
        if usage_percent >= 1.0:
            alert_level = MemoryAlert.EMERGENCY
        elif usage_percent >= 0.95:
            alert_level = MemoryAlert.CRITICAL
        elif usage_percent >= 0.85:
            alert_level = MemoryAlert.HIGH
        elif usage_percent >= 0.75:
            alert_level = MemoryAlert.MEDIUM
        else:
            alert_level = MemoryAlert.LOW

        # Check for memory leak
        leak_detected, leak_rate = self._detect_memory_leak()

        return MemoryStats(
            process_id=self.process_id,
            start_time=self.start_time,
            current_memory_mb=current_memory,
            peak_memory_mb=self.peak_memory,
            average_memory_mb=avg_memory,
            memory_growth_rate=growth_rate,
            total_snapshots=len(self.snapshots),
            alert_level=alert_level,
            last_cleanup=self.last_cleanup,
            leak_detected=leak_detected,
            leak_rate=leak_rate
        )

    def _detect_memory_leak(self) -> tuple[bool, float]:
        """Detect if there's a memory leak based on recent snapshots"""
        if len(self.snapshots) < self.leak_detection_window:
            return False, 0.0

        # Get recent snapshots
        recent_snapshots = list(self.snapshots)[-self.leak_detection_window:]

        # Calculate trend
        memory_values = [s.memory_mb for s in recent_snapshots]
        time_values = [(s.timestamp - recent_snapshots[0].timestamp).total_seconds() / 60
                      for s in recent_snapshots]

        # Simple linear regression to detect trend
        n = len(memory_values)
        sum_x = sum(time_values)
        sum_y = sum(memory_values)
        sum_xy = sum(x * y for x, y in zip(time_values, memory_values))
        sum_x2 = sum(x * x for x in time_values)

        if n * sum_x2 - sum_x * sum_x == 0:
            return False, 0.0

        # Calculate slope (memory growth rate per minute)
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)

        # Leak detected if consistent growth above threshold
        leak_detected = slope > (self.leak_threshold / self.leak_detection_window)

        return leak_detected, slope if leak_detected else 0.0

    def _check_alerts(self, snapshot: MemorySnapshot, stats: MemoryStats):
        """Check for memory alerts and trigger callbacks"""
        alert_level = stats.alert_level

        if alert_level in [MemoryAlert.HIGH, MemoryAlert.CRITICAL, MemoryAlert.EMERGENCY]:
            message = (f"MEMORY ALERT [{alert_level.value.upper()}] {self.process_id}: "
                      f"{snapshot.memory_mb:.1f}MB / {self.memory_limit_mb}MB "
                      f"({snapshot.memory_mb/self.memory_limit_mb*100:.1f}%)")

            if alert_level == MemoryAlert.EMERGENCY:
                self.logger.critical(message)
                # Trigger emergency callbacks
                for callback in self.emergency_callbacks:
                    try:
                        callback(snapshot, stats)
                    except Exception as e:
                        self.logger.error(f"Emergency callback failed: {e}")
            else:
                self.logger.warning(message)

            # Trigger alert callbacks
            for callback in self.alert_callbacks:
                try:
                    callback(snapshot, stats, alert_level)
                except Exception as e:
                    self.logger.error(f"Alert callback failed: {e}")

        # Memory leak alerts
        if stats.leak_detected:
            self.logger.warning(f"MEMORY LEAK DETECTED in {self.process_id}: "
                               f"Growing at {stats.leak_rate:.2f}MB/min")

    def _should_auto_cleanup(self, snapshot: MemorySnapshot) -> bool:
        """Determine if automatic cleanup should be triggered"""
        usage_percent = snapshot.memory_mb / self.memory_limit_mb

        # Cleanup if over threshold
        if usage_percent >= self.cleanup_threshold:
            return True

        # Cleanup if leak detected and growing fast
        stats = self.get_memory_stats()
        if stats.leak_detected and stats.leak_rate > 10.0:  # 10MB/min
            return True

        return False

    def cleanup_memory(self, force: bool = False) -> bool:
        """Perform memory cleanup and garbage collection"""
        try:
            self.logger.info(f"Starting memory cleanup for {self.process_id}")

            # Record pre-cleanup memory
            pre_cleanup = self.take_snapshot()
            pre_memory = pre_cleanup.memory_mb

            # Explicit garbage collection
            self.logger.debug("Running garbage collection...")
            collected = gc.collect()

            # Force collection of all generations
            for generation in range(3):
                gc.collect(generation)

            # Clear weak references
            gc.collect()

            # Additional cleanup for specific objects if callbacks registered
            for name, callback in self._counter_callbacks.items():
                if hasattr(callback, 'cleanup'):
                    try:
                        callback.cleanup()
                    except Exception as e:
                        self.logger.warning(f"Cleanup callback '{name}' failed: {e}")

            # Wait a moment for cleanup to take effect
            time.sleep(1)

            # Record post-cleanup memory
            post_cleanup = self.take_snapshot()
            post_memory = post_cleanup.memory_mb

            # Calculate cleanup effectiveness
            memory_freed = pre_memory - post_memory
            cleanup_percent = (memory_freed / pre_memory * 100) if pre_memory > 0 else 0

            self.last_cleanup = datetime.now()
            self.cleanup_count += 1

            self.logger.info(f"Memory cleanup completed: "
                           f"Freed {memory_freed:.1f}MB ({cleanup_percent:.1f}%) "
                           f"- {pre_memory:.1f}MB -> {post_memory:.1f}MB")

            return memory_freed > 0

        except Exception as e:
            self.logger.error(f"Memory cleanup failed: {e}")
            return False

    def add_alert_callback(self, callback: Callable):
        """Add callback for memory alerts"""
        self.alert_callbacks.append(callback)

    def add_emergency_callback(self, callback: Callable):
        """Add callback for emergency memory situations"""
        self.emergency_callbacks.append(callback)

    def add_custom_counter(self, name: str, callback: Callable):
        """Add custom counter with callback"""
        self._counter_callbacks[name] = callback

    def set_custom_counter(self, name: str, value: Any):
        """Set static custom counter value"""
        self.custom_counters[name] = value

    def _update_redis_state(self, snapshot: MemorySnapshot, stats: MemoryStats):
        """Update memory state in Redis for system-wide monitoring"""
        if not self.redis_manager:
            return

        try:
            # Store current snapshot
            snapshot_key = f"memory_snapshot_{self.process_id}"
            snapshot_data = asdict(snapshot)
            # Convert datetime to string for Redis
            snapshot_data['timestamp'] = snapshot.timestamp.isoformat()

            self.redis_manager.set_data(snapshot_key, snapshot_data, ttl=300)  # 5 min TTL

            # Store stats
            stats_key = f"memory_stats_{self.process_id}"
            stats_data = asdict(stats)
            # Convert datetime to string for Redis
            stats_data['start_time'] = stats.start_time.isoformat()
            if stats.last_cleanup:
                stats_data['last_cleanup'] = stats.last_cleanup.isoformat()
            stats_data['alert_level'] = stats.alert_level.value

            self.redis_manager.set_data(stats_key, stats_data, ttl=300)  # 5 min TTL

            # Update system-wide memory registry
            registry_key = "system_memory_registry"
            registry_data = {
                'process_id': self.process_id,
                'memory_mb': snapshot.memory_mb,
                'memory_limit_mb': self.memory_limit_mb,
                'alert_level': stats.alert_level.value,
                'last_update': datetime.now().isoformat()
            }

            self.redis_manager.hset(registry_key, self.process_id, registry_data)

        except Exception as e:
            self.logger.error(f"Failed to update Redis memory state: {e}")

    def get_system_memory_overview(self) -> Dict[str, Any]:
        """Get overview of memory usage across all processes"""
        if not self.redis_manager:
            return {}

        try:
            registry_key = "system_memory_registry"
            registry_data = self.redis_manager.hgetall(registry_key)

            overview = {
                'total_processes': len(registry_data),
                'total_memory_mb': 0,
                'total_limit_mb': 0,
                'processes': {},
                'alerts': []
            }

            for process_id, data in registry_data.items():
                if isinstance(data, dict):
                    overview['processes'][process_id] = data
                    overview['total_memory_mb'] += data.get('memory_mb', 0)
                    overview['total_limit_mb'] += data.get('memory_limit_mb', 0)

                    # Collect alerts
                    alert_level = data.get('alert_level', 'low')
                    if alert_level not in ['low', 'medium']:
                        overview['alerts'].append({
                            'process_id': process_id,
                            'alert_level': alert_level,
                            'memory_mb': data.get('memory_mb', 0),
                            'memory_limit_mb': data.get('memory_limit_mb', 0)
                        })

            return overview

        except Exception as e:
            self.logger.error(f"Failed to get system memory overview: {e}")
            return {}

    def _check_for_leaks(self, stats: MemoryStats):
        """Check for memory leaks and trigger alerts"""
        try:
            if stats.leak_detected:
                self.logger.warning(f"MEMORY LEAK DETECTED in {self.process_id}: "
                                   f"Growing at {stats.leak_rate:.2f}MB/min")

                # Trigger leak-specific callbacks
                for callback in self.alert_callbacks:
                    try:
                        # DEFENSIVE FIX: Create a dummy snapshot to prevent callback errors
                        dummy_snapshot = type('MemorySnapshot', (), {
                            'memory_mb': getattr(stats, 'current_memory', 0.0),
                            'timestamp': datetime.now(),
                            'process_id': self.process_id
                        })()
                        callback(dummy_snapshot, stats, MemoryAlert.HIGH)
                    except Exception as e:
                        self.logger.error(f"Leak callback failed: {e}")
        except Exception as e:
            self.logger.error(f"Error in _check_for_leaks: {e}")

def memory_tracked(monitor: MemoryMonitor,
                  counter_name: str = None,
                  cleanup_on_exit: bool = True):
    """
    Decorator to track memory usage of functions

    Args:
        monitor: MemoryMonitor instance
        counter_name: Name for custom counter tracking
        cleanup_on_exit: Whether to cleanup after function execution
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Take snapshot before execution
            pre_snapshot = monitor.take_snapshot()

            try:
                # Execute function
                result = func(*args, **kwargs)

                # Take snapshot after execution
                post_snapshot = monitor.take_snapshot()

                # Calculate memory usage
                memory_used = post_snapshot.memory_mb - pre_snapshot.memory_mb

                # Update custom counter if specified
                if counter_name:
                    monitor.set_custom_counter(f"{counter_name}_calls",
                                             monitor.custom_counters.get(f"{counter_name}_calls", 0) + 1)
                    monitor.set_custom_counter(f"{counter_name}_memory_used", memory_used)

                # Log if significant memory usage
                if abs(memory_used) > 10:  # More than 10MB
                    monitor.logger.info(f"Function {func.__name__} used {memory_used:.1f}MB")

                # Cleanup if requested and memory usage is high
                if cleanup_on_exit and memory_used > 50:  # More than 50MB
                    monitor.cleanup_memory()

                return result

            except Exception as e:
                # Take snapshot on error
                error_snapshot = monitor.take_snapshot()
                memory_used = error_snapshot.memory_mb - pre_snapshot.memory_mb

                monitor.logger.error(f"Function {func.__name__} failed after using {memory_used:.1f}MB")

                # Cleanup on error if significant memory was used
                if cleanup_on_exit and memory_used > 20:
                    monitor.cleanup_memory()

                raise

        return wrapper
    return decorator

# Global memory monitor instances
_global_monitors: Dict[str, MemoryMonitor] = {}

def get_memory_monitor(process_id: str,
                      memory_limit_mb: float = 250,
                      auto_start: bool = True) -> MemoryMonitor:
    """Get or create a memory monitor for a process"""
    if process_id not in _global_monitors:
        monitor = MemoryMonitor(process_id, memory_limit_mb)
        _global_monitors[process_id] = monitor

        if auto_start:
            monitor.start_monitoring()

    return _global_monitors[process_id]

def cleanup_all_monitors():
    """Cleanup all global memory monitors"""
    for monitor in _global_monitors.values():
        monitor.stop_monitoring()
    _global_monitors.clear()
