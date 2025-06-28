#!/usr/bin/env python3
"""
memory_cleanup_manager.py

Advanced Memory Cleanup and Leak Prevention Manager

Features:
- Intelligent cleanup strategies for different data types
- Process-specific cleanup routines
- Emergency memory recovery
- System-wide memory coordination
- Proactive leak prevention
"""

import gc
import os
import sys
import time
import threading
import logging
import weakref
from typing import Dict, Any, List, Set, Optional, Callable
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from Friren_V1.multiprocess_infrastructure.memory_monitor import MemoryMonitor, MemoryAlert

@dataclass
class CleanupStrategy:
    """Definition of a cleanup strategy"""
    name: str
    description: str
    priority: int  # Lower number = higher priority
    target_types: List[type]
    cleanup_function: Callable
    emergency_only: bool = False
    memory_threshold_mb: float = 100  # Trigger when this much memory could be freed

class MemoryCleanupManager:
    """
    Advanced memory cleanup manager with intelligent strategies
    """

    def __init__(self, process_id: str, memory_monitor=None):
        self.process_id = process_id
        self.memory_monitor = memory_monitor
        self.logger = logging.getLogger(f"memory_cleanup.{process_id}")

        # Cleanup strategies registry
        self.cleanup_strategies = []
        self.emergency_strategies = []

        # Tracked objects for cleanup
        self.tracked_objects = {}  # name -> weak references
        self.cleanup_callbacks = {}  # name -> cleanup function
        self.object_counters = defaultdict(int)

        # Cleanup statistics
        self.cleanup_count = 0
        self.total_memory_freed = 0.0
        self.last_cleanup_time = None

        # Emergency state
        self.emergency_mode = False
        self.emergency_triggered_count = 0

        # Threading
        self._lock = threading.RLock()

        # Initialize default strategies
        self._register_default_strategies()

        self.logger.info(f"Memory Cleanup Manager initialized for {process_id}")

    def _register_default_strategies(self):
        """Register default cleanup strategies"""

        # Strategy 1: Basic garbage collection
        self.register_strategy(CleanupStrategy(
            name="basic_gc",
            description="Basic garbage collection",
            priority=1,
            target_types=[object],
            cleanup_function=self._basic_gc_cleanup,
            memory_threshold_mb=10
        ))

        # Strategy 2: Cache cleanup
        self.register_strategy(CleanupStrategy(
            name="cache_cleanup",
            description="Clear internal caches",
            priority=2,
            target_types=[dict, list],
            cleanup_function=self._cache_cleanup,
            memory_threshold_mb=50
        ))

        # Strategy 3: Large object cleanup
        self.register_strategy(CleanupStrategy(
            name="large_objects",
            description="Cleanup large objects",
            priority=3,
            target_types=[list, dict, str, bytes],
            cleanup_function=self._large_object_cleanup,
            memory_threshold_mb=100
        ))

        # Strategy 4: DataFrame cleanup (for pandas if used)
        self.register_strategy(CleanupStrategy(
            name="dataframe_cleanup",
            description="Cleanup pandas DataFrames",
            priority=4,
            target_types=[],  # Will check dynamically
            cleanup_function=self._dataframe_cleanup,
            memory_threshold_mb=200
        ))

        # Emergency Strategy: Aggressive cleanup
        self.register_strategy(CleanupStrategy(
            name="aggressive_cleanup",
            description="Aggressive memory cleanup",
            priority=99,
            target_types=[object],
            cleanup_function=self._aggressive_cleanup,
            emergency_only=True,
            memory_threshold_mb=500
        ))

    def register_strategy(self, strategy: CleanupStrategy):
        """Register a new cleanup strategy"""
        with self._lock:
            if strategy.emergency_only:
                self.emergency_strategies.append(strategy)
                self.emergency_strategies.sort(key=lambda s: s.priority)
            else:
                self.cleanup_strategies.append(strategy)
                self.cleanup_strategies.sort(key=lambda s: s.priority)

        self.logger.debug(f"Registered cleanup strategy: {strategy.name}")

    def track_object(self, name: str, obj: Any, cleanup_callback: Callable = None):
        """Track an object for potential cleanup"""
        with self._lock:
            try:
                # Store weak reference to avoid keeping object alive
                self.tracked_objects[name] = weakref.ref(obj)

                if cleanup_callback:
                    self.cleanup_callbacks[name] = cleanup_callback

                # Count object type
                obj_type = type(obj).__name__
                self.object_counters[obj_type] += 1

                self.logger.debug(f"Tracking object '{name}' of type {obj_type}")

            except TypeError:
                # Object doesn't support weak references
                self.logger.debug(f"Cannot track object '{name}' - no weak reference support")

    def untrack_object(self, name: str):
        """Stop tracking an object"""
        with self._lock:
            if name in self.tracked_objects:
                del self.tracked_objects[name]
            if name in self.cleanup_callbacks:
                del self.cleanup_callbacks[name]

    def perform_cleanup(self, emergency: bool = False, target_memory_reduction: float = 0) -> float:
        """
        Perform memory cleanup using registered strategies

        Args:
            emergency: Whether this is an emergency cleanup
            target_memory_reduction: Target memory reduction in MB

        Returns:
            Amount of memory freed in MB
        """
        with self._lock:
            cleanup_start = time.time()

            # Get initial memory if monitor available
            initial_memory = 0
            if self.memory_monitor:
                initial_memory = self.memory_monitor.take_snapshot().memory_mb

            self.logger.info(f"Starting {'EMERGENCY ' if emergency else ''}cleanup "
                           f"(target: {target_memory_reduction}MB)")

            total_freed = 0.0
            strategies_used = []

            # Select strategies based on mode
            strategies = self.emergency_strategies if emergency else self.cleanup_strategies
            if emergency:
                self.emergency_mode = True
                self.emergency_triggered_count += 1

            # Execute strategies in priority order
            for strategy in strategies:
                try:
                    strategy_start = time.time()

                    # Execute strategy
                    freed = strategy.cleanup_function()

                    strategy_time = time.time() - strategy_start

                    if freed > 0:
                        total_freed += freed
                        strategies_used.append(strategy.name)

                        self.logger.info(f"Strategy '{strategy.name}' freed {freed:.1f}MB "
                                       f"in {strategy_time:.2f}s")

                    # Check if we've reached target
                    if target_memory_reduction > 0 and total_freed >= target_memory_reduction:
                        break

                except Exception as e:
                    self.logger.error(f"Strategy '{strategy.name}' failed: {e}")

            # Cleanup tracked objects
            freed_tracked = self._cleanup_tracked_objects(emergency)
            total_freed += freed_tracked

            # Final measurement
            final_memory = 0
            if self.memory_monitor:
                final_memory = self.memory_monitor.take_snapshot().memory_mb
                actual_freed = initial_memory - final_memory
            else:
                actual_freed = total_freed

            cleanup_time = time.time() - cleanup_start

            # Update statistics
            self.cleanup_count += 1
            self.total_memory_freed += actual_freed
            self.last_cleanup_time = datetime.now()

            self.logger.info(f"Cleanup completed: {actual_freed:.1f}MB freed "
                           f"in {cleanup_time:.2f}s using {len(strategies_used)} strategies")

            if emergency:
                self.emergency_mode = False

            return actual_freed

    def _cleanup_tracked_objects(self, emergency: bool = False) -> float:
        """Cleanup tracked objects"""
        freed = 0.0
        cleaned_objects = []

        # Get initial memory if available
        initial_memory = 0
        if self.memory_monitor:
            initial_memory = self.memory_monitor.take_snapshot().memory_mb

        for name, callback in list(self.cleanup_callbacks.items()):
            try:
                obj_ref = self.tracked_objects.get(name)
                if obj_ref and obj_ref():  # Object still exists
                    callback()
                    cleaned_objects.append(name)
                else:
                    # Object was garbage collected
                    self.untrack_object(name)

            except Exception as e:
                self.logger.warning(f"Cleanup callback for '{name}' failed: {e}")

        # Force garbage collection after callbacks
        gc.collect()

        # Calculate freed memory if monitor available
        if self.memory_monitor:
            final_memory = self.memory_monitor.take_snapshot().memory_mb
            freed = initial_memory - final_memory

        if cleaned_objects:
            self.logger.debug(f"Cleaned {len(cleaned_objects)} tracked objects: "
                            f"{', '.join(cleaned_objects)}")

        return max(0, freed)

    def _basic_gc_cleanup(self) -> float:
        """Basic garbage collection cleanup"""
        initial_memory = 0
        if self.memory_monitor:
            initial_memory = self.memory_monitor.take_snapshot().memory_mb

        # Collect all generations
        collected = 0
        for generation in range(3):
            collected += gc.collect(generation)

        # Additional collection
        collected += gc.collect()

        final_memory = 0
        freed = 0
        if self.memory_monitor:
            final_memory = self.memory_monitor.take_snapshot().memory_mb
            freed = initial_memory - final_memory

        self.logger.debug(f"Basic GC: collected {collected} objects, freed {freed:.1f}MB")
        return max(0, freed)

    def _cache_cleanup(self) -> float:
        """Cleanup internal caches"""
        initial_memory = 0
        if self.memory_monitor:
            initial_memory = self.memory_monitor.take_snapshot().memory_mb

        # Clear function caches
        try:
            import functools
            # Clear lru_cache decorated functions
            for obj in gc.get_objects():
                if hasattr(obj, 'cache_clear') and callable(getattr(obj, 'cache_clear')):
                    try:
                        obj.cache_clear()
                    except:
                        pass
        except:
            pass

        # Force GC after cache clearing
        gc.collect()

        final_memory = 0
        freed = 0
        if self.memory_monitor:
            final_memory = self.memory_monitor.take_snapshot().memory_mb
            freed = initial_memory - final_memory

        self.logger.debug(f"Cache cleanup freed {freed:.1f}MB")
        return max(0, freed)

    def _large_object_cleanup(self) -> float:
        """Cleanup large objects"""
        initial_memory = 0
        if self.memory_monitor:
            initial_memory = self.memory_monitor.take_snapshot().memory_mb

        # Find large objects
        large_objects = []
        for obj in gc.get_objects():
            try:
                size = sys.getsizeof(obj)
                if size > 10 * 1024 * 1024:  # 10MB threshold
                    large_objects.append((obj, size))
            except:
                pass

        # Sort by size (largest first)
        large_objects.sort(key=lambda x: x[1], reverse=True)

        # Attempt to cleanup largest objects
        cleaned = 0
        for obj, size in large_objects[:10]:  # Top 10 largest
            try:
                # Clear if it's a collection
                if hasattr(obj, 'clear'):
                    obj.clear()
                    cleaned += 1
                elif isinstance(obj, list):
                    obj[:] = []
                    cleaned += 1
                elif isinstance(obj, dict):
                    obj.clear()
                    cleaned += 1
            except:
                pass

        gc.collect()

        final_memory = 0
        freed = 0
        if self.memory_monitor:
            final_memory = self.memory_monitor.take_snapshot().memory_mb
            freed = initial_memory - final_memory

        self.logger.debug(f"Large object cleanup: cleaned {cleaned} objects, freed {freed:.1f}MB")
        return max(0, freed)

    def _dataframe_cleanup(self) -> float:
        """Cleanup pandas DataFrames if present"""
        initial_memory = 0
        if self.memory_monitor:
            initial_memory = self.memory_monitor.take_snapshot().memory_mb

        try:
            import pandas as pd

            # Find DataFrame objects
            dataframes = []
            for obj in gc.get_objects():
                if isinstance(obj, pd.DataFrame):
                    dataframes.append(obj)

            # Clear large DataFrames
            cleaned = 0
            for df in dataframes:
                try:
                    if len(df) > 1000:  # Large DataFrame
                        df.drop(df.index, inplace=True)
                        cleaned += 1
                except:
                    pass

            gc.collect()

            final_memory = 0
            freed = 0
            if self.memory_monitor:
                final_memory = self.memory_monitor.take_snapshot().memory_mb
                freed = initial_memory - final_memory

            self.logger.debug(f"DataFrame cleanup: cleaned {cleaned} DataFrames, freed {freed:.1f}MB")
            return max(0, freed)

        except ImportError:
            # Pandas not available
            return 0.0

    def _aggressive_cleanup(self) -> float:
        """Aggressive emergency cleanup"""
        initial_memory = 0
        if self.memory_monitor:
            initial_memory = self.memory_monitor.take_snapshot().memory_mb

        self.logger.warning("Executing AGGRESSIVE cleanup - may affect functionality")

        total_freed = 0.0

        # Run all other strategies first
        total_freed += self._basic_gc_cleanup()
        total_freed += self._cache_cleanup()
        total_freed += self._large_object_cleanup()
        total_freed += self._dataframe_cleanup()

        # Additional aggressive measures
        try:
            # Clear weak references
            weakref._remove_dead_weakref = lambda x: None

            # Force multiple GC cycles
            for _ in range(5):
                collected = gc.collect()
                if collected == 0:
                    break

            # Clear all tracked objects aggressively
            for name in list(self.tracked_objects.keys()):
                self.untrack_object(name)

        except Exception as e:
            self.logger.error(f"Aggressive cleanup error: {e}")

        final_memory = 0
        freed = 0
        if self.memory_monitor:
            final_memory = self.memory_monitor.take_snapshot().memory_mb
            freed = initial_memory - final_memory

        self.logger.warning(f"Aggressive cleanup freed {freed:.1f}MB total")
        return max(0, freed)

    def get_cleanup_stats(self) -> Dict[str, Any]:
        """Get cleanup statistics"""
        return {
            'cleanup_count': self.cleanup_count,
            'total_memory_freed_mb': self.total_memory_freed,
            'last_cleanup_time': self.last_cleanup_time.isoformat() if self.last_cleanup_time else None,
            'emergency_triggered_count': self.emergency_triggered_count,
            'tracked_objects_count': len(self.tracked_objects),
            'object_type_counts': dict(self.object_counters),
            'registered_strategies': len(self.cleanup_strategies),
            'emergency_strategies': len(self.emergency_strategies)
        }

    def suggest_cleanup(self) -> List[str]:
        """Suggest cleanup actions based on current state"""
        suggestions = []

        current_memory = 0
        stats = None
        if self.memory_monitor:
            current_memory = self.memory_monitor.take_snapshot().memory_mb
            stats = self.memory_monitor.get_memory_stats()

        # Memory-based suggestions
        if stats and stats.alert_level in [MemoryAlert.HIGH, MemoryAlert.CRITICAL]:
            suggestions.append("immediate_cleanup")

        if stats and stats.leak_detected:
            suggestions.append("investigate_leak")
            suggestions.append("aggressive_cleanup")

        # Object count based suggestions
        if len(self.tracked_objects) > 100:
            suggestions.append("cleanup_tracked_objects")

        if self.object_counters.get('DataFrame', 0) > 10:
            suggestions.append("dataframe_cleanup")

        if sum(self.object_counters.values()) > 10000:
            suggestions.append("general_cleanup")

        return suggestions

class SystemMemoryCoordinator:
    """
    Coordinates memory cleanup across all processes in the system
    """

    def __init__(self, redis_manager):
        self.redis_manager = redis_manager
        self.logger = logging.getLogger("system_memory_coordinator")

        # Process cleanup managers
        self.cleanup_managers = {}

        # System-wide thresholds
        self.system_memory_limit_gb = 1.0  # 1GB t3.micro limit
        self.emergency_threshold = 0.9  # 90% of system limit

        self.logger.info("System Memory Coordinator initialized")

    def register_process_manager(self, process_id: str, cleanup_manager: MemoryCleanupManager):
        """Register a process cleanup manager"""
        self.cleanup_managers[process_id] = cleanup_manager
        self.logger.info(f"Registered cleanup manager for {process_id}")

    def check_system_memory(self) -> Dict[str, Any]:
        """Check system-wide memory usage"""
        try:
            import psutil

            # System memory
            system_memory = psutil.virtual_memory()
            used_gb = system_memory.used / (1024**3)
            total_gb = system_memory.total / (1024**3)

            # Process memory overview
            process_overview = {}
            for process_id in self.cleanup_managers.keys():
                try:
                    registry_data = self.redis_manager.hget("system_memory_registry", process_id)
                    if registry_data:
                        process_overview[process_id] = registry_data
                except:
                    pass

            return {
                'system_memory_gb': {
                    'used': used_gb,
                    'total': total_gb,
                    'percent': system_memory.percent
                },
                'processes': process_overview,
                'emergency_threshold_reached': used_gb >= (self.system_memory_limit_gb * self.emergency_threshold)
            }

        except Exception as e:
            self.logger.error(f"Failed to check system memory: {e}")
            return {}

    def coordinate_cleanup(self, target_reduction_gb: float = 1.0) -> float:
        """Coordinate cleanup across all processes"""
        self.logger.info(f"Coordinating system-wide cleanup (target: {target_reduction_gb}GB)")

        total_freed = 0.0
        target_per_process = (target_reduction_gb * 1024) / max(len(self.cleanup_managers), 1)

        # Execute cleanup in parallel across processes
        for process_id, manager in self.cleanup_managers.items():
            try:
                freed = manager.perform_cleanup(target_memory_reduction=target_per_process)
                total_freed += freed

                self.logger.info(f"Process {process_id} freed {freed:.1f}MB")

            except Exception as e:
                self.logger.error(f"Cleanup failed for {process_id}: {e}")

        total_freed_gb = total_freed / 1024
        self.logger.info(f"System-wide cleanup completed: {total_freed_gb:.2f}GB freed")

        return total_freed_gb

    def emergency_system_cleanup(self) -> float:
        """Emergency cleanup across all processes"""
        self.logger.critical("EMERGENCY: System-wide memory cleanup initiated")

        total_freed = 0.0

        # Emergency cleanup for all processes
        for process_id, manager in self.cleanup_managers.items():
            try:
                freed = manager.perform_cleanup(emergency=True)
                total_freed += freed

                self.logger.critical(f"Emergency cleanup for {process_id}: {freed:.1f}MB freed")

            except Exception as e:
                self.logger.error(f"Emergency cleanup failed for {process_id}: {e}")

        total_freed_gb = total_freed / 1024
        self.logger.critical(f"Emergency system cleanup completed: {total_freed_gb:.2f}GB freed")

        return total_freed_gb

# Global system coordinator
_system_coordinator = None

def get_system_memory_coordinator(redis_manager=None):
    """Get or create the global system memory coordinator"""
    global _system_coordinator

    if _system_coordinator is None and redis_manager:
        _system_coordinator = SystemMemoryCoordinator(redis_manager)

    return _system_coordinator

# Global cleanup managers
_cleanup_managers: Dict[str, MemoryCleanupManager] = {}

def get_cleanup_manager(process_id: str, memory_monitor=None) -> MemoryCleanupManager:
    """Get or create a cleanup manager for a process"""
    if process_id not in _cleanup_managers:
        manager = MemoryCleanupManager(process_id, memory_monitor)
        _cleanup_managers[process_id] = manager

    return _cleanup_managers[process_id]

def cleanup_all_managers():
    """Cleanup all global managers"""
    _cleanup_managers.clear()
