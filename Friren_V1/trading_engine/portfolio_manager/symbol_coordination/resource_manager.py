#!/usr/bin/env python3
"""
resource_manager.py

Symbol Coordination Resource Manager

Manages resource allocation across symbols for optimal trading system performance.
Handles API rate limiting, memory management, and CPU allocation for multi-symbol
trading on resource-constrained AWS EC2 t3.micro instances.

Key Features:
- Dynamic resource allocation based on symbol priority and performance
- API rate limiting with intelligent distribution
- Memory and CPU monitoring and optimization
- Symbol-specific resource budgets and tracking
- Adaptive resource reallocation based on market conditions

Resource Types Managed:
- API calls (rate limiting across all symbols)
- Memory allocation (process-level tracking)
- CPU time allocation (process scheduling)
- Network bandwidth (API call prioritization)
"""

import time
import threading
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
from collections import defaultdict, deque

from .symbol_config import SymbolMonitoringConfig, MonitoringIntensity, SymbolState, SymbolHealth


class ResourceType(Enum):
    """Types of resources managed by the system"""
    API_CALLS = "api_calls"
    MEMORY = "memory"
    CPU = "cpu"
    NETWORK = "network"


class ResourcePriority(Enum):
    """Resource allocation priority levels"""
    CRITICAL = "critical"      # Emergency situations, position exits
    HIGH = "high"             # Active trading decisions
    NORMAL = "normal"         # Regular monitoring
    LOW = "low"              # Background analysis
    MINIMAL = "minimal"       # Passive monitoring


@dataclass
class ResourceAllocation:
    """Resource allocation for a specific symbol"""
    symbol: str
    resource_type: ResourceType
    allocated_amount: float
    used_amount: float = 0.0
    remaining_amount: float = 0.0
    priority: ResourcePriority = ResourcePriority.NORMAL
    last_updated: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        self.remaining_amount = self.allocated_amount - self.used_amount

    def update_usage(self, amount: float) -> bool:
        """Update resource usage, return True if within limits"""
        if self.used_amount + amount <= self.allocated_amount:
            self.used_amount += amount
            self.remaining_amount = self.allocated_amount - self.used_amount
            self.last_updated = datetime.now()
            return True
        return False

    def reset_usage(self):
        """Reset usage counters (for hourly resets)"""
        self.used_amount = 0.0
        self.remaining_amount = self.allocated_amount
        self.last_updated = datetime.now()


@dataclass
class ResourceQuota:
    """System-wide resource quotas"""
    api_calls_per_hour: int = 400
    memory_limit_mb: float = 800.0
    cpu_limit_percent: float = 80.0
    network_bandwidth_limit: float = 100.0  # Requests per minute

    # Buffer percentages (don't use 100% of limits)
    api_buffer: float = 0.8
    memory_buffer: float = 0.85
    cpu_buffer: float = 0.8
    network_buffer: float = 0.9


@dataclass
class ResourceUsageMetrics:
    """Current resource usage metrics"""
    timestamp: datetime = field(default_factory=datetime.now)

    # API usage
    api_calls_used: int = 0
    api_calls_remaining: int = 0
    api_rate_per_minute: float = 0.0

    # Memory usage
    memory_used_mb: float = 0.0
    memory_available_mb: float = 0.0
    memory_usage_percent: float = 0.0

    # CPU usage
    cpu_usage_percent: float = 0.0
    cpu_available_percent: float = 0.0

    # Network usage
    network_requests_per_minute: float = 0.0
    network_usage_percent: float = 0.0

    # Symbol-specific usage
    symbols_using_resources: int = 0
    high_priority_symbols: int = 0
    resource_pressure: float = 0.0  # 0.0 to 1.0, overall pressure indicator


class ResourceManager:
    """Manages resource allocation across symbols"""

    def __init__(self, quota: Optional[ResourceQuota] = None):
        self.quota = quota or ResourceQuota()
        self.logger = logging.getLogger(__name__)

        # Resource tracking
        self.allocations: Dict[str, Dict[ResourceType, ResourceAllocation]] = defaultdict(dict)
        self.usage_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=60))  # 60 minutes

        # System metrics
        self.current_metrics = ResourceUsageMetrics()
        self.last_reset_time = datetime.now()

        # Thread safety
        self._lock = threading.RLock()

        # Monitoring
        self._resource_monitor_thread = None
        self._shutdown_event = threading.Event()

        self.logger.info("ResourceManager initialized")

    def start_monitoring(self):
        """Start resource monitoring thread"""
        if self._resource_monitor_thread is None:
            self._resource_monitor_thread = threading.Thread(
                target=self._monitor_resources,
                name="ResourceMonitor",
                daemon=True
            )
            self._resource_monitor_thread.start()
            self.logger.info("Resource monitoring started")

    def stop_monitoring(self):
        """Stop resource monitoring"""
        self._shutdown_event.set()
        if self._resource_monitor_thread:
            self._resource_monitor_thread.join(timeout=5)
            self._resource_monitor_thread = None
        self.logger.info("Resource monitoring stopped")

    def allocate_resources(self, symbol: str, config: SymbolMonitoringConfig) -> bool:
        """Allocate resources for a symbol based on its configuration"""
        with self._lock:
            try:
                # Calculate base allocations based on monitoring intensity
                base_api_calls = self._calculate_api_allocation(config.monitoring_intensity)
                base_memory = self._calculate_memory_allocation(config.monitoring_intensity)
                base_cpu = self._calculate_cpu_allocation(config.monitoring_intensity)

                # Apply symbol-specific parameters
                api_allocation = min(base_api_calls, config.api_call_budget)
                memory_allocation = base_memory  # Use base memory allocation
                cpu_allocation = base_cpu        # Use base CPU allocation

                # Ensure we don't exceed system limits
                if not self._can_allocate_resources(api_allocation, memory_allocation, cpu_allocation):
                    self.logger.warning(f"Cannot allocate resources for {symbol}: system limits exceeded")
                    return False

                # Create allocations
                self.allocations[symbol] = {
                    ResourceType.API_CALLS: ResourceAllocation(
                        symbol=symbol,
                        resource_type=ResourceType.API_CALLS,
                        allocated_amount=api_allocation,
                        priority=self._get_priority_from_intensity(config.monitoring_intensity)
                    ),
                    ResourceType.MEMORY: ResourceAllocation(
                        symbol=symbol,
                        resource_type=ResourceType.MEMORY,
                        allocated_amount=memory_allocation,
                        priority=self._get_priority_from_intensity(config.monitoring_intensity)
                    ),
                    ResourceType.CPU: ResourceAllocation(
                        symbol=symbol,
                        resource_type=ResourceType.CPU,
                        allocated_amount=cpu_allocation,
                        priority=self._get_priority_from_intensity(config.monitoring_intensity)
                    )
                }

                self.logger.info(f"Resources allocated for {symbol}: API={api_allocation}, Memory={memory_allocation}MB, CPU={cpu_allocation}%")
                return True

            except Exception as e:
                self.logger.error(f"Error allocating resources for {symbol}: {e}")
                return False

    def deallocate_resources(self, symbol: str):
        """Deallocate resources for a symbol"""
        with self._lock:
            if symbol in self.allocations:
                del self.allocations[symbol]
                if symbol in self.usage_history:
                    del self.usage_history[symbol]
                self.logger.info(f"Resources deallocated for {symbol}")

    def can_make_api_call(self, symbol: str, calls_needed: int = 1) -> bool:
        """Check if symbol can make API calls within its allocation"""
        with self._lock:
            if symbol not in self.allocations:
                return False

            api_allocation = self.allocations[symbol].get(ResourceType.API_CALLS)
            if not api_allocation:
                return False

            return api_allocation.remaining_amount >= calls_needed

    def record_api_call(self, symbol: str, calls_made: int = 1) -> bool:
        """Record API call usage for symbol"""
        with self._lock:
            if symbol not in self.allocations:
                return False

            api_allocation = self.allocations[symbol].get(ResourceType.API_CALLS)
            if not api_allocation:
                return False

            success = api_allocation.update_usage(calls_made)
            if success:
                # Update usage history
                self.usage_history[symbol].append({
                    'timestamp': datetime.now(),
                    'resource_type': ResourceType.API_CALLS,
                    'amount': calls_made
                })

                # Update system metrics
                self.current_metrics.api_calls_used += calls_made
                self.current_metrics.api_calls_remaining = max(0,
                    self.current_metrics.api_calls_remaining - calls_made)

            return success

    def get_symbol_resource_status(self, symbol: str) -> Dict[str, Any]:
        """Get current resource status for a symbol"""
        with self._lock:
            if symbol not in self.allocations:
                return {}

            status = {}
            for resource_type, allocation in self.allocations[symbol].items():
                status[resource_type.value] = {
                    'allocated': allocation.allocated_amount,
                    'used': allocation.used_amount,
                    'remaining': allocation.remaining_amount,
                    'utilization_percent': (allocation.used_amount / allocation.allocated_amount) * 100 if allocation.allocated_amount > 0 else 0,
                    'priority': allocation.priority.value,
                    'last_updated': allocation.last_updated.isoformat()
                }

            return status

    def get_system_resource_status(self) -> Dict[str, Any]:
        """Get overall system resource status"""
        with self._lock:
            # Calculate totals
            total_api_allocated = sum(
                alloc[ResourceType.API_CALLS].allocated_amount
                for alloc in self.allocations.values()
                if ResourceType.API_CALLS in alloc
            )

            total_api_used = sum(
                alloc[ResourceType.API_CALLS].used_amount
                for alloc in self.allocations.values()
                if ResourceType.API_CALLS in alloc
            )

            total_memory_allocated = sum(
                alloc[ResourceType.MEMORY].allocated_amount
                for alloc in self.allocations.values()
                if ResourceType.MEMORY in alloc
            )

            return {
                'quota': {
                    'api_calls_per_hour': self.quota.api_calls_per_hour,
                    'memory_limit_mb': self.quota.memory_limit_mb,
                    'cpu_limit_percent': self.quota.cpu_limit_percent
                },
                'usage': {
                    'api_calls_allocated': total_api_allocated,
                    'api_calls_used': total_api_used,
                    'api_calls_remaining': self.quota.api_calls_per_hour * self.quota.api_buffer - total_api_used,
                    'memory_allocated_mb': total_memory_allocated,
                    'memory_used_mb': self.current_metrics.memory_used_mb,
                    'cpu_usage_percent': self.current_metrics.cpu_usage_percent
                },
                'symbols': {
                    'total_managed': len(self.allocations),
                    'high_priority': sum(1 for alloc in self.allocations.values()
                                       if ResourceType.API_CALLS in alloc and
                                       alloc[ResourceType.API_CALLS].priority in [ResourcePriority.HIGH, ResourcePriority.CRITICAL])
                },
                'resource_pressure': self._calculate_resource_pressure(),
                'last_reset': self.last_reset_time.isoformat()
            }

    def optimize_allocations(self) -> Dict[str, Any]:
        """Optimize resource allocations based on usage patterns"""
        with self._lock:
            optimization_results = {
                'reallocations': 0,
                'api_calls_freed': 0,
                'memory_freed': 0,
                'symbols_optimized': []
            }

            # Find underutilized symbols
            for symbol, allocations in self.allocations.items():
                if ResourceType.API_CALLS in allocations:
                    api_alloc = allocations[ResourceType.API_CALLS]
                    utilization = api_alloc.used_amount / api_alloc.allocated_amount if api_alloc.allocated_amount > 0 else 0

                    # If utilization is very low, reduce allocation
                    if utilization < 0.3 and api_alloc.allocated_amount > 10:
                        reduction = api_alloc.allocated_amount * 0.2
                        api_alloc.allocated_amount -= reduction
                        api_alloc.remaining_amount = api_alloc.allocated_amount - api_alloc.used_amount

                        optimization_results['reallocations'] += 1
                        optimization_results['api_calls_freed'] += reduction
                        optimization_results['symbols_optimized'].append(symbol)

                        self.logger.info(f"Reduced API allocation for {symbol} by {reduction:.0f} calls due to low utilization")

            return optimization_results

    def reset_hourly_usage(self):
        """Reset hourly usage counters"""
        with self._lock:
            for symbol_allocations in self.allocations.values():
                for allocation in symbol_allocations.values():
                    if allocation.resource_type == ResourceType.API_CALLS:
                        allocation.reset_usage()

            self.current_metrics.api_calls_used = 0
            self.current_metrics.api_calls_remaining = int(self.quota.api_calls_per_hour * self.quota.api_buffer)
            self.last_reset_time = datetime.now()

            self.logger.info("Hourly resource usage reset")

    def _calculate_api_allocation(self, intensity: MonitoringIntensity) -> float:
        """Calculate base API allocation based on monitoring intensity"""
        base_budget = self.quota.api_calls_per_hour * self.quota.api_buffer

        if intensity == MonitoringIntensity.INTENSIVE:
            return base_budget * 0.4  # 40% for intensive
        elif intensity == MonitoringIntensity.ACTIVE:
            return base_budget * 0.25  # 25% for active
        else:  # PASSIVE
            return base_budget * 0.1   # 10% for passive

    def _calculate_memory_allocation(self, intensity: MonitoringIntensity) -> float:
        """Calculate base memory allocation based on monitoring intensity"""
        base_memory = self.quota.memory_limit_mb * self.quota.memory_buffer

        if intensity == MonitoringIntensity.INTENSIVE:
            return base_memory * 0.3  # 30% for intensive
        elif intensity == MonitoringIntensity.ACTIVE:
            return base_memory * 0.2  # 20% for active
        else:  # PASSIVE
            return base_memory * 0.1  # 10% for passive

    def _calculate_cpu_allocation(self, intensity: MonitoringIntensity) -> float:
        """Calculate base CPU allocation based on monitoring intensity"""
        base_cpu = self.quota.cpu_limit_percent * self.quota.cpu_buffer

        if intensity == MonitoringIntensity.INTENSIVE:
            return base_cpu * 0.3  # 30% for intensive
        elif intensity == MonitoringIntensity.ACTIVE:
            return base_cpu * 0.2  # 20% for active
        else:  # PASSIVE
            return base_cpu * 0.1  # 10% for passive

    def _get_priority_from_intensity(self, intensity: MonitoringIntensity) -> ResourcePriority:
        """Convert monitoring intensity to resource priority"""
        if intensity == MonitoringIntensity.INTENSIVE:
            return ResourcePriority.HIGH
        elif intensity == MonitoringIntensity.ACTIVE:
            return ResourcePriority.NORMAL
        else:  # PASSIVE
            return ResourcePriority.LOW

    def _can_allocate_resources(self, api_calls: float, memory_mb: float, cpu_percent: float) -> bool:
        """Check if we can allocate the requested resources"""
        # Calculate current total allocations
        current_api = sum(
            alloc[ResourceType.API_CALLS].allocated_amount
            for alloc in self.allocations.values()
            if ResourceType.API_CALLS in alloc
        )

        current_memory = sum(
            alloc[ResourceType.MEMORY].allocated_amount
            for alloc in self.allocations.values()
            if ResourceType.MEMORY in alloc
        )

        current_cpu = sum(
            alloc[ResourceType.CPU].allocated_amount
            for alloc in self.allocations.values()
            if ResourceType.CPU in alloc
        )

        # Check if adding new allocations would exceed limits
        max_api = self.quota.api_calls_per_hour * self.quota.api_buffer
        max_memory = self.quota.memory_limit_mb * self.quota.memory_buffer
        max_cpu = self.quota.cpu_limit_percent * self.quota.cpu_buffer

        return (current_api + api_calls <= max_api and
                current_memory + memory_mb <= max_memory and
                current_cpu + cpu_percent <= max_cpu)

    def _calculate_resource_pressure(self) -> float:
        """Calculate overall resource pressure (0.0 to 1.0)"""
        # API pressure
        total_api = sum(
            alloc[ResourceType.API_CALLS].allocated_amount
            for alloc in self.allocations.values()
            if ResourceType.API_CALLS in alloc
        )
        api_pressure = total_api / (self.quota.api_calls_per_hour * self.quota.api_buffer)

        # Memory pressure
        memory_pressure = self.current_metrics.memory_used_mb / self.quota.memory_limit_mb

        # CPU pressure
        cpu_pressure = self.current_metrics.cpu_usage_percent / self.quota.cpu_limit_percent

        # Overall pressure is the maximum of individual pressures
        return min(1.0, max(api_pressure, memory_pressure, cpu_pressure))

    def _monitor_resources(self):
        """Background resource monitoring thread"""
        while not self._shutdown_event.is_set():
            try:
                # Update system metrics
                self._update_system_metrics()

                # Check for hourly reset
                now = datetime.now()
                if now - self.last_reset_time >= timedelta(hours=1):
                    self.reset_hourly_usage()

                # Check for optimization opportunities
                if now.minute % 15 == 0:  # Every 15 minutes
                    self.optimize_allocations()

                time.sleep(60)  # Monitor every minute

            except Exception as e:
                self.logger.error(f"Error in resource monitoring: {e}")
                time.sleep(60)

    def _update_system_metrics(self):
        """Update current system resource metrics"""
        try:
            import psutil

            # Memory metrics
            memory = psutil.virtual_memory()
            self.current_metrics.memory_used_mb = (memory.total - memory.available) / 1024 / 1024
            self.current_metrics.memory_available_mb = memory.available / 1024 / 1024
            self.current_metrics.memory_usage_percent = memory.percent

            # CPU metrics
            self.current_metrics.cpu_usage_percent = psutil.cpu_percent(interval=1)
            self.current_metrics.cpu_available_percent = 100 - self.current_metrics.cpu_usage_percent

            # API metrics (calculated from allocations)
            total_api_used = sum(
                alloc[ResourceType.API_CALLS].used_amount
                for alloc in self.allocations.values()
                if ResourceType.API_CALLS in alloc
            )
            self.current_metrics.api_calls_used = int(total_api_used)
            self.current_metrics.api_calls_remaining = int(
                self.quota.api_calls_per_hour * self.quota.api_buffer - total_api_used
            )

            # Resource pressure
            self.current_metrics.resource_pressure = self._calculate_resource_pressure()

            # Symbol counts
            self.current_metrics.symbols_using_resources = len(self.allocations)
            self.current_metrics.high_priority_symbols = sum(
                1 for alloc in self.allocations.values()
                if ResourceType.API_CALLS in alloc and
                alloc[ResourceType.API_CALLS].priority in [ResourcePriority.HIGH, ResourcePriority.CRITICAL]
            )

            self.current_metrics.timestamp = datetime.now()

        except ImportError:
            # Fallback if psutil not available
            self.current_metrics.memory_usage_percent = 50.0  # Assume moderate usage
            self.current_metrics.cpu_usage_percent = 30.0
