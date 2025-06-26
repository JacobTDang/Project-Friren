"""
Symbol Configuration Classes

Defines configuration and state management classes for per-symbol coordination.
These classes handle monitoring intensity, resource allocation, and state tracking
for individual symbols in the enhanced multi-symbol trading system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum


class MonitoringIntensity(Enum):
    """Symbol monitoring intensity levels"""
    PASSIVE = "passive"      # 15-minute intervals, minimal resources
    ACTIVE = "active"        # 5-minute intervals, moderate resources
    INTENSIVE = "intensive"  # 1-minute intervals, maximum resources


class SymbolHealth(Enum):
    """Symbol health status"""
    HEALTHY = "healthy"        # Normal operation
    DEGRADED = "degraded"      # Some issues but functional
    ERROR = "error"            # Significant issues
    SUSPENDED = "suspended"    # Temporarily disabled


@dataclass
class SymbolMonitoringConfig:
    """Configuration for per-symbol monitoring behavior"""

    symbol: str
    monitoring_intensity: MonitoringIntensity = MonitoringIntensity.ACTIVE
    update_frequency: int = 300  # seconds between updates
    api_call_budget: int = 80    # max API calls per hour
    resource_priority: int = 2   # 1=high, 2=medium, 3=low

    # Risk management
    volatility_threshold: float = 0.02  # 2% threshold for increased monitoring
    position_limit: float = 0.15        # 15% max position size
    stop_loss_threshold: float = 0.05   # 5% stop loss

    # Performance thresholds
    max_decision_time: int = 30         # max seconds for decision processing
    max_error_count: int = 5            # max errors before degrading
    recovery_wait_time: int = 300       # seconds before retry after errors

    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.update_frequency < 60:
            raise ValueError("Update frequency must be at least 60 seconds")
        if self.api_call_budget < 10:
            raise ValueError("API call budget must be at least 10 per hour")
        if not (1 <= self.resource_priority <= 3):
            raise ValueError("Resource priority must be 1, 2, or 3")
        if not (0.0 < self.position_limit <= 1.0):
            raise ValueError("Position limit must be between 0.0 and 1.0")


@dataclass
class SymbolResourceBudget:
    """Resource budget allocation for a symbol"""

    # API limits
    api_calls_per_hour: int = 80
    api_calls_used: int = 0
    api_reset_time: Optional[datetime] = None

    # CPU allocation (percentage of total)
    cpu_allocation_pct: float = 16.0  # 16% for 5 symbols on average
    cpu_usage_current: float = 0.0

    # Memory allocation (MB)
    memory_allocation_mb: float = 160.0  # 160MB for 5 symbols on average
    memory_usage_current: float = 0.0

    # Processing time limits
    max_processing_time: int = 30  # max seconds per processing cycle
    average_processing_time: float = 0.0

    def get_remaining_api_calls(self) -> int:
        """Get remaining API calls for current hour"""
        return max(0, self.api_calls_per_hour - self.api_calls_used)

    def can_make_api_call(self) -> bool:
        """Check if API call is allowed within budget"""
        return self.get_remaining_api_calls() > 0

    def record_api_call(self) -> None:
        """Record an API call usage"""
        self.api_calls_used += 1

    def reset_hourly_usage(self) -> None:
        """Reset hourly usage counters"""
        self.api_calls_used = 0
        self.api_reset_time = datetime.now()

    def is_over_cpu_limit(self) -> bool:
        """Check if CPU usage is over allocated limit"""
        return self.cpu_usage_current > self.cpu_allocation_pct * 1.2  # 20% buffer

    def is_over_memory_limit(self) -> bool:
        """Check if memory usage is over allocated limit"""
        return self.memory_usage_current > self.memory_allocation_mb * 1.2  # 20% buffer


@dataclass
class SymbolState:
    """Current state and metrics for a symbol"""

    # Basic info
    symbol: str
    config: SymbolMonitoringConfig
    resource_budget: SymbolResourceBudget = field(default_factory=SymbolResourceBudget)

    # Status tracking
    health_status: SymbolHealth = SymbolHealth.HEALTHY
    last_update: Optional[datetime] = None
    last_successful_update: Optional[datetime] = None

    # Decision tracking
    last_decision: Optional[Dict[str, Any]] = None
    last_decision_time: Optional[datetime] = None
    pending_decisions: List[Dict[str, Any]] = field(default_factory=list)

    # Error tracking
    error_count: int = 0
    consecutive_errors: int = 0
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None

    # Performance metrics
    total_cycles: int = 0
    successful_cycles: int = 0
    average_cycle_time: float = 0.0

    # Position info
    current_position: float = 0.0
    position_value: float = 0.0
    unrealized_pnl: float = 0.0

    # Market data
    last_price: Optional[float] = None
    price_change_pct: Optional[float] = None
    volume: Optional[int] = None
    volatility: Optional[float] = None

    def update_health_status(self) -> None:
        """Update health status based on current metrics"""
        current_time = datetime.now()

        # Check for critical errors
        if self.consecutive_errors >= self.config.max_error_count:
            self.health_status = SymbolHealth.ERROR
            return

        # Check for resource overages
        if (self.resource_budget.is_over_cpu_limit() or
            self.resource_budget.is_over_memory_limit()):
            self.health_status = SymbolHealth.DEGRADED
            return

        # Check for stale data
        if (self.last_successful_update and
            (current_time - self.last_successful_update).total_seconds() >
            self.config.update_frequency * 3):  # 3x normal interval
            self.health_status = SymbolHealth.DEGRADED
            return

        # All checks passed
        self.health_status = SymbolHealth.HEALTHY

    def record_successful_cycle(self, processing_time: float) -> None:
        """Record a successful processing cycle"""
        self.total_cycles += 1
        self.successful_cycles += 1
        self.consecutive_errors = 0
        self.last_successful_update = datetime.now()

        # Update average cycle time
        if self.average_cycle_time == 0.0:
            self.average_cycle_time = processing_time
        else:
            self.average_cycle_time = (self.average_cycle_time * 0.9 +
                                     processing_time * 0.1)  # Rolling average

    def record_error(self, error_message: str) -> None:
        """Record an error during processing"""
        self.total_cycles += 1
        self.error_count += 1
        self.consecutive_errors += 1
        self.last_error = error_message
        self.last_error_time = datetime.now()

        # Update health status
        self.update_health_status()

    def get_success_rate(self) -> float:
        """Get success rate as percentage"""
        if self.total_cycles == 0:
            return 100.0
        return (self.successful_cycles / self.total_cycles) * 100.0

    def should_increase_intensity(self) -> bool:
        """Check if monitoring intensity should be increased"""
        # Increase intensity for high volatility
        if (self.volatility and
            self.volatility > self.config.volatility_threshold):
            return True

        # Increase intensity for large positions
        if abs(self.current_position) > self.config.position_limit * 0.8:
            return True

        # Increase intensity for significant price movements
        if (self.price_change_pct and
            abs(self.price_change_pct) > self.config.volatility_threshold):
            return True

        return False

    def should_decrease_intensity(self) -> bool:
        """Check if monitoring intensity should be decreased"""
        # Decrease intensity for stable, low-volatility situations
        if (self.volatility and self.volatility < self.config.volatility_threshold * 0.5 and
            self.price_change_pct and abs(self.price_change_pct) < self.config.volatility_threshold * 0.5 and
            abs(self.current_position) < self.config.position_limit * 0.3):
            return True

        return False
