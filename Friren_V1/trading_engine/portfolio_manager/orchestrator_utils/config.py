#!/usr/bin/env python3
"""
config.py

Configuration classes and enums for the orchestrator system
"""

from typing import List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class SystemState(Enum):
    """System-wide operational states"""
    INITIALIZING = "initializing"
    STARTING = "starting"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    EMERGENCY_STOP = "emergency_stop"


class TradingMode(Enum):
    """Trading operational modes"""
    LIVE_TRADING = "live_trading"          # Full live trading
    PAPER_TRADING = "paper_trading"        # Paper trading mode
    ANALYSIS_ONLY = "analysis_only"        # Analysis without trading
    MAINTENANCE = "maintenance"            # Maintenance mode
    EMERGENCY = "emergency"                # Emergency mode


@dataclass
class SystemConfig:
    """System-wide configuration"""
    # Trading configuration
    trading_mode: TradingMode = TradingMode.PAPER_TRADING
    symbols: List[str] = field(default_factory=lambda: ['AAPL', 'MSFT', 'GOOGL'])
    max_positions: int = 5
    portfolio_value: float = 100000.0

    # Process configuration
    process_restart_policy: str = "on_failure"
    health_check_interval: int = 300  # seconds - increased from 10 to 300 to reduce database load (5 minutes)
    decision_cycle_interval: int = 60  # seconds

    # Resource limits (for t3.micro)
    max_memory_mb: int = 800  # Leave 200MB buffer on 1GB instance
    max_cpu_percent: float = 80.0
    api_rate_limit_buffer: float = 0.8  # Use 80% of API limits

    # Emergency thresholds
    max_daily_loss_pct: float = 5.0
    max_position_loss_pct: float = 10.0
    emergency_exit_threshold: float = 15.0

    # Symbol coordination configuration
    enable_symbol_coordination: bool = True
    total_api_budget: int = 400  # Total API calls per hour across all symbols
    max_intensive_symbols: int = 2  # Max symbols allowed in intensive monitoring
    symbol_coordination_enabled: bool = True
    default_symbol_intensity: str = "active"  # passive, active, intensive
    symbol_rotation_enabled: bool = True
    intensive_monitoring_limit: int = 2

    # Per-symbol resource allocation
    api_budget_per_symbol: int = 80  # Default API calls per hour per symbol
    max_concurrent_symbols: int = 5  # Resource limit for concurrent processing


@dataclass
class SystemStatus:
    """Current system status"""
    state: SystemState = SystemState.INITIALIZING
    trading_mode: TradingMode = TradingMode.PAPER_TRADING
    start_time: datetime = field(default_factory=datetime.now)

    # Process status
    total_processes: int = 0
    healthy_processes: int = 0
    failed_processes: int = 0

    # Trading metrics
    trades_today: int = 0
    total_pnl: float = 0.0
    active_positions: int = 0

    # Account data (from AccountManager)
    portfolio_value: float = 0.0
    cash_available: float = 0.0
    buying_power: float = 0.0
    day_pnl: float = 0.0
    day_pnl_pct: float = 0.0
    account_healthy: bool = True
    account_health_message: str = "Unknown"

    # System metrics
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    api_calls_remaining: int = 0

    # Last activity
    last_decision_time: Optional[datetime] = None
    last_trade_time: Optional[datetime] = None
    last_health_check: Optional[datetime] = None
    last_account_sync: Optional[datetime] = None

    # Symbol coordination metrics
    symbol_coordination_enabled: bool = False
    total_symbols_managed: int = 0
    intensive_symbols_count: int = 0
    active_symbols_count: int = 0
    passive_symbols_count: int = 0
    symbols_with_errors: int = 0
    last_symbol_coordination_update: Optional[datetime] = None
