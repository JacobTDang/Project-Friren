#!/usr/bin/env python3
"""
orchestrator.py

Main Trading System Orchestrator - Central Nervous System

The Master Orchestrator coordinates all 5 processes and manages the complete
trading workflow from market data through decision making to trade execution.

Architecture:
- Coordinates 5 core processes via ProcessManager
- Integrates with DecisionEngine for trade decisions
- Manages execution pipeline via ExecutionOrchestrator
- Handles system-wide state management and monitoring
- Optimized for AWS EC2 t3.micro (2 cores, 5 processes max)

5 Core Processes:
1. Market Decision Engine (main process)
2. Position Health Monitor
3. Strategy Analyzer
4. Sentiment Analyzer (FinBERT)
5. Enhanced News Pipeline (News + FinBERT + XGBoost recommendations)

Key Features:
- Process lifecycle management
- Real-time decision coordination
- Trade execution orchestration
- System health monitoring
- Emergency shutdown capabilities
- Resource optimization for t3.micro
"""

import sys
import os
import time
import signal
import threading
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
from pathlib import Path

# Import path resolution
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Core infrastructure imports
try:
    from Friren_V1.multiprocess_infrastructure.process_manager import ProcessManager, ProcessConfig, RestartPolicy  # type: ignore
    from Friren_V1.multiprocess_infrastructure.base_process import BaseProcess, ProcessState  # type: ignore
    from Friren_V1.multiprocess_infrastructure.queue_manager import QueueManager, QueueMessage, MessageType, MessagePriority  # type: ignore
    from Friren_V1.multiprocess_infrastructure.shared_state_manager import SharedStateManager  # type: ignore
    INFRASTRUCTURE_AVAILABLE = True
except ImportError:
    INFRASTRUCTURE_AVAILABLE = False
    # Fallback stubs
    class ProcessManager:
        def __init__(self, max_processes=5): pass
        def register_process(self, config): pass
        def start_all_processes(self, dependency_order=None): pass
        def stop_all_processes(self): pass
        def get_process_status(self): return {}

    class ProcessConfig:
        def __init__(self, **kwargs): pass

    class RestartPolicy:
        ON_FAILURE = "on_failure"
        ALWAYS = "always"

    class SharedStateManager:
        def __init__(self): pass
        def get(self, key): return None
        def set(self, key, value): pass
        def cleanup(self): pass

# Trading engine imports
try:
    from .decision_engine.decision_engine import MarketDecisionEngine  # type: ignore
    from .decision_engine.execution_orchestrator import ExecutionOrchestrator, create_execution_orchestrator  # type: ignore
    from .processes.position_health_monitor import PositionHealthMonitor  # type: ignore
    from .processes.strategy_analyzer_process import StrategyAnalyzerProcess  # type: ignore
    from .processes.finbert_sentiment_process import FinBERTSentimentProcess  # type: ignore
    from .processes.news_collector_procses import EnhancedNewsCollectorProcess  # type: ignore
    from .processes.enhanced_news_pipeline_process import EnhancedNewsPipelineProcess, PipelineConfig, get_default_pipeline_config  # type: ignore

    # Symbol coordination imports
    from .symbol_coordination.symbol_config import SymbolMonitoringConfig, MonitoringIntensity  # type: ignore
    from .symbol_coordination.symbol_coordinator import SymbolCoordinator  # type: ignore
    from .symbol_coordination.resource_manager import ResourceManager, ResourceQuota  # type: ignore
    from .symbol_coordination.message_router import MessageRouter, RoutingStrategy  # type: ignore
    SYMBOL_COORDINATION_AVAILABLE = True
    TRADING_COMPONENTS_AVAILABLE = True
except ImportError:
    TRADING_COMPONENTS_AVAILABLE = False
    SYMBOL_COORDINATION_AVAILABLE = False
    # Fallback stubs
    class MarketDecisionEngine:
        def __init__(self, process_id="decision_engine"):
            self.process_id = process_id

    class PositionHealthMonitor:
        def __init__(self, process_id="health_monitor"):
            self.process_id = process_id

    class StrategyAnalyzerProcess:
        def __init__(self, process_id="strategy_analyzer"):
            self.process_id = process_id

    class FinBERTSentimentProcess:
        def __init__(self, process_id="sentiment_analyzer"):
            self.process_id = process_id

    class EnhancedNewsCollectorProcess:
        def __init__(self, process_id="news_collector"):
            self.process_id = process_id

    class EnhancedNewsPipelineProcess:
        def __init__(self, process_id="enhanced_news_pipeline"):
            self.process_id = process_id

    def get_default_pipeline_config():
        return type('MockConfig', (), {})()

    PipelineConfig = type('MockPipelineConfig', (), {})

    class ExecutionOrchestrator:
        def __init__(self): pass
        def execute_approved_decision(self, *args, **kwargs): return None

    def create_execution_orchestrator(*args, **kwargs):
        return ExecutionOrchestrator()

    # Symbol coordination fallback stubs
    class SymbolCoordinator:
        def __init__(self, *args, **kwargs):
            self.symbol_states = {}
        def add_symbol(self, symbol: str, config=None): return True
        def remove_symbol(self, symbol: str): return True
        def get_coordination_status(self): return {}
        def can_make_api_call(self, symbol: str): return True
        def record_api_call(self, symbol: str): pass

    class SymbolMonitoringConfig:
        def __init__(self, *args, **kwargs): pass

    class MonitoringIntensity:
        PASSIVE = "passive"
        ACTIVE = "active"
        INTENSIVE = "intensive"

    class ResourceManager:
        def __init__(self, *args, **kwargs): pass
        def start_monitoring(self): pass
        def stop_monitoring(self): pass
        def allocate_resources(self, symbol: str, config): return True
        def can_make_api_call(self, symbol: str): return True
        def record_api_call(self, symbol: str): pass
        def get_system_resource_status(self): return {}

    class ResourceQuota:
        def __init__(self, *args, **kwargs): pass

    class MessageRouter:
        def __init__(self, *args, **kwargs): pass
        def start(self): pass
        def stop(self): pass
        def register_symbol(self, symbol: str, config): pass
        def route_message(self, symbol: str, message): return True
        def get_routing_status(self): return {}

    class RoutingStrategy:
        HYBRID = "hybrid"


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
    health_check_interval: int = 30
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


class MainOrchestrator:
    """
    Main Trading System Orchestrator

    The central nervous system that coordinates all trading system components.
    Manages the complete lifecycle from startup to shutdown, coordinates
    the 5 core processes, and handles trade execution workflow.
    """

    def __init__(self, config: Optional[SystemConfig] = None):
        """Initialize the main orchestrator"""
        self.config = config or SystemConfig()
        self.status = SystemStatus()

        # Core components
        self.process_manager: Optional[ProcessManager] = None
        self.execution_orchestrator: Optional[ExecutionOrchestrator] = None
        self.shared_state: Optional[SharedStateManager] = None
        self.queue_manager: Optional[QueueManager] = None

        # Symbol coordination
        self.symbol_coordinator: Optional[SymbolCoordinator] = None

        # Message Routing and Resource Management (Phase 2 Enhancement)
        self.resource_manager: Optional[ResourceManager] = None
        self.message_router: Optional[MessageRouter] = None

        # Control and monitoring
        self._shutdown_event = threading.Event()
        self._main_thread: Optional[threading.Thread] = None
        self._monitoring_thread: Optional[threading.Thread] = None
        self._decision_thread: Optional[threading.Thread] = None

        # Emergency controls
        self._emergency_stop = False
        self._manual_override = False

        # Setup logging
        self.logger = self._setup_logging()

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        self.logger.info("MainOrchestrator initialized")
        self.logger.info(f"Trading mode: {self.config.trading_mode.value}")
        self.logger.info(f"Symbols: {self.config.symbols}")

    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging"""
        logger = logging.getLogger("main_orchestrator")
        logger.setLevel(logging.INFO)

        # Create logs directory if not exists
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

        # File handler
        file_handler = logging.FileHandler(logs_dir / "main_orchestrator.log")
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        return logger

    def initialize_system(self):
        """Initialize all system components"""
        self.logger.info("Initializing trading system...")
        self.status.state = SystemState.INITIALIZING

        try:
            # Initialize shared resources
            self._initialize_shared_resources()

            # Initialize account management
            self._initialize_account_manager()

            # Initialize process manager and register processes
            self._initialize_process_manager()

            # Initialize execution orchestrator
            self._initialize_execution_orchestrator()

            # Initialize symbol coordination
            self._initialize_symbol_coordination()

            # Validate system readiness
            self._validate_system_readiness()

            self.logger.info("System initialization complete")

        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            self.status.state = SystemState.ERROR
            raise

    def start_system(self):
        """Start the complete trading system"""
        self.logger.info("Starting trading system...")
        self.status.state = SystemState.STARTING

        try:
            # Start all processes
            self._start_processes()

            # Start main orchestration loop
            self._start_main_loop()

            # Start monitoring
            self._start_monitoring()

            # Start decision engine coordination
            self._start_decision_coordination()

            self.status.state = SystemState.RUNNING
            self.logger.info("Trading system fully operational")

        except Exception as e:
            self.logger.error(f"Failed to start trading system: {e}")
            self.status.state = SystemState.ERROR
            self.stop_system()
            raise

    def stop_system(self, emergency: bool = False):
        """Stop the trading system gracefully or emergency"""
        if emergency:
            self.logger.warning("EMERGENCY SHUTDOWN INITIATED")
            self.status.state = SystemState.EMERGENCY_STOP
            self._emergency_stop = True
        else:
            self.logger.info("Graceful shutdown initiated")
            self.status.state = SystemState.STOPPING

        try:
            # Signal shutdown
            self._shutdown_event.set()

            # Stop all threads
            self._stop_threads()

            # Stop symbol coordination components (Phase 2)
            if self.message_router:
                self.message_router.stop()
            if self.resource_manager:
                self.resource_manager.stop_monitoring()

            # Stop all processes
            if self.process_manager:
                self.process_manager.stop_all_processes()

            # Final cleanup
            self._cleanup_resources()

            # Only set to STOPPED if not an emergency shutdown
            if not emergency:
                self.status.state = SystemState.STOPPED
                self.logger.info("System shutdown complete")
            else:
                self.logger.warning("Emergency shutdown complete - system in emergency state")

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        # Update process status
        if self.process_manager:
            process_status = self.process_manager.get_process_status()
            self.status.total_processes = len(process_status)
            self.status.healthy_processes = sum(
                1 for p in process_status.values()
                if p.get('is_healthy', False)
            )
            self.status.failed_processes = self.status.total_processes - self.status.healthy_processes

        # Update system metrics
        self._update_system_metrics()

        return {
            'system_state': self.status.state.value,
            'trading_mode': self.status.trading_mode.value,
            'uptime_seconds': (datetime.now() - self.status.start_time).total_seconds(),
            'processes': {
                'total': self.status.total_processes,
                'healthy': self.status.healthy_processes,
                'failed': self.status.failed_processes
            },
            'trading': {
                'trades_today': self.status.trades_today,
                'total_pnl': self.status.total_pnl,
                'active_positions': self.status.active_positions
            },
            'account': {
                'portfolio_value': self.status.portfolio_value,
                'cash_available': self.status.cash_available,
                'buying_power': self.status.buying_power,
                'day_pnl': self.status.day_pnl,
                'day_pnl_pct': self.status.day_pnl_pct,
                'account_healthy': self.status.account_healthy,
                'account_health_message': self.status.account_health_message,
                'last_sync': self.status.last_account_sync.isoformat() if self.status.last_account_sync else None
            },
            'resources': {
                'memory_usage_mb': self.status.memory_usage_mb,
                'cpu_usage_percent': self.status.cpu_usage_percent,
                'api_calls_remaining': self.status.api_calls_remaining
            },
            'last_activity': {
                'decision': self.status.last_decision_time.isoformat() if self.status.last_decision_time else None,
                'trade': self.status.last_trade_time.isoformat() if self.status.last_trade_time else None,
                'health_check': self.status.last_health_check.isoformat() if self.status.last_health_check else None
            },
            'symbol_coordination': {
                'enabled': self.status.symbol_coordination_enabled,
                'total_symbols': self.status.total_symbols_managed,
                'intensive_symbols': self.status.intensive_symbols_count,
                'active_symbols': self.status.active_symbols_count,
                'passive_symbols': self.status.passive_symbols_count,
                'symbols_with_errors': self.status.symbols_with_errors,
                'last_update': self.status.last_symbol_coordination_update.isoformat() if self.status.last_symbol_coordination_update else None,
                'resource_status': self.resource_manager.get_system_resource_status() if self.resource_manager else {},
                'routing_status': self.message_router.get_routing_status() if self.message_router else {}
            }
        }

    def execute_trade_decision(self, symbol: str, decision_data: Dict[str, Any]) -> bool:
        """Execute a trade decision through the execution orchestrator"""
        try:
            self.logger.info(f"Executing trade decision for {symbol}")

            if not self.execution_orchestrator:
                self.logger.error("Execution orchestrator not available")
                return False

            # Check system state
            if self.status.state != SystemState.RUNNING:
                self.logger.warning(f"System not in running state: {self.status.state}")
                return False

            # Check emergency conditions
            if self._emergency_stop or self._check_emergency_conditions():
                self.logger.warning("Emergency conditions detected, blocking trade")
                return False

            # Execute through orchestrator
            execution_result = self.execution_orchestrator.execute_approved_decision(
                decision_data.get('risk_validation'),
                decision_data.get('strategy_name', 'unknown'),
                decision_data.get('confidence', 0.5)
            )

            if execution_result and execution_result.was_successful:
                self.status.trades_today += 1
                self.status.last_trade_time = datetime.now()
                self.logger.info(f"Trade executed successfully: {execution_result.execution_summary}")
                return True
            else:
                error_msg = execution_result.error_message if execution_result else "Unknown error"
                self.logger.error(f"Trade execution failed: {error_msg}")
                return False

        except Exception as e:
            self.logger.error(f"Error executing trade decision: {e}")
            return False

    def pause_trading(self):
        """Pause trading operations"""
        self.logger.info("Pausing trading operations")
        self.status.state = SystemState.PAUSING
        # Implementation: Signal processes to pause trading
        self.status.state = SystemState.PAUSED

    def resume_trading(self):
        """Resume trading operations"""
        self.logger.info("Resuming trading operations")
        self.status.state = SystemState.RUNNING

    def emergency_stop_all_trades(self):
        """Emergency stop all trading activity"""
        self.logger.warning("EMERGENCY STOP - All trading halted")
        self._emergency_stop = True
        self.status.state = SystemState.EMERGENCY_STOP

        # TODO: Cancel all pending orders
        # TODO: Close all positions if configured
        # TODO: Alert administrators

    # Private implementation methods

    def _initialize_shared_resources(self):
        """Initialize shared state and queue managers"""
        if INFRASTRUCTURE_AVAILABLE:
            self.shared_state = SharedStateManager()
            self.queue_manager = QueueManager()
            self.logger.info("Shared resources initialized")
        else:
            self.logger.warning("Infrastructure not available, using fallbacks")

    def _initialize_account_manager(self):
        """Initialize account management with hybrid storage strategy"""
        try:
            # Try to import AccountManager
            try:
                from .tools.account_manager import AccountManager
                from .tools.db_manager import TradingDBManager
                from .tools.alpaca_interface import SimpleAlpacaInterface

                # Initialize with real components
                db_manager = TradingDBManager("main_orchestrator")
                alpaca_interface = SimpleAlpacaInterface()
                self.account_manager = AccountManager(
                    db_manager=db_manager,
                    alpaca_interface=alpaca_interface,
                    cache_duration=30  # 30-second cache
                )

                self.logger.info("AccountManager initialized with hybrid storage strategy")

            except ImportError as e:
                self.logger.warning(f"Could not import AccountManager: {e}")
                # Use fallback mock for testing
                self.account_manager = type('MockAccountManager', (), {
                    'get_account_snapshot': lambda: type('Snapshot', (), {
                        'portfolio_value': 50000.0,
                        'cash': 25000.0,
                        'buying_power': 50000.0,
                        'total_pnl': 0.0,
                        'day_pnl': 0.0,
                        'day_pnl_pct': 0.0,
                        'timestamp': datetime.now()
                    })(),
                    'is_account_healthy': lambda: (True, "Mock account healthy"),
                    'get_cash_available': lambda: 25000.0,
                    'get_buying_power': lambda: 50000.0,
                    'get_portfolio_value': lambda: 50000.0,
                    'get_total_pnl': lambda: 0.0,
                    'get_day_pnl': lambda: (0.0, 0.0)
                })()
                self.logger.info("Using mock AccountManager for testing")

        except Exception as e:
            self.logger.error(f"Failed to initialize AccountManager: {e}")
            raise

    def _initialize_process_manager(self):
        """Initialize and register all processes"""
        if not INFRASTRUCTURE_AVAILABLE:
            self.logger.warning("Process manager not available")
            return

        self.process_manager = ProcessManager(max_processes=5)

        # Register the 5 core processes
        # Note: Enhanced news pipeline replaces the basic news collector
        processes = [
            ('decision_engine', MarketDecisionEngine),
            ('position_health_monitor', PositionHealthMonitor),
            ('strategy_analyzer', StrategyAnalyzerProcess),
            ('sentiment_analyzer', FinBERTSentimentProcess),
            ('enhanced_news_pipeline', EnhancedNewsPipelineProcess)  # Replaces news_collector
        ]

        for process_id, process_class in processes:
            # Special configuration for enhanced news pipeline
            if process_id == 'enhanced_news_pipeline' and TRADING_COMPONENTS_AVAILABLE:
                # Create enhanced pipeline with optimized configuration
                pipeline_config = get_default_pipeline_config()
                # Configure for the system's symbols
                enhanced_process = process_class(
                    process_id=process_id,
                    watchlist_symbols=self.config.symbols,
                    config=pipeline_config
                )
                config = ProcessConfig(
                    process_class=lambda: enhanced_process,  # Use pre-configured instance
                    process_id=process_id,
                    restart_policy=RestartPolicy.ON_FAILURE,
                    max_restarts=3,
                    health_check_interval=self.config.health_check_interval
                )
            else:
                config = ProcessConfig(
                    process_class=process_class,
                    process_id=process_id,
                    restart_policy=RestartPolicy.ON_FAILURE,
                    max_restarts=3,
                    health_check_interval=self.config.health_check_interval
                )

            self.process_manager.register_process(config)
            self.logger.info(f"Registered process: {process_id}")

    def _initialize_execution_orchestrator(self):
        """Initialize the execution orchestrator"""
        try:
            # Use the imported create_execution_orchestrator if available
            if TRADING_COMPONENTS_AVAILABLE:
                self.execution_orchestrator = create_execution_orchestrator()
            else:
                self.execution_orchestrator = ExecutionOrchestrator()
            self.logger.info("Execution orchestrator initialized")
        except Exception as e:
            self.execution_orchestrator = ExecutionOrchestrator()
            self.logger.warning(f"Using fallback execution orchestrator: {e}")

    def _initialize_symbol_coordination(self):
        """Initialize the symbol coordination system"""
        try:
            if self.config.symbol_coordination_enabled and SYMBOL_COORDINATION_AVAILABLE:
                # Initialize resource manager first
                self._initialize_resource_manager()

                # Initialize message router
                self._initialize_message_router()

                # Create symbol coordinator with configuration
                self.symbol_coordinator = SymbolCoordinator(
                    total_api_budget=self.config.total_api_budget,
                    max_intensive_symbols=self.config.max_intensive_symbols
                )

                # Add configured symbols to coordinator
                for symbol in self.config.symbols:
                    intensity = getattr(MonitoringIntensity, self.config.default_symbol_intensity.upper(), MonitoringIntensity.ACTIVE)
                    config = SymbolMonitoringConfig(
                        symbol=symbol,
                        monitoring_intensity=intensity,
                        api_call_budget=self.config.api_budget_per_symbol
                    )
                    success = self.symbol_coordinator.add_symbol(symbol, config)
                    if success:
                        self.logger.info(f"Added symbol {symbol} to coordination with intensity {intensity}")

                        # Register symbol with resource manager and message router
                        if self.resource_manager:
                            self.resource_manager.allocate_resources(symbol, config)
                        if self.message_router:
                            self.message_router.register_symbol(symbol, config)
                    else:
                        self.logger.warning(f"Failed to add symbol {symbol} to coordination")

                # Start resource monitoring and message routing
                if self.resource_manager:
                    self.resource_manager.start_monitoring()
                if self.message_router:
                    self.message_router.start()

                # Update status
                self.status.symbol_coordination_enabled = True
                self.status.last_symbol_coordination_update = datetime.now()

                self.logger.info(f"Symbol coordination initialized with {len(self.config.symbols)} symbols")
            else:
                # Use fallback coordinator
                self.symbol_coordinator = SymbolCoordinator()
                self.resource_manager = ResourceManager()
                self.message_router = MessageRouter()
                self.status.symbol_coordination_enabled = False
                if not self.config.symbol_coordination_enabled:
                    self.logger.info("Symbol coordination disabled by configuration")
                else:
                    self.logger.warning("Symbol coordination not available, using fallback")

        except Exception as e:
            # Fallback to stub coordinator
            self.symbol_coordinator = SymbolCoordinator()
            self.resource_manager = ResourceManager()
            self.message_router = MessageRouter()
            self.status.symbol_coordination_enabled = False
            self.logger.error(f"Failed to initialize symbol coordination: {e}")
            self.logger.info("Using fallback symbol coordinator")

    def _initialize_resource_manager(self):
        """Initialize the resource manager"""
        try:
            # Create resource quota based on system configuration
            quota = ResourceQuota(
                api_calls_per_hour=self.config.total_api_budget,
                memory_limit_mb=self.config.max_memory_mb,
                cpu_limit_percent=self.config.max_cpu_percent,
                api_buffer=self.config.api_rate_limit_buffer
            )

            self.resource_manager = ResourceManager(quota)
            self.logger.info("Resource manager initialized")

        except Exception as e:
            self.resource_manager = ResourceManager()
            self.logger.error(f"Failed to initialize resource manager: {e}")

    def _initialize_message_router(self):
        """Initialize the message router"""
        try:
            self.message_router = MessageRouter(self.resource_manager)
            self.logger.info("Message router initialized")

        except Exception as e:
            self.message_router = MessageRouter()
            self.logger.error(f"Failed to initialize message router: {e}")

    def _validate_system_readiness(self):
        """Validate that all required components are ready"""
        issues = []

        # Only validate execution orchestrator for now, process manager is optional for testing
        if not self.execution_orchestrator:
            issues.append("Execution orchestrator not initialized")

        if not self.config.symbols:
            issues.append("No symbols configured for trading")

        if issues:
            raise RuntimeError(f"System readiness validation failed: {issues}")

    def _start_processes(self):
        """Start all registered processes"""
        if self.process_manager:
            # Define dependency order - decision engine should start first
            dependency_order = [
                'decision_engine',
                'position_health_monitor',
                'strategy_analyzer',
                'sentiment_analyzer',
                'enhanced_news_pipeline'  # Replaces news_collector
            ]

            self.process_manager.start_all_processes(dependency_order)
            self.logger.info("All processes started")
        else:
            self.logger.warning("Process manager not available")

    def _start_main_loop(self):
        """Start the main orchestration loop"""
        self._main_thread = threading.Thread(target=self._main_orchestration_loop, daemon=True)
        self._main_thread.start()
        self.logger.info("Main orchestration loop started")

    def _start_monitoring(self):
        """Start system monitoring"""
        self._monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self._monitoring_thread.start()
        self.logger.info("System monitoring started")

    def _start_decision_coordination(self):
        """Start decision engine coordination with symbol awareness"""
        self._decision_thread = threading.Thread(target=self._decision_coordination_loop, daemon=True)
        self._decision_thread.start()

        # Start symbol coordination if enabled
        if self.config.symbol_coordination_enabled and self.symbol_coordinator:
            self._start_symbol_coordination_integration()

        self.logger.info("Decision coordination started")

    def _start_symbol_coordination_integration(self):
        """Start symbol coordination integration with existing processes"""
        try:
            if not self.symbol_coordinator:
                return

            # Register symbols from config with appropriate monitoring intensity
            for symbol in self.config.symbols:
                monitoring_config = SymbolMonitoringConfig(
                    symbol=symbol,
                    monitoring_intensity=MonitoringIntensity.ACTIVE,
                    api_budget_per_hour=self.config.api_budget_per_symbol,
                    max_news_articles=10,
                    sentiment_threshold=0.7,
                    risk_threshold=0.8
                )
                self.symbol_coordinator.add_symbol(symbol, monitoring_config)

            self.logger.info(f"Symbol coordination integrated with {len(self.config.symbols)} symbols")

        except Exception as e:
            self.logger.error(f"Error starting symbol coordination integration: {e}")

    def _coordinate_symbol_processing(self):
        """Coordinate symbol processing across all processes"""
        try:
            if not self.symbol_coordinator:
                return

            # Get next symbol for processing based on priority
            symbol_to_process = self.symbol_coordinator.get_next_symbol_to_process()

            if symbol_to_process:
                # Send symbol processing message to relevant processes
                self._send_symbol_processing_message(symbol_to_process)

                # Update symbol coordination metrics
                self._update_symbol_coordination_status()

                self.logger.debug(f"Coordinated processing for symbol {symbol_to_process}")

        except Exception as e:
            self.logger.error(f"Error in symbol processing coordination: {e}")

    def _send_symbol_processing_message(self, symbol: str):
        """Send symbol processing message to relevant processes"""
        try:
            if not self.message_router:
                return

            # Create symbol processing message
            message_data = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'source': 'orchestrator',
                'action': 'process_symbol'
            }

            # Create message object
            from Friren_V1.multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority

            message = QueueMessage(
                message_type=MessageType.STRATEGY_SIGNAL,
                priority=MessagePriority.HIGH,
                sender_id="main_orchestrator",
                recipient_id="symbol_coordination",
                payload=message_data
            )

            # Route message through symbol coordination
            self.message_router.route_message(symbol, message)

        except Exception as e:
            self.logger.error(f"Error sending symbol processing message for {symbol}: {e}")

    def _update_symbol_coordination_status(self):
        """Update symbol coordination status metrics"""
        try:
            if not self.symbol_coordinator:
                return

            # Get coordination status
            coordination_status = self.symbol_coordinator.get_system_status()

            # Update system status
            self.status.symbol_coordination_enabled = True
            self.status.total_symbols_managed = len(self.config.symbols)
            self.status.last_symbol_coordination_update = datetime.now()

            # Update intensity counts from coordination status
            if 'symbol_states' in coordination_status:
                intensity_counts = {'intensive': 0, 'active': 0, 'passive': 0}
                for symbol_state in coordination_status['symbol_states'].values():
                    intensity = symbol_state.get('monitoring_intensity', 'active')
                    if intensity in intensity_counts:
                        intensity_counts[intensity] += 1

                self.status.intensive_symbols_count = intensity_counts['intensive']
                self.status.active_symbols_count = intensity_counts['active']
                self.status.passive_symbols_count = intensity_counts['passive']

        except Exception as e:
            self.logger.error(f"Error updating symbol coordination status: {e}")

    def _main_orchestration_loop(self):
        """Main orchestration loop"""
        while not self._shutdown_event.is_set():
            try:
                # Check system health
                self._check_system_health()

                # Update shared state
                self._update_shared_state()

                # Check for emergency conditions
                if self._check_emergency_conditions():
                    self.emergency_stop_all_trades()
                    break

                # Brief sleep
                time.sleep(10)

            except Exception as e:
                self.logger.error(f"Error in main orchestration loop: {e}")
                time.sleep(30)

    def _monitoring_loop(self):
        """System monitoring loop"""
        while not self._shutdown_event.is_set():
            try:
                # Update system metrics
                self._update_system_metrics()

                # Log system status periodically
                self._log_system_status()

                self.status.last_health_check = datetime.now()
                time.sleep(self.config.health_check_interval)

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(60)

    def _decision_coordination_loop(self):
        """Decision engine coordination loop with symbol awareness"""
        while not self._shutdown_event.is_set():
            try:
                # Symbol-aware coordination if enabled
                if self.config.symbol_coordination_enabled and self.symbol_coordinator:
                    self._coordinate_symbol_processing()

                # Check for pending decisions from decision engine
                self._check_pending_decisions()

                # Coordinate strategy analysis
                self._coordinate_strategy_analysis()

                time.sleep(self.config.decision_cycle_interval)

            except Exception as e:
                self.logger.error(f"Error in decision coordination loop: {e}")
                time.sleep(60)

    def _check_system_health(self):
        """Check overall system health"""
        if self.process_manager:
            status = self.process_manager.get_process_status()
            failed_processes = [
                pid for pid, pstatus in status.items()
                if not pstatus.get('is_healthy', False)
            ]

            if failed_processes:
                self.logger.warning(f"Unhealthy processes: {failed_processes}")

    def _update_shared_state(self):
        """Update shared state with system information"""
        if self.shared_state:
            self.shared_state.set('system_status', self.get_system_status())
            self.shared_state.set('last_update', datetime.now().isoformat())

    def _check_emergency_conditions(self) -> bool:
        """Check for emergency conditions that require immediate stop"""
        # Check daily loss limit
        if abs(self.status.total_pnl) > self.config.max_daily_loss_pct:
            self.logger.warning(f"Daily loss limit exceeded: {self.status.total_pnl}%")
            return True

        # Check memory usage
        if self.status.memory_usage_mb > self.config.max_memory_mb:
            self.logger.warning(f"Memory usage critical: {self.status.memory_usage_mb}MB")
            return True

        return False

    def _check_pending_decisions(self):
        """Check for pending decisions from decision engine"""
        # This would integrate with the decision engine to check for
        # completed analysis that needs execution
        pass

    def _coordinate_strategy_analysis(self):
        """Coordinate strategy analysis across processes"""
        # Enhanced coordination with symbol-specific message routing
        if self.symbol_coordinator and self.message_router:
            try:
                # Get next symbol for processing based on priority
                symbol_to_process = self.symbol_coordinator.get_next_symbol_to_process()

                if symbol_to_process:
                    # Create strategy analysis message
                    if QUEUE_INFRASTRUCTURE_AVAILABLE:
                        from Friren_V1.multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority

                        message = QueueMessage(
                            message_type=MessageType.STRATEGY_SIGNAL,
                            priority=MessagePriority.HIGH,
                            data={
                                'symbol': symbol_to_process,
                                'timestamp': datetime.now().isoformat(),
                                'source': 'orchestrator'
                            }
                        )

                        # Route message through symbol coordination
                        self.message_router.route_message(symbol_to_process, message)

                        self.logger.debug(f"Coordinated strategy analysis for {symbol_to_process}")

            except Exception as e:
                self.logger.error(f"Error in strategy analysis coordination: {e}")
        else:
            # Fallback to basic coordination
            pass

    def _update_system_metrics(self):
        """Update system resource metrics"""
        try:
            import psutil
            process = psutil.Process()
            self.status.memory_usage_mb = process.memory_info().rss / 1024 / 1024
            self.status.cpu_usage_percent = process.cpu_percent()
        except ImportError:
            # Fallback if psutil not available
            pass

        # Update account data using AccountManager
        self._update_account_data()

    def _update_account_data(self):
        """Update account data from AccountManager (hybrid storage strategy)"""
        try:
            if self.account_manager:
                # Get account snapshot (will use cache if fresh)
                snapshot = self.account_manager.get_account_snapshot()

                # Update system status with account data
                self.status.portfolio_value = snapshot.portfolio_value
                self.status.cash_available = snapshot.cash
                self.status.buying_power = snapshot.buying_power
                self.status.day_pnl = snapshot.day_pnl
                self.status.day_pnl_pct = snapshot.day_pnl_pct
                self.status.total_pnl = snapshot.total_pnl
                self.status.last_account_sync = snapshot.timestamp

                # Check account health
                is_healthy, message = self.account_manager.is_account_healthy()
                self.status.account_healthy = is_healthy
                self.status.account_health_message = message

        except Exception as e:
            self.logger.error(f"Error updating account data: {e}")
            self.status.account_healthy = False
            self.status.account_health_message = f"Update failed: {e}"

    def _log_system_status(self):
        """Log periodic system status"""
        status = self.get_system_status()
        self.logger.info(
            f"System Status: {status['system_state']} | "
            f"Processes: {status['processes']['healthy']}/{status['processes']['total']} | "
            f"Trades: {status['trading']['trades_today']} | "
            f"PnL: {status['trading']['total_pnl']:.2f}% | "
            f"Memory: {status['resources']['memory_usage_mb']:.1f}MB"
        )

    def _stop_threads(self):
        """Stop all monitoring threads"""
        threads = [
            ('main', self._main_thread),
            ('monitoring', self._monitoring_thread),
            ('decision', self._decision_thread)
        ]

        for name, thread in threads:
            if thread and thread.is_alive():
                thread.join(timeout=10)
                if thread.is_alive():
                    self.logger.warning(f"{name} thread did not stop gracefully")

    def _cleanup_resources(self):
        """Final resource cleanup"""
        try:
            if self.shared_state:
                self.shared_state.cleanup()
        except Exception as e:
            self.logger.error(f"Error during resource cleanup: {e}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self.stop_system()


def create_main_orchestrator(config: Optional[SystemConfig] = None) -> MainOrchestrator:
    """Factory function to create a main orchestrator"""
    return MainOrchestrator(config)


def main():
    """Main entry point for running the trading system"""
    print("Starting Friren Trading System...")

    # Create configuration
    config = SystemConfig(
        trading_mode=TradingMode.PAPER_TRADING,  # Start in paper trading
        symbols=['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA'],
        max_positions=5,
        portfolio_value=100000.0
    )

    # Create and initialize orchestrator
    orchestrator = create_main_orchestrator(config)

    try:
        # Initialize and start system
        orchestrator.initialize_system()
        orchestrator.start_system()

        # Keep running until interrupted
        while orchestrator.status.state == SystemState.RUNNING:
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"System error: {e}")
    finally:
        orchestrator.stop_system()
        print("System shutdown complete")


if __name__ == "__main__":
    main()
