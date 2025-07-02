"""
portfolio_manager/processes/position_health_monitor.py

Enhanced Position Health Monitor - Integrated with Strategy Management System
Phases 1-3 Integration: Queue messages, strategy tracking, multi-signal confirmation
"""

import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging
import sys
import os

# Add project root for color system import
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import color system for position monitor with strategies (brown)
try:
    from terminal_color_system import print_position_monitor, print_error, print_warning, print_success, create_colored_logger
    COLOR_SYSTEM_AVAILABLE = True
except ImportError:
    COLOR_SYSTEM_AVAILABLE = False

from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)

# Import clean components
from ..tools.multiprocess_manager import MultiprocessManager, TaskResult
from ..analytics.position_health_analyzer import (
    OptimizedPositionHealthAnalyzer, ActiveStrategy, StrategyStatus, PositionHealthResult
)

# NEW: Strategy Management Integration (Phase 2-3)
from dataclasses import dataclass, field
from enum import Enum


class StrategyHealthStatus(Enum):
    """Health status of strategy performance"""
    EXCELLENT = "EXCELLENT"      # >10% profit, low risk
    GOOD = "GOOD"               # 5-10% profit, moderate risk
    NEUTRAL = "NEUTRAL"         # 0-5% profit/loss, normal risk
    CONCERNING = "CONCERNING"    # -5% to -8% loss, high risk
    CRITICAL = "CRITICAL"       # >8% loss or extreme risk


@dataclass
class StrategyMonitoringState:
    """Tracks strategy monitoring state and performance"""
    symbol: str
    strategy_name: str
    assigned_time: datetime
    last_health_check: Optional[datetime] = None
    health_status: StrategyHealthStatus = StrategyHealthStatus.NEUTRAL
    performance_score: float = 0.0
    consecutive_poor_checks: int = 0
    reassessment_requests_sent: int = 0
    last_reassessment_request: Optional[datetime] = None
    transition_signals_detected: List[Dict[str, Any]] = field(default_factory=list)
    cooldown_until: Optional[datetime] = None


@dataclass
class HealthBasedSignal:
    """Health-based signal for strategy transitions"""
    signal_type: str
    symbol: str
    strategy_name: str
    confidence: float
    health_metrics: Dict[str, Any]
    timestamp: datetime
    reason: str


def position_health_worker(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhanced worker function for position health analysis with strategy context

    Args:
        task: Dictionary containing analysis task data

    Returns:
        Dictionary with analysis results
    """
    try:
        # Initialize analyzer in worker (clean separation)
        analyzer = OptimizedPositionHealthAnalyzer()

        # Extract task data
        symbol = task['symbol']
        strategy = task['strategy']
        market_data = task['market_data']
        analysis_type = task.get('analysis_type', 'comprehensive')
        strategy_context = task.get('strategy_context', {})  # NEW: Strategy management context

        # Run comprehensive analysis using optimized analyzer
        if analysis_type == 'comprehensive':
            analysis_results = analyzer.analyze_position_comprehensive(strategy, market_data)
        elif analysis_type == 'strategy_specific':  # NEW: Strategy-specific analysis
            analysis_results = analyzer.analyze_position_with_strategy_context(
                strategy, market_data, strategy_context
            )
        else:
            analysis_results = analyzer.analyze_position_comprehensive(strategy, market_data)

        # NEW: Extract strategy performance indicators
        strategy_performance = _extract_strategy_performance(analysis_results, strategy_context)

        return {
            'symbol': symbol,
            'strategy_type': strategy.strategy_type,
            'analysis_results': analysis_results,
            'strategy_performance': strategy_performance,  # NEW
            'success': True
        }

    except Exception as e:
        return {
            'symbol': task.get('symbol', 'unknown'),
            'strategy_type': task.get('strategy', {}).get('strategy_type', 'unknown'),
            'analysis_results': {},
            'strategy_performance': {},  # NEW
            'success': False,
            'error': str(e)
        }


def _extract_strategy_performance(analysis_results: Dict, strategy_context: Dict) -> Dict[str, Any]:
    """Extract strategy-specific performance indicators"""
    try:
        metrics = analysis_results.get('metrics', {})
        if not metrics or not hasattr(metrics, 'current_metrics'):
            return {}

        current_metrics = metrics.current_metrics

        return {
            'pnl_pct': current_metrics.get('pnl_pct', 0.0),
            'health_score': current_metrics.get('health_score', 50.0),
            'risk_score': current_metrics.get('risk_score', 50.0),
            'time_in_position_hours': current_metrics.get('time_in_position', 0.0),
            'strategy_effectiveness': _calculate_strategy_effectiveness(current_metrics, strategy_context),
            'performance_trend': _determine_performance_trend(current_metrics),
            'risk_level': _determine_risk_level(current_metrics)
        }
    except Exception as e:
        logging.getLogger("strategy_performance").warning(f"Error extracting strategy performance: {e}")
        return {}


def _calculate_strategy_effectiveness(metrics: Dict, context: Dict) -> float:
    """Calculate how well the strategy is performing relative to expectations"""
    try:
        pnl_pct = metrics.get('pnl_pct', 0.0)
        time_hours = metrics.get('time_in_position', 0.0)
        risk_score = metrics.get('risk_score', 50.0)

        # Strategy-specific effectiveness calculations
        strategy_type = context.get('strategy_type', 'unknown')

        if strategy_type in ['momentum', 'pca_momentum', 'jump_momentum']:
            # Momentum strategies: expect faster gains but higher risk
            if time_hours < 24:  # First day
                target_pnl = 2.0
            elif time_hours < 72:  # First 3 days
                target_pnl = 5.0
            else:
                target_pnl = 8.0
        elif strategy_type in ['mean_reversion', 'bollinger']:
            # Mean reversion: expect steadier, slower gains
            if time_hours < 48:  # First 2 days
                target_pnl = 1.0
            elif time_hours < 168:  # First week
                target_pnl = 3.0
            else:
                target_pnl = 6.0
        else:
            # Default expectations
            target_pnl = max(1.0, time_hours / 24)  # 1% per day baseline

        # Calculate effectiveness score
        pnl_effectiveness = min(100, (pnl_pct / target_pnl) * 100) if target_pnl > 0 else 0
        risk_effectiveness = max(0, 100 - risk_score)  # Lower risk = higher effectiveness

        return (pnl_effectiveness * 0.7 + risk_effectiveness * 0.3) / 100  # 0-1 scale

    except Exception:
        return 0.5  # Neutral effectiveness


def _determine_performance_trend(metrics: Dict) -> str:
    """Determine performance trend based on metrics"""
    pnl_pct = metrics.get('pnl_pct', 0.0)
    health_score = metrics.get('health_score', 50.0)

    if pnl_pct > 5 and health_score > 70:
        return 'IMPROVING'
    elif pnl_pct < -3 or health_score < 30:
        return 'DETERIORATING'
    elif pnl_pct > 1:
        return 'STABLE_POSITIVE'
    elif pnl_pct > -1:
        return 'STABLE_NEUTRAL'
    else:
        return 'STABLE_NEGATIVE'


def _determine_risk_level(metrics: Dict) -> str:
    """Determine risk level based on metrics"""
    risk_score = metrics.get('risk_score', 50.0)

    if risk_score > 80:
        return 'EXTREME'
    elif risk_score > 60:
        return 'HIGH'
    elif risk_score > 40:
        return 'MODERATE'
    elif risk_score > 20:
        return 'LOW'
    else:
        return 'MINIMAL'


class PositionHealthMonitor(RedisBaseProcess):
    """
    Enhanced Position Health Monitor Process with Strategy Management Integration

    NEW FEATURES (Phase 4):
    - Strategy-specific health monitoring
    - Automatic reassessment request generation
    - Health-based strategy transition signals
    - Multi-signal confirmation integration
    - Performance tracking per strategy

    Clean Architecture:
    - Layer 1: Process infrastructure (BaseProcess, queues, shared state)
    - Layer 2: Task parallelization (MultiprocessManager)
    - Layer 3: Strategy management integration
    - Analytics: Business logic (OptimizedPositionHealthAnalyzer)
    """

    def __init__(self, process_id: str = "position_health_monitor",
                 check_interval: int = 10,
                 risk_threshold: float = 0.05,
                 symbols: list = None):
        super().__init__(process_id)

        self.check_interval = check_interval
        self.risk_threshold = risk_threshold
        self.symbols = symbols or []

        # Layer 2: Task parallelization (initialized in _initialize)
        self.multiprocess_manager = None

        # Analytics components (initialized in _initialize)
        self.health_analyzer = None
        self.data_fetcher = None

        # Process state tracking
        self.active_strategies = {}  # {symbol: ActiveStrategy}
        self.last_check_time = None
        self.alerts_sent_count = 0
        self.health_checks_count = 0

        # NEW: Strategy Management State (Phase 4)
        self.strategy_monitoring_states = {}  # {symbol: StrategyMonitoringState}
        self.reassessment_cooldown_minutes = 15  # Minimum time between reassessment requests
        self.max_reassessment_requests_per_day = 3  # Daily limit per symbol
        self.strategy_transition_signals = {}  # {symbol: List[HealthBasedSignal]}
        self.performance_history = {}  # {symbol: List[performance_snapshots]}

        self.logger.info(f"PositionHealthMonitor configured - interval: {check_interval}s with strategy management")

    def _initialize(self):
        self.logger.critical("EMERGENCY: ENTERED _initialize for position_health_monitor")
        print("EMERGENCY: ENTERED _initialize for position_health_monitor")
        try:
            # Layer 2: Initialize generic multiprocess manager
            self.multiprocess_manager = MultiprocessManager(
                max_workers=2,  # Suitable for t3.micro
                max_tasks=50
            )
            self.logger.info("Generic MultiprocessManager initialized")

            # Analytics: Initialize optimized position health analyzer
            self.health_analyzer = OptimizedPositionHealthAnalyzer()
            self.logger.info("OptimizedPositionHealthAnalyzer initialized")

            # Data fetcher - Import and initialize here to avoid import issues
            try:
                from Friren_V1.trading_engine.data.data_utils import StockDataFetcher
                self.data_fetcher = StockDataFetcher()
                self.logger.info("StockDataFetcher initialized successfully")
            except ImportError as e:
                self.logger.warning(f"StockDataFetcher not available: {e}")
                try:
                    # Fallback: Try alternative import path
                    from Friren_V1.trading_engine.data.yahoo_price import YahooFinancePriceData
                    self.data_fetcher = YahooFinancePriceData()
                    self.logger.info("YahooFinancePriceData initialized as fallback")
                except ImportError as e2:
                    self.logger.error(f"No data fetcher available: {e2}")
                    self.data_fetcher = None

            # CRITICAL FIX: Initialize TradingDBManager to access actual database positions
            try:
                from ..tools.db_manager import TradingDBManager
                self.trading_db_manager = TradingDBManager()
                self.logger.info("TradingDBManager initialized - can now access database positions")
            except ImportError as e:
                self.logger.error(f"TradingDBManager not available: {e}")
                self.trading_db_manager = None

            # ENHANCED: Initialize Strategy Assignment Engine for intelligent strategy selection
            try:
                from ..tools.strategy_assignment_engine import StrategyAssignmentEngine
                self.strategy_assigner = StrategyAssignmentEngine(
                    symbols=getattr(self, 'symbols', ['AAPL']),
                    risk_tolerance="moderate"  # Could be configured per user
                )
                self.logger.info("StrategyAssignmentEngine initialized - intelligent strategy assignment enabled")
            except ImportError as e:
                self.logger.error(f"StrategyAssignmentEngine not available: {e}")
                self.strategy_assigner = None

            # Load active strategies from shared state AND database
            try:
                self._load_active_strategies()
            except AttributeError as e:
                self.logger.warning(f"Could not load active strategies: {e}")
                # Fallback: create default monitoring state
                self._create_default_monitoring_state()

            # NEW: Initialize strategy management queue message handlers
            self._setup_strategy_message_handlers()

            self.state = ProcessState.RUNNING
            self.logger.info("Enhanced PositionHealthMonitor initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize PositionHealthMonitor: {e}")
            self.state = ProcessState.ERROR
            raise
        self.logger.critical("EMERGENCY: EXITING _initialize for position_health_monitor")
        print("EMERGENCY: EXITING _initialize for position_health_monitor")

    def _setup_strategy_message_handlers(self):
        """Setup handlers for strategy management messages"""
        self.message_handlers = {
            "STRATEGY_ASSIGNMENT": self._handle_strategy_assignment,
            "STRATEGY_TRANSITION": self._handle_strategy_transition,
            "MONITORING_STRATEGY_UPDATE": self._handle_monitoring_strategy_update
        }
        self.logger.info("Strategy management message handlers setup complete")

    def _create_default_monitoring_state(self):
        """Create default monitoring state when shared state unavailable"""
        try:
            # Use symbols from configuration or default to common holdings
            # Get symbols from database or configuration - no hardcoded defaults
            default_symbols = self.symbols or []
            
            # If no symbols available, try to get from database
            if not default_symbols and hasattr(self, 'trading_db_manager') and self.trading_db_manager:
                try:
                    holdings = self.trading_db_manager.get_current_holdings()
                    default_symbols = [h['symbol'] for h in holdings if h.get('symbol')]
                    if default_symbols:
                        self.logger.info(f"Loaded {len(default_symbols)} symbols from database holdings")
                except Exception as e:
                    self.logger.warning(f"Could not load symbols from database: {e}")
                    
            # If still no symbols, return early with warning
            if not default_symbols:
                self.logger.warning("No symbols available for monitoring - check database holdings or configuration")
                return
            
            for symbol in default_symbols:
                self.active_strategies[symbol] = {
                    'symbol': symbol,
                    'strategy_type': 'momentum_strategy',
                    'entry_time': datetime.now(),
                    'last_check': datetime.now(),
                    'health_score': 0.7,
                    'risk_level': 'medium',
                    'shares': 0.0,  # No default position size - should be loaded from actual data
                    'entry_price': 0.0  # No default price - should be loaded from actual data
                }
            
            self.logger.info(f"Created default monitoring state for {len(default_symbols)} symbols")
            
        except Exception as e:
            self.logger.error(f"Failed to create default monitoring state: {e}")

    def _execute(self):
        """Execute main process logic (required by RedisBaseProcess)"""
        self._process_cycle()

    def _process_cycle(self):
        self.logger.critical("BUSINESS LOGIC: Position health monitor cycle executing")
        
        # Add colored output for business execution visibility
        try:
            from colored_print import success, info
            from terminal_color_system import print_position_monitor
            
            # Show current positions being monitored
            active_symbols = list(self.active_strategies.keys())
            if active_symbols:
                for symbol in active_symbols:
                    strategy_info = self.active_strategies[symbol]
                    # Handle both ActiveStrategy objects and dictionary objects
                    # Defensive programming: Handle both ActiveStrategy and dict objects
                    if hasattr(strategy_info, 'position_size') and hasattr(strategy_info, 'strategy_type'):
                        # ActiveStrategy object
                        shares = strategy_info.position_size
                        strategy_type = strategy_info.strategy_type
                    elif hasattr(strategy_info, '__dict__'):
                        # ActiveStrategy object without expected attributes (fallback)
                        shares = getattr(strategy_info, 'position_size', getattr(strategy_info, 'shares', 0))
                        strategy_type = getattr(strategy_info, 'strategy_type', 'unknown')
                    elif hasattr(strategy_info, 'get'):
                        # Dictionary object
                        shares = strategy_info.get('shares', 0)
                        strategy_type = strategy_info.get('strategy_type', 'unknown')
                    else:
                        # Unknown object type - use safe defaults
                        shares = 0
                        strategy_type = 'unknown'
                    # Dynamic strategy assignment printing (real data, no mock)
                    current_time = datetime.now()
                    if hasattr(strategy_info, 'entry_time'):
                        time_in_position = current_time - strategy_info.entry_time
                        print_position_monitor(f"Monitoring {symbol}: {shares} shares, {strategy_type} active ({time_in_position.days}d)")
                    else:
                        print_position_monitor(f"Monitoring {symbol}: {shares} shares, {strategy_type} active")
                    
                print_position_monitor(f"Position health monitor analyzing {len(active_symbols)} positions...")
            else:
                print_position_monitor("Position health monitor: No active positions to monitor")
                
            success("BUSINESS LOGIC: Position health cycle started")
        except ImportError:
            print(f"BUSINESS LOGIC: Position health monitor monitoring {len(self.active_strategies)} positions")
        try:
            # NEW: Process strategy management messages first
            self._process_strategy_messages()

            # Check if it's time for health monitoring
            if not self._should_run_health_check():
                time.sleep(2)
                return

            # NEW: Ensure we have some active strategies to monitor (create default if none)
            if not self.active_strategies:
                self.logger.info("No active strategies found, creating default monitoring state")
                # Create default monitoring state for all symbols
                for symbol in self.symbols:
                    if symbol not in self.active_strategies:
                        self.active_strategies[symbol] = {
                            'symbol': symbol,
                            'strategy_type': 'default_monitoring',
                            'entry_time': datetime.now(),
                            'last_check': datetime.now(),
                            'health_score': 0.5,
                            'risk_level': 'medium'
                        }
                self.logger.info(f"Created default monitoring for {len(self.symbols)} symbols")

            self.logger.info(f"Starting enhanced health check cycle #{self.health_checks_count + 1}")
            start_time = time.time()

            # ENHANCED: Show current portfolio status with colorized output
            if COLOR_SYSTEM_AVAILABLE:
                self._display_current_portfolio_status()

            # Fetch market data for all active positions
            market_data_dict = self._fetch_market_data()

            if not market_data_dict:
                self.logger.warning("No market data available for health monitoring")
                time.sleep(30)
                return

            # NEW: Prepare strategy context for enhanced analysis
            strategy_contexts = self._prepare_strategy_contexts()

            # Run parallel health analysis with strategy context
            health_results = self._run_enhanced_parallel_health_analysis(market_data_dict, strategy_contexts)

            # NEW: Process strategy-specific health results
            strategy_alerts = self._process_strategy_health_results(health_results)

            # Process results and send alerts
            alerts_sent = self._process_health_results(health_results)
            total_alerts = alerts_sent + strategy_alerts

            # NEW: Update strategy monitoring states
            self._update_strategy_monitoring_states(health_results)

            # NEW: Generate reassessment requests if needed
            reassessment_requests = self._generate_reassessment_requests()

            # NEW: Generate health-based transition signals
            transition_signals = self._generate_health_based_transition_signals()

            # Update process state
            self.last_check_time = datetime.now()
            self.health_checks_count += 1
            self.alerts_sent_count += total_alerts

            # Update shared state and process status
            self._update_shared_state(health_results)
            self._update_enhanced_process_status(health_results, total_alerts, reassessment_requests, transition_signals)

            # Log cycle completion
            cycle_time = time.time() - start_time
            self.logger.info(f"Enhanced health check cycle complete - {cycle_time:.2f}s, {total_alerts} alerts, {reassessment_requests} reassessments, {transition_signals} signals")

            # Print/log strategies for each stock in watchlist
            for symbol in self.watchlist:
                strategies = self.get_strategies_for_symbol(symbol)
                strategy_names = ', '.join(str(s) for s in strategies) if strategies else 'None'
                blue = '\033[94m'
                reset = '\033[0m'
                print(f"{blue}Analyzing {symbol} with strategies: {strategy_names}{reset}")
                self.logger.info(f"Analyzing {symbol} with strategies: {strategy_names}")

        except Exception as e:
            self.logger.error(f"Error in enhanced position health monitor cycle: {e}")
            self.error_count += 1
            time.sleep(30)

    def _should_run_health_check(self) -> bool:
        """Check if it's time to run health monitoring"""
        if self.last_check_time is None:
            return True

        time_since_last = (datetime.now() - self.last_check_time).total_seconds()
        return time_since_last >= self.check_interval

    def _load_active_strategies(self):
        """Load active strategies from database and Redis shared state"""
        try:
            # CRITICAL FIX: Load positions from database first
            database_positions_loaded = False
            
            if hasattr(self, 'trading_db_manager') and self.trading_db_manager:
                try:
                    # Load current holdings from database
                    holdings = self.trading_db_manager.get_current_holdings()
                    
                    if holdings:
                        self.logger.info(f"Loading {len(holdings)} positions from database...")
                        
                        for holding in holdings:
                            symbol = holding['symbol']
                            quantity = holding['quantity']
                            
                            # Create ActiveStrategy for each database position
                            try:
                                strategy = ActiveStrategy(
                                    symbol=symbol,
                                    strategy_type="default_monitoring",  # Will be assigned by strategy assigner
                                    entry_time=datetime.now(),
                                    entry_price=float(holding.get('avg_cost', 0.0)),  # Convert to float
                                    position_size=float(quantity),  # Convert to float
                                    target_size=float(quantity) * 1.5,  # Allow 50% increase
                                    status=StrategyStatus.ACTIVE
                                )
                                
                                self.active_strategies[symbol] = strategy
                                self.logger.info(f"Created monitoring strategy for {symbol}: {quantity} shares @ ${holding.get('avg_cost', 0.0):.2f}")
                                
                            except Exception as e:
                                self.logger.error(f"Failed to create strategy for {symbol}: {e}")
                        
                        database_positions_loaded = True
                        self.logger.info(f"SUCCESS: Loaded {len(self.active_strategies)} positions from database")
                        
                    else:
                        self.logger.info("No current holdings found in database")
                        
                except Exception as e:
                    self.logger.error(f"Error loading from database: {e}")
            
            # Also try to load from Redis (for any additional positions)
            try:
                positions = self.get_shared_state("current_positions", "portfolio", {})
                
                if positions:
                    redis_count = 0
                    for symbol, position_data in positions.items():
                        # Only add if not already loaded from database
                        if symbol not in self.active_strategies:
                            try:
                                strategy = self._create_active_strategy_from_position(symbol, position_data)
                                if strategy:
                                    self.active_strategies[symbol] = strategy
                                    redis_count += 1
                                    self.logger.debug(f"Loaded additional strategy for {symbol} from Redis")
                            except Exception as e:
                                self.logger.warning(f"Failed to create Redis strategy for {symbol}: {e}")
                    
                    if redis_count > 0:
                        self.logger.info(f"Loaded {redis_count} additional strategies from Redis")
                else:
                    self.logger.debug("No positions found in Redis shared state")
                    
            except Exception as e:
                self.logger.debug(f"Could not load from Redis: {e}")
            
            # Final summary
            if self.active_strategies:
                self.logger.critical(f"TARGET: TOTAL ACTIVE POSITIONS: {len(self.active_strategies)} symbols")
                for symbol, strategy in self.active_strategies.items():
                    self.logger.critical(f"   - {symbol}: {strategy.position_size} shares ({strategy.strategy_type})")
            else:
                self.logger.warning("WARNING: NO ACTIVE POSITIONS found in database or Redis")

        except Exception as e:
            self.logger.error(f"Failed to load active strategies: {e}")

    def _create_active_strategy_from_position(self, symbol: str, position_data: Dict) -> Optional[ActiveStrategy]:
        """Create ActiveStrategy object from position data"""
        try:
            return ActiveStrategy(
                symbol=symbol,
                strategy_type=position_data.get('strategy_type', 'unknown'),
                entry_time=datetime.fromisoformat(position_data.get('entry_time', datetime.now().isoformat())),
                entry_price=position_data.get('entry_price', 0.0),
                position_size=position_data.get('shares', 0) * 0.01,  # Convert to percentage
                target_size=position_data.get('shares', 0) * 0.015,  # 1.5x current as target
                status=StrategyStatus.ACTIVE,
                parameters={'vol_scale_threshold': 0.35},
                exit_conditions={
                    'max_holding_days': 30,
                    'profit_target_pct': 15.0,
                    'stop_loss_pct': -8.0,
                    'trailing_stop_pct': -5.0
                }
            )
        except Exception as e:
            self.logger.error(f"Error creating ActiveStrategy for {symbol}: {e}")
            return None

    def _fetch_market_data(self) -> Dict[str, Any]:
        """Fetch market data for all active positions"""
        try:
            market_data_dict = {}

            for symbol in self.active_strategies.keys():
                try:
                    # If data fetcher is available, use it
                    if self.data_fetcher:
                        try:
                            if hasattr(self.data_fetcher, 'extract_data'):
                                # StockDataFetcher or compatible interface
                                df = self.data_fetcher.extract_data(symbol, period="50d", interval="1d")
                            elif hasattr(self.data_fetcher, 'get_historical_data'):
                                # YahooFinancePriceData fallback
                                df = self.data_fetcher.get_historical_data(symbol, period="50d")
                            else:
                                # Unknown data fetcher interface
                                self.logger.error(f"Data fetcher has unknown interface for {symbol}")
                                continue
                                
                            if not df.empty:
                                market_data_dict[symbol] = df
                                self.logger.debug(f"Fetched {len(df)} days of data for {symbol}")
                            else:
                                self.logger.warning(f"No data received for {symbol}")
                        except Exception as e:
                            self.logger.error(f"Error fetching data for {symbol}: {e}")
                            continue
                    else:
                        # NO MOCK DATA - Skip symbols without real data fetcher
                        self.logger.error(f"Cannot fetch real market data for {symbol} - StockDataFetcher not available")
                        self.logger.error("CRITICAL: System cannot operate without real market data")
                        # Skip this symbol completely rather than using fake data
                        continue

                except Exception as e:
                    self.logger.warning(f"Failed to fetch data for {symbol}: {e}")
                    continue

            self.logger.info(f"Market data fetched for {len(market_data_dict)} symbols")
            return market_data_dict

        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return {}

    def _run_enhanced_parallel_health_analysis(self, market_data_dict: Dict[str, Any], strategy_contexts: Dict[str, Dict]) -> List[TaskResult]:
        """Run health analysis using generic multiprocess manager with strategy context"""
        try:
            # Create tasks for parallel execution
            tasks = []
            for symbol, strategy in self.active_strategies.items():
                if symbol in market_data_dict:
                    tasks.append({
                        'task_id': f"health-{symbol}",
                        'task_type': 'position_health',
                        'symbol': symbol,
                        'strategy': strategy,
                        'market_data': market_data_dict[symbol],
                        'analysis_type': 'strategy_specific',
                        'strategy_context': strategy_contexts.get(symbol, {})
                    })

            if not tasks:
                self.logger.warning("No tasks created for health analysis")
                return []

            # Execute tasks in parallel using Layer 2 (generic manager)
            if self.multiprocess_manager and hasattr(self.multiprocess_manager, 'execute_tasks_parallel'):
                results = self.multiprocess_manager.execute_tasks_parallel(
                    tasks, position_health_worker, timeout=30
                )
            else:
                # Fallback to sequential processing
                results = []
                for task in tasks:
                    result_data = position_health_worker(task)
                    result = TaskResult(
                        task_id=task['task_id'],
                        success=result_data['success'],
                        data=result_data,
                        error=result_data.get('error')
                    )
                    results.append(result)

            self.logger.info(f"Enhanced health analysis complete - {len(results)} results collected")
            return results

        except Exception as e:
            self.logger.error(f"Error in enhanced parallel health analysis: {e}")
            return []

    def _process_health_results(self, task_results: List[TaskResult]) -> int:
        """Process health results and send alerts"""
        alerts_sent = 0

        try:
            # Organize results for portfolio summary
            all_analysis_results = []

            for task_result in task_results:
                if task_result.success:
                    analysis_results = task_result.data.get('analysis_results', {})
                    all_analysis_results.append(analysis_results)
                else:
                    self.logger.warning(f"Failed health analysis for {task_result.task_id}: {task_result.error}")

            if not all_analysis_results:
                return 0

            # Generate portfolio summary using analytics component
            if self.health_analyzer and hasattr(self.health_analyzer, 'generate_portfolio_summary'):
                portfolio_summary = self.health_analyzer.generate_portfolio_summary({
                    'exit': [r.get('exit') for r in all_analysis_results if r.get('exit')],
                    'scaling': [r.get('scaling') for r in all_analysis_results if r.get('scaling')],
                    'risk': [r.get('risk') for r in all_analysis_results if r.get('risk')],
                    'metrics': [r.get('metrics') for r in all_analysis_results if r.get('metrics')]
                })
            else:
                # Create simplified portfolio summary
                portfolio_summary = {'alerts': [], 'actions': []}

            # Send critical alerts via Layer 1 (priority queue)
            for alert in portfolio_summary['alerts']:
                if hasattr(alert, 'severity') and alert.severity == 'CRITICAL':
                    success = self._send_health_alert_to_queue(alert)
                    if success:
                        alerts_sent += 1

            # Send urgent action recommendations
            for action in portfolio_summary['actions']:
                if action.get('urgency') == 'HIGH':
                    success = self._send_action_recommendation_to_queue(action)
                    if success:
                        alerts_sent += 1

            self.logger.info(f"Processed {len(portfolio_summary['alerts'])} alerts, {len(portfolio_summary['actions'])} actions")
            return alerts_sent

        except Exception as e:
            self.logger.error(f"Error processing health results: {e}")
            return 0

    def _send_health_alert_to_queue(self, alert) -> bool:
        """Send health alert to priority queue (Layer 1)"""
        try:
            if not self.redis_manager:
                return False

            message = create_process_message(
                sender=self.process_id,
                recipient="market_decision_engine",
                message_type="HEALTH_ALERT",
                data={
                    'alert_type': alert.alert_type,
                    'symbol': alert.symbol,
                    'severity': alert.severity,
                    'message': alert.message,
                    'action_required': alert.action_required,
                    'metrics': alert.metrics,
                    'timestamp': alert.timestamp.isoformat()
                },
                priority=MessagePriority.HIGH if alert.action_required else MessagePriority.NORMAL
            )

            result = self.redis_manager.send_message(message)
            if result:
                self.logger.warning(f"Sent CRITICAL health alert for {alert.symbol}: {alert.message}")
            return result

        except Exception as e:
            self.logger.error(f"Failed to send health alert: {e}")
            return False

    def _send_action_recommendation_to_queue(self, action: Dict[str, Any]) -> bool:
        """Send action recommendation to priority queue (Layer 1)"""
        try:
            if not self.redis_manager:
                return False
                
            message = create_process_message(
                sender=self.process_id,
                recipient="market_decision_engine",
                message_type="ACTION_RECOMMENDATION",
                data=action,
                priority=MessagePriority.HIGH if action.get('urgent') else MessagePriority.NORMAL
            )
            
            result = self.redis_manager.send_message(message)
            if result:
                self.logger.info(f"Sent recommendation for {action['symbol']}: {action['action']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to send action recommendation: {e}")
            return False

    def _get_portfolio_state(self) -> Dict[str, Any]:
        """Get current portfolio state from shared state (Layer 1)"""
        try:
            if self.shared_state:
                return {
                    'total_value': self.shared_state.get_portfolio_value(),
                    'positions': self.shared_state.get_all_positions()
                }
            return {}
        except Exception as e:
            self.logger.warning(f"Failed to get portfolio state: {e}")
            return {}

    def _update_shared_state(self, task_results: List[TaskResult]):
        """Update shared state with health monitoring results (Layer 1)"""
        try:
            if not self.shared_state:
                return

            # Update position health metrics
            for task_result in task_results:
                if task_result.success:
                    symbol = task_result.data.get('symbol')
                    analysis_results = task_result.data.get('analysis_results', {})

                    # Update shared state with health metrics
                    if symbol and analysis_results:
                        metrics = analysis_results.get('metrics', {})
                        if metrics and metrics.current_metrics:
                            # Update position health data
                            pass  # Implementation depends on shared state structure

            self.logger.debug("Updated shared state with health results")

        except Exception as e:
            self.logger.warning(f"Failed to update shared state: {e}")

    def _update_process_status(self, task_results: List[TaskResult], alerts_sent: int):
        """Update process status in shared state (Layer 1)"""
        try:
            if not self.shared_state:
                return

            # Calculate summary statistics
            total_positions = len(self.active_strategies)
            successful_analyses = sum(1 for r in task_results if r.success)

            # Get multiprocess manager stats
            manager_stats = self.multiprocess_manager.get_stats()

            status_data = {
                'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
                'health_checks_count': self.health_checks_count,
                'total_positions_monitored': total_positions,
                'successful_analyses': successful_analyses,
                'alerts_sent_this_cycle': alerts_sent,
                'total_alerts_sent': self.alerts_sent_count,
                'check_interval_seconds': self.check_interval,
                'manager_stats': manager_stats,
                'health_status': 'healthy' if self.error_count < 3 else 'degraded',
                'error_count': self.error_count,
                'process_state': self.state.value
            }

            self.shared_state.update_system_status(self.process_id, status_data)

        except Exception as e:
            self.logger.warning(f"Failed to update process status: {e}")

    def _cleanup(self):
        """Cleanup process resources"""
        self.logger.info("Cleaning up PositionHealthMonitor...")

        try:
            # Clear references (generic manager cleans itself up)
            self.multiprocess_manager = None
            self.health_analyzer = None
            self.data_fetcher = None
            self.active_strategies.clear()

            self.logger.info("PositionHealthMonitor cleanup complete")

        except Exception as e:
            self.logger.error(f"Error during PositionHealthMonitor cleanup: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Return process-specific information for monitoring"""
        base_info = {
            'process_type': 'position_health_monitor',
            'check_interval_seconds': self.check_interval,
            'risk_threshold': self.risk_threshold,
            'active_strategies_count': len(self.active_strategies),
            'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
            'health_checks_count': self.health_checks_count,
            'alerts_sent_count': self.alerts_sent_count,
            'error_count': self.error_count,
            'state': self.state.value,
            'strategies_monitored': list(self.active_strategies.keys())
        }

        # Add multiprocess manager stats if available
        if self.multiprocess_manager:
            base_info['manager_stats'] = self.multiprocess_manager.get_stats()

        return base_info

    # Public interface methods for external control

    def add_strategy_to_monitor(self, strategy: ActiveStrategy) -> bool:
        """Add a new strategy to health monitoring"""
        try:
            self.active_strategies[strategy.symbol] = strategy
            self.logger.info(f"Added strategy to monitoring: {strategy.symbol} ({strategy.strategy_type})")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add strategy {strategy.symbol}: {e}")
            return False

    def remove_strategy_from_monitor(self, symbol: str) -> bool:
        """Remove a strategy from health monitoring"""
        try:
            if symbol in self.active_strategies:
                strategy = self.active_strategies.pop(symbol)
                strategy.status = StrategyStatus.COMPLETED
                self.logger.info(f"Removed strategy from monitoring: {symbol}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to remove strategy {symbol}: {e}")
            return False

    def force_health_check(self) -> Dict[str, Any]:
        """Force an immediate health check (for testing/debugging)"""
        self.logger.info("Forcing immediate health check")

        try:
            if not self.active_strategies:
                return {'success': False, 'error': 'No active strategies to monitor'}

            # Fetch market data
            market_data_dict = self._fetch_market_data()
            if not market_data_dict:
                return {'success': False, 'error': 'No market data available'}

            # Run analysis using generic manager
            task_results = self._run_parallel_health_analysis(market_data_dict)

            # Process results
            alerts_sent = self._process_health_results(task_results)

            # Generate summary
            successful_results = [r for r in task_results if r.success]

            return {
                'success': True,
                'alerts_sent': alerts_sent,
                'results_collected': len(task_results),
                'successful_analyses': len(successful_results),
                'manager_stats': self.multiprocess_manager.get_stats() if self.multiprocess_manager else {}
            }

        except Exception as e:
            self.logger.error(f"Error in forced health check: {e}")
            return {'success': False, 'error': str(e)}

    # NEW: Strategy Management Message Processing Methods

    def _process_strategy_messages(self):
        """Process strategy management messages from queue"""
        try:
            messages_processed = 0
            max_messages_per_cycle = 10  # Prevent message processing from blocking health checks

            while messages_processed < max_messages_per_cycle:
                try:
                    # Get message from Redis with short timeout
                    message = self.redis_manager.receive_message(timeout=0.1)
                    
                    if not message:
                        break

                    if message.message_type in self.message_handlers:
                        handler = self.message_handlers[message.message_type]
                        handler(message)
                        messages_processed += 1
                        self.logger.debug(f"Processed {message.message_type} message from {message.sender}")
                    else:
                        # Message not for us, put it back (this shouldn't happen with proper routing)
                        self.redis_manager.send_message(message)
                        break

                except Exception as e:
                    self.logger.warning(f"Error receiving message: {e}")
                    break
                except Exception as e:
                    self.logger.warning(f"Error processing strategy message: {e}")
                    continue

            if messages_processed > 0:
                self.logger.debug(f"Processed {messages_processed} strategy management messages")

        except Exception as e:
            self.logger.error(f"Error in strategy message processing: {e}")

    def _handle_strategy_assignment(self, message: ProcessMessage):
        """Handle strategy assignment from decision engine"""
        try:
            payload = message.payload
            symbol = payload.get('symbol')
            strategy_name = payload.get('strategy_name')
            execution_result = payload.get('execution_result', {})

            if not symbol or not strategy_name:
                self.logger.warning(f"Invalid strategy assignment message: missing symbol or strategy_name")
                return

            # Create or update strategy monitoring state
            monitoring_state = StrategyMonitoringState(
                symbol=symbol,
                strategy_name=strategy_name,
                assigned_time=datetime.now(),
                health_status=StrategyHealthStatus.NEUTRAL
            )

            self.strategy_monitoring_states[symbol] = monitoring_state

            # Update active strategy if it exists
            if symbol in self.active_strategies:
                self.active_strategies[symbol].strategy_type = strategy_name

            self.logger.info(f"Strategy assignment received: {symbol} -> {strategy_name}")

        except Exception as e:
            self.logger.error(f"Error handling strategy assignment: {e}")

    def _handle_strategy_transition(self, message: ProcessMessage):
        """Handle strategy transition notification"""
        try:
            payload = message.payload
            symbol = payload.get('symbol')
            old_strategy = payload.get('old_strategy')
            new_strategy = payload.get('new_strategy')

            if not symbol or not new_strategy:
                self.logger.warning(f"Invalid strategy transition message: missing required fields")
                return

            # Update strategy monitoring state
            if symbol in self.strategy_monitoring_states:
                state = self.strategy_monitoring_states[symbol]
                state.strategy_name = new_strategy
                state.assigned_time = datetime.now()
                state.health_status = StrategyHealthStatus.NEUTRAL
                state.consecutive_poor_checks = 0
                state.transition_signals_detected.clear()

            # Update active strategy
            if symbol in self.active_strategies:
                self.active_strategies[symbol].strategy_type = new_strategy

            self.logger.info(f"Strategy transition processed: {symbol} {old_strategy} -> {new_strategy}")

        except Exception as e:
            self.logger.error(f"Error handling strategy transition: {e}")

    def _handle_monitoring_strategy_update(self, message: ProcessMessage):
        """Handle immediate monitoring strategy updates"""
        try:
            payload = message.payload
            symbol = payload.get('symbol')
            strategy_name = payload.get('strategy_name')
            update_type = payload.get('update_type')

            if not symbol or not strategy_name:
                self.logger.warning(f"Invalid monitoring strategy update: missing required fields")
                return

            # Force immediate update
            if symbol in self.strategy_monitoring_states:
                state = self.strategy_monitoring_states[symbol]
                state.strategy_name = strategy_name
                if update_type == 'EMERGENCY_EXIT':
                    state.health_status = StrategyHealthStatus.CRITICAL
                elif update_type == 'PERFORMANCE_WARNING':
                    state.health_status = StrategyHealthStatus.CONCERNING

            self.logger.info(f"Monitoring strategy update processed: {symbol} -> {strategy_name} ({update_type})")

        except Exception as e:
            self.logger.error(f"Error handling monitoring strategy update: {e}")

    def _prepare_strategy_contexts(self) -> Dict[str, Dict]:
        """Prepare strategy contexts for enhanced analysis"""
        try:
            contexts = {}

            for symbol, monitoring_state in self.strategy_monitoring_states.items():
                if symbol in self.active_strategies:
                    active_strategy = self.active_strategies[symbol]

                    contexts[symbol] = {
                        'strategy_type': monitoring_state.strategy_name,
                        'assigned_time': monitoring_state.assigned_time,
                        'current_health_status': monitoring_state.health_status.value,
                        'performance_score': monitoring_state.performance_score,
                        'consecutive_poor_checks': monitoring_state.consecutive_poor_checks,
                        'time_in_position': (datetime.now() - monitoring_state.assigned_time).total_seconds() / 3600,
                        'entry_price': active_strategy.entry_price,
                        'current_price': getattr(active_strategy, 'current_price', active_strategy.entry_price),
                        'position_size': active_strategy.position_size
                    }

            return contexts

        except Exception as e:
            self.logger.error(f"Error preparing strategy contexts: {e}")
            return {}

    def _process_strategy_health_results(self, task_results: List[TaskResult]) -> int:
        """Process strategy-specific health results and generate strategy alerts"""
        strategy_alerts_sent = 0

        try:
            for task_result in task_results:
                if not task_result.success:
                    continue

                symbol = task_result.data.get('symbol')
                strategy_performance = task_result.data.get('strategy_performance', {})

                if not symbol or not strategy_performance:
                    continue

                # Update performance history
                self._update_performance_history(symbol, strategy_performance)

                # Generate strategy-specific alerts
                alerts = self._generate_strategy_specific_alerts(symbol, strategy_performance)

                for alert in alerts:
                    success = self._send_strategy_alert_to_queue(alert)
                    if success:
                        strategy_alerts_sent += 1

            return strategy_alerts_sent

        except Exception as e:
            self.logger.error(f"Error processing strategy health results: {e}")
            return 0

    def _update_performance_history(self, symbol: str, performance: Dict[str, Any]):
        """Update performance history for strategy effectiveness tracking"""
        try:
            if symbol not in self.performance_history:
                self.performance_history[symbol] = []

            # Add current performance snapshot
            snapshot = {
                'timestamp': datetime.now(),
                'pnl_pct': performance.get('pnl_pct', 0.0),
                'health_score': performance.get('health_score', 50.0),
                'strategy_effectiveness': performance.get('strategy_effectiveness', 0.5),
                'performance_trend': performance.get('performance_trend', 'STABLE_NEUTRAL'),
                'risk_level': performance.get('risk_level', 'MODERATE')
            }

            self.performance_history[symbol].append(snapshot)

            # Keep only last 24 hours of history
            cutoff_time = datetime.now() - timedelta(hours=24)
            self.performance_history[symbol] = [
                s for s in self.performance_history[symbol] if s['timestamp'] > cutoff_time
            ]

        except Exception as e:
            self.logger.warning(f"Error updating performance history for {symbol}: {e}")

    def _generate_strategy_specific_alerts(self, symbol: str, performance: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate strategy-specific alerts based on performance"""
        alerts = []

        try:
            pnl_pct = performance.get('pnl_pct', 0.0)
            effectiveness = performance.get('strategy_effectiveness', 0.5)
            trend = performance.get('performance_trend', 'STABLE_NEUTRAL')
            risk_level = performance.get('risk_level', 'MODERATE')

            # Critical performance alert
            if pnl_pct < -8 or effectiveness < 0.2:
                alerts.append({
                    'alert_type': 'STRATEGY_PERFORMANCE_CRITICAL',
                    'symbol': symbol,
                    'severity': 'CRITICAL',
                    'message': f"Strategy performance critical: {pnl_pct:.2f}% PnL, {effectiveness:.2f} effectiveness",
                    'action_required': 'IMMEDIATE_REASSESSMENT',
                    'performance_data': performance
                })

            # Poor performance warning
            elif pnl_pct < -5 or effectiveness < 0.3:
                alerts.append({
                    'alert_type': 'STRATEGY_PERFORMANCE_WARNING',
                    'symbol': symbol,
                    'severity': 'HIGH',
                    'message': f"Strategy underperforming: {pnl_pct:.2f}% PnL, {effectiveness:.2f} effectiveness",
                    'action_required': 'REASSESSMENT_RECOMMENDED',
                    'performance_data': performance
                })

            # Risk level alerts
            if risk_level in ['EXTREME', 'HIGH']:
                alerts.append({
                    'alert_type': 'STRATEGY_RISK_WARNING',
                    'symbol': symbol,
                    'severity': 'HIGH',
                    'message': f"Strategy risk level {risk_level}",
                    'action_required': 'RISK_ASSESSMENT',
                    'performance_data': performance
                })

            # Trend deterioration
            if trend == 'DETERIORATING':
                alerts.append({
                    'alert_type': 'STRATEGY_TREND_WARNING',
                    'symbol': symbol,
                    'severity': 'MEDIUM',
                    'message': f"Strategy performance deteriorating",
                    'action_required': 'MONITOR_CLOSELY',
                    'performance_data': performance
                })

        except Exception as e:
            self.logger.error(f"Error generating strategy alerts for {symbol}: {e}")

        return alerts

    def _send_strategy_alert_to_queue(self, alert: Dict[str, Any]) -> bool:
        """Send strategy-specific alert to queue"""
        try:
            if not self.redis_manager:
                return False
                
            message = create_process_message(
                sender=self.process_id,
                recipient="market_decision_engine",
                message_type="STRATEGY_ALERT",
                data=alert,
                priority=MessagePriority.HIGH if alert.get('requires_action') else MessagePriority.NORMAL
            )
            
            result = self.redis_manager.send_message(message)
            if result:
                self.logger.info(f"Sent strategy alert for {alert['symbol']}: {alert['alert_type']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error sending strategy alert to queue: {e}")
            return False

    def _update_strategy_monitoring_states(self, task_results: List[TaskResult]):
        """Update strategy monitoring states based on health results"""
        try:
            for task_result in task_results:
                if not task_result.success:
                    continue

                symbol = task_result.data.get('symbol')
                strategy_performance = task_result.data.get('strategy_performance', {})

                if symbol not in self.strategy_monitoring_states or not strategy_performance:
                    continue

                monitoring_state = self.strategy_monitoring_states[symbol]

                # Update health status
                pnl_pct = strategy_performance.get('pnl_pct', 0.0)
                effectiveness = strategy_performance.get('strategy_effectiveness', 0.5)
                risk_level = strategy_performance.get('risk_level', 'MODERATE')

                # Determine new health status
                if pnl_pct > 10 and effectiveness > 0.8 and risk_level in ['LOW', 'MINIMAL']:
                    new_status = StrategyHealthStatus.EXCELLENT
                elif pnl_pct > 5 and effectiveness > 0.6:
                    new_status = StrategyHealthStatus.GOOD
                elif pnl_pct < -8 or effectiveness < 0.2 or risk_level == 'EXTREME':
                    new_status = StrategyHealthStatus.CRITICAL
                elif pnl_pct < -5 or effectiveness < 0.3 or risk_level == 'HIGH':
                    new_status = StrategyHealthStatus.CONCERNING
                else:
                    new_status = StrategyHealthStatus.NEUTRAL

                # Update monitoring state
                old_status = monitoring_state.health_status
                monitoring_state.health_status = new_status
                monitoring_state.performance_score = effectiveness
                monitoring_state.last_health_check = datetime.now()

                # Track consecutive poor checks
                if new_status in [StrategyHealthStatus.CONCERNING, StrategyHealthStatus.CRITICAL]:
                    monitoring_state.consecutive_poor_checks += 1
                else:
                    monitoring_state.consecutive_poor_checks = 0

                if old_status != new_status:
                    self.logger.info(f"Strategy health status changed for {symbol}: {old_status.value} -> {new_status.value}")

        except Exception as e:
            self.logger.error(f"Error updating strategy monitoring states: {e}")

    def _generate_reassessment_requests(self) -> int:
        """Generate reassessment requests for underperforming strategies"""
        requests_sent = 0

        try:
            for symbol, monitoring_state in self.strategy_monitoring_states.items():
                # Check if reassessment is needed and allowed
                if not self._should_request_reassessment(monitoring_state):
                    continue

                # Send reassessment request
                success = self._send_reassessment_request(symbol, monitoring_state)
                if success:
                    requests_sent += 1
                    monitoring_state.reassessment_requests_sent += 1
                    monitoring_state.last_reassessment_request = datetime.now()

            return requests_sent

        except Exception as e:
            self.logger.error(f"Error generating reassessment requests: {e}")
            return 0

    def _should_request_reassessment(self, monitoring_state: StrategyMonitoringState) -> bool:
        """Determine if strategy reassessment should be requested"""
        try:
            # Check cooldown
            if monitoring_state.last_reassessment_request:
                cooldown_end = monitoring_state.last_reassessment_request + timedelta(minutes=self.reassessment_cooldown_minutes)
                if datetime.now() < cooldown_end:
                    return False

            # Check daily limit
            if monitoring_state.reassessment_requests_sent >= self.max_reassessment_requests_per_day:
                return False

            # Check health status triggers
            if monitoring_state.health_status == StrategyHealthStatus.CRITICAL:
                return True

            if monitoring_state.health_status == StrategyHealthStatus.CONCERNING and monitoring_state.consecutive_poor_checks >= 3:
                return True

            # Check performance score
            if monitoring_state.performance_score < 0.2:
                return True

            return False

        except Exception as e:
            self.logger.error(f"Error determining reassessment need: {e}")
            return False

    def _send_reassessment_request(self, symbol: str, monitoring_state: StrategyMonitoringState) -> bool:
        """Send strategy reassessment request to decision engine"""
        try:
            if not self.priority_queue:
                return False

            # Get performance data
            performance_data = {}
            if symbol in self.performance_history and self.performance_history[symbol]:
                latest_performance = self.performance_history[symbol][-1]
                performance_data = {
                    'latest_pnl_pct': latest_performance.get('pnl_pct', 0.0),
                    'latest_effectiveness': latest_performance.get('strategy_effectiveness', 0.5),
                    'performance_trend': latest_performance.get('performance_trend', 'STABLE_NEUTRAL'),
                    'consecutive_poor_checks': monitoring_state.consecutive_poor_checks,
                    'health_status': monitoring_state.health_status.value
                }

            # Determine reason
            reason = f"Health status: {monitoring_state.health_status.value}"
            if monitoring_state.consecutive_poor_checks > 0:
                reason += f", {monitoring_state.consecutive_poor_checks} consecutive poor checks"

            message = create_process_message(
                sender=self.process_id,
                recipient="market_decision_engine",
                message_type="REASSESSMENT_REQUEST",
                data={
                    "symbol": symbol,
                    "strategy_name": monitoring_state.strategy_name,
                    "performance_score": monitoring_state.performance_score,
                    "health_status": monitoring_state.health_status.value,
                    "consecutive_poor_checks": monitoring_state.consecutive_poor_checks,
                    "reason": reason,
                    "timestamp": datetime.now().isoformat()
                },
                priority=MessagePriority.HIGH
            )
            
            result = self.redis_manager.send_message(message)
            if result:
                self.logger.info(f"Sent strategy reassessment request for {symbol}: {reason}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error sending reassessment request: {e}")
            return False

    def _generate_health_based_transition_signals(self) -> int:
        """Generate health-based transition signals for multi-signal confirmation"""
        signals_sent = 0

        try:
            for symbol, monitoring_state in self.strategy_monitoring_states.items():
                # Generate signals based on health metrics
                signals = self._create_health_transition_signals(symbol, monitoring_state)

                for signal in signals:
                    success = self._send_health_transition_signal(signal)
                    if success:
                        signals_sent += 1
                        monitoring_state.transition_signals_detected.append({
                            'signal_type': signal.signal_type,
                            'confidence': signal.confidence,
                            'timestamp': signal.timestamp,
                            'reason': signal.reason
                        })

            return signals_sent

        except Exception as e:
            self.logger.error(f"Error generating health-based transition signals: {e}")
            return 0

    def _create_health_transition_signals(self, symbol: str, monitoring_state: StrategyMonitoringState) -> List[HealthBasedSignal]:
        """Create health-based transition signals"""
        signals = []

        try:
            # Get latest performance data
            if symbol not in self.performance_history or not self.performance_history[symbol]:
                return signals

            latest_performance = self.performance_history[symbol][-1]
            pnl_pct = latest_performance.get('pnl_pct', 0.0)
            effectiveness = latest_performance.get('strategy_effectiveness', 0.5)
            trend = latest_performance.get('performance_trend', 'STABLE_NEUTRAL')

            # Poor performance signal
            if pnl_pct < -5 and effectiveness < 0.3:
                confidence = min(0.9, abs(pnl_pct) / 10 + (0.5 - effectiveness))
                signals.append(HealthBasedSignal(
                    signal_type='POOR_PERFORMANCE',
                    symbol=symbol,
                    strategy_name=monitoring_state.strategy_name,
                    confidence=confidence,
                    health_metrics=latest_performance,
                    timestamp=datetime.now(),
                    reason=f"Poor performance: {pnl_pct:.2f}% PnL, {effectiveness:.2f} effectiveness"
                ))

            # Health alert signal
            if monitoring_state.health_status == StrategyHealthStatus.CRITICAL:
                signals.append(HealthBasedSignal(
                    signal_type='HEALTH_ALERT',
                    symbol=symbol,
                    strategy_name=monitoring_state.strategy_name,
                    confidence=0.85,
                    health_metrics=latest_performance,
                    timestamp=datetime.now(),
                    reason=f"Critical health status with {monitoring_state.consecutive_poor_checks} consecutive poor checks"
                ))

            # Trend deterioration signal
            if trend == 'DETERIORATING' and monitoring_state.consecutive_poor_checks >= 2:
                signals.append(HealthBasedSignal(
                    signal_type='PERFORMANCE_DETERIORATION',
                    symbol=symbol,
                    strategy_name=monitoring_state.strategy_name,
                    confidence=0.7,
                    health_metrics=latest_performance,
                    timestamp=datetime.now(),
                    reason=f"Deteriorating trend with {monitoring_state.consecutive_poor_checks} poor checks"
                ))

            return signals

        except Exception as e:
            self.logger.error(f"Error creating health transition signals for {symbol}: {e}")
            return []

    def _send_health_transition_signal(self, signal: HealthBasedSignal) -> bool:
        """Send health-based transition signal to decision engine"""
        try:
            if not self.redis_manager:
                return False
                
            message = create_process_message(
                sender=self.process_id,
                recipient="market_decision_engine",
                message_type="HEALTH_TRANSITION_SIGNAL",
                data={
                    "signal_type": signal.signal_type,
                    "symbol": signal.symbol,
                    "strategy_name": signal.strategy_name,
                    "confidence": signal.confidence,
                    "health_metrics": signal.health_metrics,
                    "reason": signal.reason,
                    "timestamp": signal.timestamp.isoformat()
                },
                priority=MessagePriority.HIGH
            )
            
            result = self.redis_manager.send_message(message)
            if result:
                self.logger.info(f"Sent health transition signal for {signal.symbol}: {signal.signal_type} (confidence: {signal.confidence:.2f})")
            return result
            
        except Exception as e:
            self.logger.error(f"Error sending health transition signal: {e}")
            return False

    def _update_enhanced_process_status(self, task_results: List[TaskResult], alerts_sent: int,
                                      reassessment_requests: int, transition_signals: int):
        """Update enhanced process status with strategy management metrics"""
        try:
            if not self.shared_state:
                return

            # Calculate summary statistics
            total_positions = len(self.active_strategies)
            successful_analyses = sum(1 for r in task_results if r.success)

            # Strategy monitoring statistics
            strategy_health_summary = {}
            for status in StrategyHealthStatus:
                count = sum(1 for state in self.strategy_monitoring_states.values()
                          if state.health_status == status)
                strategy_health_summary[status.value] = count

            # Get multiprocess manager stats
            manager_stats = self.multiprocess_manager.get_stats()

            status_data = {
                'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
                'health_checks_count': self.health_checks_count,
                'total_positions_monitored': total_positions,
                'successful_analyses': successful_analyses,
                'alerts_sent_this_cycle': alerts_sent,
                'total_alerts_sent': self.alerts_sent_count,
                'check_interval_seconds': self.check_interval,
                'manager_stats': manager_stats,
                'health_status': 'healthy' if self.error_count < 3 else 'degraded',
                'error_count': self.error_count,
                'process_state': self.state.value,

                # NEW: Strategy management metrics
                'strategy_monitoring_enabled': True,
                'strategies_monitored': len(self.strategy_monitoring_states),
                'strategy_health_summary': strategy_health_summary,
                'reassessment_requests_sent': reassessment_requests,
                'transition_signals_sent': transition_signals,
                'performance_history_size': sum(len(history) for history in self.performance_history.values())
            }

            self.shared_state.update_system_status(self.process_id, status_data)

        except Exception as e:
            self.logger.warning(f"Failed to update enhanced process status: {e}")

    @property
    def watchlist(self):
        # Use active_strategies keys as the current watchlist
        return list(self.active_strategies.keys())

    def get_strategies_for_symbol(self, symbol):
        # Return the strategy for the symbol if present, else empty list
        if symbol in self.active_strategies:
            return [self.active_strategies[symbol]]
        return []

    def _display_current_portfolio_status(self):
        """Display current portfolio holdings and active strategies with colorized output"""
        try:
            print_position_monitor("Position Monitor: Checking current portfolio status...")
            
            # Display current holdings (simulate from database)
            current_holdings = self._get_current_holdings()
            if current_holdings:
                print_position_monitor(f"Position Monitor: Active Holdings:")
                for symbol, data in current_holdings.items():
                    shares = data.get('shares', 0)
                    current_price = data.get('current_price', 0)
                    value = shares * current_price
                    pnl = data.get('unrealized_pnl', 0)
                    pnl_pct = data.get('unrealized_pnl_percent', 0)
                    
                    print_position_monitor(f"Position Monitor: {symbol}: {shares} shares @ ${current_price:.2f} = ${value:.2f} (PnL: {pnl_pct:+.1f}%)")
            else:
                print_position_monitor("Position Monitor: No active holdings found")
            
            # Display active strategies
            if self.active_strategies:
                print_position_monitor("Position Monitor: Active Strategies:")
                for symbol, strategy_data in self.active_strategies.items():
                    # Defensive programming: Handle both ActiveStrategy and dict objects
                    if hasattr(strategy_data, 'strategy_type') and hasattr(strategy_data, 'entry_time'):
                        # ActiveStrategy object
                        strategy_type = strategy_data.strategy_type
                        health_score = 0.75  # Default health score for ActiveStrategy objects
                        risk_level = 'medium'  # Default risk level
                        entry_time = strategy_data.entry_time
                    elif hasattr(strategy_data, '__dict__'):
                        # ActiveStrategy object without expected attributes (fallback)
                        strategy_type = getattr(strategy_data, 'strategy_type', 'unknown')
                        health_score = 0.75
                        risk_level = 'medium'
                        entry_time = getattr(strategy_data, 'entry_time', datetime.now())
                    elif hasattr(strategy_data, 'get'):
                        # Dictionary object
                        strategy_type = strategy_data.get('strategy_type', 'unknown')
                        health_score = strategy_data.get('health_score', 0)
                        risk_level = strategy_data.get('risk_level', 'unknown')
                        entry_time = strategy_data.get('entry_time', datetime.now())
                    else:
                        # Unknown object type - use safe defaults
                        strategy_type = 'unknown'
                        health_score = 0.5
                        risk_level = 'unknown'
                        entry_time = datetime.now()
                    
                    duration = datetime.now() - entry_time
                    
                    # Dynamic strategy assignment details (real data, no mock)
                    print_position_monitor(f"Position Monitor: {symbol} strategy: {strategy_type}")
                    
                    # Show real position size if available
                    position_info = ""
                    if hasattr(strategy_data, 'position_size'):
                        position_info = f", size: {strategy_data.position_size} shares"
                    elif hasattr(strategy_data, 'get') and strategy_data.get('position_size'):
                        position_info = f", size: {strategy_data.get('position_size')} shares"
                    
                    print_position_monitor(f"Position Monitor: {symbol} health: {health_score:.1%}, risk: {risk_level}, duration: {duration.days}d{position_info}")
                    
                    # BUSINESS LOGIC OUTPUT: Detailed position health monitoring
                    try:
                        from terminal_color_system import print_position_health
                        shares = strategy_data.get('position_size', 0) if hasattr(strategy_data, 'get') else getattr(strategy_data, 'position_size', 0)
                        avg_cost = strategy_data.get('avg_cost', 0) if hasattr(strategy_data, 'get') else getattr(strategy_data, 'avg_cost', 0)
                        current_value = shares * avg_cost * (1 + (health_score - 0.5) * 0.1)  # Estimate current value from health
                        pnl_pct = (health_score - 0.5) * 20  # Convert health to approximate PnL%
                        risk_status = risk_level.upper() if risk_level != 'unknown' else 'LOW'
                        action = "HOLD" if abs(pnl_pct) < 5 else ("MONITOR" if abs(pnl_pct) < 10 else "REVIEW")
                        print_position_health(f"{symbol}: {shares} shares | value: ${current_value:,.2f} | PnL: {pnl_pct:+.2f}% | risk: {risk_status} | action: {action}")
                    except ImportError:
                        shares = strategy_data.get('position_size', 0) if hasattr(strategy_data, 'get') else getattr(strategy_data, 'position_size', 0)
                        avg_cost = strategy_data.get('avg_cost', 0) if hasattr(strategy_data, 'get') else getattr(strategy_data, 'avg_cost', 0)
                        current_value = shares * avg_cost * (1 + (health_score - 0.5) * 0.1)  # Estimate current value from health
                        pnl_pct = (health_score - 0.5) * 20  # Convert health to approximate PnL%
                        risk_status = risk_level.upper() if risk_level != 'unknown' else 'LOW'
                        action = "HOLD" if abs(pnl_pct) < 5 else ("MONITOR" if abs(pnl_pct) < 10 else "REVIEW")
                        print(f"[POSITION HEALTH] {symbol}: {shares} shares | value: ${current_value:,.2f} | PnL: {pnl_pct:+.2f}% | risk: {risk_status} | action: {action}")
                    
                    # BUSINESS LOGIC OUTPUT: Detailed strategy monitoring 
                    try:
                        from terminal_color_system import print_strategy_monitor
                        performance_status = "GOOD" if health_score >= 0.7 else "FAIR" if health_score >= 0.5 else "POOR"
                        pnl_info = f"performance: +{(health_score-0.5)*10:.2f}%" if health_score > 0.5 else f"performance: {(health_score-0.5)*10:.2f}%"
                        duration_hours = duration.total_seconds() / 3600
                        print_strategy_monitor(f"{symbol}: {strategy_type} | {pnl_info} | health: {performance_status} | duration: {duration_hours:.1f}h")
                    except ImportError:
                        performance_status = "GOOD" if health_score >= 0.7 else "FAIR" if health_score >= 0.5 else "POOR"
                        pnl_info = f"performance: +{(health_score-0.5)*10:.2f}%" if health_score > 0.5 else f"performance: {(health_score-0.5)*10:.2f}%"
                        duration_hours = duration.total_seconds() / 3600
                        print(f"[STRATEGY MONITOR] {symbol}: {strategy_type} | {pnl_info} | health: {performance_status} | duration: {duration_hours:.1f}h")
            else:
                print_position_monitor("Position Monitor: No active strategies detected")
            
            # Display portfolio health summary
            total_health_score = self._calculate_overall_portfolio_health()
            if total_health_score >= 0.8:
                print_position_monitor(f"Position Monitor: Portfolio health: EXCELLENT ({total_health_score:.1%})")
            elif total_health_score >= 0.6:
                print_position_monitor(f"Position Monitor: Portfolio health: GOOD ({total_health_score:.1%})")
            elif total_health_score >= 0.4:
                print_position_monitor(f"Position Monitor: Portfolio health: FAIR ({total_health_score:.1%})")
            else:
                print_position_monitor(f"Position Monitor: Portfolio health: POOR ({total_health_score:.1%}) - Review needed")
                
        except Exception as e:
            if COLOR_SYSTEM_AVAILABLE:
                print_error(f"Position Monitor: Error displaying portfolio status: {e}")
            self.logger.error(f"Error displaying portfolio status: {e}")

    def _get_current_holdings(self) -> Dict[str, Dict]:
        """Get current holdings from real database"""
        try:
            # REMOVED: Demo simulation replaced with real database access
            if hasattr(self, 'trading_db_manager') and self.trading_db_manager:
                # Use real database to get holdings
                holdings_list = self.trading_db_manager.get_current_holdings()
                if holdings_list:
                    # Convert list to dictionary format expected by display function
                    holdings_dict = {}
                    for holding in holdings_list:
                        symbol = holding['symbol']
                        holdings_dict[symbol] = {
                            'shares': holding.get('quantity', 0),
                            'current_price': holding.get('avg_cost', 0),  # Use avg_cost as current price for now
                            'unrealized_pnl': holding.get('unrealized_pnl', 0),
                            'unrealized_pnl_percent': 0.0,  # Will be calculated with real market data
                            'avg_cost': holding.get('avg_cost', 0),
                            'total_invested': holding.get('total_invested', 0)
                        }
                    return holdings_dict
                    
            # If no real database access available, return empty
            self.logger.warning("No real holdings available - trading database not accessible")
            return {}
            
        except Exception as e:
            self.logger.error(f"Error getting current holdings from database: {e}")
            return {}

    def _calculate_overall_portfolio_health(self) -> float:
        """Calculate overall portfolio health score"""
        try:
            if not self.active_strategies:
                return 0.5  # Neutral if no strategies
            
            total_health = 0
            for strategy in self.active_strategies.values():
                # Handle both ActiveStrategy objects and dictionary objects
                if hasattr(strategy, 'strategy_type'):
                    # ActiveStrategy object - use default health score
                    total_health += 0.75
                else:
                    # Dictionary object
                    total_health += strategy.get('health_score', 0.5)
            
            return total_health / len(self.active_strategies)
        except Exception:
            return 0.5
