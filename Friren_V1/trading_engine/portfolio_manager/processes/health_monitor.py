"""
Health Monitor - Extracted from Position Health Monitor

This module handles core health monitoring orchestration and strategy management.
Provides real-time health assessment with zero hardcoded values and market-driven analysis.

Features:
- Core health monitoring orchestration
- Strategy state management
- Market data integration
- Multi-process coordination
- Real-time health assessment
- Strategy transition management
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
from enum import Enum
import time

# Import Redis infrastructure and components
from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)

# Import analytics components
from ..analytics.position_health_analyzer import (
    OptimizedPositionHealthAnalyzer, ActiveStrategy, StrategyStatus, PositionHealthResult
)
from ..tools.multiprocess_manager import MultiprocessManager, TaskResult

# Import extracted components
from .performance_tracker import PerformanceTracker, StrategyHealthStatus, PerformanceTrend
from .alert_generator import AlertGenerator, Alert, ActionRecommendation, AlertType, AlertPriority

# Import market metrics
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


class HealthCheckType(Enum):
    """Types of health checks"""
    STANDARD = "STANDARD"
    COMPREHENSIVE = "COMPREHENSIVE"
    EMERGENCY = "EMERGENCY"
    STRATEGY_FOCUSED = "STRATEGY_FOCUSED"


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


class HealthMonitor:
    """Core health monitoring orchestrator with extracted components"""
    
    def __init__(self, check_interval: int = 10, risk_threshold: float = 0.05, symbols: list = None):
        self.logger = logging.getLogger(f"{__name__}.HealthMonitor")
        
        # Configuration
        self.check_interval = check_interval
        self.risk_threshold = risk_threshold
        self.symbols = symbols or []
        
        # Initialize extracted components
        self.performance_tracker = PerformanceTracker()
        self.alert_generator = AlertGenerator()
        
        # Core components (initialized in setup)
        self.multiprocess_manager = None
        self.health_analyzer = None
        self.data_fetcher = None
        
        # Strategy monitoring state
        self.strategy_monitoring_states = {}  # {symbol: StrategyMonitoringState}
        self.active_strategies = {}  # {symbol: ActiveStrategy}
        
        # Process state tracking
        self.last_check_time = None
        self.alerts_sent_count = 0
        self.health_checks_count = 0
        
        # Strategy management configuration
        self.reassessment_cooldown_minutes = 15
        self.max_reassessment_requests_per_day = 3
        self.strategy_transition_signals = {}  # {symbol: List[HealthBasedSignal]}
        
        self.logger.info(f"HealthMonitor initialized with extracted components - interval: {check_interval}s")
    
    def setup_components(self):
        """Initialize core components"""
        try:
            # Initialize multiprocess manager
            self.multiprocess_manager = MultiprocessManager(
                max_workers=2,  # Suitable for t3.micro
                max_tasks=50
            )
            self.logger.info("MultiprocessManager initialized")
            
            # Initialize health analyzer
            self.health_analyzer = OptimizedPositionHealthAnalyzer()
            self.logger.info("OptimizedPositionHealthAnalyzer initialized")
            
            # Initialize data fetcher
            try:
                from Friren_V1.trading_engine.data.data_utils import StockDataFetcher
                self.data_fetcher = StockDataFetcher()
                self.logger.info("StockDataFetcher initialized successfully")
            except ImportError as e:
                self.logger.warning(f"StockDataFetcher not available: {e}")
                self.data_fetcher = None
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting up components: {e}")
            return False
    
    def should_run_health_check(self) -> bool:
        """Determine if health check should run based on market-driven timing"""
        try:
            if not self.last_check_time:
                return True
            
            time_since_last = (datetime.now() - self.last_check_time).total_seconds()
            
            # Market-driven check frequency
            base_interval = self.check_interval
            
            # Adjust frequency based on market conditions
            if self.symbols:
                # Get market stress level for primary symbol
                primary_symbol = self.symbols[0] if self.symbols else None
                if primary_symbol:
                    market_metrics = get_all_metrics(primary_symbol)
                    if market_metrics:
                        market_stress = getattr(market_metrics, 'market_stress', 0.3)
                        volatility = getattr(market_metrics, 'volatility', 0.3)
                        
                        # Increase frequency in stressed/volatile markets
                        if market_stress > 0.7 or volatility > 0.6:
                            adjusted_interval = base_interval * 0.7  # 30% more frequent
                        elif market_stress < 0.3 and volatility < 0.2:
                            adjusted_interval = base_interval * 1.3  # 30% less frequent
                        else:
                            adjusted_interval = base_interval
                    else:
                        adjusted_interval = base_interval
                else:
                    adjusted_interval = base_interval
            else:
                adjusted_interval = base_interval
            
            return time_since_last >= adjusted_interval
            
        except Exception as e:
            self.logger.error(f"Error checking health check timing: {e}")
            return True  # Default to running check
    
    def load_active_strategies(self):
        """Load active strategies from portfolio state"""
        try:
            # Get portfolio state from Redis or database
            portfolio_state = self._get_portfolio_state()
            
            if not portfolio_state:
                self.logger.warning("No portfolio state available")
                return
            
            positions = portfolio_state.get('positions', {})
            
            for symbol, position_data in positions.items():
                if position_data.get('quantity', 0) != 0:  # Active position
                    active_strategy = self._create_active_strategy_from_position(symbol, position_data)
                    if active_strategy:
                        self.active_strategies[symbol] = active_strategy
                        
                        # Initialize monitoring state if needed
                        if symbol not in self.strategy_monitoring_states:
                            self.strategy_monitoring_states[symbol] = self._create_default_monitoring_state(symbol, active_strategy.strategy_name)
            
            self.logger.info(f"Loaded {len(self.active_strategies)} active strategies")
            
        except Exception as e:
            self.logger.error(f"Error loading active strategies: {e}")
    
    def _create_active_strategy_from_position(self, symbol: str, position_data: Dict) -> Optional[ActiveStrategy]:
        """Create ActiveStrategy from position data"""
        try:
            strategy_name = position_data.get('strategy_name', 'UNKNOWN')
            
            return ActiveStrategy(
                symbol=symbol,
                strategy_name=strategy_name,
                status=StrategyStatus.ACTIVE,
                entry_time=datetime.fromisoformat(position_data.get('entry_time', datetime.now().isoformat())),
                quantity=position_data.get('quantity', 0),
                entry_price=position_data.get('entry_price', 0.0),
                current_price=position_data.get('current_price', 0.0),
                unrealized_pnl=position_data.get('unrealized_pnl', 0.0),
                last_update=datetime.now(),
                metadata=position_data.get('metadata', {})
            )
            
        except Exception as e:
            self.logger.error(f"Error creating active strategy for {symbol}: {e}")
            return None
    
    def _create_default_monitoring_state(self, symbol: str, strategy_name: str) -> StrategyMonitoringState:
        """Create default monitoring state for a symbol"""
        return StrategyMonitoringState(
            symbol=symbol,
            strategy_name=strategy_name,
            assigned_time=datetime.now()
        )
    
    def fetch_market_data(self) -> Dict[str, Any]:
        """Fetch market data for all active symbols"""
        try:
            market_data_dict = {}
            
            if not self.data_fetcher:
                self.logger.warning("No data fetcher available")
                return market_data_dict
            
            for symbol in self.active_strategies.keys():
                try:
                    # Fetch market data for symbol
                    market_data = self.data_fetcher.get_stock_data(
                        symbol=symbol,
                        period="1d",
                        interval="5m"
                    )
                    
                    if market_data is not None and not market_data.empty:
                        market_data_dict[symbol] = market_data
                    else:
                        self.logger.warning(f"No market data available for {symbol}")
                        
                except Exception as e:
                    self.logger.error(f"Error fetching market data for {symbol}: {e}")
            
            self.logger.debug(f"Fetched market data for {len(market_data_dict)} symbols")
            return market_data_dict
            
        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return {}
    
    def run_parallel_health_analysis(self, market_data_dict: Dict[str, Any], 
                                   strategy_contexts: Dict[str, Dict]) -> List[TaskResult]:
        """Run parallel health analysis using multiprocess manager"""
        try:
            if not self.multiprocess_manager:
                self.logger.error("MultiprocessManager not initialized")
                return []
            
            # Create tasks for parallel execution
            tasks = []
            for symbol, strategy in self.active_strategies.items():
                if symbol in market_data_dict:
                    task = {
                        'symbol': symbol,
                        'strategy': strategy.__dict__,
                        'market_data': market_data_dict[symbol],
                        'analysis_type': 'comprehensive',
                        'strategy_context': strategy_contexts.get(symbol, {})
                    }
                    tasks.append(task)
            
            if not tasks:
                self.logger.warning("No tasks created for health analysis")
                return []
            
            # Submit tasks and get results
            from .position_health_monitor import position_health_worker  # Import the worker function
            task_results = self.multiprocess_manager.process_tasks(
                tasks=tasks,
                worker_function=position_health_worker
            )
            
            self.logger.info(f"Completed {len(task_results)} health analysis tasks")
            return task_results
            
        except Exception as e:
            self.logger.error(f"Error running parallel health analysis: {e}")
            return []
    
    def process_health_results(self, task_results: List[TaskResult]) -> int:
        """Process health analysis results and generate alerts"""
        try:
            alerts_sent = 0
            
            for task_result in task_results:
                if not task_result.success:
                    self.logger.warning(f"Health analysis failed for task: {task_result.error}")
                    continue
                
                # Extract results
                analysis_results = task_result.result
                symbol = analysis_results.get('symbol', 'UNKNOWN')
                
                # Update performance tracking
                strategy_context = analysis_results.get('strategy_context', {})
                performance = self.performance_tracker.extract_strategy_performance(
                    analysis_results, strategy_context
                )
                
                self.performance_tracker.update_performance_history(symbol, performance)
                
                # Generate alerts using alert generator
                alerts = self.alert_generator.generate_strategy_specific_alerts(symbol, performance)
                
                # Send alerts
                for alert in alerts:
                    if self.alert_generator.send_alert_to_queue(alert):
                        alerts_sent += 1
                
                # Update strategy monitoring states
                self._update_strategy_monitoring_state(symbol, performance, analysis_results)
            
            self.logger.info(f"Processed {len(task_results)} health results, sent {alerts_sent} alerts")
            return alerts_sent
            
        except Exception as e:
            self.logger.error(f"Error processing health results: {e}")
            return 0
    
    def _update_strategy_monitoring_state(self, symbol: str, performance: Dict[str, Any], 
                                        analysis_results: Dict[str, Any]):
        """Update strategy monitoring state with latest results"""
        try:
            if symbol not in self.strategy_monitoring_states:
                strategy_name = performance.get('strategy_name', 'UNKNOWN')
                self.strategy_monitoring_states[symbol] = self._create_default_monitoring_state(symbol, strategy_name)
            
            state = self.strategy_monitoring_states[symbol]
            state.last_health_check = datetime.now()
            state.health_status = StrategyHealthStatus(performance.get('health_status', 'NEUTRAL'))
            state.performance_score = performance.get('market_adjusted_score', 50.0)
            
            # Update consecutive poor checks
            if state.health_status in [StrategyHealthStatus.CONCERNING, StrategyHealthStatus.CRITICAL]:
                state.consecutive_poor_checks += 1
            else:
                state.consecutive_poor_checks = 0
            
        except Exception as e:
            self.logger.error(f"Error updating strategy monitoring state for {symbol}: {e}")
    
    def prepare_strategy_contexts(self) -> Dict[str, Dict]:
        """Prepare strategy contexts for health analysis"""
        try:
            contexts = {}
            
            for symbol, strategy in self.active_strategies.items():
                # Get monitoring state
                monitoring_state = self.strategy_monitoring_states.get(symbol)
                
                # Get market metrics
                market_metrics = get_all_metrics(symbol)
                
                # Build context
                context = {
                    'symbol': symbol,
                    'strategy_name': strategy.strategy_name,
                    'strategy_metadata': strategy.metadata,
                    'monitoring_state': monitoring_state.__dict__ if monitoring_state else {},
                    'market_context': {
                        'regime': getattr(market_metrics, 'primary_regime', 'NEUTRAL') if market_metrics else 'NEUTRAL',
                        'volatility': getattr(market_metrics, 'volatility', 0.3) if market_metrics else 0.3,
                        'trend_strength': getattr(market_metrics, 'trend_strength', 0.5) if market_metrics else 0.5
                    },
                    'performance_history': self.performance_tracker.performance_history.get(symbol, []),
                    'risk_threshold': self.risk_threshold
                }
                
                contexts[symbol] = context
            
            return contexts
            
        except Exception as e:
            self.logger.error(f"Error preparing strategy contexts: {e}")
            return {}
    
    def generate_reassessment_requests(self) -> int:
        """Generate strategy reassessment requests based on performance"""
        try:
            requests_sent = 0
            
            for symbol, state in self.strategy_monitoring_states.items():
                if self._should_request_reassessment(state):
                    if self._send_reassessment_request(symbol, state):
                        requests_sent += 1
            
            self.logger.info(f"Sent {requests_sent} reassessment requests")
            return requests_sent
            
        except Exception as e:
            self.logger.error(f"Error generating reassessment requests: {e}")
            return 0
    
    def _should_request_reassessment(self, monitoring_state: StrategyMonitoringState) -> bool:
        """Check if strategy reassessment should be requested"""
        try:
            now = datetime.now()
            
            # Check cooldown
            if monitoring_state.cooldown_until and now < monitoring_state.cooldown_until:
                return False
            
            # Check daily limits
            if monitoring_state.reassessment_requests_sent >= self.max_reassessment_requests_per_day:
                return False
            
            # Check minimum interval since last request
            if (monitoring_state.last_reassessment_request and 
                (now - monitoring_state.last_reassessment_request).total_seconds() < self.reassessment_cooldown_minutes * 60):
                return False
            
            # Trigger conditions
            if (monitoring_state.consecutive_poor_checks >= 3 or 
                monitoring_state.health_status == StrategyHealthStatus.CRITICAL or
                monitoring_state.performance_score < 20.0):
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking reassessment conditions: {e}")
            return False
    
    def _send_reassessment_request(self, symbol: str, monitoring_state: StrategyMonitoringState) -> bool:
        """Send reassessment request to decision engine"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                return False
            
            # Create reassessment request
            request_message = create_process_message(
                message_type="reassessment_request",
                sender_id="position_health_monitor",
                priority=MessagePriority.HIGH,
                data={
                    'symbol': symbol,
                    'strategy_name': monitoring_state.strategy_name,
                    'reason': 'Poor health performance',
                    'consecutive_poor_checks': monitoring_state.consecutive_poor_checks,
                    'health_status': monitoring_state.health_status.value,
                    'performance_score': monitoring_state.performance_score,
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            # Send request
            success = redis_manager.send_message("decision_engine", request_message)
            
            if success:
                # Update state
                monitoring_state.last_reassessment_request = datetime.now()
                monitoring_state.reassessment_requests_sent += 1
                monitoring_state.cooldown_until = datetime.now() + timedelta(minutes=self.reassessment_cooldown_minutes)
                
                self.logger.info(f"Reassessment request sent for {symbol}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending reassessment request for {symbol}: {e}")
            return False
    
    def _get_portfolio_state(self) -> Dict[str, Any]:
        """Get current portfolio state from Redis or database"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                return {}
            
            # Try to get portfolio state from Redis
            portfolio_data = redis_manager.get_shared_data("portfolio_state")
            
            if portfolio_data:
                return portfolio_data
            else:
                # Fallback to creating minimal state
                return {
                    'positions': {},
                    'cash_balance': 100000.0,
                    'total_value': 100000.0,
                    'last_update': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"Error getting portfolio state: {e}")
            return {}
    
    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get comprehensive monitoring summary"""
        try:
            return {
                'active_strategies_count': len(self.active_strategies),
                'monitoring_states_count': len(self.strategy_monitoring_states),
                'last_check_time': self.last_check_time.isoformat() if self.last_check_time else None,
                'health_checks_count': self.health_checks_count,
                'alerts_sent_count': self.alerts_sent_count,
                'performance_summaries': self.performance_tracker.get_all_performance_summaries(),
                'active_alerts': len(self.alert_generator.get_active_alerts()),
                'symbols_monitored': list(self.active_strategies.keys())
            }
            
        except Exception as e:
            self.logger.error(f"Error getting monitoring summary: {e}")
            return {'error': str(e)}
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.multiprocess_manager:
                self.multiprocess_manager.shutdown()
                self.logger.info("MultiprocessManager shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
