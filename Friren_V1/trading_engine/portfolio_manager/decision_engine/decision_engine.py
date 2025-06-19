"""
trading_engine/portfolio_manager/decision_engine/decision_engine.py

Enhanced Market Decision Engine Process - Complete Integration

This integrates all the new decision engine components into your existing
MarketDecisionEngineProcess while maintaining your current architecture.

- SolidRiskManager integration for final veto authority
- ParameterAdapter for adaptive tuning based on performance
- ExecutionOrchestrator for complete execution coordination
- Enhanced signal processing with conflict resolution
- Real-time parameter adaptation every 15 minutes
- Comprehensive execution tracking and metrics
"""

import time
import threading
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import deque, defaultdict
import logging
from enum import Enum
import sys
import os

# NEW: Strategy Management Enums (from implementation_rules.xml)
class MonitoringStrategyStatus(Enum):
    """Status of active monitoring strategies per symbol"""
    ACTIVE = "active"                    # Strategy actively monitoring
    TRANSITIONING = "transitioning"     # In process of changing strategies
    PENDING_CONFIRMATION = "pending_confirmation"  # Awaiting multi-signal confirmation
    FAILED = "failed"                   # Strategy failed, needs reassignment
    EMERGENCY_EXIT = "emergency_exit"   # Emergency exit mode

class TransitionSignalType(Enum):
    """Types of signals that can trigger strategy transitions"""
    REGIME_CHANGE = "regime_change"
    POOR_PERFORMANCE = "poor_performance"
    SENTIMENT_SHIFT = "sentiment_shift"
    VOLATILITY_SPIKE = "volatility_spike"
    HEALTH_ALERT = "health_alert"
    MANUAL_OVERRIDE = "manual_override"

@dataclass
class TransitionSignal:
    """Signal indicating a potential strategy transition"""
    signal_type: TransitionSignalType
    symbol: str
    confidence: float
    timestamp: datetime
    source_process: str
    signal_data: Dict[str, Any]
    requires_immediate_action: bool = False

@dataclass
class RateLimiter:
    """Simple rate limiter for API calls"""
    max_calls_per_minute: int = 200  # Alpaca free tier limit
    calls_made: int = 0
    window_start: datetime = field(default_factory=datetime.now)

    def can_make_call(self) -> bool:
        """Check if we can make an API call within rate limits"""
        now = datetime.now()
        if (now - self.window_start).total_seconds() >= 60:
            # Reset window
            self.calls_made = 0
            self.window_start = now

        return self.calls_made < int(self.max_calls_per_minute * 0.8)  # 80% buffer per rules

    def record_call(self):
        """Record that an API call was made"""
        self.calls_made += 1

# Handle numpy import gracefully
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    # Create minimal numpy stub for basic operations
    class NumpyStub:
        @staticmethod
        def mean(data):
            return sum(data) / len(data) if data else 0.0
        @staticmethod
        def std(data):
            if not data or len(data) < 2:
                return 0.0
            mean_val = sum(data) / len(data)
            variance = sum((x - mean_val) ** 2 for x in data) / len(data)
            return variance ** 0.5
    np = NumpyStub()

# Add project root for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import existing infrastructure
try:
    from multiprocess_infrastructure.base_process import BaseProcess as InfraBaseProcess, ProcessState as InfraProcessState
    from multiprocess_infrastructure.queue_manager import QueueMessage as InfraQueueMessage, MessageType as InfraMessageType, MessagePriority as InfraMessagePriority
    # Use the infrastructure versions
    BaseProcess = InfraBaseProcess
    ProcessState = InfraProcessState
    QueueMessage = InfraQueueMessage
    MessageType = InfraMessageType
    MessagePriority = InfraMessagePriority
    INFRASTRUCTURE_AVAILABLE = True
except ImportError:
    # Fallback stubs for development
    from enum import Enum
    from abc import ABC, abstractmethod
    INFRASTRUCTURE_AVAILABLE = False

    class ProcessState(Enum):
        INITIALIZING = "initializing"
        RUNNING = "running"
        STOPPED = "stopped"
        ERROR = "error"

    class MessageType(Enum):
        STRATEGY_SIGNAL = "strategy_signal"
        SENTIMENT_UPDATE = "sentiment_update"
        HEALTH_ALERT = "health_alert"
        REGIME_CHANGE = "regime_change"

    class MessagePriority(Enum):
        CRITICAL = 1
        HIGH = 2
        NORMAL = 3
        LOW = 4

    @dataclass
    class QueueMessage:
        message_type: MessageType
        priority: MessagePriority
        sender_id: str
        recipient_id: str
        payload: Dict[str, Any]
        timestamp: datetime = field(default_factory=datetime.now)

    class BaseProcess(ABC):
        def __init__(self, process_id: str):
            self.process_id = process_id
            self.state = ProcessState.INITIALIZING
            self.logger = logging.getLogger(f"process.{process_id}")
            self.shared_state = None
            self.priority_queue = None

# Import new decision engine components with better error handling
try:
    from .risk_manager import SolidRiskManager as RealRiskManager, validate_trading_decision
    from .parameter_adapter import ParameterAdapter as RealParameterAdapter, AdaptationLevel, create_adaptation_metrics, get_system_metrics
    from .execution_orchestrator import ExecutionOrchestrator as RealExecutionOrchestrator, create_execution_orchestrator
    ENHANCED_COMPONENTS_AVAILABLE = True

    # Try conflict resolver separately since it has complex dependencies
    try:
        from .conflict_resolver import ConflictResolver as RealConflictResolver
        CONFLICT_RESOLVER_AVAILABLE = True
    except ImportError:
        CONFLICT_RESOLVER_AVAILABLE = False
        RealConflictResolver = None

except ImportError as e:
    ENHANCED_COMPONENTS_AVAILABLE = False
    CONFLICT_RESOLVER_AVAILABLE = False
    logging.warning(f"Enhanced decision engine components not available: {e}")
    RealRiskManager = None
    RealParameterAdapter = None
    RealExecutionOrchestrator = None
    RealConflictResolver = None

# Define fallback classes with proper method signatures
class FallbackRiskManager:
    def __init__(self, position_sizer=None, db_manager=None, alpaca_interface=None):
        self.limits = {'max_position_size_pct': 0.1, 'max_daily_trades': 50}

    def validate_decision(self, decision):
        return type('RiskValidation', (), {
            'is_approved': True,
            'should_execute': True,
            'reason': 'Fallback approval',
            'symbol': getattr(decision, 'symbol', 'UNKNOWN'),
            'original_decision': decision
        })()

    def get_risk_summary(self):
        return {'status': 'fallback', 'active_alerts': 0}

    def resume_trading(self):
        pass

    def emergency_halt_trading(self, reason):
        pass

class FallbackParameterAdapter:
    def __init__(self, level):
        pass

    def adapt_parameters(self, metrics):
        return type('AdaptedParams', (), {
            'signal_weights': {},
            'adaptation_reason': 'No adaptation - fallback mode',
            'max_position_size': 0.1,
            'max_daily_trades': 50
        })()

    def get_adaptation_status(self):
        return {'current_weights': {}, 'status': 'fallback'}

    def force_parameter_reset(self):
        pass

class FallbackExecutionOrchestrator:
    def __init__(self):
        self.active_executions = {}

    def execute_approved_decision(self, validation, **kwargs):
        return type('ExecutionResult', (), {
            'was_successful': True,
            'symbol': getattr(validation, 'symbol', 'TEST'),
            'execution_summary': 'Fallback execution',
            'error_message': None,
            'execution_slippage': 0.001,
            'executed_amount': 100.0
        })()

    def get_execution_status(self):
        return {'execution_stats': {'total_executions': 0}}

class FallbackConflictResolver:
    def resolve_conflict(self, signal):
        return type('ResolvedDecision', (), {
            'symbol': getattr(signal, 'symbol', 'UNKNOWN'),
            'final_direction': getattr(signal, 'final_direction', 0.0),
            'final_confidence': getattr(signal, 'confidence', 50.0) / 100.0,
            'resolution_method': 'simple_fallback'
        })()

# Select which implementations to use
SolidRiskManager = RealRiskManager if ENHANCED_COMPONENTS_AVAILABLE else FallbackRiskManager
ParameterAdapter = RealParameterAdapter if ENHANCED_COMPONENTS_AVAILABLE else FallbackParameterAdapter
ExecutionOrchestrator = RealExecutionOrchestrator if ENHANCED_COMPONENTS_AVAILABLE else FallbackExecutionOrchestrator
ConflictResolver = RealConflictResolver if CONFLICT_RESOLVER_AVAILABLE else FallbackConflictResolver


class DecisionType(Enum):
    """Types of trading decisions"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    CLOSE = "close"
    FORCE_CLOSE = "force_close"


class SignalWeight(Enum):
    """Signal weighting categories - now adaptive"""
    TECHNICAL = 0.30    # 30% - from strategy signals
    MARKET = 0.30       # 30% - from regime changes
    SENTIMENT = 0.30    # 30% - from news sentiment
    RISK = 0.10         # 10% - from risk management


@dataclass
class AggregatedSignal:
    """Enhanced aggregated trading signal"""
    symbol: str
    decision_type: DecisionType
    confidence: float  # 0-100
    signal_components: Dict[str, float]  # breakdown by source
    risk_score: float  # 0-100, higher = riskier
    timestamp: datetime = field(default_factory=datetime.now)

    # Enhanced signal source details
    technical_signals: List[Dict] = field(default_factory=list)
    sentiment_score: Optional[float] = None
    market_regime: Optional[str] = None
    risk_alerts: List[str] = field(default_factory=list)

    # New: Conflict analysis
    signal_agreement: float = 0.0  # How much signals agree
    uncertainty: float = 0.0       # Signal uncertainty level
    final_direction: float = 0.0   # -1 to 1 direction
    final_strength: float = 0.0    # 0 to 1 strength


@dataclass
class DecisionMetrics:
    """Enhanced decision tracking metrics"""
    decisions_made: int = 0
    successful_decisions: int = 0
    failed_decisions: int = 0
    average_confidence: float = 0.0
    risk_vetoes: int = 0
    force_closes: int = 0
    processing_time_ms: float = 0.0

    # New: Enhanced metrics
    execution_success_rate: float = 0.0
    parameter_adaptations: int = 0
    avg_slippage_pct: float = 0.0
    total_volume_executed: float = 0.0


@dataclass
class PerformanceTracker:
    """Track performance for parameter adaptation"""
    recent_decisions: deque = field(default_factory=lambda: deque(maxlen=50))
    strategy_performance: Dict[str, deque] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=20)))
    execution_results: deque = field(default_factory=lambda: deque(maxlen=30))
    last_adaptation: datetime = field(default_factory=datetime.now)

    def add_decision_result(self, symbol: str, strategy: str, was_successful: bool, confidence: float):
        """Add decision result for tracking"""
        self.recent_decisions.append({
            'symbol': symbol,
            'strategy': strategy,
            'success': was_successful,
            'confidence': confidence,
            'timestamp': datetime.now()
        })
        self.strategy_performance[strategy].append(was_successful)

    def get_recent_performance(self) -> float:
        """Get recent success rate"""
        if not self.recent_decisions:
            return 0.5
        return sum(d['success'] for d in self.recent_decisions) / len(self.recent_decisions)

    def get_strategy_performance(self) -> Dict[str, float]:
        """Get performance by strategy"""
        return {
            strategy: sum(results) / len(results) if results else 0.5
            for strategy, results in self.strategy_performance.items()
        }


class EnhancedMarketDecisionEngineProcess(BaseProcess):
    """
    Enhanced Market Decision Engine Process

    **What's New:**
    - Complete risk management with final veto authority
    - Real-time parameter adaptation based on performance
    - Complete execution orchestration with tool integration
    - Enhanced conflict resolution for signal disagreements
    - Comprehensive performance tracking and metrics

    **Integration Philosophy:**
    - Maintains your existing architecture and interfaces
    - Adds new capabilities without breaking existing code
    - Graceful fallbacks when enhanced components unavailable
    - Clear separation between core logic and enhancements
    """

    def __init__(self):
        super().__init__("market_decision_engine")

        # Your existing signal aggregation (unchanged)
        self.signal_buffer = defaultdict(lambda: deque(maxlen=50))
        self.sentiment_cache = {}
        self.regime_state = {"current": "normal", "confidence": 0.5, "updated": datetime.now()}

        # Enhanced adaptive signal weights
        self.signal_weights = {
            "technical": SignalWeight.TECHNICAL.value,
            "market": SignalWeight.MARKET.value,
            "sentiment": SignalWeight.SENTIMENT.value,
            "risk": SignalWeight.RISK.value
        }

        # Enhanced metrics and tracking
        self.metrics = DecisionMetrics()
        self.recent_decisions = deque(maxlen=100)
        self.performance_window = deque(maxlen=50)

        # NEW: Performance tracking for adaptation
        self.performance_tracker = PerformanceTracker()

        # Your existing risk management state (enhanced)
        self.risk_alerts_active = set()
        self.force_close_symbols = set()

        # Your existing tool integration (will be enhanced)
        self.strategy_selector = None
        self.position_sizer = None
        self.db_manager = None

        # NEW: Enhanced decision engine components
        self.risk_manager = None
        self.parameter_adapter = None
        self.execution_orchestrator = None
        self.conflict_resolver = None

        # NEW: Strategy Management State (from implementation_rules.xml)
        self.active_monitoring_strategies: Dict[str, MonitoringStrategyStatus] = {}
        self.strategy_transition_signals: Dict[str, List[TransitionSignal]] = defaultdict(list)
        self.api_rate_limiter = RateLimiter()

        # Strategy transition tracking
        self.strategy_transition_counts: Dict[str, int] = defaultdict(int)  # Daily transition count per symbol
        self.last_transition_time: Dict[str, datetime] = {}  # Last transition time per symbol
        self.transition_cooldown_minutes = 15  # From implementation rules

        # Enhanced performance optimization
        self.last_weight_adjustment = datetime.now()
        self.weight_adjustment_interval = timedelta(minutes=15)

        # NEW: Execution tracking
        self.daily_execution_count = 0
        self.last_execution_reset = datetime.now().date()

        self.logger.info("Enhanced Market Decision Engine initialized")
        if not ENHANCED_COMPONENTS_AVAILABLE:
            self.logger.warning(" Running with fallback components - some features limited")
            self.logger.warning(" Enhanced decision engine components not available. Consider installing missing dependencies.")
        if not CONFLICT_RESOLVER_AVAILABLE:
            self.logger.warning(" Conflict resolver using simple fallback - XGBoost features unavailable")
        if not NUMPY_AVAILABLE:
            self.logger.warning(" NumPy not available - using basic statistical functions")
        if not INFRASTRUCTURE_AVAILABLE:
            self.logger.warning(" Multiprocess infrastructure not available - using fallback stubs")

    def _initialize(self):
        """Enhanced initialization with new components"""
        try:
            self.logger.info("Initializing Enhanced Market Decision Engine...")

            # Initialize your existing tools
            self._initialize_tool_connections()

            # NEW: Initialize enhanced decision engine components
            self._initialize_enhanced_components()

            # Load persisted state
            self._load_persisted_state()

            # Setup enhanced performance tracking
            self._setup_enhanced_performance_tracking()

            self.state = ProcessState.RUNNING
            self.logger.info("Enhanced Market Decision Engine initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize enhanced decision engine: {e}")
            self.state = ProcessState.ERROR
            raise

    def _initialize_enhanced_components(self):
        """Initialize the new decision engine components"""
        try:
            if ENHANCED_COMPONENTS_AVAILABLE and RealRiskManager:
                # Initialize risk manager with existing tools
                self.risk_manager = RealRiskManager(
                    position_sizer=self.position_sizer,
                    db_manager=self.db_manager,
                    alpaca_interface=getattr(self, 'alpaca_interface', None)
                )

                # Initialize parameter adapter
                self.parameter_adapter = RealParameterAdapter(AdaptationLevel.MODERATE)

                # Initialize execution orchestrator
                self.execution_orchestrator = create_execution_orchestrator(
                    risk_manager=self.risk_manager,
                    position_sizer=self.position_sizer,
                    db_manager=self.db_manager,
                    alpaca_interface=getattr(self, 'alpaca_interface', None)
                )

                self.logger.info(" Enhanced components initialized successfully")
            else:
                # Use fallback components
                self.risk_manager = FallbackRiskManager()
                self.parameter_adapter = FallbackParameterAdapter(None)
                self.execution_orchestrator = FallbackExecutionOrchestrator()

                self.logger.warning(" Using fallback components")

            # Initialize conflict resolver (separate check)
            if CONFLICT_RESOLVER_AVAILABLE and RealConflictResolver:
                self.conflict_resolver = RealConflictResolver()
                self.logger.info(" Conflict resolver initialized with XGBoost support")
            else:
                self.conflict_resolver = FallbackConflictResolver()
                self.logger.warning(" Conflict resolver using simple fallback")

        except Exception as e:
            self.logger.error(f"Error initializing enhanced components: {e}")
            # Create minimal fallbacks
            self.risk_manager = FallbackRiskManager()
            self.parameter_adapter = FallbackParameterAdapter(None)
            self.execution_orchestrator = FallbackExecutionOrchestrator()
            self.conflict_resolver = FallbackConflictResolver()

    def _initialize_tool_connections(self):
        """Initialize connections to existing trading tools"""
        # TODO: These will be injected by the orchestrator in real system
        self.logger.info("Connecting to existing trading tools...")

        # Your existing tool connections would go here
        # self.strategy_selector = StrategySelector()
        # self.position_sizer = PositionSizer()
        # self.db_manager = TradingDBManager()

        self.logger.info("Tool connections established")

    def _process_cycle(self):
        """Enhanced processing cycle with new pipeline"""
        try:
            messages_processed = 0
            cycle_start = time.time()

            # Reset daily execution count if new day
            self._reset_daily_counters()

            # Process all available messages aggressively (your existing logic)
            while True:
                message = self._get_next_message(timeout=0.1)
                if message is None:
                    break

                self._process_message(message)
                messages_processed += 1

                # Safety check for processing time
                if time.time() - cycle_start > 2.0:
                    self.logger.warning(f"Processing cycle taking too long, processed {messages_processed} messages")
                    break

            # After processing messages, make enhanced decisions
            if messages_processed > 0:
                self._make_enhanced_trading_decisions()

            # NEW: Periodic parameter adaptation
            self._maybe_adapt_parameters()

            # Periodic maintenance
            self._periodic_maintenance()

            # Update metrics
            cycle_time = (time.time() - cycle_start) * 1000
            self.metrics.processing_time_ms = cycle_time

            if messages_processed > 0:
                self.logger.debug(f"Enhanced cycle: {messages_processed} messages in {cycle_time:.1f}ms")

        except Exception as e:
            self.logger.error(f"Error in enhanced process cycle: {e}")
            self.error_count += 1

    def _process_message(self, message: QueueMessage):
        """Enhanced message processing (keeps your existing logic)"""
        try:
            if message.message_type == MessageType.STRATEGY_SIGNAL:
                self._handle_strategy_signal(message)
            elif message.message_type == MessageType.SENTIMENT_UPDATE:
                self._handle_sentiment_update(message)
            elif message.message_type == MessageType.HEALTH_ALERT:
                self._handle_health_alert(message)
            elif message.message_type == MessageType.REGIME_CHANGE:
                self._handle_regime_change(message)

            # NEW: Strategy Management Message Handling (from implementation_rules.xml)
            elif hasattr(MessageType, 'STRATEGY_REASSESSMENT_REQUEST') and message.message_type == MessageType.STRATEGY_REASSESSMENT_REQUEST:
                self._handle_strategy_reassessment_request(message)
            elif hasattr(MessageType, 'SENTIMENT_UPDATE') and message.message_type == MessageType.SENTIMENT_UPDATE:
                self._handle_sentiment_update(message)
            else:
                self.logger.warning(f"Unknown message type: {message.message_type}")

        except Exception as e:
            self.logger.error(f"Error processing {message.message_type}: {e}")

    def _handle_strategy_signal(self, message: QueueMessage):
        """Enhanced strategy signal handling"""
        payload = message.payload
        symbol = payload.get('symbol')
        signal_data = payload.get('signal', {})

        if not symbol:
            self.logger.warning("Strategy signal missing symbol")
            return

        # Add to signal buffer (your existing logic)
        signal_entry = {
            'timestamp': message.timestamp,
            'sender': message.sender_id,
            'signal': signal_data,
            'confidence': signal_data.get('confidence', 0),
            'strategy_name': payload.get('strategy_name', 'unknown')  # NEW: track strategy
        }

        self.signal_buffer[symbol].append(signal_entry)

        self.logger.debug(f"Added strategy signal for {symbol}, confidence: {signal_data.get('confidence', 0)}")

    def _handle_sentiment_update(self, message: QueueMessage):
        """Enhanced sentiment update handling"""
        payload = message.payload
        symbol = payload.get('symbol')
        sentiment_data = payload.get('sentiment', {})

        if not symbol:
            self.logger.warning("Sentiment update missing symbol")
            return

        # Update sentiment cache with enhanced data
        self.sentiment_cache[symbol] = {
            'score': sentiment_data.get('sentiment_score', 0),
            'confidence': sentiment_data.get('confidence', 0),
            'timestamp': message.timestamp,
            'source': message.sender_id,
            'article_count': sentiment_data.get('article_count', 0)  # NEW: track article count
        }

        self.logger.debug(f"Updated sentiment for {symbol}: {sentiment_data.get('sentiment_score', 0)}")

    def _handle_health_alert(self, message: QueueMessage):
        """Enhanced health alert handling"""
        payload = message.payload
        alert_type = payload.get('alert_type')
        symbol = payload.get('symbol')

        if alert_type == 'force_close':
            self.force_close_symbols.add(symbol)
            self.logger.warning(f" Force close alert for {symbol}")

            # NEW: Immediate force close execution
            if symbol:
                self._execute_force_close(symbol, payload.get('reason', 'Health alert'))

        elif alert_type == 'risk_warning':
            self.risk_alerts_active.add(symbol)
            self.logger.warning(f" Risk alert for {symbol}: {payload.get('message', '')}")

    def _handle_regime_change(self, message: QueueMessage):
        """Enhanced regime change handling"""
        payload = message.payload
        new_regime = payload.get('regime_type')
        confidence = payload.get('confidence', 0.5)

        if new_regime:
            old_regime = self.regime_state.get('current')
            self.regime_state = {
                'current': new_regime,
                'confidence': confidence,
                'updated': message.timestamp,
                'volatility': payload.get('volatility', 'normal'),  # NEW: track volatility
                'trend': payload.get('trend', 'sideways')           # NEW: track trend
            }

            self.logger.info(f" Regime change: {old_regime} → {new_regime} (confidence: {confidence:.2f})")

    def _make_enhanced_trading_decisions(self):
        """Enhanced decision making with complete pipeline"""
        try:
            # Get symbols with sufficient signals
            ready_symbols = self._get_symbols_ready_for_decision()

            for symbol in ready_symbols:
                try:
                    # Step 1: Aggregate signals (enhanced version of your existing logic)
                    aggregated_signal = self._create_enhanced_aggregated_signal(symbol)
                    if not aggregated_signal:
                        continue

                    # Step 2: NEW - Resolve conflicts using conflict resolver
                    resolved_decision = self._resolve_signal_conflicts(aggregated_signal)
                    if not resolved_decision:
                        continue

                    # Step 3: NEW - Risk validation with enhanced risk manager
                    risk_validation = self._validate_decision_risk(resolved_decision)
                    if not risk_validation.is_approved:
                        self.metrics.risk_vetoes += 1
                        self.logger.info(f" Risk denied: {symbol} - {risk_validation.reason}")
                        continue

                    # Step 4: NEW - Execute using execution orchestrator
                    execution_result = self._execute_validated_decision(
                        risk_validation,
                        strategy_name=self._get_dominant_strategy(symbol)
                    )

                    # Step 5: Track performance for adaptation
                    self._track_decision_performance(symbol, execution_result, resolved_decision)

                    # NEW: Step 6: Assign monitoring strategy immediately after successful execution (from implementation_rules.xml)
                    if hasattr(execution_result, 'was_successful') and execution_result.was_successful:
                        strategy_name = self._get_dominant_strategy(symbol)
                        self._assign_monitoring_strategy(symbol, strategy_name, {
                            'execution_result': execution_result.__dict__ if hasattr(execution_result, '__dict__') else str(execution_result),
                            'decision_confidence': getattr(resolved_decision, 'final_confidence', 0.5),
                            'execution_time': datetime.now().isoformat()
                        })

                    # Update metrics
                    self.metrics.decisions_made += 1
                    if execution_result.was_successful:
                        self.metrics.successful_decisions += 1
                    else:
                        self.metrics.failed_decisions += 1

                except Exception as e:
                    self.logger.error(f"Error making decision for {symbol}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Error in enhanced decision making: {e}")

    def _create_enhanced_aggregated_signal(self, symbol: str) -> Optional[AggregatedSignal]:
        """Create enhanced aggregated signal with conflict analysis and position-aware logic"""
        try:
            signals = list(self.signal_buffer[symbol])
            if not signals:
                return None

            # Get sentiment data
            sentiment = self.sentiment_cache.get(symbol, {})

            # Calculate signal components with adaptive weights
            technical_component = self._calculate_technical_component(signals)
            sentiment_component = sentiment.get('score', 0) * self.signal_weights['sentiment']
            market_component = self._calculate_market_component()
            risk_component = self._calculate_risk_component(symbol)

            # Calculate aggregated values
            final_direction = (
                technical_component * self.signal_weights['technical'] +
                sentiment_component * self.signal_weights['sentiment'] +
                market_component * self.signal_weights['market'] +
                risk_component * self.signal_weights['risk']
            )

            # Calculate signal agreement and uncertainty
            signal_agreement = self._calculate_signal_agreement(signals, sentiment)
            uncertainty = self._calculate_signal_uncertainty(signals, sentiment)

            # Determine base decision type
            if final_direction > 0.3:
                base_decision_type = DecisionType.BUY
            elif final_direction < -0.3:
                base_decision_type = DecisionType.SELL
            else:
                base_decision_type = DecisionType.HOLD

            # Calculate overall confidence
            base_confidence = np.mean([s['confidence'] for s in signals]) if signals else 0
            confidence = base_confidence * (1 - uncertainty) * signal_agreement

            # Apply position-aware decision logic
            final_decision_type = self._apply_position_aware_logic(
                symbol, base_decision_type, sentiment.get('score', 0), confidence
            )

            return AggregatedSignal(
                symbol=symbol,
                decision_type=final_decision_type,
                confidence=confidence * 100,  # Convert to 0-100 scale
                signal_components={
                    'technical': technical_component,
                    'sentiment': sentiment_component,
                    'market': market_component,
                    'risk': risk_component
                },
                risk_score=self._calculate_risk_score(symbol),
                technical_signals=[s['signal'] for s in signals],
                sentiment_score=sentiment.get('score'),
                market_regime=self.regime_state.get('current'),
                risk_alerts=list(self.risk_alerts_active) if symbol in self.risk_alerts_active else [],
                signal_agreement=signal_agreement,
                uncertainty=uncertainty,
                final_direction=final_direction,
                final_strength=abs(final_direction)
            )

        except Exception as e:
            self.logger.error(f"Error creating aggregated signal for {symbol}: {e}")
            return None

    def _resolve_signal_conflicts(self, aggregated_signal: AggregatedSignal):
        """NEW: Resolve signal conflicts using conflict resolver"""
        try:
            if self.conflict_resolver:
                return self.conflict_resolver.resolve_conflict(aggregated_signal)
            else:
                # Fallback: return signal as-is
                return type('ResolvedDecision', (), {
                    'symbol': aggregated_signal.symbol,
                    'final_direction': aggregated_signal.final_direction,
                    'final_confidence': aggregated_signal.confidence / 100,
                    'resolution_method': 'fallback'
                })()
        except Exception as e:
            self.logger.error(f"Error resolving conflicts for {aggregated_signal.symbol}: {e}")
            return None

    def _validate_decision_risk(self, resolved_decision):
        """NEW: Validate decision with enhanced risk manager"""
        try:
            if self.risk_manager:
                return self.risk_manager.validate_decision(resolved_decision)
            else:
                # Fallback: approve all decisions
                return type('RiskValidation', (), {
                    'is_approved': True,
                    'should_execute': True,
                    'reason': 'Fallback approval'
                })()
        except Exception as e:
            self.logger.error(f"Error validating risk: {e}")
            return type('RiskValidation', (), {'is_approved': False, 'reason': f'Validation error: {e}'})()

    def _execute_validated_decision(self, risk_validation, strategy_name: str):
        """NEW: Execute using execution orchestrator"""
        try:
            if self.execution_orchestrator and risk_validation.should_execute:
                result = self.execution_orchestrator.execute_approved_decision(
                    risk_validation=risk_validation,
                    strategy_name=strategy_name,
                    strategy_confidence=getattr(risk_validation.original_decision, 'final_confidence', 0.5)
                )

                self.daily_execution_count += 1

                if result.was_successful:
                    self.logger.info(f" {result.execution_summary}")
                else:
                    self.logger.warning(f" Execution failed: {result.error_message}")

                return result
            else:
                # Fallback: simulate execution
                return type('ExecutionResult', (), {
                    'was_successful': True,
                    'execution_summary': f"Simulated execution for {risk_validation.symbol}",
                    'symbol': risk_validation.symbol
                })()

        except Exception as e:
            self.logger.error(f"Error executing decision: {e}")
            return type('ExecutionResult', (), {
                'was_successful': False,
                'error_message': str(e),
                'symbol': getattr(risk_validation, 'symbol', 'unknown')
            })()

    def _execute_force_close(self, symbol: str, reason: str):
        """NEW: Execute force close using execution orchestrator"""
        try:
            self.logger.warning(f" Force closing {symbol}: {reason}")

            if self.execution_orchestrator:
                # Create force close execution request
                # This would be implemented based on your execution orchestrator interface
                pass

            # Remove from active tracking
            self.force_close_symbols.discard(symbol)

        except Exception as e:
            self.logger.error(f"Error force closing {symbol}: {e}")

    def _maybe_adapt_parameters(self):
        """NEW: Adapt parameters based on performance"""
        # Adapt every 15 minutes
        if (datetime.now() - self.performance_tracker.last_adaptation).total_seconds() < 900:
            return

        if len(self.performance_tracker.recent_decisions) < 10:
            return  # Need enough data

        # Get performance metrics
        overall_performance = self.performance_tracker.get_recent_performance()
        strategy_performance = self.performance_tracker.get_strategy_performance()
        system_metrics = get_system_metrics() if ENHANCED_COMPONENTS_AVAILABLE else {}

        # Create adaptation metrics
        adaptation_metrics = create_adaptation_metrics(
            performance_data={'overall': overall_performance, **strategy_performance},
            market_data={
                'regime': self.regime_state.get('current', 'sideways'),
                'volatility_percentile': 0.5,
                'trend_strength': 0.5
            },
            system_data=system_metrics,
            risk_data={
                'portfolio_stress': len(self.risk_alerts_active) / 10.0,  # Normalize
                'drawdown': 0.05  # Placeholder
            }
        ) if ENHANCED_COMPONENTS_AVAILABLE else None

        if adaptation_metrics and self.parameter_adapter:
            # Adapt parameters
            adapted_params = self.parameter_adapter.adapt_parameters(adaptation_metrics)

            # Apply adapted signal weights
            if hasattr(adapted_params, 'signal_weights') and adapted_params.signal_weights:
                old_weights = self.signal_weights.copy()
                self.signal_weights.update(adapted_params.signal_weights)

                self.logger.info(f" Weights adapted: {adapted_params.adaptation_reason}")
                self.logger.debug(f"Old: {old_weights}")
                self.logger.debug(f"New: {self.signal_weights}")

            # Apply risk parameter changes to risk manager
            if self.risk_manager and hasattr(adapted_params, 'max_position_size'):
                self.risk_manager.limits['max_position_size_pct'] = adapted_params.max_position_size
                if hasattr(adapted_params, 'max_daily_trades'):
                    self.risk_manager.limits['max_daily_trades'] = adapted_params.max_daily_trades

            self.metrics.parameter_adaptations += 1
            self.performance_tracker.last_adaptation = datetime.now()

    # NEW: Strategy Management Methods (from implementation_rules.xml)

    def _assign_monitoring_strategy(self, symbol: str, entry_strategy: str, execution_result: Dict[str, Any]):
        """Assign monitoring strategy immediately after successful execution"""
        try:
            self.logger.info(f" Assigning monitoring strategy for {symbol}: {entry_strategy}")

            # Set strategy as active
            self.active_monitoring_strategies[symbol] = MonitoringStrategyStatus.ACTIVE

            # Send assignment message to position health monitor
            if hasattr(self, 'queue_manager') and self.queue_manager:
                self.queue_manager.send_strategy_assignment(
                    sender_id=self.process_id,
                    symbol=symbol,
                    strategy_name=entry_strategy,
                    execution_result=execution_result
                )

            # Log for audit trail (per implementation rules)
            self.logger.info(f" Strategy assignment complete - {symbol}: {entry_strategy}")

        except Exception as e:
            self.logger.error(f" Error assigning monitoring strategy for {symbol}: {e}")

    def _handle_strategy_reassessment_request(self, message: QueueMessage):
        """Handle strategy reassessment request from position health monitor"""
        try:
            payload = message.payload
            symbol = payload.get('symbol')
            current_strategy = payload.get('current_strategy')
            reason = payload.get('reassessment_reason')
            performance_data = payload.get('performance_data', {})

            self.logger.info(f" Strategy reassessment request for {symbol}: {reason}")

            # Evaluate if strategy transition is needed
            self._evaluate_strategy_transition(symbol, [{
                'type': reason,
                'data': performance_data,
                'source': message.sender_id,
                'confidence': performance_data.get('confidence', 0.5)
            }])

        except Exception as e:
            self.logger.error(f" Error handling strategy reassessment request: {e}")

    def _evaluate_strategy_transition(self, symbol: str, signals: List[Dict]):
        """Evaluate whether strategy transition is warranted based on multiple signals"""
        try:
            # Check cooldown period (15 minutes per implementation rules)
            if symbol in self.last_transition_time:
                time_since_last = datetime.now() - self.last_transition_time[symbol]
                if time_since_last.total_seconds() < (self.transition_cooldown_minutes * 60):
                    self.logger.info(f"⏳ Strategy transition for {symbol} blocked by cooldown")
                    return

            # Check daily transition limits (max 3 per symbol per rules)
            if self.strategy_transition_counts[symbol] >= 3:
                self.logger.warning(f" Daily transition limit reached for {symbol}")
                return

            # Add signals to buffer for multi-signal analysis
            for signal_data in signals:
                transition_signal = TransitionSignal(
                    signal_type=TransitionSignalType(signal_data.get('type', 'poor_performance')),
                    symbol=symbol,
                    confidence=signal_data.get('confidence', 0.5),
                    timestamp=datetime.now(),
                    source_process=signal_data.get('source', 'unknown'),
                    signal_data=signal_data.get('data', {}),
                    requires_immediate_action=signal_data.get('immediate', False)
                )
                self.strategy_transition_signals[symbol].append(transition_signal)

            # Clean old signals (15-minute window per rules)
            cutoff_time = datetime.now() - timedelta(minutes=15)
            self.strategy_transition_signals[symbol] = [
                s for s in self.strategy_transition_signals[symbol]
                if s.timestamp > cutoff_time
            ]

            # NEW: Use enhanced conflict resolver for sophisticated multi-signal analysis
            if (self.conflict_resolver and
                hasattr(self.conflict_resolver, 'resolve_strategy_transition_signals') and
                len(recent_signals) >= 1):

                # Convert TransitionSignal objects to dict format for conflict resolver
                signals_dict = []
                for ts in recent_signals:
                    signals_dict.append({
                        'type': ts.signal_type.value,
                        'confidence': ts.confidence,
                        'timestamp': ts.timestamp,
                        'source': ts.source_process,
                        'data': ts.signal_data,
                        'immediate': ts.requires_immediate_action
                    })

                current_strategy = self._get_current_monitoring_strategy(symbol)
                current_performance = self._get_strategy_performance(symbol)

                # Get enhanced decision from conflict resolver
                transition_decision = self.conflict_resolver.resolve_strategy_transition_signals(
                    symbol=symbol,
                    transition_signals=signals_dict,
                    current_strategy=current_strategy,
                    current_performance=current_performance
                )

                # Act on the enhanced decision
                if transition_decision.should_transition:
                    self._execute_strategy_transition_enhanced(symbol, transition_decision)
                elif transition_decision.wait_for_more_signals:
                    self.logger.info(f"⏳ {symbol} transition pending: {transition_decision.transition_reasoning}")
                else:
                    self.logger.info(f" {symbol} strategy continuity: {transition_decision.transition_reasoning}")
                    # Clear signals since decision was made to not transition
                    self.strategy_transition_signals[symbol].clear()
            else:
                # Fallback to legacy logic if enhanced conflict resolver unavailable
                if len(recent_signals) >= 2:
                    combined_confidence = sum(s.confidence for s in recent_signals) / len(recent_signals)

                    # Proceed with transition if confidence threshold met (>0.75 per rules)
                    if combined_confidence > 0.75:
                        self._execute_strategy_transition(symbol, recent_signals)
                    else:
                        self.logger.info(f" Insufficient confidence for {symbol} transition: {combined_confidence:.2f}")
                else:
                    self.logger.info(f" Awaiting additional signals for {symbol} transition ({len(recent_signals)}/2)")

        except Exception as e:
            self.logger.error(f" Error evaluating strategy transition for {symbol}: {e}")

    def _execute_strategy_transition(self, symbol: str, confirming_signals: List[TransitionSignal]):
        """Execute strategy transition after multi-signal confirmation (legacy method)"""
        try:
            current_strategy = self._get_current_monitoring_strategy(symbol)

            # Use strategy selector to choose new strategy
            if hasattr(self, 'strategy_selector') and self.strategy_selector:
                # Get market context for strategy selection
                market_context = {
                    'regime': self.regime_state.get('current', 'normal'),
                    'volatility': self._calculate_market_volatility(),
                    'sentiment': self.sentiment_cache.get(symbol, {}).get('score', 0),
                    'signals': [s.signal_data for s in confirming_signals]
                }

                new_strategy = self.strategy_selector.select_best_strategy(symbol, market_context)

                if new_strategy and new_strategy != current_strategy:
                    self._broadcast_strategy_change(symbol, current_strategy, new_strategy)

                    # Update state
                    self.active_monitoring_strategies[symbol] = MonitoringStrategyStatus.TRANSITIONING
                    self.strategy_transition_counts[symbol] += 1
                    self.last_transition_time[symbol] = datetime.now()

                    self.logger.info(f" Strategy transition executed: {symbol} {current_strategy} → {new_strategy}")
                else:
                    self.logger.info(f" Strategy assessment complete: {symbol} keeping {current_strategy}")

        except Exception as e:
            self.logger.error(f" Error executing strategy transition for {symbol}: {e}")

    def _execute_strategy_transition_enhanced(self, symbol: str, transition_decision):
        """
        NEW: Execute strategy transition using enhanced conflict resolver decision

        This integrates the Phase 3 conflict resolution with strategy execution
        """
        try:
            current_strategy = self._get_current_monitoring_strategy(symbol)

            # Get recommended strategy from conflict resolver or use selector
            if transition_decision.recommended_strategy:
                new_strategy = transition_decision.recommended_strategy
            elif hasattr(self, 'strategy_selector') and self.strategy_selector:
                # Fallback to strategy selector if no specific recommendation
                market_context = {
                    'regime': self.regime_state.get('current', 'normal'),
                    'volatility': self._calculate_market_volatility(),
                    'sentiment': self.sentiment_cache.get(symbol, {}).get('score', 0),
                    'transition_reason': transition_decision.transition_reasoning
                }
                new_strategy = self.strategy_selector.select_best_strategy(symbol, market_context)
            else:
                self.logger.warning(f"No strategy recommendation available for {symbol}, keeping current")
                return

            if new_strategy and new_strategy != current_strategy:
                # Enhanced broadcasting with conflict resolver context
                self._broadcast_strategy_change_enhanced(symbol, current_strategy, new_strategy, transition_decision)

                # Update state with enhanced tracking
                self.active_monitoring_strategies[symbol] = MonitoringStrategyStatus.TRANSITIONING
                self.strategy_transition_counts[symbol] += 1
                self.last_transition_time[symbol] = datetime.now()

                # Log enhanced transition info
                self.logger.info(
                    f" Enhanced strategy transition executed: {symbol} {current_strategy} → {new_strategy}"
                    f" | Confidence: {transition_decision.transition_confidence:.2f}"
                    f" | Emergency: {transition_decision.emergency_change}"
                    f" | Reasoning: {transition_decision.transition_reasoning}"
                )
            else:
                self.logger.info(f" Enhanced strategy assessment complete: {symbol} keeping {current_strategy}")

        except Exception as e:
            self.logger.error(f" Error executing enhanced strategy transition for {symbol}: {e}")

    def _get_current_strategy(self, symbol: str) -> str:
        """Get current strategy for enhanced conflict resolver"""
        return self._get_current_monitoring_strategy(symbol)

    def _get_strategy_performance(self, symbol: str) -> float:
        """Get current strategy performance for enhanced conflict resolver"""
        try:
            # Look up performance from strategy tracking
            if hasattr(self, 'performance_tracker'):
                strategy_perf = self.performance_tracker.get_strategy_performance()
                current_strategy = self._get_current_monitoring_strategy(symbol)
                return strategy_perf.get(current_strategy, 0.5)  # Default to neutral

            # Fallback: use general success rate
            if self.metrics.decisions_made > 0:
                return self.metrics.successful_decisions / self.metrics.decisions_made

            return 0.5  # Neutral performance if no data

        except Exception as e:
            self.logger.error(f"Error getting strategy performance for {symbol}: {e}")
            return 0.5  # Safe fallback

    def _broadcast_strategy_change(self, symbol: str, old_strategy: str, new_strategy: str):
        """Broadcast strategy change to all interested processes"""
        try:
            if hasattr(self, 'queue_manager') and self.queue_manager:
                self.queue_manager.send_strategy_transition(
                    sender_id=self.process_id,
                    symbol=symbol,
                    old_strategy=old_strategy,
                    new_strategy=new_strategy,
                    transition_signals=[s.__dict__ for s in self.strategy_transition_signals[symbol]],
                    confidence=sum(s.confidence for s in self.strategy_transition_signals[symbol]) / len(self.strategy_transition_signals[symbol])
                )

                # Also send immediate update to position health monitor
                self.queue_manager.send_monitoring_strategy_update(
                    sender_id=self.process_id,
                    symbol=symbol,
                    strategy_name=new_strategy,
                    update_type="strategy_transition"
                )

            # Clear processed signals
            self.strategy_transition_signals[symbol].clear()

        except Exception as e:
            self.logger.error(f" Error broadcasting strategy change for {symbol}: {e}")

    def _broadcast_strategy_change_enhanced(self, symbol: str, old_strategy: str, new_strategy: str, transition_decision):
        """
        NEW: Enhanced broadcasting with conflict resolver context

        Includes additional metadata from the conflict resolver decision
        """
        try:
            if hasattr(self, 'queue_manager') and self.queue_manager:
                # Enhanced strategy transition message with conflict resolver data
                enhanced_signals = []
                for signal in self.strategy_transition_signals[symbol]:
                    enhanced_signals.append({
                        'type': signal.signal_type.value,
                        'confidence': signal.confidence,
                        'source': signal.source_process,
                        'timestamp': signal.timestamp.isoformat(),
                        'data': signal.signal_data
                    })

                self.queue_manager.send_strategy_transition(
                    sender_id=self.process_id,
                    symbol=symbol,
                    old_strategy=old_strategy,
                    new_strategy=new_strategy,
                    transition_signals=enhanced_signals,
                    confidence=transition_decision.transition_confidence
                )

                # Enhanced monitoring update with transition context
                self.queue_manager.send_monitoring_strategy_update(
                    sender_id=self.process_id,
                    symbol=symbol,
                    strategy_name=new_strategy,
                    update_type="enhanced_strategy_transition",
                    metadata={
                        'transition_confidence': transition_decision.transition_confidence,
                        'emergency_change': transition_decision.emergency_change,
                        'transition_reasoning': transition_decision.transition_reasoning,
                        'supporting_signals': transition_decision.supporting_signals,
                        'risk_factors': transition_decision.risk_factors
                    }
                )

            # Clear processed signals
            self.strategy_transition_signals[symbol].clear()

            self.logger.info(f" Enhanced strategy change broadcast complete for {symbol}")

        except Exception as e:
            self.logger.error(f" Error broadcasting enhanced strategy change for {symbol}: {e}")

    def _get_current_monitoring_strategy(self, symbol: str) -> str:
        """Get current monitoring strategy for symbol"""
        # This would typically come from shared memory or database
        # For now, use strategy selector to get current best strategy
        if hasattr(self, 'strategy_selector') and self.strategy_selector:
            return self.strategy_selector.get_current_strategy(symbol)
        return "momentum_strategy"  # Default fallback

    def _calculate_market_volatility(self) -> float:
        """Calculate current market volatility for strategy selection"""
        # Simplified volatility calculation
        # In production, this would use real market data
        return 0.15  # Default moderate volatility

    def _cleanup(self):
        """Cleanup resources when shutting down"""
        try:
            self.logger.info("Cleaning up enhanced decision engine resources...")

            # Clear signal buffers
            self.signal_buffer.clear()
            self.sentiment_cache.clear()

            # Clear risk alerts
            self.risk_alerts_active.clear()
            self.force_close_symbols.clear()

            # Stop any active executions if possible
            if self.execution_orchestrator and hasattr(self.execution_orchestrator, 'active_executions'):
                active_count = len(self.execution_orchestrator.active_executions)
                if active_count > 0:
                    self.logger.warning(f"Stopping with {active_count} active executions")

            self.logger.info("Enhanced decision engine cleanup complete")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Get detailed process information for monitoring"""
        try:
            base_info = {
                'process_id': self.process_id,
                'state': self.state.value if hasattr(self.state, 'value') else str(self.state),
                'uptime_seconds': (datetime.now() - getattr(self, 'start_time', datetime.now())).total_seconds(),

                # Enhanced decision engine specific info
                'enhanced_components_available': ENHANCED_COMPONENTS_AVAILABLE,
                'signal_processing': {
                    'active_symbols': len(self.signal_buffer),
                    'total_signals_buffered': sum(len(signals) for signals in self.signal_buffer.values()),
                    'sentiment_cache_size': len(self.sentiment_cache),
                    'risk_alerts_active': len(self.risk_alerts_active),
                    'force_close_symbols': len(self.force_close_symbols)
                },

                'decision_metrics': {
                    'decisions_made': self.metrics.decisions_made,
                    'successful_decisions': self.metrics.successful_decisions,
                    'failed_decisions': self.metrics.failed_decisions,
                    'success_rate': (self.metrics.successful_decisions / max(1, self.metrics.decisions_made)) * 100,
                    'risk_vetoes': self.metrics.risk_vetoes,
                    'parameter_adaptations': self.metrics.parameter_adaptations,
                    'daily_executions': self.daily_execution_count
                },

                'system_health': {
                    'current_regime': self.regime_state.get('current', 'unknown'),
                    'regime_confidence': self.regime_state.get('confidence', 0.0),
                    'signal_weights': self.signal_weights.copy(),
                    'last_weight_adjustment': self.last_weight_adjustment.isoformat(),
                    'performance_window_size': len(self.performance_window)
                },

                'enhanced_components': {
                    'risk_manager_active': self.risk_manager is not None and hasattr(self.risk_manager, 'get_risk_summary'),
                    'parameter_adapter_active': self.parameter_adapter is not None and hasattr(self.parameter_adapter, 'get_adaptation_status'),
                    'execution_orchestrator_active': self.execution_orchestrator is not None and hasattr(self.execution_orchestrator, 'get_execution_status'),
                    'conflict_resolver_active': self.conflict_resolver is not None
                }
            }

            # Add component-specific info if available
            if self.risk_manager and hasattr(self.risk_manager, 'get_risk_summary'):
                try:
                    base_info['risk_manager_status'] = self.risk_manager.get_risk_summary()
                except Exception as e:
                    base_info['risk_manager_status'] = {'error': str(e)}

            if self.parameter_adapter and hasattr(self.parameter_adapter, 'get_adaptation_status'):
                try:
                    base_info['parameter_adapter_status'] = self.parameter_adapter.get_adaptation_status()
                except Exception as e:
                    base_info['parameter_adapter_status'] = {'error': str(e)}

            if self.execution_orchestrator and hasattr(self.execution_orchestrator, 'get_execution_status'):
                try:
                    base_info['execution_orchestrator_status'] = self.execution_orchestrator.get_execution_status()
                except Exception as e:
                    base_info['execution_orchestrator_status'] = {'error': str(e)}

            return base_info

        except Exception as e:
            return {
                'process_id': self.process_id,
                'state': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    def _track_decision_performance(self, symbol: str, execution_result, resolved_decision):
        """Track decision performance for adaptation feedback"""
        try:
            strategy_name = self._get_dominant_strategy(symbol)
            confidence = getattr(resolved_decision, 'final_confidence', 0.5)

            self.performance_tracker.add_decision_result(
                symbol=symbol,
                strategy=strategy_name,
                was_successful=execution_result.was_successful,
                confidence=confidence
            )

            # Track execution metrics
            if hasattr(execution_result, 'execution_slippage'):
                old_slippage = self.metrics.avg_slippage_pct
                new_slippage = execution_result.execution_slippage * 100
                count = self.metrics.successful_decisions
                if count > 0:
                    self.metrics.avg_slippage_pct = ((old_slippage * (count - 1)) + new_slippage) / count

            if hasattr(execution_result, 'executed_amount'):
                self.metrics.total_volume_executed += execution_result.executed_amount

        except Exception as e:
            self.logger.error(f"Error tracking performance: {e}")

    def _get_symbols_ready_for_decision(self) -> List[str]:
        """Get symbols that have enough signals for decision making"""
        ready_symbols = []
        for symbol, signals in self.signal_buffer.items():
            # Need at least 2 recent signals (within last 5 minutes)
            recent_signals = [
                s for s in signals
                if (datetime.now() - s['timestamp']).total_seconds() < 300
            ]
            if len(recent_signals) >= 2:
                ready_symbols.append(symbol)
        return ready_symbols

    def _calculate_technical_component(self, signals: List[Dict]) -> float:
        """Calculate technical signal component"""
        if not signals:
            return 0.0

        # Weight recent signals more heavily
        weighted_sum = 0.0
        weight_sum = 0.0

        for signal in signals:
            age_minutes = (datetime.now() - signal['timestamp']).total_seconds() / 60
            weight = max(0.1, 1.0 - (age_minutes / 60))  # Decay over 1 hour

            signal_value = signal['signal'].get('direction', 0) * signal['confidence']
            weighted_sum += signal_value * weight
            weight_sum += weight

        return weighted_sum / weight_sum if weight_sum > 0 else 0.0

    def _calculate_market_component(self) -> float:
        """Calculate market regime component"""
        regime = self.regime_state.get('current', 'normal')
        confidence = self.regime_state.get('confidence', 0.5)

        # Map regime to signal strength
        regime_map = {
            'bull_trending': 0.5,
            'bear_trending': -0.5,
            'high_volatility': 0.0,  # Neutral in high vol
            'sideways': 0.0,
            'crisis': -0.7,
            'recovery': 0.3,
            'normal': 0.0
        }

        base_signal = regime_map.get(regime, 0.0)
        return base_signal * confidence

    def _calculate_risk_component(self, symbol: str) -> float:
        """Calculate risk-based signal component"""
        risk_signal = 0.0

        # Risk alerts create negative signal
        if symbol in self.risk_alerts_active:
            risk_signal -= 0.5

        # Force close creates strong negative signal
        if symbol in self.force_close_symbols:
            risk_signal -= 1.0

        return max(-1.0, risk_signal)

    def _calculate_signal_agreement(self, signals: List[Dict], sentiment: Dict) -> float:
        """Calculate how much signals agree with each other"""
        if len(signals) < 2:
            return 1.0

        directions = []
        for signal in signals:
            directions.append(signal['signal'].get('direction', 0))

        if sentiment.get('score'):
            directions.append(sentiment['score'])

        if not directions:
            return 0.5

        # Calculate standard deviation of directions
        std_dev = np.std(directions)
        # Convert to agreement score (lower std = higher agreement)
        agreement = max(0.0, 1.0 - std_dev)
        return agreement

    def _calculate_signal_uncertainty(self, signals: List[Dict], sentiment: Dict) -> float:
        """Calculate signal uncertainty"""
        uncertainties = []

        # Signal confidence uncertainty
        confidences = [s['confidence'] for s in signals]
        if confidences:
            avg_confidence = np.mean(confidences)
            uncertainties.append(1.0 - avg_confidence)

        # Sentiment confidence uncertainty
        if sentiment.get('confidence'):
            uncertainties.append(1.0 - sentiment['confidence'])

        # Age-based uncertainty
        if signals:
            ages = [(datetime.now() - s['timestamp']).total_seconds() / 3600 for s in signals]
            avg_age = np.mean(ages)
            age_uncertainty = min(1.0, avg_age / 4.0)  # Max uncertainty after 4 hours
            uncertainties.append(age_uncertainty)

        return np.mean(uncertainties) if uncertainties else 0.5

    def _apply_position_aware_logic(self, symbol: str, base_decision: DecisionType,
                                   sentiment_score: float, confidence: float) -> DecisionType:
        """Apply position-aware decision logic from enhanced decision rules"""
        try:
            # Get current position size
            current_position_pct = 0.0
            if self.position_sizer:
                current_position_pct = self.position_sizer.get_current_size(symbol)

            # Position-aware decision rules
            if current_position_pct > 0:
                # Already holding position - apply position-aware rules
                if base_decision == DecisionType.BUY:
                    # Check if sentiment allows position addition
                    if sentiment_score >= 0.7 and current_position_pct < 0.15:
                        # High sentiment allows adding to position
                        self.logger.info(f"Position addition considered for {symbol}: "
                                       f"current {current_position_pct:.1%}, sentiment {sentiment_score:.2f}")
                        return DecisionType.BUY  # Will be sized appropriately by risk manager
                    else:
                        # Normal buy signal becomes hold when already positioned
                        self.logger.debug(f"Converting BUY to HOLD for {symbol}: "
                                        f"already holding {current_position_pct:.1%}")
                        return DecisionType.HOLD
                elif base_decision == DecisionType.SELL:
                    # Sell signal - close position
                    return DecisionType.SELL
                else:
                    # Hold signal - continue holding
                    return DecisionType.HOLD
            else:
                # No current position - normal logic applies
                return base_decision

        except Exception as e:
            self.logger.error(f"Error in position-aware logic for {symbol}: {e}")
            return base_decision  # Fallback to base decision

    def _calculate_risk_score(self, symbol: str) -> float:
        """Calculate overall risk score for symbol"""
        risk_score = 20.0  # Base risk

        # Risk alerts increase risk
        if symbol in self.risk_alerts_active:
            risk_score += 30.0

        # Force close symbols are maximum risk
        if symbol in self.force_close_symbols:
            risk_score = 100.0

        # Market regime affects risk
        regime = self.regime_state.get('current', 'normal')
        if regime in ['crisis', 'high_volatility']:
            risk_score += 25.0
        elif regime in ['bull_trending', 'recovery']:
            risk_score -= 10.0

        return max(0.0, min(100.0, risk_score))

    def _get_dominant_strategy(self, symbol: str) -> str:
        """Get the dominant strategy for this symbol"""
        signals = list(self.signal_buffer[symbol])
        if not signals:
            return 'unknown'

        # Count strategy occurrences
        strategy_counts = defaultdict(int)
        for signal in signals:
            strategy = signal.get('strategy_name', 'unknown')
            strategy_counts[strategy] += 1

        # Return most common strategy
        return max(strategy_counts.items(), key=lambda x: x[1])[0] if strategy_counts else 'unknown'

    def _reset_daily_counters(self):
        """Reset daily execution counter if new day"""
        today = datetime.now().date()
        if today > self.last_execution_reset:
            self.daily_execution_count = 0
            self.last_execution_reset = today

    def _setup_enhanced_performance_tracking(self):
        """Setup enhanced performance tracking"""
        self.logger.info("Setting up enhanced performance tracking")
        # Additional setup for performance tracking if needed

    def _load_persisted_state(self):
        """Load any persisted state/weights"""
        # Load persisted signal weights, performance data, etc.
        pass

    def _periodic_maintenance(self):
        """Enhanced periodic maintenance"""
        try:
            # Clean old signals
            cutoff_time = datetime.now() - timedelta(hours=6)
            for symbol in list(self.signal_buffer.keys()):
                self.signal_buffer[symbol] = deque(
                    [s for s in self.signal_buffer[symbol] if s['timestamp'] > cutoff_time],
                    maxlen=50
                )

                # Remove empty signal buffers
                if not self.signal_buffer[symbol]:
                    del self.signal_buffer[symbol]

            # Clean old sentiment data
            for symbol in list(self.sentiment_cache.keys()):
                if (datetime.now() - self.sentiment_cache[symbol]['timestamp']).total_seconds() > 7200:  # 2 hours
                    del self.sentiment_cache[symbol]

            # Clear resolved risk alerts (older than 1 hour)
            alerts_to_remove = set()
            for symbol in self.risk_alerts_active:
                # This would check actual alert timestamps in a real implementation
                pass

            # Update execution success rate
            if self.metrics.decisions_made > 0:
                self.metrics.execution_success_rate = (
                    self.metrics.successful_decisions / self.metrics.decisions_made
                )

        except Exception as e:
            self.logger.error(f"Error in periodic maintenance: {e}")

    def _get_next_message(self, timeout: float = 0.1) -> Optional[QueueMessage]:
        """Get next message from priority queue"""
        try:
            if self.priority_queue:
                return self.priority_queue.get(timeout=timeout)
        except:
            pass
        return None

    # Public interface methods for monitoring and control

    def get_enhanced_status(self) -> Dict[str, Any]:
        """Get comprehensive status including new components"""
        try:
            base_status = {
                'state': self.state.value,
                'daily_executions': self.daily_execution_count,
                'active_symbols': len(self.signal_buffer),
                'risk_alerts': len(self.risk_alerts_active),
                'force_close_symbols': len(self.force_close_symbols),
                'current_regime': self.regime_state.get('current'),
                'signal_weights': self.signal_weights.copy(),
                'metrics': {
                    'decisions_made': self.metrics.decisions_made,
                    'success_rate': self.metrics.execution_success_rate,
                    'avg_slippage_pct': self.metrics.avg_slippage_pct,
                    'total_volume': self.metrics.total_volume_executed,
                    'parameter_adaptations': self.metrics.parameter_adaptations
                }
            }

            # Add enhanced component status
            if ENHANCED_COMPONENTS_AVAILABLE:
                if self.risk_manager:
                    base_status['risk_manager'] = self.risk_manager.get_risk_summary()
                if self.execution_orchestrator:
                    base_status['execution_orchestrator'] = self.execution_orchestrator.get_execution_status()
                if self.parameter_adapter:
                    base_status['parameter_adapter'] = self.parameter_adapter.get_adaptation_status()

            # Add performance tracking
            base_status['performance'] = {
                'recent_decisions': len(self.performance_tracker.recent_decisions),
                'recent_performance': self.performance_tracker.get_recent_performance(),
                'strategy_performance': self.performance_tracker.get_strategy_performance()
            }

            return base_status

        except Exception as e:
            return {'error': str(e), 'state': 'error'}

    def force_parameter_reset(self):
        """Emergency parameter reset"""
        try:
            # Reset to default weights
            self.signal_weights = {
                "technical": SignalWeight.TECHNICAL.value,
                "market": SignalWeight.MARKET.value,
                "sentiment": SignalWeight.SENTIMENT.value,
                "risk": SignalWeight.RISK.value
            }

            # Reset enhanced components
            if self.parameter_adapter:
                self.parameter_adapter.force_parameter_reset()
            if self.risk_manager:
                self.risk_manager.resume_trading()  # Clear any halts

            self.logger.warning(" Parameters reset to defaults")

        except Exception as e:
            self.logger.error(f"Error resetting parameters: {e}")

    def emergency_halt_all_trading(self, reason: str):
        """Emergency halt all trading"""
        try:
            if self.risk_manager:
                self.risk_manager.emergency_halt_trading(reason)

            # Clear all pending signals
            self.signal_buffer.clear()

            self.logger.critical(f" EMERGENCY HALT: {reason}")

        except Exception as e:
            self.logger.error(f"Error in emergency halt: {e}")


# Backwards compatibility - alias to your existing name
MarketDecisionEngineProcess = EnhancedMarketDecisionEngineProcess


# Example integration and testing
if __name__ == "__main__":
    # Test the enhanced decision engine
    print(" Testing Enhanced Decision Engine...")

    try:
        engine = EnhancedMarketDecisionEngineProcess()

        print(f" Enhanced components available: {ENHANCED_COMPONENTS_AVAILABLE}")
        print(f" Engine state: {engine.state}")
        print(f" Signal weights: {engine.signal_weights}")

        # Test status
        status = engine.get_enhanced_status()
        print(f" Status keys: {list(status.keys())}")

        print("\n Enhanced Decision Engine ready for integration!")

    except Exception as e:
        print(f" Error testing enhanced engine: {e}")
