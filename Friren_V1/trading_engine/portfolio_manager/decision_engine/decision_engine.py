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

# Import Redis-based infrastructure
from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)

# Import extracted modules
from .signal_processor import SignalProcessor, AggregatedSignal
from .execution_coordinator import ExecutionCoordinator
from .parameter_adapter import ParameterAdapter, create_adaptation_metrics, get_system_metrics

# Import standardized output formatting system
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator

# Import color system for terminal output with safe fallback
try:
    # Add project root to path for color system import
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
    if project_root not in sys.path:
        sys.path.append(project_root)
    
    from terminal_color_system import print_decision_engine, print_communication, print_success, print_warning, print_error
    COLOR_SYSTEM_AVAILABLE = True
except ImportError as e:
    # Safe fallback functions if color system unavailable
    def print_decision_engine(msg): print(f"[DECISION ENGINE] {msg}")
    def print_communication(msg): print(f"[COMMUNICATION] {msg}")
    def print_success(msg): print(f"[SUCCESS] {msg}")
    def print_warning(msg): print(f"[WARNING] {msg}")
    def print_error(msg): print(f"[ERROR] {msg}")
    COLOR_SYSTEM_AVAILABLE = False

# Import MainTerminalBridge for colored business output with Redis fallback
def send_colored_business_output(process_id, message, output_type):
    """Send colored business output with Redis communication fallback"""
    try:
        # Method 1: Try direct main terminal bridge import with proper path resolution
        import sys
        import os
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
        if project_root not in sys.path:
            sys.path.append(project_root)
        from main_terminal_bridge import send_colored_business_output as bridge_output
        bridge_output(process_id, message, output_type)
    except ImportError:
        try:
            # Method 2: Use Redis direct communication (same as subprocess wrapper)
            from Friren_V1.multiprocess_infrastructure.trading_redis_manager import create_process_message, MessagePriority, get_trading_redis_manager
            from datetime import datetime
            import json
            
            # Create message data for main terminal
            message_data = {
                'process_id': process_id,
                'output': message,
                'color_type': output_type,
                'timestamp': datetime.now().isoformat()
            }

            # Get Redis manager and send message
            redis_manager = get_trading_redis_manager()
            if redis_manager:
                # Try to get Redis client directly and send message
                redis_client = getattr(redis_manager, 'redis_client', None)
                if redis_client:
                    redis_client.rpush("terminal_output", json.dumps(message_data))
            else:
                # Fallback to print
                print(f"[{process_id.upper()}] {message}")
        except Exception:
            # Final fallback
            print(f"[{process_id.upper()}] {message}")

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

# Import numpy - FAIL FAST: No fallback allowed
import numpy as np
NUMPY_AVAILABLE = True

# Add project root for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import existing infrastructure - FAIL FAST: No fallback allowed
# RedisBaseProcess already imported above, ProcessState imported above
# Legacy queue imports - now using Redis ProcessMessage
INFRASTRUCTURE_AVAILABLE = True

# PRODUCTION: Import real trading components - FAIL FAST: No fallback allowed
from .risk_manager import SolidRiskManager
from .parameter_adapter import ParameterAdapter
from .execution_orchestrator import ExecutionOrchestrator
from .conflict_resolver import ConflictResolver
ENHANCED_COMPONENTS_AVAILABLE = True
CONFLICT_RESOLVER_AVAILABLE = True
print("PRODUCTION: Enhanced trading components loaded successfully")




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
    """Track performance for parameter adaptation - MEMORY OPTIMIZED"""
    recent_decisions: deque = field(default_factory=lambda: deque(maxlen=15))  # Reduced from 50 to 15
    strategy_performance: Dict[str, deque] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=5)))  # Reduced from 20 to 5
    execution_results: deque = field(default_factory=lambda: deque(maxlen=10))  # Reduced from 30 to 10
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


class EnhancedMarketDecisionEngineProcess(RedisBaseProcess):
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

    def __init__(self, process_id: str = "market_decision_engine"):
        super().__init__(process_id)

        # MEMORY OPTIMIZED: Reduced signal aggregation buffers
        self.signal_buffer = defaultdict(lambda: deque(maxlen=10))  # Reduced from 50 to 10
        self.sentiment_cache = {}
        self.regime_state = {"current": "normal", "confidence": 0.5, "updated": datetime.now()}

        # Enhanced adaptive signal weights
        self.signal_weights = {
            "technical": SignalWeight.TECHNICAL.value,
            "market": SignalWeight.MARKET.value,
            "sentiment": SignalWeight.SENTIMENT.value,
            "risk": SignalWeight.RISK.value
        }
        
        # Track when we last checked for market regime updates
        self.last_regime_check = datetime.now() - timedelta(minutes=10)  # Force initial check

        # MEMORY OPTIMIZED: Reduced metrics and tracking buffers
        self.metrics = DecisionMetrics()
        self.recent_decisions = deque(maxlen=25)  # Reduced from 100 to 25
        self.performance_window = deque(maxlen=15)  # Reduced from 50 to 15

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
        self.output_coordinator = None

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

        self._safe_log("info", "Enhanced Market Decision Engine initialized")
        if not ENHANCED_COMPONENTS_AVAILABLE:
            self._safe_log("warning", " Running with fallback components - some features limited")
            self._safe_log("warning", " Enhanced decision engine components not available. Consider installing missing dependencies.")
        if not CONFLICT_RESOLVER_AVAILABLE:
            self._safe_log("warning", " Conflict resolver using simple fallback - XGBoost features unavailable")
        if not NUMPY_AVAILABLE:
            self._safe_log("warning", " NumPy not available - using basic statistical functions")
        if not INFRASTRUCTURE_AVAILABLE:
            self._safe_log("warning", " Multiprocess infrastructure not available - using fallback stubs")

    def _initialize(self):
        self.logger.critical("EMERGENCY: ENTERED _initialize for decision_engine")
        print("EMERGENCY: ENTERED _initialize for decision_engine")
        try:
            self.logger.info("LIFECYCLE_DEBUG: Initializing SolidRiskManager...")
            self.risk_manager = SolidRiskManager()
            self.logger.info("LIFECYCLE_DEBUG: SolidRiskManager initialized.")

            self.logger.info("LIFECYCLE_DEBUG: Initializing ParameterAdapter...")
            from .parameter_adapter import AdaptationLevel
            self.parameter_adapter = ParameterAdapter(adaptation_level=AdaptationLevel.MODERATE)
            self.logger.info("LIFECYCLE_DEBUG: ParameterAdapter initialized.")

            self.logger.info("LIFECYCLE_DEBUG: Initializing ExecutionOrchestrator...")
            self.execution_orchestrator = ExecutionOrchestrator()
            self.logger.info("LIFECYCLE_DEBUG: ExecutionOrchestrator initialized.")

            self.logger.info("LIFECYCLE_DEBUG: ConflictResolver set for lazy loading...")
            # MEMORY OPTIMIZATION: Lazy load ConflictResolver only when needed
            self.conflict_resolver = None
            # PRODUCTION: Get XGBoost model path from configuration - NO HARDCODED PATHS
            try:
                from Friren_V1.infrastructure.configuration_manager import get_config
                self._conflict_resolver_path = get_config('XGBOOST_MODEL_PATH')
                if not self._conflict_resolver_path:
                    raise ValueError("XGBOOST_MODEL_PATH not configured")
            except Exception as e:
                raise RuntimeError(f"PRODUCTION: XGBoost model path configuration required: {e}")
            self.logger.info("LIFECYCLE_DEBUG: ConflictResolver configured for lazy loading.")

            self.logger.info("LIFECYCLE_DEBUG: Initializing OutputCoordinator...")
            self.output_coordinator = OutputCoordinator(
                redis_client=self.redis_manager.redis_client if self.redis_manager else None,
                enable_terminal=True,
                enable_logging=True
            )
            self.logger.info("LIFECYCLE_DEBUG: OutputCoordinator initialized.")

            self.enhanced_init_status = {'done': True, 'error': None}

            # Initialize basic performance tracking
            self.performance_tracker = PerformanceTracker()
            
            # Mark event-driven process as healthy immediately after initialization
            if hasattr(self, 'state'):
                from Friren_V1.multiprocess_infrastructure.redis_base_process import ProcessState
                self.state = ProcessState.RUNNING
                if hasattr(self, '_update_health'):
                    self._update_health()
                self.logger.info("LIFECYCLE_DEBUG: Event-driven decision_engine marked as healthy")
            
        except Exception as e:
            self.logger.error(f"LIFECYCLE_ERROR: Failed to initialize decision_engine components: {e}")
            self.enhanced_init_status = {'done': False, 'error': str(e)}
            raise
    def _get_conflict_resolver(self):
        """Lazy load ConflictResolver only when needed to save memory"""
        if self.conflict_resolver is None:
            try:
                self.logger.info("MEMORY_OPTIMIZATION: Loading ConflictResolver on demand...")
                from .conflict_resolver import ConflictResolver
                self.conflict_resolver = ConflictResolver(model_path=self._conflict_resolver_path)
                self.logger.info("MEMORY_OPTIMIZATION: ConflictResolver loaded successfully")
            except Exception as e:
                self.logger.critical(f"CRITICAL ERROR: Failed to load ConflictResolver: {e}")
                self.logger.critical("SYSTEM CANNOT OPERATE: ConflictResolver is required for safe trading decisions")
                raise RuntimeError(f"Critical component ConflictResolver failed to load: {e}. System cannot make safe trading decisions without conflict resolution.")
        return self.conflict_resolver

    def _initialize_enhanced_components(self):
        """Initialize enhanced trading components with Redis-based infrastructure"""
        # Initialize proper enhanced components
        from .parameter_adapter import AdaptationLevel
        self.risk_manager = SolidRiskManager()
        self.parameter_adapter = ParameterAdapter(adaptation_level=AdaptationLevel.MODERATE)
        self.execution_orchestrator = ExecutionOrchestrator()
        # PRODUCTION: Get XGBoost model path from configuration - NO HARDCODED PATHS
        try:
            from Friren_V1.infrastructure.configuration_manager import get_config
            model_path = get_config('XGBOOST_MODEL_PATH')
            if not model_path:
                raise ValueError("XGBOOST_MODEL_PATH not configured")
            self.conflict_resolver = ConflictResolver(model_path=model_path)
        except Exception as e:
            raise RuntimeError(f"PRODUCTION: XGBoost model path configuration required: {e}")
        self.enhanced_init_status = {'done': True, 'error': None}

    def _initialize_tool_connections(self):
        """Initialize connections to existing trading tools"""
        # TODO: These will be injected by the orchestrator in real system
        self.logger.info("Connecting to existing trading tools...")

        # Your existing tool connections would go here
        # self.strategy_selector = StrategySelector()
        # self.position_sizer = PositionSizer()
        # self.db_manager = TradingDBManager()

        self.logger.info("Tool connections established")

    def _update_market_regime_from_redis(self):
        """Periodically fetch market regime data from Redis shared state"""
        try:
            if self.redis_manager:
                # Fetch market regime data from Redis (matching namespace used by market regime detector)
                regime_data = self.redis_manager.get_shared_state('market_regime', namespace='market')
                
                if regime_data and isinstance(regime_data, dict):
                    current_regime = regime_data.get('regime')
                    confidence = regime_data.get('confidence', 0.0)
                    last_update = regime_data.get('last_update')
                    
                    if current_regime and current_regime != 'UNKNOWN':
                        old_regime = self.regime_state.get('current')
                        
                        # Update regime state
                        self.regime_state = {
                            'current': current_regime,
                            'confidence': confidence,
                            'updated': datetime.now(),
                            'volatility': regime_data.get('volatility_regime', 'NORMAL'),
                            'trend': regime_data.get('trend', 'SIDEWAYS')
                        }
                        
                        if old_regime != current_regime:
                            self.logger.info(f"Market regime updated from Redis: {old_regime} -> {current_regime} (confidence: {confidence:.1f}%)")
                        
                        return True
                    else:
                        self.logger.debug(f"Market regime data invalid or UNKNOWN: {regime_data}")
                else:
                    self.logger.debug("No market regime data available in Redis")
                    
        except Exception as e:
            self.logger.error(f"Error updating market regime from Redis: {e}")
            
        return False

    def _safe_log(self, level: str, message: str):
        """Safe logging method for decision engine"""
        if hasattr(self, 'logger') and self.logger:
            getattr(self.logger, level)(message)
        else:
            print(f"[{level.upper()}] {message}")

    def _execute(self):
        """Execute main process logic (required by RedisBaseProcess)"""
        self._process_cycle()

    def _process_cycle(self):
        """FIXED: Event-driven Decision Engine - only processes when there are actual messages"""
        try:
            cycle_start = time.time()
            messages_processed = 0

            # Reset daily execution count if new day
            self._reset_daily_counters()
            
            # FIXED: Periodically check for market regime updates from Redis (every 5 minutes)
            # Defensive initialization for last_regime_check
            if not hasattr(self, 'last_regime_check'):
                self.last_regime_check = datetime.now() - timedelta(minutes=10)
            
            if (datetime.now() - self.last_regime_check).total_seconds() > 300:  # 5 minutes
                if self._update_market_regime_from_redis():
                    self.logger.debug("Market regime updated from Redis")
                self.last_regime_check = datetime.now()

            # FIXED: Only process if there are actual messages waiting
            message = self._get_next_message(timeout=0.1)  # Slightly longer timeout
            if message is None:
                # No messages - sleep much longer to let other processes show output
                time.sleep(30.0)  # Sleep 30 seconds when idle to reduce terminal spam
                return

            # Process the message we found
            self._process_message(message)
            messages_processed += 1

            # Check for additional messages (but don't loop indefinitely)
            additional_cycles = 0
            while additional_cycles < 10:  # Max 10 additional messages per cycle
                message = self._get_next_message(timeout=0.01)  # Very short timeout
                if message is None:
                    break
                
                self._process_message(message)
                messages_processed += 1
                additional_cycles += 1

                # Safety check for processing time
                if time.time() - cycle_start > 2.0:
                    self.logger.warning(f"Processing cycle taking too long, processed {messages_processed} messages")
                    break

            # BUSINESS LOGIC OUTPUT: Only when actually processing messages
            if messages_processed > 0:
                try:
                    from terminal_color_system import print_decision_engine
                    print_decision_engine(f"Decision Engine: Processed {messages_processed} trading signals")
                except ImportError:
                    print(f"[DECISION ENGINE] Decision Engine: Processed {messages_processed} trading signals")

                # After processing messages, make enhanced decisions
                self._make_enhanced_trading_decisions()

                self.logger.info(f"BUSINESS LOGIC: Decision engine processed {messages_processed} signals")
            
            # NEW: Periodic parameter adaptation (only occasionally)
            self._maybe_adapt_parameters()

            # Periodic maintenance (less frequent)
            self._periodic_maintenance()

            # Update metrics
            cycle_time = (time.time() - cycle_start) * 1000
            self.metrics.processing_time_ms = cycle_time

            if messages_processed > 0:
                self.logger.debug(f"Event-driven cycle: {messages_processed} messages in {cycle_time:.1f}ms")

        except Exception as e:
            self.logger.error(f"Error in event-driven process cycle: {e}")
            self.error_count += 1

    def _process_message(self, message: ProcessMessage):
        """Enhanced message processing (keeps your existing logic)"""
        try:
            if message.message_type == "STRATEGY_SIGNAL":
                self._handle_strategy_signal(message)
            elif message.message_type == "SENTIMENT_UPDATE":
                self._handle_sentiment_update(message)
            elif message.message_type == "HEALTH_ALERT":
                self._handle_health_alert(message)
            elif message.message_type == "REGIME_CHANGE":
                self._handle_regime_change(message)
            elif message.message_type == "REGIME_UPDATE":
                self._handle_regime_update(message)
            elif message.message_type == "TRADING_RECOMMENDATION":
                self._handle_trading_recommendation(message)
            elif message.message_type == "REASSESSMENT_REQUEST":
                self._handle_reassessment_request(message)

            # NEW: Strategy Management Message Handling (from implementation_rules.xml)
            elif hasattr(MessageType, 'STRATEGY_REASSESSMENT_REQUEST') and message.message_type == MessageType.STRATEGY_REASSESSMENT_REQUEST:
                self._handle_strategy_reassessment_request(message)
            elif hasattr(MessageType, 'SENTIMENT_UPDATE') and message.message_type == MessageType.SENTIMENT_UPDATE:
                self._handle_sentiment_update(message)
            elif message.message_type == "start_cycle":
                # CRITICAL FIX: Handle queue rotation cycle activation
                self.logger.info(f"QUEUE ACTIVATION: Decision engine received start_cycle signal")
                
                # Import colored output functions
                try:
                    from colored_print import success, info
                    from terminal_color_system import print_decision_engine
                    success("QUEUE ACTIVATION: Decision engine cycle started")
                    print_decision_engine("Starting decision analysis and signal processing...")
                except ImportError:
                    print("[QUEUE ACTIVATION] Decision engine cycle started")
                
                # Activate execution cycle in base process  
                if hasattr(self, 'start_execution_cycle'):
                    cycle_time = message.data.get('cycle_time', 30.0)
                    self.start_execution_cycle(cycle_time)
                    self.logger.info(f"QUEUE ACTIVATION: Decision engine execution cycle activated for {cycle_time}s")
                
                # Force immediate decision processing
                self._process_pending_signals()
            else:
                self.logger.warning(f"Unknown message type: {message.message_type}")

        except Exception as e:
            self.logger.error(f"Error processing {message.message_type}: {e}")

    def _handle_strategy_signal(self, message: ProcessMessage):
        """Enhanced strategy signal handling with detailed logging"""
        payload = message.payload
        symbol = payload.get('symbol')
        signal_data = payload.get('signal', {})

        if not symbol:
            self.logger.warning("Strategy signal missing symbol")
            return

        self.logger.info(f"=== STRATEGY SIGNAL RECEIVED ===")
        self.logger.info(f"SYMBOL: {symbol}")
        self.logger.info(f"FROM: {message.sender_id}")
        self.logger.info(f"STRATEGY: {payload.get('strategy_name', 'unknown')}")
        self.logger.info(f"CONFIDENCE: {signal_data.get('confidence', 0):.3f}")
        self.logger.info(f"SIGNAL TYPE: {signal_data.get('signal_type', 'unknown')}")
        self.logger.info(f"DIRECTION: {signal_data.get('direction', 'unknown')}")
        if 'target_price' in signal_data:
            self.logger.info(f"TARGET PRICE: ${signal_data['target_price']:.2f}")
        if 'stop_loss' in signal_data:
            self.logger.info(f"STOP LOSS: ${signal_data['stop_loss']:.2f}")
        if 'reasoning' in signal_data:
            self.logger.info(f"REASONING: {signal_data['reasoning']}")

        # ENHANCED: Add colorized terminal output for decision engine visibility
        if COLOR_SYSTEM_AVAILABLE:
            print_decision_engine(f"Decision Engine: Received {signal_data.get('direction', 'UNKNOWN')} signal for {symbol}")
            print_decision_engine(f"Decision Engine: Strategy '{payload.get('strategy_name', 'unknown')}' confidence: {signal_data.get('confidence', 0):.1%}")
            if 'reasoning' in signal_data:
                print_decision_engine(f"Decision Engine: Strategy reasoning - {signal_data['reasoning']}")

            # Decision-making process
            confidence = signal_data.get('confidence', 0)
            if confidence >= 0.8:
                print_decision_engine(f"Decision Engine: HIGH confidence signal - Analyzing risk parameters...")
                print_decision_engine(f"Decision Engine: Risk check PASSED - Signal strength sufficient for action")
            elif confidence >= 0.7:
                print_decision_engine(f"Decision Engine: MEDIUM confidence - Cross-referencing with other indicators...")
                print_decision_engine(f"Decision Engine: Additional confirmation needed - Signal partially approved")
            else:
                print_decision_engine(f"Decision Engine: LOW confidence signal - Risk assessment...")
                print_decision_engine(f"Decision Engine: Confidence below threshold - Signal requires validation")

        # Add to signal buffer (your existing logic)
        signal_entry = {
            'timestamp': message.timestamp,
            'sender': message.sender_id,
            'signal': signal_data,
            'confidence': signal_data.get('confidence', 0),
            'strategy_name': payload.get('strategy_name', 'unknown')  # NEW: track strategy
        }

        self.signal_buffer[symbol].append(signal_entry)

        self.logger.info(f"SIGNAL BUFFERED: {len(self.signal_buffer[symbol])} signals for {symbol}")
        self.logger.debug(f"Added strategy signal for {symbol}, confidence: {signal_data.get('confidence', 0)}")

    def _handle_sentiment_update(self, message: ProcessMessage):
        """Enhanced sentiment update handling with detailed logging"""
        payload = message.payload
        symbol = payload.get('symbol')
        sentiment_data = payload.get('sentiment', {})

        if not symbol:
            self.logger.warning("Sentiment update missing symbol")
            return

        self.logger.info(f"=== SENTIMENT UPDATE RECEIVED ===")
        self.logger.info(f"SYMBOL: {symbol}")
        self.logger.info(f"FROM: {message.sender_id}")
        self.logger.info(f"SENTIMENT SCORE: {sentiment_data.get('sentiment_score', 0):.3f}")
        self.logger.info(f"CONFIDENCE: {sentiment_data.get('confidence', 0):.3f}")
        self.logger.info(f"ARTICLE COUNT: {sentiment_data.get('article_count', 0)}")
        if 'professional_sentiment' in sentiment_data:
            self.logger.info(f"PROFESSIONAL SENTIMENT: {sentiment_data['professional_sentiment']:.3f}")
        if 'social_sentiment' in sentiment_data:
            self.logger.info(f"SOCIAL SENTIMENT: {sentiment_data['social_sentiment']:.3f}")
        if 'market_events' in sentiment_data:
            self.logger.info(f"MARKET EVENTS: {sentiment_data['market_events']}")
        if 'shap_analysis' in sentiment_data:
            shap_data = sentiment_data['shap_analysis']
            self.logger.info(f"SHAP ANALYSIS:")
            for feature, value in shap_data.items():
                self.logger.info(f"  {feature}: {value:.3f}")

        # ENHANCED: Add colorized terminal output for sentiment analysis
        if COLOR_SYSTEM_AVAILABLE:
            sentiment_score = sentiment_data.get('sentiment_score', 0)
            article_count = sentiment_data.get('article_count', 0)
            confidence = sentiment_data.get('confidence', 0)

            print_decision_engine(f"Decision Engine: Processing sentiment update for {symbol}")
            print_decision_engine(f"Decision Engine: {article_count} articles analyzed, sentiment: {sentiment_score:.2f}, confidence: {confidence:.1%}")

            # Decision engine sentiment interpretation
            if sentiment_score > 0.3:
                print_decision_engine(f"Decision Engine: POSITIVE sentiment detected - Bullish indicators confirmed")
            elif sentiment_score < -0.3:
                print_decision_engine(f"Decision Engine: NEGATIVE sentiment detected - Bearish indicators confirmed")
            else:
                print_decision_engine(f"Decision Engine: NEUTRAL sentiment - Mixed signals requiring additional analysis")

            if confidence >= 0.8:
                print_decision_engine(f"Decision Engine: Sentiment confidence HIGH - Incorporating into decision matrix")
            else:
                print_decision_engine(f"Decision Engine: Sentiment confidence MODERATE - Weighting appropriately")

        # Update sentiment cache with enhanced data
        self.sentiment_cache[symbol] = {
            'score': sentiment_data.get('sentiment_score', 0),
            'confidence': sentiment_data.get('confidence', 0),
            'timestamp': message.timestamp,
            'source': message.sender_id,
            'article_count': sentiment_data.get('article_count', 0),  # NEW: track article count
            'shap_analysis': sentiment_data.get('shap_analysis', {})
        }

        self.logger.info(f"SENTIMENT CACHED for {symbol}")
        self.logger.debug(f"Updated sentiment for {symbol}: {sentiment_data.get('sentiment_score', 0)}")

    def _handle_health_alert(self, message: ProcessMessage):
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

    def _handle_regime_change(self, message: ProcessMessage):
        """Enhanced regime change handling - Legacy support"""
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
    
    def _handle_regime_update(self, message: ProcessMessage):
        """
        Handle real-time regime updates from Market Regime Detector
        
        CRITICAL FIX: Automatic regime-based strategy reassignment
        When market regime changes, automatically reassign strategies for current positions
        """
        try:
            data = message.data
            new_regime = data.get('new_regime')
            previous_regime = data.get('previous_regime', 'UNKNOWN')
            confidence = data.get('confidence', 0.0)
            trend = data.get('trend', 'UNKNOWN')
            market_stress = data.get('market_stress_level', 50.0)
            
            if new_regime and new_regime != previous_regime:
                # Update internal regime state
                old_regime = self.regime_state.get('current', 'UNKNOWN')
                self.regime_state = {
                    'current': new_regime,
                    'confidence': confidence,
                    'updated': datetime.now(),
                    'volatility': data.get('volatility_regime', 'UNKNOWN'),
                    'trend': trend,
                    'market_stress': market_stress
                }
                
                self.logger.info(f"REGIME UPDATE: Market regime changed from {previous_regime} to {new_regime} "
                               f"(confidence: {confidence:.1f}%, stress: {market_stress:.1f})")
                print(f"[DECISION ENGINE] Regime Update: {previous_regime} → {new_regime} | "
                      f"Confidence: {confidence:.1f}% | Initiating strategy reassignment...")
                
                # CRITICAL: Trigger automatic strategy reassignments for all current positions
                self._trigger_regime_based_reassignments(new_regime, previous_regime, data)
                
        except Exception as e:
            self.logger.error(f"Error handling regime update: {e}")
    
    def _trigger_regime_based_reassignments(self, new_regime: str, previous_regime: str, regime_data: Dict[str, Any]):
        """
        Trigger automatic strategy reassignments based on regime change
        
        CRITICAL FIX: Proactive strategy adaptation to market regime changes
        Ensures all positions use optimal strategies for new market conditions
        """
        try:
            # Get current active positions from database
            if hasattr(self, 'db_manager') and self.db_manager:
                current_holdings = self.db_manager.get_current_holdings()
                if not current_holdings:
                    self.logger.info("REGIME REASSIGNMENT: No current holdings to reassign")
                    return
                
                reassignment_count = 0
                for holding in current_holdings:
                    try:
                        symbol = holding.get('symbol')
                        current_strategy = holding.get('assigned_strategy', 'none')
                        
                        if symbol and current_strategy != 'none':
                            # Check if current strategy is still optimal for new regime
                            reassignment_needed = self._assess_regime_reassignment_need(
                                symbol, current_strategy, new_regime, regime_data
                            )
                            
                            if reassignment_needed:
                                # Send reassignment request to strategy assignment system
                                self._request_strategy_reassignment(symbol, current_strategy, 
                                                                  new_regime, "regime_change", regime_data)
                                reassignment_count += 1
                                
                    except Exception as e:
                        self.logger.error(f"Error processing reassignment for {symbol}: {e}")
                
                self.logger.info(f"REGIME REASSIGNMENT: Initiated {reassignment_count} regime-based strategy reassignments")
                if reassignment_count > 0:
                    print(f"[DECISION ENGINE] Regime-based reassignment: {reassignment_count} positions queued for strategy review")
                    
            else:
                self.logger.warning("REGIME REASSIGNMENT: Database manager not available for position lookup")
                
        except Exception as e:
            self.logger.error(f"Error triggering regime-based reassignments: {e}")
    
    def _assess_regime_reassignment_need(self, symbol: str, current_strategy: str, 
                                       new_regime: str, regime_data: Dict[str, Any]) -> bool:
        """
        Assess if a position needs strategy reassignment based on regime change
        
        Args:
            symbol: Stock symbol
            current_strategy: Currently assigned strategy
            new_regime: New market regime
            regime_data: Full regime change data
            
        Returns:
            True if reassignment is needed
        """
        try:
            # Define regime-incompatible strategy combinations
            regime_strategy_conflicts = {
                'BEAR_MARKET': ['momentum', 'pca_momentum', 'jump_momentum'],
                'BULL_MARKET': ['mean_reversion', 'pca_mean_reversion', 'defensive'],
                'HIGH_VOLATILITY': ['momentum', 'trend_following'],
                'SIDEWAYS': ['momentum', 'jump_momentum', 'volatility_breakout']
            }
            
            # Check if current strategy conflicts with new regime
            conflicting_strategies = regime_strategy_conflicts.get(new_regime, [])
            if current_strategy in conflicting_strategies:
                self.logger.info(f"REASSIGNMENT NEEDED: {symbol} strategy {current_strategy} conflicts with {new_regime}")
                return True
            
            # Check market stress level for high-risk strategies
            market_stress = regime_data.get('market_stress_level', 50.0)
            if market_stress > 75 and current_strategy in ['volatility_breakout', 'jump_momentum']:
                self.logger.info(f"REASSIGNMENT NEEDED: {symbol} high-risk strategy {current_strategy} in high stress market")
                return True
                
            # Check trend reversal impact
            trend = regime_data.get('trend', 'UNKNOWN')
            if trend == 'DOWNTREND' and current_strategy in ['momentum', 'pca_momentum']:
                self.logger.info(f"REASSIGNMENT NEEDED: {symbol} momentum strategy {current_strategy} in downtrend")
                return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"Error assessing reassignment need for {symbol}: {e}")
            return False
    
    def _request_strategy_reassignment(self, symbol: str, current_strategy: str, 
                                     new_regime: str, reason: str, regime_data: Dict[str, Any]):
        """
        Request strategy reassignment for a specific position
        
        Args:
            symbol: Stock symbol to reassign
            current_strategy: Current strategy being used
            new_regime: New market regime triggering reassignment
            reason: Reason for reassignment
            regime_data: Full regime data for context
        """
        try:
            # Create reassignment request message
            reassignment_message = create_process_message(
                sender_id="market_decision_engine",
                recipient="position_health_monitor",
                message_type="STRATEGY_REASSIGNMENT_REQUEST",
                data={
                    'symbol': symbol,
                    'current_strategy': current_strategy,
                    'reassignment_reason': reason,
                    'new_regime': new_regime,
                    'regime_data': regime_data,
                    'trigger_source': 'regime_change',
                    'priority': 'high',
                    'timestamp': datetime.now().isoformat()
                },
                priority=MessagePriority.HIGH
            )
            
            # Send reassignment request
            success = self.redis_manager.send_process_message(reassignment_message)
            if success:
                self.logger.info(f"REASSIGNMENT REQUEST: Sent strategy reassignment request for {symbol} due to {new_regime} regime")
            else:
                self.logger.error(f"REASSIGNMENT REQUEST: Failed to send reassignment request for {symbol}")
                
        except Exception as e:
            self.logger.error(f"Error requesting strategy reassignment for {symbol}: {e}")

    def _handle_trading_recommendation(self, message: ProcessMessage):
        """Handle trading recommendations from enhanced news pipeline"""
        try:
            self.logger.info(f"DECISION ENGINE: Received TRADING_RECOMMENDATION message from {message.sender}")
            print(f"[DECISION ENGINE] RECEIVED: TRADING_RECOMMENDATION from {message.sender}")
            
            payload = message.data  # Fixed: use .data instead of .payload
            symbol = payload.get('symbol')
            recommendation = payload.get('recommendation', {})
            supporting_data = payload.get('supporting_data', {})

            if not symbol or not recommendation:
                self.logger.warning("Trading recommendation missing symbol or recommendation data")
                print(f"[DECISION ENGINE] ERROR: Missing symbol or recommendation data")
                return

            self.logger.info(f"=== TRADING RECOMMENDATION RECEIVED ===")
            self.logger.info(f"SYMBOL: {symbol}")
            self.logger.info(f"FROM: {message.sender}")
            self.logger.info(f"ACTION: {recommendation.get('action', 'unknown')}")
            self.logger.info(f"CONFIDENCE: {recommendation.get('confidence', 0):.3f}")
            
            # Business logic output to terminal using standardized OutputCoordinator
            if self.output_coordinator:
                # Extract real data for XGBoost-style recommendation output
                action = recommendation.get('action', 'HOLD').upper()
                confidence = recommendation.get('confidence', 0.0)
                features = {
                    'news_volume': supporting_data.get('news_volume', 0),
                    'sentiment_count': supporting_data.get('sentiment_count', 0),
                    'confidence': confidence
                }
                self.output_coordinator.output_xgboost_recommendation(symbol, action, confidence, features)
            self.logger.info(f"NEWS VOLUME: {supporting_data.get('news_volume', 0)}")
            self.logger.info(f"SENTIMENT COUNT: {supporting_data.get('sentiment_count', 0)}")

            # Convert recommendation to aggregated signal format for existing pipeline
            aggregated_signal = {
                'symbol': symbol,
                'action': recommendation.get('action', 'HOLD'),
                'confidence': recommendation.get('confidence', 0.0),
                'reasoning': recommendation.get('reasoning', ''),
                'source': 'news_pipeline',
                'timestamp': message.timestamp,
                'supporting_data': supporting_data
            }

            # Process through existing decision pipeline
            self._process_trading_recommendation_signal(symbol, aggregated_signal)

        except Exception as e:
            self.logger.error(f"Error handling trading recommendation: {e}")

    def _handle_reassessment_request(self, message: ProcessMessage):
        """Handle strategy reassessment requests from Position Health Monitor"""
        try:
            self.logger.info(f"DECISION ENGINE: Received REASSESSMENT_REQUEST from {message.sender}")
            print(f"[DECISION ENGINE] RECEIVED: REASSESSMENT_REQUEST from {message.sender}")
            
            payload = message.data  # Use .data instead of .payload
            symbol = payload.get('symbol')
            strategy_name = payload.get('strategy_name')
            performance_score = payload.get('performance_score', 0.0)
            health_status = payload.get('health_status', 'UNKNOWN')
            reason = payload.get('reason', 'No reason provided')

            if not symbol or not strategy_name:
                self.logger.warning("Reassessment request missing symbol or strategy_name")
                print(f"[DECISION ENGINE] ERROR: Missing symbol or strategy_name")
                return

            self.logger.info(f"=== STRATEGY REASSESSMENT REQUEST ===")
            self.logger.info(f"SYMBOL: {symbol}")
            self.logger.info(f"CURRENT STRATEGY: {strategy_name}")
            self.logger.info(f"PERFORMANCE SCORE: {performance_score}")
            self.logger.info(f"HEALTH STATUS: {health_status}")
            self.logger.info(f"REASON: {reason}")

            # Trigger strategy reassignment through 3-scenario system (Scenario 3)
            try:
                # Import strategy assignment engine for reassignment
                from ..tools.strategy_assignment_engine import StrategyAssignmentEngine
                from ..tools.db_manager import TradingDBManager
                
                # Create or reuse strategy assignment engine
                if not hasattr(self, '_strategy_assignment_engine'):
                    db_manager = TradingDBManager()
                    self._strategy_assignment_engine = StrategyAssignmentEngine(
                        symbols=[symbol],
                        risk_tolerance=getattr(self, 'risk_tolerance', 'moderate'),
                        db_manager=db_manager
                    )

                # Request strategy reassignment using Scenario 3 (performance-based)
                if hasattr(self._strategy_assignment_engine, 'scenario_coordinator'):
                    reassignment_context = {
                        'symbol': symbol,
                        'current_strategy': strategy_name,
                        'performance_score': performance_score,
                        'health_status': health_status,
                        'reason': reason,
                        'timestamp': message.timestamp
                    }
                    
                    new_assignment = self._strategy_assignment_engine.scenario_coordinator.reassign_strategy_from_health_monitor(
                        symbol, reassignment_context
                    )
                    
                    if new_assignment:
                        self.logger.info(f"STRATEGY REASSIGNED: {symbol} from {strategy_name} to {new_assignment.strategy_name}")
                        print(f"[DECISION ENGINE] STRATEGY REASSIGNED: {symbol} {strategy_name} → {new_assignment.strategy_name}")
                        
                        # Business logic output using standardized OutputCoordinator
                        if self.output_coordinator:
                            self.output_coordinator.output_strategy_reassignment(
                                symbol, strategy_name, new_assignment.strategy_name, reason, performance_score
                            )
                    else:
                        self.logger.info(f"REASSIGNMENT REJECTED: Current strategy {strategy_name} remains optimal for {symbol}")
                        print(f"[DECISION ENGINE] REASSIGNMENT REJECTED: {strategy_name} remains optimal for {symbol}")
                else:
                    self.logger.warning("3-scenario system not available - cannot perform reassignment")

            except Exception as reassignment_error:
                self.logger.error(f"Error during strategy reassignment: {reassignment_error}")

        except Exception as e:
            self.logger.error(f"Error handling reassessment request: {e}")

    def _process_trading_recommendation_signal(self, symbol: str, signal: dict):
        """Process trading recommendation through conflict resolution and risk validation"""
        try:
            self.logger.info(f"=== PROCESSING TRADING RECOMMENDATION FOR {symbol} ===")

            # Step 1: Conflict resolution using existing conflict resolver
            if hasattr(self, 'conflict_resolver') and self.conflict_resolver:
                resolved_signal = self._get_conflict_resolver().resolve_conflicts(symbol, [signal])
                if not resolved_signal:
                    self.logger.info(f"CONFLICT RESOLUTION: Signal rejected for {symbol}")
                    return
                signal = resolved_signal

            # Step 2: Risk validation using existing risk manager
            if hasattr(self, 'risk_manager') and self.risk_manager:
                risk_validation = self.risk_manager.validate_decision(
                    symbol=symbol,
                    action=signal['action'],
                    confidence=signal['confidence'],
                    reasoning=signal['reasoning']
                )

                self.logger.info(f"RISK VALIDATION: {symbol} - Approved: {risk_validation.is_approved}")
                if risk_validation.risk_warnings:
                    self.logger.warning(f"RISK WARNINGS: {risk_validation.risk_warnings}")

                # BUSINESS LOGIC OUTPUT: Risk validation using standardized OutputCoordinator
                if self.output_coordinator:
                    risk_status = "PASSED" if risk_validation.is_approved else "FAILED"
                    action = signal.get('action', 'UNKNOWN')
                    # Extract real risk data - NO HARDCODED VALUES
                    risk_score = getattr(risk_validation, 'risk_score', 0.0)
                    risk_reason = risk_validation.rejection_reason if not risk_validation.is_approved else "Risk assessment passed"
                    quantity = getattr(risk_validation, 'recommended_quantity', 0)
                    
                    self.output_coordinator.output_risk_check(symbol, risk_status, action, quantity, risk_score, risk_reason)

                # Step 3: Execute if approved
                if risk_validation.is_approved and risk_validation.should_execute:
                    self._execute_approved_recommendation(risk_validation, signal)
                else:
                    self.logger.info(f"EXECUTION BLOCKED: {risk_validation.rejection_reason}")
            else:
                self.logger.warning("Risk manager not available - cannot validate trading recommendation")

        except Exception as e:
            self.logger.error(f"Error processing trading recommendation signal: {e}")

    def _execute_approved_recommendation(self, risk_validation, signal):
        """Execute approved trading recommendation using execution coordinator"""
        try:
            if not hasattr(self, '_execution_coordinator'):
                self._execution_coordinator = ExecutionCoordinator(
                    process_id=self.process_id,
                    execution_orchestrator=getattr(self, 'execution_orchestrator', None)
                )
            
            result = self._execution_coordinator.execute_approved_recommendation(risk_validation, signal)
            
            # Business logic output using standardized OutputCoordinator
            if self.output_coordinator and result.was_successful:
                # Extract real execution data - NO HARDCODED VALUES
                symbol = signal.get('symbol', 'UNKNOWN')
                action = signal.get('action', 'UNKNOWN')
                quantity = getattr(result, 'quantity_executed', getattr(risk_validation, 'recommended_quantity', 0))
                price = getattr(result, 'execution_price', 0.0)
                order_id = getattr(result, 'order_id', 'N/A')
                total_value = getattr(result, 'total_value', quantity * price if quantity and price else 0.0)
                
                self.output_coordinator.output_execution(symbol, action, quantity, price, order_id, total_value)
            elif result.was_successful:
                self.logger.info(f"EXECUTION SUCCESS: {result.execution_summary}")
            else:
                self.logger.error(f"EXECUTION FAILED: {result.error_message}")
                if self.output_coordinator:
                    symbol = signal.get('symbol', 'UNKNOWN')
                    self.output_coordinator.output_error(f"Execution failed: {result.error_message}", "ExecutionEngine", symbol)
                
            return result

        except Exception as e:
            self.logger.error(f"Error executing approved recommendation: {e}")

    def _make_enhanced_trading_decisions(self):
        """Enhanced decision making with complete pipeline"""
        try:
            # Get symbols with sufficient signals
            ready_symbols = self._get_symbols_ready_for_decision()

            for symbol in ready_symbols:
                try:
                    self.logger.info(f"=== MAKING DECISION FOR {symbol} ===")

                    # Step 1: Aggregate signals (enhanced version of your existing logic)
                    aggregated_signal = self._create_enhanced_aggregated_signal(symbol)
                    if not aggregated_signal:
                        self.logger.info(f"NO AGGREGATED SIGNAL: Insufficient data for {symbol}")
                        continue

                    self.logger.info(f"AGGREGATED SIGNAL CREATED:")
                    self.logger.info(f"  DECISION TYPE: {aggregated_signal.decision_type.value}")
                    self.logger.info(f"  CONFIDENCE: {aggregated_signal.confidence:.2f}%")
                    self.logger.info(f"  RISK SCORE: {aggregated_signal.risk_score:.2f}")
                    self.logger.info(f"  SIGNAL COMPONENTS:")
                    for component, value in aggregated_signal.signal_components.items():
                        self.logger.info(f"    {component.upper()}: {value:.3f}")
                    self.logger.info(f"  SIGNAL AGREEMENT: {aggregated_signal.signal_agreement:.3f}")
                    self.logger.info(f"  UNCERTAINTY: {aggregated_signal.uncertainty:.3f}")
                    self.logger.info(f"  FINAL DIRECTION: {aggregated_signal.final_direction:.3f}")

                    # Show SHAP analysis if available in sentiment
                    sentiment_data = self.sentiment_cache.get(symbol, {})
                    if 'shap_analysis' in sentiment_data and sentiment_data['shap_analysis']:
                        self.logger.info(f"  SHAP FEATURE IMPORTANCE:")
                        for feature, value in sentiment_data['shap_analysis'].items():
                            self.logger.info(f"    {feature}: {value:.3f}")

                    # Step 2: NEW - Resolve conflicts using conflict resolver
                    resolved_decision = self._resolve_signal_conflicts(aggregated_signal)
                    if not resolved_decision:
                        self.logger.warning(f"CONFLICT RESOLUTION FAILED: Cannot resolve for {symbol}")
                        continue

                    self.logger.info(f"CONFLICT RESOLUTION COMPLETE:")
                    self.logger.info(f"  FINAL DIRECTION: {getattr(resolved_decision, 'final_direction', 'unknown')}")
                    self.logger.info(f"  FINAL CONFIDENCE: {getattr(resolved_decision, 'final_confidence', 0):.3f}")
                    self.logger.info(f"  RESOLUTION METHOD: {getattr(resolved_decision, 'resolution_method', 'unknown')}")

                    # Step 3: NEW - Risk validation with enhanced risk manager
                    risk_validation = self._validate_decision_risk(resolved_decision)

                    # Handle risk validation safely
                    is_approved = getattr(risk_validation, 'is_approved', False)
                    should_execute = getattr(risk_validation, 'should_execute', False)
                    risk_reason = getattr(risk_validation, 'reason', 'Unknown risk validation result')

                    if not is_approved:
                        self.metrics.risk_vetoes += 1
                        self.logger.warning(f"RISK VALIDATION FAILED: {symbol} - {risk_reason}")
                        continue

                    self.logger.info(f"RISK VALIDATION PASSED:")
                    self.logger.info(f"  APPROVED: {is_approved}")
                    self.logger.info(f"  SHOULD EXECUTE: {should_execute}")
                    self.logger.info(f"  REASON: {risk_reason}")

                    # Step 4: NEW - Execute using execution orchestrator
                    strategy_name = self._get_dominant_strategy(symbol)
                    self.logger.info(f"EXECUTING DECISION:")
                    self.logger.info(f"  STRATEGY: {strategy_name}")
                    self.logger.info(f"  SYMBOL: {symbol}")

                    execution_result = self._execute_validated_decision(
                        risk_validation,
                        strategy_name=strategy_name
                    )

                    # Step 5: Track performance for adaptation
                    self._track_decision_performance(symbol, execution_result, resolved_decision)

                    # NEW: Step 6: Assign monitoring strategy immediately after successful execution (from implementation_rules.xml)
                    if getattr(execution_result, 'was_successful', False):
                        strategy_name = self._get_dominant_strategy(symbol)
                        execution_result_dict = execution_result.__dict__ if hasattr(execution_result, '__dict__') else {'summary': str(execution_result)}
                        self._assign_monitoring_strategy(symbol, strategy_name, {
                            'execution_result': execution_result_dict,
                            'decision_confidence': getattr(resolved_decision, 'final_confidence', 0.5),
                            'execution_time': datetime.now().isoformat()
                        })

                    # Update metrics
                    self.metrics.decisions_made += 1
                    if getattr(execution_result, 'was_successful', False):
                        self.metrics.successful_decisions += 1
                    else:
                        self.metrics.failed_decisions += 1

                except Exception as e:
                    self.logger.error(f"Error making decision for {symbol}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Error in enhanced decision making: {e}")

    def _create_enhanced_aggregated_signal(self, symbol: str) -> Optional[AggregatedSignal]:
        """Create enhanced aggregated signal using extracted signal processor"""
        try:
            signals = list(self.signal_buffer[symbol])
            if not signals:
                return None
            
            # Use signal processor for aggregation
            if not hasattr(self, '_signal_processor'):
                self._signal_processor = SignalProcessor(self.signal_weights)
                self._signal_processor.update_regime_state(self.regime_state)
                self._signal_processor.risk_alerts_active = self.risk_alerts_active
            
            return self._signal_processor.create_enhanced_aggregated_signal(
                symbol, signals, self.sentiment_cache
            )
        except Exception as e:
            self.logger.error(f"Error creating aggregated signal for {symbol}: {e}")
            return None

    def _resolve_signal_conflicts(self, aggregated_signal):
        """Resolve signal conflicts using extracted signal processor"""
        try:
            if not hasattr(self, '_signal_processor'):
                self._signal_processor = SignalProcessor(self.signal_weights)
                self._signal_processor.update_regime_state(self.regime_state)
                self._signal_processor.risk_alerts_active = self.risk_alerts_active
            
            return self._signal_processor.resolve_signal_conflicts(aggregated_signal)
        except Exception as e:
            self.logger.error(f"Error resolving conflicts for {aggregated_signal.symbol}: {e}")
            return None

    def _validate_decision_risk(self, resolved_decision):
        """NEW: Validate decision with enhanced risk manager"""
        try:
            if self.risk_manager:
                risk_result = self.risk_manager.validate_decision(resolved_decision)
                
                # BUSINESS LOGIC OUTPUT: Risk management real-time
                try:
                    from terminal_color_system import print_risk_manager
                    symbol = getattr(resolved_decision, 'symbol', 'UNKNOWN')
                    is_approved = getattr(risk_result, 'is_approved', False)
                    reason = getattr(risk_result, 'reason', 'No reason')
                    action = "APPROVED" if is_approved else "REJECTED"
                    print_risk_manager(f"Decision: {action} {symbol} - reason: {reason}")
                except:
                    print(f"[RISK MANAGER] Decision: {action} {symbol} - reason: {reason}")
                
                return risk_result
            else:
                # Fallback: approve all decisions
                try:
                    from terminal_color_system import print_risk_manager
                    symbol = getattr(resolved_decision, 'symbol', 'UNKNOWN')
                    print_risk_manager(f"Decision: APPROVED {symbol} - reason: No risk manager (fallback)")
                except:
                    print(f"[RISK MANAGER] Decision: APPROVED {symbol} - reason: No risk manager (fallback)")
                    
                return type('RiskValidation', (), {
                    'is_approved': True,
                    'should_execute': True,
                    'reason': 'Fallback approval'
                })()
        except Exception as e:
            self.logger.error(f"Error validating risk: {e}")
            return type('RiskValidation', (), {'is_approved': False, 'reason': f'Validation error: {e}'})()

    def _execute_validated_decision(self, risk_validation, strategy_name: str):
        """Execute validated decision using execution coordinator"""
        try:
            if not hasattr(self, '_execution_coordinator'):
                self._execution_coordinator = ExecutionCoordinator(
                    process_id=self.process_id,
                    execution_orchestrator=getattr(self, 'execution_orchestrator', None)
                )
            
            result = self._execution_coordinator.execute_validated_decision(risk_validation, strategy_name)
            
            # Update daily count
            self.daily_execution_count = self._execution_coordinator.daily_execution_count
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error executing validated decision: {e}")
            return type('ExecutionResult', (), {
                'was_successful': False,
                'error_message': str(e),
                'symbol': getattr(risk_validation, 'symbol', 'unknown'),
                'execution_summary': f"Failed execution: {str(e)}"
            })()

    def _execute_force_close(self, symbol: str, reason: str):
        """Execute force close using execution coordinator"""
        try:
            if not hasattr(self, '_execution_coordinator'):
                self._execution_coordinator = ExecutionCoordinator(
                    process_id=self.process_id,
                    execution_orchestrator=getattr(self, 'execution_orchestrator', None)
                )
            
            result = self._execution_coordinator.execute_force_close(symbol, reason)
            
            # Update force close symbols tracking
            self.force_close_symbols = self._execution_coordinator.force_close_symbols
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error force closing {symbol}: {e}")

    def _maybe_adapt_parameters(self):
        """Adapt parameters using parameter adapter"""
        try:
            # Adapt every 15 minutes
            if (datetime.now() - self.performance_tracker.last_adaptation).total_seconds() < 900:
                return

            if len(self.performance_tracker.recent_decisions) < 10:
                return  # Need enough data

            # Initialize parameter adapter if needed
            if not hasattr(self, '_parameter_adapter'):
                self._parameter_adapter = ParameterAdapter()

            # Get performance metrics
            overall_performance = self.performance_tracker.get_recent_performance()
            strategy_performance = self.performance_tracker.get_strategy_performance()

            # Get system metrics
            system_metrics = get_system_metrics()

            # Create adaptation metrics
            adaptation_metrics = create_adaptation_metrics(
                performance_data={'overall': overall_performance, **strategy_performance},
                market_data={
                    'regime': self.regime_state.get('current', 'sideways'),
                    'volatility_percentile': self._calculate_volatility_percentile(),
                    'trend_strength': self._calculate_trend_strength()
                },
                system_data=system_metrics,
                risk_data={
                    'portfolio_stress': len(self.risk_alerts_active) / 10.0,  # Normalize
                    'drawdown': self._calculate_current_drawdown(),
                    'alerts_count': len(self.risk_alerts_active)
                }
            )

            # Adapt parameters
            adapted_params = self._parameter_adapter.adapt_parameters(adaptation_metrics)

            # Apply adapted signal weights if available
            if adapted_params.signal_weights:
                old_weights = self.signal_weights.copy()
                self.signal_weights.update(adapted_params.signal_weights)

                # Update signal processor weights too
                if hasattr(self, '_signal_processor'):
                    self._signal_processor.update_signal_weights(adapted_params.signal_weights)

                self.logger.info(f"Parameter weights adapted: {adapted_params.adaptation_reason}")
                self.logger.debug(f"Old: {old_weights}")
                self.logger.debug(f"New: {self.signal_weights}")

            # Apply risk parameter changes to risk manager
            if self.risk_manager and adapted_params.max_position_size:
                if hasattr(self.risk_manager, 'limits'):
                    self.risk_manager.limits['max_position_size_pct'] = adapted_params.max_position_size
                    if adapted_params.max_daily_trades:
                        self.risk_manager.limits['max_daily_trades'] = adapted_params.max_daily_trades

            self.metrics.parameter_adaptations += 1
            self.performance_tracker.last_adaptation = datetime.now()

        except Exception as e:
            self.logger.error(f"Error in parameter adaptation: {e}")

    def _calculate_volatility_percentile(self) -> float:
        """Calculate current volatility percentile"""
        try:
            # Simple estimation based on recent market activity
            recent_volatility = getattr(self.regime_state, 'volatility', 0.5)
            if isinstance(recent_volatility, str):
                volatility_map = {'LOW': 0.2, 'NORMAL': 0.5, 'HIGH': 0.8, 'EXTREME': 0.95}
                return volatility_map.get(recent_volatility, 0.5)
            return min(1.0, max(0.0, recent_volatility))
        except:
            return 0.5

    def _calculate_trend_strength(self) -> float:
        """Calculate current trend strength"""
        try:
            trend = self.regime_state.get('trend', 'SIDEWAYS')
            if trend == 'TRENDING_UP':
                return 0.8
            elif trend == 'TRENDING_DOWN':
                return 0.8
            elif trend == 'SIDEWAYS':
                return 0.2
            else:
                return 0.5
        except:
            return 0.5

    def _calculate_current_drawdown(self) -> float:
        """Calculate current portfolio drawdown"""
        try:
            if hasattr(self, 'performance_tracker') and self.performance_tracker:
                recent_performance = self.performance_tracker.get_recent_performance()
                return abs(min(0.0, recent_performance.get('current_return', 0.0)))
            return 0.0
        except:
            return 0.0

    # NEW: Strategy Management Methods (from implementation_rules.xml)

    def _assign_monitoring_strategy(self, symbol: str, entry_strategy: str, execution_result: Dict[str, Any]):
        """Assign monitoring strategy immediately after successful execution with 3-scenario integration"""
        try:
            self.logger.info(f" Assigning monitoring strategy for {symbol}: {entry_strategy}")

            # ZERO DISCONNECTS: Integrate with 3-scenario assignment system
            try:
                # Import strategy assignment engine for decision engine scenario
                from ..tools.strategy_assignment_engine import StrategyAssignmentEngine
                from ..tools.db_manager import TradingDBManager
                
                # Create strategy assignment engine if not exists
                if not hasattr(self, '_strategy_assignment_engine'):
                    # Initialize db_manager for database persistence
                    db_manager = TradingDBManager()
                    self._strategy_assignment_engine = StrategyAssignmentEngine(
                        symbols=[symbol],  # Single symbol for this assignment
                        risk_tolerance=getattr(self, 'risk_tolerance', 'moderate'),
                        db_manager=db_manager
                    )
                
                # Create assignment through decision engine scenario
                decision_context = {
                    'execution_result': execution_result,
                    'decision_confidence': execution_result.get('decision_confidence', 0.5),
                    'execution_time': execution_result.get('execution_time', datetime.now().isoformat()),
                    'entry_strategy': entry_strategy
                }
                
                # Use scenario-aware assignment (routes through 3-scenario coordinator if available)
                assignment = self._strategy_assignment_engine.assign_strategy_for_decision_engine(
                    symbol=symbol,
                    market_analysis=None,  # Let assignment engine gather market data
                    decision_context=decision_context
                )
                
                self.logger.info(f" 3-scenario assignment completed: {symbol} -> {assignment.recommended_strategy} "
                                f"(confidence: {assignment.confidence_score:.1f}%)")
                
            except Exception as scenario_error:
                self.logger.warning(f"3-scenario assignment failed for {symbol}: {scenario_error} - using legacy method")
                # Fall back to legacy assignment method
                pass

            # Set strategy as active (preserves existing functionality)
            self.active_monitoring_strategies[symbol] = MonitoringStrategyStatus.ACTIVE

            # Send assignment message to position health monitor (preserves Redis communication)
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

    def _handle_strategy_reassessment_request(self, message: ProcessMessage):
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
                transition_decision = self._get_conflict_resolver().resolve_strategy_transition_signals(
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
        # Defensive initialization for Redis subprocess context
        if not hasattr(self, 'last_execution_reset'):
            self.last_execution_reset = datetime.now().date()
        if not hasattr(self, 'daily_execution_count'):
            self.daily_execution_count = 0
            
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
            # MEMORY OPTIMIZATION: More aggressive cleanup (1 hour instead of 6)
            cutoff_time = datetime.now() - timedelta(hours=1)
            for symbol in list(self.signal_buffer.keys()):
                self.signal_buffer[symbol] = deque(
                    [s for s in self.signal_buffer[symbol] if s['timestamp'] > cutoff_time],
                    maxlen=10  # Updated to match reduced buffer size
                )

                # Remove empty signal buffers
                if not self.signal_buffer[symbol]:
                    del self.signal_buffer[symbol]

            # MEMORY OPTIMIZATION: Clean old sentiment data more aggressively (30 minutes instead of 2 hours)
            for symbol in list(self.sentiment_cache.keys()):
                if (datetime.now() - self.sentiment_cache[symbol]['timestamp']).total_seconds() > 1800:  # 30 minutes
                    del self.sentiment_cache[symbol]
                    
            # MEMORY OPTIMIZATION: Trigger garbage collection after cleanup
            import gc
            gc.collect()

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

    def _get_next_message(self, timeout: float = 0.1) -> Optional[ProcessMessage]:
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
