"""
decision_engine/market_decision_engine_process.py

Market Decision Engine Process - Core Trading Decision Coordinator

This is the central decision-making process that:
1. Consumes all queue messages (STRATEGY_SIGNAL, SENTIMENT_UPDATE, HEALTH_ALERT, REGIME_CHANGE)
2. Aggregates signals with adaptive weighting
3. Makes trading decisions with risk validation
4. Coordinates execution through existing tools

Architecture reasoning:
- Inherits from BaseProcess for lifecycle management
- Uses aggressive processing (processes every message immediately)
- Integrates with existing tools (StrategySelector, PositionSizer, etc.)
- Two-checkpoint risk validation system
- Optimized for t3.micro resource constraints
"""

import time
import threading
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import deque, defaultdict
import logging
import numpy as np
from enum import Enum
import sys
import os

# Add project root for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import existing infrastructure
try:
    from multiprocess_infrastructure.base_process import BaseProcess, ProcessState
    from multiprocess_infrastructure.queue_manager import QueueMessage, MessageType, MessagePriority
except ImportError:
    # Fallback stubs for development
    from enum import Enum
    from abc import ABC, abstractmethod

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

    class QueueMessage:
        def __init__(self, message_type, priority, sender_id, recipient_id, payload):
            self.message_type = message_type
            self.priority = priority
            self.sender_id = sender_id
            self.recipient_id = recipient_id
            self.payload = payload
            self.timestamp = datetime.now()

    class BaseProcess(ABC):
        def __init__(self, process_id: str):
            self.process_id = process_id
            self.state = ProcessState.INITIALIZING
            self.logger = logging.getLogger(f"process.{process_id}")
            self.shared_state = None
            self.priority_queue = None


class DecisionType(Enum):
    """Types of trading decisions"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    CLOSE = "close"
    FORCE_CLOSE = "force_close"


class SignalWeight(Enum):
    """Signal weighting categories"""
    TECHNICAL = 0.30    # 30% - from strategy signals
    MARKET = 0.30       # 30% - from regime changes
    SENTIMENT = 0.30    # 30% - from news sentiment
    RISK = 0.10         # 10% - from risk management


@dataclass
class AggregatedSignal:
    """Represents an aggregated trading signal for a symbol"""
    symbol: str
    decision_type: DecisionType
    confidence: float  # 0-100
    signal_components: Dict[str, float]  # breakdown by source
    risk_score: float  # 0-100, higher = riskier
    timestamp: datetime = field(default_factory=datetime.now)

    # Signal source details
    technical_signals: List[Dict] = field(default_factory=list)
    sentiment_score: Optional[float] = None
    market_regime: Optional[str] = None
    risk_alerts: List[str] = field(default_factory=list)


@dataclass
class DecisionMetrics:
    """Tracking metrics for decision quality"""
    decisions_made: int = 0
    successful_decisions: int = 0
    failed_decisions: int = 0
    average_confidence: float = 0.0
    risk_vetoes: int = 0
    force_closes: int = 0
    processing_time_ms: float = 0.0


class MarketDecisionEngineProcess(BaseProcess):
    """
    Central decision engine that processes all trading signals and makes execution decisions

    Key Design Decisions:
    1. Aggressive processing - handle every message immediately for maximum responsiveness
    2. Adaptive signal weighting - adjust weights based on market regime and performance
    3. Two-checkpoint risk validation - initial + final veto power
    4. One position per symbol rule - simplifies conflict resolution
    5. Memory-efficient for t3.micro constraints
    """

    def __init__(self):
        super().__init__("market_decision_engine")

        # Signal aggregation - memory-efficient deques with size limits
        self.signal_buffer = defaultdict(lambda: deque(maxlen=50))  # 50 signals per symbol max
        self.sentiment_cache = {}  # symbol -> latest sentiment
        self.regime_state = {"current": "normal", "confidence": 0.5, "updated": datetime.now()}

        # Adaptive signal weights (start with defaults, adapt over time)
        self.signal_weights = {
            "technical": SignalWeight.TECHNICAL.value,
            "market": SignalWeight.MARKET.value,
            "sentiment": SignalWeight.SENTIMENT.value,
            "risk": SignalWeight.RISK.value
        }

        # Decision tracking and metrics
        self.metrics = DecisionMetrics()
        self.recent_decisions = deque(maxlen=100)  # Last 100 decisions for analysis
        self.performance_window = deque(maxlen=50)  # Track success rate

        # Risk management state
        self.risk_alerts_active = set()
        self.force_close_symbols = set()

        # Tool integration (will be injected by orchestrator)
        self.strategy_selector = None
        self.position_sizer = None
        self.db_manager = None
        self.risk_manager = None

        # Performance optimization
        self.last_weight_adjustment = datetime.now()
        self.weight_adjustment_interval = timedelta(minutes=15)  # Adjust weights every 15min

        self.logger.info("MarketDecisionEngine initialized with aggressive processing mode")

    def _initialize(self):
        """Initialize decision engine components"""
        try:
            self.logger.info("Initializing Market Decision Engine...")

            # Initialize tool connections (these would be injected in real system)
            self._initialize_tool_connections()

            # Load any persisted state/weights
            self._load_persisted_state()

            # Setup performance tracking
            self._setup_performance_tracking()

            self.state = ProcessState.RUNNING
            self.logger.info("Market Decision Engine initialization complete")

        except Exception as e:
            self.logger.error(f"Failed to initialize decision engine: {e}")
            self.state = ProcessState.ERROR
            raise

    def _initialize_tool_connections(self):
        """Initialize connections to existing trading tools"""
        # TODO: These will be injected by the orchestrator in real system
        # For now, create stub connections that show the integration pattern

        self.logger.info("Connecting to existing trading tools...")

        # Strategy Selector - for getting current strategies
        # self.strategy_selector = StrategySelector()

        # Position Sizer - for calculating position sizes
        # self.position_sizer = PositionSizer()

        # Database Manager - for recording trades
        # self.db_manager = TradingDBManager()

        # Risk Manager - for risk validation
        # self.risk_manager = RiskManager()

        self.logger.info("Tool connections established (stub mode)")

    def _process_cycle(self):
        """
        Main processing cycle - aggressive mode processes every available message

        Design reasoning:
        - Process ALL available messages in each cycle (aggressive mode)
        - Batch processing for efficiency on t3.micro
        - Immediate decision making after signal aggregation
        - Memory-conscious processing with deque limits
        """
        try:
            messages_processed = 0
            cycle_start = time.time()

            # Process all available messages aggressively
            while True:
                message = self._get_next_message(timeout=0.1)  # Short timeout for aggressive processing
                if message is None:
                    break

                self._process_message(message)
                messages_processed += 1

                # Safety check - don't exceed processing time budget
                if time.time() - cycle_start > 2.0:  # Max 2 seconds per cycle
                    self.logger.warning(f"Processing cycle taking too long, processed {messages_processed} messages")
                    break

            # After processing messages, make decisions for any ready symbols
            if messages_processed > 0:
                self._make_trading_decisions()

            # Periodic maintenance
            self._periodic_maintenance()

            # Update metrics
            cycle_time = (time.time() - cycle_start) * 1000
            self.metrics.processing_time_ms = cycle_time

            if messages_processed > 0:
                self.logger.debug(f"Processed {messages_processed} messages in {cycle_time:.1f}ms")

        except Exception as e:
            self.logger.error(f"Error in process cycle: {e}")
            self.error_count += 1

    def _process_message(self, message: QueueMessage):
        """Process individual queue message based on type"""
        try:
            if message.message_type == MessageType.STRATEGY_SIGNAL:
                self._handle_strategy_signal(message)
            elif message.message_type == MessageType.SENTIMENT_UPDATE:
                self._handle_sentiment_update(message)
            elif message.message_type == MessageType.HEALTH_ALERT:
                self._handle_health_alert(message)
            elif message.message_type == MessageType.REGIME_CHANGE:
                self._handle_regime_change(message)
            else:
                self.logger.warning(f"Unknown message type: {message.message_type}")

        except Exception as e:
            self.logger.error(f"Error processing {message.message_type}: {e}")

    def _handle_strategy_signal(self, message: QueueMessage):
        """Handle strategy signal messages"""
        payload = message.payload
        symbol = payload.get('symbol')
        signal_data = payload.get('signal', {})

        if not symbol:
            self.logger.warning("Strategy signal missing symbol")
            return

        # Add to signal buffer
        self.signal_buffer[symbol].append({
            'timestamp': message.timestamp,
            'sender': message.sender_id,
            'signal': signal_data,
            'confidence': signal_data.get('confidence', 0)
        })

        self.logger.debug(f"Added strategy signal for {symbol}, confidence: {signal_data.get('confidence', 0)}")

    def _handle_sentiment_update(self, message: QueueMessage):
        """Handle sentiment update messages"""
        payload = message.payload
        symbol = payload.get('symbol')
        sentiment_score = payload.get('sentiment_score')

        if symbol and sentiment_score is not None:
            self.sentiment_cache[symbol] = {
                'score': sentiment_score,
                'timestamp': message.timestamp,
                'confidence': payload.get('confidence', 0.5)
            }
            self.logger.debug(f"Updated sentiment for {symbol}: {sentiment_score}")

    def _handle_health_alert(self, message: QueueMessage):
        """Handle health alert messages - these can trigger force closes"""
        payload = message.payload
        alert_type = payload.get('alert_type')
        severity = payload.get('severity', 'warning')

        if severity == 'critical':
            self.risk_alerts_active.add(alert_type)

            # Check if this requires force closing positions
            if alert_type in ['position_risk', 'margin_call', 'system_overload']:
                affected_symbols = payload.get('symbols', [])
                self.force_close_symbols.update(affected_symbols)
                self.logger.warning(f"Force close triggered for {affected_symbols} due to {alert_type}")

        self.logger.info(f"Health alert: {alert_type} ({severity})")

    def _handle_regime_change(self, message: QueueMessage):
        """Handle market regime changes - these affect signal weighting"""
        payload = message.payload
        new_regime = payload.get('new_regime')
        confidence = payload.get('confidence', 0.5)

        if new_regime:
            old_regime = self.regime_state['current']
            self.regime_state = {
                'current': new_regime,
                'confidence': confidence,
                'updated': message.timestamp
            }

            # Adjust signal weights based on new regime
            self._adjust_weights_for_regime(new_regime, confidence)

            self.logger.info(f"Regime change: {old_regime} -> {new_regime} (confidence: {confidence})")

    def _make_trading_decisions(self):
        """
        Make trading decisions for symbols with sufficient signal data

        Decision Flow:
        1. Aggregate signals for each symbol
        2. Initial risk validation
        3. Generate trading decision
        4. Final risk veto check
        5. Execute if approved
        """
        for symbol in list(self.signal_buffer.keys()):
            try:
                # Skip if we don't have recent signals
                if not self.signal_buffer[symbol] or \
                   (datetime.now() - self.signal_buffer[symbol][-1]['timestamp']).seconds > 300:  # 5 min stale
                    continue

                # Skip if force close is active
                if symbol in self.force_close_symbols:
                    self._execute_force_close(symbol)
                    continue

                # Aggregate signals
                aggregated_signal = self._aggregate_signals_for_symbol(symbol)

                if aggregated_signal is None:
                    continue

                # Initial risk validation
                if not self._initial_risk_validation(aggregated_signal):
                    self.logger.info(f"Initial risk validation failed for {symbol}")
                    continue

                # Final risk manager veto check
                if not self._final_risk_veto_check(aggregated_signal):
                    self.metrics.risk_vetoes += 1
                    self.logger.info(f"Risk manager vetoed decision for {symbol}")
                    continue

                # Execute the decision
                self._execute_decision(aggregated_signal)

            except Exception as e:
                self.logger.error(f"Error making decision for {symbol}: {e}")

    def _aggregate_signals_for_symbol(self, symbol: str) -> Optional[AggregatedSignal]:
        """Aggregate all signals for a symbol into a single decision"""
        try:
            # Get recent technical signals
            recent_signals = [s for s in self.signal_buffer[symbol]
                            if (datetime.now() - s['timestamp']).seconds < 300]  # 5 minutes

            if not recent_signals:
                return None

            # Calculate weighted technical score
            technical_score = sum(s['confidence'] * s['signal'].get('strength', 1.0)
                                for s in recent_signals) / len(recent_signals)

            # Get sentiment score
            sentiment_data = self.sentiment_cache.get(symbol, {})
            sentiment_score = sentiment_data.get('score', 0.0)

            # Get market regime influence
            regime_multiplier = self._get_regime_multiplier()

            # Calculate final weighted score
            final_score = (
                technical_score * self.signal_weights['technical'] +
                sentiment_score * self.signal_weights['sentiment'] +
                regime_multiplier * self.signal_weights['market']
            )

            # Determine decision type and confidence
            if final_score > 70:
                decision_type = DecisionType.BUY
                confidence = min(final_score, 95)
            elif final_score < -70:
                decision_type = DecisionType.SELL
                confidence = min(abs(final_score), 95)
            else:
                decision_type = DecisionType.HOLD
                confidence = 50

            return AggregatedSignal(
                symbol=symbol,
                decision_type=decision_type,
                confidence=confidence,
                signal_components={
                    'technical': technical_score,
                    'sentiment': sentiment_score,
                    'market': regime_multiplier
                },
                risk_score=self._calculate_risk_score(symbol, recent_signals),
                technical_signals=recent_signals,
                sentiment_score=sentiment_score,
                market_regime=self.regime_state['current']
            )

        except Exception as e:
            self.logger.error(f"Error aggregating signals for {symbol}: {e}")
            return None

    def _get_next_message(self, timeout: float = 1.0) -> Optional[QueueMessage]:
        """Get next message from priority queue"""
        # TODO: This would use the actual queue manager in real system
        # For now, return None to simulate no messages
        return None

    def _initial_risk_validation(self, signal: AggregatedSignal) -> bool:
        """Initial risk validation checkpoint"""
        # Basic risk checks
        if signal.risk_score > 80:  # High risk threshold
            return False

        if signal.confidence < 60:  # Low confidence threshold
            return False

        # Check for conflicting risk alerts
        if any(alert in self.risk_alerts_active for alert in ['system_overload', 'margin_call']):
            return False

        return True

    def _final_risk_veto_check(self, signal: AggregatedSignal) -> bool:
        """Final risk manager veto - this is where risk manager has final say"""
        # TODO: Integration with actual risk manager
        # For now, implement basic checks

        # Risk manager would check:
        # - Portfolio exposure limits
        # - Position sizing limits
        # - Market conditions
        # - Account balance/margin

        return True  # Stub - approve all for testing

    def _execute_decision(self, signal: AggregatedSignal):
        """Execute trading decision"""
        try:
            self.logger.info(f"Executing {signal.decision_type.value} for {signal.symbol} "
                           f"(confidence: {signal.confidence:.1f}%)")

            # TODO: Integration with actual execution system
            # This would:
            # 1. Use PositionSizer to calculate position size
            # 2. Submit market order through Alpaca
            # 3. Record trade in database
            # 4. Update shared state

            # Record decision for tracking
            self.recent_decisions.append({
                'timestamp': datetime.now(),
                'symbol': signal.symbol,
                'decision': signal.decision_type.value,
                'confidence': signal.confidence,
                'components': signal.signal_components
            })

            self.metrics.decisions_made += 1

        except Exception as e:
            self.logger.error(f"Error executing decision for {signal.symbol}: {e}")
            self.metrics.failed_decisions += 1

    def _execute_force_close(self, symbol: str):
        """Execute force close of position"""
        self.logger.warning(f"Force closing position for {symbol}")
        # TODO: Implement force close logic
        self.force_close_symbols.discard(symbol)
        self.metrics.force_closes += 1

    def _get_regime_multiplier(self) -> float:
        """Get market regime influence multiplier"""
        regime = self.regime_state['current']
        confidence = self.regime_state['confidence']

        multipliers = {
            'bull': 1.2,
            'bear': -1.2,
            'sideways': 0.0,
            'volatile': 0.5,
            'normal': 1.0
        }

        return multipliers.get(regime, 1.0) * confidence

    def _calculate_risk_score(self, symbol: str, signals: List[Dict]) -> float:
        """Calculate risk score for symbol"""
        # Simple risk calculation based on signal volatility and confidence spread
        confidences = [s['confidence'] for s in signals]
        if not confidences:
            return 50.0

        confidence_std = np.std(confidences) if len(confidences) > 1 else 0
        avg_confidence = np.mean(confidences)

        # Higher std deviation = higher risk
        # Lower average confidence = higher risk
        risk_score = min(confidence_std * 2 + (100 - avg_confidence) * 0.3, 100)
        return risk_score

    def _adjust_weights_for_regime(self, regime: str, confidence: float):
        """Adjust signal weights based on market regime"""
        # Regime-based weight adjustments
        adjustments = {
            'bull': {'technical': 0.35, 'sentiment': 0.35, 'market': 0.25, 'risk': 0.05},
            'bear': {'technical': 0.25, 'sentiment': 0.25, 'market': 0.35, 'risk': 0.15},
            'volatile': {'technical': 0.20, 'sentiment': 0.20, 'market': 0.40, 'risk': 0.20},
            'normal': {'technical': 0.30, 'sentiment': 0.30, 'market': 0.30, 'risk': 0.10}
        }

        if regime in adjustments:
            # Apply adjustments with confidence weighting
            new_weights = adjustments[regime]
            for key in self.signal_weights:
                # Blend old and new weights based on confidence
                self.signal_weights[key] = (
                    self.signal_weights[key] * (1 - confidence) +
                    new_weights[key] * confidence
                )

            self.logger.info(f"Adjusted weights for {regime} regime: {self.signal_weights}")

    def _periodic_maintenance(self):
        """Periodic maintenance tasks"""
        now = datetime.now()

        # Clean old data every 5 minutes
        if not hasattr(self, '_last_cleanup') or (now - self._last_cleanup).seconds > 300:
            self._cleanup_old_data()
            self._last_cleanup = now

        # Update performance metrics
        if self.recent_decisions:
            # Calculate success rate (stub - would need actual trade outcomes)
            # For now, assume decisions with confidence > 75 are successful
            successful = sum(1 for d in self.recent_decisions if d['confidence'] > 75)
            self.metrics.successful_decisions = successful
            self.metrics.average_confidence = np.mean([d['confidence'] for d in self.recent_decisions])

    def _cleanup_old_data(self):
        """Clean up old data to manage memory usage"""
        cutoff = datetime.now() - timedelta(hours=1)

        # Clean sentiment cache
        self.sentiment_cache = {
            k: v for k, v in self.sentiment_cache.items()
            if v['timestamp'] > cutoff
        }

        # Clean risk alerts (keep for 30 minutes)
        alert_cutoff = datetime.now() - timedelta(minutes=30)
        # TODO: Clean risk alerts based on timestamp

    def _load_persisted_state(self):
        """Load any persisted state/weights"""
        # TODO: Load from database or file
        pass

    def _cleanup(self):
        """Cleanup resources on shutdown"""
        self.logger.info("Cleaning up Market Decision Engine...")
        # TODO: Save current state/weights
        # TODO: Close tool connections
        self.logger.info("Market Decision Engine cleanup complete")

    def get_process_info(self) -> Dict[str, Any]:
        """Return process information for monitoring"""
        return {
            'process_id': self.process_id,
            'state': self.state.value if hasattr(self.state, 'value') else str(self.state),
            'metrics': {
                'decisions_made': self.metrics.decisions_made,
                'success_rate': (self.metrics.successful_decisions / max(self.metrics.decisions_made, 1)) * 100,
                'risk_vetoes': self.metrics.risk_vetoes,
                'force_closes': self.metrics.force_closes,
                'avg_confidence': self.metrics.average_confidence,
                'processing_time_ms': self.metrics.processing_time_ms
            },
            'signal_weights': self.signal_weights.copy(),
            'current_regime': self.regime_state,
            'active_symbols': len(self.signal_buffer),
            'risk_alerts': len(self.risk_alerts_active),
            'force_close_symbols': len(self.force_close_symbols)
        }
