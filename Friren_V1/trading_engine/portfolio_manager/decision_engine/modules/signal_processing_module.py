"""
Signal Processing Module for Enhanced Decision Engine
====================================================

Handles signal aggregation, conflict resolution, message processing, and
signal analysis for the enhanced decision engine. This module provides
intelligent signal processing with weighted analysis and conflict resolution.

Extracted from decision_engine.py for better maintainability.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict

# Import infrastructure
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    ProcessMessage, MessagePriority, create_process_message
)

# Import enhanced components
from ..conflict_resolver import ConflictResolver
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator


class DecisionType(Enum):
    """Types of trading decisions"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    CLOSE = "close"
    FORCE_CLOSE = "force_close"


class SignalWeight(Enum):
    """Signal weighting categories - adaptive weighting system"""
    TECHNICAL = 0.30    # 30% - from strategy signals
    MARKET = 0.30       # 30% - from regime changes  
    SENTIMENT = 0.30    # 30% - from news sentiment
    RISK = 0.10         # 10% - from risk management


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
class AggregatedSignal:
    """Enhanced aggregated trading signal"""
    symbol: str
    decision_type: DecisionType
    confidence: float  # 0-100
    weight: float
    sources: List[str]
    signal_data: Dict[str, Any]
    timestamp: datetime
    
    # Enhanced fields
    risk_assessment: Optional[Dict[str, Any]] = None
    market_regime_context: Optional[str] = None
    sentiment_context: Optional[Dict[str, Any]] = None
    technical_context: Optional[Dict[str, Any]] = None
    execution_priority: MessagePriority = MessagePriority.NORMAL
    
    # Conflict resolution
    conflicts_detected: List[str] = field(default_factory=list)
    resolution_applied: Optional[str] = None


@dataclass
class SignalProcessingMetrics:
    """Signal processing performance metrics"""
    signals_processed: int = 0
    conflicts_resolved: int = 0
    messages_processed: int = 0
    processing_time_ms: float = 0.0
    average_confidence: float = 0.0
    signal_distribution: Dict[str, int] = field(default_factory=dict)
    error_count: int = 0
    timestamp: datetime = field(default_factory=datetime.now)


class SignalProcessingModule:
    """
    Signal Processing Module
    
    Handles comprehensive signal processing including message parsing,
    signal aggregation, conflict resolution, and weighted analysis
    for intelligent trading decisions.
    """
    
    def __init__(self, output_coordinator: Optional[OutputCoordinator] = None,
                 process_id: str = "signal_processing"):
        """
        Initialize signal processing module
        
        Args:
            output_coordinator: Output coordinator for standardized output
            process_id: Process identifier for output
        """
        self.output_coordinator = output_coordinator
        self.process_id = process_id
        self.logger = logging.getLogger(__name__)
        
        # Core components  
        self.conflict_resolver: Optional[ConflictResolver] = None
        
        # Signal storage and tracking
        self.pending_signals: Dict[str, List[AggregatedSignal]] = defaultdict(list)
        self.signal_history: deque = deque(maxlen=100)
        self.transition_signals: Dict[str, List[TransitionSignal]] = defaultdict(list)
        
        # Processing state
        self.last_processing_time: Optional[datetime] = None
        self.metrics = SignalProcessingMetrics()
        
        # Configuration - EXTENDED for XGBoost integration: XGBoost runs every 15 minutes, need 20-minute window
        self.signal_timeout_minutes = 20
        self.max_signals_per_symbol = 10
        self.confidence_threshold = 60.0
        
        # Message processing tracking
        self.processed_message_ids = set()
        self.duplicate_count = 0
        
    def initialize(self) -> bool:
        """
        Initialize signal processing components
        
        Returns:
            True if initialization successful
        """
        try:
            self.logger.info("Initializing signal processing module...")
            
            # Initialize conflict resolver on demand for memory optimization
            # Will be loaded when first needed
            
            self.logger.info("Signal processing module initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize signal processing module: {e}")
            return False
    
    def process_message(self, message: ProcessMessage) -> Optional[AggregatedSignal]:
        """
        Process incoming message and convert to aggregated signal
        
        Args:
            message: Incoming process message
            
        Returns:
            Aggregated signal if processing successful
        """
        try:
            # Check for duplicate messages
            if hasattr(message, 'message_id') and message.message_id in self.processed_message_ids:
                self.duplicate_count += 1
                self.logger.debug(f"Duplicate message detected: {message.message_id}")
                return None
            
            start_time = time.time()
            
            # Route message based on type
            signal = None
            if message.message_type == "STRATEGY_SIGNAL":
                signal = self._process_strategy_signal(message)
            elif message.message_type == "SENTIMENT_SIGNAL":
                signal = self._process_sentiment_signal(message)
            elif message.message_type == "REGIME_CHANGE":
                signal = self._process_regime_signal(message)
            elif message.message_type == "RISK_ALERT":
                signal = self._process_risk_signal(message)
            elif message.message_type == "TRADING_RECOMMENDATION":
                signal = self._process_recommendation_signal(message)
            else:
                self.logger.debug(f"Unknown message type: {message.message_type}")
                return None
            
            if signal:
                # Store signal for aggregation
                self._store_signal(signal)
                
                # Record processing metrics
                if hasattr(message, 'message_id'):
                    self.processed_message_ids.add(message.message_id)
                
                processing_time = (time.time() - start_time) * 1000
                self.metrics.processing_time_ms += processing_time
                self.metrics.messages_processed += 1
                
                self.logger.debug(f"Processed {message.message_type} signal for {signal.symbol} in {processing_time:.1f}ms")
                
                return signal
            
        except Exception as e:
            self.logger.error(f"Error processing message {message.message_type}: {e}")
            self.metrics.error_count += 1
            
        return None
    
    def _process_strategy_signal(self, message: ProcessMessage) -> Optional[AggregatedSignal]:
        """Process strategy signal message"""
        try:
            data = message.payload
            symbol = data.get('symbol')
            signal_data = data.get('signal', {})
            
            if not symbol or not signal_data:
                return None
            
            action = signal_data.get('action', 'HOLD').upper()
            confidence = signal_data.get('confidence', 0.0)
            
            # Convert action to decision type
            decision_type = DecisionType.HOLD
            if action == 'BUY':
                decision_type = DecisionType.BUY
            elif action == 'SELL':
                decision_type = DecisionType.SELL
            
            return AggregatedSignal(
                symbol=symbol,
                decision_type=decision_type,
                confidence=confidence,
                weight=SignalWeight.TECHNICAL.value,
                sources=[message.sender],
                signal_data={
                    'strategy_name': signal_data.get('strategy'),
                    'reasoning': signal_data.get('reasoning', ''),
                    'metadata': signal_data.get('metadata', {})
                },
                timestamp=datetime.now(),
                technical_context=signal_data
            )
            
        except Exception as e:
            self.logger.error(f"Error processing strategy signal: {e}")
            return None
    
    def _process_sentiment_signal(self, message: ProcessMessage) -> Optional[AggregatedSignal]:
        """Process sentiment signal message"""
        try:
            data = message.payload
            symbol = data.get('symbol')
            
            if not symbol:
                return None
            
            # Extract sentiment data
            sentiment = data.get('sentiment', 'neutral').lower()
            confidence = data.get('confidence', 0.0)
            impact_score = data.get('impact_score', 0.0)
            
            # Convert sentiment to decision type
            decision_type = DecisionType.HOLD
            if sentiment == 'positive' and confidence > 70:
                decision_type = DecisionType.BUY
            elif sentiment == 'negative' and confidence > 70:
                decision_type = DecisionType.SELL
            
            return AggregatedSignal(
                symbol=symbol,
                decision_type=decision_type,
                confidence=confidence * impact_score,  # Weight by impact
                weight=SignalWeight.SENTIMENT.value,
                sources=[message.sender],
                signal_data={
                    'sentiment': sentiment,
                    'impact_score': impact_score,
                    'article_count': data.get('article_count', 1)
                },
                timestamp=datetime.now(),
                sentiment_context=data
            )
            
        except Exception as e:
            self.logger.error(f"Error processing sentiment signal: {e}")
            return None
    
    def _process_regime_signal(self, message: ProcessMessage) -> Optional[AggregatedSignal]:
        """Process market regime signal message"""
        try:
            data = message.payload
            regime = data.get('regime')
            confidence = data.get('confidence', 0.0)
            
            if not regime:
                return None
            
            # Create signals for all monitored symbols based on regime
            # This is a broadcast signal that affects all positions
            return AggregatedSignal(
                symbol="*",  # Wildcard for all symbols
                decision_type=DecisionType.HOLD,  # Regime changes modify risk, not direct action
                confidence=confidence,
                weight=SignalWeight.MARKET.value,
                sources=[message.sender],
                signal_data={
                    'regime': regime,
                    'previous_regime': data.get('previous_regime'),
                    'volatility': data.get('volatility_regime', 'NORMAL')
                },
                timestamp=datetime.now(),
                market_regime_context=regime
            )
            
        except Exception as e:
            self.logger.error(f"Error processing regime signal: {e}")
            return None
    
    def _process_risk_signal(self, message: ProcessMessage) -> Optional[AggregatedSignal]:
        """Process risk alert signal message"""
        try:
            data = message.payload
            symbol = data.get('symbol')
            alert_type = data.get('alert_type')
            severity = data.get('severity', 'MEDIUM')
            
            if not symbol or not alert_type:
                return None
            
            # Risk signals typically suggest caution or position reduction
            decision_type = DecisionType.HOLD
            confidence = 50.0
            
            if severity == 'CRITICAL':
                decision_type = DecisionType.SELL
                # Calculate confidence based on actual risk metrics instead of hardcoded value
                risk_metrics = data.get('risk_metrics', {})
                base_confidence = 65.0  # Base confidence for critical risk signals
                
                # Adjust confidence based on risk severity indicators
                drawdown_severity = risk_metrics.get('max_drawdown', 0.0)
                volatility_spike = risk_metrics.get('volatility_ratio', 1.0)
                
                # Higher drawdown and volatility indicate higher confidence in risk signal
                confidence_adjustment = min(25.0, (drawdown_severity * 100) + (volatility_spike - 1.0) * 15)
                confidence = min(95.0, base_confidence + confidence_adjustment)
            elif severity == 'HIGH':
                decision_type = DecisionType.SELL
                # Calculate confidence based on risk metrics for HIGH severity
                risk_metrics = data.get('risk_metrics', {})
                base_confidence = 55.0  # Base confidence for high risk signals
                
                # Adjust confidence based on risk indicators
                volatility_factor = risk_metrics.get('volatility_ratio', 1.0)
                trend_reversal = risk_metrics.get('trend_reversal_strength', 0.0)
                
                # Higher risk indicators increase confidence in SELL signal
                confidence_adjustment = min(20.0, (volatility_factor - 1.0) * 20 + trend_reversal * 10)
                confidence = min(85.0, base_confidence + confidence_adjustment)
            
            return AggregatedSignal(
                symbol=symbol,
                decision_type=decision_type,
                confidence=confidence,
                weight=SignalWeight.RISK.value,
                sources=[message.sender],
                signal_data={
                    'alert_type': alert_type,
                    'severity': severity,
                    'risk_metrics': data.get('risk_metrics', {})
                },
                timestamp=datetime.now(),
                risk_assessment=data,
                execution_priority=MessagePriority.HIGH if severity == 'CRITICAL' else MessagePriority.NORMAL
            )
            
        except Exception as e:
            self.logger.error(f"Error processing risk signal: {e}")
            return None
    
    def _process_recommendation_signal(self, message: ProcessMessage) -> Optional[AggregatedSignal]:
        """Process trading recommendation signal message"""
        try:
            data = message.payload
            symbol = data.get('symbol')
            action = data.get('action', 'HOLD').upper()
            confidence = data.get('confidence', 0.0)
            
            if not symbol:
                return None
            
            # Convert action to decision type
            decision_type = DecisionType.HOLD
            if action == 'BUY':
                decision_type = DecisionType.BUY
            elif action == 'SELL':
                decision_type = DecisionType.SELL
            
            return AggregatedSignal(
                symbol=symbol,
                decision_type=decision_type,
                confidence=confidence,
                weight=SignalWeight.SENTIMENT.value,  # Recommendations often come from sentiment analysis
                sources=[message.sender],
                signal_data={
                    'reasoning': data.get('reasoning', ''),
                    'expected_return': data.get('expected_return', 0.0),
                    'risk_score': data.get('risk_score', 0.0),
                    'time_horizon': data.get('time_horizon', 'short')
                },
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Error processing recommendation signal: {e}")
            return None
    
    def _store_signal(self, signal: AggregatedSignal):
        """Store signal for aggregation"""
        try:
            # Store in pending signals
            self.pending_signals[signal.symbol].append(signal)
            
            # Limit signals per symbol
            if len(self.pending_signals[signal.symbol]) > self.max_signals_per_symbol:
                # Remove oldest signal
                self.pending_signals[signal.symbol].pop(0)
            
            # Add to history
            self.signal_history.append(signal)
            
            # Update metrics
            self.metrics.signals_processed += 1
            decision_key = signal.decision_type.value
            self.metrics.signal_distribution[decision_key] = self.metrics.signal_distribution.get(decision_key, 0) + 1
            
        except Exception as e:
            self.logger.error(f"Error storing signal: {e}")
    
    def aggregate_signals_for_symbol(self, symbol: str) -> Optional[AggregatedSignal]:
        """
        Aggregate all pending signals for a symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Aggregated signal or None
        """
        try:
            signals = self.pending_signals.get(symbol, [])
            if not signals:
                return None
            
            # Remove expired signals
            current_time = datetime.now()
            valid_signals = [
                s for s in signals 
                if (current_time - s.timestamp).total_seconds() < (self.signal_timeout_minutes * 60)
            ]
            
            if not valid_signals:
                self.pending_signals[symbol] = []
                return None
            
            # Check for conflicts and resolve if needed
            conflicts = self._detect_conflicts(valid_signals)
            if conflicts:
                resolved_signals = self._resolve_conflicts(valid_signals, conflicts)
                if resolved_signals:
                    valid_signals = resolved_signals
                else:
                    self.logger.warning(f"Could not resolve conflicts for {symbol}, using original signals")
            
            # Aggregate valid signals
            aggregated = self._compute_weighted_aggregation(symbol, valid_signals)
            
            if aggregated:
                # Clear processed signals
                self.pending_signals[symbol] = []
                self.logger.debug(f"Aggregated {len(valid_signals)} signals for {symbol}: {aggregated.decision_type.value} (confidence: {aggregated.confidence:.1f})")
            
            return aggregated
            
        except Exception as e:
            self.logger.error(f"Error aggregating signals for {symbol}: {e}")
            return None
    
    def _detect_conflicts(self, signals: List[AggregatedSignal]) -> List[str]:
        """Detect conflicts in signal list"""
        try:
            conflicts = []
            
            # Check for opposing decisions
            buy_signals = [s for s in signals if s.decision_type == DecisionType.BUY]
            sell_signals = [s for s in signals if s.decision_type == DecisionType.SELL]
            
            if buy_signals and sell_signals:
                conflicts.append("opposing_decisions")
            
            # Check for high confidence disagreement
            if len(signals) >= 2:
                high_conf_signals = [s for s in signals if s.confidence > 80]
                if len(high_conf_signals) >= 2:
                    decisions = [s.decision_type for s in high_conf_signals]
                    if len(set(decisions)) > 1:
                        conflicts.append("high_confidence_disagreement")
            
            # Check for timing conflicts (signals too close together from same source)
            source_times = defaultdict(list)
            for signal in signals:
                for source in signal.sources:
                    source_times[source].append(signal.timestamp)
            
            for source, times in source_times.items():
                if len(times) > 1:
                    times.sort()
                    for i in range(1, len(times)):
                        if (times[i] - times[i-1]).total_seconds() < 60:  # Less than 1 minute apart
                            conflicts.append(f"rapid_signals_from_{source}")
                            break
            
            return conflicts
            
        except Exception as e:
            self.logger.error(f"Error detecting conflicts: {e}")
            return []
    
    def _resolve_conflicts(self, signals: List[AggregatedSignal], conflicts: List[str]) -> Optional[List[AggregatedSignal]]:
        """Resolve conflicts using conflict resolver"""
        try:
            # Initialize conflict resolver if needed
            if not self.conflict_resolver:
                self.conflict_resolver = ConflictResolver(model_path="models/demo_xgb_model.json")
                self.logger.info("Conflict resolver loaded on demand")
            
            # Use conflict resolver to resolve conflicts
            resolved_signals = self.conflict_resolver.resolve_signal_conflicts(signals, conflicts)
            
            if resolved_signals:
                self.metrics.conflicts_resolved += 1
                self.logger.info(f"Resolved {len(conflicts)} conflicts, resulting in {len(resolved_signals)} signals")
                
                # Mark signals as resolved
                for signal in resolved_signals:
                    signal.conflicts_detected = conflicts
                    signal.resolution_applied = "ml_conflict_resolution"
            
            return resolved_signals
            
        except Exception as e:
            self.logger.error(f"Error resolving conflicts: {e}")
            return None
    
    def _compute_weighted_aggregation(self, symbol: str, signals: List[AggregatedSignal]) -> Optional[AggregatedSignal]:
        """Compute weighted aggregation of signals"""
        try:
            if not signals:
                return None
            
            # Calculate weighted decision scores
            decision_scores = defaultdict(float)
            total_weight = 0.0
            
            for signal in signals:
                weight = signal.weight * (signal.confidence / 100.0)
                decision_scores[signal.decision_type] += weight
                total_weight += weight
            
            # Find highest scoring decision
            if not decision_scores:
                return None
            
            best_decision = max(decision_scores.items(), key=lambda x: x[1])
            final_decision = best_decision[0]
            decision_score = best_decision[1]
            
            # Calculate final confidence
            final_confidence = min(100.0, (decision_score / total_weight) * 100.0) if total_weight > 0 else 0.0
            
            # Only proceed if confidence meets threshold
            if final_confidence < self.confidence_threshold:
                final_decision = DecisionType.HOLD
                final_confidence = max(50.0, final_confidence)
            
            # Aggregate sources and data
            all_sources = []
            aggregated_data = {}
            
            for signal in signals:
                all_sources.extend(signal.sources)
                aggregated_data.update(signal.signal_data)
            
            # Create aggregated signal
            aggregated = AggregatedSignal(
                symbol=symbol,
                decision_type=final_decision,
                confidence=final_confidence,
                weight=total_weight / len(signals),
                sources=list(set(all_sources)),
                signal_data=aggregated_data,
                timestamp=datetime.now(),
                execution_priority=MessagePriority.HIGH if final_confidence > 85 else MessagePriority.NORMAL
            )
            
            return aggregated
            
        except Exception as e:
            self.logger.error(f"Error computing weighted aggregation: {e}")
            return None
    
    def get_pending_symbols(self) -> List[str]:
        """Get list of symbols with pending signals"""
        return [symbol for symbol, signals in self.pending_signals.items() if signals]
    
    def clear_expired_signals(self):
        """Clear expired signals from pending queue"""
        try:
            current_time = datetime.now()
            timeout_seconds = self.signal_timeout_minutes * 60
            
            for symbol in list(self.pending_signals.keys()):
                valid_signals = [
                    s for s in self.pending_signals[symbol]
                    if (current_time - s.timestamp).total_seconds() < timeout_seconds
                ]
                self.pending_signals[symbol] = valid_signals
                
                if not valid_signals:
                    del self.pending_signals[symbol]
            
        except Exception as e:
            self.logger.error(f"Error clearing expired signals: {e}")
    
    def get_metrics(self) -> SignalProcessingMetrics:
        """Get current signal processing metrics"""
        return self.metrics
    
    def cleanup(self):
        """Cleanup module resources"""
        try:
            self.pending_signals.clear()
            self.signal_history.clear()
            self.transition_signals.clear()
            self.processed_message_ids.clear()
            
            if self.conflict_resolver:
                if hasattr(self.conflict_resolver, 'cleanup'):
                    self.conflict_resolver.cleanup()
                self.conflict_resolver = None
            
            self.logger.info("Signal processing module cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during signal processing cleanup: {e}")