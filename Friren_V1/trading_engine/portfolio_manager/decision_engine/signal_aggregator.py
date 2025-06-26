"""
decision_engine/signal_aggregator.py

Adaptive Signal Aggregator - 30/30/30/10 Weighting System

Aggregates signals from multiple sources with adaptive weighting:
- Technical Signals (30% base): Strategy analyzer output
- Market Regime (30% base): Combined analyzer regime detection
- Sentiment (30% base): FinBERT sentiment analysis
- Risk (10% base): Position health monitor alerts

Key Features:
- Adaptive weight adjustment based on performance and market conditions
- Signal quality scoring and confidence weighting
- Memory-efficient for t3.micro constraints
- Detailed logging for decision audit trail
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import logging
import json


class SignalType(Enum):
    """Types of signals that can be aggregated"""
    TECHNICAL = "technical"
    SENTIMENT = "sentiment"
    MARKET_REGIME = "market_regime"
    RISK = "risk"


class SignalStrength(Enum):
    """Signal strength categories"""
    VERY_STRONG = 5    # 90-100% confidence
    STRONG = 4         # 75-89% confidence
    MODERATE = 3       # 60-74% confidence
    WEAK = 2           # 40-59% confidence
    VERY_WEAK = 1      # 0-39% confidence


@dataclass
class RawSignal:
    """Individual signal from any source"""
    signal_type: SignalType
    symbol: str
    direction: float  # -1.0 to 1.0 (bearish to bullish)
    confidence: float  # 0.0 to 1.0
    strength: float   # 0.0 to 1.0 (signal magnitude)
    source_id: str    # Which analyzer/process sent this
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def get_strength_category(self) -> SignalStrength:
        """Convert confidence to strength category"""
        conf_pct = self.confidence * 100
        if conf_pct >= 90:
            return SignalStrength.VERY_STRONG
        elif conf_pct >= 75:
            return SignalStrength.STRONG
        elif conf_pct >= 60:
            return SignalStrength.MODERATE
        elif conf_pct >= 40:
            return SignalStrength.WEAK
        else:
            return SignalStrength.VERY_WEAK


@dataclass
class AggregatedSignal:
    """Final aggregated signal for a symbol"""
    symbol: str
    final_direction: float  # -1.0 to 1.0 (final recommendation)
    final_confidence: float  # 0.0 to 1.0 (overall confidence)
    final_strength: float   # 0.0 to 1.0 (overall signal strength)

    # Component breakdowns
    technical_component: float
    sentiment_component: float
    market_component: float
    risk_component: float

    # Weighting applied
    applied_weights: Dict[str, float]

    # Signal quality metrics
    signal_count: int
    signal_agreement: float  # How much signals agree (-1 to 1)
    confidence_spread: float  # Variance in confidence levels

    # Supporting data
    raw_signals: List[RawSignal] = field(default_factory=list)
    aggregation_metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def get_recommendation(self) -> str:
        """Get human-readable recommendation"""
        if self.final_direction > 0.6:
            return "STRONG_BUY"
        elif self.final_direction > 0.3:
            return "BUY"
        elif self.final_direction > -0.3:
            return "HOLD"
        elif self.final_direction > -0.6:
            return "SELL"
        else:
            return "STRONG_SELL"


@dataclass
class AdaptiveWeights:
    """Adaptive weighting system configuration"""
    technical: float = 0.30
    sentiment: float = 0.30
    market: float = 0.30
    risk: float = 0.10

    # Adaptation parameters
    learning_rate: float = 0.05  # How quickly to adapt
    min_weight: float = 0.05     # Minimum weight for any category
    max_weight: float = 0.60     # Maximum weight for any category

    # Performance tracking
    performance_history: deque = field(default_factory=lambda: deque(maxlen=100))
    last_adjustment: datetime = field(default_factory=datetime.now)
    adjustment_frequency: timedelta = field(default=timedelta(minutes=15))

    def normalize(self):
        """Ensure weights sum to 1.0"""
        total = self.technical + self.sentiment + self.market + self.risk
        if total > 0:
            self.technical /= total
            self.sentiment /= total
            self.market /= total
            self.risk /= total

    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary for logging"""
        return {
            'technical': self.technical,
            'sentiment': self.sentiment,
            'market': self.market,
            'risk': self.risk
        }


class SignalAggregator:
    """
    Adaptive Signal Aggregator with 30/30/30/10 Base Weighting

    Design Philosophy:
    1. Start with balanced 30/30/30/10 weights
    2. Adapt weights based on signal source performance
    3. Consider market regime when adjusting weights
    4. Maintain minimum diversification (no weight < 5%)
    5. Quality over quantity - prefer fewer high-confidence signals
    """

    def __init__(self, symbol_limit: int = 50):
        self.logger = logging.getLogger("signal_aggregator")

        # Memory management for t3.micro
        self.symbol_limit = symbol_limit
        self.signal_buffer = defaultdict(lambda: deque(maxlen=20))  # 20 signals per symbol max

        # Adaptive weighting system
        self.weights = AdaptiveWeights()

        # Signal quality tracking
        self.source_performance = defaultdict(lambda: {
            'success_rate': 0.5,  # Start neutral
            'avg_confidence': 0.5,
            'signal_count': 0,
            'recent_accuracy': deque(maxlen=50)
        })

        # Market regime context (affects weight adaptation)
        self.current_regime = {
            'type': 'normal',
            'volatility': 'medium',
            'trend': 'sideways',
            'confidence': 0.5,
            'updated': datetime.now()
        }

        # Aggregation statistics
        self.stats = {
            'signals_processed': 0,
            'symbols_aggregated': 0,
            'weight_adjustments': 0,
            'avg_processing_time_ms': 0.0
        }

        self.logger.info("SignalAggregator initialized with adaptive 30/30/30/10 weighting")

    def add_signal(self, signal: RawSignal) -> bool:
        """
        Add a new signal to the aggregation buffer

        Args:
            signal: RawSignal from any source

        Returns:
            bool: True if signal was added successfully
        """
        try:
            # Validate signal
            if not self._validate_signal(signal):
                return False

            # Memory management - remove oldest signals if at limit
            if len(self.signal_buffer) >= self.symbol_limit:
                self._cleanup_old_signals()

            # Add to buffer
            self.signal_buffer[signal.symbol].append(signal)
            self.stats['signals_processed'] += 1

            # Update source performance tracking
            self._update_source_performance(signal)

            self.logger.debug(f"Added {signal.signal_type.value} signal for {signal.symbol} "
                            f"(confidence: {signal.confidence:.2f})")

            return True

        except Exception as e:
            self.logger.error(f"Error adding signal: {e}")
            return False

    def aggregate_signals(self, symbol: str, max_age_minutes: int = 10) -> Optional[AggregatedSignal]:
        """
        Aggregate all recent signals for a symbol

        Args:
            symbol: Symbol to aggregate signals for
            max_age_minutes: Maximum age of signals to consider

        Returns:
            AggregatedSignal or None if insufficient data
        """
        try:
            start_time = datetime.now()

            # Get recent signals for symbol
            recent_signals = self._get_recent_signals(symbol, max_age_minutes)

            if len(recent_signals) < 2:  # Need at least 2 signals
                return None

            # Group signals by type
            signals_by_type = self._group_signals_by_type(recent_signals)

            # Calculate component scores
            components = self._calculate_components(signals_by_type)

            # Apply adaptive weights
            current_weights = self._get_current_weights()

            # Calculate final weighted score
            final_direction = (
                components['technical'] * current_weights['technical'] +
                components['sentiment'] * current_weights['sentiment'] +
                components['market'] * current_weights['market'] +
                components['risk'] * current_weights['risk']
            )

            # Calculate overall confidence and strength
            confidence_metrics = self._calculate_confidence_metrics(recent_signals)

            # Create aggregated signal
            aggregated = AggregatedSignal(
                symbol=symbol,
                final_direction=final_direction,
                final_confidence=confidence_metrics['overall_confidence'],
                final_strength=confidence_metrics['overall_strength'],
                technical_component=components['technical'],
                sentiment_component=components['sentiment'],
                market_component=components['market'],
                risk_component=components['risk'],
                applied_weights=current_weights.copy(),
                signal_count=len(recent_signals),
                signal_agreement=confidence_metrics['agreement'],
                confidence_spread=confidence_metrics['spread'],
                raw_signals=recent_signals,
                aggregation_metadata={
                    'processing_time_ms': (datetime.now() - start_time).total_seconds() * 1000,
                    'regime_context': self.current_regime.copy(),
                    'source_breakdown': self._get_source_breakdown(recent_signals)
                }
            )

            self.stats['symbols_aggregated'] += 1

            self.logger.info(f"Aggregated {len(recent_signals)} signals for {symbol}: "
                           f"{aggregated.get_recommendation()} "
                           f"(confidence: {aggregated.final_confidence:.2f})")

            return aggregated

        except Exception as e:
            self.logger.error(f"Error aggregating signals for {symbol}: {e}")
            return None

    def update_regime_context(self, regime_type: str, volatility: str,
                            trend: str, confidence: float):
        """Update market regime context for weight adaptation"""
        self.current_regime = {
            'type': regime_type,
            'volatility': volatility,
            'trend': trend,
            'confidence': confidence,
            'updated': datetime.now()
        }

        # Trigger weight adaptation if regime changed significantly
        self._adapt_weights_for_regime()

        self.logger.info(f"Updated regime context: {regime_type}/{volatility}/{trend} "
                        f"(confidence: {confidence:.2f})")

    def update_source_performance(self, source_id: str, was_successful: bool,
                                actual_confidence: float):
        """Update performance tracking for a signal source"""
        perf = self.source_performance[source_id]
        perf['recent_accuracy'].append(1.0 if was_successful else 0.0)

        # Update rolling success rate
        if perf['recent_accuracy']:
            perf['success_rate'] = np.mean(perf['recent_accuracy'])

        perf['signal_count'] += 1

        # Trigger weight adaptation if performance changed significantly
        if len(perf['recent_accuracy']) >= 10:  # Need enough data
            self._adapt_weights_for_performance()

    def get_aggregation_stats(self) -> Dict[str, Any]:
        """Get aggregation statistics and current state"""
        return {
            'stats': self.stats.copy(),
            'current_weights': self.weights.to_dict(),
            'active_symbols': len(self.signal_buffer),
            'total_signals_buffered': sum(len(signals) for signals in self.signal_buffer.values()),
            'source_performance': {
                source: {
                    'success_rate': perf['success_rate'],
                    'signal_count': perf['signal_count']
                }
                for source, perf in self.source_performance.items()
            },
            'regime_context': self.current_regime.copy()
        }

    def _validate_signal(self, signal: RawSignal) -> bool:
        """Validate signal data"""
        if not signal.symbol or not signal.signal_type:
            return False
        if not (-1.0 <= signal.direction <= 1.0):
            return False
        if not (0.0 <= signal.confidence <= 1.0):
            return False
        if not (0.0 <= signal.strength <= 1.0):
            return False
        return True

    def _get_recent_signals(self, symbol: str, max_age_minutes: int) -> List[RawSignal]:
        """Get recent signals for a symbol"""
        if symbol not in self.signal_buffer:
            return []

        cutoff_time = datetime.now() - timedelta(minutes=max_age_minutes)
        return [s for s in self.signal_buffer[symbol] if s.timestamp > cutoff_time]

    def _group_signals_by_type(self, signals: List[RawSignal]) -> Dict[SignalType, List[RawSignal]]:
        """Group signals by their type"""
        grouped = defaultdict(list)
        for signal in signals:
            grouped[signal.signal_type].append(signal)
        return grouped

    def _calculate_components(self, signals_by_type: Dict[SignalType, List[RawSignal]]) -> Dict[str, float]:
        """Calculate component scores for each signal type"""
        components = {
            'technical': 0.0,
            'sentiment': 0.0,
            'market': 0.0,
            'risk': 0.0
        }

        # Technical signals
        tech_signals = signals_by_type.get(SignalType.TECHNICAL, [])
        if tech_signals:
            components['technical'] = self._aggregate_signal_group(tech_signals)

        # Sentiment signals
        sent_signals = signals_by_type.get(SignalType.SENTIMENT, [])
        if sent_signals:
            components['sentiment'] = self._aggregate_signal_group(sent_signals)

        # Market regime signals
        market_signals = signals_by_type.get(SignalType.MARKET_REGIME, [])
        if market_signals:
            components['market'] = self._aggregate_signal_group(market_signals)

        # Risk signals (negative impact)
        risk_signals = signals_by_type.get(SignalType.RISK, [])
        if risk_signals:
            components['risk'] = -abs(self._aggregate_signal_group(risk_signals))  # Risk is negative

        return components

    def _aggregate_signal_group(self, signals: List[RawSignal]) -> float:
        """Aggregate signals of the same type"""
        if not signals:
            return 0.0

        # Weight by confidence and recency
        weighted_sum = 0.0
        weight_sum = 0.0

        for signal in signals:
            # Recency weight (more recent = higher weight)
            age_minutes = (datetime.now() - signal.timestamp).total_seconds() / 60
            recency_weight = max(0.1, 1.0 - (age_minutes / 60))  # Decay over 1 hour

            # Combined weight
            total_weight = signal.confidence * signal.strength * recency_weight

            weighted_sum += signal.direction * total_weight
            weight_sum += total_weight

        return weighted_sum / weight_sum if weight_sum > 0 else 0.0

    def _calculate_confidence_metrics(self, signals: List[RawSignal]) -> Dict[str, float]:
        """Calculate overall confidence metrics"""
        if not signals:
            return {'overall_confidence': 0.0, 'overall_strength': 0.0,
                   'agreement': 0.0, 'spread': 1.0}

        confidences = [s.confidence for s in signals]
        strengths = [s.strength for s in signals]
        directions = [s.direction for s in signals]

        # Overall confidence (weighted average)
        overall_confidence = np.mean(confidences)
        overall_strength = np.mean(strengths)

        # Agreement (how much signals agree on direction)
        if len(directions) > 1:
            agreement = 1.0 - np.std(directions)  # Lower std = higher agreement
        else:
            agreement = 1.0

        # Confidence spread (lower is better)
        confidence_spread = np.std(confidences) if len(confidences) > 1 else 0.0

        return {
            'overall_confidence': overall_confidence,
            'overall_strength': overall_strength,
            'agreement': max(0.0, agreement),
            'spread': confidence_spread
        }

    def _get_current_weights(self) -> Dict[str, float]:
        """Get current adaptive weights"""
        return self.weights.to_dict()

    def _adapt_weights_for_regime(self):
        """Adapt weights based on current market regime"""
        regime = self.current_regime['type']
        volatility = self.current_regime['volatility']
        confidence = self.current_regime['confidence']

        # Regime-based adjustments
        if regime == 'bull' and confidence > 0.7:
            # In bull markets, emphasize technical and sentiment
            self._adjust_weights({'technical': 0.35, 'sentiment': 0.35, 'market': 0.20, 'risk': 0.10})
        elif regime == 'bear' and confidence > 0.7:
            # In bear markets, emphasize risk and market signals
            self._adjust_weights({'technical': 0.20, 'sentiment': 0.20, 'market': 0.40, 'risk': 0.20})
        elif volatility == 'high':
            # High volatility - increase risk weighting
            self._adjust_weights({'technical': 0.25, 'sentiment': 0.25, 'market': 0.30, 'risk': 0.20})

        # Only adjust if enough time has passed
        if datetime.now() - self.weights.last_adjustment > self.weights.adjustment_frequency:
            self.weights.last_adjustment = datetime.now()
            self.stats['weight_adjustments'] += 1

    def _adapt_weights_for_performance(self):
        """Adapt weights based on source performance"""
        # Get performance by signal type
        type_performance = defaultdict(list)

        for source_id, perf in self.source_performance.items():
            # Map source to signal type (would need better mapping in real system)
            if 'strategy' in source_id or 'technical' in source_id:
                type_performance['technical'].append(perf['success_rate'])
            elif 'sentiment' in source_id or 'finbert' in source_id:
                type_performance['sentiment'].append(perf['success_rate'])
            elif 'regime' in source_id or 'market' in source_id:
                type_performance['market'].append(perf['success_rate'])
            elif 'risk' in source_id or 'health' in source_id:
                type_performance['risk'].append(perf['success_rate'])

        # Adjust weights based on relative performance
        adjustments = {}
        for signal_type, success_rates in type_performance.items():
            if success_rates:
                avg_success = np.mean(success_rates)
                # Better performance = higher weight (with limits)
                if avg_success > 0.6:  # Good performance
                    adjustments[signal_type] = min(1.1, 1.0 + (avg_success - 0.5))
                elif avg_success < 0.4:  # Poor performance
                    adjustments[signal_type] = max(0.8, avg_success + 0.4)

        if adjustments:
            self._apply_performance_adjustments(adjustments)

    def _adjust_weights(self, target_weights: Dict[str, float]):
        """Gradually adjust weights toward targets"""
        learning_rate = self.weights.learning_rate

        for weight_type, target in target_weights.items():
            current = getattr(self.weights, weight_type)
            new_weight = current + learning_rate * (target - current)

            # Apply constraints
            new_weight = max(self.weights.min_weight,
                           min(self.weights.max_weight, new_weight))

            setattr(self.weights, weight_type, new_weight)

        # Normalize to ensure sum = 1.0
        self.weights.normalize()

    def _apply_performance_adjustments(self, adjustments: Dict[str, float]):
        """Apply performance-based weight adjustments"""
        for weight_type, multiplier in adjustments.items():
            if hasattr(self.weights, weight_type):
                current = getattr(self.weights, weight_type)
                new_weight = current * multiplier

                # Apply constraints
                new_weight = max(self.weights.min_weight,
                               min(self.weights.max_weight, new_weight))

                setattr(self.weights, weight_type, new_weight)

        # Normalize
        self.weights.normalize()

    def _update_source_performance(self, signal: RawSignal):
        """Update performance tracking for signal source"""
        source_id = signal.source_id
        perf = self.source_performance[source_id]

        # Update average confidence
        perf['avg_confidence'] = (
            (perf['avg_confidence'] * perf['signal_count'] + signal.confidence) /
            (perf['signal_count'] + 1)
        )

    def _cleanup_old_signals(self):
        """Clean up old signals to manage memory"""
        # Remove symbols with no recent signals
        cutoff_time = datetime.now() - timedelta(hours=2)
        symbols_to_remove = []

        for symbol, signals in self.signal_buffer.items():
            if not signals or signals[-1].timestamp < cutoff_time:
                symbols_to_remove.append(symbol)

        for symbol in symbols_to_remove:
            del self.signal_buffer[symbol]

        self.logger.debug(f"Cleaned up {len(symbols_to_remove)} old symbol buffers")

    def _get_source_breakdown(self, signals: List[RawSignal]) -> Dict[str, int]:
        """Get breakdown of signals by source"""
        breakdown = defaultdict(int)
        for signal in signals:
            breakdown[signal.source_id] += 1
        return dict(breakdown)
