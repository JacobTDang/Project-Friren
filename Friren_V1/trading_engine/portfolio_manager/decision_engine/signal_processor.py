"""
Signal Processor - Extracted from Decision Engine

This module handles signal aggregation, conflict resolution, and signal processing logic.
Provides centralized signal analysis with market-driven calculations.

Features:
- Signal aggregation with adaptive weights
- Conflict resolution between multiple signals
- Market regime-aware signal processing
- Real-time signal quality assessment
"""

import logging
import statistics
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import numpy as np

# Import market metrics for dynamic calculations
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


class DecisionType(Enum):
    """Trading decision types"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class AggregatedSignal:
    """Aggregated signal from multiple sources"""
    symbol: str
    decision_type: DecisionType
    confidence: float
    signal_components: Dict[str, float]
    risk_score: float
    technical_signals: List[str]
    sentiment_score: Optional[float]
    market_regime: Optional[str]
    risk_alerts: List[str]
    signal_agreement: float
    uncertainty: float
    final_direction: float
    final_strength: float


class SignalProcessor:
    """Processes and aggregates trading signals with market-driven calculations"""

    def __init__(self, signal_weights: Optional[Dict[str, float]] = None):
        self.logger = logging.getLogger(f"{__name__}.SignalProcessor")
        
        # Default signal weights - can be adapted dynamically
        self.signal_weights = signal_weights or {
            'technical': 0.4,
            'sentiment': 0.3,
            'market': 0.2,
            'risk': 0.1
        }
        
        # State tracking
        self.regime_state = {
            'current': 'sideways',
            'confidence': 0.5,
            'updated': datetime.now(),
            'volatility': 'NORMAL',
            'trend': 'SIDEWAYS'
        }
        
        self.risk_alerts_active = set()

    def create_enhanced_aggregated_signal(self, 
                                        symbol: str, 
                                        signals: List[Dict[str, Any]], 
                                        sentiment_cache: Dict[str, Any]) -> Optional[AggregatedSignal]:
        """Create enhanced aggregated signal with conflict analysis and position-aware logic"""
        try:
            if not signals:
                return None

            # Get sentiment data
            sentiment = sentiment_cache.get(symbol, {})

            # Calculate signal components with adaptive weights
            technical_component = self._calculate_technical_component(signals)
            sentiment_component = sentiment.get('score', 0) * self.signal_weights['sentiment']
            market_component = self._calculate_market_component(symbol)
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

    def resolve_signal_conflicts(self, aggregated_signal: AggregatedSignal):
        """Resolve signal conflicts using market-driven conflict resolution"""
        try:
            # Get market metrics for conflict resolution context
            market_metrics = get_all_metrics(aggregated_signal.symbol)
            
            # Analyze signal disagreement level
            disagreement_level = 1.0 - aggregated_signal.signal_agreement
            
            # High disagreement requires conservative approach
            if disagreement_level > 0.7:
                # Use market volatility to determine resolution approach
                if market_metrics and market_metrics.volatility and market_metrics.volatility > 0.5:
                    # High volatility + high disagreement = HOLD
                    return self._create_resolved_decision(
                        aggregated_signal, 
                        DecisionType.HOLD, 
                        aggregated_signal.confidence * 0.5,
                        'high_volatility_disagreement'
                    )
                else:
                    # Low volatility + high disagreement = Use strongest signal
                    strongest_component = max(aggregated_signal.signal_components.items(), key=lambda x: abs(x[1]))
                    return self._create_resolved_decision(
                        aggregated_signal,
                        self._component_to_decision(strongest_component[1]),
                        aggregated_signal.confidence * 0.7,
                        f'strongest_signal_{strongest_component[0]}'
                    )
            
            # Moderate disagreement - weight by market conditions
            elif disagreement_level > 0.4:
                market_weight = self._calculate_market_condition_weight(market_metrics)
                adjusted_confidence = aggregated_signal.confidence * (1.0 - disagreement_level * 0.5) * market_weight
                
                return self._create_resolved_decision(
                    aggregated_signal,
                    aggregated_signal.decision_type,
                    adjusted_confidence,
                    'market_weighted_resolution'
                )
            
            # Low disagreement - proceed with original signal
            else:
                return self._create_resolved_decision(
                    aggregated_signal,
                    aggregated_signal.decision_type,
                    aggregated_signal.confidence * 0.95,  # Small confidence boost for agreement
                    'signal_agreement'
                )

        except Exception as e:
            self.logger.error(f"Error resolving conflicts for {aggregated_signal.symbol}: {e}")
            # Fallback: return conservative HOLD decision
            return self._create_resolved_decision(
                aggregated_signal,
                DecisionType.HOLD,
                20.0,  # Low confidence
                f'error_fallback: {str(e)}'
            )

    def _calculate_technical_component(self, signals: List[Dict[str, Any]]) -> float:
        """Calculate technical analysis component with market-driven weighting"""
        if not signals:
            return 0.0
        
        try:
            # Extract signal strengths and directions
            signal_values = []
            for signal in signals:
                if 'direction' in signal and 'strength' in signal:
                    # Convert direction to numeric (-1 to 1) and multiply by strength
                    direction = 1 if signal['direction'] == 'BUY' else -1 if signal['direction'] == 'SELL' else 0
                    strength = float(signal.get('strength', 0.5))
                    signal_values.append(direction * strength)
                elif 'score' in signal:
                    # Direct score format
                    signal_values.append(float(signal['score']))
            
            if not signal_values:
                return 0.0
            
            # Calculate weighted average with recency bias
            weights = [1.0 / (i + 1) for i in range(len(signal_values))]  # More recent signals weighted higher
            weighted_sum = sum(val * weight for val, weight in zip(signal_values, weights))
            weight_sum = sum(weights)
            
            return weighted_sum / weight_sum if weight_sum > 0 else 0.0
            
        except Exception as e:
            self.logger.error(f"Error calculating technical component: {e}")
            return 0.0

    def _calculate_market_component(self, symbol: str) -> float:
        """Calculate market regime component with real market data"""
        try:
            # Get real market metrics
            market_metrics = get_all_metrics(symbol)
            
            if not market_metrics:
                return 0.0
            
            # Combine various market factors
            components = []
            
            # Trend component
            if hasattr(market_metrics, 'trend_strength') and market_metrics.trend_strength is not None:
                components.append(market_metrics.trend_strength)
            
            # Volatility component (inverse - high volatility = negative component)
            if hasattr(market_metrics, 'volatility') and market_metrics.volatility is not None:
                volatility_component = max(-0.5, min(0.5, -market_metrics.volatility + 0.3))
                components.append(volatility_component)
            
            # Volume component
            if hasattr(market_metrics, 'volume_factor') and market_metrics.volume_factor is not None:
                components.append(market_metrics.volume_factor * 0.3)  # Scale down volume impact
            
            # Market regime bias
            regime_bias = {
                'bull': 0.2,
                'bear': -0.2,
                'sideways': 0.0,
                'TRENDING_UP': 0.15,
                'TRENDING_DOWN': -0.15,
                'VOLATILE': -0.1
            }.get(self.regime_state.get('current', 'sideways'), 0.0)
            
            components.append(regime_bias)
            
            # Return average of available components
            return statistics.mean(components) if components else 0.0
            
        except Exception as e:
            self.logger.error(f"Error calculating market component for {symbol}: {e}")
            return 0.0

    def _calculate_risk_component(self, symbol: str) -> float:
        """Calculate risk component with real market risk data"""
        try:
            # Get market metrics for risk assessment
            market_metrics = get_all_metrics(symbol)
            
            risk_factors = []
            
            # Volatility risk
            if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility is not None:
                # Higher volatility = negative risk component
                volatility_risk = max(-0.3, min(0.1, -market_metrics.volatility * 0.5))
                risk_factors.append(volatility_risk)
            
            # Active risk alerts
            if symbol in self.risk_alerts_active:
                risk_factors.append(-0.2)  # Negative component for active alerts
            
            # Risk score from market metrics
            if market_metrics and hasattr(market_metrics, 'risk_score') and market_metrics.risk_score is not None:
                # Convert risk score (0-100) to component (-0.3 to 0.1)
                risk_component = max(-0.3, min(0.1, (50 - market_metrics.risk_score) / 100))
                risk_factors.append(risk_component)
            
            # Market regime risk
            regime_risk = {
                'VOLATILE': -0.15,
                'CRISIS': -0.3,
                'UNCERTAIN': -0.1,
                'STABLE': 0.05,
                'bull': 0.05,
                'bear': -0.1
            }.get(self.regime_state.get('current', 'sideways'), 0.0)
            
            risk_factors.append(regime_risk)
            
            return statistics.mean(risk_factors) if risk_factors else 0.0
            
        except Exception as e:
            self.logger.error(f"Error calculating risk component for {symbol}: {e}")
            return -0.1  # Conservative negative bias on error

    def _calculate_signal_agreement(self, signals: List[Dict[str, Any]], sentiment: Dict[str, Any]) -> float:
        """Calculate agreement level between different signals"""
        try:
            if len(signals) < 2:
                return 1.0  # Perfect agreement with single signal
            
            # Extract signal directions
            directions = []
            for signal in signals:
                if 'direction' in signal:
                    direction = signal['direction']
                    if direction == 'BUY':
                        directions.append(1)
                    elif direction == 'SELL':
                        directions.append(-1)
                    else:
                        directions.append(0)
                elif 'score' in signal:
                    score = float(signal['score'])
                    if score > 0.3:
                        directions.append(1)
                    elif score < -0.3:
                        directions.append(-1)
                    else:
                        directions.append(0)
            
            # Add sentiment direction if available
            if sentiment.get('score') is not None:
                sent_score = float(sentiment['score'])
                if sent_score > 0.3:
                    directions.append(1)
                elif sent_score < -0.3:
                    directions.append(-1)
                else:
                    directions.append(0)
            
            if not directions:
                return 0.5  # Neutral agreement
            
            # Calculate agreement as inverse of standard deviation
            if len(directions) == 1:
                return 1.0
            
            std_dev = np.std(directions)
            max_std = np.sqrt(2)  # Maximum possible std dev for directions {-1, 0, 1}
            
            # Convert to agreement (0-1 scale)
            agreement = 1.0 - (std_dev / max_std)
            return max(0.0, min(1.0, agreement))
            
        except Exception as e:
            self.logger.error(f"Error calculating signal agreement: {e}")
            return 0.5

    def _calculate_signal_uncertainty(self, signals: List[Dict[str, Any]], sentiment: Dict[str, Any]) -> float:
        """Calculate uncertainty level in signals"""
        try:
            uncertainty_factors = []
            
            # Signal confidence uncertainty
            confidences = [float(s.get('confidence', 0.5)) for s in signals]
            if confidences:
                avg_confidence = statistics.mean(confidences)
                confidence_uncertainty = 1.0 - avg_confidence
                uncertainty_factors.append(confidence_uncertainty)
            
            # Sentiment confidence uncertainty
            if sentiment.get('confidence') is not None:
                sent_confidence = float(sentiment['confidence'])
                sentiment_uncertainty = 1.0 - sent_confidence
                uncertainty_factors.append(sentiment_uncertainty)
            
            # Signal count uncertainty (fewer signals = higher uncertainty)
            signal_count_uncertainty = max(0.0, (5 - len(signals)) / 5.0)  # Optimal around 5 signals
            uncertainty_factors.append(signal_count_uncertainty)
            
            # Time-based uncertainty (older signals less certain)
            time_uncertainties = []
            current_time = datetime.now()
            for signal in signals:
                if 'timestamp' in signal:
                    try:
                        signal_time = datetime.fromisoformat(signal['timestamp'].replace('Z', '+00:00'))
                        age_hours = (current_time - signal_time).total_seconds() / 3600
                        time_uncertainty = min(0.5, age_hours / 24)  # Uncertainty increases with age
                        time_uncertainties.append(time_uncertainty)
                    except:
                        time_uncertainties.append(0.3)  # Default uncertainty for unparseable timestamp
            
            if time_uncertainties:
                uncertainty_factors.append(statistics.mean(time_uncertainties))
            
            return statistics.mean(uncertainty_factors) if uncertainty_factors else 0.5
            
        except Exception as e:
            self.logger.error(f"Error calculating signal uncertainty: {e}")
            return 0.5

    def _apply_position_aware_logic(self, symbol: str, base_decision: DecisionType, sentiment_score: float, confidence: float) -> DecisionType:
        """Apply position-aware decision logic with market context"""
        try:
            # Get current position information (would be injected in real system)
            # For now, use conservative logic
            
            # Low confidence decisions default to HOLD
            if confidence < 0.3:
                return DecisionType.HOLD
            
            # Very negative sentiment overrides BUY decisions in uncertain markets
            if base_decision == DecisionType.BUY and sentiment_score < -0.6:
                market_metrics = get_all_metrics(symbol)
                if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility and market_metrics.volatility > 0.6:
                    return DecisionType.HOLD
            
            # Very positive sentiment can override SELL in strong markets
            if base_decision == DecisionType.SELL and sentiment_score > 0.6:
                if self.regime_state.get('current') in ['bull', 'TRENDING_UP']:
                    return DecisionType.HOLD
            
            return base_decision
            
        except Exception as e:
            self.logger.error(f"Error in position-aware logic for {symbol}: {e}")
            return DecisionType.HOLD

    def _calculate_risk_score(self, symbol: str) -> float:
        """Calculate overall risk score for symbol"""
        try:
            market_metrics = get_all_metrics(symbol)
            
            if market_metrics and hasattr(market_metrics, 'risk_score') and market_metrics.risk_score is not None:
                return float(market_metrics.risk_score)
            
            # Fallback risk calculation
            risk_factors = []
            
            # Volatility risk
            if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility is not None:
                volatility_risk = min(100, market_metrics.volatility * 100)
                risk_factors.append(volatility_risk)
            
            # Market regime risk
            regime_risk = {
                'VOLATILE': 80,
                'CRISIS': 95,
                'UNCERTAIN': 70,
                'bear': 75,
                'bull': 40,
                'sideways': 50
            }.get(self.regime_state.get('current', 'sideways'), 50)
            
            risk_factors.append(regime_risk)
            
            # Active alerts risk
            if symbol in self.risk_alerts_active:
                risk_factors.append(85)
            
            return statistics.mean(risk_factors) if risk_factors else 50.0
            
        except Exception as e:
            self.logger.error(f"Error calculating risk score for {symbol}: {e}")
            return 75.0  # Conservative high risk on error

    def _create_resolved_decision(self, aggregated_signal: AggregatedSignal, decision_type: DecisionType, confidence: float, method: str):
        """Create resolved decision object"""
        return type('ResolvedDecision', (), {
            'symbol': aggregated_signal.symbol,
            'action': decision_type.value,
            'final_direction': 1.0 if decision_type == DecisionType.BUY else -1.0 if decision_type == DecisionType.SELL else 0.0,
            'final_confidence': confidence / 100.0,  # Convert to 0-1 scale
            'resolution_method': method,
            'original_signal': aggregated_signal
        })()

    def _component_to_decision(self, component_value: float) -> DecisionType:
        """Convert component value to decision type"""
        if component_value > 0.3:
            return DecisionType.BUY
        elif component_value < -0.3:
            return DecisionType.SELL
        else:
            return DecisionType.HOLD

    def _calculate_market_condition_weight(self, market_metrics) -> float:
        """Calculate market condition weight for confidence adjustment"""
        try:
            if not market_metrics:
                return 0.8  # Conservative weight
            
            weights = []
            
            # Volatility weight (lower volatility = higher weight)
            if hasattr(market_metrics, 'volatility') and market_metrics.volatility is not None:
                volatility_weight = max(0.5, min(1.0, 1.0 - market_metrics.volatility))
                weights.append(volatility_weight)
            
            # Volume weight
            if hasattr(market_metrics, 'volume_factor') and market_metrics.volume_factor is not None:
                volume_weight = max(0.7, min(1.0, 0.7 + market_metrics.volume_factor * 0.3))
                weights.append(volume_weight)
            
            # Trend clarity weight
            regime = self.regime_state.get('current', 'sideways')
            trend_weight = {
                'bull': 0.9,
                'bear': 0.9,
                'TRENDING_UP': 0.85,
                'TRENDING_DOWN': 0.85,
                'sideways': 0.7,
                'VOLATILE': 0.6
            }.get(regime, 0.75)
            
            weights.append(trend_weight)
            
            return statistics.mean(weights) if weights else 0.8
            
        except Exception as e:
            self.logger.error(f"Error calculating market condition weight: {e}")
            return 0.7  # Conservative weight on error

    def update_regime_state(self, regime_data: Dict[str, Any]):
        """Update market regime state"""
        try:
            if regime_data and isinstance(regime_data, dict):
                self.regime_state.update({
                    'current': regime_data.get('regime', self.regime_state['current']),
                    'confidence': regime_data.get('confidence', self.regime_state['confidence']),
                    'updated': datetime.now(),
                    'volatility': regime_data.get('volatility_regime', self.regime_state['volatility']),
                    'trend': regime_data.get('trend', self.regime_state['trend'])
                })
                self.logger.debug(f"Regime state updated: {self.regime_state}")
        except Exception as e:
            self.logger.error(f"Error updating regime state: {e}")

    def update_signal_weights(self, new_weights: Dict[str, float]):
        """Update signal weights (for parameter adaptation)"""
        try:
            if new_weights and isinstance(new_weights, dict):
                self.signal_weights.update(new_weights)
                self.logger.info(f"Signal weights updated: {self.signal_weights}")
        except Exception as e:
            self.logger.error(f"Error updating signal weights: {e}")