"""
trading_engine/portfolio_manager/decision_engine/parameter_adapter.py

Parameter Adapter - Multi-Level Adaptation Engine

Adapts system parameters based on multiple feedback loops:
1. Performance-based adaptation (strategy success rates)
2. Market regime adaptation (volatility, trend, chaos)
3. Stress-based adaptation (t3.micro resource constraints)
4. Risk-based adaptation (portfolio stress, drawdowns)
5. Time-based adaptation (market hours, session changes)

Key Features:
- Adaptive signal weights in real-time
- Risk limit adjustments based on market conditions
- Strategy confidence thresholds that adapt to regime
- Position sizing parameters that scale with volatility
- Memory-efficient for t3.micro constraints
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
import numpy as np
from collections import deque, defaultdict
import json


class AdaptationLevel(Enum):
    """Levels of parameter adaptation"""
    CONSERVATIVE = "conservative"  # Slow, careful adaptation
    MODERATE = "moderate"         # Balanced adaptation
    AGGRESSIVE = "aggressive"     # Fast adaptation to changes


class MarketRegime(Enum):
    """Market regime types for adaptation"""
    BULL_TRENDING = "bull_trending"
    BEAR_TRENDING = "bear_trending"
    SIDEWAYS = "sideways"
    HIGH_VOLATILITY = "high_volatility"
    CRISIS = "crisis"
    RECOVERY = "recovery"


class SystemStress(Enum):
    """System stress levels"""
    LOW = "low"           # <50% resource usage
    MEDIUM = "medium"     # 50-75% resource usage
    HIGH = "high"         # 75-90% resource usage
    CRITICAL = "critical" # >90% resource usage


@dataclass
class AdaptationMetrics:
    """Metrics for adaptation decisions"""
    # Performance metrics
    strategy_success_rates: Dict[str, float] = field(default_factory=dict)
    signal_accuracy_by_type: Dict[str, float] = field(default_factory=dict)
    overall_performance: float = 0.5

    # Market metrics
    current_regime: MarketRegime = MarketRegime.SIDEWAYS
    volatility_percentile: float = 0.5  # 0-1 scale
    trend_strength: float = 0.5         # 0-1 scale

    # System metrics
    cpu_usage: float = 0.3             # 0-1 scale
    memory_usage: float = 0.4          # 0-1 scale
    processing_latency: float = 0.0    # milliseconds

    # Risk metrics
    portfolio_stress: float = 0.2      # 0-1 scale
    max_drawdown: float = 0.0          # 0-1 scale
    risk_alerts_count: int = 0

    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class AdaptedParameters:
    """Parameters that have been adapted"""
    # Signal weighting adaptations
    signal_weights: Dict[str, float] = field(default_factory=lambda: {
        'technical': 0.30, 'market': 0.30, 'sentiment': 0.30, 'risk': 0.10
    })

    # Risk management adaptations
    max_position_size: float = 0.15      # % of portfolio
    max_portfolio_allocation: float = 0.80
    risk_multiplier: float = 1.0         # Scales all risk limits

    # Strategy adaptations
    confidence_threshold: float = 0.60   # Minimum confidence for trades
    signal_staleness_hours: float = 4.0  # Max age for signals

    # Execution adaptations
    max_daily_trades: int = 50
    position_sizing_multiplier: float = 1.0

    # System adaptations
    processing_frequency: float = 150.0  # seconds between runs
    batch_size: int = 10                 # symbols per batch

    # Metadata
    adaptation_reason: str = ""
    last_updated: datetime = field(default_factory=datetime.now)
    update_count: int = 0


class ParameterAdapter:
    """
    Multi-Level Parameter Adaptation Engine

    **Adaptation Philosophy:**
    The system should be like a skilled trader who adjusts their approach
    based on changing market conditions, their own performance, and
    available resources.

    **Key Design Decisions:**

    1. **Multi-Input Adaptation:**
       - Performance feedback (what's working?)
       - Market regime changes (what type of market?)
       - System stress (resource constraints)
       - Risk levels (how much danger?)

    2. **Graduated Response:**
       - Small changes for minor shifts
       - Larger changes for regime shifts
       - Emergency adaptation for crisis conditions

    3. **Stability vs Responsiveness:**
       - Exponential smoothing for stable adaptation
       - Rapid response for critical conditions
       - Minimum/maximum bounds to prevent extremes
    """

    def __init__(self, adaptation_level: AdaptationLevel = AdaptationLevel.MODERATE):
        self.logger = logging.getLogger("parameter_adapter")

        # Configuration
        self.adaptation_level = adaptation_level
        self.config = self._load_adaptation_config()

        # Current state
        self.current_parameters = AdaptedParameters()
        self.base_parameters = AdaptedParameters()  # Fallback defaults

        # Adaptation history
        self.adaptation_history = deque(maxlen=100)
        self.metrics_history = deque(maxlen=200)

        # Performance tracking
        self.adaptation_stats = {
            'total_adaptations': 0,
            'performance_adaptations': 0,
            'regime_adaptations': 0,
            'stress_adaptations': 0,
            'risk_adaptations': 0,
            'emergency_adaptations': 0
        }

        # Smoothing factors for stability
        self.smoothing_factors = {
            'signal_weights': 0.1,      # Very gradual weight changes
            'risk_params': 0.2,         # Moderate risk changes
            'execution_params': 0.3,    # Faster execution changes
            'system_params': 0.5        # Quick system adjustments
        }

        self.logger.info(f"ParameterAdapter initialized with {adaptation_level.value} adaptation level")

    def adapt_parameters(self, metrics: AdaptationMetrics) -> AdaptedParameters:
        """
        Main adaptation method - adjust parameters based on current metrics

        **Adaptation Flow:**
        1. Analyze current metrics vs history
        2. Determine adaptation triggers
        3. Calculate parameter adjustments
        4. Apply smoothing and bounds checking
        5. Update parameters and log changes

        Args:
            metrics: Current system and market metrics

        Returns:
            AdaptedParameters with updated values
        """
        try:
            start_time = datetime.now()

            # Store metrics for history
            self.metrics_history.append(metrics)

            # Analyze adaptation needs
            adaptation_triggers = self._analyze_adaptation_triggers(metrics)

            if not adaptation_triggers:
                # No adaptation needed
                return self.current_parameters

            # Create new parameter set
            new_parameters = AdaptedParameters()
            new_parameters.__dict__.update(self.current_parameters.__dict__)

            # Apply different types of adaptations
            for trigger_type, trigger_data in adaptation_triggers.items():
                if trigger_type == 'performance':
                    new_parameters = self._adapt_for_performance(new_parameters, metrics, trigger_data)
                elif trigger_type == 'regime':
                    new_parameters = self._adapt_for_regime(new_parameters, metrics, trigger_data)
                elif trigger_type == 'stress':
                    new_parameters = self._adapt_for_stress(new_parameters, metrics, trigger_data)
                elif trigger_type == 'risk':
                    new_parameters = self._adapt_for_risk(new_parameters, metrics, trigger_data)
                elif trigger_type == 'emergency':
                    new_parameters = self._adapt_for_emergency(new_parameters, metrics, trigger_data)

            # Apply smoothing to prevent oscillation
            smoothed_parameters = self._apply_smoothing(self.current_parameters, new_parameters)

            # Validate and bound-check parameters
            validated_parameters = self._validate_parameters(smoothed_parameters)

            # Update current parameters
            self.current_parameters = validated_parameters
            self.current_parameters.last_updated = datetime.now()
            self.current_parameters.update_count += 1

            # Track adaptation
            self._track_adaptation(adaptation_triggers, metrics)

            # Log significant changes
            self._log_adaptation_changes(adaptation_triggers)

            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self.logger.debug(f"Parameter adaptation completed in {processing_time:.2f}ms")

            return self.current_parameters

        except Exception as e:
            self.logger.error(f"Error in parameter adaptation: {e}")
            return self.current_parameters

    def get_adaptation_status(self) -> Dict[str, Any]:
        """Get current adaptation status for monitoring"""
        return {
            'current_parameters': {
                'signal_weights': self.current_parameters.signal_weights,
                'max_position_size': self.current_parameters.max_position_size,
                'confidence_threshold': self.current_parameters.confidence_threshold,
                'risk_multiplier': self.current_parameters.risk_multiplier,
                'max_daily_trades': self.current_parameters.max_daily_trades
            },
            'adaptation_stats': self.adaptation_stats.copy(),
            'adaptation_level': self.adaptation_level.value,
            'last_updated': self.current_parameters.last_updated.isoformat(),
            'update_count': self.current_parameters.update_count,
            'adaptation_history_length': len(self.adaptation_history),
            'metrics_history_length': len(self.metrics_history)
        }

    def force_parameter_reset(self):
        """Reset parameters to base defaults (emergency function)"""
        self.logger.warning("Forcing parameter reset to defaults")
        self.current_parameters = AdaptedParameters()
        self.current_parameters.adaptation_reason = "Manual reset"
        self.current_parameters.last_updated = datetime.now()

    # Private adaptation methods

    def _analyze_adaptation_triggers(self, metrics: AdaptationMetrics) -> Dict[str, Any]:
        """Analyze which adaptations are needed"""
        triggers = {}

        # Performance-based triggers
        if metrics.overall_performance < 0.4:  # Poor performance
            triggers['performance'] = {
                'type': 'poor_performance',
                'severity': 'high' if metrics.overall_performance < 0.3 else 'medium',
                'details': {'performance': metrics.overall_performance}
            }
        elif metrics.overall_performance > 0.7:  # Good performance
            triggers['performance'] = {
                'type': 'good_performance',
                'severity': 'low',
                'details': {'performance': metrics.overall_performance}
            }

        # Regime-based triggers
        if self._regime_changed(metrics):
            triggers['regime'] = {
                'type': 'regime_change',
                'severity': 'medium',
                'details': {'new_regime': metrics.current_regime.value}
            }

        # Stress-based triggers
        system_stress = self._calculate_system_stress(metrics)
        if system_stress in [SystemStress.HIGH, SystemStress.CRITICAL]:
            triggers['stress'] = {
                'type': 'high_stress',
                'severity': 'critical' if system_stress == SystemStress.CRITICAL else 'high',
                'details': {'stress_level': system_stress.value}
            }

        # Risk-based triggers
        if metrics.portfolio_stress > 0.7 or metrics.risk_alerts_count > 5:
            triggers['risk'] = {
                'type': 'high_risk',
                'severity': 'high',
                'details': {'portfolio_stress': metrics.portfolio_stress, 'alerts': metrics.risk_alerts_count}
            }

        # Emergency triggers
        if (metrics.max_drawdown > 0.15 or
            system_stress == SystemStress.CRITICAL or
            metrics.portfolio_stress > 0.8):
            triggers['emergency'] = {
                'type': 'emergency_conditions',
                'severity': 'critical',
                'details': {'drawdown': metrics.max_drawdown, 'stress': metrics.portfolio_stress}
            }

        return triggers

    def _adapt_for_performance(self, params: AdaptedParameters, metrics: AdaptationMetrics,
                             trigger_data: Dict) -> AdaptedParameters:
        """Adapt parameters based on performance feedback"""
        performance = metrics.overall_performance

        if trigger_data['type'] == 'poor_performance':
            # Reduce risk when performing poorly
            params.max_position_size *= 0.9
            params.confidence_threshold = min(0.8, params.confidence_threshold + 0.05)
            params.risk_multiplier = min(1.5, params.risk_multiplier + 0.1)

            # Adjust signal weights based on what's working
            if metrics.signal_accuracy_by_type:
                best_signal = max(metrics.signal_accuracy_by_type.items(), key=lambda x: x[1])
                worst_signal = min(metrics.signal_accuracy_by_type.items(), key=lambda x: x[1])

                if best_signal[0] in params.signal_weights:
                    params.signal_weights[best_signal[0]] = min(0.5, params.signal_weights[best_signal[0]] + 0.05)
                if worst_signal[0] in params.signal_weights:
                    params.signal_weights[worst_signal[0]] = max(0.1, params.signal_weights[worst_signal[0]] - 0.05)

                # Renormalize weights
                total_weight = sum(params.signal_weights.values())
                if total_weight > 0:
                    for key in params.signal_weights:
                        params.signal_weights[key] /= total_weight

            params.adaptation_reason = f"Poor performance adaptation: {performance:.2f}"

        elif trigger_data['type'] == 'good_performance':
            # Slightly increase risk when performing well
            params.max_position_size = min(0.20, params.max_position_size * 1.05)
            params.confidence_threshold = max(0.5, params.confidence_threshold - 0.02)
            params.risk_multiplier = max(0.7, params.risk_multiplier - 0.05)

            params.adaptation_reason = f"Good performance adaptation: {performance:.2f}"

        self.adaptation_stats['performance_adaptations'] += 1
        return params

    def _adapt_for_regime(self, params: AdaptedParameters, metrics: AdaptationMetrics,
                        trigger_data: Dict) -> AdaptedParameters:
        """Adapt parameters based on market regime changes"""
        regime = metrics.current_regime

        # Regime-specific adaptations
        if regime == MarketRegime.HIGH_VOLATILITY:
            # High volatility: reduce position sizes, increase risk awareness
            params.max_position_size *= 0.8
            params.signal_weights['risk'] = min(0.25, params.signal_weights['risk'] + 0.05)
            params.confidence_threshold = min(0.8, params.confidence_threshold + 0.1)
            params.max_daily_trades = max(20, int(params.max_daily_trades * 0.7))

        elif regime == MarketRegime.CRISIS:
            # Crisis: maximum risk reduction
            params.max_position_size *= 0.6
            params.max_portfolio_allocation *= 0.8
            params.signal_weights['risk'] = min(0.3, params.signal_weights['risk'] + 0.1)
            params.confidence_threshold = min(0.9, params.confidence_threshold + 0.15)
            params.max_daily_trades = max(10, int(params.max_daily_trades * 0.5))

        elif regime in [MarketRegime.BULL_TRENDING, MarketRegime.BEAR_TRENDING]:
            # Trending markets: emphasize technical signals
            params.signal_weights['technical'] = min(0.45, params.signal_weights['technical'] + 0.1)
            params.signal_weights['market'] = min(0.45, params.signal_weights['market'] + 0.05)

        elif regime == MarketRegime.SIDEWAYS:
            # Sideways market: emphasize mean reversion and sentiment
            params.signal_weights['sentiment'] = min(0.4, params.signal_weights['sentiment'] + 0.05)
            params.confidence_threshold = max(0.5, params.confidence_threshold - 0.05)

        # Renormalize signal weights
        total_weight = sum(params.signal_weights.values())
        if total_weight > 0:
            for key in params.signal_weights:
                params.signal_weights[key] /= total_weight

        params.adaptation_reason = f"Regime adaptation: {regime.value}"
        self.adaptation_stats['regime_adaptations'] += 1
        return params

    def _adapt_for_stress(self, params: AdaptedParameters, metrics: AdaptationMetrics,
                        trigger_data: Dict) -> AdaptedParameters:
        """Adapt parameters based on system stress"""
        stress_level = self._calculate_system_stress(metrics)

        if stress_level == SystemStress.HIGH:
            # Reduce processing load
            params.processing_frequency = min(300.0, params.processing_frequency * 1.2)
            params.batch_size = max(5, int(params.batch_size * 0.8))
            params.max_daily_trades = max(30, int(params.max_daily_trades * 0.8))

        elif stress_level == SystemStress.CRITICAL:
            # Emergency stress reduction
            params.processing_frequency = min(600.0, params.processing_frequency * 1.5)
            params.batch_size = max(3, int(params.batch_size * 0.6))
            params.max_daily_trades = max(20, int(params.max_daily_trades * 0.6))
            params.signal_staleness_hours = min(8.0, params.signal_staleness_hours * 1.5)

        params.adaptation_reason = f"Stress adaptation: {stress_level.value}"
        self.adaptation_stats['stress_adaptations'] += 1
        return params

    def _adapt_for_risk(self, params: AdaptedParameters, metrics: AdaptationMetrics,
                      trigger_data: Dict) -> AdaptedParameters:
        """Adapt parameters based on risk levels"""
        portfolio_stress = metrics.portfolio_stress

        if portfolio_stress > 0.7:
            # High portfolio stress: reduce all risk
            params.max_position_size *= 0.8
            params.max_portfolio_allocation *= 0.9
            params.risk_multiplier = min(2.0, params.risk_multiplier + 0.2)
            params.confidence_threshold = min(0.85, params.confidence_threshold + 0.1)

            # Emphasize risk signals
            params.signal_weights['risk'] = min(0.25, params.signal_weights['risk'] + 0.08)

        # Renormalize signal weights
        total_weight = sum(params.signal_weights.values())
        if total_weight > 0:
            for key in params.signal_weights:
                params.signal_weights[key] /= total_weight

        params.adaptation_reason = f"Risk adaptation: stress {portfolio_stress:.2f}"
        self.adaptation_stats['risk_adaptations'] += 1
        return params

    def _adapt_for_emergency(self, params: AdaptedParameters, metrics: AdaptationMetrics,
                           trigger_data: Dict) -> AdaptedParameters:
        """Emergency adaptation for crisis conditions"""
        # Severe risk reduction
        params.max_position_size = min(0.05, params.max_position_size)  # Max 5% positions
        params.max_portfolio_allocation = min(0.5, params.max_portfolio_allocation)  # Max 50% invested
        params.risk_multiplier = 2.0  # Double all risk limits
        params.confidence_threshold = 0.9  # Very high confidence required
        params.max_daily_trades = min(10, params.max_daily_trades)  # Limit trading

        # Maximum risk emphasis
        params.signal_weights = {
            'technical': 0.2, 'market': 0.2, 'sentiment': 0.1, 'risk': 0.5
        }

        params.adaptation_reason = f"Emergency adaptation: {trigger_data['type']}"
        self.adaptation_stats['emergency_adaptations'] += 1
        return params

    def _apply_smoothing(self, old_params: AdaptedParameters,
                       new_params: AdaptedParameters) -> AdaptedParameters:
        """Apply exponential smoothing to prevent parameter oscillation"""
        smoothed = AdaptedParameters()

        # Signal weights (very gradual changes)
        alpha = self.smoothing_factors['signal_weights']
        for key in old_params.signal_weights:
            if key in new_params.signal_weights:
                smoothed.signal_weights[key] = (old_params.signal_weights[key] * (1 - alpha) +
                                              new_params.signal_weights[key] * alpha)

        # Risk parameters (moderate smoothing)
        alpha_risk = self.smoothing_factors['risk_params']
        smoothed.max_position_size = (old_params.max_position_size * (1 - alpha_risk) +
                                    new_params.max_position_size * alpha_risk)
        smoothed.max_portfolio_allocation = (old_params.max_portfolio_allocation * (1 - alpha_risk) +
                                           new_params.max_portfolio_allocation * alpha_risk)
        smoothed.risk_multiplier = (old_params.risk_multiplier * (1 - alpha_risk) +
                                  new_params.risk_multiplier * alpha_risk)
        smoothed.confidence_threshold = (old_params.confidence_threshold * (1 - alpha_risk) +
                                       new_params.confidence_threshold * alpha_risk)

        # Execution parameters (faster adaptation)
        alpha_exec = self.smoothing_factors['execution_params']
        smoothed.max_daily_trades = int(old_params.max_daily_trades * (1 - alpha_exec) +
                                      new_params.max_daily_trades * alpha_exec)
        smoothed.position_sizing_multiplier = (old_params.position_sizing_multiplier * (1 - alpha_exec) +
                                             new_params.position_sizing_multiplier * alpha_exec)

        # System parameters (quick adaptation)
        alpha_sys = self.smoothing_factors['system_params']
        smoothed.processing_frequency = (old_params.processing_frequency * (1 - alpha_sys) +
                                       new_params.processing_frequency * alpha_sys)
        smoothed.batch_size = int(old_params.batch_size * (1 - alpha_sys) +
                                new_params.batch_size * alpha_sys)
        smoothed.signal_staleness_hours = (old_params.signal_staleness_hours * (1 - alpha_sys) +
                                         new_params.signal_staleness_hours * alpha_sys)

        # Copy metadata
        smoothed.adaptation_reason = new_params.adaptation_reason
        smoothed.last_updated = new_params.last_updated
        smoothed.update_count = new_params.update_count

        return smoothed

    def _validate_parameters(self, params: AdaptedParameters) -> AdaptedParameters:
        """Validate and bound-check parameters"""
        # Signal weights must sum to 1.0 and be within bounds
        total_weight = sum(params.signal_weights.values())
        if total_weight > 0:
            for key in params.signal_weights:
                params.signal_weights[key] /= total_weight
                params.signal_weights[key] = max(0.05, min(0.6, params.signal_weights[key]))

        # Position size bounds
        params.max_position_size = max(0.02, min(0.25, params.max_position_size))

        # Portfolio allocation bounds
        params.max_portfolio_allocation = max(0.3, min(1.0, params.max_portfolio_allocation))

        # Risk multiplier bounds
        params.risk_multiplier = max(0.5, min(3.0, params.risk_multiplier))

        # Confidence threshold bounds
        params.confidence_threshold = max(0.3, min(0.95, params.confidence_threshold))

        # Daily trades bounds
        params.max_daily_trades = max(5, min(100, params.max_daily_trades))

        # Processing frequency bounds
        params.processing_frequency = max(30.0, min(600.0, params.processing_frequency))

        # Batch size bounds
        params.batch_size = max(1, min(50, params.batch_size))

        # Signal staleness bounds
        params.signal_staleness_hours = max(1.0, min(24.0, params.signal_staleness_hours))

        return params

    def _calculate_system_stress(self, metrics: AdaptationMetrics) -> SystemStress:
        """Calculate overall system stress level"""
        cpu_stress = metrics.cpu_usage
        memory_stress = metrics.memory_usage
        latency_stress = min(1.0, metrics.processing_latency / 1000.0)  # Normalize to 0-1

        overall_stress = (cpu_stress + memory_stress + latency_stress) / 3.0

        if overall_stress > 0.9:
            return SystemStress.CRITICAL
        elif overall_stress > 0.75:
            return SystemStress.HIGH
        elif overall_stress > 0.5:
            return SystemStress.MEDIUM
        else:
            return SystemStress.LOW

    def _regime_changed(self, metrics: AdaptationMetrics) -> bool:
        """Check if market regime has changed significantly"""
        if not self.metrics_history:
            return False

        recent_regime = self.metrics_history[-1].current_regime
        return recent_regime != metrics.current_regime

    def _track_adaptation(self, triggers: Dict[str, Any], metrics: AdaptationMetrics):
        """Track adaptation for analysis"""
        adaptation_record = {
            'timestamp': datetime.now(),
            'triggers': triggers,
            'metrics_snapshot': {
                'performance': metrics.overall_performance,
                'regime': metrics.current_regime.value,
                'cpu_usage': metrics.cpu_usage,
                'memory_usage': metrics.memory_usage,
                'portfolio_stress': metrics.portfolio_stress
            },
            'parameters_after': {
                'signal_weights': self.current_parameters.signal_weights.copy(),
                'max_position_size': self.current_parameters.max_position_size,
                'confidence_threshold': self.current_parameters.confidence_threshold
            }
        }

        self.adaptation_history.append(adaptation_record)
        self.adaptation_stats['total_adaptations'] += 1

    def _log_adaptation_changes(self, triggers: Dict[str, Any]):
        """Log significant adaptation changes"""
        trigger_types = list(triggers.keys())
        self.logger.info(f"Parameter adaptation triggered by: {', '.join(trigger_types)}")

        # Log specific parameter changes
        params = self.current_parameters
        self.logger.info(f"Updated parameters - Position size: {params.max_position_size:.1%}, "
                        f"Confidence: {params.confidence_threshold:.2f}, "
                        f"Risk multiplier: {params.risk_multiplier:.2f}")

    def _load_adaptation_config(self) -> Dict[str, Any]:
        """Load adaptation configuration based on adaptation level"""
        if self.adaptation_level == AdaptationLevel.CONSERVATIVE:
            return {
                'max_weight_change_per_update': 0.02,
                'max_risk_change_per_update': 0.05,
                'adaptation_frequency_minutes': 30,
                'stability_priority': 0.8
            }
        elif self.adaptation_level == AdaptationLevel.AGGRESSIVE:
            return {
                'max_weight_change_per_update': 0.10,
                'max_risk_change_per_update': 0.20,
                'adaptation_frequency_minutes': 5,
                'stability_priority': 0.3
            }
        else:  # MODERATE
            return {
                'max_weight_change_per_update': 0.05,
                'max_risk_change_per_update': 0.10,
                'adaptation_frequency_minutes': 15,
                'stability_priority': 0.6
            }


# Utility functions for integration

def create_adaptation_metrics(performance_data: Dict[str, float],
                            market_data: Dict[str, Any],
                            system_data: Dict[str, float],
                            risk_data: Dict[str, float]) -> AdaptationMetrics:
    """
    Factory function to create AdaptationMetrics from various data sources

    **Usage Example:**
    ```python
    metrics = create_adaptation_metrics(
        performance_data={'overall': 0.65, 'strategy_rsi': 0.7},
        market_data={'regime': 'high_volatility', 'vix': 35.2},
        system_data={'cpu': 0.8, 'memory': 0.6, 'latency': 250},
        risk_data={'portfolio_stress': 0.3, 'drawdown': 0.08}
    )
    ```
    """
    # Map market regime
    regime_map = {
        'bull_trending': MarketRegime.BULL_TRENDING,
        'bear_trending': MarketRegime.BEAR_TRENDING,
        'sideways': MarketRegime.SIDEWAYS,
        'high_volatility': MarketRegime.HIGH_VOLATILITY,
        'crisis': MarketRegime.CRISIS,
        'recovery': MarketRegime.RECOVERY
    }

    regime_str = market_data.get('regime', 'sideways').lower()
    current_regime = regime_map.get(regime_str, MarketRegime.SIDEWAYS)

    return AdaptationMetrics(
        # Performance metrics
        overall_performance=performance_data.get('overall', 0.5),
        strategy_success_rates={k: v for k, v in performance_data.items() if k != 'overall'},

        # Market metrics
        current_regime=current_regime,
        volatility_percentile=market_data.get('volatility_percentile', 0.5),
        trend_strength=market_data.get('trend_strength', 0.5),

        # System metrics
        cpu_usage=system_data.get('cpu', 0.3),
        memory_usage=system_data.get('memory', 0.4),
        processing_latency=system_data.get('latency', 0.0),

        # Risk metrics
        portfolio_stress=risk_data.get('portfolio_stress', 0.2),
        max_drawdown=risk_data.get('drawdown', 0.0),
        risk_alerts_count=int(risk_data.get('alerts_count', 0))
    )


def get_system_metrics() -> Dict[str, float]:
    """
    Utility function to get current system metrics for t3.micro optimization

    **Integration Helper:**
    Provides system metrics that the parameter adapter uses for stress-based adaptation.
    """
    import psutil
    import time

    try:
        # CPU and memory usage
        cpu_percent = psutil.cpu_percent(interval=0.1) / 100.0
        memory_percent = psutil.virtual_memory().percent / 100.0

        # Simple latency measurement (placeholder)
        start_time = time.time()
        time.sleep(0.001)  # 1ms test operation
        latency_ms = (time.time() - start_time) * 1000

        return {
            'cpu': cpu_percent,
            'memory': memory_percent,
            'latency': latency_ms
        }

    except ImportError:
        # Fallback if psutil not available
        return {
            'cpu': 0.3,
            'memory': 0.4,
            'latency': 100.0
        }
    except Exception as e:
        logging.getLogger("parameter_adapter").warning(f"Error getting system metrics: {e}")
        return {
            'cpu': 0.5,
            'memory': 0.5,
            'latency': 200.0
        }


# Integration example for decision engine
if __name__ == "__main__":
    # Example usage
    adapter = ParameterAdapter(AdaptationLevel.MODERATE)

    # Create sample metrics
    metrics = AdaptationMetrics(
        overall_performance=0.35,  # Poor performance
        current_regime=MarketRegime.HIGH_VOLATILITY,
        cpu_usage=0.85,  # High CPU usage
        memory_usage=0.70,
        portfolio_stress=0.6,
        max_drawdown=0.12
    )

    # Adapt parameters
    adapted_params = adapter.adapt_parameters(metrics)

    print("Adaptation Results:")
    print(f"Signal weights: {adapted_params.signal_weights}")
    print(f"Max position size: {adapted_params.max_position_size:.1%}")
    print(f"Confidence threshold: {adapted_params.confidence_threshold:.2f}")
    print(f"Risk multiplier: {adapted_params.risk_multiplier:.2f}")
    print(f"Reason: {adapted_params.adaptation_reason}")
