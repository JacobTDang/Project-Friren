"""
Enhanced Position Sizer with Sentiment & Regime Integration

This enhanced position sizer integrates:
- Your existing position sizing logic
- Sentiment-based adjustments (FinBERT + news)
- Dual regime detection adjustments
- Dynamic portfolio allocation (up to 15% max)
- Real-time risk management
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

# Import your existing position sizer
from trading_engine.portfolio_manager.position_sizer import PositionSizer, SizingMethod, PositionSizeResult

@dataclass
class EnhancedPositionSizeResult(PositionSizeResult):
    """Enhanced position size result with sentiment and regime adjustments"""

    # Additional sentiment-based adjustments
    sentiment_adjustment: float
    sentiment_score: float
    sentiment_confidence: float
    news_volume_adjustment: float

    # Regime-based adjustments
    market_regime_adjustment: float
    entropy_regime_adjustment: float
    volatility_regime_adjustment: float
    regime_transition_adjustment: float

    # Dynamic sizing factors
    correlation_adjustment: float
    momentum_adjustment: float
    mean_reversion_adjustment: float

    # Enhanced rationale
    sentiment_rationale: str
    regime_rationale: str
    dynamic_rationale: str

class EnhancedPositionSizer(PositionSizer):
    """
    Enhanced Position Sizer with Sentiment & Regime Intelligence

    Extends your existing position sizer with:
    - Sentiment-driven sizing (FinBERT + news volume)
    - Dual regime adjustments (market + entropy regimes)
    - Dynamic correlation-based adjustments
    - Maximum 15% portfolio allocation with smart scaling
    """

    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)

        # Enhanced configuration
        self.enhanced_config = self._enhanced_default_config()
        self.config.update(self.enhanced_config)

        print(f"🎯 Enhanced Position Sizer initialized")
        print(f"   Base size: {self.config['base_position_size']:.1%}")
        print(f"   Max size: {self.config['max_position_size']:.1%}")
        print(f"   Sentiment scaling: {self.config['sentiment_scaling']}")
        print(f"   Regime scaling: {self.config['regime_scaling']}")

    def _enhanced_default_config(self) -> Dict:
        """Enhanced configuration with sentiment and regime parameters"""
        return {
            # Enhanced sizing limits
            'max_portfolio_allocation': 0.15,    # 15% absolute maximum
            'sentiment_scaling_enabled': True,
            'regime_scaling_enabled': True,
            'dynamic_scaling_enabled': True,

            # Sentiment adjustments
            'sentiment_scaling': {
                'max_boost': 1.5,           # 50% boost for extreme positive sentiment
                'max_reduction': 0.6,       # 40% reduction for extreme negative sentiment
                'neutral_threshold': 0.1,   # ±0.1 considered neutral
                'extreme_threshold': 0.7    # ±0.7 considered extreme
            },

            # News volume adjustments
            'news_volume_scaling': {
                'high_volume_boost': 1.2,   # 20% boost for high news volume
                'low_volume_reduction': 0.9, # 10% reduction for low news volume
                'volume_threshold': 5       # 5+ articles considered high volume
            },

            # Market regime multipliers (enhanced)
            'market_regime_multipliers': {
                'BULL_MARKET': 1.3,         # 30% larger in bull markets
                'BEAR_MARKET': 0.6,         # 40% smaller in bear markets
                'SIDEWAYS': 0.9,            # 10% smaller in sideways
                'HIGH_VOLATILITY': 0.5,     # 50% smaller in high vol
                'LOW_VOLATILITY': 1.1,      # 10% larger in low vol
                'UNKNOWN': 1.0
            },

            # Entropy regime multipliers
            'entropy_regime_multipliers': {
                'TRENDING': 1.2,            # 20% larger in trending markets
                'MEAN_REVERTING': 1.0,      # Normal size in mean reverting
                'STABLE': 1.1,              # 10% larger in stable periods
                'VOLATILE': 0.7,            # 30% smaller in volatile periods
                'HIGH_ENTROPY': 0.6,        # 40% smaller in high entropy
                'LOW_ENTROPY': 1.1,         # 10% larger in low entropy
                'UNKNOWN': 1.0
            },

            # Volatility regime adjustments
            'volatility_regime_multipliers': {
                'HIGH_VOLATILITY': 0.6,     # 40% smaller in high vol
                'NORMAL_VOLATILITY': 1.0,   # Normal size
                'LOW_VOLATILITY': 1.2,      # 20% larger in low vol
                'UNKNOWN': 1.0
            },

            # Regime transition adjustments
            'regime_transition_scaling': {
                'significant_transition': 0.8,  # 20% reduction during major transitions
                'minor_transition': 0.95,       # 5% reduction during minor transitions
                'stable_regime': 1.0             # No adjustment when regime is stable
            },

            # Dynamic correlation adjustments
            'correlation_scaling': {
                'high_correlation_reduction': 0.8,  # Reduce size if highly correlated
                'low_correlation_boost': 1.1,       # Boost size if uncorrelated
                'correlation_threshold': 0.7        # 0.7+ considered high correlation
            }
        }

    def calculate_enhanced_position_size(self,
                                       symbol: str,
                                       strategy_signal: Any,
                                       market_data: pd.DataFrame,
                                       sentiment_data: Any,
                                       regime_data: Dict,
                                       current_portfolio: Optional[Dict] = None,
                                       method: SizingMethod = SizingMethod.HYBRID) -> EnhancedPositionSizeResult:
        """
        Calculate enhanced position size with sentiment and regime adjustments

        Args:
            symbol: Stock symbol
            strategy_signal: Strategy signal with confidence
            market_data: OHLCV DataFrame
            sentiment_data: Sentiment analysis result
            regime_data: Market and entropy regime information
            current_portfolio: Current portfolio state
            method: Base sizing methodology

        Returns:
            EnhancedPositionSizeResult with comprehensive analysis
        """

        # Start with base position sizing
        base_result = super().calculate_position_size(
            symbol, strategy_signal, market_data,
            regime_data.get('market_regime', 'UNKNOWN'),
            current_portfolio, method
        )

        # Extract base values
        base_size = base_result.recommended_size
        confidence = base_result.confidence_score

        # Calculate all enhanced adjustments
        adjustments = self._calculate_enhanced_adjustments(
            symbol, strategy_signal, market_data, sentiment_data,
            regime_data, current_portfolio, confidence
        )

        # Apply adjustments sequentially
        enhanced_size = base_size
        for adj_name, adj_value in adjustments.items():
            enhanced_size *= adj_value

        # Apply absolute maximum limit (15% portfolio)
        enhanced_size = min(enhanced_size, self.config['max_portfolio_allocation'])

        # Calculate enhanced risk metrics
        enhanced_risk_metrics = self._calculate_enhanced_risk_metrics(
            market_data, enhanced_size, sentiment_data, regime_data
        )

        # Generate enhanced rationale
        rationale_parts = self._generate_enhanced_rationale(
            symbol, base_size, enhanced_size, adjustments,
            sentiment_data, regime_data, confidence
        )

        # Generate enhanced warnings
        enhanced_warnings = self._generate_enhanced_warnings(
            enhanced_size, enhanced_risk_metrics, sentiment_data, regime_data
        )

        # Create enhanced result
        return EnhancedPositionSizeResult(
            # Base fields
            symbol=symbol,
            recommended_size=enhanced_size,
            base_size=base_size,
            sizing_method=method.value,
            confidence_score=confidence,

            # Original adjustments
            volatility_adjustment=base_result.volatility_adjustment,
            confidence_adjustment=base_result.confidence_adjustment,
            regime_adjustment=base_result.regime_adjustment,
            risk_adjustment=base_result.risk_adjustment,

            # Enhanced risk metrics
            estimated_volatility=enhanced_risk_metrics['volatility'],
            estimated_var_1d=enhanced_risk_metrics['var_1d'],
            risk_contribution=enhanced_risk_metrics['risk_contribution'],

            # Enhanced adjustments
            sentiment_adjustment=adjustments['sentiment'],
            sentiment_score=getattr(sentiment_data, 'sentiment_score', 0),
            sentiment_confidence=getattr(sentiment_data, 'confidence', 0),
            news_volume_adjustment=adjustments['news_volume'],

            market_regime_adjustment=adjustments['market_regime'],
            entropy_regime_adjustment=adjustments['entropy_regime'],
            volatility_regime_adjustment=adjustments['volatility_regime'],
            regime_transition_adjustment=adjustments['regime_transition'],

            correlation_adjustment=adjustments['correlation'],
            momentum_adjustment=adjustments['momentum'],
            mean_reversion_adjustment=adjustments['mean_reversion'],

            # Enhanced rationale
            sizing_rationale=rationale_parts['main'],
            sentiment_rationale=rationale_parts['sentiment'],
            regime_rationale=rationale_parts['regime'],
            dynamic_rationale=rationale_parts['dynamic'],

            risk_warnings=enhanced_warnings,
            timestamp=datetime.now()
        )

    def _calculate_enhanced_adjustments(self,
                                      symbol: str,
                                      strategy_signal: Any,
                                      market_data: pd.DataFrame,
                                      sentiment_data: Any,
                                      regime_data: Dict,
                                      current_portfolio: Optional[Dict],
                                      confidence: float) -> Dict[str, float]:
        """Calculate all enhanced position sizing adjustments"""

        adjustments = {}

        # 1. Sentiment-based adjustments
        adjustments.update(self._calculate_sentiment_adjustments(sentiment_data))

        # 2. Regime-based adjustments
        adjustments.update(self._calculate_regime_adjustments(regime_data))

        # 3. Dynamic market adjustments
        adjustments.update(self._calculate_dynamic_adjustments(
            symbol, market_data, current_portfolio
        ))

        # 4. Strategy-specific adjustments
        adjustments.update(self._calculate_strategy_adjustments(
            strategy_signal, market_data
        ))

        return adjustments

    def _calculate_sentiment_adjustments(self, sentiment_data: Any) -> Dict[str, float]:
        """Calculate sentiment-based position size adjustments"""

        adjustments = {}

        if not sentiment_data or not self.config['sentiment_scaling_enabled']:
            adjustments['sentiment'] = 1.0
            adjustments['news_volume'] = 1.0
            return adjustments

        # Extract sentiment metrics
        sentiment_score = getattr(sentiment_data, 'sentiment_score', 0)
        sentiment_confidence = getattr(sentiment_data, 'confidence', 0)
        article_count = getattr(sentiment_data, 'article_count', 0)

        # Sentiment scaling configuration
        config = self.config['sentiment_scaling']

        # 1. Core sentiment adjustment
        if abs(sentiment_score) < config['neutral_threshold']:
            # Neutral sentiment - no adjustment
            sentiment_adj = 1.0
        elif sentiment_score > 0:
            # Positive sentiment - boost position
            intensity = min(sentiment_score / config['extreme_threshold'], 1.0)
            max_boost = config['max_boost']
            sentiment_adj = 1.0 + (intensity * (max_boost - 1.0))
        else:
            # Negative sentiment - reduce position
            intensity = min(abs(sentiment_score) / config['extreme_threshold'], 1.0)
            max_reduction = config['max_reduction']
            sentiment_adj = 1.0 - (intensity * (1.0 - max_reduction))

        # Apply confidence weighting
        if sentiment_confidence > 0:
            confidence_weight = sentiment_confidence / 100.0
            sentiment_adj = 1.0 + ((sentiment_adj - 1.0) * confidence_weight)

        adjustments['sentiment'] = sentiment_adj

        # 2. News volume adjustment
        volume_config = self.config['news_volume_scaling']

        if article_count >= volume_config['volume_threshold']:
            # High news volume - boost position (more conviction)
            news_adj = volume_config['high_volume_boost']
        elif article_count <= 1:
            # Low news volume - reduce position (less conviction)
            news_adj = volume_config['low_volume_reduction']
        else:
            # Normal news volume
            news_adj = 1.0

        adjustments['news_volume'] = news_adj

        return adjustments

    def _calculate_regime_adjustments(self, regime_data: Dict) -> Dict[str, float]:
        """Calculate regime-based position size adjustments"""

        adjustments = {}

        if not self.config['regime_scaling_enabled']:
            adjustments['market_regime'] = 1.0
            adjustments['entropy_regime'] = 1.0
            adjustments['volatility_regime'] = 1.0
            adjustments['regime_transition'] = 1.0
            return adjustments

        # 1. Market regime adjustment
        market_regime = regime_data.get('market_regime', 'UNKNOWN')
        market_confidence = regime_data.get('regime_confidence', 0)

        market_multiplier = self.config['market_regime_multipliers'].get(market_regime, 1.0)

        # Weight by confidence
        if market_confidence > 0:
            confidence_weight = market_confidence / 100.0
            market_adj = 1.0 + ((market_multiplier - 1.0) * confidence_weight)
        else:
            market_adj = market_multiplier

        adjustments['market_regime'] = market_adj

        # 2. Entropy regime adjustment
        entropy_regime = regime_data.get('entropy_regime', 'UNKNOWN')
        entropy_confidence = regime_data.get('entropy_confidence', 0)

        entropy_multiplier = self.config['entropy_regime_multipliers'].get(entropy_regime, 1.0)

        if entropy_confidence > 0:
            confidence_weight = entropy_confidence / 100.0
            entropy_adj = 1.0 + ((entropy_multiplier - 1.0) * confidence_weight)
        else:
            entropy_adj = entropy_multiplier

        adjustments['entropy_regime'] = entropy_adj

        # 3. Volatility regime adjustment
        volatility_regime = regime_data.get('volatility_regime', 'UNKNOWN')
        vol_multiplier = self.config['volatility_regime_multipliers'].get(volatility_regime, 1.0)
        adjustments['volatility_regime'] = vol_multiplier

        # 4. Regime transition adjustment
        is_transition = regime_data.get('regime_transition', False)
        transition_significance = regime_data.get('transition_significance', 'stable')

        if is_transition:
            transition_config = self.config['regime_transition_scaling']
            adjustments['regime_transition'] = transition_config.get(transition_significance, 1.0)
        else:
            adjustments['regime_transition'] = 1.0

        return adjustments

    def _calculate_dynamic_adjustments(self,
                                     symbol: str,
                                     market_data: pd.DataFrame,
                                     current_portfolio: Optional[Dict]) -> Dict[str, float]:
        """Calculate dynamic market-based adjustments"""

        adjustments = {}

        if not self.config['dynamic_scaling_enabled']:
            adjustments['correlation'] = 1.0
            adjustments['momentum'] = 1.0
            adjustments['mean_reversion'] = 1.0
            return adjustments

        # 1. Correlation adjustment (reduce size if portfolio highly correlated)
        correlation_adj = self._calculate_correlation_adjustment(symbol, current_portfolio)
        adjustments['correlation'] = correlation_adj

        # 2. Momentum adjustment
        momentum_adj = self._calculate_momentum_adjustment(market_data)
        adjustments['momentum'] = momentum_adj

        # 3. Mean reversion adjustment
        mean_reversion_adj = self._calculate_mean_reversion_adjustment(market_data)
        adjustments['mean_reversion'] = mean_reversion_adj

        return adjustments

    def _calculate_correlation_adjustment(self, symbol: str, current_portfolio: Optional[Dict]) -> float:
        """Adjust position size based on portfolio correlation"""

        if not current_portfolio or 'positions' not in current_portfolio:
            return 1.0

        # Simplified correlation check
        # In real implementation, you'd calculate actual correlations
        current_positions = len(current_portfolio['positions'])

        if current_positions >= 8:
            # High diversification - normal sizing
            return 1.0
        elif current_positions >= 5:
            # Medium diversification - slight reduction
            return 0.95
        else:
            # Low diversification - reduce new position size
            return 0.9

    def _calculate_momentum_adjustment(self, market_data: pd.DataFrame) -> float:
        """Adjust position size based on momentum characteristics"""

        if len(market_data) < 20:
            return 1.0

        # Calculate price momentum (20-day return)
        returns = market_data['Close'].pct_change()
        momentum_20d = (market_data['Close'].iloc[-1] / market_data['Close'].iloc[-21] - 1) if len(market_data) >= 21 else 0

        # Strong momentum gets slight boost
        if abs(momentum_20d) > 0.05:  # 5%+ move
            return 1.05
        else:
            return 1.0

    def _calculate_mean_reversion_adjustment(self, market_data: pd.DataFrame) -> float:
        """Adjust position size based on mean reversion characteristics"""

        if len(market_data) < 50:
            return 1.0

        # Calculate distance from 50-day moving average
        sma_50 = market_data['Close'].rolling(50).mean().iloc[-1]
        current_price = market_data['Close'].iloc[-1]
        distance_from_mean = (current_price - sma_50) / sma_50

        # Extreme deviations get slight boost (mean reversion opportunity)
        if abs(distance_from_mean) > 0.1:  # 10%+ deviation
            return 1.05
        else:
            return 1.0

    def _calculate_strategy_adjustments(self, strategy_signal: Any, market_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate strategy-specific adjustments"""

        # Get strategy type and signal strength
        strategy_type = getattr(strategy_signal, 'strategy_type', 'UNKNOWN')
        signal_strength = getattr(strategy_signal, 'signal_strength', 1.0)

        # Strategy-specific multipliers (from your existing config)
        strategy_multiplier = self.config['strategy_multipliers'].get(strategy_type, 1.0)

        return {'strategy': strategy_multiplier}

    def _calculate_enhanced_risk_metrics(self,
                                       market_data: pd.DataFrame,
                                       position_size: float,
                                       sentiment_data: Any,
                                       regime_data: Dict) -> Dict[str, float]:
        """Calculate enhanced risk metrics including sentiment and regime factors"""

        # Base risk metrics
        base_metrics = super()._calculate_risk_metrics(market_data, position_size)

        # Enhanced metrics
        enhanced_metrics = base_metrics.copy()

        # Sentiment risk adjustment
        sentiment_score = getattr(sentiment_data, 'sentiment_score', 0)
        sentiment_vol_multiplier = 1.0 + (abs(sentiment_score) * 0.2)  # Extreme sentiment increases risk
        enhanced_metrics['sentiment_adjusted_volatility'] = base_metrics['volatility'] * sentiment_vol_multiplier

        # Regime risk adjustment
        market_regime = regime_data.get('market_regime', 'UNKNOWN')
        regime_risk_multipliers = {
            'HIGH_VOLATILITY': 1.5,
            'BEAR_MARKET': 1.3,
            'BULL_MARKET': 0.9,
            'SIDEWAYS': 1.0,
            'LOW_VOLATILITY': 0.8
        }

        regime_multiplier = regime_risk_multipliers.get(market_regime, 1.0)
        enhanced_metrics['regime_adjusted_var'] = base_metrics['var_1d'] * regime_multiplier

        return enhanced_metrics

    def _generate_enhanced_rationale(self,
                                   symbol: str,
                                   base_size: float,
                                   final_size: float,
                                   adjustments: Dict,
                                   sentiment_data: Any,
                                   regime_data: Dict,
                                   confidence: float) -> Dict[str, str]:
        """Generate comprehensive rationale for enhanced position sizing"""

        size_change_pct = ((final_size - base_size) / base_size) * 100

        # Main rationale
        main_rationale = f"{symbol}: {final_size:.1%} allocation ({size_change_pct:+.0f}% vs base)\n"
        main_rationale += f"Confidence: {confidence:.0f}% | Base: {base_size:.1%}"

        # Sentiment rationale
        sentiment_score = getattr(sentiment_data, 'sentiment_score', 0)
        sentiment_confidence = getattr(sentiment_data, 'confidence', 0)
        article_count = getattr(sentiment_data, 'article_count', 0)

        sentiment_rationale = f"Sentiment Analysis:\n"
        sentiment_rationale += f"• Score: {sentiment_score:+.2f} (confidence: {sentiment_confidence:.0f}%)\n"
        sentiment_rationale += f"• News volume: {article_count} articles\n"
        sentiment_rationale += f"• Position adjustment: {adjustments['sentiment']:.1%}"

        # Regime rationale
        market_regime = regime_data.get('market_regime', 'UNKNOWN')
        entropy_regime = regime_data.get('entropy_regime', 'UNKNOWN')
        regime_confidence = regime_data.get('regime_confidence', 0)

        regime_rationale = f"Regime Analysis:\n"
        regime_rationale += f"• Market regime: {market_regime} (conf: {regime_confidence:.0f}%)\n"
        regime_rationale += f"• Entropy regime: {entropy_regime}\n"
        regime_rationale += f"• Combined adjustment: {adjustments['market_regime'] * adjustments['entropy_regime']:.1%}"

        # Dynamic rationale
        dynamic_rationale = f"Dynamic Factors:\n"
        dynamic_rationale += f"• Correlation adj: {adjustments['correlation']:.1%}\n"
        dynamic_rationale += f"• Momentum adj: {adjustments['momentum']:.1%}\n"
        dynamic_rationale += f"• Mean reversion adj: {adjustments['mean_reversion']:.1%}"

        return {
            'main': main_rationale,
            'sentiment': sentiment_rationale,
            'regime': regime_rationale,
            'dynamic': dynamic_rationale
        }

    def _generate_enhanced_warnings(self,
                                  position_size: float,
                                  risk_metrics: Dict,
                                  sentiment_data: Any,
                                  regime_data: Dict) -> List[str]:
        """Generate enhanced warnings including sentiment and regime factors"""

        warnings = []

        # Size warnings
        if position_size > 0.12:  # 12%+ position
            warnings.append(f"⚠️ Large position ({position_size:.1%}) - monitor closely")

        if position_size >= 0.15:  # At maximum
            warnings.append(f"🚨 Maximum position size reached ({position_size:.1%})")

        # Sentiment warnings
        sentiment_score = getattr(sentiment_data, 'sentiment_score', 0)
        sentiment_confidence = getattr(sentiment_data, 'confidence', 0)

        if abs(sentiment_score) > 0.8:
            warnings.append(f"⚠️ Extreme sentiment ({sentiment_score:+.2f}) - high volatility expected")

        if sentiment_confidence < 30:
            warnings.append(f"⚠️ Low sentiment confidence ({sentiment_confidence:.0f}%)")

        # Regime warnings
        market_regime = regime_data.get('market_regime', 'UNKNOWN')

        if market_regime in ['HIGH_VOLATILITY', 'BEAR_MARKET']:
            warnings.append(f"⚠️ Challenging market regime: {market_regime}")

        if regime_data.get('regime_transition', False):
            warnings.append(f"⚠️ Market regime in transition - increased uncertainty")

        # Risk warnings
        if risk_metrics.get('volatility', 0) > 0.35:
            warnings.append(f"⚠️ High volatility ({risk_metrics['volatility']:.1%})")

        return warnings

    def calculate_portfolio_allocation(self,
                                     strategy_signals: List[Any],
                                     market_data_dict: Dict[str, pd.DataFrame],
                                     sentiment_data_dict: Dict[str, Any],
                                     regime_data: Dict,
                                     current_portfolio: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Calculate optimal portfolio allocation across multiple positions

        Ensures total allocation doesn't exceed reasonable limits while
        maximizing risk-adjusted returns based on sentiment and regime.
        """

        print(f"🎯 Calculating portfolio allocation for {len(strategy_signals)} signals...")

        # Calculate individual position sizes
        position_results = []

        for signal in strategy_signals:
            symbol = getattr(signal, 'symbol', 'UNKNOWN')

            if symbol not in market_data_dict or symbol not in sentiment_data_dict:
                continue

            result = self.calculate_enhanced_position_size(
                symbol=symbol,
                strategy_signal=signal,
                market_data=market_data_dict[symbol],
                sentiment_data=sentiment_data_dict[symbol],
                regime_data=regime_data,
                current_portfolio=current_portfolio
            )

            position_results.append(result)

        # Sort by attractiveness (size * confidence)
        position_results.sort(
            key=lambda x: x.recommended_size * x.confidence_score,
            reverse=True
        )

        # Apply portfolio-level constraints
        final_allocation = self._apply_portfolio_constraints(position_results)

        # Generate portfolio summary
        portfolio_summary = self._generate_portfolio_summary(final_allocation, regime_data)

        print(f"✅ Portfolio allocation complete:")
        print(f"   Total positions: {len(final_allocation)}")
        print(f"   Total allocation: {sum(p.recommended_size for p in final_allocation):.1%}")
        print(f"   Average confidence: {np.mean([p.confidence_score for p in final_allocation]):.1f}%")

        return {
            'positions': final_allocation,
            'summary': portfolio_summary,
            'total_allocation': sum(p.recommended_size for p in final_allocation),
            'position_count': len(final_allocation),
            'timestamp': datetime.now()
        }

    def _apply_portfolio_constraints(self, position_results: List[EnhancedPositionSizeResult]) -> List[EnhancedPositionSizeResult]:
        """Apply portfolio-level constraints and optimization"""

        # Maximum total allocation (e.g., 80% of portfolio)
        max_total_allocation = 0.80

        # Calculate current total allocation
        total_allocation = sum(p.recommended_size for p in position_results)

        if total_allocation <= max_total_allocation:
            # No scaling needed
            return position_results

        # Scale down proportionally
        scale_factor = max_total_allocation / total_allocation

        scaled_results = []
        for result in position_results:
            # Create new result with scaled size
            scaled_size = result.recommended_size * scale_factor

            # Don't scale below minimum threshold
            if scaled_size >= 0.02:  # 2% minimum
                # Update the result (in real implementation, you'd create a new instance)
                result.recommended_size = scaled_size
                scaled_results.append(result)

        return scaled_results

    def _generate_portfolio_summary(self, positions: List[EnhancedPositionSizeResult], regime_data: Dict) -> Dict[str, Any]:
        """Generate comprehensive portfolio allocation summary"""

        if not positions:
            return {}

        total_allocation = sum(p.recommended_size for p in positions)
        avg_confidence = np.mean([p.confidence_score for p in positions])
        avg_sentiment = np.mean([p.sentiment_score for p in positions])

        # Risk metrics
        total_risk = sum(p.risk_contribution for p in positions)
        max_position = max(p.recommended_size for p in positions)

        # Regime analysis
        market_regime = regime_data.get('market_regime', 'UNKNOWN')
        regime_confidence = regime_data.get('regime_confidence', 0)

        return {
            'total_allocation': total_allocation,
            'position_count': len(positions),
            'average_confidence': avg_confidence,
            'average_sentiment': avg_sentiment,
            'largest_position': max_position,
            'total_risk_contribution': total_risk,
            'market_regime': market_regime,
            'regime_confidence': regime_confidence,
            'diversification_score': self._calculate_diversification_score(positions),
            'risk_score': self._calculate_portfolio_risk_score(positions),
            'allocation_efficiency': avg_confidence * total_allocation  # Higher = better
        }

    def _calculate_diversification_score(self, positions: List[EnhancedPositionSizeResult]) -> float:
        """Calculate portfolio diversification score (0-100)"""

        if len(positions) <= 1:
            return 0.0

        # Simple diversification metrics
        position_count = len(positions)
        max_position = max(p.recommended_size for p in positions)
        min_position = min(p.recommended_size for p in positions)

        # Diversification factors
        count_factor = min(100, position_count * 10)  # More positions = better diversification
        concentration_penalty = max_position * 100    # High concentration = lower score
        balance_factor = (min_position / max_position) * 100 if max_position > 0 else 0

        # Combined diversification score
        diversification_score = (count_factor + balance_factor - concentration_penalty) / 2
        return max(0, min(100, diversification_score))

    def _calculate_portfolio_risk_score(self, positions: List[EnhancedPositionSizeResult]) -> float:
        """Calculate overall portfolio risk score (0-100, lower is better)"""

        if not positions:
            return 0.0

        # Risk factors
        avg_volatility = np.mean([p.estimated_volatility for p in positions])
        total_allocation = sum(p.recommended_size for p in positions)
        max_position = max(p.recommended_size for p in positions)

        # Warning count
        total_warnings = sum(len(p.risk_warnings) for p in positions)
        warning_penalty = min(50, total_warnings * 5)

        # Risk score components
        volatility_risk = min(50, avg_volatility * 100)  # High vol = high risk
        concentration_risk = min(30, max_position * 200)  # High concentration = high risk
        allocation_risk = min(20, total_allocation * 25)  # Over-allocation = high risk

        # Combined risk score
        risk_score = volatility_risk + concentration_risk + allocation_risk + warning_penalty
        return min(100, risk_score)


# Integration example with your real-time trading system
class EnhancedRealTimeTradingOrchestrator:
    """
    Real-time trading orchestrator with enhanced position sizing
    """

    def __init__(self, alpaca_config: Dict, xgboost_model_path: str = None):
        # Initialize components
        from trading_engine.portfolio_manager.orchestrator import XGBoostTradingOrchestrator

        self.trading_orchestrator = XGBoostTradingOrchestrator()
        self.enhanced_position_sizer = EnhancedPositionSizer()

        # Replace default position sizer with enhanced version
        self.trading_orchestrator.position_sizer = self.enhanced_position_sizer

        print("🎯 Enhanced Real-time Trading Orchestrator initialized")

    async def execute_enhanced_trading_cycle(self,
                                           symbols: List[str],
                                           market_data_dict: Dict[str, pd.DataFrame],
                                           sentiment_data_dict: Dict[str, Any],
                                           regime_data: Dict) -> Dict[str, Any]:
        """
        Execute enhanced trading cycle with sentiment and regime-based position sizing
        """

        print(f"\n🚀 Enhanced Trading Cycle - {datetime.now().strftime('%H:%M:%S')}")

        cycle_results = {
            'trades_executed': [],
            'position_analysis': {},
            'portfolio_allocation': {},
            'risk_analysis': {},
            'regime_impact': {},
            'sentiment_impact': {}
        }

        # 1. Generate strategy signals for each symbol
        strategy_signals = []

        for symbol in symbols:
            if symbol not in market_data_dict or symbol not in sentiment_data_dict:
                continue

            try:
                # Get XGBoost strategy recommendation
                strategy_rec = await self._get_strategy_recommendation(
                    symbol, market_data_dict[symbol], sentiment_data_dict[symbol], regime_data
                )

                if strategy_rec and strategy_rec['confidence'] >= 70:
                    # Create strategy signal object
                    signal = self._create_strategy_signal(symbol, strategy_rec, market_data_dict[symbol])
                    strategy_signals.append(signal)

            except Exception as e:
                print(f"❌ Error generating signal for {symbol}: {e}")
                continue

        print(f"📊 Generated {len(strategy_signals)} actionable signals")

        # 2. Calculate enhanced portfolio allocation
        if strategy_signals:
            portfolio_allocation = self.enhanced_position_sizer.calculate_portfolio_allocation(
                strategy_signals=strategy_signals,
                market_data_dict=market_data_dict,
                sentiment_data_dict=sentiment_data_dict,
                regime_data=regime_data,
                current_portfolio=self.trading_orchestrator.portfolio
            )

            cycle_results['portfolio_allocation'] = portfolio_allocation

            # 3. Execute trades based on enhanced sizing
            trades_executed = await self._execute_enhanced_trades(portfolio_allocation)
            cycle_results['trades_executed'] = trades_executed

            # 4. Analyze results
            cycle_results['position_analysis'] = self._analyze_position_decisions(portfolio_allocation)
            cycle_results['risk_analysis'] = self._analyze_portfolio_risk(portfolio_allocation)
            cycle_results['regime_impact'] = self._analyze_regime_impact(portfolio_allocation, regime_data)
            cycle_results['sentiment_impact'] = self._analyze_sentiment_impact(portfolio_allocation, sentiment_data_dict)

        # 5. Generate comprehensive report
        self._generate_cycle_report(cycle_results)

        return cycle_results

    async def _get_strategy_recommendation(self, symbol: str, df: pd.DataFrame,
                                         sentiment_data: Any, regime_data: Dict) -> Dict:
        """Get XGBoost strategy recommendation with enhanced features"""

        # Prepare enhanced features including sentiment and regime
        features = {
            # Technical features
            'rsi_14': df['RSI'].iloc[-1] if 'RSI' in df.columns else 50,
            'bb_width': df.get('bb_width', pd.Series([0])).iloc[-1],
            'macd_signal': df.get('MACD_Signal', pd.Series([0])).iloc[-1],
            'volume_ratio': df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1] if len(df) >= 20 else 1,

            # Sentiment features
            'finbert_sentiment': getattr(sentiment_data, 'sentiment_score', 0),
            'sentiment_confidence': getattr(sentiment_data, 'confidence', 0),
            'article_count': getattr(sentiment_data, 'article_count', 0),
            'news_momentum': getattr(sentiment_data, 'sentiment_momentum', 0),

            # Regime features
            'market_regime_encoded': self._encode_regime(regime_data.get('market_regime', 'UNKNOWN')),
            'regime_confidence': regime_data.get('regime_confidence', 0),
            'entropy_regime_encoded': self._encode_regime(regime_data.get('entropy_regime', 'UNKNOWN')),
            'volatility_regime_encoded': self._encode_regime(regime_data.get('volatility_regime', 'UNKNOWN')),

            # Market context
            'vix_level': regime_data.get('vix_level', 20),
            'volatility_20': df['Close'].pct_change().rolling(20).std().iloc[-1] * np.sqrt(252) if len(df) >= 20 else 0.2
        }

        # Use XGBoost model for strategy selection
        if hasattr(self.trading_orchestrator, 'xgb_strategy_model') and self.trading_orchestrator.xgb_strategy_model:
            feature_array = np.array(list(features.values())).reshape(1, -1)
            probabilities = self.trading_orchestrator.xgb_strategy_model.predict_proba(feature_array)[0]

            best_idx = np.argmax(probabilities)
            strategy_name = self.trading_orchestrator.strategy_encoder.inverse_transform([best_idx])[0]
            confidence = probabilities[best_idx] * 100

            return {
                'strategy': strategy_name,
                'confidence': confidence,
                'features_used': features,
                'reasoning': f"XGBoost selected {strategy_name} based on sentiment ({features['finbert_sentiment']:.2f}) and regime ({regime_data.get('market_regime', 'UNKNOWN')})"
            }

        return None

    def _create_strategy_signal(self, symbol: str, strategy_rec: Dict, df: pd.DataFrame):
        """Create strategy signal object with enhanced metadata"""

        class EnhancedSignal:
            def __init__(self, symbol, strategy_rec, df):
                self.symbol = symbol
                self.strategy_type = strategy_rec['strategy']
                self.confidence = strategy_rec['confidence']
                self.signal_strength = min(2.0, strategy_rec['confidence'] / 50.0)  # 1.0-2.0 range
                self.features_used = strategy_rec['features_used']
                self.reasoning = strategy_rec['reasoning']

                # Market data reference
                self.market_data = df

                # Determine action based on strategy and market conditions
                self.action = self._determine_action()

            def _determine_action(self):
                # Simplified action determination
                if self.confidence >= 80:
                    return 'BUY'
                elif self.confidence >= 70:
                    return 'BUY'  # Could be more nuanced
                else:
                    return 'HOLD'

        return EnhancedSignal(symbol, strategy_rec, df)

    async def _execute_enhanced_trades(self, portfolio_allocation: Dict) -> List[Dict]:
        """Execute trades with enhanced position sizing"""

        trades_executed = []
        positions = portfolio_allocation.get('positions', [])

        for position_result in positions:
            try:
                # Get current market price
                symbol = position_result.symbol
                recommended_size = position_result.recommended_size

                # Calculate dollar amount
                portfolio_value = self.trading_orchestrator.portfolio['cash'] + sum(
                    pos.get('value', 0) for pos in self.trading_orchestrator.portfolio['positions'].values()
                )

                dollar_amount = recommended_size * portfolio_value

                # Skip if amount too small
                if dollar_amount < 100:  # Minimum $100 position
                    continue

                # Execute trade (simplified)
                trade_result = {
                    'symbol': symbol,
                    'action': 'BUY',
                    'dollar_amount': dollar_amount,
                    'position_size_pct': recommended_size,
                    'confidence': position_result.confidence_score,
                    'strategy': position_result.sizing_method,
                    'sentiment_score': position_result.sentiment_score,
                    'sentiment_adjustment': position_result.sentiment_adjustment,
                    'regime_adjustment': position_result.market_regime_adjustment * position_result.entropy_regime_adjustment,
                    'rationale': position_result.sizing_rationale,
                    'warnings': position_result.risk_warnings,
                    'timestamp': datetime.now()
                }

                trades_executed.append(trade_result)

                print(f"✅ ENHANCED TRADE: {symbol}")
                print(f"   Size: {recommended_size:.1%} (${dollar_amount:,.0f})")
                print(f"   Confidence: {position_result.confidence_score:.1f}%")
                print(f"   Sentiment: {position_result.sentiment_score:+.2f} (adj: {position_result.sentiment_adjustment:.1%})")
                print(f"   Regime adj: {position_result.market_regime_adjustment:.1%}")

            except Exception as e:
                print(f"❌ Error executing trade for {position_result.symbol}: {e}")
                continue

        return trades_executed

    def _analyze_position_decisions(self, portfolio_allocation: Dict) -> Dict:
        """Analyze position sizing decisions"""

        positions = portfolio_allocation.get('positions', [])
        if not positions:
            return {}

        # Analyze sizing factors
        sentiment_impact = np.mean([p.sentiment_adjustment for p in positions])
        regime_impact = np.mean([p.market_regime_adjustment * p.entropy_regime_adjustment for p in positions])
        correlation_impact = np.mean([p.correlation_adjustment for p in positions])

        # Size distribution
        sizes = [p.recommended_size for p in positions]

        return {
            'total_positions': len(positions),
            'average_size': np.mean(sizes),
            'largest_position': max(sizes),
            'size_std': np.std(sizes),
            'sentiment_impact': sentiment_impact,
            'regime_impact': regime_impact,
            'correlation_impact': correlation_impact,
            'positions_at_max': len([p for p in positions if p.recommended_size >= 0.14]),  # Near 15% max
            'high_confidence_positions': len([p for p in positions if p.confidence_score >= 85])
        }

    def _analyze_portfolio_risk(self, portfolio_allocation: Dict) -> Dict:
        """Analyze overall portfolio risk metrics"""

        summary = portfolio_allocation.get('summary', {})
        positions = portfolio_allocation.get('positions', [])

        if not positions:
            return {}

        # Risk concentrations
        total_warnings = sum(len(p.risk_warnings) for p in positions)
        high_vol_positions = len([p for p in positions if p.estimated_volatility > 0.30])

        # Sentiment risks
        extreme_sentiment_positions = len([p for p in positions if abs(p.sentiment_score) > 0.7])

        return {
            'total_allocation': summary.get('total_allocation', 0),
            'risk_score': summary.get('risk_score', 0),
            'diversification_score': summary.get('diversification_score', 0),
            'total_warnings': total_warnings,
            'high_volatility_positions': high_vol_positions,
            'extreme_sentiment_positions': extreme_sentiment_positions,
            'allocation_efficiency': summary.get('allocation_efficiency', 0)
        }

    def _analyze_regime_impact(self, portfolio_allocation: Dict, regime_data: Dict) -> Dict:
        """Analyze how regime detection impacted position sizing"""

        positions = portfolio_allocation.get('positions', [])
        if not positions:
            return {}

        market_regime = regime_data.get('market_regime', 'UNKNOWN')
        entropy_regime = regime_data.get('entropy_regime', 'UNKNOWN')
        regime_confidence = regime_data.get('regime_confidence', 0)

        # Calculate regime adjustments
        market_adjustments = [p.market_regime_adjustment for p in positions]
        entropy_adjustments = [p.entropy_regime_adjustment for p in positions]
        combined_adjustments = [m * e for m, e in zip(market_adjustments, entropy_adjustments)]

        return {
            'market_regime': market_regime,
            'entropy_regime': entropy_regime,
            'regime_confidence': regime_confidence,
            'avg_market_adjustment': np.mean(market_adjustments),
            'avg_entropy_adjustment': np.mean(entropy_adjustments),
            'avg_combined_adjustment': np.mean(combined_adjustments),
            'positions_boosted': len([adj for adj in combined_adjustments if adj > 1.05]),
            'positions_reduced': len([adj for adj in combined_adjustments if adj < 0.95])
        }

    def _analyze_sentiment_impact(self, portfolio_allocation: Dict, sentiment_data_dict: Dict) -> Dict:
        """Analyze how sentiment impacted position sizing"""

        positions = portfolio_allocation.get('positions', [])
        if not positions:
            return {}

        # Sentiment metrics
        sentiment_scores = [p.sentiment_score for p in positions]
        sentiment_adjustments = [p.sentiment_adjustment for p in positions]
        news_adjustments = [p.news_volume_adjustment for p in positions]

        # Overall sentiment analysis
        avg_sentiment = np.mean(sentiment_scores)
        sentiment_dispersion = np.std(sentiment_scores)

        return {
            'average_sentiment': avg_sentiment,
            'sentiment_dispersion': sentiment_dispersion,
            'avg_sentiment_adjustment': np.mean(sentiment_adjustments),
            'avg_news_adjustment': np.mean(news_adjustments),
            'positive_sentiment_positions': len([s for s in sentiment_scores if s > 0.1]),
            'negative_sentiment_positions': len([s for s in sentiment_scores if s < -0.1]),
            'extreme_sentiment_positions': len([s for s in sentiment_scores if abs(s) > 0.7]),
            'high_news_volume_positions': len([p for p in positions if getattr(p, 'article_count', 0) >= 5])
        }

    def _generate_cycle_report(self, cycle_results: Dict):
        """Generate comprehensive trading cycle report"""

        print(f"\n📊 ENHANCED TRADING CYCLE REPORT")
        print(f"=" * 60)

        # Trades executed
        trades = cycle_results.get('trades_executed', [])
        print(f"💰 TRADES EXECUTED: {len(trades)}")

        if trades:
            total_allocated = sum(t['position_size_pct'] for t in trades)
            avg_confidence = np.mean([t['confidence'] for t in trades])
            avg_sentiment = np.mean([t['sentiment_score'] for t in trades])

            print(f"   Total allocation: {total_allocated:.1%}")
            print(f"   Average confidence: {avg_confidence:.1f}%")
            print(f"   Average sentiment: {avg_sentiment:+.2f}")

        # Position analysis
        pos_analysis = cycle_results.get('position_analysis', {})
        if pos_analysis:
            print(f"\n📈 POSITION ANALYSIS:")
            print(f"   Total positions: {pos_analysis.get('total_positions', 0)}")
            print(f"   Average size: {pos_analysis.get('average_size', 0):.1%}")
            print(f"   Sentiment impact: {pos_analysis.get('sentiment_impact', 1):.1%}")
            print(f"   Regime impact: {pos_analysis.get('regime_impact', 1):.1%}")

        # Risk analysis
        risk_analysis = cycle_results.get('risk_analysis', {})
        if risk_analysis:
            print(f"\n⚠️ RISK ANALYSIS:")
            print(f"   Diversification score: {risk_analysis.get('diversification_score', 0):.0f}/100")
            print(f"   Risk score: {risk_analysis.get('risk_score', 0):.0f}/100")
            print(f"   Total warnings: {risk_analysis.get('total_warnings', 0)}")

        # Regime impact
        regime_impact = cycle_results.get('regime_impact', {})
        if regime_impact:
            print(f"\n🔄 REGIME IMPACT:")
            print(f"   Market regime: {regime_impact.get('market_regime', 'UNKNOWN')}")
            print(f"   Combined adjustment: {regime_impact.get('avg_combined_adjustment', 1):.1%}")
            print(f"   Positions boosted: {regime_impact.get('positions_boosted', 0)}")
            print(f"   Positions reduced: {regime_impact.get('positions_reduced', 0)}")

        # Sentiment impact
        sentiment_impact = cycle_results.get('sentiment_impact', {})
        if sentiment_impact:
            print(f"\n🧠 SENTIMENT IMPACT:")
            print(f"   Average sentiment: {sentiment_impact.get('average_sentiment', 0):+.2f}")
            print(f"   Sentiment adjustment: {sentiment_impact.get('avg_sentiment_adjustment', 1):.1%}")
            print(f"   Positive positions: {sentiment_impact.get('positive_sentiment_positions', 0)}")
            print(f"   Negative positions: {sentiment_impact.get('negative_sentiment_positions', 0)}")

        print(f"=" * 60)

    def _encode_regime(self, regime: str) -> int:
        """Encode regime string to integer for ML models"""
        regime_mapping = {
            'BULL_MARKET': 0, 'BULLISH_TRENDING': 0,
            'BEAR_MARKET': 1, 'BEARISH_TRENDING': 1,
            'SIDEWAYS': 2, 'RANGE_BOUND_STABLE': 2,
            'HIGH_VOLATILITY': 3, 'VOLATILE': 3,
            'LOW_VOLATILITY': 4, 'STABLE': 4,
            'TRENDING': 5, 'MEAN_REVERTING': 6,
            'UNKNOWN': 7
        }
        return regime_mapping.get(regime, 7)


# Example usage
if __name__ == "__main__":
    # Example of how to use the enhanced position sizer

    # Initialize enhanced position sizer
    enhanced_sizer = EnhancedPositionSizer()

    # Mock data for example
    class MockSignal:
        def __init__(self, symbol, confidence, strategy_type):
            self.symbol = symbol
            self.confidence = confidence
            self.strategy_type = strategy_type
            self.signal_strength = confidence / 50.0

    class MockSentiment:
        def __init__(self, sentiment_score, confidence, article_count):
            self.sentiment_score = sentiment_score
            self.confidence = confidence
            self.article_count = article_count

    # Example calculation
    symbol = "AAPL"
    signal = MockSignal("AAPL", 85.0, "momentum")

    # Create sample market data
    dates = pd.date_range(start='2024-01-01', periods=100, freq='D')
    prices = 150 + np.cumsum(np.random.randn(100) * 0.02)
    market_data = pd.DataFrame({
        'Close': prices,
        'Volume': np.random.randint(1000000, 5000000, 100)
    }, index=dates)

    # Mock sentiment and regime data
    sentiment_data = MockSentiment(0.75, 85.0, 8)  # Strong positive sentiment

    regime_data = {
        'market_regime': 'BULL_MARKET',
        'regime_confidence': 90.0,
        'entropy_regime': 'TRENDING',
        'entropy_confidence': 85.0,
        'volatility_regime': 'NORMAL_VOLATILITY',
        'regime_transition': False
    }

    # Calculate enhanced position size
    result = enhanced_sizer.calculate_enhanced_position_size(
        symbol=symbol,
        strategy_signal=signal,
        market_data=market_data,
        sentiment_data=sentiment_data,
        regime_data=regime_data
    )

    print(f"\n🎯 ENHANCED POSITION SIZE EXAMPLE:")
    print(f"Symbol: {result.symbol}")
    print(f"Recommended size: {result.recommended_size:.1%}")
    print(f"Base size: {result.base_size:.1%}")
    print(f"Confidence: {result.confidence_score:.1f}%")
    print(f"\nAdjustments:")
    print(f"• Sentiment: {result.sentiment_adjustment:.1%} (score: {result.sentiment_score:+.2f})")
    print(f"• Market regime: {result.market_regime_adjustment:.1%}")
    print(f"• Entropy regime: {result.entropy_regime_adjustment:.1%}")
    print(f"• Correlation: {result.correlation_adjustment:.1%}")
    print(f"\nRationale:")
    print(result.sizing_rationale)
    print(f"\nSentiment Analysis:")
    print(result.sentiment_rationale)
    print(f"\nRegime Analysis:")
    print(result.regime_rationale)

    if result.risk_warnings:
        print(f"\n⚠️ Warnings:")
        for warning in result.risk_warnings:
            print(f"   {warning}")
