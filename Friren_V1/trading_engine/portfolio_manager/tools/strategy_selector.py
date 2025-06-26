"""
Pure Strategy Selector - Extracted from PortfolioStrategySelector

This module contains ONLY strategy selection logic:
- Strategy fitness evaluation
- Strategy ranking and filtering
- Strategy diversification
- Strategy metadata matching

All market analysis, risk assessment, and portfolio construction
has been moved to separate components.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Import from strategies package (pure strategies only)
from .strategies import (
    BaseStrategy, StrategySignal, StrategyMetadata,
    discover_all_strategies, get_strategies_by_category,
    STRATEGY_CATEGORIES
)

@dataclass
class StrategyRecommendation:
    """Strategy selection result"""
    strategy_name: str
    signal: StrategySignal
    strategy_metadata: StrategyMetadata
    market_fit_score: float  # 0-100 how well strategy fits current market
    selection_confidence: float  # 0-100 confidence in this strategy selection

class StrategySelector:
    """
    PURE Strategy Selection Component

    Responsibilities:
    - Evaluate strategy fitness for current market conditions
    - Rank strategies by suitability
    - Apply diversification rules
    - Filter strategies by confidence thresholds

    Does NOT:
    - Analyze market regimes (MarketAnalyzer does this)
    - Assess portfolio risk (RiskAnalyzer does this)
    - Calculate position sizes (PositionSizer does this)
    - Manage sentiment (SentimentAnalyzer does this)
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()

        # Load all available strategies
        self.strategies = discover_all_strategies()
        self.strategy_performance = self._initialize_performance_tracking()

        print(f"Strategy Selector initialized with {len(self.strategies)} strategies")

    def _default_config(self) -> Dict:
        """Default configuration for strategy selection"""
        return {
            'max_strategies_per_selection': 3,
            'min_confidence_threshold': 60.0,
            'diversification_penalty': 0.1,  # Penalty for similar strategies
            'performance_weight': 0.15,  # How much historical performance affects selection
        }

    def _initialize_performance_tracking(self) -> Dict:
        """Initialize performance tracking for all strategies"""
        performance = {}
        for strategy_name in self.strategies.keys():
            performance[strategy_name] = {
                'total_signals': 0,
                'successful_signals': 0,
                'confidence_history': [],
                'last_used': None,
                'win_rate': 0.5,  # Start neutral
                'avg_confidence': 60.0
            }
        return performance

    def select_strategies(self, df: pd.DataFrame, market_regime: Dict,
                         sentiment: float = 0.0, max_strategies: int = None,
                         confidence_threshold: float = None) -> List[StrategyRecommendation]:
        """
        MAIN STRATEGY SELECTION METHOD

        Args:
            df: OHLCV DataFrame with technical indicators
            market_regime: Market regime analysis from MarketAnalyzer
            sentiment: Sentiment score from SentimentAnalyzer
            max_strategies: Maximum strategies to return
            confidence_threshold: Minimum confidence required

        Returns:
            List of StrategyRecommendation objects ranked by suitability
        """
        max_strategies = max_strategies or self.config['max_strategies_per_selection']
        confidence_threshold = confidence_threshold or self.config['min_confidence_threshold']

        recommendations = []

        # Test each strategy for market fit
        for strategy_name, strategy in self.strategies.items():
            try:
                # Skip pairs strategies for single symbol analysis
                if strategy.metadata.requires_pairs:
                    continue

                # Generate signal from strategy
                signal = strategy.generate_signal(df, sentiment, confidence_threshold)

                # Only consider actionable signals
                if signal.action == 'HOLD':
                    continue

                # Calculate market fit score
                market_fit_score = self._calculate_market_fit(strategy, market_regime, sentiment)

                # Calculate selection confidence (different from signal confidence)
                selection_confidence = self._calculate_selection_confidence(
                    strategy, signal, market_fit_score, strategy_name
                )

                # Only include if meets threshold
                if selection_confidence >= confidence_threshold:
                    recommendations.append(StrategyRecommendation(
                        strategy_name=strategy_name,
                        signal=signal,
                        strategy_metadata=strategy.metadata,
                        market_fit_score=market_fit_score,
                        selection_confidence=selection_confidence
                    ))

                    # Update performance tracking
                    self._update_performance_tracking(strategy_name, signal)

            except Exception as e:
                print(f"Error testing strategy {strategy_name}: {e}")
                continue

        # Rank and diversify recommendations
        ranked_recommendations = self._rank_strategies(recommendations)
        diversified_recommendations = self._apply_diversification(ranked_recommendations, max_strategies)

        return diversified_recommendations

    def _calculate_market_fit(self, strategy: BaseStrategy, market_regime: Dict,
                            sentiment: float) -> float:
        """
        Calculate how well strategy fits current market conditions

        This is the core strategy selection logic - determines which strategies
        are most suitable for current market environment
        """
        base_fit = 50.0
        regime = market_regime.get('regime', 'UNKNOWN')
        category = strategy.metadata.category

        # Core strategy-regime matching matrix
        fit_bonuses = {
            # Momentum strategies
            ('MOMENTUM', 'BULLISH_TRENDING'): 30,
            ('MOMENTUM', 'BEARISH_TRENDING'): 30,
            ('MOMENTUM', 'MIXED_SIGNALS'): -10,

            # Mean reversion strategies
            ('MEAN_REVERSION', 'RANGE_BOUND_STABLE'): 35,
            ('MEAN_REVERSION', 'HIGH_VOLATILITY_UNSTABLE'): 25,
            ('MEAN_REVERSION', 'BULLISH_TRENDING'): -15,
            ('MEAN_REVERSION', 'BEARISH_TRENDING'): -15,

            # Volatility strategies
            ('VOLATILITY', 'HIGH_VOLATILITY_UNSTABLE'): 40,
            ('VOLATILITY', 'RANGE_BOUND_STABLE'): -20,

            # Factor strategies
            ('FACTOR', 'BULLISH_TRENDING'): 20,
            ('FACTOR', 'RANGE_BOUND_STABLE'): 15,
            ('FACTOR', 'HIGH_VOLATILITY_UNSTABLE'): 10,

            # Pairs strategies
            ('PAIRS', 'RANGE_BOUND_STABLE'): 25,
            ('PAIRS', 'HIGH_VOLATILITY_UNSTABLE'): 15,
        }

        base_fit += fit_bonuses.get((category, regime), 0)

        # RSI condition adjustments
        rsi_condition = market_regime.get('rsi_condition', 'NEUTRAL')
        if category == 'MEAN_REVERSION':
            if rsi_condition == 'OVERBOUGHT':
                base_fit += 20  # Mean reversion loves overbought conditions
            elif rsi_condition == 'OVERSOLD':
                base_fit += 20  # Mean reversion loves oversold conditions
            elif rsi_condition == 'NEUTRAL':
                base_fit -= 10  # Mean reversion less effective in neutral RSI

        # Volatility regime adjustments
        vol_regime = market_regime.get('volatility_regime', 'NORMAL_VOLATILITY')
        if category == 'VOLATILITY' and vol_regime == 'HIGH_VOLATILITY':
            base_fit += 15
        elif category == 'MOMENTUM' and vol_regime == 'LOW_VOLATILITY':
            base_fit += 10  # Momentum works well in stable low vol

        # Trend strength adjustments
        trend_strength = market_regime.get('trend_strength', 0.0)
        if category == 'MOMENTUM' and trend_strength > 0.05:
            base_fit += min(20, trend_strength * 400)  # Strong trends favor momentum
        elif category == 'MEAN_REVERSION' and trend_strength < 0.02:
            base_fit += 15  # Weak trends favor mean reversion

        # Sentiment alignment bonuses
        if abs(sentiment) > 0.3:  # Strong sentiment
            if category == 'MOMENTUM':
                base_fit += 15  # Momentum benefits from strong sentiment
            elif category == 'MEAN_REVERSION':
                base_fit += 20  # Mean reversion benefits from extreme sentiment
        elif abs(sentiment) < 0.1:  # Neutral sentiment
            if category == 'FACTOR':
                base_fit += 10  # Factor strategies work well in neutral sentiment

        return max(0, min(100, base_fit))

    def _calculate_selection_confidence(self, strategy: BaseStrategy, signal: StrategySignal,
                                      market_fit: float, strategy_name: str) -> float:
        """
        Calculate confidence in selecting this strategy

        Different from signal confidence - this is about strategy selection confidence
        """
        # Base confidence from signal quality
        base_confidence = signal.confidence * 0.7  # Scale down signal confidence

        # Market fit adjustment (major factor in selection)
        fit_adjustment = (market_fit - 50) * 0.4  # Market fit heavily influences selection

        # Historical performance bonus/penalty
        performance_adjustment = self._get_performance_adjustment(strategy_name)

        # Strategy stability bonus (some strategies are more reliable)
        stability_bonus = self._get_strategy_stability_bonus(strategy)

        total_confidence = base_confidence + fit_adjustment + performance_adjustment + stability_bonus

        return max(0, min(100, total_confidence))

    def _rank_strategies(self, recommendations: List[StrategyRecommendation]) -> List[StrategyRecommendation]:
        """Rank strategies by selection confidence"""
        return sorted(recommendations, key=lambda x: x.selection_confidence, reverse=True)

    def _apply_diversification(self, recommendations: List[StrategyRecommendation],
                             max_strategies: int) -> List[StrategyRecommendation]:
        """
        Apply diversification rules to avoid too many similar strategies

        Ensures we don't select multiple strategies from the same category
        unless they significantly outperform alternatives
        """
        if len(recommendations) <= max_strategies:
            return recommendations

        diversified = []
        used_categories = set()
        category_usage = {}

        # Track how many strategies we've used per category
        for category in STRATEGY_CATEGORIES.keys():
            category_usage[category] = 0

        # First pass: one strategy per category (highest confidence)
        for rec in recommendations:
            category = rec.strategy_metadata.category

            if category not in used_categories:
                diversified.append(rec)
                used_categories.add(category)
                category_usage[category] += 1

                if len(diversified) >= max_strategies:
                    break

        # Second pass: fill remaining slots with best remaining strategies
        # but apply diversification penalty for repeat categories
        if len(diversified) < max_strategies:
            remaining = [r for r in recommendations if r not in diversified]

            for rec in remaining:
                if len(diversified) >= max_strategies:
                    break

                category = rec.strategy_metadata.category

                # Apply diversification penalty
                penalty = category_usage[category] * self.config['diversification_penalty'] * 10
                adjusted_confidence = rec.selection_confidence - penalty

                # Only add if still meets threshold after penalty
                if adjusted_confidence >= self.config['min_confidence_threshold']:
                    # Update the confidence to reflect diversification penalty
                    rec.selection_confidence = adjusted_confidence
                    diversified.append(rec)
                    category_usage[category] += 1

        return diversified

    def _get_performance_adjustment(self, strategy_name: str) -> float:
        """Get performance-based adjustment for strategy selection"""
        if strategy_name not in self.strategy_performance:
            return 0.0

        perf = self.strategy_performance[strategy_name]

        # Win rate adjustment
        win_rate_adj = (perf['win_rate'] - 0.5) * 20  # ±10 points for win rate

        # Confidence history adjustment
        if perf['confidence_history']:
            avg_confidence = np.mean(perf['confidence_history'])
            confidence_adj = (avg_confidence - 60) * 0.1  # Small adjustment for confidence
        else:
            confidence_adj = 0

        total_adjustment = (win_rate_adj + confidence_adj) * self.config['performance_weight']
        return max(-15, min(15, total_adjustment))  # Cap at ±15 points

    def _get_strategy_stability_bonus(self, strategy: BaseStrategy) -> float:
        """Get stability bonus based on strategy characteristics"""
        category = strategy.metadata.category

        # Some strategy types are inherently more stable
        stability_bonuses = {
            'MOMENTUM': 5,      # Generally reliable in trending markets
            'MEAN_REVERSION': 8,  # Very reliable in ranging markets
            'FACTOR': 6,        # Stable long-term
            'VOLATILITY': 3,    # More experimental
            'PAIRS': 4,         # Dependent on correlations
        }

        return stability_bonuses.get(category, 0)

    def _update_performance_tracking(self, strategy_name: str, signal: StrategySignal):
        """Update strategy performance tracking for future selections"""
        if strategy_name in self.strategy_performance:
            perf = self.strategy_performance[strategy_name]
            perf['total_signals'] += 1
            perf['confidence_history'].append(signal.confidence)
            perf['last_used'] = pd.Timestamp.now()

            # Keep rolling window of last 50 signals
            if len(perf['confidence_history']) > 50:
                perf['confidence_history'] = perf['confidence_history'][-50:]

    def get_strategy_fitness_report(self, market_regime: Dict, sentiment: float) -> Dict:
        """
        Generate a report of all strategies and their market fitness
        Useful for debugging and analysis
        """
        fitness_report = {}

        for strategy_name, strategy in self.strategies.items():
            if strategy.metadata.requires_pairs:
                continue

            market_fit = self._calculate_market_fit(strategy, market_regime, sentiment)
            performance_adj = self._get_performance_adjustment(strategy_name)
            stability_bonus = self._get_strategy_stability_bonus(strategy)

            fitness_report[strategy_name] = {
                'category': strategy.metadata.category,
                'market_fit_score': market_fit,
                'performance_adjustment': performance_adj,
                'stability_bonus': stability_bonus,
                'total_fitness': market_fit + performance_adj + stability_bonus,
                'metadata': {
                    'typical_holding_days': strategy.metadata.typical_holding_days,
                    'works_best_in': strategy.metadata.works_best_in,
                    'min_confidence': strategy.metadata.min_confidence
                }
            }

        return fitness_report

    def get_available_strategies(self) -> List[str]:
        """Get list of all available strategy names"""
        return list(self.strategies.keys())

    def get_strategies_by_category(self, category: str) -> List[str]:
        """Get strategies filtered by category"""
        return [
            name for name, strategy in self.strategies.items()
            if strategy.metadata.category == category.upper()
        ]
