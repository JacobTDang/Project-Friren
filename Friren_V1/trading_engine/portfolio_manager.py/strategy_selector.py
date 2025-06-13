"""
Strategy Selection and Orchestration

This module belongs in the portfolio manager layer, not in strategies.
It orchestrates strategy selection, market regime analysis, and signal aggregation.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

# Import from strategies package (pure strategies only)
from .strategies import (
    BaseStrategy, StrategySignal, StrategyMetadata,
    discover_all_strategies, get_strategies_by_category,
    STRATEGY_CATEGORIES
)
from trading_engine.data.data_utils import StockDataTools

@dataclass
class StrategyRecommendation:
    """Recommendation from strategy selector"""
    strategy_name: str
    signal: StrategySignal
    strategy_metadata: StrategyMetadata
    market_fit_score: float  # 0-100 how well strategy fits current market
    risk_score: float  # 0-100 risk assessment
    recommendation_strength: float  # 0-100 overall recommendation strength

class PortfolioStrategySelector:
    """
    MOVED TO PORTFOLIO MANAGER LAYER

    Strategy selector that orchestrates multiple trading strategies.
    This belongs in portfolio management, not in the strategies package.

    Responsibilities:
    - Market regime analysis
    - Strategy selection and ranking
    - Signal aggregation and filtering
    - Risk assessment and position sizing recommendations
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
        self.data_tools = StockDataTools()

        # Load all available strategies
        self.strategies = discover_all_strategies()
        self.strategy_performance = self._initialize_performance_tracking()

        print(f"Portfolio Strategy Selector initialized with {len(self.strategies)} strategies")

    def _default_config(self) -> Dict:
        """Default configuration for strategy selector"""
        return {
            'max_strategies_per_signal': 3,
            'min_confidence_threshold': 60.0,
            'diversification_penalty': 0.1,  # Penalty for similar strategies
            'sentiment_weight': 0.15,  # How much sentiment affects selection
            'market_regime_weight': 0.25,  # How much regime affects selection
            'risk_adjustment_factor': 0.2,  # Risk penalty factor
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

    def analyze_market_and_recommend(self, symbol: str, df: pd.DataFrame,
                                   sentiment: float = 0.0) -> Dict:
        """
        MAIN ORCHESTRATION METHOD

        Comprehensive market analysis and strategy recommendations
        """
        # 1. Market Regime Analysis
        market_regime = self._analyze_market_regime(df, sentiment)

        # 2. Strategy Selection
        recommendations = self.get_best_strategies(
            df, sentiment,
            max_strategies=self.config['max_strategies_per_signal'],
            confidence_threshold=self.config['min_confidence_threshold']
        )

        # 3. Risk Assessment
        risk_assessment = self._assess_portfolio_risk(market_regime, recommendations)

        # 4. Position Sizing Recommendations
        position_sizing = self._recommend_position_sizing(recommendations, risk_assessment)

        # 5. Execution Recommendations
        execution_plan = self._create_execution_plan(recommendations, market_regime)

        return {
            'symbol': symbol,
            'timestamp': pd.Timestamp.now(),
            'market_regime': market_regime,
            'strategy_recommendations': [self._format_recommendation(rec) for rec in recommendations],
            'risk_assessment': risk_assessment,
            'position_sizing': position_sizing,
            'execution_plan': execution_plan,
            'summary': {
                'action_consensus': self._get_consensus_action(recommendations),
                'confidence_range': self._get_confidence_range(recommendations),
                'risk_level': risk_assessment.get('overall_risk_level', 'MEDIUM'),
                'recommended_allocation': position_sizing.get('total_allocation', 0.0)
            }
        }

    def get_best_strategies(self, df: pd.DataFrame, sentiment: float = 0.0,
                          max_strategies: int = 3, confidence_threshold: float = 60.0) -> List[StrategyRecommendation]:
        """
        Select best strategies for current market conditions
        """
        # Ensure we have required indicators
        df_enhanced = self._ensure_indicators(df)

        # Analyze market regime first
        market_regime = self._analyze_market_regime(df_enhanced, sentiment)

        recommendations = []

        # Test each strategy
        for strategy_name, strategy in self.strategies.items():
            try:
                # Skip pairs strategies for single symbol analysis
                if strategy.metadata.requires_pairs:
                    continue

                # Generate signal
                signal = strategy.generate_signal(df_enhanced, sentiment, confidence_threshold)

                if signal.action == 'HOLD':
                    continue

                # Calculate market fit
                market_fit_score = self._calculate_market_fit(strategy, market_regime)

                # Calculate risk score
                risk_score = self._calculate_strategy_risk(strategy, signal, market_regime)

                # Calculate recommendation strength
                rec_strength = self._calculate_recommendation_strength(
                    signal, market_fit_score, risk_score, strategy_name
                )

                if rec_strength >= confidence_threshold:
                    recommendations.append(StrategyRecommendation(
                        strategy_name=strategy_name,
                        signal=signal,
                        strategy_metadata=strategy.metadata,
                        market_fit_score=market_fit_score,
                        risk_score=risk_score,
                        recommendation_strength=rec_strength
                    ))

                    # Update performance tracking
                    self._update_performance_tracking(strategy_name, signal)

            except Exception as e:
                print(f"Error testing strategy {strategy_name}: {e}")
                continue

        # Sort and diversify
        recommendations.sort(key=lambda x: x.recommendation_strength, reverse=True)
        diversified_recs = self._apply_diversification(recommendations, max_strategies)

        return diversified_recs

    def _analyze_market_regime(self, df: pd.DataFrame, sentiment: float) -> Dict:
        """Analyze current market regime - MOVED FROM STRATEGIES"""

        if len(df) < 50:
            return {'regime': 'INSUFFICIENT_DATA', 'confidence': 0.0}

        regime_indicators = {}

        # Trend Analysis
        if all(col in df.columns for col in ['SMA_20', 'SMA_50']):
            current_price = df['Close'].iloc[-1]
            sma_20 = df['SMA_20'].iloc[-1]
            sma_50 = df['SMA_50'].iloc[-1]

            if current_price > sma_20 > sma_50:
                trend = 'UPTREND'
                trend_strength = (current_price - sma_50) / sma_50
            elif current_price < sma_20 < sma_50:
                trend = 'DOWNTREND'
                trend_strength = (sma_50 - current_price) / sma_50
            else:
                trend = 'SIDEWAYS'
                trend_strength = abs(sma_20 - sma_50) / sma_50
        else:
            trend = 'UNKNOWN'
            trend_strength = 0.0

        regime_indicators['trend'] = trend
        regime_indicators['trend_strength'] = trend_strength

        # Volatility Analysis
        returns = df['Close'].pct_change()
        current_vol = returns.rolling(20).std().iloc[-1] * np.sqrt(252)
        historical_vol = returns.rolling(60).std().mean() * np.sqrt(252)

        if current_vol > historical_vol * 1.3:
            vol_regime = 'HIGH_VOLATILITY'
        elif current_vol < historical_vol * 0.7:
            vol_regime = 'LOW_VOLATILITY'
        else:
            vol_regime = 'NORMAL_VOLATILITY'

        regime_indicators['volatility_regime'] = vol_regime
        regime_indicators['current_volatility'] = current_vol

        # RSI Analysis
        if 'RSI' in df.columns:
            current_rsi = df['RSI'].iloc[-1]
            if current_rsi > 70:
                rsi_condition = 'OVERBOUGHT'
            elif current_rsi < 30:
                rsi_condition = 'OVERSOLD'
            else:
                rsi_condition = 'NEUTRAL'
        else:
            rsi_condition = 'UNKNOWN'
            current_rsi = 50

        regime_indicators['rsi_condition'] = rsi_condition
        regime_indicators['current_rsi'] = current_rsi

        # Overall Regime Classification
        if trend == 'UPTREND' and vol_regime != 'HIGH_VOLATILITY':
            overall_regime = 'BULLISH_TRENDING'
        elif trend == 'DOWNTREND' and vol_regime != 'HIGH_VOLATILITY':
            overall_regime = 'BEARISH_TRENDING'
        elif vol_regime == 'HIGH_VOLATILITY':
            overall_regime = 'HIGH_VOLATILITY_UNSTABLE'
        elif trend == 'SIDEWAYS' and vol_regime == 'LOW_VOLATILITY':
            overall_regime = 'RANGE_BOUND_STABLE'
        else:
            overall_regime = 'MIXED_SIGNALS'

        regime_indicators['regime'] = overall_regime
        regime_indicators['sentiment'] = sentiment
        regime_indicators['regime_confidence'] = min(100, abs(trend_strength) * 100 + 50)

        return regime_indicators

    def _calculate_market_fit(self, strategy: BaseStrategy, market_regime: Dict) -> float:
        """Calculate how well strategy fits current market - MOVED FROM STRATEGIES"""

        base_fit = 50.0
        regime = market_regime.get('regime', 'UNKNOWN')
        category = strategy.metadata.category

        # Strategy-regime matching
        fit_bonuses = {
            ('MOMENTUM', 'BULLISH_TRENDING'): 30,
            ('MOMENTUM', 'BEARISH_TRENDING'): 30,
            ('MEAN_REVERSION', 'RANGE_BOUND_STABLE'): 30,
            ('MEAN_REVERSION', 'HIGH_VOLATILITY_UNSTABLE'): 20,
            ('VOLATILITY', 'HIGH_VOLATILITY_UNSTABLE'): 35,
            ('FACTOR', 'BULLISH_TRENDING'): 20,
            ('FACTOR', 'RANGE_BOUND_STABLE'): 15,
        }

        base_fit += fit_bonuses.get((category, regime), 0)

        # RSI condition adjustments
        rsi_condition = market_regime.get('rsi_condition', 'NEUTRAL')
        if category == 'MEAN_REVERSION' and rsi_condition in ['OVERBOUGHT', 'OVERSOLD']:
            base_fit += 15

        # Sentiment alignment
        sentiment = market_regime.get('sentiment', 0.0)
        if category == 'MOMENTUM' and abs(sentiment) > 0.3:
            base_fit += 10
        elif category == 'MEAN_REVERSION' and abs(sentiment) > 0.5:
            base_fit += 15

        return max(0, min(100, base_fit))

    def _ensure_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has all required indicators"""
        return self.data_tools.add_all_indicators(df)

    def _calculate_strategy_risk(self, strategy: BaseStrategy, signal: StrategySignal,
                               market_regime: Dict) -> float:
        """Calculate risk score for strategy"""
        base_risk = 30.0

        # High volatility increases risk
        vol_regime = market_regime.get('volatility_regime', 'NORMAL_VOLATILITY')
        if vol_regime == 'HIGH_VOLATILITY':
            base_risk += 25
        elif vol_regime == 'LOW_VOLATILITY':
            base_risk -= 10

        # Strategy-specific risks
        category_risks = {
            'VOLATILITY': 15,
            'PAIRS': 10,
            'FACTOR': 5,
            'MOMENTUM': 8,
            'MEAN_REVERSION': 5
        }

        base_risk += category_risks.get(strategy.metadata.category, 0)

        # Confidence-based risk adjustment
        confidence_risk = (100 - signal.confidence) * 0.2
        base_risk += confidence_risk

        return max(0, min(100, base_risk))

    def _calculate_recommendation_strength(self, signal: StrategySignal,
                                         market_fit: float, risk_score: float,
                                         strategy_name: str) -> float:
        """Calculate overall recommendation strength"""

        # Base from signal confidence
        base_strength = signal.confidence

        # Market fit adjustment
        fit_adj = (market_fit - 50) * self.config['market_regime_weight']

        # Risk penalty
        risk_penalty = (risk_score - 30) * self.config['risk_adjustment_factor']

        # Historical performance
        performance_bonus = self._get_performance_bonus(strategy_name)

        total_strength = base_strength + fit_adj - risk_penalty + performance_bonus

        return max(0, min(100, total_strength))

    def _apply_diversification(self, recommendations: List[StrategyRecommendation],
                             max_strategies: int) -> List[StrategyRecommendation]:
        """Apply diversification to avoid too many similar strategies"""

        if len(recommendations) <= max_strategies:
            return recommendations

        diversified = []
        used_categories = set()

        # First pass: one per category
        for rec in recommendations:
            category = rec.strategy_metadata.category
            if category not in used_categories:
                diversified.append(rec)
                used_categories.add(category)
                if len(diversified) >= max_strategies:
                    break

        # Second pass: fill remaining with highest strength
        if len(diversified) < max_strategies:
            remaining = [r for r in recommendations if r not in diversified]
            diversified.extend(remaining[:max_strategies - len(diversified)])

        return diversified

    def _assess_portfolio_risk(self, market_regime: Dict,
                             recommendations: List[StrategyRecommendation]) -> Dict:
        """Assess overall portfolio risk"""

        if not recommendations:
            return {
                'overall_risk_level': 'HIGH',
                'risk_score': 80,
                'risk_factors': ['No clear trading signals']
            }

        # Calculate weighted risk
        total_weight = sum(r.recommendation_strength for r in recommendations)
        weighted_risk = sum(r.risk_score * r.recommendation_strength for r in recommendations) / total_weight

        # Market regime risk
        regime_risk = {
            'HIGH_VOLATILITY_UNSTABLE': 25,
            'BEARISH_TRENDING': 15,
            'MIXED_SIGNALS': 20
        }.get(market_regime.get('regime', ''), 0)

        overall_risk = weighted_risk + regime_risk

        # Risk level classification
        if overall_risk < 40:
            risk_level = 'LOW'
        elif overall_risk < 65:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'HIGH'

        return {
            'overall_risk_level': risk_level,
            'risk_score': min(100, overall_risk),
            'strategy_risk': weighted_risk,
            'market_risk': regime_risk,
            'risk_factors': self._identify_risk_factors(market_regime, recommendations)
        }

    def _recommend_position_sizing(self, recommendations: List[StrategyRecommendation],
                                 risk_assessment: Dict) -> Dict:
        """Recommend position sizing based on signals and risk"""

        if not recommendations:
            return {'total_allocation': 0.0, 'individual_allocations': {}}

        # Base allocation percentage
        risk_level = risk_assessment.get('overall_risk_level', 'MEDIUM')
        base_allocations = {
            'LOW': 0.15,     # 15% max in low risk
            'MEDIUM': 0.10,  # 10% max in medium risk
            'HIGH': 0.05     # 5% max in high risk
        }

        max_allocation = base_allocations.get(risk_level, 0.10)

        # Distribute allocation based on recommendation strength
        total_strength = sum(r.recommendation_strength for r in recommendations)

        allocations = {}
        total_allocation = 0.0

        for rec in recommendations:
            weight = rec.recommendation_strength / total_strength
            allocation = max_allocation * weight
            allocations[rec.strategy_name] = allocation
            total_allocation += allocation

        return {
            'total_allocation': total_allocation,
            'individual_allocations': allocations,
            'max_single_allocation': max(allocations.values()) if allocations else 0.0,
            'risk_adjusted': True
        }

    def _create_execution_plan(self, recommendations: List[StrategyRecommendation],
                             market_regime: Dict) -> Dict:
        """Create execution plan for recommended trades"""

        if not recommendations:
            return {'action': 'WAIT', 'urgency': 'NONE'}

        # Determine consensus action
        actions = [r.signal.action for r in recommendations]
        action_counts = {action: actions.count(action) for action in set(actions)}
        consensus_action = max(action_counts, key=action_counts.get)

        # Determine urgency based on market conditions
        vol_regime = market_regime.get('volatility_regime', 'NORMAL_VOLATILITY')
        avg_confidence = np.mean([r.recommendation_strength for r in recommendations])

        if vol_regime == 'HIGH_VOLATILITY' and avg_confidence > 80:
            urgency = 'HIGH'
        elif avg_confidence > 75:
            urgency = 'MEDIUM'
        else:
            urgency = 'LOW'

        return {
            'action': consensus_action,
            'urgency': urgency,
            'execution_style': 'GRADUAL' if urgency == 'LOW' else 'IMMEDIATE',
            'market_timing': 'Consider market hours and volatility',
            'strategies_count': len(recommendations)
        }

    # Helper methods for formatting and utilities
    def _format_recommendation(self, rec: StrategyRecommendation) -> Dict:
        """Format recommendation for output"""
        return {
            'strategy': rec.strategy_name,
            'action': rec.signal.action,
            'confidence': rec.signal.confidence,
            'reasoning': rec.signal.reasoning,
            'market_fit': rec.market_fit_score,
            'risk_score': rec.risk_score,
            'recommendation_strength': rec.recommendation_strength,
            'category': rec.strategy_metadata.category,
            'metadata': rec.signal.metadata
        }

    def _get_consensus_action(self, recommendations: List[StrategyRecommendation]) -> str:
        """Get consensus action from recommendations"""
        if not recommendations:
            return 'HOLD'

        actions = [r.signal.action for r in recommendations]
        action_counts = {action: actions.count(action) for action in set(actions)}
        return max(action_counts, key=action_counts.get)

    def _get_confidence_range(self, recommendations: List[StrategyRecommendation]) -> str:
        """Get confidence range from recommendations"""
        if not recommendations:
            return "0-0%"

        confidences = [r.recommendation_strength for r in recommendations]
        return f"{min(confidences):.0f}-{max(confidences):.0f}%"

    def _update_performance_tracking(self, strategy_name: str, signal: StrategySignal):
        """Update strategy performance tracking"""
        if strategy_name in self.strategy_performance:
            perf = self.strategy_performance[strategy_name]
            perf['total_signals'] += 1
            perf['confidence_history'].append(signal.confidence)
            perf['last_used'] = pd.Timestamp.now()

            # Keep rolling window
            if len(perf['confidence_history']) > 50:
                perf['confidence_history'] = perf['confidence_history'][-50:]

    def _get_performance_bonus(self, strategy_name: str) -> float:
        """Get performance bonus for strategy"""
        if strategy_name not in self.strategy_performance:
            return 0.0

        perf = self.strategy_performance[strategy_name]
        if perf['confidence_history']:
            avg_confidence = np.mean(perf['confidence_history'])
            return (avg_confidence - 60) * 0.05  # Small bonus/penalty
        return 0.0

    def _identify_risk_factors(self, market_regime: Dict,
                             recommendations: List[StrategyRecommendation]) -> List[str]:
        """Identify specific risk factors"""
        factors = []

        regime = market_regime.get('regime', '')
        if 'HIGH_VOLATILITY' in regime:
            factors.append("High market volatility increases execution risk")

        sentiment = market_regime.get('sentiment', 0.0)
        if abs(sentiment) > 0.6:
            factors.append("Extreme sentiment may lead to sudden reversals")

        if len(recommendations) == 0:
            factors.append("No clear trading signals - market indecision")

        # Check for conflicting signals
        actions = [r.signal.action for r in recommendations]
        if 'BUY' in actions and 'SELL' in actions:
            factors.append("Conflicting signals from different strategies")

        return factors

# Convenience function for easy import
def create_portfolio_strategy_selector(config: Optional[Dict] = None) -> PortfolioStrategySelector:
    """Create a portfolio strategy selector instance"""
    return PortfolioStrategySelector(config)
