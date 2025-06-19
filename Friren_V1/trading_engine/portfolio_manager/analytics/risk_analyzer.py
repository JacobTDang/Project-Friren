"""
Risk Analyzer - Comprehensive Portfolio Risk Assessment

This component provides multi-layered risk analysis for portfolio management:
- Position-level risk assessment
- Portfolio-level risk aggregation
- Market regime risk adjustments
- Sentiment-based risk factors
- Real-time risk monitoring

Integrates with strategy selections to provide risk-adjusted recommendations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Import components for integrated risk analysis
from .market_analyzer import MarketRegimeResult
from .sentiment_analyzer import SentimentReading, MarketSentimentSummary
from .strategy_selector import StrategyRecommendation

@dataclass
class PositionRisk:
    """Risk metrics for individual position"""
    symbol: str
    strategy_name: str

    # Basic risk metrics
    volatility: float  # Annualized volatility
    beta: float  # Market beta
    var_1d: float  # 1-day Value at Risk (95% confidence)
    var_5d: float  # 5-day Value at Risk
    max_drawdown: float  # Historical maximum drawdown

    # Strategy-specific risks
    strategy_risk_score: float  # 0-100 based on strategy type
    confidence_risk: float  # Risk from low signal confidence
    regime_risk: float  # Risk from market regime mismatch
    sentiment_risk: float  # Risk from sentiment extremes

    # Liquidity and execution risks
    liquidity_risk: float  # Based on volume and spread
    execution_risk: float  # Based on volatility and market conditions

    # Overall risk assessment
    total_risk_score: float  # 0-100 composite risk score
    risk_category: str  # LOW, MEDIUM, HIGH, EXTREME

    # Risk factors and warnings
    risk_factors: List[str]
    risk_warnings: List[str]

    timestamp: datetime

@dataclass
class PortfolioRisk:
    """Portfolio-level risk assessment"""

    # Portfolio risk metrics
    portfolio_volatility: float  # Portfolio-level volatility
    portfolio_var: float  # Portfolio Value at Risk
    correlation_risk: float  # Risk from position correlations
    concentration_risk: float  # Risk from position concentration

    # Market and regime risks
    market_regime_risk: float  # Risk from current market regime
    sentiment_risk: float  # Risk from sentiment extremes
    macro_risk: float  # Macroeconomic risk factors

    # Strategy and diversification risks
    strategy_concentration: float  # Risk from strategy concentration
    sector_concentration: float  # Risk from sector concentration
    diversification_score: float  # 0-100 diversification effectiveness

    # Dynamic risk factors
    regime_transition_risk: float  # Risk from regime changes
    volatility_regime_risk: float  # Risk from volatility regime
    liquidity_risk: float  # Overall portfolio liquidity risk

    # Risk capacity and limits
    risk_budget_utilization: float  # % of risk budget used
    margin_of_safety: float  # Safety margin before risk limits

    # Overall assessment
    total_portfolio_risk: float  # 0-100 composite portfolio risk
    risk_level: str  # LOW, MEDIUM, HIGH, CRITICAL

    # Recommendations and alerts
    risk_recommendations: List[str]
    risk_alerts: List[str]
    position_adjustments: Dict[str, str]  # symbol -> recommendation

    timestamp: datetime

class RiskAnalyzer:
    """
    Comprehensive Risk Analysis Component

    Provides multi-layered risk assessment for portfolio management:
    - Individual position risk analysis
    - Portfolio-level risk aggregation
    - Market regime risk adjustments
    - Real-time risk monitoring
    - Risk-based position sizing recommendations
    - Professional regime compatibility matrix
    - Risk-adjusted position sizing engine
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()

        # Risk calculation parameters
        self.risk_free_rate = self.config['risk_free_rate']
        self.confidence_levels = self.config['confidence_levels']

        # Professional regime compatibility matrix
        self.regime_compatibility_matrix = self._build_regime_compatibility_matrix()

        # Risk history for tracking
        self.position_risk_history = {}  # symbol -> list of PositionRisk
        self.portfolio_risk_history = []  # list of PortfolioRisk

        print("Risk Analyzer initialized with professional regime compatibility and position sizing")

    def _default_config(self) -> Dict:
        """Default configuration for risk analysis"""
        return {
            # Risk calculation parameters
            'risk_free_rate': 0.02,  # 2% risk-free rate
            'confidence_levels': [0.95, 0.99],  # 95% and 99% confidence
            'lookback_volatility': 252,  # 1 year for volatility calculation
            'lookback_correlation': 126,  # 6 months for correlation

            # Risk thresholds
            'low_risk_threshold': 30,
            'medium_risk_threshold': 60,
            'high_risk_threshold': 85,

            # Position risk limits
            'max_position_volatility': 0.50,  # 50% max volatility
            'max_position_var': 0.05,  # 5% max daily VaR
            'max_beta': 2.0,  # Maximum beta exposure

            # Portfolio risk limits
            'max_portfolio_volatility': 0.25,  # 25% max portfolio volatility
            'max_portfolio_var': 0.03,  # 3% max portfolio VaR
            'max_concentration': 0.20,  # 20% max single position
            'max_correlation': 0.8,  # 80% max correlation threshold

            # Risk scoring weights
            'volatility_weight': 0.25,
            'var_weight': 0.20,
            'beta_weight': 0.15,
            'strategy_weight': 0.15,
            'regime_weight': 0.15,
            'sentiment_weight': 0.10,

            # Market data requirements
            'min_data_points': 60,  # Minimum data for risk calculation
            'volume_percentile_threshold': 0.1,  # 10th percentile for liquidity risk

            # Position sizing parameters
            'base_position_size': 0.10,  # 10% base allocation
            'max_position_size': 0.20,   # 20% maximum single position
            'min_position_size': 0.02,   # 2% minimum position
            'risk_adjustment_factor': 0.5,  # How much risk affects sizing

            # Regime compatibility weights
            'compatibility_weight': 0.20,  # How much regime compatibility affects risk
        }

    def _build_regime_compatibility_matrix(self) -> Dict[Tuple[str, str], float]:
        """
        Build professional regime compatibility matrix

        Returns compatibility scores for (strategy_category, market_regime) pairs:
        - Positive scores = penalty (bad fit)
        - Negative scores = bonus (good fit)
        - Zero = neutral fit
        """
        matrix = {}

        # MOMENTUM strategy compatibility
        matrix[('MOMENTUM', 'BULLISH_TRENDING')] = -15  # Excellent fit
        matrix[('MOMENTUM', 'BEARISH_TRENDING')] = -10  # Good fit
        matrix[('MOMENTUM', 'RANGE_BOUND_STABLE')] = 20  # Poor fit
        matrix[('MOMENTUM', 'HIGH_VOLATILITY_UNSTABLE')] = 15  # Poor fit
        matrix[('MOMENTUM', 'MIXED_SIGNALS')] = 25  # Very poor fit

        # Volatility regime compatibility for MOMENTUM
        matrix[('MOMENTUM', 'HIGH_VOLATILITY')] = 10   # Penalty for high vol
        matrix[('MOMENTUM', 'NORMAL_VOLATILITY')] = -5  # Slight bonus
        matrix[('MOMENTUM', 'LOW_VOLATILITY')] = 0      # Neutral

        # RSI condition compatibility for MOMENTUM
        matrix[('MOMENTUM', 'OVERBOUGHT')] = 5   # Slight penalty
        matrix[('MOMENTUM', 'OVERSOLD')] = -5    # Slight bonus
        matrix[('MOMENTUM', 'NEUTRAL')] = -5     # Slight bonus

        # MEAN_REVERSION strategy compatibility
        matrix[('MEAN_REVERSION', 'BULLISH_TRENDING')] = 10   # Poor fit
        matrix[('MEAN_REVERSION', 'BEARISH_TRENDING')] = 10   # Poor fit
        matrix[('MEAN_REVERSION', 'RANGE_BOUND_STABLE')] = -20  # Excellent fit
        matrix[('MEAN_REVERSION', 'HIGH_VOLATILITY_UNSTABLE')] = -10  # Good fit
        matrix[('MEAN_REVERSION', 'MIXED_SIGNALS')] = -5      # Slight bonus

        # Volatility regime compatibility for MEAN_REVERSION
        matrix[('MEAN_REVERSION', 'HIGH_VOLATILITY')] = -10  # Good fit
        matrix[('MEAN_REVERSION', 'NORMAL_VOLATILITY')] = -5  # Slight bonus
        matrix[('MEAN_REVERSION', 'LOW_VOLATILITY')] = 5     # Slight penalty

        # RSI condition compatibility for MEAN_REVERSION
        matrix[('MEAN_REVERSION', 'OVERBOUGHT')] = -15   # Excellent fit
        matrix[('MEAN_REVERSION', 'OVERSOLD')] = -15     # Excellent fit
        matrix[('MEAN_REVERSION', 'NEUTRAL')] = 10       # Poor fit

        # VOLATILITY strategy compatibility
        matrix[('VOLATILITY', 'BULLISH_TRENDING')] = 5    # Slight penalty
        matrix[('VOLATILITY', 'BEARISH_TRENDING')] = 5    # Slight penalty
        matrix[('VOLATILITY', 'RANGE_BOUND_STABLE')] = 10  # Poor fit
        matrix[('VOLATILITY', 'HIGH_VOLATILITY_UNSTABLE')] = -25  # Excellent fit
        matrix[('VOLATILITY', 'MIXED_SIGNALS')] = -10     # Good fit

        # Volatility regime compatibility for VOLATILITY
        matrix[('VOLATILITY', 'HIGH_VOLATILITY')] = -20  # Excellent fit
        matrix[('VOLATILITY', 'NORMAL_VOLATILITY')] = 0  # Neutral
        matrix[('VOLATILITY', 'LOW_VOLATILITY')] = 15    # Poor fit

        # PAIRS strategy compatibility
        matrix[('PAIRS', 'BULLISH_TRENDING')] = 0        # Neutral
        matrix[('PAIRS', 'BEARISH_TRENDING')] = 0        # Neutral
        matrix[('PAIRS', 'RANGE_BOUND_STABLE')] = -10    # Good fit
        matrix[('PAIRS', 'HIGH_VOLATILITY_UNSTABLE')] = 15  # Poor fit
        matrix[('PAIRS', 'MIXED_SIGNALS')] = 5           # Slight penalty

        # Volatility regime compatibility for PAIRS
        matrix[('PAIRS', 'HIGH_VOLATILITY')] = 20        # Poor fit
        matrix[('PAIRS', 'NORMAL_VOLATILITY')] = -5      # Slight bonus
        matrix[('PAIRS', 'LOW_VOLATILITY')] = -10        # Good fit

        # FACTOR strategy compatibility
        matrix[('FACTOR', 'BULLISH_TRENDING')] = -10     # Good fit
        matrix[('FACTOR', 'BEARISH_TRENDING')] = -5      # Slight bonus
        matrix[('FACTOR', 'RANGE_BOUND_STABLE')] = -5    # Slight bonus
        matrix[('FACTOR', 'HIGH_VOLATILITY_UNSTABLE')] = 10  # Poor fit
        matrix[('FACTOR', 'MIXED_SIGNALS')] = 0          # Neutral

        # Volatility regime compatibility for FACTOR
        matrix[('FACTOR', 'HIGH_VOLATILITY')] = 5        # Slight penalty
        matrix[('FACTOR', 'NORMAL_VOLATILITY')] = -5     # Slight bonus
        matrix[('FACTOR', 'LOW_VOLATILITY')] = 0         # Neutral

        # DEFENSIVE strategy compatibility
        matrix[('DEFENSIVE', 'BULLISH_TRENDING')] = 5    # Slight penalty
        matrix[('DEFENSIVE', 'BEARISH_TRENDING')] = -15  # Excellent fit
        matrix[('DEFENSIVE', 'RANGE_BOUND_STABLE')] = -10  # Good fit
        matrix[('DEFENSIVE', 'HIGH_VOLATILITY_UNSTABLE')] = -20  # Excellent fit
        matrix[('DEFENSIVE', 'MIXED_SIGNALS')] = -15     # Excellent fit

        # Volatility regime compatibility for DEFENSIVE
        matrix[('DEFENSIVE', 'HIGH_VOLATILITY')] = -15   # Excellent fit
        matrix[('DEFENSIVE', 'NORMAL_VOLATILITY')] = -5  # Slight bonus
        matrix[('DEFENSIVE', 'LOW_VOLATILITY')] = 5      # Slight penalty

        return matrix

    def get_regime_compatibility_penalty(self, strategy_category: str, market_regime) -> float:
        """
        Calculate regime compatibility penalty/bonus for a strategy

        Args:
            strategy_category: Strategy category (MOMENTUM, MEAN_REVERSION, etc.)
            market_regime: MarketRegimeResult object

        Returns:
            Penalty/bonus score (positive = penalty, negative = bonus)
        """
        total_penalty = 0.0

        # Primary regime compatibility
        primary_regime = getattr(market_regime, 'primary_regime', 'MIXED_SIGNALS')
        primary_key = (strategy_category, primary_regime)
        if primary_key in self.regime_compatibility_matrix:
            total_penalty += self.regime_compatibility_matrix[primary_key] * 0.6  # 60% weight

        # Volatility regime compatibility
        vol_regime = getattr(market_regime, 'volatility_regime', 'NORMAL_VOLATILITY')
        vol_key = (strategy_category, vol_regime)
        if vol_key in self.regime_compatibility_matrix:
            total_penalty += self.regime_compatibility_matrix[vol_key] * 0.3  # 30% weight

        # RSI condition compatibility
        rsi_condition = getattr(market_regime, 'rsi_condition', 'NEUTRAL')
        rsi_key = (strategy_category, rsi_condition)
        if rsi_key in self.regime_compatibility_matrix:
            total_penalty += self.regime_compatibility_matrix[rsi_key] * 0.1  # 10% weight

        # Apply regime confidence weighting
        regime_confidence = getattr(market_regime, 'regime_confidence', 50) / 100.0
        total_penalty *= regime_confidence  # Reduce impact if regime uncertain

        return total_penalty

    def analyze_position_risk(self, symbol: str, df: pd.DataFrame,
                            strategy_rec: StrategyRecommendation,
                            market_regime: MarketRegimeResult,
                            sentiment: SentimentReading,
                            position_size: float = 0.1) -> PositionRisk:
        """
        MAIN POSITION RISK ANALYSIS METHOD

        Analyzes comprehensive risk for a single position including:
        - Market risk (volatility, beta, VaR)
        - Strategy-specific risk
        - Regime and sentiment risk
        - Liquidity and execution risk
        """

        if len(df) < self.config['min_data_points']:
            return self._insufficient_data_position_risk(symbol, strategy_rec.strategy_name)

        try:
            # Calculate basic market risk metrics
            market_risk_metrics = self._calculate_market_risk_metrics(df, position_size)

            # Calculate strategy-specific risk
            strategy_risk = self._calculate_strategy_risk(strategy_rec, market_regime)

            # Calculate regime risk
            regime_risk = self._calculate_regime_risk(strategy_rec, market_regime)

            # Calculate sentiment risk
            sentiment_risk = self._calculate_sentiment_risk(sentiment, strategy_rec)

            # Calculate liquidity and execution risk
            liquidity_risk = self._calculate_liquidity_risk(df)
            execution_risk = self._calculate_execution_risk(df, market_regime)

            # Calculate composite risk score
            total_risk_score = self._calculate_composite_risk_score(
                market_risk_metrics, strategy_risk, regime_risk,
                sentiment_risk, liquidity_risk, execution_risk
            )

            # Determine risk category
            risk_category = self._determine_risk_category(total_risk_score)

            # Generate risk factors and warnings
            risk_factors, risk_warnings = self._generate_risk_factors_and_warnings(
                market_risk_metrics, strategy_risk, regime_risk, sentiment_risk,
                liquidity_risk, execution_risk, total_risk_score
            )

            return PositionRisk(
                symbol=symbol,
                strategy_name=strategy_rec.strategy_name,
                volatility=market_risk_metrics['volatility'],
                beta=market_risk_metrics['beta'],
                var_1d=market_risk_metrics['var_1d'],
                var_5d=market_risk_metrics['var_5d'],
                max_drawdown=market_risk_metrics['max_drawdown'],
                strategy_risk_score=strategy_risk,
                confidence_risk=100 - strategy_rec.signal.confidence,
                regime_risk=regime_risk,
                sentiment_risk=sentiment_risk,
                liquidity_risk=liquidity_risk,
                execution_risk=execution_risk,
                total_risk_score=total_risk_score,
                risk_category=risk_category,
                risk_factors=risk_factors,
                risk_warnings=risk_warnings,
                timestamp=datetime.now()
            )

        except Exception as e:
            print(f"Error analyzing position risk for {symbol}: {e}")
            return self._error_fallback_position_risk(symbol, strategy_rec.strategy_name)

    def analyze_portfolio_risk(self, position_risks: List[PositionRisk],
                             portfolio_weights: Dict[str, float],
                             market_regime: MarketRegimeResult,
                             market_sentiment: MarketSentimentSummary,
                             correlation_matrix: Optional[pd.DataFrame] = None) -> PortfolioRisk:
        """
        MAIN PORTFOLIO RISK ANALYSIS METHOD

        Analyzes comprehensive portfolio-level risk including:
        - Portfolio volatility and VaR
        - Concentration and correlation risks
        - Regime and sentiment risks
        - Risk budget utilization
        """

        if not position_risks:
            return self._empty_portfolio_risk()

        try:
            # Calculate portfolio-level market risk
            portfolio_market_risk = self._calculate_portfolio_market_risk(
                position_risks, portfolio_weights, correlation_matrix
            )

            # Calculate concentration risks
            concentration_risks = self._calculate_concentration_risks(
                position_risks, portfolio_weights
            )

            # Calculate regime and sentiment risks
            regime_risk = self._calculate_portfolio_regime_risk(market_regime, position_risks)
            sentiment_risk = self._calculate_portfolio_sentiment_risk(market_sentiment, position_risks)

            # Calculate diversification effectiveness
            diversification_score = self._calculate_diversification_score(
                position_risks, portfolio_weights, correlation_matrix
            )

            # Calculate dynamic risk factors
            dynamic_risks = self._calculate_dynamic_risk_factors(market_regime, market_sentiment)

            # Calculate risk budget utilization
            risk_budget_metrics = self._calculate_risk_budget_utilization(
                portfolio_market_risk, concentration_risks
            )

            # Calculate total portfolio risk
            total_portfolio_risk = self._calculate_total_portfolio_risk(
                portfolio_market_risk, concentration_risks, regime_risk,
                sentiment_risk, dynamic_risks
            )

            # Determine risk level
            risk_level = self._determine_portfolio_risk_level(total_portfolio_risk)

            # Generate recommendations and alerts
            recommendations, alerts, adjustments = self._generate_portfolio_risk_recommendations(
                position_risks, portfolio_market_risk, concentration_risks,
                regime_risk, sentiment_risk, total_portfolio_risk
            )

            return PortfolioRisk(
                portfolio_volatility=portfolio_market_risk['volatility'],
                portfolio_var=portfolio_market_risk['var'],
                correlation_risk=portfolio_market_risk['correlation_risk'],
                concentration_risk=concentration_risks['overall_concentration'],
                market_regime_risk=regime_risk,
                sentiment_risk=sentiment_risk,
                macro_risk=dynamic_risks['macro_risk'],
                strategy_concentration=concentration_risks['strategy_concentration'],
                sector_concentration=concentration_risks['sector_concentration'],
                diversification_score=diversification_score,
                regime_transition_risk=dynamic_risks['transition_risk'],
                volatility_regime_risk=dynamic_risks['volatility_risk'],
                liquidity_risk=portfolio_market_risk['liquidity_risk'],
                risk_budget_utilization=risk_budget_metrics['utilization'],
                margin_of_safety=risk_budget_metrics['margin_of_safety'],
                total_portfolio_risk=total_portfolio_risk,
                risk_level=risk_level,
                risk_recommendations=recommendations,
                risk_alerts=alerts,
                position_adjustments=adjustments,
                timestamp=datetime.now()
            )

        except Exception as e:
            print(f"Error analyzing portfolio risk: {e}")
            return self._error_fallback_portfolio_risk()

    def _calculate_market_risk_metrics(self, df: pd.DataFrame, position_size: float) -> Dict:
        """Calculate basic market risk metrics for a position"""
        returns = df['Close'].pct_change().dropna()

        # Volatility (annualized)
        volatility = returns.std() * np.sqrt(252)

        # Beta calculation (simplified - relative to own volatility)
        # In production, you'd use market index data
        rolling_vol = returns.rolling(60).std()
        market_vol = rolling_vol.median()
        beta = volatility / market_vol if market_vol > 0 else 1.0
        beta = max(0.1, min(3.0, beta))  # Cap beta between 0.1 and 3.0

        # Value at Risk (parametric approach)
        confidence_95 = 1.645  # 95% confidence z-score
        confidence_99 = 2.326  # 99% confidence z-score

        daily_vol = volatility / np.sqrt(252)
        var_1d_95 = confidence_95 * daily_vol * position_size
        var_1d_99 = confidence_99 * daily_vol * position_size
        var_5d_95 = var_1d_95 * np.sqrt(5)

        # Maximum drawdown
        cumulative_returns = (1 + returns).cumprod()
        rolling_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - rolling_max) / rolling_max
        max_drawdown = abs(drawdown.min())

        return {
            'volatility': volatility,
            'beta': beta,
            'var_1d': var_1d_95,
            'var_5d': var_5d_95,
            'var_1d_99': var_1d_99,
            'max_drawdown': max_drawdown,
            'daily_volatility': daily_vol
        }

    def _calculate_strategy_risk(self, strategy_rec: StrategyRecommendation,
                               market_regime: MarketRegimeResult) -> float:
        """Calculate strategy-specific risk score"""
        base_risk = 30.0  # Base strategy risk

        # Strategy category risk adjustments
        category_risks = {
            'MOMENTUM': 15,      # Higher risk in choppy markets
            'MEAN_REVERSION': 10,  # Medium risk
            'VOLATILITY': 25,    # Higher risk due to complexity
            'PAIRS': 20,         # Higher risk due to correlation dependency
            'FACTOR': 12,        # Medium-low risk
            'DEFENSIVE': 5       # Low risk
        }

        category = strategy_rec.strategy_metadata.category
        base_risk += category_risks.get(category, 15)

        # Market fit risk adjustment
        market_fit = strategy_rec.market_fit_score
        if market_fit < 40:  # Poor market fit
            base_risk += 20
        elif market_fit > 80:  # Excellent market fit
            base_risk -= 10

        # Confidence risk adjustment
        confidence = strategy_rec.signal.confidence
        if confidence < 60:
            base_risk += 15
        elif confidence > 85:
            base_risk -= 10

        # Typical holding period risk (longer = more risk)
        holding_days = strategy_rec.strategy_metadata.typical_holding_days
        if holding_days > 20:
            base_risk += 10
        elif holding_days < 5:
            base_risk += 5  # Very short-term also adds risk

        return max(0, min(100, base_risk))

    def _calculate_regime_risk(self, strategy_rec: StrategyRecommendation,
                             market_regime: MarketRegimeResult) -> float:
        """Calculate risk from market regime mismatch using professional compatibility matrix"""
        base_regime_risk = 20.0

        # PROFESSIONAL REGIME COMPATIBILITY ASSESSMENT
        strategy_category = strategy_rec.strategy_metadata.category
        compatibility_penalty = self.get_regime_compatibility_penalty(strategy_category, market_regime)

        # Add compatibility penalty to base risk
        base_regime_risk += compatibility_penalty

        # High volatility regime adds risk
        if market_regime.volatility_regime == 'HIGH_VOLATILITY':
            base_regime_risk += 25
        elif market_regime.volatility_regime == 'LOW_VOLATILITY':
            base_regime_risk -= 5

        # Regime confidence affects risk
        regime_confidence = market_regime.regime_confidence
        if regime_confidence < 50:
            base_regime_risk += 20  # Uncertain regime = higher risk
        elif regime_confidence > 80:
            base_regime_risk -= 10

        # Regime transition risk
        transition_prob = market_regime.regime_transition_probability
        base_regime_risk += transition_prob * 30  # Up to 30 points for high transition risk

        # Market stress affects all strategies
        stress_level = market_regime.market_stress_level
        base_regime_risk += stress_level * 0.3  # Up to 30 points for max stress

        return max(0, min(100, base_regime_risk))

    def _calculate_sentiment_risk(self, sentiment: SentimentReading,
                                strategy_rec: StrategyRecommendation) -> float:
        """Calculate risk from sentiment extremes"""
        base_sentiment_risk = 15.0

        # Extreme sentiment adds risk
        abs_sentiment = abs(sentiment.sentiment_score)
        if abs_sentiment > 0.7:
            base_sentiment_risk += 25  # Very extreme sentiment
        elif abs_sentiment > 0.5:
            base_sentiment_risk += 15  # Moderately extreme sentiment

        # Low sentiment confidence adds risk
        if sentiment.confidence < 30:
            base_sentiment_risk += 20
        elif sentiment.confidence < 50:
            base_sentiment_risk += 10

        # Sentiment momentum risk (rapid changes)
        momentum = abs(sentiment.sentiment_momentum)
        if momentum > 0.5:
            base_sentiment_risk += 15  # Rapidly changing sentiment

        # Strategy-sentiment alignment risk
        strategy_category = strategy_rec.strategy_metadata.category
        if strategy_category == 'MOMENTUM' and abs_sentiment < 0.2:
            base_sentiment_risk += 10  # Momentum strategy in neutral sentiment
        elif strategy_category == 'MEAN_REVERSION' and abs_sentiment < 0.3:
            base_sentiment_risk -= 5  # Mean reversion benefits from neutral sentiment

        return max(0, min(100, base_sentiment_risk))

    def _calculate_liquidity_risk(self, df: pd.DataFrame) -> float:
        """Calculate liquidity risk based on volume patterns"""
        if 'Volume' not in df.columns:
            return 50.0  # Default moderate risk if no volume data

        base_liquidity_risk = 20.0

        # Recent volume analysis
        recent_volume = df['Volume'].tail(20)
        avg_volume = recent_volume.mean()
        volume_volatility = recent_volume.std() / avg_volume if avg_volume > 0 else 1.0

        # Low average volume increases risk
        if avg_volume < df['Volume'].quantile(0.2):
            base_liquidity_risk += 25
        elif avg_volume > df['Volume'].quantile(0.8):
            base_liquidity_risk -= 10

        # High volume volatility increases risk
        if volume_volatility > 0.5:
            base_liquidity_risk += 15

        # Volume trend analysis
        volume_trend = np.polyfit(range(len(recent_volume)), recent_volume, 1)[0]
        if volume_trend < -avg_volume * 0.1:  # Declining volume
            base_liquidity_risk += 10

        return max(0, min(100, base_liquidity_risk))

    def _calculate_execution_risk(self, df: pd.DataFrame, market_regime: MarketRegimeResult) -> float:
        """Calculate execution risk based on market conditions"""
        base_execution_risk = 15.0

        # High volatility increases execution risk
        if market_regime.volatility_regime == 'HIGH_VOLATILITY':
            base_execution_risk += 20
        elif market_regime.volatility_regime == 'LOW_VOLATILITY':
            base_execution_risk -= 5

        # Market regime uncertainty increases execution risk
        if market_regime.regime_confidence < 60:
            base_execution_risk += 15

        # Bid-ask spread proxy (high-low range)
        returns = df['Close'].pct_change()
        recent_returns = returns.tail(20)
        intraday_range = ((df['High'] - df['Low']) / df['Close']).tail(20)

        # High intraday ranges suggest wider spreads
        avg_range = intraday_range.mean()
        if avg_range > 0.03:  # 3% average range
            base_execution_risk += 15
        elif avg_range < 0.01:  # 1% average range
            base_execution_risk -= 5

        return max(0, min(100, base_execution_risk))

    def _calculate_composite_risk_score(self, market_risk: Dict, strategy_risk: float,
                                      regime_risk: float, sentiment_risk: float,
                                      liquidity_risk: float, execution_risk: float) -> float:
        """Calculate weighted composite risk score"""
        weights = self.config

        # Normalize market risk components to 0-100 scale
        vol_score = min(100, market_risk['volatility'] * 200)  # 50% vol = 100 points
        var_score = min(100, market_risk['var_1d'] * 2000)  # 5% VaR = 100 points
        beta_score = min(100, abs(market_risk['beta'] - 1) * 50)  # Beta deviation
        drawdown_score = min(100, market_risk['max_drawdown'] * 200)  # 50% DD = 100 points

        # Weighted average of all risk components
        composite_score = (
            vol_score * weights['volatility_weight'] +
            var_score * weights['var_weight'] +
            beta_score * weights['beta_weight'] +
            strategy_risk * weights['strategy_weight'] +
            regime_risk * weights['regime_weight'] +
            sentiment_risk * weights['sentiment_weight'] +
            liquidity_risk * 0.05 +  # 5% weight for liquidity
            execution_risk * 0.05    # 5% weight for execution
        )

        return max(0, min(100, composite_score))

    def _calculate_portfolio_market_risk(self, position_risks: List[PositionRisk],
                                       portfolio_weights: Dict[str, float],
                                       correlation_matrix: Optional[pd.DataFrame] = None) -> Dict:
        """Calculate portfolio-level market risk metrics"""

        # Weighted portfolio volatility
        weighted_vol_squared = 0
        total_correlation_risk = 0

        for i, risk1 in enumerate(position_risks):
            weight1 = portfolio_weights.get(risk1.symbol, 0)
            weighted_vol_squared += (weight1 * risk1.volatility) ** 2

            # Add correlation effects if correlation matrix available
            if correlation_matrix is not None:
                for j, risk2 in enumerate(position_risks):
                    if i != j:
                        weight2 = portfolio_weights.get(risk2.symbol, 0)

                        # Get correlation if available
                        corr = 0.3  # Default correlation assumption
                        if (risk1.symbol in correlation_matrix.index and
                            risk2.symbol in correlation_matrix.columns):
                            corr = correlation_matrix.loc[risk1.symbol, risk2.symbol]

                        covariance_term = weight1 * weight2 * risk1.volatility * risk2.volatility * corr
                        weighted_vol_squared += covariance_term

                        # Track correlation risk
                        if abs(corr) > self.config['max_correlation']:
                            total_correlation_risk += abs(corr) * weight1 * weight2 * 100

        portfolio_volatility = np.sqrt(max(0, weighted_vol_squared))

        # Weighted portfolio VaR (simplified)
        weighted_var = sum(
            portfolio_weights.get(risk.symbol, 0) * risk.var_1d
            for risk in position_risks
        )

        # Portfolio liquidity risk
        weighted_liquidity_risk = sum(
            portfolio_weights.get(risk.symbol, 0) * risk.liquidity_risk
            for risk in position_risks
        )

        return {
            'volatility': portfolio_volatility,
            'var': weighted_var,
            'correlation_risk': total_correlation_risk,
            'liquidity_risk': weighted_liquidity_risk
        }

    def _calculate_concentration_risks(self, position_risks: List[PositionRisk],
                                     portfolio_weights: Dict[str, float]) -> Dict:
        """Calculate various concentration risks"""

        # Position concentration (single position risk)
        max_position_weight = max(portfolio_weights.values()) if portfolio_weights else 0
        position_concentration = max_position_weight * 100

        # Strategy concentration
        strategy_weights = {}
        for risk in position_risks:
            strategy = risk.strategy_name
            weight = portfolio_weights.get(risk.symbol, 0)
            strategy_weights[strategy] = strategy_weights.get(strategy, 0) + weight

        max_strategy_weight = max(strategy_weights.values()) if strategy_weights else 0
        strategy_concentration = max_strategy_weight * 100

        # Sector concentration (simplified - would need sector mapping in production)
        # For now, assume all positions are in same sector
        sector_concentration = sum(portfolio_weights.values()) * 100

        # Overall concentration risk
        overall_concentration = max(position_concentration, strategy_concentration * 0.8)

        return {
            'position_concentration': position_concentration,
            'strategy_concentration': strategy_concentration,
            'sector_concentration': sector_concentration,
            'overall_concentration': overall_concentration
        }

    def _calculate_portfolio_regime_risk(self, market_regime: MarketRegimeResult,
                                       position_risks: List[PositionRisk]) -> float:
        """Calculate portfolio-level regime risk"""
        base_regime_risk = 25.0

        # Market stress affects entire portfolio
        stress_level = market_regime.market_stress_level
        base_regime_risk += stress_level * 0.4  # Up to 40 points

        # Regime transition risk
        transition_prob = market_regime.regime_transition_probability
        base_regime_risk += transition_prob * 30

        # Volatility regime risk
        if market_regime.volatility_regime == 'HIGH_VOLATILITY':
            base_regime_risk += 20

        # Average position regime risk
        avg_position_regime_risk = np.mean([risk.regime_risk for risk in position_risks])
        base_regime_risk = (base_regime_risk + avg_position_regime_risk) / 2

        return max(0, min(100, base_regime_risk))

    def _calculate_portfolio_sentiment_risk(self, market_sentiment: MarketSentimentSummary,
                                          position_risks: List[PositionRisk]) -> float:
        """Calculate portfolio-level sentiment risk"""
        base_sentiment_risk = 20.0

        # Market-wide sentiment extremes
        abs_market_sentiment = abs(market_sentiment.overall_sentiment)
        if abs_market_sentiment > 0.7:
            base_sentiment_risk += 25
        elif abs_market_sentiment > 0.5:
            base_sentiment_risk += 15

        # Sentiment dispersion risk
        if market_sentiment.sentiment_dispersion > 0.4:
            base_sentiment_risk += 15

        # Market stress signals
        if len(market_sentiment.market_stress_signals) > 0:
            base_sentiment_risk += len(market_sentiment.market_stress_signals) * 5

        # Average position sentiment risk
        avg_position_sentiment_risk = np.mean([risk.sentiment_risk for risk in position_risks])
        base_sentiment_risk = (base_sentiment_risk + avg_position_sentiment_risk) / 2

        return max(0, min(100, base_sentiment_risk))

    def _calculate_diversification_score(self, position_risks: List[PositionRisk],
                                       portfolio_weights: Dict[str, float],
                                       correlation_matrix: Optional[pd.DataFrame] = None) -> float:
        """Calculate portfolio diversification effectiveness (0-100)"""

        if len(position_risks) <= 1:
            return 0.0  # No diversification with single position

        base_score = 50.0

        # Number of positions bonus
        num_positions = len(position_risks)
        if num_positions >= 10:
            base_score += 25
        elif num_positions >= 5:
            base_score += 15
        elif num_positions >= 3:
            base_score += 10

        # Weight distribution bonus (more even = better)
        weights = list(portfolio_weights.values())
        if weights:
            weight_gini = self._calculate_gini_coefficient(weights)
            base_score += (1 - weight_gini) * 15  # Lower Gini = more even = bonus

        # Strategy diversification bonus
        strategies = set(risk.strategy_name for risk in position_risks)
        strategy_bonus = min(15, len(strategies) * 3)
        base_score += strategy_bonus

        # Correlation penalty
        if correlation_matrix is not None and len(position_risks) > 1:
            avg_correlation = self._calculate_average_correlation(
                [risk.symbol for risk in position_risks], correlation_matrix
            )
            correlation_penalty = max(0, (avg_correlation - 0.3) * 30)  # Penalty if avg > 30%
            base_score -= correlation_penalty

        return max(0, min(100, base_score))

    def _calculate_dynamic_risk_factors(self, market_regime: MarketRegimeResult,
                                      market_sentiment: MarketSentimentSummary) -> Dict:
        """Calculate dynamic risk factors"""

        # Regime transition risk
        transition_risk = market_regime.regime_transition_probability * 100

        # Volatility regime risk
        vol_risk_map = {
            'HIGH_VOLATILITY': 75,
            'NORMAL_VOLATILITY': 40,
            'LOW_VOLATILITY': 25
        }
        volatility_risk = vol_risk_map.get(market_regime.volatility_regime, 40)

        # Macro risk (simplified - would include economic indicators in production)
        macro_risk = 30.0

        # Market stress amplifies all risks
        stress_multiplier = 1 + (market_regime.market_stress_level / 200)  # 0% stress = 1x, 100% stress = 1.5x

        return {
            'transition_risk': transition_risk * stress_multiplier,
            'volatility_risk': volatility_risk * stress_multiplier,
            'macro_risk': macro_risk * stress_multiplier
        }

    def _calculate_risk_budget_utilization(self, portfolio_market_risk: Dict,
                                         concentration_risks: Dict) -> Dict:
        """Calculate risk budget utilization metrics"""

        # Portfolio volatility vs limit
        vol_utilization = (portfolio_market_risk['volatility'] /
                          self.config['max_portfolio_volatility']) * 100

        # Portfolio VaR vs limit
        var_utilization = (portfolio_market_risk['var'] /
                          self.config['max_portfolio_var']) * 100

        # Concentration vs limit
        conc_utilization = (concentration_risks['overall_concentration'] /
                           (self.config['max_concentration'] * 100)) * 100

        # Overall utilization (worst case)
        overall_utilization = max(vol_utilization, var_utilization, conc_utilization)

        # Margin of safety
        margin_of_safety = max(0, 100 - overall_utilization)

        return {
            'utilization': overall_utilization,
            'vol_utilization': vol_utilization,
            'var_utilization': var_utilization,
            'concentration_utilization': conc_utilization,
            'margin_of_safety': margin_of_safety
        }

    def _calculate_total_portfolio_risk(self, portfolio_market_risk: Dict,
                                      concentration_risks: Dict, regime_risk: float,
                                      sentiment_risk: float, dynamic_risks: Dict) -> float:
        """Calculate total portfolio risk score"""

        # Market risk component (30% weight)
        market_risk_score = (
            min(100, portfolio_market_risk['volatility'] * 400) * 0.4 +  # Volatility
            min(100, portfolio_market_risk['var'] * 3333) * 0.3 +        # VaR
            portfolio_market_risk['correlation_risk'] * 0.3              # Correlation
        )

        # Concentration risk component (25% weight)
        concentration_score = concentration_risks['overall_concentration']

        # Regime and sentiment risk component (25% weight)
        regime_sentiment_score = (regime_risk + sentiment_risk) / 2

        # Dynamic risk component (20% weight)
        dynamic_score = (
            dynamic_risks['transition_risk'] * 0.4 +
            dynamic_risks['volatility_risk'] * 0.4 +
            dynamic_risks['macro_risk'] * 0.2
        )

        # Weighted total risk
        total_risk = (
            market_risk_score * 0.30 +
            concentration_score * 0.25 +
            regime_sentiment_score * 0.25 +
            dynamic_score * 0.20
        )

        return max(0, min(100, total_risk))

    def _determine_risk_category(self, risk_score: float) -> str:
        """Determine risk category from risk score"""
        if risk_score < self.config['low_risk_threshold']:
            return 'LOW'
        elif risk_score < self.config['medium_risk_threshold']:
            return 'MEDIUM'
        elif risk_score < self.config['high_risk_threshold']:
            return 'HIGH'
        else:
            return 'EXTREME'

    def _determine_portfolio_risk_level(self, risk_score: float) -> str:
        """Determine portfolio risk level"""
        if risk_score < 35:
            return 'LOW'
        elif risk_score < 65:
            return 'MEDIUM'
        elif risk_score < 85:
            return 'HIGH'
        else:
            return 'CRITICAL'

    def _generate_risk_factors_and_warnings(self, market_risk: Dict, strategy_risk: float,
                                          regime_risk: float, sentiment_risk: float,
                                          liquidity_risk: float, execution_risk: float,
                                          total_risk: float) -> Tuple[List[str], List[str]]:
        """Generate risk factors and warnings for position"""

        risk_factors = []
        risk_warnings = []

        # Market risk factors
        if market_risk['volatility'] > 0.35:
            risk_factors.append(f"High volatility ({market_risk['volatility']:.1%})")
        if market_risk['beta'] > 1.5:
            risk_factors.append(f"High beta exposure ({market_risk['beta']:.1f})")
        if market_risk['var_1d'] > 0.03:
            risk_factors.append(f"High daily VaR ({market_risk['var_1d']:.1%})")
        if market_risk['max_drawdown'] > 0.25:
            risk_factors.append(f"High historical drawdown ({market_risk['max_drawdown']:.1%})")

        # Strategy risk factors
        if strategy_risk > 60:
            risk_factors.append("Strategy-specific risk concerns")

        # Regime risk factors
        if regime_risk > 60:
            risk_factors.append("Market regime mismatch risk")

        # Sentiment risk factors
        if sentiment_risk > 60:
            risk_factors.append("Sentiment extreme risk")

        # Liquidity risk factors
        if liquidity_risk > 60:
            risk_factors.append("Liquidity constraints")

        # Execution risk factors
        if execution_risk > 60:
            risk_factors.append("Execution risk in current market")

        # Generate warnings
        if total_risk > 85:
            risk_warnings.append("EXTREME RISK: Consider reducing position size")
        elif total_risk > 70:
            risk_warnings.append("HIGH RISK: Monitor position closely")

        if market_risk['volatility'] > self.config['max_position_volatility']:
            risk_warnings.append(f"Volatility exceeds limit ({market_risk['volatility']:.1%})")

        if market_risk['var_1d'] > self.config['max_position_var']:
            risk_warnings.append(f"VaR exceeds limit ({market_risk['var_1d']:.1%})")

        return risk_factors, risk_warnings

    def _generate_portfolio_risk_recommendations(self, position_risks: List[PositionRisk],
                                               portfolio_market_risk: Dict,
                                               concentration_risks: Dict,
                                               regime_risk: float, sentiment_risk: float,
                                               total_risk: float) -> Tuple[List[str], List[str], Dict[str, str]]:
        """Generate portfolio risk recommendations, alerts, and position adjustments"""

        recommendations = []
        alerts = []
        adjustments = {}

        # Portfolio-level recommendations
        if total_risk > 80:
            recommendations.append("URGENT: Reduce overall portfolio risk")
            alerts.append("Portfolio risk at critical levels")
        elif total_risk > 65:
            recommendations.append("Consider reducing portfolio risk exposure")

        # Concentration recommendations
        if concentration_risks['overall_concentration'] > 80:
            recommendations.append("Reduce position concentration")
            alerts.append("High concentration risk detected")

        if concentration_risks['strategy_concentration'] > 70:
            recommendations.append("Diversify across more strategies")

        # Volatility recommendations
        if portfolio_market_risk['volatility'] > self.config['max_portfolio_volatility']:
            recommendations.append(f"Portfolio volatility too high ({portfolio_market_risk['volatility']:.1%})")
            alerts.append("Portfolio volatility limit exceeded")

        # VaR recommendations
        if portfolio_market_risk['var'] > self.config['max_portfolio_var']:
            recommendations.append(f"Portfolio VaR too high ({portfolio_market_risk['var']:.1%})")
            alerts.append("Portfolio VaR limit exceeded")

        # Regime-specific recommendations
        if regime_risk > 70:
            recommendations.append("Consider defensive positioning given market regime")

        # Sentiment-specific recommendations
        if sentiment_risk > 70:
            recommendations.append("Monitor sentiment extremes closely")

        # Position-specific adjustments
        for risk in position_risks:
            if risk.total_risk_score > 85:
                adjustments[risk.symbol] = "REDUCE: Extreme risk"
            elif risk.total_risk_score > 70:
                adjustments[risk.symbol] = "MONITOR: High risk"
            elif risk.liquidity_risk > 80:
                adjustments[risk.symbol] = "CAUTION: Liquidity concerns"

        return recommendations, alerts, adjustments

    def calculate_risk_adjusted_position_size(self, base_allocation: float,
                                            position_risk: PositionRisk,
                                            portfolio_risk: Optional[PortfolioRisk] = None) -> Dict[str, Any]:
        """
        Calculate risk-adjusted position size based on comprehensive risk analysis

        Args:
            base_allocation: Base position size (e.g., 0.10 for 10%)
            position_risk: PositionRisk object from analyze_position_risk()
            portfolio_risk: Optional PortfolioRisk for portfolio-level adjustments

        Returns:
            Dict with sizing recommendation and rationale
        """

        # Start with base allocation
        adjusted_size = base_allocation
        adjustments_applied = []

        # 1. Risk Score Adjustment (primary adjustment)
        risk_score = position_risk.total_risk_score

        if risk_score > 85:  # Extreme risk
            risk_multiplier = 0.25  # 75% reduction
            adjustments_applied.append(f"Extreme risk reduction: {risk_score:.0f}/100")
        elif risk_score > 70:  # High risk
            risk_multiplier = 0.50  # 50% reduction
            adjustments_applied.append(f"High risk reduction: {risk_score:.0f}/100")
        elif risk_score > 60:  # Medium-high risk
            risk_multiplier = 0.75  # 25% reduction
            adjustments_applied.append(f"Medium risk reduction: {risk_score:.0f}/100")
        elif risk_score < 30:  # Low risk
            risk_multiplier = 1.25  # 25% increase
            adjustments_applied.append(f"Low risk bonus: {risk_score:.0f}/100")
        elif risk_score < 40:  # Medium-low risk
            risk_multiplier = 1.10  # 10% increase
            adjustments_applied.append(f"Medium-low risk bonus: {risk_score:.0f}/100")
        else:  # Normal risk
            risk_multiplier = 1.0

        adjusted_size *= risk_multiplier

        # 2. Volatility Adjustment
        volatility = position_risk.volatility
        if volatility > self.config['max_position_volatility']:
            vol_penalty = 1 - ((volatility - self.config['max_position_volatility']) * 2)
            vol_penalty = max(0.2, vol_penalty)  # Don't reduce below 20%
            adjusted_size *= vol_penalty
            adjustments_applied.append(f"Volatility penalty: {volatility:.1%} vol")
        elif volatility < 0.15:  # Low volatility bonus
            vol_bonus = min(1.15, 1 + (0.15 - volatility))
            adjusted_size *= vol_bonus
            adjustments_applied.append(f"Low volatility bonus: {volatility:.1%} vol")

        # 3. VaR Adjustment
        var_1d = position_risk.var_1d
        if var_1d > self.config['max_position_var']:
            var_penalty = 1 - ((var_1d - self.config['max_position_var']) * 10)
            var_penalty = max(0.3, var_penalty)  # Don't reduce below 30%
            adjusted_size *= var_penalty
            adjustments_applied.append(f"VaR penalty: {var_1d:.1%} daily VaR")

        # 4. Beta Adjustment
        beta = position_risk.beta
        if beta > self.config['max_beta']:
            beta_penalty = 1 - ((beta - self.config['max_beta']) * 0.2)
            beta_penalty = max(0.5, beta_penalty)  # Don't reduce below 50%
            adjusted_size *= beta_penalty
            adjustments_applied.append(f"High beta penalty: {beta:.1f} beta")
        elif beta < 0.5:  # Low beta bonus for defensive positioning
            beta_bonus = min(1.1, 1 + (0.5 - beta) * 0.2)
            adjusted_size *= beta_bonus
            adjustments_applied.append(f"Low beta bonus: {beta:.1f} beta")

        # 5. Liquidity Adjustment
        liquidity_risk = position_risk.liquidity_risk
        if liquidity_risk > 70:
            liquidity_penalty = 0.7  # 30% reduction for high liquidity risk
            adjusted_size *= liquidity_penalty
            adjustments_applied.append(f"Liquidity penalty: {liquidity_risk:.0f}/100")

        # 6. Confidence Adjustment
        confidence_risk = position_risk.confidence_risk
        if confidence_risk > 50:  # Low confidence signal
            confidence_penalty = 1 - (confidence_risk - 50) * 0.01  # Up to 50% reduction
            confidence_penalty = max(0.5, confidence_penalty)
            adjusted_size *= confidence_penalty
            adjustments_applied.append(f"Low confidence penalty: {100-confidence_risk:.0f}% confidence")
        elif confidence_risk < 15:  # High confidence bonus
            confidence_bonus = min(1.15, 1 + (15 - confidence_risk) * 0.01)
            adjusted_size *= confidence_bonus
            adjustments_applied.append(f"High confidence bonus: {100-confidence_risk:.0f}% confidence")

        # 7. Portfolio-Level Adjustments
        if portfolio_risk is not None:
            # Portfolio risk budget utilization
            if portfolio_risk.risk_budget_utilization > 80:
                portfolio_penalty = 0.8  # 20% reduction if near risk limits
                adjusted_size *= portfolio_penalty
                adjustments_applied.append(f"Portfolio risk limit penalty: {portfolio_risk.risk_budget_utilization:.0f}% used")

            # Portfolio concentration
            if portfolio_risk.concentration_risk > 70:
                concentration_penalty = 0.85  # 15% reduction for high concentration
                adjusted_size *= concentration_penalty
                adjustments_applied.append(f"Portfolio concentration penalty: {portfolio_risk.concentration_risk:.0f}/100")

        # 8. Apply Position Size Limits
        final_size = max(self.config['min_position_size'],
                        min(self.config['max_position_size'], adjusted_size))

        # If we hit limits, note it
        if final_size == self.config['min_position_size'] and adjusted_size < final_size:
            adjustments_applied.append(f"Minimum position size floor applied: {self.config['min_position_size']:.1%}")
        elif final_size == self.config['max_position_size'] and adjusted_size > final_size:
            adjustments_applied.append(f"Maximum position size cap applied: {self.config['max_position_size']:.1%}")

        # Calculate percentage change
        size_change_pct = ((final_size - base_allocation) / base_allocation) * 100

        # Determine size direction
        if size_change_pct > 10:
            size_direction = "INCREASE"
        elif size_change_pct < -10:
            size_direction = "DECREASE"
        else:
            size_direction = "MAINTAIN"

        # Generate recommendations
        recommendations = []
        if final_size < base_allocation * 0.6:
            recommendations.append("Consider reducing exposure due to high risk")
        elif final_size > base_allocation * 1.3:
            recommendations.append("Consider increased allocation due to favorable risk profile")

        if position_risk.total_risk_score > 85:
            recommendations.append("AVOID: Risk too high for current market conditions")
        elif position_risk.total_risk_score < 30:
            recommendations.append("OPPORTUNITY: Low risk, consider larger allocation")

        # Sizing rationale
        sizing_rationale = self._generate_sizing_rationale(
            base_allocation, final_size, risk_score, position_risk.volatility, adjustments_applied
        )

        return {
            'recommended_size': final_size,
            'base_allocation': base_allocation,
            'size_change_pct': size_change_pct,
            'size_direction': size_direction,
            'adjustments_applied': adjustments_applied,
            'recommendations': recommendations,
            'sizing_rationale': sizing_rationale,
            'risk_adjusted': True
        }

    def _generate_sizing_rationale(self, base_allocation: float, final_size: float,
                                 risk_score: float, volatility: float,
                                 adjustments: List[str]) -> str:
        """Generate human-readable sizing rationale"""

        change_pct = ((final_size - base_allocation) / base_allocation) * 100

        if abs(change_pct) < 5:
            rationale = f"Position size maintained at {final_size:.1%} (minimal risk adjustments)"
        elif change_pct > 0:
            rationale = f"Position size increased {change_pct:.0f}% to {final_size:.1%} due to favorable risk profile"
        else:
            rationale = f"Position size reduced {abs(change_pct):.0f}% to {final_size:.1%} due to risk concerns"

        if risk_score > 70:
            rationale += f" (high risk score: {risk_score:.0f}/100)"
        elif risk_score < 40:
            rationale += f" (low risk score: {risk_score:.0f}/100)"

        if volatility > 0.4:
            rationale += f" with high volatility adjustment ({volatility:.1%})"

        return rationale

    def get_position_sizing_recommendation(self, symbol: str, base_allocation: float,
                                         strategy_rec: StrategyRecommendation,
                                         market_regime: MarketRegimeResult,
                                         sentiment: SentimentReading,
                                         df: pd.DataFrame,
                                         portfolio_risk: Optional[PortfolioRisk] = None) -> Dict[str, Any]:
        """
        COMPLETE POSITION SIZING WORKFLOW

        Combines risk analysis with position sizing in one method for convenience.
        This is the main method portfolio managers should use.
        """

        # First, analyze position risk
        position_risk = self.analyze_position_risk(
            symbol=symbol,
            df=df,
            strategy_rec=strategy_rec,
            market_regime=market_regime,
            sentiment=sentiment,
            position_size=base_allocation  # Use base allocation for initial risk calculation
        )

        # Then, calculate risk-adjusted position size
        sizing_result = self.calculate_risk_adjusted_position_size(
            base_allocation=base_allocation,
            position_risk=position_risk,
            portfolio_risk=portfolio_risk
        )

        # Combine results for comprehensive recommendation
        return {
            'symbol': symbol,
            'strategy': strategy_rec.strategy_name,

            # Position sizing results
            'recommended_size': sizing_result['recommended_size'],
            'base_allocation': sizing_result['base_allocation'],
            'size_change_pct': sizing_result['size_change_pct'],
            'size_direction': sizing_result['size_direction'],

            # Risk assessment
            'risk_score': position_risk.total_risk_score,
            'risk_category': position_risk.risk_category,
            'volatility': position_risk.volatility,
            'var_1d': position_risk.var_1d,

            # Key risk factors
            'primary_risk_factors': position_risk.risk_factors[:3],
            'risk_warnings': position_risk.risk_warnings,

            # Regime compatibility
            'regime_compatibility': self.get_regime_compatibility_penalty(
                strategy_rec.strategy_metadata.category, market_regime
            ),
            'regime_risk': position_risk.regime_risk,

            # Sizing rationale
            'adjustments_applied': sizing_result['adjustments_applied'],
            'recommendations': sizing_result['recommendations'],
            'sizing_details': sizing_result['sizing_rationale'],

            # Summary assessment
            'overall_assessment': self._generate_overall_assessment(
                position_risk, sizing_result, strategy_rec, market_regime
            ),

            'timestamp': datetime.now()
        }

    def _generate_overall_assessment(self, position_risk: PositionRisk,
                                   sizing_result: Dict, strategy_rec: StrategyRecommendation,
                                   market_regime: MarketRegimeResult) -> str:
        """Generate overall assessment of position recommendation"""

        risk_score = position_risk.total_risk_score
        size_change = sizing_result['size_change_pct']
        strategy_category = strategy_rec.strategy_metadata.category

        # Regime compatibility assessment
        compatibility = self.get_regime_compatibility_penalty(strategy_category, market_regime)

        if risk_score > 85:
            assessment = f" AVOID: Extreme risk ({risk_score:.0f}/100)"
        elif risk_score > 70:
            assessment = f" CAUTION: High risk position, reduced size by {abs(size_change):.0f}%"
        elif compatibility < -10:  # Very good regime compatibility
            assessment = f" FAVORABLE: Good regime fit, {sizing_result['size_direction'].lower()} allocation"
        elif size_change < -20:
            assessment = f" PROCEED WITH CAUTION: Size reduced by {abs(size_change):.0f}% due to risk"
        elif size_change > 10:
            assessment = f" OPPORTUNITY: Low risk, increased allocation by {size_change:.0f}%"
        else:
            assessment = f" ACCEPTABLE: Normal risk profile, standard allocation"

        return assessment

    def get_regime_compatibility_report(self, strategy_category: str,
                                      market_regime: MarketRegimeResult) -> Dict[str, Any]:
        """
        Generate detailed regime compatibility report for a strategy
        Useful for understanding why position sizes are being adjusted
        """

        compatibility_score = self.get_regime_compatibility_penalty(strategy_category, market_regime)

        # Determine compatibility level
        if compatibility_score < -15:
            compatibility_level = "EXCELLENT"
            color = ""
        elif compatibility_score < -5:
            compatibility_level = "GOOD"
            color = ""
        elif compatibility_score < 5:
            compatibility_level = "NEUTRAL"
            color = ""
        elif compatibility_score < 15:
            compatibility_level = "POOR"
            color = "🟠"
        else:
            compatibility_level = "VERY POOR"
            color = ""

        # Get specific compatibility factors
        primary_regime = market_regime.primary_regime
        vol_regime = market_regime.volatility_regime
        rsi_condition = market_regime.rsi_condition

        primary_key = (strategy_category, primary_regime)
        vol_key = (strategy_category, vol_regime)
        rsi_key = (strategy_category, rsi_condition)

        compatibility_factors = {}

        if primary_key in self.regime_compatibility_matrix:
            primary_score = self.regime_compatibility_matrix[primary_key]
            compatibility_factors['primary_regime'] = {
                'regime': primary_regime,
                'score': primary_score,
                'impact': 'Positive' if primary_score > 0 else 'Negative' if primary_score < 0 else 'Neutral'
            }

        if vol_key in self.regime_compatibility_matrix:
            vol_score = self.regime_compatibility_matrix[vol_key]
            compatibility_factors['volatility_regime'] = {
                'regime': vol_regime,
                'score': vol_score,
                'impact': 'Positive' if vol_score > 0 else 'Negative' if vol_score < 0 else 'Neutral'
            }

        if rsi_key in self.regime_compatibility_matrix:
            rsi_score = self.regime_compatibility_matrix[rsi_key]
            compatibility_factors['rsi_condition'] = {
                'condition': rsi_condition,
                'score': rsi_score,
                'impact': 'Positive' if rsi_score > 0 else 'Negative' if rsi_score < 0 else 'Neutral'
            }

        return {
            'strategy_category': strategy_category,
            'compatibility_level': compatibility_level,
            'compatibility_score': compatibility_score,
            'color_indicator': color,
            'market_regime': {
                'primary': primary_regime,
                'volatility': vol_regime,
                'rsi': rsi_condition,
                'confidence': market_regime.regime_confidence
            },
            'compatibility_factors': compatibility_factors,
            'recommendation': self._get_compatibility_recommendation(
                compatibility_level, strategy_category, primary_regime
            )
        }

    def _get_compatibility_recommendation(self, compatibility_level: str,
                                        strategy_category: str, regime: str) -> str:
        """Get recommendation based on regime compatibility"""

        if compatibility_level == "EXCELLENT":
            return f"{strategy_category} strategies are ideal for {regime} conditions"
        elif compatibility_level == "GOOD":
            return f"{strategy_category} strategies work well in {regime} markets"
        elif compatibility_level == "NEUTRAL":
            return f"{strategy_category} strategies are acceptable in {regime} conditions"
        elif compatibility_level == "POOR":
            return f"Consider reducing {strategy_category} exposure in {regime} markets"
        else:  # VERY POOR
            return f"Avoid {strategy_category} strategies in {regime} conditions"

    def monitor_risk_limits(self, portfolio_risk: PortfolioRisk) -> Dict[str, bool]:
        """Monitor if portfolio is within risk limits"""
        return {
            'volatility_limit_ok': portfolio_risk.portfolio_volatility <= self.config['max_portfolio_volatility'],
            'var_limit_ok': portfolio_risk.portfolio_var <= self.config['max_portfolio_var'],
            'concentration_limit_ok': portfolio_risk.concentration_risk <= self.config['max_concentration'] * 100,
            'overall_risk_ok': portfolio_risk.total_portfolio_risk < 85,
            'margin_of_safety_ok': portfolio_risk.margin_of_safety > 15
        }

    def get_portfolio_sizing_summary(self, position_sizing_results: List[Dict]) -> Dict[str, Any]:
        """
        Generate portfolio-level position sizing summary

        Args:
            position_sizing_results: List of results from get_position_sizing_recommendation()

        Returns:
            Portfolio-level sizing analysis and recommendations
        """

        if not position_sizing_results:
            return {'error': 'No position sizing results provided'}

        # Calculate portfolio metrics
        total_recommended_allocation = sum(result['recommended_size'] for result in position_sizing_results)
        total_base_allocation = sum(result['base_allocation'] for result in position_sizing_results)

        # Risk distribution
        risk_categories = {}
        for result in position_sizing_results:
            category = result['risk_category']
            risk_categories[category] = risk_categories.get(category, 0) + 1

        # Strategy distribution
        strategy_distribution = {}
        for result in position_sizing_results:
            strategy = result['strategy']
            size = result['recommended_size']
            strategy_distribution[strategy] = strategy_distribution.get(strategy, 0) + size

        # Size adjustment analysis
        increased_positions = [r for r in position_sizing_results if r['size_change_pct'] > 10]
        reduced_positions = [r for r in position_sizing_results if r['size_change_pct'] < -10]

        # Risk warnings aggregation
        all_warnings = []
        for result in position_sizing_results:
            all_warnings.extend(result.get('risk_warnings', []))

        # Portfolio recommendations
        portfolio_recommendations = []

        if total_recommended_allocation > 0.95:
            portfolio_recommendations.append(" Very high portfolio allocation - consider cash reserve")
        elif total_recommended_allocation < 0.50:
            portfolio_recommendations.append(" Conservative allocation - consider increasing exposure")

        if len([r for r in position_sizing_results if r['risk_category'] in ['HIGH', 'EXTREME']]) > len(position_sizing_results) * 0.3:
            portfolio_recommendations.append(" High concentration of risky positions")

        if len(strategy_distribution) < 3:
            portfolio_recommendations.append(" Consider more strategy diversification")

        return {
            'portfolio_allocation': {
                'total_recommended': total_recommended_allocation,
                'total_base': total_base_allocation,
                'allocation_change_pct': ((total_recommended_allocation - total_base_allocation) / total_base_allocation * 100) if total_base_allocation > 0 else 0,
                'cash_reserve': max(0, 1.0 - total_recommended_allocation)
            },

            'risk_distribution': {
                'by_category': risk_categories,
                'high_risk_count': risk_categories.get('HIGH', 0) + risk_categories.get('EXTREME', 0),
                'low_risk_count': risk_categories.get('LOW', 0),
                'avg_risk_score': np.mean([r['risk_score'] for r in position_sizing_results])
            },

            'strategy_distribution': strategy_distribution,

            'sizing_adjustments': {
                'positions_increased': len(increased_positions),
                'positions_reduced': len(reduced_positions),
                'positions_unchanged': len(position_sizing_results) - len(increased_positions) - len(reduced_positions),
                'avg_size_change': np.mean([r['size_change_pct'] for r in position_sizing_results]),
                'largest_reduction': min([r['size_change_pct'] for r in position_sizing_results], default=0),
                'largest_increase': max([r['size_change_pct'] for r in position_sizing_results], default=0)
            },

            'top_opportunities': sorted(
                [r for r in position_sizing_results if r['size_change_pct'] > 5],
                key=lambda x: x['size_change_pct'], reverse=True
            )[:3],

            'top_risks': sorted(
                [r for r in position_sizing_results if r['risk_score'] > 60],
                key=lambda x: x['risk_score'], reverse=True
            )[:3],

            'portfolio_warnings': list(set(all_warnings)),
            'portfolio_recommendations': portfolio_recommendations,

            'regime_compatibility_summary': self._analyze_portfolio_regime_compatibility(position_sizing_results),

            'summary_assessment': self._generate_portfolio_sizing_assessment(
                total_recommended_allocation, risk_categories, len(strategy_distribution)
            )
        }

    def _analyze_portfolio_regime_compatibility(self, position_sizing_results: List[Dict]) -> Dict[str, Any]:
        """Analyze regime compatibility across the portfolio"""

        compatibility_scores = []
        strategy_regime_fit = {}

        for result in position_sizing_results:
            compatibility = result.get('regime_compatibility', 0)
            compatibility_scores.append(compatibility)

            strategy = result['strategy']
            if strategy not in strategy_regime_fit:
                strategy_regime_fit[strategy] = []
            strategy_regime_fit[strategy].append(compatibility)

        # Calculate average compatibility by strategy
        strategy_avg_compatibility = {
            strategy: np.mean(scores)
            for strategy, scores in strategy_regime_fit.items()
        }

        # Overall portfolio regime fit
        overall_compatibility = np.mean(compatibility_scores) if compatibility_scores else 0

        if overall_compatibility < -10:
            regime_assessment = " EXCELLENT regime fit across portfolio"
        elif overall_compatibility < -5:
            regime_assessment = " GOOD regime fit for most positions"
        elif overall_compatibility < 5:
            regime_assessment = " NEUTRAL regime compatibility"
        elif overall_compatibility < 15:
            regime_assessment = "🟠 POOR regime fit - consider adjustments"
        else:
            regime_assessment = " VERY POOR regime fit - major adjustments needed"

        return {
            'overall_compatibility': overall_compatibility,
            'regime_assessment': regime_assessment,
            'strategy_compatibility': strategy_avg_compatibility,
            'best_fit_strategies': sorted(
                strategy_avg_compatibility.items(),
                key=lambda x: x[1]
            )[:3],
            'worst_fit_strategies': sorted(
                strategy_avg_compatibility.items(),
                key=lambda x: x[1], reverse=True
            )[:3]
        }

    def _generate_portfolio_sizing_assessment(self, total_allocation: float,
                                            risk_categories: Dict, strategy_count: int) -> str:
        """Generate overall portfolio sizing assessment"""

        high_risk_positions = risk_categories.get('HIGH', 0) + risk_categories.get('EXTREME', 0)
        total_positions = sum(risk_categories.values())

        if total_allocation > 0.9:
            allocation_desc = "Very aggressive allocation"
        elif total_allocation > 0.7:
            allocation_desc = "Moderate allocation"
        elif total_allocation > 0.5:
            allocation_desc = "Conservative allocation"
        else:
            allocation_desc = "Very conservative allocation"

        if high_risk_positions > total_positions * 0.4:
            risk_desc = "high risk concentration"
        elif high_risk_positions > total_positions * 0.2:
            risk_desc = "moderate risk levels"
        else:
            risk_desc = "well-managed risk"

        if strategy_count >= 4:
            diversification_desc = "good diversification"
        elif strategy_count >= 2:
            diversification_desc = "moderate diversification"
        else:
            diversification_desc = "limited diversification"

        return f"{allocation_desc} ({total_allocation:.0%}) with {risk_desc} and {diversification_desc} across {strategy_count} strategies"

    def export_risk_report(self, position_risks: List[PositionRisk],
                          portfolio_risk: PortfolioRisk,
                          position_sizing_results: List[Dict]) -> Dict[str, Any]:
        """
        Export comprehensive risk report for external analysis or compliance
        """

        return {
            'report_timestamp': datetime.now().isoformat(),
            'report_type': 'COMPREHENSIVE_RISK_ANALYSIS',

            'executive_summary': {
                'portfolio_risk_level': portfolio_risk.risk_level,
                'total_portfolio_risk': portfolio_risk.total_portfolio_risk,
                'risk_budget_utilization': portfolio_risk.risk_budget_utilization,
                'margin_of_safety': portfolio_risk.margin_of_safety,
                'key_alerts': portfolio_risk.risk_alerts[:5]
            },

            'portfolio_metrics': {
                'volatility': portfolio_risk.portfolio_volatility,
                'var_95': portfolio_risk.portfolio_var,
                'concentration_risk': portfolio_risk.concentration_risk,
                'diversification_score': portfolio_risk.diversification_score,
                'correlation_risk': portfolio_risk.correlation_risk
            },

            'position_analysis': [
                {
                    'symbol': risk.symbol,
                    'strategy': risk.strategy_name,
                    'risk_score': risk.total_risk_score,
                    'risk_category': risk.risk_category,
                    'volatility': risk.volatility,
                    'var_1d': risk.var_1d,
                    'beta': risk.beta,
                    'risk_factors': risk.risk_factors,
                    'warnings': risk.risk_warnings
                }
                for risk in position_risks
            ],

            'position_sizing': [
                {
                    'symbol': result['symbol'],
                    'strategy': result['strategy'],
                    'base_allocation': result['base_allocation'],
                    'recommended_size': result['recommended_size'],
                    'size_change_pct': result['size_change_pct'],
                    'size_direction': result['size_direction'],
                    'adjustments': result['adjustments_applied'],
                    'overall_assessment': result['overall_assessment']
                }
                for result in position_sizing_results
            ],

            'risk_limits_compliance': self.monitor_risk_limits(portfolio_risk),

            'regime_analysis': {
                'market_regime_risk': portfolio_risk.market_regime_risk,
                'sentiment_risk': portfolio_risk.sentiment_risk,
                'transition_risk': portfolio_risk.regime_transition_risk,
                'volatility_risk': portfolio_risk.volatility_regime_risk
            },

            'recommendations': {
                'portfolio_recommendations': portfolio_risk.risk_recommendations,
                'position_adjustments': portfolio_risk.position_adjustments,
                'priority_actions': [
                    rec for rec in portfolio_risk.risk_recommendations
                    if any(word in rec.upper() for word in ['URGENT', 'CRITICAL', 'IMMEDIATE'])
                ]
            },

            'configuration': {
                'risk_thresholds': {
                    'max_portfolio_volatility': self.config['max_portfolio_volatility'],
                    'max_portfolio_var': self.config['max_portfolio_var'],
                    'max_concentration': self.config['max_concentration']
                },
                'position_sizing_params': {
                    'base_position_size': self.config['base_position_size'],
                    'max_position_size': self.config['max_position_size'],
                    'min_position_size': self.config['min_position_size']
                }
            }
        }

    # Utility methods for calculations
    def _calculate_gini_coefficient(self, values: List[float]) -> float:
        """Calculate Gini coefficient for measuring inequality"""
        if not values or len(values) <= 1:
            return 0.0

        sorted_values = sorted(values)
        n = len(sorted_values)
        cumsum = np.cumsum(sorted_values)

        return (n + 1 - 2 * np.sum(cumsum) / cumsum[-1]) / n

    def _calculate_average_correlation(self, symbols: List[str],
                                     correlation_matrix: pd.DataFrame) -> float:
        """Calculate average correlation between symbols"""
        correlations = []

        for i, symbol1 in enumerate(symbols):
            for j, symbol2 in enumerate(symbols):
                if i < j and symbol1 in correlation_matrix.index and symbol2 in correlation_matrix.columns:
                    corr = correlation_matrix.loc[symbol1, symbol2]
                    if not pd.isna(corr):
                        correlations.append(abs(corr))

        return np.mean(correlations) if correlations else 0.3

    # Fallback methods for error handling
    def _insufficient_data_position_risk(self, symbol: str, strategy_name: str) -> PositionRisk:
        """Fallback for insufficient data"""
        return PositionRisk(
            symbol=symbol,
            strategy_name=strategy_name,
            volatility=0.30,  # Default high volatility assumption
            beta=1.0,
            var_1d=0.03,
            var_5d=0.07,
            max_drawdown=0.20,
            strategy_risk_score=60.0,
            confidence_risk=50.0,
            regime_risk=60.0,
            sentiment_risk=50.0,
            liquidity_risk=70.0,  # Assume high liquidity risk
            execution_risk=60.0,
            total_risk_score=80.0,  # High risk due to uncertainty
            risk_category='HIGH',
            risk_factors=['Insufficient data for analysis'],
            risk_warnings=['High risk due to data limitations'],
            timestamp=datetime.now()
        )

    def _error_fallback_position_risk(self, symbol: str, strategy_name: str) -> PositionRisk:
        """Fallback for analysis errors"""
        return PositionRisk(
            symbol=symbol,
            strategy_name=strategy_name,
            volatility=0.35,
            beta=1.0,
            var_1d=0.035,
            var_5d=0.08,
            max_drawdown=0.25,
            strategy_risk_score=70.0,
            confidence_risk=60.0,
            regime_risk=70.0,
            sentiment_risk=60.0,
            liquidity_risk=80.0,
            execution_risk=70.0,
            total_risk_score=85.0,
            risk_category='EXTREME',
            risk_factors=['Risk analysis error'],
            risk_warnings=['EXTREME RISK: Analysis failed'],
            timestamp=datetime.now()
        )

    def _empty_portfolio_risk(self) -> PortfolioRisk:
        """Fallback for empty portfolio"""
        return PortfolioRisk(
            portfolio_volatility=0.0,
            portfolio_var=0.0,
            correlation_risk=0.0,
            concentration_risk=0.0,
            market_regime_risk=0.0,
            sentiment_risk=0.0,
            macro_risk=0.0,
            strategy_concentration=0.0,
            sector_concentration=0.0,
            diversification_score=0.0,
            regime_transition_risk=0.0,
            volatility_regime_risk=0.0,
            liquidity_risk=0.0,
            risk_budget_utilization=0.0,
            margin_of_safety=100.0,
            total_portfolio_risk=0.0,
            risk_level='LOW',
            risk_recommendations=['No positions in portfolio'],
            risk_alerts=[],
            position_adjustments={},
            timestamp=datetime.now()
        )

    def _error_fallback_portfolio_risk(self) -> PortfolioRisk:
        """Fallback for portfolio analysis errors"""
        return PortfolioRisk(
            portfolio_volatility=0.25,
            portfolio_var=0.03,
            correlation_risk=50.0,
            concentration_risk=70.0,
            market_regime_risk=60.0,
            sentiment_risk=60.0,
            macro_risk=50.0,
            strategy_concentration=60.0,
            sector_concentration=70.0,
            diversification_score=30.0,
            regime_transition_risk=50.0,
            volatility_regime_risk=60.0,
            liquidity_risk=60.0,
            risk_budget_utilization=80.0,
            margin_of_safety=20.0,
            total_portfolio_risk=85.0,
            risk_level='CRITICAL',
            risk_recommendations=['URGENT: Portfolio risk analysis failed'],
            risk_alerts=['Portfolio risk analysis error'],
            position_adjustments={},
            timestamp=datetime.now()
        )

    def get_risk_summary(self, position_risk: PositionRisk) -> Dict:
        """Get risk summary for a position"""
        return {
            'symbol': position_risk.symbol,
            'strategy': position_risk.strategy_name,
            'risk_level': position_risk.risk_category,
            'total_risk_score': f"{position_risk.total_risk_score:.0f}/100",
            'volatility': f"{position_risk.volatility:.1%}",
            'var_1d': f"{position_risk.var_1d:.1%}",
            'beta': f"{position_risk.beta:.1f}",
            'key_risks': position_risk.risk_factors[:3],  # Top 3 risks
            'warnings': position_risk.risk_warnings,
            'last_updated': position_risk.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        }

    def get_portfolio_risk_summary(self, portfolio_risk: PortfolioRisk) -> Dict:
        """Get portfolio risk summary"""
        return {
            'risk_level': portfolio_risk.risk_level,
            'total_risk_score': f"{portfolio_risk.total_portfolio_risk:.0f}/100",
            'portfolio_volatility': f"{portfolio_risk.portfolio_volatility:.1%}",
            'portfolio_var': f"{portfolio_risk.portfolio_var:.1%}",
            'diversification_score': f"{portfolio_risk.diversification_score:.0f}/100",
            'risk_budget_used': f"{portfolio_risk.risk_budget_utilization:.0f}%",
            'margin_of_safety': f"{portfolio_risk.margin_of_safety:.0f}%",
            'key_recommendations': portfolio_risk.risk_recommendations[:3],
            'alerts': portfolio_risk.risk_alerts,
            'high_risk_positions': [
                symbol for symbol, action in portfolio_risk.position_adjustments.items()
                if 'REDUCE' in action or 'EXTREME' in action
            ],
            'last_updated': portfolio_risk.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        }


# Convenience function for easy import
def create_risk_analyzer(config: Optional[Dict] = None) -> RiskAnalyzer:
    """Create a risk analyzer instance"""
    return RiskAnalyzer(config)
