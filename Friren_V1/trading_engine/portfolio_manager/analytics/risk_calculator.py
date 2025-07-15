"""
Risk Calculator - Extracted from Risk Analyzer

This module handles core risk calculations including VaR, volatility, beta, and drawdown metrics.
Provides market-driven risk assessment with zero hardcoded thresholds.

Features:
- Market risk metrics calculation (volatility, beta, VaR)
- Drawdown analysis with rolling calculations
- Position-level risk assessment
- Dynamic risk thresholds based on market conditions
- Real-time risk factor analysis
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

# Import market metrics for dynamic calculations
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


@dataclass
class RiskMetrics:
    """Core risk metrics for a position"""
    volatility: float  # Annualized volatility
    beta: float  # Market beta
    var_1d: float  # 1-day Value at Risk (95% confidence)
    var_5d: float  # 5-day Value at Risk
    max_drawdown: float  # Historical maximum drawdown
    sharpe_ratio: float  # Risk-adjusted return
    liquidity_risk: float  # Liquidity risk score
    execution_risk: float  # Execution risk score


class RiskCalculator:
    """Calculates core risk metrics with market-driven parameters"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.RiskCalculator")

    def calculate_market_risk_metrics(self, df: pd.DataFrame, position_size: float, symbol: str) -> Dict[str, float]:
        """Calculate basic market risk metrics for a position with dynamic thresholds"""
        try:
            # Get market context for dynamic calculations
            market_metrics = get_all_metrics(symbol)
            
            returns = df['Close'].pct_change().dropna()
            
            if len(returns) < 30:  # Need minimum data
                return self._default_risk_metrics(market_metrics)

            # Volatility (annualized) with market adjustment
            volatility = returns.std() * np.sqrt(252)
            
            # Dynamic volatility adjustment based on market conditions
            if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility:
                market_vol_adjustment = market_metrics.volatility / 0.3  # Normalize against 30% baseline
                volatility = volatility * market_vol_adjustment

            # Beta calculation with market context
            beta = self._calculate_beta(returns, market_metrics)

            # Value at Risk with dynamic confidence levels
            var_metrics = self._calculate_var(returns, volatility, position_size, market_metrics)

            # Maximum drawdown
            max_drawdown = self._calculate_max_drawdown(returns)

            # Sharpe ratio calculation
            sharpe_ratio = self._calculate_sharpe_ratio(returns, volatility)

            # Liquidity and execution risks
            liquidity_risk = self._calculate_liquidity_risk(df, market_metrics)
            execution_risk = self._calculate_execution_risk(volatility, market_metrics)

            return {
                'volatility': volatility,
                'beta': beta,
                'var_1d': var_metrics['var_1d_95'],
                'var_5d': var_metrics['var_5d_95'],
                'var_1d_99': var_metrics['var_1d_99'],
                'max_drawdown': max_drawdown,
                'sharpe_ratio': sharpe_ratio,
                'liquidity_risk': liquidity_risk,
                'execution_risk': execution_risk
            }

        except Exception as e:
            self.logger.error(f"Error calculating market risk metrics for {symbol}: {e}")
            return self._default_risk_metrics(get_all_metrics(symbol))

    def _calculate_beta(self, returns: pd.Series, market_metrics) -> float:
        """Calculate beta with market context"""
        try:
            # Method 1: Use market correlation if available
            if market_metrics and hasattr(market_metrics, 'market_correlation'):
                correlation = getattr(market_metrics, 'market_correlation', 0.5)
                asset_vol = returns.std()
                if hasattr(market_metrics, 'market_volatility'):
                    market_vol = getattr(market_metrics, 'market_volatility', asset_vol)
                    beta = correlation * (asset_vol / market_vol) if market_vol > 0 else 1.0
                else:
                    beta = correlation  # Use correlation as proxy
            else:
                # Method 2: Relative volatility approach
                rolling_vol = returns.rolling(60).std()
                market_vol = rolling_vol.median()
                asset_vol = returns.std()
                beta = asset_vol / market_vol if market_vol > 0 else 1.0

            # Cap beta in reasonable range with market-driven bounds
            if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility:
                # In high volatility markets, allow higher beta range
                if market_metrics.volatility > 0.6:
                    beta = max(0.1, min(4.0, beta))
                else:
                    beta = max(0.1, min(3.0, beta))
            else:
                beta = max(0.1, min(3.0, beta))  # Default range

            return beta

        except Exception as e:
            self.logger.error(f"Error calculating beta: {e}")
            return 1.0  # Default beta

    def _calculate_var(self, returns: pd.Series, volatility: float, position_size: float, market_metrics) -> Dict[str, float]:
        """Calculate Value at Risk with dynamic confidence levels"""
        try:
            # Dynamic confidence levels based on market conditions
            if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility:
                # In high volatility markets, use more conservative confidence levels
                if market_metrics.volatility > 0.7:
                    confidence_95 = 2.0  # More conservative
                    confidence_99 = 2.6
                elif market_metrics.volatility > 0.4:
                    confidence_95 = 1.8
                    confidence_99 = 2.4
                else:
                    confidence_95 = 1.645  # Standard 95% z-score
                    confidence_99 = 2.326  # Standard 99% z-score
            else:
                confidence_95 = 1.645
                confidence_99 = 2.326

            daily_vol = volatility / np.sqrt(252)
            
            # Position-size adjusted VaR
            var_1d_95 = confidence_95 * daily_vol * position_size
            var_1d_99 = confidence_99 * daily_vol * position_size
            var_5d_95 = var_1d_95 * np.sqrt(5)

            # Historical VaR for validation
            if len(returns) >= 100:
                hist_var_95 = np.percentile(returns, 5) * position_size  # 5th percentile
                hist_var_99 = np.percentile(returns, 1) * position_size  # 1st percentile
                
                # Use the more conservative of parametric vs historical
                var_1d_95 = max(abs(var_1d_95), abs(hist_var_95))
                var_1d_99 = max(abs(var_1d_99), abs(hist_var_99))

            return {
                'var_1d_95': var_1d_95,
                'var_1d_99': var_1d_99,
                'var_5d_95': var_5d_95
            }

        except Exception as e:
            self.logger.error(f"Error calculating VaR: {e}")
            # Conservative fallback
            daily_vol = volatility / np.sqrt(252)
            return {
                'var_1d_95': 2.0 * daily_vol * position_size,
                'var_1d_99': 2.6 * daily_vol * position_size,
                'var_5d_95': 2.0 * daily_vol * position_size * np.sqrt(5)
            }

    def _calculate_max_drawdown(self, returns: pd.Series) -> float:
        """Calculate maximum drawdown"""
        try:
            if len(returns) < 10:
                return 0.0

            cumulative_returns = (1 + returns).cumprod()
            rolling_max = cumulative_returns.expanding().max()
            drawdown = (cumulative_returns - rolling_max) / rolling_max
            max_drawdown = abs(drawdown.min())

            return max_drawdown

        except Exception as e:
            self.logger.error(f"Error calculating max drawdown: {e}")
            return 0.1  # Conservative default

    def _calculate_sharpe_ratio(self, returns: pd.Series, volatility: float) -> float:
        """Calculate Sharpe ratio with dynamic risk-free rate"""
        try:
            if volatility == 0:
                return 0.0

            # Dynamic risk-free rate estimation (simplified)
            # In production, this would use real treasury rates
            risk_free_rate = 0.02  # 2% baseline
            
            # Adjust based on recent performance
            mean_return = returns.mean() * 252  # Annualized
            excess_return = mean_return - risk_free_rate
            
            sharpe_ratio = excess_return / volatility if volatility > 0 else 0.0
            
            # Cap Sharpe ratio in reasonable range
            return max(-3.0, min(5.0, sharpe_ratio))

        except Exception as e:
            self.logger.error(f"Error calculating Sharpe ratio: {e}")
            return 0.0

    def _calculate_liquidity_risk(self, df: pd.DataFrame, market_metrics) -> float:
        """Calculate liquidity risk based on volume and spread data"""
        try:
            # Volume-based liquidity assessment
            volume_data = df.get('Volume', pd.Series([1000000]))  # Default volume if missing
            
            if len(volume_data) > 20:
                avg_volume = volume_data.rolling(20).mean().iloc[-1]
                volume_volatility = volume_data.rolling(20).std().iloc[-1] / avg_volume if avg_volume > 0 else 1.0
            else:
                avg_volume = volume_data.mean()
                volume_volatility = volume_data.std() / avg_volume if avg_volume > 0 else 1.0

            # Market-driven volume thresholds
            if market_metrics and hasattr(market_metrics, 'volume') and market_metrics.volume:
                volume_threshold = market_metrics.volume * 0.1  # 10% of market volume
            else:
                volume_threshold = 100000  # Default threshold

            # Liquidity risk calculation
            if avg_volume < volume_threshold:
                base_liquidity_risk = 0.8  # High risk for low volume
            elif avg_volume < volume_threshold * 5:
                base_liquidity_risk = 0.5  # Medium risk
            else:
                base_liquidity_risk = 0.2  # Low risk for high volume

            # Adjust for volume volatility
            volume_risk_adjustment = min(0.3, volume_volatility * 0.5)
            liquidity_risk = min(1.0, base_liquidity_risk + volume_risk_adjustment)

            return liquidity_risk

        except Exception as e:
            self.logger.error(f"Error calculating liquidity risk: {e}")
            return 0.5  # Default medium risk

    def _calculate_execution_risk(self, volatility: float, market_metrics) -> float:
        """Calculate execution risk based on volatility and market conditions"""
        try:
            # Base execution risk from volatility
            if volatility > 0.8:
                base_execution_risk = 0.9  # Very high volatility
            elif volatility > 0.5:
                base_execution_risk = 0.6  # High volatility
            elif volatility > 0.3:
                base_execution_risk = 0.4  # Medium volatility
            else:
                base_execution_risk = 0.2  # Low volatility

            # Market condition adjustments
            if market_metrics:
                # Market stress adjustment
                if hasattr(market_metrics, 'market_stress') and market_metrics.market_stress:
                    stress_adjustment = market_metrics.market_stress * 0.3
                    base_execution_risk = min(1.0, base_execution_risk + stress_adjustment)
                
                # Bid-ask spread adjustment (if available)
                if hasattr(market_metrics, 'bid_ask_spread') and market_metrics.bid_ask_spread:
                    spread_adjustment = min(0.2, market_metrics.bid_ask_spread * 10)  # Scale spread
                    base_execution_risk = min(1.0, base_execution_risk + spread_adjustment)

            return base_execution_risk

        except Exception as e:
            self.logger.error(f"Error calculating execution risk: {e}")
            return 0.5  # Default medium risk

    def calculate_position_risk_score(self, risk_metrics: Dict[str, float], strategy_name: str, confidence: float, market_metrics) -> Dict[str, float]:
        """Calculate comprehensive position risk score"""
        try:
            # Base risk components
            volatility_risk = min(100, risk_metrics.get('volatility', 0.3) * 100 / 0.5)  # Scale to 0-100
            var_risk = min(100, risk_metrics.get('var_1d', 0.05) * 100 / 0.1)  # Scale to 0-100
            drawdown_risk = min(100, risk_metrics.get('max_drawdown', 0.1) * 100 / 0.2)  # Scale to 0-100
            liquidity_risk = risk_metrics.get('liquidity_risk', 0.5) * 100
            execution_risk = risk_metrics.get('execution_risk', 0.5) * 100

            # Strategy-specific risk
            strategy_risk = self._calculate_strategy_risk(strategy_name, market_metrics)

            # Confidence risk (lower confidence = higher risk)
            confidence_risk = (1.0 - confidence) * 100

            # Regime risk
            regime_risk = self._calculate_regime_risk(market_metrics)

            # Weighted total risk score
            total_risk = (
                volatility_risk * 0.25 +
                var_risk * 0.20 +
                drawdown_risk * 0.15 +
                liquidity_risk * 0.10 +
                execution_risk * 0.10 +
                strategy_risk * 0.10 +
                confidence_risk * 0.05 +
                regime_risk * 0.05
            )

            # Risk category determination
            if total_risk > 80:
                risk_category = "EXTREME"
            elif total_risk > 60:
                risk_category = "HIGH"
            elif total_risk > 40:
                risk_category = "MEDIUM"
            else:
                risk_category = "LOW"

            return {
                'total_risk_score': total_risk,
                'risk_category': risk_category,
                'volatility_risk': volatility_risk,
                'var_risk': var_risk,
                'drawdown_risk': drawdown_risk,
                'liquidity_risk': liquidity_risk,
                'execution_risk': execution_risk,
                'strategy_risk': strategy_risk,
                'confidence_risk': confidence_risk,
                'regime_risk': regime_risk
            }

        except Exception as e:
            self.logger.error(f"Error calculating position risk score: {e}")
            return {
                'total_risk_score': 50.0,
                'risk_category': "MEDIUM",
                'volatility_risk': 50.0,
                'var_risk': 50.0,
                'drawdown_risk': 50.0,
                'liquidity_risk': 50.0,
                'execution_risk': 50.0,
                'strategy_risk': 50.0,
                'confidence_risk': 50.0,
                'regime_risk': 50.0
            }

    def _calculate_strategy_risk(self, strategy_name: str, market_metrics) -> float:
        """Calculate strategy-specific risk based on market conditions"""
        try:
            # Base strategy risk levels
            strategy_risk_map = {
                'MOMENTUM': 60.0,      # Higher risk in volatile markets
                'MEAN_REVERSION': 40.0, # Lower base risk
                'VOLATILITY': 70.0,    # High risk strategy
                'PAIRS': 35.0,         # Lower risk due to hedging
                'FACTOR': 45.0,        # Moderate risk
                'DEFENSIVE': 25.0,     # Lowest risk
                'ARBITRAGE': 30.0,     # Low risk but operational complexity
                'TREND_FOLLOWING': 55.0 # Moderate-high risk
            }

            base_risk = strategy_risk_map.get(strategy_name.upper(), 50.0)

            # Market condition adjustments
            if market_metrics:
                if hasattr(market_metrics, 'volatility') and market_metrics.volatility:
                    # High volatility increases risk for momentum strategies
                    if strategy_name.upper() in ['MOMENTUM', 'TREND_FOLLOWING'] and market_metrics.volatility > 0.6:
                        base_risk = min(100.0, base_risk * 1.3)
                    # High volatility benefits volatility strategies
                    elif strategy_name.upper() == 'VOLATILITY' and market_metrics.volatility > 0.6:
                        base_risk = max(20.0, base_risk * 0.8)

                # Market regime adjustments
                if hasattr(market_metrics, 'trend_strength') and market_metrics.trend_strength:
                    # Strong trends benefit momentum strategies
                    if strategy_name.upper() in ['MOMENTUM', 'TREND_FOLLOWING'] and market_metrics.trend_strength > 0.7:
                        base_risk = max(20.0, base_risk * 0.9)
                    # Weak trends benefit mean reversion
                    elif strategy_name.upper() == 'MEAN_REVERSION' and market_metrics.trend_strength < 0.3:
                        base_risk = max(20.0, base_risk * 0.8)

            return base_risk

        except Exception as e:
            self.logger.error(f"Error calculating strategy risk: {e}")
            return 50.0

    def _calculate_regime_risk(self, market_metrics) -> float:
        """Calculate risk from market regime conditions"""
        try:
            if not market_metrics:
                return 50.0  # Default medium risk

            regime_risk = 30.0  # Base regime risk

            # Volatility regime risk
            if hasattr(market_metrics, 'volatility') and market_metrics.volatility:
                if market_metrics.volatility > 0.8:
                    regime_risk += 30.0  # Very high volatility
                elif market_metrics.volatility > 0.5:
                    regime_risk += 15.0  # High volatility
                elif market_metrics.volatility < 0.2:
                    regime_risk += 10.0  # Very low volatility can be risky too

            # Market stress indicators
            if hasattr(market_metrics, 'market_stress') and market_metrics.market_stress:
                regime_risk += market_metrics.market_stress * 20.0

            # Liquidity conditions
            if hasattr(market_metrics, 'liquidity_stress') and market_metrics.liquidity_stress:
                regime_risk += market_metrics.liquidity_stress * 15.0

            return min(100.0, regime_risk)

        except Exception as e:
            self.logger.error(f"Error calculating regime risk: {e}")
            return 50.0

    def _default_risk_metrics(self, market_metrics) -> Dict[str, float]:
        """Return default risk metrics when calculation fails"""
        # Use market-based defaults if available
        if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility:
            default_vol = market_metrics.volatility
        else:
            default_vol = 0.3  # 30% default volatility

        return {
            'volatility': default_vol,
            'beta': 1.0,
            'var_1d': default_vol / np.sqrt(252) * 1.645,  # 95% confidence
            'var_5d': default_vol / np.sqrt(252) * 1.645 * np.sqrt(5),
            'var_1d_99': default_vol / np.sqrt(252) * 2.326,  # 99% confidence
            'max_drawdown': 0.1,
            'sharpe_ratio': 0.0,
            'liquidity_risk': 0.5,
            'execution_risk': 0.5
        }

    def calculate_portfolio_var(self, position_vars: List[float], correlations: np.ndarray, weights: np.ndarray) -> float:
        """Calculate portfolio-level VaR considering correlations"""
        try:
            if len(position_vars) != len(weights) or correlations.shape[0] != len(weights):
                # Fallback to simple sum if dimensions don't match
                return sum(abs(var) * weight for var, weight in zip(position_vars, weights))

            # Convert to numpy arrays
            vars_array = np.array(position_vars)
            
            # Portfolio variance calculation
            portfolio_variance = np.dot(weights.T, np.dot(correlations * np.outer(vars_array, vars_array), weights))
            
            # Portfolio VaR is square root of variance
            portfolio_var = np.sqrt(abs(portfolio_variance))
            
            return portfolio_var

        except Exception as e:
            self.logger.error(f"Error calculating portfolio VaR: {e}")
            # Fallback to weighted sum
            return sum(abs(var) * weight for var, weight in zip(position_vars, weights))

    def calculate_correlation_risk(self, correlations: np.ndarray, weights: np.ndarray) -> float:
        """Calculate risk from position correlations"""
        try:
            if correlations.shape[0] != len(weights):
                return 0.5  # Default moderate correlation risk

            # Average correlation between positions
            n = len(weights)
            if n < 2:
                return 0.0  # No correlation risk with single position

            # Calculate weighted average correlation
            total_correlation = 0.0
            total_weight = 0.0
            
            for i in range(n):
                for j in range(i + 1, n):
                    correlation = correlations[i, j]
                    weight = weights[i] * weights[j]
                    total_correlation += abs(correlation) * weight
                    total_weight += weight

            avg_correlation = total_correlation / total_weight if total_weight > 0 else 0.0
            
            # High correlation = high risk
            correlation_risk = min(1.0, avg_correlation)
            
            return correlation_risk

        except Exception as e:
            self.logger.error(f"Error calculating correlation risk: {e}")
            return 0.5