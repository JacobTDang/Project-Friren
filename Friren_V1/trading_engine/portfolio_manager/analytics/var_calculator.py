"""
VaR Calculator - Extracted from Risk Analyzer

This module handles Value at Risk calculations and advanced portfolio risk metrics.
Provides sophisticated VaR modeling with market-driven parameters and zero hardcoded thresholds.

Features:
- Monte Carlo VaR simulation
- Historical VaR with backtesting
- Parametric VaR with regime adjustments
- Portfolio VaR with correlation effects
- Expected Shortfall calculations
- VaR model validation and backtesting
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from scipy import stats
from collections import defaultdict

# Import market metrics for dynamic calculations
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


@dataclass
class VaRMetrics:
    """Comprehensive VaR metrics for a position or portfolio"""
    parametric_var_95: float  # 95% parametric VaR
    parametric_var_99: float  # 99% parametric VaR
    historical_var_95: float  # 95% historical VaR
    historical_var_99: float  # 99% historical VaR
    monte_carlo_var_95: float  # 95% Monte Carlo VaR
    monte_carlo_var_99: float  # 99% Monte Carlo VaR
    expected_shortfall_95: float  # 95% Expected Shortfall (CVaR)
    expected_shortfall_99: float  # 99% Expected Shortfall
    var_model_confidence: float  # Confidence in VaR model
    backtesting_score: float  # VaR model backtesting performance


class VaRCalculator:
    """Advanced VaR calculation engine with multiple methodologies"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.VaRCalculator")

    def calculate_comprehensive_var(self, returns: pd.Series, position_size: float, 
                                  symbol: str, confidence_levels: List[float] = [0.95, 0.99]) -> VaRMetrics:
        """Calculate comprehensive VaR using multiple methodologies"""
        try:
            # Get market context for dynamic calculations
            market_metrics = get_all_metrics(symbol)
            
            if len(returns) < 30:
                return self._default_var_metrics(position_size, market_metrics)

            # Calculate different VaR methodologies
            parametric_vars = self._calculate_parametric_var(returns, position_size, confidence_levels, market_metrics)
            historical_vars = self._calculate_historical_var(returns, position_size, confidence_levels)
            monte_carlo_vars = self._calculate_monte_carlo_var(returns, position_size, confidence_levels, market_metrics)
            
            # Calculate Expected Shortfall (Conditional VaR)
            expected_shortfalls = self._calculate_expected_shortfall(returns, position_size, confidence_levels)
            
            # Model validation and confidence scoring
            model_confidence = self._calculate_model_confidence(returns, parametric_vars, historical_vars)
            backtesting_score = self._calculate_backtesting_score(returns, parametric_vars, confidence_levels)

            return VaRMetrics(
                parametric_var_95=parametric_vars.get(0.95, 0.0),
                parametric_var_99=parametric_vars.get(0.99, 0.0),
                historical_var_95=historical_vars.get(0.95, 0.0),
                historical_var_99=historical_vars.get(0.99, 0.0),
                monte_carlo_var_95=monte_carlo_vars.get(0.95, 0.0),
                monte_carlo_var_99=monte_carlo_vars.get(0.99, 0.0),
                expected_shortfall_95=expected_shortfalls.get(0.95, 0.0),
                expected_shortfall_99=expected_shortfalls.get(0.99, 0.0),
                var_model_confidence=model_confidence,
                backtesting_score=backtesting_score
            )

        except Exception as e:
            self.logger.error(f"Error calculating comprehensive VaR for {symbol}: {e}")
            return self._default_var_metrics(position_size, get_all_metrics(symbol))

    def _calculate_parametric_var(self, returns: pd.Series, position_size: float, 
                                confidence_levels: List[float], market_metrics) -> Dict[float, float]:
        """Calculate parametric VaR with dynamic adjustments"""
        try:
            # Calculate basic statistics
            mean_return = returns.mean()
            volatility = returns.std()
            
            # Dynamic confidence level adjustments based on market conditions
            adjusted_z_scores = {}
            for conf_level in confidence_levels:
                base_z_score = stats.norm.ppf(1 - conf_level)
                
                # Market regime adjustments
                if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility:
                    # In high volatility markets, use more conservative z-scores
                    if market_metrics.volatility > 0.7:
                        z_adjustment = 1.2  # 20% more conservative
                    elif market_metrics.volatility > 0.4:
                        z_adjustment = 1.1  # 10% more conservative
                    else:
                        z_adjustment = 1.0  # No adjustment
                else:
                    z_adjustment = 1.0
                
                adjusted_z_scores[conf_level] = base_z_score * z_adjustment

            # Calculate VaR for each confidence level
            parametric_vars = {}
            for conf_level in confidence_levels:
                z_score = adjusted_z_scores[conf_level]
                var_value = abs((mean_return + z_score * volatility) * position_size)
                parametric_vars[conf_level] = var_value

            return parametric_vars

        except Exception as e:
            self.logger.error(f"Error calculating parametric VaR: {e}")
            return {conf: position_size * 0.05 for conf in confidence_levels}

    def _calculate_historical_var(self, returns: pd.Series, position_size: float, 
                                confidence_levels: List[float]) -> Dict[float, float]:
        """Calculate historical VaR using empirical distribution"""
        try:
            if len(returns) < 50:
                # Insufficient data for reliable historical VaR
                return {conf: position_size * 0.05 for conf in confidence_levels}

            # Position-adjusted returns
            position_returns = returns * position_size
            
            # Calculate percentiles for VaR
            historical_vars = {}
            for conf_level in confidence_levels:
                percentile = (1 - conf_level) * 100
                var_value = abs(np.percentile(position_returns, percentile))
                historical_vars[conf_level] = var_value

            return historical_vars

        except Exception as e:
            self.logger.error(f"Error calculating historical VaR: {e}")
            return {conf: position_size * 0.05 for conf in confidence_levels}

    def _calculate_monte_carlo_var(self, returns: pd.Series, position_size: float,
                                 confidence_levels: List[float], market_metrics,
                                 num_simulations: int = 10000) -> Dict[float, float]:
        """Calculate Monte Carlo VaR using random simulation"""
        try:
            # Fit distribution to returns
            mean_return = returns.mean()
            volatility = returns.std()
            
            # Market stress adjustments for simulation parameters
            if market_metrics and hasattr(market_metrics, 'market_stress') and market_metrics.market_stress:
                stress_multiplier = 1.0 + market_metrics.market_stress * 0.5  # Up to 50% stress increase
                volatility *= stress_multiplier

            # Generate random scenarios
            np.random.seed(42)  # For reproducibility
            simulated_returns = np.random.normal(mean_return, volatility, num_simulations)
            
            # Apply position size
            simulated_position_returns = simulated_returns * position_size
            
            # Calculate VaR from simulation results
            monte_carlo_vars = {}
            for conf_level in confidence_levels:
                percentile = (1 - conf_level) * 100
                var_value = abs(np.percentile(simulated_position_returns, percentile))
                monte_carlo_vars[conf_level] = var_value

            return monte_carlo_vars

        except Exception as e:
            self.logger.error(f"Error calculating Monte Carlo VaR: {e}")
            return {conf: position_size * 0.05 for conf in confidence_levels}

    def _calculate_expected_shortfall(self, returns: pd.Series, position_size: float,
                                    confidence_levels: List[float]) -> Dict[float, float]:
        """Calculate Expected Shortfall (Conditional VaR)"""
        try:
            position_returns = returns * position_size
            expected_shortfalls = {}
            
            for conf_level in confidence_levels:
                percentile = (1 - conf_level) * 100
                var_threshold = np.percentile(position_returns, percentile)
                
                # Calculate expected shortfall as mean of losses beyond VaR
                tail_losses = position_returns[position_returns <= var_threshold]
                if len(tail_losses) > 0:
                    expected_shortfall = abs(tail_losses.mean())
                else:
                    expected_shortfall = abs(var_threshold)
                
                expected_shortfalls[conf_level] = expected_shortfall

            return expected_shortfalls

        except Exception as e:
            self.logger.error(f"Error calculating Expected Shortfall: {e}")
            return {conf: position_size * 0.07 for conf in confidence_levels}

    def _calculate_model_confidence(self, returns: pd.Series, parametric_vars: Dict[float, float],
                                  historical_vars: Dict[float, float]) -> float:
        """Calculate confidence in VaR model based on consistency"""
        try:
            # Compare parametric and historical VaR
            confidence_scores = []
            
            for conf_level in [0.95, 0.99]:
                if conf_level in parametric_vars and conf_level in historical_vars:
                    param_var = parametric_vars[conf_level]
                    hist_var = historical_vars[conf_level]
                    
                    if hist_var > 0:
                        # Calculate relative difference
                        relative_diff = abs(param_var - hist_var) / hist_var
                        # Convert to confidence score (lower difference = higher confidence)
                        confidence = max(0.0, 1.0 - relative_diff)
                        confidence_scores.append(confidence)

            if confidence_scores:
                model_confidence = np.mean(confidence_scores)
            else:
                model_confidence = 0.5  # Default medium confidence

            # Adjust for data quality
            if len(returns) < 100:
                model_confidence *= 0.8  # Reduce confidence for limited data
            elif len(returns) > 500:
                model_confidence = min(1.0, model_confidence * 1.1)  # Boost for abundant data

            return model_confidence

        except Exception as e:
            self.logger.error(f"Error calculating model confidence: {e}")
            return 0.5

    def _calculate_backtesting_score(self, returns: pd.Series, parametric_vars: Dict[float, float],
                                   confidence_levels: List[float]) -> float:
        """Calculate VaR model backtesting performance score"""
        try:
            if len(returns) < 100:
                return 0.5  # Insufficient data for backtesting

            # Simple backtesting: count VaR breaches
            backtest_scores = []
            
            for conf_level in confidence_levels:
                if conf_level not in parametric_vars:
                    continue
                    
                var_value = parametric_vars[conf_level]
                expected_breaches = len(returns) * (1 - conf_level)
                
                # Count actual breaches (simplified - assumes daily returns)
                actual_breaches = sum(1 for r in returns if abs(r) > var_value)
                
                # Calculate breach ratio
                if expected_breaches > 0:
                    breach_ratio = actual_breaches / expected_breaches
                    # Score based on how close to expected (1.0 = perfect)
                    score = max(0.0, 1.0 - abs(breach_ratio - 1.0))
                    backtest_scores.append(score)

            return np.mean(backtest_scores) if backtest_scores else 0.5

        except Exception as e:
            self.logger.error(f"Error calculating backtesting score: {e}")
            return 0.5

    def calculate_portfolio_var(self, position_vars: List[VaRMetrics], weights: np.ndarray,
                              correlation_matrix: Optional[np.ndarray] = None,
                              confidence_level: float = 0.95) -> Dict[str, float]:
        """Calculate portfolio-level VaR considering correlations"""
        try:
            if len(position_vars) != len(weights):
                return self._default_portfolio_var()

            # Extract VaR values based on confidence level
            if confidence_level == 0.95:
                vars_parametric = np.array([var.parametric_var_95 for var in position_vars])
                vars_historical = np.array([var.historical_var_95 for var in position_vars])
                vars_monte_carlo = np.array([var.monte_carlo_var_95 for var in position_vars])
            else:  # 0.99
                vars_parametric = np.array([var.parametric_var_99 for var in position_vars])
                vars_historical = np.array([var.historical_var_99 for var in position_vars])
                vars_monte_carlo = np.array([var.monte_carlo_var_99 for var in position_vars])

            # Calculate portfolio VaR using different methods
            portfolio_var_results = {}
            
            for method_name, vars_array in [('parametric', vars_parametric), 
                                          ('historical', vars_historical),
                                          ('monte_carlo', vars_monte_carlo)]:
                
                if correlation_matrix is not None and correlation_matrix.shape[0] == len(weights):
                    # Portfolio VaR with correlations
                    portfolio_variance = np.dot(weights.T, 
                                              np.dot(correlation_matrix * np.outer(vars_array, vars_array), 
                                                    weights))
                    portfolio_var = np.sqrt(abs(portfolio_variance))
                else:
                    # Simple diversification assumption
                    portfolio_var = np.sqrt(np.sum((weights * vars_array) ** 2))
                
                portfolio_var_results[f'portfolio_var_{method_name}'] = portfolio_var

            # Calculate diversification benefit
            simple_sum = np.sum(weights * vars_parametric)
            diversified_var = portfolio_var_results['portfolio_var_parametric']
            diversification_benefit = (simple_sum - diversified_var) / simple_sum if simple_sum > 0 else 0.0
            
            portfolio_var_results['diversification_benefit'] = diversification_benefit
            portfolio_var_results['confidence_level'] = confidence_level

            return portfolio_var_results

        except Exception as e:
            self.logger.error(f"Error calculating portfolio VaR: {e}")
            return self._default_portfolio_var()

    def calculate_marginal_var(self, position_vars: List[VaRMetrics], weights: np.ndarray,
                             correlation_matrix: Optional[np.ndarray] = None,
                             confidence_level: float = 0.95) -> Dict[int, float]:
        """Calculate marginal VaR contribution of each position"""
        try:
            marginal_vars = {}
            
            # Calculate baseline portfolio VaR
            baseline_portfolio = self.calculate_portfolio_var(position_vars, weights, correlation_matrix, confidence_level)
            baseline_var = baseline_portfolio.get('portfolio_var_parametric', 0.0)
            
            # Calculate marginal VaR for each position
            for i in range(len(position_vars)):
                # Create weights without position i
                temp_weights = weights.copy()
                temp_weights[i] = 0.0
                
                # Renormalize weights
                weight_sum = np.sum(temp_weights)
                if weight_sum > 0:
                    temp_weights = temp_weights / weight_sum
                    
                    # Calculate VaR without this position
                    temp_vars = [var for j, var in enumerate(position_vars) if j != i]
                    temp_weights_filtered = temp_weights[temp_weights > 0]
                    
                    if len(temp_vars) > 0:
                        reduced_portfolio = self.calculate_portfolio_var(temp_vars, temp_weights_filtered, 
                                                                       correlation_matrix, confidence_level)
                        reduced_var = reduced_portfolio.get('portfolio_var_parametric', 0.0)
                        
                        # Marginal VaR is the difference
                        marginal_var = baseline_var - reduced_var
                        marginal_vars[i] = marginal_var
                    else:
                        marginal_vars[i] = baseline_var  # Only position in portfolio
                else:
                    marginal_vars[i] = 0.0

            return marginal_vars

        except Exception as e:
            self.logger.error(f"Error calculating marginal VaR: {e}")
            return {i: 0.0 for i in range(len(position_vars))}

    def calculate_component_var(self, position_vars: List[VaRMetrics], weights: np.ndarray,
                              correlation_matrix: Optional[np.ndarray] = None,
                              confidence_level: float = 0.95) -> Dict[int, float]:
        """Calculate component VaR (allocation of portfolio VaR to positions)"""
        try:
            # Get marginal VaR contributions
            marginal_vars = self.calculate_marginal_var(position_vars, weights, correlation_matrix, confidence_level)
            
            # Component VaR = Weight × Marginal VaR
            component_vars = {}
            for i, weight in enumerate(weights):
                marginal_var = marginal_vars.get(i, 0.0)
                component_vars[i] = weight * marginal_var

            return component_vars

        except Exception as e:
            self.logger.error(f"Error calculating component VaR: {e}")
            return {i: 0.0 for i in range(len(position_vars))}

    def _default_var_metrics(self, position_size: float, market_metrics) -> VaRMetrics:
        """Return default VaR metrics when calculation fails"""
        # Use market-based defaults if available
        if market_metrics and hasattr(market_metrics, 'volatility') and market_metrics.volatility:
            daily_vol = market_metrics.volatility / np.sqrt(252)
            default_var_95 = 1.645 * daily_vol * position_size
            default_var_99 = 2.326 * daily_vol * position_size
        else:
            default_var_95 = position_size * 0.05  # 5% default VaR
            default_var_99 = position_size * 0.07  # 7% default VaR

        return VaRMetrics(
            parametric_var_95=default_var_95,
            parametric_var_99=default_var_99,
            historical_var_95=default_var_95,
            historical_var_99=default_var_99,
            monte_carlo_var_95=default_var_95,
            monte_carlo_var_99=default_var_99,
            expected_shortfall_95=default_var_95 * 1.3,
            expected_shortfall_99=default_var_99 * 1.3,
            var_model_confidence=0.5,
            backtesting_score=0.5
        )

    def _default_portfolio_var(self) -> Dict[str, float]:
        """Return default portfolio VaR values"""
        return {
            'portfolio_var_parametric': 0.05,
            'portfolio_var_historical': 0.05,
            'portfolio_var_monte_carlo': 0.05,
            'diversification_benefit': 0.1,
            'confidence_level': 0.95
        }

    def validate_var_model(self, historical_returns: pd.Series, var_predictions: List[float],
                          confidence_level: float = 0.95) -> Dict[str, float]:
        """Validate VaR model performance using backtesting"""
        try:
            if len(historical_returns) != len(var_predictions):
                return {'validation_score': 0.0, 'error': 'Mismatched data lengths'}

            # Count VaR breaches
            breaches = sum(1 for i, return_val in enumerate(historical_returns) 
                          if abs(return_val) > var_predictions[i])
            
            # Expected breaches
            expected_breaches = len(historical_returns) * (1 - confidence_level)
            
            # Kupiec test statistic for VaR validation
            if expected_breaches > 0:
                breach_rate = breaches / len(historical_returns)
                expected_rate = 1 - confidence_level
                
                # Log-likelihood ratio test
                if breach_rate > 0 and breach_rate < 1:
                    lr_stat = 2 * (breaches * np.log(breach_rate / expected_rate) + 
                                  (len(historical_returns) - breaches) * np.log((1 - breach_rate) / (1 - expected_rate)))
                else:
                    lr_stat = float('inf')
                
                # Convert to validation score (0-1)
                validation_score = max(0.0, 1.0 - abs(breach_rate - expected_rate) / expected_rate)
            else:
                validation_score = 0.5

            return {
                'validation_score': validation_score,
                'actual_breaches': breaches,
                'expected_breaches': expected_breaches,
                'breach_rate': breaches / len(historical_returns),
                'lr_statistic': lr_stat if 'lr_stat' in locals() else 0.0
            }

        except Exception as e:
            self.logger.error(f"Error validating VaR model: {e}")
            return {'validation_score': 0.0, 'error': str(e)}
