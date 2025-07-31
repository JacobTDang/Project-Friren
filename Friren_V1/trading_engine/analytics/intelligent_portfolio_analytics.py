"""
Intelligent Portfolio Analytics Module for Friren Trading System
===============================================================

Implements sophisticated portfolio-level analytics including advanced risk
attribution, performance attribution, dynamic optimization, and intelligent
rebalancing algorithms. This module provides intelligent portfolio management
beyond traditional metrics.


"""

import numpy as np
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import statistics
from collections import deque
from scipy import optimize
from scipy.stats import norm


@dataclass
class PortfolioRiskAttribution:
    """Advanced portfolio risk attribution analysis"""

    # Total portfolio risk decomposition
    total_portfolio_var: float
    marginal_var: Dict[str, float]        # Marginal VaR by position
    component_var: Dict[str, float]       # Component VaR by position
    concentration_index: float            # Herfindahl index for concentration

    # Risk factor decomposition
    systematic_risk: float                # Market/factor risk
    idiosyncratic_risk: float            # Stock-specific risk
    correlation_risk: float              # Risk from correlations
    liquidity_risk: float                # Liquidity-related risk

    # Dynamic risk metrics
    conditional_var: float               # Expected Shortfall (CVaR)
    stressed_var: float                  # VaR under stress conditions
    tail_expectation: float              # Expected loss in tail events

    # Risk trend analysis
    risk_momentum: str                   # INCREASING/DECREASING/STABLE
    risk_regime_probability: Dict[str, float] # Probability of different risk regimes

    timestamp: datetime


@dataclass
class PerformanceAttribution:
    """Advanced performance attribution analysis"""

    # Return decomposition
    total_return: float
    benchmark_return: float
    active_return: float                 # Total - Benchmark

    # Alpha decomposition
    selection_alpha: Dict[str, float]    # Stock selection contribution by position
    timing_alpha: float                  # Market timing contribution
    interaction_alpha: float            # Selection-timing interaction

    # Factor attribution
    factor_returns: Dict[str, float]     # Returns from various factors
    factor_exposures: Dict[str, float]   # Current factor exposures
    factor_attribution: Dict[str, float] # Return attribution to factors

    # Risk-adjusted metrics
    information_ratio: float             # Active return / tracking error
    sharpe_ratio: float                  # Risk-adjusted return
    sortino_ratio: float                 # Downside risk-adjusted return
    calmar_ratio: float                  # Return / max drawdown

    # Dynamic performance metrics
    rolling_alpha: List[float]           # Rolling alpha over time
    alpha_stability: float               # Consistency of alpha generation
    performance_persistence: float      # Performance persistence score

    timestamp: datetime


@dataclass
class OptimizationResult:
    """Portfolio optimization result"""

    # Optimal weights
    optimal_weights: Dict[str, float]
    current_weights: Dict[str, float]
    weight_changes: Dict[str, float]

    # Optimization metrics
    expected_return: float
    expected_volatility: float
    expected_sharpe: float

    # Risk metrics
    portfolio_var: float
    max_component_risk: float
    concentration_score: float

    # Trade requirements
    required_trades: Dict[str, int]      # Shares to trade
    trade_costs: Dict[str, float]        # Estimated trade costs
    total_turnover: float                # Portfolio turnover

    # Constraints applied
    constraints_active: List[str]
    optimization_status: str

    timestamp: datetime


class RiskModelEngine:
    """Advanced risk modeling for portfolio analytics"""

    @staticmethod
    def calculate_portfolio_var(weights: Dict[str, float], returns_data: pd.DataFrame,
                              confidence_level: float = 0.05, horizon_days: int = 1) -> Tuple[float, Dict[str, float]]:
        """
        Calculate portfolio Value at Risk with component attribution

        Returns:
            (portfolio_var, component_var_dict)
        """
        try:
            # Align data
            symbols = list(weights.keys())
            available_symbols = [s for s in symbols if s in returns_data.columns]

            if not available_symbols:
                return 0.0, {}

            # Get returns and weights for available symbols
            returns = returns_data[available_symbols].dropna()
            w = np.array([weights[s] for s in available_symbols])

            if len(returns) < 30:  # Need sufficient data
                return 0.0, {}

            # Calculate covariance matrix
            cov_matrix = returns.cov().values * 252  # Annualized

            # Portfolio variance
            portfolio_variance = np.dot(w.T, np.dot(cov_matrix, w))
            portfolio_vol = np.sqrt(portfolio_variance)

            # Portfolio VaR (parametric approach)
            portfolio_var = portfolio_vol * norm.ppf(confidence_level) * np.sqrt(horizon_days)

            # Component VaR calculation
            marginal_var = np.dot(cov_matrix, w) / portfolio_vol * norm.ppf(confidence_level) * np.sqrt(horizon_days)
            component_var = w * marginal_var

            # Create component VaR dictionary
            component_var_dict = {symbol: component_var[i] for i, symbol in enumerate(available_symbols)}

            return abs(portfolio_var), component_var_dict

        except Exception as e:
            logging.getLogger(__name__).error(f"Error calculating portfolio VaR: {e}")
            return 0.0, {}

    @staticmethod
    def calculate_conditional_var(weights: Dict[str, float], returns_data: pd.DataFrame,
                                confidence_level: float = 0.05) -> float:
        """Calculate Conditional Value at Risk (Expected Shortfall)"""
        try:
            symbols = list(weights.keys())
            available_symbols = [s for s in symbols if s in returns_data.columns]

            if not available_symbols:
                return 0.0

            returns = returns_data[available_symbols].dropna()
            w = np.array([weights[s] for s in available_symbols])

            # Portfolio returns
            portfolio_returns = returns.dot(w)

            # Calculate CVaR
            var_threshold = portfolio_returns.quantile(confidence_level)
            tail_returns = portfolio_returns[portfolio_returns <= var_threshold]

            if len(tail_returns) > 0:
                conditional_var = -tail_returns.mean()
            else:
                conditional_var = abs(var_threshold)

            return conditional_var

        except Exception as e:
            logging.getLogger(__name__).error(f"Error calculating CVaR: {e}")
            return 0.0

    @staticmethod
    def calculate_stressed_var(weights: Dict[str, float], returns_data: pd.DataFrame,
                             stress_factor: float = 2.0) -> float:
        """Calculate VaR under stressed conditions"""
        try:
            normal_var, _ = RiskModelEngine.calculate_portfolio_var(weights, returns_data)
            return normal_var * stress_factor
        except Exception:
            return 0.0


class PerformanceAttributionEngine:
    """Advanced performance attribution analysis"""

    @staticmethod
    def calculate_brinson_attribution(portfolio_weights: Dict[str, float],
                                    benchmark_weights: Dict[str, float],
                                    portfolio_returns: Dict[str, float],
                                    benchmark_returns: Dict[str, float]) -> Dict[str, float]:
        """
        Calculate Brinson-style performance attribution

        Returns:
            Dictionary with attribution components
        """
        try:
            attribution = {
                'allocation_effect': 0.0,
                'selection_effect': 0.0,
                'interaction_effect': 0.0,
                'total_effect': 0.0
            }

            symbols = set(portfolio_weights.keys()) | set(benchmark_weights.keys())

            for symbol in symbols:
                pw = portfolio_weights.get(symbol, 0.0)
                bw = benchmark_weights.get(symbol, 0.0)
                pr = portfolio_returns.get(symbol, 0.0)
                br = benchmark_returns.get(symbol, 0.0)

                # Benchmark average return
                avg_benchmark_return = np.mean(list(benchmark_returns.values()))

                # Attribution effects
                allocation = (pw - bw) * (br - avg_benchmark_return)
                selection = bw * (pr - br)
                interaction = (pw - bw) * (pr - br)

                attribution['allocation_effect'] += allocation
                attribution['selection_effect'] += selection
                attribution['interaction_effect'] += interaction

            attribution['total_effect'] = (
                attribution['allocation_effect'] +
                attribution['selection_effect'] +
                attribution['interaction_effect']
            )

            return attribution

        except Exception as e:
            logging.getLogger(__name__).error(f"Error in Brinson attribution: {e}")
            return {'allocation_effect': 0.0, 'selection_effect': 0.0, 'interaction_effect': 0.0, 'total_effect': 0.0}


class PortfolioOptimizer:
    """Advanced portfolio optimization engine"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def optimize_portfolio(self, expected_returns: Dict[str, float],
                          returns_data: pd.DataFrame,
                          current_weights: Dict[str, float],
                          constraints: Dict[str, Any] = None) -> OptimizationResult:
        """
        Optimize portfolio using advanced techniques

        Args:
            expected_returns: Expected returns by symbol
            returns_data: Historical returns data
            current_weights: Current portfolio weights
            constraints: Optimization constraints

        Returns:
            OptimizationResult with optimal allocation
        """
        try:
            # Default constraints
            if constraints is None:
                constraints = {
                    'max_weight': 0.15,      # Maximum 15% in any position
                    'min_weight': 0.0,       # No short selling
                    'max_turnover': 0.5,     # Maximum 50% turnover
                    'target_return': None,   # No target return constraint
                    'max_risk': 0.20        # Maximum 20% portfolio volatility
                }

            # Get available symbols
            symbols = list(expected_returns.keys())
            available_symbols = [s for s in symbols if s in returns_data.columns]

            if len(available_symbols) < 2:
                return self._create_default_optimization_result(current_weights)

            # Prepare data
            returns = returns_data[available_symbols].dropna()
            if len(returns) < 30:
                return self._create_default_optimization_result(current_weights)

            mu = np.array([expected_returns[s] for s in available_symbols])
            cov_matrix = returns.cov().values * 252  # Annualized
            current_w = np.array([current_weights.get(s, 0.0) for s in available_symbols])

            # Optimization bounds
            bounds = [(constraints['min_weight'], constraints['max_weight']) for _ in available_symbols]

            # Constraint functions
            constraints_list = []

            # Weights sum to 1
            constraints_list.append({
                'type': 'eq',
                'fun': lambda w: np.sum(w) - 1.0
            })

            # Turnover constraint
            if 'max_turnover' in constraints:
                constraints_list.append({
                    'type': 'ineq',
                    'fun': lambda w: constraints['max_turnover'] - np.sum(np.abs(w - current_w))
                })

            # Risk constraint
            if 'max_risk' in constraints:
                constraints_list.append({
                    'type': 'ineq',
                    'fun': lambda w: constraints['max_risk']**2 - np.dot(w.T, np.dot(cov_matrix, w))
                })

            # Target return constraint
            if constraints.get('target_return'):
                constraints_list.append({
                    'type': 'eq',
                    'fun': lambda w: np.dot(w, mu) - constraints['target_return']
                })

            # Objective function (maximize Sharpe ratio)
            def negative_sharpe(w):
                port_return = np.dot(w, mu)
                port_vol = np.sqrt(np.dot(w.T, np.dot(cov_matrix, w)))
                if port_vol == 0:
                    return -np.inf
                return -(port_return - 0.02) / port_vol  # Assume 2% risk-free rate

            # Optimize
            result = optimize.minimize(
                negative_sharpe,
                current_w,
                method='SLSQP',
                bounds=bounds,
                constraints=constraints_list,
                options={'maxiter': 1000}
            )

            if result.success:
                optimal_weights_array = result.x
                optimal_weights = {symbol: optimal_weights_array[i] for i, symbol in enumerate(available_symbols)}

                # Add missing symbols with zero weight
                for symbol in symbols:
                    if symbol not in optimal_weights:
                        optimal_weights[symbol] = 0.0

                # Calculate metrics
                expected_return = np.dot(optimal_weights_array, mu)
                expected_volatility = np.sqrt(np.dot(optimal_weights_array.T, np.dot(cov_matrix, optimal_weights_array)))
                expected_sharpe = (expected_return - 0.02) / expected_volatility if expected_volatility > 0 else 0

                # Calculate changes and trades
                weight_changes = {symbol: optimal_weights[symbol] - current_weights.get(symbol, 0.0)
                                for symbol in optimal_weights}

                total_turnover = sum(abs(change) for change in weight_changes.values())

                # Estimate required trades (assuming $100k portfolio)
                portfolio_value = 100000  # Default portfolio value
                required_trades = {}
                for symbol, weight_change in weight_changes.items():
                    if abs(weight_change) > 0.001:  # Only trade if change > 0.1%
                        trade_value = weight_change * portfolio_value
                        # Estimate shares (assuming $100 average price)
                        required_trades[symbol] = int(trade_value / 100)

                # Calculate portfolio VaR
                portfolio_var, component_var = RiskModelEngine.calculate_portfolio_var(
                    optimal_weights, returns_data
                )

                constraints_active = []
                if total_turnover >= constraints.get('max_turnover', 1.0) * 0.95:
                    constraints_active.append('turnover_limit')
                if expected_volatility >= constraints.get('max_risk', 1.0) * 0.95:
                    constraints_active.append('risk_limit')

                return OptimizationResult(
                    optimal_weights=optimal_weights,
                    current_weights=current_weights,
                    weight_changes=weight_changes,
                    expected_return=expected_return,
                    expected_volatility=expected_volatility,
                    expected_sharpe=expected_sharpe,
                    portfolio_var=portfolio_var,
                    max_component_risk=max(component_var.values()) if component_var else 0.0,
                    concentration_score=self._calculate_concentration_score(optimal_weights),
                    required_trades=required_trades,
                    trade_costs=self._estimate_trade_costs(required_trades),
                    total_turnover=total_turnover,
                    constraints_active=constraints_active,
                    optimization_status='success',
                    timestamp=datetime.now()
                )
            else:
                self.logger.warning(f"Portfolio optimization failed: {result.message}")
                return self._create_default_optimization_result(current_weights)

        except Exception as e:
            self.logger.error(f"Error in portfolio optimization: {e}")
            return self._create_default_optimization_result(current_weights)

    def _calculate_concentration_score(self, weights: Dict[str, float]) -> float:
        """Calculate portfolio concentration score (Herfindahl index)"""
        try:
            weight_values = list(weights.values())
            return sum(w**2 for w in weight_values)
        except Exception:
            return 0.0

    def _estimate_trade_costs(self, required_trades: Dict[str, int]) -> Dict[str, float]:
        """Estimate trading costs"""
        try:
            trade_costs = {}
            for symbol, shares in required_trades.items():
                # Simple cost model: 0.1% of trade value
                estimated_value = abs(shares) * 100  # Assume $100 average price
                trade_costs[symbol] = estimated_value * 0.001
            return trade_costs
        except Exception:
            return {}

    def _create_default_optimization_result(self, current_weights: Dict[str, float]) -> OptimizationResult:
        """Create default optimization result when optimization fails"""
        return OptimizationResult(
            optimal_weights=current_weights,
            current_weights=current_weights,
            weight_changes={symbol: 0.0 for symbol in current_weights},
            expected_return=0.08,  # Conservative default
            expected_volatility=0.15,
            expected_sharpe=0.4,
            portfolio_var=0.0,
            max_component_risk=0.0,
            concentration_score=0.0,
            required_trades={},
            trade_costs={},
            total_turnover=0.0,
            constraints_active=[],
            optimization_status='failed',
            timestamp=datetime.now()
        )


class IntelligentPortfolioAnalytics:
    """
    Intelligent Portfolio Analytics Engine

    Provides sophisticated portfolio-level analytics including advanced risk
    attribution, performance attribution, optimization, and intelligent
    rebalancing recommendations.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Initialize engines
        self.risk_engine = RiskModelEngine()
        self.attribution_engine = PerformanceAttributionEngine()
        self.optimizer = PortfolioOptimizer()

        # Analytics cache
        self.analytics_cache: Dict[str, Any] = {}
        self.cache_ttl_minutes = 10

        # Performance tracking
        self.calculation_times = deque(maxlen=50)

    def analyze_portfolio_risk(self, portfolio_weights: Dict[str, float],
                             returns_data: pd.DataFrame,
                             benchmark_weights: Dict[str, float] = None) -> PortfolioRiskAttribution:
        """
        Perform comprehensive portfolio risk attribution analysis

        Args:
            portfolio_weights: Current portfolio weights
            returns_data: Historical returns data
            benchmark_weights: Optional benchmark weights

        Returns:
            Comprehensive risk attribution analysis
        """
        start_time = time.time()

        try:
            # Calculate portfolio VaR and component attribution
            total_var, component_var = self.risk_engine.calculate_portfolio_var(
                portfolio_weights, returns_data
            )

            # Calculate marginal VaR
            marginal_var = {}
            for symbol in portfolio_weights:
                if symbol in component_var:
                    weight = portfolio_weights[symbol]
                    if weight > 0:
                        marginal_var[symbol] = component_var[symbol] / weight
                    else:
                        marginal_var[symbol] = 0.0
                else:
                    marginal_var[symbol] = 0.0

            # Calculate concentration index (Herfindahl)
            weight_values = list(portfolio_weights.values())
            concentration_index = sum(w**2 for w in weight_values)

            # Risk decomposition
            systematic_risk = self._estimate_systematic_risk(portfolio_weights, returns_data)
            idiosyncratic_risk = max(0, total_var - systematic_risk)
            correlation_risk = self._estimate_correlation_risk(portfolio_weights, returns_data)
            liquidity_risk = self._estimate_liquidity_risk(portfolio_weights)

            # Advanced risk metrics
            conditional_var = self.risk_engine.calculate_conditional_var(portfolio_weights, returns_data)
            stressed_var = self.risk_engine.calculate_stressed_var(portfolio_weights, returns_data)
            tail_expectation = conditional_var * 1.2  # Approximate tail expectation

            # Risk trend analysis
            risk_momentum = self._analyze_risk_momentum(portfolio_weights, returns_data)
            risk_regime_prob = self._estimate_risk_regime_probabilities(returns_data)

            calculation_time = (time.time() - start_time) * 1000
            self.calculation_times.append(calculation_time)

            return PortfolioRiskAttribution(
                total_portfolio_var=total_var,
                marginal_var=marginal_var,
                component_var=component_var,
                concentration_index=concentration_index,
                systematic_risk=systematic_risk,
                idiosyncratic_risk=idiosyncratic_risk,
                correlation_risk=correlation_risk,
                liquidity_risk=liquidity_risk,
                conditional_var=conditional_var,
                stressed_var=stressed_var,
                tail_expectation=tail_expectation,
                risk_momentum=risk_momentum,
                risk_regime_probability=risk_regime_prob,
                timestamp=datetime.now()
            )

        except Exception as e:
            self.logger.error(f"Error in portfolio risk analysis: {e}")
            return self._create_default_risk_attribution(portfolio_weights)

    def analyze_performance_attribution(self, portfolio_weights: Dict[str, float],
                                      benchmark_weights: Dict[str, float],
                                      returns_data: pd.DataFrame,
                                      benchmark_returns: pd.Series = None) -> PerformanceAttribution:
        """
        Perform comprehensive performance attribution analysis

        Args:
            portfolio_weights: Portfolio weights
            benchmark_weights: Benchmark weights
            returns_data: Returns data
            benchmark_returns: Benchmark return series

        Returns:
            Performance attribution analysis
        """
        start_time = time.time()

        try:
            # Calculate returns
            portfolio_return = self._calculate_portfolio_return(portfolio_weights, returns_data)

            if benchmark_returns is not None:
                benchmark_return = benchmark_returns.iloc[-1] if len(benchmark_returns) > 0 else 0.0
            else:
                benchmark_return = self._calculate_portfolio_return(benchmark_weights, returns_data)

            active_return = portfolio_return - benchmark_return

            # Get individual security returns
            latest_returns = {}
            for symbol in portfolio_weights:
                if symbol in returns_data.columns:
                    symbol_returns = returns_data[symbol].dropna()
                    if len(symbol_returns) > 0:
                        latest_returns[symbol] = symbol_returns.iloc[-1]
                    else:
                        latest_returns[symbol] = 0.0
                else:
                    latest_returns[symbol] = 0.0

            benchmark_security_returns = {}
            for symbol in benchmark_weights:
                if symbol in returns_data.columns:
                    symbol_returns = returns_data[symbol].dropna()
                    if len(symbol_returns) > 0:
                        benchmark_security_returns[symbol] = symbol_returns.iloc[-1]
                    else:
                        benchmark_security_returns[symbol] = 0.0
                else:
                    benchmark_security_returns[symbol] = 0.0

            # Brinson attribution
            attribution = self.attribution_engine.calculate_brinson_attribution(
                portfolio_weights, benchmark_weights, latest_returns, benchmark_security_returns
            )

            # Selection alpha by position
            selection_alpha = {}
            for symbol in portfolio_weights:
                if symbol in latest_returns and symbol in benchmark_security_returns:
                    weight = portfolio_weights[symbol]
                    excess_return = latest_returns[symbol] - benchmark_security_returns[symbol]
                    selection_alpha[symbol] = weight * excess_return
                else:
                    selection_alpha[symbol] = 0.0

            # Risk-adjusted metrics
            portfolio_vol = self._calculate_portfolio_volatility(portfolio_weights, returns_data)
            tracking_error = self._calculate_tracking_error(portfolio_weights, benchmark_weights, returns_data)

            information_ratio = active_return / tracking_error if tracking_error > 0 else 0.0
            sharpe_ratio = (portfolio_return - 0.02) / portfolio_vol if portfolio_vol > 0 else 0.0
            sortino_ratio = self._calculate_sortino_ratio(portfolio_weights, returns_data)
            calmar_ratio = self._calculate_calmar_ratio(portfolio_weights, returns_data)

            # Rolling analysis
            rolling_alpha = self._calculate_rolling_alpha(portfolio_weights, benchmark_weights, returns_data)
            alpha_stability = np.std(rolling_alpha) if len(rolling_alpha) > 1 else 0.0
            performance_persistence = self._calculate_performance_persistence(rolling_alpha)

            return PerformanceAttribution(
                total_return=portfolio_return,
                benchmark_return=benchmark_return,
                active_return=active_return,
                selection_alpha=selection_alpha,
                timing_alpha=attribution['allocation_effect'],
                interaction_alpha=attribution['interaction_effect'],
                factor_returns={},  # Would require factor model
                factor_exposures={},
                factor_attribution={},
                information_ratio=information_ratio,
                sharpe_ratio=sharpe_ratio,
                sortino_ratio=sortino_ratio,
                calmar_ratio=calmar_ratio,
                rolling_alpha=rolling_alpha,
                alpha_stability=alpha_stability,
                performance_persistence=performance_persistence,
                timestamp=datetime.now()
            )

        except Exception as e:
            self.logger.error(f"Error in performance attribution: {e}")
            return self._create_default_performance_attribution()

    def optimize_portfolio_allocation(self, expected_returns: Dict[str, float],
                                    returns_data: pd.DataFrame,
                                    current_weights: Dict[str, float],
                                    optimization_constraints: Dict[str, Any] = None) -> OptimizationResult:
        """
        Optimize portfolio allocation using advanced techniques

        Args:
            expected_returns: Expected returns by symbol
            returns_data: Historical returns data
            current_weights: Current portfolio weights
            optimization_constraints: Optimization constraints

        Returns:
            Portfolio optimization result
        """
        return self.optimizer.optimize_portfolio(
            expected_returns, returns_data, current_weights, optimization_constraints
        )

    def _estimate_systematic_risk(self, weights: Dict[str, float], returns_data: pd.DataFrame) -> float:
        """Estimate systematic (market) risk component"""
        try:
            # Simplified: assume 70% of total risk is systematic
            total_var, _ = self.risk_engine.calculate_portfolio_var(weights, returns_data)
            return total_var * 0.7
        except Exception:
            return 0.0

    def _estimate_correlation_risk(self, weights: Dict[str, float], returns_data: pd.DataFrame) -> float:
        """Estimate risk from correlations"""
        try:
            symbols = [s for s in weights.keys() if s in returns_data.columns and weights[s] > 0]
            if len(symbols) < 2:
                return 0.0

            returns = returns_data[symbols]
            corr_matrix = returns.corr()
            avg_correlation = corr_matrix.values[np.triu_indices_from(corr_matrix.values, k=1)].mean()

            # Higher average correlation = higher correlation risk
            return min(0.1, avg_correlation * 0.1)  # Cap at 10%

        except Exception:
            return 0.05

    def _estimate_liquidity_risk(self, weights: Dict[str, float]) -> float:
        """Estimate liquidity risk (simplified)"""
        try:
            # Simplified: assume smaller positions have higher liquidity risk
            concentration = sum(w**2 for w in weights.values())
            return min(0.05, concentration * 0.1)  # Max 5% liquidity risk
        except Exception:
            return 0.02

    def _analyze_risk_momentum(self, weights: Dict[str, float], returns_data: pd.DataFrame) -> str:
        """Analyze risk momentum trend"""
        try:
            symbols = [s for s in weights.keys() if s in returns_data.columns]
            if not symbols:
                return "STABLE"

            returns = returns_data[symbols].dropna()
            if len(returns) < 20:
                return "STABLE"

            # Calculate rolling volatility
            rolling_vol = returns.std(axis=1).rolling(5).mean()

            if len(rolling_vol) >= 10:
                recent_vol = rolling_vol.iloc[-5:].mean()
                historical_vol = rolling_vol.iloc[-15:-5].mean()

                if recent_vol > historical_vol * 1.2:
                    return "INCREASING"
                elif recent_vol < historical_vol * 0.8:
                    return "DECREASING"

            return "STABLE"

        except Exception:
            return "STABLE"

    def _estimate_risk_regime_probabilities(self, returns_data: pd.DataFrame) -> Dict[str, float]:
        """Estimate probabilities of different risk regimes"""
        try:
            if len(returns_data) < 50:
                return {"low_vol": 0.33, "normal_vol": 0.34, "high_vol": 0.33}

            # Calculate overall market volatility
            portfolio_returns = returns_data.mean(axis=1)
            rolling_vol = portfolio_returns.rolling(20).std() * np.sqrt(252)

            # Classify into regimes
            low_vol_threshold = rolling_vol.quantile(0.33)
            high_vol_threshold = rolling_vol.quantile(0.67)

            current_vol = rolling_vol.iloc[-1]

            if current_vol <= low_vol_threshold:
                return {"low_vol": 0.6, "normal_vol": 0.3, "high_vol": 0.1}
            elif current_vol >= high_vol_threshold:
                return {"low_vol": 0.1, "normal_vol": 0.3, "high_vol": 0.6}
            else:
                return {"low_vol": 0.2, "normal_vol": 0.6, "high_vol": 0.2}

        except Exception:
            return {"low_vol": 0.33, "normal_vol": 0.34, "high_vol": 0.33}

    def _calculate_portfolio_return(self, weights: Dict[str, float], returns_data: pd.DataFrame) -> float:
        """Calculate portfolio return"""
        try:
            latest_returns = {}
            for symbol, weight in weights.items():
                if symbol in returns_data.columns and weight > 0:
                    symbol_returns = returns_data[symbol].dropna()
                    if len(symbol_returns) > 0:
                        latest_returns[symbol] = symbol_returns.iloc[-1] * weight

            return sum(latest_returns.values())

        except Exception:
            return 0.0

    def _calculate_portfolio_volatility(self, weights: Dict[str, float], returns_data: pd.DataFrame) -> float:
        """Calculate portfolio volatility"""
        try:
            symbols = [s for s in weights.keys() if s in returns_data.columns and weights[s] > 0]
            if not symbols:
                return 0.15  # Default volatility

            returns = returns_data[symbols].dropna()
            w = np.array([weights[s] for s in symbols])

            if len(returns) > 1:
                cov_matrix = returns.cov().values
                portfolio_variance = np.dot(w.T, np.dot(cov_matrix, w))
                return np.sqrt(portfolio_variance * 252)  # Annualized

            return 0.15

        except Exception:
            return 0.15

    def _calculate_tracking_error(self, portfolio_weights: Dict[str, float],
                                benchmark_weights: Dict[str, float], returns_data: pd.DataFrame) -> float:
        """Calculate tracking error"""
        try:
            # Get common symbols
            common_symbols = set(portfolio_weights.keys()) & set(benchmark_weights.keys())
            common_symbols = [s for s in common_symbols if s in returns_data.columns]

            if not common_symbols:
                return 0.05  # Default tracking error

            returns = returns_data[common_symbols].dropna()

            if len(returns) < 2:
                return 0.05

            # Calculate portfolio and benchmark returns
            portfolio_returns = returns.dot([portfolio_weights[s] for s in common_symbols])
            benchmark_returns = returns.dot([benchmark_weights[s] for s in common_symbols])

            # Tracking error is std of active returns
            active_returns = portfolio_returns - benchmark_returns
            return active_returns.std() * np.sqrt(252)  # Annualized

        except Exception:
            return 0.05

    def _calculate_sortino_ratio(self, weights: Dict[str, float], returns_data: pd.DataFrame) -> float:
        """Calculate Sortino ratio (downside risk-adjusted return)"""
        try:
            portfolio_return = self._calculate_portfolio_return(weights, returns_data)

            # Calculate downside deviation
            symbols = [s for s in weights.keys() if s in returns_data.columns and weights[s] > 0]
            if not symbols:
                return 0.0

            returns = returns_data[symbols].dropna()
            w = np.array([weights[s] for s in symbols])
            portfolio_returns = returns.dot(w)

            # Downside returns (negative returns only)
            downside_returns = portfolio_returns[portfolio_returns < 0]

            if len(downside_returns) > 0:
                downside_deviation = downside_returns.std() * np.sqrt(252)
                return (portfolio_return - 0.02) / downside_deviation

            return 2.0  # High Sortino if no downside

        except Exception:
            return 0.0

    def _calculate_calmar_ratio(self, weights: Dict[str, float], returns_data: pd.DataFrame) -> float:
        """Calculate Calmar ratio (return / max drawdown)"""
        try:
            portfolio_return = self._calculate_portfolio_return(weights, returns_data)

            # Calculate max drawdown
            symbols = [s for s in weights.keys() if s in returns_data.columns and weights[s] > 0]
            if not symbols:
                return 0.0

            returns = returns_data[symbols].dropna()
            w = np.array([weights[s] for s in symbols])
            portfolio_returns = returns.dot(w)

            # Calculate cumulative returns and drawdown
            cumulative = (1 + portfolio_returns).cumprod()
            running_max = cumulative.expanding().max()
            drawdown = (cumulative - running_max) / running_max
            max_drawdown = abs(drawdown.min())

            if max_drawdown > 0:
                return portfolio_return / max_drawdown

            return 10.0  # High Calmar if no drawdown

        except Exception:
            return 0.0

    def _calculate_rolling_alpha(self, portfolio_weights: Dict[str, float],
                               benchmark_weights: Dict[str, float], returns_data: pd.DataFrame) -> List[float]:
        """Calculate rolling alpha over time"""
        try:
            window = 20  # 20-day rolling window
            alphas = []

            for i in range(window, len(returns_data)):
                window_data = returns_data.iloc[i-window:i]
                portfolio_return = self._calculate_portfolio_return(portfolio_weights, window_data)
                benchmark_return = self._calculate_portfolio_return(benchmark_weights, window_data)
                alpha = portfolio_return - benchmark_return
                alphas.append(alpha)

            return alphas[-10:] if len(alphas) >= 10 else alphas  # Return last 10 periods

        except Exception:
            return [0.0]

    def _calculate_performance_persistence(self, rolling_alpha: List[float]) -> float:
        """Calculate performance persistence score"""
        try:
            if len(rolling_alpha) < 2:
                return 0.5

            # Calculate autocorrelation of alpha
            alpha_series = pd.Series(rolling_alpha)
            if len(alpha_series) > 1:
                autocorr = alpha_series.autocorr(lag=1)
                return max(0, min(1, (autocorr + 1) / 2))  # Convert to 0-1 scale

            return 0.5

        except Exception:
            return 0.5

    def _create_default_risk_attribution(self, weights: Dict[str, float]) -> PortfolioRiskAttribution:
        """Create default risk attribution when calculation fails"""
        return PortfolioRiskAttribution(
            total_portfolio_var=0.1,
            marginal_var={symbol: 0.02 for symbol in weights},
            component_var={symbol: weight * 0.02 for symbol, weight in weights.items()},
            concentration_index=sum(w**2 for w in weights.values()),
            systematic_risk=0.07,
            idiosyncratic_risk=0.03,
            correlation_risk=0.02,
            liquidity_risk=0.01,
            conditional_var=0.12,
            stressed_var=0.2,
            tail_expectation=0.15,
            risk_momentum="STABLE",
            risk_regime_probability={"low_vol": 0.33, "normal_vol": 0.34, "high_vol": 0.33},
            timestamp=datetime.now()
        )

    def _create_default_performance_attribution(self) -> PerformanceAttribution:
        """Create default performance attribution when calculation fails"""
        return PerformanceAttribution(
            total_return=0.08,
            benchmark_return=0.07,
            active_return=0.01,
            selection_alpha={},
            timing_alpha=0.005,
            interaction_alpha=0.005,
            factor_returns={},
            factor_exposures={},
            factor_attribution={},
            information_ratio=0.2,
            sharpe_ratio=0.4,
            sortino_ratio=0.6,
            calmar_ratio=0.8,
            rolling_alpha=[0.01],
            alpha_stability=0.5,
            performance_persistence=0.5,
            timestamp=datetime.now()
        )
