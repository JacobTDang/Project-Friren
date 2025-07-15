"""
Portfolio Analyzer - Extracted from Risk Analyzer

This module handles portfolio-level risk analysis including concentration risk, 
diversification scoring, and portfolio-level risk aggregation.

Features:
- Portfolio concentration risk analysis
- Diversification effectiveness scoring
- Strategy and sector concentration analysis
- Portfolio-level risk aggregation
- Dynamic risk budget utilization
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict

# Import market metrics for dynamic calculations
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


@dataclass
class ConcentrationRisk:
    """Portfolio concentration risk metrics"""
    overall_concentration: float
    strategy_concentration: float
    sector_concentration: float
    single_position_risk: float
    top_positions_risk: float


@dataclass 
class DiversificationMetrics:
    """Portfolio diversification effectiveness metrics"""
    diversification_score: float
    effective_positions: int
    correlation_benefit: float
    concentration_penalty: float


class PortfolioAnalyzer:
    """Analyzes portfolio-level risk characteristics and concentrations"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.PortfolioAnalyzer")

    def calculate_concentration_risks(self, position_risks: List[Dict], portfolio_weights: np.ndarray) -> Dict[str, float]:
        """Calculate various concentration risk metrics"""
        try:
            if len(position_risks) == 0 or len(portfolio_weights) == 0:
                return self._default_concentration_risks()

            # Overall concentration using Herfindahl-Hirschman Index
            hhi = sum(w**2 for w in portfolio_weights)
            overall_concentration = min(1.0, hhi * len(portfolio_weights))  # Normalize

            # Strategy concentration
            strategy_concentration = self._calculate_strategy_concentration(position_risks, portfolio_weights)

            # Sector concentration (if sector data available)
            sector_concentration = self._calculate_sector_concentration(position_risks, portfolio_weights)

            # Single largest position risk
            single_position_risk = max(portfolio_weights) if len(portfolio_weights) > 0 else 0.0

            # Top 3 positions concentration
            sorted_weights = sorted(portfolio_weights, reverse=True)
            top_3_concentration = sum(sorted_weights[:3]) if len(sorted_weights) >= 3 else sum(sorted_weights)

            return {
                'overall_concentration': overall_concentration,
                'strategy_concentration': strategy_concentration,
                'sector_concentration': sector_concentration,
                'single_position_risk': single_position_risk,
                'top_positions_risk': top_3_concentration
            }

        except Exception as e:
            self.logger.error(f"Error calculating concentration risks: {e}")
            return self._default_concentration_risks()

    def calculate_diversification_score(self, position_risks: List[Dict], portfolio_weights: np.ndarray, 
                                       correlation_matrix: Optional[np.ndarray] = None) -> float:
        """Calculate portfolio diversification effectiveness score"""
        try:
            if len(position_risks) <= 1:
                return 0.0  # No diversification with single position

            # Base diversification from number of positions
            n_positions = len(position_risks)
            base_diversification = min(100.0, n_positions * 10)  # 10 points per position, max 100

            # Weight distribution bonus
            weight_distribution_score = self._calculate_weight_distribution_score(portfolio_weights)

            # Strategy diversification bonus
            strategy_diversification_score = self._calculate_strategy_diversification_score(position_risks, portfolio_weights)

            # Correlation benefit (if correlation matrix available)
            correlation_benefit = 0.0
            if correlation_matrix is not None:
                correlation_benefit = self._calculate_correlation_benefit(correlation_matrix, portfolio_weights)

            # Risk diversification benefit
            risk_diversification_score = self._calculate_risk_diversification_score(position_risks, portfolio_weights)

            # Combined diversification score
            total_score = (
                base_diversification * 0.3 +
                weight_distribution_score * 0.25 +
                strategy_diversification_score * 0.2 +
                correlation_benefit * 0.15 +
                risk_diversification_score * 0.1
            )

            return min(100.0, max(0.0, total_score))

        except Exception as e:
            self.logger.error(f"Error calculating diversification score: {e}")
            return 50.0  # Default medium diversification

    def calculate_portfolio_market_risk(self, position_risks: List[Dict], portfolio_weights: np.ndarray, 
                                       correlation_matrix: Optional[np.ndarray] = None) -> Dict[str, float]:
        """Calculate portfolio-level market risk metrics"""
        try:
            if len(position_risks) == 0 or len(portfolio_weights) == 0:
                return self._default_portfolio_market_risk()

            # Portfolio volatility calculation
            portfolio_volatility = self._calculate_portfolio_volatility(position_risks, portfolio_weights, correlation_matrix)

            # Portfolio VaR
            portfolio_var = self._calculate_portfolio_var(position_risks, portfolio_weights, correlation_matrix)

            # Correlation risk
            correlation_risk = self._calculate_correlation_risk(correlation_matrix, portfolio_weights) if correlation_matrix is not None else 0.5

            # Liquidity risk aggregation
            liquidity_risk = self._calculate_portfolio_liquidity_risk(position_risks, portfolio_weights)

            # Beta aggregation
            portfolio_beta = self._calculate_portfolio_beta(position_risks, portfolio_weights)

            return {
                'volatility': portfolio_volatility,
                'var': portfolio_var,
                'correlation_risk': correlation_risk,
                'liquidity_risk': liquidity_risk,
                'beta': portfolio_beta
            }

        except Exception as e:
            self.logger.error(f"Error calculating portfolio market risk: {e}")
            return self._default_portfolio_market_risk()

    def calculate_risk_budget_utilization(self, portfolio_market_risk: Dict[str, float], 
                                        concentration_risks: Dict[str, float]) -> Dict[str, float]:
        """Calculate risk budget utilization metrics"""
        try:
            # Define risk budget limits (these could be configurable)
            max_portfolio_vol = 0.25  # 25% max portfolio volatility
            max_single_position = 0.15  # 15% max single position
            max_concentration = 0.6  # 60% max concentration index

            # Calculate utilization ratios
            vol_utilization = portfolio_market_risk.get('volatility', 0.15) / max_portfolio_vol
            position_utilization = concentration_risks.get('single_position_risk', 0.1) / max_single_position
            concentration_utilization = concentration_risks.get('overall_concentration', 0.4) / max_concentration

            # Overall utilization
            overall_utilization = max(vol_utilization, position_utilization, concentration_utilization)

            # Margin of safety
            margin_of_safety = max(0.0, 1.0 - overall_utilization)

            # Risk capacity remaining
            risk_capacity = {
                'volatility_capacity': max(0.0, max_portfolio_vol - portfolio_market_risk.get('volatility', 0.15)),
                'position_capacity': max(0.0, max_single_position - concentration_risks.get('single_position_risk', 0.1)),
                'concentration_capacity': max(0.0, max_concentration - concentration_risks.get('overall_concentration', 0.4))
            }

            return {
                'utilization': overall_utilization,
                'vol_utilization': vol_utilization,
                'position_utilization': position_utilization,
                'concentration_utilization': concentration_utilization,
                'margin_of_safety': margin_of_safety,
                'risk_capacity': risk_capacity
            }

        except Exception as e:
            self.logger.error(f"Error calculating risk budget utilization: {e}")
            return {
                'utilization': 0.5,
                'vol_utilization': 0.5,
                'position_utilization': 0.5,
                'concentration_utilization': 0.5,
                'margin_of_safety': 0.5,
                'risk_capacity': {'volatility_capacity': 0.1, 'position_capacity': 0.05, 'concentration_capacity': 0.2}
            }

    def _calculate_strategy_concentration(self, position_risks: List[Dict], portfolio_weights: np.ndarray) -> float:
        """Calculate concentration by strategy type"""
        try:
            strategy_weights = defaultdict(float)
            
            for i, position in enumerate(position_risks):
                if i < len(portfolio_weights):
                    strategy = position.get('strategy_name', 'UNKNOWN')
                    strategy_weights[strategy] += portfolio_weights[i]

            # Calculate strategy HHI
            strategy_hhi = sum(w**2 for w in strategy_weights.values())
            n_strategies = len(strategy_weights)
            
            # Normalize by number of strategies
            strategy_concentration = strategy_hhi * n_strategies if n_strategies > 0 else 1.0
            
            return min(1.0, strategy_concentration)

        except Exception as e:
            self.logger.error(f"Error calculating strategy concentration: {e}")
            return 0.5

    def _calculate_sector_concentration(self, position_risks: List[Dict], portfolio_weights: np.ndarray) -> float:
        """Calculate concentration by sector (if sector data available)"""
        try:
            sector_weights = defaultdict(float)
            
            for i, position in enumerate(position_risks):
                if i < len(portfolio_weights):
                    # Extract sector from symbol or use default
                    symbol = position.get('symbol', 'UNKNOWN')
                    sector = self._get_sector_from_symbol(symbol)
                    sector_weights[sector] += portfolio_weights[i]

            if not sector_weights:
                return 0.5  # Default if no sector data

            # Calculate sector HHI
            sector_hhi = sum(w**2 for w in sector_weights.values())
            n_sectors = len(sector_weights)
            
            # Normalize by number of sectors
            sector_concentration = sector_hhi * n_sectors if n_sectors > 0 else 1.0
            
            return min(1.0, sector_concentration)

        except Exception as e:
            self.logger.error(f"Error calculating sector concentration: {e}")
            return 0.5

    def _get_sector_from_symbol(self, symbol: str) -> str:
        """Simple sector mapping - in production this would use real sector data"""
        # Simplified sector mapping based on common symbols
        tech_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA']
        finance_symbols = ['JPM', 'BAC', 'WFC', 'GS', 'MS', 'C']
        healthcare_symbols = ['JNJ', 'PFE', 'UNH', 'ABBV', 'MRK']
        
        if symbol in tech_symbols:
            return 'TECHNOLOGY'
        elif symbol in finance_symbols:
            return 'FINANCIALS'
        elif symbol in healthcare_symbols:
            return 'HEALTHCARE'
        else:
            return 'OTHER'

    def _calculate_weight_distribution_score(self, portfolio_weights: np.ndarray) -> float:
        """Calculate score based on how evenly distributed the weights are"""
        try:
            if len(portfolio_weights) <= 1:
                return 0.0

            # Perfect equal weighting would be 1/n for each position
            n = len(portfolio_weights)
            equal_weight = 1.0 / n
            
            # Calculate deviation from equal weighting
            weight_deviations = [abs(w - equal_weight) for w in portfolio_weights]
            avg_deviation = sum(weight_deviations) / n
            
            # Convert to score (lower deviation = higher score)
            max_possible_deviation = (n - 1) / n  # Maximum possible average deviation
            score = 100.0 * (1.0 - avg_deviation / max_possible_deviation)
            
            return max(0.0, min(100.0, score))

        except Exception as e:
            self.logger.error(f"Error calculating weight distribution score: {e}")
            return 50.0

    def _calculate_strategy_diversification_score(self, position_risks: List[Dict], portfolio_weights: np.ndarray) -> float:
        """Calculate diversification score based on strategy mix"""
        try:
            strategy_weights = defaultdict(float)
            
            for i, position in enumerate(position_risks):
                if i < len(portfolio_weights):
                    strategy = position.get('strategy_name', 'UNKNOWN')
                    strategy_weights[strategy] += portfolio_weights[i]

            n_strategies = len(strategy_weights)
            
            if n_strategies <= 1:
                return 0.0

            # Bonus for having multiple strategies
            base_score = min(80.0, n_strategies * 20.0)  # 20 points per strategy, max 80
            
            # Bonus for balanced strategy allocation
            strategy_balance_score = self._calculate_weight_distribution_score(list(strategy_weights.values()))
            
            return (base_score + strategy_balance_score * 0.2) * 0.8  # Scale to 0-100

        except Exception as e:
            self.logger.error(f"Error calculating strategy diversification score: {e}")
            return 25.0

    def _calculate_correlation_benefit(self, correlation_matrix: np.ndarray, portfolio_weights: np.ndarray) -> float:
        """Calculate diversification benefit from low correlations"""
        try:
            if correlation_matrix.shape[0] != len(portfolio_weights) or len(portfolio_weights) <= 1:
                return 0.0

            # Calculate weighted average correlation
            n = len(portfolio_weights)
            total_correlation = 0.0
            total_weight = 0.0
            
            for i in range(n):
                for j in range(i + 1, n):
                    correlation = correlation_matrix[i, j]
                    weight = portfolio_weights[i] * portfolio_weights[j]
                    total_correlation += correlation * weight
                    total_weight += weight

            avg_correlation = total_correlation / total_weight if total_weight > 0 else 0.0
            
            # Lower correlation = higher diversification benefit
            correlation_benefit = 100.0 * (1.0 - abs(avg_correlation))
            
            return max(0.0, min(100.0, correlation_benefit))

        except Exception as e:
            self.logger.error(f"Error calculating correlation benefit: {e}")
            return 50.0

    def _calculate_risk_diversification_score(self, position_risks: List[Dict], portfolio_weights: np.ndarray) -> float:
        """Calculate diversification score based on risk distribution"""
        try:
            if len(position_risks) <= 1:
                return 0.0

            # Get risk scores for all positions
            risk_scores = []
            for position in position_risks:
                risk_score = position.get('total_risk_score', 50.0)
                risk_scores.append(risk_score)

            # Calculate weighted average risk
            weighted_risk = sum(risk * weight for risk, weight in zip(risk_scores, portfolio_weights))
            
            # Calculate risk concentration (similar to weight concentration)
            risk_weights = np.array(risk_scores) / sum(risk_scores) if sum(risk_scores) > 0 else np.ones(len(risk_scores))
            risk_hhi = sum(w**2 for w in risk_weights)
            
            # Convert to diversification score
            n = len(risk_scores)
            min_hhi = 1.0 / n  # Perfect diversification
            max_hhi = 1.0  # Perfect concentration
            
            diversification_score = 100.0 * (max_hhi - risk_hhi) / (max_hhi - min_hhi)
            
            return max(0.0, min(100.0, diversification_score))

        except Exception as e:
            self.logger.error(f"Error calculating risk diversification score: {e}")
            return 50.0

    def _calculate_portfolio_volatility(self, position_risks: List[Dict], portfolio_weights: np.ndarray, 
                                       correlation_matrix: Optional[np.ndarray] = None) -> float:
        """Calculate portfolio-level volatility"""
        try:
            volatilities = np.array([pos.get('volatility', 0.3) for pos in position_risks])
            
            if correlation_matrix is not None and correlation_matrix.shape[0] == len(volatilities):
                # Use correlation matrix for accurate calculation
                portfolio_variance = np.dot(portfolio_weights.T, 
                                          np.dot(correlation_matrix * np.outer(volatilities, volatilities), 
                                                portfolio_weights))
                portfolio_volatility = np.sqrt(abs(portfolio_variance))
            else:
                # Simplified calculation assuming some correlation
                avg_correlation = 0.3  # Assume 30% average correlation
                individual_variance = sum((w * vol)**2 for w, vol in zip(portfolio_weights, volatilities))
                correlation_term = avg_correlation * sum(
                    portfolio_weights[i] * portfolio_weights[j] * volatilities[i] * volatilities[j]
                    for i in range(len(volatilities))
                    for j in range(i + 1, len(volatilities))
                ) * 2
                
                portfolio_variance = individual_variance + correlation_term
                portfolio_volatility = np.sqrt(abs(portfolio_variance))

            return portfolio_volatility

        except Exception as e:
            self.logger.error(f"Error calculating portfolio volatility: {e}")
            return 0.25  # Default 25% portfolio volatility

    def _calculate_portfolio_var(self, position_risks: List[Dict], portfolio_weights: np.ndarray,
                                correlation_matrix: Optional[np.ndarray] = None) -> float:
        """Calculate portfolio-level VaR"""
        try:
            position_vars = np.array([pos.get('var_1d', 0.05) for pos in position_risks])
            
            if correlation_matrix is not None and correlation_matrix.shape[0] == len(position_vars):
                # Portfolio VaR with correlations
                portfolio_variance = np.dot(portfolio_weights.T,
                                          np.dot(correlation_matrix * np.outer(position_vars, position_vars),
                                                portfolio_weights))
                portfolio_var = np.sqrt(abs(portfolio_variance))
            else:
                # Simplified portfolio VaR
                avg_correlation = 0.3
                individual_var = sum((w * var)**2 for w, var in zip(portfolio_weights, position_vars))
                correlation_var = avg_correlation * sum(
                    portfolio_weights[i] * portfolio_weights[j] * position_vars[i] * position_vars[j]
                    for i in range(len(position_vars))
                    for j in range(i + 1, len(position_vars))
                ) * 2
                
                portfolio_var = np.sqrt(abs(individual_var + correlation_var))

            return portfolio_var

        except Exception as e:
            self.logger.error(f"Error calculating portfolio VaR: {e}")
            return 0.05  # Default 5% portfolio VaR

    def _calculate_correlation_risk(self, correlation_matrix: np.ndarray, portfolio_weights: np.ndarray) -> float:
        """Calculate risk from high correlations"""
        try:
            if correlation_matrix.shape[0] != len(portfolio_weights) or len(portfolio_weights) <= 1:
                return 0.5

            # Calculate weighted average correlation
            n = len(portfolio_weights)
            total_correlation = 0.0
            total_weight = 0.0
            
            for i in range(n):
                for j in range(i + 1, n):
                    correlation = abs(correlation_matrix[i, j])  # Use absolute correlation
                    weight = portfolio_weights[i] * portfolio_weights[j]
                    total_correlation += correlation * weight
                    total_weight += weight

            avg_correlation = total_correlation / total_weight if total_weight > 0 else 0.0
            
            # High correlation = high risk
            return min(1.0, avg_correlation)

        except Exception as e:
            self.logger.error(f"Error calculating correlation risk: {e}")
            return 0.5

    def _calculate_portfolio_liquidity_risk(self, position_risks: List[Dict], portfolio_weights: np.ndarray) -> float:
        """Calculate portfolio-level liquidity risk"""
        try:
            liquidity_risks = [pos.get('liquidity_risk', 0.5) for pos in position_risks]
            
            # Weighted average liquidity risk
            weighted_liquidity_risk = sum(risk * weight for risk, weight in zip(liquidity_risks, portfolio_weights))
            
            # Concentration penalty - concentrated portfolios have higher liquidity risk
            concentration_penalty = max(portfolio_weights) * 0.3  # Up to 30% penalty for concentration
            
            total_liquidity_risk = min(1.0, weighted_liquidity_risk + concentration_penalty)
            
            return total_liquidity_risk

        except Exception as e:
            self.logger.error(f"Error calculating portfolio liquidity risk: {e}")
            return 0.5

    def _calculate_portfolio_beta(self, position_risks: List[Dict], portfolio_weights: np.ndarray) -> float:
        """Calculate portfolio-level beta"""
        try:
            betas = [pos.get('beta', 1.0) for pos in position_risks]
            
            # Weighted average beta
            portfolio_beta = sum(beta * weight for beta, weight in zip(betas, portfolio_weights))
            
            return portfolio_beta

        except Exception as e:
            self.logger.error(f"Error calculating portfolio beta: {e}")
            return 1.0

    def _default_concentration_risks(self) -> Dict[str, float]:
        """Return default concentration risk values"""
        return {
            'overall_concentration': 0.5,
            'strategy_concentration': 0.5,
            'sector_concentration': 0.5,
            'single_position_risk': 0.2,
            'top_positions_risk': 0.6
        }

    def _default_portfolio_market_risk(self) -> Dict[str, float]:
        """Return default portfolio market risk values"""
        return {
            'volatility': 0.25,
            'var': 0.05,
            'correlation_risk': 0.5,
            'liquidity_risk': 0.5,
            'beta': 1.0
        }