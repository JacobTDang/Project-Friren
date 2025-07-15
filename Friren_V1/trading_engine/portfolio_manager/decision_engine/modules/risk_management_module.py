"""
Risk Management Module for Enhanced Decision Engine
==================================================

Handles comprehensive risk management including risk assessment, rate limiting,
parameter adaptation, position sizing, and risk monitoring for the enhanced
decision engine. This module ensures safe trading operations.

Extracted from decision_engine.py for better maintainability.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import numpy as np

# Import enhanced components
from ..risk_manager import SolidRiskManager
from ..parameter_adapter import ParameterAdapter, AdaptationLevel, create_adaptation_metrics, get_system_metrics
from Friren_V1.trading_engine.output.output_coordinator import OutputCoordinator


@dataclass
class RateLimiter:
    """Advanced rate limiter for API calls and trading operations"""
    max_calls_per_minute: int = 200  # Alpaca free tier limit
    calls_made: int = 0
    window_start: datetime = field(default_factory=datetime.now)
    
    # Enhanced rate limiting
    burst_allowance: int = 10  # Allow short bursts
    burst_calls_made: int = 0
    last_burst_reset: datetime = field(default_factory=datetime.now)
    
    def can_make_call(self) -> bool:
        """Check if we can make an API call within rate limits"""
        now = datetime.now()
        
        # Reset minute window if needed
        if (now - self.window_start).total_seconds() >= 60:
            self.calls_made = 0
            self.window_start = now
        
        # Reset burst window if needed (every 10 seconds)
        if (now - self.last_burst_reset).total_seconds() >= 10:
            self.burst_calls_made = 0
            self.last_burst_reset = now
        
        # Check both limits
        minute_ok = self.calls_made < int(self.max_calls_per_minute * 0.8)  # 80% buffer
        burst_ok = self.burst_calls_made < self.burst_allowance
        
        return minute_ok and burst_ok
    
    def record_call(self):
        """Record that an API call was made"""
        self.calls_made += 1
        self.burst_calls_made += 1
    
    def get_wait_time(self) -> float:
        """Get recommended wait time before next call"""
        now = datetime.now()
        
        # Check burst limit
        if self.burst_calls_made >= self.burst_allowance:
            burst_reset_in = 10 - (now - self.last_burst_reset).total_seconds()
            if burst_reset_in > 0:
                return burst_reset_in
        
        # Check minute limit
        if self.calls_made >= int(self.max_calls_per_minute * 0.8):
            minute_reset_in = 60 - (now - self.window_start).total_seconds()
            if minute_reset_in > 0:
                return minute_reset_in
        
        return 0.0


@dataclass
class RiskAssessment:
    """Comprehensive risk assessment result"""
    symbol: str
    overall_risk_score: float  # 0-100
    position_risk: float
    market_risk: float
    liquidity_risk: float
    concentration_risk: float
    
    # Risk factors
    volatility_risk: float
    correlation_risk: float
    drawdown_risk: float
    
    # Assessment metadata
    assessment_time: datetime
    confidence: float
    risk_factors: List[str]
    recommendations: List[str]
    
    # Quantitative metrics
    var_estimate: float  # Value at Risk
    expected_shortfall: float
    sharpe_ratio: float
    max_drawdown: float
    
    def is_high_risk(self) -> bool:
        """Check if this is considered high risk"""
        return self.overall_risk_score > 75.0
    
    def is_acceptable_risk(self) -> bool:
        """Check if risk is within acceptable limits"""
        return self.overall_risk_score <= 60.0


@dataclass
class PositionSizingResult:
    """Position sizing calculation result"""
    symbol: str
    recommended_size: int  # Number of shares
    max_size: int
    min_size: int
    
    # Sizing rationale
    sizing_method: str
    risk_budget_pct: float
    portfolio_allocation_pct: float
    
    # Risk metrics
    position_value: float
    risk_per_share: float
    total_risk: float
    
    # Constraints applied
    constraints_applied: List[str]
    confidence: float


@dataclass
class RiskManagementMetrics:
    """Risk management performance metrics"""
    assessments_completed: int = 0
    high_risk_positions: int = 0
    risk_alerts_generated: int = 0
    position_sizes_calculated: int = 0
    
    # Performance metrics
    average_risk_score: float = 0.0
    assessment_time_ms: float = 0.0
    adaptation_cycles: int = 0
    
    # Rate limiting metrics
    api_calls_made: int = 0
    rate_limit_hits: int = 0
    
    # Error tracking
    error_count: int = 0
    last_error_time: Optional[datetime] = None
    
    timestamp: datetime = field(default_factory=datetime.now)


class RiskLevel(Enum):
    """Risk level classifications"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    EXTREME = "EXTREME"


class RiskManagementModule:
    """
    Risk Management Module
    
    Provides comprehensive risk management including risk assessment,
    position sizing, rate limiting, parameter adaptation, and risk monitoring
    for safe trading operations.
    """
    
    def __init__(self, output_coordinator: Optional[OutputCoordinator] = None,
                 process_id: str = "risk_management"):
        """
        Initialize risk management module
        
        Args:
            output_coordinator: Output coordinator for standardized output
            process_id: Process identifier for output
        """
        self.output_coordinator = output_coordinator
        self.process_id = process_id
        self.logger = logging.getLogger(__name__)
        
        # Core components
        self.risk_manager: Optional[SolidRiskManager] = None
        self.parameter_adapter: Optional[ParameterAdapter] = None
        
        # Rate limiting
        self.rate_limiter = RateLimiter()
        
        # Risk tracking
        self.current_assessments: Dict[str, RiskAssessment] = {}
        self.risk_history: Dict[str, List[RiskAssessment]] = {}
        self.position_sizes: Dict[str, PositionSizingResult] = {}
        
        # Configuration
        self.max_portfolio_risk = 0.02  # 2% portfolio VaR limit
        self.max_position_risk = 0.01   # 1% position VaR limit
        self.max_correlation = 0.7      # Maximum correlation between positions
        self.max_concentration = 0.15   # Maximum 15% in any single position
        
        # Adaptation tracking
        self.last_adaptation_time: Optional[datetime] = None
        self.adaptation_interval_minutes = 15
        
        # Performance tracking
        self.metrics = RiskManagementMetrics()
        
    def initialize(self) -> bool:
        """
        Initialize risk management components
        
        Returns:
            True if initialization successful
        """
        try:
            self.logger.info("Initializing risk management module...")
            
            # Initialize risk manager
            self.risk_manager = SolidRiskManager()
            
            # Initialize parameter adapter
            self.parameter_adapter = ParameterAdapter(adaptation_level=AdaptationLevel.MODERATE)
            
            self.logger.info("Risk management module initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize risk management module: {e}")
            return False
    
    def assess_trading_risk(self, symbol: str, action: str, quantity: int, 
                          market_data: Optional[Dict] = None) -> RiskAssessment:
        """
        Perform comprehensive risk assessment for a trading decision
        
        Args:
            symbol: Stock symbol
            action: Trading action (BUY/SELL/HOLD)
            quantity: Position quantity
            market_data: Optional market data for analysis
            
        Returns:
            Comprehensive risk assessment
        """
        start_time = time.time()
        
        try:
            self.logger.debug(f"Assessing risk for {action} {quantity} {symbol}")
            
            # Get current market data if not provided
            if not market_data:
                market_data = self._get_market_data(symbol)
            
            # Calculate component risks
            position_risk = self._calculate_position_risk(symbol, action, quantity, market_data)
            market_risk = self._calculate_market_risk(symbol, market_data)
            liquidity_risk = self._calculate_liquidity_risk(symbol, quantity, market_data)
            concentration_risk = self._calculate_concentration_risk(symbol, quantity)
            
            # Calculate volatility and correlation risks
            volatility_risk = self._calculate_volatility_risk(symbol, market_data)
            correlation_risk = self._calculate_correlation_risk(symbol)
            drawdown_risk = self._calculate_drawdown_risk(symbol, market_data)
            
            # Compute overall risk score (weighted average)
            overall_risk = self._compute_overall_risk_score(
                position_risk, market_risk, liquidity_risk, concentration_risk,
                volatility_risk, correlation_risk, drawdown_risk
            )
            
            # Calculate quantitative metrics
            var_estimate = self._calculate_var(symbol, quantity, market_data)
            expected_shortfall = self._calculate_expected_shortfall(symbol, quantity, market_data)
            sharpe_ratio = self._calculate_sharpe_ratio(symbol, market_data)
            max_drawdown = self._calculate_max_drawdown(symbol, market_data)
            
            # Generate risk factors and recommendations
            risk_factors = self._identify_risk_factors(
                position_risk, market_risk, liquidity_risk, concentration_risk,
                volatility_risk, correlation_risk, drawdown_risk
            )
            recommendations = self._generate_risk_recommendations(overall_risk, risk_factors)
            
            # Create risk assessment
            assessment = RiskAssessment(
                symbol=symbol,
                overall_risk_score=overall_risk,
                position_risk=position_risk,
                market_risk=market_risk,
                liquidity_risk=liquidity_risk,
                concentration_risk=concentration_risk,
                volatility_risk=volatility_risk,
                correlation_risk=correlation_risk,
                drawdown_risk=drawdown_risk,
                assessment_time=datetime.now(),
                confidence=self._calculate_assessment_confidence(market_data),
                risk_factors=risk_factors,
                recommendations=recommendations,
                var_estimate=var_estimate,
                expected_shortfall=expected_shortfall,
                sharpe_ratio=sharpe_ratio,
                max_drawdown=max_drawdown
            )
            
            # Store assessment
            self.current_assessments[symbol] = assessment
            if symbol not in self.risk_history:
                self.risk_history[symbol] = []
            self.risk_history[symbol].append(assessment)
            
            # Keep only recent history
            if len(self.risk_history[symbol]) > 50:
                self.risk_history[symbol] = self.risk_history[symbol][-50:]
            
            # Output risk check using OutputCoordinator
            if self.output_coordinator:
                self._output_risk_check(symbol, action, quantity, assessment)
            
            # Update metrics
            assessment_time = (time.time() - start_time) * 1000
            self.metrics.assessment_time_ms += assessment_time
            self.metrics.assessments_completed += 1
            
            if assessment.is_high_risk():
                self.metrics.high_risk_positions += 1
                self.metrics.risk_alerts_generated += 1
            
            self.logger.debug(f"Risk assessment completed for {symbol} in {assessment_time:.1f}ms: {overall_risk:.1f}")
            
            return assessment
            
        except Exception as e:
            self.logger.error(f"Error assessing risk for {symbol}: {e}")
            self.metrics.error_count += 1
            self.metrics.last_error_time = datetime.now()
            
            # Return conservative high-risk assessment
            return RiskAssessment(
                symbol=symbol,
                overall_risk_score=90.0,
                position_risk=90.0,
                market_risk=90.0,
                liquidity_risk=90.0,
                concentration_risk=90.0,
                volatility_risk=90.0,
                correlation_risk=90.0,
                drawdown_risk=90.0,
                assessment_time=datetime.now(),
                confidence=0.0,
                risk_factors=["assessment_error"],
                recommendations=["avoid_trading"],
                var_estimate=0.0,
                expected_shortfall=0.0,
                sharpe_ratio=0.0,
                max_drawdown=1.0
            )
    
    def calculate_position_size(self, symbol: str, action: str, signal_confidence: float,
                              risk_assessment: RiskAssessment) -> PositionSizingResult:
        """
        Calculate optimal position size based on risk assessment
        
        Args:
            symbol: Stock symbol
            action: Trading action
            signal_confidence: Signal confidence (0-100)
            risk_assessment: Risk assessment result
            
        Returns:
            Position sizing result
        """
        try:
            self.logger.debug(f"Calculating position size for {symbol} ({action})")
            
            # Get current market data
            market_data = self._get_market_data(symbol)
            current_price = market_data.get('price', 100.0)  # Default price
            
            # Base sizing using Kelly criterion modified for confidence
            kelly_fraction = self._calculate_kelly_fraction(symbol, signal_confidence, risk_assessment)
            
            # Apply risk constraints
            risk_adjusted_fraction = self._apply_risk_constraints(kelly_fraction, risk_assessment)
            
            # Calculate portfolio allocation
            portfolio_value = self._get_portfolio_value()
            max_position_value = portfolio_value * risk_adjusted_fraction
            
            # Calculate share quantities
            base_shares = int(max_position_value / current_price)
            
            # Apply sizing constraints
            min_shares = max(1, int(base_shares * 0.1))  # Minimum 10% of calculated size
            max_shares = int(base_shares * 2.0)  # Maximum 200% of calculated size
            
            # Apply concentration limits
            max_concentration_shares = int(portfolio_value * self.max_concentration / current_price)
            max_shares = min(max_shares, max_concentration_shares)
            
            # Final recommended size
            recommended_shares = base_shares
            
            # Apply additional constraints based on risk level
            constraints_applied = []
            
            if risk_assessment.is_high_risk():
                recommended_shares = int(recommended_shares * 0.5)  # Reduce by 50% for high risk
                constraints_applied.append("high_risk_reduction")
            
            if risk_assessment.concentration_risk > 70:
                recommended_shares = int(recommended_shares * 0.7)  # Reduce for concentration risk
                constraints_applied.append("concentration_limit")
            
            if risk_assessment.liquidity_risk > 80:
                recommended_shares = int(recommended_shares * 0.6)  # Reduce for liquidity risk
                constraints_applied.append("liquidity_constraint")
            
            # Ensure within bounds
            recommended_shares = max(min_shares, min(recommended_shares, max_shares))
            
            # Calculate final metrics
            position_value = recommended_shares * current_price
            portfolio_allocation_pct = (position_value / portfolio_value) * 100
            risk_per_share = current_price * (risk_assessment.overall_risk_score / 100) * 0.01
            total_risk = recommended_shares * risk_per_share
            
            # Create result
            result = PositionSizingResult(
                symbol=symbol,
                recommended_size=recommended_shares,
                max_size=max_shares,
                min_size=min_shares,
                sizing_method="kelly_risk_adjusted",
                risk_budget_pct=risk_adjusted_fraction * 100,
                portfolio_allocation_pct=portfolio_allocation_pct,
                position_value=position_value,
                risk_per_share=risk_per_share,
                total_risk=total_risk,
                constraints_applied=constraints_applied,
                confidence=min(signal_confidence, 100.0 - risk_assessment.overall_risk_score)
            )
            
            # Store result
            self.position_sizes[symbol] = result
            self.metrics.position_sizes_calculated += 1
            
            self.logger.debug(f"Position size calculated for {symbol}: {recommended_shares} shares")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error calculating position size for {symbol}: {e}")
            self.metrics.error_count += 1
            
            # Return conservative minimal position
            return PositionSizingResult(
                symbol=symbol,
                recommended_size=1,
                max_size=1,
                min_size=1,
                sizing_method="error_fallback",
                risk_budget_pct=0.1,
                portfolio_allocation_pct=0.1,
                position_value=100.0,
                risk_per_share=1.0,
                total_risk=1.0,
                constraints_applied=["error_constraint"],
                confidence=0.0
            )
    
    def check_rate_limits(self) -> bool:
        """
        Check if we can proceed with trading operations within rate limits
        
        Returns:
            True if operation can proceed
        """
        return self.rate_limiter.can_make_call()
    
    def record_api_call(self):
        """Record that an API call was made"""
        self.rate_limiter.record_call()
        self.metrics.api_calls_made += 1
    
    def get_rate_limit_wait_time(self) -> float:
        """Get recommended wait time before next operation"""
        return self.rate_limiter.get_wait_time()
    
    def adapt_parameters(self) -> bool:
        """
        Perform parameter adaptation based on system performance
        
        Returns:
            True if adaptation was performed
        """
        try:
            # Check if it's time for adaptation
            if self.last_adaptation_time:
                time_since_last = (datetime.now() - self.last_adaptation_time).total_seconds() / 60
                if time_since_last < self.adaptation_interval_minutes:
                    return False
            
            if not self.parameter_adapter:
                return False
            
            self.logger.info("Performing parameter adaptation...")
            
            # Create adaptation metrics
            adaptation_metrics = create_adaptation_metrics(
                success_rate=self._calculate_success_rate(),
                avg_return=self._calculate_average_return(),
                risk_score=self._calculate_portfolio_risk_score(),
                volatility=self._calculate_portfolio_volatility(),
                drawdown=self._calculate_current_drawdown()
            )
            
            # Get system metrics
            system_metrics = get_system_metrics()
            
            # Perform adaptation
            adaptation_result = self.parameter_adapter.adapt_parameters(
                adaptation_metrics, system_metrics
            )
            
            if adaptation_result:
                self.logger.info(f"Parameter adaptation completed: {adaptation_result.adaptations_made} changes")
                self.metrics.adaptation_cycles += 1
                self.last_adaptation_time = datetime.now()
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error in parameter adaptation: {e}")
            return False
    
    def _calculate_position_risk(self, symbol: str, action: str, quantity: int, market_data: Dict) -> float:
        """Calculate position-specific risk"""
        try:
            if not self.risk_manager:
                return 50.0
            
            return self.risk_manager.calculate_position_risk(symbol, action, quantity, market_data)
        except Exception:
            return 70.0  # Conservative default
    
    def _calculate_market_risk(self, symbol: str, market_data: Dict) -> float:
        """Calculate market risk"""
        try:
            # Simple market risk based on volatility and market conditions
            volatility = market_data.get('volatility', 0.2)
            beta = market_data.get('beta', 1.0)
            
            # Base risk from volatility and beta
            risk_score = min(100.0, volatility * 100 + abs(beta - 1.0) * 20)
            
            return risk_score
            
        except Exception:
            return 50.0
    
    def _calculate_liquidity_risk(self, symbol: str, quantity: int, market_data: Dict) -> float:
        """Calculate liquidity risk"""
        try:
            avg_volume = market_data.get('avg_volume', 1000000)
            current_price = market_data.get('price', 100.0)
            
            position_value = quantity * current_price
            daily_volume_value = avg_volume * current_price
            
            # Risk increases with position size relative to daily volume
            volume_ratio = position_value / daily_volume_value if daily_volume_value > 0 else 1.0
            
            risk_score = min(100.0, volume_ratio * 100)
            
            return risk_score
            
        except Exception:
            return 30.0
    
    def _calculate_concentration_risk(self, symbol: str, quantity: int) -> float:
        """Calculate concentration risk"""
        try:
            current_price = self._get_market_data(symbol).get('price', 100.0)
            position_value = quantity * current_price
            portfolio_value = self._get_portfolio_value()
            
            concentration = position_value / portfolio_value if portfolio_value > 0 else 0.0
            
            # Risk increases exponentially with concentration
            risk_score = min(100.0, (concentration / self.max_concentration) ** 2 * 100)
            
            return risk_score
            
        except Exception:
            return 20.0
    
    def _calculate_volatility_risk(self, symbol: str, market_data: Dict) -> float:
        """Calculate volatility risk"""
        try:
            volatility = market_data.get('volatility', 0.2)
            return min(100.0, volatility * 200)  # Scale volatility to 0-100
        except Exception:
            return 40.0
    
    def _calculate_correlation_risk(self, symbol: str) -> float:
        """Calculate correlation risk with existing positions"""
        try:
            # This would require portfolio position data
            # For now, return moderate risk
            return 30.0
        except Exception:
            return 30.0
    
    def _calculate_drawdown_risk(self, symbol: str, market_data: Dict) -> float:
        """Calculate drawdown risk"""
        try:
            max_drawdown = market_data.get('max_drawdown', 0.1)
            return min(100.0, max_drawdown * 200)
        except Exception:
            return 25.0
    
    def _compute_overall_risk_score(self, *risk_components) -> float:
        """Compute weighted overall risk score"""
        try:
            weights = [0.25, 0.20, 0.15, 0.15, 0.10, 0.10, 0.05]  # Position, market, liquidity, concentration, volatility, correlation, drawdown
            
            if len(risk_components) != len(weights):
                weights = [1.0 / len(risk_components)] * len(risk_components)
            
            weighted_score = sum(risk * weight for risk, weight in zip(risk_components, weights))
            return min(100.0, weighted_score)
            
        except Exception:
            return 60.0  # Conservative default
    
    def _calculate_var(self, symbol: str, quantity: int, market_data: Dict) -> float:
        """Calculate Value at Risk"""
        try:
            price = market_data.get('price', 100.0)
            volatility = market_data.get('volatility', 0.2)
            
            # 95% VaR calculation
            var_95 = quantity * price * volatility * 1.645  # 95% confidence level
            return var_95
            
        except Exception:
            return 0.0
    
    def _calculate_expected_shortfall(self, symbol: str, quantity: int, market_data: Dict) -> float:
        """Calculate Expected Shortfall (Conditional VaR)"""
        try:
            var = self._calculate_var(symbol, quantity, market_data)
            return var * 1.3  # Approximate ES as 130% of VaR
        except Exception:
            return 0.0
    
    def _calculate_sharpe_ratio(self, symbol: str, market_data: Dict) -> float:
        """Calculate Sharpe ratio"""
        try:
            return market_data.get('sharpe_ratio', 0.0)
        except Exception:
            return 0.0
    
    def _calculate_max_drawdown(self, symbol: str, market_data: Dict) -> float:
        """Calculate maximum drawdown"""
        try:
            return market_data.get('max_drawdown', 0.1)
        except Exception:
            return 0.1
    
    def _calculate_assessment_confidence(self, market_data: Dict) -> float:
        """Calculate confidence in risk assessment"""
        try:
            data_quality = len([k for k in ['price', 'volatility', 'volume'] if k in market_data]) / 3.0
            return data_quality * 100.0
        except Exception:
            return 50.0
    
    def _identify_risk_factors(self, *risk_scores) -> List[str]:
        """Identify primary risk factors"""
        factors = []
        risk_names = ['position', 'market', 'liquidity', 'concentration', 'volatility', 'correlation', 'drawdown']
        
        for score, name in zip(risk_scores, risk_names):
            if score > 70:
                factors.append(f"high_{name}_risk")
            elif score > 50:
                factors.append(f"moderate_{name}_risk")
        
        return factors
    
    def _generate_risk_recommendations(self, overall_risk: float, risk_factors: List[str]) -> List[str]:
        """Generate risk management recommendations"""
        recommendations = []
        
        if overall_risk > 80:
            recommendations.append("avoid_new_positions")
            recommendations.append("consider_position_reduction")
        elif overall_risk > 60:
            recommendations.append("reduce_position_size")
            recommendations.append("increase_monitoring")
        elif overall_risk > 40:
            recommendations.append("normal_position_size")
            recommendations.append("regular_monitoring")
        else:
            recommendations.append("consider_larger_position")
        
        return recommendations
    
    def _output_risk_check(self, symbol: str, action: str, quantity: int, assessment: RiskAssessment):
        """Output risk check using OutputCoordinator"""
        try:
            if self.output_coordinator:
                status = "PASSED" if assessment.is_acceptable_risk() else "FAILED"
                decision = f"{action} {quantity} shares"
                reason = f"Risk score: {assessment.overall_risk_score:.1f}%"
                
                if assessment.risk_factors:
                    reason += f", Factors: {', '.join(assessment.risk_factors[:3])}"
                
                self.output_coordinator.output_risk_check(
                    symbol=symbol,
                    status=status,
                    decision=decision,
                    quantity=quantity,
                    risk_score=assessment.overall_risk_score,
                    reason=reason
                )
        except Exception as e:
            self.logger.warning(f"Error outputting risk check for {symbol}: {e}")
    
    def _get_market_data(self, symbol: str) -> Dict:
        """Get market data for symbol"""
        # This would integrate with actual market data provider
        return {
            'price': 100.0,
            'volatility': 0.2,
            'volume': 1000000,
            'avg_volume': 1000000,
            'beta': 1.0,
            'max_drawdown': 0.1,
            'sharpe_ratio': 0.5
        }
    
    def _get_portfolio_value(self) -> float:
        """Get current portfolio value"""
        # This would integrate with actual portfolio data
        return 100000.0  # Default $100k portfolio
    
    def _calculate_kelly_fraction(self, symbol: str, confidence: float, assessment: RiskAssessment) -> float:
        """Calculate Kelly fraction for position sizing"""
        try:
            # Simplified Kelly: f = (bp - q) / b
            # Where b = odds, p = win probability, q = loss probability
            win_prob = confidence / 100.0
            loss_prob = 1.0 - win_prob
            
            # Estimate expected return based on confidence and risk
            expected_return = (confidence / 100.0) * 0.1 - (assessment.overall_risk_score / 100.0) * 0.05
            
            if expected_return <= 0:
                return 0.01  # Minimal position
            
            # Conservative Kelly fraction
            kelly_fraction = min(0.25, expected_return / 0.04)  # Cap at 25% of portfolio
            
            return max(0.01, kelly_fraction)
            
        except Exception:
            return 0.02  # Default 2% position
    
    def _apply_risk_constraints(self, kelly_fraction: float, assessment: RiskAssessment) -> float:
        """Apply risk constraints to Kelly fraction"""
        try:
            adjusted_fraction = kelly_fraction
            
            # Reduce for high overall risk
            if assessment.overall_risk_score > 70:
                adjusted_fraction *= 0.5
            elif assessment.overall_risk_score > 50:
                adjusted_fraction *= 0.7
            
            # Apply maximum position limit
            adjusted_fraction = min(adjusted_fraction, self.max_concentration)
            
            return max(0.005, adjusted_fraction)  # Minimum 0.5% position
            
        except Exception:
            return 0.01
    
    def _calculate_success_rate(self) -> float:
        """Calculate recent trading success rate"""
        # This would analyze recent trading history
        return 0.6  # Default 60% success rate
    
    def _calculate_average_return(self) -> float:
        """Calculate average return"""
        # This would analyze recent returns
        return 0.05  # Default 5% return
    
    def _calculate_portfolio_risk_score(self) -> float:
        """Calculate overall portfolio risk score"""
        if self.current_assessments:
            return sum(a.overall_risk_score for a in self.current_assessments.values()) / len(self.current_assessments)
        return 50.0
    
    def _calculate_portfolio_volatility(self) -> float:
        """Calculate portfolio volatility"""
        return 0.15  # Default 15% volatility
    
    def _calculate_current_drawdown(self) -> float:
        """Calculate current portfolio drawdown"""
        return 0.05  # Default 5% drawdown
    
    def get_metrics(self) -> RiskManagementMetrics:
        """Get current risk management metrics"""
        # Update average risk score
        if self.current_assessments:
            self.metrics.average_risk_score = sum(
                a.overall_risk_score for a in self.current_assessments.values()
            ) / len(self.current_assessments)
        
        return self.metrics
    
    def cleanup(self):
        """Cleanup module resources"""
        try:
            self.current_assessments.clear()
            self.risk_history.clear()
            self.position_sizes.clear()
            
            if self.risk_manager and hasattr(self.risk_manager, 'cleanup'):
                self.risk_manager.cleanup()
            
            if self.parameter_adapter and hasattr(self.parameter_adapter, 'cleanup'):
                self.parameter_adapter.cleanup()
            
            self.logger.info("Risk management module cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during risk management cleanup: {e}")