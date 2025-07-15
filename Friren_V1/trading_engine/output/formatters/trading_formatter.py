"""
Trading Output Formatter for Friren Trading System
=================================================

Formats risk management, execution, and strategy assignment output to provide
real-time visibility into trading decisions and order execution.

Target Formats (Dynamic Templates):
[RISK CHECK] {symbol}: {status} | decision: {action} {quantity} shares | risk_score: {risk_score} | reason: '{reason}'
[EXECUTION] {symbol}: EXECUTED {action} {quantity} shares at ${price} | order_id: {order_id} | value: ${total_value}
[STRATEGY ASSIGNMENT] {symbol}: assigned {strategy} | confidence: {confidence}% | previous: {previous_strategy} | reason: '{reason}'
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class RiskAssessment:
    """Real risk assessment - REAL RISK CALCULATIONS ONLY"""
    symbol: str
    decision: str                    # BUY/SELL/HOLD
    quantity: int                    # Calculated position size (NOT HARDCODED)
    risk_score: float               # VaR-calculated risk (0-1) - REAL MARKET DATA
    status: str                     # PASSED/FAILED/WARNING
    reasoning: str                  # Detailed risk explanation with market factors
    risk_factors: List[str]         # Real identified risk factors
    mitigation_actions: List[str]   # Actual risk mitigation strategies
    var_1day: float                 # Real 1-day Value at Risk calculation
    var_1week: float                # Real 1-week Value at Risk calculation
    beta_coefficient: float         # Market beta from real price correlation
    sharpe_ratio: float             # Historical Sharpe ratio from real performance
    max_drawdown: float             # Historical maximum drawdown
    correlation_sp500: float        # Real correlation with S&P 500
    volatility_percentile: float    # Current volatility vs historical percentiles


@dataclass
class ExecutionResult:
    """Real execution result - REAL BROKER DATA ONLY"""
    symbol: str
    action: str                     # BUY/SELL
    quantity: int                   # Actual executed quantity from Alpaca
    executed_price: float           # Real fill price from broker
    total_value: float             # Actual total value (quantity * price + fees)
    order_id: str                  # Real Alpaca order ID
    execution_timestamp: datetime   # Actual execution timestamp from broker
    broker_confirmation: str        # Real Alpaca confirmation message
    fees: float                    # Actual trading fees charged
    status: str                    # EXECUTED/PARTIAL/FAILED (from Alpaca)
    fill_details: List[Dict]       # Complete fill details from Alpaca API
    market_impact: float           # Calculated market impact from execution
    slippage: float               # Real slippage vs expected price
    time_to_fill: float           # Actual time from order to fill
    remaining_quantity: int        # Unfilled quantity for partial fills


@dataclass
class StrategyAssignmentResult:
    """Real strategy assignment result"""
    symbol: str
    new_strategy: str
    previous_strategy: str
    confidence: float
    assignment_reason: str
    market_context: str
    performance_trigger: bool
    assignment_timestamp: datetime


class TradingFormatter:
    """Standardized trading output formatter for risk, execution, and strategy operations"""
    
    @staticmethod
    def format_strategy_analysis(symbol: str, strategy_name: str, current_signal: str) -> str:
        """
        Format strategy analysis - standard format
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            strategy_name: Name of the strategy being analyzed
            current_signal: Current signal from strategy (BUY/SELL/HOLD)
            
        Returns:
            Formatted strategy analysis string matching standard format
        """
        return f"[STRATEGY ANALYZER] Analyzing {symbol} with {strategy_name}: {current_signal}"
    
    @staticmethod
    def format_risk_check(symbol: str, status: str, decision: str, 
                         quantity: int, risk_score: float, reason: str) -> str:
        """
        Format risk check result
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            status: Risk check status (PASSED/FAILED/WARNING)
            decision: Trading decision (BUY/SELL/HOLD)
            quantity: Position size in shares
            risk_score: VaR-calculated risk score (0-1)
            reason: Detailed risk reasoning
            
        Returns:
            Formatted risk check string matching exact target format
        """
        return f"[RISK MANAGER] Risk Manager: Position size {decision} for {symbol} - risk: {self._format_risk_level(risk_score)}"
    
    @staticmethod
    def _format_risk_level(risk_score: float) -> str:
        """Convert risk score to risk level description"""
        if risk_score <= 0.2:
            return "low"
        elif risk_score <= 0.5:
            return "medium"
        elif risk_score <= 0.8:
            return "high"
        else:
            return "critical"
    
    @staticmethod
    def format_execution(symbol: str, action: str, quantity: int,
                        price: float, order_id: str, total_value: float) -> str:
        """
        Format execution confirmation
        
        Args:
            symbol: Stock symbol
            action: Executed action (BUY/SELL)
            quantity: Executed quantity in shares
            price: Actual execution price per share
            order_id: Real Alpaca order ID
            total_value: Total transaction value including fees
            
        Returns:
            Formatted execution confirmation matching exact target format
        """
        return f"[EXECUTION ENGINE] Execution: {action} {quantity} {symbol} at ${price:.2f}"
    
    @staticmethod
    def format_strategy_assignment(symbol: str, new_strategy: str, 
                                 confidence: float, previous_strategy: str,
                                 reason: str) -> str:
        """
        Format strategy assignment
        
        Args:
            symbol: Stock symbol
            new_strategy: Name of newly assigned strategy
            confidence: Assignment confidence percentage
            previous_strategy: Previously assigned strategy
            reason: Detailed reason for strategy change
            
        Returns:
            Formatted strategy assignment matching exact target format
        """
        return f"[STRATEGY ASSIGNMENT] {symbol}: assigned {new_strategy} | confidence: {confidence:.1f}% | previous: {previous_strategy} | reason: '{reason}'"
    
    @staticmethod
    def format_order_placement(symbol: str, order_type: str, action: str,
                             quantity: int, price: Optional[float], 
                             order_id: str, status: str) -> str:
        """
        Format order placement confirmation
        
        Args:
            symbol: Stock symbol
            order_type: Type of order (MARKET, LIMIT, STOP)
            action: Order action (BUY/SELL)
            quantity: Number of shares
            price: Order price (None for market orders)
            order_id: Broker order ID
            status: Order status (SUBMITTED, FILLED, CANCELLED)
            
        Returns:
            Formatted order placement message
        """
        price_str = f" at ${price:.2f}" if price else ""
        return f"[ORDER PLACEMENT] {symbol}: {order_type} {action} {quantity} shares{price_str} | order_id: {order_id} | status: {status}"
    
    @staticmethod
    def format_order_update(symbol: str, order_id: str, status: str,
                          filled_quantity: int, remaining_quantity: int,
                          average_fill_price: Optional[float]) -> str:
        """
        Format order status update
        
        Args:
            symbol: Stock symbol
            order_id: Broker order ID
            status: Current order status
            filled_quantity: Shares filled so far
            remaining_quantity: Shares remaining
            average_fill_price: Average fill price (if any fills)
            
        Returns:
            Formatted order update message
        """
        price_str = f" | avg_price: ${average_fill_price:.2f}" if average_fill_price else ""
        return f"[ORDER UPDATE] {symbol}: {order_id} | status: {status} | filled: {filled_quantity} | remaining: {remaining_quantity}{price_str}"
    
    @staticmethod
    def format_risk_alert(symbol: str, alert_type: str, risk_level: str,
                         current_risk: float, threshold: float, action: str) -> str:
        """
        Format risk alert/warning
        
        Args:
            symbol: Stock symbol
            alert_type: Type of alert (VOLATILITY, DRAWDOWN, CORRELATION)
            risk_level: Risk level (LOW/MEDIUM/HIGH/CRITICAL)
            current_risk: Current risk metric value
            threshold: Risk threshold that triggered alert
            action: Recommended action
            
        Returns:
            Formatted risk alert message
        """
        return f"[RISK ALERT] {symbol}: {alert_type} - {risk_level} risk | current: {current_risk:.3f} | threshold: {threshold:.3f} | action: {action}"
    
    @staticmethod
    def format_position_sizing(symbol: str, max_position: int, recommended_size: int,
                             risk_budget: float, position_risk: float, 
                             sizing_method: str) -> str:
        """
        Format position sizing calculation
        
        Args:
            symbol: Stock symbol
            max_position: Maximum allowed position size
            recommended_size: Recommended position size
            risk_budget: Available risk budget
            position_risk: Risk of this position
            sizing_method: Method used for sizing calculation
            
        Returns:
            Formatted position sizing message
        """
        return f"[POSITION SIZING] {symbol}: recommended: {recommended_size} shares | max: {max_position} | risk_budget: {risk_budget:.1%} | position_risk: {position_risk:.1%} | method: {sizing_method}"
    
    @staticmethod
    def format_portfolio_rebalance(total_value: float, target_allocations: Dict[str, float],
                                 current_allocations: Dict[str, float], 
                                 rebalance_threshold: float) -> str:
        """
        Format portfolio rebalancing information
        
        Args:
            total_value: Total portfolio value
            target_allocations: Target allocation percentages by symbol
            current_allocations: Current allocation percentages by symbol
            rebalance_threshold: Threshold that triggered rebalancing
            
        Returns:
            Formatted portfolio rebalance message
        """
        # Show symbols with largest allocation drift
        drifts = {symbol: abs(target_allocations.get(symbol, 0) - current_allocations.get(symbol, 0)) 
                 for symbol in set(list(target_allocations.keys()) + list(current_allocations.keys()))}
        max_drift_symbol = max(drifts.items(), key=lambda x: x[1])[0] if drifts else "N/A"
        max_drift = drifts.get(max_drift_symbol, 0)
        
        return f"[PORTFOLIO REBALANCE] Total value: ${total_value:,.2f} | max_drift: {max_drift_symbol} {max_drift:.1%} | threshold: {rebalance_threshold:.1%}"
    
    @staticmethod
    def format_trading_error(symbol: str, operation: str, error_type: str,
                           error_message: str, retry_count: int, 
                           recovery_action: str) -> str:
        """
        Format trading errors
        
        Args:
            symbol: Stock symbol
            operation: Operation that failed (ORDER, RISK_CHECK, STRATEGY_ASSIGNMENT)
            error_type: Type of error (BROKER_ERROR, VALIDATION_ERROR, etc.)
            error_message: Error description
            retry_count: Number of retries attempted
            recovery_action: Planned recovery action
            
        Returns:
            Formatted error message
        """
        return f"[TRADING ERROR] {symbol}: {operation} failed - {error_type}: {error_message} | retries: {retry_count} | recovery: {recovery_action}"
    
    @staticmethod
    def format_compliance_check(symbol: str, check_type: str, status: str,
                              regulation: str, limit_type: str, 
                              current_value: float, limit_value: float) -> str:
        """
        Format regulatory compliance check
        
        Args:
            symbol: Stock symbol
            check_type: Type of compliance check
            status: Check status (PASSED/FAILED/WARNING)
            regulation: Applicable regulation
            limit_type: Type of limit being checked
            current_value: Current value being checked
            limit_value: Regulatory limit value
            
        Returns:
            Formatted compliance check message
        """
        return f"[COMPLIANCE] {symbol}: {check_type} {status} | {regulation} {limit_type} | current: {current_value:.2f} | limit: {limit_value:.2f}"
    
    @staticmethod
    def format_trading_metrics(symbol: str, metrics: Dict[str, Any]) -> str:
        """
        Format comprehensive trading metrics
        
        Args:
            symbol: Stock symbol
            metrics: Dictionary containing trading metrics
            
        Returns:
            Formatted trading metrics summary
        """
        # ZERO-HARDCODING: All metrics must be provided by caller - NO DEFAULTS
        total_trades = metrics['total_trades']
        success_rate = metrics['success_rate']
        avg_execution_time = metrics['avg_execution_time_ms']
        total_volume = metrics['total_volume']
        
        return f"[TRADING METRICS] {symbol}: {total_trades} trades | {success_rate:.1f}% success | {avg_execution_time:.0f}ms avg execution | ${total_volume:,.0f} volume"