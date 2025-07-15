"""
Enhanced Output Formatting Infrastructure for Friren Trading System
================================================================

This module provides centralized, standardized output formatting for all
trading intelligence components, ensuring consistent real-time visibility
into all trading decisions and business logic.

Target Output Format (Dynamic Templates):
[NEWS COLLECTOR] {symbol}: '{real_headline}' from {source} - collected {timestamp}
[FINBERT] {symbol}: {classification} (confidence: {confidence}%) | article: '{headline_snippet}' | impact: {impact_score}
[XGBOOST] {symbol}: {action} (score: {confidence}) | features: sentiment={sentiment}, volume={volume}, impact={impact}
[RISK CHECK] {symbol}: {status} | decision: {action} {quantity} shares | risk_score: {risk} | reason: '{risk_analysis}'
[EXECUTION] {symbol}: EXECUTED {action} {quantity} shares at ${price} | order_id: {order_id} | value: ${total_value}
[STRATEGY ASSIGNMENT] {symbol}: assigned {strategy} | confidence: {confidence}% | previous: {previous_strategy} | reason: '{reason}'
[POSITION HEALTH] {symbol}: {quantity} shares | value: ${current_value} | PnL: {pnl}% | risk: {risk_level} | action: {recommendation}
"""

from .formatters.news_formatter import NewsFormatter
from .formatters.analysis_formatter import AnalysisFormatter
from .formatters.trading_formatter import TradingFormatter
from .formatters.monitoring_formatter import MonitoringFormatter
from .message_router import MessageRouter
from .output_coordinator import OutputCoordinator

__all__ = [
    'NewsFormatter',
    'AnalysisFormatter', 
    'TradingFormatter',
    'MonitoringFormatter',
    'MessageRouter',
    'OutputCoordinator'
]