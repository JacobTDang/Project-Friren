"""
Output Coordinator for Friren Trading System
============================================

Coordinates all output streams and formatting across the entire trading system.
Provides a unified interface for all components to produce standardized output.

This is the central coordination point that ensures all trading intelligence
output follows the target format and reaches the appropriate destinations.
"""

import logging
try:
    import redis
except ImportError:
    redis = None
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass

from .formatters.news_formatter import NewsFormatter
from .formatters.analysis_formatter import AnalysisFormatter
from .formatters.trading_formatter import TradingFormatter
from .formatters.monitoring_formatter import MonitoringFormatter
from .message_router import MessageRouter, MessageType, MessagePriority, FormattedMessage


class OutputCoordinator:
    """Central coordinator for all trading system output"""
    
    def __init__(self, redis_client: Optional[Union['redis.Redis', Any]] = None,
                 enable_terminal: bool = True, enable_logging: bool = True):
        """
        Initialize output coordinator
        
        Args:
            redis_client: Redis client for inter-process communication
            enable_terminal: Enable terminal output
            enable_logging: Enable log file output
        """
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        
        # Initialize formatters
        self.news_formatter = NewsFormatter()
        self.analysis_formatter = AnalysisFormatter()
        self.trading_formatter = TradingFormatter()
        self.monitoring_formatter = MonitoringFormatter()
        
        # Initialize message router
        self.message_router = MessageRouter(
            redis_client=redis_client,
            enable_terminal=enable_terminal,
            enable_logging=enable_logging
        )
        
        # Component tracking
        self.active_components = set()
        self.component_last_seen = {}
        
        # Output statistics
        self.output_stats = {
            'news_outputs': 0,
            'analysis_outputs': 0,
            'trading_outputs': 0,
            'monitoring_outputs': 0,
            'total_outputs': 0
        }
    
    # NEWS OUTPUT METHODS
    def output_news_collection(self, symbol: str, title: str, source: str, 
                              timestamp: datetime = None) -> bool:
        """
        Output news collection message
        
        Args:
            symbol: Stock symbol
            title: Article title
            source: News source
            timestamp: Collection timestamp
            
        Returns:
            True if output was successful
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        formatted_message = self.news_formatter.format_news_collection(
            symbol=symbol, title=title, source=source, timestamp=timestamp
        )
        
        self.output_stats['news_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.NEWS, 
                                MessagePriority.LOW, symbol, "NewsCollector")
    
    def output_news_collection_with_sentiment(self, symbol: str, title: str, sentiment: str) -> bool:
        """
        Output news collection with sentiment - standard format
        
        Args:
            symbol: Stock symbol
            title: Article title
            sentiment: Sentiment analysis result
            
        Returns:
            True if output was successful
        """
        formatted_message = self.news_formatter.format_news_collection_with_sentiment(
            symbol=symbol, title=title, sentiment=sentiment
        )
        
        self.output_stats['news_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.NEWS, 
                                MessagePriority.MEDIUM, symbol, "NewsCollector")
    
    def output_news_processing(self, symbol: str, articles_count: int, 
                             processing_time_ms: float) -> bool:
        """
        Output news processing summary
        
        Args:
            symbol: Stock symbol
            articles_count: Number of articles processed
            processing_time_ms: Processing time in milliseconds
            
        Returns:
            True if output was successful
        """
        formatted_message = self.news_formatter.format_article_processing(
            symbol=symbol, articles_count=articles_count, 
            processing_time_ms=processing_time_ms
        )
        
        self.output_stats['news_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.NEWS,
                                MessagePriority.MEDIUM, symbol, "NewsProcessor")
    
    # ANALYSIS OUTPUT METHODS
    def output_finbert_analysis(self, symbol: str, sentiment: str, confidence: float,
                              article_snippet: str, impact: float) -> bool:
        """
        Output FinBERT analysis result
        
        Args:
            symbol: Stock symbol
            sentiment: Sentiment classification
            confidence: Model confidence
            article_snippet: Article snippet
            impact: Market impact score
            
        Returns:
            True if output was successful
        """
        formatted_message = self.analysis_formatter.format_finbert_result(
            symbol=symbol, sentiment=sentiment, confidence=confidence,
            article_snippet=article_snippet, impact=impact
        )
        
        self.output_stats['analysis_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.ANALYSIS,
                                MessagePriority.MEDIUM, symbol, "FinBERT")
    
    def output_xgboost_recommendation(self, symbol: str, action: str, score: float,
                                    features: Dict[str, float]) -> bool:
        """
        Output XGBoost recommendation
        
        Args:
            symbol: Stock symbol
            action: Recommended action
            score: Model score
            features: Key features used
            
        Returns:
            True if output was successful
        """
        formatted_message = self.analysis_formatter.format_xgboost_recommendation(
            symbol=symbol, action=action, score=score, features=features
        )
        
        self.output_stats['analysis_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.ANALYSIS,
                                MessagePriority.HIGH, symbol, "XGBoost")
    
    # TRADING OUTPUT METHODS
    def output_strategy_analysis(self, symbol: str, strategy_name: str, current_signal: str) -> bool:
        """
        Output strategy analysis - standard format
        
        Args:
            symbol: Stock symbol
            strategy_name: Name of the strategy
            current_signal: Current signal from strategy
            
        Returns:
            True if output was successful
        """
        formatted_message = self.trading_formatter.format_strategy_analysis(
            symbol=symbol, strategy_name=strategy_name, current_signal=current_signal
        )
        
        self.output_stats['trading_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.TRADING,
                                MessagePriority.MEDIUM, symbol, "StrategyAnalyzer")
    
    def output_risk_check(self, symbol: str, status: str, decision: str, 
                         quantity: int, risk_score: float, reason: str) -> bool:
        """
        Output risk check result
        
        Args:
            symbol: Stock symbol
            status: Risk check status
            decision: Trading decision
            quantity: Position size
            risk_score: Risk score
            reason: Risk reasoning
            
        Returns:
            True if output was successful
        """
        formatted_message = self.trading_formatter.format_risk_check(
            symbol=symbol, status=status, decision=decision,
            quantity=quantity, risk_score=risk_score, reason=reason
        )
        
        self.output_stats['trading_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        priority = MessagePriority.HIGH if status == "FAILED" else MessagePriority.MEDIUM
        return self._route_output(formatted_message, MessageType.TRADING,
                                priority, symbol, "RiskManager")
    
    def output_execution(self, symbol: str, action: str, quantity: int,
                        price: float, order_id: str, total_value: float) -> bool:
        """
        Output execution confirmation
        
        Args:
            symbol: Stock symbol
            action: Executed action
            quantity: Executed quantity
            price: Execution price
            order_id: Order ID
            total_value: Total value
            
        Returns:
            True if output was successful
        """
        formatted_message = self.trading_formatter.format_execution(
            symbol=symbol, action=action, quantity=quantity,
            price=price, order_id=order_id, total_value=total_value
        )
        
        self.output_stats['trading_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.TRADING,
                                MessagePriority.CRITICAL, symbol, "ExecutionEngine")
    
    def output_strategy_assignment(self, symbol: str, new_strategy: str, 
                                 confidence: float, previous_strategy: str,
                                 reason: str) -> bool:
        """
        Output strategy assignment
        
        Args:
            symbol: Stock symbol
            new_strategy: New strategy name
            confidence: Assignment confidence
            previous_strategy: Previous strategy
            reason: Assignment reason
            
        Returns:
            True if output was successful
        """
        formatted_message = self.trading_formatter.format_strategy_assignment(
            symbol=symbol, new_strategy=new_strategy, confidence=confidence,
            previous_strategy=previous_strategy, reason=reason
        )
        
        self.output_stats['trading_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.TRADING,
                                MessagePriority.MEDIUM, symbol, "StrategyAssignment")
    
    # MONITORING OUTPUT METHODS
    def output_position_health(self, symbol: str, shares: int, position_value: float,
                             pnl_pct: float, risk_level: str, action: str) -> bool:
        """
        Output position health status
        
        Args:
            symbol: Stock symbol
            shares: Number of shares
            position_value: Current position value
            pnl_pct: PnL percentage
            risk_level: Risk level
            action: Recommended action
            
        Returns:
            True if output was successful
        """
        formatted_message = self.monitoring_formatter.format_position_health(
            symbol=symbol, shares=shares, position_value=position_value,
            pnl_pct=pnl_pct, risk_level=risk_level, action=action
        )
        
        self.output_stats['monitoring_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        priority = MessagePriority.HIGH if risk_level == "HIGH" else MessagePriority.MEDIUM
        return self._route_output(formatted_message, MessageType.MONITORING,
                                priority, symbol, "PositionHealthMonitor")
    
    def output_portfolio_summary(self, total_value: float, daily_pnl: float,
                               daily_pnl_pct: float, active_positions: int) -> bool:
        """
        Output portfolio summary
        
        Args:
            total_value: Total portfolio value
            daily_pnl: Daily PnL amount
            daily_pnl_pct: Daily PnL percentage
            active_positions: Number of active positions
            
        Returns:
            True if output was successful
        """
        formatted_message = self.monitoring_formatter.format_portfolio_summary(
            total_value=total_value, daily_pnl=daily_pnl,
            daily_pnl_pct=daily_pnl_pct, active_positions=active_positions
        )
        
        self.output_stats['monitoring_outputs'] += 1
        self.output_stats['total_outputs'] += 1
        
        return self._route_output(formatted_message, MessageType.MONITORING,
                                MessagePriority.MEDIUM, None, "PortfolioMonitor")
    
    # UTILITY METHODS
    def _route_output(self, message: str, message_type: MessageType,
                     priority: MessagePriority, symbol: Optional[str] = None,
                     component: Optional[str] = None) -> bool:
        """
        Route formatted output through message router
        
        Args:
            message: Formatted message
            message_type: Type of message
            priority: Message priority
            symbol: Associated symbol
            component: Source component
            
        Returns:
            True if routing was successful
        """
        # Track component activity
        if component:
            self.active_components.add(component)
            self.component_last_seen[component] = datetime.now()
        
        # Create structured message
        formatted_message = FormattedMessage(
            content=message,
            message_type=message_type,
            priority=priority,
            symbol=symbol,
            component=component
        )
        
        return self.message_router.route_message(formatted_message)
    
    def output_error(self, error_text: str, component: str, 
                    symbol: Optional[str] = None) -> bool:
        """
        Output error message
        
        Args:
            error_text: Error message
            component: Component that generated the error
            symbol: Associated symbol
            
        Returns:
            True if output was successful
        """
        return self.message_router.route_error(error_text, component, symbol)
    
    def output_critical(self, critical_text: str, component: str,
                       symbol: Optional[str] = None) -> bool:
        """
        Output critical message
        
        Args:
            critical_text: Critical message
            component: Component that generated the message
            symbol: Associated symbol
            
        Returns:
            True if output was successful
        """
        return self.message_router.route_critical(critical_text, component, symbol)
    
    def output_execution_simulation(self, symbol: str, action: str, quantity: int,
                                  price: float, reasoning: str) -> bool:
        """
        Output execution simulation for testing
        
        Args:
            symbol: Stock symbol
            action: Execution action (BUY/SELL)
            quantity: Number of shares
            price: Execution price
            reasoning: Execution reasoning
            
        Returns:
            True if output was successful
        """
        execution_text = f"SIMULATION - {action} {quantity} {symbol} at ${price:.2f} - {reasoning}"
        return self.output_execution(symbol, action, quantity, price, reasoning)
    
    def output_risk_assessment(self, symbol: str, risk_level: str, confidence: float,
                             recommendation: str) -> bool:
        """
        Output risk assessment analysis
        
        Args:
            symbol: Stock symbol
            risk_level: Risk level (LOW/MEDIUM/HIGH)
            confidence: Confidence score
            recommendation: Trading recommendation
            
        Returns:
            True if output was successful
        """
        risk_text = f"Risk: {risk_level} | Confidence: {confidence:.2f} | Rec: {recommendation}"
        return self.output_risk_check(symbol, risk_level, recommendation, confidence)
    
    def get_output_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive output statistics
        
        Returns:
            Dictionary containing output statistics
        """
        return {
            'output_stats': self.output_stats,
            'routing_stats': self.message_router.get_message_stats(),
            'active_components': list(self.active_components),
            'component_count': len(self.active_components),
            'last_activity': max(self.component_last_seen.values()) if self.component_last_seen else None
        }
    
    def validate_output_system(self) -> bool:
        """
        Validate output system functionality using REAL DATA ONLY
        
        This method only validates that the routing infrastructure works,
        it does NOT use any hardcoded or simulated test data.
        
        Returns:
            True if output system infrastructure is ready
        """
        try:
            # Validate message router is initialized
            if self.message_router is None:
                self.logger.error("Message router not initialized")
                return False
            
            # Validate formatters are initialized
            if not all([self.news_formatter, self.analysis_formatter, 
                       self.trading_formatter, self.monitoring_formatter]):
                self.logger.error("Not all formatters initialized")
                return False
            
            # Validate router infrastructure (no hardcoded data)
            return self.message_router.validate_routing_infrastructure()
            
        except Exception as e:
            self.logger.error(f"Output system validation failed: {e}")
            return False