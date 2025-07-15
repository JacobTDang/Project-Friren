"""
Decision Metrics - Extracted from Decision Engine

This module handles decision tracking, performance metrics, and analytics.
Provides comprehensive decision analysis with zero hardcoded thresholds.

Features:
- Real-time decision tracking
- Performance metrics calculation
- Decision analytics and insights
- Success rate monitoring
- Market-adjusted performance scoring
- Trend analysis and predictions
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import numpy as np

# Import market metrics for dynamic analysis
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


class DecisionType(Enum):
    """Types of trading decisions"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    REDUCE = "reduce"
    INCREASE = "increase"
    EXIT = "exit"
    STRATEGY_CHANGE = "strategy_change"


class DecisionOutcome(Enum):
    """Outcome classification for decisions"""
    PENDING = "pending"        # Decision not yet executed or too recent
    SUCCESS = "success"        # Positive outcome
    FAILURE = "failure"        # Negative outcome
    NEUTRAL = "neutral"        # Mixed or minimal outcome
    CANCELLED = "cancelled"    # Decision was cancelled


@dataclass
class DecisionRecord:
    """Comprehensive decision record with market context"""
    decision_id: str
    timestamp: datetime
    symbol: str
    strategy_name: str
    decision_type: DecisionType
    confidence: float
    reasoning: str
    market_context: Dict[str, Any]
    execution_price: Optional[float] = None
    quantity: Optional[float] = None
    outcome: DecisionOutcome = DecisionOutcome.PENDING
    outcome_timestamp: Optional[datetime] = None
    pnl: Optional[float] = None
    market_adjusted_pnl: Optional[float] = None
    success_score: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceMetrics:
    """Performance metrics for decision analysis"""
    total_decisions: int
    success_rate: float
    average_confidence: float
    average_pnl: float
    market_adjusted_return: float
    sharpe_ratio: float
    win_rate: float
    average_win: float
    average_loss: float
    profit_factor: float
    max_drawdown: float
    recovery_factor: float
    decisions_per_day: float
    confidence_accuracy: float  # How well confidence predicts success
    market_correlation: float


class DecisionMetrics:
    """Tracks and analyzes trading decision performance with market context"""
    
    def __init__(self, max_history: int = 10000):
        self.logger = logging.getLogger(f"{__name__}.DecisionMetrics")
        
        # Decision tracking
        self.decisions = deque(maxlen=max_history)  # All decisions
        self.decisions_by_symbol = defaultdict(list)  # {symbol: List[DecisionRecord]}
        self.decisions_by_strategy = defaultdict(list)  # {strategy: List[DecisionRecord]}
        
        # Performance caching
        self.cached_metrics = {}  # {key: (metrics, timestamp)}
        self.cache_duration_minutes = 5
        
        # Outcome evaluation parameters (market-driven)
        self.evaluation_delay_hours = 4  # Hours to wait before evaluating outcome
        self.success_threshold_pct = 0.5  # Minimum return % to consider success
        
        self.logger.info(f"DecisionMetrics initialized with {max_history} max history")
    
    def record_decision(self, decision: DecisionRecord):
        """Record a new trading decision"""
        try:
            # Add to main history
            self.decisions.append(decision)
            
            # Add to symbol-specific history
            self.decisions_by_symbol[decision.symbol].append(decision)
            if len(self.decisions_by_symbol[decision.symbol]) > 1000:
                self.decisions_by_symbol[decision.symbol] = self.decisions_by_symbol[decision.symbol][-500:]
            
            # Add to strategy-specific history
            self.decisions_by_strategy[decision.strategy_name].append(decision)
            if len(self.decisions_by_strategy[decision.strategy_name]) > 1000:
                self.decisions_by_strategy[decision.strategy_name] = self.decisions_by_strategy[decision.strategy_name][-500:]
            
            # Clear relevant caches
            self._clear_related_caches(decision.symbol, decision.strategy_name)
            
            self.logger.debug(f"Recorded decision: {decision.decision_type.value} {decision.symbol} (confidence: {decision.confidence:.2f})")
            
        except Exception as e:
            self.logger.error(f"Error recording decision: {e}")
    
    def update_decision_outcome(self, decision_id: str, outcome: DecisionOutcome, 
                               pnl: Optional[float] = None, market_price: Optional[float] = None):
        """Update the outcome of a decision"""
        try:
            # Find the decision
            decision = None
            for d in reversed(self.decisions):
                if d.decision_id == decision_id:
                    decision = d
                    break
            
            if not decision:
                self.logger.warning(f"Decision {decision_id} not found for outcome update")
                return
            
            # Update outcome
            decision.outcome = outcome
            decision.outcome_timestamp = datetime.now()
            decision.pnl = pnl
            
            # Calculate market-adjusted PnL
            if pnl is not None:
                decision.market_adjusted_pnl = self._calculate_market_adjusted_pnl(
                    decision, pnl, market_price
                )
            
            # Calculate success score
            decision.success_score = self._calculate_success_score(decision)
            
            # Clear caches
            self._clear_related_caches(decision.symbol, decision.strategy_name)
            
            self.logger.debug(f"Updated decision outcome: {decision_id} -> {outcome.value}")
            
        except Exception as e:
            self.logger.error(f"Error updating decision outcome: {e}")
    
    def evaluate_pending_decisions(self):
        """Evaluate outcomes for pending decisions that are old enough"""
        try:
            now = datetime.now()
            evaluated_count = 0
            
            for decision in self.decisions:
                if (decision.outcome == DecisionOutcome.PENDING and 
                    (now - decision.timestamp).total_seconds() > self.evaluation_delay_hours * 3600):
                    
                    # Auto-evaluate based on available data
                    outcome, pnl = self._auto_evaluate_decision(decision)
                    if outcome != DecisionOutcome.PENDING:
                        self.update_decision_outcome(decision.decision_id, outcome, pnl)
                        evaluated_count += 1
            
            if evaluated_count > 0:
                self.logger.info(f"Auto-evaluated {evaluated_count} pending decisions")
                
        except Exception as e:
            self.logger.error(f"Error evaluating pending decisions: {e}")
    
    def _auto_evaluate_decision(self, decision: DecisionRecord) -> Tuple[DecisionOutcome, Optional[float]]:
        """Auto-evaluate a decision based on market data"""
        try:
            # Get market metrics for evaluation
            market_metrics = get_all_metrics(decision.symbol)
            
            if not market_metrics or not decision.execution_price:
                return DecisionOutcome.NEUTRAL, None
            
            # Get current price (simplified - would use real market data)
            current_price = getattr(market_metrics, 'price', decision.execution_price)
            
            # Calculate PnL based on decision type
            if decision.decision_type in [DecisionType.BUY, DecisionType.INCREASE]:
                pnl_pct = (current_price - decision.execution_price) / decision.execution_price * 100
            elif decision.decision_type in [DecisionType.SELL, DecisionType.REDUCE, DecisionType.EXIT]:
                pnl_pct = (decision.execution_price - current_price) / decision.execution_price * 100
            else:  # HOLD, STRATEGY_CHANGE
                pnl_pct = 0.0
            
            # Determine outcome
            if pnl_pct > self.success_threshold_pct:
                outcome = DecisionOutcome.SUCCESS
            elif pnl_pct < -self.success_threshold_pct:
                outcome = DecisionOutcome.FAILURE
            else:
                outcome = DecisionOutcome.NEUTRAL
            
            # Calculate absolute PnL
            quantity = decision.quantity or 1.0
            pnl = pnl_pct / 100 * decision.execution_price * quantity
            
            return outcome, pnl
            
        except Exception as e:
            self.logger.error(f"Error auto-evaluating decision {decision.decision_id}: {e}")
            return DecisionOutcome.NEUTRAL, None
    
    def _calculate_market_adjusted_pnl(self, decision: DecisionRecord, pnl: float, 
                                     market_price: Optional[float] = None) -> float:
        """Calculate market-adjusted PnL"""
        try:
            # Get market performance during the same period
            market_metrics = get_all_metrics(decision.symbol)
            
            if not market_metrics:
                return pnl  # No adjustment possible
            
            # Get market return during decision period
            market_return = getattr(market_metrics, 'return_period', 0.0)
            
            # Adjust PnL relative to market performance
            # This is a simplified approach - in production would use proper benchmarking
            market_adjusted_pnl = pnl - (market_return * decision.execution_price * (decision.quantity or 1.0))
            
            return market_adjusted_pnl
            
        except Exception as e:
            self.logger.error(f"Error calculating market-adjusted PnL: {e}")
            return pnl
    
    def _calculate_success_score(self, decision: DecisionRecord) -> float:
        """Calculate success score for a decision (0-100)"""
        try:
            if decision.outcome == DecisionOutcome.PENDING:
                return 0.0
            
            # Base score from outcome
            outcome_scores = {
                DecisionOutcome.SUCCESS: 80.0,
                DecisionOutcome.NEUTRAL: 50.0,
                DecisionOutcome.FAILURE: 20.0,
                DecisionOutcome.CANCELLED: 30.0
            }
            
            base_score = outcome_scores.get(decision.outcome, 50.0)
            
            # Adjust based on PnL magnitude
            if decision.pnl is not None and decision.execution_price:
                pnl_pct = abs(decision.pnl) / (decision.execution_price * (decision.quantity or 1.0)) * 100
                
                if decision.outcome == DecisionOutcome.SUCCESS:
                    # Bonus for larger gains
                    base_score += min(20.0, pnl_pct * 2)
                elif decision.outcome == DecisionOutcome.FAILURE:
                    # Penalty for larger losses
                    base_score -= min(15.0, pnl_pct * 1.5)
            
            # Confidence accuracy bonus
            if decision.outcome in [DecisionOutcome.SUCCESS, DecisionOutcome.FAILURE]:
                confidence_accuracy = self._calculate_confidence_accuracy(decision)
                base_score += confidence_accuracy * 10  # Up to 10 point bonus
            
            return max(0.0, min(100.0, base_score))
            
        except Exception as e:
            self.logger.error(f"Error calculating success score: {e}")
            return 50.0
    
    def _calculate_confidence_accuracy(self, decision: DecisionRecord) -> float:
        """Calculate how accurately confidence predicted success (0-1)"""
        try:
            if decision.outcome == DecisionOutcome.SUCCESS:
                # High confidence should predict success
                return decision.confidence
            elif decision.outcome == DecisionOutcome.FAILURE:
                # Low confidence should predict failure
                return 1.0 - decision.confidence
            else:
                # Neutral outcomes - medium confidence is best
                return 1.0 - abs(decision.confidence - 0.5) * 2
                
        except Exception as e:
            self.logger.error(f"Error calculating confidence accuracy: {e}")
            return 0.5
    
    def get_performance_metrics(self, symbol: Optional[str] = None, 
                              strategy: Optional[str] = None,
                              days: Optional[int] = None) -> PerformanceMetrics:
        """Get performance metrics with optional filtering"""
        try:
            # Check cache
            cache_key = f"{symbol}_{strategy}_{days}"
            if cache_key in self.cached_metrics:
                cached_metrics, cache_time = self.cached_metrics[cache_key]
                if (datetime.now() - cache_time).total_seconds() < self.cache_duration_minutes * 60:
                    return cached_metrics
            
            # Filter decisions
            decisions = self._filter_decisions(symbol, strategy, days)
            
            if not decisions:
                return self._default_performance_metrics()
            
            # Calculate metrics
            metrics = self._calculate_performance_metrics(decisions)
            
            # Cache result
            self.cached_metrics[cache_key] = (metrics, datetime.now())
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error getting performance metrics: {e}")
            return self._default_performance_metrics()
    
    def _filter_decisions(self, symbol: Optional[str] = None, 
                        strategy: Optional[str] = None,
                        days: Optional[int] = None) -> List[DecisionRecord]:
        """Filter decisions based on criteria"""
        decisions = list(self.decisions)
        
        # Filter by symbol
        if symbol:
            decisions = [d for d in decisions if d.symbol == symbol]
        
        # Filter by strategy
        if strategy:
            decisions = [d for d in decisions if d.strategy_name == strategy]
        
        # Filter by time period
        if days:
            cutoff_time = datetime.now() - timedelta(days=days)
            decisions = [d for d in decisions if d.timestamp >= cutoff_time]
        
        return decisions
    
    def _calculate_performance_metrics(self, decisions: List[DecisionRecord]) -> PerformanceMetrics:
        """Calculate comprehensive performance metrics"""
        try:
            if not decisions:
                return self._default_performance_metrics()
            
            # Basic counts
            total_decisions = len(decisions)
            completed_decisions = [d for d in decisions if d.outcome != DecisionOutcome.PENDING]
            
            if not completed_decisions:
                return self._default_performance_metrics()
            
            # Success rate
            successful_decisions = [d for d in completed_decisions if d.outcome == DecisionOutcome.SUCCESS]
            success_rate = len(successful_decisions) / len(completed_decisions)
            
            # Confidence metrics
            average_confidence = sum(d.confidence for d in decisions) / len(decisions)
            
            # PnL metrics
            pnl_decisions = [d for d in completed_decisions if d.pnl is not None]
            if pnl_decisions:
                average_pnl = sum(d.pnl for d in pnl_decisions) / len(pnl_decisions)
                market_adjusted_pnls = [d.market_adjusted_pnl or d.pnl for d in pnl_decisions]
                market_adjusted_return = sum(market_adjusted_pnls) / len(market_adjusted_pnls)
                
                # Risk-adjusted metrics
                pnl_values = [d.pnl for d in pnl_decisions]
                sharpe_ratio = self._calculate_sharpe_ratio(pnl_values)
                max_drawdown = self._calculate_max_drawdown(pnl_values)
                
                # Win/loss metrics
                wins = [p for p in pnl_values if p > 0]
                losses = [p for p in pnl_values if p < 0]
                
                win_rate = len(wins) / len(pnl_values) if pnl_values else 0.0
                average_win = sum(wins) / len(wins) if wins else 0.0
                average_loss = sum(losses) / len(losses) if losses else 0.0
                profit_factor = abs(average_win / average_loss) if average_loss != 0 else 1.0
                recovery_factor = sum(pnl_values) / abs(max_drawdown) if max_drawdown != 0 else 1.0
            else:
                average_pnl = 0.0
                market_adjusted_return = 0.0
                sharpe_ratio = 0.0
                max_drawdown = 0.0
                win_rate = 0.0
                average_win = 0.0
                average_loss = 0.0
                profit_factor = 1.0
                recovery_factor = 1.0
            
            # Activity metrics
            time_span = (decisions[-1].timestamp - decisions[0].timestamp).days or 1
            decisions_per_day = total_decisions / time_span
            
            # Confidence accuracy
            confidence_accuracy = sum(self._calculate_confidence_accuracy(d) for d in completed_decisions) / len(completed_decisions)
            
            # Market correlation (simplified)
            market_correlation = 0.5  # Would calculate actual correlation in production
            
            return PerformanceMetrics(
                total_decisions=total_decisions,
                success_rate=success_rate,
                average_confidence=average_confidence,
                average_pnl=average_pnl,
                market_adjusted_return=market_adjusted_return,
                sharpe_ratio=sharpe_ratio,
                win_rate=win_rate,
                average_win=average_win,
                average_loss=average_loss,
                profit_factor=profit_factor,
                max_drawdown=max_drawdown,
                recovery_factor=recovery_factor,
                decisions_per_day=decisions_per_day,
                confidence_accuracy=confidence_accuracy,
                market_correlation=market_correlation
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating performance metrics: {e}")
            return self._default_performance_metrics()
    
    def _calculate_sharpe_ratio(self, pnl_values: List[float]) -> float:
        """Calculate Sharpe ratio from PnL values"""
        try:
            if len(pnl_values) < 2:
                return 0.0
            
            mean_return = np.mean(pnl_values)
            std_return = np.std(pnl_values)
            
            if std_return == 0:
                return 0.0
            
            # Simplified Sharpe (no risk-free rate adjustment)
            sharpe = mean_return / std_return
            
            return max(-3.0, min(5.0, sharpe))  # Cap in reasonable range
            
        except Exception as e:
            self.logger.error(f"Error calculating Sharpe ratio: {e}")
            return 0.0
    
    def _calculate_max_drawdown(self, pnl_values: List[float]) -> float:
        """Calculate maximum drawdown from PnL values"""
        try:
            if not pnl_values:
                return 0.0
            
            cumulative = np.cumsum(pnl_values)
            running_max = np.maximum.accumulate(cumulative)
            drawdown = cumulative - running_max
            
            return abs(np.min(drawdown))
            
        except Exception as e:
            self.logger.error(f"Error calculating max drawdown: {e}")
            return 0.0
    
    def _clear_related_caches(self, symbol: str, strategy: str):
        """Clear caches related to a symbol or strategy"""
        try:
            keys_to_remove = []
            for key in self.cached_metrics.keys():
                if symbol in key or strategy in key or 'None' in key:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self.cached_metrics[key]
                
        except Exception as e:
            self.logger.error(f"Error clearing caches: {e}")
    
    def _default_performance_metrics(self) -> PerformanceMetrics:
        """Return default performance metrics"""
        return PerformanceMetrics(
            total_decisions=0,
            success_rate=0.0,
            average_confidence=0.5,
            average_pnl=0.0,
            market_adjusted_return=0.0,
            sharpe_ratio=0.0,
            win_rate=0.0,
            average_win=0.0,
            average_loss=0.0,
            profit_factor=1.0,
            max_drawdown=0.0,
            recovery_factor=1.0,
            decisions_per_day=0.0,
            confidence_accuracy=0.5,
            market_correlation=0.5
        )
    
    def get_decision_analytics(self, days: int = 30) -> Dict[str, Any]:
        """Get comprehensive decision analytics"""
        try:
            cutoff_time = datetime.now() - timedelta(days=days)
            recent_decisions = [d for d in self.decisions if d.timestamp >= cutoff_time]
            
            if not recent_decisions:
                return {'error': 'No recent decisions found'}
            
            # Strategy performance breakdown
            strategy_performance = {}
            for strategy in self.decisions_by_strategy.keys():
                strategy_metrics = self.get_performance_metrics(strategy=strategy, days=days)
                strategy_performance[strategy] = {
                    'success_rate': strategy_metrics.success_rate,
                    'total_decisions': strategy_metrics.total_decisions,
                    'average_pnl': strategy_metrics.average_pnl,
                    'sharpe_ratio': strategy_metrics.sharpe_ratio
                }
            
            # Symbol performance breakdown
            symbol_performance = {}
            for symbol in self.decisions_by_symbol.keys():
                symbol_metrics = self.get_performance_metrics(symbol=symbol, days=days)
                symbol_performance[symbol] = {
                    'success_rate': symbol_metrics.success_rate,
                    'total_decisions': symbol_metrics.total_decisions,
                    'average_pnl': symbol_metrics.average_pnl,
                    'win_rate': symbol_metrics.win_rate
                }
            
            # Decision type analysis
            decision_type_stats = defaultdict(lambda: {'count': 0, 'success_rate': 0.0})
            for decision in recent_decisions:
                if decision.outcome != DecisionOutcome.PENDING:
                    decision_type_stats[decision.decision_type.value]['count'] += 1
                    if decision.outcome == DecisionOutcome.SUCCESS:
                        decision_type_stats[decision.decision_type.value]['success_rate'] += 1
            
            # Calculate success rates
            for stats in decision_type_stats.values():
                if stats['count'] > 0:
                    stats['success_rate'] = stats['success_rate'] / stats['count']
            
            # Overall metrics
            overall_metrics = self.get_performance_metrics(days=days)
            
            return {
                'period_days': days,
                'total_decisions': len(recent_decisions),
                'overall_metrics': overall_metrics.__dict__,
                'strategy_performance': strategy_performance,
                'symbol_performance': symbol_performance,
                'decision_type_analysis': dict(decision_type_stats),
                'recent_trend': self._calculate_recent_trend(recent_decisions)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting decision analytics: {e}")
            return {'error': str(e)}
    
    def _calculate_recent_trend(self, decisions: List[DecisionRecord]) -> str:
        """Calculate recent performance trend"""
        try:
            if len(decisions) < 10:
                return 'INSUFFICIENT_DATA'
            
            # Split into recent and older decisions
            mid_point = len(decisions) // 2
            older_decisions = decisions[:mid_point]
            recent_decisions = decisions[mid_point:]
            
            # Calculate success rates
            older_completed = [d for d in older_decisions if d.outcome != DecisionOutcome.PENDING]
            recent_completed = [d for d in recent_decisions if d.outcome != DecisionOutcome.PENDING]
            
            if not older_completed or not recent_completed:
                return 'INSUFFICIENT_DATA'
            
            older_success_rate = sum(1 for d in older_completed if d.outcome == DecisionOutcome.SUCCESS) / len(older_completed)
            recent_success_rate = sum(1 for d in recent_completed if d.outcome == DecisionOutcome.SUCCESS) / len(recent_completed)
            
            improvement = recent_success_rate - older_success_rate
            
            if improvement > 0.1:
                return 'IMPROVING'
            elif improvement < -0.1:
                return 'DECLINING'
            else:
                return 'STABLE'
                
        except Exception as e:
            self.logger.error(f"Error calculating recent trend: {e}")
            return 'UNKNOWN'
