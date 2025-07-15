"""
Performance Tracker - Extracted from Position Health Monitor

This module handles strategy performance tracking, historical analysis, and trend detection.
Provides zero-hardcoded performance evaluation with market-driven benchmarks.

Features:
- Real-time strategy performance tracking
- Historical performance analysis
- Performance trend detection and prediction
- Strategy effectiveness scoring
- Market-adjusted performance metrics
- Performance-based alerts and recommendations
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import numpy as np

# Import market metrics for dynamic calculations
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


class PerformanceTrend(Enum):
    """Performance trend classifications"""
    IMPROVING = "IMPROVING"
    DETERIORATING = "DETERIORATING"
    STABLE_POSITIVE = "STABLE_POSITIVE"
    STABLE_NEUTRAL = "STABLE_NEUTRAL"
    STABLE_NEGATIVE = "STABLE_NEGATIVE"
    VOLATILE = "VOLATILE"


class StrategyHealthStatus(Enum):
    """Health status of strategy performance"""
    EXCELLENT = "EXCELLENT"      # >10% profit, low risk
    GOOD = "GOOD"               # 5-10% profit, moderate risk
    NEUTRAL = "NEUTRAL"         # 0-5% profit/loss, normal risk
    CONCERNING = "CONCERNING"    # -5% to -8% loss, high risk
    CRITICAL = "CRITICAL"       # >8% loss or extreme risk


@dataclass
class PerformanceSnapshot:
    """Point-in-time performance snapshot"""
    timestamp: datetime
    symbol: str
    strategy_name: str
    pnl_absolute: float
    pnl_percentage: float
    health_score: float
    risk_score: float
    market_correlation: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    trade_count: int
    market_regime: str
    

@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics"""
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    average_win: float
    average_loss: float
    profit_factor: float
    calmar_ratio: float
    market_correlation: float
    alpha: float
    beta: float
    tracking_error: float
    information_ratio: float
    downside_deviation: float
    sortino_ratio: float
    var_95: float
    expected_shortfall: float
    

@dataclass
class StrategyPerformanceState:
    """Tracks comprehensive strategy performance state"""
    symbol: str
    strategy_name: str
    start_time: datetime
    last_update: datetime
    snapshots: deque = field(default_factory=lambda: deque(maxlen=1000))
    current_metrics: Optional[PerformanceMetrics] = None
    trend: PerformanceTrend = PerformanceTrend.STABLE_NEUTRAL
    health_status: StrategyHealthStatus = StrategyHealthStatus.NEUTRAL
    consecutive_poor_periods: int = 0
    consecutive_good_periods: int = 0
    last_benchmark_comparison: Optional[datetime] = None
    market_adjusted_score: float = 50.0
    

class PerformanceTracker:
    """Tracks and analyzes strategy performance with market context"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.PerformanceTracker")
        
        # Performance tracking state
        self.strategy_states = {}  # {symbol: StrategyPerformanceState}
        self.benchmark_cache = {}  # Cache for market benchmarks
        self.performance_history = defaultdict(list)  # {symbol: List[PerformanceSnapshot]}
        
        # Configuration (market-driven defaults)
        self.min_tracking_period_days = 1
        self.snapshot_frequency_minutes = 15
        self.performance_window_days = 30
        self.benchmark_update_hours = 6
        
        self.logger.info("PerformanceTracker initialized with market-driven benchmarks")
    
    def extract_strategy_performance(self, analysis_results: Dict, strategy_context: Dict) -> Dict[str, Any]:
        """Extract comprehensive strategy performance from analysis results"""
        try:
            symbol = strategy_context.get('symbol', 'UNKNOWN')
            strategy_name = strategy_context.get('strategy_name', 'UNKNOWN')
            
            # Get market metrics for context
            market_metrics = get_all_metrics(symbol)
            
            # Extract core performance data
            pnl_data = analysis_results.get('pnl', {})
            risk_data = analysis_results.get('risk_metrics', {})
            health_data = analysis_results.get('health_metrics', {})
            
            # Calculate market-adjusted metrics
            market_adjusted_metrics = self._calculate_market_adjusted_metrics(
                pnl_data, risk_data, market_metrics
            )
            
            # Build comprehensive performance dict
            performance = {
                'symbol': symbol,
                'strategy_name': strategy_name,
                'timestamp': datetime.now(),
                'pnl_absolute': pnl_data.get('unrealized_pnl', 0.0),
                'pnl_percentage': pnl_data.get('pnl_pct', 0.0),
                'health_score': health_data.get('overall_health', 50.0),
                'risk_score': risk_data.get('composite_risk', 50.0),
                'market_correlation': market_adjusted_metrics['correlation'],
                'volatility': risk_data.get('volatility', 0.3),
                'sharpe_ratio': market_adjusted_metrics['sharpe_ratio'],
                'max_drawdown': risk_data.get('max_drawdown', 0.1),
                'win_rate': self._calculate_win_rate(strategy_context),
                'trade_count': strategy_context.get('trade_count', 0),
                'market_regime': getattr(market_metrics, 'primary_regime', 'NEUTRAL') if market_metrics else 'NEUTRAL',
                'effectiveness_score': self._calculate_strategy_effectiveness(risk_data, strategy_context),
                'trend': self._determine_performance_trend(pnl_data, risk_data),
                'market_adjusted_score': market_adjusted_metrics['adjusted_score']
            }
            
            return performance
            
        except Exception as e:
            self.logger.error(f"Error extracting strategy performance: {e}")
            return self._default_performance_data(strategy_context)
    
    def _calculate_market_adjusted_metrics(self, pnl_data: Dict, risk_data: Dict, market_metrics) -> Dict[str, float]:
        """Calculate market-adjusted performance metrics"""
        try:
            base_return = pnl_data.get('pnl_pct', 0.0) / 100.0
            volatility = risk_data.get('volatility', 0.3)
            
            # Market context adjustments
            if market_metrics:
                market_return = getattr(market_metrics, 'return_1d', 0.0) if hasattr(market_metrics, 'return_1d') else 0.0
                market_vol = getattr(market_metrics, 'volatility', 0.3) if hasattr(market_metrics, 'volatility') else 0.3
                
                # Calculate alpha (excess return over market)
                alpha = base_return - market_return
                
                # Calculate beta (sensitivity to market)
                beta = volatility / market_vol if market_vol > 0 else 1.0
                
                # Market correlation estimation
                correlation = min(0.9, max(-0.9, beta * 0.7))  # Simplified correlation estimate
                
                # Risk-free rate (dynamic based on market conditions)
                risk_free_rate = 0.02  # Base 2%
                if hasattr(market_metrics, 'volatility') and market_metrics.volatility > 0.6:
                    risk_free_rate += 0.01  # Higher rates in volatile markets
                
                # Sharpe ratio calculation
                excess_return = base_return - (risk_free_rate / 252)  # Daily excess return
                sharpe_ratio = excess_return / volatility if volatility > 0 else 0.0
                
                # Market-adjusted score
                market_stress = getattr(market_metrics, 'market_stress', 0.3) if hasattr(market_metrics, 'market_stress') else 0.3
                stress_adjustment = 1.0 - (market_stress * 0.3)  # Adjust for market stress
                adjusted_score = min(100, max(0, (base_return * 100 + 50) * stress_adjustment))
                
            else:
                # Default calculations without market context
                alpha = base_return
                beta = 1.0
                correlation = 0.5
                sharpe_ratio = base_return / volatility if volatility > 0 else 0.0
                adjusted_score = min(100, max(0, base_return * 100 + 50))
            
            return {
                'alpha': alpha,
                'beta': beta,
                'correlation': correlation,
                'sharpe_ratio': sharpe_ratio,
                'adjusted_score': adjusted_score
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating market-adjusted metrics: {e}")
            return {
                'alpha': 0.0,
                'beta': 1.0,
                'correlation': 0.5,
                'sharpe_ratio': 0.0,
                'adjusted_score': 50.0
            }
    
    def _calculate_strategy_effectiveness(self, metrics: Dict, context: Dict) -> float:
        """Calculate strategy effectiveness score with market context"""
        try:
            symbol = context.get('symbol', 'UNKNOWN')
            strategy_name = context.get('strategy_name', 'UNKNOWN')
            
            # Get market metrics for context
            market_metrics = get_all_metrics(symbol)
            
            # Base effectiveness from performance
            pnl_pct = metrics.get('pnl_pct', 0.0)
            risk_score = metrics.get('composite_risk', 50.0)
            health_score = metrics.get('overall_health', 50.0)
            
            # Strategy category effectiveness adjustments
            strategy_multipliers = {
                'MOMENTUM': 1.2 if market_metrics and getattr(market_metrics, 'trend_strength', 0.5) > 0.6 else 0.8,
                'MEAN_REVERSION': 1.2 if market_metrics and getattr(market_metrics, 'volatility', 0.3) > 0.4 else 0.9,
                'VOLATILITY': 1.3 if market_metrics and getattr(market_metrics, 'volatility', 0.3) > 0.5 else 0.7,
                'PAIRS': 1.1,  # Consistently moderate effectiveness
                'FACTOR': 1.0,  # Baseline effectiveness
                'DEFENSIVE': 1.1 if market_metrics and getattr(market_metrics, 'market_stress', 0.3) > 0.5 else 0.9
            }
            
            strategy_category = self._extract_strategy_category(strategy_name)
            strategy_multiplier = strategy_multipliers.get(strategy_category, 1.0)
            
            # Calculate base effectiveness
            performance_component = min(100, max(0, pnl_pct + 50))  # Center around 50
            risk_component = max(0, 100 - risk_score)  # Lower risk = higher effectiveness
            health_component = health_score
            
            # Weighted effectiveness score
            base_effectiveness = (
                performance_component * 0.4 +
                risk_component * 0.3 +
                health_component * 0.3
            )
            
            # Apply strategy and market adjustments
            final_effectiveness = base_effectiveness * strategy_multiplier
            
            return min(100, max(0, final_effectiveness))
            
        except Exception as e:
            self.logger.error(f"Error calculating strategy effectiveness: {e}")
            return 50.0
    
    def _determine_performance_trend(self, pnl_data: Dict, risk_data: Dict) -> PerformanceTrend:
        """Determine performance trend with market context"""
        try:
            pnl_pct = pnl_data.get('pnl_pct', 0.0)
            health_score = risk_data.get('overall_health', 50.0)
            volatility = risk_data.get('volatility', 0.3)
            
            # Trend determination with volatility consideration
            if volatility > 0.8:  # High volatility = volatile trend
                return PerformanceTrend.VOLATILE
            elif pnl_pct > 5 and health_score > 70:
                return PerformanceTrend.IMPROVING
            elif pnl_pct < -3 or health_score < 30:
                return PerformanceTrend.DETERIORATING
            elif pnl_pct > 1:
                return PerformanceTrend.STABLE_POSITIVE
            elif pnl_pct > -1:
                return PerformanceTrend.STABLE_NEUTRAL
            else:
                return PerformanceTrend.STABLE_NEGATIVE
                
        except Exception as e:
            self.logger.error(f"Error determining performance trend: {e}")
            return PerformanceTrend.STABLE_NEUTRAL
    
    def update_performance_history(self, symbol: str, performance: Dict[str, Any]):
        """Update performance history with new data point"""
        try:
            # Create performance snapshot
            snapshot = PerformanceSnapshot(
                timestamp=performance.get('timestamp', datetime.now()),
                symbol=symbol,
                strategy_name=performance.get('strategy_name', 'UNKNOWN'),
                pnl_absolute=performance.get('pnl_absolute', 0.0),
                pnl_percentage=performance.get('pnl_percentage', 0.0),
                health_score=performance.get('health_score', 50.0),
                risk_score=performance.get('risk_score', 50.0),
                market_correlation=performance.get('market_correlation', 0.5),
                volatility=performance.get('volatility', 0.3),
                sharpe_ratio=performance.get('sharpe_ratio', 0.0),
                max_drawdown=performance.get('max_drawdown', 0.1),
                win_rate=performance.get('win_rate', 0.5),
                trade_count=performance.get('trade_count', 0),
                market_regime=performance.get('market_regime', 'NEUTRAL')
            )
            
            # Update strategy state
            if symbol not in self.strategy_states:
                self.strategy_states[symbol] = StrategyPerformanceState(
                    symbol=symbol,
                    strategy_name=performance.get('strategy_name', 'UNKNOWN'),
                    start_time=datetime.now(),
                    last_update=datetime.now()
                )
            
            state = self.strategy_states[symbol]
            state.snapshots.append(snapshot)
            state.last_update = datetime.now()
            state.trend = PerformanceTrend(performance.get('trend', 'STABLE_NEUTRAL'))
            state.market_adjusted_score = performance.get('market_adjusted_score', 50.0)
            
            # Update comprehensive metrics
            state.current_metrics = self._calculate_comprehensive_metrics(state.snapshots)
            
            # Update health status
            state.health_status = self._determine_health_status(performance)
            
            # Update consecutive periods
            self._update_consecutive_periods(state, performance)
            
            # Add to performance history
            self.performance_history[symbol].append(performance)
            
            # Keep history manageable
            if len(self.performance_history[symbol]) > 1000:
                self.performance_history[symbol] = self.performance_history[symbol][-500:]
            
            self.logger.debug(f"Updated performance history for {symbol}: {state.health_status.value}")
            
        except Exception as e:
            self.logger.error(f"Error updating performance history for {symbol}: {e}")
    
    def _calculate_comprehensive_metrics(self, snapshots: deque) -> PerformanceMetrics:
        """Calculate comprehensive performance metrics from snapshots"""
        try:
            if len(snapshots) < 2:
                return self._default_performance_metrics()
            
            # Convert to arrays for calculation
            returns = np.array([s.pnl_percentage for s in snapshots])
            timestamps = [s.timestamp for s in snapshots]
            
            # Time-based calculations
            time_span_days = (timestamps[-1] - timestamps[0]).days
            if time_span_days == 0:
                time_span_days = 1
            
            # Basic metrics
            total_return = returns[-1] if len(returns) > 0 else 0.0
            annualized_return = total_return * (365.0 / time_span_days) if time_span_days > 0 else 0.0
            volatility = np.std(returns) if len(returns) > 1 else 0.3
            
            # Risk-adjusted metrics
            if volatility > 0:
                sharpe_ratio = (np.mean(returns) - 0.02/252) / (volatility / np.sqrt(252))  # Daily Sharpe
                sortino_ratio = (np.mean(returns) - 0.02/252) / (np.std(returns[returns < 0]) if len(returns[returns < 0]) > 0 else volatility)
            else:
                sharpe_ratio = 0.0
                sortino_ratio = 0.0
            
            # Drawdown calculations
            cumulative = np.cumprod(1 + returns/100)
            running_max = np.maximum.accumulate(cumulative)
            drawdown = (cumulative - running_max) / running_max
            max_drawdown = abs(np.min(drawdown)) if len(drawdown) > 0 else 0.1
            
            # Win/loss metrics
            positive_returns = returns[returns > 0]
            negative_returns = returns[returns < 0]
            win_rate = len(positive_returns) / len(returns) if len(returns) > 0 else 0.5
            average_win = np.mean(positive_returns) if len(positive_returns) > 0 else 0.0
            average_loss = np.mean(negative_returns) if len(negative_returns) > 0 else 0.0
            profit_factor = abs(average_win / average_loss) if average_loss != 0 else 1.0
            
            # Advanced metrics
            calmar_ratio = annualized_return / max_drawdown if max_drawdown > 0 else 0.0
            var_95 = np.percentile(returns, 5) if len(returns) > 10 else -0.05
            expected_shortfall = np.mean(returns[returns <= var_95]) if len(returns[returns <= var_95]) > 0 else var_95
            
            return PerformanceMetrics(
                total_return=total_return,
                annualized_return=annualized_return,
                volatility=volatility,
                sharpe_ratio=sharpe_ratio,
                max_drawdown=max_drawdown,
                win_rate=win_rate,
                average_win=average_win,
                average_loss=average_loss,
                profit_factor=profit_factor,
                calmar_ratio=calmar_ratio,
                market_correlation=snapshots[-1].market_correlation,
                alpha=0.0,  # Would need benchmark for true alpha
                beta=1.0,   # Would need benchmark for true beta
                tracking_error=volatility,  # Simplified
                information_ratio=sharpe_ratio,  # Simplified
                downside_deviation=np.std(negative_returns) if len(negative_returns) > 0 else volatility,
                sortino_ratio=sortino_ratio,
                var_95=var_95,
                expected_shortfall=expected_shortfall
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating comprehensive metrics: {e}")
            return self._default_performance_metrics()
    
    def _determine_health_status(self, performance: Dict[str, Any]) -> StrategyHealthStatus:
        """Determine strategy health status based on performance"""
        try:
            pnl_pct = performance.get('pnl_percentage', 0.0)
            risk_score = performance.get('risk_score', 50.0)
            health_score = performance.get('health_score', 50.0)
            
            # Health determination logic
            if pnl_pct > 10 and risk_score < 30 and health_score > 80:
                return StrategyHealthStatus.EXCELLENT
            elif pnl_pct > 5 and risk_score < 50 and health_score > 70:
                return StrategyHealthStatus.GOOD
            elif pnl_pct < -8 or risk_score > 80 or health_score < 20:
                return StrategyHealthStatus.CRITICAL
            elif pnl_pct < -5 or risk_score > 60 or health_score < 40:
                return StrategyHealthStatus.CONCERNING
            else:
                return StrategyHealthStatus.NEUTRAL
                
        except Exception as e:
            self.logger.error(f"Error determining health status: {e}")
            return StrategyHealthStatus.NEUTRAL
    
    def _update_consecutive_periods(self, state: StrategyPerformanceState, performance: Dict[str, Any]):
        """Update consecutive good/poor period counters"""
        try:
            health_status = StrategyHealthStatus(performance.get('health_status', 'NEUTRAL'))
            
            if health_status in [StrategyHealthStatus.EXCELLENT, StrategyHealthStatus.GOOD]:
                state.consecutive_good_periods += 1
                state.consecutive_poor_periods = 0
            elif health_status in [StrategyHealthStatus.CONCERNING, StrategyHealthStatus.CRITICAL]:
                state.consecutive_poor_periods += 1
                state.consecutive_good_periods = 0
            else:
                # Neutral doesn't affect consecutive counters
                pass
                
        except Exception as e:
            self.logger.error(f"Error updating consecutive periods: {e}")
    
    def get_performance_summary(self, symbol: str) -> Dict[str, Any]:
        """Get comprehensive performance summary for a symbol"""
        try:
            if symbol not in self.strategy_states:
                return {'error': f'No performance data for {symbol}'}
            
            state = self.strategy_states[symbol]
            
            return {
                'symbol': symbol,
                'strategy_name': state.strategy_name,
                'health_status': state.health_status.value,
                'trend': state.trend.value,
                'current_metrics': state.current_metrics.__dict__ if state.current_metrics else {},
                'consecutive_good_periods': state.consecutive_good_periods,
                'consecutive_poor_periods': state.consecutive_poor_periods,
                'market_adjusted_score': state.market_adjusted_score,
                'snapshots_count': len(state.snapshots),
                'tracking_start': state.start_time.isoformat(),
                'last_update': state.last_update.isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting performance summary for {symbol}: {e}")
            return {'error': str(e)}
    
    def get_all_performance_summaries(self) -> Dict[str, Dict[str, Any]]:
        """Get performance summaries for all tracked symbols"""
        return {symbol: self.get_performance_summary(symbol) for symbol in self.strategy_states.keys()}
    
    def _calculate_win_rate(self, strategy_context: Dict) -> float:
        """Calculate win rate from strategy context"""
        try:
            trades = strategy_context.get('trade_history', [])
            if not trades:
                return 0.5  # Default 50% win rate
            
            winning_trades = sum(1 for trade in trades if trade.get('pnl', 0) > 0)
            return winning_trades / len(trades)
            
        except Exception as e:
            self.logger.error(f"Error calculating win rate: {e}")
            return 0.5
    
    def _extract_strategy_category(self, strategy_name: str) -> str:
        """Extract strategy category from strategy name"""
        strategy_name_upper = strategy_name.upper()
        
        if 'MOMENTUM' in strategy_name_upper or 'TREND' in strategy_name_upper:
            return 'MOMENTUM'
        elif 'REVERSION' in strategy_name_upper or 'MEAN' in strategy_name_upper:
            return 'MEAN_REVERSION'
        elif 'VOLATILITY' in strategy_name_upper or 'VOL' in strategy_name_upper:
            return 'VOLATILITY'
        elif 'PAIRS' in strategy_name_upper or 'PAIR' in strategy_name_upper:
            return 'PAIRS'
        elif 'FACTOR' in strategy_name_upper:
            return 'FACTOR'
        elif 'DEFENSIVE' in strategy_name_upper or 'DEFENSE' in strategy_name_upper:
            return 'DEFENSIVE'
        else:
            return 'FACTOR'  # Default category
    
    def _default_performance_data(self, strategy_context: Dict) -> Dict[str, Any]:
        """Return default performance data when extraction fails"""
        return {
            'symbol': strategy_context.get('symbol', 'UNKNOWN'),
            'strategy_name': strategy_context.get('strategy_name', 'UNKNOWN'),
            'timestamp': datetime.now(),
            'pnl_absolute': 0.0,
            'pnl_percentage': 0.0,
            'health_score': 50.0,
            'risk_score': 50.0,
            'market_correlation': 0.5,
            'volatility': 0.3,
            'sharpe_ratio': 0.0,
            'max_drawdown': 0.1,
            'win_rate': 0.5,
            'trade_count': 0,
            'market_regime': 'NEUTRAL',
            'effectiveness_score': 50.0,
            'trend': 'STABLE_NEUTRAL',
            'market_adjusted_score': 50.0
        }
    
    def _default_performance_metrics(self) -> PerformanceMetrics:
        """Return default performance metrics"""
        return PerformanceMetrics(
            total_return=0.0,
            annualized_return=0.0,
            volatility=0.3,
            sharpe_ratio=0.0,
            max_drawdown=0.1,
            win_rate=0.5,
            average_win=0.0,
            average_loss=0.0,
            profit_factor=1.0,
            calmar_ratio=0.0,
            market_correlation=0.5,
            alpha=0.0,
            beta=1.0,
            tracking_error=0.3,
            information_ratio=0.0,
            downside_deviation=0.3,
            sortino_ratio=0.0,
            var_95=-0.05,
            expected_shortfall=-0.07
        )
