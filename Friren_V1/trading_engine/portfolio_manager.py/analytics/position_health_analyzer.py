"""
portfolio_manager/analytics/position_health_analyzer.py

Optimized position health analysis - leverages existing risk_analyzer,
market_analyzer, and sentiment_analyzer for efficiency.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging

# Import existing analyzers
from risk_analyzer import RiskAnalyzer, PositionRisk
from market_analyzer import MarketAnalyzer, MarketRegimeResult
from sentiment_analyzer import SentimentAnalyzer, SentimentReading


class StrategyStatus(Enum):
    """Status of active strategies"""
    ACTIVE = "ACTIVE"
    MONITORING = "MONITORING"
    SCALING = "SCALING"
    EXITING = "EXITING"
    COMPLETED = "COMPLETED"
    STOPPED = "STOPPED"


@dataclass
class ActiveStrategy:
    """Lightweight active strategy tracking"""
    symbol: str
    strategy_type: str
    entry_time: datetime
    entry_price: float
    current_price: float = 0.0
    position_size: float = 0.0
    target_size: float = 0.0
    status: StrategyStatus = StrategyStatus.ACTIVE

    # Performance tracking
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    high_water_mark: float = 0.0
    max_drawdown: float = 0.0

    # Strategy-specific parameters
    parameters: Dict[str, Any] = field(default_factory=dict)
    exit_conditions: Dict[str, Any] = field(default_factory=dict)

    def update_metrics(self, current_price: float):
        """Update strategy metrics with current price"""
        self.current_price = current_price

        # Update P&L
        if self.position_size > 0:
            self.unrealized_pnl = (current_price - self.entry_price) * self.position_size
            self.unrealized_pnl_pct = ((current_price / self.entry_price) - 1) * 100

            # Update high water mark and drawdown
            if current_price > self.high_water_mark:
                self.high_water_mark = current_price

            current_drawdown = ((current_price - self.high_water_mark) / self.high_water_mark) * 100
            self.max_drawdown = min(self.max_drawdown, current_drawdown)

    def time_in_position(self) -> timedelta:
        """Calculate time since entry"""
        return datetime.now() - self.entry_time


@dataclass
class PositionHealthAlert:
    """Simplified alert structure"""
    symbol: str
    alert_type: str  # 'EXIT', 'SCALE_DOWN', 'SCALE_UP', 'RISK_WARNING'
    severity: str  # 'INFO', 'WARNING', 'CRITICAL'
    message: str
    action_required: str
    metrics: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class PositionHealthResult:
    """Streamlined health result"""
    symbol: str
    strategy_type: str
    analysis_type: str
    success: bool

    # Simplified results
    should_exit: bool = False
    exit_reason: str = ""
    scaling_action: str = "HOLD"
    scaling_reason: str = ""
    new_size: float = 0.0
    health_score: float = 50.0  # 0-100

    alerts: List[PositionHealthAlert] = field(default_factory=list)
    current_metrics: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


class OptimizedPositionHealthAnalyzer:
    """
    Optimized Position Health Analyzer

    Leverages existing analyzers to avoid duplication:
    - RiskAnalyzer: For comprehensive risk assessment
    - MarketAnalyzer: For regime-based adjustments
    - SentimentAnalyzer: For sentiment-based risk factors
    - PositionSizer: For scaling recommendations

    This analyzer focuses on:
    - Orchestrating existing components
    - Exit condition logic
    - Health scoring and alerting
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._default_config()
        self.logger = logging.getLogger("optimized_position_health")

        # Initialize existing analyzers
        self.risk_analyzer = RiskAnalyzer()
        self.market_analyzer = MarketAnalyzer()
        self.sentiment_analyzer = SentimentAnalyzer()

        # Strategy instances (loaded lazily)
        self._strategy_map = None

        self.logger.info("OptimizedPositionHealthAnalyzer initialized with existing analyzers")

    def _default_config(self) -> Dict[str, Any]:
        """Simplified configuration"""
        return {
            'exit_conditions': {
                'max_holding_days': 30,
                'profit_target_pct': 15.0,
                'stop_loss_pct': -8.0,
                'trailing_stop_pct': -5.0
            },
            'scaling_thresholds': {
                'performance_threshold': 5.0,  # 5% profit for scale up
                'risk_threshold': 50.0,        # Risk score > 50 for scale down
                'vol_threshold': 0.35          # High volatility threshold
            },
            'health_weights': {
                'performance': 0.3,
                'risk': 0.4,
                'time': 0.2,
                'sentiment': 0.1
            }
        }

    def analyze_position_comprehensive(self, active_strategy: ActiveStrategy,
                                     market_data: pd.DataFrame) -> Dict[str, PositionHealthResult]:
        """
        Single comprehensive analysis using all existing analyzers

        Returns all analysis types in one efficient call
        """
        try:
            current_price = market_data['Close'].iloc[-1]
            active_strategy.update_metrics(current_price)

            # Get market context using existing analyzers
            market_regime = self._get_market_regime(market_data)
            sentiment = self._get_sentiment(active_strategy.symbol)

            # Use RiskAnalyzer for comprehensive risk assessment
            position_risk = self._get_position_risk(active_strategy, market_data, market_regime, sentiment)

            # Analyze all aspects using the risk analysis
            results = {}

            # Exit analysis
            results['exit'] = self._analyze_exit_with_risk(active_strategy, position_risk, market_regime)

            # Scaling analysis
            results['scaling'] = self._analyze_scaling_with_risk(active_strategy, position_risk, market_regime)

            # Risk analysis (already done)
            results['risk'] = self._create_risk_result(active_strategy, position_risk)

            # Metrics analysis
            results['metrics'] = self._create_metrics_result(active_strategy, position_risk)

            return results

        except Exception as e:
            self.logger.error(f"Error in comprehensive analysis for {active_strategy.symbol}: {e}")
            return self._create_error_results(active_strategy, str(e))

    def _get_market_regime(self, market_data: pd.DataFrame) -> MarketRegimeResult:
        """Get market regime using existing MarketAnalyzer"""
        try:
            return self.market_analyzer.analyze_market_regime(market_data)
        except Exception as e:
            self.logger.warning(f"Failed to get market regime: {e}")
            # Return default regime
            from .market_analyzer import MarketRegimeResult
            return MarketRegimeResult(
                primary_regime='UNKNOWN',
                regime_confidence=50.0,
                trend='SIDEWAYS',
                trend_strength=0.5,
                volatility_regime='NORMAL_VOLATILITY',
                current_volatility=0.2,
                rsi_condition='NEUTRAL',
                current_rsi=50.0,
                enhanced_regime='UNKNOWN',
                enhanced_confidence=50.0,
                entropy_regime='UNKNOWN',
                entropy_confidence=50.0,
                entropy_measures={},
                regime_persistence=0.5,
                regime_transition_probability=0.3,
                regime_scores={},
                market_stress_level=25.0,
                regime_consistency=0.5
            )

    def _get_sentiment(self, symbol: str) -> SentimentReading:
        """Get sentiment using existing SentimentAnalyzer"""
        try:
            return self.sentiment_analyzer.analyze_sentiment(symbol)
        except Exception as e:
            self.logger.warning(f"Failed to get sentiment for {symbol}: {e}")
            # Return neutral sentiment
            from .sentiment_analyzer import SentimentReading
            return SentimentReading(
                symbol=symbol,
                sentiment_score=0.0,
                confidence=50.0,
                article_count=0,
                sentiment_trend='NEUTRAL',
                sentiment_momentum=0.0,
                news_volume=0,
                timestamp=datetime.now()
            )

    def _get_position_risk(self, active_strategy: ActiveStrategy, market_data: pd.DataFrame,
                          market_regime: MarketRegimeResult, sentiment: SentimentReading) -> PositionRisk:
        """Use existing RiskAnalyzer for comprehensive risk assessment"""
        try:
            # Create a mock strategy recommendation for RiskAnalyzer
            from .strategy_selector import StrategyRecommendation
            from ..strategies.base_strategy import StrategySignal, StrategyMetadata

            # Create mock signal based on current position
            signal = StrategySignal(
                action='HOLD',  # We're analyzing existing position
                confidence=75.0,
                reasoning=f"Monitoring {active_strategy.strategy_type} position"
            )

            # Create mock metadata
            metadata = StrategyMetadata(
                name=active_strategy.strategy_type,
                category=self._get_strategy_category(active_strategy.strategy_type),
                typical_holding_days=self.config['exit_conditions']['max_holding_days'],
                works_best_in=market_regime.primary_regime,
                min_confidence=60.0
            )

            strategy_rec = StrategyRecommendation(
                strategy_name=active_strategy.strategy_type,
                signal=signal,
                strategy_metadata=metadata,
                market_fit_score=75.0,
                selection_confidence=75.0
            )

            # Use RiskAnalyzer for comprehensive analysis
            return self.risk_analyzer.analyze_position_risk(
                symbol=active_strategy.symbol,
                df=market_data,
                strategy_rec=strategy_rec,
                market_regime=market_regime,
                sentiment=sentiment,
                position_size=active_strategy.position_size
            )

        except Exception as e:
            self.logger.error(f"Error getting position risk: {e}")
            # Return minimal risk assessment
            return self._create_minimal_risk(active_strategy)

    def _analyze_exit_with_risk(self, active_strategy: ActiveStrategy,
                               position_risk: PositionRisk,
                               market_regime: MarketRegimeResult) -> PositionHealthResult:
        """Analyze exit conditions using risk assessment"""

        alerts = []
        should_exit = False
        exit_reason = ""

        # 1. Risk-based exit (use RiskAnalyzer results)
        if position_risk.total_risk_score > 75:  # High risk
            should_exit = True
            exit_reason = f"High risk score ({position_risk.total_risk_score:.0f})"
            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='EXIT',
                severity='CRITICAL',
                message=f"Risk score too high: {position_risk.total_risk_score:.0f}",
                action_required='SELL',
                metrics={'risk_score': position_risk.total_risk_score}
            ))

        # 2. Time-based exit
        time_in_position = active_strategy.time_in_position()
        max_holding = active_strategy.exit_conditions.get(
            'max_holding_days',
            self.config['exit_conditions']['max_holding_days']
        )

        if time_in_position.days >= max_holding:
            should_exit = True
            exit_reason = f"Max holding period reached ({time_in_position.days} days)"
            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='EXIT',
                severity='INFO',
                message=exit_reason,
                action_required='SELL',
                metrics={'days_held': time_in_position.days}
            ))

        # 3. Performance-based exit
        profit_target = active_strategy.exit_conditions.get(
            'profit_target_pct',
            self.config['exit_conditions']['profit_target_pct']
        )
        stop_loss = active_strategy.exit_conditions.get(
            'stop_loss_pct',
            self.config['exit_conditions']['stop_loss_pct']
        )

        if active_strategy.unrealized_pnl_pct >= profit_target:
            should_exit = True
            exit_reason = f"Profit target reached ({active_strategy.unrealized_pnl_pct:.1f}%)"
            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='EXIT',
                severity='INFO',
                message=exit_reason,
                action_required='SELL',
                metrics={'profit_pct': active_strategy.unrealized_pnl_pct}
            ))
        elif active_strategy.unrealized_pnl_pct <= stop_loss:
            should_exit = True
            exit_reason = f"Stop loss triggered ({active_strategy.unrealized_pnl_pct:.1f}%)"
            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='EXIT',
                severity='CRITICAL',
                message=exit_reason,
                action_required='SELL',
                metrics={'loss_pct': active_strategy.unrealized_pnl_pct}
            ))

        # 4. Regime-based exit (use MarketAnalyzer results)
        if market_regime.market_stress_level > 80:
            should_exit = True
            exit_reason = f"High market stress ({market_regime.market_stress_level:.0f})"
            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='EXIT',
                severity='WARNING',
                message=exit_reason,
                action_required='SELL',
                metrics={'stress_level': market_regime.market_stress_level}
            ))

        return PositionHealthResult(
            symbol=active_strategy.symbol,
            strategy_type=active_strategy.strategy_type,
            analysis_type='exit',
            success=True,
            should_exit=should_exit,
            exit_reason=exit_reason,
            alerts=alerts,
            current_metrics={
                'price': active_strategy.current_price,
                'pnl_pct': active_strategy.unrealized_pnl_pct,
                'risk_score': position_risk.total_risk_score,
                'time_in_position': time_in_position.total_seconds() / 3600
            }
        )

    def _analyze_scaling_with_risk(self, active_strategy: ActiveStrategy,
                                  position_risk: PositionRisk,
                                  market_regime: MarketRegimeResult) -> PositionHealthResult:
        """Analyze scaling using risk assessment"""

        alerts = []
        scaling_action = "HOLD"
        scaling_reason = ""
        new_size = active_strategy.position_size

        # Use existing risk analysis for scaling decisions

        # 1. Scale down if high risk
        if position_risk.total_risk_score > self.config['scaling_thresholds']['risk_threshold']:
            scaling_action = "SCALE_DOWN"
            scaling_reason = f"High risk score ({position_risk.total_risk_score:.0f})"
            new_size = active_strategy.position_size * 0.5

            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='SCALE_DOWN',
                severity='WARNING',
                message=scaling_reason,
                action_required='REDUCE_POSITION',
                metrics={'risk_score': position_risk.total_risk_score, 'new_size': new_size}
            ))

        # 2. Scale up if performing well with low risk
        elif (active_strategy.unrealized_pnl_pct > self.config['scaling_thresholds']['performance_threshold'] and
              position_risk.total_risk_score < 30 and
              active_strategy.position_size < active_strategy.target_size):

            scaling_action = "SCALE_UP"
            scaling_reason = f"Strong performance ({active_strategy.unrealized_pnl_pct:.1f}%) with low risk"
            new_size = min(active_strategy.target_size, active_strategy.position_size * 1.2)

            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='SCALE_UP',
                severity='INFO',
                message=scaling_reason,
                action_required='INCREASE_POSITION',
                metrics={'pnl_pct': active_strategy.unrealized_pnl_pct, 'new_size': new_size}
            ))

        # 3. Scale down if high volatility (use risk analyzer's volatility)
        elif position_risk.volatility > self.config['scaling_thresholds']['vol_threshold']:
            scaling_action = "SCALE_DOWN"
            scaling_reason = f"High volatility ({position_risk.volatility:.1%})"
            new_size = active_strategy.position_size * 0.7

            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='SCALE_DOWN',
                severity='WARNING',
                message=scaling_reason,
                action_required='REDUCE_POSITION',
                metrics={'volatility': position_risk.volatility, 'new_size': new_size}
            ))

        return PositionHealthResult(
            symbol=active_strategy.symbol,
            strategy_type=active_strategy.strategy_type,
            analysis_type='scaling',
            success=True,
            scaling_action=scaling_action,
            scaling_reason=scaling_reason,
            new_size=new_size,
            alerts=alerts,
            current_metrics={
                'current_size': active_strategy.position_size,
                'size_change': new_size - active_strategy.position_size,
                'risk_score': position_risk.total_risk_score
            }
        )

    def _create_risk_result(self, active_strategy: ActiveStrategy,
                           position_risk: PositionRisk) -> PositionHealthResult:
        """Create risk result from RiskAnalyzer output"""

        # Convert risk warnings to alerts
        alerts = []
        for warning in position_risk.risk_warnings:
            severity = 'CRITICAL' if 'critical' in warning.lower() else 'WARNING'
            alerts.append(PositionHealthAlert(
                symbol=active_strategy.symbol,
                alert_type='RISK_WARNING',
                severity=severity,
                message=warning,
                action_required='REVIEW_POSITION',
                metrics={'risk_score': position_risk.total_risk_score}
            ))

        return PositionHealthResult(
            symbol=active_strategy.symbol,
            strategy_type=active_strategy.strategy_type,
            analysis_type='risk',
            success=True,
            alerts=alerts,
            current_metrics={
                'risk_score': position_risk.total_risk_score,
                'risk_category': position_risk.risk_category,
                'volatility': position_risk.volatility,
                'var_1d': position_risk.var_1d,
                'max_drawdown': position_risk.max_drawdown
            }
        )

    def _create_metrics_result(self, active_strategy: ActiveStrategy,
                              position_risk: PositionRisk) -> PositionHealthResult:
        """Create metrics result combining strategy and risk data"""

        # Calculate health score using existing risk analysis
        health_score = self._calculate_health_score(active_strategy, position_risk)

        return PositionHealthResult(
            symbol=active_strategy.symbol,
            strategy_type=active_strategy.strategy_type,
            analysis_type='metrics',
            success=True,
            health_score=health_score,
            current_metrics={
                'health_score': health_score,
                'current_price': active_strategy.current_price,
                'entry_price': active_strategy.entry_price,
                'pnl': active_strategy.unrealized_pnl,
                'pnl_pct': active_strategy.unrealized_pnl_pct,
                'position_size': active_strategy.position_size,
                'risk_score': position_risk.total_risk_score,
                'volatility': position_risk.volatility,
                'time_in_position': active_strategy.time_in_position().total_seconds() / 3600
            }
        )

    def _calculate_health_score(self, active_strategy: ActiveStrategy,
                               position_risk: PositionRisk) -> float:
        """Calculate health score using weighted components"""

        weights = self.config['health_weights']
        base_score = 50.0

        # Performance component
        pnl_pct = active_strategy.unrealized_pnl_pct
        if pnl_pct > 10:
            performance_score = 50
        elif pnl_pct > 5:
            performance_score = 30
        elif pnl_pct > 0:
            performance_score = 20
        elif pnl_pct > -5:
            performance_score = -10
        else:
            performance_score = -30

        # Risk component (inverse of risk score)
        risk_score = max(-50, min(50, 50 - position_risk.total_risk_score))

        # Time component
        time_ratio = active_strategy.time_in_position().days / 30  # Normalize to 30 days
        time_score = max(-20, min(20, 20 - (time_ratio * 30)))

        # Sentiment component (simplified)
        sentiment_score = 0  # Could integrate sentiment if needed

        final_score = (base_score +
                      performance_score * weights['performance'] +
                      risk_score * weights['risk'] +
                      time_score * weights['time'] +
                      sentiment_score * weights['sentiment'])

        return max(0, min(100, final_score))

    def _get_strategy_category(self, strategy_type: str) -> str:
        """Map strategy type to category"""
        mapping = {
            'momentum': 'MOMENTUM',
            'mean_reversion': 'MEAN_REVERSION',
            'bollinger': 'MEAN_REVERSION',
            'volatility': 'VOLATILITY',
            'pairs': 'PAIRS',
            'pca_momentum': 'MOMENTUM',
            'jump_momentum': 'MOMENTUM'
        }
        return mapping.get(strategy_type, 'UNKNOWN')

    def _create_minimal_risk(self, active_strategy: ActiveStrategy) -> PositionRisk:
        """Create minimal risk assessment on error"""
        from .risk_analyzer import PositionRisk

        return PositionRisk(
            symbol=active_strategy.symbol,
            strategy_name=active_strategy.strategy_type,
            volatility=0.2,
            beta=1.0,
            var_1d=0.02,
            var_5d=0.05,
            max_drawdown=active_strategy.max_drawdown,
            strategy_risk_score=30.0,
            confidence_risk=25.0,
            regime_risk=25.0,
            sentiment_risk=25.0,
            liquidity_risk=20.0,
            execution_risk=20.0,
            total_risk_score=50.0,
            risk_category='MEDIUM',
            risk_factors=['Incomplete analysis'],
            risk_warnings=[],
            timestamp=datetime.now()
        )

    def _create_error_results(self, active_strategy: ActiveStrategy, error: str) -> Dict[str, PositionHealthResult]:
        """Create error results for all analysis types"""
        error_result = PositionHealthResult(
            symbol=active_strategy.symbol,
            strategy_type=active_strategy.strategy_type,
            analysis_type='error',
            success=False,
            current_metrics={'error': error}
        )

        return {
            'exit': error_result,
            'scaling': error_result,
            'risk': error_result,
            'metrics': error_result
        }

    # Simplified public interface

    def analyze_multiple_positions_optimized(self, strategies: Dict[str, ActiveStrategy],
                                           market_data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Optimized analysis of multiple positions using existing analyzers

        More efficient than the original by:
        1. Using existing analyzers instead of duplicating logic
        2. Single comprehensive analysis per position
        3. Simplified alert structure
        """

        all_results = {'exit': [], 'scaling': [], 'risk': [], 'metrics': []}

        for symbol, strategy in strategies.items():
            if symbol not in market_data_dict:
                continue

            try:
                # Single comprehensive analysis
                results = self.analyze_position_comprehensive(strategy, market_data_dict[symbol])

                # Collect results by type
                for analysis_type, result in results.items():
                    all_results[analysis_type].append(result)

            except Exception as e:
                self.logger.error(f"Error analyzing {symbol}: {e}")
                continue

        # Generate simplified portfolio summary
        return self._generate_simplified_summary(all_results)

    def _generate_simplified_summary(self, all_results: Dict[str, List[PositionHealthResult]]) -> Dict[str, Any]:
        """Generate simplified portfolio summary"""

        # Collect all alerts
        all_alerts = []
        for results_list in all_results.values():
            for result in results_list:
                all_alerts.extend(result.alerts)

        # Generate actions
        actions = []

        # Exit actions
        for result in all_results['exit']:
            if result.success and result.should_exit:
                actions.append({
                    'symbol': result.symbol,
                    'action': 'EXIT',
                    'reason': result.exit_reason,
                    'urgency': 'HIGH'
                })

        # Scaling actions
        for result in all_results['scaling']:
            if result.success and result.scaling_action != 'HOLD':
                actions.append({
                    'symbol': result.symbol,
                    'action': result.scaling_action,
                    'reason': result.scaling_reason,
                    'urgency': 'MEDIUM'
                })

        # Summary stats
        total_positions = len(set(r.symbol for results in all_results.values() for r in results))
        avg_health_score = np.mean([r.health_score for r in all_results['metrics'] if r.success])

        return {
            'alerts': sorted(all_alerts, key=lambda x: x.timestamp, reverse=True),
            'actions': sorted(actions, key=lambda x: {'HIGH': 0, 'MEDIUM': 1}[x['urgency']]),
            'summary': {
                'total_positions': total_positions,
                'total_alerts': len(all_alerts),
                'critical_alerts': len([a for a in all_alerts if a.severity == 'CRITICAL']),
                'actions_required': len(actions),
                'average_health_score': avg_health_score,
                'high_risk_positions': len([r for r in all_results['risk'] if r.success and
                                          r.current_metrics.get('risk_score', 0) > 75])
            },
            'timestamp': datetime.now()
        }
