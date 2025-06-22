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
from .risk_analyzer import RiskAnalyzer, PositionRisk
from .market_analyzer import MarketAnalyzer, MarketRegimeResult
from .sentiment_analyzer import SentimentAnalyzer, SentimentReading


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
        Comprehensive position analysis orchestrating all components

        Args:
            active_strategy: Strategy to analyze
            market_data: Recent market data

        Returns:
            Dictionary with analysis results for different aspects
        """

        try:
            # Get current price
            if not market_data.empty:
                current_price = market_data.iloc[-1]['Close']
                active_strategy.current_price = current_price

            # Run risk analysis using existing analyzer
            position_risk = self.risk_analyzer.analyze_position_risk(active_strategy, market_data)

            # Get market regime (optional - may fail gracefully)
            market_regime = None
            try:
                market_regime = self.market_analyzer.analyze_market_regime(market_data)
            except Exception as e:
                self.logger.warning(f"Market regime analysis failed: {e}")

            # Comprehensive analysis using optimized methods
            results = {
                'exit': self._analyze_exit_with_risk(active_strategy, position_risk, market_regime),
                'scaling': self._analyze_scaling_with_risk(active_strategy, position_risk, market_regime),
                'risk': self._create_risk_result(active_strategy, position_risk),
                'metrics': self._create_metrics_result(active_strategy, position_risk)
            }

            self.logger.debug(f"Comprehensive analysis complete for {active_strategy.symbol}")
            return results

        except Exception as e:
            self.logger.error(f"Error in comprehensive analysis for {active_strategy.symbol}: {e}")
            return self._create_error_results(active_strategy.symbol, str(e))

    def analyze_position_with_strategy_context(self, active_strategy: ActiveStrategy,
                                             market_data: pd.DataFrame,
                                             strategy_context: Dict[str, Any]) -> Dict[str, PositionHealthResult]:
        """
        Enhanced position analysis with strategy management context

        Args:
            active_strategy: Strategy to analyze
            market_data: Recent market data
            strategy_context: Strategy management context from position health monitor

        Returns:
            Dictionary with enhanced analysis results including strategy-specific metrics
        """
        try:
            # Get current price
            if not market_data.empty:
                current_price = market_data.iloc[-1]['Close']
                active_strategy.current_price = current_price

            # Run risk analysis using existing analyzer
            position_risk = self.risk_analyzer.analyze_position_risk(active_strategy, market_data)

            # Get market regime (optional - may fail gracefully)
            market_regime = None
            try:
                market_regime = self.market_analyzer.analyze_market_regime(market_data)
            except Exception as e:
                self.logger.warning(f"Market regime analysis failed: {e}")

            # Enhanced analysis with strategy context
            results = {
                'exit': self._analyze_exit_with_strategy_context(active_strategy, position_risk, market_regime, strategy_context),
                'scaling': self._analyze_scaling_with_strategy_context(active_strategy, position_risk, market_regime, strategy_context),
                'risk': self._create_risk_result_with_context(active_strategy, position_risk, strategy_context),
                'metrics': self._create_metrics_result_with_context(active_strategy, position_risk, strategy_context)
            }

            self.logger.debug(f"Strategy-context analysis complete for {active_strategy.symbol}")
            return results

        except Exception as e:
            self.logger.error(f"Error in strategy-context analysis for {active_strategy.symbol}: {e}")
            # Fallback to regular comprehensive analysis
            return self.analyze_position_comprehensive(active_strategy, market_data)

    def _analyze_exit_with_strategy_context(self, active_strategy: ActiveStrategy,
                                          position_risk: PositionRisk,
                                          market_regime: MarketRegimeResult,
                                          strategy_context: Dict[str, Any]) -> PositionHealthResult:
        """Enhanced exit analysis with strategy context"""
        try:
            # Start with regular exit analysis
            base_result = self._analyze_exit_with_risk(active_strategy, position_risk, market_regime)

            # Enhance with strategy-specific context
            strategy_type = strategy_context.get('strategy_type', 'unknown')
            consecutive_poor_checks = strategy_context.get('consecutive_poor_checks', 0)
            health_status = strategy_context.get('current_health_status', 'NEUTRAL')

            # Strategy-specific exit modifications
            if strategy_type in ['momentum', 'pca_momentum', 'jump_momentum']:
                # Momentum strategies: more aggressive exits on poor performance
                if consecutive_poor_checks >= 2 and active_strategy.unrealized_pnl_pct < -3:
                    base_result.should_exit = True
                    base_result.exit_reason = f"Momentum strategy failing: {consecutive_poor_checks} poor checks, {active_strategy.unrealized_pnl_pct:.2f}% loss"

            elif strategy_type in ['mean_reversion', 'bollinger']:
                # Mean reversion: more patient with temporary losses
                if active_strategy.unrealized_pnl_pct < -10 or consecutive_poor_checks >= 4:
                    base_result.should_exit = True
                    base_result.exit_reason = f"Mean reversion strategy exhausted: {consecutive_poor_checks} poor checks"

            # Health status overrides
            if health_status == 'CRITICAL' and consecutive_poor_checks >= 3:
                base_result.should_exit = True
                base_result.exit_reason = f"Critical health status with {consecutive_poor_checks} consecutive poor checks"

            return base_result

        except Exception as e:
            self.logger.error(f"Error in strategy-context exit analysis: {e}")
            return self._analyze_exit_with_risk(active_strategy, position_risk, market_regime)

    def _analyze_scaling_with_strategy_context(self, active_strategy: ActiveStrategy,
                                             position_risk: PositionRisk,
                                             market_regime: MarketRegimeResult,
                                             strategy_context: Dict[str, Any]) -> PositionHealthResult:
        """Enhanced scaling analysis with strategy context"""
        try:
            # Start with regular scaling analysis
            base_result = self._analyze_scaling_with_risk(active_strategy, position_risk, market_regime)

            # Enhance with strategy-specific context
            strategy_type = strategy_context.get('strategy_type', 'unknown')
            performance_score = strategy_context.get('performance_score', 0.5)
            health_status = strategy_context.get('current_health_status', 'NEUTRAL')

            # Strategy-specific scaling modifications
            if health_status in ['CONCERNING', 'CRITICAL']:
                # Don't scale up if health is poor
                if base_result.scaling_action == 'SCALE_UP':
                    base_result.scaling_action = 'HOLD'
                    base_result.scaling_reason = f"Health status {health_status} - holding position"

            elif health_status == 'EXCELLENT' and performance_score > 0.8:
                # More aggressive scaling for excellent performers
                if strategy_type in ['momentum', 'pca_momentum'] and active_strategy.unrealized_pnl_pct > 8:
                    base_result.scaling_action = 'SCALE_UP'
                    base_result.scaling_reason = f"Excellent momentum performance: {active_strategy.unrealized_pnl_pct:.2f}%"
                    base_result.new_size = min(active_strategy.target_size * 1.2, active_strategy.position_size * 1.3)

            return base_result

        except Exception as e:
            self.logger.error(f"Error in strategy-context scaling analysis: {e}")
            return self._analyze_scaling_with_risk(active_strategy, position_risk, market_regime)

    def _create_risk_result_with_context(self, active_strategy: ActiveStrategy,
                                       position_risk: PositionRisk,
                                       strategy_context: Dict[str, Any]) -> PositionHealthResult:
        """Enhanced risk analysis with strategy context"""
        try:
            # Start with regular risk result
            base_result = self._create_risk_result(active_strategy, position_risk)

            # Add strategy-specific risk considerations
            strategy_type = strategy_context.get('strategy_type', 'unknown')
            consecutive_poor_checks = strategy_context.get('consecutive_poor_checks', 0)

            # Adjust risk assessment based on strategy type
            strategy_risk_multiplier = 1.0
            if strategy_type in ['momentum', 'jump_momentum']:
                strategy_risk_multiplier = 1.2  # Momentum strategies inherently riskier
            elif strategy_type in ['pairs', 'mean_reversion']:
                strategy_risk_multiplier = 0.9  # More conservative strategies

            # Add strategy context to current metrics
            if hasattr(base_result, 'current_metrics') and base_result.current_metrics:
                base_result.current_metrics.update({
                    'strategy_type': strategy_type,
                    'consecutive_poor_checks': consecutive_poor_checks,
                    'strategy_risk_multiplier': strategy_risk_multiplier,
                    'adjusted_risk_score': base_result.current_metrics.get('risk_score', 50) * strategy_risk_multiplier
                })

            return base_result

        except Exception as e:
            self.logger.error(f"Error in strategy-context risk analysis: {e}")
            return self._create_risk_result(active_strategy, position_risk)

    def _create_metrics_result_with_context(self, active_strategy: ActiveStrategy,
                                          position_risk: PositionRisk,
                                          strategy_context: Dict[str, Any]) -> PositionHealthResult:
        """Enhanced metrics with strategy context"""
        try:
            # Start with regular metrics result
            base_result = self._create_metrics_result(active_strategy, position_risk)

            # Add strategy-specific metrics
            strategy_type = strategy_context.get('strategy_type', 'unknown')
            performance_score = strategy_context.get('performance_score', 0.5)
            health_status = strategy_context.get('current_health_status', 'NEUTRAL')
            time_in_position = strategy_context.get('time_in_position', 0.0)

            # Calculate strategy effectiveness
            strategy_effectiveness = self._calculate_strategy_effectiveness_internal(
                active_strategy, strategy_context
            )

            # Enhanced metrics
            if hasattr(base_result, 'current_metrics') and base_result.current_metrics:
                base_result.current_metrics.update({
                    'strategy_type': strategy_type,
                    'strategy_performance_score': performance_score,
                    'strategy_health_status': health_status,
                    'strategy_effectiveness': strategy_effectiveness,
                    'time_in_position_hours': time_in_position,
                    'strategy_category': self._get_strategy_category(strategy_type)
                })

            return base_result

        except Exception as e:
            self.logger.error(f"Error in strategy-context metrics analysis: {e}")
            return self._create_metrics_result(active_strategy, position_risk)

    def _calculate_strategy_effectiveness_internal(self, active_strategy: ActiveStrategy,
                                                 strategy_context: Dict[str, Any]) -> float:
        """Calculate strategy effectiveness using internal logic"""
        try:
            pnl_pct = active_strategy.unrealized_pnl_pct
            time_hours = strategy_context.get('time_in_position', 0.0)
            strategy_type = strategy_context.get('strategy_type', 'unknown')

            # Strategy-specific effectiveness calculations (similar to position health monitor)
            if strategy_type in ['momentum', 'pca_momentum', 'jump_momentum']:
                if time_hours < 24:
                    target_pnl = 2.0
                elif time_hours < 72:
                    target_pnl = 5.0
                else:
                    target_pnl = 8.0
            elif strategy_type in ['mean_reversion', 'bollinger']:
                if time_hours < 48:
                    target_pnl = 1.0
                elif time_hours < 168:
                    target_pnl = 3.0
                else:
                    target_pnl = 6.0
            else:
                target_pnl = max(1.0, time_hours / 24)

            # Calculate effectiveness score
            if target_pnl > 0:
                effectiveness = min(1.0, pnl_pct / target_pnl)
            else:
                effectiveness = 0.5  # Neutral

            return max(0.0, min(1.0, effectiveness))

        except Exception:
            return 0.5  # Neutral effectiveness

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
            from Friren_V1.trading_engine.portfolio_manager.tools.strategy_selector import StrategyRecommendation
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
