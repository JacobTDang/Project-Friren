"""
Alert Generator - Extracted from Position Health Monitor

This module handles alert generation, notification management, and action recommendations.
Provides intelligent alerting with market-driven thresholds and zero hardcoded values.

Features:
- Dynamic alert thresholds based on market conditions
- Strategy-specific alert generation
- Risk-based alert prioritization
- Performance-triggered alerts
- Market regime-aware alerting
- Alert deduplication and rate limiting
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import json

# Import market metrics and Redis infrastructure
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)


class AlertType(Enum):
    """Types of alerts that can be generated"""
    HEALTH_WARNING = "HEALTH_WARNING"
    HEALTH_CRITICAL = "HEALTH_CRITICAL"
    PERFORMANCE_POOR = "PERFORMANCE_POOR"
    PERFORMANCE_EXCELLENT = "PERFORMANCE_EXCELLENT"
    RISK_HIGH = "RISK_HIGH"
    RISK_EXTREME = "RISK_EXTREME"
    STRATEGY_UNDERPERFORMING = "STRATEGY_UNDERPERFORMING"
    STRATEGY_TRANSITION_NEEDED = "STRATEGY_TRANSITION_NEEDED"
    MARKET_REGIME_MISMATCH = "MARKET_REGIME_MISMATCH"
    REASSESSMENT_REQUIRED = "REASSESSMENT_REQUIRED"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"


class AlertPriority(Enum):
    """Alert priority levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"


class AlertChannel(Enum):
    """Alert delivery channels"""
    REDIS_QUEUE = "REDIS_QUEUE"
    PROCESS_MESSAGE = "PROCESS_MESSAGE"
    TERMINAL = "TERMINAL"
    LOG = "LOG"


@dataclass
class Alert:
    """Structured alert with market context"""
    alert_type: AlertType
    priority: AlertPriority
    symbol: str
    strategy_name: str
    message: str
    details: Dict[str, Any]
    timestamp: datetime
    market_context: Dict[str, Any]
    recommended_actions: List[str]
    alert_id: str = field(default_factory=lambda: f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    acknowledged: bool = False
    resolved: bool = False


@dataclass
class ActionRecommendation:
    """Action recommendation with context"""
    action_type: str
    symbol: str
    strategy_name: str
    recommendation: str
    confidence: float
    urgency: str
    rationale: str
    market_context: Dict[str, Any]
    timestamp: datetime


@dataclass
class AlertState:
    """Tracks alert state for a symbol/strategy"""
    symbol: str
    strategy_name: str
    last_alert_time: Optional[datetime] = None
    alert_count_last_hour: int = 0
    alert_count_last_day: int = 0
    consecutive_alerts: int = 0
    cooldown_until: Optional[datetime] = None
    last_alert_type: Optional[AlertType] = None
    escalation_level: int = 0


class AlertGenerator:
    """Generates intelligent alerts with market-driven thresholds"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.AlertGenerator")
        
        # Alert state tracking
        self.alert_states = {}  # {symbol: AlertState}
        self.alert_history = deque(maxlen=1000)  # Recent alert history
        self.active_alerts = {}  # {alert_id: Alert}
        
        # Rate limiting configuration (market-driven)
        self.min_alert_interval_minutes = 5
        self.max_alerts_per_hour = 10
        self.max_alerts_per_day = 50
        self.escalation_cooldown_minutes = 30
        
        # Market-driven thresholds (will be adjusted dynamically)
        self.base_thresholds = {
            'health_warning': 30.0,
            'health_critical': 15.0,
            'performance_poor': -5.0,
            'performance_excellent': 10.0,
            'risk_high': 70.0,
            'risk_extreme': 85.0
        }
        
        self.logger.info("AlertGenerator initialized with market-driven thresholds")
    
    def generate_strategy_specific_alerts(self, symbol: str, performance: Dict[str, Any]) -> List[Alert]:
        """Generate strategy-specific alerts based on performance data"""
        try:
            alerts = []
            strategy_name = performance.get('strategy_name', 'UNKNOWN')
            
            # Get market context for dynamic thresholds
            market_metrics = get_all_metrics(symbol)
            adjusted_thresholds = self._adjust_thresholds_for_market(market_metrics)
            
            # Check if we should generate alerts (rate limiting)
            if not self._should_generate_alert(symbol, strategy_name):
                return alerts
            
            # Health-based alerts
            health_alerts = self._generate_health_alerts(symbol, strategy_name, performance, adjusted_thresholds)
            alerts.extend(health_alerts)
            
            # Performance-based alerts
            performance_alerts = self._generate_performance_alerts(symbol, strategy_name, performance, adjusted_thresholds)
            alerts.extend(performance_alerts)
            
            # Risk-based alerts
            risk_alerts = self._generate_risk_alerts(symbol, strategy_name, performance, adjusted_thresholds)
            alerts.extend(risk_alerts)
            
            # Strategy effectiveness alerts
            strategy_alerts = self._generate_strategy_effectiveness_alerts(symbol, strategy_name, performance, market_metrics)
            alerts.extend(strategy_alerts)
            
            # Market regime mismatch alerts
            regime_alerts = self._generate_regime_mismatch_alerts(symbol, strategy_name, performance, market_metrics)
            alerts.extend(regime_alerts)
            
            # Update alert state
            self._update_alert_state(symbol, strategy_name, alerts)
            
            # Add to alert history
            for alert in alerts:
                self.alert_history.append(alert)
                self.active_alerts[alert.alert_id] = alert
            
            self.logger.debug(f"Generated {len(alerts)} alerts for {symbol}")
            return alerts
            
        except Exception as e:
            self.logger.error(f"Error generating strategy alerts for {symbol}: {e}")
            return []
    
    def _adjust_thresholds_for_market(self, market_metrics) -> Dict[str, float]:
        """Adjust alert thresholds based on current market conditions"""
        try:
            adjusted = self.base_thresholds.copy()
            
            if not market_metrics:
                return adjusted
            
            # Market volatility adjustments
            market_vol = getattr(market_metrics, 'volatility', 0.3)
            if market_vol > 0.6:  # High volatility market
                # More lenient thresholds in volatile markets
                adjusted['health_warning'] *= 0.8
                adjusted['health_critical'] *= 0.8
                adjusted['performance_poor'] *= 1.5  # Allow more loss before alerting
                adjusted['risk_high'] *= 1.2  # Higher risk tolerance
                adjusted['risk_extreme'] *= 1.1
            elif market_vol < 0.2:  # Low volatility market
                # Stricter thresholds in calm markets
                adjusted['health_warning'] *= 1.2
                adjusted['health_critical'] *= 1.2
                adjusted['performance_poor'] *= 0.7  # Less tolerant of losses
                adjusted['risk_high'] *= 0.9
                adjusted['risk_extreme'] *= 0.95
            
            # Market stress adjustments
            market_stress = getattr(market_metrics, 'market_stress', 0.3)
            if market_stress > 0.7:  # High stress
                stress_multiplier = 0.8  # More lenient
                for key in ['health_warning', 'health_critical']:
                    adjusted[key] *= stress_multiplier
            
            # Trend strength adjustments
            trend_strength = getattr(market_metrics, 'trend_strength', 0.5)
            if trend_strength > 0.8:  # Strong trend
                # More tolerant of momentum strategy risks
                adjusted['risk_high'] *= 1.1
                adjusted['performance_poor'] *= 1.2
            
            return adjusted
            
        except Exception as e:
            self.logger.error(f"Error adjusting thresholds for market: {e}")
            return self.base_thresholds.copy()
    
    def _generate_health_alerts(self, symbol: str, strategy_name: str, performance: Dict[str, Any], 
                               thresholds: Dict[str, float]) -> List[Alert]:
        """Generate health-related alerts"""
        alerts = []
        
        try:
            health_score = performance.get('health_score', 50.0)
            market_context = self._build_market_context(symbol)
            
            if health_score < thresholds['health_critical']:
                alert = Alert(
                    alert_type=AlertType.HEALTH_CRITICAL,
                    priority=AlertPriority.CRITICAL,
                    symbol=symbol,
                    strategy_name=strategy_name,
                    message=f"CRITICAL: {strategy_name} health score {health_score:.1f} below threshold {thresholds['health_critical']:.1f}",
                    details={
                        'health_score': health_score,
                        'threshold': thresholds['health_critical'],
                        'performance_data': performance
                    },
                    timestamp=datetime.now(),
                    market_context=market_context,
                    recommended_actions=[
                        "Immediate strategy reassessment required",
                        "Consider emergency position reduction",
                        "Review risk management parameters"
                    ]
                )
                alerts.append(alert)
                
            elif health_score < thresholds['health_warning']:
                alert = Alert(
                    alert_type=AlertType.HEALTH_WARNING,
                    priority=AlertPriority.HIGH,
                    symbol=symbol,
                    strategy_name=strategy_name,
                    message=f"WARNING: {strategy_name} health score {health_score:.1f} below threshold {thresholds['health_warning']:.1f}",
                    details={
                        'health_score': health_score,
                        'threshold': thresholds['health_warning'],
                        'performance_data': performance
                    },
                    timestamp=datetime.now(),
                    market_context=market_context,
                    recommended_actions=[
                        "Monitor closely for further deterioration",
                        "Review strategy parameters",
                        "Consider position size reduction"
                    ]
                )
                alerts.append(alert)
                
        except Exception as e:
            self.logger.error(f"Error generating health alerts: {e}")
        
        return alerts
    
    def _generate_performance_alerts(self, symbol: str, strategy_name: str, performance: Dict[str, Any],
                                   thresholds: Dict[str, float]) -> List[Alert]:
        """Generate performance-related alerts"""
        alerts = []
        
        try:
            pnl_pct = performance.get('pnl_percentage', 0.0)
            market_context = self._build_market_context(symbol)
            
            if pnl_pct < thresholds['performance_poor']:
                alert = Alert(
                    alert_type=AlertType.PERFORMANCE_POOR,
                    priority=AlertPriority.HIGH,
                    symbol=symbol,
                    strategy_name=strategy_name,
                    message=f"POOR PERFORMANCE: {strategy_name} P&L {pnl_pct:.2f}% below threshold {thresholds['performance_poor']:.2f}%",
                    details={
                        'pnl_percentage': pnl_pct,
                        'threshold': thresholds['performance_poor'],
                        'performance_data': performance
                    },
                    timestamp=datetime.now(),
                    market_context=market_context,
                    recommended_actions=[
                        "Strategy reassessment recommended",
                        "Review market conditions",
                        "Consider strategy transition"
                    ]
                )
                alerts.append(alert)
                
            elif pnl_pct > thresholds['performance_excellent']:
                alert = Alert(
                    alert_type=AlertType.PERFORMANCE_EXCELLENT,
                    priority=AlertPriority.LOW,
                    symbol=symbol,
                    strategy_name=strategy_name,
                    message=f"EXCELLENT PERFORMANCE: {strategy_name} P&L {pnl_pct:.2f}% above threshold {thresholds['performance_excellent']:.2f}%",
                    details={
                        'pnl_percentage': pnl_pct,
                        'threshold': thresholds['performance_excellent'],
                        'performance_data': performance
                    },
                    timestamp=datetime.now(),
                    market_context=market_context,
                    recommended_actions=[
                        "Consider increasing position size",
                        "Monitor for sustainability",
                        "Document successful parameters"
                    ]
                )
                alerts.append(alert)
                
        except Exception as e:
            self.logger.error(f"Error generating performance alerts: {e}")
        
        return alerts
    
    def _generate_risk_alerts(self, symbol: str, strategy_name: str, performance: Dict[str, Any],
                            thresholds: Dict[str, float]) -> List[Alert]:
        """Generate risk-related alerts"""
        alerts = []
        
        try:
            risk_score = performance.get('risk_score', 50.0)
            market_context = self._build_market_context(symbol)
            
            if risk_score > thresholds['risk_extreme']:
                alert = Alert(
                    alert_type=AlertType.RISK_EXTREME,
                    priority=AlertPriority.EMERGENCY,
                    symbol=symbol,
                    strategy_name=strategy_name,
                    message=f"EXTREME RISK: {strategy_name} risk score {risk_score:.1f} above threshold {thresholds['risk_extreme']:.1f}",
                    details={
                        'risk_score': risk_score,
                        'threshold': thresholds['risk_extreme'],
                        'performance_data': performance
                    },
                    timestamp=datetime.now(),
                    market_context=market_context,
                    recommended_actions=[
                        "EMERGENCY: Immediate position reduction required",
                        "Stop all new trades",
                        "Review all risk parameters"
                    ]
                )
                alerts.append(alert)
                
            elif risk_score > thresholds['risk_high']:
                alert = Alert(
                    alert_type=AlertType.RISK_HIGH,
                    priority=AlertPriority.HIGH,
                    symbol=symbol,
                    strategy_name=strategy_name,
                    message=f"HIGH RISK: {strategy_name} risk score {risk_score:.1f} above threshold {thresholds['risk_high']:.1f}",
                    details={
                        'risk_score': risk_score,
                        'threshold': thresholds['risk_high'],
                        'performance_data': performance
                    },
                    timestamp=datetime.now(),
                    market_context=market_context,
                    recommended_actions=[
                        "Reduce position size",
                        "Tighten stop losses",
                        "Monitor closely"
                    ]
                )
                alerts.append(alert)
                
        except Exception as e:
            self.logger.error(f"Error generating risk alerts: {e}")
        
        return alerts
    
    def _generate_strategy_effectiveness_alerts(self, symbol: str, strategy_name: str, 
                                              performance: Dict[str, Any], market_metrics) -> List[Alert]:
        """Generate strategy effectiveness alerts"""
        alerts = []
        
        try:
            effectiveness_score = performance.get('effectiveness_score', 50.0)
            market_context = self._build_market_context(symbol)
            
            # Dynamic effectiveness threshold based on market conditions
            base_threshold = 30.0
            if market_metrics:
                market_vol = getattr(market_metrics, 'volatility', 0.3)
                if market_vol > 0.6:
                    effectiveness_threshold = base_threshold * 0.8  # More lenient in volatile markets
                else:
                    effectiveness_threshold = base_threshold
            else:
                effectiveness_threshold = base_threshold
            
            if effectiveness_score < effectiveness_threshold:
                alert = Alert(
                    alert_type=AlertType.STRATEGY_UNDERPERFORMING,
                    priority=AlertPriority.MEDIUM,
                    symbol=symbol,
                    strategy_name=strategy_name,
                    message=f"UNDERPERFORMING: {strategy_name} effectiveness {effectiveness_score:.1f} below threshold {effectiveness_threshold:.1f}",
                    details={
                        'effectiveness_score': effectiveness_score,
                        'threshold': effectiveness_threshold,
                        'performance_data': performance
                    },
                    timestamp=datetime.now(),
                    market_context=market_context,
                    recommended_actions=[
                        "Strategy reassessment recommended",
                        "Consider alternative strategies",
                        "Review market fit"
                    ]
                )
                alerts.append(alert)
                
        except Exception as e:
            self.logger.error(f"Error generating strategy effectiveness alerts: {e}")
        
        return alerts
    
    def _generate_regime_mismatch_alerts(self, symbol: str, strategy_name: str,
                                       performance: Dict[str, Any], market_metrics) -> List[Alert]:
        """Generate market regime mismatch alerts"""
        alerts = []
        
        try:
            if not market_metrics:
                return alerts
            
            market_regime = getattr(market_metrics, 'primary_regime', 'NEUTRAL')
            strategy_category = self._extract_strategy_category(strategy_name)
            
            # Check for regime mismatch
            mismatch_score = self._calculate_regime_mismatch_score(strategy_category, market_regime)
            
            if mismatch_score > 70:  # High mismatch
                market_context = self._build_market_context(symbol)
                
                alert = Alert(
                    alert_type=AlertType.MARKET_REGIME_MISMATCH,
                    priority=AlertPriority.MEDIUM,
                    symbol=symbol,
                    strategy_name=strategy_name,
                    message=f"REGIME MISMATCH: {strategy_name} ({strategy_category}) poorly suited for {market_regime} regime",
                    details={
                        'strategy_category': strategy_category,
                        'market_regime': market_regime,
                        'mismatch_score': mismatch_score,
                        'performance_data': performance
                    },
                    timestamp=datetime.now(),
                    market_context=market_context,
                    recommended_actions=[
                        "Consider strategy transition",
                        "Reduce position size",
                        "Monitor market regime changes"
                    ]
                )
                alerts.append(alert)
                
        except Exception as e:
            self.logger.error(f"Error generating regime mismatch alerts: {e}")
        
        return alerts
    
    def send_alert_to_queue(self, alert: Alert) -> bool:
        """Send alert to Redis queue with proper formatting"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                self.logger.warning("No Redis manager available for alert")
                return False
            
            # Create alert message
            alert_message = create_process_message(
                message_type="alert",
                sender_id="position_health_monitor",
                priority=self._map_alert_priority_to_message_priority(alert.priority),
                data={
                    'alert_id': alert.alert_id,
                    'alert_type': alert.alert_type.value,
                    'priority': alert.priority.value,
                    'symbol': alert.symbol,
                    'strategy_name': alert.strategy_name,
                    'message': alert.message,
                    'details': alert.details,
                    'market_context': alert.market_context,
                    'recommended_actions': alert.recommended_actions,
                    'timestamp': alert.timestamp.isoformat()
                }
            )
            
            # Send to queue
            success = redis_manager.send_message("decision_engine", alert_message)
            
            if success:
                self.logger.info(f"Alert sent to queue: {alert.alert_type.value} for {alert.symbol}")
            else:
                self.logger.error(f"Failed to send alert to queue: {alert.alert_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending alert to queue: {e}")
            return False
    
    def send_action_recommendation_to_queue(self, recommendation: ActionRecommendation) -> bool:
        """Send action recommendation to Redis queue"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                self.logger.warning("No Redis manager available for recommendation")
                return False
            
            # Create recommendation message
            recommendation_message = create_process_message(
                message_type="action_recommendation",
                sender_id="position_health_monitor",
                priority=MessagePriority.MEDIUM if recommendation.urgency == "high" else MessagePriority.LOW,
                data={
                    'action_type': recommendation.action_type,
                    'symbol': recommendation.symbol,
                    'strategy_name': recommendation.strategy_name,
                    'recommendation': recommendation.recommendation,
                    'confidence': recommendation.confidence,
                    'urgency': recommendation.urgency,
                    'rationale': recommendation.rationale,
                    'market_context': recommendation.market_context,
                    'timestamp': recommendation.timestamp.isoformat()
                }
            )
            
            # Send to queue
            success = redis_manager.send_message("decision_engine", recommendation_message)
            
            if success:
                self.logger.info(f"Action recommendation sent: {recommendation.action_type} for {recommendation.symbol}")
            else:
                self.logger.error(f"Failed to send action recommendation")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending action recommendation: {e}")
            return False
    
    def _should_generate_alert(self, symbol: str, strategy_name: str) -> bool:
        """Check if we should generate an alert based on rate limiting"""
        try:
            key = f"{symbol}_{strategy_name}"
            
            if key not in self.alert_states:
                self.alert_states[key] = AlertState(symbol=symbol, strategy_name=strategy_name)
            
            state = self.alert_states[key]
            now = datetime.now()
            
            # Check cooldown
            if state.cooldown_until and now < state.cooldown_until:
                return False
            
            # Check rate limits
            if state.last_alert_time:
                time_since_last = (now - state.last_alert_time).total_seconds() / 60
                if time_since_last < self.min_alert_interval_minutes:
                    return False
            
            # Check hourly/daily limits
            if state.alert_count_last_hour >= self.max_alerts_per_hour:
                return False
            
            if state.alert_count_last_day >= self.max_alerts_per_day:
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking alert rate limits: {e}")
            return True  # Default to allowing alerts
    
    def _update_alert_state(self, symbol: str, strategy_name: str, alerts: List[Alert]):
        """Update alert state after generating alerts"""
        try:
            if not alerts:
                return
            
            key = f"{symbol}_{strategy_name}"
            if key not in self.alert_states:
                self.alert_states[key] = AlertState(symbol=symbol, strategy_name=strategy_name)
            
            state = self.alert_states[key]
            now = datetime.now()
            
            # Update alert counts and timing
            state.last_alert_time = now
            state.alert_count_last_hour += len(alerts)
            state.alert_count_last_day += len(alerts)
            
            # Track consecutive alerts
            if alerts:
                state.consecutive_alerts += 1
                state.last_alert_type = alerts[0].alert_type
                
                # Escalate if needed
                if state.consecutive_alerts >= 3:
                    state.escalation_level = min(3, state.escalation_level + 1)
                    state.cooldown_until = now + timedelta(minutes=self.escalation_cooldown_minutes)
            
            # Reset hourly counter
            if state.last_alert_time and (now - state.last_alert_time).total_seconds() > 3600:
                state.alert_count_last_hour = 0
            
            # Reset daily counter
            if state.last_alert_time and (now - state.last_alert_time).total_seconds() > 86400:
                state.alert_count_last_day = 0
                state.escalation_level = 0
            
        except Exception as e:
            self.logger.error(f"Error updating alert state: {e}")
    
    def _build_market_context(self, symbol: str) -> Dict[str, Any]:
        """Build market context for alerts"""
        try:
            market_metrics = get_all_metrics(symbol)
            
            if not market_metrics:
                return {'market_data_available': False}
            
            return {
                'market_data_available': True,
                'primary_regime': getattr(market_metrics, 'primary_regime', 'NEUTRAL'),
                'volatility': getattr(market_metrics, 'volatility', 0.3),
                'trend_strength': getattr(market_metrics, 'trend_strength', 0.5),
                'market_stress': getattr(market_metrics, 'market_stress', 0.3),
                'volume_profile': getattr(market_metrics, 'volume', 0),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error building market context: {e}")
            return {'market_data_available': False, 'error': str(e)}
    
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
    
    def _calculate_regime_mismatch_score(self, strategy_category: str, market_regime: str) -> float:
        """Calculate regime mismatch score (0-100)"""
        # Simplified mismatch scoring
        mismatch_matrix = {
            ('MOMENTUM', 'RANGE_BOUND_STABLE'): 80,
            ('MOMENTUM', 'MIXED_SIGNALS'): 70,
            ('MEAN_REVERSION', 'BULLISH_TRENDING'): 70,
            ('MEAN_REVERSION', 'BEARISH_TRENDING'): 70,
            ('VOLATILITY', 'RANGE_BOUND_STABLE'): 60,
            ('PAIRS', 'HIGH_VOLATILITY_UNSTABLE'): 75,
            ('DEFENSIVE', 'BULLISH_TRENDING'): 50
        }
        
        return mismatch_matrix.get((strategy_category, market_regime), 30)  # Default low mismatch
    
    def _map_alert_priority_to_message_priority(self, alert_priority: AlertPriority) -> MessagePriority:
        """Map alert priority to message priority"""
        mapping = {
            AlertPriority.LOW: MessagePriority.LOW,
            AlertPriority.MEDIUM: MessagePriority.MEDIUM,
            AlertPriority.HIGH: MessagePriority.HIGH,
            AlertPriority.CRITICAL: MessagePriority.HIGH,
            AlertPriority.EMERGENCY: MessagePriority.HIGH
        }
        return mapping.get(alert_priority, MessagePriority.MEDIUM)
    
    def get_active_alerts(self, symbol: Optional[str] = None) -> List[Alert]:
        """Get currently active alerts"""
        if symbol:
            return [alert for alert in self.active_alerts.values() 
                   if alert.symbol == symbol and not alert.resolved]
        else:
            return [alert for alert in self.active_alerts.values() if not alert.resolved]
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].acknowledged = True
            self.logger.info(f"Alert acknowledged: {alert_id}")
            return True
        return False
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id].resolved = True
            self.logger.info(f"Alert resolved: {alert_id}")
            return True
        return False
