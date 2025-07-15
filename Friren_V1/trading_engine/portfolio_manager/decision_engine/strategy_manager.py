"""
Strategy Manager - Extracted from Decision Engine

This module handles strategy management, transitions, and monitoring state.
Provides zero-hardcoded strategy lifecycle management with market-driven decisions.

Features:
- Strategy lifecycle management
- Market-driven strategy transitions
- Multi-signal confirmation system
- Strategy performance monitoring
- Emergency exit coordination
- Rate limiting and cooldown management
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import time

# Import market metrics for dynamic decisions
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)


class MonitoringStrategyStatus(Enum):
    """Status of active monitoring strategies per symbol"""
    ACTIVE = "active"                    # Strategy actively monitoring
    TRANSITIONING = "transitioning"     # In process of changing strategies
    PENDING_CONFIRMATION = "pending_confirmation"  # Awaiting multi-signal confirmation
    FAILED = "failed"                   # Strategy failed, needs reassignment
    EMERGENCY_EXIT = "emergency_exit"   # Emergency exit mode


class TransitionSignalType(Enum):
    """Types of strategy transition signals"""
    PERFORMANCE_DECLINE = "performance_decline"
    MARKET_REGIME_CHANGE = "market_regime_change"
    RISK_THRESHOLD_BREACH = "risk_threshold_breach"
    MULTI_SIGNAL_CONFIRMATION = "multi_signal_confirmation"
    MANUAL_OVERRIDE = "manual_override"
    EMERGENCY_EXIT = "emergency_exit"


@dataclass
class TransitionSignal:
    """Strategy transition signal with market context"""
    signal_type: TransitionSignalType
    symbol: str
    current_strategy: str
    recommended_strategy: str
    confidence: float
    market_context: Dict[str, Any]
    performance_metrics: Dict[str, Any]
    timestamp: datetime
    urgency: str  # low, medium, high, emergency
    reason: str
    source_process: str


@dataclass
class StrategyState:
    """Complete strategy state for a symbol"""
    symbol: str
    current_strategy: str
    status: MonitoringStrategyStatus
    assignment_time: datetime
    last_transition_time: Optional[datetime] = None
    transition_count: int = 0
    performance_score: float = 50.0
    consecutive_poor_performance: int = 0
    pending_signals: List[TransitionSignal] = field(default_factory=list)
    confirmation_required: bool = False
    emergency_mode: bool = False
    cooldown_until: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class RateLimiter:
    """Rate limiter for strategy transitions with market-aware thresholds"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.RateLimiter")
        self.transition_history = defaultdict(deque)  # {symbol: deque of timestamps}
        
        # Base rate limits (will be adjusted based on market conditions)
        self.base_max_transitions_per_hour = 2
        self.base_max_transitions_per_day = 5
        self.base_min_strategy_duration_minutes = 30
        
    def can_transition(self, symbol: str, current_strategy_start: datetime) -> Tuple[bool, str]:
        """Check if strategy transition is allowed with market-aware limits"""
        try:
            now = datetime.now()
            
            # Get market metrics for dynamic rate limiting
            market_metrics = get_all_metrics(symbol)
            adjusted_limits = self._adjust_limits_for_market(market_metrics)
            
            # Check minimum strategy duration
            strategy_duration = (now - current_strategy_start).total_seconds() / 60
            if strategy_duration < adjusted_limits['min_duration_minutes']:
                return False, f"Strategy duration {strategy_duration:.1f}min < minimum {adjusted_limits['min_duration_minutes']}min"
            
            # Check recent transition history
            recent_transitions = self.transition_history[symbol]
            
            # Remove old transitions (older than 24 hours)
            cutoff_time = now - timedelta(hours=24)
            while recent_transitions and recent_transitions[0] < cutoff_time:
                recent_transitions.popleft()
            
            # Check daily limit
            daily_transitions = len(recent_transitions)
            if daily_transitions >= adjusted_limits['max_per_day']:
                return False, f"Daily transition limit reached: {daily_transitions}/{adjusted_limits['max_per_day']}"
            
            # Check hourly limit
            hour_cutoff = now - timedelta(hours=1)
            hourly_transitions = sum(1 for t in recent_transitions if t > hour_cutoff)
            if hourly_transitions >= adjusted_limits['max_per_hour']:
                return False, f"Hourly transition limit reached: {hourly_transitions}/{adjusted_limits['max_per_hour']}"
            
            return True, "Transition allowed"
            
        except Exception as e:
            self.logger.error(f"Error checking transition rate limit for {symbol}: {e}")
            return False, f"Rate limit check error: {e}"
    
    def record_transition(self, symbol: str):
        """Record a strategy transition"""
        self.transition_history[symbol].append(datetime.now())
        
        # Keep history manageable
        if len(self.transition_history[symbol]) > 20:
            self.transition_history[symbol].popleft()
    
    def _adjust_limits_for_market(self, market_metrics) -> Dict[str, Any]:
        """Adjust rate limits based on market conditions"""
        try:
            # Start with base limits
            limits = {
                'max_per_hour': self.base_max_transitions_per_hour,
                'max_per_day': self.base_max_transitions_per_day,
                'min_duration_minutes': self.base_min_strategy_duration_minutes
            }
            
            if not market_metrics:
                return limits
            
            # Market volatility adjustments
            volatility = getattr(market_metrics, 'volatility', 0.3)
            if volatility > 0.6:  # High volatility
                # Allow more frequent transitions in volatile markets
                limits['max_per_hour'] = min(4, int(limits['max_per_hour'] * 1.5))
                limits['max_per_day'] = min(8, int(limits['max_per_day'] * 1.3))
                limits['min_duration_minutes'] = max(15, int(limits['min_duration_minutes'] * 0.7))
            elif volatility < 0.2:  # Low volatility
                # Be more conservative in calm markets
                limits['max_per_hour'] = max(1, int(limits['max_per_hour'] * 0.7))
                limits['max_per_day'] = max(3, int(limits['max_per_day'] * 0.8))
                limits['min_duration_minutes'] = int(limits['min_duration_minutes'] * 1.3)
            
            # Market stress adjustments
            market_stress = getattr(market_metrics, 'market_stress', 0.3)
            if market_stress > 0.7:  # High stress
                # Allow emergency transitions
                limits['max_per_hour'] = min(6, int(limits['max_per_hour'] * 2))
                limits['min_duration_minutes'] = max(10, int(limits['min_duration_minutes'] * 0.5))
            
            return limits
            
        except Exception as e:
            self.logger.error(f"Error adjusting rate limits: {e}")
            return {
                'max_per_hour': self.base_max_transitions_per_hour,
                'max_per_day': self.base_max_transitions_per_day,
                'min_duration_minutes': self.base_min_strategy_duration_minutes
            }


class StrategyManager:
    """Manages strategy lifecycle, transitions, and monitoring with market context"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.StrategyManager")
        
        # Strategy state tracking
        self.strategy_states = {}  # {symbol: StrategyState}
        self.transition_signals = defaultdict(list)  # {symbol: List[TransitionSignal]}
        
        # Rate limiting
        self.rate_limiter = RateLimiter()
        
        # Configuration (market-driven defaults)
        self.confirmation_required_threshold = 0.7  # Confidence threshold for requiring confirmation
        self.emergency_exit_threshold = 0.9  # Confidence threshold for emergency exit
        self.max_pending_signals = 5
        self.signal_expiry_minutes = 60
        
        self.logger.info("StrategyManager initialized with market-driven parameters")
    
    def update_strategy_state(self, symbol: str, strategy_name: str, performance_metrics: Dict[str, Any]):
        """Update strategy state with latest performance data"""
        try:
            if symbol not in self.strategy_states:
                self.strategy_states[symbol] = StrategyState(
                    symbol=symbol,
                    current_strategy=strategy_name,
                    status=MonitoringStrategyStatus.ACTIVE,
                    assignment_time=datetime.now()
                )
            
            state = self.strategy_states[symbol]
            state.performance_score = performance_metrics.get('effectiveness_score', 50.0)
            
            # Update consecutive poor performance tracking
            if state.performance_score < 30.0:
                state.consecutive_poor_performance += 1
            else:
                state.consecutive_poor_performance = 0
            
            # Check for automatic status updates
            self._update_strategy_status(state, performance_metrics)
            
        except Exception as e:
            self.logger.error(f"Error updating strategy state for {symbol}: {e}")
    
    def process_transition_signal(self, signal: TransitionSignal) -> bool:
        """Process a strategy transition signal"""
        try:
            symbol = signal.symbol
            
            # Initialize state if needed
            if symbol not in self.strategy_states:
                self.strategy_states[symbol] = StrategyState(
                    symbol=symbol,
                    current_strategy=signal.current_strategy,
                    status=MonitoringStrategyStatus.ACTIVE,
                    assignment_time=datetime.now()
                )
            
            state = self.strategy_states[symbol]
            
            # Check if transition is allowed
            can_transition, reason = self.rate_limiter.can_transition(
                symbol, state.assignment_time
            )
            
            if not can_transition and signal.urgency != "emergency":
                self.logger.warning(f"Transition blocked for {symbol}: {reason}")
                return False
            
            # Add to pending signals
            self._add_pending_signal(state, signal)
            
            # Process based on signal type and confidence
            if signal.signal_type == TransitionSignalType.EMERGENCY_EXIT:
                return self._handle_emergency_exit(state, signal)
            elif signal.confidence >= self.emergency_exit_threshold:
                return self._handle_high_confidence_transition(state, signal)
            elif signal.confidence >= self.confirmation_required_threshold:
                return self._handle_confirmation_required_transition(state, signal)
            else:
                return self._handle_low_confidence_signal(state, signal)
            
        except Exception as e:
            self.logger.error(f"Error processing transition signal for {signal.symbol}: {e}")
            return False
    
    def _add_pending_signal(self, state: StrategyState, signal: TransitionSignal):
        """Add signal to pending list with cleanup"""
        # Remove expired signals
        now = datetime.now()
        state.pending_signals = [
            s for s in state.pending_signals 
            if (now - s.timestamp).total_seconds() < self.signal_expiry_minutes * 60
        ]
        
        # Add new signal
        state.pending_signals.append(signal)
        
        # Keep list manageable
        if len(state.pending_signals) > self.max_pending_signals:
            state.pending_signals = state.pending_signals[-self.max_pending_signals:]
    
    def _handle_emergency_exit(self, state: StrategyState, signal: TransitionSignal) -> bool:
        """Handle emergency exit signal"""
        try:
            self.logger.critical(f"EMERGENCY EXIT triggered for {state.symbol}: {signal.reason}")
            
            # Set emergency mode
            state.emergency_mode = True
            state.status = MonitoringStrategyStatus.EMERGENCY_EXIT
            
            # Send emergency exit message
            success = self._send_strategy_transition_message(state, signal, emergency=True)
            
            if success:
                self.rate_limiter.record_transition(state.symbol)
                state.last_transition_time = datetime.now()
                state.transition_count += 1
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error handling emergency exit for {state.symbol}: {e}")
            return False
    
    def _handle_high_confidence_transition(self, state: StrategyState, signal: TransitionSignal) -> bool:
        """Handle high confidence transition signal"""
        try:
            self.logger.info(f"High confidence transition for {state.symbol}: {signal.confidence:.2f}")
            
            # Execute transition immediately
            state.status = MonitoringStrategyStatus.TRANSITIONING
            
            success = self._send_strategy_transition_message(state, signal)
            
            if success:
                self.rate_limiter.record_transition(state.symbol)
                state.current_strategy = signal.recommended_strategy
                state.assignment_time = datetime.now()
                state.last_transition_time = datetime.now()
                state.transition_count += 1
                state.status = MonitoringStrategyStatus.ACTIVE
                
                # Clear pending signals
                state.pending_signals = []
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error handling high confidence transition for {state.symbol}: {e}")
            return False
    
    def _handle_confirmation_required_transition(self, state: StrategyState, signal: TransitionSignal) -> bool:
        """Handle transition requiring confirmation"""
        try:
            self.logger.info(f"Confirmation required for {state.symbol} transition: {signal.confidence:.2f}")
            
            state.status = MonitoringStrategyStatus.PENDING_CONFIRMATION
            state.confirmation_required = True
            
            # Check if we have multiple confirming signals
            confirming_signals = [
                s for s in state.pending_signals 
                if s.recommended_strategy == signal.recommended_strategy and s.confidence >= 0.5
            ]
            
            if len(confirming_signals) >= 2:  # Multi-signal confirmation
                self.logger.info(f"Multi-signal confirmation achieved for {state.symbol}")
                return self._handle_high_confidence_transition(state, signal)
            else:
                # Send confirmation request
                return self._send_confirmation_request(state, signal)
                
        except Exception as e:
            self.logger.error(f"Error handling confirmation required transition for {state.symbol}: {e}")
            return False
    
    def _handle_low_confidence_signal(self, state: StrategyState, signal: TransitionSignal) -> bool:
        """Handle low confidence signal"""
        try:
            self.logger.debug(f"Low confidence signal for {state.symbol}: {signal.confidence:.2f}")
            
            # Just add to pending signals for potential future confirmation
            # Signal is already added in process_transition_signal
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling low confidence signal for {state.symbol}: {e}")
            return False
    
    def _update_strategy_status(self, state: StrategyState, performance_metrics: Dict[str, Any]):
        """Update strategy status based on performance"""
        try:
            # Check for failure conditions
            if (state.consecutive_poor_performance >= 3 or 
                performance_metrics.get('risk_score', 50) > 80):
                if state.status != MonitoringStrategyStatus.FAILED:
                    self.logger.warning(f"Strategy marked as FAILED for {state.symbol}")
                    state.status = MonitoringStrategyStatus.FAILED
            
            # Check for recovery
            elif (state.status == MonitoringStrategyStatus.FAILED and 
                  state.performance_score > 60.0 and 
                  state.consecutive_poor_performance == 0):
                self.logger.info(f"Strategy recovered for {state.symbol}")
                state.status = MonitoringStrategyStatus.ACTIVE
            
        except Exception as e:
            self.logger.error(f"Error updating strategy status for {state.symbol}: {e}")
    
    def _send_strategy_transition_message(self, state: StrategyState, signal: TransitionSignal, emergency: bool = False) -> bool:
        """Send strategy transition message to execution system"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                return False
            
            message_data = {
                'action_type': 'strategy_transition',
                'symbol': state.symbol,
                'current_strategy': state.current_strategy,
                'new_strategy': signal.recommended_strategy,
                'confidence': signal.confidence,
                'reason': signal.reason,
                'urgency': signal.urgency,
                'emergency': emergency,
                'market_context': signal.market_context,
                'performance_metrics': signal.performance_metrics,
                'timestamp': datetime.now().isoformat()
            }
            
            priority = MessagePriority.HIGH if emergency else MessagePriority.MEDIUM
            
            transition_message = create_process_message(
                message_type="strategy_transition",
                sender_id="decision_engine",
                priority=priority,
                data=message_data
            )
            
            success = redis_manager.send_message("strategy_assignment_engine", transition_message)
            
            if success:
                self.logger.info(f"Strategy transition message sent for {state.symbol}")
            else:
                self.logger.error(f"Failed to send strategy transition message for {state.symbol}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending strategy transition message: {e}")
            return False
    
    def _send_confirmation_request(self, state: StrategyState, signal: TransitionSignal) -> bool:
        """Send confirmation request for strategy transition"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                return False
            
            confirmation_data = {
                'action_type': 'confirmation_request',
                'symbol': state.symbol,
                'current_strategy': state.current_strategy,
                'proposed_strategy': signal.recommended_strategy,
                'confidence': signal.confidence,
                'reason': signal.reason,
                'pending_signals_count': len(state.pending_signals),
                'market_context': signal.market_context,
                'timestamp': datetime.now().isoformat()
            }
            
            confirmation_message = create_process_message(
                message_type="confirmation_request",
                sender_id="decision_engine",
                priority=MessagePriority.MEDIUM,
                data=confirmation_data
            )
            
            success = redis_manager.send_message("strategy_assignment_engine", confirmation_message)
            
            if success:
                self.logger.info(f"Confirmation request sent for {state.symbol}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending confirmation request: {e}")
            return False
    
    def get_strategy_state(self, symbol: str) -> Optional[StrategyState]:
        """Get strategy state for a symbol"""
        return self.strategy_states.get(symbol)
    
    def get_all_strategy_states(self) -> Dict[str, StrategyState]:
        """Get all strategy states"""
        return self.strategy_states.copy()
    
    def clear_emergency_mode(self, symbol: str) -> bool:
        """Clear emergency mode for a symbol"""
        try:
            if symbol in self.strategy_states:
                state = self.strategy_states[symbol]
                state.emergency_mode = False
                if state.status == MonitoringStrategyStatus.EMERGENCY_EXIT:
                    state.status = MonitoringStrategyStatus.ACTIVE
                self.logger.info(f"Emergency mode cleared for {symbol}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error clearing emergency mode for {symbol}: {e}")
            return False
    
    def cleanup_expired_signals(self):
        """Clean up expired signals from all states"""
        try:
            now = datetime.now()
            expired_count = 0
            
            for state in self.strategy_states.values():
                original_count = len(state.pending_signals)
                state.pending_signals = [
                    s for s in state.pending_signals 
                    if (now - s.timestamp).total_seconds() < self.signal_expiry_minutes * 60
                ]
                expired_count += original_count - len(state.pending_signals)
            
            if expired_count > 0:
                self.logger.debug(f"Cleaned up {expired_count} expired signals")
                
        except Exception as e:
            self.logger.error(f"Error cleaning up expired signals: {e}")
    
    def get_strategy_summary(self) -> Dict[str, Any]:
        """Get comprehensive strategy management summary"""
        try:
            return {
                'total_strategies': len(self.strategy_states),
                'active_strategies': sum(1 for s in self.strategy_states.values() 
                                       if s.status == MonitoringStrategyStatus.ACTIVE),
                'transitioning_strategies': sum(1 for s in self.strategy_states.values() 
                                              if s.status == MonitoringStrategyStatus.TRANSITIONING),
                'failed_strategies': sum(1 for s in self.strategy_states.values() 
                                       if s.status == MonitoringStrategyStatus.FAILED),
                'emergency_strategies': sum(1 for s in self.strategy_states.values() 
                                          if s.emergency_mode),
                'pending_confirmations': sum(1 for s in self.strategy_states.values() 
                                           if s.confirmation_required),
                'total_pending_signals': sum(len(s.pending_signals) for s in self.strategy_states.values()),
                'average_performance': sum(s.performance_score for s in self.strategy_states.values()) / len(self.strategy_states) if self.strategy_states else 0.0
            }
            
        except Exception as e:
            self.logger.error(f"Error getting strategy summary: {e}")
            return {'error': str(e)}
