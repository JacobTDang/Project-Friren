"""
Signal Conflict Resolver - Enhanced Signal Aggregation for News Pipeline Integration

This module extends the existing SignalAggregator to handle specific conflicts that arise
when multiple strategies (news pipeline, technical analysis, etc.) generate competing 
recommendations for the same symbol.

Key Features:
- Symbol-level execution locking to prevent simultaneous trades
- Priority-based signal resolution (risk > news > technical)
- Execution cooldown enforcement
- Real-time conflict detection and resolution
- Integration with existing signal aggregation system

CRITICAL: This prevents financial losses from conflicting recommendations.
"""

import time
import threading
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import logging

# Import existing signal aggregation components
from .signal_aggregator import SignalAggregator, RawSignal, AggregatedSignal, SignalType

# Import Redis and shared models for inter-process communication
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority
)
from Friren_V1.multiprocess_infrastructure.shared_models import (
    SystemMessage, MessageType, TradingRecommendation
)


class SignalPriority(Enum):
    """Priority levels for different signal sources"""
    EMERGENCY = 1      # Emergency halt, system errors
    RISK_OVERRIDE = 2  # Risk manager overrides
    NEWS_HIGH_CONF = 3 # High-confidence news recommendations (>0.8)
    TECHNICAL = 4      # Technical analysis signals
    NEWS_MEDIUM_CONF = 5 # Medium-confidence news recommendations (0.75-0.8)
    SENTIMENT_ONLY = 6 # Pure sentiment without technical backing
    LOW_CONFIDENCE = 7 # Any signal with confidence < 0.75


class ConflictType(Enum):
    """Types of signal conflicts"""
    DIRECTIONAL_CONFLICT = "directional"  # BUY vs SELL for same symbol
    TIMING_CONFLICT = "timing"           # Multiple signals too close together
    STRATEGY_CONFLICT = "strategy"       # Different strategies claiming same symbol
    COOLDOWN_VIOLATION = "cooldown"      # Signal violates execution cooldown
    RESOURCE_CONFLICT = "resource"       # Insufficient resources for all signals


@dataclass
class ConflictingSignalGroup:
    """Group of conflicting signals for the same symbol"""
    symbol: str
    signals: List[RawSignal]
    conflict_types: List[ConflictType]
    detected_at: datetime
    resolution_required: bool = True
    
    def get_highest_priority_signal(self) -> Optional[RawSignal]:
        """Get the signal with highest priority"""
        if not self.signals:
            return None
        
        # Sort by priority (lower enum value = higher priority)
        priority_map = {
            'emergency': SignalPriority.EMERGENCY,
            'risk_manager': SignalPriority.RISK_OVERRIDE,
            'news_pipeline_high': SignalPriority.NEWS_HIGH_CONF,
            'strategy_analyzer': SignalPriority.TECHNICAL,
            'news_pipeline_medium': SignalPriority.NEWS_MEDIUM_CONF,
            'sentiment_analyzer': SignalPriority.SENTIMENT_ONLY
        }
        
        def get_signal_priority(signal: RawSignal) -> int:
            # Determine priority based on source and confidence
            source_id = signal.source_id.lower()
            
            if 'emergency' in source_id or 'halt' in source_id:
                return SignalPriority.EMERGENCY.value
            elif 'risk' in source_id:
                return SignalPriority.RISK_OVERRIDE.value
            elif 'news' in source_id and signal.confidence > 0.8:
                return SignalPriority.NEWS_HIGH_CONF.value
            elif 'strategy' in source_id or 'technical' in source_id:
                return SignalPriority.TECHNICAL.value
            elif 'news' in source_id and signal.confidence >= 0.75:
                return SignalPriority.NEWS_MEDIUM_CONF.value
            elif 'sentiment' in source_id:
                return SignalPriority.SENTIMENT_ONLY.value
            else:
                return SignalPriority.LOW_CONFIDENCE.value
        
        return min(self.signals, key=get_signal_priority)


@dataclass
class ExecutionLock:
    """Per-symbol execution lock to prevent simultaneous trades"""
    symbol: str
    locked_by: str  # Process/source that owns the lock
    locked_at: datetime
    expires_at: datetime
    lock_reason: str
    
    def is_expired(self) -> bool:
        """Check if lock has expired"""
        return datetime.now() > self.expires_at
    
    def time_remaining(self) -> float:
        """Get remaining lock time in seconds"""
        remaining = (self.expires_at - datetime.now()).total_seconds()
        return max(0.0, remaining)


class SignalConflictResolver:
    """
    Enhanced Signal Conflict Resolver
    
    Handles conflicts between multiple signal sources to prevent:
    1. Simultaneous execution attempts for same symbol
    2. Conflicting directional recommendations
    3. Violation of execution cooldown periods
    4. Resource conflicts and position sizing issues
    
    Integrates with existing SignalAggregator for seamless operation.
    """
    
    def __init__(self, base_aggregator: SignalAggregator, cooldown_minutes: int = 15):
        self.logger = logging.getLogger("signal_conflict_resolver")
        self.base_aggregator = base_aggregator
        self.cooldown_minutes = cooldown_minutes
        
        # Thread-safe execution locks
        self.execution_locks: Dict[str, ExecutionLock] = {}
        self.lock_mutex = threading.RLock()
        
        # Execution cooldown tracking
        self.last_executions: Dict[str, datetime] = {}
        self.cooldown_mutex = threading.RLock()
        
        # Conflict detection and resolution
        self.active_conflicts: Dict[str, ConflictingSignalGroup] = {}
        self.conflict_history = deque(maxlen=1000)  # Keep history for analysis
        
        # Strategy ownership tracking
        self.strategy_assignments: Dict[str, str] = {}  # symbol -> strategy_name
        self.assignment_mutex = threading.RLock()
        
        # Redis integration for inter-process coordination
        self.redis_manager = get_trading_redis_manager()
        
        # Performance metrics
        self.stats = {
            'conflicts_detected': 0,
            'conflicts_resolved': 0,
            'signals_blocked': 0,
            'executions_prevented': 0,
            'locks_acquired': 0,
            'locks_expired': 0
        }
        
        self.logger.info(f"SignalConflictResolver initialized with {cooldown_minutes}min cooldown")
    
    def process_signal_with_conflict_resolution(self, signal: RawSignal) -> Optional[AggregatedSignal]:
        """
        Process a signal through conflict resolution before aggregation
        
        Args:
            signal: Raw signal to process
            
        Returns:
            AggregatedSignal if approved for execution, None if blocked
        """
        try:
            symbol = signal.symbol
            
            # Step 1: Check execution cooldown
            if not self._check_execution_cooldown(symbol, signal):
                self.logger.info(f"Signal blocked due to cooldown: {symbol} from {signal.source_id}")
                self.stats['signals_blocked'] += 1
                return None
            
            # Step 2: Check for execution lock
            if not self._check_execution_lock(symbol, signal):
                self.logger.info(f"Signal blocked due to execution lock: {symbol}")
                self.stats['signals_blocked'] += 1
                return None
            
            # Step 3: Detect conflicts with existing signals
            conflicts = self._detect_conflicts(signal)
            
            if conflicts:
                self.stats['conflicts_detected'] += 1
                
                # Resolve conflicts and determine if this signal should proceed
                resolution_result = self._resolve_conflicts(signal, conflicts)
                
                if not resolution_result.should_proceed:
                    self.logger.info(f"Signal blocked after conflict resolution: {symbol}")
                    self.stats['signals_blocked'] += 1
                    return None
                
                # If we get here, this signal won the conflict resolution
                self.stats['conflicts_resolved'] += 1
            
            # Step 4: Add to base aggregator
            success = self.base_aggregator.add_signal(signal)
            if not success:
                self.logger.warning(f"Base aggregator rejected signal: {symbol}")
                return None
            
            # Step 5: Aggregate signals (this may return None if insufficient data)
            aggregated = self.base_aggregator.aggregate_signals(symbol)
            
            if aggregated and aggregated.final_confidence >= 0.75:
                # Step 6: Acquire execution lock for high-confidence signals
                self._acquire_execution_lock(
                    symbol, 
                    signal.source_id, 
                    f"High-confidence signal: {aggregated.get_recommendation()}"
                )
                
                self.logger.info(f"Signal approved for execution: {symbol} "
                                f"{aggregated.get_recommendation()} "
                                f"(confidence: {aggregated.final_confidence:.2f})")
                
                return aggregated
            
            # Signal was aggregated but doesn't meet execution threshold
            return aggregated
            
        except Exception as e:
            self.logger.error(f"Error in conflict resolution for {signal.symbol}: {e}")
            return None
    
    def release_execution_lock(self, symbol: str, executing_process: str) -> bool:
        """
        Release execution lock after trade completion
        
        Args:
            symbol: Symbol to release lock for
            executing_process: Process that owns the lock
            
        Returns:
            bool: True if lock was released successfully
        """
        try:
            with self.lock_mutex:
                if symbol in self.execution_locks:
                    lock = self.execution_locks[symbol]
                    
                    # Verify ownership
                    if lock.locked_by == executing_process:
                        del self.execution_locks[symbol]
                        
                        # Record execution time for cooldown
                        with self.cooldown_mutex:
                            self.last_executions[symbol] = datetime.now()
                        
                        self.logger.info(f"Execution lock released: {symbol} by {executing_process}")
                        return True
                    else:
                        self.logger.warning(f"Lock release denied: {symbol} not owned by {executing_process}")
                        return False
                else:
                    self.logger.debug(f"No lock to release for {symbol}")
                    return True  # No lock exists, consider it "released"
                    
        except Exception as e:
            self.logger.error(f"Error releasing execution lock for {symbol}: {e}")
            return False
    
    def assign_strategy_ownership(self, symbol: str, strategy_name: str) -> bool:
        """
        Assign strategy ownership for a symbol
        
        Args:
            symbol: Symbol to assign
            strategy_name: Name of the strategy taking ownership
            
        Returns:
            bool: True if assignment successful
        """
        try:
            with self.assignment_mutex:
                current_owner = self.strategy_assignments.get(symbol)
                
                if current_owner and current_owner != strategy_name:
                    # Check if current owner is still active
                    if self._is_strategy_active(symbol, current_owner):
                        self.logger.info(f"Strategy assignment blocked: {symbol} owned by {current_owner}")
                        return False
                
                # Assign ownership
                self.strategy_assignments[symbol] = strategy_name
                self.logger.info(f"Strategy ownership assigned: {symbol} -> {strategy_name}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error assigning strategy ownership: {e}")
            return False
    
    def get_conflict_status(self) -> Dict[str, Any]:
        """Get current conflict resolution status"""
        with self.lock_mutex, self.cooldown_mutex, self.assignment_mutex:
            return {
                'active_locks': {
                    symbol: {
                        'locked_by': lock.locked_by,
                        'expires_in_seconds': lock.time_remaining(),
                        'reason': lock.lock_reason
                    }
                    for symbol, lock in self.execution_locks.items()
                    if not lock.is_expired()
                },
                'cooldown_status': {
                    symbol: {
                        'last_execution': last_exec.isoformat(),
                        'cooldown_remaining_minutes': max(0, 
                            self.cooldown_minutes - 
                            (datetime.now() - last_exec).total_seconds() / 60
                        )
                    }
                    for symbol, last_exec in self.last_executions.items()
                    if (datetime.now() - last_exec).total_seconds() / 60 < self.cooldown_minutes
                },
                'strategy_assignments': self.strategy_assignments.copy(),
                'active_conflicts': len(self.active_conflicts),
                'stats': self.stats.copy()
            }
    
    def cleanup_expired_locks(self):
        """Clean up expired locks and old data"""
        try:
            current_time = datetime.now()
            
            # Clean up expired execution locks
            with self.lock_mutex:
                expired_symbols = [
                    symbol for symbol, lock in self.execution_locks.items()
                    if lock.is_expired()
                ]
                
                for symbol in expired_symbols:
                    del self.execution_locks[symbol]
                    self.stats['locks_expired'] += 1
                
                if expired_symbols:
                    self.logger.debug(f"Cleaned up {len(expired_symbols)} expired locks")
            
            # Clean up old execution records (older than 2 hours)
            cutoff_time = current_time - timedelta(hours=2)
            with self.cooldown_mutex:
                old_symbols = [
                    symbol for symbol, last_exec in self.last_executions.items()
                    if last_exec < cutoff_time
                ]
                
                for symbol in old_symbols:
                    del self.last_executions[symbol]
            
            # Clean up inactive strategy assignments
            with self.assignment_mutex:
                inactive_symbols = [
                    symbol for symbol, strategy in self.strategy_assignments.items()
                    if not self._is_strategy_active(symbol, strategy)
                ]
                
                for symbol in inactive_symbols:
                    del self.strategy_assignments[symbol]
                    
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def _check_execution_cooldown(self, symbol: str, signal: RawSignal) -> bool:
        """Check if signal violates execution cooldown"""
        with self.cooldown_mutex:
            if symbol not in self.last_executions:
                return True  # No previous execution
            
            last_execution = self.last_executions[symbol]
            minutes_since = (datetime.now() - last_execution).total_seconds() / 60
            
            if minutes_since < self.cooldown_minutes:
                # Check if this is a high-priority override
                if self._is_high_priority_override(signal):
                    self.logger.info(f"Cooldown override approved for high-priority signal: {symbol}")
                    return True
                
                return False
            
            return True
    
    def _check_execution_lock(self, symbol: str, signal: RawSignal) -> bool:
        """Check if symbol has an active execution lock"""
        with self.lock_mutex:
            if symbol not in self.execution_locks:
                return True  # No lock
            
            lock = self.execution_locks[symbol]
            
            # Check if lock has expired
            if lock.is_expired():
                del self.execution_locks[symbol]
                self.stats['locks_expired'] += 1
                return True
            
            # Check if this signal can override the lock
            if self._can_override_lock(signal, lock):
                self.logger.info(f"Execution lock override approved: {symbol}")
                return True
            
            return False
    
    def _detect_conflicts(self, signal: RawSignal) -> List[ConflictType]:
        """Detect conflicts with existing signals"""
        conflicts = []
        symbol = signal.symbol
        
        # Get recent signals for this symbol from base aggregator
        recent_signals = self.base_aggregator._get_recent_signals(symbol, max_age_minutes=5)
        
        if not recent_signals:
            return conflicts
        
        # Check for directional conflicts
        for existing_signal in recent_signals:
            if existing_signal.source_id != signal.source_id:
                # Opposite directions indicate conflict
                if (signal.direction > 0.3 and existing_signal.direction < -0.3) or \
                   (signal.direction < -0.3 and existing_signal.direction > 0.3):
                    conflicts.append(ConflictType.DIRECTIONAL_CONFLICT)
                    break
        
        # Check for timing conflicts (multiple signals too close together)
        recent_signal_times = [s.timestamp for s in recent_signals[-3:]]  # Last 3 signals
        if len(recent_signal_times) >= 2:
            time_diffs = [
                (recent_signal_times[i] - recent_signal_times[i-1]).total_seconds()
                for i in range(1, len(recent_signal_times))
            ]
            if any(diff < 30 for diff in time_diffs):  # Less than 30 seconds apart
                conflicts.append(ConflictType.TIMING_CONFLICT)
        
        return conflicts
    
    def _resolve_conflicts(self, signal: RawSignal, conflicts: List[ConflictType]) -> 'ConflictResolutionResult':
        """Resolve conflicts and determine if signal should proceed"""
        
        @dataclass
        class ConflictResolutionResult:
            should_proceed: bool
            resolution_reason: str
            blocked_signals: List[RawSignal] = field(default_factory=list)
        
        symbol = signal.symbol
        
        # Get competing signals
        recent_signals = self.base_aggregator._get_recent_signals(symbol, max_age_minutes=5)
        competing_signals = [s for s in recent_signals if s.source_id != signal.source_id]
        
        if not competing_signals:
            return ConflictResolutionResult(
                should_proceed=True,
                resolution_reason="No competing signals"
            )
        
        # Create conflict group
        all_signals = competing_signals + [signal]
        conflict_group = ConflictingSignalGroup(
            symbol=symbol,
            signals=all_signals,
            conflict_types=conflicts,
            detected_at=datetime.now()
        )
        
        # Resolve based on priority
        highest_priority_signal = conflict_group.get_highest_priority_signal()
        
        if highest_priority_signal == signal:
            # Our signal has highest priority
            self.logger.info(f"Conflict resolved in favor of {signal.source_id}: {symbol}")
            return ConflictResolutionResult(
                should_proceed=True,
                resolution_reason=f"Highest priority signal from {signal.source_id}",
                blocked_signals=competing_signals
            )
        else:
            # Another signal has higher priority
            self.logger.info(f"Conflict resolved against {signal.source_id}: {symbol}")
            return ConflictResolutionResult(
                should_proceed=False,
                resolution_reason=f"Lower priority than {highest_priority_signal.source_id}"
            )
    
    def _acquire_execution_lock(self, symbol: str, source_id: str, reason: str) -> bool:
        """Acquire execution lock for a symbol"""
        try:
            with self.lock_mutex:
                # Check if already locked
                if symbol in self.execution_locks:
                    existing_lock = self.execution_locks[symbol]
                    if not existing_lock.is_expired():
                        return False
                    else:
                        # Remove expired lock
                        del self.execution_locks[symbol]
                        self.stats['locks_expired'] += 1
                
                # Create new lock (5 minute duration)
                lock = ExecutionLock(
                    symbol=symbol,
                    locked_by=source_id,
                    locked_at=datetime.now(),
                    expires_at=datetime.now() + timedelta(minutes=5),
                    lock_reason=reason
                )
                
                self.execution_locks[symbol] = lock
                self.stats['locks_acquired'] += 1
                
                self.logger.debug(f"Execution lock acquired: {symbol} by {source_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error acquiring execution lock: {e}")
            return False
    
    def _is_high_priority_override(self, signal: RawSignal) -> bool:
        """Check if signal qualifies for high-priority override"""
        source_id = signal.source_id.lower()
        
        # Emergency signals can always override
        if 'emergency' in source_id or 'halt' in source_id:
            return True
        
        # Risk manager signals can override
        if 'risk' in source_id and signal.confidence > 0.8:
            return True
        
        # Very high confidence news signals can override
        if 'news' in source_id and signal.confidence > 0.9:
            return True
        
        return False
    
    def _can_override_lock(self, signal: RawSignal, lock: ExecutionLock) -> bool:
        """Check if signal can override an existing lock"""
        # Only high-priority signals can override locks
        return self._is_high_priority_override(signal)
    
    def _is_strategy_active(self, symbol: str, strategy_name: str) -> bool:
        """Check if a strategy is still actively managing a symbol"""
        # In a real implementation, this would check recent activity
        # For now, assume strategies are active if they have recent signals
        recent_signals = self.base_aggregator._get_recent_signals(symbol, max_age_minutes=30)
        
        return any(strategy_name.lower() in s.source_id.lower() for s in recent_signals)


# Integration helper function
def create_enhanced_signal_resolver(symbol_limit: int = 50, cooldown_minutes: int = 15) -> SignalConflictResolver:
    """
    Create an enhanced signal resolver with conflict resolution
    
    Args:
        symbol_limit: Maximum symbols to track
        cooldown_minutes: Minimum minutes between executions per symbol
        
    Returns:
        SignalConflictResolver instance
    """
    base_aggregator = SignalAggregator(symbol_limit=symbol_limit)
    return SignalConflictResolver(base_aggregator, cooldown_minutes)