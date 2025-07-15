"""
System-Wide Message Coordination Manager
=======================================

Ensures all processes communicate harmoniously through Redis queues without conflicts.
Prevents message collisions, manages priorities, and coordinates complex workflows.

Features:
- Global message priority management
- Cross-process coordination locks
- Message sequencing and deduplication
- Queue health monitoring
- Process synchronization
"""

import uuid
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from collections import defaultdict, deque

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    MessagePriority, ProcessMessage, create_process_message
)


class ProcessType(Enum):
    """Types of trading system processes"""
    DECISION_ENGINE = "decision_engine"
    NEWS_PIPELINE = "news_pipeline"
    STRATEGY_ANALYZER = "strategy_analyzer"
    POSITION_MONITOR = "position_monitor"
    RISK_MANAGER = "risk_manager"
    EXECUTION_ENGINE = "execution_engine"


class MessageType(Enum):
    """Types of messages in the system"""
    NEWS_COLLECTION = "news_collection"
    SENTIMENT_ANALYSIS = "sentiment_analysis"
    TRADING_SIGNAL = "trading_signal"
    RISK_ASSESSMENT = "risk_assessment"
    EXECUTION_ORDER = "execution_order"
    POSITION_UPDATE = "position_update"
    STRATEGY_ASSIGNMENT = "strategy_assignment"
    MARKET_DATA = "market_data"
    SYSTEM_HEALTH = "system_health"


@dataclass
class MessageCoordinationRule:
    """Rules for coordinating specific message types"""
    message_type: MessageType
    max_concurrent: int = 5  # Max concurrent messages of this type
    cooldown_seconds: float = 0.0  # Min time between messages
    requires_lock: bool = False  # Whether this message type requires exclusive lock
    priority_boost: bool = False  # Whether to boost priority during conflicts


@dataclass
class ProcessCoordinationState:
    """Coordination state for a single process"""
    process_id: str
    process_type: ProcessType
    active_messages: Dict[str, datetime] = field(default_factory=dict)
    last_message_time: Dict[MessageType, datetime] = field(default_factory=dict)
    message_sequence: int = 0
    locks_held: Set[str] = field(default_factory=set)


@dataclass
class SystemCoordinationState:
    """Global system coordination state"""
    processes: Dict[str, ProcessCoordinationState] = field(default_factory=dict)
    global_message_sequence: int = 0
    active_locks: Dict[str, Tuple[str, datetime]] = field(default_factory=dict)  # lock_name -> (process_id, timestamp)
    message_queues: Dict[str, deque] = field(default_factory=lambda: defaultdict(deque))
    system_paused: bool = False
    pause_reason: Optional[str] = None


class SystemMessageCoordinator:
    """
    System-wide message coordination manager
    Ensures all processes work in harmony without conflicts
    """
    
    def __init__(self, redis_manager=None):
        self.redis_manager = redis_manager
        self.logger = logging.getLogger(__name__)
        
        # Load configuration from config manager
        from .coordination_config import get_coordination_config
        self.config_manager = get_coordination_config()
        
        # Global coordination state
        self.state = SystemCoordinationState()
        self.coordination_lock = threading.RLock()  # Reentrant lock for nested operations
        
        # Load coordination rules from configuration
        self.coordination_rules = self._load_coordination_rules()
        
        # Load process priority mapping from configuration
        self.process_priorities = self._load_process_priorities()
    
    def _load_coordination_rules(self) -> Dict[MessageType, MessageCoordinationRule]:
        """Load coordination rules from configuration"""
        try:
            message_config = self.config_manager.message_config
            rules = {}
            
            for message_type in MessageType:
                # Get configuration values for this message type
                message_type_str = message_type.value
                
                max_concurrent = message_config.max_concurrent_limits.get(message_type_str, 5)
                cooldown_seconds = message_config.cooldown_periods.get(message_type_str, 0.0)
                
                # Determine if lock is required (for critical message types)
                requires_lock = message_type_str in ['trading_signal', 'risk_assessment', 'execution_order']
                priority_boost = message_type_str in ['trading_signal', 'risk_assessment', 'execution_order']
                
                rules[message_type] = MessageCoordinationRule(
                    message_type=message_type,
                    max_concurrent=max_concurrent,
                    cooldown_seconds=cooldown_seconds,
                    requires_lock=requires_lock,
                    priority_boost=priority_boost
                )
            
            self.logger.info(f"Loaded {len(rules)} coordination rules from configuration")
            return rules
            
        except Exception as e:
            self.logger.error(f"Error loading coordination rules: {e}")
            # Return minimal default rules
            return {
                MessageType.EXECUTION_ORDER: MessageCoordinationRule(
                    MessageType.EXECUTION_ORDER, max_concurrent=1, requires_lock=True, priority_boost=True
                )
            }
    
    def _load_process_priorities(self) -> Dict[ProcessType, MessagePriority]:
        """Load process priorities from configuration"""
        try:
            process_config = self.config_manager.process_config
            priority_weights = process_config.priority_weights
            
            # Convert priority weights to MessagePriority enum values
            priorities = {}
            for process_type in ProcessType:
                process_type_str = process_type.value
                weight = priority_weights.get(process_type_str, 2)  # Default to NORMAL
                
                # Convert weight to MessagePriority
                if weight >= 4:
                    priorities[process_type] = MessagePriority.CRITICAL
                elif weight >= 3:
                    priorities[process_type] = MessagePriority.HIGH
                elif weight >= 2:
                    priorities[process_type] = MessagePriority.NORMAL
                else:
                    priorities[process_type] = MessagePriority.LOW
            
            self.logger.info(f"Loaded process priorities from configuration")
            return priorities
            
        except Exception as e:
            self.logger.error(f"Error loading process priorities: {e}")
            # Return minimal default priorities
            return {
                ProcessType.EXECUTION_ENGINE: MessagePriority.CRITICAL,
                ProcessType.POSITION_MONITOR: MessagePriority.HIGH
            }
    
    def register_process(self, process_id: str, process_type: ProcessType) -> bool:
        """
        Register a process with the coordination system
        
        Args:
            process_id: Unique process identifier
            process_type: Type of process
            
        Returns:
            True if registration successful
        """
        with self.coordination_lock:
            try:
                if process_id not in self.state.processes:
                    self.state.processes[process_id] = ProcessCoordinationState(
                        process_id=process_id,
                        process_type=process_type
                    )
                    self.logger.info(f"Process registered: {process_id} ({process_type.value})")
                return True
            except Exception as e:
                self.logger.error(f"Error registering process {process_id}: {e}")
                return False
    
    def coordinate_message(self, sender: str, recipient: str, message_type: MessageType,
                         data: Dict[str, Any], symbols: Optional[List[str]] = None) -> Optional[str]:
        """
        Coordinate a message send to ensure no conflicts
        
        Args:
            sender: Sending process ID
            recipient: Receiving process ID  
            message_type: Type of message
            data: Message data
            symbols: Symbols involved (for lock coordination)
            
        Returns:
            Message ID if coordination successful, None if rejected
        """
        with self.coordination_lock:
            try:
                # Check if system is paused
                if self.state.system_paused:
                    self.logger.debug(f"Message coordination rejected - system paused: {self.state.pause_reason}")
                    return None
                
                # Get coordination rule
                rule = self.coordination_rules.get(message_type)
                if not rule:
                    rule = MessageCoordinationRule(message_type)  # Default rule
                
                # Check if message can be sent
                if not self._can_send_message(sender, message_type, rule, symbols):
                    return None
                
                # Generate coordinated message
                message_id = f"{message_type.value}_{sender}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
                
                # Acquire locks if needed
                locks_acquired = []
                if rule.requires_lock and symbols:
                    for symbol in symbols:
                        lock_name = f"{message_type.value}_{symbol}"
                        if self._acquire_lock(sender, lock_name):
                            locks_acquired.append(lock_name)
                        else:
                            # Release acquired locks and fail
                            for acquired_lock in locks_acquired:
                                self._release_lock(sender, acquired_lock)
                            return None
                
                # Send coordinated message
                success = self._send_coordinated_message(
                    message_id, sender, recipient, message_type, data, rule, symbols
                )
                
                if success:
                    # Update process state
                    self._update_process_state(sender, message_id, message_type)
                    self.logger.debug(f"Message coordinated: {message_id}")
                    return message_id
                else:
                    # Release locks on failure
                    for acquired_lock in locks_acquired:
                        self._release_lock(sender, acquired_lock)
                    return None
                
            except Exception as e:
                self.logger.error(f"Error coordinating message: {e}")
                return None
    
    def _can_send_message(self, sender: str, message_type: MessageType, 
                         rule: MessageCoordinationRule, symbols: Optional[List[str]]) -> bool:
        """
        Check if message can be sent without conflicts
        
        Args:
            sender: Sending process ID
            message_type: Type of message
            rule: Coordination rule
            symbols: Symbols involved
            
        Returns:
            True if message can be sent
        """
        # Check concurrent message limit
        active_count = sum(
            len([msg for msg_id, timestamp in proc_state.active_messages.items() 
                if msg_id.startswith(message_type.value) and 
                datetime.now() - timestamp < timedelta(minutes=1)])
            for proc_state in self.state.processes.values()
        )
        
        if active_count >= rule.max_concurrent:
            return False
        
        # Check cooldown period
        process_state = self.state.processes.get(sender)
        if process_state:
            last_time = process_state.last_message_time.get(message_type)
            if (last_time and 
                datetime.now() - last_time < timedelta(seconds=rule.cooldown_seconds)):
                return False
        
        # Check symbol locks if required
        if rule.requires_lock and symbols:
            for symbol in symbols:
                lock_name = f"{message_type.value}_{symbol}"
                if lock_name in self.state.active_locks:
                    locked_by, _ = self.state.active_locks[lock_name]
                    if locked_by != sender:  # Different process holds lock
                        return False
        
        return True
    
    def _acquire_lock(self, process_id: str, lock_name: str) -> bool:
        """
        Acquire a coordination lock
        
        Args:
            process_id: Process requesting lock
            lock_name: Name of lock to acquire
            
        Returns:
            True if lock acquired
        """
        if lock_name in self.state.active_locks:
            locked_by, _ = self.state.active_locks[lock_name]
            return locked_by == process_id  # Already holds lock
        
        # Acquire lock
        self.state.active_locks[lock_name] = (process_id, datetime.now())
        
        # Add to process state
        process_state = self.state.processes.get(process_id)
        if process_state:
            process_state.locks_held.add(lock_name)
        
        return True
    
    def _release_lock(self, process_id: str, lock_name: str):
        """
        Release a coordination lock
        
        Args:
            process_id: Process releasing lock
            lock_name: Name of lock to release
        """
        if lock_name in self.state.active_locks:
            locked_by, _ = self.state.active_locks[lock_name]
            if locked_by == process_id:
                del self.state.active_locks[lock_name]
        
        # Remove from process state
        process_state = self.state.processes.get(process_id)
        if process_state:
            process_state.locks_held.discard(lock_name)
    
    def _send_coordinated_message(self, message_id: str, sender: str, recipient: str,
                                message_type: MessageType, data: Dict[str, Any],
                                rule: MessageCoordinationRule, symbols: Optional[List[str]]) -> bool:
        """
        Send coordinated message through Redis
        
        Args:
            message_id: Generated message ID
            sender: Sending process
            recipient: Receiving process
            message_type: Type of message
            data: Message data
            rule: Coordination rule
            symbols: Symbols involved
            
        Returns:
            True if message sent successfully
        """
        if not self.redis_manager:
            return False
        
        try:
            # Determine priority
            sender_state = self.state.processes.get(sender)
            if sender_state:
                base_priority = self.process_priorities.get(sender_state.process_type, MessagePriority.NORMAL)
                if rule.priority_boost:
                    # Boost priority by one level
                    priority_values = list(MessagePriority)
                    current_index = priority_values.index(base_priority)
                    if current_index < len(priority_values) - 1:
                        priority = priority_values[current_index + 1]
                    else:
                        priority = base_priority
                else:
                    priority = base_priority
            else:
                priority = MessagePriority.NORMAL
            
            # Enhanced message data with coordination metadata
            enhanced_data = {
                **data,
                "coordination": {
                    "message_id": message_id,
                    "message_type": message_type.value,
                    "symbols": symbols or [],
                    "sequence": self.state.global_message_sequence,
                    "locks_required": rule.requires_lock,
                    "coordination_timestamp": datetime.now().isoformat()
                }
            }
            
            # Create process message
            message = create_process_message(
                sender=sender,
                recipient=recipient,
                message_type=f"coordinated_{message_type.value}",
                data=enhanced_data,
                priority=priority
            )
            
            # Send to appropriate queue
            queue_name = f"trading_system:coordinated:{recipient}:queue"
            success = self.redis_manager.send_message(message, queue_name)
            
            if success:
                self.state.global_message_sequence += 1
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending coordinated message: {e}")
            return False
    
    def _update_process_state(self, sender: str, message_id: str, message_type: MessageType):
        """
        Update process state after successful message coordination
        
        Args:
            sender: Sending process
            message_id: Message ID
            message_type: Type of message
        """
        process_state = self.state.processes.get(sender)
        if process_state:
            process_state.active_messages[message_id] = datetime.now()
            process_state.last_message_time[message_type] = datetime.now()
            process_state.message_sequence += 1
    
    def complete_message(self, message_id: str, sender: str, success: bool = True):
        """
        Mark message as complete and release resources
        
        Args:
            message_id: Message ID
            sender: Process that sent the message
            success: Whether message was successful
        """
        with self.coordination_lock:
            try:
                process_state = self.state.processes.get(sender)
                if process_state and message_id in process_state.active_messages:
                    # Remove from active messages
                    del process_state.active_messages[message_id]
                    
                    # Release any locks held by this message
                    # (In practice, locks might be released separately)
                    
                    self.logger.debug(f"Message completed: {message_id} (success: {success})")
                
            except Exception as e:
                self.logger.error(f"Error completing message: {e}")
    
    def pause_system(self, reason: str):
        """
        Pause system message coordination
        
        Args:
            reason: Reason for pause
        """
        with self.coordination_lock:
            self.state.system_paused = True
            self.state.pause_reason = reason
            self.logger.warning(f"System message coordination paused: {reason}")
    
    def resume_system(self):
        """Resume system message coordination"""
        with self.coordination_lock:
            self.state.system_paused = False
            self.state.pause_reason = None
            self.logger.info("System message coordination resumed")
    
    def cleanup_stale_state(self):
        """Clean up stale coordination state"""
        with self.coordination_lock:
            try:
                current_time = datetime.now()
                
                # Clean up stale active messages (older than 5 minutes)
                for process_state in self.state.processes.values():
                    stale_messages = [
                        msg_id for msg_id, timestamp in process_state.active_messages.items()
                        if current_time - timestamp > timedelta(minutes=5)
                    ]
                    for msg_id in stale_messages:
                        del process_state.active_messages[msg_id]
                
                # Clean up stale locks (older than 10 minutes)
                stale_locks = [
                    lock_name for lock_name, (process_id, timestamp) in self.state.active_locks.items()
                    if current_time - timestamp > timedelta(minutes=10)
                ]
                for lock_name in stale_locks:
                    process_id, _ = self.state.active_locks[lock_name]
                    self._release_lock(process_id, lock_name)
                
                if stale_messages or stale_locks:
                    self.logger.info(f"Cleaned up {len(stale_messages)} stale messages and {len(stale_locks)} stale locks")
                
            except Exception as e:
                self.logger.error(f"Error cleaning up stale state: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Get current system coordination status
        
        Returns:
            Dictionary with system status information
        """
        with self.coordination_lock:
            return {
                "system_paused": self.state.system_paused,
                "pause_reason": self.state.pause_reason,
                "active_processes": len(self.state.processes),
                "global_sequence": self.state.global_message_sequence,
                "active_locks": len(self.state.active_locks),
                "total_active_messages": sum(
                    len(proc.active_messages) for proc in self.state.processes.values()
                ),
                "processes": {
                    proc_id: {
                        "type": proc_state.process_type.value,
                        "active_messages": len(proc_state.active_messages),
                        "locks_held": len(proc_state.locks_held),
                        "sequence": proc_state.message_sequence
                    }
                    for proc_id, proc_state in self.state.processes.items()
                }
            }


# Global coordinator instance
_system_coordinator: Optional[SystemMessageCoordinator] = None


def get_system_coordinator(redis_manager=None) -> SystemMessageCoordinator:
    """
    Get or create global system message coordinator
    
    Args:
        redis_manager: Redis manager instance
        
    Returns:
        SystemMessageCoordinator instance
    """
    global _system_coordinator
    
    if _system_coordinator is None:
        _system_coordinator = SystemMessageCoordinator(redis_manager)
    
    return _system_coordinator


def coordinate_message(sender: str, recipient: str, message_type: MessageType,
                      data: Dict[str, Any], symbols: Optional[List[str]] = None,
                      redis_manager=None) -> Optional[str]:
    """
    Coordinate a message send through the system coordinator
    
    Args:
        sender: Sending process ID
        recipient: Receiving process ID
        message_type: Type of message
        data: Message data
        symbols: Symbols involved
        redis_manager: Redis manager instance
        
    Returns:
        Message ID if successful
    """
    coordinator = get_system_coordinator(redis_manager)
    return coordinator.coordinate_message(sender, recipient, message_type, data, symbols)


def register_process(process_id: str, process_type: ProcessType, redis_manager=None) -> bool:
    """
    Register a process with the system coordinator
    
    Args:
        process_id: Process identifier
        process_type: Type of process
        redis_manager: Redis manager instance
        
    Returns:
        True if registration successful
    """
    coordinator = get_system_coordinator(redis_manager)
    return coordinator.register_process(process_id, process_type)


def complete_message(message_id: str, sender: str, success: bool = True):
    """
    Mark a coordinated message as complete
    
    Args:
        message_id: Message ID
        sender: Sending process
        success: Whether message was successful
    """
    coordinator = get_system_coordinator()
    coordinator.complete_message(message_id, sender, success)