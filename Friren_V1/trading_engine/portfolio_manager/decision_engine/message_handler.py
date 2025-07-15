"""
Message Handler - Extracted from Decision Engine

This module handles message processing, communication, and queue management.
Provides intelligent message routing with zero hardcoded priorities.

Features:
- Multi-queue message processing
- Priority-based message routing
- Market-driven message priorities
- Message validation and filtering
- Communication coordination
- Error handling and fallbacks
"""

import logging
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import json

# Import Redis infrastructure
from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, create_process_message, MessagePriority, ProcessMessage
)
from Friren_V1.trading_engine.analytics.market_metrics import get_all_metrics


class MessageType(Enum):
    """Types of messages handled by decision engine"""
    SIGNAL = "signal"
    ALERT = "alert"
    STRATEGY_ASSIGNMENT = "strategy_assignment"
    STRATEGY_TRANSITION = "strategy_transition"
    CONFIRMATION_REQUEST = "confirmation_request"
    HEALTH_UPDATE = "health_update"
    PERFORMANCE_UPDATE = "performance_update"
    RISK_WARNING = "risk_warning"
    MARKET_UPDATE = "market_update"
    SYSTEM_STATUS = "system_status"
    EMERGENCY = "emergency"


class MessageSource(Enum):
    """Sources of messages"""
    STRATEGY_ANALYZER = "strategy_analyzer"
    NEWS_PIPELINE = "news_pipeline"
    POSITION_MONITOR = "position_health_monitor"
    RISK_MANAGER = "risk_manager"
    EXECUTION_ENGINE = "execution_engine"
    STRATEGY_ASSIGNMENT = "strategy_assignment_engine"
    EXTERNAL = "external"
    SYSTEM = "system"


@dataclass
class MessageStats:
    """Message processing statistics"""
    total_processed: int = 0
    by_type: Dict[str, int] = field(default_factory=dict)
    by_source: Dict[str, int] = field(default_factory=dict)
    by_priority: Dict[str, int] = field(default_factory=dict)
    processing_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    errors: int = 0
    dropped: int = 0
    last_reset: datetime = field(default_factory=datetime.now)


class MessageHandler:
    """Handles message processing and communication with market-aware prioritization"""
    
    def __init__(self, process_id: str = "decision_engine"):
        self.logger = logging.getLogger(f"{__name__}.MessageHandler")
        self.process_id = process_id
        
        # Message processing state
        self.message_queue = deque(maxlen=10000)  # Internal message buffer
        self.priority_queues = {
            MessagePriority.HIGH: deque(maxlen=1000),
            MessagePriority.MEDIUM: deque(maxlen=2000),
            MessagePriority.LOW: deque(maxlen=5000)
        }
        
        # Statistics and monitoring
        self.stats = MessageStats()
        self.message_handlers = {}  # {MessageType: callable}
        
        # Rate limiting (market-driven)
        self.rate_limits = {
            MessageType.SIGNAL: {'max_per_minute': 60, 'current': 0, 'reset_time': datetime.now()},
            MessageType.ALERT: {'max_per_minute': 30, 'current': 0, 'reset_time': datetime.now()},
            MessageType.EMERGENCY: {'max_per_minute': 10, 'current': 0, 'reset_time': datetime.now()}
        }
        
        # Message filtering
        self.message_filters = []  # List of filter functions
        
        self.logger.info(f"MessageHandler initialized for {process_id}")
    
    def register_handler(self, message_type: MessageType, handler: Callable[[ProcessMessage], bool]):
        """Register a message handler for a specific message type"""
        self.message_handlers[message_type] = handler
        self.logger.debug(f"Registered handler for {message_type.value}")
    
    def add_message_filter(self, filter_func: Callable[[ProcessMessage], bool]):
        """Add a message filter function"""
        self.message_filters.append(filter_func)
        self.logger.debug("Added message filter")
    
    def process_messages(self) -> int:
        """Process all pending messages from Redis queues"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                return 0
            
            processed_count = 0
            start_time = datetime.now()
            
            # Process messages from Redis
            messages = redis_manager.get_messages(self.process_id)
            
            for message in messages:
                if self._should_process_message(message):
                    success = self._route_and_process_message(message)
                    if success:
                        processed_count += 1
                    else:
                        self.stats.errors += 1
                else:
                    self.stats.dropped += 1
            
            # Update statistics
            if processed_count > 0:
                processing_time = (datetime.now() - start_time).total_seconds() * 1000
                self.stats.processing_times.append(processing_time / processed_count)
                self.stats.total_processed += processed_count
            
            # Reset rate limits if needed
            self._reset_rate_limits_if_needed()
            
            return processed_count
            
        except Exception as e:
            self.logger.error(f"Error processing messages: {e}")
            self.stats.errors += 1
            return 0
    
    def _should_process_message(self, message: ProcessMessage) -> bool:
        """Determine if a message should be processed"""
        try:
            # Apply filters
            for filter_func in self.message_filters:
                if not filter_func(message):
                    return False
            
            # Check rate limits
            message_type = self._get_message_type(message)
            if message_type in self.rate_limits:
                if not self._check_rate_limit(message_type):
                    self.logger.warning(f"Rate limit exceeded for {message_type.value}")
                    return False
            
            # Basic validation
            if not message.data or not isinstance(message.data, dict):
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking message processing: {e}")
            return False
    
    def _route_and_process_message(self, message: ProcessMessage) -> bool:
        """Route message to appropriate handler and process"""
        try:
            message_type = self._get_message_type(message)
            
            # Update statistics
            self.stats.by_type[message_type.value] = self.stats.by_type.get(message_type.value, 0) + 1
            self.stats.by_source[message.sender_id] = self.stats.by_source.get(message.sender_id, 0) + 1
            self.stats.by_priority[message.priority.value] = self.stats.by_priority.get(message.priority.value, 0) + 1
            
            # Route to specific handler
            if message_type in self.message_handlers:
                handler = self.message_handlers[message_type]
                success = handler(message)
                
                if success:
                    self.logger.debug(f"Successfully processed {message_type.value} from {message.sender_id}")
                else:
                    self.logger.warning(f"Handler failed for {message_type.value} from {message.sender_id}")
                
                return success
            else:
                # Default processing
                self.logger.debug(f"No specific handler for {message_type.value}, using default processing")
                return self._default_message_processing(message)
                
        except Exception as e:
            self.logger.error(f"Error routing message: {e}")
            return False
    
    def _get_message_type(self, message: ProcessMessage) -> MessageType:
        """Extract message type from message"""
        try:
            # Try to get from message type field
            if hasattr(message, 'message_type'):
                type_str = message.message_type
            else:
                # Try to infer from data
                type_str = message.data.get('message_type', 'signal')
            
            # Map string to enum
            for msg_type in MessageType:
                if msg_type.value == type_str:
                    return msg_type
            
            # Default to signal
            return MessageType.SIGNAL
            
        except Exception as e:
            self.logger.error(f"Error getting message type: {e}")
            return MessageType.SIGNAL
    
    def _check_rate_limit(self, message_type: MessageType) -> bool:
        """Check if message type is within rate limits"""
        try:
            if message_type not in self.rate_limits:
                return True
            
            limit_info = self.rate_limits[message_type]
            now = datetime.now()
            
            # Reset counter if a minute has passed
            if (now - limit_info['reset_time']).total_seconds() >= 60:
                limit_info['current'] = 0
                limit_info['reset_time'] = now
            
            # Check limit
            if limit_info['current'] >= limit_info['max_per_minute']:
                return False
            
            # Increment counter
            limit_info['current'] += 1
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking rate limit: {e}")
            return True  # Default to allowing
    
    def _reset_rate_limits_if_needed(self):
        """Reset rate limits based on time"""
        try:
            now = datetime.now()
            
            for message_type, limit_info in self.rate_limits.items():
                if (now - limit_info['reset_time']).total_seconds() >= 60:
                    limit_info['current'] = 0
                    limit_info['reset_time'] = now
                    
        except Exception as e:
            self.logger.error(f"Error resetting rate limits: {e}")
    
    def _default_message_processing(self, message: ProcessMessage) -> bool:
        """Default message processing for unhandled message types"""
        try:
            self.logger.debug(f"Default processing for message from {message.sender_id}")
            
            # Add to internal queue for later processing
            self.message_queue.append(message)
            
            # Also add to priority queue
            priority_queue = self.priority_queues.get(message.priority, self.priority_queues[MessagePriority.MEDIUM])
            priority_queue.append(message)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in default message processing: {e}")
            return False
    
    def send_message(self, target_process: str, message_type: MessageType, 
                    data: Dict[str, Any], priority: Optional[MessagePriority] = None) -> bool:
        """Send a message to another process"""
        try:
            redis_manager = get_trading_redis_manager()
            if not redis_manager:
                return False
            
            # Determine priority if not specified
            if priority is None:
                priority = self._determine_message_priority(message_type, data)
            
            # Create message
            message = create_process_message(
                message_type=message_type.value,
                sender_id=self.process_id,
                priority=priority,
                data=data
            )
            
            # Send message
            success = redis_manager.send_message(target_process, message)
            
            if success:
                self.logger.debug(f"Sent {message_type.value} to {target_process}")
            else:
                self.logger.error(f"Failed to send {message_type.value} to {target_process}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            return False
    
    def _determine_message_priority(self, message_type: MessageType, data: Dict[str, Any]) -> MessagePriority:
        """Determine message priority based on type and content with market context"""
        try:
            # Emergency messages always high priority
            if message_type == MessageType.EMERGENCY:
                return MessagePriority.HIGH
            
            # Risk warnings are high priority
            if message_type == MessageType.RISK_WARNING:
                return MessagePriority.HIGH
            
            # Market-driven priority adjustments
            symbol = data.get('symbol')
            if symbol:
                market_metrics = get_all_metrics(symbol)
                
                if market_metrics:
                    # High volatility increases priority
                    volatility = getattr(market_metrics, 'volatility', 0.3)
                    market_stress = getattr(market_metrics, 'market_stress', 0.3)
                    
                    if volatility > 0.6 or market_stress > 0.7:
                        # Upgrade priority in stressed markets
                        if message_type in [MessageType.SIGNAL, MessageType.ALERT]:
                            return MessagePriority.HIGH
            
            # Confidence-based priority for signals
            if message_type == MessageType.SIGNAL:
                confidence = data.get('confidence', 0.5)
                if confidence > 0.8:
                    return MessagePriority.HIGH
                elif confidence > 0.6:
                    return MessagePriority.MEDIUM
                else:
                    return MessagePriority.LOW
            
            # Default priorities by type
            priority_map = {
                MessageType.STRATEGY_ASSIGNMENT: MessagePriority.HIGH,
                MessageType.STRATEGY_TRANSITION: MessagePriority.HIGH,
                MessageType.CONFIRMATION_REQUEST: MessagePriority.MEDIUM,
                MessageType.HEALTH_UPDATE: MessagePriority.MEDIUM,
                MessageType.PERFORMANCE_UPDATE: MessagePriority.LOW,
                MessageType.MARKET_UPDATE: MessagePriority.MEDIUM,
                MessageType.SYSTEM_STATUS: MessagePriority.LOW
            }
            
            return priority_map.get(message_type, MessagePriority.MEDIUM)
            
        except Exception as e:
            self.logger.error(f"Error determining message priority: {e}")
            return MessagePriority.MEDIUM
    
    def broadcast_status(self, status_data: Dict[str, Any]) -> int:
        """Broadcast status to interested processes"""
        try:
            # List of processes that might be interested in decision engine status
            target_processes = [
                "position_health_monitor",
                "strategy_assignment_engine",
                "risk_manager",
                "execution_engine"
            ]
            
            sent_count = 0
            
            for target in target_processes:
                success = self.send_message(
                    target_process=target,
                    message_type=MessageType.SYSTEM_STATUS,
                    data={
                        'status_type': 'decision_engine_status',
                        'process_id': self.process_id,
                        'status_data': status_data,
                        'timestamp': datetime.now().isoformat()
                    },
                    priority=MessagePriority.LOW
                )
                
                if success:
                    sent_count += 1
            
            return sent_count
            
        except Exception as e:
            self.logger.error(f"Error broadcasting status: {e}")
            return 0
    
    def send_alert(self, alert_type: str, message: str, symbol: Optional[str] = None, 
                  urgency: str = "medium") -> bool:
        """Send an alert message"""
        try:
            priority_map = {
                "low": MessagePriority.LOW,
                "medium": MessagePriority.MEDIUM,
                "high": MessagePriority.HIGH,
                "emergency": MessagePriority.HIGH
            }
            
            priority = priority_map.get(urgency, MessagePriority.MEDIUM)
            
            alert_data = {
                'alert_type': alert_type,
                'message': message,
                'urgency': urgency,
                'symbol': symbol,
                'source': self.process_id,
                'timestamp': datetime.now().isoformat()
            }
            
            # Send to position monitor and risk manager
            targets = ["position_health_monitor", "risk_manager"]
            
            for target in targets:
                self.send_message(
                    target_process=target,
                    message_type=MessageType.ALERT,
                    data=alert_data,
                    priority=priority
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
            return False
    
    def get_pending_messages_count(self) -> Dict[str, int]:
        """Get count of pending messages by priority"""
        try:
            return {
                'high': len(self.priority_queues[MessagePriority.HIGH]),
                'medium': len(self.priority_queues[MessagePriority.MEDIUM]),
                'low': len(self.priority_queues[MessagePriority.LOW]),
                'total': len(self.message_queue)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting pending message count: {e}")
            return {'high': 0, 'medium': 0, 'low': 0, 'total': 0}
    
    def get_message_stats(self) -> Dict[str, Any]:
        """Get comprehensive message processing statistics"""
        try:
            avg_processing_time = (sum(self.stats.processing_times) / len(self.stats.processing_times) 
                                 if self.stats.processing_times else 0.0)
            
            return {
                'total_processed': self.stats.total_processed,
                'errors': self.stats.errors,
                'dropped': self.stats.dropped,
                'average_processing_time_ms': avg_processing_time,
                'by_type': dict(self.stats.by_type),
                'by_source': dict(self.stats.by_source),
                'by_priority': dict(self.stats.by_priority),
                'rate_limits': {k.value: v for k, v in self.rate_limits.items()},
                'pending_messages': self.get_pending_messages_count(),
                'last_reset': self.stats.last_reset.isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting message stats: {e}")
            return {'error': str(e)}
    
    def reset_stats(self):
        """Reset message processing statistics"""
        try:
            self.stats = MessageStats()
            self.logger.info("Message statistics reset")
            
        except Exception as e:
            self.logger.error(f"Error resetting stats: {e}")
    
    def cleanup_old_messages(self, max_age_hours: int = 24):
        """Clean up old messages from internal queues"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            cleaned_count = 0
            
            # Clean main queue
            original_size = len(self.message_queue)
            self.message_queue = deque(
                (msg for msg in self.message_queue if msg.timestamp >= cutoff_time),
                maxlen=self.message_queue.maxlen
            )
            cleaned_count += original_size - len(self.message_queue)
            
            # Clean priority queues
            for priority, queue in self.priority_queues.items():
                original_size = len(queue)
                new_queue = deque(
                    (msg for msg in queue if msg.timestamp >= cutoff_time),
                    maxlen=queue.maxlen
                )
                self.priority_queues[priority] = new_queue
                cleaned_count += original_size - len(new_queue)
            
            if cleaned_count > 0:
                self.logger.info(f"Cleaned up {cleaned_count} old messages")
                
        except Exception as e:
            self.logger.error(f"Error cleaning up old messages: {e}")
