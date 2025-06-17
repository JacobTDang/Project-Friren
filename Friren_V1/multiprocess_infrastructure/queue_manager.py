"""
multiprocess_infrastructure/queue_manager.py

Priority queue management for inter-process communication.
Handles message routing, priority processing, and queue monitoring.
"""

import multiprocessing as mp
import queue
import time
import threading
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging


class MessagePriority(Enum):
    """Message priority levels"""
    CRITICAL = 1    # Health alerts, system errors
    HIGH = 2        # High-confidence trading signals
    NORMAL = 3      # Regular updates, low-confidence signals
    LOW = 4         # Routine maintenance, logging


class MessageType(Enum):
    """Message types for routing"""
    HEALTH_ALERT = "health_alert"
    STRATEGY_SIGNAL = "strategy_signal"
    REGIME_CHANGE = "regime_change"
    POSITION_UPDATE = "position_update"
    SYSTEM_STATUS = "system_status"
    HEARTBEAT = "heartbeat"


@dataclass
class QueueMessage:
    """Standardized message format for all queues"""
    message_type: MessageType
    priority: MessagePriority
    sender_id: str
    recipient_id: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)
    message_id: str = field(default_factory=lambda: f"msg_{int(time.time() * 1000000)}")

    def __lt__(self, other):
        """Enable priority queue sorting"""
        return self.priority.value < other.priority.value


class QueueManager:
    """
    Manages priority queues and message routing for multiprocess system

    Features:
    - Priority-based message processing
    - Message routing and filtering
    - Queue monitoring and statistics
    - Dead letter queue for failed messages
    - Rate limiting and backpressure handling
    """

    def __init__(self, max_queue_size: int = 1000):
        self.max_queue_size = max_queue_size
        self.logger = logging.getLogger("queue_manager")

        # Main priority queue for decision engine
        self.priority_queue = mp.Queue(maxsize=max_queue_size)

        # Health monitoring queue
        self.health_queue = mp.Queue(maxsize=100)

        # Dead letter queue for failed messages
        self.dead_letter_queue = mp.Queue(maxsize=100)

        # Statistics tracking
        self.stats = {
            'messages_sent': 0,
            'messages_processed': 0,
            'messages_failed': 0,
            'queue_full_count': 0,
            'avg_processing_time': 0.0
        }

        # Message routing rules
        self.routing_rules = self._setup_routing_rules()

        # Rate limiting
        self.rate_limits = {
            MessageType.STRATEGY_SIGNAL: 60,    # Max 60 per minute
            MessageType.HEALTH_ALERT: 10,       # Max 10 per minute
            MessageType.REGIME_CHANGE: 5,       # Max 5 per minute
        }
        self.rate_counters = {msg_type: 0 for msg_type in self.rate_limits}
        self.rate_reset_time = time.time()

        self.logger.info("QueueManager initialized")

    def _setup_routing_rules(self) -> Dict[MessageType, Dict]:
        """Setup message routing and filtering rules"""
        return {
            MessageType.HEALTH_ALERT: {
                'priority': MessagePriority.CRITICAL,
                'recipients': ['market_decision_engine', 'orchestrator'],
                'retain_seconds': 300  # 5 minutes
            },
            MessageType.STRATEGY_SIGNAL: {
                'priority': MessagePriority.HIGH,
                'recipients': ['market_decision_engine'],
                'retain_seconds': 180  # 3 minutes
            },
            MessageType.REGIME_CHANGE: {
                'priority': MessagePriority.HIGH,
                'recipients': ['market_decision_engine', 'strategy_analyzer'],
                'retain_seconds': 600  # 10 minutes
            },
            MessageType.POSITION_UPDATE: {
                'priority': MessagePriority.NORMAL,
                'recipients': ['position_health_monitor', 'orchestrator'],
                'retain_seconds': 60
            },
            MessageType.SYSTEM_STATUS: {
                'priority': MessagePriority.LOW,
                'recipients': ['orchestrator'],
                'retain_seconds': 30
            }
        }

    def send_message(self, message: QueueMessage) -> bool:
        """
        Send message to appropriate queue with rate limiting

        Args:
            message: QueueMessage to send

        Returns:
            True if message sent successfully, False otherwise
        """
        try:
            # Check rate limits
            if not self._check_rate_limit(message.message_type):
                self.logger.warning(f"Rate limit exceeded for {message.message_type}")
                return False

            # Apply routing rules
            routing_rule = self.routing_rules.get(message.message_type)
            if routing_rule:
                message.priority = routing_rule['priority']

            # Try to send to priority queue
            try:
                self.priority_queue.put(message, block=False)
                self.stats['messages_sent'] += 1

                self.logger.debug(f"Sent {message.message_type} from {message.sender_id}")
                return True

            except queue.Full:
                self.stats['queue_full_count'] += 1
                self.logger.warning("Priority queue full, attempting cleanup")

                # Try to clean old messages and retry
                if self._cleanup_old_messages():
                    try:
                        self.priority_queue.put(message, block=False)
                        self.stats['messages_sent'] += 1
                        return True
                    except queue.Full:
                        pass

                # Send to dead letter queue as last resort
                self._send_to_dead_letter(message, "Queue full")
                return False

        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            self._send_to_dead_letter(message, str(e))
            return False

    def get_next_message(self, timeout: float = 1.0) -> Optional[QueueMessage]:
        """
        Get next highest priority message

        Args:
            timeout: Maximum time to wait for message

        Returns:
            QueueMessage or None if timeout
        """
        try:
            start_time = time.time()
            message = self.priority_queue.get(timeout=timeout)

            processing_time = time.time() - start_time
            self._update_processing_stats(processing_time)

            self.stats['messages_processed'] += 1
            return message

        except queue.Empty:
            return None
        except Exception as e:
            self.logger.error(f"Error getting message: {e}")
            return None

    def send_health_alert(self, sender_id: str, alert_type: str, details: Dict[str, Any]) -> bool:
        """Convenience method for sending health alerts"""
        message = QueueMessage(
            message_type=MessageType.HEALTH_ALERT,
            priority=MessagePriority.CRITICAL,
            sender_id=sender_id,
            recipient_id="market_decision_engine",
            payload={
                'alert_type': alert_type,
                'details': details,
                'severity': 'critical' if alert_type in ['position_risk', 'system_error'] else 'warning'
            }
        )
        return self.send_message(message)

    def send_strategy_signal(self, sender_id: str, symbol: str, signal_data: Dict[str, Any]) -> bool:
        """Convenience method for sending strategy signals"""
        # Determine priority based on confidence
        confidence = signal_data.get('confidence', 0)
        priority = MessagePriority.HIGH if confidence >= 80 else MessagePriority.NORMAL

        message = QueueMessage(
            message_type=MessageType.STRATEGY_SIGNAL,
            priority=priority,
            sender_id=sender_id,
            recipient_id="market_decision_engine",
            payload={
                'symbol': symbol,
                'signal': signal_data
            }
        )
        return self.send_message(message)

    def send_regime_change(self, sender_id: str, old_regime: str, new_regime: str, confidence: float) -> bool:
        """Convenience method for sending regime change notifications"""
        message = QueueMessage(
            message_type=MessageType.REGIME_CHANGE,
            priority=MessagePriority.HIGH,
            sender_id=sender_id,
            recipient_id="ALL",  # Broadcast to all processes
            payload={
                'old_regime': old_regime,
                'new_regime': new_regime,
                'confidence': confidence,
                'timestamp': datetime.now()
            }
        )
        return self.send_message(message)

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics and health metrics"""
        try:
            priority_queue_size = self.priority_queue.qsize()
            health_queue_size = self.health_queue.qsize()
            dead_letter_size = self.dead_letter_queue.qsize()
        except NotImplementedError:
            # Some systems don't support qsize()
            priority_queue_size = -1
            health_queue_size = -1
            dead_letter_size = -1

        return {
            **self.stats,
            'priority_queue_size': priority_queue_size,
            'health_queue_size': health_queue_size,
            'dead_letter_size': dead_letter_size,
            'queue_utilization': priority_queue_size / self.max_queue_size if priority_queue_size >= 0 else 0,
            'uptime_minutes': (time.time() - self.rate_reset_time) / 60
        }

    def _check_rate_limit(self, message_type: MessageType) -> bool:
        """Check if message type is within rate limits"""
        if message_type not in self.rate_limits:
            return True

        # Reset counters every minute
        if time.time() - self.rate_reset_time > 60:
            self.rate_counters = {msg_type: 0 for msg_type in self.rate_limits}
            self.rate_reset_time = time.time()

        # Check limit
        if self.rate_counters[message_type] >= self.rate_limits[message_type]:
            return False

        self.rate_counters[message_type] += 1
        return True

    def _cleanup_old_messages(self) -> bool:
        """Remove old messages to free queue space"""
        try:
            # This is a simplified cleanup - in production you'd want more sophisticated logic
            cleaned = 0
            temp_messages = []

            # Drain some messages and keep only recent ones
            while cleaned < 10 and not self.priority_queue.empty():
                try:
                    msg = self.priority_queue.get_nowait()
                    age_seconds = (datetime.now() - msg.timestamp).total_seconds()

                    # Keep message if it's recent or high priority
                    if age_seconds < 300 or msg.priority in [MessagePriority.CRITICAL, MessagePriority.HIGH]:
                        temp_messages.append(msg)
                    else:
                        cleaned += 1

                except queue.Empty:
                    break

            # Put kept messages back
            for msg in temp_messages:
                try:
                    self.priority_queue.put_nowait(msg)
                except queue.Full:
                    break

            self.logger.info(f"Cleaned {cleaned} old messages from queue")
            return cleaned > 0

        except Exception as e:
            self.logger.error(f"Error during queue cleanup: {e}")
            return False

    def _send_to_dead_letter(self, message: QueueMessage, reason: str):
        """Send failed message to dead letter queue"""
        try:
            dead_letter_entry = {
                'original_message': message,
                'failure_reason': reason,
                'failure_time': datetime.now()
            }
            self.dead_letter_queue.put_nowait(dead_letter_entry)
            self.stats['messages_failed'] += 1

        except queue.Full:
            self.logger.error("Dead letter queue full - dropping message")
        except Exception as e:
            self.logger.error(f"Error sending to dead letter queue: {e}")

    def _update_processing_stats(self, processing_time: float):
        """Update running average of processing times"""
        alpha = 0.1  # Smoothing factor
        self.stats['avg_processing_time'] = (
            alpha * processing_time +
            (1 - alpha) * self.stats['avg_processing_time']
        )
