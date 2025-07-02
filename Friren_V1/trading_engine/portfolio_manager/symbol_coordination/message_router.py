"""
message_router.py

Symbol Coordination Message Router

Routes messages between processes based on symbol-specific priorities and resource
availability. Provides intelligent message routing for multi-symbol trading
coordination while maintaining system resource constraints.

Key Features:
- Symbol-specific message routing and prioritization
- Queue management for different message types
- Priority-based message scheduling
- Resource-aware message throttling
- Inter-process communication optimization
- Message deduplication and batching

Message Types Handled:
- Decision requests (symbol-specific trading decisions)
- Market data updates (symbol-specific market information)
- Health monitoring (symbol-specific health checks)
- Strategy signals (symbol-specific strategy recommendations)
- Resource allocation (symbol-specific resource requests)
"""

import threading
import time
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
from queue import Queue, PriorityQueue, Empty
import logging
import json
import uuid

# Import existing message infrastructure - FIXED: Use Redis-based infrastructure
try:
    from Friren_V1.multiprocess_infrastructure.trading_redis_manager import ProcessMessage, MessagePriority
    REDIS_INFRASTRUCTURE_AVAILABLE = True
    
    # Adapter for compatibility with existing code
    class MessageType(Enum):
        DECISION_REQUEST = "decision_request"
        MARKET_DATA_UPDATE = "market_data_update"
        HEALTH_CHECK = "health_check"
        STRATEGY_SIGNAL = "strategy_signal"
        RESOURCE_REQUEST = "resource_request"
        SYMBOL_COORDINATION = "symbol_coordination"
        POSITION_UPDATE = "position_update"
        HEALTH_ALERT = "health_alert"
        STRATEGY_REASSESSMENT_REQUEST = "strategy_reassessment_request"
        # FIXED: Add missing message types that were causing router initialization failure
        SYSTEM_STATUS = "system_status"
        SENTIMENT_UPDATE = "sentiment_update"
        NEWS_REQUEST = "news_request"
        NEWS_UPDATE = "news_update"
        MARKET_REGIME_UPDATE = "market_regime_update"

    @dataclass
    class QueueMessage:
        """Compatibility wrapper for ProcessMessage"""
        message_type: MessageType
        priority: MessagePriority
        sender_id: str = ""
        recipient_id: str = ""
        payload: Dict[str, Any] = field(default_factory=dict)
        timestamp: datetime = field(default_factory=datetime.now)
        correlation_id: str = ""
        
        def to_process_message(self) -> ProcessMessage:
            """Convert to Redis ProcessMessage format"""
            return ProcessMessage(
                message_id=self.correlation_id or str(uuid.uuid4()),
                sender=self.sender_id,
                recipient=self.recipient_id,
                message_type=self.message_type.value,
                priority=self.priority,
                timestamp=self.timestamp,
                data=self.payload
            )
            
except ImportError:
    REDIS_INFRASTRUCTURE_AVAILABLE = False
    # Fallback definitions for development/testing
    class MessageType(Enum):
        DECISION_REQUEST = "decision_request"
        MARKET_DATA_UPDATE = "market_data_update"
        HEALTH_CHECK = "health_check"
        STRATEGY_SIGNAL = "strategy_signal"
        RESOURCE_REQUEST = "resource_request"
        SYMBOL_COORDINATION = "symbol_coordination"
        POSITION_UPDATE = "position_update"
        HEALTH_ALERT = "health_alert"
        STRATEGY_REASSESSMENT_REQUEST = "strategy_reassessment_request"
        # FIXED: Add missing message types
        SYSTEM_STATUS = "system_status"
        SENTIMENT_UPDATE = "sentiment_update"
        NEWS_REQUEST = "news_request"
        NEWS_UPDATE = "news_update"
        MARKET_REGIME_UPDATE = "market_regime_update"

    class MessagePriority(Enum):
        CRITICAL = 4
        HIGH = 3
        NORMAL = 2
        LOW = 1

    @dataclass
    class QueueMessage:
        message_type: MessageType
        priority: MessagePriority
        sender_id: str = ""
        recipient_id: str = ""
        payload: Dict[str, Any] = field(default_factory=dict)
        timestamp: datetime = field(default_factory=datetime.now)
        correlation_id: str = ""

from .symbol_config import SymbolMonitoringConfig, MonitoringIntensity, SymbolState
from .resource_manager import ResourceManager, ResourcePriority


class RoutingStrategy(Enum):
    """Message routing strategies"""
    ROUND_ROBIN = "round_robin"          # Even distribution
    PRIORITY_BASED = "priority_based"    # Priority-first routing
    RESOURCE_AWARE = "resource_aware"    # Resource-constrained routing
    SYMBOL_SPECIFIC = "symbol_specific"  # Symbol-based routing
    HYBRID = "hybrid"                    # Combined strategy


class MessageStatus(Enum):
    """Message processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DROPPED = "dropped"
    DEFERRED = "deferred"


@dataclass
class SymbolMessage:
    """Symbol-specific message wrapper"""
    symbol: str
    message: QueueMessage
    routing_priority: int = 0  # 0 = highest priority
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = field(default_factory=datetime.now)
    last_attempt: Optional[datetime] = None
    status: MessageStatus = MessageStatus.PENDING
    processing_time_ms: Optional[float] = None
    error_message: Optional[str] = None

    def __lt__(self, other):
        """Enable priority queue ordering"""
        return self.routing_priority < other.routing_priority


@dataclass
class RoutingRule:
    """Message routing rule"""
    symbol_pattern: str = "*"  # Symbol pattern to match
    message_type: Optional[MessageType] = None
    priority: Optional[MessagePriority] = None
    target_process: Optional[str] = None
    resource_threshold: Optional[float] = None  # Minimum resource availability
    routing_strategy: RoutingStrategy = RoutingStrategy.PRIORITY_BASED
    enabled: bool = True


@dataclass
class RoutingMetrics:
    """Message routing performance metrics"""
    messages_routed: int = 0
    messages_dropped: int = 0
    messages_failed: int = 0
    messages_deferred: int = 0

    # Per-symbol metrics
    symbol_message_counts: Dict[str, int] = field(default_factory=dict)
    symbol_processing_times: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))

    # Queue metrics
    queue_sizes: Dict[str, int] = field(default_factory=dict)
    queue_wait_times: Dict[str, float] = field(default_factory=dict)

    # Resource metrics
    resource_throttle_events: int = 0
    priority_escalations: int = 0

    last_reset: datetime = field(default_factory=datetime.now)


class MessageRouter:
    """Routes messages between processes with symbol-specific optimization"""

    def __init__(self, resource_manager: Optional[ResourceManager] = None):
        self.resource_manager = resource_manager
        self.logger = logging.getLogger(__name__)

        # Message queues - separate queue per symbol for better control
        self.symbol_queues: Dict[str, PriorityQueue] = {}
        self.process_queues: Dict[str, Queue] = {}

        # Routing configuration
        self.routing_rules: List[RoutingRule] = []
        self.routing_strategy = RoutingStrategy.HYBRID

        # Message handlers
        self.message_handlers: Dict[MessageType, Callable] = {}
        self.symbol_handlers: Dict[str, Callable] = {}

        # Metrics and monitoring
        self.metrics = RoutingMetrics()
        self.processing_history: deque = deque(maxlen=1000)

        # Thread management
        self._routing_thread = None
        self._monitoring_thread = None
        self._shutdown_event = threading.Event()
        self._lock = threading.RLock()

        # Message deduplication
        self.message_cache: Dict[str, datetime] = {}
        self.cache_ttl = timedelta(minutes=5)

        # Initialize default routing rules
        self._setup_default_routing_rules()

        infrastructure_status = "Redis-based" if REDIS_INFRASTRUCTURE_AVAILABLE else "Fallback"
        self.logger.info(f"MessageRouter initialized with {infrastructure_status} message infrastructure")
        self.logger.info(f"Supported message types: {[mt.value for mt in MessageType]}")

    def start(self):
        """Start message routing threads"""
        if self._routing_thread is None:
            self._routing_thread = threading.Thread(
                target=self._routing_loop,
                name="MessageRouter",
                daemon=True
            )
            self._routing_thread.start()

        if self._monitoring_thread is None:
            self._monitoring_thread = threading.Thread(
                target=self._monitoring_loop,
                name="MessageRouterMonitor",
                daemon=True
            )
            self._monitoring_thread.start()

        self.logger.info("Message routing started")

    def stop(self):
        """Stop message routing"""
        self._shutdown_event.set()

        if self._routing_thread:
            self._routing_thread.join(timeout=5)
            self._routing_thread = None

        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)
            self._monitoring_thread = None

        self.logger.info("Message routing stopped")

    def register_symbol(self, symbol: str, config: SymbolMonitoringConfig):
        """Register a symbol for message routing"""
        with self._lock:
            if symbol not in self.symbol_queues:
                self.symbol_queues[symbol] = PriorityQueue()
                self.metrics.symbol_message_counts[symbol] = 0
                self.logger.info(f"Registered symbol {symbol} for message routing")

    def unregister_symbol(self, symbol: str):
        """Unregister a symbol from message routing"""
        with self._lock:
            if symbol in self.symbol_queues:
                # Clear remaining messages
                while not self.symbol_queues[symbol].empty():
                    try:
                        self.symbol_queues[symbol].get_nowait()
                    except Empty:
                        break

                del self.symbol_queues[symbol]
                if symbol in self.metrics.symbol_message_counts:
                    del self.metrics.symbol_message_counts[symbol]

                self.logger.info(f"Unregistered symbol {symbol} from message routing")

    def route_message(self, symbol: str, message: QueueMessage) -> bool:
        """Route a message for a specific symbol"""
        with self._lock:
            try:
                # Check if symbol is registered
                if symbol not in self.symbol_queues:
                    self.logger.warning(f"Symbol {symbol} not registered for routing")
                    return False

                # Check for message deduplication
                if self._is_duplicate_message(symbol, message):
                    self.logger.debug(f"Duplicate message filtered for {symbol}")
                    return True

                # Check resource availability
                if not self._check_resource_availability(symbol, message):
                    self.logger.warning(f"Resource constraints prevent routing message for {symbol}")
                    self.metrics.resource_throttle_events += 1
                    return False

                # Create symbol message wrapper
                symbol_message = SymbolMessage(
                    symbol=symbol,
                    message=message,
                    routing_priority=self._calculate_routing_priority(symbol, message)
                )

                # Add to appropriate queue
                self.symbol_queues[symbol].put(symbol_message)
                self.metrics.messages_routed += 1
                self.metrics.symbol_message_counts[symbol] += 1

                self.logger.debug(f"Message routed for {symbol}: {message.message_type.value}")
                return True

            except Exception as e:
                self.logger.error(f"Error routing message for {symbol}: {e}")
                self.metrics.messages_failed += 1
                return False

    def get_next_message(self, symbol: str, timeout: float = 1.0) -> Optional[SymbolMessage]:
        """Get next message for a symbol"""
        if symbol not in self.symbol_queues:
            return None

        try:
            # Try to get message with timeout
            symbol_message = self.symbol_queues[symbol].get(timeout=timeout)
            symbol_message.status = MessageStatus.PROCESSING
            symbol_message.last_attempt = datetime.now()

            return symbol_message

        except Empty:
            return None
        except Exception as e:
            self.logger.error(f"Error getting message for {symbol}: {e}")
            return None

    def mark_message_completed(self, symbol_message: SymbolMessage, processing_time_ms: float):
        """Mark a message as completed"""
        symbol_message.status = MessageStatus.COMPLETED
        symbol_message.processing_time_ms = processing_time_ms

        # Update metrics
        if symbol_message.symbol in self.metrics.symbol_processing_times:
            self.metrics.symbol_processing_times[symbol_message.symbol].append(processing_time_ms)

        # Add to processing history
        self.processing_history.append({
            'symbol': symbol_message.symbol,
            'message_type': symbol_message.message.message_type.value,
            'processing_time_ms': processing_time_ms,
            'completed_at': datetime.now()
        })

        self.logger.debug(f"Message completed for {symbol_message.symbol}: {processing_time_ms:.2f}ms")

    def mark_message_failed(self, symbol_message: SymbolMessage, error_message: str):
        """Mark a message as failed"""
        symbol_message.status = MessageStatus.FAILED
        symbol_message.error_message = error_message
        symbol_message.retry_count += 1

        # Retry if under limit
        if symbol_message.retry_count < symbol_message.max_retries:
            symbol_message.status = MessageStatus.PENDING
            symbol_message.routing_priority += 1  # Lower priority for retries

            # Put back in queue for retry
            if symbol_message.symbol in self.symbol_queues:
                self.symbol_queues[symbol_message.symbol].put(symbol_message)
                self.logger.warning(f"Message retry {symbol_message.retry_count} for {symbol_message.symbol}")
        else:
            self.metrics.messages_failed += 1
            self.logger.error(f"Message failed permanently for {symbol_message.symbol}: {error_message}")

    def get_routing_status(self) -> Dict[str, Any]:
        """Get current routing status"""
        with self._lock:
            queue_sizes = {}
            for symbol, queue in self.symbol_queues.items():
                queue_sizes[symbol] = queue.qsize()

            return {
                'registered_symbols': list(self.symbol_queues.keys()),
                'routing_strategy': self.routing_strategy.value,
                'queue_sizes': queue_sizes,
                'metrics': {
                    'messages_routed': self.metrics.messages_routed,
                    'messages_dropped': self.metrics.messages_dropped,
                    'messages_failed': self.metrics.messages_failed,
                    'messages_deferred': self.metrics.messages_deferred,
                    'resource_throttle_events': self.metrics.resource_throttle_events,
                    'priority_escalations': self.metrics.priority_escalations
                },
                'symbol_metrics': dict(self.metrics.symbol_message_counts),
                'processing_history_size': len(self.processing_history),
                'last_reset': self.metrics.last_reset.isoformat()
            }

    def get_symbol_routing_metrics(self, symbol: str) -> Dict[str, Any]:
        """Get routing metrics for a specific symbol"""
        if symbol not in self.symbol_queues:
            return {}

        processing_times = self.metrics.symbol_processing_times.get(symbol, [])

        return {
            'messages_routed': self.metrics.symbol_message_counts.get(symbol, 0),
            'queue_size': self.symbol_queues[symbol].qsize(),
            'processing_times': {
                'count': len(processing_times),
                'avg_ms': sum(processing_times) / len(processing_times) if processing_times else 0,
                'min_ms': min(processing_times) if processing_times else 0,
                'max_ms': max(processing_times) if processing_times else 0
            },
            'recent_messages': [
                entry for entry in self.processing_history
                if entry['symbol'] == symbol
            ][-10:]  # Last 10 messages
        }

    def add_routing_rule(self, rule: RoutingRule):
        """Add a custom routing rule"""
        with self._lock:
            self.routing_rules.append(rule)
            self.logger.info(f"Added routing rule: {rule.symbol_pattern} -> {rule.target_process}")

    def remove_routing_rule(self, rule: RoutingRule):
        """Remove a routing rule"""
        with self._lock:
            if rule in self.routing_rules:
                self.routing_rules.remove(rule)
                self.logger.info(f"Removed routing rule: {rule.symbol_pattern}")

    def _setup_default_routing_rules(self):
        """Setup default routing rules"""
        # High priority messages go first
        self.routing_rules.extend([
            RoutingRule(
                message_type=MessageType.HEALTH_ALERT,
                priority=MessagePriority.CRITICAL,
                routing_strategy=RoutingStrategy.PRIORITY_BASED
            ),
            RoutingRule(
                message_type=MessageType.STRATEGY_SIGNAL,
                priority=MessagePriority.HIGH,
                routing_strategy=RoutingStrategy.RESOURCE_AWARE
            ),
            RoutingRule(
                message_type=MessageType.SYSTEM_STATUS,
                routing_strategy=RoutingStrategy.SYMBOL_SPECIFIC
            )
        ])

    def _calculate_routing_priority(self, symbol: str, message: QueueMessage) -> int:
        """Calculate routing priority for a message (lower = higher priority)"""
        base_priority = 100

        # Message type priority
        if message.message_type == MessageType.HEALTH_ALERT:
            base_priority -= 50
        elif message.message_type == MessageType.STRATEGY_SIGNAL:
            base_priority -= 30
        elif message.message_type == MessageType.SENTIMENT_UPDATE:
            base_priority -= 20
        elif message.message_type == MessageType.SYSTEM_STATUS:
            base_priority -= 10

        # Message priority
        if message.priority == MessagePriority.CRITICAL:
            base_priority -= 40
        elif message.priority == MessagePriority.HIGH:
            base_priority -= 20
        elif message.priority == MessagePriority.NORMAL:
            base_priority -= 10

        # Resource availability boost
        if self.resource_manager and self.resource_manager.can_make_api_call(symbol):
            base_priority -= 5

        return max(0, base_priority)

    def _check_resource_availability(self, symbol: str, message: QueueMessage) -> bool:
        """Check if resources are available for message processing"""
        if not self.resource_manager:
            return True

        # Check API rate limits for certain message types
        if message.message_type in [MessageType.STRATEGY_SIGNAL, MessageType.SYSTEM_STATUS]:
            return self.resource_manager.can_make_api_call(symbol)

        return True

    def _is_duplicate_message(self, symbol: str, message: QueueMessage) -> bool:
        """Check if message is a duplicate"""
        # Create message hash for deduplication
        message_hash = f"{symbol}_{message.message_type.value}_{message.priority.value}_{hash(str(message.payload))}"

        now = datetime.now()

        # Clean old cache entries
        expired_keys = [
            key for key, timestamp in self.message_cache.items()
            if now - timestamp > self.cache_ttl
        ]
        for key in expired_keys:
            del self.message_cache[key]

        # Check if message is duplicate
        if message_hash in self.message_cache:
            if now - self.message_cache[message_hash] < self.cache_ttl:
                return True

        # Cache this message
        self.message_cache[message_hash] = now
        return False

    def _routing_loop(self):
        """Main message routing loop"""
        while not self._shutdown_event.is_set():
            try:
                # Process messages from all symbol queues
                for symbol in list(self.symbol_queues.keys()):
                    try:
                        message = self.get_next_message(symbol, timeout=0.1)
                        if message:
                            # Route message based on strategy
                            self._route_message_to_process(message)
                    except Exception as e:
                        self.logger.error(f"Error processing message for {symbol}: {e}")

                time.sleep(0.1)  # Small delay to prevent CPU spinning

            except Exception as e:
                self.logger.error(f"Error in routing loop: {e}")
                time.sleep(1)

    def _route_message_to_process(self, symbol_message: SymbolMessage):
        """Route message to appropriate process"""
        # This would integrate with the actual process communication
        # For now, we'll just mark it as processed
        processing_time = 10.0  # Simulated processing time
        self.mark_message_completed(symbol_message, processing_time)

    def _monitoring_loop(self):
        """Monitor routing performance and adjust strategies"""
        while not self._shutdown_event.is_set():
            try:
                # Check queue sizes and adjust priorities
                for symbol, queue in self.symbol_queues.items():
                    if queue.qsize() > 50:  # Queue getting full
                        self.logger.warning(f"Queue for {symbol} is getting full: {queue.qsize()} messages")
                        self.metrics.priority_escalations += 1

                # Reset metrics hourly
                now = datetime.now()
                if now - self.metrics.last_reset >= timedelta(hours=1):
                    self._reset_metrics()

                time.sleep(30)  # Monitor every 30 seconds

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(60)

    def _reset_metrics(self):
        """Reset routing metrics"""
        self.metrics = RoutingMetrics()
        self.processing_history.clear()
        self.logger.info("Routing metrics reset")
