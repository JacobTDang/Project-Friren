"""
Redis Queue Coordinator
======================

Manages cross-queue message routing and coordination.
Ensures messages are delivered to the correct queues without conflicts.
Implements intelligent routing, load balancing, and queue health monitoring.

Features:
- Cross-queue message routing
- Load balancing across multiple queues
- Queue health monitoring
- Message deduplication
- Priority-based routing
- Emergency failover mechanisms
"""

import time
import threading
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
import logging
import json
from collections import defaultdict, deque

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    MessagePriority, ProcessMessage, create_process_message, get_trading_redis_manager
)
from .utils.coordination_utils import (
    hash_message_content, calculate_message_priority_score, generate_coordination_id
)


class QueueHealth(Enum):
    """Queue health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    FAILED = "failed"
    UNKNOWN = "unknown"


class RoutingStrategy(Enum):
    """Message routing strategies"""
    ROUND_ROBIN = "round_robin"
    LOAD_BALANCED = "load_balanced"
    PRIORITY_BASED = "priority_based"
    HASH_BASED = "hash_based"
    FAILOVER = "failover"


@dataclass
class QueueInfo:
    """Information about a Redis queue"""
    queue_name: str
    full_queue_key: str
    queue_type: str  # priority, process, topic, etc.
    
    # Health metrics
    health_status: QueueHealth = QueueHealth.UNKNOWN
    last_health_check: Optional[datetime] = None
    
    # Load metrics
    current_size: int = 0
    max_size: int = 10000
    message_rate_per_second: float = 0.0
    avg_processing_time_ms: float = 0.0
    
    # Routing metrics
    total_messages_sent: int = 0
    total_messages_failed: int = 0
    last_message_time: Optional[datetime] = None
    
    # Configuration
    enabled: bool = True
    priority_weight: float = 1.0
    max_concurrent_messages: int = 1000


@dataclass
class RoutingRule:
    """Rules for routing messages to queues"""
    rule_id: str
    source_pattern: str  # Pattern to match source processes
    target_queues: List[str]  # Target queue names
    message_types: List[str]  # Message types this rule applies to
    routing_strategy: RoutingStrategy
    priority_threshold: Optional[MessagePriority] = None
    load_balance_weights: Dict[str, float] = field(default_factory=dict)
    enabled: bool = True


class RedisQueueCoordinator:
    """
    Coordinates message routing across Redis queues
    Manages queue health, load balancing, and routing strategies
    """
    
    def __init__(self, redis_manager=None):
        self.redis_manager = redis_manager or get_trading_redis_manager()
        self.logger = logging.getLogger(__name__)
        
        # Load configuration from config manager
        from .coordination_config import get_coordination_config
        self.config_manager = get_coordination_config()
        queue_config = self.config_manager.queue_config
        
        # Queue management
        self.queues: Dict[str, QueueInfo] = {}
        self.routing_rules: Dict[str, RoutingRule] = {}
        self.coordination_lock = threading.RLock()
        
        # Message tracking with configuration
        dedup_window_minutes = queue_config.deduplication_window_minutes
        max_messages_for_dedup = dedup_window_minutes * 1000  # Estimate based on expected message rate
        self.message_history = deque(maxlen=max_messages_for_dedup)
        self.routing_cache = {}  # Cache routing decisions
        self.cache_ttl_seconds = queue_config.routing_cache_ttl_seconds
        
        # Monitoring with configuration
        self.monitoring_active = False
        self.monitoring_thread = None
        self.health_check_interval = queue_config.health_check_interval_seconds
        
        # Routing state
        self.round_robin_state = defaultdict(int)
        self.last_routing_decision = {}
        
        # Initialize queues and routing rules from configuration
        self._initialize_configuration()
        
        self.logger.info("RedisQueueCoordinator initialized")
    
    def _initialize_configuration(self):
        """Initialize queues and routing rules from configuration"""
        try:
            # Load queues from database or environment, fallback to defaults
            queue_configs = self._load_queue_configurations()
            
            # Register configured queues
            for queue_name, queue_key, queue_type in queue_configs:
                max_size = self.config_manager.queue_config.max_queue_size
                self.register_queue(queue_name, queue_key, queue_type, max_size)
            
            # Load routing rules from configuration
            routing_rules = self._load_routing_rules()
            
            # Register routing rules
            for rule in routing_rules:
                self.routing_rules[rule.rule_id] = rule
            
            self.logger.info(f"Initialized {len(queue_configs)} queues and {len(routing_rules)} routing rules")
            
        except Exception as e:
            self.logger.error(f"Error initializing configuration: {e}")
    
    def _load_queue_configurations(self) -> List[Tuple[str, str, str]]:
        """Load queue configurations from database or environment"""
        try:
            import os
            
            # Try to load from database first
            if hasattr(self.config_manager, 'db_manager') and self.config_manager.db_manager:
                try:
                    db_queues = self._load_queues_from_database()
                    if db_queues:
                        return db_queues
                except Exception as e:
                    self.logger.debug(f"Database queue loading failed: {e}")
            
            # Load from environment or defaults
            queue_configs_env = os.getenv('FRIREN_QUEUE_CONFIGS')
            if queue_configs_env:
                # Parse JSON configuration from environment
                import json
                queue_configs = json.loads(queue_configs_env)
                return [(q['name'], q['key'], q['type']) for q in queue_configs]
            
            # Default queue configurations
            return [
                ("priority", f"{self.redis_manager.QUEUE_PREFIX}:priority", "priority"),
                ("decisions", f"{self.redis_manager.QUEUE_PREFIX}:decisions", "process"),
                ("execution", f"{self.redis_manager.QUEUE_PREFIX}:execution", "process"),
                ("news_general", f"{self.redis_manager.QUEUE_PREFIX}:news_general", "topic"),
                ("news_symbols", f"{self.redis_manager.QUEUE_PREFIX}:news_symbols", "topic"),
                ("health", f"{self.redis_manager.QUEUE_PREFIX}:health", "system"),
                ("coordinated_decision", f"{self.redis_manager.QUEUE_PREFIX}:coordinated:decision_engine", "coordinated"),
                ("coordinated_news", f"{self.redis_manager.QUEUE_PREFIX}:coordinated:news_pipeline", "coordinated"),
                ("coordinated_strategy", f"{self.redis_manager.QUEUE_PREFIX}:coordinated:strategy_analyzer", "coordinated"),
                ("coordinated_position", f"{self.redis_manager.QUEUE_PREFIX}:coordinated:position_monitor", "coordinated")
            ]
            
        except Exception as e:
            self.logger.error(f"Error loading queue configurations: {e}")
            # Minimal fallback
            return [("priority", f"{self.redis_manager.QUEUE_PREFIX}:priority", "priority")]
    
    def _load_queues_from_database(self) -> List[Tuple[str, str, str]]:
        """Load queue configurations from database"""
        # This would query a queue_configurations table
        return []
    
    def _load_routing_rules(self) -> List[RoutingRule]:
        """Load routing rules from configuration"""
        try:
            import os
            
            # Try to load from database first
            if hasattr(self.config_manager, 'db_manager') and self.config_manager.db_manager:
                try:
                    db_rules = self._load_routing_rules_from_database()
                    if db_rules:
                        return db_rules
                except Exception as e:
                    self.logger.debug(f"Database routing rules loading failed: {e}")
            
            # Load from environment or use defaults
            routing_rules_env = os.getenv('FRIREN_ROUTING_RULES')
            if routing_rules_env:
                import json
                rules_config = json.loads(routing_rules_env)
                return [self._create_routing_rule_from_config(rule_config) for rule_config in rules_config]
            
            # Default routing rules
            return [
                RoutingRule(
                    rule_id="news_coordination",
                    source_pattern="news_*",
                    target_queues=["news_general", "news_symbols"],
                    message_types=["coordinated_news_collection"],
                    routing_strategy=RoutingStrategy.HASH_BASED
                ),
                RoutingRule(
                    rule_id="decision_coordination", 
                    source_pattern="decision_*",
                    target_queues=["coordinated_decision"],
                    message_types=["coordinated_trading_signal", "coordinated_risk_assessment"],
                    routing_strategy=RoutingStrategy.PRIORITY_BASED
                ),
                RoutingRule(
                    rule_id="execution_coordination",
                    source_pattern="*",
                    target_queues=["execution"],
                    message_types=["coordinated_execution_order"],
                    routing_strategy=RoutingStrategy.FAILOVER
                )
            ]
            
        except Exception as e:
            self.logger.error(f"Error loading routing rules: {e}")
            return []
    
    def _load_routing_rules_from_database(self) -> List[RoutingRule]:
        """Load routing rules from database"""
        # This would query a routing_rules table
        return []
    
    def _create_routing_rule_from_config(self, config: Dict[str, Any]) -> RoutingRule:
        """Create routing rule from configuration dictionary"""
        return RoutingRule(
            rule_id=config['rule_id'],
            source_pattern=config['source_pattern'],
            target_queues=config['target_queues'],
            message_types=config['message_types'],
            routing_strategy=RoutingStrategy(config['routing_strategy']),
            priority_threshold=MessagePriority(config['priority_threshold']) if 'priority_threshold' in config else None,
            load_balance_weights=config.get('load_balance_weights', {}),
            enabled=config.get('enabled', True)
        )
    
    def register_queue(self, queue_name: str, queue_key: str, queue_type: str,
                      max_size: int = 10000, priority_weight: float = 1.0) -> bool:
        """
        Register a queue for coordination
        
        Args:
            queue_name: Logical queue name
            queue_key: Full Redis queue key
            queue_type: Type of queue (priority, process, topic, etc.)
            max_size: Maximum queue size
            priority_weight: Weight for load balancing
            
        Returns:
            True if registration successful
        """
        with self.coordination_lock:
            try:
                queue_info = QueueInfo(
                    queue_name=queue_name,
                    full_queue_key=queue_key,
                    queue_type=queue_type,
                    max_size=max_size,
                    priority_weight=priority_weight
                )
                
                self.queues[queue_name] = queue_info
                
                # Initialize health check
                self._check_queue_health(queue_name)
                
                self.logger.info(f"Queue registered: {queue_name} ({queue_type})")
                return True
                
            except Exception as e:
                self.logger.error(f"Error registering queue {queue_name}: {e}")
                return False
    
    def route_message(self, message: ProcessMessage, routing_hint: Optional[str] = None) -> bool:
        """
        Route message to appropriate queue(s)
        
        Args:
            message: Message to route
            routing_hint: Optional hint for routing decision
            
        Returns:
            True if routing successful
        """
        with self.coordination_lock:
            try:
                # Check for message deduplication
                message_hash = hash_message_content(message.__dict__)
                if self._is_duplicate_message(message_hash):
                    self.logger.debug(f"Duplicate message detected, skipping: {message.message_id}")
                    return True
                
                # Find applicable routing rules
                applicable_rules = self._find_applicable_rules(message)
                
                if not applicable_rules:
                    # Use default routing
                    return self._route_to_default_queue(message)
                
                # Route using applicable rules
                success = True
                for rule in applicable_rules:
                    if not self._route_with_rule(message, rule, routing_hint):
                        success = False
                
                # Add to message history for deduplication
                self.message_history.append({
                    "hash": message_hash,
                    "timestamp": datetime.now(),
                    "message_id": message.message_id
                })
                
                return success
                
            except Exception as e:
                self.logger.error(f"Error routing message {message.message_id}: {e}")
                return False
    
    def _is_duplicate_message(self, message_hash: str) -> bool:
        """Check if message is a duplicate"""
        cutoff_time = datetime.now() - timedelta(minutes=5)
        
        for hist_entry in self.message_history:
            if (hist_entry["hash"] == message_hash and 
                hist_entry["timestamp"] > cutoff_time):
                return True
        
        return False
    
    def _find_applicable_rules(self, message: ProcessMessage) -> List[RoutingRule]:
        """Find routing rules applicable to the message"""
        applicable_rules = []
        
        for rule in self.routing_rules.values():
            if not rule.enabled:
                continue
            
            # Check message type
            if rule.message_types and message.message_type not in rule.message_types:
                continue
            
            # Check source pattern
            if not self._matches_pattern(message.sender, rule.source_pattern):
                continue
            
            # Check priority threshold
            if (rule.priority_threshold and 
                message.priority.value < rule.priority_threshold.value):
                continue
            
            applicable_rules.append(rule)
        
        return applicable_rules
    
    def _matches_pattern(self, value: str, pattern: str) -> bool:
        """Check if value matches pattern (supports wildcards)"""
        if pattern == "*":
            return True
        
        if "*" in pattern:
            # Simple wildcard matching
            if pattern.endswith("*"):
                return value.startswith(pattern[:-1])
            elif pattern.startswith("*"):
                return value.endswith(pattern[1:])
            else:
                # Pattern has * in middle
                parts = pattern.split("*")
                return value.startswith(parts[0]) and value.endswith(parts[1])
        else:
            return value == pattern
    
    def _route_with_rule(self, message: ProcessMessage, rule: RoutingRule, 
                        routing_hint: Optional[str]) -> bool:
        """Route message using specific rule"""
        try:
            # Select target queue(s) based on routing strategy
            target_queues = self._select_target_queues(message, rule, routing_hint)
            
            if not target_queues:
                self.logger.warning(f"No target queues selected for message {message.message_id}")
                return False
            
            # Send to selected queue(s)
            success = True
            for queue_name in target_queues:
                if not self._send_to_queue(message, queue_name):
                    success = False
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error routing with rule {rule.rule_id}: {e}")
            return False
    
    def _select_target_queues(self, message: ProcessMessage, rule: RoutingRule,
                             routing_hint: Optional[str]) -> List[str]:
        """Select target queue(s) based on routing strategy"""
        available_queues = [q for q in rule.target_queues if self._is_queue_available(q)]
        
        if not available_queues:
            return []
        
        strategy = rule.routing_strategy
        
        if strategy == RoutingStrategy.ROUND_ROBIN:
            return self._round_robin_selection(available_queues, rule.rule_id)
        
        elif strategy == RoutingStrategy.LOAD_BALANCED:
            return self._load_balanced_selection(available_queues)
        
        elif strategy == RoutingStrategy.PRIORITY_BASED:
            return self._priority_based_selection(available_queues, message.priority)
        
        elif strategy == RoutingStrategy.HASH_BASED:
            return self._hash_based_selection(available_queues, message, routing_hint)
        
        elif strategy == RoutingStrategy.FAILOVER:
            return self._failover_selection(available_queues)
        
        else:
            # Default to first available queue
            return [available_queues[0]]
    
    def _is_queue_available(self, queue_name: str) -> bool:
        """Check if queue is available for routing"""
        queue_info = self.queues.get(queue_name)
        if not queue_info:
            return False
        
        return (queue_info.enabled and
                queue_info.health_status in [QueueHealth.HEALTHY, QueueHealth.DEGRADED] and
                queue_info.current_size < queue_info.max_size)
    
    def _round_robin_selection(self, queues: List[str], rule_id: str) -> List[str]:
        """Round-robin queue selection"""
        if not queues:
            return []
        
        current_index = self.round_robin_state[rule_id]
        selected_queue = queues[current_index % len(queues)]
        self.round_robin_state[rule_id] = (current_index + 1) % len(queues)
        
        return [selected_queue]
    
    def _load_balanced_selection(self, queues: List[str]) -> List[str]:
        """Load-balanced queue selection"""
        if not queues:
            return []
        
        # Calculate load scores (lower is better)
        queue_scores = []
        for queue_name in queues:
            queue_info = self.queues[queue_name]
            
            # Load score based on current size and processing time
            size_score = queue_info.current_size / queue_info.max_size
            time_score = min(queue_info.avg_processing_time_ms / 1000.0, 10.0) / 10.0
            
            # Weight by priority weight (higher weight = lower score)
            weighted_score = (size_score + time_score) / queue_info.priority_weight
            
            queue_scores.append((queue_name, weighted_score))
        
        # Select queue with lowest score
        queue_scores.sort(key=lambda x: x[1])
        return [queue_scores[0][0]]
    
    def _priority_based_selection(self, queues: List[str], priority: MessagePriority) -> List[str]:
        """Priority-based queue selection"""
        if not queues:
            return []
        
        # For high priority messages, prefer queues with higher priority weights
        if priority.value >= MessagePriority.HIGH.value:
            queue_weights = [(q, self.queues[q].priority_weight) for q in queues]
            queue_weights.sort(key=lambda x: x[1], reverse=True)
            return [queue_weights[0][0]]
        else:
            # For lower priority, use load balancing
            return self._load_balanced_selection(queues)
    
    def _hash_based_selection(self, queues: List[str], message: ProcessMessage,
                             routing_hint: Optional[str]) -> List[str]:
        """Hash-based queue selection for consistent routing"""
        if not queues:
            return []
        
        # Create hash key
        hash_key = routing_hint or message.sender
        hash_value = hash(hash_key)
        
        # Select queue based on hash
        selected_index = hash_value % len(queues)
        return [queues[selected_index]]
    
    def _failover_selection(self, queues: List[str]) -> List[str]:
        """Failover queue selection (prefer healthiest queue)"""
        if not queues:
            return []
        
        # Sort by health status and load
        queue_priorities = []
        for queue_name in queues:
            queue_info = self.queues[queue_name]
            
            # Health priority (lower is better)
            health_priority = {
                QueueHealth.HEALTHY: 1,
                QueueHealth.DEGRADED: 2,
                QueueHealth.UNHEALTHY: 3,
                QueueHealth.FAILED: 4,
                QueueHealth.UNKNOWN: 5
            }[queue_info.health_status]
            
            # Load priority
            load_priority = queue_info.current_size / queue_info.max_size
            
            total_priority = health_priority + load_priority
            queue_priorities.append((queue_name, total_priority))
        
        # Select best queue
        queue_priorities.sort(key=lambda x: x[1])
        return [queue_priorities[0][0]]
    
    def _send_to_queue(self, message: ProcessMessage, queue_name: str) -> bool:
        """Send message to specific queue"""
        try:
            queue_info = self.queues.get(queue_name)
            if not queue_info:
                self.logger.error(f"Queue not found: {queue_name}")
                return False
            
            # Send message using Redis manager
            success = self.redis_manager.send_message(message, queue_info.full_queue_key.split(':')[-1])
            
            # Update queue metrics
            if success:
                queue_info.total_messages_sent += 1
                queue_info.last_message_time = datetime.now()
            else:
                queue_info.total_messages_failed += 1
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error sending message to queue {queue_name}: {e}")
            return False
    
    def _route_to_default_queue(self, message: ProcessMessage) -> bool:
        """Route message to default queue when no rules apply"""
        # Use configured default queue
        default_queue = self.config_manager.queue_config.default_queue_name
        
        if default_queue in self.queues:
            return self._send_to_queue(message, default_queue)
        else:
            # Fallback to any available queue
            available_queues = [name for name, queue in self.queues.items() if queue.enabled]
            if available_queues:
                fallback_queue = available_queues[0]
                self.logger.warning(f"Default queue {default_queue} not available, using fallback: {fallback_queue}")
                return self._send_to_queue(message, fallback_queue)
            else:
                self.logger.error("No queues available for message routing")
                return False
    
    def start_monitoring(self):
        """Start queue health monitoring"""
        if not self.monitoring_active:
            self.monitoring_active = True
            self.monitoring_thread = threading.Thread(
                target=self._monitoring_worker,
                daemon=True
            )
            self.monitoring_thread.start()
            self.logger.info("Queue monitoring started")
    
    def stop_monitoring(self):
        """Stop queue health monitoring"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=10)
            self.logger.info("Queue monitoring stopped")
    
    def _monitoring_worker(self):
        """Background worker for queue monitoring"""
        while self.monitoring_active:
            try:
                # Check health of all queues
                for queue_name in list(self.queues.keys()):
                    self._check_queue_health(queue_name)
                
                # Clean up routing cache
                self._cleanup_routing_cache()
                
                # Sleep before next check
                time.sleep(self.health_check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in queue monitoring worker: {e}")
                time.sleep(30)  # Wait before retrying
    
    def _check_queue_health(self, queue_name: str):
        """Check health of a specific queue"""
        try:
            queue_info = self.queues.get(queue_name)
            if not queue_info:
                return
            
            # Get current queue size
            current_size = self.redis_manager.get_queue_size(queue_info.full_queue_key.split(':')[-1])
            queue_info.current_size = current_size
            
            # Determine health status
            if current_size == 0:
                queue_info.health_status = QueueHealth.HEALTHY
            elif current_size < queue_info.max_size * 0.7:
                queue_info.health_status = QueueHealth.HEALTHY
            elif current_size < queue_info.max_size * 0.9:
                queue_info.health_status = QueueHealth.DEGRADED
            elif current_size < queue_info.max_size:
                queue_info.health_status = QueueHealth.UNHEALTHY
            else:
                queue_info.health_status = QueueHealth.FAILED
            
            queue_info.last_health_check = datetime.now()
            
        except Exception as e:
            self.logger.error(f"Error checking health of queue {queue_name}: {e}")
            if queue_name in self.queues:
                self.queues[queue_name].health_status = QueueHealth.UNKNOWN
    
    def _cleanup_routing_cache(self):
        """Clean up expired routing cache entries"""
        try:
            current_time = time.time()
            expired_keys = []
            
            for key, (timestamp, _) in self.routing_cache.items():
                if current_time - timestamp > self.cache_ttl_seconds:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self.routing_cache[key]
                
        except Exception as e:
            self.logger.error(f"Error cleaning up routing cache: {e}")
    
    def add_routing_rule(self, rule: RoutingRule) -> bool:
        """Add a new routing rule"""
        with self.coordination_lock:
            try:
                self.routing_rules[rule.rule_id] = rule
                self.logger.info(f"Routing rule added: {rule.rule_id}")
                return True
                
            except Exception as e:
                self.logger.error(f"Error adding routing rule {rule.rule_id}: {e}")
                return False
    
    def remove_routing_rule(self, rule_id: str) -> bool:
        """Remove a routing rule"""
        with self.coordination_lock:
            try:
                if rule_id in self.routing_rules:
                    del self.routing_rules[rule_id]
                    self.logger.info(f"Routing rule removed: {rule_id}")
                    return True
                else:
                    self.logger.warning(f"Routing rule not found: {rule_id}")
                    return False
                    
            except Exception as e:
                self.logger.error(f"Error removing routing rule {rule_id}: {e}")
                return False
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get status of all queues"""
        with self.coordination_lock:
            return {
                queue_name: {
                    "health_status": queue_info.health_status.value,
                    "current_size": queue_info.current_size,
                    "max_size": queue_info.max_size,
                    "enabled": queue_info.enabled,
                    "total_messages_sent": queue_info.total_messages_sent,
                    "total_messages_failed": queue_info.total_messages_failed,
                    "last_message_time": queue_info.last_message_time.isoformat() if queue_info.last_message_time else None,
                    "last_health_check": queue_info.last_health_check.isoformat() if queue_info.last_health_check else None
                }
                for queue_name, queue_info in self.queues.items()
            }
    
    def get_routing_statistics(self) -> Dict[str, Any]:
        """Get routing statistics"""
        with self.coordination_lock:
            total_messages = sum(q.total_messages_sent for q in self.queues.values())
            total_failures = sum(q.total_messages_failed for q in self.queues.values())
            
            return {
                "total_messages_routed": total_messages,
                "total_routing_failures": total_failures,
                "success_rate": (total_messages / (total_messages + total_failures)) if (total_messages + total_failures) > 0 else 0,
                "active_rules": len([r for r in self.routing_rules.values() if r.enabled]),
                "total_rules": len(self.routing_rules),
                "active_queues": len([q for q in self.queues.values() if q.enabled]),
                "healthy_queues": len([q for q in self.queues.values() if q.health_status == QueueHealth.HEALTHY])
            }


# Global instance
_queue_coordinator: Optional[RedisQueueCoordinator] = None


def get_queue_coordinator(redis_manager=None) -> RedisQueueCoordinator:
    """Get or create global queue coordinator"""
    global _queue_coordinator
    
    if _queue_coordinator is None:
        _queue_coordinator = RedisQueueCoordinator(redis_manager)
    
    return _queue_coordinator


def route_coordinated_message(message: ProcessMessage, routing_hint: Optional[str] = None) -> bool:
    """Route coordinated message (convenience function)"""
    coordinator = get_queue_coordinator()
    return coordinator.route_message(message, routing_hint)


def start_queue_monitoring():
    """Start queue monitoring (convenience function)"""
    coordinator = get_queue_coordinator()
    coordinator.start_monitoring()


def get_queue_status() -> Dict[str, Any]:
    """Get queue status (convenience function)"""
    coordinator = get_queue_coordinator()
    return coordinator.get_queue_status()