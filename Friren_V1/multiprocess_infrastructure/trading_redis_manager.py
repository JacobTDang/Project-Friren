#!/usr/bin/env python3
"""
TradingRedisManager - Centralized Redis Interface for Trading System

Replaces multiprocessing queues and shared state with Redis-based communication.
Provides cross-platform compatibility and better process isolation.
"""

import redis
import json
import time
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, is_dataclass
from enum import Enum
import threading
import uuid

logger = logging.getLogger(__name__)

class MessagePriority(Enum):
    """Message priority levels"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

@dataclass
class ProcessMessage:
    """Standardized process message format"""
    message_id: str
    sender: str
    recipient: str
    message_type: str
    priority: MessagePriority
    timestamp: datetime
    data: Dict[str, Any]
    expires_at: Optional[datetime] = None

class TradingRedisManager:
    """
    Centralized Redis manager for trading system process communication.

    Features:
    - Message queues with priority handling
    - Shared state management
    - Process health monitoring
    - Cross-platform compatibility
    - Automatic cleanup and expiration
    """

    def __init__(self, host='localhost', port=6379, db=0):
        """Initialize Redis connection and setup trading system namespace"""
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )

        # Test connection
        try:
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

        # Redis key prefixes for organization
        self.KEY_PREFIX = "trading_system"
        self.QUEUE_PREFIX = f"{self.KEY_PREFIX}:queue"
        self.STATE_PREFIX = f"{self.KEY_PREFIX}:state"
        self.HEALTH_PREFIX = f"{self.KEY_PREFIX}:health"
        self.POSITION_PREFIX = f"{self.KEY_PREFIX}:positions"
        self.ACCOUNT_PREFIX = f"{self.KEY_PREFIX}:account"
        self.COORDINATION_PREFIX = f"{self.KEY_PREFIX}:coordination"

        # Message queues
        self.PRIORITY_QUEUE = f"{self.QUEUE_PREFIX}:priority"
        self.HEALTH_QUEUE = f"{self.QUEUE_PREFIX}:health"
        self.DEAD_LETTER_QUEUE = f"{self.QUEUE_PREFIX}:dead_letter"
        self.DECISION_QUEUE = f"{self.QUEUE_PREFIX}:decisions"
        self.EXECUTION_QUEUE = f"{self.QUEUE_PREFIX}:execution"

        # Pub/Sub channels
        self.BROADCAST_CHANNEL = f"{self.KEY_PREFIX}:broadcast"
        self.EMERGENCY_CHANNEL = f"{self.KEY_PREFIX}:emergency"

        # Initialize system state
        self._initialize_system_state()

        # Background cleanup thread
        self._cleanup_thread = None
        self._cleanup_active = False

        logger.info("TradingRedisManager initialized successfully")

    def _initialize_system_state(self):
        """Initialize basic system state in Redis"""
        system_state = {
            'system_status': 'initializing',
            'start_time': datetime.now().isoformat(),
            'active_processes': 0,
            'message_count': 0,
            'last_activity': datetime.now().isoformat()
        }

        self.redis_client.hset(
            f"{self.STATE_PREFIX}:system",
            mapping=system_state
        )

        # Set expiration for health data (5 minutes)
        self.redis_client.expire(f"{self.HEALTH_PREFIX}:*", 300)

    def start_cleanup_thread(self):
        """Start background cleanup thread"""
        if not self._cleanup_active:
            self._cleanup_active = True
            self._cleanup_thread = threading.Thread(
                target=self._cleanup_worker,
                daemon=True
            )
            self._cleanup_thread.start()
            logger.info("Redis cleanup thread started")

    def stop_cleanup_thread(self):
        """Stop background cleanup thread"""
        self._cleanup_active = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
            logger.info("Redis cleanup thread stopped")

    def _cleanup_worker(self):
        """Background worker for cleanup tasks"""
        while self._cleanup_active:
            try:
                self._cleanup_expired_messages()
                self._cleanup_stale_health_data()
                time.sleep(60)  # Run every minute
            except Exception as e:
                logger.error(f"Error in cleanup worker: {e}")
                time.sleep(30)  # Wait before retrying

    def _cleanup_expired_messages(self):
        """Remove expired messages from queues"""
        try:
            # Get all queue keys
            queue_keys = self.redis_client.keys(f"{self.QUEUE_PREFIX}:*")

            for queue_key in queue_keys:
                # Check each message in the queue for expiration
                messages = self.redis_client.lrange(queue_key, 0, -1)

                for message_json in messages:
                    try:
                        message_data = json.loads(message_json)
                        if 'expires_at' in message_data and message_data['expires_at']:
                            expires_at = datetime.fromisoformat(message_data['expires_at'])
                            if datetime.now() > expires_at:
                                # Remove expired message
                                self.redis_client.lrem(queue_key, 1, message_json)
                                logger.debug(f"Removed expired message from {queue_key}")
                    except (json.JSONDecodeError, ValueError):
                        # Invalid message format, remove it
                        self.redis_client.lrem(queue_key, 1, message_json)

        except Exception as e:
            logger.error(f"Error cleaning up expired messages: {e}")

    def _cleanup_stale_health_data(self):
        """Remove stale health monitoring data"""
        try:
            # Remove health data older than 10 minutes
            cutoff_time = datetime.now() - timedelta(minutes=10)

            health_keys = self.redis_client.keys(f"{self.HEALTH_PREFIX}:*")
            for key in health_keys:
                # Check if health data is stale
                data = self.redis_client.hgetall(key)
                if 'last_update' in data:
                    last_update = datetime.fromisoformat(data['last_update'])
                    if last_update < cutoff_time:
                        self.redis_client.delete(key)
                        logger.debug(f"Removed stale health data: {key}")

        except Exception as e:
            logger.error(f"Error cleaning up stale health data: {e}")

    # Message Queue Operations
    def send_message(self, message: ProcessMessage, queue_name: str = None) -> bool:
        """Send a message to specified queue or priority queue"""
        try:
            # Serialize message
            message_dict = asdict(message)
            message_dict['timestamp'] = message.timestamp.isoformat()
            if message.expires_at:
                message_dict['expires_at'] = message.expires_at.isoformat()
            message_dict['priority'] = message.priority.value

            message_json = json.dumps(message_dict)

            # Determine target queue
            if queue_name:
                target_queue = f"{self.QUEUE_PREFIX}:{queue_name}"
            else:
                # Use priority queue with priority-based insertion
                target_queue = self.PRIORITY_QUEUE

            # Add to queue (left push for FIFO)
            self.redis_client.lpush(target_queue, message_json)

            # Update system stats
            self.redis_client.hincrby(f"{self.STATE_PREFIX}:system", 'message_count', 1)
            self.redis_client.hset(
                f"{self.STATE_PREFIX}:system",
                'last_activity',
                datetime.now().isoformat()
            )

            logger.debug(f"Message sent to {target_queue}: {message.message_type}")
            return True

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def get_message(self, block: bool = True, timeout: float = 1.0) -> Optional[ProcessMessage]:
        """Get a message (alias for receive_message for compatibility)"""
        return self.receive_message(timeout=int(timeout))

    def receive_message(self, queue_name: str = None, timeout: int = 1) -> Optional[ProcessMessage]:
        """Receive a message from specified queue or priority queue"""
        try:
            # Determine source queue
            if queue_name:
                source_queue = f"{self.QUEUE_PREFIX}:{queue_name}"
            else:
                source_queue = self.PRIORITY_QUEUE

            # Blocking pop with timeout
            result = self.redis_client.brpop(source_queue, timeout=timeout)

            if not result:
                return None

            queue_key, message_json = result
            message_dict = json.loads(message_json)

            # Reconstruct message
            message = ProcessMessage(
                message_id=message_dict['message_id'],
                sender=message_dict['sender'],
                recipient=message_dict['recipient'],
                message_type=message_dict['message_type'],
                priority=MessagePriority(message_dict['priority']),
                timestamp=datetime.fromisoformat(message_dict['timestamp']),
                data=message_dict['data'],
                expires_at=datetime.fromisoformat(message_dict['expires_at']) if message_dict.get('expires_at') else None
            )

            # Check if message is expired
            if message.expires_at and datetime.now() > message.expires_at:
                logger.debug(f"Received expired message: {message.message_id}")
                return None

            return message

        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None

    def get_queue_size(self, queue_name: str = None) -> int:
        """Get the size of a queue"""
        try:
            if queue_name:
                queue_key = f"{self.QUEUE_PREFIX}:{queue_name}"
            else:
                queue_key = self.PRIORITY_QUEUE

            return self.redis_client.llen(queue_key)
        except Exception as e:
            logger.error(f"Error getting queue size: {e}")
            return 0

    def get_queue_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all message queues"""
        try:
            queue_status = {}

            # Standard queues
            standard_queues = {
                'priority': self.PRIORITY_QUEUE,
                'health': self.HEALTH_QUEUE,
                'dead_letter': self.DEAD_LETTER_QUEUE,
                'decisions': self.DECISION_QUEUE,
                'execution': self.EXECUTION_QUEUE
            }

            for queue_name, queue_key in standard_queues.items():
                size = self.redis_client.llen(queue_key)

                # Get recent messages to analyze types
                recent_messages = self.redis_client.lrange(queue_key, 0, 9)  # Last 10 messages
                message_types = []

                for msg_json in recent_messages:
                    try:
                        msg_data = json.loads(msg_json)
                        msg_type = msg_data.get('message_type', 'unknown')
                        if msg_type not in message_types:
                            message_types.append(msg_type)
                    except (json.JSONDecodeError, KeyError):
                        pass

                queue_status[queue_name] = {
                    'size': size,
                    'pending_messages': size,  # For Redis lists, size == pending
                    'messages_processed': 0,  # Would need tracking to implement
                    'last_message_time': 'Unknown',  # Would need tracking to implement
                    'message_types': message_types
                }

            # Process-specific queues
            process_queue_keys = self.redis_client.keys(f"{self.QUEUE_PREFIX}:process_*")
            for queue_key in process_queue_keys:
                queue_name = queue_key.split(':')[-1]  # Extract process name
                size = self.redis_client.llen(queue_key)

                queue_status[queue_name] = {
                    'size': size,
                    'pending_messages': size,
                    'messages_processed': 0,
                    'last_message_time': 'Unknown',
                    'message_types': []
                }

            return queue_status

        except Exception as e:
            logger.error(f"Error getting queue status: {e}")
            return {}

    # Shared State Management
    def set_shared_state(self, key: str, value: Any, namespace: str = "general") -> bool:
        """Set shared state value"""
        try:
            state_key = f"{self.STATE_PREFIX}:{namespace}:{key}"

            if isinstance(value, (dict, list)):
                value_json = json.dumps(value, default=str)
                self.redis_client.set(state_key, value_json)
            else:
                self.redis_client.set(state_key, str(value))

            return True
        except Exception as e:
            logger.error(f"Error setting shared state: {e}")
            return False

    def get_shared_state(self, key: str, namespace: str = "general", default: Any = None) -> Any:
        """Get shared state value"""
        try:
            state_key = f"{self.STATE_PREFIX}:{namespace}:{key}"
            value = self.redis_client.get(state_key)

            if value is None:
                return default

            # Try to parse as JSON
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        except Exception as e:
            logger.error(f"Error getting shared state: {e}")
            return default

    def update_shared_state(self, updates: Dict[str, Any], namespace: str = "general") -> bool:
        """Update multiple shared state values atomically"""
        try:
            pipe = self.redis_client.pipeline()

            for key, value in updates.items():
                state_key = f"{self.STATE_PREFIX}:{namespace}:{key}"
                if isinstance(value, (dict, list)):
                    value_json = json.dumps(value, default=str)
                    pipe.set(state_key, value_json)
                else:
                    pipe.set(state_key, str(value))

            pipe.execute()
            return True
        except Exception as e:
            logger.error(f"Error updating shared state: {e}")
            return False

    def get_all_shared_keys(self, namespace: str = None) -> List[str]:
        """Get all shared state keys, optionally filtered by namespace"""
        try:
            if namespace:
                pattern = f"{self.STATE_PREFIX}:{namespace}:*"
            else:
                pattern = f"{self.STATE_PREFIX}:*"

            keys = self.redis_client.keys(pattern)

            # Strip the prefix to get just the key names
            cleaned_keys = []
            for key in keys:
                # Remove the prefix and namespace to get the actual key
                if namespace:
                    prefix_to_remove = f"{self.STATE_PREFIX}:{namespace}:"
                else:
                    prefix_to_remove = f"{self.STATE_PREFIX}:"

                if key.startswith(prefix_to_remove):
                    cleaned_key = key[len(prefix_to_remove):]
                    cleaned_keys.append(cleaned_key)

            return cleaned_keys

        except Exception as e:
            logger.error(f"Error getting all shared keys: {e}")
            return []

    # Position Management
    def update_position(self, symbol: str, position_data: Dict[str, Any]) -> bool:
        """Update position data for a symbol"""
        try:
            position_key = f"{self.POSITION_PREFIX}:{symbol}"
            position_data['last_update'] = datetime.now().isoformat()

            self.redis_client.hset(position_key, mapping=position_data)
            return True
        except Exception as e:
            logger.error(f"Error updating position for {symbol}: {e}")
            return False

    def get_position(self, symbol: str) -> Dict[str, Any]:
        """Get position data for a symbol"""
        try:
            position_key = f"{self.POSITION_PREFIX}:{symbol}"
            return self.redis_client.hgetall(position_key)
        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {e}")
            return {}

    def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get all position data"""
        try:
            positions = {}
            position_keys = self.redis_client.keys(f"{self.POSITION_PREFIX}:*")

            for key in position_keys:
                symbol = key.split(':')[-1]
                positions[symbol] = self.redis_client.hgetall(key)

            return positions
        except Exception as e:
            logger.error(f"Error getting all positions: {e}")
            return {}

    # Health Monitoring
    def update_process_health(self, process_id: str, health_data: Dict[str, Any]) -> bool:
        """Update process health data"""
        try:
            health_key = f"{self.HEALTH_PREFIX}:{process_id}"
            health_data['last_update'] = datetime.now().isoformat()
            health_data['process_id'] = process_id

            self.redis_client.hset(health_key, mapping=health_data)

            # Set expiration for health data (5 minutes)
            self.redis_client.expire(health_key, 300)

            return True
        except Exception as e:
            logger.error(f"Error updating health for {process_id}: {e}")
            return False

    def get_process_health(self, process_id: str) -> Dict[str, Any]:
        """Get process health data"""
        try:
            health_key = f"{self.HEALTH_PREFIX}:{process_id}"
            return self.redis_client.hgetall(health_key)
        except Exception as e:
            logger.error(f"Error getting health for {process_id}: {e}")
            return {}

    def get_all_process_health(self) -> Dict[str, Dict[str, Any]]:
        """Get all process health data"""
        try:
            health_data = {}
            health_keys = self.redis_client.keys(f"{self.HEALTH_PREFIX}:*")

            for key in health_keys:
                process_id = key.split(':')[-1]
                health_data[process_id] = self.redis_client.hgetall(key)

            return health_data
        except Exception as e:
            logger.error(f"Error getting all health data: {e}")
            return {}

    # Symbol Coordination
    def update_symbol_coordination(self, coordination_data: Dict[str, Any]) -> bool:
        """Update symbol coordination data"""
        try:
            coord_key = f"{self.COORDINATION_PREFIX}:status"
            coordination_data['last_update'] = datetime.now().isoformat()

            # Serialize nested dictionaries to JSON strings
            serialized_data = {}
            for key, value in coordination_data.items():
                if isinstance(value, (dict, list)):
                    serialized_data[key] = json.dumps(value, default=str)
                else:
                    serialized_data[key] = str(value)

            self.redis_client.hset(coord_key, mapping=serialized_data)
            return True
        except Exception as e:
            logger.error(f"Error updating symbol coordination: {e}")
            return False

    def get_symbol_coordination(self) -> Dict[str, Any]:
        """Get symbol coordination data"""
        try:
            coord_key = f"{self.COORDINATION_PREFIX}:status"
            raw_data = self.redis_client.hgetall(coord_key)

            # Deserialize JSON strings back to dictionaries
            coordination_data = {}
            for key, value in raw_data.items():
                try:
                    # Try to parse as JSON
                    coordination_data[key] = json.loads(value)
                except json.JSONDecodeError:
                    # If not JSON, keep as string
                    coordination_data[key] = value

            return coordination_data
        except Exception as e:
            logger.error(f"Error getting symbol coordination: {e}")
            return {}

    # Pub/Sub Communication
    def publish_broadcast(self, message: str, data: Dict[str, Any] = None) -> bool:
        """Publish a broadcast message to all processes"""
        try:
            broadcast_data = {
                'message': message,
                'timestamp': datetime.now().isoformat(),
                'data': data or {}
            }

            self.redis_client.publish(
                self.BROADCAST_CHANNEL,
                json.dumps(broadcast_data)
            )
            return True
        except Exception as e:
            logger.error(f"Error publishing broadcast: {e}")
            return False

    def publish_emergency(self, message: str, data: Dict[str, Any] = None) -> bool:
        """Publish an emergency message"""
        try:
            emergency_data = {
                'message': message,
                'timestamp': datetime.now().isoformat(),
                'data': data or {},
                'emergency': True
            }

            self.redis_client.publish(
                self.EMERGENCY_CHANNEL,
                json.dumps(emergency_data)
            )
            return True
        except Exception as e:
            logger.error(f"Error publishing emergency: {e}")
            return False

    # System Status and Cleanup
    def get_system_status(self) -> Dict[str, Any]:
        """Get complete system status"""
        try:
            # System state
            system_state = self.redis_client.hgetall(f"{self.STATE_PREFIX}:system")

            # Queue statistics
            queue_stats = {
                'priority_queue_size': self.get_queue_size(),
                'health_queue_size': self.get_queue_size('health'),
                'dead_letter_queue_size': self.get_queue_size('dead_letter')
            }

            # Process health summary
            health_data = self.get_all_process_health()
            healthy_processes = sum(1 for h in health_data.values()
                                  if h.get('status') == 'healthy')

            # Memory usage
            memory_info = self.redis_client.memory_usage(f"{self.KEY_PREFIX}:*") or 0

            return {
                'system_state': system_state,
                'queue_stats': queue_stats,
                'process_health': {
                    'total_processes': len(health_data),
                    'healthy_processes': healthy_processes,
                    'unhealthy_processes': len(health_data) - healthy_processes
                },
                'redis_info': {
                    'connected_clients': self.redis_client.info().get('connected_clients', 0),
                    'memory_usage_bytes': memory_info,
                    'total_commands_processed': self.redis_client.info().get('total_commands_processed', 0)
                },
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {'error': str(e)}

    def cleanup_all_data(self) -> bool:
        """Clean up all trading system data from Redis"""
        try:
            # Get all keys with trading system prefix
            keys = self.redis_client.keys(f"{self.KEY_PREFIX}:*")

            if keys:
                self.redis_client.delete(*keys)
                logger.info(f"Cleaned up {len(keys)} Redis keys")

            return True
        except Exception as e:
            logger.error(f"Error cleaning up Redis data: {e}")
            return False

    def __del__(self):
        """Cleanup when manager is destroyed"""
        try:
            self.stop_cleanup_thread()
        except:
            pass


# Global singleton instance
_redis_manager = None

def get_trading_redis_manager() -> TradingRedisManager:
    """Get the global TradingRedisManager instance"""
    global _redis_manager
    if _redis_manager is None:
        _redis_manager = TradingRedisManager()
        _redis_manager.start_cleanup_thread()
    return _redis_manager

def create_process_message(
    sender: str,
    recipient: str,
    message_type: str,
    data: Dict[str, Any],
    priority: MessagePriority = MessagePriority.NORMAL,
    expires_in_seconds: Optional[int] = None
) -> ProcessMessage:
    """Helper function to create a ProcessMessage"""
    expires_at = None
    if expires_in_seconds:
        expires_at = datetime.now() + timedelta(seconds=expires_in_seconds)

    return ProcessMessage(
        message_id=str(uuid.uuid4()),
        sender=sender,
        recipient=recipient,
        message_type=message_type,
        priority=priority,
        timestamp=datetime.now(),
        data=data,
        expires_at=expires_at
    )
