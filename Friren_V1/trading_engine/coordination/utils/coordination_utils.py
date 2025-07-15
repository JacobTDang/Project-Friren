"""
Coordination Utilities
====================

Shared utilities for all coordination modules.
Provides common functions for message handling, timing, validation, and system coordination.

Features:
- Message validation and serialization
- Timing and scheduling utilities
- System state validation
- Error handling and logging
- Coordination protocol helpers
"""

import time
import uuid
import hashlib
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import logging
import json
import functools
import asyncio
from collections import defaultdict, deque


class CoordinationError(Exception):
    """Base exception for coordination errors"""
    pass


class ValidationError(CoordinationError):
    """Error in message or state validation"""
    pass


class TimingError(CoordinationError):
    """Error in timing or scheduling"""
    pass


class StateError(CoordinationError):
    """Error in system state"""
    pass


@dataclass
class TimingMetrics:
    """Timing metrics for coordination operations"""
    operation: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None


@dataclass
class CoordinationContext:
    """Context for coordination operations"""
    operation_id: str
    coordinator_id: str
    timestamp: datetime
    timeout_seconds: float = 30.0
    retry_count: int = 0
    max_retries: int = 3
    context_data: Dict[str, Any] = field(default_factory=dict)


class MessageValidator:
    """Validates coordination messages"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Load required fields from configuration
        self.required_fields = self._load_required_fields()
        
        # Load valid message types from configuration
        self.valid_message_types = self._load_valid_message_types()
        
        # Field validators
        self.field_validators = {
            "message_id": self._validate_message_id,
            "sender": self._validate_process_id,
            "recipient": self._validate_process_id,
            "process_id": self._validate_process_id,
            "timestamp": self._validate_timestamp,
            "message_type": self._validate_message_type
        }
    
    def _load_required_fields(self) -> Dict[str, List[str]]:
        """Load required fields from configuration"""
        try:
            # Try to get from coordination config if available
            try:
                from ..coordination_config import get_coordination_config
                config_manager = get_coordination_config()
                return config_manager.message_config.required_fields
            except ImportError:
                pass
            
            # Load from environment or use defaults
            import os
            import json
            
            required_fields_env = os.getenv('FRIREN_REQUIRED_FIELDS')
            if required_fields_env:
                return json.loads(required_fields_env)
            
            # Default required fields
            return {
                "coordination": ["message_id", "sender", "recipient", "message_type", "timestamp"],
                "heartbeat": ["process_id", "timestamp", "status"],
                "state_update": ["process_id", "state_data", "timestamp"],
                "recovery": ["process_id", "action_type", "reason", "timestamp"]
            }
            
        except Exception as e:
            self.logger.error(f"Error loading required fields: {e}")
            # Minimal fallback
            return {
                "coordination": ["message_id", "sender", "recipient", "message_type", "timestamp"]
            }
    
    def _load_valid_message_types(self) -> List[str]:
        """Load valid message types from configuration"""
        try:
            # Try to get from coordination config if available
            try:
                from ..coordination_config import get_coordination_config
                config_manager = get_coordination_config()
                return config_manager.message_config.valid_message_types
            except ImportError:
                pass
            
            # Load from environment or use defaults
            import os
            
            types_env = os.getenv('FRIREN_VALID_MESSAGE_TYPES')
            if types_env:
                return types_env.split(',')
            
            # Default message types
            return [
                "coordination", "heartbeat", "state_update", "recovery",
                "news_collection", "sentiment_analysis", "trading_signal",
                "risk_assessment", "execution_order", "position_update",
                "strategy_assignment", "market_data", "system_health"
            ]
            
        except Exception as e:
            self.logger.error(f"Error loading valid message types: {e}")
            # Minimal fallback
            return ["coordination", "heartbeat", "trading_signal"]
    
    def validate_message(self, message: Dict[str, Any], message_category: str = "coordination") -> bool:
        """
        Validate a coordination message
        
        Args:
            message: Message to validate
            message_category: Category of message (coordination, heartbeat, etc.)
            
        Returns:
            True if valid
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            # Check required fields
            required = self.required_fields.get(message_category, [])
            for field in required:
                if field not in message:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Validate individual fields
            for field, value in message.items():
                if field in self.field_validators:
                    if not self.field_validators[field](value):
                        raise ValidationError(f"Invalid value for field {field}: {value}")
            
            # Message-specific validation
            if message_category == "coordination":
                self._validate_coordination_message(message)
            elif message_category == "heartbeat":
                self._validate_heartbeat_message(message)
            
            return True
            
        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"Validation error: {e}")
    
    def _validate_message_id(self, value: Any) -> bool:
        """Validate message ID format"""
        return isinstance(value, str) and len(value) > 0 and len(value) <= 100
    
    def _validate_process_id(self, value: Any) -> bool:
        """Validate process ID format"""
        return isinstance(value, str) and len(value) > 0 and len(value) <= 50
    
    def _validate_timestamp(self, value: Any) -> bool:
        """Validate timestamp format"""
        try:
            if isinstance(value, str):
                datetime.fromisoformat(value.replace('Z', '+00:00'))
                return True
            elif isinstance(value, datetime):
                return True
            return False
        except (ValueError, TypeError):
            return False
    
    def _validate_message_type(self, value: Any) -> bool:
        """Validate message type"""
        return isinstance(value, str) and value in self.valid_message_types
    
    def _validate_coordination_message(self, message: Dict[str, Any]):
        """Validate coordination-specific message fields"""
        # Check data field exists and is dict
        if "data" in message and not isinstance(message["data"], dict):
            raise ValidationError("Data field must be a dictionary")
        
        # Check priority if present
        if "priority" in message:
            if not isinstance(message["priority"], (int, str)):
                raise ValidationError("Priority must be int or string")
    
    def _validate_heartbeat_message(self, message: Dict[str, Any]):
        """Validate heartbeat-specific message fields"""
        # Check status values
        valid_statuses = ["healthy", "unhealthy", "starting", "stopping", "failed"]
        if message.get("status") not in valid_statuses:
            raise ValidationError(f"Invalid status: {message.get('status')}")


class MessageSerializer:
    """Serializes and deserializes coordination messages"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def serialize_message(self, message: Dict[str, Any]) -> str:
        """
        Serialize message to JSON string
        
        Args:
            message: Message dictionary
            
        Returns:
            JSON string
            
        Raises:
            ValidationError: If serialization fails
        """
        try:
            # Convert datetime objects to ISO format
            serializable_message = self._make_serializable(message)
            
            # Serialize to JSON
            return json.dumps(serializable_message, default=str, separators=(',', ':'))
            
        except Exception as e:
            raise ValidationError(f"Serialization error: {e}")
    
    def deserialize_message(self, message_json: str) -> Dict[str, Any]:
        """
        Deserialize JSON string to message dictionary
        
        Args:
            message_json: JSON string
            
        Returns:
            Message dictionary
            
        Raises:
            ValidationError: If deserialization fails
        """
        try:
            message = json.loads(message_json)
            
            # Convert ISO timestamp strings back to datetime objects
            return self._restore_datetime_objects(message)
            
        except json.JSONDecodeError as e:
            raise ValidationError(f"JSON decode error: {e}")
        except Exception as e:
            raise ValidationError(f"Deserialization error: {e}")
    
    def _make_serializable(self, obj: Any) -> Any:
        """Convert object to JSON-serializable format"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(asdict(obj) if hasattr(obj, '__dataclass_fields__') else obj.__dict__)
        else:
            return obj
    
    def _restore_datetime_objects(self, obj: Any) -> Any:
        """Restore datetime objects from serialized format"""
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                if k in ['timestamp', 'start_time', 'end_time', 'last_update'] and isinstance(v, str):
                    try:
                        result[k] = datetime.fromisoformat(v.replace('Z', '+00:00'))
                    except ValueError:
                        result[k] = v
                else:
                    result[k] = self._restore_datetime_objects(v)
            return result
        elif isinstance(obj, list):
            return [self._restore_datetime_objects(item) for item in obj]
        else:
            return obj


class TimingManager:
    """Manages timing and scheduling for coordination operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metrics = deque(maxlen=1000)  # Keep last 1000 timing metrics
        self.active_operations = {}
        self.lock = threading.Lock()
    
    def start_operation(self, operation: str, context: Optional[CoordinationContext] = None) -> str:
        """
        Start timing an operation
        
        Args:
            operation: Operation name
            context: Optional coordination context
            
        Returns:
            Operation ID for tracking
        """
        with self.lock:
            operation_id = str(uuid.uuid4())
            
            timing_metric = TimingMetrics(
                operation=operation,
                start_time=datetime.now()
            )
            
            self.active_operations[operation_id] = {
                "timing": timing_metric,
                "context": context
            }
            
            return operation_id
    
    def end_operation(self, operation_id: str, success: bool = True, error_message: Optional[str] = None):
        """
        End timing an operation
        
        Args:
            operation_id: Operation ID from start_operation
            success: Whether operation succeeded
            error_message: Error message if failed
        """
        with self.lock:
            if operation_id not in self.active_operations:
                self.logger.warning(f"Unknown operation ID: {operation_id}")
                return
            
            operation_data = self.active_operations.pop(operation_id)
            timing_metric = operation_data["timing"]
            
            timing_metric.end_time = datetime.now()
            timing_metric.duration_ms = (timing_metric.end_time - timing_metric.start_time).total_seconds() * 1000
            timing_metric.success = success
            timing_metric.error_message = error_message
            
            self.metrics.append(timing_metric)
    
    def get_operation_metrics(self, operation: Optional[str] = None) -> List[TimingMetrics]:
        """
        Get timing metrics for operations
        
        Args:
            operation: Optional operation name filter
            
        Returns:
            List of timing metrics
        """
        with self.lock:
            if operation:
                return [m for m in self.metrics if m.operation == operation]
            else:
                return list(self.metrics)
    
    def get_operation_stats(self, operation: str) -> Dict[str, Any]:
        """
        Get statistics for an operation
        
        Args:
            operation: Operation name
            
        Returns:
            Statistics dictionary
        """
        with self.lock:
            operation_metrics = [m for m in self.metrics if m.operation == operation]
            
            if not operation_metrics:
                return {"error": "No metrics found"}
            
            durations = [m.duration_ms for m in operation_metrics if m.duration_ms is not None]
            success_count = sum(1 for m in operation_metrics if m.success)
            
            return {
                "operation": operation,
                "total_calls": len(operation_metrics),
                "success_count": success_count,
                "failure_count": len(operation_metrics) - success_count,
                "success_rate": success_count / len(operation_metrics),
                "avg_duration_ms": sum(durations) / len(durations) if durations else 0,
                "min_duration_ms": min(durations) if durations else 0,
                "max_duration_ms": max(durations) if durations else 0
            }


class StateValidator:
    """Validates system state for coordination operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Load state validation rules from configuration
        self.state_rules = self._load_state_rules()
    
    def _load_state_rules(self) -> Dict[str, Dict[str, Any]]:
        """Load state validation rules from configuration"""
        try:
            import os
            import json
            
            # Load from environment or use defaults
            state_rules_env = os.getenv('FRIREN_STATE_RULES')
            if state_rules_env:
                return json.loads(state_rules_env)
            
            # Load individual rule components from environment
            memory_max = int(os.getenv('FRIREN_STATE_MEMORY_MAX', '100'))
            queue_max = int(os.getenv('FRIREN_STATE_QUEUE_MAX', '10000'))
            process_max = int(os.getenv('FRIREN_STATE_PROCESS_MAX', '10'))
            response_max = int(os.getenv('FRIREN_STATE_RESPONSE_MAX', '30'))
            
            # Default state validation rules
            return {
                "memory_usage": {"min": 0, "max": memory_max, "unit": "percent"},
                "queue_depth": {"min": 0, "max": queue_max, "unit": "messages"},
                "process_count": {"min": 1, "max": process_max, "unit": "processes"},
                "response_time": {"min": 0, "max": response_max, "unit": "seconds"}
            }
            
        except Exception as e:
            self.logger.error(f"Error loading state rules: {e}")
            # Minimal fallback
            return {
                "memory_usage": {"min": 0, "max": 100, "unit": "percent"},
                "queue_depth": {"min": 0, "max": 1000, "unit": "messages"}
            }
    
    def validate_system_state(self, state: Dict[str, Any]) -> bool:
        """
        Validate system state
        
        Args:
            state: System state dictionary
            
        Returns:
            True if valid
            
        Raises:
            StateError: If validation fails
        """
        try:
            for key, value in state.items():
                if key in self.state_rules:
                    rule = self.state_rules[key]
                    
                    if not isinstance(value, (int, float)):
                        raise StateError(f"State value {key} must be numeric, got {type(value)}")
                    
                    if value < rule["min"] or value > rule["max"]:
                        raise StateError(f"State value {key} out of range: {value} (expected {rule['min']}-{rule['max']} {rule['unit']})")
            
            return True
            
        except StateError:
            raise
        except Exception as e:
            raise StateError(f"State validation error: {e}")
    
    def validate_process_state(self, process_id: str, state: Dict[str, Any]) -> bool:
        """
        Validate process-specific state
        
        Args:
            process_id: Process identifier
            state: Process state dictionary
            
        Returns:
            True if valid
            
        Raises:
            StateError: If validation fails
        """
        try:
            required_fields = ["status", "last_update"]
            
            for field in required_fields:
                if field not in state:
                    raise StateError(f"Missing required process state field: {field}")
            
            # Validate status
            valid_statuses = ["healthy", "unhealthy", "starting", "stopping", "failed", "recovering"]
            if state["status"] not in valid_statuses:
                raise StateError(f"Invalid process status: {state['status']}")
            
            # Validate timestamp
            if not self._validate_timestamp(state["last_update"]):
                raise StateError(f"Invalid timestamp in process state: {state['last_update']}")
            
            return True
            
        except StateError:
            raise
        except Exception as e:
            raise StateError(f"Process state validation error: {e}")
    
    def _validate_timestamp(self, timestamp: Any) -> bool:
        """Validate timestamp format"""
        try:
            if isinstance(timestamp, str):
                datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return True
            elif isinstance(timestamp, datetime):
                return True
            return False
        except (ValueError, TypeError):
            return False


def coordination_timer(operation_name: str = None):
    """
    Decorator for timing coordination operations
    
    Args:
        operation_name: Optional operation name override
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            timing_manager = TimingManager()
            op_name = operation_name or f"{func.__module__}.{func.__name__}"
            
            operation_id = timing_manager.start_operation(op_name)
            
            try:
                result = func(*args, **kwargs)
                timing_manager.end_operation(operation_id, success=True)
                return result
            except Exception as e:
                timing_manager.end_operation(operation_id, success=False, error_message=str(e))
                raise
        
        return wrapper
    return decorator


def coordination_retry(max_retries: int = 3, delay_seconds: float = 1.0, 
                      exponential_backoff: bool = True):
    """
    Decorator for retrying coordination operations
    
    Args:
        max_retries: Maximum number of retries
        delay_seconds: Initial delay between retries
        exponential_backoff: Whether to use exponential backoff
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        delay = delay_seconds * (2 ** attempt) if exponential_backoff else delay_seconds
                        time.sleep(delay)
                    else:
                        break
            
            raise last_exception
        
        return wrapper
    return decorator


def generate_coordination_id(prefix: str = "coord") -> str:
    """
    Generate a unique coordination ID
    
    Args:
        prefix: Optional prefix for the ID
        
    Returns:
        Unique coordination ID
    """
    timestamp = int(time.time() * 1000)  # milliseconds
    random_id = uuid.uuid4().hex[:8]
    return f"{prefix}_{timestamp}_{random_id}"


def hash_message_content(message: Dict[str, Any]) -> str:
    """
    Create hash of message content for deduplication
    
    Args:
        message: Message dictionary
        
    Returns:
        Hash string
    """
    # Create stable string representation
    content_str = json.dumps(message, sort_keys=True, default=str)
    
    # Generate hash
    return hashlib.sha256(content_str.encode()).hexdigest()[:16]


def calculate_message_priority_score(message: Dict[str, Any]) -> float:
    """
    Calculate numeric priority score for message ordering
    
    Args:
        message: Message dictionary
        
    Returns:
        Priority score (higher = more important)
    """
    # Load priority scores from configuration
    base_scores = _load_priority_scores()
    
    message_type = message.get("message_type", "unknown")
    base_score = base_scores.get(message_type, 10.0)
    
    # Adjust based on message priority if present
    priority_multipliers = {
        "critical": 2.0,
        "high": 1.5,
        "normal": 1.0,
        "low": 0.5
    }
    
    priority = message.get("priority", "normal")
    if isinstance(priority, int):
        # Convert numeric priority to string
        priority_map = {4: "critical", 3: "high", 2: "normal", 1: "low"}
        priority = priority_map.get(priority, "normal")
    
    multiplier = priority_multipliers.get(priority, 1.0)
    
    return base_score * multiplier


def is_coordination_message(message: Dict[str, Any]) -> bool:
    """
    Check if message is a coordination message
    
    Args:
        message: Message dictionary
        
    Returns:
        True if coordination message
    """
    coordination_fields = ["coordination", "message_id", "sender", "recipient"]
    return any(field in message for field in coordination_fields)


def extract_symbols_from_message(message: Dict[str, Any]) -> List[str]:
    """
    Extract symbol list from message data
    
    Args:
        message: Message dictionary
        
    Returns:
        List of symbols
    """
    symbols = []
    
    # Check various possible locations for symbols
    data = message.get("data", {})
    
    # Direct symbols field
    if "symbols" in data:
        symbols.extend(data["symbols"])
    
    # Symbol field (single symbol)
    if "symbol" in data:
        symbols.append(data["symbol"])
    
    # Coordination metadata
    if "coordination" in data and "symbols" in data["coordination"]:
        symbols.extend(data["coordination"]["symbols"])
    
    # Remove duplicates and return
    return list(set(symbols))


def _load_priority_scores() -> Dict[str, float]:
    """Load message priority scores from configuration"""
    try:
        # Try to get from coordination config if available
        try:
            from ..coordination_config import get_coordination_config
            config_manager = get_coordination_config()
            return config_manager.message_config.priority_scores
        except ImportError:
            pass
        
        # Load from environment or use defaults
        import os
        import json
        
        # Load from environment JSON
        priority_scores_env = os.getenv('FRIREN_PRIORITY_SCORES')
        if priority_scores_env:
            return json.loads(priority_scores_env)
        
        # Load individual scores from environment
        scores = {}
        for key, value in os.environ.items():
            if key.startswith('FRIREN_SCORE_'):
                message_type = key.replace('FRIREN_SCORE_', '').lower()
                scores[message_type] = float(value)
        
        if scores:
            return scores
        
        # Default priority scores
        return {
            "execution_order": 100.0,
            "risk_assessment": 90.0,
            "trading_signal": 80.0,
            "position_update": 70.0,
            "sentiment_analysis": 60.0,
            "news_collection": 50.0,
            "strategy_assignment": 40.0,
            "market_data": 30.0,
            "system_health": 20.0
        }
        
    except Exception as e:
        # Minimal fallback
        return {
            "execution_order": 100.0,
            "trading_signal": 80.0,
            "system_health": 20.0
        }


# Global instances
_message_validator = MessageValidator()
_message_serializer = MessageSerializer()
_timing_manager = TimingManager()
_state_validator = StateValidator()


def validate_message(message: Dict[str, Any], category: str = "coordination") -> bool:
    """Global message validation function"""
    return _message_validator.validate_message(message, category)


def serialize_message(message: Dict[str, Any]) -> str:
    """Global message serialization function"""
    return _message_serializer.serialize_message(message)


def deserialize_message(message_json: str) -> Dict[str, Any]:
    """Global message deserialization function"""
    return _message_serializer.deserialize_message(message_json)


def validate_system_state(state: Dict[str, Any]) -> bool:
    """Global system state validation function"""
    return _state_validator.validate_system_state(state)


def get_timing_stats(operation: str) -> Dict[str, Any]:
    """Global timing statistics function"""
    return _timing_manager.get_operation_stats(operation)