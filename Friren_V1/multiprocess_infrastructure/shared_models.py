"""
Shared Models for Friren Trading System Multiprocess Infrastructure

This module provides shared data structures and message types used across
the multiprocess trading system components.
"""

from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, Any, Optional, List
from datetime import datetime
import json


def serialize_datetime_dict(data_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Helper function to serialize datetime objects in dictionaries"""
    result = {}
    for key, value in data_dict.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()
        elif isinstance(value, dict):
            result[key] = serialize_datetime_dict(value)
        elif isinstance(value, list):
            result[key] = [
                item.isoformat() if isinstance(item, datetime) else
                serialize_datetime_dict(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            result[key] = value
    return result


class MessageType(Enum):
    """Message types for inter-process communication"""
    
    # Business Logic Messages
    TEST_BUSINESS_OUTPUT = "test_business_output"
    TRADING_RECOMMENDATION = "trading_recommendation"
    RISK_ASSESSMENT = "risk_assessment"
    EXECUTION_ORDER = "execution_order"
    POSITION_UPDATE = "position_update"
    
    # System Messages
    HEALTH_CHECK = "health_check"
    PROCESS_STATUS = "process_status"
    MEMORY_WARNING = "memory_warning"
    MEMORY_CRITICAL = "memory_critical"
    
    # Coordination Messages
    SYSTEM_COORDINATION = "system_coordination"
    QUEUE_ROTATION = "queue_rotation"
    PROCESS_SYNC = "process_sync"
    
    # News and Analysis Messages
    NEWS_COLLECTED = "news_collected"
    SENTIMENT_ANALYSIS = "sentiment_analysis"
    FINBERT_RESULT = "finbert_result"
    XGBOOST_RECOMMENDATION = "xgboost_recommendation"
    
    # Market Data Messages
    MARKET_DATA_UPDATE = "market_data_update"
    REGIME_CHANGE = "regime_change"
    VOLATILITY_UPDATE = "volatility_update"


@dataclass
class SystemMessage:
    """Standard message structure for inter-process communication"""
    
    message_type: MessageType
    content: Dict[str, Any]
    timestamp: float
    process_id: str
    priority: int = 5  # 1 = highest, 10 = lowest
    correlation_id: Optional[str] = None
    routing_key: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Initialize default values after dataclass creation"""
        if self.metadata is None:
            self.metadata = {}
        
        # Ensure timestamp is float
        if isinstance(self.timestamp, datetime):
            self.timestamp = self.timestamp.timestamp()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for serialization with datetime handling"""
        base_dict = {
            'message_type': self.message_type.value,
            'content': self.content,
            'timestamp': self.timestamp,
            'process_id': self.process_id,
            'priority': self.priority,
            'correlation_id': self.correlation_id,
            'routing_key': self.routing_key,
            'metadata': self.metadata
        }
        # Ensure datetime serialization for nested content
        return serialize_datetime_dict(base_dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SystemMessage':
        """Create message from dictionary"""
        return cls(
            message_type=MessageType(data['message_type']),
            content=data['content'],
            timestamp=data['timestamp'],
            process_id=data['process_id'],
            priority=data.get('priority', 5),
            correlation_id=data.get('correlation_id'),
            routing_key=data.get('routing_key'),
            metadata=data.get('metadata', {})
        )
    
    def to_json(self) -> str:
        """Convert message to JSON string"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'SystemMessage':
        """Create message from JSON string"""
        return cls.from_dict(json.loads(json_str))


@dataclass
class ProcessStatus:
    """Process health and status information"""
    
    process_id: str
    status: str  # 'healthy', 'warning', 'critical', 'failed'
    last_heartbeat: float
    memory_usage_mb: float
    cpu_usage_percent: float
    queue_depth: int
    error_count: int
    last_error: Optional[str] = None
    uptime_seconds: float = 0.0
    cycles_completed: int = 0
    performance_metrics: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Initialize default values"""
        if self.performance_metrics is None:
            self.performance_metrics = {}
    
    def is_healthy(self) -> bool:
        """Check if process is healthy"""
        return self.status in ['healthy', 'warning']
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'process_id': self.process_id,
            'status': self.status,
            'last_heartbeat': self.last_heartbeat,
            'memory_usage_mb': self.memory_usage_mb,
            'cpu_usage_percent': self.cpu_usage_percent,
            'queue_depth': self.queue_depth,
            'error_count': self.error_count,
            'last_error': self.last_error,
            'uptime_seconds': self.uptime_seconds,
            'cycles_completed': self.cycles_completed,
            'performance_metrics': self.performance_metrics
        }


@dataclass
class TradingRecommendation:
    """Trading recommendation structure"""
    
    symbol: str
    action: str  # 'BUY', 'SELL', 'HOLD'
    confidence: float
    quantity: Optional[int] = None
    price_target: Optional[float] = None
    stop_loss: Optional[float] = None
    reasoning: Optional[str] = None
    sentiment_score: Optional[float] = None
    technical_indicators: Optional[Dict[str, Any]] = None
    risk_score: Optional[float] = None
    timestamp: Optional[float] = None
    
    def __post_init__(self):
        """Initialize default values"""
        if self.timestamp is None:
            self.timestamp = datetime.now().timestamp()
        if self.technical_indicators is None:
            self.technical_indicators = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with datetime serialization"""
        base_dict = {
            'symbol': self.symbol,
            'action': self.action,
            'confidence': self.confidence,
            'quantity': self.quantity,
            'price_target': self.price_target,
            'stop_loss': self.stop_loss,
            'reasoning': self.reasoning,
            'sentiment_score': self.sentiment_score,
            'technical_indicators': self.technical_indicators,
            'risk_score': self.risk_score,
            'timestamp': self.timestamp
        }
        return serialize_datetime_dict(base_dict)


@dataclass
class MemoryAlert:
    """Memory usage alert structure"""
    
    process_id: str
    memory_usage_mb: float
    memory_limit_mb: float
    growth_rate_mb_per_min: float
    alert_level: str  # 'warning', 'critical', 'emergency'
    timestamp: float
    recommended_action: Optional[str] = None
    
    def utilization_percentage(self) -> float:
        """Calculate memory utilization percentage"""
        if self.memory_limit_mb <= 0:
            return 0.0
        return (self.memory_usage_mb / self.memory_limit_mb) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'process_id': self.process_id,
            'memory_usage_mb': self.memory_usage_mb,
            'memory_limit_mb': self.memory_limit_mb,
            'growth_rate_mb_per_min': self.growth_rate_mb_per_min,
            'alert_level': self.alert_level,
            'timestamp': self.timestamp,
            'recommended_action': self.recommended_action,
            'utilization_percentage': self.utilization_percentage()
        }


# Helper functions for message creation
def create_test_message(process_id: str, content: str) -> SystemMessage:
    """Create a test business output message"""
    return SystemMessage(
        message_type=MessageType.TEST_BUSINESS_OUTPUT,
        content={'message': content},
        timestamp=datetime.now().timestamp(),
        process_id=process_id,
        priority=1
    )


def create_trading_recommendation_message(
    process_id: str, 
    recommendation: TradingRecommendation
) -> SystemMessage:
    """Create a trading recommendation message"""
    return SystemMessage(
        message_type=MessageType.TRADING_RECOMMENDATION,
        content=recommendation.to_dict(),
        timestamp=datetime.now().timestamp(),
        process_id=process_id,
        priority=2
    )


def create_memory_alert_message(
    process_id: str, 
    alert: MemoryAlert
) -> SystemMessage:
    """Create a memory alert message"""
    return SystemMessage(
        message_type=MessageType.MEMORY_WARNING if alert.alert_level == 'warning' else MessageType.MEMORY_CRITICAL,
        content=alert.to_dict(),
        timestamp=datetime.now().timestamp(),
        process_id=process_id,
        priority=1 if alert.alert_level == 'critical' else 3
    )


def create_health_check_message(process_id: str, status: ProcessStatus) -> SystemMessage:
    """Create a health check message"""
    return SystemMessage(
        message_type=MessageType.HEALTH_CHECK,
        content=status.to_dict(),
        timestamp=datetime.now().timestamp(),
        process_id=process_id,
        priority=5
    )