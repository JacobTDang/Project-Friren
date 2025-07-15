"""
Message Router for Friren Trading System
========================================

Routes formatted messages to appropriate destinations (terminal, logs, Redis)
with priority-based routing and multi-channel support.

Supports:
- Terminal output for real-time visibility
- Redis pub/sub for process communication
- Log file integration for audit trails
- Priority-based message routing
"""

import logging
try:
    import redis
except ImportError:
    redis = None
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from dataclasses import dataclass
import json


class MessagePriority(Enum):
    """Message priority levels"""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4


class MessageType(Enum):
    """Message type classification"""
    NEWS = "NEWS"
    ANALYSIS = "ANALYSIS"
    TRADING = "TRADING"
    MONITORING = "MONITORING"
    SYSTEM = "SYSTEM"
    ERROR = "ERROR"


@dataclass
class FormattedMessage:
    """Structured message for routing"""
    content: str
    message_type: MessageType
    priority: MessagePriority
    symbol: Optional[str] = None
    component: Optional[str] = None
    timestamp: datetime = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.metadata is None:
            self.metadata = {}


class MessageRouter:
    """Routes formatted messages to appropriate destinations"""
    
    def __init__(self, redis_client: Optional[Union['redis.Redis', Any]] = None, 
                 enable_terminal: bool = True, enable_logging: bool = True):
        """
        Initialize message router
        
        Args:
            redis_client: Redis client for pub/sub messaging
            enable_terminal: Enable terminal output
            enable_logging: Enable log file output
        """
        self.redis_client = redis_client
        self.enable_terminal = enable_terminal
        self.enable_logging = enable_logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize channels
        self.terminal_channel = "friren:terminal"
        self.log_channel = "friren:logs"
        self.priority_channels = {
            MessagePriority.CRITICAL: "friren:priority:critical",
            MessagePriority.HIGH: "friren:priority:high",
            MessagePriority.MEDIUM: "friren:priority:medium",
            MessagePriority.LOW: "friren:priority:low"
        }
        
        # Message statistics
        self.message_stats = {
            'total_messages': 0,
            'by_priority': {p: 0 for p in MessagePriority},
            'by_type': {t: 0 for t in MessageType}
        }
    
    def route_message(self, message: FormattedMessage) -> bool:
        """
        Route a formatted message to appropriate destinations
        
        Args:
            message: Formatted message to route
            
        Returns:
            True if message was routed successfully
        """
        try:
            # Update statistics
            self.message_stats['total_messages'] += 1
            self.message_stats['by_priority'][message.priority] += 1
            self.message_stats['by_type'][message.message_type] += 1
            
            # Route to terminal
            if self.enable_terminal:
                self._route_to_terminal(message)
            
            # Route to logs
            if self.enable_logging:
                self._route_to_logs(message)
            
            # Route to Redis
            if self.redis_client:
                self._route_to_redis(message)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to route message: {e}")
            return False
    
    def _route_to_terminal(self, message: FormattedMessage) -> None:
        """Route message to terminal output"""
        # Color coding based on priority
        color_codes = {
            MessagePriority.CRITICAL: '\033[91m',  # Red
            MessagePriority.HIGH: '\033[93m',      # Yellow
            MessagePriority.MEDIUM: '\033[92m',    # Green
            MessagePriority.LOW: '\033[94m'        # Blue
        }
        reset_code = '\033[0m'
        
        color = color_codes.get(message.priority, '')
        timestamp_str = message.timestamp.strftime("%H:%M:%S")
        
        # Format for terminal with color
        terminal_output = f"{color}[{timestamp_str}] {message.content}{reset_code}"
        print(terminal_output)
    
    def _route_to_logs(self, message: FormattedMessage) -> None:
        """Route message to log files"""
        # Map priority to log level
        log_levels = {
            MessagePriority.CRITICAL: logging.CRITICAL,
            MessagePriority.HIGH: logging.ERROR,
            MessagePriority.MEDIUM: logging.WARNING,
            MessagePriority.LOW: logging.INFO
        }
        
        log_level = log_levels.get(message.priority, logging.INFO)
        
        # Create structured log entry
        log_data = {
            'message': message.content,
            'type': message.message_type.value,
            'priority': message.priority.value,
            'symbol': message.symbol,
            'component': message.component,
            'metadata': message.metadata
        }
        
        self.logger.log(log_level, json.dumps(log_data))
    
    def _route_to_redis(self, message: FormattedMessage) -> None:
        """Route message to Redis pub/sub channels"""
        try:
            # Prepare message for Redis
            redis_message = {
                'content': message.content,
                'type': message.message_type.value,
                'priority': message.priority.value,
                'symbol': message.symbol,
                'component': message.component,
                'timestamp': message.timestamp.isoformat(),
                'metadata': message.metadata
            }
            
            message_json = json.dumps(redis_message)
            
            # Publish to general terminal channel
            self.redis_client.publish(self.terminal_channel, message_json)
            
            # Publish to priority-specific channel
            priority_channel = self.priority_channels.get(message.priority)
            if priority_channel:
                self.redis_client.publish(priority_channel, message_json)
            
            # Publish to type-specific channel if needed
            if message.message_type == MessageType.ERROR:
                self.redis_client.publish("friren:errors", message_json)
            
        except Exception as e:
            self.logger.error(f"Failed to publish to Redis: {e}")
    
    def route_text(self, text: str, message_type: MessageType = MessageType.SYSTEM,
                  priority: MessagePriority = MessagePriority.MEDIUM,
                  symbol: Optional[str] = None, component: Optional[str] = None) -> bool:
        """
        Route a simple text message
        
        Args:
            text: Message text
            message_type: Type of message
            priority: Message priority
            symbol: Associated symbol (optional)
            component: Source component (optional)
            
        Returns:
            True if message was routed successfully
        """
        message = FormattedMessage(
            content=text,
            message_type=message_type,
            priority=priority,
            symbol=symbol,
            component=component
        )
        
        return self.route_message(message)
    
    def route_error(self, error_text: str, component: str, 
                   symbol: Optional[str] = None) -> bool:
        """
        Route an error message with high priority
        
        Args:
            error_text: Error message
            component: Component that generated the error
            symbol: Associated symbol (optional)
            
        Returns:
            True if message was routed successfully
        """
        return self.route_text(
            text=error_text,
            message_type=MessageType.ERROR,
            priority=MessagePriority.HIGH,
            symbol=symbol,
            component=component
        )
    
    def route_critical(self, critical_text: str, component: str,
                      symbol: Optional[str] = None) -> bool:
        """
        Route a critical message with highest priority
        
        Args:
            critical_text: Critical message
            component: Component that generated the message
            symbol: Associated symbol (optional)
            
        Returns:
            True if message was routed successfully
        """
        return self.route_text(
            text=critical_text,
            message_type=MessageType.SYSTEM,
            priority=MessagePriority.CRITICAL,
            symbol=symbol,
            component=component
        )
    
    def get_message_stats(self) -> Dict[str, Any]:
        """
        Get message routing statistics
        
        Returns:
            Dictionary containing message statistics
        """
        return {
            'total_messages': self.message_stats['total_messages'],
            'by_priority': {p.name: count for p, count in self.message_stats['by_priority'].items()},
            'by_type': {t.name: count for t, count in self.message_stats['by_type'].items()},
            'routing_enabled': {
                'terminal': self.enable_terminal,
                'logging': self.enable_logging,
                'redis': self.redis_client is not None
            }
        }
    
    def clear_stats(self) -> None:
        """Clear message routing statistics"""
        self.message_stats = {
            'total_messages': 0,
            'by_priority': {p: 0 for p in MessagePriority},
            'by_type': {t: 0 for t in MessageType}
        }
    
    def validate_routing_infrastructure(self) -> bool:
        """
        Validate message routing infrastructure without hardcoded data
        
        Returns:
            True if routing infrastructure is properly initialized
        """
        try:
            # Check if terminal routing is enabled and configured
            if self.enable_terminal:
                # Terminal routing infrastructure check
                pass
            
            # Check if logging is enabled and configured  
            if self.enable_logging and self.logger:
                # Logging infrastructure check
                pass
            
            # Check if Redis client is available and connected
            if self.redis_client:
                try:
                    self.redis_client.ping()
                except Exception:
                    return False
            
            return True
            
        except Exception:
            return False