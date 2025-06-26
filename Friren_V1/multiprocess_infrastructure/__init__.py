"""
Multiprocess Infrastructure Module

Provides fallback and Redis-compatible infrastructure for the trading system.
Only imports what's available to avoid dependency errors.
"""

# Always available - fallback manager
from .fallback_manager import FallbackManager, get_fallback_manager

# Try to import Redis components if available
try:
    from .redis_base_process import RedisBaseProcess, ProcessState
    from .redis_process_manager import RedisProcessManager, ProcessConfig, RestartPolicy
    from .trading_redis_manager import TradingRedisManager, get_trading_redis_manager, ProcessMessage, MessagePriority
    
    REDIS_AVAILABLE = True
    
    __all__ = [
        # Fallback components
        'FallbackManager',
        'get_fallback_manager',
        # Redis components
        'RedisBaseProcess',
        'ProcessState',
        'RedisProcessManager', 
        'ProcessConfig',
        'RestartPolicy',
        'TradingRedisManager',
        'get_trading_redis_manager',
        'ProcessMessage',
        'MessagePriority'
    ]
    
except ImportError:
    REDIS_AVAILABLE = False
    
    __all__ = [
        # Only fallback components
        'FallbackManager',
        'get_fallback_manager'
    ]