"""
Multiprocess Infrastructure Module

PRODUCTION: Redis-based infrastructure only. No fallback systems allowed.
All components require Redis for proper operation.
"""

# PRODUCTION: Redis components required - no fallback allowed
from .redis_base_process import RedisBaseProcess, ProcessState
from .redis_process_manager import RedisProcessManager, ProcessConfig, RestartPolicy
from .trading_redis_manager import TradingRedisManager, get_trading_redis_manager, ProcessMessage, MessagePriority
from .windows_handle_monitor import (
    WindowsHandleMonitor, get_windows_handle_monitor, register_process_handle, 
    unregister_process_handle, start_handle_monitoring, stop_handle_monitoring
)

# Redis is required for production
REDIS_AVAILABLE = True

__all__ = [
    # Redis components only
    'RedisBaseProcess',
    'ProcessState',
    'RedisProcessManager', 
    'ProcessConfig',
    'RestartPolicy',
    'TradingRedisManager',
    'get_trading_redis_manager',
    'ProcessMessage',
    'MessagePriority',
    'REDIS_AVAILABLE',
    
    # Windows handle management
    'WindowsHandleMonitor',
    'get_windows_handle_monitor',
    'register_process_handle',
    'unregister_process_handle', 
    'start_handle_monitoring',
    'stop_handle_monitoring'
]