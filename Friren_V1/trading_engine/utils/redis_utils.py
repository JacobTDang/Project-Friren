"""
Redis Connection Reliability Utils
==================================

Provides reliable Redis connection handling with graceful fallbacks
and connection verification for the Friren trading system.
"""

import logging
import redis
from typing import Optional


def get_safe_redis_manager():
    """
    Get Redis manager with connection verification
    
    Returns:
        Redis manager if available and connected, None otherwise
    """
    try:
        from Friren_V1.multiprocess_infrastructure.trading_redis_manager import get_trading_redis_manager
        redis_manager = get_trading_redis_manager()
        if redis_manager and hasattr(redis_manager, 'redis_client'):
            # Test connection
            redis_manager.redis_client.ping()
            return redis_manager
        else:
            return None
    except Exception as e:
        logging.getLogger(__name__).warning(f"Redis connection failed: {e}")
        return None


def safe_redis_operation(redis_client: Optional[redis.Redis], operation_name: str, operation_func, *args, **kwargs):
    """
    Execute Redis operation with error handling
    
    Args:
        redis_client: Redis client
        operation_name: Name of operation for logging
        operation_func: Function to execute
        *args, **kwargs: Arguments for operation_func
        
    Returns:
        Operation result or None if failed
    """
    if not redis_client:
        logging.getLogger(__name__).warning(f"Redis operation '{operation_name}' skipped - no client available")
        return None
        
    try:
        return operation_func(*args, **kwargs)
    except redis.ConnectionError as e:
        logging.getLogger(__name__).error(f"Redis connection error in '{operation_name}': {e}")
        return None
    except redis.TimeoutError as e:
        logging.getLogger(__name__).error(f"Redis timeout in '{operation_name}': {e}")
        return None
    except Exception as e:
        logging.getLogger(__name__).error(f"Redis error in '{operation_name}': {e}")
        return None


def test_redis_connection(redis_client: Optional[redis.Redis]) -> bool:
    """
    Test Redis connection
    
    Args:
        redis_client: Redis client to test
        
    Returns:
        True if connection is working, False otherwise
    """
    if not redis_client:
        return False
        
    try:
        redis_client.ping()
        return True
    except Exception as e:
        logging.getLogger(__name__).warning(f"Redis connection test failed: {e}")
        return False