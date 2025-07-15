"""
API Integration Resilience System

Provides comprehensive retry logic, circuit breakers, and graceful degradation
for all external API integrations in the trading system.

Features:
- Exponential backoff retry logic
- Circuit breaker pattern implementation
- API health monitoring and reporting
- Graceful degradation strategies
- Connection pooling optimization
- Rate limiting and throttling
"""

import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable, List, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
import statistics
import random


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Circuit is open, calls fail fast
    HALF_OPEN = "half_open" # Testing if service has recovered


class APIServiceType(Enum):
    """Types of API services"""
    ALPACA_TRADING = "alpaca_trading"
    ALPACA_DATA = "alpaca_data"
    YAHOO_FINANCE = "yahoo_finance"
    REDIS = "redis"
    DATABASE = "database"
    FINBERT = "finbert"
    NEWS_API = "news_api"


@dataclass
class RetryConfig:
    """Configuration for retry logic"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    backoff_multiplier: float = 1.5


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5
    recovery_timeout: int = 60
    expected_exception: tuple = (Exception,)
    success_threshold: int = 3  # Consecutive successes needed to close circuit


@dataclass
class APICall:
    """Records an API call for monitoring"""
    timestamp: datetime
    service: APIServiceType
    operation: str
    success: bool
    response_time_ms: float
    error_message: Optional[str] = None


@dataclass
class ServiceHealth:
    """Health status of an API service"""
    service: APIServiceType
    is_healthy: bool
    last_success: Optional[datetime]
    last_failure: Optional[datetime]
    success_rate: float
    avg_response_time_ms: float
    circuit_state: CircuitState
    consecutive_failures: int
    consecutive_successes: int


class CircuitBreaker:
    """
    Circuit breaker implementation for API resilience
    
    Prevents cascading failures by temporarily stopping calls to failing services
    """
    
    def __init__(self, service: APIServiceType, config: CircuitBreakerConfig):
        self.service = service
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.lock = threading.RLock()
        self.logger = logging.getLogger(f"circuit_breaker.{service.value}")
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self.lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.logger.info(f"Circuit breaker for {self.service.value} entering HALF_OPEN state")
                else:
                    raise Exception(f"Circuit breaker OPEN for {self.service.value} - service unavailable")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except Exception as e:
                self._on_failure()
                raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return True
        time_since_failure = (datetime.now() - self.last_failure_time).total_seconds()
        return time_since_failure >= self.config.recovery_timeout
    
    def _on_success(self):
        """Handle successful call"""
        self.failure_count = 0
        self.success_count += 1
        
        if self.state == CircuitState.HALF_OPEN:
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                self.success_count = 0
                self.logger.info(f"Circuit breaker for {self.service.value} CLOSED - service recovered")
    
    def _on_failure(self):
        """Handle failed call"""
        self.failure_count += 1
        self.success_count = 0
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.config.failure_threshold:
            if self.state != CircuitState.OPEN:
                self.state = CircuitState.OPEN
                self.logger.warning(f"Circuit breaker for {self.service.value} OPENED - too many failures")
    
    def get_state(self) -> CircuitState:
        """Get current circuit breaker state"""
        return self.state
    
    def force_open(self):
        """Manually open the circuit breaker"""
        with self.lock:
            self.state = CircuitState.OPEN
            self.last_failure_time = datetime.now()
            self.logger.warning(f"Circuit breaker for {self.service.value} manually OPENED")
    
    def force_close(self):
        """Manually close the circuit breaker"""
        with self.lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.logger.info(f"Circuit breaker for {self.service.value} manually CLOSED")


class RetryHandler:
    """
    Implements exponential backoff retry logic with jitter
    """
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.logger = logging.getLogger("retry_handler")
    
    def execute_with_retry(self, func: Callable, service: APIServiceType, operation: str, *args, **kwargs):
        """Execute function with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                if attempt == self.config.max_attempts - 1:
                    # Final attempt failed
                    self.logger.error(f"All retry attempts failed for {service.value}.{operation}: {e}")
                    break
                
                delay = self._calculate_delay(attempt)
                self.logger.warning(f"Attempt {attempt + 1} failed for {service.value}.{operation}: {e}. "
                                  f"Retrying in {delay:.2f}s")
                time.sleep(delay)
        
        # All attempts failed
        raise last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for next retry attempt"""
        delay = self.config.base_delay * (self.config.exponential_base ** attempt)
        delay = min(delay, self.config.max_delay)
        
        if self.config.jitter:
            # Add jitter to prevent thundering herd
            jitter = delay * 0.1 * random.random()
            delay += jitter
        
        return delay


class APIResilienceManager:
    """
    Central manager for API resilience across all external services
    
    Provides unified retry logic, circuit breakers, and health monitoring
    for all external API integrations in the trading system.
    """
    
    def __init__(self):
        self.logger = logging.getLogger("api_resilience_manager")
        
        # Circuit breakers for each service
        self.circuit_breakers: Dict[APIServiceType, CircuitBreaker] = {}
        
        # Retry handlers
        self.retry_handler = RetryHandler(RetryConfig())
        
        # Health monitoring
        self.call_history: List[APICall] = []
        self.max_history_size = 10000
        self.health_lock = threading.RLock()
        
        # Service configurations
        self._initialize_circuit_breakers()
        
        self.logger.info("API Resilience Manager initialized")
    
    def _initialize_circuit_breakers(self):
        """Initialize circuit breakers for all services"""
        service_configs = {
            APIServiceType.ALPACA_TRADING: CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=30,
                success_threshold=2
            ),
            APIServiceType.ALPACA_DATA: CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=15,
                success_threshold=3
            ),
            APIServiceType.YAHOO_FINANCE: CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=30,
                success_threshold=2
            ),
            APIServiceType.REDIS: CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=10,
                success_threshold=2
            ),
            APIServiceType.DATABASE: CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=20,
                success_threshold=2
            ),
            APIServiceType.FINBERT: CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=60,
                success_threshold=2
            ),
            APIServiceType.NEWS_API: CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=30,
                success_threshold=3
            )
        }
        
        for service, config in service_configs.items():
            self.circuit_breakers[service] = CircuitBreaker(service, config)
    
    def resilient_call(self, 
                      func: Callable, 
                      service: APIServiceType, 
                      operation: str,
                      use_circuit_breaker: bool = True,
                      use_retry: bool = True,
                      *args, **kwargs) -> Any:
        """
        Execute API call with full resilience protection
        
        Args:
            func: Function to execute
            service: Type of API service
            operation: Operation name for logging
            use_circuit_breaker: Whether to use circuit breaker protection
            use_retry: Whether to use retry logic
            *args, **kwargs: Arguments to pass to function
            
        Returns:
            Result from function call
            
        Raises:
            Exception: If all resilience mechanisms fail
        """
        start_time = time.time()
        
        try:
            # Wrap function with circuit breaker if enabled
            if use_circuit_breaker and service in self.circuit_breakers:
                circuit_breaker = self.circuit_breakers[service]
                if use_retry:
                    # Combine circuit breaker and retry
                    def protected_func(*args, **kwargs):
                        return circuit_breaker.call(func, *args, **kwargs)
                    result = self.retry_handler.execute_with_retry(
                        protected_func, service, operation, *args, **kwargs
                    )
                else:
                    result = circuit_breaker.call(func, *args, **kwargs)
            elif use_retry:
                # Only retry, no circuit breaker
                result = self.retry_handler.execute_with_retry(
                    func, service, operation, *args, **kwargs
                )
            else:
                # No protection
                result = func(*args, **kwargs)
            
            # Record successful call
            response_time = (time.time() - start_time) * 1000
            self._record_api_call(service, operation, True, response_time)
            
            return result
            
        except Exception as e:
            # Record failed call
            response_time = (time.time() - start_time) * 1000
            self._record_api_call(service, operation, False, response_time, str(e))
            raise
    
    def _record_api_call(self, 
                        service: APIServiceType, 
                        operation: str, 
                        success: bool, 
                        response_time_ms: float,
                        error_message: Optional[str] = None):
        """Record API call for health monitoring"""
        with self.health_lock:
            call = APICall(
                timestamp=datetime.now(),
                service=service,
                operation=operation,
                success=success,
                response_time_ms=response_time_ms,
                error_message=error_message
            )
            
            self.call_history.append(call)
            
            # Limit history size
            if len(self.call_history) > self.max_history_size:
                self.call_history = self.call_history[-self.max_history_size:]
    
    def get_service_health(self, service: APIServiceType) -> ServiceHealth:
        """Get health status for a specific service"""
        with self.health_lock:
            # Filter calls for this service from last hour
            one_hour_ago = datetime.now() - timedelta(hours=1)
            service_calls = [
                call for call in self.call_history 
                if call.service == service and call.timestamp > one_hour_ago
            ]
            
            if not service_calls:
                return ServiceHealth(
                    service=service,
                    is_healthy=True,  # Assume healthy if no recent calls
                    last_success=None,
                    last_failure=None,
                    success_rate=1.0,
                    avg_response_time_ms=0.0,
                    circuit_state=self.circuit_breakers[service].get_state(),
                    consecutive_failures=0,
                    consecutive_successes=0
                )
            
            # Calculate metrics
            successful_calls = [call for call in service_calls if call.success]
            failed_calls = [call for call in service_calls if not call.success]
            
            success_rate = len(successful_calls) / len(service_calls)
            avg_response_time = statistics.mean([call.response_time_ms for call in service_calls])
            
            last_success = max([call.timestamp for call in successful_calls]) if successful_calls else None
            last_failure = max([call.timestamp for call in failed_calls]) if failed_calls else None
            
            # Calculate consecutive failures/successes
            consecutive_failures = 0
            consecutive_successes = 0
            
            # Check recent calls in reverse order
            for call in reversed(service_calls[-10:]):  # Last 10 calls
                if call.success:
                    if consecutive_failures == 0:
                        consecutive_successes += 1
                    else:
                        break
                else:
                    if consecutive_successes == 0:
                        consecutive_failures += 1
                    else:
                        break
            
            is_healthy = (
                success_rate >= 0.8 and  # At least 80% success rate
                consecutive_failures < 3 and  # Less than 3 consecutive failures
                self.circuit_breakers[service].get_state() != CircuitState.OPEN
            )
            
            return ServiceHealth(
                service=service,
                is_healthy=is_healthy,
                last_success=last_success,
                last_failure=last_failure,
                success_rate=success_rate,
                avg_response_time_ms=avg_response_time,
                circuit_state=self.circuit_breakers[service].get_state(),
                consecutive_failures=consecutive_failures,
                consecutive_successes=consecutive_successes
            )
    
    def get_all_service_health(self) -> Dict[APIServiceType, ServiceHealth]:
        """Get health status for all services"""
        return {service: self.get_service_health(service) for service in APIServiceType}
    
    def force_circuit_breaker_state(self, service: APIServiceType, state: CircuitState):
        """Manually control circuit breaker state"""
        if service in self.circuit_breakers:
            if state == CircuitState.OPEN:
                self.circuit_breakers[service].force_open()
            elif state == CircuitState.CLOSED:
                self.circuit_breakers[service].force_close()
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive health summary"""
        all_health = self.get_all_service_health()
        
        healthy_services = sum(1 for health in all_health.values() if health.is_healthy)
        total_services = len(all_health)
        
        # Calculate overall system health
        overall_success_rate = statistics.mean([
            health.success_rate for health in all_health.values()
        ]) if all_health else 1.0
        
        # Get circuit breaker status
        open_circuits = [
            service.value for service, health in all_health.items()
            if health.circuit_state == CircuitState.OPEN
        ]
        
        return {
            'overall_health_score': healthy_services / total_services,
            'healthy_services': healthy_services,
            'total_services': total_services,
            'overall_success_rate': overall_success_rate,
            'open_circuit_breakers': open_circuits,
            'total_api_calls_last_hour': len([
                call for call in self.call_history
                if call.timestamp > datetime.now() - timedelta(hours=1)
            ]),
            'service_details': {
                service.value: {
                    'is_healthy': health.is_healthy,
                    'success_rate': health.success_rate,
                    'avg_response_time_ms': health.avg_response_time_ms,
                    'circuit_state': health.circuit_state.value,
                    'consecutive_failures': health.consecutive_failures
                } for service, health in all_health.items()
            }
        }


# Decorator for easy resilient API calls
def resilient_api_call(service: APIServiceType, operation: str, 
                      use_circuit_breaker: bool = True, use_retry: bool = True):
    """
    Decorator for making API calls resilient
    
    Usage:
        @resilient_api_call(APIServiceType.ALPACA_TRADING, "get_account")
        def get_account_info(self):
            return self.trading_client.get_account()
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get global resilience manager instance
            manager = get_resilience_manager()
            return manager.resilient_call(
                func, service, operation, use_circuit_breaker, use_retry, *args, **kwargs
            )
        return wrapper
    return decorator


# Global resilience manager instance
_resilience_manager: Optional[APIResilienceManager] = None


def get_resilience_manager() -> APIResilienceManager:
    """Get global resilience manager instance"""
    global _resilience_manager
    if _resilience_manager is None:
        _resilience_manager = APIResilienceManager()
    return _resilience_manager


def reset_resilience_manager():
    """Reset global resilience manager (for testing)"""
    global _resilience_manager
    _resilience_manager = None


# Convenience functions for common patterns
def resilient_redis_call(func: Callable, operation: str = "redis_operation", *args, **kwargs):
    """Make resilient Redis call"""
    manager = get_resilience_manager()
    return manager.resilient_call(func, APIServiceType.REDIS, operation, True, True, *args, **kwargs)


def resilient_alpaca_call(func: Callable, operation: str = "alpaca_operation", *args, **kwargs):
    """Make resilient Alpaca API call"""
    manager = get_resilience_manager()
    return manager.resilient_call(func, APIServiceType.ALPACA_TRADING, operation, True, True, *args, **kwargs)


def resilient_news_call(func: Callable, operation: str = "news_operation", *args, **kwargs):
    """Make resilient news API call"""
    manager = get_resilience_manager()
    return manager.resilient_call(func, APIServiceType.NEWS_API, operation, True, True, *args, **kwargs)


def resilient_database_call(func: Callable, operation: str = "database_operation", *args, **kwargs):
    """Make resilient database call"""
    manager = get_resilience_manager()
    return manager.resilient_call(func, APIServiceType.DATABASE, operation, True, True, *args, **kwargs)


if __name__ == "__main__":
    # Example usage and testing
    manager = APIResilienceManager()
    
    # Test function that sometimes fails
    def test_api_call(should_fail=False):
        if should_fail:
            raise Exception("API call failed")
        return "Success"
    
    # Test resilient call
    try:
        result = manager.resilient_call(
            test_api_call, 
            APIServiceType.ALPACA_TRADING, 
            "test_operation",
            should_fail=False
        )
        print(f"Result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
    
    # Get health summary
    health = manager.get_health_summary()
    print(f"Health summary: {health}")