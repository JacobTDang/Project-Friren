"""
Memory-Aware LRU Cache System

Provides intelligent caching with memory monitoring and pressure-based eviction
specifically designed for AWS t3.micro instances and memory-constrained environments.

Features:
- LRU (Least Recently Used) eviction policy
- Memory pressure detection and adaptive sizing
- Configurable memory limits and thresholds
- Cache statistics and performance monitoring
- Thread-safe operations
- Smart eviction strategies for different data types
"""

import time
import sys
import threading
import psutil
import logging
from typing import Dict, Any, Optional, List, Tuple, Union, Callable
from dataclasses import dataclass, field
from collections import OrderedDict
from datetime import datetime, timedelta
from enum import Enum
import weakref
import gc


class MemoryPressureLevel(Enum):
    """Memory pressure levels for adaptive caching"""
    LOW = "low"           # < 60% memory usage
    MEDIUM = "medium"     # 60-80% memory usage  
    HIGH = "high"         # 80-90% memory usage
    CRITICAL = "critical" # > 90% memory usage


@dataclass
class CacheEntry:
    """Individual cache entry with metadata"""
    key: str
    value: Any
    size_bytes: int
    access_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.now)
    created_at: datetime = field(default_factory=datetime.now)
    ttl_seconds: Optional[int] = None
    
    def is_expired(self) -> bool:
        """Check if entry has expired"""
        if self.ttl_seconds is None:
            return False
        return (datetime.now() - self.created_at).total_seconds() > self.ttl_seconds
    
    def touch(self):
        """Update access metadata"""
        self.access_count += 1
        self.last_accessed = datetime.now()


@dataclass
class CacheStats:
    """Cache performance statistics"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    memory_pressure_evictions: int = 0
    expired_evictions: int = 0
    total_entries_added: int = 0
    current_size_bytes: int = 0
    current_entry_count: int = 0
    max_size_bytes_reached: int = 0
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.hits + self.misses
        return (self.hits / total) if total > 0 else 0.0
    
    @property
    def eviction_rate(self) -> float:
        """Calculate eviction rate"""
        return (self.evictions / self.total_entries_added) if self.total_entries_added > 0 else 0.0


class MemoryAwareLRUCache:
    """
    Memory-aware LRU cache with adaptive sizing and pressure-based eviction
    
    Optimized for AWS t3.micro instances and memory-constrained environments.
    Provides intelligent caching that adapts to memory pressure and system load.
    """
    
    def __init__(self, 
                 max_size_bytes: int = 50 * 1024 * 1024,  # 50MB default
                 max_entries: int = 1000,
                 memory_pressure_threshold: float = 0.8,   # 80% memory usage
                 enable_memory_monitoring: bool = True,
                 monitoring_interval: int = 30,            # seconds
                 enable_ttl: bool = True,
                 default_ttl_seconds: int = 3600):         # 1 hour
        
        self.max_size_bytes = max_size_bytes
        self.max_entries = max_entries
        self.memory_pressure_threshold = memory_pressure_threshold
        self.enable_memory_monitoring = enable_memory_monitoring
        self.monitoring_interval = monitoring_interval
        self.enable_ttl = enable_ttl
        self.default_ttl_seconds = default_ttl_seconds
        
        # Cache storage (using OrderedDict for LRU behavior)
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        
        # Statistics
        self.stats = CacheStats()
        
        # Memory monitoring
        self.current_memory_pressure = MemoryPressureLevel.LOW
        self._memory_monitor_thread = None
        self._memory_monitoring_active = False
        
        # Logger
        self.logger = logging.getLogger("memory_aware_cache")
        
        # Start memory monitoring if enabled
        if self.enable_memory_monitoring:
            self._start_memory_monitoring()
        
        self.logger.info(f"Memory-aware LRU cache initialized: max_size={max_size_bytes/1024/1024:.1f}MB, max_entries={max_entries}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from cache with LRU update"""
        with self._lock:
            if key not in self._cache:
                self.stats.misses += 1
                return default
            
            entry = self._cache[key]
            
            # Check if expired
            if entry.is_expired():
                self._remove_entry(key)
                self.stats.misses += 1
                self.stats.expired_evictions += 1
                return default
            
            # Update access info and move to end (most recently used)
            entry.touch()
            self._cache.move_to_end(key)
            
            self.stats.hits += 1
            self.logger.debug(f"Cache hit for key: {key[:50]}...")
            
            return entry.value
    
    def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """Put value in cache with memory-aware eviction"""
        with self._lock:
            # Calculate value size
            value_size = self._calculate_size(value)
            
            # Check if single value exceeds cache limits
            if value_size > self.max_size_bytes:
                self.logger.warning(f"Value too large for cache: {value_size/1024/1024:.1f}MB > {self.max_size_bytes/1024/1024:.1f}MB")
                return False
            
            # Remove existing entry if updating
            if key in self._cache:
                self._remove_entry(key)
            
            # Create new entry
            entry = CacheEntry(
                key=key,
                value=value,
                size_bytes=value_size,
                ttl_seconds=ttl_seconds if ttl_seconds is not None else (self.default_ttl_seconds if self.enable_ttl else None)
            )
            
            # Make room if necessary
            self._ensure_capacity(entry.size_bytes)
            
            # Add to cache
            self._cache[key] = entry
            self.stats.current_size_bytes += entry.size_bytes
            self.stats.current_entry_count += 1
            self.stats.total_entries_added += 1
            
            # Update max size reached
            if self.stats.current_size_bytes > self.stats.max_size_bytes_reached:
                self.stats.max_size_bytes_reached = self.stats.current_size_bytes
            
            self.logger.debug(f"Cached value for key: {key[:50]}... (size: {value_size/1024:.1f}KB)")
            
            return True
    
    def remove(self, key: str) -> bool:
        """Remove specific key from cache"""
        with self._lock:
            if key in self._cache:
                self._remove_entry(key)
                return True
            return False
    
    def clear(self):
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()
            self.stats.current_size_bytes = 0
            self.stats.current_entry_count = 0
            self.logger.info("Cache cleared")
    
    def _ensure_capacity(self, needed_bytes: int):
        """Ensure cache has capacity for new entry"""
        # Check memory pressure and adjust limits
        if self.enable_memory_monitoring:
            self._adapt_to_memory_pressure()
        
        # Remove expired entries first
        self._remove_expired_entries()
        
        # Check if we need to evict based on memory or count limits
        while (self.stats.current_size_bytes + needed_bytes > self.max_size_bytes or
               self.stats.current_entry_count >= self.max_entries):
            
            if not self._cache:
                break
            
            # Remove least recently used entry
            lru_key = next(iter(self._cache))
            self._remove_entry(lru_key)
            self.stats.evictions += 1
    
    def _remove_entry(self, key: str):
        """Remove entry and update statistics"""
        if key in self._cache:
            entry = self._cache[key]
            self.stats.current_size_bytes -= entry.size_bytes
            self.stats.current_entry_count -= 1
            del self._cache[key]
    
    def _remove_expired_entries(self):
        """Remove all expired entries"""
        expired_keys = []
        
        for key, entry in self._cache.items():
            if entry.is_expired():
                expired_keys.append(key)
        
        for key in expired_keys:
            self._remove_entry(key)
            self.stats.expired_evictions += 1
    
    def _calculate_size(self, obj: Any) -> int:
        """Calculate approximate size of object in bytes"""
        try:
            # Use sys.getsizeof as baseline
            size = sys.getsizeof(obj)
            
            # Add size of container contents for common types
            if isinstance(obj, dict):
                size += sum(sys.getsizeof(k) + self._calculate_size(v) for k, v in obj.items())
            elif isinstance(obj, (list, tuple)):
                size += sum(self._calculate_size(item) for item in obj)
            elif isinstance(obj, str):
                size = len(obj.encode('utf-8'))  # More accurate for strings
            
            return size
            
        except Exception:
            # Fallback to basic size estimation
            if isinstance(obj, str):
                return len(obj) * 2  # Rough estimate for Unicode
            elif isinstance(obj, (int, float)):
                return 8
            elif isinstance(obj, bool):
                return 1
            else:
                return 64  # Conservative estimate for complex objects
    
    def _start_memory_monitoring(self):
        """Start background memory monitoring thread"""
        if self._memory_monitoring_active:
            return
        
        self._memory_monitoring_active = True
        self._memory_monitor_thread = threading.Thread(
            target=self._memory_monitoring_loop,
            daemon=True
        )
        self._memory_monitor_thread.start()
        self.logger.info("Memory monitoring started")
    
    def _stop_memory_monitoring(self):
        """Stop background memory monitoring"""
        self._memory_monitoring_active = False
        if self._memory_monitor_thread:
            self._memory_monitor_thread.join(timeout=5)
        self.logger.info("Memory monitoring stopped")
    
    def _memory_monitoring_loop(self):
        """Background memory monitoring loop"""
        while self._memory_monitoring_active:
            try:
                self._update_memory_pressure()
                time.sleep(self.monitoring_interval)
            except Exception as e:
                self.logger.error(f"Error in memory monitoring: {e}")
                time.sleep(self.monitoring_interval)
    
    def _update_memory_pressure(self):
        """Update current memory pressure level"""
        try:
            memory = psutil.virtual_memory()
            memory_percent = memory.percent / 100.0
            
            # Determine pressure level
            if memory_percent < 0.6:
                pressure = MemoryPressureLevel.LOW
            elif memory_percent < 0.8:
                pressure = MemoryPressureLevel.MEDIUM
            elif memory_percent < 0.9:
                pressure = MemoryPressureLevel.HIGH
            else:
                pressure = MemoryPressureLevel.CRITICAL
            
            if pressure != self.current_memory_pressure:
                self.logger.info(f"Memory pressure changed: {self.current_memory_pressure.value} -> {pressure.value} ({memory_percent:.1%})")
                self.current_memory_pressure = pressure
                
                # Trigger adaptive resizing if pressure increased
                if pressure in [MemoryPressureLevel.HIGH, MemoryPressureLevel.CRITICAL]:
                    with self._lock:
                        self._adapt_to_memory_pressure()
                        
        except Exception as e:
            self.logger.error(f"Error updating memory pressure: {e}")
    
    def _adapt_to_memory_pressure(self):
        """Adapt cache size based on memory pressure"""
        if self.current_memory_pressure == MemoryPressureLevel.CRITICAL:
            # Critical: Reduce cache to 25% of max size
            target_size = self.max_size_bytes * 0.25
            self._evict_to_size(int(target_size))
            self.stats.memory_pressure_evictions += 1
            
        elif self.current_memory_pressure == MemoryPressureLevel.HIGH:
            # High: Reduce cache to 50% of max size
            target_size = self.max_size_bytes * 0.50
            self._evict_to_size(int(target_size))
            self.stats.memory_pressure_evictions += 1
            
        elif self.current_memory_pressure == MemoryPressureLevel.MEDIUM:
            # Medium: Reduce cache to 75% of max size
            target_size = self.max_size_bytes * 0.75
            self._evict_to_size(int(target_size))
    
    def _evict_to_size(self, target_size: int):
        """Evict entries until cache size is under target"""
        while self.stats.current_size_bytes > target_size and self._cache:
            # Remove least recently used entry
            lru_key = next(iter(self._cache))
            self._remove_entry(lru_key)
            self.stats.evictions += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        with self._lock:
            return {
                'hits': self.stats.hits,
                'misses': self.stats.misses,
                'hit_rate': self.stats.hit_rate,
                'evictions': self.stats.evictions,
                'memory_pressure_evictions': self.stats.memory_pressure_evictions,
                'expired_evictions': self.stats.expired_evictions,
                'eviction_rate': self.stats.eviction_rate,
                'current_size_bytes': self.stats.current_size_bytes,
                'current_size_mb': self.stats.current_size_bytes / 1024 / 1024,
                'max_size_bytes': self.max_size_bytes,
                'max_size_mb': self.max_size_bytes / 1024 / 1024,
                'size_utilization': self.stats.current_size_bytes / self.max_size_bytes,
                'current_entry_count': self.stats.current_entry_count,
                'max_entries': self.max_entries,
                'entry_utilization': self.stats.current_entry_count / self.max_entries,
                'max_size_bytes_reached': self.stats.max_size_bytes_reached,
                'memory_pressure_level': self.current_memory_pressure.value,
                'memory_monitoring_enabled': self.enable_memory_monitoring
            }
    
    def get_memory_info(self) -> Dict[str, Any]:
        """Get current memory information"""
        try:
            memory = psutil.virtual_memory()
            process = psutil.Process()
            process_memory = process.memory_info()
            
            return {
                'system_memory_total_mb': memory.total / 1024 / 1024,
                'system_memory_available_mb': memory.available / 1024 / 1024,
                'system_memory_percent': memory.percent,
                'process_memory_rss_mb': process_memory.rss / 1024 / 1024,
                'process_memory_vms_mb': process_memory.vms / 1024 / 1024,
                'cache_memory_mb': self.stats.current_size_bytes / 1024 / 1024,
                'cache_memory_percent_of_process': (self.stats.current_size_bytes / process_memory.rss) * 100,
                'memory_pressure_level': self.current_memory_pressure.value
            }
        except Exception as e:
            self.logger.error(f"Error getting memory info: {e}")
            return {}
    
    def optimize_for_environment(self, environment: str = "t3.micro"):
        """Optimize cache settings for specific environments"""
        if environment == "t3.micro":
            # AWS t3.micro: 1GB RAM, be very conservative
            self.max_size_bytes = 30 * 1024 * 1024  # 30MB max
            self.max_entries = 500
            self.memory_pressure_threshold = 0.7  # More aggressive
            self.default_ttl_seconds = 1800  # 30 minutes TTL
            self.logger.info("Cache optimized for AWS t3.micro environment")
            
        elif environment == "development":
            # Development: More generous limits
            self.max_size_bytes = 100 * 1024 * 1024  # 100MB max
            self.max_entries = 2000
            self.memory_pressure_threshold = 0.8
            self.default_ttl_seconds = 3600  # 1 hour TTL
            self.logger.info("Cache optimized for development environment")
            
        elif environment == "production":
            # Production: Balanced approach
            self.max_size_bytes = 64 * 1024 * 1024  # 64MB max
            self.max_entries = 1000
            self.memory_pressure_threshold = 0.75
            self.default_ttl_seconds = 2400  # 40 minutes TTL
            self.logger.info("Cache optimized for production environment")
    
    def cleanup(self):
        """Cleanup cache and stop monitoring"""
        self._stop_memory_monitoring()
        self.clear()
        self.logger.info("Cache cleanup completed")
    
    def __del__(self):
        """Destructor to ensure cleanup"""
        try:
            self.cleanup()
        except:
            pass


# Factory function for creating environment-optimized caches
def create_memory_aware_cache(cache_type: str = "finbert", 
                            environment: str = "production") -> MemoryAwareLRUCache:
    """
    Create a memory-aware cache optimized for specific use cases and environments
    
    Args:
        cache_type: Type of cache ("finbert", "news", "general")
        environment: Environment type ("t3.micro", "development", "production")
    
    Returns:
        Configured MemoryAwareLRUCache instance
    """
    
    if cache_type == "finbert":
        # FinBERT sentiment analysis cache
        if environment == "t3.micro":
            cache = MemoryAwareLRUCache(
                max_size_bytes=20 * 1024 * 1024,  # 20MB
                max_entries=300,
                memory_pressure_threshold=0.7,
                default_ttl_seconds=1800  # 30 minutes
            )
        else:
            cache = MemoryAwareLRUCache(
                max_size_bytes=50 * 1024 * 1024,  # 50MB
                max_entries=1000,
                memory_pressure_threshold=0.8,
                default_ttl_seconds=3600  # 1 hour
            )
    
    elif cache_type == "news":
        # News article cache
        cache = MemoryAwareLRUCache(
            max_size_bytes=30 * 1024 * 1024,  # 30MB
            max_entries=500,
            memory_pressure_threshold=0.8,
            default_ttl_seconds=7200  # 2 hours
        )
    
    else:
        # General purpose cache
        cache = MemoryAwareLRUCache()
    
    # Apply environment optimization
    cache.optimize_for_environment(environment)
    
    return cache


if __name__ == "__main__":
    # Example usage and testing
    print("Testing Memory-Aware LRU Cache")
    
    # Create cache optimized for t3.micro
    cache = create_memory_aware_cache("finbert", "t3.micro")
    
    # Test basic operations
    cache.put("test_key_1", "test_value_1")
    cache.put("test_key_2", {"sentiment": "positive", "confidence": 0.95})
    
    print(f"Value 1: {cache.get('test_key_1')}")
    print(f"Value 2: {cache.get('test_key_2')}")
    print(f"Missing: {cache.get('missing_key', 'default')}")
    
    # Show statistics
    stats = cache.get_statistics()
    print(f"Cache stats: {stats}")
    
    # Show memory info
    memory_info = cache.get_memory_info()
    print(f"Memory info: {memory_info}")
    
    # Cleanup
    cache.cleanup()