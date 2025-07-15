"""
Memory-Aware Queue Rotation Manager
==================================

Manages Redis queue rotation and memory pressure to stay within 1GB system limit.
Implements intelligent queue cleanup, priority-based retention, and memory optimization.

Features:
- Real-time memory monitoring
- Priority-based message retention
- Automatic queue rotation when memory pressure detected
- Emergency cleanup mechanisms
- Memory optimization strategies
"""

import time
import threading
import psutil
import gc
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from collections import deque, defaultdict

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    MessagePriority, get_trading_redis_manager
)


class MemoryPressureLevel(Enum):
    """Memory pressure severity levels"""
    LOW = "low"           # < 70% memory usage
    MODERATE = "moderate" # 70-80% memory usage
    HIGH = "high"         # 80-90% memory usage
    CRITICAL = "critical" # > 90% memory usage
    EMERGENCY = "emergency" # > 95% memory usage


@dataclass
class QueueMetrics:
    """Metrics for a specific queue"""
    name: str
    size: int
    memory_usage_bytes: int
    oldest_message_age: float
    newest_message_age: float
    priority_distribution: Dict[str, int] = field(default_factory=dict)
    message_types: Dict[str, int] = field(default_factory=dict)
    last_cleanup: Optional[datetime] = None
    cleanup_count: int = 0


@dataclass
class MemorySnapshot:
    """System memory snapshot"""
    timestamp: datetime
    total_memory_mb: int
    used_memory_mb: int
    available_memory_mb: int
    redis_memory_mb: int
    process_memory_mb: int
    memory_pressure: MemoryPressureLevel
    queue_metrics: Dict[str, QueueMetrics] = field(default_factory=dict)


class MemoryAwareQueueRotation:
    """
    Manages Redis queue rotation with memory awareness
    Ensures system stays within 1GB memory limit through intelligent cleanup
    """
    
    def __init__(self, redis_manager=None):
        self.redis_manager = redis_manager or get_trading_redis_manager()
        self.logger = logging.getLogger(__name__)
        
        # Load configuration from config manager
        from .coordination_config import get_coordination_config
        self.config_manager = get_coordination_config()
        memory_config = self.config_manager.memory_config
        
        # Memory configuration from config manager
        self.target_memory_mb = memory_config.target_memory_mb
        self.redis_memory_limit_mb = int(self.target_memory_mb * memory_config.redis_memory_ratio)
        self.emergency_threshold_mb = int(self.target_memory_mb * memory_config.emergency_threshold_ratio)
        
        # Memory pressure thresholds from configuration
        self.pressure_thresholds = {
            MemoryPressureLevel.LOW: memory_config.pressure_thresholds.get('low', 0.70),
            MemoryPressureLevel.MODERATE: memory_config.pressure_thresholds.get('moderate', 0.80),
            MemoryPressureLevel.HIGH: memory_config.pressure_thresholds.get('high', 0.90),
            MemoryPressureLevel.CRITICAL: memory_config.pressure_thresholds.get('critical', 0.95),
            MemoryPressureLevel.EMERGENCY: memory_config.pressure_thresholds.get('emergency', 0.98)
        }
        
        # Queue management
        self.queue_rotation_lock = threading.RLock()
        self.memory_snapshots = deque(maxlen=60)  # Keep 60 snapshots (1 minute at 1s intervals)
        
        # Cleanup strategies
        self.cleanup_strategies = {
            MemoryPressureLevel.LOW: self._cleanup_expired_messages,
            MemoryPressureLevel.MODERATE: self._cleanup_low_priority_messages,
            MemoryPressureLevel.HIGH: self._cleanup_old_messages,
            MemoryPressureLevel.CRITICAL: self._emergency_cleanup,
            MemoryPressureLevel.EMERGENCY: self._panic_cleanup
        }
        
        # Load retention policies from configuration  
        self.retention_policies = self._load_retention_policies()
        
        # Load message importance from configuration
        self.message_importance = self._load_message_importance()
        
        # Background monitoring
        self.monitoring_active = False
        self.monitoring_thread = None
        self.last_cleanup_time = {}
        
        self.logger.info(f"MemoryAwareQueueRotation initialized - Target: {self.target_memory_mb}MB from configuration")
    
    def _load_retention_policies(self) -> Dict[MessagePriority, timedelta]:
        """Load message retention policies from configuration"""
        try:
            # Get base retention times from environment or use defaults
            import os
            
            critical_minutes = int(os.getenv('FRIREN_RETENTION_CRITICAL_MIN', '30'))
            high_minutes = int(os.getenv('FRIREN_RETENTION_HIGH_MIN', '15'))
            normal_minutes = int(os.getenv('FRIREN_RETENTION_NORMAL_MIN', '5'))
            low_minutes = int(os.getenv('FRIREN_RETENTION_LOW_MIN', '2'))
            
            return {
                MessagePriority.CRITICAL: timedelta(minutes=critical_minutes),
                MessagePriority.HIGH: timedelta(minutes=high_minutes),
                MessagePriority.NORMAL: timedelta(minutes=normal_minutes),
                MessagePriority.LOW: timedelta(minutes=low_minutes)
            }
            
        except Exception as e:
            self.logger.error(f"Error loading retention policies: {e}")
            # Emergency fallback
            return {
                MessagePriority.CRITICAL: timedelta(minutes=30),
                MessagePriority.HIGH: timedelta(minutes=15),
                MessagePriority.NORMAL: timedelta(minutes=5),
                MessagePriority.LOW: timedelta(minutes=2)
            }
    
    def _load_message_importance(self) -> Dict[str, int]:
        """Load message importance scores from configuration"""
        try:
            message_config = self.config_manager.message_config
            
            # Convert priority scores to importance rankings
            priority_scores = message_config.priority_scores
            
            # Sort by score and assign importance ranks
            sorted_messages = sorted(priority_scores.items(), key=lambda x: x[1], reverse=True)
            importance = {}
            
            for rank, (message_type, score) in enumerate(sorted_messages, 1):
                importance[message_type] = rank
            
            self.logger.info(f"Loaded message importance rankings from configuration")
            return importance
            
        except Exception as e:
            self.logger.error(f"Error loading message importance: {e}")
            # Emergency fallback with minimal importance mapping
            return {
                'execution_order': 1,
                'risk_assessment': 2,
                'trading_signal': 3,
                'position_update': 4
            }
    
    def start_monitoring(self):
        """Start background memory monitoring"""
        if not self.monitoring_active:
            self.monitoring_active = True
            self.monitoring_thread = threading.Thread(
                target=self._monitoring_worker,
                daemon=True
            )
            self.monitoring_thread.start()
            self.logger.info("Memory monitoring started")
    
    def stop_monitoring(self):
        """Stop background memory monitoring"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
            self.logger.info("Memory monitoring stopped")
    
    def _monitoring_worker(self):
        """Background worker for memory monitoring"""
        while self.monitoring_active:
            try:
                # Take memory snapshot
                snapshot = self._take_memory_snapshot()
                self.memory_snapshots.append(snapshot)
                
                # CRITICAL: MEMORY RESTRICTIONS DISABLED - User has unlimited compute resources
                # Memory pressure system disabled per user requirements
                # Only log for monitoring, no cleanup actions
                if snapshot.memory_pressure != MemoryPressureLevel.LOW:
                    self.logger.debug(f"Memory pressure {snapshot.memory_pressure.value} detected but cleanup disabled - unlimited compute mode")
                
                # Sleep before next check
                time.sleep(1.0)  # Check every second
                
            except Exception as e:
                self.logger.error(f"Error in memory monitoring worker: {e}")
                time.sleep(5.0)  # Wait before retrying
    
    def _take_memory_snapshot(self) -> MemorySnapshot:
        """Take a comprehensive memory snapshot"""
        try:
            # Get system memory info
            memory_info = psutil.virtual_memory()
            total_memory_mb = memory_info.total // (1024 * 1024)
            used_memory_mb = memory_info.used // (1024 * 1024)
            available_memory_mb = memory_info.available // (1024 * 1024)
            
            # Get Redis memory info
            redis_memory_mb = self._get_redis_memory_usage()
            
            # Get process memory info
            process_memory_mb = self._get_process_memory_usage()
            
            # Determine memory pressure level
            memory_usage_ratio = used_memory_mb / total_memory_mb
            pressure_level = self._determine_pressure_level(memory_usage_ratio)
            
            # Get queue metrics
            queue_metrics = self._collect_queue_metrics()
            
            return MemorySnapshot(
                timestamp=datetime.now(),
                total_memory_mb=total_memory_mb,
                used_memory_mb=used_memory_mb,
                available_memory_mb=available_memory_mb,
                redis_memory_mb=redis_memory_mb,
                process_memory_mb=process_memory_mb,
                memory_pressure=pressure_level,
                queue_metrics=queue_metrics
            )
            
        except Exception as e:
            self.logger.error(f"Error taking memory snapshot: {e}")
            # Return emergency snapshot using configured values
            return MemorySnapshot(
                timestamp=datetime.now(),
                total_memory_mb=self.target_memory_mb,
                used_memory_mb=self.target_memory_mb,
                available_memory_mb=0,
                redis_memory_mb=self.redis_memory_limit_mb,
                process_memory_mb=self.target_memory_mb - self.redis_memory_limit_mb,
                memory_pressure=MemoryPressureLevel.EMERGENCY
            )
    
    def _get_redis_memory_usage(self) -> int:
        """Get Redis memory usage in MB"""
        try:
            redis_info = self.redis_manager.redis_client.info('memory')
            used_memory = redis_info.get('used_memory', 0)
            return used_memory // (1024 * 1024)
        except Exception as e:
            self.logger.error(f"Error getting Redis memory usage: {e}")
            return 0
    
    def _get_process_memory_usage(self) -> int:
        """Get current process memory usage in MB"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            return memory_info.rss // (1024 * 1024)
        except Exception as e:
            self.logger.error(f"Error getting process memory usage: {e}")
            return 0
    
    def _determine_pressure_level(self, usage_ratio: float) -> MemoryPressureLevel:
        """Determine memory pressure level based on usage ratio"""
        if usage_ratio >= self.pressure_thresholds[MemoryPressureLevel.EMERGENCY]:
            return MemoryPressureLevel.EMERGENCY
        elif usage_ratio >= self.pressure_thresholds[MemoryPressureLevel.CRITICAL]:
            return MemoryPressureLevel.CRITICAL
        elif usage_ratio >= self.pressure_thresholds[MemoryPressureLevel.HIGH]:
            return MemoryPressureLevel.HIGH
        elif usage_ratio >= self.pressure_thresholds[MemoryPressureLevel.MODERATE]:
            return MemoryPressureLevel.MODERATE
        else:
            return MemoryPressureLevel.LOW
    
    def _collect_queue_metrics(self) -> Dict[str, QueueMetrics]:
        """Collect metrics for all queues"""
        try:
            queue_metrics = {}
            
            # Get all queue keys
            queue_keys = self.redis_manager.redis_client.keys(f"{self.redis_manager.QUEUE_PREFIX}:*")
            
            for queue_key in queue_keys:
                try:
                    queue_name = queue_key.split(':')[-1]
                    
                    # Get queue size
                    size = self.redis_manager.redis_client.llen(queue_key)
                    
                    # Estimate memory usage (approximate)
                    memory_usage = self._estimate_queue_memory_usage(queue_key, size)
                    
                    # Get message age info
                    oldest_age, newest_age = self._get_message_ages(queue_key)
                    
                    # Get priority and type distributions
                    priority_dist, type_dist = self._get_message_distributions(queue_key)
                    
                    queue_metrics[queue_name] = QueueMetrics(
                        name=queue_name,
                        size=size,
                        memory_usage_bytes=memory_usage,
                        oldest_message_age=oldest_age,
                        newest_message_age=newest_age,
                        priority_distribution=priority_dist,
                        message_types=type_dist
                    )
                    
                except Exception as e:
                    self.logger.error(f"Error collecting metrics for queue {queue_key}: {e}")
                    continue
            
            return queue_metrics
            
        except Exception as e:
            self.logger.error(f"Error collecting queue metrics: {e}")
            return {}
    
    def _estimate_queue_memory_usage(self, queue_key: str, size: int) -> int:
        """Estimate memory usage for a queue"""
        if size == 0:
            return 0
        
        try:
            # Sample a few messages to estimate average size
            sample_size = min(5, size)
            messages = self.redis_manager.redis_client.lrange(queue_key, 0, sample_size - 1)
            
            if not messages:
                return 0
            
            # Calculate average message size
            total_size = sum(len(msg.encode('utf-8')) for msg in messages)
            avg_message_size = total_size / len(messages)
            
            # Estimate total queue memory usage
            estimated_usage = int(avg_message_size * size * 1.2)  # 20% overhead
            
            return estimated_usage
            
        except Exception as e:
            self.logger.error(f"Error estimating queue memory usage: {e}")
            # Use configurable fallback estimate
            fallback_bytes_per_message = int(os.getenv('FRIREN_FALLBACK_MESSAGE_SIZE', '500'))
            return size * fallback_bytes_per_message
    
    def _get_message_ages(self, queue_key: str) -> Tuple[float, float]:
        """Get oldest and newest message ages in seconds"""
        try:
            # Get oldest message (from tail)
            oldest_messages = self.redis_manager.redis_client.lrange(queue_key, -1, -1)
            # Get newest message (from head)  
            newest_messages = self.redis_manager.redis_client.lrange(queue_key, 0, 0)
            
            current_time = time.time()
            oldest_age = newest_age = 0.0
            
            if oldest_messages:
                oldest_msg = oldest_messages[0]
                oldest_age = self._extract_message_age(oldest_msg, current_time)
            
            if newest_messages:
                newest_msg = newest_messages[0]
                newest_age = self._extract_message_age(newest_msg, current_time)
            
            return oldest_age, newest_age
            
        except Exception as e:
            self.logger.error(f"Error getting message ages: {e}")
            return 0.0, 0.0
    
    def _extract_message_age(self, message_json: str, current_time: float) -> float:
        """Extract age of a message from JSON"""
        try:
            import json
            message_data = json.loads(message_json)
            timestamp_str = message_data.get('timestamp', '')
            
            if timestamp_str:
                message_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                message_timestamp = message_time.timestamp()
                return current_time - message_timestamp
            
            return 0.0
            
        except Exception as e:
            self.logger.debug(f"Error extracting message age: {e}")
            return 0.0
    
    def _get_message_distributions(self, queue_key: str) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Get priority and message type distributions"""
        try:
            import json
            
            # Sample messages to analyze distribution
            sample_size = min(20, self.redis_manager.redis_client.llen(queue_key))
            messages = self.redis_manager.redis_client.lrange(queue_key, 0, sample_size - 1)
            
            priority_dist = defaultdict(int)
            type_dist = defaultdict(int)
            
            for message_json in messages:
                try:
                    message_data = json.loads(message_json)
                    
                    # Count priority
                    priority = message_data.get('priority', 2)  # Default to NORMAL
                    priority_name = MessagePriority(priority).name
                    priority_dist[priority_name] += 1
                    
                    # Count message type
                    msg_type = message_data.get('message_type', 'unknown')
                    type_dist[msg_type] += 1
                    
                except (json.JSONDecodeError, ValueError):
                    continue
            
            return dict(priority_dist), dict(type_dist)
            
        except Exception as e:
            self.logger.error(f"Error getting message distributions: {e}")
            return {}, {}
    
    def _handle_memory_pressure(self, snapshot: MemorySnapshot):
        """Handle memory pressure by triggering appropriate cleanup"""
        with self.queue_rotation_lock:
            try:
                pressure_level = snapshot.memory_pressure
                
                self.logger.warning(f"Memory pressure detected: {pressure_level.value} "
                                  f"({snapshot.used_memory_mb}MB/{snapshot.total_memory_mb}MB)")
                
                # Execute cleanup strategy
                cleanup_strategy = self.cleanup_strategies.get(pressure_level)
                if cleanup_strategy:
                    cleanup_strategy(snapshot)
                
                # Force garbage collection for high pressure
                if pressure_level in [MemoryPressureLevel.HIGH, MemoryPressureLevel.CRITICAL, MemoryPressureLevel.EMERGENCY]:
                    gc.collect()
                
            except Exception as e:
                self.logger.error(f"Error handling memory pressure: {e}")
    
    def _cleanup_expired_messages(self, snapshot: MemorySnapshot):
        """Clean up expired messages (LOW pressure)"""
        try:
            # This is handled by Redis manager's cleanup thread
            # Just log that we're in low pressure mode
            self.logger.debug("Low memory pressure - standard cleanup active")
            
        except Exception as e:
            self.logger.error(f"Error in expired message cleanup: {e}")
    
    def _cleanup_low_priority_messages(self, snapshot: MemorySnapshot):
        """Clean up low priority messages (MODERATE pressure)"""
        try:
            queue_keys = self.redis_manager.redis_client.keys(f"{self.redis_manager.QUEUE_PREFIX}:*")
            cleanup_count = 0
            
            for queue_key in queue_keys:
                cleanup_count += self._cleanup_queue_by_priority(queue_key, [MessagePriority.LOW])
            
            self.logger.info(f"Moderate pressure cleanup: removed {cleanup_count} low priority messages")
            
        except Exception as e:
            self.logger.error(f"Error in low priority cleanup: {e}")
    
    def _cleanup_old_messages(self, snapshot: MemorySnapshot):
        """Clean up old messages (HIGH pressure)"""
        try:
            queue_keys = self.redis_manager.redis_client.keys(f"{self.redis_manager.QUEUE_PREFIX}:*")
            cleanup_count = 0
            
            # Clean up messages older than retention policies
            for queue_key in queue_keys:
                cleanup_count += self._cleanup_queue_by_age(queue_key, aggressive=True)
            
            self.logger.warning(f"High pressure cleanup: removed {cleanup_count} old messages")
            
        except Exception as e:
            self.logger.error(f"Error in old message cleanup: {e}")
    
    def _emergency_cleanup(self, snapshot: MemorySnapshot):
        """Emergency cleanup (CRITICAL pressure)"""
        try:
            # Clean up both low priority and normal priority messages
            queue_keys = self.redis_manager.redis_client.keys(f"{self.redis_manager.QUEUE_PREFIX}:*")
            cleanup_count = 0
            
            for queue_key in queue_keys:
                cleanup_count += self._cleanup_queue_by_priority(
                    queue_key, [MessagePriority.LOW, MessagePriority.NORMAL]
                )
            
            self.logger.critical(f"Emergency cleanup: removed {cleanup_count} messages")
            
        except Exception as e:
            self.logger.error(f"Error in emergency cleanup: {e}")
    
    def _panic_cleanup(self, snapshot: MemorySnapshot):
        """Panic cleanup - preserve only critical messages (EMERGENCY pressure)"""
        try:
            queue_keys = self.redis_manager.redis_client.keys(f"{self.redis_manager.QUEUE_PREFIX}:*")
            cleanup_count = 0
            
            for queue_key in queue_keys:
                # Keep only CRITICAL messages
                cleanup_count += self._cleanup_queue_keep_only_critical(queue_key)
            
            self.logger.critical(f"PANIC CLEANUP: removed {cleanup_count} messages, keeping only CRITICAL")
            
        except Exception as e:
            self.logger.error(f"Error in panic cleanup: {e}")
    
    def _cleanup_queue_by_priority(self, queue_key: str, priorities_to_remove: List[MessagePriority]) -> int:
        """Remove messages of specific priorities from queue"""
        try:
            import json
            
            # Get all messages
            messages = self.redis_manager.redis_client.lrange(queue_key, 0, -1)
            removed_count = 0
            
            for message_json in messages:
                try:
                    message_data = json.loads(message_json)
                    priority = MessagePriority(message_data.get('priority', 2))
                    
                    if priority in priorities_to_remove:
                        # Remove this message
                        self.redis_manager.redis_client.lrem(queue_key, 1, message_json)
                        removed_count += 1
                        
                except (json.JSONDecodeError, ValueError):
                    continue
            
            return removed_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning queue by priority: {e}")
            return 0
    
    def _cleanup_queue_by_age(self, queue_key: str, aggressive: bool = False) -> int:
        """Remove messages older than retention policies"""
        try:
            import json
            
            current_time = datetime.now()
            messages = self.redis_manager.redis_client.lrange(queue_key, 0, -1)
            removed_count = 0
            
            for message_json in messages:
                try:
                    message_data = json.loads(message_json)
                    priority = MessagePriority(message_data.get('priority', 2))
                    timestamp_str = message_data.get('timestamp', '')
                    
                    if timestamp_str:
                        message_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        age = current_time - message_time
                        
                        # Get retention policy for this priority
                        retention_limit = self.retention_policies.get(priority, timedelta(minutes=5))
                        
                        # In aggressive mode, reduce retention by 50%
                        if aggressive:
                            retention_limit = retention_limit / 2
                        
                        if age > retention_limit:
                            # Remove this message
                            self.redis_manager.redis_client.lrem(queue_key, 1, message_json)
                            removed_count += 1
                            
                except (json.JSONDecodeError, ValueError):
                    continue
            
            return removed_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning queue by age: {e}")
            return 0
    
    def _cleanup_queue_keep_only_critical(self, queue_key: str) -> int:
        """Keep only CRITICAL priority messages"""
        try:
            import json
            
            # Get all messages
            messages = self.redis_manager.redis_client.lrange(queue_key, 0, -1)
            critical_messages = []
            removed_count = 0
            
            for message_json in messages:
                try:
                    message_data = json.loads(message_json)
                    priority = MessagePriority(message_data.get('priority', 2))
                    
                    if priority == MessagePriority.CRITICAL:
                        critical_messages.append(message_json)
                    else:
                        removed_count += 1
                        
                except (json.JSONDecodeError, ValueError):
                    continue
            
            # Replace queue with only critical messages
            pipe = self.redis_manager.redis_client.pipeline()
            pipe.delete(queue_key)
            if critical_messages:
                pipe.lpush(queue_key, *critical_messages)
            pipe.execute()
            
            return removed_count
            
        except Exception as e:
            self.logger.error(f"Error keeping only critical messages: {e}")
            return 0
    
    def get_memory_status(self) -> Dict[str, Any]:
        """Get current memory status"""
        try:
            if not self.memory_snapshots:
                return {"status": "no_data"}
            
            latest_snapshot = self.memory_snapshots[-1]
            
            return {
                "timestamp": latest_snapshot.timestamp.isoformat(),
                "memory_pressure": latest_snapshot.memory_pressure.value,
                "memory_usage": {
                    "total_mb": latest_snapshot.total_memory_mb,
                    "used_mb": latest_snapshot.used_memory_mb,
                    "available_mb": latest_snapshot.available_memory_mb,
                    "usage_percent": (latest_snapshot.used_memory_mb / latest_snapshot.total_memory_mb) * 100
                },
                "redis_memory_mb": latest_snapshot.redis_memory_mb,
                "process_memory_mb": latest_snapshot.process_memory_mb,
                "queue_count": len(latest_snapshot.queue_metrics),
                "total_queue_messages": sum(q.size for q in latest_snapshot.queue_metrics.values())
            }
            
        except Exception as e:
            self.logger.error(f"Error getting memory status: {e}")
            return {"status": "error", "error": str(e)}
    
    def force_cleanup(self, pressure_level: MemoryPressureLevel = MemoryPressureLevel.HIGH):
        """Force cleanup at specified pressure level"""
        try:
            # Create snapshot with configured values for forced cleanup
            used_memory_mb = int(self.target_memory_mb * 0.9)  # Simulate 90% usage
            available_memory_mb = self.target_memory_mb - used_memory_mb
            
            force_snapshot = MemorySnapshot(
                timestamp=datetime.now(),
                total_memory_mb=self.target_memory_mb,
                used_memory_mb=used_memory_mb,
                available_memory_mb=available_memory_mb,
                redis_memory_mb=self.redis_memory_limit_mb,
                process_memory_mb=self.target_memory_mb - self.redis_memory_limit_mb,
                memory_pressure=pressure_level
            )
            
            self._handle_memory_pressure(force_snapshot)
            self.logger.info(f"Force cleanup completed at {pressure_level.value} level")
            
        except Exception as e:
            self.logger.error(f"Error in force cleanup: {e}")


# Global instance
_memory_rotation_manager: Optional[MemoryAwareQueueRotation] = None


def get_memory_rotation_manager(redis_manager=None) -> MemoryAwareQueueRotation:
    """Get or create global memory rotation manager"""
    global _memory_rotation_manager
    
    if _memory_rotation_manager is None:
        _memory_rotation_manager = MemoryAwareQueueRotation(redis_manager)
    
    return _memory_rotation_manager


def start_memory_monitoring():
    """Start memory monitoring (convenience function)"""
    manager = get_memory_rotation_manager()
    manager.start_monitoring()


def get_memory_status() -> Dict[str, Any]:
    """Get current memory status (convenience function)"""
    manager = get_memory_rotation_manager()
    return manager.get_memory_status()