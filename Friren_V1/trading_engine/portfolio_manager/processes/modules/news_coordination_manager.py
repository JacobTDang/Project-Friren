"""
News Coordination Manager
========================

Ensures both general news collection and symbol-specific news collection
work in harmony without queue conflicts or message collision.

Features:
- Message priority management for different news types
- Queue coordination to prevent conflicts
- Deduplication across news collection types
- Proper sequencing of news pipeline stages
"""

import uuid
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
from enum import Enum
import logging
from collections import defaultdict

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    MessagePriority, ProcessMessage, create_process_message
)


class NewsCollectionType(Enum):
    """Types of news collection"""
    GENERAL = "general"          # General news scanning for symbol mentions
    SYMBOL_SPECIFIC = "symbol"   # Direct symbol-targeted collection


@dataclass
class NewsCollectionTask:
    """Individual news collection task"""
    task_id: str
    collection_type: NewsCollectionType
    symbols: List[str]
    priority: MessagePriority
    timestamp: datetime
    estimated_duration_seconds: float = 30.0
    status: str = "pending"  # pending, running, completed, failed


@dataclass
class NewsCoordinationState:
    """Current state of news collection coordination"""
    active_tasks: Dict[str, NewsCollectionTask] = field(default_factory=dict)
    symbol_locks: Set[str] = field(default_factory=set)  # Symbols being processed
    collection_sequence: int = 0
    last_general_collection: Optional[datetime] = None
    last_symbol_collection: Dict[str, datetime] = field(default_factory=dict)


class NewsCoordinationManager:
    """
    Coordinates multiple news collection processes to ensure harmony
    """
    
    def __init__(self, redis_manager=None, process_id: str = "news_coordinator"):
        self.redis_manager = redis_manager
        self.process_id = process_id
        self.logger = logging.getLogger(__name__)
        
        # Coordination state
        self.state = NewsCoordinationState()
        self.coordination_lock = threading.Lock()
        
        # Configuration
        self.max_concurrent_tasks = 2  # Limit concurrent news collection
        self.symbol_cooldown_minutes = 5  # Wait between symbol-specific collections
        self.general_cooldown_minutes = 10  # Wait between general collections
        
        # Message queues for different news types
        self.news_queues = {
            NewsCollectionType.GENERAL: "trading_system:news_general:queue",
            NewsCollectionType.SYMBOL_SPECIFIC: "trading_system:news_symbols:queue"
        }
        
        # Priority mapping
        self.priority_mapping = {
            NewsCollectionType.GENERAL: MessagePriority.NORMAL,
            NewsCollectionType.SYMBOL_SPECIFIC: MessagePriority.HIGH
        }
    
    def coordinate_news_collection(self, collection_type: NewsCollectionType, 
                                 symbols: List[str]) -> Optional[str]:
        """
        Coordinate news collection request to prevent conflicts
        
        Args:
            collection_type: Type of news collection
            symbols: Symbols to collect news for
            
        Returns:
            Task ID if coordination successful, None if rejected
        """
        with self.coordination_lock:
            try:
                # Check if we can proceed with this collection
                if not self._can_start_collection(collection_type, symbols):
                    self.logger.debug(f"News collection coordination rejected: {collection_type.value} for {symbols}")
                    return None
                
                # Create coordination task
                task_id = f"news_{collection_type.value}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
                task = NewsCollectionTask(
                    task_id=task_id,
                    collection_type=collection_type,
                    symbols=symbols,
                    priority=self.priority_mapping[collection_type],
                    timestamp=datetime.now()
                )
                
                # Reserve resources
                self._reserve_resources(task)
                
                # Send coordinated message
                self._send_coordinated_news_message(task)
                
                self.logger.info(f"News collection coordinated: {task_id} ({collection_type.value}) for {symbols}")
                return task_id
                
            except Exception as e:
                self.logger.error(f"Error coordinating news collection: {e}")
                return None
    
    def _can_start_collection(self, collection_type: NewsCollectionType, 
                            symbols: List[str]) -> bool:
        """
        Check if news collection can start without conflicts
        
        Args:
            collection_type: Type of collection
            symbols: Symbols to collect for
            
        Returns:
            True if collection can proceed
        """
        # Check concurrent task limit
        active_count = len([t for t in self.state.active_tasks.values() if t.status == "running"])
        if active_count >= self.max_concurrent_tasks:
            return False
        
        # Check symbol locks (prevent duplicate symbol processing)
        if collection_type == NewsCollectionType.SYMBOL_SPECIFIC:
            for symbol in symbols:
                if symbol in self.state.symbol_locks:
                    return False
        
        # Check cooldown periods
        if collection_type == NewsCollectionType.GENERAL:
            if (self.state.last_general_collection and 
                datetime.now() - self.state.last_general_collection < timedelta(minutes=self.general_cooldown_minutes)):
                return False
        
        elif collection_type == NewsCollectionType.SYMBOL_SPECIFIC:
            for symbol in symbols:
                last_collection = self.state.last_symbol_collection.get(symbol)
                if (last_collection and 
                    datetime.now() - last_collection < timedelta(minutes=self.symbol_cooldown_minutes)):
                    return False
        
        return True
    
    def _reserve_resources(self, task: NewsCollectionTask):
        """
        Reserve coordination resources for the task
        
        Args:
            task: News collection task
        """
        # Add to active tasks
        self.state.active_tasks[task.task_id] = task
        
        # Lock symbols if symbol-specific
        if task.collection_type == NewsCollectionType.SYMBOL_SPECIFIC:
            for symbol in task.symbols:
                self.state.symbol_locks.add(symbol)
        
        # Update last collection times
        if task.collection_type == NewsCollectionType.GENERAL:
            self.state.last_general_collection = task.timestamp
        elif task.collection_type == NewsCollectionType.SYMBOL_SPECIFIC:
            for symbol in task.symbols:
                self.state.last_symbol_collection[symbol] = task.timestamp
    
    def _send_coordinated_news_message(self, task: NewsCollectionTask):
        """
        Send coordinated news collection message with proper priority
        
        Args:
            task: News collection task
        """
        if not self.redis_manager:
            return
        
        try:
            # Create message with coordination metadata
            message_data = {
                "task_id": task.task_id,
                "collection_type": task.collection_type.value,
                "symbols": task.symbols,
                "sequence": self.state.collection_sequence,
                "coordination_timestamp": task.timestamp.isoformat(),
                "estimated_duration": task.estimated_duration_seconds
            }
            
            # Create process message with appropriate priority
            message = create_process_message(
                sender=self.process_id,
                recipient="enhanced_news_pipeline",
                message_type="coordinated_news_collection",
                data=message_data,
                priority=task.priority
            )
            
            # Send to appropriate queue
            queue_name = self.news_queues[task.collection_type]
            success = self.redis_manager.send_message(message, queue_name)
            
            if success:
                self.state.collection_sequence += 1
                self.logger.debug(f"Coordinated news message sent: {task.task_id}")
            else:
                self.logger.error(f"Failed to send coordinated news message: {task.task_id}")
                
        except Exception as e:
            self.logger.error(f"Error sending coordinated news message: {e}")
    
    def complete_news_collection(self, task_id: str, success: bool = True):
        """
        Mark news collection task as complete and release resources
        
        Args:
            task_id: Task identifier
            success: Whether collection was successful
        """
        with self.coordination_lock:
            try:
                task = self.state.active_tasks.get(task_id)
                if not task:
                    return
                
                # Release symbol locks
                if task.collection_type == NewsCollectionType.SYMBOL_SPECIFIC:
                    for symbol in task.symbols:
                        self.state.symbol_locks.discard(symbol)
                
                # Update task status
                task.status = "completed" if success else "failed"
                
                # Remove from active tasks after brief delay (for monitoring)
                threading.Timer(30.0, lambda: self.state.active_tasks.pop(task_id, None)).start()
                
                self.logger.info(f"News collection completed: {task_id} (success: {success})")
                
            except Exception as e:
                self.logger.error(f"Error completing news collection: {e}")
    
    def get_coordination_status(self) -> Dict[str, Any]:
        """
        Get current coordination status
        
        Returns:
            Dictionary with coordination state information
        """
        with self.coordination_lock:
            return {
                "active_tasks": len(self.state.active_tasks),
                "locked_symbols": list(self.state.symbol_locks),
                "sequence": self.state.collection_sequence,
                "last_general": self.state.last_general_collection.isoformat() if self.state.last_general_collection else None,
                "last_symbol_collections": {
                    symbol: timestamp.isoformat() 
                    for symbol, timestamp in self.state.last_symbol_collection.items()
                }
            }
    
    def cleanup_stale_tasks(self):
        """
        Clean up stale or expired coordination tasks
        """
        with self.coordination_lock:
            try:
                current_time = datetime.now()
                stale_tasks = []
                
                for task_id, task in self.state.active_tasks.items():
                    # Tasks older than 5 minutes are considered stale
                    if current_time - task.timestamp > timedelta(minutes=5):
                        stale_tasks.append(task_id)
                
                # Clean up stale tasks
                for task_id in stale_tasks:
                    self.complete_news_collection(task_id, success=False)
                    self.logger.warning(f"Cleaned up stale news collection task: {task_id}")
                
            except Exception as e:
                self.logger.error(f"Error cleaning up stale tasks: {e}")


# Global coordination manager instance
_coordination_manager: Optional[NewsCoordinationManager] = None


def get_news_coordinator(redis_manager=None) -> NewsCoordinationManager:
    """
    Get or create global news coordination manager
    
    Args:
        redis_manager: Redis manager instance
        
    Returns:
        NewsCoordinationManager instance
    """
    global _coordination_manager
    
    if _coordination_manager is None:
        _coordination_manager = NewsCoordinationManager(redis_manager)
    
    return _coordination_manager


def coordinate_general_news_collection(symbols: List[str], redis_manager=None) -> Optional[str]:
    """
    Coordinate general news collection
    
    Args:
        symbols: Symbols that might be mentioned in general news
        redis_manager: Redis manager instance
        
    Returns:
        Task ID if successful
    """
    coordinator = get_news_coordinator(redis_manager)
    return coordinator.coordinate_news_collection(NewsCollectionType.GENERAL, symbols)


def coordinate_symbol_news_collection(symbols: List[str], redis_manager=None) -> Optional[str]:
    """
    Coordinate symbol-specific news collection
    
    Args:
        symbols: Symbols to collect news for specifically
        redis_manager: Redis manager instance
        
    Returns:
        Task ID if successful
    """
    coordinator = get_news_coordinator(redis_manager)
    return coordinator.coordinate_news_collection(NewsCollectionType.SYMBOL_SPECIFIC, symbols)


def complete_news_task(task_id: str, success: bool = True):
    """
    Mark news collection task as complete
    
    Args:
        task_id: Task identifier
        success: Whether task was successful
    """
    coordinator = get_news_coordinator()
    coordinator.complete_news_collection(task_id, success)