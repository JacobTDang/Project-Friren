#!/usr/bin/env python3
"""
news_scheduler.py

Smart News Collection Scheduler

This scheduler implements intelligent news collection:
1. Scheduled intervals (15-30 minutes) instead of continuous running
2. Market hours awareness + buffer time
3. Memory threshold integration
4. Efficient resource utilization
"""

import time
import threading
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, time as dt_time
from enum import Enum
from dataclasses import dataclass

# Trading hours and scheduler logic
from .trading_redis_manager import get_trading_redis_manager, create_process_message, MessagePriority

class NewsScheduleMode(Enum):
    """News collection scheduling modes"""
    DISABLED = "disabled"           # News collection disabled
    SCHEDULED = "scheduled"         # Normal scheduled collection
    MARKET_HOURS = "market_hours"   # Only during market hours + buffer
    EMERGENCY_PAUSE = "emergency_pause"  # Paused due to memory/system issues

@dataclass
class NewsScheduleConfig:
    """Configuration for news scheduling - OPTIMIZED for continuous collection"""
    collection_interval_minutes: int = 5       # Collect every 5 minutes (was 20)
    collection_duration_minutes: int = 2       # Collect for 2 minutes (was 3)
    market_hours_only: bool = False            # 24/7 collection (was True)
    market_buffer_hours: int = 1               # 1 hour buffer before/after market
    memory_threshold_pause: bool = False       # Don't pause for memory (was True)
    weekend_collection: bool = True            # Collect on weekends (was False)
    
    # Market hours (EST)
    market_open_time: dt_time = dt_time(9, 30)    # 9:30 AM EST
    market_close_time: dt_time = dt_time(16, 0)   # 4:00 PM EST

class NewsScheduler:
    """
    Smart News Collection Scheduler
    
    Features:
    - Scheduled collection instead of continuous
    - Market hours awareness
    - Memory threshold integration
    - Configurable intervals and duration
    """
    
    def __init__(self, config: Optional[NewsScheduleConfig] = None):
        self.config = config or NewsScheduleConfig()
        
        # Current state
        self.mode = NewsScheduleMode.SCHEDULED
        self.is_collecting = False
        self.last_collection_start: Optional[datetime] = None
        self.next_collection_time: Optional[datetime] = None
        
        # Monitoring and control
        self.logger = logging.getLogger("news_scheduler")
        self.redis_manager = get_trading_redis_manager()
        self._scheduler_active = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Statistics
        self.stats = {
            'total_collections': 0,
            'successful_collections': 0,
            'skipped_collections': 0,
            'memory_pauses': 0,
            'start_time': datetime.now()
        }
        
        # Calculate next collection time
        self._calculate_next_collection()
        
        self.logger.info(f"NewsScheduler initialized")
        self.logger.info(f"Collection interval: {self.config.collection_interval_minutes}min")
        self.logger.info(f"Collection duration: {self.config.collection_duration_minutes}min")
        self.logger.info(f"Market hours only: {self.config.market_hours_only}")
        if self.next_collection_time:
            self.logger.info(f"Next collection: {self.next_collection_time.strftime('%H:%M:%S')}")
    
    def start_scheduler(self):
        """Start the news collection scheduler"""
        if self._scheduler_active:
            return
            
        self._scheduler_active = True
        self._stop_event.clear()
        
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            daemon=True,
            name="NewsScheduler"
        )
        self._scheduler_thread.start()
        
        self.logger.info("News scheduler started")
    
    def stop_scheduler(self):
        """Stop the news collection scheduler"""
        self._scheduler_active = False
        self._stop_event.set()
        
        # Stop any active collection
        if self.is_collecting:
            self._stop_collection()
        
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=10)
            
        self.logger.info("News scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop"""
        while not self._stop_event.is_set():
            try:
                current_time = datetime.now()
                
                # Check if it's time for news collection
                if self._should_collect_news(current_time):
                    self._start_collection()
                
                # Check if current collection should stop
                elif self.is_collecting and self._should_stop_collection(current_time):
                    self._stop_collection()
                
                # Update next collection time if not collecting
                if not self.is_collecting:
                    self._calculate_next_collection()
                
                # Update Redis state
                self._update_redis_state()
                
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
            
            # Sleep for 30 seconds between checks
            self._stop_event.wait(30)
    
    def _should_collect_news(self, current_time: datetime) -> bool:
        """Determine if news collection should start"""
        # Don't start if already collecting
        if self.is_collecting:
            return False
        
        # Check if scheduler is disabled or paused
        if self.mode in [NewsScheduleMode.DISABLED, NewsScheduleMode.EMERGENCY_PAUSE]:
            return False
        
        # Check if it's time for the next collection
        if self.next_collection_time and current_time >= self.next_collection_time:
            # Additional checks
            if self._is_market_hours_appropriate(current_time):
                if self._is_memory_threshold_ok():
                    return True
                else:
                    self.logger.warning("NEWS_SCHEDULER: Skipping collection due to memory threshold")
                    self.stats['memory_pauses'] += 1
                    return False
            else:
                self.logger.debug("NEWS_SCHEDULER: Skipping collection - outside market hours")
                self.stats['skipped_collections'] += 1
                return False
        
        return False
    
    def _should_stop_collection(self, current_time: datetime) -> bool:
        """Determine if current news collection should stop"""
        if not self.is_collecting or not self.last_collection_start:
            return False
        
        # Stop after configured duration
        elapsed = current_time - self.last_collection_start
        if elapsed.total_seconds() >= (self.config.collection_duration_minutes * 60):
            return True
        
        # Stop if memory threshold exceeded
        if not self._is_memory_threshold_ok():
            self.logger.warning("NEWS_SCHEDULER: Stopping collection due to memory threshold")
            return True
        
        return False
    
    def _is_market_hours_appropriate(self, current_time: datetime) -> bool:
        """MODIFIED: Always return True for 24/7 news collection"""
        # Enable continuous news monitoring regardless of market hours
        return True
    
    def _is_memory_threshold_ok(self) -> bool:
        """Check if memory threshold allows news collection"""
        if not self.config.memory_threshold_pause:
            return True
        
        try:
            # Get memory controller status from Redis
            memory_state = self.redis_manager.get_shared_state(
                'memory_controller_state', 
                namespace='system'
            )
            
            if memory_state:
                current_mode = memory_state.get('current_mode', 'normal')
                # Allow collection only in normal mode
                return current_mode == 'normal'
        except Exception as e:
            self.logger.debug(f"Could not check memory threshold: {e}")
        
        # Default to allowing collection if can't check
        return True
    
    def _start_collection(self):
        """Start news collection"""
        try:
            self.logger.info("NEWS_SCHEDULER: Starting scheduled news collection")
            self.is_collecting = True
            self.last_collection_start = datetime.now()
            self.stats['total_collections'] += 1
            
            # Send start collection message to enhanced news pipeline
            message = create_process_message(
                sender="news_scheduler",
                recipient="enhanced_news_pipeline",
                message_type="START_COLLECTION",
                data={
                    'collection_duration_minutes': self.config.collection_duration_minutes,
                    'scheduled_collection': True,
                    'start_time': self.last_collection_start.isoformat()
                },
                priority=MessagePriority.NORMAL
            )
            
            self.redis_manager.send_message(message, "process_enhanced_news_pipeline")
            
        except Exception as e:
            self.logger.error(f"Error starting collection: {e}")
            self.is_collecting = False
    
    def _stop_collection(self):
        """Stop news collection"""
        try:
            if self.is_collecting:
                self.logger.info("NEWS_SCHEDULER: Stopping scheduled news collection")
                
                # Send stop collection message
                message = create_process_message(
                    sender="news_scheduler",
                    recipient="enhanced_news_pipeline",
                    message_type="STOP_COLLECTION",
                    data={
                        'collection_duration_actual': (datetime.now() - self.last_collection_start).total_seconds() / 60 if self.last_collection_start else 0,
                        'scheduled_stop': True,
                        'stop_time': datetime.now().isoformat()
                    },
                    priority=MessagePriority.NORMAL
                )
                
                self.redis_manager.send_message(message, "process_enhanced_news_pipeline")
                self.stats['successful_collections'] += 1
            
            self.is_collecting = False
            
        except Exception as e:
            self.logger.error(f"Error stopping collection: {e}")
    
    def _calculate_next_collection(self):
        """Calculate the next collection time"""
        current_time = datetime.now()
        
        # Calculate next collection time based on interval
        if self.last_collection_start:
            # Next collection after the interval from last start
            next_time = self.last_collection_start + timedelta(minutes=self.config.collection_interval_minutes)
        else:
            # First collection - start in 1 minute
            next_time = current_time + timedelta(minutes=1)
        
        # If the calculated time is in the past, move to next valid time
        while next_time <= current_time:
            next_time += timedelta(minutes=self.config.collection_interval_minutes)
        
        # If market hours only, adjust to next valid market time
        if self.config.market_hours_only:
            next_time = self._adjust_for_market_hours(next_time)
        
        self.next_collection_time = next_time
        self.logger.debug(f"Next collection scheduled for: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def _adjust_for_market_hours(self, target_time: datetime) -> datetime:
        """Adjust target time to fall within market hours"""
        # If weekend and no weekend collection, move to Monday
        if target_time.weekday() >= 5 and not self.config.weekend_collection:
            days_until_monday = 7 - target_time.weekday()
            target_time = target_time.replace(hour=9, minute=30, second=0, microsecond=0)
            target_time += timedelta(days=days_until_monday)
        
        # Calculate market hours with buffer
        buffer_delta = timedelta(hours=self.config.market_buffer_hours)
        market_open_dt = datetime.combine(target_time.date(), self.config.market_open_time)
        market_close_dt = datetime.combine(target_time.date(), self.config.market_close_time)
        
        effective_start = market_open_dt - buffer_delta
        effective_end = market_close_dt + buffer_delta
        
        # If before market hours, move to start of market hours
        if target_time < effective_start:
            target_time = effective_start
        
        # If after market hours, move to next day's market hours
        elif target_time > effective_end:
            next_day = target_time + timedelta(days=1)
            next_market_open = datetime.combine(next_day.date(), self.config.market_open_time)
            target_time = next_market_open - buffer_delta
        
        return target_time
    
    def _update_redis_state(self):
        """Update scheduler state in Redis"""
        try:
            state_data = {
                'mode': self.mode.value,
                'is_collecting': self.is_collecting,
                'last_collection_start': self.last_collection_start.isoformat() if self.last_collection_start else None,
                'next_collection_time': self.next_collection_time.isoformat() if self.next_collection_time else None,
                'config': {
                    'interval_minutes': self.config.collection_interval_minutes,
                    'duration_minutes': self.config.collection_duration_minutes,
                    'market_hours_only': self.config.market_hours_only,
                    'buffer_hours': self.config.market_buffer_hours
                },
                'stats': self.stats,
                'last_update': datetime.now().isoformat()
            }
            
            self.redis_manager.set_data(
                'news_scheduler_state',
                state_data,
                namespace='system',
                ttl=300
            )
            
        except Exception as e:
            self.logger.error(f"Error updating Redis state: {e}")
    
    def pause_for_memory(self):
        """Pause news collection due to memory constraints"""
        self.mode = NewsScheduleMode.EMERGENCY_PAUSE
        if self.is_collecting:
            self._stop_collection()
        self.logger.warning("NEWS_SCHEDULER: Paused due to memory constraints")
    
    def resume_from_memory_pause(self):
        """Resume news collection after memory constraints resolved"""
        if self.mode == NewsScheduleMode.EMERGENCY_PAUSE:
            self.mode = NewsScheduleMode.SCHEDULED
            self._calculate_next_collection()
            self.logger.info("NEWS_SCHEDULER: Resumed from memory pause")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status"""
        return {
            'mode': self.mode.value,
            'is_collecting': self.is_collecting,
            'last_collection': self.last_collection_start.isoformat() if self.last_collection_start else None,
            'next_collection': self.next_collection_time.isoformat() if self.next_collection_time else None,
            'config': {
                'interval_minutes': self.config.collection_interval_minutes,
                'duration_minutes': self.config.collection_duration_minutes,
                'market_hours_only': self.config.market_hours_only
            },
            'stats': self.stats,
            'uptime_seconds': (datetime.now() - self.stats['start_time']).total_seconds()
        }

# Global scheduler instance
_news_scheduler = None

def get_news_scheduler(config: Optional[NewsScheduleConfig] = None) -> NewsScheduler:
    """Get the global news scheduler instance"""
    global _news_scheduler
    if _news_scheduler is None:
        _news_scheduler = NewsScheduler(config)
    return _news_scheduler

def start_news_scheduling(config: Optional[NewsScheduleConfig] = None):
    """Start the global news scheduling"""
    scheduler = get_news_scheduler(config)
    scheduler.start_scheduler()
    return scheduler

def stop_news_scheduling():
    """Stop the global news scheduling"""
    global _news_scheduler
    if _news_scheduler:
        _news_scheduler.stop_scheduler()