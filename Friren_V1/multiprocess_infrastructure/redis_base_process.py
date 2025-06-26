#!/usr/bin/env python3
"""
redis_base_process.py

Redis-compatible base class for all trading system processes.
Replaces multiprocessing queues with Redis-based communication.
"""

import time
import threading
import logging
import sys
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager, 
    create_process_message, 
    MessagePriority,
    ProcessMessage
)

class ProcessState(Enum):
    """Process lifecycle states"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"

@dataclass
class ProcessHealth:
    """Process health information"""
    process_id: str
    state: ProcessState
    last_heartbeat: datetime
    error_count: int
    restart_count: int
    uptime_seconds: float
    memory_usage_mb: float
    cpu_percent: float
    messages_processed: int = 0
    last_activity: Optional[datetime] = None

class RedisBaseProcess(ABC):
    """
    Redis-compatible base class for all trading system processes
    
    Features:
    - Redis-based message communication
    - Shared state management via Redis
    - Health monitoring via Redis
    - Cross-platform compatibility
    - Standardized lifecycle management
    """
    
    def __init__(self, process_id: str, heartbeat_interval: int = 30):
        self.process_id = process_id
        self.heartbeat_interval = heartbeat_interval
        
        # Process state
        self.state = ProcessState.INITIALIZING
        self.start_time = time.time()
        self.error_count = 0
        self.restart_count = 0
        self.messages_processed = 0
        self.last_activity = datetime.now()
        
        # Threading components
        self._stop_event = threading.Event()
        self._heartbeat_thread = None
        self._main_thread = None
        
        # Redis manager (will be initialized in start())
        self.redis_manager = None
        
        # Queue names for this process
        self.process_queue = f"process_{self.process_id}"
        
        # Configure logging
        self._setup_logging()
        
        self.logger.info(f"RedisBaseProcess {self.process_id} initialized")
    
    def _setup_logging(self):
        """Setup process-specific logging"""
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(project_root, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        # Setup logger for this process
        self.logger = logging.getLogger(f"redis_process.{self.process_id}")
        
        if not self.logger.handlers:
            # File handler
            log_file = os.path.join(logs_dir, f'{self.process_id}_redis.log')
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
            self.logger.setLevel(logging.INFO)
    
    def start(self):
        """Start the process"""
        try:
            self.logger.info(f"Starting process {self.process_id}")
            
            # Initialize Redis connection
            self.redis_manager = get_trading_redis_manager()
            
            # Initialize the process
            self._initialize()
            
            # Start heartbeat thread
            self._start_heartbeat()
            
            # Transition to running state
            self.state = ProcessState.RUNNING
            self._update_health()
            
            # Start main thread
            self._main_thread = threading.Thread(target=self._main_loop, daemon=False)
            self._main_thread.start()
            
            self.logger.info(f"Process {self.process_id} started successfully")
            
        except Exception as e:
            self.logger.error(f"Error starting process {self.process_id}: {e}")
            self.state = ProcessState.ERROR
            self.error_count += 1
            self._update_health()
            raise
    
    def stop(self, timeout: int = 30):
        """Stop the process gracefully"""
        try:
            self.logger.info(f"Stopping process {self.process_id}")
            self.state = ProcessState.STOPPING
            self._update_health()
            
            # Signal stop
            self._stop_event.set()
            
            # Call cleanup
            self._cleanup()
            
            # Wait for main thread to finish
            if self._main_thread and self._main_thread.is_alive():
                self._main_thread.join(timeout=timeout)
            
            # Stop heartbeat thread
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join(timeout=5)
            
            self.state = ProcessState.STOPPED
            self._update_health()
            
            self.logger.info(f"Process {self.process_id} stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping process {self.process_id}: {e}")
    
    def _start_heartbeat(self):
        """Start the heartbeat monitoring thread"""
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
        self._heartbeat_thread.start()
        self.logger.debug(f"Heartbeat thread started for {self.process_id}")
    
    def _heartbeat_worker(self):
        """Heartbeat worker thread"""
        while not self._stop_event.is_set():
            try:
                self._update_health()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                self.logger.error(f"Error in heartbeat worker: {e}")
                time.sleep(5)  # Brief pause before retry
    
    def _update_health(self):
        """Update process health in Redis"""
        try:
            if not self.redis_manager:
                return
            
            # Calculate uptime
            uptime_seconds = time.time() - self.start_time
            
            # Get system metrics (basic implementation)
            try:
                import psutil
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                cpu_percent = process.cpu_percent()
            except:
                memory_mb = 0.0
                cpu_percent = 0.0
            
            health_data = {
                'status': 'healthy' if self.state == ProcessState.RUNNING else 'unhealthy',
                'state': self.state.value,
                'last_heartbeat': datetime.now().isoformat(),
                'error_count': self.error_count,
                'restart_count': self.restart_count,
                'uptime_seconds': uptime_seconds,
                'memory_usage_mb': memory_mb,
                'cpu_percent': cpu_percent,
                'messages_processed': self.messages_processed,
                'last_activity': self.last_activity.isoformat() if self.last_activity else None
            }
            
            self.redis_manager.update_process_health(self.process_id, health_data)
            
        except Exception as e:
            self.logger.error(f"Error updating health: {e}")
    
    def _main_loop(self):
        """Main process loop"""
        self.logger.info(f"Entering main loop for {self.process_id}")
        
        try:
            while not self._stop_event.is_set():
                try:
                    # Process messages from Redis queue
                    self._process_messages()
                    
                    # Execute main process logic
                    self._execute()
                    
                    # Update activity timestamp
                    self.last_activity = datetime.now()
                    
                    # Brief pause to prevent CPU spinning
                    time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"Error in main loop: {e}")
                    self.error_count += 1
                    self._update_health()
                    
                    # Exponential backoff on errors
                    error_delay = min(2 ** min(self.error_count, 6), 60)  # Max 60 seconds
                    time.sleep(error_delay)
                    
        except Exception as e:
            self.logger.error(f"Fatal error in main loop: {e}")
            self.state = ProcessState.ERROR
            self.error_count += 1
            self._update_health()
        
        finally:
            self.logger.info(f"Main loop finished for {self.process_id}")
    
    def _process_messages(self):
        """Process incoming messages from Redis"""
        try:
            # Check for messages addressed to this process
            message = self.redis_manager.receive_message(self.process_queue, timeout=1)
            
            if message:
                self.logger.debug(f"Received message: {message.message_type}")
                self.messages_processed += 1
                
                # Handle message
                self._handle_message(message)
                
                # Send acknowledgment if required
                if message.data.get('require_ack', False):
                    ack_message = create_process_message(
                        sender=self.process_id,
                        recipient=message.sender,
                        message_type='message_ack',
                        data={
                            'original_message_id': message.message_id,
                            'status': 'processed'
                        }
                    )
                    self.send_message(ack_message)
                    
        except Exception as e:
            self.logger.error(f"Error processing messages: {e}")
    
    def send_message(self, message: ProcessMessage, queue_name: str = None) -> bool:
        """Send a message via Redis"""
        try:
            if not self.redis_manager:
                self.logger.warning("Redis manager not available for sending message")
                return False
            
            return self.redis_manager.send_message(message, queue_name)
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            return False
    
    def send_simple_message(self, recipient: str, message_type: str, data: Dict[str, Any], 
                           priority: MessagePriority = MessagePriority.NORMAL) -> bool:
        """Send a simple message"""
        message = create_process_message(
            sender=self.process_id,
            recipient=recipient,
            message_type=message_type,
            data=data,
            priority=priority
        )
        return self.send_message(message)
    
    def get_shared_state(self, key: str, namespace: str = "general", default: Any = None) -> Any:
        """Get shared state from Redis"""
        try:
            if not self.redis_manager:
                return default
            return self.redis_manager.get_shared_state(key, namespace, default)
        except Exception as e:
            self.logger.error(f"Error getting shared state: {e}")
            return default
    
    def set_shared_state(self, key: str, value: Any, namespace: str = "general") -> bool:
        """Set shared state in Redis"""
        try:
            if not self.redis_manager:
                return False
            return self.redis_manager.set_shared_state(key, value, namespace)
        except Exception as e:
            self.logger.error(f"Error setting shared state: {e}")
            return False
    
    def update_shared_state(self, updates: Dict[str, Any], namespace: str = "general") -> bool:
        """Update multiple shared state values"""
        try:
            if not self.redis_manager:
                return False
            return self.redis_manager.update_shared_state(updates, namespace)
        except Exception as e:
            self.logger.error(f"Error updating shared state: {e}")
            return False
    
    # Abstract methods that subclasses must implement
    @abstractmethod
    def _initialize(self):
        """Initialize the process (called once during startup)"""
        pass
    
    @abstractmethod
    def _execute(self):
        """Execute main process logic (called repeatedly in main loop)"""
        pass
    
    def _handle_message(self, message: ProcessMessage):
        """Handle incoming message (override in subclasses if needed)"""
        self.logger.debug(f"Received message type: {message.message_type}")
        
        # Default message handling
        if message.message_type == 'ping':
            # Respond to ping
            response = create_process_message(
                sender=self.process_id,
                recipient=message.sender,
                message_type='pong',
                data={'timestamp': datetime.now().isoformat()}
            )
            self.send_message(response)
        
        elif message.message_type == 'stop':
            # Stop the process
            self.logger.info(f"Received stop message from {message.sender}")
            self.stop()
        
        elif message.message_type == 'status':
            # Send status
            status_data = {
                'process_id': self.process_id,
                'state': self.state.value,
                'uptime_seconds': time.time() - self.start_time,
                'error_count': self.error_count,
                'messages_processed': self.messages_processed
            }
            
            response = create_process_message(
                sender=self.process_id,
                recipient=message.sender,
                message_type='status_response',
                data=status_data
            )
            self.send_message(response)
    
    def _cleanup(self):
        """Cleanup resources (override in subclasses if needed)"""
        self.logger.info(f"Cleaning up process {self.process_id}")
        
        # Update final health status
        self._update_health()
    
    def __str__(self):
        return f"RedisBaseProcess({self.process_id}, state={self.state.value})"
    
    def __repr__(self):
        return self.__str__()