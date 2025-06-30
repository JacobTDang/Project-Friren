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
import signal
import atexit
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
from Friren_V1.multiprocess_infrastructure.memory_monitor import (
    MemoryMonitor,
    MemoryAlert,
    memory_tracked,
    get_memory_monitor
)

class ProcessState(Enum):
    """Enhanced process lifecycle states for smart memory management"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"           # Waiting for memory/turn (minimal resources)
    IDLE = "idle"               # Completed work, minimal memory footprint
    RESETTING = "resetting"     # Clearing memory to init state
    STOPPING = "stopping"
    STOPPED = "stopped"
    KILLED = "killed"           # Terminated, ready for cleanup
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
    - Queue-aware execution cycles
    - Comprehensive memory monitoring and leak prevention
    """

    def __init__(self, process_id: str, heartbeat_interval: int = 30, memory_limit_mb: float = 250):
        self.process_id = process_id
        self.heartbeat_interval = heartbeat_interval
        self.memory_limit_mb = memory_limit_mb

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

        # Queue-aware execution control
        self._cycle_active = threading.Event()
        self._cycle_pause = threading.Event()
        self.current_cycle_time = 30.0  # Default cycle time
        self.queue_mode = False  # Whether running in queue mode

        # Redis manager (will be initialized in start())
        self.redis_manager = None

        # Queue names for this process
        self.process_queue = f"process_{self.process_id}"

        # Memory monitoring
        self.memory_monitor = None
        self._emergency_shutdown_triggered = False
        self._memory_high = False  # Flag for queue blocking instead of shutdown

        # Configure logging
        self._setup_logging()
        
        # Setup signal handlers to prevent orphaned subprocesses
        self._setup_signal_handlers()
        
        # Register cleanup function
        atexit.register(self._emergency_cleanup)

        self.logger.info(f"RedisBaseProcess {self.process_id} initialized with {memory_limit_mb}MB memory limit")

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

    def _setup_signal_handlers(self):
        """Setup signal handlers to prevent orphaned subprocesses"""
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            if hasattr(signal, 'SIGHUP'):
                signal.signal(signal.SIGHUP, self._signal_handler)
        except Exception as e:
            self.logger.warning(f"Could not setup signal handlers: {e}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals to prevent orphaned processes"""
        self.logger.critical(f"Process {self.process_id} received signal {signum} - initiating emergency shutdown")
        self._emergency_shutdown_triggered = True
        self._stop_event.set()
        self.stop()
        
    def _emergency_cleanup(self):
        """Emergency cleanup function called at exit"""
        try:
            if hasattr(self, 'memory_monitor') and self.memory_monitor:
                self.memory_monitor.stop()
            if hasattr(self, 'redis_manager') and self.redis_manager:
                # Clean up Redis connections
                pass
            self.logger.info(f"Emergency cleanup completed for {self.process_id}")
        except Exception as e:
            # Don't raise exceptions during cleanup
            pass

    def _setup_memory_monitoring(self):
        """Setup memory monitoring for this process"""
        try:
            # Initialize memory monitor
            self.memory_monitor = get_memory_monitor(
                process_id=self.process_id,
                memory_limit_mb=self.memory_limit_mb,
                auto_start=True
            )

            # Add emergency callback for critical memory situations
            def emergency_callback(snapshot, stats):
                if not self._emergency_shutdown_triggered:
                    self._emergency_shutdown_triggered = True
                    self.logger.critical(f"MEMORY HIGH: {snapshot.memory_mb:.1f}MB exceeds limit - blocking queue operations")
                    # Set flag to block queue rotation instead of shutdown
                    self._memory_high = True
                    # Try aggressive cleanup
                    self.memory_monitor.cleanup_memory(force=True)

            self.memory_monitor.add_emergency_callback(emergency_callback)

            # Add alert callback for memory warnings
            def alert_callback(snapshot, stats, alert_level):
                if alert_level == MemoryAlert.HIGH:
                    self.logger.warning(f"HIGH MEMORY USAGE: {snapshot.memory_mb:.1f}MB")
                    # Trigger cleanup
                    self.memory_monitor.cleanup_memory()
                elif alert_level == MemoryAlert.CRITICAL:
                    self.logger.error(f"CRITICAL MEMORY USAGE: {snapshot.memory_mb:.1f}MB")
                    # Force cleanup and reduce activity
                    self.memory_monitor.cleanup_memory(force=True)

            self.memory_monitor.add_alert_callback(alert_callback)

            # Custom counters for process-specific tracking
            self.memory_monitor.add_custom_counter("messages_processed",
                                                 lambda: self.messages_processed)
            self.memory_monitor.add_custom_counter("error_count",
                                                 lambda: self.error_count)

            self.logger.info(f"Memory monitoring setup complete for {self.process_id}")

        except Exception as e:
            self.logger.error(f"Failed to setup memory monitoring: {e}")

    def _emergency_shutdown(self):
        """Emergency shutdown due to memory issues"""
        try:
            self.logger.critical(f"EMERGENCY SHUTDOWN: Process {self.process_id} memory critical")

            # Immediate cleanup
            if self.memory_monitor:
                self.memory_monitor.cleanup_memory(force=True)

            # Set error state
            self.state = ProcessState.ERROR
            self._update_health()

            # Force stop
            self._stop_event.set()

            # Send emergency notification
            if self.redis_manager:
                try:
                    emergency_message = create_process_message(
                        sender=self.process_id,
                        recipient="orchestrator",
                        message_type="emergency_shutdown",
                        data={
                            "reason": "memory_critical",
                            "memory_mb": self.memory_monitor.take_snapshot().memory_mb if self.memory_monitor else 0,
                            "timestamp": datetime.now().isoformat()
                        },
                        priority=MessagePriority.CRITICAL
                    )
                    self.redis_manager.send_message(emergency_message, "orchestrator_queue")
                except Exception as e:
                    self.logger.error(f"Failed to send emergency notification: {e}")

        except Exception as e:
            self.logger.error(f"Error during emergency shutdown: {e}")

    # SMART MEMORY MANAGEMENT METHODS
    def _handle_pause_request(self, message: ProcessMessage):
        """Handle request to pause process for memory management"""
        try:
            reason = message.data.get('reason', 'unknown')
            total_memory = message.data.get('total_memory_mb', 0)
            
            self.logger.info(f"SMART_MEMORY: Pausing process due to {reason} (system memory: {total_memory:.1f}MB)")
            
            # Set state to paused
            self.state = ProcessState.PAUSED
            
            # If this process has queue mode, pause cycles
            if self.queue_mode:
                self._cycle_pause.set()
                self._cycle_active.clear()
            
            # Perform memory cleanup
            self._emergency_memory_cleanup()
            
            # Update health with paused status
            self._update_health()
            
            # Send acknowledgment
            ack_message = create_process_message(
                sender=self.process_id,
                recipient=message.sender,
                message_type='PROCESS_PAUSED',
                data={
                    'process_id': self.process_id,
                    'reason': reason,
                    'memory_freed_mb': 0,  # TODO: Calculate actual memory freed
                    'timestamp': datetime.now().isoformat()
                },
                priority=MessagePriority.HIGH
            )
            self.send_message(ack_message)
            
        except Exception as e:
            self.logger.error(f"Error handling pause request: {e}")
    
    def _handle_resume_request(self, message: ProcessMessage):
        """Handle request to resume process after memory management"""
        try:
            reason = message.data.get('reason', 'unknown')
            total_memory = message.data.get('total_memory_mb', 0)
            
            self.logger.info(f"SMART_MEMORY: Resuming process due to {reason} (system memory: {total_memory:.1f}MB)")
            
            # Set state to running
            self.state = ProcessState.RUNNING
            
            # If this process has queue mode, resume cycles
            if self.queue_mode:
                self._cycle_pause.clear()
                self._cycle_active.set()
            
            # Update health with running status
            self._update_health()
            
            # Send acknowledgment
            ack_message = create_process_message(
                sender=self.process_id,
                recipient=message.sender,
                message_type='PROCESS_RESUMED',
                data={
                    'process_id': self.process_id,
                    'reason': reason,
                    'timestamp': datetime.now().isoformat()
                },
                priority=MessagePriority.HIGH
            )
            self.send_message(ack_message)
            
        except Exception as e:
            self.logger.error(f"Error handling resume request: {e}")
    
    def _handle_reset_request(self, message: ProcessMessage):
        """Handle request to reset process to initialization state"""
        try:
            self.logger.info("SMART_MEMORY: Resetting process to initialization state")
            
            # Set state to resetting
            self.state = ProcessState.RESETTING
            
            # Call the reset method (to be implemented by subclasses)
            if hasattr(self, '_reset_to_initialization'):
                self._reset_to_initialization()
            else:
                self.logger.warning("Process does not implement _reset_to_initialization method")
            
            # Perform aggressive memory cleanup
            self._emergency_memory_cleanup()
            
            # Set state to idle
            self.state = ProcessState.IDLE
            self._update_health()
            
            # Send acknowledgment
            ack_message = create_process_message(
                sender=self.process_id,
                recipient=message.sender,
                message_type='PROCESS_RESET',
                data={
                    'process_id': self.process_id,
                    'timestamp': datetime.now().isoformat()
                },
                priority=MessagePriority.HIGH
            )
            self.send_message(ack_message)
            
        except Exception as e:
            self.logger.error(f"Error handling reset request: {e}")
    
    def _emergency_memory_cleanup(self):
        """Perform emergency memory cleanup"""
        try:
            import gc
            
            # Force garbage collection
            collected = gc.collect()
            self.logger.debug(f"Garbage collection freed {collected} objects")
            
            # Clear any caches if they exist
            if hasattr(self, '_clear_caches'):
                self._clear_caches()
            
            # Additional cleanup for subclasses
            if hasattr(self, '_emergency_cleanup'):
                self._emergency_cleanup()
                
        except Exception as e:
            self.logger.error(f"Error during emergency memory cleanup: {e}")

    def start(self):
        """Start the process"""
        try:
            self.logger.info(f"Starting process {self.process_id}")

            # Initialize Redis connection
            self.redis_manager = get_trading_redis_manager()

            # Setup memory monitoring
            self._setup_memory_monitoring()

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

            # Stop memory monitoring
            if self.memory_monitor:
                self.memory_monitor.stop_monitoring()

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

            # Enhanced status for smart memory management
            if self.state in [ProcessState.RUNNING, ProcessState.PAUSED, ProcessState.IDLE]:
                status = 'healthy'
            elif self.state in [ProcessState.RESETTING]:
                status = 'resetting'
            else:
                status = 'unhealthy'

            health_data = {
                'status': status,
                'state': self.state.value,
                'last_heartbeat': datetime.now().isoformat(),
                'error_count': self.error_count,
                'restart_count': self.restart_count,
                'uptime_seconds': uptime_seconds,
                'memory_usage_mb': memory_mb,
                'cpu_percent': cpu_percent,
                'messages_processed': self.messages_processed,
                'last_activity': self.last_activity.isoformat() if self.last_activity else None,
                'can_be_paused': True,  # All processes support smart memory management
                'memory_management_enabled': True
            }

            self.redis_manager.update_process_health(self.process_id, health_data)

        except Exception as e:
            self.logger.error(f"Error updating health: {e}")

    def _main_loop(self):
        """Main process loop - supports both continuous and queue-aware execution"""
        self.logger.info(f"Entering main loop for {self.process_id} (queue_mode: {self.queue_mode})")

        try:
            while not self._stop_event.is_set():
                try:
                    # Always process messages from Redis queue
                    self._process_messages()

                    if self.queue_mode:
                        # Queue-aware execution: wait for cycle activation
                        self._queue_aware_execution()
                    else:
                        # Continuous execution (legacy mode)
                        self._continuous_execution()

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

    def _continuous_execution(self):
        """Continuous execution mode (legacy)"""
        # Execute main process logic
        self._execute()

        # Update activity timestamp
        self.last_activity = datetime.now()

        # Brief pause to prevent CPU spinning
        time.sleep(0.1)

    def _queue_aware_execution(self):
        """Queue-aware execution mode"""
        # Wait for cycle activation signal
        if not self._cycle_active.is_set():
            time.sleep(1)  # Wait for activation
            return

        # Check if cycle is paused
        if self._cycle_pause.is_set():
            self.logger.debug(f"Process {self.process_id} cycle paused")
            time.sleep(1)
            return

        # Execute process logic during active cycle
        cycle_start = time.time()
        self.logger.debug(f"Executing cycle for {self.process_id} (max time: {self.current_cycle_time}s)")

        try:
            # Execute until cycle time expires or pause signal received
            while (time.time() - cycle_start < self.current_cycle_time and
                   self._cycle_active.is_set() and
                   not self._cycle_pause.is_set() and
                   not self._stop_event.is_set()):

                # Execute main process logic
                self._execute()

                # Update activity timestamp
                self.last_activity = datetime.now()

                # Brief pause between executions
                time.sleep(0.5)

            # Log cycle completion
            actual_time = time.time() - cycle_start
            self.logger.debug(f"Cycle completed for {self.process_id} (actual time: {actual_time:.1f}s)")

        except Exception as e:
            self.logger.error(f"Error during cycle execution for {self.process_id}: {e}")
            raise

        finally:
            # Clear cycle active state
            self._cycle_active.clear()

    def enable_queue_mode(self, cycle_time: float = 30.0):
        """Enable queue-aware execution mode"""
        self.queue_mode = True
        self.current_cycle_time = cycle_time
        self._cycle_active.clear()
        self._cycle_pause.clear()
        self.logger.info(f"Enabled queue mode for {self.process_id} (cycle_time: {cycle_time}s)")

    def disable_queue_mode(self):
        """Disable queue-aware execution mode (return to continuous)"""
        self.queue_mode = False
        self._cycle_active.clear()
        self._cycle_pause.clear()
        self.logger.info(f"Disabled queue mode for {self.process_id}")

    def start_execution_cycle(self, cycle_time: float = None):
        """Signal the process to start its execution cycle (queue mode)"""
        if not self.queue_mode:
            self.logger.warning(f"Process {self.process_id} not in queue mode")
            return

        if cycle_time:
            self.current_cycle_time = cycle_time

        self._cycle_pause.clear()
        self._cycle_active.set()
        self.logger.debug(f"Started execution cycle for {self.process_id}")

    def pause_execution_cycle(self):
        """Signal the process to pause its execution cycle (queue mode)"""
        if not self.queue_mode:
            return

        self._cycle_pause.set()
        self._cycle_active.clear()
        self.logger.debug(f"Paused execution cycle for {self.process_id}")

    def is_cycle_active(self) -> bool:
        """Check if the process is in an active execution cycle"""
        return self._cycle_active.is_set() and not self._cycle_pause.is_set()

    def _process_messages(self):
        """Process incoming messages from Redis"""
        try:
            # CRITICAL FIX: Read from process-specific queue first, then shared queue
            message = None
            
            # Try process-specific queue first
            if hasattr(self, 'process_queue'):
                message = self.redis_manager.receive_message(queue_name=self.process_queue, timeout=0.05)
            
            # If no message in process queue, try shared priority queue
            if not message:
                message = self.redis_manager.receive_message(queue_name=None, timeout=0.05)

            if message:
                # Filter messages for this process only
                if message.recipient == self.process_id or message.recipient == "all":
                    self.logger.debug(f"Received message for {self.process_id}: {message.message_type}")
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
                else:
                    # Message not for this process - put it back in the queue for other processes
                    self.redis_manager.send_message(message)
                    self.logger.debug(f"Message {message.message_type} not for {self.process_id} (recipient: {message.recipient}) - routing to correct process")

        except Exception as e:
            # Only log actual errors, not timeouts
            if "timeout" not in str(e).lower():
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

    @memory_tracked
    def _execute(self):
        """Execute process logic - decorated with memory tracking"""
        pass

    def _handle_message(self, message: ProcessMessage):
        """Handle incoming message - enhanced with smart memory management"""
        self.logger.debug(f"Received message type: {message.message_type}")

        # SMART MEMORY MANAGEMENT: Handle pause/resume messages
        if message.message_type == 'PAUSE_PROCESS':
            self._handle_pause_request(message)
            
        elif message.message_type == 'RESUME_PROCESS':
            self._handle_resume_request(message)
            
        elif message.message_type == 'RESET_TO_INIT':
            self._handle_reset_request(message)
            
        # Default message handling
        elif message.message_type == 'ping':
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
                'messages_processed': self.messages_processed,
                'queue_mode': self.queue_mode,
                'cycle_active': self.is_cycle_active() if self.queue_mode else None
            }

            response = create_process_message(
                sender=self.process_id,
                recipient=message.sender,
                message_type='status_response',
                data=status_data
            )
            self.send_message(response)

        elif message.message_type == 'start_cycle':
            # Queue manager signals to start execution cycle
            cycle_time = message.data.get('cycle_time', self.current_cycle_time)
            self.logger.info(f"Received start_cycle from {message.sender} (cycle_time: {cycle_time}s)")
            self.start_execution_cycle(cycle_time)

        elif message.message_type == 'pause_cycle':
            # Queue manager signals to pause execution cycle
            self.logger.info(f"Received pause_cycle from {message.sender}")
            self.pause_execution_cycle()

        elif message.message_type == 'enable_queue_mode':
            # Enable queue-aware execution
            cycle_time = message.data.get('cycle_time', 30.0)
            self.logger.info(f"Received enable_queue_mode from {message.sender}")
            self.enable_queue_mode(cycle_time)

        elif message.message_type == 'disable_queue_mode':
            # Disable queue-aware execution
            self.logger.info(f"Received disable_queue_mode from {message.sender}")
            self.disable_queue_mode()

    def _cleanup(self):
        """Process cleanup logic - enhanced with memory cleanup and Redis disconnection"""
        try:
            # Process-specific cleanup (to be overridden)
            self._process_cleanup()

            # Memory cleanup
            if self.memory_monitor:
                self.memory_monitor.cleanup_memory(force=True)
                self.memory_monitor.stop_monitoring()

            # CRITICAL: Explicitly disconnect from Redis to prevent connection leaks
            if self.redis_manager:
                try:
                    # Send final shutdown message if possible
                    shutdown_message = create_process_message(
                        sender=self.process_id,
                        recipient="orchestrator",
                        message_type="process_shutdown",
                        data={"reason": "cleanup", "final_message": True}
                    )
                    self.redis_manager.send_message(shutdown_message, "orchestrator_queue", timeout=2)
                except Exception as e:
                    self.logger.debug(f"Could not send final shutdown message: {e}")
                
                # Disconnect Redis connections
                try:
                    if hasattr(self.redis_manager, 'disconnect'):
                        self.redis_manager.disconnect()
                    elif hasattr(self.redis_manager, 'close'):
                        self.redis_manager.close()
                    elif hasattr(self.redis_manager, 'cleanup'):
                        self.redis_manager.cleanup()
                    self.logger.info(f"Redis connections closed for {self.process_id}")
                except Exception as e:
                    self.logger.warning(f"Could not properly close Redis connections: {e}")

            self.logger.info(f"Cleanup completed for {self.process_id}")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def _process_cleanup(self):
        """Override this method for process-specific cleanup"""
        pass

    def __str__(self):
        return f"RedisBaseProcess({self.process_id}, state={self.state.value})"

    def __repr__(self):
        return self.__str__()
    
    def is_memory_healthy(self) -> bool:
        """Check if process memory is within acceptable limits for queue operations"""
        return not self._memory_high
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        if self.memory_monitor:
            return self.memory_monitor.get_current_memory()
        return 0.0
