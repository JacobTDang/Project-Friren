"""
multiprocess_infrastructure/base_process.py

Abstract base class for all trading system processes.
Provides standardized lifecycle, heartbeat, and error handling.
"""

import multiprocessing as mp
import time
import signal
import threading
import logging
import sys
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

# Windows-specific multiprocessing configuration
if sys.platform == "win32":
    try:
        if mp.get_start_method(allow_none=True) is None:
            mp.set_start_method('spawn', force=False)
    except RuntimeError:
        pass
    mp.freeze_support()


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


class BaseProcess(ABC):
    """
    Abstract base class for all trading system processes

    Provides:
    - Standardized lifecycle management
    - Heartbeat monitoring
    - Error handling with exponential backoff
    - Graceful shutdown
    - Resource monitoring
    """

    def __init__(self, process_id: str, heartbeat_interval: int = 30):
        self.process_id = process_id
        self.heartbeat_interval = heartbeat_interval

        # Process state
        self.state = ProcessState.INITIALIZING
        self.start_time = time.time()
        self.error_count = 0
        self.restart_count = 0

        # Threading components - initialize as None for Windows compatibility
        # These will be created in _initialize_threading() after process spawn
        self._stop_event = None
        self._heartbeat_thread = None
        self._main_thread = None

        # Shared state and queues (set by process manager)
        self.shared_state = None
        self.priority_queue = None
        self.health_queue = None

        # Configure logging
        self._setup_logging()

        # Initialize threading components if not on Windows or not in spawned process
        self._initialize_threading()

        self.logger.info(f"Process {self.process_id} initialized")

    def _initialize_threading(self):
        """Initialize threading components - called after process spawn on Windows"""
        if self._stop_event is None:
            self._stop_event = threading.Event()

        # Setup signal handlers (Windows-compatible)
        # Skip signal handling in spawned processes on Windows
        if sys.platform != "win32":
            try:
                signal.signal(signal.SIGTERM, self._signal_handler)
                signal.signal(signal.SIGINT, self._signal_handler)
            except (ValueError, OSError):
                # Signal handling not available in this context
                pass
        else:
            # On Windows, only set up signal handlers in the main process
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
            except (ValueError, OSError):
                # Signal handling not available in this context (spawned process)
                pass

    def _setup_logging(self):
        """Setup process-specific logging"""
        self.logger = logging.getLogger(f"process.{self.process_id}")

        # Create process-specific log file if not exists
        log_file = f"logs/{self.process_id}.log"
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def start(self):
        """Start the process"""
        try:
            self.logger.info(f"Starting process {self.process_id}")
            self.state = ProcessState.RUNNING

            # Ensure threading components are initialized (for Windows compatibility)
            self._initialize_threading()

            # Initialize process-specific components
            self._initialize()

            # Start heartbeat thread
            self._start_heartbeat()

            # Start main processing thread
            self._main_thread = threading.Thread(target=self._run_main_loop, daemon=True)
            self._main_thread.start()

            self.logger.info(f"Process {self.process_id} started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start process {self.process_id}: {e}")
            self.state = ProcessState.ERROR
            self.error_count += 1
            raise

    def stop(self, timeout: int = 30):
        """Gracefully stop the process"""
        self.logger.info(f"Stopping process {self.process_id}")
        self.state = ProcessState.STOPPING

        # Signal threads to stop
        if self._stop_event is not None:
            self._stop_event.set()

        # Wait for main thread to finish
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=timeout)

        # Stop heartbeat
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=5)

        # Cleanup process-specific resources
        try:
            self._cleanup()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

        self.state = ProcessState.STOPPED
        self.logger.info(f"Process {self.process_id} stopped")

    def restart(self):
        """Restart the process"""
        self.logger.info(f"Restarting process {self.process_id}")
        self.restart_count += 1

        # Stop current instance
        self.stop()

        # Wait a bit before restart
        time.sleep(min(5 * self.restart_count, 30))  # Exponential backoff

        # Reset state
        if self._stop_event is not None:
            self._stop_event.clear()
        self.error_count = 0

        # Start again
        self.start()

    def get_health(self) -> ProcessHealth:
        """Get current process health"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent()
        except:
            memory_mb = 0.0
            cpu_percent = 0.0

        return ProcessHealth(
            process_id=self.process_id,
            state=self.state,
            last_heartbeat=datetime.now(),
            error_count=self.error_count,
            restart_count=self.restart_count,
            uptime_seconds=time.time() - self.start_time,
            memory_usage_mb=memory_mb,
            cpu_percent=cpu_percent
        )

    def _start_heartbeat(self):
        """Start heartbeat monitoring thread"""
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    def _heartbeat_loop(self):
        """Heartbeat monitoring loop"""
        while self._stop_event is None or not self._stop_event.is_set():
            try:
                if self.health_queue:
                    health = self.get_health()
                    self.health_queue.put(health, block=False)
            except Exception as e:
                self.logger.warning(f"Heartbeat error: {e}")

            time.sleep(self.heartbeat_interval)

            # Safety check for uninitialized stop event
            if self._stop_event is None:
                break

    def _run_main_loop(self):
        """Main processing loop with error handling"""
        while self._stop_event is None or not self._stop_event.is_set():
            try:
                # Call the process-specific logic
                self._process_cycle()

            except Exception as e:
                self.error_count += 1
                self.logger.error(f"Process cycle error: {e}")

                # If too many errors, transition to error state
                if self.error_count >= 5:
                    self.state = ProcessState.ERROR
                    self.logger.critical(f"Process {self.process_id} entering error state")
                    break

                # Brief pause before retry
                time.sleep(1)

            # Safety check for uninitialized stop event
            if self._stop_event is None:
                break

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self.stop()

    # Abstract methods that subclasses must implement

    @abstractmethod
    def _initialize(self):
        """Initialize process-specific components"""
        pass

    @abstractmethod
    def _process_cycle(self):
        """Main processing logic - called repeatedly"""
        pass

    @abstractmethod
    def _cleanup(self):
        """Cleanup process-specific resources"""
        pass

    @abstractmethod
    def get_process_info(self) -> Dict[str, Any]:
        """Return process-specific information for monitoring"""
        pass


# Utility functions for process management

def create_process_queues():
    """Create standard queues for process communication"""
    return {
        'priority_queue': mp.Queue(),
        'health_queue': mp.Queue(),
        'shared_state_queue': mp.Queue()
    }


def setup_process_logging():
    """Setup centralized logging configuration"""
    import os

    # Create logs directory
    os.makedirs('logs', exist_ok=True)

    # Setup root logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/trading_system.log'),
            logging.StreamHandler()
        ]
    )
