"""
multiprocess_infrastructure/windows_compatible_base_process.py

Windows-compatible base class for all trading system processes.
Avoids pickle issues with threading components while providing standardized lifecycle.
"""

import multiprocessing as mp
import time
import signal
import logging
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass
from enum import Enum


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


class WindowsCompatibleBaseProcess(ABC):
    """
    Windows-compatible base class for all trading system processes

    Provides:
    - Standardized lifecycle management
    - Error handling with exponential backoff
    - Graceful shutdown
    - Resource monitoring
    - Avoids threading components that cause pickle issues
    """

    def __init__(self, process_id: str, heartbeat_interval: int = 30):
        self.process_id = process_id
        self.heartbeat_interval = heartbeat_interval

        # Process state
        self.state = ProcessState.INITIALIZING
        self.start_time = time.time()
        self.error_count = 0
        self.restart_count = 0

        # Simple flags instead of threading.Event
        self._should_stop = False
        self._is_running = False

        # Shared state and queues (set by process manager)
        self.shared_state = None
        self.priority_queue = None
        self.health_queue = None

        # Configure logging
        self._setup_logging()

        self.logger.info(f"Process {self.process_id} initialized")

    def _setup_logging(self):
        """Setup process-specific logging"""
        self.logger = logging.getLogger(f"process.{self.process_id}")

        # Ensure logs directory exists
        if not os.path.exists("logs"):
            os.makedirs("logs")

        # Create process-specific log file if not exists
        log_file = f"logs/{self.process_id}.log"
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)

        # Only add handler if not already present
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(f"{self.process_id}.log")
                  for h in self.logger.handlers):
            self.logger.addHandler(handler)

        self.logger.setLevel(logging.INFO)

    def run_process(self):
        """Main process entry point - called by multiprocessing"""
        try:
            self.logger.info(f"Starting process {self.process_id}")
            self.state = ProcessState.RUNNING
            self._is_running = True

            # Initialize process-specific components
            self._initialize()

            # Main processing loop
            last_heartbeat = time.time()

            while not self._should_stop and self._is_running:
                try:
                    # Send heartbeat periodically
                    current_time = time.time()
                    if current_time - last_heartbeat >= self.heartbeat_interval:
                        self._send_heartbeat()
                        last_heartbeat = current_time

                    # Process cycle
                    self._process_cycle()

                    # Small sleep to prevent excessive CPU usage
                    time.sleep(0.1)

                except KeyboardInterrupt:
                    self.logger.info(f"Process {self.process_id} received interrupt signal")
                    break
                except Exception as e:
                    self.error_count += 1
                    self.logger.error(f"Error in process cycle: {e}")

                    # Exponential backoff on errors
                    sleep_time = min(2 ** self.error_count, 60)
                    time.sleep(sleep_time)

                    # If too many errors, stop the process
                    if self.error_count > 10:
                        self.logger.error(f"Too many errors ({self.error_count}), stopping process")
                        break

            self.logger.info(f"Process {self.process_id} finished main loop")

        except Exception as e:
            self.logger.error(f"Failed to start process {self.process_id}: {e}")
            self.state = ProcessState.ERROR
            self.error_count += 1
        finally:
            # Cleanup
            try:
                self._cleanup()
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")

            self.state = ProcessState.STOPPED
            self._is_running = False
            self.logger.info(f"Process {self.process_id} stopped")

    def stop_process(self):
        """Stop the process gracefully"""
        self.logger.info(f"Stopping process {self.process_id}")
        self.state = ProcessState.STOPPING
        self._should_stop = True

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

    def _send_heartbeat(self):
        """Send heartbeat to health monitoring"""
        try:
            if self.health_queue:
                health = self.get_health()
                self.health_queue.put(health, block=False)
        except Exception as e:
            self.logger.warning(f"Heartbeat error: {e}")

    @abstractmethod
    def _initialize(self):
        """Initialize process-specific components"""
        pass

    @abstractmethod
    def _process_cycle(self):
        """Execute one processing cycle"""
        pass

    @abstractmethod
    def _cleanup(self):
        """Cleanup process-specific resources"""
        pass

    @abstractmethod
    def get_process_info(self) -> Dict[str, Any]:
        """Get process information for monitoring"""
        pass


def run_windows_compatible_process(process_class, process_id: str, *args, **kwargs):
    """Function to run a Windows-compatible process in multiprocessing"""
    try:
        # Create process instance
        process_instance = process_class(process_id, *args, **kwargs)

        # Run the process
        process_instance.run_process()

    except Exception as e:
        logging.error(f"Error running process {process_id}: {e}")
        raise


def create_process_queues():
    """Create queues for inter-process communication"""
    try:
        # Use spawn-compatible queue creation
        ctx = mp.get_context('spawn')
        priority_queue = ctx.Queue()
        health_queue = ctx.Queue()
        return priority_queue, health_queue
    except Exception as e:
        logging.error(f"Error creating process queues: {e}")
        return None, None


def setup_process_logging():
    """Setup logging for multiprocess environment"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/multiprocess.log'),
            logging.StreamHandler()
        ]
    )
