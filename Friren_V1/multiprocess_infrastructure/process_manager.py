"""
multiprocess_infrastructure/process_manager.py

Process lifecycle management for multiprocess trading system.
Handles process creation, monitoring, restart policies, and resource management.
"""

import multiprocessing as mp
import time
import signal
import threading
from typing import Dict, List, Optional, Type, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import logging
import psutil

from .base_process import BaseProcess, ProcessState, ProcessHealth
from .queue_manager import QueueManager
from .shared_state_manager import SharedStateManager


class RestartPolicy(Enum):
    """Process restart policies"""
    NEVER = "never"           # Never restart
    ON_FAILURE = "on_failure" # Restart only on failure
    ALWAYS = "always"         # Always restart if stopped
    UNLESS_STOPPED = "unless_stopped"  # Restart unless manually stopped


@dataclass
class ProcessConfig:
    """Configuration for a managed process"""
    process_class: Type[BaseProcess]
    process_id: str
    restart_policy: RestartPolicy = RestartPolicy.ON_FAILURE
    max_restarts: int = 5
    restart_delay_seconds: int = 5
    max_restart_delay: int = 300  # 5 minutes max
    health_check_interval: int = 30
    startup_timeout: int = 60
    shutdown_timeout: int = 30
    resource_limits: Optional[Dict[str, Any]] = None
    process_args: Optional[Dict[str, Any]] = None


@dataclass
class ProcessStatus:
    """Current status of a managed process"""
    config: ProcessConfig
    process: Optional[mp.Process]
    health: Optional[ProcessHealth]
    last_start_time: Optional[datetime]
    last_stop_time: Optional[datetime]
    restart_count: int
    consecutive_failures: int
    is_healthy: bool
    last_health_check: Optional[datetime]


class ProcessManager:
    """
    Manages lifecycle of all trading system processes

    Features:
    - Process creation and monitoring
    - Automatic restart policies
    - Resource limit enforcement
    - Health monitoring with automatic remediation
    - Graceful shutdown handling
    - Process dependency management
    """

    def __init__(self, max_processes: int = 5):
        self.max_processes = max_processes
        self.logger = logging.getLogger("process_manager")

        # Process tracking
        self.processes: Dict[str, ProcessStatus] = {}
        self.process_configs: Dict[str, ProcessConfig] = {}

        # Shared resources
        self.queue_manager = QueueManager()
        self.shared_state = SharedStateManager()

        # Management components
        self._monitor_thread = None
        self._shutdown_event = threading.Event()
        self._startup_complete = threading.Event()

        # Resource monitoring
        self._system_resources = self._get_system_resources()

        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        self.logger.info(f"ProcessManager initialized - Max processes: {max_processes}")
        self.logger.info(f"System resources: {self._system_resources}")

    def register_process(self, config: ProcessConfig):
        """Register a process configuration"""
        if len(self.process_configs) >= self.max_processes:
            raise RuntimeError(f"Cannot register more than {self.max_processes} processes")

        if config.process_id in self.process_configs:
            raise ValueError(f"Process {config.process_id} already registered")

        self.process_configs[config.process_id] = config
        self.processes[config.process_id] = ProcessStatus(
            config=config,
            process=None,
            health=None,
            last_start_time=None,
            last_stop_time=None,
            restart_count=0,
            consecutive_failures=0,
            is_healthy=False,
            last_health_check=None
        )

        self.logger.info(f"Registered process: {config.process_id}")

    def start_all_processes(self, dependency_order: Optional[List[str]] = None):
        """Start all registered processes"""
        self.logger.info("Starting all processes...")

        # Start processes in dependency order if provided
        if dependency_order:
            start_order = dependency_order
        else:
            start_order = list(self.process_configs.keys())

        for process_id in start_order:
            if process_id in self.process_configs:
                self._start_process(process_id)
                time.sleep(2)  # Brief pause between starts

        # Start monitoring thread
        self._start_monitoring()

        # Wait for all processes to be healthy
        self._wait_for_startup_complete()

        self.logger.info("All processes started successfully")

    def stop_all_processes(self, graceful_timeout: int = 30):
        """Stop all processes gracefully"""
        self.logger.info("Stopping all processes...")

        # Signal shutdown
        self._shutdown_event.set()

        # Stop monitoring first
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)

        # Stop processes in reverse dependency order
        process_ids = list(self.processes.keys())
        process_ids.reverse()

        for process_id in process_ids:
            self._stop_process(process_id, graceful_timeout)

        # Cleanup shared resources
        self.shared_state.cleanup()

        self.logger.info("All processes stopped")

    def restart_process(self, process_id: str):
        """Manually restart a specific process"""
        if process_id not in self.processes:
            raise ValueError(f"Unknown process: {process_id}")

        self.logger.info(f"Manually restarting process: {process_id}")

        # Stop the process
        self._stop_process(process_id)

        # Wait a moment
        time.sleep(2)

        # Start it again
        self._start_process(process_id)

    def get_process_status(self, process_id: Optional[str] = None) -> Dict[str, Any]:
        """Get status of specific process or all processes"""
        if process_id:
            if process_id not in self.processes:
                raise ValueError(f"Unknown process: {process_id}")
            return self._get_single_process_status(process_id)
        else:
            return {
                pid: self._get_single_process_status(pid)
                for pid in self.processes.keys()
            }

    def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health report"""
        healthy_processes = sum(1 for status in self.processes.values() if status.is_healthy)
        total_processes = len(self.processes)

        # Get resource usage
        system_health = self._get_system_resources()

        # Check shared state health
        shared_state_health = self.shared_state.health_check()

        # Check queue health
        queue_stats = self.queue_manager.get_queue_stats()

        overall_health = "healthy"
        if healthy_processes < total_processes:
            overall_health = "degraded"
        if healthy_processes == 0:
            overall_health = "critical"
        if shared_state_health['status'] != 'healthy':
            overall_health = "critical"

        return {
            'overall_health': overall_health,
            'processes': {
                'healthy': healthy_processes,
                'total': total_processes,
                'details': self.get_process_status()
            },
            'system_resources': system_health,
            'shared_state': shared_state_health,
            'queue_stats': queue_stats,
            'timestamp': datetime.now().isoformat()
        }

    def _start_process(self, process_id: str) -> bool:
        """Start a specific process"""
        try:
            config = self.process_configs[process_id]
            status = self.processes[process_id]

            self.logger.info(f"Starting process: {process_id}")

            # Create process instance
            process_args = config.process_args or {}
            process_instance = config.process_class(
                process_id=process_id,
                **process_args
            )

            # Set up shared resources
            process_instance.shared_state = self.shared_state
            process_instance.priority_queue = self.queue_manager.priority_queue
            process_instance.health_queue = self.queue_manager.health_queue

            # Create and start process
            process = mp.Process(
                target=self._run_process_wrapper,
                args=(process_instance,),
                name=process_id
            )

            process.start()

            # Update status
            status.process = process
            status.last_start_time = datetime.now()
            status.restart_count += 1

            # Wait for startup
            startup_success = self._wait_for_process_startup(process_id, config.startup_timeout)

            if startup_success:
                self.logger.info(f"Process {process_id} started successfully")
                return True
            else:
                self.logger.error(f"Process {process_id} failed to start within timeout")
                self._stop_process(process_id)
                return False

        except Exception as e:
            self.logger.error(f"Error starting process {process_id}: {e}")
            return False

    def _stop_process(self, process_id: str, timeout: int = 30):
        """Stop a specific process"""
        status = self.processes[process_id]

        if not status.process or not status.process.is_alive():
            return

        self.logger.info(f"Stopping process: {process_id}")

        try:
            # Try graceful shutdown first
            status.process.terminate()
            status.process.join(timeout=timeout)

            # Force kill if still alive
            if status.process.is_alive():
                self.logger.warning(f"Force killing process: {process_id}")
                status.process.kill()
                status.process.join(timeout=5)

            status.last_stop_time = datetime.now()
            status.process = None
            status.is_healthy = False

            self.logger.info(f"Process {process_id} stopped")

        except Exception as e:
            self.logger.error(f"Error stopping process {process_id}: {e}")

    def _run_process_wrapper(self, process_instance: BaseProcess):
        """Wrapper to run process with error handling"""
        try:
            process_instance.start()

            # Keep process alive until stopped
            while process_instance.state != ProcessState.STOPPED:
                time.sleep(1)

        except Exception as e:
            logging.error(f"Process {process_instance.process_id} crashed: {e}")
        finally:
            try:
                process_instance.stop()
            except:
                pass

    def _start_monitoring(self):
        """Start the process monitoring thread"""
        self._monitor_thread = threading.Thread(target=self._monitor_processes, daemon=True)
        self._monitor_thread.start()
        self.logger.info("Process monitoring started")

    def _monitor_processes(self):
        """Main monitoring loop"""
        while not self._shutdown_event.is_set():
            try:
                # Check each process
                for process_id in list(self.processes.keys()):
                    self._check_process_health(process_id)

                # Brief pause
                time.sleep(10)

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)

    def _check_process_health(self, process_id: str):
        """Check health of a specific process"""
        status = self.processes[process_id]
        config = self.process_configs[process_id]

        # Check if process is alive
        if not status.process or not status.process.is_alive():
            status.is_healthy = False
            status.consecutive_failures += 1

            # Check if we should restart
            if self._should_restart_process(process_id):
                self.logger.warning(f"Process {process_id} died, restarting...")
                self._restart_failed_process(process_id)

            return

        # Check health metrics (placeholder - would get from health queue)
        status.last_health_check = datetime.now()

        # For now, assume alive = healthy
        # In full implementation, would check CPU, memory, responsiveness
        status.is_healthy = True
        status.consecutive_failures = 0

    def _should_restart_process(self, process_id: str) -> bool:
        """Determine if a process should be restarted"""
        status = self.processes[process_id]
        config = self.process_configs[process_id]

        # Check restart policy
        if config.restart_policy == RestartPolicy.NEVER:
            return False

        if config.restart_policy == RestartPolicy.ON_FAILURE and status.consecutive_failures == 0:
            return False

        # Check restart limits
        if status.restart_count >= config.max_restarts:
            self.logger.error(f"Process {process_id} exceeded max restarts ({config.max_restarts})")
            return False

        return True

    def _restart_failed_process(self, process_id: str):
        """Restart a failed process with backoff"""
        config = self.process_configs[process_id]
        status = self.processes[process_id]

        # Calculate backoff delay
        delay = min(
            config.restart_delay_seconds * (status.consecutive_failures ** 2),
            config.max_restart_delay
        )

        self.logger.info(f"Waiting {delay}s before restarting {process_id}")
        time.sleep(delay)

        # Attempt restart
        self._start_process(process_id)

    def _wait_for_startup_complete(self, timeout: int = 120):
        """Wait for all processes to become healthy"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            healthy_count = sum(1 for status in self.processes.values() if status.is_healthy)

            if healthy_count == len(self.processes):
                self._startup_complete.set()
                return True

            time.sleep(5)

        raise RuntimeError("Timeout waiting for processes to start")

    def _wait_for_process_startup(self, process_id: str, timeout: int) -> bool:
        """Wait for specific process to start"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            status = self.processes[process_id]
            if status.process and status.process.is_alive():
                return True
            time.sleep(1)

        return False

    def _get_single_process_status(self, process_id: str) -> Dict[str, Any]:
        """Get detailed status for single process"""
        status = self.processes[process_id]
        config = self.process_configs[process_id]

        return {
            'process_id': process_id,
            'is_alive': status.process.is_alive() if status.process else False,
            'is_healthy': status.is_healthy,
            'restart_count': status.restart_count,
            'consecutive_failures': status.consecutive_failures,
            'last_start_time': status.last_start_time.isoformat() if status.last_start_time else None,
            'last_stop_time': status.last_stop_time.isoformat() if status.last_stop_time else None,
            'last_health_check': status.last_health_check.isoformat() if status.last_health_check else None,
            'restart_policy': config.restart_policy.value,
            'max_restarts': config.max_restarts,
            'process_class': config.process_class.__name__
        }

    def _get_system_resources(self) -> Dict[str, Any]:
        """Get current system resource usage"""
        try:
            memory = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent(interval=1)

            return {
                'memory_total_gb': memory.total / (1024**3),
                'memory_available_gb': memory.available / (1024**3),
                'memory_percent': memory.percent,
                'cpu_percent': cpu_percent,
                'cpu_count': psutil.cpu_count(),
                'process_count': len(psutil.pids())
            }
        except Exception as e:
            self.logger.error(f"Error getting system resources: {e}")
            return {'error': str(e)}

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop_all_processes()
