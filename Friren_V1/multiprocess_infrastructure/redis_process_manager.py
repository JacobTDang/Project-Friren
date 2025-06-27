#!/usr/bin/env python3
"""
redis_process_manager.py

Redis-compatible process manager for trading system.
Manages process lifecycle using Redis instead of multiprocessing queues.
"""

import sys
import os
import subprocess
import time
import threading
import logging
import signal
import tempfile
import json
from typing import Dict, List, Optional, Type, Any
from dataclasses import dataclass, asdict, is_dataclass
from datetime import datetime, timedelta
from enum import Enum

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    get_trading_redis_manager,
    create_process_message,
    MessagePriority
)
from Friren_V1.multiprocess_infrastructure.redis_base_process import RedisBaseProcess, ProcessState

logger = logging.getLogger(__name__)

class RestartPolicy(Enum):
    """Process restart policies"""
    NEVER = "never"
    ON_FAILURE = "on_failure"
    ALWAYS = "always"
    UNLESS_STOPPED = "unless_stopped"

@dataclass
class ProcessConfig:
    """Configuration for a managed process"""
    process_class: Type[RedisBaseProcess]
    process_id: str
    restart_policy: RestartPolicy = RestartPolicy.ON_FAILURE
    max_restarts: int = 5
    restart_delay_seconds: int = 5
    max_restart_delay: int = 300
    health_check_interval: int = 30
    startup_timeout: int = 120
    shutdown_timeout: int = 30
    resource_limits: Optional[Dict[str, Any]] = None
    process_args: Optional[Dict[str, Any]] = None

@dataclass
class ProcessStatus:
    """Current status of a managed process"""
    config: ProcessConfig
    process: Optional[subprocess.Popen]
    process_id: str
    last_start_time: Optional[datetime]
    last_stop_time: Optional[datetime]
    restart_count: int
    consecutive_failures: int
    is_healthy: bool
    last_health_check: Optional[datetime]
    config_file_path: Optional[str] = None

class RedisProcessManager:
    """
    Redis-compatible process manager for trading system

    Features:
    - Redis-based process communication
    - Cross-platform subprocess management
    - Automatic restart policies
    - Health monitoring via Redis
    - Graceful shutdown handling
    """

    def __init__(self, max_processes: int = 3, enable_queue_rotation: bool = True, cycle_time_seconds: float = 60.0):
        self.max_processes = max_processes
        self.enable_queue_rotation = enable_queue_rotation
        self.cycle_time_seconds = cycle_time_seconds
        self.logger = logging.getLogger("redis_process_manager")

        # Process tracking
        self.processes: Dict[str, ProcessStatus] = {}
        self.process_configs: Dict[str, ProcessConfig] = {}

        # Redis manager
        self.redis_manager = get_trading_redis_manager()

        # Enhanced queue rotation manager with priority system
        self.queue_manager = None
        if enable_queue_rotation:
            from .process_queue_manager import ProcessQueueManager
            self.queue_manager = ProcessQueueManager(
                redis_process_manager=self, 
                cycle_time_seconds=cycle_time_seconds,
                max_concurrent_processes=max_processes,
                target_memory_mb=800  # Target 800MB total memory usage (more aggressive)
            )
            self.logger.info(f"Enhanced queue rotation enabled:")
            self.logger.info(f"  - Max concurrent: {max_processes} processes")
            self.logger.info(f"  - Cycle time: {cycle_time_seconds}s")
            self.logger.info(f"  - Target memory: 800MB")

        # Control flags
        self._shutdown_event = threading.Event()
        self._startup_complete = threading.Event()
        self._monitor_thread = None

        # Signal handling (only in main thread)
        try:
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
                self.logger.info("Signal handlers registered")
        except ValueError:
            self.logger.info("Skipping signal handler registration (not in main thread)")

        execution_mode = "queue rotation" if enable_queue_rotation else "parallel"
        self.logger.info(f"RedisProcessManager initialized with max_processes={max_processes}, execution_mode={execution_mode}")

    def register_process(self, config: ProcessConfig):
        """Register a process configuration"""
        self.logger.info(f"Registering process: {config.process_id}")

        if len(self.process_configs) >= self.max_processes:
            raise RuntimeError(f"Cannot register more than {self.max_processes} processes")

        if config.process_id in self.process_configs:
            raise ValueError(f"Process {config.process_id} already registered")

        self.process_configs[config.process_id] = config
        status = ProcessStatus(
            config=config,
            process=None,
            process_id=config.process_id,
            last_start_time=None,
            last_stop_time=None,
            restart_count=0,
            consecutive_failures=0,
            is_healthy=False,
            last_health_check=None
        )
        self.processes[config.process_id] = status

        # Add to queue manager if rotation is enabled
        if self.queue_manager:
            self.queue_manager.add_process(config.process_id, config, status)

        self.logger.info(f"Registered process: {config.process_id}")

    def start_all_processes(self, dependency_order: Optional[List[str]] = None):
        """Start all registered processes"""
        execution_mode = "queue rotation" if self.enable_queue_rotation else "parallel"
        self.logger.info(f"Starting all processes in {execution_mode} mode...")

        # Determine startup order
        if dependency_order:
            start_order = dependency_order
        else:
            start_order = list(self.process_configs.keys())

        self.logger.info(f"Process startup order: {start_order}")

        # Start processes with staggered startup
        for i, process_id in enumerate(start_order):
            if process_id in self.process_configs:
                self.logger.info(f"Starting process {i+1}/{len(start_order)}: {process_id}")

                success = self._start_process(process_id)

                if success:
                    self.logger.info(f"Process {process_id} started successfully")

                    # Enable queue mode for the process if rotation is enabled
                    if self.enable_queue_rotation:
                        self._enable_process_queue_mode(process_id)

                    # Wait between process starts
                    if i < len(start_order) - 1:
                        wait_time = 8 if i == 0 else 5
                        self.logger.info(f"Waiting {wait_time} seconds before starting next process...")
                        time.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to start process {process_id}")

        # Start monitoring
        self._start_monitoring()

        # Wait for startup completion
        self._wait_for_startup_complete()

        # Start queue rotation if enabled
        if self.enable_queue_rotation and self.queue_manager:
            self.logger.info("Starting process queue rotation...")
            self.queue_manager.start_queue_rotation()

        self.logger.info(f"All processes started in {execution_mode} mode")

    def stop_all_processes(self, graceful_timeout: int = 30):
        """Stop all processes gracefully"""
        self.logger.info("Stopping all processes...")

        # Stop queue rotation first if enabled
        if self.queue_manager:
            self.logger.info("Stopping process queue rotation...")
            self.queue_manager.stop_queue_rotation()

        # Signal shutdown
        self._shutdown_event.set()

        # Stop monitoring
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)

        # Stop processes in reverse order
        process_ids = list(self.processes.keys())
        process_ids.reverse()

        for process_id in process_ids:
            self._stop_process(process_id, graceful_timeout)

        self.logger.info("All processes stopped")

    def restart_process(self, process_id: str):
        """Manually restart a specific process"""
        if process_id not in self.processes:
            raise ValueError(f"Unknown process: {process_id}")

        self.logger.info(f"Manually restarting process: {process_id}")

        self._stop_process(process_id)
        time.sleep(2)
        self._start_process(process_id)

    def _start_process(self, process_id: str) -> bool:
        """Start a specific process using subprocess"""
        try:
            config = self.process_configs[process_id]
            status = self.processes[process_id]

            self.logger.info(f"Starting process: {process_id}")

            # Create temporary config file
            config_data = {
                'process_id': process_id,
                'module_path': config.process_class.__module__,
                'class_name': config.process_class.__name__,
                'process_args': config.process_args or {},
                'heartbeat_interval': config.health_check_interval
            }

            # Create temp file for config
            temp_file = tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.json',
                delete=False,
                prefix=f'redis_process_{process_id}_'
            )

            # Serialize config with dataclass support
            def serialize_obj(obj):
                if is_dataclass(obj):
                    return asdict(obj)
                elif hasattr(obj, '__dict__'):
                    return obj.__dict__
                else:
                    return str(obj)

            json.dump(config_data, temp_file, default=serialize_obj, indent=2)
            temp_file.close()

            # Create subprocess runner script path
            runner_script = os.path.join(
                os.path.dirname(__file__),
                'redis_process_runner.py'
            )

            # Launch process
            cmd = [sys.executable, runner_script, temp_file.name]

            self.logger.info(f"Launching process with command: {' '.join(cmd)}")

            # CRITICAL FIX: Add process group management for proper cleanup
            import platform
            if platform.system() == "Windows":
                # Windows: Use CREATE_NEW_PROCESS_GROUP for proper termination
                process = subprocess.Popen(
                    cmd,
                    stdout=None,  # Inherit from parent process (main terminal)
                    stderr=None,  # Inherit from parent process (main terminal)
                    text=True,
                    cwd=project_root,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:
                # Unix: Use process group for proper cleanup
                process = subprocess.Popen(
                    cmd,
                    stdout=None,  # Inherit from parent process (main terminal)
                    stderr=None,  # Inherit from parent process (main terminal)
                    text=True,
                    cwd=project_root,
                    preexec_fn=os.setsid  # Create new process group
                )

            # Update status
            status.process = process
            status.config_file_path = temp_file.name
            status.last_start_time = datetime.now()
            status.restart_count += 1

            # Wait for startup
            startup_success = self._wait_for_process_startup(process_id, config.startup_timeout)

            if startup_success:
                self.logger.info(f"Process {process_id} started successfully (PID: {process.pid})")
                return True
            else:
                self.logger.error(f"Process {process_id} failed to start within timeout")
                self._cleanup_process(process_id)
                return False

        except Exception as e:
            self.logger.error(f"Error starting process {process_id}: {e}")
            return False

    def _stop_process(self, process_id: str, timeout: int = 30):
        """Stop a specific process"""
        status = self.processes[process_id]

        if not status.process:
            return

        self.logger.info(f"Stopping process: {process_id}")

        try:
            # Send stop message via Redis first (if available)
            try:
                stop_message = create_process_message(
                    sender='process_manager',
                    recipient=process_id,
                    message_type='stop',
                    data={'reason': 'shutdown_requested'}
                )
                self.redis_manager.send_message(stop_message, f"process_{process_id}")
                self.logger.debug(f"Sent Redis stop message to {process_id}")
            except Exception as e:
                self.logger.debug(f"Could not send Redis stop message to {process_id}: {e}")

            # ENHANCED: Terminate process group to catch all child processes
            import platform
            
            # Send SIGTERM first for graceful shutdown
            if hasattr(status.process, 'terminate'):
                self.logger.info(f"Sending SIGTERM to process {process_id} (PID: {status.process.pid})")
                
                # Try to terminate the entire process group
                try:
                    if platform.system() == "Windows":
                        # Windows: Send CTRL_BREAK_EVENT to process group
                        os.kill(status.process.pid, signal.CTRL_BREAK_EVENT)
                    else:
                        # Unix: Send SIGTERM to process group
                        os.killpg(os.getpgid(status.process.pid), signal.SIGTERM)
                    self.logger.info(f"Sent SIGTERM to process group for {process_id}")
                except Exception as e:
                    self.logger.debug(f"Could not terminate process group for {process_id}: {e}")
                    # Fallback to individual process termination
                    status.process.terminate()

            # Wait for graceful shutdown - increased timeout for proper cleanup
            try:
                graceful_timeout = min(timeout, 45)  # Increased from 30 to 45 seconds
                status.process.wait(timeout=graceful_timeout)
                self.logger.info(f"Process {process_id} terminated gracefully")
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Process {process_id} did not terminate gracefully after {graceful_timeout}s, force killing...")

                # Force kill the entire process group
                try:
                    if platform.system() == "Windows":
                        # Windows: Use taskkill to force terminate process tree
                        subprocess.run(['taskkill', '/F', '/T', '/PID', str(status.process.pid)], 
                                     capture_output=True, timeout=10)
                        self.logger.info(f"Force killed process tree for {process_id}")
                    else:
                        # Unix: Send SIGKILL to process group
                        os.killpg(os.getpgid(status.process.pid), signal.SIGKILL)
                        self.logger.info(f"Force killed process group for {process_id}")
                except Exception as e:
                    self.logger.warning(f"Could not force kill process group for {process_id}: {e}")
                    # Fallback to individual process kill
                    if hasattr(status.process, 'kill'):
                        status.process.kill()

                try:
                    force_timeout = 20  # Increased from 15 to 20 seconds
                    status.process.wait(timeout=force_timeout)
                    self.logger.info(f"Process {process_id} force killed")
                except subprocess.TimeoutExpired:
                    self.logger.error(f"CRITICAL: Process {process_id} could not be killed after {force_timeout}s! (PID: {status.process.pid})")
                    # Log the zombie process for manual cleanup
                    self.logger.error(f"ZOMBIE PROCESS DETECTED: PID {status.process.pid} - may need manual cleanup")

            self._cleanup_process(process_id)
            self.logger.info(f"Process {process_id} stopped")

        except Exception as e:
            self.logger.error(f"Error stopping process {process_id}: {e}")
            # Try force cleanup anyway
            try:
                if status.process and hasattr(status.process, 'kill'):
                    status.process.kill()
                self._cleanup_process(process_id)
            except:
                pass

    def _cleanup_process(self, process_id: str):
        """Cleanup process resources"""
        status = self.processes[process_id]

        # Remove temp config file
        if status.config_file_path and os.path.exists(status.config_file_path):
            try:
                os.unlink(status.config_file_path)
            except Exception as e:
                self.logger.warning(f"Could not remove temp config file: {e}")

        # Reset status
        status.process = None
        status.config_file_path = None
        status.last_stop_time = datetime.now()
        status.is_healthy = False

    def _start_monitoring(self):
        """Start the process monitoring thread"""
        self._monitor_thread = threading.Thread(target=self._monitor_processes, daemon=True)
        self._monitor_thread.start()
        self.logger.info("Process monitoring started")

    def _monitor_processes(self):
        """Main monitoring loop"""
        while not self._shutdown_event.is_set():
            try:
                for process_id in list(self.processes.keys()):
                    self._check_process_health(process_id)

                time.sleep(10)  # Check every 10 seconds

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)

    def _check_process_health(self, process_id: str):
        """Check health of a specific process"""
        status = self.processes[process_id]

        # Check if subprocess is still alive
        subprocess_running = status.process and status.process.poll() is None

        if status.process and not subprocess_running:
            # Process died
            self.logger.warning(f"Process {process_id} died (exit code: {status.process.returncode})")
            status.is_healthy = False
            status.consecutive_failures += 1

            # Check if we should restart
            if self._should_restart_process(process_id):
                self.logger.info(f"Restarting failed process: {process_id}")
                self._restart_failed_process(process_id)

            return

        # Use intelligent health determination
        try:
            health_data = self.redis_manager.get_process_health(process_id)

            # Calculate uptime for health determination
            uptime_seconds = 0
            if status.last_start_time:
                uptime_seconds = (datetime.now() - status.last_start_time).total_seconds()

            # Use the new intelligent health check
            new_health_status = self._determine_process_health(
                process_id, health_data, subprocess_running, uptime_seconds
            )

            # Update status
            if new_health_status != status.is_healthy:
                if new_health_status:
                    self.logger.info(f"Process {process_id} became healthy")
                    status.consecutive_failures = 0
                else:
                    self.logger.warning(f"Process {process_id} became unhealthy")
                    status.consecutive_failures += 1

            status.is_healthy = new_health_status
            status.last_health_check = datetime.now()

        except Exception as e:
            self.logger.error(f"Error checking health for {process_id}: {e}")
            status.is_healthy = False

    def _should_restart_process(self, process_id: str) -> bool:
        """Determine if a process should be restarted"""
        status = self.processes[process_id]
        config = self.process_configs[process_id]

        if config.restart_policy == RestartPolicy.NEVER:
            return False

        if config.restart_policy == RestartPolicy.ON_FAILURE and status.consecutive_failures == 0:
            return False

        if status.restart_count >= config.max_restarts:
            self.logger.error(f"Process {process_id} exceeded max restarts")
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

        # Cleanup current process
        self._cleanup_process(process_id)

        # Attempt restart
        self._start_process(process_id)

    def _wait_for_startup_complete(self, timeout: int = 120):
        """Wait for all processes to become healthy"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            # Check health via Redis
            healthy_count = 0

            for process_id in self.processes.keys():
                health_data = self.redis_manager.get_process_health(process_id)
                if health_data and health_data.get('status') == 'healthy':
                    healthy_count += 1

            if healthy_count == len(self.processes):
                self._startup_complete.set()
                self.logger.info("All processes are healthy")
                return True

            self.logger.info(f"Waiting for processes to become healthy: {healthy_count}/{len(self.processes)}")
            time.sleep(5)

        self.logger.warning("Timeout waiting for all processes to become healthy")
        return False

    def _wait_for_process_startup(self, process_id: str, timeout: int) -> bool:
        """Wait for specific process to start"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            # Check if process is still running
            status = self.processes[process_id]
            if not status.process or status.process.poll() is not None:
                return False

            # Check if process is healthy in Redis
            health_data = self.redis_manager.get_process_health(process_id)
            if health_data and health_data.get('status') == 'healthy':
                return True

            time.sleep(1)

        return False

    def get_process_status(self, process_id: Optional[str] = None) -> Dict[str, Any]:
        """Get status of processes"""
        if process_id:
            if process_id not in self.processes:
                raise ValueError(f"Unknown process: {process_id}")
            return self._get_single_process_status(process_id)
        else:
            return {
                pid: self._get_single_process_status(pid)
                for pid in self.processes.keys()
            }

    def _get_single_process_status(self, process_id: str) -> Dict[str, Any]:
        """Get detailed status for single process"""
        status = self.processes[process_id]

        # Get health data from Redis
        health_data = self.redis_manager.get_process_health(process_id)

        uptime_seconds = 0
        if status.last_start_time:
            uptime_seconds = (datetime.now() - status.last_start_time).total_seconds()

        # Determine if subprocess is running
        subprocess_running = (
            status.process is not None and
            status.process.poll() is None
        )

        # Determine health status more intelligently
        is_healthy = self._determine_process_health(process_id, health_data, subprocess_running, uptime_seconds)

        # A process is considered "running" if the subprocess is alive
        # Health is a separate metric that considers Redis heartbeat
        is_running = subprocess_running

        return {
            'process_id': process_id,
            'is_running': is_running,
            'is_healthy': is_healthy,
            'uptime_seconds': uptime_seconds,
            'restart_count': status.restart_count,
            'consecutive_failures': status.consecutive_failures,
            'last_start_time': status.last_start_time.isoformat() if status.last_start_time else None,
            'last_stop_time': status.last_stop_time.isoformat() if status.last_stop_time else None,
            'last_health_check': status.last_health_check.isoformat() if status.last_health_check else None,
            'subprocess_pid': status.process.pid if status.process else None,
            'redis_health_data': health_data,
            'messages_processed': health_data.get('messages_processed', 0) if health_data else 0,
            'last_activity': health_data.get('last_activity', 'Unknown') if health_data else 'Unknown'
        }

    def _determine_process_health(self, process_id: str, health_data: Dict[str, Any],
                                subprocess_running: bool, uptime_seconds: float) -> bool:
        """
        Intelligently determine process health considering startup timing and Redis connectivity

        Args:
            process_id: The process identifier
            health_data: Health data from Redis (may be empty)
            subprocess_running: Whether the subprocess is currently running
            uptime_seconds: How long the process has been running

        Returns:
            bool: True if process is considered healthy
        """
        # If subprocess is not running, it's definitely not healthy
        if not subprocess_running:
            return False

        # If process just started (< 60 seconds), be more lenient with health checks
        startup_grace_period = 60  # seconds
        is_in_startup = uptime_seconds < startup_grace_period

        # If we have Redis health data, use it
        if health_data:
            redis_status = health_data.get('status', 'unknown')

            # Check for explicit healthy status
            if redis_status == 'healthy':
                return True

            # Check heartbeat recency
            last_heartbeat_str = health_data.get('last_heartbeat')
            if last_heartbeat_str:
                try:
                    last_heartbeat = datetime.fromisoformat(last_heartbeat_str)
                    heartbeat_age = (datetime.now() - last_heartbeat).total_seconds()

                    # During startup, be more lenient with heartbeat timing
                    max_heartbeat_age = 120 if is_in_startup else 60

                    if heartbeat_age <= max_heartbeat_age:
                        return True
                    else:
                        self.logger.debug(f"Process {process_id} has stale heartbeat ({heartbeat_age:.1f}s old)")
                        return False
                except (ValueError, TypeError):
                    self.logger.debug(f"Process {process_id} has invalid heartbeat timestamp")

        # If no Redis health data but subprocess is running
        if is_in_startup:
            # During startup, assume healthy if subprocess is running
            self.logger.debug(f"Process {process_id} in startup grace period, assuming healthy")
            return True
        else:
            # After startup period, require Redis health data
            self.logger.debug(f"Process {process_id} missing Redis health data after startup period")
            return False

    def _enable_process_queue_mode(self, process_id: str):
        """Enable queue mode for a specific process"""
        try:
            message = create_process_message(
                sender='process_manager',
                recipient=process_id,
                message_type='enable_queue_mode',
                data={
                    'cycle_time': self.cycle_time_seconds,
                    'timestamp': datetime.now().isoformat()
                }
            )
            self.redis_manager.send_message(message, f"process_{process_id}")
            self.logger.info(f"Enabled queue mode for process {process_id}")
        except Exception as e:
            self.logger.error(f"Error enabling queue mode for {process_id}: {e}")

    def get_queue_status(self) -> Optional[Dict[str, Any]]:
        """Get queue rotation status if enabled"""
        if self.queue_manager:
            return self.queue_manager.get_queue_status()
        return None

    def pause_queue_rotation(self):
        """Pause queue rotation if enabled"""
        if self.queue_manager:
            self.queue_manager.pause_queue_rotation()

    def resume_queue_rotation(self):
        """Resume queue rotation if enabled"""
        if self.queue_manager:
            self.queue_manager.resume_queue_rotation()

    def set_queue_cycle_time(self, cycle_time_seconds: float):
        """Update the queue cycle time if enabled"""
        if self.queue_manager:
            self.queue_manager.set_cycle_time(cycle_time_seconds)
            self.cycle_time_seconds = cycle_time_seconds

    def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health report"""
        # Get all health data from Redis
        all_health = self.redis_manager.get_all_process_health()

        healthy_processes = sum(1 for h in all_health.values()
                              if h.get('status') == 'healthy')
        total_processes = len(self.processes)

        # Get Redis system status
        redis_status = self.redis_manager.get_system_status()

        overall_health = "healthy"
        if healthy_processes < total_processes:
            overall_health = "degraded"
        if healthy_processes == 0 and total_processes > 0:
            overall_health = "critical"

        system_health = {
            'overall_health': overall_health,
            'processes': {
                'healthy': healthy_processes,
                'total': total_processes,
                'details': self.get_process_status()
            },
            'redis_status': redis_status,
            'timestamp': datetime.now().isoformat()
        }

        # Add queue status if enabled
        if self.enable_queue_rotation and self.queue_manager:
            system_health['queue_status'] = self.queue_manager.get_queue_status()

        return system_health

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop_all_processes()

    def cleanup(self):
        """Cleanup all resources"""
        self.stop_all_processes()

        # Cleanup any remaining temp files
        for status in self.processes.values():
            if status.config_file_path and os.path.exists(status.config_file_path):
                try:
                    os.unlink(status.config_file_path)
                except:
                    pass
