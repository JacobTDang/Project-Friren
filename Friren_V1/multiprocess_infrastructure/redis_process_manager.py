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
    
    def __init__(self, max_processes: int = 5):
        self.max_processes = max_processes
        self.logger = logging.getLogger("redis_process_manager")
        
        # Process tracking
        self.processes: Dict[str, ProcessStatus] = {}
        self.process_configs: Dict[str, ProcessConfig] = {}
        
        # Redis manager
        self.redis_manager = get_trading_redis_manager()
        
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
        
        self.logger.info(f"RedisProcessManager initialized with max_processes={max_processes}")
    
    def register_process(self, config: ProcessConfig):
        """Register a process configuration"""
        self.logger.info(f"Registering process: {config.process_id}")
        
        if len(self.process_configs) >= self.max_processes:
            raise RuntimeError(f"Cannot register more than {self.max_processes} processes")
        
        if config.process_id in self.process_configs:
            raise ValueError(f"Process {config.process_id} already registered")
        
        self.process_configs[config.process_id] = config
        self.processes[config.process_id] = ProcessStatus(
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
        
        self.logger.info(f"Registered process: {config.process_id}")
    
    def start_all_processes(self, dependency_order: Optional[List[str]] = None):
        """Start all registered processes"""
        self.logger.info("Starting all processes...")
        
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
        
        self.logger.info("All processes started")
    
    def stop_all_processes(self, graceful_timeout: int = 30):
        """Stop all processes gracefully"""
        self.logger.info("Stopping all processes...")
        
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
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=project_root
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
            # Send stop message via Redis first
            stop_message = create_process_message(
                sender='process_manager',
                recipient=process_id,
                message_type='stop',
                data={'reason': 'shutdown_requested'}
            )
            self.redis_manager.send_message(stop_message, f"process_{process_id}")
            
            # Wait for graceful shutdown
            try:
                status.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Force terminating process: {process_id}")
                status.process.terminate()
                
                try:
                    status.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Force killing process: {process_id}")
                    status.process.kill()
                    status.process.wait()
            
            self._cleanup_process(process_id)
            self.logger.info(f"Process {process_id} stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping process {process_id}: {e}")
    
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
        if status.process and status.process.poll() is not None:
            # Process died
            self.logger.warning(f"Process {process_id} died (exit code: {status.process.returncode})")
            status.is_healthy = False
            status.consecutive_failures += 1
            
            # Check if we should restart
            if self._should_restart_process(process_id):
                self.logger.info(f"Restarting failed process: {process_id}")
                self._restart_failed_process(process_id)
            
            return
        
        # Check health via Redis
        try:
            health_data = self.redis_manager.get_process_health(process_id)
            
            if health_data:
                # Process is reporting health
                last_heartbeat_str = health_data.get('last_heartbeat')
                if last_heartbeat_str:
                    last_heartbeat = datetime.fromisoformat(last_heartbeat_str)
                    
                    # Check if heartbeat is recent (within 2x heartbeat interval)
                    config = self.process_configs[process_id]
                    max_heartbeat_age = config.health_check_interval * 2
                    
                    if (datetime.now() - last_heartbeat).total_seconds() > max_heartbeat_age:
                        # Stale heartbeat
                        status.is_healthy = False
                        status.consecutive_failures += 1
                        self.logger.warning(f"Process {process_id} has stale heartbeat")
                    else:
                        # Healthy
                        status.is_healthy = True
                        status.consecutive_failures = 0
                else:
                    # No heartbeat data
                    status.is_healthy = False
            else:
                # No health data in Redis
                status.is_healthy = False
            
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
        
        is_running = (
            status.process is not None and 
            status.process.poll() is None and
            status.is_healthy
        )
        
        return {
            'process_id': process_id,
            'is_running': is_running,
            'is_healthy': status.is_healthy,
            'uptime_seconds': uptime_seconds,
            'restart_count': status.restart_count,
            'consecutive_failures': status.consecutive_failures,
            'last_start_time': status.last_start_time.isoformat() if status.last_start_time else None,
            'last_stop_time': status.last_stop_time.isoformat() if status.last_stop_time else None,
            'last_health_check': status.last_health_check.isoformat() if status.last_health_check else None,
            'subprocess_pid': status.process.pid if status.process else None,
            'redis_health_data': health_data
        }
    
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
        
        return {
            'overall_health': overall_health,
            'processes': {
                'healthy': healthy_processes,
                'total': total_processes,
                'details': self.get_process_status()
            },
            'redis_status': redis_status,
            'timestamp': datetime.now().isoformat()
        }
    
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