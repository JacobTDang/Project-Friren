"""
Process Recovery Manager
=======================

Handles process failure recovery with Redis state caching.
Ensures position health monitor always runs with highest priority.
Implements graceful process recovery and state restoration.

Features:
- Process health monitoring and failure detection
- State caching and restoration
- Priority-based process recovery
- Graceful shutdown and restart mechanisms
- Position health monitor priority enforcement
"""

import time
import threading
import subprocess
import psutil
import signal
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging
import json
from collections import defaultdict

from Friren_V1.multiprocess_infrastructure.trading_redis_manager import (
    MessagePriority, get_trading_redis_manager, create_process_message
)
from Friren_V1.trading_engine.coordination.process_recovery_integration import (
    request_recovery_authority, release_recovery_authority, report_recovery_action
)


class ProcessStatus(Enum):
    """Process status states"""
    STARTING = "starting"
    RUNNING = "running"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
    RECOVERING = "recovering"


class ProcessPriority(Enum):
    """Process recovery priority levels"""
    CRITICAL = 1   # Position health monitor - must always run
    HIGH = 2       # Decision engine, risk manager
    NORMAL = 3     # News pipeline, strategy analyzer
    LOW = 4        # Monitoring, logging processes


@dataclass
class ProcessInfo:
    """Information about a managed process"""
    process_id: str
    process_name: str
    process_type: str
    priority: ProcessPriority
    
    # Process state
    pid: Optional[int] = None
    status: ProcessStatus = ProcessStatus.STOPPED
    start_time: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    
    # Process configuration
    command: List[str] = field(default_factory=list)
    working_directory: str = ""
    environment: Dict[str, str] = field(default_factory=dict)
    restart_policy: str = "always"  # always, on-failure, never
    max_restarts: int = 5
    restart_delay_seconds: int = 10
    
    # Recovery state
    restart_count: int = 0
    last_restart_time: Optional[datetime] = None
    failure_reason: Optional[str] = None
    
    # State caching
    cached_state: Dict[str, Any] = field(default_factory=dict)
    state_cache_timestamp: Optional[datetime] = None


@dataclass
class RecoveryAction:
    """Recovery action to be performed"""
    process_id: str
    action_type: str  # restart, stop, kill, cache_state, restore_state
    scheduled_time: datetime
    priority: ProcessPriority
    attempts: int = 0
    max_attempts: int = 3
    context: Dict[str, Any] = field(default_factory=dict)


class ProcessRecoveryManager:
    """
    Manages process recovery with Redis state caching
    Ensures critical processes (especially position health monitor) stay running
    """
    
    def __init__(self, redis_manager=None):
        self.redis_manager = redis_manager or get_trading_redis_manager()
        self.logger = logging.getLogger(__name__)
        
        # Load configuration from config manager
        from .coordination_config import get_coordination_config
        self.config_manager = get_coordination_config()
        process_config = self.config_manager.process_config
        
        # Process management
        self.processes: Dict[str, ProcessInfo] = {}
        self.recovery_lock = threading.RLock()
        
        # Recovery queue
        self.recovery_queue: List[RecoveryAction] = []
        self.recovery_thread: Optional[threading.Thread] = None
        self.recovery_active = False
        
        # Monitoring configuration from config manager
        self.monitoring_thread: Optional[threading.Thread] = None
        self.monitoring_active = False
        self.heartbeat_timeout_seconds = process_config.heartbeat_timeout_seconds
        self.health_check_interval = process_config.health_check_interval_seconds
        
        # Load process configurations dynamically
        self.process_configs = self._load_process_configurations()
        
        # State cache keys
        self.state_cache_prefix = "trading_system:process_state"
        self.heartbeat_prefix = "trading_system:heartbeat"
        
        self.logger.info("ProcessRecoveryManager initialized")
    
    def _load_process_configurations(self) -> Dict[str, ProcessInfo]:
        """Load process configurations from environment and database"""
        try:
            import os
            process_configs = {}
            
            # Load process configurations from database if available
            if hasattr(self.config_manager, 'db_manager') and self.config_manager.db_manager:
                try:
                    db_configs = self._load_process_configs_from_database()
                    if db_configs:
                        process_configs.update(db_configs)
                        self.logger.info(f"Loaded {len(db_configs)} process configs from database")
                except Exception as e:
                    self.logger.warning(f"Could not load process configs from database: {e}")
            
            # Load from environment variables (fallback or supplement)
            env_configs = self._load_process_configs_from_environment()
            process_configs.update(env_configs)
            
            self.logger.info(f"Total process configurations loaded: {len(process_configs)}")
            return process_configs
            
        except Exception as e:
            self.logger.error(f"Error loading process configurations: {e}")
            return self._get_minimal_process_configs()
    
    def _load_process_configs_from_database(self) -> Dict[str, ProcessInfo]:
        """Load process configurations from database"""
        try:
            # This would query a process_configurations table
            # For now, return empty dict (database schema not implemented)
            return {}
        except Exception as e:
            self.logger.debug(f"Database process config loading failed: {e}")
            return {}
    
    def _load_process_configs_from_environment(self) -> Dict[str, ProcessInfo]:
        """Load process configurations from environment variables"""
        try:
            import os
            configs = {}
            
            # Standard processes with environment-based configuration
            processes = [
                ("position_health_monitor", "Position Health Monitor", "monitor", ProcessPriority.CRITICAL),
                ("decision_engine", "Decision Engine", "engine", ProcessPriority.HIGH),
                ("enhanced_news_pipeline", "Enhanced News Pipeline", "pipeline", ProcessPriority.NORMAL),
                ("strategy_analyzer", "Strategy Analyzer", "analyzer", ProcessPriority.NORMAL)
            ]
            
            for process_id, process_name, process_type, default_priority in processes:
                # Get process-specific configuration from environment
                env_prefix = f"FRIREN_PROCESS_{process_id.upper()}"
                
                # Command configuration
                command_env = f"{env_prefix}_COMMAND"
                default_command = f"python -m Friren_V1.trading_engine.portfolio_manager.processes.{process_id}"
                if process_id == "decision_engine":
                    default_command = "python -m Friren_V1.trading_engine.portfolio_manager.decision_engine.decision_engine"
                
                command = os.getenv(command_env, default_command).split()
                
                # Other configurations with environment overrides
                max_restarts = int(os.getenv(f"{env_prefix}_MAX_RESTARTS", self.config_manager.process_config.max_restarts))
                restart_delay = int(os.getenv(f"{env_prefix}_RESTART_DELAY", self.config_manager.process_config.restart_delay_seconds))
                restart_policy = os.getenv(f"{env_prefix}_RESTART_POLICY", "always")
                
                # Priority from configuration
                priority_weight = self.config_manager.process_config.priority_weights.get(process_id, 2)
                if priority_weight >= 4:
                    priority = ProcessPriority.CRITICAL
                elif priority_weight >= 3:
                    priority = ProcessPriority.HIGH
                elif priority_weight >= 2:
                    priority = ProcessPriority.NORMAL
                else:
                    priority = ProcessPriority.LOW
                
                configs[process_id] = ProcessInfo(
                    process_id=process_id,
                    process_name=process_name,
                    process_type=process_type,
                    priority=priority,
                    command=command,
                    restart_policy=restart_policy,
                    max_restarts=max_restarts,
                    restart_delay_seconds=restart_delay
                )
            
            return configs
            
        except Exception as e:
            self.logger.error(f"Error loading process configs from environment: {e}")
            return {}
    
    def _get_minimal_process_configs(self) -> Dict[str, ProcessInfo]:
        """Get minimal process configurations for emergency fallback"""
        return {
            "position_health_monitor": ProcessInfo(
                process_id="position_health_monitor",
                process_name="Position Health Monitor",
                process_type="monitor",
                priority=ProcessPriority.CRITICAL,
                command=["python", "-m", "Friren_V1.trading_engine.portfolio_manager.processes.position_health_monitor"],
                restart_policy="always",
                max_restarts=10,
                restart_delay_seconds=5
            )
        }
    
    def start_monitoring(self):
        """Start process monitoring and recovery"""
        if not self.monitoring_active:
            self.monitoring_active = True
            self.recovery_active = True
            
            # Start monitoring thread
            self.monitoring_thread = threading.Thread(
                target=self._monitoring_worker,
                daemon=True
            )
            self.monitoring_thread.start()
            
            # Start recovery thread
            self.recovery_thread = threading.Thread(
                target=self._recovery_worker,
                daemon=True
            )
            self.recovery_thread.start()
            
            self.logger.info("Process monitoring and recovery started")
    
    def stop_monitoring(self):
        """Stop process monitoring and recovery"""
        self.monitoring_active = False
        self.recovery_active = False
        
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=10)
        if self.recovery_thread:
            self.recovery_thread.join(timeout=10)
        
        self.logger.info("Process monitoring and recovery stopped")
    
    def register_process(self, process_id: str, process_config: Optional[ProcessInfo] = None) -> bool:
        """Register a process for monitoring"""
        with self.recovery_lock:
            try:
                if process_config:
                    self.processes[process_id] = process_config
                elif process_id in self.process_configs:
                    self.processes[process_id] = self.process_configs[process_id]
                else:
                    self.logger.error(f"No configuration found for process: {process_id}")
                    return False
                
                # Initialize process state in Redis
                self._initialize_process_state(process_id)
                
                self.logger.info(f"Process registered: {process_id}")
                return True
                
            except Exception as e:
                self.logger.error(f"Error registering process {process_id}: {e}")
                return False
    
    def start_process(self, process_id: str) -> bool:
        """Start a managed process"""
        with self.recovery_lock:
            try:
                if process_id not in self.processes:
                    self.logger.error(f"Process not registered: {process_id}")
                    return False
                
                process_info = self.processes[process_id]
                
                # Check if already running
                if process_info.status == ProcessStatus.RUNNING:
                    self.logger.info(f"Process {process_id} already running")
                    return True
                
                # Update status
                process_info.status = ProcessStatus.STARTING
                process_info.start_time = datetime.now()
                
                # Start the process
                if self._start_process_subprocess(process_info):
                    process_info.status = ProcessStatus.RUNNING
                    self.logger.info(f"Process started: {process_id} (PID: {process_info.pid})")
                    return True
                else:
                    process_info.status = ProcessStatus.FAILED
                    self.logger.error(f"Failed to start process: {process_id}")
                    return False
                
            except Exception as e:
                self.logger.error(f"Error starting process {process_id}: {e}")
                return False
    
    def _start_process_subprocess(self, process_info: ProcessInfo) -> bool:
        """Start process as subprocess"""
        try:
            # Prepare environment
            env = os.environ.copy()
            env.update(process_info.environment)
            
            # Start subprocess
            process = subprocess.Popen(
                process_info.command,
                cwd=process_info.working_directory or None,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE
            )
            
            process_info.pid = process.pid
            
            # Give process time to start
            time.sleep(2)
            
            # Check if process is still running
            if process.poll() is None:
                return True
            else:
                # Process died immediately
                stdout, stderr = process.communicate(timeout=5)
                self.logger.error(f"Process {process_info.process_id} died immediately: {stderr.decode()}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error starting subprocess for {process_info.process_id}: {e}")
            return False
    
    def stop_process(self, process_id: str, graceful: bool = True) -> bool:
        """Stop a managed process"""
        with self.recovery_lock:
            try:
                if process_id not in self.processes:
                    self.logger.error(f"Process not registered: {process_id}")
                    return False
                
                process_info = self.processes[process_id]
                
                if process_info.status == ProcessStatus.STOPPED:
                    return True
                
                # Cache state before stopping
                self._cache_process_state(process_id)
                
                # Update status
                process_info.status = ProcessStatus.STOPPING
                
                # Stop the process
                if self._stop_process_subprocess(process_info, graceful):
                    process_info.status = ProcessStatus.STOPPED
                    process_info.pid = None
                    self.logger.info(f"Process stopped: {process_id}")
                    return True
                else:
                    self.logger.error(f"Failed to stop process: {process_id}")
                    return False
                
            except Exception as e:
                self.logger.error(f"Error stopping process {process_id}: {e}")
                return False
    
    def _stop_process_subprocess(self, process_info: ProcessInfo, graceful: bool) -> bool:
        """Stop process subprocess"""
        try:
            if not process_info.pid:
                return True
            
            try:
                process = psutil.Process(process_info.pid)
                
                if graceful:
                    # Send SIGTERM first
                    process.terminate()
                    
                    # Wait for graceful shutdown
                    try:
                        process.wait(timeout=10)
                        return True
                    except psutil.TimeoutExpired:
                        self.logger.warning(f"Process {process_info.process_id} did not respond to SIGTERM")
                
                # Force kill if graceful failed or not requested
                process.kill()
                process.wait(timeout=5)
                return True
                
            except psutil.NoSuchProcess:
                # Process already dead
                return True
                
        except Exception as e:
            self.logger.error(f"Error stopping subprocess for {process_info.process_id}: {e}")
            return False
    
    def _monitoring_worker(self):
        """Background worker for process monitoring"""
        while self.monitoring_active:
            try:
                # Check all registered processes
                for process_id in list(self.processes.keys()):
                    self._check_process_health(process_id)
                
                # Special priority check for position health monitor
                self._ensure_position_monitor_running()
                
                # Sleep before next check
                time.sleep(self.health_check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring worker: {e}")
                time.sleep(10)  # Wait before retrying
    
    def _check_process_health(self, process_id: str):
        """Check health of a specific process"""
        try:
            process_info = self.processes.get(process_id)
            if not process_info:
                return
            
            # Check if process is still running
            if process_info.pid:
                try:
                    process = psutil.Process(process_info.pid)
                    if process.is_running():
                        # Check heartbeat
                        if self._check_process_heartbeat(process_id):
                            process_info.status = ProcessStatus.HEALTHY
                        else:
                            process_info.status = ProcessStatus.UNHEALTHY
                            self.logger.warning(f"Process {process_id} heartbeat timeout")
                            self._schedule_recovery(process_id, "heartbeat_timeout")
                    else:
                        # Process died
                        process_info.status = ProcessStatus.FAILED
                        process_info.pid = None
                        self.logger.error(f"Process {process_id} died")
                        self._schedule_recovery(process_id, "process_died")
                        
                except psutil.NoSuchProcess:
                    # Process no longer exists
                    process_info.status = ProcessStatus.FAILED
                    process_info.pid = None
                    self.logger.error(f"Process {process_id} no longer exists")
                    self._schedule_recovery(process_id, "process_not_found")
            
            elif process_info.status == ProcessStatus.RUNNING:
                # Process should be running but has no PID
                process_info.status = ProcessStatus.FAILED
                self.logger.error(f"Process {process_id} missing PID")
                self._schedule_recovery(process_id, "missing_pid")
                
        except Exception as e:
            self.logger.error(f"Error checking health of process {process_id}: {e}")
    
    def _check_process_heartbeat(self, process_id: str) -> bool:
        """Check if process heartbeat is recent"""
        try:
            heartbeat_key = f"{self.heartbeat_prefix}:{process_id}"
            heartbeat_data = self.redis_manager.redis_client.get(heartbeat_key)
            
            if not heartbeat_data:
                return False
            
            heartbeat_time = datetime.fromisoformat(heartbeat_data)
            age = (datetime.now() - heartbeat_time).total_seconds()
            
            return age < self.heartbeat_timeout_seconds
            
        except Exception as e:
            self.logger.error(f"Error checking heartbeat for {process_id}: {e}")
            return False
    
    def _ensure_position_monitor_running(self):
        """Ensure position health monitor is always running (highest priority)"""
        try:
            position_monitor = self.processes.get("position_health_monitor")
            if not position_monitor:
                return
            
            if position_monitor.status not in [ProcessStatus.RUNNING, ProcessStatus.HEALTHY]:
                self.logger.critical("Position health monitor not running - immediate recovery needed")
                self._schedule_recovery("position_health_monitor", "priority_enforcement", immediate=True)
                
        except Exception as e:
            self.logger.error(f"Error ensuring position monitor running: {e}")
    
    def _schedule_recovery(self, process_id: str, reason: str, immediate: bool = False):
        """Schedule process recovery action"""
        with self.recovery_lock:
            try:
                process_info = self.processes.get(process_id)
                if not process_info:
                    return
                
                # Check restart limits
                if process_info.restart_count >= process_info.max_restarts:
                    self.logger.error(f"Process {process_id} exceeded max restarts ({process_info.max_restarts})")
                    return
                
                # Calculate delay
                delay = 0 if immediate else process_info.restart_delay_seconds
                scheduled_time = datetime.now() + timedelta(seconds=delay)
                
                # Create recovery action
                recovery_action = RecoveryAction(
                    process_id=process_id,
                    action_type="restart",
                    scheduled_time=scheduled_time,
                    priority=process_info.priority,
                    context={"reason": reason}
                )
                
                # Add to recovery queue (sorted by priority and time)
                self.recovery_queue.append(recovery_action)
                self.recovery_queue.sort(key=lambda x: (x.priority.value, x.scheduled_time))
                
                self.logger.info(f"Recovery scheduled for {process_id}: {reason} (delay: {delay}s)")
                
            except Exception as e:
                self.logger.error(f"Error scheduling recovery for {process_id}: {e}")
    
    def _recovery_worker(self):
        """Background worker for process recovery"""
        while self.recovery_active:
            try:
                current_time = datetime.now()
                
                # Process recovery queue
                while self.recovery_queue:
                    with self.recovery_lock:
                        if not self.recovery_queue:
                            break
                        
                        # Get next recovery action
                        recovery_action = self.recovery_queue[0]
                        
                        # Check if it's time to execute
                        if recovery_action.scheduled_time <= current_time:
                            # Remove from queue
                            self.recovery_queue.pop(0)
                            
                            # Execute recovery action
                            self._execute_recovery_action(recovery_action)
                        else:
                            # Not time yet, break
                            break
                
                # Sleep before next check
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error in recovery worker: {e}")
                time.sleep(5)  # Wait before retrying
    
    def _execute_recovery_action(self, recovery_action: RecoveryAction):
        """Execute a recovery action with integrated coordination"""
        process_id = recovery_action.process_id
        action_type = recovery_action.action_type
        success = False
        
        try:
            self.logger.info(f"Executing recovery action: {action_type} for {process_id}")
            
            # Request authority from integration manager
            authority_granted = request_recovery_authority(
                process_id, 'strategic', action_type
            )
            
            if not authority_granted:
                self.logger.warning(f"Recovery authority denied for {process_id} - {action_type}")
                # Report failure and potentially retry later
                report_recovery_action(process_id, 'strategic', action_type, False, 
                                     {'reason': 'authority_denied'})
                return
            
            # Execute the recovery action
            if action_type == "restart":
                success = self._recover_process(process_id, recovery_action.context.get("reason", "unknown"))
            
            elif action_type == "cache_state":
                success = self._cache_process_state(process_id)
            
            elif action_type == "restore_state":
                success = self._restore_process_state(process_id)
            
            else:
                self.logger.warning(f"Unknown recovery action type: {action_type}")
                success = False
            
            # Report the result
            report_recovery_action(process_id, 'strategic', action_type, success, 
                                 recovery_action.context)
                
        except Exception as e:
            self.logger.error(f"Error executing recovery action: {e}")
            success = False
            
            # Report failure
            report_recovery_action(process_id, 'strategic', action_type, False, 
                                 {'error': str(e)})
            
        finally:
            # Always release authority
            release_recovery_authority(process_id, 'strategic')
            
        # Retry if failed and not exceeded max attempts
        if not success:
            recovery_action.attempts += 1
            if recovery_action.attempts < recovery_action.max_attempts:
                recovery_action.scheduled_time = datetime.now() + timedelta(seconds=30)
                self.recovery_queue.append(recovery_action)
                self.recovery_queue.sort(key=lambda x: (x.priority.value, x.scheduled_time))
                self.logger.info(f"Recovery action retry scheduled for {process_id} (attempt {recovery_action.attempts})")
            else:
                self.logger.error(f"Recovery action failed permanently for {process_id} after {recovery_action.attempts} attempts")
    
    def _recover_process(self, process_id: str, reason: str) -> bool:
        """Recover a failed process"""
        try:
            process_info = self.processes.get(process_id)
            if not process_info:
                self.logger.error(f"Cannot recover {process_id}: process not found")
                return False
            
            # Update restart count
            process_info.restart_count += 1
            process_info.last_restart_time = datetime.now()
            process_info.failure_reason = reason
            
            # Stop process if still running
            if process_info.pid:
                self._stop_process_subprocess(process_info, graceful=False)
            
            # Cache current state
            self._cache_process_state(process_id)
            
            # Start process
            if self._start_process_subprocess(process_info):
                process_info.status = ProcessStatus.RUNNING
                self.logger.info(f"Process recovered: {process_id} (attempt {process_info.restart_count})")
                
                # Restore state after brief delay
                time.sleep(2)
                self._restore_process_state(process_id)
                return True
                
            else:
                process_info.status = ProcessStatus.FAILED
                self.logger.error(f"Failed to recover process: {process_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error recovering process {process_id}: {e}")
            return False
    
    def _cache_process_state(self, process_id: str) -> bool:
        """Cache process state to Redis"""
        try:
            # Get current state from Redis
            state_data = {
                "process_id": process_id,
                "timestamp": datetime.now().isoformat(),
                "cached_by": "recovery_manager"
            }
            
            # Get process-specific state
            process_state = self.redis_manager.get_all_shared_keys(f"process_{process_id}")
            if process_state:
                state_data["process_state"] = process_state
            
            # Cache in Redis
            cache_key = f"{self.state_cache_prefix}:{process_id}"
            self.redis_manager.redis_client.set(
                cache_key,
                json.dumps(state_data, default=str),
                ex=3600  # 1 hour expiration
            )
            
            self.logger.debug(f"State cached for process: {process_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error caching state for process {process_id}: {e}")
            return False
    
    def _restore_process_state(self, process_id: str) -> bool:
        """Restore process state from Redis cache"""
        try:
            cache_key = f"{self.state_cache_prefix}:{process_id}"
            cached_data = self.redis_manager.redis_client.get(cache_key)
            
            if not cached_data:
                self.logger.debug(f"No cached state found for process: {process_id}")
                return True  # Not having cached state is not a failure
            
            state_data = json.loads(cached_data)
            
            # Restore process-specific state
            if "process_state" in state_data:
                for key, value in state_data["process_state"].items():
                    self.redis_manager.set_shared_state(key, value, f"process_{process_id}")
            
            self.logger.debug(f"State restored for process: {process_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error restoring state for process {process_id}: {e}")
            return False
    
    def _initialize_process_state(self, process_id: str):
        """Initialize process state in Redis"""
        try:
            # Set initial heartbeat
            heartbeat_key = f"{self.heartbeat_prefix}:{process_id}"
            self.redis_manager.redis_client.set(
                heartbeat_key,
                datetime.now().isoformat(),
                ex=self.heartbeat_timeout_seconds * 2
            )
            
            # Initialize process state
            process_state = {
                "status": ProcessStatus.STOPPED.value,
                "registered_time": datetime.now().isoformat(),
                "restart_count": 0
            }
            
            self.redis_manager.update_shared_state(process_state, f"process_{process_id}")
            
        except Exception as e:
            self.logger.error(f"Error initializing state for process {process_id}: {e}")
    
    def update_process_heartbeat(self, process_id: str):
        """Update process heartbeat (called by processes)"""
        try:
            heartbeat_key = f"{self.heartbeat_prefix}:{process_id}"
            self.redis_manager.redis_client.set(
                heartbeat_key,
                datetime.now().isoformat(),
                ex=self.heartbeat_timeout_seconds * 2
            )
            
        except Exception as e:
            self.logger.error(f"Error updating heartbeat for process {process_id}: {e}")
    
    def get_process_status(self, process_id: str) -> Dict[str, Any]:
        """Get status of a specific process"""
        try:
            process_info = self.processes.get(process_id)
            if not process_info:
                return {"error": "Process not found"}
            
            return {
                "process_id": process_info.process_id,
                "process_name": process_info.process_name,
                "status": process_info.status.value,
                "priority": process_info.priority.value,
                "pid": process_info.pid,
                "start_time": process_info.start_time.isoformat() if process_info.start_time else None,
                "last_heartbeat": process_info.last_heartbeat.isoformat() if process_info.last_heartbeat else None,
                "restart_count": process_info.restart_count,
                "last_restart": process_info.last_restart_time.isoformat() if process_info.last_restart_time else None,
                "failure_reason": process_info.failure_reason
            }
            
        except Exception as e:
            self.logger.error(f"Error getting process status for {process_id}: {e}")
            return {"error": str(e)}
    
    def get_all_process_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all processes"""
        try:
            status = {}
            for process_id in self.processes.keys():
                status[process_id] = self.get_process_status(process_id)
            return status
            
        except Exception as e:
            self.logger.error(f"Error getting all process status: {e}")
            return {"error": str(e)}
    
    def force_restart_process(self, process_id: str) -> bool:
        """Force restart a process immediately"""
        try:
            self._schedule_recovery(process_id, "manual_restart", immediate=True)
            return True
            
        except Exception as e:
            self.logger.error(f"Error force restarting process {process_id}: {e}")
            return False


# Global instance
_recovery_manager: Optional[ProcessRecoveryManager] = None


def get_recovery_manager(redis_manager=None) -> ProcessRecoveryManager:
    """Get or create global process recovery manager"""
    global _recovery_manager
    
    if _recovery_manager is None:
        _recovery_manager = ProcessRecoveryManager(redis_manager)
    
    return _recovery_manager


def start_process_monitoring():
    """Start process monitoring (convenience function)"""
    manager = get_recovery_manager()
    manager.start_monitoring()


def update_heartbeat(process_id: str):
    """Update process heartbeat (convenience function)"""
    manager = get_recovery_manager()
    manager.update_process_heartbeat(process_id)


def get_process_status(process_id: str) -> Dict[str, Any]:
    """Get process status (convenience function)"""
    manager = get_recovery_manager()
    return manager.get_process_status(process_id)