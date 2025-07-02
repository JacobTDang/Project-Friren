"""
process_queue_manager.py

Process Queue Rotation Manager for trading system.
Implements round-robin process execution where processes run one at a time
and are rotated in a queue after completing their execution cycle.
"""

import time
import threading
import logging
from typing import List, Dict, Optional, Any
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from .redis_process_manager import RedisProcessManager, ProcessStatus, ProcessConfig

class QueueState(Enum):
    """Queue rotation states"""
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"

@dataclass
class ProcessPriority(Enum):
    """Process priority levels for queue management"""
    ALWAYS_RUNNING = "always_running"    # Never rotated out (position_health_monitor)
    HIGH_PRIORITY = "high_priority"      # Rarely rotated (decision_engine)
    NORMAL = "normal"                    # Regular rotation (news, sentiment, strategy)
    LOW_PRIORITY = "low_priority"        # First to be rotated out

@dataclass
class QueuedProcess:
    """Represents a process in the rotation queue"""
    process_id: str
    config: ProcessConfig
    status: ProcessStatus
    priority: ProcessPriority = field(default_factory=lambda: ProcessPriority.NORMAL)
    last_execution_time: Optional[datetime] = None
    execution_count: int = 0
    total_execution_time: float = 0.0
    is_ready: bool = True
    is_currently_running: bool = False

class ProcessQueueManager:
    """
    Manages process execution in a round-robin queue system.
    
    Features:
    - Round-robin process rotation
    - Configurable execution cycle times
    - Process health monitoring during queue execution
    - Graceful queue pause/resume
    - Execution statistics tracking
    """
    
    def __init__(self, redis_process_manager: RedisProcessManager, cycle_time_seconds: float = 30.0, 
                 max_concurrent_processes: int = 3, target_memory_mb: int = 750):
        """
        Initialize the process queue manager with memory optimization.
        
        Args:
            redis_process_manager: The underlying Redis process manager
            cycle_time_seconds: How long each process gets to execute before rotation
            max_concurrent_processes: Maximum processes running simultaneously (default 4)
            target_memory_mb: Target total memory usage in MB (default 900MB)
        """
        self.redis_process_manager = redis_process_manager
        self.cycle_time_seconds = cycle_time_seconds
        self.max_concurrent_processes = max_concurrent_processes
        self.target_memory_mb = target_memory_mb
        
        # Queue management with priority separation
        self.process_queue: deque[QueuedProcess] = deque()
        self.process_lookup: Dict[str, QueuedProcess] = {}
        self.always_running_processes: Dict[str, QueuedProcess] = {}  # position_health_monitor
        self.high_priority_processes: Dict[str, QueuedProcess] = {}  # decision_engine
        self.current_running_processes: Dict[str, QueuedProcess] = {}
        
        # State management
        self.queue_state = QueueState.STOPPED
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._queue_thread: Optional[threading.Thread] = None
        
        # Statistics and monitoring
        self.total_cycles = 0
        self.start_time: Optional[datetime] = None
        self.last_memory_check = datetime.now()
        self.rotation_count = 0
        
        # Process priority mappings
        self.process_priority_map = {
            'position_health_monitor': ProcessPriority.ALWAYS_RUNNING,
            'decision_engine': ProcessPriority.HIGH_PRIORITY,
            'enhanced_news_pipeline': ProcessPriority.NORMAL,
            'sentiment_analyzer': ProcessPriority.NORMAL,
            'finbert_sentiment': ProcessPriority.NORMAL,
            'strategy_analyzer': ProcessPriority.NORMAL,
            'market_regime_detector': ProcessPriority.NORMAL
        }
        
        # Logging
        self.logger = logging.getLogger("process_queue_manager")
        self.logger.info(f"Enhanced ProcessQueueManager initialized:")
        self.logger.info(f"  - Cycle time: {cycle_time_seconds}s")
        self.logger.info(f"  - Max concurrent: {max_concurrent_processes} processes") 
        self.logger.info(f"  - Target memory: {target_memory_mb}MB")
        self.logger.info(f"  - Priority system: ALWAYS_RUNNING, HIGH_PRIORITY, NORMAL rotation")
    
    def add_process(self, process_id: str, config: ProcessConfig, status: ProcessStatus):
        """Add a process to the priority-based rotation queue"""
        if process_id in self.process_lookup:
            self.logger.warning(f"Process {process_id} already in queue")
            return
        
        # Determine process priority based on process_id
        priority = self._get_process_priority(process_id)
        
        queued_process = QueuedProcess(
            process_id=process_id,
            config=config,
            status=status,
            priority=priority
        )
        
        # Place process in appropriate queue based on priority
        self.process_lookup[process_id] = queued_process
        
        if priority == ProcessPriority.ALWAYS_RUNNING:
            self.always_running_processes[process_id] = queued_process
            self.logger.info(f"Added ALWAYS_RUNNING process: {process_id}")
        elif priority == ProcessPriority.HIGH_PRIORITY:
            self.high_priority_processes[process_id] = queued_process
            self.logger.info(f"Added HIGH_PRIORITY process: {process_id}")
        else:
            self.process_queue.append(queued_process)
            self.logger.info(f"Added NORMAL process: {process_id} to rotation queue (position {len(self.process_queue)})")
    
    def _get_process_priority(self, process_id: str) -> ProcessPriority:
        """Determine process priority based on process_id"""
        # Check for exact matches first
        if process_id in self.process_priority_map:
            return self.process_priority_map[process_id]
        
        # Check for partial matches (handles cases like 'finbert_sentiment_process')
        for key, priority in self.process_priority_map.items():
            if key in process_id.lower():
                return priority
        
        # Default to NORMAL priority
        return ProcessPriority.NORMAL
    
    def remove_process(self, process_id: str):
        """Remove a process from the rotation queue"""
        if process_id not in self.process_lookup:
            self.logger.warning(f"Process {process_id} not in queue")
            return
        
        queued_process = self.process_lookup[process_id]
        
        # Remove from queue
        try:
            self.process_queue.remove(queued_process)
        except ValueError:
            pass
        
        # Remove from lookup
        del self.process_lookup[process_id]
        
        # If this was the current process, move to next
        if self.current_process == queued_process:
            self.current_process = None
        
        self.logger.info(f"Removed process {process_id} from rotation queue")
    
    def start_queue_rotation(self):
        """Start the process queue rotation"""
        if self.queue_state != QueueState.STOPPED:
            self.logger.warning("Queue rotation already running or in transition")
            return
        
        if not self.process_queue:
            self.logger.warning("No processes in queue to rotate")
            return
        
        self.logger.info("Starting process queue rotation")
        self.queue_state = QueueState.RUNNING
        self.start_time = datetime.now()
        self._stop_event.clear()
        self._pause_event.clear()
        
        # Enable queue mode for all processes in rotation
        self._enable_queue_mode_for_all_processes()
        
        # Start the queue rotation thread
        self._queue_thread = threading.Thread(target=self._queue_rotation_loop, daemon=True)
        self._queue_thread.start()
        
        self.logger.info(f"Queue rotation started with {len(self.process_queue)} processes")
    
    def stop_queue_rotation(self, timeout: int = 30):
        """Stop the process queue rotation"""
        if self.queue_state == QueueState.STOPPED:
            return
        
        self.logger.info("Stopping process queue rotation")
        self.queue_state = QueueState.STOPPING
        self._stop_event.set()
        self._pause_event.set()  # Unpause if paused
        
        # Stop current process execution
        if self.current_process:
            self._stop_current_process()
        
        # Wait for queue thread to finish
        if self._queue_thread and self._queue_thread.is_alive():
            self._queue_thread.join(timeout=timeout)
        
        self.queue_state = QueueState.STOPPED
        self.current_process = None
        self.logger.info("Queue rotation stopped")
    
    def pause_queue_rotation(self):
        """Pause the queue rotation (current process finishes its cycle)"""
        if self.queue_state != QueueState.RUNNING:
            self.logger.warning("Queue rotation not running")
            return
        
        self.logger.info("Pausing queue rotation")
        self.queue_state = QueueState.PAUSED
        self._pause_event.set()
    
    def resume_queue_rotation(self):
        """Resume the queue rotation"""
        if self.queue_state != QueueState.PAUSED:
            self.logger.warning("Queue rotation not paused")
            return
        
        self.logger.info("Resuming queue rotation")
        self.queue_state = QueueState.RUNNING
        self._pause_event.clear()
    
    def _queue_rotation_loop(self):
        """Event-driven queue rotation loop - starts processes when others finish"""
        self.logger.info("Event-driven queue rotation loop started")
        
        # Start ALWAYS_RUNNING processes immediately
        self._start_always_running_processes()
        
        # Start HIGH_PRIORITY processes
        self._ensure_priority_processes_running()
        
        # Fill remaining slots with NORMAL processes
        self._fill_remaining_slots()
        
        try:
            while not self._stop_event.is_set():
                # Check if paused
                if self._pause_event.is_set():
                    self.logger.debug("Queue rotation paused, waiting...")
                    self._pause_event.wait()
                    continue
                
                # Check for processes that have finished their business cycles
                self._check_for_finished_processes()
                
                # Check memory usage and adjust if needed (emergency only)
                self._check_memory_and_adjust()
                
                # Ensure critical processes are still running
                self._ensure_priority_processes_running()
                
                # Fill any available slots with waiting processes
                self._fill_remaining_slots()
                
                # Update statistics
                self.total_cycles += 1
                
                # Sleep to avoid busy waiting - increased for production stability
                time.sleep(30)  # Check every 30 seconds for production
                
                self.total_cycles += 1
                
                # Pause between cycles for production stability
                time.sleep(5)
                
        except Exception as e:
            self.logger.error(f"Error in queue rotation loop: {e}")
        finally:
            self.logger.info("Queue rotation loop finished")
    
    def _get_next_process(self) -> Optional[QueuedProcess]:
        """Get the next process to execute"""
        if not self.process_queue:
            return None
        
        # Get first process in queue
        next_process = self.process_queue[0]
        
        # Check if process is healthy and ready
        if not self._is_process_ready(next_process):
            # Move unhealthy process to back and try next
            self.process_queue.rotate(-1)
            return self._get_next_process() if len(self.process_queue) > 1 else None
        
        return next_process
    
    def _is_process_ready(self, queued_process: QueuedProcess) -> bool:
        """Check if a process is ready to execute"""
        # Check if the underlying subprocess is healthy
        process_status = self.redis_process_manager.get_process_status(queued_process.process_id)
        if not process_status:
            return False
        
        is_running = process_status.get('is_running', False)
        is_healthy = process_status.get('is_healthy', False)
        
        return is_running and is_healthy
    
    def _execute_process_cycle(self, queued_process: QueuedProcess):
        """Execute a single cycle for the given process"""
        self.current_process = queued_process
        cycle_start = datetime.now()
        
        self.logger.info(f"Starting execution cycle for {queued_process.process_id} (cycle time: {self.cycle_time_seconds}s)")
        
        try:
            # Send execution signal to process
            self._signal_process_start(queued_process.process_id)
            
            # Wait for the cycle time or until stopped/paused
            cycle_end_time = time.time() + self.cycle_time_seconds
            
            while time.time() < cycle_end_time and not self._stop_event.is_set() and not self._pause_event.is_set():
                # Monitor process health during execution
                if not self._is_process_ready(queued_process):
                    self.logger.warning(f"Process {queued_process.process_id} became unhealthy during execution")
                    break
                
                time.sleep(1)  # Check every second
            
            # Signal process to pause/stop its cycle
            self._signal_process_pause(queued_process.process_id)
            
            # Update statistics
            cycle_duration = (datetime.now() - cycle_start).total_seconds()
            queued_process.last_execution_time = cycle_start
            queued_process.execution_count += 1
            queued_process.total_execution_time += cycle_duration
            
            self.logger.info(f"Completed execution cycle for {queued_process.process_id} (duration: {cycle_duration:.1f}s)")
            
        except Exception as e:
            self.logger.error(f"Error executing process cycle for {queued_process.process_id}: {e}")
        
        finally:
            self.current_process = None
    
    def _signal_process_start(self, process_id: str):
        """Signal a process to start its execution cycle"""
        # Send message via Redis to start execution
        try:
            redis_manager = self.redis_process_manager.redis_manager
            message = {
                'sender': 'queue_manager',
                'recipient': process_id,
                'message_type': 'start_cycle',
                'data': {
                    'cycle_time': self.cycle_time_seconds,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            from .trading_redis_manager import create_process_message
            process_message = create_process_message(**message)
            redis_manager.send_message(process_message, f"process_{process_id}")
            
            self.logger.debug(f"Sent start_cycle signal to {process_id}")
            
        except Exception as e:
            self.logger.error(f"Error signaling process start for {process_id}: {e}")
    
    def _signal_process_pause(self, process_id: str):
        """Signal a process to pause/complete its current cycle"""
        try:
            redis_manager = self.redis_process_manager.redis_manager
            message = {
                'sender': 'queue_manager',
                'recipient': process_id,
                'message_type': 'pause_cycle',
                'data': {
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            from .trading_redis_manager import create_process_message
            process_message = create_process_message(**message)
            redis_manager.send_message(process_message, f"process_{process_id}")
            
            self.logger.debug(f"Sent pause_cycle signal to {process_id}")
            
        except Exception as e:
            self.logger.error(f"Error signaling process pause for {process_id}: {e}")
    
    def _stop_current_process(self):
        """Stop the currently executing process"""
        if self.current_process:
            self._signal_process_pause(self.current_process.process_id)
    
    def _rotate_queue(self):
        """Rotate the queue (move first process to back)"""
        if self.process_queue:
            self.process_queue.rotate(-1)
            self.logger.debug(f"Rotated queue. Next process: {self.process_queue[0].process_id}")
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status and statistics"""
        queue_order = [p.process_id for p in self.process_queue]
        
        process_stats = {}
        for process_id, queued_process in self.process_lookup.items():
            avg_execution_time = (queued_process.total_execution_time / queued_process.execution_count 
                                if queued_process.execution_count > 0 else 0.0)
            
            process_stats[process_id] = {
                'execution_count': queued_process.execution_count,
                'total_execution_time': queued_process.total_execution_time,
                'avg_execution_time': avg_execution_time,
                'last_execution_time': queued_process.last_execution_time.isoformat() if queued_process.last_execution_time else None,
                'is_ready': self._is_process_ready(queued_process)
            }
        
        uptime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            'queue_state': self.queue_state.value,
            'current_process': self.current_process.process_id if self.current_process else None,
            'queue_order': queue_order,
            'process_count': len(self.process_queue),
            'total_cycles': self.total_cycles,
            'cycle_time_seconds': self.cycle_time_seconds,
            'uptime_seconds': uptime,
            'process_statistics': process_stats
        }
    
    def _enable_queue_mode_for_all_processes(self):
        """Enable queue mode for all processes in the rotation queue"""
        self.logger.info("Enabling queue mode for all processes in rotation")
        
        # Enable queue mode for regular processes
        for queued_process in self.process_queue:
            self._enable_queue_mode_for_process(queued_process.process_id)
        
        # Enable queue mode for high priority processes
        for process_id in self.high_priority_processes:
            self._enable_queue_mode_for_process(process_id)
        
        # Enable queue mode for always running processes
        for process_id in self.always_running_processes:
            self._enable_queue_mode_for_process(process_id)
    
    def _enable_queue_mode_for_process(self, process_id: str):
        """Enable queue mode for a specific process"""
        try:
            self.redis_process_manager.enable_queue_mode_for_process(process_id)
            self.logger.debug(f"Enabled queue mode for process {process_id}")
        except Exception as e:
            self.logger.error(f"Error enabling queue mode for {process_id}: {e}")
    
    def set_cycle_time(self, cycle_time_seconds: float):
        """Update the cycle time for process execution"""
        self.cycle_time_seconds = cycle_time_seconds

    # ===== ENHANCED PRIORITY-BASED QUEUE METHODS =====
    
    def _start_always_running_processes(self):
        """Start all ALWAYS_RUNNING processes (position_health_monitor)"""
        for process_id, queued_process in self.always_running_processes.items():
            if not queued_process.is_currently_running:
                self._start_process(queued_process)
                self.logger.info(f"Started ALWAYS_RUNNING process: {process_id}")
    
    def _ensure_priority_processes_running(self):
        """Ensure ALWAYS_RUNNING and HIGH_PRIORITY processes are running"""
        # Check ALWAYS_RUNNING processes
        for process_id, queued_process in self.always_running_processes.items():
            if not queued_process.is_currently_running:
                self._start_process(queued_process)
                self.logger.info(f"Restarted ALWAYS_RUNNING process: {process_id}")
        
        # Check HIGH_PRIORITY processes - start if we have capacity
        if self._can_start_more_processes():
            for process_id, queued_process in self.high_priority_processes.items():
                if not queued_process.is_currently_running:
                    self._start_process(queued_process)
                    self.logger.info(f"Started HIGH_PRIORITY process: {process_id}")
                    break  # Only start one high priority process per cycle
    
    def _can_start_more_processes(self) -> bool:
        """Check if we can start more processes based on concurrent limit and TOTAL memory health"""
        running_count = len(self.current_running_processes)
        concurrent_limit_ok = running_count < self.max_concurrent_processes
        
        # CRITICAL FIX: Check TOTAL system memory before allowing new processes
        memory_healthy = True
        try:
            import psutil
            main_process = psutil.Process()
            total_memory_mb = main_process.memory_info().rss / 1024 / 1024
            
            # Add memory from all child processes
            for child in main_process.children(recursive=True):
                try:
                    total_memory_mb += child.memory_info().rss / 1024 / 1024
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # Block new processes if we're approaching 90% of target memory
            memory_threshold = self.target_memory_mb * 0.9
            if total_memory_mb > memory_threshold:
                memory_healthy = False
                self.logger.warning(f"Cannot start more processes: Total system memory {total_memory_mb:.1f}MB > {memory_threshold:.1f}MB (90% of {self.target_memory_mb}MB)")
        except Exception as e:
            self.logger.debug(f"Could not check system memory: {e}")
        
        can_start = concurrent_limit_ok and memory_healthy
        
        if not concurrent_limit_ok:
            self.logger.debug(f"Cannot start more processes: {running_count}/{self.max_concurrent_processes} running")
        elif not memory_healthy:
            self.logger.info(f"Cannot start more processes: High memory usage in system")
        
        return can_start
    
    def _get_next_normal_process(self) -> Optional[QueuedProcess]:
        """Get the next NORMAL priority process to run"""
        if not self.process_queue:
            return None
        
        # Find the first process that's not currently running
        for _ in range(len(self.process_queue)):
            queued_process = self.process_queue[0]
            if not queued_process.is_currently_running:
                return queued_process
            # Rotate to check next process
            self.process_queue.rotate(-1)
        
        return None
    
    def _start_process(self, queued_process: QueuedProcess):
        """Start a process and track it as running"""
        try:
            # Start the actual process via Redis process manager
            success = self.redis_process_manager._start_process(queued_process.process_id)
            
            if success:
                queued_process.is_currently_running = True
                queued_process.last_execution_time = datetime.now()
                queued_process.execution_count += 1
                self.current_running_processes[queued_process.process_id] = queued_process
                self.logger.info(f"Started process: {queued_process.process_id} ({queued_process.priority.value})")
            else:
                self.logger.error(f"Failed to start process: {queued_process.process_id}")
                
        except Exception as e:
            self.logger.error(f"Error starting process {queued_process.process_id}: {e}")
    
    def _stop_process(self, queued_process: QueuedProcess):
        """Stop a process and remove it from running tracking"""
        try:
            # Stop the actual process via Redis process manager
            self.redis_process_manager._stop_process(queued_process.process_id)
            
            queued_process.is_currently_running = False
            if queued_process.process_id in self.current_running_processes:
                del self.current_running_processes[queued_process.process_id]
            
            # Calculate execution time
            if queued_process.last_execution_time:
                execution_time = (datetime.now() - queued_process.last_execution_time).total_seconds()
                queued_process.total_execution_time += execution_time
            
            self.logger.info(f"Stopped process: {queued_process.process_id}")
            
        except Exception as e:
            self.logger.error(f"Error stopping process {queued_process.process_id}: {e}")
    
    def _check_for_finished_processes(self):
        """Check for processes that have completed their business cycles and can be rotated"""
        finished_processes = []
        
        for process_id, queued_process in self.current_running_processes.items():
            # Only check NORMAL priority processes for rotation
            if queued_process.priority != ProcessPriority.NORMAL:
                continue
                
            # Check if process has finished its business cycle via Redis
            try:
                # Check process health/status from Redis
                health_data = self.redis_process_manager.redis_manager.get_process_health(process_id)
                
                # Look for indicators that the business cycle is complete
                if health_data:
                    cycle_status = health_data.get('cycle_status', 'running')
                    last_activity = health_data.get('last_activity')
                    
                    # If cycle is marked as complete, or no activity for extended period
                    if (cycle_status == 'complete' or 
                        (last_activity and self._is_process_idle(last_activity))):
                        
                        finished_processes.append(queued_process)
                        self.logger.info(f"Process {process_id} completed business cycle - ready for rotation")
                
            except Exception as e:
                self.logger.debug(f"Could not check cycle status for {process_id}: {e}")
        
        # Rotate finished processes
        for queued_process in finished_processes:
            self._rotate_process(queued_process)
    
    def _is_process_idle(self, last_activity_str: str) -> bool:
        """Check if process has been idle for rotation threshold"""
        try:
            last_activity = datetime.fromisoformat(last_activity_str)
            idle_time = (datetime.now() - last_activity).total_seconds()
            
            # Consider process idle if no activity for 2x cycle time
            idle_threshold = self.cycle_time_seconds * 2
            return idle_time >= idle_threshold
            
        except (ValueError, TypeError):
            return False
    
    def _rotate_process(self, queued_process: QueuedProcess):
        """Rotate a process: stop it and move to back of queue"""
        self.logger.info(f"Rotating process: {queued_process.process_id}")
        
        # Stop the process
        self._stop_process(queued_process)
        
        # Move to back of queue for future execution
        if queued_process in self.process_queue:
            self.process_queue.remove(queued_process)
            self.process_queue.append(queued_process)
        
        self.rotation_count += 1
        
        # Immediately try to fill the newly available slot
        self._fill_remaining_slots()
    
    def _fill_remaining_slots(self):
        """Fill available process slots with waiting NORMAL processes"""
        while self._can_start_more_processes():
            next_process = self._get_next_normal_process()
            if next_process:
                self._start_process(next_process)
                self.logger.info(f"Filled slot with process: {next_process.process_id}")
            else:
                # No more processes waiting
                break
    
    def _check_memory_and_adjust(self):
        """Monitor memory usage and stop processes if needed"""
        try:
            import psutil
            
            # CRITICAL FIX: Check TOTAL system memory including all subprocesses
            main_process = psutil.Process()
            total_memory_mb = main_process.memory_info().rss / 1024 / 1024
            
            # Add memory from all child processes (subprocesses)
            try:
                for child in main_process.children(recursive=True):
                    try:
                        child_memory = child.memory_info().rss / 1024 / 1024
                        total_memory_mb += child_memory
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue  # Child may have terminated
            except Exception as e:
                self.logger.debug(f"Error getting child processes memory: {e}")
            
            # Log total system memory usage
            self.logger.info(f"TOTAL SYSTEM MEMORY: {total_memory_mb:.1f}MB (target: {self.target_memory_mb}MB)")
            
            if total_memory_mb > self.target_memory_mb:
                self.logger.warning(f"Memory usage high: {total_memory_mb:.1f}MB > {self.target_memory_mb}MB")
                
                # Stop lowest priority running processes first
                normal_running = [p for p in self.current_running_processes.values() 
                                 if p.priority == ProcessPriority.NORMAL]
                
                if normal_running:
                    # Stop the process that's been running longest
                    oldest_process = min(normal_running, 
                                       key=lambda p: p.last_execution_time or datetime.min)
                    self.logger.warning(f"Stopping {oldest_process.process_id} due to memory pressure")
                    self._stop_process(oldest_process)
            
            # Update last memory check time
            self.last_memory_check = datetime.now()
            
        except Exception as e:
            self.logger.error(f"Error checking memory usage: {e}")
    
    def get_enhanced_queue_status(self) -> Dict[str, Any]:
        """Get enhanced queue status with priority information"""
        status = self.get_queue_status()
        
        status.update({
            'always_running_processes': list(self.always_running_processes.keys()),
            'high_priority_processes': list(self.high_priority_processes.keys()),
            'currently_running_processes': list(self.current_running_processes.keys()),
            'max_concurrent_processes': self.max_concurrent_processes,
            'target_memory_mb': self.target_memory_mb,
            'rotation_count': self.rotation_count
        })
        
        return status
        self.logger.info(f"Updated cycle time to {cycle_time_seconds}s")
    
    def reorder_queue(self, new_order: List[str]):
        """Manually reorder the process queue"""
        if set(new_order) != set(self.process_lookup.keys()):
            raise ValueError("New order must contain all current processes")
        
        new_queue = deque()
        for process_id in new_order:
            new_queue.append(self.process_lookup[process_id])
        
        self.process_queue = new_queue
        self.logger.info(f"Reordered queue: {new_order}")