#!/usr/bin/env python3
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
from dataclasses import dataclass
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
class QueuedProcess:
    """Represents a process in the rotation queue"""
    process_id: str
    config: ProcessConfig
    status: ProcessStatus
    last_execution_time: Optional[datetime] = None
    execution_count: int = 0
    total_execution_time: float = 0.0
    is_ready: bool = True

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
    
    def __init__(self, redis_process_manager: RedisProcessManager, cycle_time_seconds: float = 30.0):
        """
        Initialize the process queue manager.
        
        Args:
            redis_process_manager: The underlying Redis process manager
            cycle_time_seconds: How long each process gets to execute before rotation
        """
        self.redis_process_manager = redis_process_manager
        self.cycle_time_seconds = cycle_time_seconds
        
        # Queue management
        self.process_queue: deque[QueuedProcess] = deque()
        self.process_lookup: Dict[str, QueuedProcess] = {}
        self.current_process: Optional[QueuedProcess] = None
        
        # State management
        self.queue_state = QueueState.STOPPED
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._queue_thread: Optional[threading.Thread] = None
        
        # Statistics
        self.total_cycles = 0
        self.start_time: Optional[datetime] = None
        
        # Logging
        self.logger = logging.getLogger("process_queue_manager")
        self.logger.info(f"ProcessQueueManager initialized with cycle_time={cycle_time_seconds}s")
    
    def add_process(self, process_id: str, config: ProcessConfig, status: ProcessStatus):
        """Add a process to the rotation queue"""
        if process_id in self.process_lookup:
            self.logger.warning(f"Process {process_id} already in queue")
            return
        
        queued_process = QueuedProcess(
            process_id=process_id,
            config=config,
            status=status
        )
        
        self.process_queue.append(queued_process)
        self.process_lookup[process_id] = queued_process
        
        self.logger.info(f"Added process {process_id} to rotation queue (position {len(self.process_queue)})")
    
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
        """Main queue rotation loop"""
        self.logger.info("Queue rotation loop started")
        
        try:
            while not self._stop_event.is_set():
                # Check if paused
                if self._pause_event.is_set():
                    self.logger.debug("Queue rotation paused, waiting...")
                    self._pause_event.wait()
                    continue
                
                # Get next process in queue
                next_process = self._get_next_process()
                if not next_process:
                    self.logger.warning("No available processes in queue")
                    time.sleep(5)
                    continue
                
                # Execute the process for its cycle time
                self._execute_process_cycle(next_process)
                
                # Rotate queue (move current process to back)
                self._rotate_queue()
                
                self.total_cycles += 1
                
                # Brief pause between cycles
                time.sleep(1)
                
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
    
    def set_cycle_time(self, cycle_time_seconds: float):
        """Update the cycle time for process execution"""
        self.cycle_time_seconds = cycle_time_seconds
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