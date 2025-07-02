"""
portfolio_manager/tools/multiprocess_manager.py

Pure multiprocess infrastructure - generic task execution with no business logic.
Optimized for EC2 t3.micro constraints with resource-aware worker allocation.
"""

import multiprocessing as mp
import sys

# Windows-specific multiprocessing configuration
if sys.platform == "win32":
    try:
        if mp.get_start_method(allow_none=True) is None:
            mp.set_start_method('spawn', force=False)
    except RuntimeError:
        pass
    mp.freeze_support()
import queue
import time
import psutil
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
import logging


@dataclass
class TaskResult:
    """Standardized result from multiprocess task execution"""
    task_id: str
    task_type: str
    success: bool
    data: Dict[str, Any]
    processing_time: float
    error: Optional[str] = None
    worker_id: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class MultiprocessManager:
    """
    Pure Multiprocess Infrastructure Manager

    Provides generic parallel task execution with:
    - Resource-aware worker allocation (optimized for t3.micro)
    - Automatic task distribution and result collection
    - Error handling and timeout management
    - Worker lifecycle management
    - No business logic - pure infrastructure

    Usage:
        manager = MultiprocessManager()
        results = manager.execute_tasks_parallel(tasks, worker_function)
    """

    def __init__(self, max_workers: Optional[int] = None, max_tasks: Optional[int] = None):
        self.logger = logging.getLogger("multiprocess_manager")

        # Auto-detect system resources
        self.memory_gb = self._get_memory_gb()
        self.cpu_count = mp.cpu_count()

        # Configure workers and task limits based on system resources
        self.num_workers, self.max_tasks = self._configure_resources(max_workers, max_tasks)

        # Multiprocess components
        self.task_queue = None
        self.result_queue = None
        self.workers = []

        # Statistics
        self.stats = {
            'tasks_executed': 0,
            'tasks_successful': 0,
            'tasks_failed': 0,
            'total_processing_time': 0.0,
            'average_task_time': 0.0,
            'worker_restarts': 0
        }

        self.logger.info(f"MultiprocessManager initialized - {self.num_workers} workers, max {self.max_tasks} tasks")
        self.logger.info(f"System: {self.memory_gb:.1f}GB RAM, {self.cpu_count} CPUs")

    def _get_memory_gb(self) -> float:
        """Get total system memory in GB"""
        try:
            return psutil.virtual_memory().total / (1024**3)
        except Exception:
            self.logger.warning("Could not detect memory, assuming 4GB")
            return 4.0

    def _configure_resources(self, max_workers: Optional[int], max_tasks: Optional[int]) -> tuple[int, int]:
        """Configure worker and task limits based on system resources"""

        # Default resource allocation based on detected hardware
        if self.memory_gb < 1:  # t3.nano/micro
            default_workers = 1
            default_max_tasks = 20
        elif self.memory_gb < 4:  # t3.small
            default_workers = 2
            default_max_tasks = 50
        elif self.memory_gb < 8:  # t3.medium
            default_workers = min(3, self.cpu_count)
            default_max_tasks = 100
        else:  # t3.large+
            default_workers = min(4, self.cpu_count)
            default_max_tasks = 200

        # Use provided limits or defaults
        workers = max_workers if max_workers is not None else default_workers
        tasks = max_tasks if max_tasks is not None else default_max_tasks

        # Ensure we don't exceed system capabilities
        workers = min(workers, self.cpu_count, 8)  # Max 8 workers regardless

        return workers, tasks

    def execute_tasks_parallel(self,
                              tasks: List[Dict[str, Any]],
                              worker_function: Callable,
                              timeout: int = 60) -> List[TaskResult]:
        """
        Execute tasks in parallel using the provided worker function

        Args:
            tasks: List of task dictionaries to process
            worker_function: Function that workers will call to process tasks
            timeout: Maximum time to wait for all tasks to complete

        Returns:
            List of TaskResult objects with results from all tasks
        """
        if not tasks:
            self.logger.warning("No tasks provided for parallel execution")
            return []

        if len(tasks) > self.max_tasks:
            self.logger.warning(f"Task count ({len(tasks)}) exceeds maximum ({self.max_tasks}), limiting")
            tasks = tasks[:self.max_tasks]

        self.logger.info(f"Executing {len(tasks)} tasks across {self.num_workers} workers")
        start_time = time.time()

        try:
            # Initialize queues
            self._initialize_queues()

            # Start workers
            self._start_workers(worker_function)

            # Submit tasks
            task_count = self._submit_tasks(tasks)

            # Collect results
            results = self._collect_results(task_count, timeout)

            # Update statistics
            total_time = time.time() - start_time
            self._update_stats(results, total_time)

            self.logger.info(f"Parallel execution complete - {len(results)}/{task_count} tasks processed in {total_time:.2f}s")

            return results

        except Exception as e:
            self.logger.error(f"Error in parallel execution: {e}")
            return []
        finally:
            # Always cleanup workers and queues
            self._shutdown_workers()
            self._cleanup_queues()

    def _initialize_queues(self):
        """Initialize multiprocess queues"""
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()

    def _start_workers(self, worker_function: Callable):
        """Start worker processes"""
        for i in range(self.num_workers):
            worker = mp.Process(
                target=self._generic_worker_wrapper,
                args=(self.task_queue, self.result_queue, worker_function, f"worker-{i}"),
                name=f"MPWorker-{i}"
            )
            worker.start()
            self.workers.append(worker)

        self.logger.debug(f"Started {len(self.workers)} worker processes")

    def _generic_worker_wrapper(self,
                               task_queue: mp.Queue,
                               result_queue: mp.Queue,
                               worker_function: Callable,
                               worker_id: str):
        """
        Generic worker wrapper that calls the provided worker function

        This wrapper handles:
        - Task queue management
        - Error handling
        - Result standardization
        - Graceful shutdown
        """
        while True:
            try:
                # Get task from queue
                task = task_queue.get(timeout=2)
                if task is None:  # Shutdown signal
                    break

                start_time = time.time()
                task_id = task.get('task_id', f"{worker_id}-{int(time.time())}")
                task_type = task.get('task_type', 'unknown')

                try:
                    # Call the provided worker function
                    result_data = worker_function(task)

                    # Create standardized result
                    result = TaskResult(
                        task_id=task_id,
                        task_type=task_type,
                        success=True,
                        data=result_data,
                        processing_time=time.time() - start_time,
                        worker_id=worker_id
                    )

                except Exception as e:
                    # Handle task processing errors
                    result = TaskResult(
                        task_id=task_id,
                        task_type=task_type,
                        success=False,
                        data={},
                        processing_time=time.time() - start_time,
                        error=str(e),
                        worker_id=worker_id
                    )

                # Send result back
                result_queue.put(result)

            except queue.Empty:
                # Timeout waiting for task - continue loop
                continue
            except Exception as e:
                # Worker-level error
                logging.error(f"Worker {worker_id} error: {e}")
                continue

    def _submit_tasks(self, tasks: List[Dict[str, Any]]) -> int:
        """Submit tasks to the task queue"""
        task_count = 0

        for i, task in enumerate(tasks):
            # Ensure task has required fields
            if 'task_id' not in task:
                task['task_id'] = f"task-{i}-{int(time.time())}"
            if 'task_type' not in task:
                task['task_type'] = 'generic'

            # Clean task data for pickle compatibility
            try:
                cleaned_task = self._clean_task_for_pickle(task)
                self.task_queue.put(cleaned_task)
                task_count += 1
            except Exception as e:
                self.logger.warning(f"Failed to clean task {i} for pickle: {e}")
                # Try submitting original task anyway
                self.task_queue.put(task)
                task_count += 1

        self.logger.debug(f"Submitted {task_count} tasks to queue")
        return task_count

    def _clean_task_for_pickle(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Clean task data to avoid pickle serialization errors"""
        import pandas as pd
        import copy
        
        cleaned_task = copy.deepcopy(task)
        
        # Clean any pandas DataFrames in the task
        for key, value in cleaned_task.items():
            if isinstance(value, pd.DataFrame):
                # Create clean DataFrame without weak references
                cleaned_task[key] = pd.DataFrame(
                    data=value.values,
                    index=value.index.copy(),
                    columns=value.columns.copy()
                )
            elif isinstance(value, dict):
                # Recursively clean nested dictionaries
                cleaned_task[key] = self._clean_dict_for_pickle(value)
        
        return cleaned_task
    
    def _clean_dict_for_pickle(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively clean dictionary data for pickle compatibility"""
        import pandas as pd
        import copy
        
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, pd.DataFrame):
                cleaned[key] = pd.DataFrame(
                    data=value.values,
                    index=value.index.copy(),
                    columns=value.columns.copy()
                )
            elif isinstance(value, dict):
                cleaned[key] = self._clean_dict_for_pickle(value)
            else:
                cleaned[key] = copy.deepcopy(value)
        
        return cleaned

    def _collect_results(self, expected_count: int, timeout: int) -> List[TaskResult]:
        """Collect results from workers"""
        results = []
        collected = 0
        start_time = time.time()

        while collected < expected_count and (time.time() - start_time) < timeout:
            try:
                result = self.result_queue.get(timeout=1)
                results.append(result)
                collected += 1

                # Log progress for large task sets
                if collected % 10 == 0 or collected == expected_count:
                    self.logger.debug(f"Collected {collected}/{expected_count} results")

            except queue.Empty:
                # Check if any workers are still alive
                alive_workers = sum(1 for w in self.workers if w.is_alive())
                if alive_workers == 0:
                    self.logger.warning("All workers died, stopping result collection")
                    break
                continue

        if collected < expected_count:
            self.logger.warning(f"Only collected {collected}/{expected_count} results (timeout: {timeout}s)")

        return results

    def _shutdown_workers(self):
        """Shutdown all worker processes gracefully"""
        if not self.workers:
            return

        # Send shutdown signals
        for _ in self.workers:
            try:
                self.task_queue.put(None)
            except Exception:
                pass

        # Wait for graceful shutdown
        for worker in self.workers:
            worker.join(timeout=5)
            if worker.is_alive():
                self.logger.warning(f"Force terminating worker {worker.name}")
                worker.terminate()
                worker.join(timeout=2)
                self.stats['worker_restarts'] += 1

        self.workers.clear()
        self.logger.debug("All workers shutdown")

    def _cleanup_queues(self):
        """Clean up multiprocess queues"""
        try:
            if self.task_queue:
                self.task_queue.close()
                self.task_queue = None
            if self.result_queue:
                self.result_queue.close()
                self.result_queue = None
        except Exception as e:
            self.logger.warning(f"Error cleaning up queues: {e}")

    def _update_stats(self, results: List[TaskResult], total_time: float):
        """Update execution statistics"""
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        avg_task_time = sum(r.processing_time for r in results) / len(results) if results else 0

        self.stats.update({
            'tasks_executed': self.stats['tasks_executed'] + len(results),
            'tasks_successful': self.stats['tasks_successful'] + successful,
            'tasks_failed': self.stats['tasks_failed'] + failed,
            'total_processing_time': self.stats['total_processing_time'] + total_time,
            'average_task_time': avg_task_time
        })

    # Public utility methods

    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics"""
        stats = self.stats.copy()
        stats.update({
            'success_rate': (stats['tasks_successful'] / stats['tasks_executed'] * 100)
                          if stats['tasks_executed'] > 0 else 0,
            'system_info': {
                'memory_gb': self.memory_gb,
                'cpu_count': self.cpu_count,
                'configured_workers': self.num_workers,
                'max_tasks': self.max_tasks
            }
        })
        return stats

    def reset_stats(self):
        """Reset execution statistics"""
        self.stats = {
            'tasks_executed': 0,
            'tasks_successful': 0,
            'tasks_failed': 0,
            'total_processing_time': 0.0,
            'average_task_time': 0.0,
            'worker_restarts': 0
        }
        self.logger.info("Statistics reset")

    def is_healthy(self) -> bool:
        """Check if the manager is in a healthy state"""
        if self.stats['tasks_executed'] == 0:
            return True  # No tasks yet, considered healthy

        success_rate = self.stats['tasks_successful'] / self.stats['tasks_executed']
        return success_rate >= 0.8  # 80% success rate threshold

    def get_resource_info(self) -> Dict[str, Any]:
        """Get system resource information"""
        try:
            memory = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent(interval=1)

            return {
                'memory_total_gb': memory.total / (1024**3),
                'memory_available_gb': memory.available / (1024**3),
                'memory_percent_used': memory.percent,
                'cpu_percent': cpu_percent,
                'cpu_count': self.cpu_count,
                'configured_workers': self.num_workers,
                'max_tasks': self.max_tasks,
                'recommended_for': self._get_instance_type()
            }
        except Exception as e:
            return {'error': f"Could not get resource info: {e}"}

    def _get_instance_type(self) -> str:
        """Guess EC2 instance type based on resources"""
        if self.memory_gb < 1:
            return "t3.nano/micro"
        elif self.memory_gb < 4:
            return "t3.small"
        elif self.memory_gb < 8:
            return "t3.medium"
        else:
            return "t3.large+"


# Example worker functions for common use cases

def example_analysis_worker(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Example worker function for analysis tasks

    Your worker functions should follow this pattern:
    - Take a task dictionary as input
    - Return a result dictionary
    - Handle their own errors (or let them bubble up)
    """
    symbol = task.get('symbol', 'UNKNOWN')
    task_type = task.get('task_type', 'analysis')

    # Simulate some processing
    time.sleep(0.1)

    return {
        'symbol': symbol,
        'task_type': task_type,
        'result': f"Processed {symbol} for {task_type}",
        'value': 42
    }


if __name__ == "__main__":
    # Example usage
    manager = MultiprocessManager()

    # Create some example tasks
    tasks = [
        {'symbol': 'AAPL', 'task_type': 'analysis'},
        {'symbol': 'GOOGL', 'task_type': 'analysis'},
        {'symbol': 'MSFT', 'task_type': 'analysis'},
    ]

    # Execute tasks
    results = manager.execute_tasks_parallel(tasks, example_analysis_worker)

    # Print results
    print(f"\nExecuted {len(results)} tasks:")
    for result in results:
        status = "" if result.success else ""
        print(f"{status} {result.task_id}: {result.data.get('result', 'No result')}")

    # Print stats
    print(f"\nStats: {manager.get_stats()}")
