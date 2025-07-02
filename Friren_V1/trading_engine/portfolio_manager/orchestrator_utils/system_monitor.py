"""
system_monitor.py

System monitoring, health checks, and resource metrics
"""

import time
import threading
import logging
from datetime import datetime
from typing import Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


class SystemMonitor:
    """Handles system monitoring, health checks, and resource metrics"""

    def __init__(self, config, status, logger):
        self.config = config
        self.status = status
        self.logger = logger
        self.shutdown_event = threading.Event()
        self.monitoring_thread: Optional[threading.Thread] = None

        # References to other components (set by orchestrator)
        self.process_manager = None
        self.account_manager = None
        self.redis_manager = None
        self.emergency_manager = None
        self.get_system_status_func = None

    def set_components(self, process_manager=None, account_manager=None, redis_manager=None,
                      emergency_manager=None, get_system_status_func=None):
        """Set references to other components"""
        self.process_manager = process_manager
        self.account_manager = account_manager
        self.redis_manager = redis_manager  # Redis replaces shared_state
        self.emergency_manager = emergency_manager
        self.get_system_status_func = get_system_status_func

    def start_monitoring(self):
        """Start system monitoring thread"""
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        self.logger.info("System monitoring started")

    def stop_monitoring(self):
        """Stop system monitoring"""
        self.shutdown_event.set()
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=10)

    def _monitoring_loop(self):
        """Main monitoring loop for system health and metrics"""
        while not self.shutdown_event.is_set():
            try:
                # Update system metrics and status
                self.update_system_metrics()

                # Check system health
                self.check_system_health()

                # Update shared state
                self.update_shared_state()

                # Log system status
                self.log_system_status()

                # Simulate process work for detailed logging
                try:
                    if hasattr(self.process_manager, 'simulate_all_processes'):
                        self.process_manager.simulate_all_processes()
                except Exception as e:
                    self.logger.error(f"Error in process simulation: {e}")

                # Check for emergency conditions
                try:
                    if self.emergency_manager and self.emergency_manager.check_emergency_conditions():
                        self.logger.critical("Emergency conditions detected - initiating emergency stop")
                        self.emergency_manager.emergency_stop_all_trades()
                        break
                except Exception as e:
                    self.logger.error(f"Error checking emergency conditions: {e}")

                # Update health check timestamp
                self.status.last_health_check = datetime.now()

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")

            # Sleep for health check interval
            time.sleep(self.config.health_check_interval)

    def update_system_metrics(self):
        """Update system resource metrics (NO DATABASE CALLS)"""
        try:
            if PSUTIL_AVAILABLE:
                process = psutil.Process()

                # Get memory info (this is fast and safe)
                memory_info = process.memory_info()
                self.status.memory_usage_mb = memory_info.rss / 1024 / 1024

                # Get CPU usage with proper interval for accurate readings
                # Use interval=1.0 for the first reading to establish baseline
                try:
                    # Get system-wide CPU usage instead of just this process for more meaningful metrics
                    self.status.cpu_usage_percent = psutil.cpu_percent(interval=1.0)

                    # Also get this process CPU for detailed monitoring
                    process_cpu = process.cpu_percent(interval=None)

                    self.logger.debug(f"System CPU: {self.status.cpu_usage_percent:.1f}%, Process CPU: {process_cpu:.1f}%")
                except:
                    # Fallback to process CPU if system CPU fails
                    try:
                        self.status.cpu_usage_percent = process.cpu_percent(interval=1.0)
                    except:
                        self.status.cpu_usage_percent = 0.0

                self.logger.debug(f"System metrics updated: Memory={self.status.memory_usage_mb:.1f}MB, CPU={self.status.cpu_usage_percent:.1f}%")

            else:
                # Fallback if psutil not available - use basic system info
                import sys
                import gc
                gc.collect()

                # Simple memory estimation
                total_size = 0
                try:
                    for obj in gc.get_objects()[:1000]:  # Sample first 1000 objects to avoid hanging
                        total_size += sys.getsizeof(obj)
                    self.status.memory_usage_mb = total_size / 1024 / 1024
                except:
                    self.status.memory_usage_mb = 0.1  # Fallback value

                self.status.cpu_usage_percent = 0.0  # Can't get CPU without psutil
                self.logger.debug(f"System metrics updated (fallback): Memory={self.status.memory_usage_mb:.1f}MB, CPU=N/A")

        except Exception as e:
            self.logger.warning(f"Failed to update system metrics: {e}")
            # Set safe fallback values
            self.status.memory_usage_mb = 0.1
            self.status.cpu_usage_percent = 0.0

    def update_account_data_on_demand(self):
        """Update account data only when specifically requested (for trades/startup)"""
        try:
            if self.account_manager:
                # Get account snapshot (will use cache if fresh)
                snapshot = self.account_manager.get_account_snapshot()

                # Update system status with account data
                self.status.portfolio_value = snapshot.portfolio_value
                self.status.cash_available = snapshot.cash
                self.status.buying_power = snapshot.buying_power
                self.status.day_pnl = snapshot.day_pnl
                self.status.day_pnl_pct = snapshot.day_pnl_pct
                self.status.total_pnl = snapshot.total_pnl
                self.status.last_account_sync = snapshot.timestamp

                # Check account health
                is_healthy, message = self.account_manager.is_account_healthy()
                self.status.account_healthy = is_healthy
                self.status.account_health_message = message

                self.logger.info("Account data updated on demand")

        except Exception as e:
            self.logger.error(f"Error updating account data on demand: {e}")
            self.status.account_healthy = False
            self.status.account_health_message = f"Update failed: {e}"

    def check_system_health(self):
        """Check overall system health"""
        if self.process_manager:
            status = self.process_manager.get_process_status()
            failed_processes = [
                pid for pid, pstatus in status.items()
                if not pstatus.get('is_healthy', False)
            ]

            if failed_processes:
                self.logger.warning(f"Unhealthy processes: {failed_processes}")

        # Update process status counts
        if self.process_manager:
            process_status = self.process_manager.get_process_status()
            self.status.total_processes = len(process_status)
            self.status.healthy_processes = sum(
                1 for p in process_status.values()
                if p.get('is_healthy', False)
            )
            self.status.failed_processes = self.status.total_processes - self.status.healthy_processes

    def update_shared_state(self):
        """Update shared state with system information"""
        if self.redis_manager and self.get_system_status_func:
            # Update Redis with system status
            system_status = self.get_system_status_func()
            self.redis_manager.set_shared_state('system_status', system_status)
            self.redis_manager.set_shared_state('last_update', datetime.now().isoformat())

    def log_system_status(self):
        """Log periodic system status"""
        if not self.get_system_status_func:
            return

        status = self.get_system_status_func()

        # Log basic system status
        self.logger.info(
            f"System Status: {status['system_state']} | "
            f"Processes: {status['processes']['healthy']}/{status['processes']['total']} | "
            f"Trades: {status['trading']['trades_today']} | "
            f"PnL: {status['trading']['total_pnl']:.2f}% | "
            f"Memory: {status['resources']['memory_usage_mb']:.1f}MB"
        )

        # Log detailed process status
        if self.process_manager and hasattr(self.process_manager, 'get_process_status'):
            process_status = self.process_manager.get_process_status()
            running_processes = []
            queued_processes = []

            for process_id, proc_status in process_status.items():
                if proc_status.get('is_running', False):
                    uptime = proc_status.get('uptime_seconds', 0)
                    health = "HEALTHY" if proc_status.get('is_healthy', False) else "UNHEALTHY"
                    running_processes.append(f"{process_id}({health}, {uptime:.0f}s)")
                else:
                    queued_processes.append(process_id)

            if running_processes:
                self.logger.info(f"RUNNING PROCESSES: {', '.join(running_processes)}")
            if queued_processes:
                self.logger.info(f"QUEUED PROCESSES: {', '.join(queued_processes)}")

        # Log recent decision activity
        if self.status.last_decision_time:
            time_since_decision = (datetime.now() - self.status.last_decision_time).total_seconds()
            self.logger.info(f"LAST DECISION: {time_since_decision:.0f}s ago")
