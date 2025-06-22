#!/usr/bin/env python3
"""
orchestrator_refactored.py

Refactored Main Trading System Orchestrator - Central Nervous System

The Master Orchestrator coordinates all 5 processes and manages the complete
trading workflow from market data through decision making to trade execution.

Now modularized using orchestrator_utils for maintainability.
"""

import sys
import os
import time
import signal
import threading
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import the modular utilities
from .orchestrator_utils import (
    SystemState, TradingMode, SystemConfig, SystemStatus,
    SystemMonitor, DecisionCoordinator, EmergencyManager,
    ProcessInitializer, SymbolCoordinationIntegration
)

# Windows-specific: Add multiprocessing protection
if sys.platform == "win32":
    import multiprocessing as mp
    if __name__ == "__main__":
        mp.freeze_support()


class MainOrchestrator:
    """
    Main Trading System Orchestrator

    The central nervous system that coordinates all trading system components.
    Manages the complete lifecycle from startup to shutdown, coordinates
    the 5 core processes, and handles trade execution workflow.

    Now uses modular utilities for better maintainability.
    """

    def __init__(self, config: Optional[SystemConfig] = None):
        """Initialize the main orchestrator"""
        self.config = config or SystemConfig()
        self.status = SystemStatus()

        # Setup logging first
        self.logger = self._setup_logging()

        # Initialize utility modules
        self.system_monitor = SystemMonitor(self.config, self.status, self.logger)
        self.decision_coordinator = DecisionCoordinator(self.config, self.status, self.logger)
        self.emergency_manager = EmergencyManager(self.config, self.status, self.logger)
        self.process_initializer = ProcessInitializer(self.config, self.status, self.logger)
        self.symbol_coordination = SymbolCoordinationIntegration(self.config, self.status, self.logger)

        # Component references (initialized by process_initializer)
        self.process_manager = None
        self.execution_orchestrator = None
        self.shared_state = None
        self.queue_manager = None
        self.account_manager = None
        self.symbol_coordinator = None
        self.resource_manager = None
        self.message_router = None

        # Control and monitoring
        self._shutdown_event = threading.Event()
        self._main_thread: Optional[threading.Thread] = None

        # Register signal handlers
        if sys.platform != "win32":
            signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)  # SIGINT works on Windows

        self.logger.info("MainOrchestrator initialized")
        self.logger.info(f"Trading mode: {self.config.trading_mode.value}")
        self.logger.info(f"Symbols: {self.config.symbols}")

    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging"""
        logger = logging.getLogger("main_orchestrator")
        logger.setLevel(logging.INFO)

        # Create logs directory if not exists
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

        # File handler
        file_handler = logging.FileHandler(logs_dir / "main_orchestrator.log")
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        return logger

    def initialize_system(self):
        """Initialize all system components"""
        self.logger.info("Initializing trading system...")
        self.status.state = SystemState.INITIALIZING

        try:
            # Initialize all components through process initializer
            self.process_initializer.initialize_all_components()

            # Get initialized components
            components = self.process_initializer.get_components()
            self.process_manager = components['process_manager']
            self.account_manager = components['account_manager']
            self.execution_orchestrator = components['execution_orchestrator']
            self.shared_state = components['shared_state']
            self.queue_manager = components['queue_manager']

            # Initialize symbol coordination
            self.symbol_coordination.initialize_symbol_coordination()

            # Get symbol coordination components
            symbol_components = self.symbol_coordination.get_components()
            self.symbol_coordinator = symbol_components['symbol_coordinator']
            self.resource_manager = symbol_components['resource_manager']
            self.message_router = symbol_components['message_router']

            # Wire components to utility modules
            self._wire_components()

            self.logger.info("System initialization complete")

        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            self.status.state = SystemState.ERROR
            raise

    def _wire_components(self):
        """Wire components between utility modules"""
        # Set components for system monitor
        self.system_monitor.set_components(
            process_manager=self.process_manager,
            account_manager=self.account_manager,
            shared_state=self.shared_state,
            emergency_manager=self.emergency_manager,
            get_system_status_func=self.get_system_status
        )

        # Set components for decision coordinator
        self.decision_coordinator.set_components(
            process_manager=self.process_manager,
            execution_orchestrator=self.execution_orchestrator,
            symbol_coordinator=self.symbol_coordinator,
            message_router=self.message_router
        )

    def start_system(self):
        """Start the complete trading system"""
        self.logger.info("Starting trading system...")
        self.status.state = SystemState.STARTING

        try:
            # Start all processes
            self.process_initializer.start_processes()

            # Start main orchestration loop
            self._start_main_loop()

            # Start monitoring
            self.system_monitor.start_monitoring()

            # Start decision coordination with loop limit for testing
            self.decision_coordinator.start_decision_coordination()

            self.status.state = SystemState.RUNNING
            self.logger.info("Trading system fully operational")

        except Exception as e:
            self.logger.error(f"Failed to start trading system: {e}")
            self.status.state = SystemState.ERROR
            self.stop_system()
            raise

    def stop_system(self, emergency: bool = False):
        """Stop the trading system gracefully or emergency"""
        if emergency:
            self.logger.warning("EMERGENCY SHUTDOWN INITIATED")
            self.status.state = SystemState.EMERGENCY_STOP
        else:
            self.logger.info("Graceful shutdown initiated")
            self.status.state = SystemState.STOPPING

        try:
            # Signal shutdown
            self._shutdown_event.set()

            # Stop utility modules
            self.system_monitor.stop_monitoring()
            self.decision_coordinator.stop_decision_coordination()
            self.symbol_coordination.stop_symbol_coordination()

            # Stop all threads
            self._stop_threads()

            # Stop all processes
            if self.process_manager:
                self.process_manager.stop_all_processes()

            # Final cleanup
            self._cleanup_resources()

            if not emergency:
                self.status.state = SystemState.STOPPED
                self.logger.info("System shutdown complete")
            else:
                self.logger.warning("Emergency shutdown complete - system in emergency state")

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        # Update system metrics first
        if self.system_monitor:
            self.system_monitor.update_system_metrics()

        # Update process status
        if self.process_manager:
            process_status = self.process_manager.get_process_status()
            self.status.total_processes = len(process_status)
            self.status.healthy_processes = sum(
                1 for p in process_status.values()
                if p.get('is_healthy', False)
            )
            self.status.failed_processes = self.status.total_processes - self.status.healthy_processes

        # Update symbol coordination status
        if self.symbol_coordination:
            self.symbol_coordination.update_symbol_coordination_status()

        return {
            'system_state': self.status.state.value,
            'trading_mode': self.status.trading_mode.value,
            'uptime_seconds': (datetime.now() - self.status.start_time).total_seconds(),
            'processes': {
                'total': self.status.total_processes,
                'healthy': self.status.healthy_processes,
                'failed': self.status.failed_processes
            },
            'trading': {
                'trades_today': self.status.trades_today,
                'total_pnl': self.status.total_pnl,
                'active_positions': self.status.active_positions
            },
            'account': {
                'portfolio_value': self.status.portfolio_value,
                'cash_available': self.status.cash_available,
                'buying_power': self.status.buying_power,
                'day_pnl': self.status.day_pnl,
                'day_pnl_pct': self.status.day_pnl_pct,
                'account_healthy': self.status.account_healthy,
                'account_health_message': self.status.account_health_message,
                'last_sync': self.status.last_account_sync.isoformat() if self.status.last_account_sync else None
            },
            'resources': {
                'memory_usage_mb': self.status.memory_usage_mb,
                'cpu_usage_percent': self.status.cpu_usage_percent,
                'api_calls_remaining': self.status.api_calls_remaining
            },
            'last_activity': {
                'decision': self.status.last_decision_time.isoformat() if self.status.last_decision_time else None,
                'trade': self.status.last_trade_time.isoformat() if self.status.last_trade_time else None,
                'health_check': self.status.last_health_check.isoformat() if self.status.last_health_check else None
            },
            'symbol_coordination': {
                'enabled': self.status.symbol_coordination_enabled,
                'total_symbols': self.status.total_symbols_managed,
                'intensive_symbols': self.status.intensive_symbols_count,
                'active_symbols': self.status.active_symbols_count,
                'passive_symbols': self.status.passive_symbols_count,
                'symbols_with_errors': self.status.symbols_with_errors,
                'last_update': self.status.last_symbol_coordination_update.isoformat() if self.status.last_symbol_coordination_update else None,
                'resource_status': self.resource_manager.get_system_resource_status() if self.resource_manager else {},
                'routing_status': self.message_router.get_routing_status() if self.message_router else {}
            }
        }

    def execute_trade_decision(self, symbol: str, decision_data: Dict[str, Any]) -> bool:
        """Execute a trade decision through the decision coordinator"""
        return self.decision_coordinator.execute_trade_decision(symbol, decision_data)

    def pause_trading(self):
        """Pause trading operations"""
        self.logger.info("Pausing trading operations")
        self.status.state = SystemState.PAUSING
        self.status.state = SystemState.PAUSED

    def resume_trading(self):
        """Resume trading operations"""
        self.logger.info("Resuming trading operations")
        self.status.state = SystemState.RUNNING

    def emergency_stop_all_trades(self):
        """Emergency stop all trading activity"""
        self.emergency_manager.emergency_stop_all_trades()

    # Private implementation methods

    def _start_main_loop(self):
        """Start the main orchestration loop"""
        self._main_thread = threading.Thread(target=self._main_orchestration_loop, daemon=True)
        self._main_thread.start()
        self.logger.info("Main orchestration loop started")

    def _main_orchestration_loop(self):
        """Main orchestration loop with iteration limit for testing"""
        iteration_count = 0
        max_iterations = 100  # Loop break for testing - remove in production

        while not self._shutdown_event.is_set() and iteration_count < max_iterations:
            try:
                # Update shared state
                if self.shared_state:
                    self.shared_state.set('system_status', self.get_system_status())
                    self.shared_state.set('last_update', datetime.now().isoformat())

                # Check for emergency conditions
                if self.emergency_manager and self.emergency_manager.check_emergency_conditions():
                    self.emergency_manager.emergency_stop_all_trades()
                    break

                # Brief sleep
                time.sleep(10)
                iteration_count += 1

            except Exception as e:
                self.logger.error(f"Error in main orchestration loop: {e}")
                time.sleep(30)

        if iteration_count >= max_iterations:
            self.logger.info(f"Main loop completed {max_iterations} iterations - stopping for testing")
            self._shutdown_event.set()

    def _stop_threads(self):
        """Stop all monitoring threads"""
        if self._main_thread and self._main_thread.is_alive():
            self._main_thread.join(timeout=10)
            if self._main_thread.is_alive():
                self.logger.warning("Main thread did not stop gracefully")

    def _cleanup_resources(self):
        """Final resource cleanup"""
        try:
            if self.shared_state:
                self.shared_state.cleanup()
        except Exception as e:
            self.logger.error(f"Error during resource cleanup: {e}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self.stop_system()


def create_main_orchestrator(config: Optional[SystemConfig] = None) -> MainOrchestrator:
    """Factory function to create a main orchestrator"""
    return MainOrchestrator(config)


def main():
    """Main entry point for running the trading system"""
    print("Starting Friren Trading System...")

    # Create configuration
    config = SystemConfig(
        trading_mode=TradingMode.PAPER_TRADING,  # Start in paper trading
        symbols=['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA'],
        max_positions=5,
        portfolio_value=100000.0
    )

    # Create and initialize orchestrator
    orchestrator = create_main_orchestrator(config)

    try:
        # Initialize and start system
        orchestrator.initialize_system()
        orchestrator.start_system()

        # Keep running until interrupted or loop completes
        while orchestrator.status.state == SystemState.RUNNING:
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"System error: {e}")
    finally:
        orchestrator.stop_system()
        print("System shutdown complete")


if __name__ == "__main__":
    main()
