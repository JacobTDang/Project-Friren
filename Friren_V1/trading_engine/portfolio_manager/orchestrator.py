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
import threading
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import the debug monitor
if project_root not in sys.path:
    sys.path.append(project_root)

# Add the debug monitor
try:
    from debug_monitor import DetailedDebugMonitor
except ImportError:
    # Fallback if import fails
    DetailedDebugMonitor = None

# Import color system for decision engine tasks
try:
    from terminal_color_system import print_decision_engine, print_communication, print_error, print_warning, print_success, create_colored_logger
    COLOR_SYSTEM_AVAILABLE = True
except ImportError:
    COLOR_SYSTEM_AVAILABLE = False

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

        # Setup enhanced logging first
        self.logger = self._setup_enhanced_logging()

        # Initialize detailed debug monitor
        self.debug_monitor = None
        if DetailedDebugMonitor:
            self.debug_monitor = DetailedDebugMonitor(self.config, self.status, self.logger)
            self.logger.info("=== DETAILED DEBUG MONITOR ACTIVATED ===")
            self.logger.info("Will show every action, queue state, and shared memory")
        else:
            self.logger.warning("Debug monitor not available - using basic logging")

        # Initialize utility modules
        self.system_monitor = SystemMonitor(self.config, self.status, self.logger)
        self.decision_coordinator = DecisionCoordinator(self.config, self.status, self.logger)
        self.emergency_manager = EmergencyManager(self.config, self.status, self.logger)
        self.process_initializer = ProcessInitializer(self.config, self.status, self.logger)
        self.symbol_coordination = SymbolCoordinationIntegration(self.config, self.status, self.logger)

        # Component references (initialized by process_initializer)
        self.process_manager = None
        self.execution_orchestrator = None
        self.redis_manager = None
        # queue_manager replaced by redis_manager
        self.account_manager = None
        self.symbol_coordinator = None
        self.resource_manager = None
        self.message_router = None

        # Control and monitoring
        self._shutdown_event = threading.Event()
        self._main_thread: Optional[threading.Thread] = None

        # Signal handlers are registered in main.py (main thread only)
        # This orchestrator responds to shutdown events via _shutdown_event

        self.logger.info("MainOrchestrator initialized with ENHANCED DEBUGGING")
        self.logger.info(f"Trading mode: {self.config.trading_mode.value}")
        self.logger.info(f"Symbols: {self.config.symbols}")

    def _setup_enhanced_logging(self) -> logging.Logger:
        """Setup comprehensive logging with detailed real-time output"""
        logger = logging.getLogger("main_orchestrator")
        logger.setLevel(logging.INFO)

        # Clear any existing handlers
        logger.handlers.clear()

        # Create logs directory if not exists
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

        # File handler with detailed formatting
        file_handler = logging.FileHandler(logs_dir / "main_orchestrator_debug.log")
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console handler with clean formatting (NO Unicode)
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # Set logging level to show everything
        logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.INFO)  # Console shows INFO and above
        file_handler.setLevel(logging.DEBUG)    # File shows everything

        return logger

    def initialize_system(self):
        """Initialize all system components with detailed logging and validation."""
        self.logger.info("=== INITIALIZING TRADING SYSTEM ===")
        self.status.state = SystemState.INITIALIZING

        try:
            self._initialize_core_components()
            self._initialize_symbol_coordination()
            self._validate_components()
            self._wire_components()
            self._sync_positions()
            self._setup_debug_monitor()
            self._final_validation()

            self.logger.info("=== SYSTEM INITIALIZATION COMPLETE ===")
            self.logger.info("All components initialized and wired successfully")

        except Exception as e:
            self.logger.critical(f"SYSTEM INITIALIZATION FAILED: {e}", exc_info=True)
            self.status.state = SystemState.ERROR
            raise

    def _initialize_core_components(self):
        """Initializes the core components of the trading system."""
        self.logger.info("STEP 1: Initializing core components...")
        try:
            self.process_initializer.initialize_all_components()
            components = self.process_initializer.get_components()
            self.process_manager = components.get('process_manager')
            self.account_manager = components.get('account_manager')
            self.execution_orchestrator = components.get('execution_orchestrator')
            self.redis_manager = components.get('redis_manager')
            self.logger.info("Core components initialized successfully.")
        except Exception as e:
            self.logger.critical(f"Error initializing core components: {e}", exc_info=True)
            raise

    def _initialize_symbol_coordination(self):
        """Initializes the symbol coordination components."""
        self.logger.info("STEP 2: Initializing symbol coordination...")
        try:
            self.symbol_coordination.initialize_symbol_coordination()
            symbol_components = self.symbol_coordination.get_components()
            self.symbol_coordinator = symbol_components.get('symbol_coordinator')
            self.resource_manager = symbol_components.get('resource_manager')
            self.message_router = symbol_components.get('message_router')
            self.logger.info("Symbol coordination components initialized successfully.")
        except Exception as e:
            self.logger.critical(f"Error initializing symbol coordination components: {e}", exc_info=True)
            raise

    def _validate_components(self):
        """Validates that all core components have been initialized."""
        self.logger.info("STEP 3: Validating component initialization...")
        if not all([self.process_manager, self.account_manager, self.execution_orchestrator, self.redis_manager,
                    self.symbol_coordinator, self.resource_manager, self.message_router]):
            raise RuntimeError("One or more core components failed to initialize.")
        self.logger.info("All core components initialized successfully.")

    def _sync_positions(self):
        """Syncs current positions to the symbol coordinator."""
        self.logger.info("STEP 5: Syncing positions to symbol coordinator...")
        try:
            sync_success = self.process_initializer.sync_positions_to_symbol_coordinator(self.symbol_coordinator)
            if not sync_success:
                self.logger.warning("Initial position sync to symbol coordinator failed. Retrying with manual sync.")
                self.symbol_coordinator.sync_positions_from_database(self.account_manager.db_manager)
        except Exception as e:
            self.logger.error(f"Exception during position sync: {e}", exc_info=True)
            # Attempt manual sync as a fallback
            try:
                self.symbol_coordinator.sync_positions_from_database(self.account_manager.db_manager)
            except Exception as manual_sync_e:
                self.logger.critical(f"Manual position sync also failed: {manual_sync_e}", exc_info=True)
                raise RuntimeError("Failed to sync positions to symbol coordinator.") from e

    def _setup_debug_monitor(self):
        """Sets up the debug monitor with all the initialized components."""
        if self.debug_monitor:
            self.logger.info("STEP 6: Setting up debug monitor...")
            self.debug_monitor.set_components(
                process_manager=self.process_manager,
                redis_manager=self.redis_manager,
                account_manager=self.account_manager,
                execution_orchestrator=self.execution_orchestrator,
                symbol_coordinator=self.symbol_coordinator,
                message_router=self.message_router
            )
            self.logger.info("Debug monitor setup complete.")

    def _final_validation(self):
        """Performs a final validation of the system state after initialization."""
        self.logger.info("STEP 7: Final validation of system state...")
        self._force_refresh_component_states()
        final_status = self.symbol_coordinator.get_coordination_status()
        active_count_final = sum(1 for data in final_status.get('symbols', {}).values() if data.get('position', 0) > 0)
        self.logger.info(f"FINAL VALIDATION: Active symbols count = {active_count_final}")

        if active_count_final == 0:
            self.logger.warning("[VALIDATION] Symbol Coordinator has no active positions. This may be expected if there are no current holdings.")
        else:
            self.logger.info("[VALIDATION] Symbol Coordinator is properly configured with active positions.")

    def _force_refresh_component_states(self):
        """Force refresh all component states to ensure consistency"""
        try:
            self.logger.info("REFRESH: Force refreshing component states...")
            
            # Refresh Symbol Coordinator state
            if self.symbol_coordinator:
                self.logger.info("REFRESH: Updating Symbol Coordinator state...")
                
                # Re-sync positions to ensure they're current
                if self.account_manager and self.account_manager.db_manager:
                    self.logger.info("REFRESH: Re-syncing positions from database...")
                    self.symbol_coordinator.sync_positions_from_database(self.account_manager.db_manager)
                    
                    # Validate the refresh worked
                    refresh_status = self.symbol_coordinator.get_coordination_status()
                    self.logger.info("REFRESH: Post-refresh Symbol Coordinator status:")
                    if 'symbols' in refresh_status:
                        for symbol, data in refresh_status['symbols'].items():
                            position = data.get('position', 0)
                            intensity = data.get('intensity', 'unknown')
                            self.logger.info(f"  {symbol}: position={position}, intensity={intensity}")
                            
                        # Count active symbols
                        active_count = sum(1 for data in refresh_status['symbols'].values() if data.get('position', 0) > 0)
                        self.logger.info(f"REFRESH: Active symbols count: {active_count}")
                        
                        if active_count > 0:
                            self.logger.info("REFRESH: [SUCCESS] Symbol Coordinator has active positions - message generation should work")
                        else:
                            self.logger.warning("REFRESH: [ISSUE] Symbol Coordinator has no active positions - investigating...")
                    else:
                        self.logger.warning("REFRESH: No symbols found in coordinator after refresh")
                else:
                    self.logger.warning("REFRESH: Cannot refresh - account manager or db_manager not available")
            else:
                self.logger.warning("REFRESH: Cannot refresh - symbol coordinator not available")
                
        except Exception as e:
            self.logger.error(f"REFRESH: Error during component state refresh: {e}")
            import traceback
            traceback.print_exc()

    def _wire_components(self):
        """Wire components between utility modules"""
        self.logger.info("WIRING: Setting up component connections...")

        # Set components for system monitor
        self.system_monitor.set_components(
            process_manager=self.process_manager,
            account_manager=self.account_manager,
            redis_manager=self.redis_manager,
            emergency_manager=self.emergency_manager,
            get_system_status_func=self.get_system_status
        )
        self.logger.info("WIRING: System monitor connected")

        # Set components for decision coordinator
        self.decision_coordinator.set_components(
            process_manager=self.process_manager,
            execution_orchestrator=self.execution_orchestrator,
            symbol_coordinator=self.symbol_coordinator,
            message_router=self.message_router
        )
        self.logger.info("WIRING: Decision coordinator connected")

    def start_system(self):
        """Start the complete trading system with a centralized event loop."""
        self.logger.info("=== STARTING TRADING SYSTEM ===")
        self.status.state = SystemState.STARTING

        try:
            if self.debug_monitor:
                self.logger.info("STARTUP STEP 1: Starting detailed debug monitoring...")
                self.debug_monitor.start_monitoring()

            self.logger.info("STARTUP STEP 2: Starting all trading processes...")
            self.process_initializer.start_processes()

            self.logger.info("STARTUP STEP 3: Waiting for processes to initialize...")
            time.sleep(5)  # Allow time for processes to start and report initial status

            self.logger.info("STARTUP STEP 4: Starting main event loop...")
            self._start_main_loop()  # This now starts the event loop

            self.logger.info("STARTUP STEP 5: Starting system monitoring...")
            self.system_monitor.start_monitoring()

            self.status.state = SystemState.RUNNING
            self.logger.info("=== TRADING SYSTEM FULLY OPERATIONAL ===")
            self._log_detailed_startup_status()

        except Exception as e:
            self.logger.critical(f"SYSTEM STARTUP FAILED: {e}", exc_info=True)
            self.status.state = SystemState.ERROR
            self.stop_system()
            raise

    def _log_detailed_startup_status(self):
        """Log detailed status after startup"""
        self.logger.info("")
        self.logger.info("=== STARTUP STATUS REPORT ===")

        try:
            # Process status
            if self.process_manager:
                process_status = self.process_manager.get_process_status()
                self.logger.info(f"PROCESSES: {len(process_status)} total")
                for pid, status in process_status.items():
                    running = "RUNNING" if status.get('is_running', False) else "STOPPED"
                    healthy = "HEALTHY" if status.get('is_healthy', False) else "UNHEALTHY"
                    self.logger.info(f"  -> {pid}: {running} | {healthy}")

            # Queue status
            if self.redis_manager:
                # Get Redis queue status using the proper method
                queue_status = self.redis_manager.get_queue_status()
                self.logger.info(f"QUEUES: {len(queue_status)} total")
                for qname, qstatus in queue_status.items():
                    # DEFENSIVE FIX: Handle case where qstatus might be an int instead of dict
                    if isinstance(qstatus, dict):
                        size = qstatus.get('size', 0)
                        self.logger.info(f"  -> {qname}: {size} messages")
                        if qstatus.get('message_types'):
                            self.logger.info(f"    Message types: {qstatus['message_types']}")
                    elif isinstance(qstatus, (int, float)):
                        # Fallback: qstatus is just the size
                        self.logger.info(f"  -> {qname}: {qstatus} messages")
                    else:
                        self.logger.warning(f"  -> {qname}: Unknown queue info format: {type(qstatus)}")

            # Shared state
            if self.redis_manager:
                # Get Redis system status instead of keys
                status = self.redis_manager.get_system_status()
                keys = list(status.get('system_state', {}).keys())
                self.logger.info(f"SHARED STATE: {len(keys)} keys stored")

            # Account status
            if self.account_manager and hasattr(self.account_manager, 'get_account_snapshot'):
                try:
                    snapshot = self.account_manager.get_account_snapshot()
                    self.logger.info(f"ACCOUNT: Portfolio Value=${snapshot.portfolio_value:.2f}")
                    self.logger.info(f"ACCOUNT: Cash Available=${snapshot.cash:.2f}")
                except Exception as e:
                    self.logger.info(f"ACCOUNT: Status check failed - {e}")

            # NEW: Check subprocess logs for recent activity
            self.logger.info("=== SUBPROCESS ACTIVITY CHECK ===")
            self._log_recent_subprocess_activity()

        except Exception as e:
            self.logger.error(f"STATUS REPORT ERROR: {e}")

        self.logger.info("=== END STARTUP STATUS REPORT ===")
        self.logger.info("")

    def _log_recent_subprocess_activity(self):
        """Check and log recent subprocess activity from log files"""
        import os
        import glob
        from datetime import datetime, timedelta

        subprocess_logs = [
            'logs/enhanced_news_pipeline_subprocess.log',
            'logs/sentiment_analyzer_subprocess.log',
            'logs/enhanced_news_collector.log',
            'logs/finbert_sentiment.log'
        ]

        recent_cutoff = datetime.now() - timedelta(minutes=5)

        for log_file in subprocess_logs:
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r') as f:
                        lines = f.readlines()
                        # Get last 5 lines to show recent activity
                        recent_lines = lines[-5:] if len(lines) >= 5 else lines

                        if recent_lines:
                            process_name = os.path.basename(log_file).replace('.log', '')
                            self.logger.info(f"SUBPROCESS {process_name.upper()}:")
                            for line in recent_lines:
                                clean_line = line.strip()
                                if clean_line and 'INFO' in clean_line:
                                    # Extract just the useful part of the log message
                                    if ' - ' in clean_line:
                                        msg_part = clean_line.split(' - ', 2)[-1]  # Get message after second dash
                                        self.logger.info(f"  -> {msg_part}")

                except Exception as e:
                    self.logger.warning(f"Could not read {log_file}: {e}")

        # Also log news collection forcing status
        self.logger.info("NEWS COLLECTION: Forced immediate execution enabled for testing")

    def stop_system(self, emergency: bool = False):
        """Stop the trading system gracefully"""
        if emergency:
            self.logger.critical("=== EMERGENCY SYSTEM SHUTDOWN ===")
        else:
            self.logger.info("=== GRACEFUL SYSTEM SHUTDOWN ===")

        self.status.state = SystemState.STOPPING

        try:
            # Stop debug monitoring
            if self.debug_monitor:
                self.logger.info("SHUTDOWN: Stopping debug monitor...")
                self.debug_monitor.stop_monitoring()

            # Emergency stop first if needed
            if emergency and self.emergency_manager:
                self.logger.critical("EMERGENCY: Executing emergency procedures...")
                self.emergency_manager.emergency_stop_all_trades()

            # Stop monitoring threads
            self.logger.info("SHUTDOWN: Stopping monitoring threads...")
            self._stop_threads()

            # Stop all processes
            if self.process_manager:
                self.logger.info("SHUTDOWN: Stopping all trading processes...")
                self.process_manager.stop_all_processes()

            # Cleanup resources
            self.logger.info("SHUTDOWN: Cleaning up resources...")
            self._cleanup_resources()

            self.status.state = SystemState.STOPPED
            self.logger.info("=== SYSTEM SHUTDOWN COMPLETE ===")

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
            self.status.state = SystemState.ERROR

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        system_status = {
            'system_state': self.status.state.value,
            'timestamp': datetime.now().isoformat(),
            'uptime_seconds': (datetime.now() - self.status.start_time).total_seconds() if self.status.start_time else 0,
            'trading': {
                'mode': self.config.trading_mode.value,
                'symbols': self.config.symbols,
                'trades_today': self.status.trades_today,
                'total_pnl': self.status.total_pnl,
                'portfolio_value': self.status.portfolio_value,
                'cash_available': self.status.cash_available,
                'last_trade_time': self.status.last_trade_time.isoformat() if self.status.last_trade_time else None
            },
            'processes': {
                'total': self.status.total_processes,
                'healthy': self.status.healthy_processes,
                'failed': self.status.failed_processes
            },
            'resources': {
                'memory_usage_mb': self.status.memory_usage_mb,
                'cpu_usage_percent': self.status.cpu_usage_percent
            },
            'account': {
                'healthy': self.status.account_healthy,
                'last_sync': self.status.last_account_sync.isoformat() if self.status.last_account_sync else None,
                'health_message': self.status.account_health_message
            },
            'last_health_check': self.status.last_health_check.isoformat() if self.status.last_health_check else None
        }

        # Add detailed process information if available
        if self.process_manager and hasattr(self.process_manager, 'get_process_status'):
            try:
                process_details = self.process_manager.get_process_status()
                system_status['process_details'] = process_details
            except Exception as e:
                system_status['process_details'] = f"Error getting process details: {e}"

        return system_status

    def execute_trade_decision(self, symbol: str, decision_data: Dict[str, Any]) -> bool:
        """Execute a trade decision with detailed logging"""
        self.logger.info(f"TRADE EXECUTION: Starting for {symbol}")

        # Log decision to debug monitor
        if self.debug_monitor:
            decision_type = decision_data.get('action', 'unknown')
            details = f"Size: {decision_data.get('size', 'N/A')}, Confidence: {decision_data.get('confidence', 'N/A')}"
            self.debug_monitor.log_decision(symbol, decision_type, details)

        # Execute through execution orchestrator
        if self.execution_orchestrator:
            try:
                result = self.execution_orchestrator.execute_trade(symbol, decision_data)
                self.logger.info(f"TRADE EXECUTION: {symbol} - {'SUCCESS' if result else 'FAILED'}")
                return result
            except Exception as e:
                self.logger.error(f"TRADE EXECUTION ERROR: {symbol} - {e}")
                if self.debug_monitor:
                    self.debug_monitor.log_error("trade_execution", f"{symbol}: {e}")
                return False

        self.logger.warning("TRADE EXECUTION: No execution orchestrator available")
        return False

    def pause_trading(self):
        """Pause trading activities"""
        self.logger.info("TRADING PAUSED by user request")
        self.status.state = SystemState.PAUSED

    def resume_trading(self):
        """Resume trading activities"""
        self.logger.info("TRADING RESUMED by user request")
        self.status.state = SystemState.RUNNING

    def emergency_stop_all_trades(self):
        """Emergency stop all trading activities"""
        self.logger.critical("EMERGENCY STOP: All trading halted immediately")
        if self.emergency_manager:
            self.emergency_manager.emergency_stop_all_trades()

    def _start_main_loop(self):
        """Start main orchestration loop"""
        self._main_thread = threading.Thread(target=self._main_orchestration_loop, daemon=True)
        self._main_thread.start()
        self.logger.info("MAIN LOOP: Orchestration thread started")

    def _main_orchestration_loop(self):
        """Main event loop for the orchestrator."""
        self.logger.info("Main event loop started.")
        while not self._shutdown_event.is_set():
            try:
                # Block until a message is available or timeout occurs
                message = self.redis_manager.get_message(block=True, timeout=1.0)
                if message:
                    self._process_message(message)

                # Perform periodic tasks
                self._periodic_tasks()

            except Exception as e:
                self.logger.error(f"Error in main event loop: {e}", exc_info=True)
                time.sleep(5) # Avoid rapid-fire logging in case of a persistent error

        self.logger.info("Main event loop stopped.")

    def _process_message(self, message: Dict[str, Any]):
        """Processes a single message from the message queue."""
        message_type = message.get('type')
        self.logger.debug(f"Processing message of type: {message_type}")

        if message_type == 'decision_request':
            self.decision_coordinator.handle_decision_request(message.get('data'))
        elif message_type == 'trade_execution_result':
            self.decision_coordinator.handle_execution_result(message.get('data'))
        elif message_type == 'health_update':
            self.system_monitor.handle_health_update(message.get('data'))
        elif message_type == 'COLORED_OUTPUT':
            # Handle colored output from subprocesses
            self._handle_colored_output(message.get('data', {}))
        else:
            self.logger.warning(f"Unknown message type received: {message_type}")

    def _handle_colored_output(self, data: Dict[str, Any]):
        """Handle colored output messages from subprocesses"""
        try:
            output_text = data.get('output', '')
            color_type = data.get('color_type', '')
            
            if output_text:
                # Import colored print functions
                if COLOR_SYSTEM_AVAILABLE:
                    if 'news' in color_type.lower():
                        from terminal_color_system import print_news_collection
                        print_news_collection(output_text)
                    elif 'decision' in color_type.lower():
                        from terminal_color_system import print_decision_engine
                        print_decision_engine(output_text)
                    elif 'position' in color_type.lower():
                        from terminal_color_system import print_position_monitor
                        print_position_monitor(output_text)
                    elif 'sentiment' in color_type.lower() or 'finbert' in color_type.lower():
                        from terminal_color_system import print_finbert_analysis
                        print_finbert_analysis(output_text)
                    else:
                        from terminal_color_system import print_communication
                        print_communication(output_text)
                else:
                    # Fallback to regular print if color system not available
                    print(f"[SUBPROCESS] {output_text}")
                    
        except Exception as e:
            self.logger.error(f"Error handling colored output: {e}")

    def _periodic_tasks(self):
        """Executes periodic tasks such as health checks."""
        # This is where you can add logic that needs to run periodically
        # For example, checking for market open/close, etc.
        
        # Check for terminal output messages
        if self.redis_manager:
            try:
                terminal_msg = self.redis_manager.get_message_from_queue("terminal_output", block=False)
                if terminal_msg:
                    self._handle_colored_output(terminal_msg.get('data', {}))
            except Exception as e:
                # Ignore errors for periodic tasks
                pass

    

    def _stop_threads(self):
        """Stop all monitoring threads"""
        self._shutdown_event.set()
        if self.system_monitor:
            self.system_monitor.stop_monitoring()

    def _cleanup_resources(self):
        """Cleanup system resources"""
        if self.redis_manager:
            self.redis_manager.stop_cleanup_thread()
        self.logger.info("CLEANUP: System resources cleaned up")


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
